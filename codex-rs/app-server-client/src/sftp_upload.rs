use std::io::Error as IoError;
use std::io::ErrorKind;
use std::io::Result as IoResult;

use crate::remote::RemoteAppServerRequestHandle;

const SSH_FXP_INIT: u8 = 1;
const SSH_FXP_VERSION: u8 = 2;
const SSH_FXP_OPEN: u8 = 3;
const SSH_FXP_CLOSE: u8 = 4;
const SSH_FXP_WRITE: u8 = 6;
const SSH_FXP_STATUS: u8 = 101;
const SSH_FXP_HANDLE: u8 = 102;

const SSH_FX_OK: u32 = 0;
const SSH_FXF_WRITE: u32 = 0x0000_0002;
const SSH_FXF_CREAT: u32 = 0x0000_0008;
const SSH_FXF_TRUNC: u32 = 0x0000_0010;
const SFTP_VERSION: u32 = 3;
const SFTP_WRITE_CHUNK_BYTES: usize = 64 * 1024;
const MAX_SFTP_PACKET_BYTES: usize = 1024 * 1024;

pub(crate) async fn upload_file_over_sftp(
    handle: &RemoteAppServerRequestHandle,
    path: &str,
    bytes: &[u8],
) -> IoResult<()> {
    let mut packets = PacketReader::default();

    handle.send_binary(init_packet()).await?;
    expect_version(&packets.next_packet(handle).await?)?;

    let mut request_id = 1;
    handle
        .send_binary(open_packet(
            request_id,
            path,
            SSH_FXF_WRITE | SSH_FXF_CREAT | SSH_FXF_TRUNC,
        ))
        .await?;
    let file_handle = expect_handle(&packets.next_packet(handle).await?, request_id)?;

    for (chunk_index, chunk) in bytes.chunks(SFTP_WRITE_CHUNK_BYTES).enumerate() {
        request_id += 1;
        let offset = chunk_index
            .checked_mul(SFTP_WRITE_CHUNK_BYTES)
            .and_then(|offset| u64::try_from(offset).ok())
            .ok_or_else(|| IoError::other("staged upload offset overflowed"))?;
        handle
            .send_binary(write_packet(request_id, &file_handle, offset, chunk))
            .await?;
        expect_ok_status(&packets.next_packet(handle).await?, request_id)?;
    }

    request_id += 1;
    handle
        .send_binary(close_packet(request_id, &file_handle))
        .await?;
    expect_ok_status(&packets.next_packet(handle).await?, request_id)
}

#[derive(Default)]
struct PacketReader {
    pending_bytes: Vec<u8>,
}

impl PacketReader {
    async fn next_packet(&mut self, handle: &RemoteAppServerRequestHandle) -> IoResult<Vec<u8>> {
        loop {
            if let Some(packet) = take_packet(&mut self.pending_bytes)? {
                return Ok(packet);
            }
            self.pending_bytes.extend(handle.next_binary().await?);
        }
    }
}

fn take_packet(buffer: &mut Vec<u8>) -> IoResult<Option<Vec<u8>>> {
    if buffer.len() < 4 {
        return Ok(None);
    }
    let packet_len = u32::from_be_bytes(
        buffer[..4]
            .try_into()
            .map_err(|_| invalid_packet("invalid SFTP packet length prefix"))?,
    ) as usize;
    if packet_len > MAX_SFTP_PACKET_BYTES {
        return Err(invalid_packet("SFTP packet exceeds 1 MiB"));
    }
    if buffer.len() < packet_len + 4 {
        return Ok(None);
    }
    let packet = buffer[4..packet_len + 4].to_vec();
    buffer.drain(..packet_len + 4);
    Ok(Some(packet))
}

fn init_packet() -> Vec<u8> {
    packet(|payload| {
        payload.push(SSH_FXP_INIT);
        payload.extend_from_slice(&SFTP_VERSION.to_be_bytes());
    })
}

fn open_packet(request_id: u32, path: &str, flags: u32) -> Vec<u8> {
    packet(|payload| {
        payload.push(SSH_FXP_OPEN);
        payload.extend_from_slice(&request_id.to_be_bytes());
        push_string(payload, path.as_bytes());
        payload.extend_from_slice(&flags.to_be_bytes());
        payload.extend_from_slice(&0_u32.to_be_bytes());
    })
}

fn write_packet(request_id: u32, handle: &[u8], offset: u64, data: &[u8]) -> Vec<u8> {
    packet(|payload| {
        payload.push(SSH_FXP_WRITE);
        payload.extend_from_slice(&request_id.to_be_bytes());
        push_string(payload, handle);
        payload.extend_from_slice(&offset.to_be_bytes());
        push_string(payload, data);
    })
}

fn close_packet(request_id: u32, handle: &[u8]) -> Vec<u8> {
    packet(|payload| {
        payload.push(SSH_FXP_CLOSE);
        payload.extend_from_slice(&request_id.to_be_bytes());
        push_string(payload, handle);
    })
}

fn expect_version(packet: &[u8]) -> IoResult<()> {
    let mut cursor = PacketCursor::new(packet);
    if cursor.read_u8()? != SSH_FXP_VERSION {
        return Err(invalid_packet(
            "staged upload returned a non-version response",
        ));
    }
    let version = cursor.read_u32()?;
    cursor.finish()?;
    if version != SFTP_VERSION {
        return Err(invalid_packet(format!(
            "staged upload returned SFTP version {version}"
        )));
    }
    Ok(())
}

fn expect_handle(packet: &[u8], request_id: u32) -> IoResult<Vec<u8>> {
    let mut cursor = PacketCursor::new(packet);
    if cursor.read_u8()? != SSH_FXP_HANDLE {
        return Err(invalid_packet(
            "staged upload returned a non-handle response",
        ));
    }
    expect_request_id(&mut cursor, request_id)?;
    let handle = cursor.read_string()?;
    cursor.finish()?;
    Ok(handle)
}

fn expect_ok_status(packet: &[u8], request_id: u32) -> IoResult<()> {
    let mut cursor = PacketCursor::new(packet);
    if cursor.read_u8()? != SSH_FXP_STATUS {
        return Err(invalid_packet(
            "staged upload returned a non-status response",
        ));
    }
    expect_request_id(&mut cursor, request_id)?;
    let status = cursor.read_u32()?;
    let message = cursor.read_string_utf8()?;
    let _language = cursor.read_string_utf8()?;
    cursor.finish()?;
    if status != SSH_FX_OK {
        return Err(invalid_packet(format!(
            "staged upload returned status {status}: {message}"
        )));
    }
    Ok(())
}

fn expect_request_id(cursor: &mut PacketCursor<'_>, request_id: u32) -> IoResult<()> {
    let response_request_id = cursor.read_u32()?;
    if response_request_id != request_id {
        return Err(invalid_packet(format!(
            "staged upload returned response {response_request_id} for request {request_id}"
        )));
    }
    Ok(())
}

fn packet(fill_payload: impl FnOnce(&mut Vec<u8>)) -> Vec<u8> {
    let mut payload = Vec::new();
    fill_payload(&mut payload);
    let mut packet = Vec::with_capacity(payload.len() + 4);
    packet.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    packet.extend_from_slice(&payload);
    packet
}

fn push_string(buffer: &mut Vec<u8>, value: &[u8]) {
    buffer.extend_from_slice(&(value.len() as u32).to_be_bytes());
    buffer.extend_from_slice(value);
}

struct PacketCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> PacketCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_u8(&mut self) -> IoResult<u8> {
        let value = *self
            .bytes
            .get(self.offset)
            .ok_or_else(|| invalid_packet("unexpected end of SFTP packet"))?;
        self.offset += 1;
        Ok(value)
    }

    fn read_u32(&mut self) -> IoResult<u32> {
        let bytes = self.read_bytes(/*len*/ 4)?;
        Ok(u32::from_be_bytes(
            bytes
                .try_into()
                .map_err(|_| invalid_packet("invalid u32 field"))?,
        ))
    }

    fn read_string(&mut self) -> IoResult<Vec<u8>> {
        let len = self.read_u32()? as usize;
        Ok(self.read_bytes(len)?.to_vec())
    }

    fn read_string_utf8(&mut self) -> IoResult<String> {
        String::from_utf8(self.read_string()?)
            .map_err(|_| invalid_packet("invalid UTF-8 string in SFTP packet"))
    }

    fn read_bytes(&mut self, len: usize) -> IoResult<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| invalid_packet("SFTP packet offset overflowed"))?;
        let bytes = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| invalid_packet("unexpected end of SFTP packet"))?;
        self.offset = end;
        Ok(bytes)
    }

    fn finish(&self) -> IoResult<()> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(invalid_packet("trailing bytes in SFTP packet"))
        }
    }
}

fn invalid_packet(message: impl Into<String>) -> IoError {
    IoError::new(ErrorKind::InvalidData, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn take_packet_waits_for_a_complete_binary_chunk() {
        let mut bytes = vec![0, 0, 0, 2, 7];
        assert_eq!(
            take_packet(&mut bytes).expect("partial packet should parse"),
            None
        );

        bytes.push(8);
        assert_eq!(
            take_packet(&mut bytes).expect("complete packet should parse"),
            Some(vec![7, 8])
        );
        assert!(bytes.is_empty());
    }
}
