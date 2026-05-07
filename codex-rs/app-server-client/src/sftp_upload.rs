use crate::remote::RemoteAppServerRequestHandle;
use russh_sftp::client::SftpSession;
use std::io::Error as IoError;
use std::io::Result as IoResult;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::task::JoinError;

const SFTP_STREAM_BUFFER_BYTES: usize = 64 * 1024;

pub(crate) async fn upload_file_over_sftp(
    handle: &RemoteAppServerRequestHandle,
    path: &str,
    bytes: &[u8],
) -> IoResult<()> {
    let (sftp_stream, bridge_stream) = tokio::io::duplex(SFTP_STREAM_BUFFER_BYTES);
    let (mut bridge_reader, mut bridge_writer) = tokio::io::split(bridge_stream);

    let outbound_handle = handle.clone();
    let mut outbound_task = tokio::spawn(async move {
        let mut buffer = vec![0; SFTP_STREAM_BUFFER_BYTES];
        loop {
            match bridge_reader.read(&mut buffer).await? {
                0 => return Ok(()),
                bytes_read => {
                    outbound_handle
                        .send_binary(buffer[..bytes_read].to_vec())
                        .await?;
                }
            }
        }
    });

    let inbound_handle = handle.clone();
    let mut inbound_task = tokio::spawn(async move {
        loop {
            let bytes = inbound_handle.next_binary().await?;
            bridge_writer.write_all(&bytes).await?;
        }
    });

    let upload = async {
        let session = SftpSession::new(sftp_stream).await.map_err(sftp_error)?;
        let mut file = session.create(path).await.map_err(sftp_error)?;
        file.write_all(bytes).await?;
        file.shutdown().await?;
        session.close().await.map_err(sftp_error)
    };
    tokio::pin!(upload);

    let result = tokio::select! {
        result = &mut upload => result,
        result = &mut outbound_task => bridge_result("send", result),
        result = &mut inbound_task => bridge_result("receive", result),
    };
    outbound_task.abort();
    inbound_task.abort();
    result
}

fn sftp_error(err: russh_sftp::client::error::Error) -> IoError {
    IoError::other(err.to_string())
}

fn bridge_result(direction: &str, result: Result<IoResult<()>, JoinError>) -> IoResult<()> {
    match result {
        Ok(result) => result,
        Err(err) => Err(IoError::other(format!(
            "staged upload {direction} bridge failed: {err}"
        ))),
    }
}
