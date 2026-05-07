use anyhow::Result;
use chrono::DateTime;
use chrono::Utc;
use codex_protocol::ThreadId;
use sqlx::Row;
use sqlx::sqlite::SqliteRow;

use super::epoch_millis_to_datetime;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadQueuedTurn {
    pub queued_turn_id: String,
    pub thread_id: ThreadId,
    pub turn_start_params_json: String,
    pub queue_order: i64,
    pub last_dispatch_error: Option<String>,
    pub created_at: DateTime<Utc>,
}

pub(crate) struct ThreadQueuedTurnRow {
    pub queued_turn_id: String,
    pub thread_id: String,
    pub turn_start_params_json: String,
    pub queue_order: i64,
    pub last_dispatch_error: Option<String>,
    pub created_at_ms: i64,
}

impl ThreadQueuedTurnRow {
    pub(crate) fn try_from_row(row: &SqliteRow) -> Result<Self> {
        Ok(Self {
            queued_turn_id: row.try_get("queued_turn_id")?,
            thread_id: row.try_get("thread_id")?,
            turn_start_params_json: row.try_get("turn_start_params_json")?,
            queue_order: row.try_get("queue_order")?,
            last_dispatch_error: row.try_get("last_dispatch_error")?,
            created_at_ms: row.try_get("created_at_ms")?,
        })
    }
}

impl TryFrom<ThreadQueuedTurnRow> for ThreadQueuedTurn {
    type Error = anyhow::Error;

    fn try_from(row: ThreadQueuedTurnRow) -> Result<Self> {
        Ok(Self {
            queued_turn_id: row.queued_turn_id,
            thread_id: ThreadId::try_from(row.thread_id)?,
            turn_start_params_json: row.turn_start_params_json,
            queue_order: row.queue_order,
            last_dispatch_error: row.last_dispatch_error,
            created_at: epoch_millis_to_datetime(row.created_at_ms)?,
        })
    }
}
