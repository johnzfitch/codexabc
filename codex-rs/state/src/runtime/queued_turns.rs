use super::*;
use uuid::Uuid;

impl StateRuntime {
    pub async fn list_thread_queued_turns(
        &self,
        thread_id: ThreadId,
    ) -> anyhow::Result<Vec<crate::ThreadQueuedTurn>> {
        let rows = sqlx::query(
            r#"
SELECT
    queued_turn_id,
    thread_id,
    turn_start_params_json,
    queue_order,
    last_dispatch_error,
    created_at_ms
FROM thread_queued_turns
WHERE thread_id = ?
ORDER BY queue_order ASC
            "#,
        )
        .bind(thread_id.to_string())
        .fetch_all(self.pool.as_ref())
        .await?;

        rows.iter().map(thread_queued_turn_from_row).collect()
    }

    pub async fn append_thread_queued_turn(
        &self,
        thread_id: ThreadId,
        turn_start_params_json: String,
    ) -> anyhow::Result<crate::ThreadQueuedTurn> {
        let queued_turn_id = Uuid::new_v4().to_string();
        let created_at_ms = datetime_to_epoch_millis(Utc::now());
        let row = sqlx::query(
            r#"
INSERT INTO thread_queued_turns (
    queued_turn_id,
    thread_id,
    turn_start_params_json,
    queue_order,
    created_at_ms
)
VALUES (
    ?,
    ?,
    ?,
    COALESCE((SELECT MAX(queue_order) + 1 FROM thread_queued_turns WHERE thread_id = ?), 0),
    ?
)
RETURNING
    queued_turn_id,
    thread_id,
    turn_start_params_json,
    queue_order,
    last_dispatch_error,
    created_at_ms
            "#,
        )
        .bind(queued_turn_id)
        .bind(thread_id.to_string())
        .bind(turn_start_params_json)
        .bind(thread_id.to_string())
        .bind(created_at_ms)
        .fetch_one(self.pool.as_ref())
        .await?;

        thread_queued_turn_from_row(&row)
    }

    pub async fn delete_thread_queued_turn(
        &self,
        thread_id: ThreadId,
        queued_turn_id: &str,
    ) -> anyhow::Result<bool> {
        Ok(sqlx::query(
            r#"
DELETE FROM thread_queued_turns
WHERE thread_id = ? AND queued_turn_id = ?
            "#,
        )
        .bind(thread_id.to_string())
        .bind(queued_turn_id)
        .execute(self.pool.as_ref())
        .await?
        .rows_affected()
            > 0)
    }

    pub async fn reorder_thread_queued_turns(
        &self,
        thread_id: ThreadId,
        ordered_queued_turn_ids: &[String],
    ) -> anyhow::Result<()> {
        let current = self.list_thread_queued_turns(thread_id).await?;
        let current_ids = current
            .iter()
            .map(|turn| turn.queued_turn_id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        let next_ids = ordered_queued_turn_ids
            .iter()
            .map(String::as_str)
            .collect::<std::collections::BTreeSet<_>>();
        if current.len() != ordered_queued_turn_ids.len() || current_ids != next_ids {
            anyhow::bail!("queued turn reorder ids must match the current queue");
        }

        let mut tx = self.pool.begin().await?;
        for (queue_order, queued_turn_id) in ordered_queued_turn_ids.iter().enumerate() {
            sqlx::query(
                r#"
UPDATE thread_queued_turns
SET queue_order = ?
WHERE thread_id = ? AND queued_turn_id = ?
                "#,
            )
            .bind(-(queue_order as i64) - 1)
            .bind(thread_id.to_string())
            .bind(queued_turn_id)
            .execute(&mut *tx)
            .await?;
        }
        for (queue_order, queued_turn_id) in ordered_queued_turn_ids.iter().enumerate() {
            sqlx::query(
                r#"
UPDATE thread_queued_turns
SET queue_order = ?
WHERE thread_id = ? AND queued_turn_id = ?
                "#,
            )
            .bind(queue_order as i64)
            .bind(thread_id.to_string())
            .bind(queued_turn_id)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn first_dispatchable_thread_queued_turn(
        &self,
        thread_id: ThreadId,
    ) -> anyhow::Result<Option<crate::ThreadQueuedTurn>> {
        let row = sqlx::query(
            r#"
SELECT
    queued_turn_id,
    thread_id,
    turn_start_params_json,
    queue_order,
    last_dispatch_error,
    created_at_ms
FROM thread_queued_turns
WHERE thread_id = ?
ORDER BY queue_order ASC
LIMIT 1
            "#,
        )
        .bind(thread_id.to_string())
        .fetch_optional(self.pool.as_ref())
        .await?;

        let queued_turn = row
            .map(|row| thread_queued_turn_from_row(&row))
            .transpose()?;
        Ok(queued_turn.filter(|turn| turn.last_dispatch_error.is_none()))
    }

    pub async fn set_thread_queued_turn_dispatch_error(
        &self,
        thread_id: ThreadId,
        queued_turn_id: &str,
        error: Option<&str>,
    ) -> anyhow::Result<bool> {
        Ok(sqlx::query(
            r#"
UPDATE thread_queued_turns
SET last_dispatch_error = ?
WHERE thread_id = ? AND queued_turn_id = ?
            "#,
        )
        .bind(error)
        .bind(thread_id.to_string())
        .bind(queued_turn_id)
        .execute(self.pool.as_ref())
        .await?
        .rows_affected()
            > 0)
    }
}

fn thread_queued_turn_from_row(
    row: &sqlx::sqlite::SqliteRow,
) -> anyhow::Result<crate::ThreadQueuedTurn> {
    crate::model::ThreadQueuedTurnRow::try_from_row(row)?.try_into()
}

#[cfg(test)]
mod tests {
    use super::super::test_support::test_thread_metadata;
    use super::super::test_support::unique_temp_dir;
    use super::*;
    use pretty_assertions::assert_eq;

    #[tokio::test]
    async fn append_list_reorder_and_delete_thread_queued_turns() {
        let codex_home = unique_temp_dir();
        let runtime = StateRuntime::init(codex_home.clone(), "mock".to_string())
            .await
            .expect("state db should initialize");
        let thread_id = ThreadId::new();
        let metadata = test_thread_metadata(&codex_home, thread_id, codex_home.clone());
        runtime
            .upsert_thread(&metadata)
            .await
            .expect("thread metadata insert should succeed");

        let first = runtime
            .append_thread_queued_turn(thread_id, "{\"first\":true}".to_string())
            .await
            .expect("first queued turn insert should succeed");
        let second = runtime
            .append_thread_queued_turn(thread_id, "{\"second\":true}".to_string())
            .await
            .expect("second queued turn insert should succeed");
        assert_eq!(
            runtime
                .list_thread_queued_turns(thread_id)
                .await
                .expect("queued turn list should succeed"),
            vec![first.clone(), second.clone()]
        );

        runtime
            .reorder_thread_queued_turns(
                thread_id,
                &[second.queued_turn_id.clone(), first.queued_turn_id.clone()],
            )
            .await
            .expect("queued turn reorder should succeed");
        assert_eq!(
            runtime
                .list_thread_queued_turns(thread_id)
                .await
                .expect("queued turn list should succeed")
                .into_iter()
                .map(|turn| turn.queued_turn_id)
                .collect::<Vec<_>>(),
            vec![second.queued_turn_id.clone(), first.queued_turn_id.clone()]
        );

        assert!(
            runtime
                .reorder_thread_queued_turns(
                    thread_id,
                    &[second.queued_turn_id.clone(), second.queued_turn_id.clone()],
                )
                .await
                .is_err()
        );

        assert!(
            runtime
                .delete_thread_queued_turn(thread_id, second.queued_turn_id.as_str())
                .await
                .expect("queued turn delete should succeed")
        );
        assert_eq!(
            runtime
                .list_thread_queued_turns(thread_id)
                .await
                .expect("queued turn list should succeed")
                .into_iter()
                .map(|turn| turn.queued_turn_id)
                .collect::<Vec<_>>(),
            vec![first.queued_turn_id]
        );
    }

    #[tokio::test]
    async fn dispatch_error_hides_turn_until_cleared() {
        let codex_home = unique_temp_dir();
        let runtime = StateRuntime::init(codex_home.clone(), "mock".to_string())
            .await
            .expect("state db should initialize");
        let thread_id = ThreadId::new();
        let metadata = test_thread_metadata(&codex_home, thread_id, codex_home.clone());
        runtime
            .upsert_thread(&metadata)
            .await
            .expect("thread metadata insert should succeed");
        let queued_turn = runtime
            .append_thread_queued_turn(thread_id, "{}".to_string())
            .await
            .expect("queued turn insert should succeed");
        runtime
            .append_thread_queued_turn(thread_id, "{\"later\":true}".to_string())
            .await
            .expect("later queued turn insert should succeed");

        assert!(
            runtime
                .set_thread_queued_turn_dispatch_error(
                    thread_id,
                    queued_turn.queued_turn_id.as_str(),
                    Some("failed"),
                )
                .await
                .expect("dispatch error update should succeed")
        );
        assert_eq!(
            runtime
                .first_dispatchable_thread_queued_turn(thread_id)
                .await
                .expect("dispatchable queued turn read should succeed"),
            None
        );
        assert!(
            runtime
                .set_thread_queued_turn_dispatch_error(
                    thread_id,
                    queued_turn.queued_turn_id.as_str(),
                    /*error*/ None,
                )
                .await
                .expect("dispatch error clear should succeed")
        );
        assert_eq!(
            runtime
                .first_dispatchable_thread_queued_turn(thread_id)
                .await
                .expect("dispatchable queued turn read should succeed")
                .map(|turn| turn.queued_turn_id),
            Some(queued_turn.queued_turn_id)
        );
    }
}
