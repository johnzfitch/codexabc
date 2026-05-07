use super::*;
use codex_app_server_protocol::QueuedTurn;
use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::ThreadQueueAddParams;
use codex_app_server_protocol::ThreadQueueAddResponse;
use codex_app_server_protocol::ThreadQueueChangedNotification;
use codex_app_server_protocol::ThreadQueueDeleteParams;
use codex_app_server_protocol::ThreadQueueDeleteResponse;
use codex_app_server_protocol::ThreadQueueListParams;
use codex_app_server_protocol::ThreadQueueListResponse;
use codex_app_server_protocol::ThreadQueueReorderParams;
use codex_app_server_protocol::ThreadQueueReorderResponse;

#[derive(Clone)]
pub(crate) struct ThreadQueueRequestProcessor {
    thread_manager: Arc<ThreadManager>,
    outgoing: Arc<OutgoingMessageSender>,
    config: Arc<Config>,
    thread_state_manager: ThreadStateManager,
    state_db: StateDbHandle,
    turn_processor: TurnRequestProcessor,
    thread_queue_locks: Arc<Mutex<HashMap<ThreadId, Arc<Mutex<()>>>>>,
}

impl ThreadQueueRequestProcessor {
    pub(crate) fn new(
        thread_manager: Arc<ThreadManager>,
        outgoing: Arc<OutgoingMessageSender>,
        config: Arc<Config>,
        thread_state_manager: ThreadStateManager,
        state_db: StateDbHandle,
        turn_processor: TurnRequestProcessor,
    ) -> Self {
        Self {
            thread_manager,
            outgoing,
            config,
            thread_state_manager,
            state_db,
            turn_processor,
            thread_queue_locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    #[expect(
        clippy::await_holding_invalid_type,
        reason = "queue mutation and opportunistic drain must stay serialized per thread"
    )]
    pub(crate) async fn thread_queue_add(
        &self,
        params: ThreadQueueAddParams,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        TurnRequestProcessor::validate_v2_input_limit(&params.turn_start_params.input)?;
        let thread_id = parse_thread_id_for_request(params.thread_id.as_str())?;
        let thread_queue_lock = self.thread_queue_lock(thread_id).await;
        let _thread_queue_guard = thread_queue_lock.lock().await;
        let state_db = self.state_db_for_materialized_thread(thread_id).await?;
        let turn_start_params_json =
            serde_json::to_string(&params.turn_start_params).map_err(|err| {
                internal_error(format!("failed to serialize queued turn params: {err}"))
            })?;
        let queued_turn = state_db
            .append_thread_queued_turn(thread_id, turn_start_params_json)
            .await
            .map_err(|err| internal_error(format!("failed to add queued turn: {err}")))?;
        let queued_turn = api_queued_turn_from_state(queued_turn)
            .map_err(|err| internal_error(format!("failed to decode queued turn params: {err}")))?;
        let queued_turns = read_api_thread_queue(&state_db, thread_id).await?;
        self.emit_thread_queue_changed_ordered(thread_id, queued_turns)
            .await;
        self.drain_thread_queue_if_idle_locked(thread_id, &state_db)
            .await;
        Ok(Some(
            ThreadQueueAddResponse {
                queued_turn: queued_turn.clone(),
            }
            .into(),
        ))
    }

    pub(crate) async fn thread_queue_list(
        &self,
        params: ThreadQueueListParams,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        let thread_id = parse_thread_id_for_request(params.thread_id.as_str())?;
        let state_db = self.state_db_for_materialized_thread(thread_id).await?;
        let queued_turns = read_api_thread_queue(&state_db, thread_id).await?;
        Ok(Some(ThreadQueueListResponse { queued_turns }.into()))
    }

    #[expect(
        clippy::await_holding_invalid_type,
        reason = "queue mutation must stay serialized with queue drains per thread"
    )]
    pub(crate) async fn thread_queue_delete(
        &self,
        params: ThreadQueueDeleteParams,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        let thread_id = parse_thread_id_for_request(params.thread_id.as_str())?;
        let thread_queue_lock = self.thread_queue_lock(thread_id).await;
        let _thread_queue_guard = thread_queue_lock.lock().await;
        let state_db = self.state_db_for_materialized_thread(thread_id).await?;
        let deleted = state_db
            .delete_thread_queued_turn(thread_id, params.queued_turn_id.as_str())
            .await
            .map_err(|err| internal_error(format!("failed to delete queued turn: {err}")))?;
        if deleted {
            let queued_turns = read_api_thread_queue(&state_db, thread_id).await?;
            self.emit_thread_queue_changed_ordered(thread_id, queued_turns)
                .await;
            self.drain_thread_queue_if_idle_locked(thread_id, &state_db)
                .await;
        }
        Ok(Some(ThreadQueueDeleteResponse { deleted }.into()))
    }

    #[expect(
        clippy::await_holding_invalid_type,
        reason = "queue mutation must stay serialized with queue drains per thread"
    )]
    pub(crate) async fn thread_queue_reorder(
        &self,
        params: ThreadQueueReorderParams,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        let thread_id = parse_thread_id_for_request(params.thread_id.as_str())?;
        let thread_queue_lock = self.thread_queue_lock(thread_id).await;
        let _thread_queue_guard = thread_queue_lock.lock().await;
        let state_db = self.state_db_for_materialized_thread(thread_id).await?;
        state_db
            .reorder_thread_queued_turns(thread_id, params.queued_turn_ids.as_slice())
            .await
            .map_err(|err| invalid_request(err.to_string()))?;
        let queued_turns = read_api_thread_queue(&state_db, thread_id).await?;
        self.emit_thread_queue_changed_ordered(thread_id, queued_turns.clone())
            .await;
        self.drain_thread_queue_if_idle_locked(thread_id, &state_db)
            .await;
        Ok(Some(ThreadQueueReorderResponse { queued_turns }.into()))
    }

    pub(crate) async fn emit_resume_queue_snapshot_and_drain(&self, thread_id: ThreadId) {
        self.emit_thread_queue_snapshot(thread_id).await;
        self.drain_thread_queue_after_terminal_turn(thread_id).await;
    }

    #[expect(
        clippy::await_holding_invalid_type,
        reason = "queue drain must stay serialized with queue mutations per thread"
    )]
    pub(crate) async fn drain_thread_queue_after_terminal_turn(&self, thread_id: ThreadId) {
        let thread_queue_lock = self.thread_queue_lock(thread_id).await;
        let _thread_queue_guard = thread_queue_lock.lock().await;
        let state_db = match self.state_db_for_materialized_thread(thread_id).await {
            Ok(state_db) => state_db,
            Err(err) => {
                warn!(
                    "failed to open state db before draining thread queue for {thread_id}: {}",
                    err.message
                );
                return;
            }
        };
        self.drain_thread_queue_if_idle_locked(thread_id, &state_db)
            .await;
    }

    async fn drain_thread_queue_if_idle_locked(
        &self,
        thread_id: ThreadId,
        state_db: &StateDbHandle,
    ) {
        let Ok(thread) = self.thread_manager.get_thread(thread_id).await else {
            return;
        };
        if self
            .thread_has_live_in_progress_turn(thread_id, thread.as_ref())
            .await
        {
            return;
        }
        let queued_turn = match state_db
            .first_dispatchable_thread_queued_turn(thread_id)
            .await
        {
            Ok(Some(queued_turn)) => queued_turn,
            Ok(None) => return,
            Err(err) => {
                warn!("failed to read next queued turn for {thread_id}: {err}");
                return;
            }
        };
        let turn_start_params = match api_queued_turn_from_state(queued_turn.clone()) {
            Ok(queued_turn) => queued_turn
                .turn_start_params
                .into_turn_start_params(thread_id.to_string()),
            Err(err) => {
                warn!("failed to decode next queued turn for {thread_id}: {err}");
                return;
            }
        };

        match self
            .turn_processor
            .start_queued_turn(turn_start_params)
            .await
        {
            Ok(_) => {
                if let Err(err) = state_db
                    .delete_thread_queued_turn(thread_id, queued_turn.queued_turn_id.as_str())
                    .await
                {
                    warn!(
                        "failed to remove dispatched queued turn {} for {thread_id}: {err}",
                        queued_turn.queued_turn_id
                    );
                    return;
                }
            }
            Err(error) => {
                if let Err(err) = state_db
                    .set_thread_queued_turn_dispatch_error(
                        thread_id,
                        queued_turn.queued_turn_id.as_str(),
                        Some(error.message.as_str()),
                    )
                    .await
                {
                    warn!(
                        "failed to record dispatch error for queued turn {} on {thread_id}: {err}",
                        queued_turn.queued_turn_id
                    );
                    return;
                }
            }
        }

        match read_api_thread_queue(state_db, thread_id).await {
            Ok(queued_turns) => {
                self.emit_thread_queue_changed_ordered(thread_id, queued_turns)
                    .await;
            }
            Err(err) => warn!("{}", err.message),
        }
    }

    async fn thread_queue_lock(&self, thread_id: ThreadId) -> Arc<Mutex<()>> {
        let mut thread_queue_locks = self.thread_queue_locks.lock().await;
        thread_queue_locks
            .entry(thread_id)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    async fn thread_has_live_in_progress_turn(
        &self,
        thread_id: ThreadId,
        thread: &CodexThread,
    ) -> bool {
        if matches!(thread.agent_status().await, AgentStatus::Running) {
            return true;
        }
        let thread_state = self.thread_state_manager.thread_state(thread_id).await;
        thread_state.lock().await.active_turn_snapshot().is_some()
    }

    async fn state_db_for_materialized_thread(
        &self,
        thread_id: ThreadId,
    ) -> Result<StateDbHandle, JSONRPCErrorError> {
        if let Ok(thread) = self.thread_manager.get_thread(thread_id).await {
            if thread.rollout_path().is_none() {
                return Err(invalid_request(format!(
                    "ephemeral thread does not support persistent app-server state: {thread_id}"
                )));
            }
            if let Some(state_db) = thread.state_db() {
                return Ok(state_db);
            }
        } else {
            find_thread_path_by_id_str(
                &self.config.codex_home,
                &thread_id.to_string(),
                Some(self.state_db.as_ref()),
            )
            .await
            .map_err(|err| {
                internal_error(format!("failed to locate thread id {thread_id}: {err}"))
            })?
            .ok_or_else(|| invalid_request(format!("thread not found: {thread_id}")))?;
        }

        Ok(self.state_db.clone())
    }

    async fn emit_thread_queue_snapshot(&self, thread_id: ThreadId) {
        let state_db = match self.state_db_for_materialized_thread(thread_id).await {
            Ok(state_db) => state_db,
            Err(err) => {
                warn!(
                    "failed to open state db before emitting thread queue resume snapshot for {thread_id}: {}",
                    err.message
                );
                return;
            }
        };
        let listener_command_tx = {
            let thread_state = self.thread_state_manager.thread_state(thread_id).await;
            let thread_state = thread_state.lock().await;
            thread_state.listener_command_tx()
        };
        if let Some(listener_command_tx) = listener_command_tx {
            let command = ThreadListenerCommand::EmitThreadQueueSnapshot {
                state_db: state_db.clone(),
            };
            if listener_command_tx.send(command).is_ok() {
                return;
            }
            warn!(
                "failed to enqueue thread queue snapshot for {thread_id}: listener command channel is closed"
            );
        }
        send_thread_queue_snapshot_notification(&self.outgoing, thread_id, &state_db).await;
    }

    async fn emit_thread_queue_changed_ordered(
        &self,
        thread_id: ThreadId,
        queued_turns: Vec<QueuedTurn>,
    ) {
        let listener_command_tx = {
            let thread_state = self.thread_state_manager.thread_state(thread_id).await;
            let thread_state = thread_state.lock().await;
            thread_state.listener_command_tx()
        };
        if let Some(listener_command_tx) = listener_command_tx {
            let command = ThreadListenerCommand::EmitThreadQueueChanged {
                queued_turns: queued_turns.clone(),
            };
            if listener_command_tx.send(command).is_ok() {
                return;
            }
            warn!(
                "failed to enqueue thread queue update for {thread_id}: listener command channel is closed"
            );
        }
        self.outgoing
            .send_server_notification(ServerNotification::ThreadQueueChanged(
                ThreadQueueChangedNotification {
                    thread_id: thread_id.to_string(),
                    queued_turns,
                },
            ))
            .await;
    }
}

fn parse_thread_id_for_request(thread_id: &str) -> Result<ThreadId, JSONRPCErrorError> {
    ThreadId::from_string(thread_id)
        .map_err(|err| invalid_request(format!("invalid thread id: {err}")))
}

fn api_queued_turn_from_state(
    queued_turn: codex_state::ThreadQueuedTurn,
) -> anyhow::Result<QueuedTurn> {
    Ok(QueuedTurn {
        id: queued_turn.queued_turn_id,
        turn_start_params: serde_json::from_str(queued_turn.turn_start_params_json.as_str())?,
        created_at: queued_turn.created_at.timestamp(),
        queue_order: queued_turn.queue_order,
        last_dispatch_error: queued_turn.last_dispatch_error,
    })
}

async fn read_api_thread_queue(
    state_db: &StateDbHandle,
    thread_id: ThreadId,
) -> Result<Vec<QueuedTurn>, JSONRPCErrorError> {
    state_db
        .list_thread_queued_turns(thread_id)
        .await
        .map_err(|err| internal_error(format!("failed to read thread queue: {err}")))?
        .into_iter()
        .map(api_queued_turn_from_state)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| internal_error(format!("failed to decode queued turn params: {err}")))
}

pub(super) async fn send_thread_queue_snapshot_notification(
    outgoing: &Arc<OutgoingMessageSender>,
    thread_id: ThreadId,
    state_db: &StateDbHandle,
) {
    match read_api_thread_queue(state_db, thread_id).await {
        Ok(queued_turns) => {
            outgoing
                .send_server_notification(ServerNotification::ThreadQueueChanged(
                    ThreadQueueChangedNotification {
                        thread_id: thread_id.to_string(),
                        queued_turns,
                    },
                ))
                .await;
        }
        Err(err) => {
            warn!(thread_id = %thread_id, "{}", err.message);
        }
    }
}
