CREATE TABLE thread_queued_turns (
    queued_turn_id TEXT PRIMARY KEY NOT NULL,
    thread_id TEXT NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
    turn_start_params_json TEXT NOT NULL,
    queue_order INTEGER NOT NULL,
    last_dispatch_error TEXT,
    created_at_ms INTEGER NOT NULL,
    UNIQUE(thread_id, queue_order)
);

CREATE INDEX idx_thread_queued_turns_thread_order
ON thread_queued_turns(thread_id, queue_order);
