//! SQLite-backed state for rollout metadata.
//!
//! This crate is intentionally small and focused: it extracts rollout metadata
//! from JSONL rollouts and mirrors it into a local SQLite database. Backfill
//! orchestration and rollout scanning live in `codex-core`.

mod extract;
pub mod log_db;
mod migrations;
mod model;
mod paths;
mod runtime;
mod telemetry;

pub use model::LogEntry;
pub use model::LogQuery;
pub use model::LogRow;
pub use model::Phase2JobClaimOutcome;
/// Preferred entrypoint: owns SQLite configuration and optional metrics injection.
pub use runtime::StateRuntime;

/// Low-level storage engine: useful for focused tests.
///
/// Most consumers should prefer [`StateRuntime`].
pub use extract::apply_rollout_item;
pub use extract::rollout_item_affects_thread_metadata;
pub use model::AgentJob;
pub use model::AgentJobCreateParams;
pub use model::AgentJobItem;
pub use model::AgentJobItemCreateParams;
pub use model::AgentJobItemStatus;
pub use model::AgentJobProgress;
pub use model::AgentJobStatus;
pub use model::Anchor;
pub use model::BackfillState;
pub use model::BackfillStats;
pub use model::BackfillStatus;
pub use model::DirectionalThreadSpawnEdgeStatus;
pub use model::ExtractionOutcome;
pub use model::SortDirection;
pub use model::SortKey;
pub use model::Stage1JobClaim;
pub use model::Stage1JobClaimOutcome;
pub use model::Stage1Output;
pub use model::Stage1StartupClaimParams;
pub use model::ThreadGoal;
pub use model::ThreadGoalStatus;
pub use model::ThreadMetadata;
pub use model::ThreadMetadataBuilder;
pub use model::ThreadsPage;
pub use runtime::RemoteControlEnrollmentRecord;
pub use runtime::ThreadFilterOptions;
pub use runtime::ThreadGoalAccountingMode;
pub use runtime::ThreadGoalAccountingOutcome;
pub use runtime::ThreadGoalUpdate;
pub use runtime::logs_db_filename;
pub use runtime::logs_db_path;
pub use runtime::state_db_filename;
pub use runtime::state_db_path;
pub use telemetry::DbMetricsRecorder;
pub use telemetry::DbMetricsRecorderHandle;

/// Environment variable for overriding the SQLite state database home directory.
pub const SQLITE_HOME_ENV: &str = "CODEX_SQLITE_HOME";

pub const LOGS_DB_FILENAME: &str = "logs";
pub const LOGS_DB_VERSION: u32 = 2;
pub const STATE_DB_FILENAME: &str = "state";
pub const STATE_DB_VERSION: u32 = 5;

/// Errors encountered during DB operations. Tags: [stage]
pub const DB_ERROR_METRIC: &str = "codex.db.error";
/// Metrics on backfill process. Tags: [status]
pub const DB_METRIC_BACKFILL: &str = "codex.db.backfill";
/// Metrics on backfill duration. Tags: [status]
pub const DB_METRIC_BACKFILL_DURATION_MS: &str = "codex.db.backfill.duration_ms";
/// SQLite startup initialization attempts. Tags: [status, phase, db, error_class, sqlite_code]
pub const DB_INIT_METRIC: &str = "codex.db.init";
/// SQLite startup initialization duration. Tags: [status, phase, db, error_class, sqlite_code]
pub const DB_INIT_DURATION_METRIC: &str = "codex.db.init.duration_ms";
/// SQLite operation attempts. Tags: [status, db, operation, access, error_class, sqlite_code]
pub const DB_OPERATION_METRIC: &str = "codex.db.operation";
/// SQLite operation duration. Tags: [status, db, operation, access, error_class, sqlite_code]
pub const DB_OPERATION_DURATION_METRIC: &str = "codex.db.operation.duration_ms";
/// Filesystem fallback after SQLite could not serve a request. Tags: [caller, reason]
pub const DB_FALLBACK_METRIC: &str = "codex.db.fallback";
/// SQLite log queue loss or flush failure. Tags: [event, reason]
pub const DB_LOG_QUEUE_METRIC: &str = "codex.db.log_queue";

pub fn record_db_fallback_metric(
    metrics: Option<&dyn DbMetricsRecorder>,
    caller: &'static str,
    reason: &'static str,
) {
    telemetry::record_fallback(metrics, caller, reason);
}

pub fn record_db_init_backfill_gate_metric(
    metrics: Option<&dyn DbMetricsRecorder>,
    duration: std::time::Duration,
    result: &anyhow::Result<()>,
) {
    telemetry::record_init_backfill_gate(metrics, duration, result);
}
