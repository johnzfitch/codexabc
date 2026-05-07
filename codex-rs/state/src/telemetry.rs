use std::borrow::Cow;
use std::future::Future;
use std::time::Duration;
use std::time::Instant;

use codex_otel::MetricsClient;

use crate::DB_FALLBACK_METRIC;
use crate::DB_INIT_DURATION_METRIC;
use crate::DB_INIT_METRIC;
use crate::DB_LOG_QUEUE_METRIC;
use crate::DB_OPERATION_DURATION_METRIC;
use crate::DB_OPERATION_METRIC;

#[derive(Clone, Copy)]
pub(crate) enum DbKind {
    State,
    Logs,
    None,
}

impl DbKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::State => "state",
            Self::Logs => "logs",
            Self::None => "none",
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum DbAccess {
    Read,
    Write,
    Transaction,
}

impl DbAccess {
    fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::Transaction => "transaction",
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct DbErrorTags {
    pub error_class: &'static str,
    pub sqlite_code: String,
}

impl DbErrorTags {
    fn none() -> Self {
        Self {
            error_class: "none",
            sqlite_code: "none".to_string(),
        }
    }
}

pub(crate) async fn record_operation<T, F>(
    metrics: Option<&MetricsClient>,
    db: DbKind,
    operation: &'static str,
    access: DbAccess,
    future: F,
) -> anyhow::Result<T>
where
    F: Future<Output = anyhow::Result<T>>,
{
    let started = Instant::now();
    let result = future.await;
    record_operation_result(metrics, db, operation, access, started.elapsed(), &result);
    result
}

pub(crate) fn record_init_result<T>(
    metrics: Option<&MetricsClient>,
    db: DbKind,
    phase: &'static str,
    duration: Duration,
    result: &anyhow::Result<T>,
) {
    let status = if result.is_ok() { "success" } else { "failed" };
    let error_tags = result
        .as_ref()
        .err()
        .map(classify_error)
        .unwrap_or_else(DbErrorTags::none);
    let tags = [
        ("status", status),
        ("phase", phase),
        ("db", db.as_str()),
        ("error_class", error_tags.error_class),
        ("sqlite_code", error_tags.sqlite_code.as_str()),
    ];
    record_counter(metrics, DB_INIT_METRIC, &tags);
    record_duration(metrics, DB_INIT_DURATION_METRIC, duration, &tags);
}

pub(crate) fn record_fallback(caller: &'static str, reason: &'static str) {
    let tags = [("caller", caller), ("reason", reason)];
    record_counter(codex_otel::global().as_ref(), DB_FALLBACK_METRIC, &tags);
}

pub(crate) fn record_log_queue(
    metrics: Option<&MetricsClient>,
    event: &'static str,
    reason: &'static str,
) {
    let tags = [("event", event), ("reason", reason)];
    record_counter(metrics, DB_LOG_QUEUE_METRIC, &tags);
}

pub(crate) fn classify_error(err: &anyhow::Error) -> DbErrorTags {
    for cause in err.chain() {
        if let Some(sqlx_err) = cause.downcast_ref::<sqlx::Error>() {
            return classify_sqlx_error(sqlx_err);
        }
        if cause
            .downcast_ref::<sqlx::migrate::MigrateError>()
            .is_some()
        {
            return DbErrorTags {
                error_class: "migration",
                sqlite_code: "none".to_string(),
            };
        }
        if cause.downcast_ref::<serde_json::Error>().is_some() {
            return DbErrorTags {
                error_class: "serde",
                sqlite_code: "none".to_string(),
            };
        }
        if cause.downcast_ref::<std::io::Error>().is_some() {
            return DbErrorTags {
                error_class: "io",
                sqlite_code: "none".to_string(),
            };
        }
    }

    DbErrorTags {
        error_class: "unknown",
        sqlite_code: "none".to_string(),
    }
}

pub(crate) fn classify_sqlite_code(code: &str) -> &'static str {
    let primary_code = code.parse::<i32>().ok().map(|code| code & 0xff);
    match primary_code {
        Some(5) => "sqlite_busy",
        Some(6) => "sqlite_locked",
        Some(8) => "sqlite_readonly",
        Some(10) => "sqlite_ioerr",
        Some(11) => "sqlite_corrupt",
        Some(13) => "sqlite_full",
        Some(14) => "sqlite_cantopen",
        Some(19) => "sqlite_constraint",
        Some(17) => "sqlite_schema",
        _ => "unknown",
    }
}

fn record_operation_result<T>(
    metrics: Option<&MetricsClient>,
    db: DbKind,
    operation: &'static str,
    access: DbAccess,
    duration: Duration,
    result: &anyhow::Result<T>,
) {
    let status = if result.is_ok() { "success" } else { "failed" };
    let error_tags = result
        .as_ref()
        .err()
        .map(classify_error)
        .unwrap_or_else(DbErrorTags::none);
    let tags = [
        ("status", status),
        ("db", db.as_str()),
        ("operation", operation),
        ("access", access.as_str()),
        ("error_class", error_tags.error_class),
        ("sqlite_code", error_tags.sqlite_code.as_str()),
    ];
    record_counter(metrics, DB_OPERATION_METRIC, &tags);
    record_duration(metrics, DB_OPERATION_DURATION_METRIC, duration, &tags);
}

fn classify_sqlx_error(err: &sqlx::Error) -> DbErrorTags {
    match err {
        sqlx::Error::Database(database_error) => {
            let code = database_error
                .code()
                .unwrap_or(Cow::Borrowed("none"))
                .to_string();
            DbErrorTags {
                error_class: classify_sqlite_code(code.as_str()),
                sqlite_code: code,
            }
        }
        sqlx::Error::PoolTimedOut => DbErrorTags {
            error_class: "pool_timeout",
            sqlite_code: "none".to_string(),
        },
        sqlx::Error::Io(_) => DbErrorTags {
            error_class: "io",
            sqlite_code: "none".to_string(),
        },
        sqlx::Error::ColumnDecode { source, .. } if source.is::<serde_json::Error>() => {
            DbErrorTags {
                error_class: "serde",
                sqlite_code: "none".to_string(),
            }
        }
        sqlx::Error::Decode(source) if source.is::<serde_json::Error>() => DbErrorTags {
            error_class: "serde",
            sqlite_code: "none".to_string(),
        },
        _ => DbErrorTags {
            error_class: "unknown",
            sqlite_code: "none".to_string(),
        },
    }
}

fn record_counter(metrics: Option<&MetricsClient>, name: &str, tags: &[(&str, &str)]) {
    if let Some(metrics) = metrics {
        let _ = metrics.counter(name, /*inc*/ 1, tags);
    }
}

fn record_duration(
    metrics: Option<&MetricsClient>,
    name: &str,
    duration: Duration,
    tags: &[(&str, &str)],
) {
    if let Some(metrics) = metrics {
        let _ = metrics.record_duration(name, duration, tags);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn classifies_sqlite_primary_codes() {
        assert_eq!(classify_sqlite_code("5"), "sqlite_busy");
        assert_eq!(classify_sqlite_code("6"), "sqlite_locked");
        assert_eq!(classify_sqlite_code("14"), "sqlite_cantopen");
        assert_eq!(classify_sqlite_code("2067"), "sqlite_constraint");
    }

    #[test]
    fn classifies_non_sqlite_errors() {
        let io_error =
            anyhow::Error::new(std::io::Error::new(std::io::ErrorKind::NotFound, "missing"));
        assert_eq!(
            classify_error(&io_error),
            DbErrorTags {
                error_class: "io",
                sqlite_code: "none".to_string()
            }
        );

        let serde_error =
            anyhow::Error::new(serde_json::from_str::<serde_json::Value>("not-json").unwrap_err());
        assert_eq!(
            classify_error(&serde_error),
            DbErrorTags {
                error_class: "serde",
                sqlite_code: "none".to_string()
            }
        );

        let unknown_error = anyhow::anyhow!("plain failure");
        assert_eq!(
            classify_error(&unknown_error),
            DbErrorTags {
                error_class: "unknown",
                sqlite_code: "none".to_string()
            }
        );
    }

    #[test]
    fn classifies_sqlx_pool_timeout() {
        let err = anyhow::Error::new(sqlx::Error::PoolTimedOut);
        assert_eq!(
            classify_error(&err),
            DbErrorTags {
                error_class: "pool_timeout",
                sqlite_code: "none".to_string()
            }
        );
    }
}
