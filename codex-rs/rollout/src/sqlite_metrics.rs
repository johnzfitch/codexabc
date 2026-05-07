use std::sync::Arc;
use std::time::Duration;

use codex_state::DbMetricsRecorder;
use codex_state::DbMetricsRecorderHandle;

struct OtelDbMetrics(codex_otel::MetricsClient);

impl DbMetricsRecorder for OtelDbMetrics {
    fn counter(&self, name: &str, inc: i64, tags: &[(&str, &str)]) {
        let _ = self.0.counter(name, inc, tags);
    }

    fn record_duration(&self, name: &str, duration: Duration, tags: &[(&str, &str)]) {
        let _ = self.0.record_duration(name, duration, tags);
    }
}

pub(crate) fn global() -> Option<DbMetricsRecorderHandle> {
    codex_otel::global().map(|metrics| Arc::new(OtelDbMetrics(metrics)) as DbMetricsRecorderHandle)
}

pub(crate) fn record_fallback(caller: &'static str, reason: &'static str) {
    let metrics = global();
    codex_state::record_db_fallback_metric(metrics.as_deref(), caller, reason);
}
