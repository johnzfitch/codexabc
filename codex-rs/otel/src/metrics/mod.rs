mod client;
mod config;
mod error;
pub(crate) mod names;
pub(crate) mod runtime_metrics;
pub(crate) mod tags;
pub(crate) mod timer;
pub(crate) mod validation;

use crate::config::StatsigMetricsSettings;
pub use crate::metrics::client::MetricsClient;
pub use crate::metrics::config::MetricsConfig;
pub use crate::metrics::config::MetricsExporter;
pub use crate::metrics::error::MetricsError;
pub use crate::metrics::error::Result;
pub use names::*;
use std::sync::OnceLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
pub use tags::ORIGINATOR_TAG;
pub use tags::SessionMetricTagValues;
pub use tags::bounded_originator_tag_value;

static GLOBAL_METRICS: OnceLock<MetricsClient> = OnceLock::new();
static GLOBAL_STATSIG_METRICS_SETTINGS: OnceLock<StatsigMetricsSettings> = OnceLock::new();
static PROCESS_START_RECORDED: AtomicBool = AtomicBool::new(false);

pub(crate) fn install_global(metrics: MetricsClient) {
    let _ = GLOBAL_METRICS.set(metrics);
}

pub fn global() -> Option<MetricsClient> {
    GLOBAL_METRICS.get().cloned()
}

pub(crate) fn install_global_statsig_settings(settings: StatsigMetricsSettings) {
    let _ = GLOBAL_STATSIG_METRICS_SETTINGS.set(settings);
}

pub(crate) fn global_statsig_settings() -> Option<StatsigMetricsSettings> {
    GLOBAL_STATSIG_METRICS_SETTINGS.get().cloned()
}

/// Record the process start counter at most once for this process.
pub fn record_process_start_once(metrics: &MetricsClient, originator: &str) -> Result<bool> {
    if PROCESS_START_RECORDED
        .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
        .is_err()
    {
        return Ok(false);
    }

    metrics.counter(
        PROCESS_START_METRIC,
        /*inc*/ 1,
        &[(ORIGINATOR_TAG, bounded_originator_tag_value(originator))],
    )?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_sdk::metrics::InMemoryMetricExporter;
    use opentelemetry_sdk::metrics::data::AggregatedMetrics;
    use opentelemetry_sdk::metrics::data::Metric;
    use opentelemetry_sdk::metrics::data::MetricData;
    use opentelemetry_sdk::metrics::data::ResourceMetrics;
    use pretty_assertions::assert_eq;
    use std::collections::BTreeMap;

    #[test]
    fn record_process_start_once_records_single_bounded_counter() {
        let exporter = InMemoryMetricExporter::default();
        let metrics = MetricsClient::new(
            MetricsConfig::in_memory("test", "codex-test", env!("CARGO_PKG_VERSION"), exporter)
                .with_runtime_reader(),
        )
        .expect("metrics client");

        assert_eq!(
            record_process_start_once(&metrics, "codex_cli_rs").expect("recorded"),
            true
        );
        assert_eq!(
            record_process_start_once(&metrics, "codex_vscode").expect("skipped"),
            false
        );

        let snapshot = metrics.snapshot().expect("snapshot");
        let (attributes, value) = counter_point(&snapshot, PROCESS_START_METRIC);

        assert_eq!(value, 1);
        assert_eq!(
            attributes,
            BTreeMap::from([(ORIGINATOR_TAG.to_string(), "codex_cli_rs".to_string())])
        );
    }

    fn find_metric<'a>(resource_metrics: &'a ResourceMetrics, name: &str) -> &'a Metric {
        for scope_metrics in resource_metrics.scope_metrics() {
            for metric in scope_metrics.metrics() {
                if metric.name() == name {
                    return metric;
                }
            }
        }
        panic!("metric {name} missing");
    }

    fn counter_point(
        resource_metrics: &ResourceMetrics,
        name: &str,
    ) -> (BTreeMap<String, String>, u64) {
        let metric = find_metric(resource_metrics, name);
        match metric.data() {
            AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                let points = sum.data_points().collect::<Vec<_>>();
                assert_eq!(points.len(), 1);
                let point = points[0];
                let attributes = point
                    .attributes()
                    .map(|kv| (kv.key.as_str().to_string(), kv.value.as_str().to_string()))
                    .collect();
                (attributes, point.value())
            }
            _ => panic!("unexpected counter metric data"),
        }
    }
}
