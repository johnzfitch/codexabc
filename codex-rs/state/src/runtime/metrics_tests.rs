use super::StateRuntime;
use super::test_support::unique_temp_dir;
use crate::DB_INIT_METRIC;
use crate::DB_OPERATION_METRIC;
use crate::DbMetricsRecorder;
use crate::DbMetricsRecorderHandle;
use pretty_assertions::assert_eq;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

#[derive(Clone, Debug)]
struct MetricEvent {
    name: String,
    tags: BTreeMap<String, String>,
}

#[derive(Default)]
struct TestMetrics {
    events: Mutex<Vec<MetricEvent>>,
}

impl TestMetrics {
    fn metric_points(&self, name: &str) -> Vec<BTreeMap<String, String>> {
        self.events
            .lock()
            .expect("metrics lock")
            .iter()
            .filter(|event| event.name == name)
            .map(|event| event.tags.clone())
            .collect()
    }
}

impl DbMetricsRecorder for TestMetrics {
    fn counter(&self, name: &str, _inc: i64, tags: &[(&str, &str)]) {
        self.events.lock().expect("metrics lock").push(MetricEvent {
            name: name.to_string(),
            tags: tags_to_map(tags),
        });
    }

    fn record_duration(&self, name: &str, _duration: Duration, tags: &[(&str, &str)]) {
        self.events.lock().expect("metrics lock").push(MetricEvent {
            name: name.to_string(),
            tags: tags_to_map(tags),
        });
    }
}

fn build_metrics() -> Arc<TestMetrics> {
    Arc::new(TestMetrics::default())
}

fn tags_to_map(tags: &[(&str, &str)]) -> BTreeMap<String, String> {
    tags.iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect()
}

fn find_point(
    points: &[BTreeMap<String, String>],
    key: &str,
    value: &str,
) -> BTreeMap<String, String> {
    points
        .iter()
        .find(|attrs| attrs.get(key).is_some_and(|actual| actual == value))
        .cloned()
        .unwrap_or_else(|| panic!("missing point with {key}={value}: {points:?}"))
}

#[tokio::test]
async fn init_records_success_metrics() {
    let metrics = build_metrics();
    let codex_home = unique_temp_dir();
    let runtime = StateRuntime::init_with_metrics(
        codex_home.clone(),
        "test-provider".to_string(),
        Some(metrics.clone() as DbMetricsRecorderHandle),
    )
    .await
    .expect("initialize runtime");

    let points = metrics.metric_points(DB_INIT_METRIC);
    let open_state = find_point(&points, "phase", "open_state");
    assert_eq!(
        open_state.get("status").map(String::as_str),
        Some("success")
    );
    assert_eq!(open_state.get("db").map(String::as_str), Some("state"));
    assert_eq!(
        open_state.get("error_class").map(String::as_str),
        Some("none")
    );
    assert_eq!(
        open_state.get("sqlite_code").map(String::as_str),
        Some("none")
    );

    runtime.pool.close().await;
    runtime.logs_pool.close().await;
    let _ = tokio::fs::remove_dir_all(codex_home).await;
}

#[tokio::test]
async fn selected_operations_record_success_metrics() {
    let metrics = build_metrics();
    let codex_home = unique_temp_dir();
    let runtime = StateRuntime::init_with_metrics(
        codex_home.clone(),
        "test-provider".to_string(),
        Some(metrics.clone() as DbMetricsRecorderHandle),
    )
    .await
    .expect("initialize runtime");

    runtime
        .get_backfill_state()
        .await
        .expect("get backfill state");
    runtime
        .checkpoint_backfill("sessions/test/rollout.jsonl")
        .await
        .expect("checkpoint backfill");

    let points = metrics.metric_points(DB_OPERATION_METRIC);
    let read = find_point(&points, "operation", "get_backfill_state");
    assert_eq!(read.get("status").map(String::as_str), Some("success"));
    assert_eq!(read.get("access").map(String::as_str), Some("read"));
    let write = find_point(&points, "operation", "checkpoint_backfill");
    assert_eq!(write.get("status").map(String::as_str), Some("success"));
    assert_eq!(write.get("access").map(String::as_str), Some("write"));

    runtime.pool.close().await;
    runtime.logs_pool.close().await;
    let _ = tokio::fs::remove_dir_all(codex_home).await;
}
