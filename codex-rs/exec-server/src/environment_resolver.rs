use std::fmt::Debug;

use async_trait::async_trait;

use crate::Environment;
use crate::ExecServerError;
use crate::ExecServerRuntimePaths;

/// Resolves environment ids that are not already present in the manager snapshot.
///
/// This is an optional extension point for embedders. `EnvironmentManager`
/// stores the resolver, but `get_environment` remains a strict snapshot lookup
/// until resolution policy is wired explicitly.
#[async_trait]
pub trait EnvironmentResolver: Send + Sync + Debug {
    async fn resolve_environment(
        &self,
        environment_id: &str,
        local_runtime_paths: &ExecServerRuntimePaths,
    ) -> Result<Option<Environment>, ExecServerError>;
}
