use anyhow::Result;
use app_test_support::McpProcess;
use app_test_support::create_final_assistant_message_sse_response;
use app_test_support::create_mock_responses_server_sequence_unchecked;
use app_test_support::create_shell_command_sse_response;
use app_test_support::to_response;
use codex_app_server_protocol::JSONRPCResponse;
use codex_app_server_protocol::PermissionProfileSelectionParams;
use codex_app_server_protocol::QueuedTurnStartParams;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::SandboxPolicy;
use codex_app_server_protocol::ThreadQueueAddResponse;
use codex_app_server_protocol::ThreadQueueDeleteResponse;
use codex_app_server_protocol::ThreadQueueListResponse;
use codex_app_server_protocol::ThreadQueueReorderResponse;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::TurnStartResponse;
use codex_app_server_protocol::UserInput;
use pretty_assertions::assert_eq;
use serde_json::json;
use tempfile::TempDir;
use tokio::time::timeout;

#[cfg(windows)]
const DEFAULT_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(25);
#[cfg(not(windows))]
const DEFAULT_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

#[tokio::test]
async fn thread_queue_supports_add_list_reorder_and_delete() -> Result<()> {
    let server = create_mock_responses_server_sequence_unchecked(vec![
        create_final_assistant_message_sse_response("materialized")?,
        create_shell_command_sse_response(
            sleep_command(/*seconds*/ 10),
            /*workdir*/ None,
            Some(10_000),
            "keep-open",
        )?,
    ])
    .await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri())?;

    let mut mcp = McpProcess::new(codex_home.path()).await?;
    timeout(DEFAULT_READ_TIMEOUT, mcp.initialize()).await??;
    let thread = start_materialized_thread(&mut mcp).await?;
    let blocking_turn = start_blocking_turn(&mut mcp, thread.id.as_str()).await?;

    let first = add_queued_turn(&mut mcp, thread.id.as_str(), "first").await?;
    let second = add_queued_turn(&mut mcp, thread.id.as_str(), "second").await?;

    let list_id = mcp
        .send_raw_request(
            "thread/queue/list",
            Some(json!({
                "threadId": thread.id,
            })),
        )
        .await?;
    let list_resp: JSONRPCResponse = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(list_id)),
    )
    .await??;
    let list: ThreadQueueListResponse = to_response(list_resp)?;
    assert_eq!(
        list.queued_turns
            .iter()
            .map(|turn| turn.id.as_str())
            .collect::<Vec<_>>(),
        vec![
            first.queued_turn.id.as_str(),
            second.queued_turn.id.as_str()
        ]
    );

    let reorder_id = mcp
        .send_raw_request(
            "thread/queue/reorder",
            Some(json!({
                "threadId": thread.id,
                "queuedTurnIds": [
                    second.queued_turn.id,
                    first.queued_turn.id,
                ],
            })),
        )
        .await?;
    let reorder_resp: JSONRPCResponse = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(reorder_id)),
    )
    .await??;
    let reordered: ThreadQueueReorderResponse = to_response(reorder_resp)?;
    assert_eq!(
        reordered
            .queued_turns
            .iter()
            .map(|turn| turn.id.as_str())
            .collect::<Vec<_>>(),
        vec![
            second.queued_turn.id.as_str(),
            first.queued_turn.id.as_str()
        ]
    );

    let delete_id = mcp
        .send_raw_request(
            "thread/queue/delete",
            Some(json!({
                "threadId": thread.id,
                "queuedTurnId": second.queued_turn.id,
            })),
        )
        .await?;
    let delete_resp: JSONRPCResponse = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(delete_id)),
    )
    .await??;
    let deleted: ThreadQueueDeleteResponse = to_response(delete_resp)?;
    assert!(deleted.deleted);

    mcp.interrupt_turn_and_wait_for_aborted(thread.id, blocking_turn.id, DEFAULT_READ_TIMEOUT)
        .await?;

    Ok(())
}

#[tokio::test]
async fn thread_queue_drains_after_the_next_terminal_turn() -> Result<()> {
    let server = create_mock_responses_server_sequence_unchecked(vec![
        create_final_assistant_message_sse_response("materialized")?,
        create_shell_command_sse_response(
            sleep_command(/*seconds*/ 1),
            /*workdir*/ None,
            Some(10_000),
            "call1",
        )?,
        create_final_assistant_message_sse_response("manual")?,
        create_final_assistant_message_sse_response("queued")?,
    ])
    .await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri())?;

    let mut mcp = McpProcess::new(codex_home.path()).await?;
    timeout(DEFAULT_READ_TIMEOUT, mcp.initialize()).await??;
    let thread = start_materialized_thread(&mut mcp).await?;
    start_blocking_turn(&mut mcp, thread.id.as_str()).await?;
    add_queued_turn(&mut mcp, thread.id.as_str(), "queued").await?;
    timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_notification_message("turn/completed"),
    )
    .await??;
    timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_notification_message("turn/started"),
    )
    .await??;
    timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_notification_message("turn/completed"),
    )
    .await??;

    let list_id = mcp
        .send_raw_request(
            "thread/queue/list",
            Some(json!({
                "threadId": thread.id,
            })),
        )
        .await?;
    let list_resp: JSONRPCResponse = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(list_id)),
    )
    .await??;
    let list: ThreadQueueListResponse = to_response(list_resp)?;
    assert!(list.queued_turns.is_empty());

    Ok(())
}

#[tokio::test]
async fn thread_queue_add_drains_immediately_when_the_thread_is_idle() -> Result<()> {
    let server = create_mock_responses_server_sequence_unchecked(vec![
        create_final_assistant_message_sse_response("materialized")?,
        create_final_assistant_message_sse_response("queued")?,
    ])
    .await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri())?;

    let mut mcp = McpProcess::new(codex_home.path()).await?;
    timeout(DEFAULT_READ_TIMEOUT, mcp.initialize()).await??;
    let thread = start_materialized_thread(&mut mcp).await?;
    add_queued_turn(&mut mcp, thread.id.as_str(), "queued").await?;

    timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_notification_message("turn/started"),
    )
    .await??;
    timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_notification_message("turn/completed"),
    )
    .await??;

    let list_id = mcp
        .send_raw_request(
            "thread/queue/list",
            Some(json!({
                "threadId": thread.id,
            })),
        )
        .await?;
    let list_resp: JSONRPCResponse = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(list_id)),
    )
    .await??;
    let list: ThreadQueueListResponse = to_response(list_resp)?;
    assert!(list.queued_turns.is_empty());

    Ok(())
}

#[tokio::test]
async fn thread_queue_delete_drains_the_next_turn_after_removing_a_failed_head() -> Result<()> {
    let server = create_mock_responses_server_sequence_unchecked(vec![
        create_final_assistant_message_sse_response("materialized")?,
        create_final_assistant_message_sse_response("queued")?,
    ])
    .await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri())?;

    let mut mcp = McpProcess::new(codex_home.path()).await?;
    timeout(DEFAULT_READ_TIMEOUT, mcp.initialize()).await??;
    let thread = start_materialized_thread(&mut mcp).await?;
    let failed_head = add_invalid_queued_turn(&mut mcp, thread.id.as_str()).await?;
    add_queued_turn(&mut mcp, thread.id.as_str(), "queued").await?;

    let delete_id = mcp
        .send_raw_request(
            "thread/queue/delete",
            Some(json!({
                "threadId": thread.id,
                "queuedTurnId": failed_head.queued_turn.id,
            })),
        )
        .await?;
    let delete_resp: JSONRPCResponse = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(delete_id)),
    )
    .await??;
    let deleted: ThreadQueueDeleteResponse = to_response(delete_resp)?;
    assert!(deleted.deleted);

    timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_notification_message("turn/started"),
    )
    .await??;
    timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_notification_message("turn/completed"),
    )
    .await??;

    let list_id = mcp
        .send_raw_request(
            "thread/queue/list",
            Some(json!({
                "threadId": thread.id,
            })),
        )
        .await?;
    let list_resp: JSONRPCResponse = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(list_id)),
    )
    .await??;
    let list: ThreadQueueListResponse = to_response(list_resp)?;
    assert!(list.queued_turns.is_empty());

    Ok(())
}

async fn start_materialized_thread(
    mcp: &mut McpProcess,
) -> Result<codex_app_server_protocol::Thread> {
    let start_id = mcp
        .send_thread_start_request(ThreadStartParams {
            model: Some("mock-model".to_string()),
            ..Default::default()
        })
        .await?;
    let start_resp: JSONRPCResponse = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(start_id)),
    )
    .await??;
    let ThreadStartResponse { thread, .. } = to_response::<ThreadStartResponse>(start_resp)?;

    let turn_id = mcp
        .send_turn_start_request(turn_start_params(thread.id.as_str(), "materialize"))
        .await?;
    timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(turn_id)),
    )
    .await??;
    timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_notification_message("turn/started"),
    )
    .await??;
    timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_notification_message("turn/completed"),
    )
    .await??;

    Ok(thread)
}

async fn add_queued_turn(
    mcp: &mut McpProcess,
    thread_id: &str,
    text: &str,
) -> Result<ThreadQueueAddResponse> {
    let add_id = mcp
        .send_raw_request(
            "thread/queue/add",
            Some(json!({
                "threadId": thread_id,
                "turnStartParams": queued_turn_start_params(text),
            })),
        )
        .await?;
    let add_resp: JSONRPCResponse = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(add_id)),
    )
    .await??;
    to_response(add_resp)
}

async fn add_invalid_queued_turn(
    mcp: &mut McpProcess,
    thread_id: &str,
) -> Result<ThreadQueueAddResponse> {
    let add_id = mcp
        .send_raw_request(
            "thread/queue/add",
            Some(json!({
                "threadId": thread_id,
                "turnStartParams": invalid_queued_turn_start_params(),
            })),
        )
        .await?;
    let add_resp: JSONRPCResponse = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(add_id)),
    )
    .await??;
    to_response(add_resp)
}

async fn start_blocking_turn(
    mcp: &mut McpProcess,
    thread_id: &str,
) -> Result<codex_app_server_protocol::Turn> {
    let turn_id = mcp
        .send_turn_start_request(turn_start_params(thread_id, "manual"))
        .await?;
    let turn_resp: JSONRPCResponse = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(turn_id)),
    )
    .await??;
    let TurnStartResponse { turn } = to_response(turn_resp)?;
    timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_notification_message("turn/started"),
    )
    .await??;
    Ok(turn)
}

fn sleep_command(seconds: u64) -> Vec<String> {
    #[cfg(target_os = "windows")]
    {
        vec![
            "powershell".to_string(),
            "-Command".to_string(),
            format!("Start-Sleep -Seconds {seconds}"),
        ]
    }
    #[cfg(not(target_os = "windows"))]
    {
        vec!["sleep".to_string(), seconds.to_string()]
    }
}

fn queued_turn_start_params(text: &str) -> QueuedTurnStartParams {
    QueuedTurnStartParams {
        input: vec![UserInput::Text {
            text: text.to_string(),
            text_elements: Vec::new(),
        }],
        ..Default::default()
    }
}

fn invalid_queued_turn_start_params() -> QueuedTurnStartParams {
    QueuedTurnStartParams {
        sandbox_policy: Some(SandboxPolicy::DangerFullAccess),
        permissions: Some(PermissionProfileSelectionParams::Profile {
            id: ":danger-no-sandbox".to_string(),
            modifications: None,
        }),
        ..queued_turn_start_params("invalid")
    }
}

fn turn_start_params(thread_id: &str, text: &str) -> TurnStartParams {
    TurnStartParams {
        thread_id: thread_id.to_string(),
        input: vec![UserInput::Text {
            text: text.to_string(),
            text_elements: Vec::new(),
        }],
        ..Default::default()
    }
}

fn create_config_toml(codex_home: &std::path::Path, server_uri: &str) -> std::io::Result<()> {
    let config_toml = codex_home.join("config.toml");
    std::fs::write(
        config_toml,
        format!(
            r#"
model = "gpt-5.3-codex"
approval_policy = "never"
sandbox_mode = "read-only"

model_provider = "mock_provider"

[features]
personality = true

[model_providers.mock_provider]
name = "Mock provider for test"
base_url = "{server_uri}/v1"
wire_api = "responses"
request_max_retries = 0
stream_max_retries = 0
"#
        ),
    )
}
