#![cfg(unix)]
// Test assertions deliberately abort on invalid fixtures, and the mock mirrors the protocol exchange.
#![allow(
    clippy::excessive_nesting,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::too_many_lines,
    clippy::unwrap_used,
    clippy::unused_async,
    clippy::unused_async_trait_impl
)]

use codex_awake_queue_probe::{Action, Outcome, run};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{net::UnixListener, task::JoinHandle};
use tokio_tungstenite::{accept_async, tungstenite::Message};

const THREAD: &str = "00000000-0000-4000-8000-000000000001";
const MESSAGE: &str = "Процесс завершился.\nПроверь результат.";

#[derive(Clone, Copy)]
enum Reply {
    Accepted,
    InvalidParams,
    InternalError,
    Disconnect,
    Timeout,
    BadReceipt,
    EmptyReceiptId,
    BadMessage,
    MalformedJson,
    WrongResponseId,
}

struct Mock {
    dir: tempfile::TempDir,
    task: JoinHandle<()>,
    requests: Arc<Mutex<Vec<Value>>>,
}

impl Mock {
    fn path(&self) -> std::path::PathBuf {
        self.dir.path().join("rpc.sock")
    }
    fn requests(&self) -> Vec<Value> {
        self.requests.lock().unwrap().clone()
    }
    fn queue_count(&self) -> usize {
        self.requests()
            .iter()
            .filter(|r| r["method"] == "thread/queue/add")
            .count()
    }

    async fn start(pages: Vec<Vec<&'static str>>, reply: Reply) -> Self {
        let dir = tempfile::Builder::new()
            .prefix("caq-")
            .tempdir_in("/tmp")
            .unwrap();
        let listener = UnixListener::bind(dir.path().join("rpc.sock")).unwrap();
        let requests = Arc::new(Mutex::new(Vec::<Value>::new()));
        let recorded = requests.clone();
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();
            let mut page = 0;
            while let Some(Ok(Message::Text(text))) = ws.next().await {
                let request: Value = serde_json::from_str(&text).unwrap();
                recorded.lock().unwrap().push(request.clone());
                let method = request["method"].as_str().unwrap();
                if method == "initialized" {
                    continue;
                }
                let id = request["id"].clone();
                let response = match method {
                    "initialize" => {
                        json!({"id": id, "result": {"userAgent": "mock/0.1", "codexHome": "/tmp/mock-codex-home", "platformFamily": "unix", "platformOs": "macos"}})
                    }
                    "thread/loaded/list" => {
                        let next_cursor = if page + 1 < pages.len() {
                            Some(format!("page-{}", page + 1))
                        } else {
                            None
                        };
                        let data = pages[page].clone();
                        page += 1;
                        json!({"id": id, "result": {"data": data, "nextCursor": next_cursor}})
                    }
                    "thread/queue/add" => {
                        match reply {
                            Reply::Disconnect => return,
                            Reply::Timeout => {
                                tokio::time::sleep(Duration::from_secs(2)).await;
                                return;
                            }
                            Reply::InvalidParams => {
                                ws.send(Message::Text(json!({"id": id, "error": {"code": -32602, "message": "invalid input"}}).to_string().into())).await.unwrap();
                                continue;
                            }
                            Reply::InternalError => {
                                ws.send(Message::Text(json!({"id": id, "error": {"code": -32603, "message": "failed after possible commit"}}).to_string().into())).await.unwrap();
                                continue;
                            }
                            Reply::MalformedJson => {
                                ws.send(Message::Text("{".into())).await.unwrap();
                                continue;
                            }
                            _ => {}
                        }
                        // Interleaved notifications must not be mistaken for a receipt.
                        ws.send(Message::Text(json!({"method": "thread/queue/changed", "params": {"threadId": THREAD}}).to_string().into())).await.unwrap();
                        let params = &request["params"];
                        let client_id = if matches!(reply, Reply::BadReceipt) {
                            json!("other-event")
                        } else {
                            params["clientUserMessageId"].clone()
                        };
                        let input = if matches!(reply, Reply::BadMessage) {
                            json!([{"type": "text", "text": "different message", "text_elements": []}])
                        } else {
                            json!([{"type": "text", "text": params["input"][0]["text"], "text_elements": []}])
                        };
                        let response_id = if matches!(reply, Reply::WrongResponseId) {
                            json!(9999)
                        } else {
                            id
                        };
                        let queued_id = if matches!(reply, Reply::EmptyReceiptId) {
                            ""
                        } else {
                            "queue-1"
                        };
                        json!({"id": response_id, "result": {"queuedSubmission": {"id": queued_id, "clientUserMessageId": client_id, "input": input}}})
                    }
                    _ => panic!("unexpected RPC: {method}"),
                };
                if ws
                    .send(Message::Text(response.to_string().into()))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });
        Self {
            dir,
            task,
            requests,
        }
    }
}

impl Drop for Mock {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn enqueue() -> Action {
    Action::Enqueue {
        client_message_id: "event-1".into(),
        message: MESSAGE.into(),
    }
}

async fn submit(mock: &Mock) -> Outcome {
    run(&mock.path(), THREAD, enqueue(), Duration::from_millis(500)).await
}

#[tokio::test]
async fn direct_queue_call_preserves_target_message_and_receipt() {
    let mock = Mock::start(vec![vec![THREAD]], Reply::Accepted).await;
    let outcome = submit(&mock).await;
    assert_eq!(
        outcome,
        Outcome::Accepted {
            thread_id: THREAD.into(),
            queued_submission_id: "queue-1".into(),
            client_message_id: "event-1".into()
        }
    );
    let requests = mock.requests();
    assert_eq!(
        requests
            .iter()
            .map(|r| r["method"].as_str().unwrap())
            .collect::<Vec<_>>(),
        [
            "initialize",
            "initialized",
            "thread/loaded/list",
            "thread/queue/add"
        ]
    );
    assert_eq!(
        requests[0]["params"]["capabilities"]["experimentalApi"],
        true
    );
    assert_eq!(
        requests[3]["params"],
        json!({"threadId": THREAD, "clientUserMessageId": "event-1", "input": [{"type": "text", "text": MESSAGE}]})
    );
    assert_eq!(mock.queue_count(), 1);
}

#[tokio::test]
async fn refuses_to_queue_into_a_server_without_the_loaded_target() {
    let mock = Mock::start(vec![vec![]], Reply::Accepted).await;
    assert_eq!(
        submit(&mock).await,
        Outcome::NotSent {
            reason: "target_not_loaded".into()
        }
    );
    assert_eq!(mock.queue_count(), 0);
}

#[tokio::test]
async fn finds_target_on_a_later_page() {
    let mock = Mock::start(vec![vec!["other-thread"], vec![THREAD]], Reply::Accepted).await;
    assert!(matches!(submit(&mock).await, Outcome::Accepted { .. }));
    let requests = mock.requests();
    assert_eq!(requests[3]["params"]["cursor"], "page-1");
}

#[tokio::test]
async fn check_only_does_not_enqueue() {
    let mock = Mock::start(vec![vec![THREAD]], Reply::Accepted).await;
    assert_eq!(
        run(
            &mock.path(),
            THREAD,
            Action::Check,
            Duration::from_millis(500)
        )
        .await,
        Outcome::Checked {
            thread_id: THREAD.into()
        }
    );
    assert_eq!(mock.queue_count(), 0);
}

#[tokio::test]
async fn explicit_invalid_params_response_is_rejected_without_retry() {
    let mock = Mock::start(vec![vec![THREAD]], Reply::InvalidParams).await;
    assert_eq!(submit(&mock).await, Outcome::Rejected { code: -32602 });
    assert_eq!(mock.queue_count(), 1);
}

#[tokio::test]
async fn internal_error_after_possible_commit_is_unknown() {
    let mock = Mock::start(vec![vec![THREAD]], Reply::InternalError).await;
    assert!(matches!(
        submit(&mock).await,
        Outcome::DeliveryUnknown { .. }
    ));
    assert_eq!(mock.queue_count(), 1);
}

#[tokio::test]
async fn disconnect_after_submission_is_unknown_without_retry() {
    let mock = Mock::start(vec![vec![THREAD]], Reply::Disconnect).await;
    assert!(matches!(
        submit(&mock).await,
        Outcome::DeliveryUnknown { .. }
    ));
    assert_eq!(mock.queue_count(), 1);
}

#[tokio::test]
async fn response_timeout_after_submission_is_unknown_without_retry() {
    let mock = Mock::start(vec![vec![THREAD]], Reply::Timeout).await;
    assert!(matches!(
        submit(&mock).await,
        Outcome::DeliveryUnknown { .. }
    ));
    assert_eq!(mock.queue_count(), 1);
}

#[tokio::test]
async fn mismatched_receipt_is_not_reported_as_accepted() {
    for reply in [
        Reply::BadReceipt,
        Reply::EmptyReceiptId,
        Reply::BadMessage,
        Reply::MalformedJson,
        Reply::WrongResponseId,
    ] {
        let mock = Mock::start(vec![vec![THREAD]], reply).await;
        assert!(matches!(
            submit(&mock).await,
            Outcome::DeliveryUnknown { .. }
        ));
        assert_eq!(mock.queue_count(), 1);
    }
}

#[tokio::test]
async fn missing_socket_is_a_pre_send_failure() {
    let dir = tempfile::Builder::new()
        .prefix("caq-")
        .tempdir_in("/tmp")
        .unwrap();
    let outcome = run(
        &dir.path().join("missing.sock"),
        THREAD,
        enqueue(),
        Duration::from_millis(500),
    )
    .await;
    assert!(matches!(outcome, Outcome::NotSent { .. }));
}

#[tokio::test]
async fn relative_endpoint_is_not_used() {
    assert!(matches!(
        run(
            Path::new("relative.sock"),
            THREAD,
            enqueue(),
            Duration::from_millis(500)
        )
        .await,
        Outcome::NotSent { .. }
    ));
}

#[tokio::test]
async fn cli_sends_a_message_file_and_prints_only_the_receipt() {
    let mock = Mock::start(vec![vec![THREAD]], Reply::Accepted).await;
    let message_file = mock.dir.path().join("message.txt");
    std::fs::write(&message_file, MESSAGE).unwrap();
    let socket = mock.path();
    let output = tokio::task::spawn_blocking(move || {
        std::process::Command::new(env!("CARGO_BIN_EXE_codex-awake-queue-probe"))
            .args([
                "--socket",
                socket.to_str().unwrap(),
                "--thread",
                THREAD,
                "--timeout-ms",
                "1000",
                "--client-message-id",
                "event-1",
                "--message-file",
                message_file.to_str().unwrap(),
            ])
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(output.status.success());
    let result: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["status"], "accepted");
    assert_eq!(result["queued_submission_id"], "queue-1");
    assert!(!String::from_utf8_lossy(&output.stdout).contains(MESSAGE));
    assert_eq!(mock.queue_count(), 1);
}

#[test]
fn cli_rejects_conflicting_check_and_send_options() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_codex-awake-queue-probe"))
        .args([
            "--socket",
            "/tmp/not-opened.sock",
            "--thread",
            THREAD,
            "--timeout-ms",
            "1000",
            "--check-only",
            "--message-file",
            "/tmp/not-read.txt",
        ])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    let result: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["status"], "not_sent");
    assert_eq!(
        result["reason"],
        "--check-only cannot include message options"
    );
}
