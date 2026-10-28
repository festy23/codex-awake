#![cfg(unix)]
// Socket fixtures and assertions deliberately stop the test on invalid setup.
#![allow(clippy::expect_used, clippy::indexing_slicing, clippy::unwrap_used)]

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::process::Command;
use tokio::net::TcpListener;
use tokio_tungstenite::{accept_async, tungstenite::Message};

const THREAD: &str = "00000000-0000-4000-8000-000000000001";

async fn mock_server(listener: TcpListener) -> Vec<String> {
    let (stream, _) = listener.accept().await.unwrap();
    let mut ws = accept_async(stream).await.unwrap();
    let mut methods = Vec::new();
    while let Some(Ok(Message::Text(text))) = ws.next().await {
        let request: Value = serde_json::from_str(&text).unwrap();
        let method = request["method"].as_str().unwrap();
        methods.push(method.to_owned());
        if method == "initialized" {
            continue;
        }
        let result = match method {
            "initialize" => {
                json!({"userAgent": "mock/0.1", "codexHome": "/tmp/mock-home", "platformFamily": "unix", "platformOs": "macos"})
            }
            "thread/loaded/list" => json!({"data": [THREAD], "nextCursor": null}),
            "thread/queue/add" => {
                assert_eq!(request["params"]["threadId"], THREAD);
                assert_eq!(request["params"]["input"][0]["text"], "Прямой WebSocket");
                json!({"queuedSubmission": {"id": "queue-ws-1", "input": request["params"]["input"], "clientUserMessageId": "event-ws-1"}})
            }
            _ => return methods,
        };
        ws.send(Message::Text(
            json!({"id": request["id"], "result": result})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
        if method == "thread/queue/add" {
            break;
        }
    }
    methods
}

#[tokio::test]
async fn cli_enqueues_directly_over_loopback_websocket() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/rpc", listener.local_addr().unwrap());
    let server = tokio::spawn(mock_server(listener));
    let dir = tempfile::tempdir().unwrap();
    let message = dir.path().join("message.txt");
    std::fs::write(&message, "Прямой WebSocket").unwrap();
    let output = tokio::task::spawn_blocking(move || {
        Command::new(env!("CARGO_BIN_EXE_codex-awake-queue-probe"))
            .args([
                "--websocket-url",
                &url,
                "--thread",
                THREAD,
                "--timeout-ms",
                "2000",
                "--client-message-id",
                "event-ws-1",
                "--message-file",
                message.to_str().unwrap(),
            ])
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    if !output.status.success() {
        server.abort();
    }
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    let result: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["status"], "accepted");
    assert_eq!(result["queued_submission_id"], "queue-ws-1");
    assert_eq!(
        server.await.unwrap(),
        [
            "initialize",
            "initialized",
            "thread/loaded/list",
            "thread/queue/add"
        ]
    );
}

#[test]
fn cli_refuses_non_loopback_websocket_before_connecting() {
    let output = Command::new(env!("CARGO_BIN_EXE_codex-awake-queue-probe"))
        .args([
            "--websocket-url",
            "ws://192.0.2.1:4500/rpc",
            "--thread",
            THREAD,
            "--timeout-ms",
            "1000",
            "--check-only",
        ])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    let result: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["reason"], "invalid_websocket_endpoint");
}

#[test]
fn cli_requires_exactly_one_endpoint() {
    let output = Command::new(env!("CARGO_BIN_EXE_codex-awake-queue-probe"))
        .args([
            "--socket",
            "/tmp/unused.sock",
            "--websocket-url",
            "ws://127.0.0.1:4500/rpc",
            "--thread",
            THREAD,
            "--timeout-ms",
            "1000",
            "--check-only",
        ])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    let result: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        result["reason"],
        "choose exactly one of --socket and --websocket-url"
    );
}
