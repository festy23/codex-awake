#![cfg(unix)]

use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use serde_json::{Value, json};
use std::{collections::HashSet, path::Path, time::Duration};
use tokio::{net::UnixStream, time::timeout};
use tokio_tungstenite::{WebSocketStream, client_async, tungstenite::Message};

pub enum Action {
    Check,
    Enqueue {
        client_message_id: String,
        message: String,
    },
}

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Outcome {
    Checked {
        thread_id: String,
    },
    Accepted {
        thread_id: String,
        queued_submission_id: String,
        client_message_id: String,
    },
    NotSent {
        reason: String,
    },
    Rejected {
        code: i64,
    },
    DeliveryUnknown {
        reason: String,
    },
}

// The probe keeps the protocol sequence together so the no-retry boundary is visible.
#[allow(clippy::too_many_lines, clippy::indexing_slicing)]
pub async fn run(socket: &Path, thread_id: &str, action: Action, deadline: Duration) -> Outcome {
    if !socket.is_absolute() || thread_id.trim().is_empty() || deadline.is_zero() {
        return not_sent("invalid_arguments");
    }
    if let Action::Enqueue {
        client_message_id,
        message,
    } = &action
        && (client_message_id.trim().is_empty() || message.trim().is_empty())
    {
        return not_sent("empty_message_or_id");
    }

    // Connect only to the endpoint supplied by the caller. No process spawning,
    // endpoint discovery, thread resume, or fallback to Desktop's internal IPC.
    let connection = timeout(deadline, async {
        let stream = UnixStream::connect(socket).await.map_err(|_| ())?;
        let (ws, _) = client_async("ws://localhost/rpc", stream)
            .await
            .map_err(|_| ())?;
        Ok::<_, ()>(ws)
    })
    .await;
    let Ok(Ok(ws)) = connection else {
        return not_sent("connection_failed_or_timed_out");
    };
    let mut client = Client {
        ws,
        next_id: 1,
        deadline,
    };
    let initialized = client.rpc("initialize", json!({
        "clientInfo": {"name": "codex_awake_queue_probe", "version": env!("CARGO_PKG_VERSION")},
        "capabilities": {"experimentalApi": true}
    })).await;
    if let Err(error) = initialized {
        return not_sent(&format!("initialize_{}", error.reason()));
    }
    if !matches!(
        timeout(
            deadline,
            client.ws.send(Message::Text(
                json!({
                    "method": "initialized", "params": {}
                })
                .to_string()
                .into()
            ))
        )
        .await,
        Ok(Ok(()))
    ) {
        return not_sent("initialized_notification_failed");
    }

    let mut cursor = None;
    let mut seen_cursors = HashSet::new();
    loop {
        let mut params = json!({"limit": 100});
        if let Some(value) = &cursor {
            params["cursor"] = json!(value);
        }
        let page = match client.rpc("thread/loaded/list", params).await {
            Ok(page) => page,
            Err(error) => return not_sent(&format!("loaded_list_{}", error.reason())),
        };
        let Some(data) = page.get("data").and_then(Value::as_array) else {
            return not_sent("invalid_loaded_list");
        };
        if data.iter().any(|id| !id.is_string()) {
            return not_sent("invalid_loaded_list");
        }
        if data.iter().any(|id| id.as_str() == Some(thread_id)) {
            break;
        }
        match page.get("nextCursor") {
            None | Some(Value::Null) => return not_sent("target_not_loaded"),
            Some(Value::String(value))
                if !value.is_empty() && seen_cursors.insert(value.clone()) =>
            {
                cursor = Some(value.clone());
            }
            _ => return not_sent("invalid_pagination_cursor"),
        }
    }
    let Action::Enqueue {
        client_message_id,
        message,
    } = action
    else {
        return Outcome::Checked {
            thread_id: thread_id.into(),
        };
    };

    // From this point any transport failure can follow a successful enqueue.
    // In particular, do not reconnect or repeat the request after a timeout.
    let result = client
        .rpc(
            "thread/queue/add",
            json!({
                "threadId": thread_id,
                "clientUserMessageId": client_message_id,
                "input": [{"type": "text", "text": message}]
            }),
        )
        .await;
    let receipt = match result {
        Ok(result) => result,
        Err(Failure::Rpc(code @ -32602..=-32600)) => return Outcome::Rejected { code },
        Err(error) => {
            return Outcome::DeliveryUnknown {
                reason: error.reason(),
            };
        }
    };
    let submission = &receipt["queuedSubmission"];
    let id = submission
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| !id.trim().is_empty());
    let input = submission.get("input").and_then(Value::as_array);
    let Some(id) = id else {
        return Outcome::DeliveryUnknown {
            reason: "invalid_or_mismatched_receipt".into(),
        };
    };
    if submission["clientUserMessageId"].as_str() != Some(&client_message_id)
        || !input.is_some_and(|items| {
            items.len() == 1
                && items[0]["type"] == "text"
                && items[0]["text"].as_str() == Some(&message)
        })
    {
        return Outcome::DeliveryUnknown {
            reason: "invalid_or_mismatched_receipt".into(),
        };
    }
    Outcome::Accepted {
        thread_id: thread_id.into(),
        queued_submission_id: id.into(),
        client_message_id,
    }
}

fn not_sent(reason: &str) -> Outcome {
    Outcome::NotSent {
        reason: reason.into(),
    }
}

enum Failure {
    Rpc(i64),
    Protocol(&'static str),
}

impl Failure {
    fn reason(&self) -> String {
        match self {
            Self::Rpc(code) => format!("rpc_error_{code}"),
            Self::Protocol(reason) => (*reason).into(),
        }
    }
}

struct Client {
    ws: WebSocketStream<UnixStream>,
    next_id: u64,
    deadline: Duration,
}

impl Client {
    // The timeout wraps the complete send/read exchange; JSON Value indexing preserves null-on-missing semantics.
    #[allow(clippy::excessive_nesting, clippy::indexing_slicing)]
    async fn rpc(&mut self, method: &str, params: Value) -> Result<Value, Failure> {
        let id = self.next_id;
        self.next_id += 1;
        let request = json!({"id": id, "method": method, "params": params});
        timeout(self.deadline, async {
            self.ws
                .send(Message::Text(request.to_string().into()))
                .await
                .map_err(|_| Failure::Protocol("send_failed"))?;
            loop {
                let frame = self
                    .ws
                    .next()
                    .await
                    .ok_or(Failure::Protocol("connection_closed"))?
                    .map_err(|_| Failure::Protocol("receive_failed"))?;
                let text = match frame {
                    Message::Text(text) => text,
                    Message::Ping(_) | Message::Pong(_) => {
                        self.ws
                            .flush()
                            .await
                            .map_err(|_| Failure::Protocol("receive_failed"))?;
                        continue;
                    }
                    Message::Close(_) => return Err(Failure::Protocol("connection_closed")),
                    _ => return Err(Failure::Protocol("unexpected_websocket_frame")),
                };
                let response: Value =
                    serde_json::from_str(&text).map_err(|_| Failure::Protocol("invalid_json"))?;
                if response.get("method").is_some() && response.get("id").is_none() {
                    continue;
                }
                if response.get("method").is_some() || response["id"] != id {
                    return Err(Failure::Protocol("unexpected_rpc_message"));
                }
                if response.get("result").is_some() && response.get("error").is_some() {
                    return Err(Failure::Protocol("invalid_rpc_response"));
                }
                if let Some(error) = response.get("error") {
                    return Err(error["code"]
                        .as_i64()
                        .map_or(Failure::Protocol("invalid_rpc_error"), Failure::Rpc));
                }
                return response
                    .get("result")
                    .cloned()
                    .ok_or(Failure::Protocol("missing_rpc_result"));
            }
        })
        .await
        .unwrap_or(Err(Failure::Protocol("timeout")))
    }
}
