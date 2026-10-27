use codex_awake_queue_probe::{Action, Outcome, run};
use std::{collections::HashMap, path::PathBuf, process::ExitCode, time::Duration};

const HELP: &str = "Usage: codex-awake-queue-probe --socket ABSOLUTE_PATH --thread ID --timeout-ms N\n  --check-only\n  OR --client-message-id ID --message-file PATH\nConnects to an existing App Server; never starts Codex or retries delivery.";

fn options() -> Result<(PathBuf, String, Action, Duration), String> {
    let mut args = std::env::args().skip(1);
    let mut values = HashMap::new();
    let mut check = false;
    while let Some(key) = args.next() {
        if key == "--check-only" {
            if check {
                return Err("duplicate --check-only".into());
            }
            check = true;
            continue;
        }
        if ![
            "--socket",
            "--thread",
            "--timeout-ms",
            "--client-message-id",
            "--message-file",
        ]
        .contains(&key.as_str())
        {
            return Err(format!("unknown option: {key}"));
        }
        let value = args.next().ok_or_else(|| format!("missing value: {key}"))?;
        if value.starts_with("--") {
            return Err(format!("missing value: {key}"));
        }
        if values.insert(key, value).is_some() {
            return Err("duplicate option".into());
        }
    }
    let mut required = |key: &str| values.remove(key).ok_or_else(|| format!("required: {key}"));
    let socket = PathBuf::from(required("--socket")?);
    let thread = required("--thread")?;
    let milliseconds = required("--timeout-ms")?
        .parse::<u64>()
        .ok()
        .filter(|n| *n > 0)
        .ok_or("--timeout-ms must be a positive integer")?;
    let action = if check {
        if !values.is_empty() {
            return Err("--check-only cannot include message options".into());
        }
        Action::Check
    } else {
        let client_message_id = values
            .remove("--client-message-id")
            .ok_or("required: --client-message-id")?;
        let path = values
            .remove("--message-file")
            .ok_or("required: --message-file")?;
        let message =
            std::fs::read_to_string(path).map_err(|_| "cannot read UTF-8 message file")?;
        Action::Enqueue {
            client_message_id,
            message,
        }
    };
    Ok((socket, thread, action, Duration::from_millis(milliseconds)))
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    if std::env::args().any(|arg| arg == "--help") {
        println!("{HELP}");
        return ExitCode::SUCCESS;
    }
    let outcome = match options() {
        Ok((socket, thread, action, deadline)) => run(&socket, &thread, action, deadline).await,
        Err(reason) => Outcome::NotSent { reason },
    };
    println!(
        "{}",
        serde_json::to_string(&outcome).expect("serializable outcome")
    );
    ExitCode::from(match outcome {
        Outcome::Checked { .. } | Outcome::Accepted { .. } => 0,
        Outcome::NotSent { .. } => 2,
        Outcome::Rejected { .. } => 3,
        Outcome::DeliveryUnknown { .. } => 4,
    })
}
