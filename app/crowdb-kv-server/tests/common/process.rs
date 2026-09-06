// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::io as std_io;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

pub struct ServerHandle {
    child: Child,
    base_url: String,
    root: Option<crowdb_test_harness::test_dirs::TestDir>,
    stderr_buf: Arc<Mutex<Vec<String>>>,
}

impl ServerHandle {
    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub async fn wait_for_ready(&self, timeout: Duration) -> std_io::Result<()> {
        let client = reqwest::Client::new();
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if let Ok(resp) = client.get(format!("{}/health", self.base_url)).send().await {
                if resp.status().is_success() || resp.status().as_u16() == 503 {
                    return Ok(());
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Err(std_io::Error::new(
            std_io::ErrorKind::TimedOut,
            "server was not ready before timeout",
        ))
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        let pid = self.child.id();
        let _ = std::process::Command::new("kill")
            .arg("-TERM")
            .arg(pid.to_string())
            .status();
        let start = Instant::now();
        loop {
            match self.child.try_wait() {
                Ok(Some(_)) | Err(_) => break,
                Ok(None) => {
                    if start.elapsed() >= Duration::from_millis(500) {
                        let _ = std::process::Command::new("kill")
                            .arg("-KILL")
                            .arg(pid.to_string())
                            .status();
                        let _ = self.child.wait();
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
        }
        if std::thread::panicking() {
            let stderr_lines = self
                .stderr_buf
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !stderr_lines.is_empty() {
                let start = stderr_lines.len().saturating_sub(80);
                eprintln!(
                    "\n=== crowdb-kv-server pid={pid} stderr (last {} lines) ===\n{}\n=== end stderr ===",
                    stderr_lines.len() - start,
                    stderr_lines[start..].join("\n")
                );
            }
            eprintln!(
                "crowdb-kv-server pid={pid} logs: look for crowdb-kv-server-*-{pid}.log under app/crowdb-kv-server/log/"
            );
        }
    }
}

#[allow(dead_code)]
pub async fn start_test_server(args: &[&str]) -> std_io::Result<ServerHandle> {
    start_test_server_with_ports(args, &[0]).await
}

/// Like [`start_test_server`] but lets the caller supply one port per store
/// (e.g. `&[0, 0]` for a two-store process). Each entry maps to a store in
/// the order given by `--stores`; `0` lets the OS assign a port.
pub async fn start_test_server_with_ports(args: &[&str], ports: &[u16]) -> std_io::Result<ServerHandle> {
    // One tempdir serves as the node root; waldata/conf/ctdata/log are
    // derived subdirs. No toml is needed (--config is optional; defaults
    // apply, and the e2e election profile is a CLI flag below).
    let root = crowdb_test_harness::test_dirs::TestDir::new("kv-server")?;
    let mut handle = start_test_server_at(root.path(), args, ports).await?;
    handle.root = Some(root); // server owns the tempdir's lifetime
    Ok(handle)
}

/// Start a server at a caller-owned `root` path. The caller is
/// responsible for keeping `root` alive for the process's lifetime
/// (e.g. holding the `TestDir`). Used by restart/restore
/// tests that need the same on-disk state across stop/start cycles.
pub async fn start_test_server_at(
    root: &std::path::Path,
    args: &[&str],
    ports: &[u16],
) -> std_io::Result<ServerHandle> {
    // Allocate ports via the flock-coordinated port allocator.
    // Port 0 is not allowed — allocate from the service ranges.
    let mgmt_port = crowdb_protocol::port_alloc::alloc_test_port(crowdb_protocol::ServicePort::KvServerMgmt);
    let alloc_ports: Vec<u16> = ports
        .iter()
        .map(|&p| {
            if p == 0 {
                crowdb_protocol::port_alloc::alloc_test_port(crowdb_protocol::ServicePort::KvServerListen)
            } else {
                p
            }
        })
        .collect();
    let ports_str = alloc_ports
        .iter()
        .map(u16::to_string)
        .collect::<Vec<_>>()
        .join(",");

    let bin = crowdb_kv_server_bin();
    let mut cmd = Command::new(bin);
    cmd.args(args)
        .arg("--root")
        .arg(root)
        .arg("--management-addr")
        .arg("127.0.0.1")
        .arg("--management-port")
        .arg(mgmt_port.to_string())
        .arg("--ports")
        .arg(&ports_str);
    cmd.arg("--election-profile")
        .arg("e2e")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = cmd.spawn()?;

    let stdout = child.stdout.take().expect("stdout should be captured");
    let stderr = child.stderr.take().expect("stderr should be captured");
    let (tx, rx) = mpsc::channel();
    let stderr_buf = Arc::new(Mutex::new(Vec::<String>::new()));
    let stderr_buf_clone = Arc::clone(&stderr_buf);

    thread::spawn(move || {
        use std::io::{BufRead, BufReader};
        let reader = BufReader::new(stdout);
        for line in reader.lines() {
            if let Ok(l) = line {
                if l.contains("management_addr=") {
                    if let Some(idx) = l.find("management_addr=") {
                        let after = &l[idx + "management_addr=".len()..];
                        let _ = tx.send(after.trim().to_string());
                        break;
                    }
                }
            } else {
                // Stdio error - process likely exited early, stop reading
                break;
            }
        }
    });

    thread::spawn(move || {
        use std::io::{BufRead, BufReader};
        let reader = BufReader::new(stderr);
        for line in reader.lines().map_while(Result::ok) {
            stderr_buf_clone.lock().unwrap().push(line);
        }
    });

    let deadline = Instant::now() + Duration::from_secs(10);
    #[allow(clippy::never_loop)]
    let addr = loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match rx.recv_timeout(remaining) {
            Ok(addr) => break addr,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                return Err(std_io::Error::new(
                    std_io::ErrorKind::TimedOut,
                    "management_addr was not found in stdout before timeout",
                ));
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                let _ = child.wait();
                let stderr_lines = stderr_buf.lock().unwrap();
                let msg = if stderr_lines.is_empty() {
                    "stdout reader thread disconnected (process exited early)".to_string()
                } else {
                    format!(
                        "stdout reader thread disconnected; stderr:\n{}",
                        stderr_lines.join("\n")
                    )
                };
                return Err(std_io::Error::new(std_io::ErrorKind::BrokenPipe, msg));
            }
        }
    };

    let handle = ServerHandle {
        child,
        base_url: format!("http://{addr}"),
        root: None,
        stderr_buf,
    };
    handle.wait_for_ready(Duration::from_secs(10)).await?;
    Ok(handle)
}

fn crowdb_kv_server_bin() -> PathBuf {
    if let Some(path) = option_env!("CARGO_BIN_EXE_crowdb-kv-server") {
        return PathBuf::from(path);
    }

    // Walk up from test executable (target/debug/deps/test-name) to target/debug/
    let mut path = std::env::current_exe().expect("current test executable path");
    while path
        .file_name()
        .is_some_and(|name| name != "debug" && name != "release")
    {
        path.pop();
    }
    // Now at target/debug/ or target/release/, push the binary name
    path.push("crowdb-kv-server");
    path
}
