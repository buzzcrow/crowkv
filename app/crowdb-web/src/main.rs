// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `crowdb-web` binary entrypoint.

use std::net::SocketAddr;

use clap::Parser;
use crowdb_common::logging::init_file_and_console_logging_split;
use crowdb_protocol::WEB_BASE;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    #[derive(Parser, Debug)]
    #[command(name = "crowdb-web")]
    struct Args {
        /// Bind address for the web server (default: 0.0.0.0)
        #[arg(long, default_value = "0.0.0.0")]
        bind: String,

        /// Port for the web server (default: 14000)
        #[arg(long, default_value_t = WEB_BASE, value_parser = clap::value_parser!(u16).range(1..))]
        port: u16,

        /// Use an in-memory registry instead of the persisted console config.
        #[arg(long)]
        test_mode: bool,

        /// Log directory. Default: ~/.crowdb-kv/log.
        #[arg(long)]
        log_dir: Option<std::path::PathBuf>,

        /// Log level for both Rust and C++ stacks. Default: "info"
        /// (or derived from `RUST_LOG`).
        #[arg(long)]
        log_level: Option<String>,

        /// Max log file size in MiB before rotation. Default: 30.
        #[arg(long, default_value_t = crowdb_common::logging::DEFAULT_LOG_MAX_FILE_MB)]
        log_max_file_mb: usize,

        /// Number of rotated log files to keep. Default: 5.
        #[arg(long, default_value_t = crowdb_common::logging::DEFAULT_LOG_MAX_FILES)]
        log_max_files: usize,

        /// Also print logs to console (in addition to file logging).
        #[arg(short = 'l', long)]
        log: bool,

        /// Mirror C++ log lines at this level or above to stderr.
        /// Default: "warn" (mirrors warn+error to stderr).
        #[arg(long)]
        log_stderr: Option<String>,
    }

    let args = Args::parse();

    // Layered logging: INFO+ to rotating file, WARN+ to console.
    // RUST_LOG overrides both sinks for debugging. The file layer uses
    // ~/.crowdb-kv/log/ by default; the guard must outlive the process
    // so the non-blocking appender flushes on exit.
    let log_dir = args.log_dir.clone().unwrap_or_else(|| {
        dirs::home_dir()
            .unwrap_or_else(std::env::temp_dir)
            .join(".crowdb-kv")
            .join("log")
    });
    let log_dir_str = log_dir.to_string_lossy().to_string();
    let cpp_level = args
        .log_level
        .clone()
        .unwrap_or_else(|| crowdb_common::logging::cpp_level_from_rust_log("info"));

    let _log_guards = if args.log {
        init_file_and_console_logging_split(
            &log_dir,
            "console-web",
            args.log_max_file_mb,
            args.log_max_files,
            "info",
            "warn",
        )
        .map_err(|e| {
            eprintln!("failed to initialize logging: {e}");
            e
        })?
    } else {
        crowdb_common::logging::init_file_logging(
            &log_dir,
            "console-web",
            args.log_max_file_mb,
            args.log_max_files,
            "info",
        )
        .map_err(|e| {
            eprintln!("failed to initialize logging: {e}");
            e
        })?
    };

    // Initialize the crowdb-rpc C++ spdlog logger so transport info/debug
    // messages go to rotating files instead of spdlog's default stderr
    // logger. Uses the SAME log directory as the Rust tracing init —
    // not the literal "log" (fixes the previous directory mismatch).
    // No-op without spdlog.
    crowdb_rpc_ffi::init_logging(
        &log_dir_str,
        &cpp_level,
        args.log_max_file_mb,
        args.log_max_files,
        "crowdb-web-rpc",
    );

    // Default: mirror warn+error to stderr (previous unconditional
    // behavior). Override with --log-stderr <level> or disable with
    // --log-stderr off.
    let stderr_level = args.log_stderr.as_deref().unwrap_or("warn");
    if stderr_level != "off" {
        crowdb_rpc_ffi::add_log_stderr(stderr_level);
    }

    let addr: SocketAddr = format!("{}:{}", args.bind, args.port).parse()?;
    info!(%addr, "crowdb-web starting");

    let listener = tokio::net::TcpListener::bind(addr).await?;

    // Load the persisted registry; absence yields an empty default.
    // Mutating handlers (rack/node/server CRUD) write back to this path.
    let path = if args.test_mode {
        None
    } else {
        crowdb_console_shared::TomlFileEngine::default_path()
    };
    let cfg = match path.as_ref() {
        Some(p) => {
            let engine = crowdb_console_shared::TomlFileEngine::new(p.clone());
            crowdb_console_shared::ConsoleConfig::load_with_engine(&engine).unwrap_or_default()
        }
        None => crowdb_console_shared::ConsoleConfig::default(),
    };
    let server_count = cfg.servers.len();
    let state = crowdb_web::AppState::with_config(cfg, path).with_test_mode(args.test_mode);
    tracing::info!(servers = server_count, "loaded registry");
    crowdb_web::mgmt::startup_topology_check(&state).await;

    axum::serve(listener, crowdb_web::router(state)).await?;
    Ok(())
}
