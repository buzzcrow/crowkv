// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `crowdb-cli` CLI entrypoint (R126 restructure).
//!
//! Four top-level domains: `cluster`, `kv`, `chunk`, `bench`. The CLI
//! talks directly to group-0 system metadata via `CrowdbSysmdClient`
//! and to individual `crowdb-kv-server` management APIs — no
//! `crowdb-web` intermediary. The connection target is
//! `--sysmd-ip`/`--sysmd-port` (a group-0 leader's crowdb-rpc endpoint).

mod commands;

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use crowdb_protocol::KV_SERVER_MGMT_BASE;

use commands::{
    run_bench_verb, run_chunk_diskdb_verb, run_chunk_stub_verb, run_cluster_verb, run_group_verb,
    run_kv_data_verb, run_kv_server_verb, run_replica_verb, run_store_verb, BenchVerb, ChunkDiskdbVerb,
    ChunkStubVerb, ClusterVerb, GroupVerb, KvDataVerb, KvServerVerb, ReplicaVerb, StoreVerb,
};

#[derive(Parser, Debug)]
#[command(name = "crowdb-cli", version, about = "CrowDB cluster console (CLI)")]
struct Cli {
    /// Group-0 leader's IP address (the sysmd endpoint).
    #[arg(long, global = true, env = "CROWDB_SYSMD_IP", default_value = "127.0.0.1")]
    sysmd_ip: String,

    /// Group-0 leader's port (the sysmd endpoint's mgmt port).
    #[arg(long, global = true, env = "CROWDB_SYSMD_PORT", default_value_t = KV_SERVER_MGMT_BASE, value_parser = clap::value_parser!(u16).range(1..))]
    sysmd_port: u16,

    /// Path to the console config file. Defaults to
    /// `$CROWDB_CONSOLE_CONFIG` or `~/.config/crowdb-kv/console.toml`.
    #[arg(short = 'p', long, global = true, env = "CROWDB_CONSOLE_CONFIG")]
    config: Option<PathBuf>,

    /// Emit JSON instead of human-readable output where applicable.
    #[arg(short = 'j', long, global = true)]
    json: bool,

    /// Root directory for this run's logs. Each invocation creates a
    /// per-run subfolder `<root>/<command-chain>-<YYYYMMDD-HHMMSS>/`
    /// holding the tracing log, crowdb-rpc transport log, and ops log.
    /// Defaults to `cli-log/` (resolved from CWD). Regression scripts
    /// typically pass a fixed root (e.g. `bench-log`) so runs accumulate
    /// a reviewable history.
    #[arg(long, global = true, env = "CROWDB_LOG_ROOT")]
    log_root: Option<PathBuf>,

    /// Per-invocation log directory, computed in `main()` after parse
    /// (not a CLI flag). Read by command handlers (e.g. `local-deploy`
    /// lands its workspace under here).
    #[arg(skip)]
    log_dir: PathBuf,

    #[command(subcommand)]
    command: Domain,
}

impl Cli {
    /// Kebab-case command chain for the per-invocation log folder name
    /// (e.g. `bench-kv`, `cluster-local-deploy`, `kv-server`). Two
    /// levels deep — enough to distinguish the high-value commands
    /// (bench-kv vs bench-rpc, cluster-local-deploy vs cluster-init)
    /// without walking every leaf verb.
    fn command_slug(&self) -> String {
        match &self.command {
            Domain::Cluster { verb } => {
                let v = match verb {
                    ClusterVerb::Init { .. } => "init",
                    ClusterVerb::LocalDeploy { .. } => "local-deploy",
                    ClusterVerb::Destroy => "destroy",
                    ClusterVerb::Reset => "reset",
                    ClusterVerb::Clean { .. } => "clean",
                    ClusterVerb::Status => "status",
                    ClusterVerb::Topology { .. } => "topology",
                    ClusterVerb::Rack { .. } => "rack",
                    ClusterVerb::Node { .. } => "node",
                    ClusterVerb::DiskGroup { .. } => "disk-group",
                    ClusterVerb::Disk { .. } => "disk",
                };
                format!("cluster-{v}")
            }
            Domain::Kv { verb } => {
                let v = match verb {
                    KvVerb::Server(_) => "server",
                    KvVerb::Store(_) => "store",
                    KvVerb::Group(_) => "group",
                    KvVerb::Replica(_) => "replica",
                    KvVerb::Data(_) => "data",
                };
                format!("kv-{v}")
            }
            Domain::Chunk { verb } => {
                let v = match verb {
                    ChunkVerb::Diskdb(_) => "diskdb",
                    ChunkVerb::Stub(_) => "stub",
                };
                format!("chunk-{v}")
            }
            Domain::Bench { verb } => {
                let v = match verb {
                    BenchVerb::Kv(_) => "kv",
                    BenchVerb::Rpc(_) => "rpc",
                    BenchVerb::Diskdb(_) => "diskdb",
                    BenchVerb::Chunkdb(_) => "chunkdb",
                    BenchVerb::Chunkio(_) => "chunkio",
                };
                format!("bench-{v}")
            }
        }
    }
}

#[derive(Subcommand, Debug)]
enum Domain {
    /// Hardware topology + cluster-level ops.
    #[command(alias = "cls")]
    Cluster {
        #[command(subcommand)]
        verb: ClusterVerb,
    },
    /// KV layer: server lifecycle + logical concepts + data-plane.
    Kv {
        #[command(subcommand)]
        verb: KvVerb,
    },
    /// Chunk storage service cluster.
    Chunk {
        #[command(subcommand)]
        verb: ChunkVerb,
    },
    /// Load injection only.
    Bench {
        #[command(subcommand)]
        verb: BenchVerb,
    },
}

#[derive(Subcommand, Debug)]
enum KvVerb {
    #[command(subcommand)]
    Server(KvServerVerb),
    #[command(subcommand)]
    Store(StoreVerb),
    #[command(subcommand)]
    Group(GroupVerb),
    #[command(subcommand)]
    Replica(ReplicaVerb),
    #[command(subcommand)]
    Data(KvDataVerb),
}

#[derive(Subcommand, Debug)]
enum ChunkVerb {
    #[command(subcommand)]
    Diskdb(ChunkDiskdbVerb),
    #[command(subcommand)]
    Stub(ChunkStubVerb),
}

fn main() -> ExitCode {
    let mut cli = Cli::parse();

    // Each CLI run gets its own log folder so logs from different
    // invocations don't interleave. The folder is
    // `<log_root>/<command-chain>-<YYYYMMDD-HHMMSS>/` and holds the
    // tracing log, the C++ crowdb-rpc transport log, and the ops log.
    // `--log-root` defaults to `cli-log/` (CWD-relative); regression
    // scripts pass a fixed root (e.g. `bench-log`) to accumulate runs.
    let log_root = cli.log_root.clone().unwrap_or_else(|| {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join("cli-log")
    });
    let invocation_dir = log_root.join(format!(
        "{}-{}",
        cli.command_slug(),
        crowdb_common::logging::timestamp_secs()
    ));
    let _ = std::fs::create_dir_all(&invocation_dir);
    cli.log_dir.clone_from(&invocation_dir);
    eprintln!("log dir: {}", invocation_dir.display());

    let _log_guards = crowdb_common::logging::init_file_logging(
        &invocation_dir,
        "crowdb-cli",
        50,
        5,
        "warn,crowdb_cli=info,crowdb_console_shared=info,crowdb_kv_client=info",
    );
    crowdb_rpc_ffi::init_logging(
        invocation_dir.to_str().unwrap_or("cli-log"),
        "info",
        50,
        5,
        "crowdb-cli-rpc",
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let cid = crowdb_console_shared::corr_id::generate();
    runtime.block_on(async move { Box::pin(crowdb_console_shared::corr_id::scope(cid, dispatch(cli))).await })
}

async fn dispatch(mut cli: Cli) -> ExitCode {
    let command = std::mem::replace(
        &mut cli.command,
        Domain::Cluster {
            verb: ClusterVerb::Status,
        },
    );
    match command {
        Domain::Cluster { verb } => run_cluster_verb(&cli, verb).await,
        Domain::Kv { verb } => match verb {
            KvVerb::Server(sv) => run_kv_server_verb(&cli, sv).await,
            KvVerb::Store(sv) => run_store_verb(&cli, sv).await,
            KvVerb::Group(gv) => run_group_verb(&cli, gv).await,
            KvVerb::Replica(rv) => run_replica_verb(&cli, rv).await,
            KvVerb::Data(dv) => run_kv_data_verb(&cli, dv).await,
        },
        Domain::Chunk { verb } => match verb {
            ChunkVerb::Diskdb(dv) => run_chunk_diskdb_verb(&cli, dv).await,
            ChunkVerb::Stub(sv) => run_chunk_stub_verb(&cli, sv).await,
        },
        Domain::Bench { verb } => run_bench_verb(&cli, verb).await,
    }
}
