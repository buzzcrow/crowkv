// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `bench` CLI verb definitions + dispatch. The heavy workload logic
//! lives in the per-workload sub-modules; this file only routes.

use std::process::ExitCode;

use clap::{Subcommand, ValueEnum};

use crate::Cli;

/// Top-level `bench` verb: either an RPC echo workload or a KV workload.
#[derive(Subcommand, Debug)]
pub enum BenchVerb {
    /// KV-layer workload (prepare / read / write / scan / clean).
    #[command(subcommand)]
    Kv(KvBenchVerb),
    /// Raw crowdb-rpc echo throughput benchmark against a fb-server.
    Rpc(RpcArgs),
    /// Distributed disk-block allocation benchmark.
    #[command(subcommand)]
    Diskdb(DiskdbBenchVerb),
    /// Distributed chunk lifecycle benchmark.
    #[command(subcommand)]
    Chunkdb(ChunkdbBenchVerb),
    /// End-to-end chunk data IO benchmark.
    #[command(subcommand)]
    Chunkio(ChunkioBenchVerb),
}

#[derive(Subcommand, Debug)]
pub enum ChunkioBenchVerb {
    /// Stream deterministic large objects through ChunkDB and DiskIO.
    Write(ChunkioArgs),
}

#[derive(clap::Args, Debug, Clone)]
pub struct ChunkioArgs {
    #[arg(long, default_value_t = 1)]
    pub objects: u64,
    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    pub object_size: u64,
    #[arg(long, default_value_t = 1)]
    pub concurrency: usize,
    #[arg(long, default_value_t = 1024 * 1024)]
    pub block_size: usize,
    #[arg(long, default_value_t = 1024 * 1024 * 1024)]
    pub chunk_size: u64,
    #[arg(long, default_value_t = 4)]
    pub data_num: usize,
    #[arg(long, default_value_t = 1)]
    pub code_num: usize,
    #[arg(long, default_value_t = 1)]
    pub seed: u8,
    #[arg(long, default_value_t = 1)]
    pub metrics_interval: u64,
}

#[derive(Subcommand, Debug)]
pub enum ChunkdbBenchVerb {
    /// Allocate chunks until the time limit.
    Allocate(ChunkdbArgs),
    /// Run a deterministic lifecycle operation mix.
    Mix(ChunkdbArgs),
}

#[derive(clap::Args, Debug, Clone)]
pub struct ChunkdbArgs {
    #[arg(long, default_value_t = 10)]
    pub duration_secs: u64,
    #[arg(long, default_value_t = 4)]
    pub concurrency: usize,
    /// Connections kept per `ChunkDB` endpoint.
    #[arg(long, default_value_t = 1)]
    pub chunkdb_connections: usize,
    /// crowdb-rpc I/O workers used by the `ChunkDB` client transport.
    #[arg(long, default_value_t = 2)]
    pub chunkdb_client_rpc_workers: u32,
    #[arg(long, default_value_t = 1)]
    pub strip_count: u32,
    #[arg(long, default_value_t = 1024)]
    pub write_granularity_kb: u32,
    #[arg(long, value_enum, default_value_t = ChunkdbStripMode::Mirror)]
    pub strip_type: ChunkdbStripMode,
    #[arg(long, default_value_t = 3)]
    pub copy_count: u32,
    #[arg(long, default_value_t = 4)]
    pub data_num: u32,
    #[arg(long, default_value_t = 2)]
    pub code_num: u32,
    #[arg(long, default_value_t = 1)]
    pub seed: u64,
    /// Metrics flush interval in seconds. 0 disables the metrics log.
    #[arg(long, default_value_t = 1)]
    pub metrics_interval: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum ChunkdbStripMode {
    Mirror,
    Ec,
}

#[derive(Subcommand, Debug)]
pub enum DiskdbBenchVerb {
    /// Allocate until capacity exhaustion or the time limit.
    Allocate(DiskdbArgs),
    /// Run deterministic 70% allocate and 30% free traffic.
    Mix(DiskdbArgs),
}

#[derive(clap::Args, Debug, Clone)]
pub struct DiskdbArgs {
    /// Run duration in seconds.
    #[arg(long, default_value_t = 10)]
    pub duration_secs: u64,
    /// Concurrent workload tasks.
    #[arg(long, default_value_t = 4)]
    pub concurrency: usize,
    /// Connections kept per `DiskDB` endpoint.
    #[arg(long, default_value_t = 1)]
    pub diskdb_connections: usize,
    /// crowdb-rpc I/O workers used by the `DiskDB` client transport.
    #[arg(long, default_value_t = 2)]
    pub diskdb_client_rpc_workers: u32,
    /// Allocation size in disk units.
    #[arg(long, default_value_t = 1)]
    pub unit_count: u32,
    /// Blocks requested by each allocation RPC.
    #[arg(long, default_value_t = 1)]
    pub blocks_per_request: u32,
    /// Expected unit size, used for space verification.
    #[arg(long, default_value_t = 1_048_576)]
    pub unit_size_bytes: u64,
    /// Storage mode configured by cluster initialization.
    #[arg(long, value_enum, default_value_t = DiskdbBenchMode::Mem)]
    pub mode: DiskdbBenchMode,
    /// Deterministic workload seed.
    #[arg(long, default_value_t = 1)]
    pub seed: u64,
    /// Metrics flush interval in seconds. 0 disables the metrics log.
    #[arg(long, default_value_t = 1)]
    pub metrics_interval: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum DiskdbBenchMode {
    Mem,
    Block,
}

#[derive(Subcommand, Debug)]
pub enum KvBenchVerb {
    /// Pre-populate `--keys` keys into store 0 / group 0.
    Prepare(PrepareArgs),
    /// Point-get workload.
    Read(ReadArgs),
    /// Put workload.
    Write(WriteArgs),
    /// Scan (list) workload.
    Scan(ScanArgs),
}

#[derive(clap::Args, Debug)]
pub struct RpcArgs {
    /// Run duration in seconds.
    #[arg(long, default_value_t = 10)]
    pub duration_secs: u64,
    /// Number of loader coroutines / tasks.
    #[arg(long, default_value_t = 1)]
    pub loader_num: usize,
    /// Number of client connections to the fb-server.
    #[arg(long, default_value_t = 1)]
    pub connections: usize,
    /// Echo payload size in bytes.
    #[arg(long, default_value_t = 128)]
    pub value_size: usize,
    /// Client transport I/O engines (must match the fb-server).
    #[arg(long, default_value_t = 1)]
    pub io_engines: u32,
    /// Client transport I/O worker threads (must match the fb-server).
    #[arg(long, default_value_t = 1)]
    pub io_workers: u32,
    /// Concurrency model: `coroutine` (C++ coroutines) or `tokio`.
    #[arg(long, value_enum, default_value_t = BenchMode::Coroutine)]
    pub mode: BenchMode,
    /// fb-server port to connect to.
    #[arg(long)]
    pub server_port: u16,
    /// Enable TCP Nagle (coalesce small frames per writev).
    #[arg(long, default_value_t = false)]
    pub enable_nagle: bool,
    /// Metrics flush interval in seconds. 0 disables the metrics log.
    #[arg(long, default_value_t = 1)]
    pub metrics_interval: u64,
}

#[derive(clap::Args, Debug)]
pub struct PrepareArgs {
    /// Number of keys to write into the target store/group.
    #[arg(long, default_value_t = 100_000)]
    pub keys: u64,
    /// Value size in bytes.
    #[arg(long, default_value_t = 64)]
    pub value_size: usize,
    /// Concurrent put tasks.
    #[arg(long, default_value_t = 16)]
    pub concurrency: usize,
    /// Target store ID (default 0 = system store).
    #[arg(long, default_value_t = 0)]
    pub store: u64,
    /// Target group ID (default 0 = system group; use 1+ for bench groups).
    #[arg(long, default_value_t = 0)]
    pub group: u64,
}

#[derive(clap::Args, Debug)]
pub struct ReadArgs {
    #[arg(long, default_value_t = 10)]
    pub duration_secs: u64,
    #[arg(long, default_value_t = 1)]
    pub loader_num: usize,
    #[arg(long, default_value_t = 1)]
    pub connections: usize,
    /// Read consistency: `linearizable` (lease barrier) or `minslot`.
    #[arg(long, value_enum, default_value_t = BenchReadMode::Linearizable)]
    pub read_mode: BenchReadMode,
    /// `MinSlot` lower bound: `auto` (client high-watermark) or `zero`.
    #[arg(long, value_enum, default_value_t = BenchMinSlot::Auto)]
    pub min_slot: BenchMinSlot,
    /// `MinSlot` endpoint selection: `leader` or `any-replica`.
    #[arg(long, value_enum, default_value_t = BenchReadEndpoint::Leader)]
    pub read_endpoint_policy: BenchReadEndpoint,
    /// Keyspace size (keys are `k{id:020}`, id in `[0, key_space)`).
    #[arg(long, default_value_t = 100_000)]
    pub key_space: u64,
    /// Value size in bytes (for correctness verification).
    #[arg(long, default_value_t = 64)]
    pub value_size: usize,
    /// Verify the first N value bytes match the expected pattern
    /// (`0..N` mod 256). 0 disables verification.
    #[arg(long, default_value_t = 0)]
    pub verify_bytes: usize,
    /// Metrics flush interval in seconds. 0 disables the metrics log.
    #[arg(long, default_value_t = 1)]
    pub metrics_interval: u64,
    /// Target store ID (default 0 = system store).
    #[arg(long, default_value_t = 0)]
    pub store: u64,
    /// Target group ID (default 0 = system group; use 1+ for bench groups).
    #[arg(long, default_value_t = 0)]
    pub group: u64,
}

#[derive(clap::Args, Debug)]
pub struct WriteArgs {
    #[arg(long, default_value_t = 10)]
    pub duration_secs: u64,
    #[arg(long, default_value_t = 1)]
    pub loader_num: usize,
    #[arg(long, default_value_t = 1)]
    pub connections: usize,
    #[arg(long, default_value_t = 1_000_000)]
    pub key_space: u64,
    #[arg(long, default_value_t = 512)]
    pub value_size: usize,
    #[arg(long, default_value_t = 0)]
    pub verify_bytes: usize,
    /// Client transport: event-write mode (coalesce frames via I/O worker).
    #[arg(long, default_value_t = false)]
    pub event_write: bool,
    /// Client transport: I/O worker threads (match server --rpc-workers).
    #[arg(long, default_value_t = 2)]
    pub rpc_workers: u32,
    /// Client transport: per-connection send queue capacity. 0 = default (4096).
    #[arg(long, default_value_t = 0)]
    pub send_queue_capacity: u32,
    /// Metrics flush interval in seconds. 0 disables the metrics log.
    #[arg(long, default_value_t = 1)]
    pub metrics_interval: u64,
    /// Target store ID (default 0 = system store).
    #[arg(long, default_value_t = 0)]
    pub store: u64,
    /// Target group ID (default 0 = system group; use 1+ for bench groups).
    #[arg(long, default_value_t = 0)]
    pub group: u64,
}

#[derive(clap::Args, Debug)]
pub struct ScanArgs {
    #[arg(long, default_value_t = 10)]
    pub duration_secs: u64,
    #[arg(long, default_value_t = 1)]
    pub loader_num: usize,
    #[arg(long, default_value_t = 1)]
    pub connections: usize,
    #[arg(long, value_enum, default_value_t = BenchReadMode::Linearizable)]
    pub read_mode: BenchReadMode,
    #[arg(long, value_enum, default_value_t = BenchMinSlot::Auto)]
    pub min_slot: BenchMinSlot,
    #[arg(long, value_enum, default_value_t = BenchReadEndpoint::Leader)]
    pub read_endpoint_policy: BenchReadEndpoint,
    /// Maximum items per scan request.
    #[arg(long, default_value_t = 1000)]
    pub scan_limit: u32,
    /// Prefix filter (empty = no prefix).
    #[arg(long, default_value = "")]
    pub scan_prefix: String,
    /// Exclusive lower bound (empty = start from beginning).
    #[arg(long, default_value = "")]
    pub scan_start_after: String,
    #[arg(long, default_value_t = 64)]
    pub value_size: usize,
    #[arg(long, default_value_t = 100_000)]
    pub key_space: u64,
    #[arg(long, default_value_t = 0)]
    pub verify_bytes: usize,
    /// Mixed value-size distribution, e.g. `64:70,1024:20,16384:10`
    /// (size:percent). When set, overrides `--value-size`.
    #[arg(long)]
    pub value_size_mix: Option<String>,
    /// Metrics flush interval in seconds. 0 disables the metrics log.
    #[arg(long, default_value_t = 1)]
    pub metrics_interval: u64,
    /// Target store ID (default 0 = system store).
    #[arg(long, default_value_t = 0)]
    pub store: u64,
    /// Target group ID (default 0 = system group; use 1+ for bench groups).
    #[arg(long, default_value_t = 0)]
    pub group: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum BenchMode {
    Coroutine,
    Tokio,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum BenchReadMode {
    Linearizable,
    Minslot,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum BenchMinSlot {
    Auto,
    Zero,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum BenchReadEndpoint {
    Leader,
    AnyReplica,
}

pub async fn run_bench_verb(cli: &Cli, verb: BenchVerb) -> ExitCode {
    match verb {
        BenchVerb::Rpc(args) => super::rpc::run(cli, args).await,
        BenchVerb::Diskdb(verb) => super::diskdb::run(cli, verb).await,
        BenchVerb::Chunkdb(verb) => super::chunkdb::run(cli, verb).await,
        BenchVerb::Chunkio(verb) => super::chunkio::run(cli, verb).await,
        BenchVerb::Kv(kv) => match kv {
            KvBenchVerb::Prepare(args) => super::kv_prepare::run(cli, args).await,
            KvBenchVerb::Read(args) => super::kv_read::run(cli, args).await,
            KvBenchVerb::Write(args) => super::kv_write::run(cli, args).await,
            KvBenchVerb::Scan(args) => super::kv_scan::run(cli, args).await,
        },
    }
}
