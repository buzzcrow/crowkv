// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `ChunkDB` allocation and lifecycle benchmarks with post-run verification.

use std::collections::HashSet;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crowdb_chunkdb_client::{ChunkdbClient, ChunkdbRpcTransport, RangeBindingClient, RetryConfig};
use crowdb_diskdb_client::{DiskdbClient, DiskdbClientError, DiskdbRpcTransport};
use crowdb_kv_client::{ReadEndpointPolicy, ServiceRegistryClient};
use crowdb_protocol::chunkdb::rpc::{
    AllocateChunkRequest, AppendChunkRequest, Chunk, ChunkState, ChunkType, DeleteChunkRequest,
    QueryChunkRequest, SealChunkRequest, Strip, StripType,
};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::diskdb::rpc::CompactZoneRequest;
use crowdb_protocol::generate_chunk_id;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

use super::kv_client::{build_kv_client, KvClientTunables};
use super::metrics::BenchMetrics;
use super::verb::{ChunkdbArgs, ChunkdbBenchVerb, ChunkdbStripMode};
use crate::Cli;

#[derive(Default)]
struct TaskResult {
    completed: u64,
    errors: u64,
    live: Vec<ChunkId>,
    latencies: Vec<u64>,
    exhausted: bool,
}

pub async fn run(cli: &Cli, verb: ChunkdbBenchVerb) -> ExitCode {
    let (args, mixed) = benchmark_args(verb);
    if !valid_args(&args) {
        return ExitCode::from(2);
    }
    let kv = match build_kv_client(cli, ReadEndpointPolicy::Leader, &KvClientTunables::default()) {
        Ok(client) => Arc::new(client),
        Err(code) => return code,
    };
    let diskdb = Arc::new(DiskdbClient::new(
        ServiceRegistryClient::from_shared(Arc::clone(&kv)),
        Arc::new(DiskdbRpcTransport::new()),
    ));
    if let Err(error) = diskdb.refresh_endpoints().await {
        eprintln!("diskdb discovery failed: {error}");
        return ExitCode::FAILURE;
    }
    if mixed {
        for _ in 0..3 {
            if let Err(error) = compact_all(&diskdb).await {
                eprintln!("diskdb baseline compaction failed: {error}");
                return ExitCode::FAILURE;
            }
        }
    }
    let baseline_busy = match busy_bytes(&diskdb).await {
        Ok(bytes) => bytes,
        Err(error) => {
            eprintln!("diskdb baseline accounting failed: {error}");
            return ExitCode::FAILURE;
        }
    };
    let range_bindings = RangeBindingClient::from_shared(Arc::clone(&kv));
    if let Err(error) = range_bindings.refresh().await {
        eprintln!("chunkdb range discovery failed: {error}");
        return ExitCode::FAILURE;
    }
    if range_bindings.snapshot().is_empty() {
        eprintln!("chunkdb range discovery failed: binding table is empty");
        return ExitCode::FAILURE;
    }
    let client = Arc::new(
        ChunkdbClient::with_retry_config(
            ServiceRegistryClient::from_shared(kv),
            RetryConfig {
                max_retries: 5,
                initial_backoff: Duration::from_millis(50),
            },
            Arc::new(ChunkdbRpcTransport::with_pool_size(
                args.chunkdb_connections,
                args.chunkdb_client_rpc_workers,
            )),
        )
        .with_range_binding(range_bindings),
    );
    if let Err(error) = client.refresh_endpoints().await {
        eprintln!("chunkdb discovery failed: {error}");
        return ExitCode::FAILURE;
    }

    let deadline = Instant::now() + Duration::from_secs(args.duration_secs);
    let (mut metrics, started) = start_metrics(cli, args.metrics_interval);
    let mut handles = Vec::with_capacity(args.concurrency);
    for task_id in 0..args.concurrency {
        handles.push(tokio::spawn(run_task(
            Arc::clone(&client),
            args.clone(),
            mixed,
            task_id,
            deadline,
        )));
    }
    let mut total = TaskResult::default();
    for handle in handles {
        match handle.await {
            Ok(result) => {
                total.completed += result.completed;
                total.errors += result.errors;
                total.live.extend(result.live);
                total.latencies.extend(result.latencies);
                total.exhausted |= result.exhausted;
            }
            Err(error) => {
                eprintln!("chunkdb benchmark task failed: {error}");
                total.errors += 1;
            }
        }
    }
    metrics.stop().await;
    verify_and_report(
        &client,
        &diskdb,
        baseline_busy,
        &args,
        mixed,
        started.elapsed(),
        &mut total,
    )
    .await
}

fn benchmark_args(verb: ChunkdbBenchVerb) -> (ChunkdbArgs, bool) {
    match verb {
        ChunkdbBenchVerb::Allocate(args) => (args, false),
        ChunkdbBenchVerb::Mix(args) => (args, true),
    }
}

fn start_metrics(cli: &Cli, interval: u64) -> (BenchMetrics, Instant) {
    let mut metrics = BenchMetrics::new(&cli.log_dir, interval);
    metrics.start();
    (metrics, Instant::now())
}

async fn run_task(
    client: Arc<ChunkdbClient>,
    args: ChunkdbArgs,
    mixed: bool,
    task_id: usize,
    deadline: Instant,
) -> TaskResult {
    let mut result = TaskResult::default();
    let mut rng = SmallRng::seed_from_u64(args.seed ^ u64::try_from(task_id).unwrap_or(u64::MAX));
    let mut sealed = HashSet::new();
    while Instant::now() < deadline {
        let started = Instant::now();
        let action = if mixed { rng.gen_range(0..100) } else { 0 };
        let (action_name, outcome) = if action < 50 || result.live.is_empty() {
            (
                "allocate",
                allocate(&client, &args).await.map(|id| result.live.push(id)),
            )
        } else {
            let index = rng.gen_range(0..result.live.len());
            let id = result.live[index];
            if action < 70 {
                ("query", query(&client, id).await.map(|_| ()))
            } else if action < 80 {
                if sealed.contains(&id) {
                    ("query-sealed", query(&client, id).await.map(|_| ()))
                } else {
                    ("append", append(&client, id, &args).await)
                }
            } else if action < 90 {
                if sealed.contains(&id) {
                    ("query-sealed", query(&client, id).await.map(|_| ()))
                } else {
                    (
                        "seal",
                        seal(&client, id).await.map(|()| {
                            sealed.insert(id);
                        }),
                    )
                }
            } else {
                (
                    "delete",
                    delete(&client, id).await.map(|()| {
                        result.live.swap_remove(index);
                        sealed.remove(&id);
                    }),
                )
            }
        };
        match outcome {
            Ok(()) => {
                result.completed += 1;
                result
                    .latencies
                    .push(started.elapsed().as_micros().try_into().unwrap_or(u64::MAX));
            }
            Err(error) => {
                if error.to_string().contains("no space") {
                    result.exhausted = true;
                    break;
                }
                eprintln!("chunkdb benchmark {action_name} failed: {error}");
                result.errors += 1;
            }
        }
    }
    result
}

async fn append(
    client: &ChunkdbClient,
    id: ChunkId,
    args: &ChunkdbArgs,
) -> crowdb_chunkdb_client::Result<()> {
    let strip_type = match args.strip_type {
        ChunkdbStripMode::Mirror => StripType::Mirror,
        ChunkdbStripMode::Ec => StripType::Ec,
    };
    client
        .append_chunk(AppendChunkRequest {
            chunk_id: Some(id),
            strip_size: 1,
            strip_count: 1,
            strip_type: strip_type as i32,
            data_num: args.data_num,
            code_num: args.code_num,
            copy_count: args.copy_count,
        })
        .await
        .map(|_| ())
}

async fn allocate(client: &ChunkdbClient, args: &ChunkdbArgs) -> crowdb_chunkdb_client::Result<ChunkId> {
    let id = generate_chunk_id(ChunkType::Repo as u8).to_proto();
    let strip_type = match args.strip_type {
        ChunkdbStripMode::Mirror => StripType::Mirror,
        ChunkdbStripMode::Ec => StripType::Ec,
    };
    let response = match client
        .allocate_chunk(AllocateChunkRequest {
            chunk_id: Some(id),
            write_granularity: args.write_granularity_kb,
            strip_count: args.strip_count,
            strip_type: strip_type as i32,
            data_num: args.data_num,
            code_num: args.code_num,
            copy_count: args.copy_count,
            chunk_type: ChunkType::Repo as i32,
        })
        .await
    {
        Ok(response) => response,
        Err(error) => {
            // Allocation may commit before an RPC timeout or disconnect is
            // observed. Keep the stable chunk ID as an ambiguous success;
            // post-run chunk and DiskDB verification determines whether the
            // mutation actually committed without issuing it a second time.
            if ambiguous_allocation_error(&error) {
                return Ok(id);
            }
            return Err(error);
        }
    };
    response.chunk.and_then(|chunk| chunk.id).ok_or_else(|| {
        crowdb_chunkdb_client::ChunkdbClientError::Rpc("allocate response has no chunk ID".into())
    })
}

fn ambiguous_allocation_error(error: &crowdb_chunkdb_client::ChunkdbClientError) -> bool {
    use crowdb_chunkdb_client::ChunkdbClientError;

    match error {
        ChunkdbClientError::Unavailable(_)
        | ChunkdbClientError::DeadlineExceeded(_)
        | ChunkdbClientError::Unreachable(_) => true,
        ChunkdbClientError::Rpc(message) => message.starts_with("rpc error:"),
        _ => false,
    }
}

async fn query(client: &ChunkdbClient, id: ChunkId) -> crowdb_chunkdb_client::Result<Chunk> {
    client
        .query_chunk(QueryChunkRequest { chunk_id: Some(id) })
        .await?
        .chunk
        .ok_or_else(|| crowdb_chunkdb_client::ChunkdbClientError::Rpc("query response has no chunk".into()))
}

async fn seal(client: &ChunkdbClient, id: ChunkId) -> crowdb_chunkdb_client::Result<()> {
    client
        .seal_chunk(SealChunkRequest {
            chunk_id: Some(id),
            seal_length: 0,
        })
        .await
        .map(|_| ())
}

async fn delete(client: &ChunkdbClient, id: ChunkId) -> crowdb_chunkdb_client::Result<()> {
    client
        .delete_chunk(DeleteChunkRequest { chunk_id: Some(id) })
        .await
        .map(|_| ())
}

async fn verify_and_report(
    client: &Arc<ChunkdbClient>,
    diskdb: &DiskdbClient,
    baseline_busy: u64,
    args: &ChunkdbArgs,
    mixed: bool,
    elapsed: Duration,
    result: &mut TaskResult,
) -> ExitCode {
    let expected_busy = verify_live_chunks(client, result).await;
    if mixed {
        for _ in 0..3 {
            if let Err(error) = compact_all(diskdb).await {
                eprintln!("diskdb compaction before verification failed: {error}");
                result.errors += 1;
                break;
            }
        }
    }
    let actual_busy = match busy_bytes(diskdb).await {
        Ok(bytes) => bytes.saturating_sub(baseline_busy),
        Err(error) => {
            eprintln!("diskdb final accounting failed: {error}");
            result.errors += 1;
            u64::MAX
        }
    };
    if actual_busy != expected_busy {
        eprintln!("diskdb accounting mismatch: expected {expected_busy} bytes, got {actual_busy}");
        result.errors += 1;
    }
    result.latencies.sort_unstable();
    let throughput = u128::from(result.completed)
        .saturating_mul(1_000_000_000)
        .checked_div(elapsed.as_nanos().max(1))
        .unwrap_or(0);
    let blocks_per_chunk = match args.strip_type {
        ChunkdbStripMode::Mirror => u128::from(args.copy_count),
        ChunkdbStripMode::Ec => u128::from(args.data_num.saturating_add(args.code_num)),
    }
    .saturating_mul(u128::from(args.strip_count));
    let block_allocs_per_sec = throughput.saturating_mul(blocks_per_chunk);
    println!(
        "chunkdb bench workload={} strip_type={:?} stop={} elapsed={:.3}s ops={} ops_per_sec={} block_allocs_per_sec={} live={} errors={} p50_us={} p99_us={} busy_delta={} expected_busy_delta={}",
        if mixed { "mix" } else { "allocate" },
        args.strip_type,
        if result.exhausted { "exhausted" } else { "deadline" },
        elapsed.as_secs_f64(),
        result.completed,
        throughput,
        block_allocs_per_sec,
        result.live.len(),
        result.errors,
        percentile(&result.latencies, 50),
        percentile(&result.latencies, 99),
        actual_busy,
        expected_busy,
    );
    if result.errors == 0 {
        ExitCode::SUCCESS
    } else {
        eprintln!("chunkdb benchmark correctness verification failed");
        ExitCode::FAILURE
    }
}

async fn verify_live_chunks(client: &Arc<ChunkdbClient>, result: &mut TaskResult) -> u64 {
    let mut physical = HashSet::new();
    let mut expected_busy = 0u64;
    // Query in bounded parallel batches. At peak load, serial verification of
    // every live chunk can take longer than the benchmark itself.
    // Keep verification below the service saturation point. A 256-query
    // burst can starve individual reads behind KV work long enough for the
    // RPC reaper to expire them, which then looks like lost allocation and
    // produces a false space-accounting mismatch.
    for ids in result.live.chunks(64) {
        let mut queries = tokio::task::JoinSet::new();
        for id in ids {
            let client = Arc::clone(client);
            let id = *id;
            queries.spawn(async move { (id, query_eventually(&client, id).await) });
        }
        while let Some(joined) = queries.join_next().await {
            let (id, query_result) = match joined {
                Ok(value) => value,
                Err(error) => {
                    eprintln!("chunkdb verification task failed: {error}");
                    result.errors += 1;
                    continue;
                }
            };
            match query_result {
                Ok(chunk)
                    if chunk.state == ChunkState::Active as i32
                        || chunk.state == ChunkState::Sealed as i32 =>
                {
                    for strip in chunk.strips {
                        let unit_bytes = u64::from(strip.unit_kb) * 1024;
                        let segments = match strip.strip {
                            Some(Strip::MirrorStrip(value)) => value.segments,
                            Some(Strip::EcStrip(value)) => value.segments,
                            None => {
                                result.errors += 1;
                                continue;
                            }
                        };
                        for segment in segments {
                            expected_busy = expected_busy
                                .saturating_add(u64::from(segment.unit_count).saturating_mul(unit_bytes));
                            let Some(disk_id) = segment.disk_id else {
                                result.errors += 1;
                                continue;
                            };
                            if segment.owner_chunk != Some(id)
                                || !physical.insert((disk_id, segment.zone_index, segment.unit_offset))
                            {
                                result.errors += 1;
                            }
                        }
                    }
                }
                Ok(chunk) => {
                    eprintln!(
                        "chunkdb verification found invalid state for {id:?}: state={}",
                        chunk.state
                    );
                    result.errors += 1;
                }
                Err(error) => {
                    eprintln!("chunkdb verification query failed for {id:?}: {error}");
                    result.errors += 1;
                }
            }
        }
    }
    expected_busy
}

async fn query_eventually(client: &ChunkdbClient, id: ChunkId) -> crowdb_chunkdb_client::Result<Chunk> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match query(client, id).await {
            Ok(chunk) => return Ok(chunk),
            Err(error) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
                drop(error);
            }
            Err(error) => return Err(error),
        }
    }
}

async fn busy_bytes(client: &DiskdbClient) -> Result<u64, DiskdbClientError> {
    let mut total = 0u64;
    for group in client.disk_group_ids() {
        let response = client.query_disk_group(group).await?;
        total = total.saturating_add(
            response
                .disk_groups
                .iter()
                .map(|entry| entry.busy_bytes)
                .sum::<u64>(),
        );
    }
    Ok(total)
}

async fn compact_all(client: &DiskdbClient) -> Result<(), DiskdbClientError> {
    for group in client.disk_group_ids() {
        let response = client.query_disk_group(group).await?;
        for disk in response.disk_groups.iter().flat_map(|entry| &entry.disks) {
            if let Some(disk_id) = disk.disk_id {
                client
                    .compact_zone(CompactZoneRequest {
                        disk_id: Some(disk_id),
                        zone_indices: Vec::new(),
                    })
                    .await?;
            }
        }
    }
    Ok(())
}

fn percentile(values: &[u64], percentile: usize) -> u64 {
    if values.is_empty() {
        return 0;
    }
    values[(values.len() - 1) * percentile / 100]
}

fn valid_args(args: &ChunkdbArgs) -> bool {
    let shape_ok = match args.strip_type {
        ChunkdbStripMode::Mirror => args.copy_count > 0,
        ChunkdbStripMode::Ec => args.data_num > 0 && args.code_num > 0,
    };
    let valid = args.duration_secs > 0
        && args.concurrency > 0
        && args.chunkdb_connections > 0
        && args.chunkdb_client_rpc_workers > 0
        && args.strip_count > 0
        && args.write_granularity_kb > 0
        && shape_ok;
    if !valid {
        eprintln!("duration, concurrency, strip size/count, and placement widths must be non-zero");
    }
    valid
}
