// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Reusable bounded large-write benchmark workload.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use serde::Serialize;
use tokio::io::AsyncReadExt;
use tokio::task::JoinSet;

use crate::{ChunkIoClient, LargeWritePolicy};

/// Deterministic large-write workload parameters.
#[derive(Debug, Clone)]
pub struct LargeWriteBenchmarkConfig {
    pub object_count: u64,
    pub object_size: u64,
    pub concurrency: usize,
    pub seed: u8,
    pub policy: LargeWritePolicy,
}

/// Aggregate workload result; application and CLI presentation independent.
#[derive(Debug, Clone, Serialize)]
pub struct LargeWriteBenchmarkResult {
    pub elapsed_secs: f64,
    pub objects: u64,
    pub errors: u64,
    pub logical_bytes: u64,
    pub physical_bytes: u64,
    pub logical_mib_per_sec: f64,
    pub physical_mib_per_sec: f64,
    pub objects_per_sec: f64,
    pub latency_p50_us: u64,
    pub latency_p99_us: u64,
    pub preparation_stalls: u64,
    pub preparation_stall_us: u64,
    pub error_messages: Vec<String>,
}

#[derive(Default)]
struct WorkerResult {
    objects: u64,
    logical_bytes: u64,
    physical_bytes: u64,
    preparation_stalls: u64,
    preparation_stall_us: u64,
    latencies: Vec<u64>,
    errors: Vec<String>,
}

/// Run concurrent deterministic large writes without allocating an
/// object-sized source buffer.
pub async fn run_large_write_benchmark(
    client: ChunkIoClient,
    config: LargeWriteBenchmarkConfig,
) -> LargeWriteBenchmarkResult {
    let started = Instant::now();
    let next_object = Arc::new(AtomicU64::new(0));
    let mut tasks = JoinSet::new();
    for worker in 0..config.concurrency.max(1) {
        let client = client.clone();
        let config = config.clone();
        let next_object = next_object.clone();
        tasks.spawn(async move { run_worker(client, config, next_object, worker).await });
    }
    let mut total = WorkerResult::default();
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(worker) => merge_worker(&mut total, worker),
            Err(error) => total.errors.push(format!("benchmark worker failed: {error}")),
        }
    }
    total.latencies.sort_unstable();
    let elapsed_secs = started.elapsed().as_secs_f64().max(f64::EPSILON);
    LargeWriteBenchmarkResult {
        elapsed_secs,
        objects: total.objects,
        errors: total.errors.len() as u64,
        logical_bytes: total.logical_bytes,
        physical_bytes: total.physical_bytes,
        logical_mib_per_sec: total.logical_bytes as f64 / 1_048_576.0 / elapsed_secs,
        physical_mib_per_sec: total.physical_bytes as f64 / 1_048_576.0 / elapsed_secs,
        objects_per_sec: total.objects as f64 / elapsed_secs,
        latency_p50_us: percentile(&total.latencies, 50),
        latency_p99_us: percentile(&total.latencies, 99),
        preparation_stalls: total.preparation_stalls,
        preparation_stall_us: total.preparation_stall_us,
        error_messages: total.errors,
    }
}

async fn run_worker(
    client: ChunkIoClient,
    config: LargeWriteBenchmarkConfig,
    next_object: Arc<AtomicU64>,
    worker: usize,
) -> WorkerResult {
    let mut result = WorkerResult::default();
    loop {
        let object = next_object.fetch_add(1, Ordering::Relaxed);
        if object >= config.object_count {
            break;
        }
        let byte = config.seed.wrapping_add(worker as u8).wrapping_add(object as u8);
        let source = tokio::io::repeat(byte).take(config.object_size);
        let started = Instant::now();
        match client
            .prepare_large_write(Some(config.object_size), config.policy.clone())
            .write_stream(source)
            .await
        {
            Ok(write) => {
                result.objects += 1;
                result.logical_bytes += write.logical_bytes;
                result.physical_bytes += write.physical_bytes;
                result.preparation_stalls += write.preparation_stalls;
                result.preparation_stall_us +=
                    u64::try_from(write.preparation_stall_time.as_micros()).unwrap_or(u64::MAX);
                result
                    .latencies
                    .push(u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX));
            }
            Err(error) => result.errors.push(format!("object {object}: {error}")),
        }
    }
    result
}

fn merge_worker(total: &mut WorkerResult, worker: WorkerResult) {
    total.objects += worker.objects;
    total.logical_bytes += worker.logical_bytes;
    total.physical_bytes += worker.physical_bytes;
    total.preparation_stalls += worker.preparation_stalls;
    total.preparation_stall_us += worker.preparation_stall_us;
    total.latencies.extend(worker.latencies);
    total.errors.extend(worker.errors);
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let index = (sorted.len() - 1) * percentile / 100;
    sorted[index]
}
