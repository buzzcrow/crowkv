// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Lifecycle lock + cache metrics for chunkdb observability.
//! Hot-path counters are `AtomicU64` with `Relaxed` ordering;
//! latency histograms are `Mutex<PreciseHistogram>` (rare contention).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crowdb_common::metrics::PreciseHistogram;
use crowdb_common::metrics::{Counter, Gauge, LatencyHistogram, MetricsRegistry};
use serde::{Deserialize, Serialize};

/// Registered `ChunkDB` RPC methods, used as stable request-metric indices.
#[derive(Clone, Copy)]
pub enum RequestKind {
    AllocateChunk,
    AppendChunk,
    QueryChunk,
    SealChunk,
    DeleteChunk,
    DeleteChunkRange,
    UpdateChunkStrip,
    ListChunks,
}

impl RequestKind {
    const ALL: [Self; 8] = [
        Self::AllocateChunk,
        Self::AppendChunk,
        Self::QueryChunk,
        Self::SealChunk,
        Self::DeleteChunk,
        Self::DeleteChunkRange,
        Self::UpdateChunkStrip,
        Self::ListChunks,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::AllocateChunk => "allocate_chunk",
            Self::AppendChunk => "append_chunk",
            Self::QueryChunk => "query_chunk",
            Self::SealChunk => "seal_chunk",
            Self::DeleteChunk => "delete_chunk",
            Self::DeleteChunkRange => "delete_chunk_range",
            Self::UpdateChunkStrip => "update_chunk_strip",
            Self::ListChunks => "list_chunks",
        }
    }

    const fn index(self) -> usize {
        self as usize
    }
}

struct RequestMetric {
    latency: Arc<LatencyHistogram>,
    inflight: Arc<Gauge>,
    errors: Arc<Counter>,
}

/// Uniform completed-request latency/count, inflight, and error metrics.
pub struct RequestMetrics {
    methods: [RequestMetric; 8],
}

impl RequestMetrics {
    fn register(registry: &mut MetricsRegistry) -> Self {
        let methods = RequestKind::ALL.map(|kind| {
            let prefix = format!("request.{}", kind.name());
            RequestMetric {
                latency: registry.register_histogram(format!("{prefix}.lh")),
                inflight: registry.register_gauge(format!("{prefix}.inflight.g")),
                errors: registry.register_counter(format!("{prefix}.errors.c")),
            }
        });
        Self { methods }
    }

    /// Start accounting for a request.
    #[must_use]
    pub fn start(&self, kind: RequestKind) -> RequestGuard {
        let metric = &self.methods[kind.index()];
        metric.inflight.inc();
        RequestGuard {
            latency: Arc::clone(&metric.latency),
            inflight: Arc::clone(&metric.inflight),
            errors: Arc::clone(&metric.errors),
            started: std::time::Instant::now(),
            success: false,
        }
    }
}

/// Completes request accounting on every synchronous or asynchronous exit.
pub struct RequestGuard {
    latency: Arc<LatencyHistogram>,
    inflight: Arc<Gauge>,
    errors: Arc<Counter>,
    started: std::time::Instant,
    success: bool,
}

impl RequestGuard {
    /// Mark the request response as successful.
    pub fn mark_success(&mut self) {
        self.success = true;
    }
}

impl Drop for RequestGuard {
    fn drop(&mut self) {
        if !self.success {
            self.errors.inc();
        }
        self.latency
            .observe(self.started.elapsed().as_nanos().try_into().unwrap_or(u64::MAX));
        self.inflight.dec();
    }
}

/// Metrics for the ChunkDB RPC and allocation workflow.
#[derive(Clone)]
pub struct ChunkdbMetrics {
    pub requests: Arc<RequestMetrics>,
    pub allocate_inflight: Arc<Gauge>,
    pub allocate_strips: Arc<Counter>,
    pub allocate_blocks: Arc<Counter>,
    pub allocate_placement: Arc<LatencyHistogram>,
    pub allocate_diskdb_round: Arc<LatencyHistogram>,
    pub allocate_diskdb_calls: Arc<Counter>,
    pub allocate_diskdb_retries: Arc<Counter>,
    pub allocate_commit: Arc<LatencyHistogram>,
    pub allocate_commit_blocks: Arc<Counter>,
    pub allocate_commit_errors: Arc<Counter>,
    pub allocate_record_build: Arc<LatencyHistogram>,
    pub allocate_kv_persist: Arc<LatencyHistogram>,
    pub allocate_rollback: Arc<LatencyHistogram>,
    pub allocate_rollback_blocks: Arc<Counter>,
    pub allocate_errors: Arc<Counter>,
}

impl ChunkdbMetrics {
    /// Register all ChunkDB workflow metrics.
    pub fn register(registry: &mut MetricsRegistry) -> Self {
        Self {
            requests: Arc::new(RequestMetrics::register(registry)),
            allocate_inflight: registry.register_gauge("allocate.inflight.g"),
            allocate_strips: registry.register_counter("allocate.strips.c"),
            allocate_blocks: registry.register_counter("allocate.blocks.c"),
            allocate_placement: registry.register_histogram("allocate.placement.lh"),
            allocate_diskdb_round: registry.register_histogram("allocate.diskdb_round.lh"),
            allocate_diskdb_calls: registry.register_counter("allocate.diskdb_calls.c"),
            allocate_diskdb_retries: registry.register_counter("allocate.diskdb_retries.c"),
            allocate_commit: registry.register_histogram("allocate.commit.lh"),
            allocate_commit_blocks: registry.register_counter("allocate.commit_blocks.c"),
            allocate_commit_errors: registry.register_counter("allocate.commit_errors.c"),
            allocate_record_build: registry.register_histogram("allocate.record_build.lh"),
            allocate_kv_persist: registry.register_histogram("allocate.kv_persist.lh"),
            allocate_rollback: registry.register_histogram("allocate.rollback.lh"),
            allocate_rollback_blocks: registry.register_counter("allocate.rollback_blocks.c"),
            allocate_errors: registry.register_counter("allocate.errors.c"),
        }
    }
}

/// Snapshot of [`LifecycleMetrics`] at a point in time.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LifecycleMetricsSnapshot {
    pub lock_timeout_count: u64,
    pub lock_busy_count: u64,
    pub cache_hit_count: u64,
    pub cache_miss_count: u64,
    pub cache_size: u64,
    pub reap_idle_count: u64,
    pub reap_idle_entries_removed: u64,
    /// Number of cache entries invalidated (one increment per chunk
    /// removed, by both `invalidate_chunk` and `invalidate_range`).
    pub invalidate_count: u64,
    pub lock_wait_count: u64,
    pub lock_wait_p50_us: u64,
    pub lock_wait_p99_us: u64,
    pub lock_wait_max_us: u64,
    pub lock_hold_count: u64,
    pub lock_hold_p50_us: u64,
    pub lock_hold_p99_us: u64,
    pub lock_hold_max_us: u64,
}

/// Latency histograms behind a `Mutex` — `PreciseHistogram` requires
/// `&mut self` for `record()`.
#[derive(Debug)]
struct LatencyHistograms {
    lock_wait: PreciseHistogram,
    lock_hold: PreciseHistogram,
}

impl Default for LatencyHistograms {
    fn default() -> Self {
        Self {
            lock_wait: PreciseHistogram::new(3),
            lock_hold: PreciseHistogram::new(3),
        }
    }
}

/// Metrics for the per-chunk lifecycle lock + payload cache.
#[derive(Debug, Default)]
pub struct LifecycleMetrics {
    lock_timeout_count: AtomicU64,
    lock_busy_count: AtomicU64,
    cache_hit_count: AtomicU64,
    cache_miss_count: AtomicU64,
    reap_idle_count: AtomicU64,
    reap_idle_entries_removed: AtomicU64,
    invalidate_count: AtomicU64,
    lat: Mutex<LatencyHistograms>,
}

impl LifecycleMetrics {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn record_lock_timeout(&self) {
        self.lock_timeout_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_lock_busy(&self) {
        self.lock_busy_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_cache_hit(&self) {
        self.cache_hit_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_cache_miss(&self) {
        self.cache_miss_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_reap_idle(&self, entries_removed: u64) {
        self.reap_idle_count.fetch_add(1, Ordering::Relaxed);
        self.reap_idle_entries_removed
            .fetch_add(entries_removed, Ordering::Relaxed);
    }

    pub(crate) fn record_invalidate(&self) {
        self.invalidate_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_lock_wait(&self, dur_us: u64) {
        if let Ok(mut g) = self.lat.lock() {
            g.lock_wait.record(dur_us.max(1));
        }
    }

    pub(crate) fn record_lock_hold(&self, dur_us: u64) {
        if let Ok(mut g) = self.lat.lock() {
            g.lock_hold.record(dur_us.max(1));
        }
    }

    /// Snapshot all counters + histogram percentiles. `cache_size` is
    /// passed in by the caller (read from `quick_cache::Cache::entry_count()`).
    #[must_use]
    pub fn snapshot(&self, cache_size: u64) -> LifecycleMetricsSnapshot {
        let lat = self.lat.lock().map_or_else(
            |_| LatencyHistograms::default(),
            |g| LatencyHistograms {
                lock_wait: g.lock_wait.clone(),
                lock_hold: g.lock_hold.clone(),
            },
        );
        LifecycleMetricsSnapshot {
            lock_timeout_count: self.lock_timeout_count.load(Ordering::Relaxed),
            lock_busy_count: self.lock_busy_count.load(Ordering::Relaxed),
            cache_hit_count: self.cache_hit_count.load(Ordering::Relaxed),
            cache_miss_count: self.cache_miss_count.load(Ordering::Relaxed),
            cache_size,
            reap_idle_count: self.reap_idle_count.load(Ordering::Relaxed),
            reap_idle_entries_removed: self.reap_idle_entries_removed.load(Ordering::Relaxed),
            invalidate_count: self.invalidate_count.load(Ordering::Relaxed),
            lock_wait_count: lat.lock_wait.len(),
            lock_wait_p50_us: lat.lock_wait.value_at_quantile(0.50),
            lock_wait_p99_us: lat.lock_wait.value_at_quantile(0.99),
            lock_wait_max_us: lat.lock_wait.max(),
            lock_hold_count: lat.lock_hold.len(),
            lock_hold_p50_us: lat.lock_hold.value_at_quantile(0.50),
            lock_hold_p99_us: lat.lock_hold.value_at_quantile(0.99),
            lock_hold_max_us: lat.lock_hold.max(),
        }
    }
}
