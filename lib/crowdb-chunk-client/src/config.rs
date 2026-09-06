// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `ChunkClientConfig` — shared configuration for the chunk data path.
//!
//! Replaces `WriterConfig`. All writers (`LargeObjectWriter`,
//! `LargeAsyncObjectWriter`, `SmallObjectWriter`, `WriterPool`,
//! `ChunkPrefetch`, `ChunkWriter`, `EcStripWriter`) read the fields
//! they need from this single config.

use crowdb_common::ec::EcScheme;

use crate::IoError;

/// Configuration for the chunk data path. Shared by all writers.
#[derive(Debug, Clone)]
pub struct ChunkClientConfig {
    // ── write path ──────────────────────────────────────────────
    /// Fetch read granularity / block size (bytes). Default 1 MB.
    pub read_buffer_size: usize,
    /// Max un-written data in fetch cache (bytes). Default 4 MB.
    pub max_cached_buffer: usize,

    // ── large write ─────────────────────────────────────────────
    /// Max chunk size before rotation (bytes). Default 1 GB.
    pub max_chunk_size: u64,
    /// Strips allocated ahead of the write cursor. Default 2.
    pub prealloc_depth: usize,
    /// Maximum completed-strip parity/finalization tasks in flight. Default 2.
    pub parity_depth: usize,
    /// Chunks allocated ahead. Default 1.
    pub chunk_prefetch_depth: usize,

    // ── prefetch ────────────────────────────────────────────────
    /// Chunks pre-created at start. Default 1 (minimal latency);
    /// raise for throughput-heavy workloads or testing.
    pub prefetch_chunk_count: usize,

    // ── memory ──────────────────────────────────────────────────
    /// Memory budget for `WriterPool` (bytes). Default 0 = unlimited
    /// (caller sets per-pool).
    pub memory_budget: usize,
}

impl Default for ChunkClientConfig {
    fn default() -> Self {
        const MB: usize = 1024 * 1024;
        const GB: usize = 1024 * 1024 * 1024;
        Self {
            read_buffer_size: MB,
            max_cached_buffer: 4 * MB,
            max_chunk_size: GB as u64,
            prealloc_depth: 2,
            parity_depth: 2,
            chunk_prefetch_depth: 1,
            prefetch_chunk_count: 1,
            memory_budget: 0,
        }
    }
}

impl ChunkClientConfig {
    /// Validate config fields. Returns `Err` on invalid combinations.
    pub fn validate(&self) -> Result<(), IoError> {
        if self.read_buffer_size == 0 {
            return Err(IoError::Internal("read_buffer_size must be > 0".into()));
        }
        if self.max_cached_buffer < self.read_buffer_size {
            return Err(IoError::Internal(
                "max_cached_buffer must be >= read_buffer_size".into(),
            ));
        }
        if self.max_chunk_size == 0 {
            return Err(IoError::Internal("max_chunk_size must be > 0".into()));
        }
        if self.prealloc_depth == 0 {
            return Err(IoError::Internal("prealloc_depth must be > 0".into()));
        }
        if self.parity_depth == 0 {
            return Err(IoError::Internal("parity_depth must be > 0".into()));
        }
        if self.prefetch_chunk_count == 0 {
            return Err(IoError::Internal("prefetch_chunk_count must be > 0".into()));
        }
        Ok(())
    }

    /// Per-writer memory footprint for `WriterPool` budgeting.
    /// Formula: max_cached_buffer + 1 block (fetch) + parity_depth *
    /// total_blocks * block.
    pub fn per_writer_memory(&self, ec_scheme: &EcScheme) -> usize {
        let block = self.read_buffer_size;
        self.max_cached_buffer + block + self.parity_depth * ec_scheme.total_blocks() * block
    }
}
