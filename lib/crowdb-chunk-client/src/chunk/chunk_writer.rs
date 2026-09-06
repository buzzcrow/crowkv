// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `ChunkWriter` — chunk wrapper + write ability.
//!
//! Owns the current `Chunk` protobuf (in `Arc`, shared with
//! `EcStripWriter`). Owns the strip-level drive loop in `push`
//! (auto-rotates strips: finish + open next when full). All chunkdb
//! chunk operations (seal, delete, append) go through this class.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::warn;

use crate::chunk::ec_strip_writer::EcStripWriter;
use crate::chunk::strip::{StripResult, StripWriter};
use crate::config::ChunkClientConfig;
use crate::disk_io::DiskWriter;
use crate::io::FeedStatus;
use crate::traits::ChunkAllocator;
use crate::{IoError, Result};
use crowdb_common::ec::EcScheme;
use crowdb_protocol::chunkdb::rpc::{
    AppendChunkRequest, Chunk, DeleteChunkRequest, Location as ProtoLocation, SealChunkRequest, StripType,
};
use crowdb_protocol::common::ChunkId;

/// Chunk wrapper + write ability. Owns `Arc<Chunk>`; the strip-level
/// drive loop is in `push` (auto-rotates strips). Collects parity
/// write + fsync handles from each `finish_strip` and joins them at
/// `seal` time — strip N+1's data writes overlap with strip N's
/// parity writes + fsyncs. Runs an internal strip-prefetch task that
/// appends strips ahead of `write_cursor`, bounded by
/// `prealloc_depth`.
pub struct ChunkWriter {
    pub(crate) allocator: Arc<dyn ChunkAllocator>,
    pub(crate) disk_writer: Arc<dyn DiskWriter>,
    pub(crate) ec_scheme: EcScheme,
    pub(crate) config: Arc<ChunkClientConfig>,
    pub(crate) chunk: Option<Arc<Chunk>>,
    pub(crate) write_cursor: u32,
    pub(crate) bytes_in_chunk: u64,
    pub(crate) object_size: Option<u64>,
    pub(crate) strips_remaining: Option<usize>,
    pub(crate) current_strip: Option<StripWriter>,
    pub(crate) parity_handles: Vec<JoinHandle<Result<()>>>,
    pub(crate) prefetch_handle: Option<JoinHandle<()>>,
    pub(crate) prefetch_rx: Option<mpsc::Receiver<Result<Chunk>>>,
    pub(crate) write_cursor_shared: Arc<AtomicU32>,
    pub(crate) preparation_stalls: u64,
    pub(crate) preparation_stall_time: Duration,
}

impl ChunkWriter {
    /// Construct a new chunk writer (no chunk open yet).
    pub fn new(
        allocator: Arc<dyn ChunkAllocator>,
        disk_writer: Arc<dyn DiskWriter>,
        ec_scheme: EcScheme,
        config: Arc<ChunkClientConfig>,
    ) -> Self {
        Self {
            allocator,
            disk_writer,
            ec_scheme,
            config,
            chunk: None,
            write_cursor: 0,
            bytes_in_chunk: 0,
            object_size: None,
            strips_remaining: None,
            current_strip: None,
            parity_handles: Vec::new(),
            prefetch_handle: None,
            prefetch_rx: None,
            write_cursor_shared: Arc::new(AtomicU32::new(0)),
            preparation_stalls: 0,
            preparation_stall_time: Duration::ZERO,
        }
    }

    /// Open a chunk from a pre-allocated `Chunk` protobuf. Wraps it in
    /// `Arc`, opens the first strip (already present from
    /// `allocate_chunk`), and starts the internal strip-prefetch task
    /// that appends strips ahead of `write_cursor` (bounded by
    /// `prealloc_depth`). `object_size` drives prefetch planning:
    /// known-size objects stop pre-appending when enough strips are
    /// allocated; unknown-size objects pre-append up to
    /// `strips_per_chunk`.
    pub fn open(&mut self, chunk: Chunk, object_size: Option<u64>) -> Result<()> {
        if chunk.id.is_none() {
            return Err(IoError::AllocationFailed("open: chunk missing id".into()));
        }
        if chunk.strips.is_empty() {
            return Err(IoError::AllocationFailed("open: chunk has no strips".into()));
        }
        self.object_size = object_size;
        self.strips_remaining = compute_strips_remaining(object_size, &self.ec_scheme, &self.config);
        let chunk = Arc::new(chunk);
        let strip = EcStripWriter::new(Arc::clone(&chunk), 0, self.disk_writer.clone(), self.ec_scheme);
        self.chunk = Some(chunk);
        self.write_cursor = 0;
        self.write_cursor_shared.store(0, Ordering::Relaxed);
        self.bytes_in_chunk = 0;
        self.current_strip = Some(StripWriter::Ec(strip));
        // Start the internal strip-prefetch task.
        self.start_strip_prefetch();
        Ok(())
    }

    /// Continue with a new strip on the same chunk. `chunk` is the
    /// cumulative `Chunk` protobuf (from `append_chunk` response) with
    /// the next strip appended. Arc-swaps `self.chunk` and opens the
    /// strip at `write_cursor + 1`.
    pub(crate) fn continue_strip(&mut self, chunk: Chunk) -> Result<()> {
        let new_id = chunk
            .id
            .ok_or_else(|| IoError::AllocationFailed("continue_strip: chunk missing id".into()))?;
        let cur_id = self.current_chunk_id();
        if cur_id != Some(new_id) {
            return Err(IoError::Internal("continue_strip with different chunk_id".into()));
        }
        let next_index = self.write_cursor + 1;
        let chunk = Arc::new(chunk);
        let strip = EcStripWriter::new(
            Arc::clone(&chunk),
            next_index,
            self.disk_writer.clone(),
            self.ec_scheme,
        );
        self.chunk = Some(chunk);
        self.write_cursor = next_index;
        self.write_cursor_shared.store(next_index, Ordering::Relaxed);
        self.current_strip = Some(StripWriter::Ec(strip));
        Ok(())
    }

    /// Push a data block to the current strip. Auto-rotates strips:
    /// if the current strip is full, finishes it, advances
    /// `write_cursor`, opens the next strip (from `chunk.strips` if
    /// pre-appended, or via `append_chunk` RPC), then pushes the
    /// block to the new strip. Returns `Pause` if the chunk is full
    /// after finishing the current strip — the block is NOT pushed
    /// (caller rotates chunks, then re-pushes).
    pub async fn push(&mut self, buffer: Bytes) -> Result<FeedStatus> {
        if self.current_strip.is_none() {
            return Err(IoError::Internal("push with no open strip".into()));
        }
        // Auto-rotate: if the current strip is full, finish it + check
        // if the chunk is now full. If so, return Pause without pushing
        // — the caller rotates chunks and re-pushes this buffer.
        if self.is_strip_full() {
            self.finish_strip().await?;
            if self.is_full() {
                return Ok(FeedStatus::Pause);
            }
            self.open_next_strip().await?;
        }
        let strip = self
            .current_strip
            .as_mut()
            .ok_or_else(|| IoError::Internal("push: strip vanished after rotate".into()))?;
        // Push to the strip. The strip's Pause (strip full) is handled
        // internally by auto-rotate on the next push — we always return
        // Continue after a successful push (only chunk-full returns Pause).
        strip.push(buffer).await?;
        Ok(FeedStatus::Continue)
    }

    /// Open the next strip on the current chunk. First drains the
    /// prefetch channel (non-blocking) to pick up any pre-appended
    /// chunks. If the next strip is in `chunk.strips`, opens it
    /// directly. Otherwise waits for the prefetch channel to deliver
    /// it (blocking) — this avoids a race where both the prefetch task
    /// and an inline `append_chunk` RPC append the same strip. Falls
    /// back to inline `append_chunk` only if the channel is closed
    /// (prefetch task finished or errored).
    async fn open_next_strip(&mut self) -> Result<()> {
        let next_index = self.write_cursor + 1;
        // Drain prefetch channel (non-blocking) — pick up latest chunk.
        self.drain_prefetch();
        loop {
            let ready = {
                let chunk = self
                    .chunk
                    .as_ref()
                    .ok_or_else(|| IoError::Internal("open_next_strip with no chunk".into()))?;
                (next_index as usize) < chunk.strips.len()
            };
            if ready {
                // Next strip is pre-appended — open it directly.
                let chunk = self
                    .chunk
                    .as_ref()
                    .ok_or_else(|| IoError::Internal("open_next_strip with no chunk".into()))?;
                let strip = EcStripWriter::new(
                    Arc::clone(chunk),
                    next_index,
                    self.disk_writer.clone(),
                    self.ec_scheme,
                );
                self.write_cursor = next_index;
                self.write_cursor_shared.store(next_index, Ordering::Relaxed);
                self.current_strip = Some(StripWriter::Ec(strip));
                return Ok(());
            }
            // Next strip not ready — wait for the prefetch task to
            // deliver it instead of appending inline (avoids duplicate
            // append_chunk calls).
            let Some(rx) = self.prefetch_rx.as_mut() else {
                // No prefetch channel — inline append as last resort.
                let started = Instant::now();
                self.preparation_stalls += 1;
                let result = self.append_strip().await;
                self.preparation_stall_time += started.elapsed();
                let new_chunk = result?;
                self.continue_strip(new_chunk)?;
                return Ok(());
            };
            let started = Instant::now();
            self.preparation_stalls += 1;
            let result = rx.recv().await;
            self.preparation_stall_time += started.elapsed();
            match result {
                Some(Ok(new_chunk)) => {
                    self.chunk = Some(Arc::new(new_chunk));
                    // Loop back: check if the next strip is now available.
                }
                Some(Err(e)) => return Err(e),
                None => {
                    // Channel closed — prefetch is done. Inline append.
                    self.prefetch_rx = None;
                    let new_chunk = self.append_strip().await?;
                    self.continue_strip(new_chunk)?;
                    return Ok(());
                }
            }
        }
    }

    /// Drain the prefetch channel (non-blocking) and Arc-swap to the
    /// latest cumulative `Chunk` from the prefetch task.
    fn drain_prefetch(&mut self) {
        if let Some(rx) = self.prefetch_rx.as_mut() {
            while let Ok(result) = rx.try_recv() {
                match result {
                    Ok(new_chunk) => {
                        self.chunk = Some(Arc::new(new_chunk));
                    }
                    Err(e) => {
                        warn!("strip prefetch error: {e}");
                        break;
                    }
                }
            }
        }
    }

    /// Stop the strip-prefetch task: drop the receiver (task's
    /// `tx.send` fails → task exits) + abort the handle.
    fn stop_prefetch(&mut self) {
        self.prefetch_rx.take();
        if let Some(handle) = self.prefetch_handle.take() {
            handle.abort();
        }
    }

    /// Start the internal strip-prefetch background task. Appends
    /// strips to the chunk ahead of `write_cursor`, bounded by
    /// `prealloc_depth`. Known-size objects stop when
    /// `strips_remaining` hits 0; unknown-size objects pre-append up
    /// to `strips_per_chunk`. Sends cumulative `Chunk` values via a
    /// channel; `drain_prefetch` picks them up.
    fn start_strip_prefetch(&mut self) {
        let Some(chunk_id) = self.current_chunk_id() else {
            return;
        };
        let (tx, rx) = mpsc::channel::<Result<Chunk>>(8);
        self.prefetch_rx = Some(rx);
        let allocator = Arc::clone(&self.allocator);
        let ec_scheme = self.ec_scheme;
        let config = Arc::clone(&self.config);
        let prealloc_depth = config.prealloc_depth as u32;
        let max_chunk_size = config.max_chunk_size;
        let unit_bytes = u64::from((config.read_buffer_size / 1024) as u32) * 1024;
        let strip_data_bytes = ec_scheme.data_num as u64 * unit_bytes;
        let strips_per_chunk = (max_chunk_size / strip_data_bytes) as u32;
        let mut strips_remaining = self.strips_remaining;
        let mut next_strip_index = self.write_cursor + 1;
        let write_cursor_shared = Arc::clone(&self.write_cursor_shared);
        let handle: JoinHandle<()> = tokio::spawn(async move {
            loop {
                // Stop conditions:
                // - known-size and all strips allocated
                if let Some(remaining) = strips_remaining {
                    if remaining == 0 {
                        break;
                    }
                }
                // - chunk full (enough strips for max_chunk_size)
                if next_strip_index >= strips_per_chunk {
                    break;
                }
                // - prealloc_depth satisfied: stop appending when the
                //   lag (allocated - written) >= prealloc_depth.
                let written = write_cursor_shared.load(Ordering::Relaxed);
                let lag = next_strip_index.saturating_sub(written);
                if lag >= prealloc_depth {
                    // Wait a bit for the writer to catch up before
                    // re-checking.
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    continue;
                }
                let write_granularity_kb = (config.read_buffer_size / 1024) as u32;
                let result = append_strip(
                    &*allocator,
                    chunk_id,
                    ec_scheme,
                    next_strip_index,
                    write_granularity_kb,
                )
                .await;
                match result {
                    Ok(chunk) => {
                        if tx.send(Ok(chunk)).await.is_err() {
                            // Receiver dropped — ChunkWriter was
                            // sealed/aborted. Stop.
                            break;
                        }
                        next_strip_index += 1;
                        if let Some(remaining) = strips_remaining.as_mut() {
                            *remaining = remaining.saturating_sub(1);
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        break;
                    }
                }
            }
        });
        self.prefetch_handle = Some(handle);
    }

    /// Finish the current strip. Records bytes written + collects
    /// parity write/fsync handles (joined at `seal` time, not here —
    /// strip N+1's data writes overlap with strip N's parity writes).
    pub async fn finish_strip(&mut self) -> Result<StripResult> {
        self.await_parity_capacity().await?;
        let mut strip = self
            .current_strip
            .take()
            .ok_or_else(|| IoError::Internal("finish_strip with no open strip".into()))?;
        let mut strip_result = strip.finish().await?;
        self.bytes_in_chunk += strip_result.bytes_written;
        // Collect parity handles for seal-time join.
        self.parity_handles.append(&mut strip_result.parity_handles);
        Ok(strip_result)
    }

    async fn await_parity_capacity(&mut self) -> Result<()> {
        let depth = self.config.parity_depth.max(1);
        while self.parity_handles.len() >= depth {
            let handle = self.parity_handles.remove(0);
            handle
                .await
                .map_err(|error| IoError::Internal(format!("parity task panicked: {error}")))??;
        }
        Ok(())
    }

    /// Is the current strip full (all data_num blocks written)?
    pub(crate) fn is_strip_full(&self) -> bool {
        match &self.current_strip {
            Some(s) => !s.ready(),
            None => true,
        }
    }

    /// Is the chunk full (bytes written >= max_chunk_size)? The object
    /// layer checks this after each push to decide chunk rotation.
    pub fn is_full(&self) -> bool {
        self.bytes_in_chunk >= self.config.max_chunk_size
    }

    pub(crate) fn preparation_metrics(&self) -> (u64, Duration) {
        (self.preparation_stalls, self.preparation_stall_time)
    }

    /// Append a new strip to the current chunk via `append_chunk` RPC.
    /// Returns the full cumulative `Chunk` (with the new strip
    /// appended). Used by the internal strip prefetch + the inline
    /// fallback in `open_next_strip`.
    pub(crate) async fn append_strip(&mut self) -> Result<Chunk> {
        let chunk_id = self
            .current_chunk_id()
            .ok_or_else(|| IoError::Internal("append_strip with no open chunk".into()))?;
        let write_granularity_kb = (self.config.read_buffer_size / 1024) as u32;
        append_strip(
            &*self.allocator,
            chunk_id,
            self.ec_scheme,
            self.write_cursor + 1,
            write_granularity_kb,
        )
        .await
    }

    /// Seal the chunk: finish the current strip (if open with data),
    /// join all in-flight parity writes + fsyncs, then `seal_chunk`
    /// RPC, return the chunk's Location. Parity writes from all strips
    /// in this chunk are joined here (decoupled from strip finish in
    /// Phase 3.1).
    pub async fn seal(&mut self) -> Result<ProtoLocation> {
        // Stop the strip-prefetch task (drop receiver → task exits).
        self.stop_prefetch();
        // Finish the current strip if it's open and has data (the
        // last strip may be partial or full — push doesn't auto-finish
        // on EOF). Skip empty strips (no blocks written → no parity).
        if let Some(strip) = &self.current_strip {
            if strip.has_data() {
                self.finish_strip().await?;
            }
        }
        let chunk_id = self.current_chunk_id();
        let bytes_in_chunk = self.bytes_in_chunk;

        let location = match chunk_id {
            Some(cid) if bytes_in_chunk > 0 => {
                // Join all in-flight parity writes + fsyncs before sealing.
                let handles = std::mem::take(&mut self.parity_handles);
                for handle in handles {
                    handle
                        .await
                        .map_err(|e| IoError::Internal(format!("parity task panicked: {e}")))??;
                }
                let unit_bytes = u64::from((self.config.read_buffer_size / 1024) as u32) * 1024;
                let sealed_length_units = (bytes_in_chunk / unit_bytes) as u32;
                self.allocator
                    .seal_chunk(SealChunkRequest {
                        chunk_id: Some(cid),
                        seal_length: sealed_length_units,
                    })
                    .await?;
                ProtoLocation {
                    chunk_id: Some(cid),
                    offset: 0,
                    length: bytes_in_chunk,
                    logical_offset: 0,
                    logical_length: bytes_in_chunk,
                }
            }
            Some(cid) => {
                warn!("seal: deleting empty chunk");
                let _ = self
                    .allocator
                    .delete_chunk(DeleteChunkRequest { chunk_id: Some(cid) })
                    .await;
                ProtoLocation {
                    chunk_id: Some(cid),
                    offset: 0,
                    length: 0,
                    logical_offset: 0,
                    logical_length: 0,
                }
            }
            None => {
                return Err(IoError::Internal("seal with no open chunk".into()));
            }
        };

        Ok(location)
    }

    /// Abort: cancel in-flight parity writes, stop the strip-prefetch
    /// task, drop the current strip, delete the partial (unsealed)
    /// chunk.
    pub async fn abort(&mut self) -> Result<()> {
        self.stop_prefetch();
        let had_strip = self.current_strip.is_some();
        if let Some(mut strip) = self.current_strip.take() {
            let _ = strip.abort().await;
        }
        // Submitted DiskIO RPCs are not cancellable. Drain finalization before
        // freeing segments so a late parity write cannot hit reused storage.
        for handle in self.parity_handles.drain(..) {
            let _ = handle.await;
        }
        // Delete the chunk if it was opened and has any data — either
        // finished strips (bytes_in_chunk > 0), an in-progress strip
        // (had_strip), or prior finished strips (write_cursor > 0).
        if let Some(chunk_id) = self.current_chunk_id() {
            if self.bytes_in_chunk > 0 || had_strip || self.write_cursor > 0 {
                warn!("abort: deleting partial chunk");
                let _ = self
                    .allocator
                    .delete_chunk(DeleteChunkRequest {
                        chunk_id: Some(chunk_id),
                    })
                    .await;
            }
        }
        Ok(())
    }

    /// Non-async capacity hint. True if the current strip has room.
    pub fn ready(&self) -> bool {
        match &self.current_strip {
            Some(s) => s.ready(),
            None => false,
        }
    }

    /// Bytes written to the current chunk so far.
    pub fn bytes_in_chunk(&self) -> u64 {
        self.bytes_in_chunk
    }

    /// Current chunk id (if any), derived from the owned `Chunk`.
    pub fn current_chunk_id(&self) -> Option<ChunkId> {
        self.chunk.as_ref().and_then(|c| c.id)
    }

    /// Strips opened in the current chunk so far (= write_cursor + 1
    /// when a chunk is open).
    pub fn strips_in_chunk(&self) -> u32 {
        if self.chunk.is_some() {
            self.write_cursor + 1
        } else {
            0
        }
    }
}

/// Compute the number of strips not yet allocated for a known-size
/// object. Returns `None` for unknown-size objects. Used by the
/// internal strip prefetch task for planning.
fn compute_strips_remaining(
    object_size: Option<u64>,
    ec_scheme: &EcScheme,
    config: &ChunkClientConfig,
) -> Option<usize> {
    let total = object_size?;
    let unit_bytes = u64::from((config.read_buffer_size / 1024) as u32) * 1024;
    let strip_data_capacity = ec_scheme.data_num as u64 * unit_bytes;
    let total_strips = total.div_ceil(strip_data_capacity) as usize;
    // Subtract 1 for the initial strip from allocate_chunk.
    Some(total_strips.saturating_sub(1))
}

/// Append 1 strip to an existing chunk and return the full cumulative
/// `Chunk`. Used by `ChunkWriter`'s internal strip prefetch + the
/// inline fallback in `open_next_strip`.
async fn append_strip(
    chunkdb: &dyn ChunkAllocator,
    chunk_id: ChunkId,
    ec_scheme: EcScheme,
    strip_index_in_chunk: u32,
    write_granularity_kb: u32,
) -> Result<Chunk> {
    let req = AppendChunkRequest {
        chunk_id: Some(chunk_id),
        strip_size: ec_scheme.data_num as u32,
        strip_count: 1,
        strip_type: StripType::Ec as i32,
        data_num: ec_scheme.data_num as u32,
        code_num: ec_scheme.code_num as u32,
        copy_count: 0,
    };
    let _ = (write_granularity_kb, strip_index_in_chunk);
    let resp = chunkdb.append_chunk(req).await?;
    resp.chunk
        .ok_or_else(|| IoError::AllocationFailed("append_chunk response missing chunk".into()))
}
