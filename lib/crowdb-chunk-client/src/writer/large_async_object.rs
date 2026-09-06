// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `LargeAsyncObjectWriter` — async stream variant, chunk-level drive
//! loop.
//!
//! Accepts an `AsyncRead` stream — a more complex flow with a fetch
//! stage + backpressure. The strip-level drive loop is in
//! `ChunkWriter::push` (auto-rotates strips). This writer owns the
//! chunk-level drive loop: pulls `Chunk` values from `ChunkPrefetch`,
//! opens `ChunkWriter` with `object_size`, pushes blocks, rotates
//! chunks when `is_full()`, seals at EOF. Implements `ChunkIoWriter`
//! for push mode.

use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::chunk::chunk_prefetch::ChunkPrefetch;
use crate::chunk::chunk_writer::ChunkWriter;
use crate::config::ChunkClientConfig;
use crate::disk_io::DiskWriter;
use crate::io::{ChunkIoWriter, FeedStatus};
use crate::traits::ChunkAllocator;
use crate::writer::fetch::run_fetch_stage;
use crate::{IoError, Result};
use crowdb_common::ec::EcScheme;
use crowdb_protocol::chunkdb::rpc::{Chunk, Location as ProtoLocation};

/// Large-object writer — async stream. Owns the chunk-level drive
/// loop + fetch stage; strip-level rotation is in `ChunkWriter::push`.
pub struct LargeAsyncObjectWriter {
    pub(crate) allocator: Arc<dyn ChunkAllocator>,
    pub(crate) disk_writer: Arc<dyn DiskWriter>,
    pub(crate) ec_scheme: EcScheme,
    pub(crate) config: Arc<ChunkClientConfig>,
    pub(crate) chunk_writer: Option<ChunkWriter>,
    pub(crate) chunk_prefetch_rx: Option<mpsc::Receiver<Result<Chunk>>>,
    pub(crate) chunk_prefetch_handle: Option<JoinHandle<()>>,
    pub(crate) locations: Vec<ProtoLocation>,
    pub(crate) logical_offset: u64,
    pub(crate) object_size: Option<u64>,
    pub(crate) finished: bool,
}

impl LargeAsyncObjectWriter {
    /// Construct a new writer.
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
            chunk_writer: None,
            chunk_prefetch_rx: None,
            chunk_prefetch_handle: None,
            locations: Vec::new(),
            logical_offset: 0,
            object_size: None,
            finished: false,
        }
    }

    /// Per-writer memory footprint.
    pub fn per_writer_memory(&self) -> usize {
        self.config.per_writer_memory(&self.ec_scheme)
    }

    /// Start bounded chunk preparation before source consumption begins.
    pub(crate) fn prepare(&mut self, object_size: Option<u64>) {
        self.object_size = object_size;
        let prefetch = ChunkPrefetch::new(
            self.allocator.clone(),
            self.ec_scheme,
            self.config.clone(),
            crowdb_protocol::chunk_id::CHUNK_TYPE_REPO,
        );
        let (chunk_rx, prefetch_handle) = prefetch.spawn(object_size);
        self.chunk_prefetch_rx = Some(chunk_rx);
        self.chunk_prefetch_handle = Some(prefetch_handle);
    }

    /// Seal the current chunk (if any) and record its ProtoLocation.
    pub(crate) async fn seal_current(&mut self) -> Result<()> {
        if let Some(mut cw) = self.chunk_writer.take() {
            let location = cw.seal().await?;
            let bytes = location.length;
            if bytes > 0 {
                self.locations.push(ProtoLocation {
                    logical_offset: self.logical_offset,
                    logical_length: bytes,
                    ..location
                });
                self.logical_offset += bytes;
            }
        }
        Ok(())
    }

    /// Pull the next `Chunk` from the prefetch channel (or on-demand
    /// if the channel is exhausted).
    pub(crate) async fn next_chunk(&mut self) -> Result<Option<Chunk>> {
        if let Some(rx) = self.chunk_prefetch_rx.as_mut() {
            match rx.recv().await {
                Some(Ok(c)) => return Ok(Some(c)),
                Some(Err(e)) => return Err(e),
                None => {
                    self.chunk_prefetch_rx = None;
                }
            }
        }
        // On-demand allocation (prefetch channel exhausted).
        let pf = ChunkPrefetch::new(
            self.allocator.clone(),
            self.ec_scheme,
            self.config.clone(),
            crowdb_protocol::chunk_id::CHUNK_TYPE_REPO,
        );
        let chunk = pf.on_demand().await?;
        Ok(Some(chunk))
    }

    /// Ensure a `ChunkWriter` is open. If none, pull the next `Chunk`
    /// + open a new `ChunkWriter` with `object_size`.
    pub(crate) async fn ensure_open(&mut self) -> Result<()> {
        if self.chunk_writer.is_some() {
            return Ok(());
        }
        let chunk = self
            .next_chunk()
            .await?
            .ok_or_else(|| IoError::Internal("no chunk available".into()))?;
        let mut cw = ChunkWriter::new(
            self.allocator.clone(),
            self.disk_writer.clone(),
            self.ec_scheme,
            self.config.clone(),
        );
        cw.open(chunk, self.object_size)?;
        self.chunk_writer = Some(cw);
        Ok(())
    }

    /// Rotate: seal the current chunk, pull the next `Chunk`, open a
    /// new `ChunkWriter`.
    pub(crate) async fn rotate_chunk(&mut self) -> Result<()> {
        self.seal_current().await?;
        self.ensure_open().await
    }

    /// Async stream write. Runs fetch stage + chunk-level drive loop
    /// concurrently. The strip-level drive loop is in
    /// `ChunkWriter::push` (auto-rotates strips).
    pub async fn write_stream(
        &mut self,
        reader: impl tokio::io::AsyncRead + Unpin + Send,
        object_size: Option<u64>,
    ) -> Result<Vec<ProtoLocation>> {
        if self.finished {
            return Err(IoError::Finished);
        }
        self.finished = true;

        if object_size == Some(0) {
            return Ok(Vec::new());
        }

        if self.chunk_prefetch_rx.is_none() {
            self.prepare(object_size);
        }

        let channel_cap = (self.config.max_cached_buffer / self.config.read_buffer_size).max(1);
        let (block_tx, mut block_rx) = mpsc::channel::<Bytes>(channel_cap);
        let fetch_fut = run_fetch_stage(reader, block_tx, self.config.read_buffer_size);

        let drive_fut = async {
            loop {
                // Ensure a ChunkWriter is open.
                self.ensure_open().await?;
                // Receive the next block from the fetch stage.
                match block_rx.recv().await {
                    Some(buffer) => {
                        // Push the block. If the chunk is full (Pause),
                        // rotate to a new chunk and re-push. Bytes is
                        // ref-counted, so clone is cheap.
                        let buffer = buffer;
                        loop {
                            let status = {
                                let cw = self
                                    .chunk_writer
                                    .as_mut()
                                    .ok_or_else(|| IoError::Internal("no chunk writer".into()))?;
                                cw.push(buffer.clone()).await?
                            };
                            if status == FeedStatus::Pause {
                                self.rotate_chunk().await?;
                                continue;
                            }
                            break;
                        }
                    }
                    None => {
                        // EOF — break out, seal the current chunk.
                        break;
                    }
                }
            }
            Ok::<(), IoError>(())
        };

        let (_fetch_result, drive_result) = tokio::join!(fetch_fut, drive_fut);
        drive_result?;

        self.seal_current().await?;
        if let Some(handle) = self.chunk_prefetch_handle.take() {
            handle.abort();
        }

        Ok(std::mem::take(&mut self.locations))
    }

    /// Abort: cancel in-flight, return already-sealed Locations.
    pub(crate) async fn abort_pipeline(&mut self) -> Result<Vec<ProtoLocation>> {
        if let Some(mut cw) = self.chunk_writer.take() {
            let _ = cw.abort().await;
        }
        if let Some(handle) = self.chunk_prefetch_handle.take() {
            handle.abort();
        }
        Ok(std::mem::take(&mut self.locations))
    }
}

#[async_trait::async_trait]
impl ChunkIoWriter for LargeAsyncObjectWriter {
    async fn on_data(&mut self, buffer: Bytes) -> Result<FeedStatus> {
        if self.finished {
            return Err(IoError::Finished);
        }
        // Lazy-start prefetch on first push.
        if self.chunk_prefetch_rx.is_none() && self.chunk_writer.is_none() {
            let prefetch = ChunkPrefetch::new(
                self.allocator.clone(),
                self.ec_scheme,
                self.config.clone(),
                crowdb_protocol::chunk_id::CHUNK_TYPE_REPO,
            );
            let (rx, handle) = prefetch.spawn(None);
            self.chunk_prefetch_rx = Some(rx);
            self.chunk_prefetch_handle = Some(handle);
        }
        // Ensure a ChunkWriter is open.
        self.ensure_open().await?;
        // Push the block (auto-rotates strips internally).
        let cw = self
            .chunk_writer
            .as_mut()
            .ok_or_else(|| IoError::Internal("no chunk writer".into()))?;
        let status = cw.push(buffer).await?;
        // If the chunk is full, rotate to a new chunk.
        if cw.is_full() {
            self.rotate_chunk().await?;
        }
        Ok(status)
    }

    async fn on_finish(&mut self) -> Result<Vec<ProtoLocation>> {
        if self.finished {
            return Err(IoError::Finished);
        }
        self.finished = true;
        self.seal_current().await?;
        if let Some(handle) = self.chunk_prefetch_handle.take() {
            handle.abort();
        }
        Ok(std::mem::take(&mut self.locations))
    }

    async fn on_error(&mut self) -> Result<Vec<ProtoLocation>> {
        self.finished = true;
        self.abort_pipeline().await
    }

    fn require_data(&self) -> bool {
        if self.finished {
            return false;
        }
        self.chunk_writer.as_ref().map_or(true, ChunkWriter::ready)
    }
}
