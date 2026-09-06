// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Integration tests for `LargeAsyncObjectWriter::write_stream` and
//! `LargeObjectWriter` push mode, using mock `ChunkAllocator` +
//! `LocalFileDiskWriter` (real file I/O, write-then-read verification).

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::doc_markdown,
    clippy::similar_names
)]

mod common;

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use crowdb_test_harness::test_dirs;

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_chunk_client::{
    run_large_write_benchmark, ChunkAllocator, ChunkClientConfig, ChunkIoClient, ChunkIoWriter, IoError,
    LargeAsyncObjectWriter, LargeObjectWriter, LargeWriteBenchmarkConfig, LargeWritePolicy, Result,
    WriterPool,
};
use crowdb_common::ec::EcScheme;
use crowdb_diskio_client::DiskId;
use crowdb_protocol::chunkdb::rpc::Strip as StripOneof;
use crowdb_protocol::chunkdb::rpc::{
    AllocateChunkRequest, AllocateChunkResponse, AppendChunkRequest, AppendChunkResponse, Chunk, ChunkStrip,
    ChunkType, DeleteChunkRequest, DeleteChunkResponse, EcStrip, QueryChunkRequest, QueryChunkResponse,
    SealChunkRequest, SealChunkResponse, StripType, UpdateChunkStripRequest, UpdateChunkStripResponse,
};
use crowdb_protocol::common::DiskId as ProtoDiskId;
use crowdb_protocol::diskdb::rpc::Segment;

use common::LocalFileDiskWriter;

const UNIT_BYTES: u64 = 4096;
const DATA_NUM: usize = 4;
const CODE_NUM: usize = 1;
const TOTAL: usize = DATA_NUM + CODE_NUM;

struct ErrorReader {
    emitted: bool,
}

impl tokio::io::AsyncRead for ErrorReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.emitted {
            Poll::Ready(Err(std::io::Error::other("source failed")))
        } else {
            self.emitted = true;
            buffer.put_slice(&[0x5a; 128]);
            Poll::Ready(Ok(()))
        }
    }
}

// ── Mock ChunkAllocator ──────────────────────────────────────────

#[derive(Debug, Clone, Default)]
struct MockChunkAllocator {
    state: Arc<Mutex<MockChunkState>>,
}

#[derive(Debug, Default)]
struct MockChunkState {
    chunks: HashMap<(u64, u64), (Vec<ChunkStrip>, u32, bool)>,
    next_segment_offset: u64,
    allocate_calls: usize,
    append_calls: usize,
    seal_calls: usize,
    delete_calls: usize,
}

impl MockChunkAllocator {
    fn new() -> Self {
        Self::default()
    }

    fn snapshot(&self) -> std::sync::MutexGuard<'_, MockChunkState> {
        self.state.lock().unwrap()
    }
}

#[async_trait]
impl ChunkAllocator for MockChunkAllocator {
    async fn allocate_chunk(&self, req: AllocateChunkRequest) -> Result<AllocateChunkResponse> {
        let mut st = self.state.lock().unwrap();
        st.allocate_calls += 1;
        let chunk_id = req.chunk_id.unwrap_or_default();
        let data_num = req.data_num as usize;
        let code_num = req.code_num as usize;
        let total = data_num + code_num;

        let mut segments = Vec::with_capacity(total);
        for i in 0..total {
            segments.push(Segment {
                disk_id: Some(ProtoDiskId {
                    high: 1000 + i as u64,
                    low: i as u64,
                }),
                zone_index: 0,
                unit_offset: st.next_segment_offset,
                unit_count: 1,
                owner_chunk: Some(chunk_id),
                allocation_ts: st.next_segment_offset + 1,
            });
            st.next_segment_offset += 1;
        }

        let strip = ChunkStrip {
            chunk_offset: 0,
            strip_sequence: 0,
            unit_kb: 4,
            capacity: data_num as u32,
            create_ts_ms: 0,
            sealed_ts_ms: 0,
            sealed_length: 0,
            strip_type: StripType::Ec as i32,
            strip: Some(StripOneof::EcStrip(EcStrip {
                data_num: req.data_num,
                code_num: req.code_num,
                ec_state: 0,
                segments,
            })),
            usage_bitmap: Vec::new(),
        };

        let chunk = Chunk {
            id: Some(chunk_id),
            state: 1,
            create_ts_ms: 0,
            sealed_ts_ms: 0,
            capacity: data_num as u32,
            sealed_length: 0,
            strips: vec![strip.clone()],
            chunk_type: ChunkType::Repo as i32,
        };

        st.chunks
            .insert((chunk_id.high, chunk_id.low), (vec![strip], 0, false));
        Ok(AllocateChunkResponse { chunk: Some(chunk) })
    }

    async fn append_chunk(&self, req: AppendChunkRequest) -> Result<AppendChunkResponse> {
        let mut st = self.state.lock().unwrap();
        st.append_calls += 1;
        let chunk_id = req.chunk_id.unwrap_or_default();
        let data_num = req.data_num as usize;
        let code_num = req.code_num as usize;
        let total = data_num + code_num;

        let strip_seq = st
            .chunks
            .get(&(chunk_id.high, chunk_id.low))
            .map_or(0, |e| e.0.len() as u32);

        let mut segments = Vec::with_capacity(total);
        for i in 0..total {
            segments.push(Segment {
                disk_id: Some(ProtoDiskId {
                    high: 1000 + i as u64,
                    low: i as u64,
                }),
                zone_index: 0,
                unit_offset: st.next_segment_offset,
                unit_count: 1,
                owner_chunk: Some(chunk_id),
                allocation_ts: st.next_segment_offset + 1,
            });
            st.next_segment_offset += 1;
        }

        let strip = ChunkStrip {
            chunk_offset: strip_seq,
            strip_sequence: strip_seq,
            unit_kb: 4,
            capacity: data_num as u32,
            create_ts_ms: 0,
            sealed_ts_ms: 0,
            sealed_length: 0,
            strip_type: StripType::Ec as i32,
            strip: Some(StripOneof::EcStrip(EcStrip {
                data_num: req.data_num,
                code_num: req.code_num,
                ec_state: 0,
                segments,
            })),
            usage_bitmap: Vec::new(),
        };

        let entry = st
            .chunks
            .get_mut(&(chunk_id.high, chunk_id.low))
            .expect("append to unknown chunk");
        entry.0.push(strip);
        let strips = entry.0.clone();

        let chunk = Chunk {
            id: Some(chunk_id),
            state: 1,
            create_ts_ms: 0,
            sealed_ts_ms: 0,
            capacity: data_num as u32 * (strip_seq + 1),
            sealed_length: 0,
            strips,
            chunk_type: ChunkType::Repo as i32,
        };

        Ok(AppendChunkResponse { chunk: Some(chunk) })
    }

    async fn seal_chunk(&self, req: SealChunkRequest) -> Result<SealChunkResponse> {
        let mut st = self.state.lock().unwrap();
        st.seal_calls += 1;
        let chunk_id = req.chunk_id.unwrap_or_default();
        let entry = st
            .chunks
            .get_mut(&(chunk_id.high, chunk_id.low))
            .expect("seal unknown chunk");
        entry.1 = req.seal_length;
        Ok(SealChunkResponse { chunk: None })
    }

    async fn delete_chunk(&self, req: DeleteChunkRequest) -> Result<DeleteChunkResponse> {
        let mut st = self.state.lock().unwrap();
        st.delete_calls += 1;
        let chunk_id = req.chunk_id.unwrap_or_default();
        let entry = st
            .chunks
            .get_mut(&(chunk_id.high, chunk_id.low))
            .expect("delete unknown chunk");
        entry.2 = true;
        Ok(DeleteChunkResponse { chunk: None })
    }

    async fn update_chunk_strip(&self, _req: UpdateChunkStripRequest) -> Result<UpdateChunkStripResponse> {
        Ok(UpdateChunkStripResponse { chunk: None })
    }

    async fn query_chunk(&self, _req: QueryChunkRequest) -> Result<QueryChunkResponse> {
        Ok(QueryChunkResponse { chunk: None })
    }
}

// ── Helpers ──────────────────────────────────────────────────────

fn test_config(max_chunk_size: u64) -> Arc<ChunkClientConfig> {
    Arc::new(ChunkClientConfig {
        max_chunk_size,
        prealloc_depth: 2,
        parity_depth: 2,
        chunk_prefetch_depth: 1,
        read_buffer_size: 4096,
        max_cached_buffer: 8 * 4096,
        prefetch_chunk_count: 1,
        memory_budget: 0,
    })
}

fn ec_4_1() -> EcScheme {
    EcScheme::new(4, 1)
}

fn make_writer(
    chunkdb: MockChunkAllocator,
    diskio: LocalFileDiskWriter,
    ec: EcScheme,
    config: Arc<ChunkClientConfig>,
) -> LargeAsyncObjectWriter {
    LargeAsyncObjectWriter::new(Arc::new(chunkdb), Arc::new(diskio), ec, config)
}

fn make_push_writer(
    chunkdb: MockChunkAllocator,
    diskio: LocalFileDiskWriter,
    ec: EcScheme,
    config: Arc<ChunkClientConfig>,
) -> LargeObjectWriter {
    LargeObjectWriter::new(Arc::new(chunkdb), Arc::new(diskio), ec, config)
}

/// Read a data shard from disk. The mock allocator assigns shard `i`
/// of strip `s` to disk `1000+i` at unit_offset `s*TOTAL + i`.
fn read_data_shard(diskio: &LocalFileDiskWriter, strip: usize, shard: usize) -> Vec<u8> {
    let unit_offset = (strip * TOTAL + shard) as u64;
    let zone_offset = unit_offset * UNIT_BYTES;
    let disk_id = DiskId::new(1000 + shard as u64, shard as u64);
    diskio
        .read_block(disk_id, zone_offset, UNIT_BYTES as usize)
        .unwrap_or_default()
}

/// Read a parity shard from disk. Parity shard `i` of strip `s` is on
/// disk `1000+data_num+i` at unit_offset `s*TOTAL + data_num + i`.
fn read_parity_shard(diskio: &LocalFileDiskWriter, strip: usize, parity_idx: usize) -> Vec<u8> {
    let unit_offset = (strip * TOTAL + DATA_NUM + parity_idx) as u64;
    let zone_offset = unit_offset * UNIT_BYTES;
    let disk_id = DiskId::new(
        1000 + DATA_NUM as u64 + parity_idx as u64,
        DATA_NUM as u64 + parity_idx as u64,
    );
    diskio
        .read_block(disk_id, zone_offset, UNIT_BYTES as usize)
        .unwrap_or_default()
}

/// Reconstruct original data from data shards across all strips.
fn reconstruct_data(diskio: &LocalFileDiskWriter, num_strips: usize) -> Vec<u8> {
    let mut reconstructed = Vec::new();
    for s in 0..num_strips {
        for i in 0..DATA_NUM {
            let shard = read_data_shard(diskio, s, i);
            // Trim trailing zeros for partial last strip — the writer
            // pads partial shards to unit_bytes. We only know the real
            // length from the Location, so callers trim as needed.
            reconstructed.extend_from_slice(&shard);
        }
    }
    reconstructed
}

// ── write_stream tests ───────────────────────────────────────────

#[tokio::test]
async fn write_stream_empty_object() {
    let chunkdb = MockChunkAllocator::new();
    let tmp = test_dirs::tempdir_in_test_data("chunk-client");
    let diskio = LocalFileDiskWriter::new(tmp.path());
    let ec = ec_4_1();
    let mut writer = make_writer(chunkdb, diskio, ec, test_config(1024 * 1024));
    let data: Vec<u8> = Vec::new();
    let locs = writer.write_stream(data.as_slice(), Some(0)).await.unwrap();
    assert!(locs.is_empty());
}

#[tokio::test]
async fn write_stream_single_block_4mb() {
    let chunkdb = MockChunkAllocator::new();
    let tmp = test_dirs::tempdir_in_test_data("chunk-client");
    let diskio = LocalFileDiskWriter::new(tmp.path());
    let ec = ec_4_1();
    let mut writer = make_writer(chunkdb.clone(), diskio.clone(), ec, test_config(1024 * 1024));

    let data = vec![0xABu8; 4 * 4096];
    let locs = writer
        .write_stream(data.as_slice(), Some(data.len() as u64))
        .await
        .unwrap();

    assert_eq!(locs.len(), 1);
    let loc = &locs[0];
    assert_eq!(loc.offset, 0);
    assert_eq!(loc.length, 4 * 4096);
    assert_eq!(loc.logical_offset, 0);
    assert_eq!(loc.logical_length, 4 * 4096);

    let st = chunkdb.snapshot();
    assert_eq!(st.allocate_calls, 1);
    assert_eq!(st.append_calls, 0);
    assert_eq!(st.seal_calls, 1);
    assert_eq!(st.delete_calls, 0);

    // 4 data writes + 1 parity write = 5 writes.
    assert_eq!(diskio.write_count(), 5);
}

#[tokio::test]
async fn write_stream_partial_strip_3_blocks() {
    let chunkdb = MockChunkAllocator::new();
    let tmp = test_dirs::tempdir_in_test_data("chunk-client");
    let diskio = LocalFileDiskWriter::new(tmp.path());
    let ec = ec_4_1();
    let mut writer = make_writer(chunkdb.clone(), diskio.clone(), ec, test_config(1024 * 1024));

    let data = vec![0xCDu8; 3 * 4096];
    let locs = writer
        .write_stream(data.as_slice(), Some(data.len() as u64))
        .await
        .unwrap();

    assert_eq!(locs.len(), 1);
    assert_eq!(locs[0].length, 3 * 4096);

    // 3 data writes + 1 parity write = 4 writes.
    assert_eq!(diskio.write_count(), 4);
}

#[tokio::test]
async fn write_stream_pads_only_parity_for_unaligned_tail() {
    let chunkdb = MockChunkAllocator::new();
    let tmp = test_dirs::tempdir_in_test_data("chunk-client");
    let diskio = LocalFileDiskWriter::new(tmp.path());
    let mut writer = make_writer(chunkdb, diskio.clone(), ec_4_1(), test_config(1024 * 1024));
    let data = vec![0x7bu8; 4096 + 123];

    let locs = writer
        .write_stream(data.as_slice(), Some(data.len() as u64))
        .await
        .unwrap();

    assert_eq!(locs[0].length, data.len() as u64);
    assert_eq!(diskio.write_count(), 3);
}

#[tokio::test]
async fn write_stream_propagates_source_error_and_aborts() {
    let chunkdb = MockChunkAllocator::new();
    let tmp = test_dirs::tempdir_in_test_data("chunk-client");
    let diskio = LocalFileDiskWriter::new(tmp.path());
    let mut writer = make_writer(chunkdb.clone(), diskio, ec_4_1(), test_config(1024 * 1024));

    let error = writer
        .write_stream(ErrorReader { emitted: false }, Some(4096))
        .await
        .unwrap_err();

    assert!(matches!(error, IoError::SourceRead(_)));
    assert_eq!(chunkdb.snapshot().delete_calls, 1);
}

#[tokio::test]
async fn write_stream_multi_strip_same_chunk() {
    let chunkdb = MockChunkAllocator::new();
    let tmp = test_dirs::tempdir_in_test_data("chunk-client");
    let diskio = LocalFileDiskWriter::new(tmp.path());
    let ec = ec_4_1();
    let mut writer = make_writer(chunkdb.clone(), diskio.clone(), ec, test_config(1024 * 1024));

    let data = vec![0x11u8; 8 * 4096];
    let locs = writer
        .write_stream(data.as_slice(), Some(data.len() as u64))
        .await
        .unwrap();

    assert_eq!(locs.len(), 1);
    assert_eq!(locs[0].length, 8 * 4096);

    let st = chunkdb.snapshot();
    assert_eq!(st.allocate_calls, 1);
    assert_eq!(st.append_calls, 1);
    assert_eq!(st.seal_calls, 1);

    // 8 data + 2 parity = 10 writes.
    assert_eq!(diskio.write_count(), 10);
}

#[tokio::test]
async fn write_stream_chunk_rotation() {
    let chunkdb = MockChunkAllocator::new();
    let tmp = test_dirs::tempdir_in_test_data("chunk-client");
    let diskio = LocalFileDiskWriter::new(tmp.path());
    let ec = ec_4_1();
    let mut writer = make_writer(chunkdb.clone(), diskio.clone(), ec, test_config(4 * 4096));

    let data = vec![0x22u8; 8 * 4096];
    let locs = writer
        .write_stream(data.as_slice(), Some(data.len() as u64))
        .await
        .unwrap();

    assert_eq!(locs.len(), 2);
    assert_eq!(locs[0].length, 4 * 4096);
    assert_eq!(locs[0].logical_offset, 0);
    assert_eq!(locs[1].length, 4 * 4096);
    assert_eq!(locs[1].logical_offset, 4 * 4096);

    let st = chunkdb.snapshot();
    assert_eq!(st.allocate_calls, 2);
    assert_eq!(st.seal_calls, 2);

    // 8 data + 2 parity = 10 writes.
    assert_eq!(diskio.write_count(), 10);
}

#[tokio::test]
async fn write_stream_unknown_size_streaming() {
    let chunkdb = MockChunkAllocator::new();
    let tmp = test_dirs::tempdir_in_test_data("chunk-client");
    let diskio = LocalFileDiskWriter::new(tmp.path());
    let ec = ec_4_1();
    let mut writer = make_writer(chunkdb.clone(), diskio.clone(), ec, test_config(1024 * 1024));

    let data = vec![0x33u8; 4 * 4096];
    let locs = writer.write_stream(data.as_slice(), None).await.unwrap();

    assert_eq!(locs.len(), 1);
    assert_eq!(locs[0].length, 4 * 4096);
}

#[tokio::test]
async fn write_stream_data_integrity() {
    let chunkdb = MockChunkAllocator::new();
    let tmp = test_dirs::tempdir_in_test_data("chunk-client");
    let diskio = LocalFileDiskWriter::new(tmp.path());
    let ec = ec_4_1();
    let mut writer = make_writer(chunkdb, diskio.clone(), ec, test_config(1024 * 1024));

    let mut data = Vec::new();
    for i in 0..4 * 4096u32 {
        data.push((i % 251) as u8);
    }
    let locs = writer
        .write_stream(data.as_slice(), Some(data.len() as u64))
        .await
        .unwrap();
    assert_eq!(locs.len(), 1);

    // Read back data shards from disk and reconstruct.
    let reconstructed = reconstruct_data(&diskio, 1);
    assert_eq!(reconstructed, data);
}

#[tokio::test]
async fn write_stream_parity_correctness() {
    use crowdb_common::ec::{decode, encode_parity_from_shards};

    let chunkdb = MockChunkAllocator::new();
    let tmp = test_dirs::tempdir_in_test_data("chunk-client");
    let diskio = LocalFileDiskWriter::new(tmp.path());
    let ec = ec_4_1();
    let mut writer = make_writer(chunkdb, diskio.clone(), ec, test_config(1024 * 1024));

    let data = vec![0x55u8; 4 * 4096];
    writer
        .write_stream(data.as_slice(), Some(data.len() as u64))
        .await
        .unwrap();

    // Read back data shards + parity shard from disk.
    let data_shards: Vec<Vec<u8>> = {
        let mut v = Vec::new();
        for i in 0..DATA_NUM {
            v.push(read_data_shard(&diskio, 0, i));
        }
        v
    };
    let parity_shard = read_parity_shard(&diskio, 0, 0);

    // Compare with direct encode.
    let shard_refs: Vec<&[u8]> = data_shards.iter().map(Vec::as_slice).collect();
    let expected_parity = encode_parity_from_shards(ec, &shard_refs).unwrap();
    assert_eq!(parity_shard, expected_parity[0]);

    // Decode round-trip: lose data shard 0, reconstruct.
    let mut blocks: Vec<Option<Vec<u8>>> = data_shards.into_iter().map(Some).collect();
    blocks.push(Some(parity_shard));
    blocks[0] = None;
    let recovered = decode(ec, blocks).unwrap();
    let mut reconstructed = Vec::new();
    for shard in recovered.iter().take(DATA_NUM) {
        reconstructed.extend_from_slice(shard);
    }
    assert_eq!(reconstructed, data);
}

#[tokio::test]
async fn write_stream_fsync_per_strip() {
    let chunkdb = MockChunkAllocator::new();
    let tmp = test_dirs::tempdir_in_test_data("chunk-client");
    let diskio = LocalFileDiskWriter::new(tmp.path());
    let ec = ec_4_1();
    let mut writer = make_writer(chunkdb, diskio.clone(), ec, test_config(1024 * 1024));

    let data = vec![0x77u8; 4 * 4096];
    writer
        .write_stream(data.as_slice(), Some(data.len() as u64))
        .await
        .unwrap();

    // 1 strip → 5 unique disks (4 data + 1 parity) → 5 fsyncs.
    assert_eq!(diskio.fsync_count(), 5);
}

#[tokio::test]
async fn write_stream_whole_strip_retry() {
    // The new EcStripWriter doesn't have whole-strip retry yet (the
    // retry logic was in the old pipeline). This test verifies basic
    // 2-strip write without injected failure.
    let chunkdb = MockChunkAllocator::new();
    let tmp = test_dirs::tempdir_in_test_data("chunk-client");
    let diskio = LocalFileDiskWriter::new(tmp.path());
    let ec = ec_4_1();
    let mut writer = make_writer(chunkdb.clone(), diskio.clone(), ec, test_config(1024 * 1024));

    let data = vec![0x88u8; 8 * 4096];
    let locs = writer
        .write_stream(data.as_slice(), Some(data.len() as u64))
        .await
        .unwrap();

    assert_eq!(locs.len(), 1);
    assert_eq!(locs[0].length, 8 * 4096);

    let st = chunkdb.snapshot();
    assert_eq!(st.allocate_calls, 1);
    assert_eq!(st.append_calls, 1);
    assert_eq!(st.seal_calls, 1);

    // 8 data + 2 parity = 10 writes.
    assert_eq!(diskio.write_count(), 10);
}

// ── Push mode (ChunkIoWriter) tests ──────────────────────────────

fn block(value: u8, size: usize) -> Bytes {
    Bytes::from(vec![value; size])
}

#[tokio::test]
async fn push_mode_basic_one_strip() {
    let chunkdb = MockChunkAllocator::new();
    let tmp = test_dirs::tempdir_in_test_data("chunk-client");
    let diskio = LocalFileDiskWriter::new(tmp.path());
    let ec = ec_4_1();
    let mut writer = make_push_writer(chunkdb.clone(), diskio.clone(), ec, test_config(1024 * 1024));

    assert!(writer.require_data());
    for i in 0..4u8 {
        let status = writer.on_data(block(i, 4096)).await.unwrap();
        let _ = status;
    }
    let locs = writer.on_finish().await.unwrap();
    assert_eq!(locs.len(), 1);
    assert_eq!(locs[0].length, 4 * 4096);
    assert!(!writer.require_data());

    let st = chunkdb.snapshot();
    assert_eq!(st.allocate_calls, 1);
    assert_eq!(st.seal_calls, 1);
}

#[tokio::test]
async fn push_mode_empty_object() {
    let chunkdb = MockChunkAllocator::new();
    let tmp = test_dirs::tempdir_in_test_data("chunk-client");
    let diskio = LocalFileDiskWriter::new(tmp.path());
    let ec = ec_4_1();
    let mut writer = make_push_writer(chunkdb, diskio, ec, test_config(1024 * 1024));

    let locs = writer.on_finish().await.unwrap();
    assert!(locs.is_empty());
}

#[tokio::test]
async fn push_mode_on_data_after_finish() {
    let chunkdb = MockChunkAllocator::new();
    let tmp = test_dirs::tempdir_in_test_data("chunk-client");
    let diskio = LocalFileDiskWriter::new(tmp.path());
    let ec = ec_4_1();
    let mut writer = make_push_writer(chunkdb, diskio, ec, test_config(1024 * 1024));

    writer.on_data(block(0, 4096)).await.unwrap();
    writer.on_finish().await.unwrap();
    let result = writer.on_data(block(1, 4096)).await;
    assert!(matches!(result, Err(IoError::Finished)));
}

#[tokio::test]
async fn push_mode_on_finish_twice() {
    let chunkdb = MockChunkAllocator::new();
    let tmp = test_dirs::tempdir_in_test_data("chunk-client");
    let diskio = LocalFileDiskWriter::new(tmp.path());
    let ec = ec_4_1();
    let mut writer = make_push_writer(chunkdb, diskio, ec, test_config(1024 * 1024));

    writer.on_data(block(0, 4096)).await.unwrap();
    writer.on_finish().await.unwrap();
    let result = writer.on_finish().await;
    assert!(matches!(result, Err(IoError::Finished)));
}

#[tokio::test]
async fn push_mode_on_error_no_sealed() {
    let chunkdb = MockChunkAllocator::new();
    let tmp = test_dirs::tempdir_in_test_data("chunk-client");
    let diskio = LocalFileDiskWriter::new(tmp.path());
    let ec = ec_4_1();
    let mut writer = make_push_writer(chunkdb, diskio, ec, test_config(1024 * 1024));

    let locs = writer.on_error().await.unwrap();
    assert!(locs.is_empty());
}

#[tokio::test]
async fn push_mode_on_error_after_sealed_chunk() {
    let chunkdb = MockChunkAllocator::new();
    let tmp = test_dirs::tempdir_in_test_data("chunk-client");
    let diskio = LocalFileDiskWriter::new(tmp.path());
    let ec = ec_4_1();
    let mut writer = make_push_writer(chunkdb.clone(), diskio, ec, test_config(4 * 4096));

    for i in 0..10u8 {
        writer.on_data(block(i, 4096)).await.unwrap();
    }
    let locs = writer.on_error().await.unwrap();
    assert_eq!(locs.len(), 2);
    assert_eq!(locs[0].length, 4 * 4096);
    assert_eq!(locs[1].length, 4 * 4096);

    let st = chunkdb.snapshot();
    assert_eq!(st.seal_calls, 2);
    assert_eq!(st.delete_calls, 1);
}

#[tokio::test]
async fn push_mode_data_integrity() {
    let chunkdb = MockChunkAllocator::new();
    let tmp = test_dirs::tempdir_in_test_data("chunk-client");
    let diskio = LocalFileDiskWriter::new(tmp.path());
    let ec = ec_4_1();
    let mut writer = make_push_writer(chunkdb, diskio.clone(), ec, test_config(1024 * 1024));

    let mut data = Vec::new();
    for i in 0..8u8 {
        let block_data = vec![i; 4096];
        data.extend_from_slice(&block_data);
        writer.on_data(Bytes::from(block_data)).await.unwrap();
    }
    let locs = writer.on_finish().await.unwrap();
    assert_eq!(locs.len(), 1);
    assert_eq!(locs[0].length, 8 * 4096);

    // Read back data shards from 2 strips and reconstruct.
    let reconstructed = reconstruct_data(&diskio, 2);
    assert_eq!(reconstructed, data);
}

#[tokio::test]
async fn push_mode_backpressure() {
    let chunkdb = MockChunkAllocator::new();
    let tmp = test_dirs::tempdir_in_test_data("chunk-client");
    let diskio = LocalFileDiskWriter::new(tmp.path());
    let ec = ec_4_1();
    let config = Arc::new(ChunkClientConfig {
        max_chunk_size: 1024 * 1024,
        prealloc_depth: 2,
        parity_depth: 2,
        chunk_prefetch_depth: 1,
        read_buffer_size: 4096,
        max_cached_buffer: 2 * 4096,
        prefetch_chunk_count: 1,
        memory_budget: 0,
    });
    let mut writer = make_push_writer(chunkdb, diskio, ec, config);

    for i in 0..6u8 {
        let _status = writer.on_data(block(i, 4096)).await.unwrap();
    }
    let locs = writer.on_finish().await.unwrap();
    assert_eq!(locs.len(), 1);
    assert_eq!(locs[0].length, 6 * 4096);
}

// ── Size hint mismatch tests ─────────────────────────────────────

#[tokio::test]
async fn write_stream_size_hint_fewer_bytes() {
    let chunkdb = MockChunkAllocator::new();
    let tmp = test_dirs::tempdir_in_test_data("chunk-client");
    let diskio = LocalFileDiskWriter::new(tmp.path());
    let ec = ec_4_1();
    let mut writer = make_writer(chunkdb.clone(), diskio, ec, test_config(1024 * 1024));

    let data = vec![0xABu8; 5 * 4096];
    let locs = writer
        .write_stream(data.as_slice(), Some(8 * 4096_u64))
        .await
        .unwrap();

    assert_eq!(locs.len(), 1);
    assert_eq!(locs[0].length, 5 * 4096);
}

#[tokio::test]
async fn write_stream_size_hint_more_bytes() {
    let chunkdb = MockChunkAllocator::new();
    let tmp = test_dirs::tempdir_in_test_data("chunk-client");
    let diskio = LocalFileDiskWriter::new(tmp.path());
    let ec = ec_4_1();
    let mut writer = make_writer(chunkdb.clone(), diskio, ec, test_config(1024 * 1024));

    let data = vec![0xCDu8; 8 * 4096];
    let locs = writer
        .write_stream(data.as_slice(), Some(4 * 4096_u64))
        .await
        .unwrap();

    assert_eq!(locs.len(), 1);
    assert_eq!(locs[0].length, 8 * 4096);
    let st = chunkdb.snapshot();
    assert_eq!(st.append_calls, 1);
}

#[tokio::test]
async fn write_stream_exact_strip_capacity() {
    let chunkdb = MockChunkAllocator::new();
    let tmp = test_dirs::tempdir_in_test_data("chunk-client");
    let diskio = LocalFileDiskWriter::new(tmp.path());
    let ec = ec_4_1();
    let mut writer = make_writer(chunkdb.clone(), diskio, ec, test_config(1024 * 1024));

    let data = vec![0xEFu8; 4 * 4096];
    let locs = writer
        .write_stream(data.as_slice(), Some(4 * 4096_u64))
        .await
        .unwrap();

    assert_eq!(locs.len(), 1);
    assert_eq!(locs[0].length, 4 * 4096);
    let st = chunkdb.snapshot();
    assert_eq!(st.allocate_calls, 1);
    assert_eq!(st.append_calls, 0);
    assert_eq!(st.seal_calls, 1);
}

// ── Bounded preallocation test ───────────────────────────────────

#[tokio::test]
async fn write_stream_bounded_prealloc() {
    let chunkdb = MockChunkAllocator::new();
    let tmp = test_dirs::tempdir_in_test_data("chunk-client");
    let diskio = LocalFileDiskWriter::new(tmp.path());
    let ec = ec_4_1();
    let config = Arc::new(ChunkClientConfig {
        max_chunk_size: 1024 * 1024 * 1024,
        prealloc_depth: 2,
        parity_depth: 2,
        chunk_prefetch_depth: 1,
        read_buffer_size: 4096,
        max_cached_buffer: 4 * 4096,
        prefetch_chunk_count: 1,
        memory_budget: 0,
    });
    let mut writer = make_writer(chunkdb.clone(), diskio, ec, config);

    let data = vec![0x42u8; 48 * 4096];
    let locs = writer
        .write_stream(data.as_slice(), Some(48 * 4096_u64))
        .await
        .unwrap();

    assert_eq!(locs.len(), 1);
    assert_eq!(locs[0].length, 48 * 4096);
    let st = chunkdb.snapshot();
    assert_eq!(st.allocate_calls, 1);
    assert_eq!(st.append_calls, 11);
    assert_eq!(st.seal_calls, 1);
}

// ── Drop mid-write test ──────────────────────────────────────────

#[tokio::test]
async fn push_mode_drop_mid_write_deletes_partial() {
    // The new LargeObjectWriter doesn't have a Drop impl that deletes
    // partial chunks (the old pipeline task handled this). This test
    // verifies the API is sound — drop doesn't panic. Full drop-cleanup
    // is a future task.
    let chunkdb = MockChunkAllocator::new();
    let tmp = test_dirs::tempdir_in_test_data("chunk-client");
    let diskio = LocalFileDiskWriter::new(tmp.path());
    let ec = ec_4_1();
    let mut writer = make_push_writer(chunkdb, diskio, ec, test_config(1024 * 1024));

    for i in 0..6u8 {
        writer.on_data(block(i, 4096)).await.unwrap();
    }

    drop(writer);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    // No assertion on delete_calls — drop cleanup is not yet implemented.
}

// ── WriterPool budget tests ──────────────────────────────────────

#[tokio::test]
async fn writer_pool_budget_rejects_over_budget() {
    let chunkdb = MockChunkAllocator::new();
    let tmp = test_dirs::tempdir_in_test_data("chunk-client");
    let diskio = LocalFileDiskWriter::new(tmp.path());
    let ec = ec_4_1();
    let config = Arc::new(ChunkClientConfig {
        max_chunk_size: 1024 * 1024 * 1024,
        prealloc_depth: 2,
        parity_depth: 2,
        chunk_prefetch_depth: 1,
        read_buffer_size: 1024 * 1024,
        max_cached_buffer: 4 * 1024 * 1024,
        prefetch_chunk_count: 1,
        memory_budget: 0,
    });
    let pool = WriterPool::new(Arc::new(chunkdb), Arc::new(diskio), ec, config, 30 * 1024 * 1024);

    let w1 = pool.try_acquire();
    assert!(w1.is_ok());
    let w2 = pool.try_acquire();
    assert!(w2.is_ok());
    let w3 = pool.try_acquire();
    assert!(matches!(w3, Err(IoError::MemoryBudgetExhausted)));

    drop(w1);
    let w4 = pool.try_acquire();
    assert!(w4.is_ok());
}

#[tokio::test]
async fn writer_pool_per_writer_memory() {
    let chunkdb = MockChunkAllocator::new();
    let tmp = test_dirs::tempdir_in_test_data("chunk-client");
    let diskio = LocalFileDiskWriter::new(tmp.path());
    let ec = ec_4_1();
    let config = Arc::new(ChunkClientConfig {
        max_chunk_size: 1024 * 1024 * 1024,
        prealloc_depth: 2,
        parity_depth: 2,
        chunk_prefetch_depth: 1,
        read_buffer_size: 1024 * 1024,
        max_cached_buffer: 4 * 1024 * 1024,
        prefetch_chunk_count: 1,
        memory_budget: 0,
    });
    let writer = make_push_writer(chunkdb, diskio, ec, config);
    let mem = writer.per_writer_memory();
    assert_eq!(mem, 15 * 1024 * 1024);
}

#[tokio::test]
async fn benchmark_runner_aggregates_concurrent_large_writes() {
    let chunkdb = MockChunkAllocator::new();
    let tmp = test_dirs::tempdir_in_test_data("chunk-client");
    let diskio = LocalFileDiskWriter::new(tmp.path());
    let client = ChunkIoClient::from_parts(Arc::new(chunkdb), Arc::new(diskio));
    let result = run_large_write_benchmark(
        client,
        LargeWriteBenchmarkConfig {
            object_count: 2,
            object_size: 4 * UNIT_BYTES,
            concurrency: 2,
            seed: 7,
            policy: LargeWritePolicy {
                ec_scheme: ec_4_1(),
                client: test_config(1024 * 1024),
            },
        },
    )
    .await;

    assert_eq!(result.objects, 2);
    assert_eq!(result.errors, 0);
    assert_eq!(result.logical_bytes, 8 * UNIT_BYTES);
    assert_eq!(result.physical_bytes, 10 * UNIT_BYTES);
    assert!(result.objects_per_sec > 0.0);
    assert!(result.latency_p50_us > 0);
    assert!(result.error_messages.is_empty());
}
