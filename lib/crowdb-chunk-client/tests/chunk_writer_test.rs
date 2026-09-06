// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Unit tests for `ChunkWriter` drive loop: strip rotation, on-demand
//! append, `is_full` + seal, abort, empty seal.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::similar_names
)]

mod common;

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crowdb_test_harness::test_dirs;

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_chunk_client::{ChunkAllocator, ChunkClientConfig, ChunkWriter, DiskWriter, Result};
use crowdb_common::ec::EcScheme;
use crowdb_diskio_client::DiskId;
use crowdb_protocol::chunkdb::rpc::Strip as StripOneof;
use crowdb_protocol::chunkdb::rpc::{
    AllocateChunkRequest, AllocateChunkResponse, AppendChunkRequest, AppendChunkResponse, Chunk, ChunkStrip,
    ChunkType, DeleteChunkRequest, DeleteChunkResponse, EcStrip, QueryChunkRequest, QueryChunkResponse,
    SealChunkRequest, SealChunkResponse, StripType, UpdateChunkStripRequest, UpdateChunkStripResponse,
};
use crowdb_protocol::common::{ChunkId, DiskId as ProtoDiskId};
use crowdb_protocol::diskdb::rpc::Segment;

use common::LocalFileDiskWriter;

const UNIT_BYTES: u64 = 4096;
const DATA_NUM: usize = 4;

#[derive(Debug, Default)]
struct OrderingDiskWriter {
    events: Mutex<Vec<String>>,
    parity_inflight: AtomicUsize,
    parity_max: AtomicUsize,
}

impl OrderingDiskWriter {
    fn events(&self) -> Vec<String> {
        self.events.lock().unwrap().clone()
    }

    fn parity_max(&self) -> usize {
        self.parity_max.load(Ordering::Relaxed)
    }

    fn parity_inflight(&self) -> usize {
        self.parity_inflight.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl DiskWriter for OrderingDiskWriter {
    async fn write(&self, seg: &Segment, _unit_bytes: u64, _data: Bytes) -> Result<()> {
        let disk_id = seg.disk_id.unwrap_or_default();
        if disk_id.high == 1004 {
            let inflight = self.parity_inflight.fetch_add(1, Ordering::Relaxed) + 1;
            self.parity_max.fetch_max(inflight, Ordering::Relaxed);
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            self.parity_inflight.fetch_sub(1, Ordering::Relaxed);
        }
        self.events
            .lock()
            .unwrap()
            .push(format!("write:{}", disk_id.high));
        Ok(())
    }

    async fn fsync(&self, disk_id: DiskId) -> Result<()> {
        self.events
            .lock()
            .unwrap()
            .push(format!("fsync:{}", disk_id.high));
        Ok(())
    }
}

// ── Mock ChunkAllocator (cumulative chunks) ──────────────────────

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

fn make_segments(chunk_id: ChunkId, count: usize, offset: &mut u64) -> Vec<Segment> {
    let mut segments = Vec::with_capacity(count);
    for i in 0..count {
        segments.push(Segment {
            disk_id: Some(ProtoDiskId {
                high: 1000 + i as u64,
                low: i as u64,
            }),
            zone_index: 0,
            unit_offset: *offset,
            unit_count: 1,
            owner_chunk: Some(chunk_id),
            allocation_ts: *offset + 1,
        });
        *offset += 1;
    }
    segments
}

fn make_strip(strip_seq: u32, data_num: u32, code_num: u32, segments: Vec<Segment>) -> ChunkStrip {
    ChunkStrip {
        chunk_offset: strip_seq,
        strip_sequence: strip_seq,
        unit_kb: 4,
        capacity: data_num,
        create_ts_ms: 0,
        sealed_ts_ms: 0,
        sealed_length: 0,
        strip_type: StripType::Ec as i32,
        strip: Some(StripOneof::EcStrip(EcStrip {
            data_num,
            code_num,
            ec_state: 0,
            segments,
        })),
        usage_bitmap: Vec::new(),
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

        let segments = make_segments(chunk_id, total, &mut st.next_segment_offset);
        let strip = make_strip(0, req.data_num, req.code_num, segments);
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

        let segments = make_segments(chunk_id, total, &mut st.next_segment_offset);
        let strip = make_strip(strip_seq, req.data_num, req.code_num, segments);

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

fn block(value: u8, size: usize) -> Bytes {
    Bytes::from(vec![value; size])
}

fn make_writer(
    chunkdb: MockChunkAllocator,
    diskio: Arc<LocalFileDiskWriter>,
    ec: EcScheme,
    config: Arc<ChunkClientConfig>,
) -> ChunkWriter {
    ChunkWriter::new(Arc::new(chunkdb), diskio, ec, config)
}

// ── Tests ────────────────────────────────────────────────────────

#[tokio::test]
async fn chunk_writer_strip_rotation() {
    let chunkdb = MockChunkAllocator::new();
    let tmp = test_dirs::tempdir_in_test_data("chunk-client");
    let diskio = Arc::new(LocalFileDiskWriter::new(tmp.path()));
    let ec = ec_4_1();
    let config = test_config(1024 * 1024 * 1024);
    let mut cw = make_writer(chunkdb.clone(), diskio, ec, config);

    // Allocate a chunk with 1 strip (mock allocator).
    let chunk = {
        let pf = crowdb_chunk_client::ChunkPrefetch::new(
            Arc::new(chunkdb.clone()),
            ec,
            test_config(1024 * 1024 * 1024),
            crowdb_protocol::chunk_id::CHUNK_TYPE_REPO,
        );
        pf.on_demand().await.unwrap()
    };
    cw.open(chunk, None).unwrap();

    // Push data_num * 3 blocks (3 strips).
    for i in 0..(DATA_NUM * 3) as u8 {
        cw.push(block(i, UNIT_BYTES as usize)).await.unwrap();
    }

    // Seal to finish the last strip (push auto-rotates but doesn't
    // finish the final strip — seal does that).
    let location = cw.seal().await.unwrap();

    // Verify 3 strips written, bytes correct.
    assert_eq!(location.length, DATA_NUM as u64 * 3 * UNIT_BYTES);
    assert_eq!(cw.strips_in_chunk(), 3);
}

#[tokio::test]
async fn chunk_writer_on_demand_append() {
    let chunkdb = MockChunkAllocator::new();
    let tmp = test_dirs::tempdir_in_test_data("chunk-client");
    let diskio = Arc::new(LocalFileDiskWriter::new(tmp.path()));
    let ec = ec_4_1();
    let config = test_config(1024 * 1024 * 1024);
    let mut cw = make_writer(chunkdb.clone(), diskio, ec, config);

    let chunk = {
        let pf = crowdb_chunk_client::ChunkPrefetch::new(
            Arc::new(chunkdb.clone()),
            ec,
            test_config(1024 * 1024 * 1024),
            crowdb_protocol::chunk_id::CHUNK_TYPE_REPO,
        );
        pf.on_demand().await.unwrap()
    };
    cw.open(chunk, None).unwrap();

    // Push data_num * 2 blocks (2 strips — second strip needs append).
    for i in 0..(DATA_NUM * 2) as u8 {
        cw.push(block(i, UNIT_BYTES as usize)).await.unwrap();
    }

    // Verify append_chunk was called (at least once — prefetch may
    // have appended more).
    let st = chunkdb.snapshot();
    assert!(st.append_calls >= 1, "append_calls = {}", st.append_calls);
}

#[tokio::test]
async fn chunk_writer_is_full_and_seal() {
    let chunkdb = MockChunkAllocator::new();
    let tmp = test_dirs::tempdir_in_test_data("chunk-client");
    let diskio = Arc::new(LocalFileDiskWriter::new(tmp.path()));
    let ec = ec_4_1();
    // max_chunk_size = 2 strips = 2 * 4 * 4096 = 32768 bytes.
    let config = test_config(DATA_NUM as u64 * UNIT_BYTES * 2);
    let mut cw = make_writer(chunkdb.clone(), diskio, ec, config);

    let chunk = {
        let pf = crowdb_chunk_client::ChunkPrefetch::new(
            Arc::new(chunkdb.clone()),
            ec,
            test_config(DATA_NUM as u64 * UNIT_BYTES * 2),
            crowdb_protocol::chunk_id::CHUNK_TYPE_REPO,
        );
        pf.on_demand().await.unwrap()
    };
    cw.open(chunk, None).unwrap();

    // Push data_num * 2 blocks (2 strips = max_chunk_size).
    for i in 0..(DATA_NUM * 2) as u8 {
        cw.push(block(i, UNIT_BYTES as usize)).await.unwrap();
    }

    // After 2 strips, the chunk should be full.
    // (push auto-rotates + finishes strips; is_full is checked
    // internally.)
    let location = cw.seal().await.unwrap();
    assert_eq!(location.length, DATA_NUM as u64 * 2 * UNIT_BYTES);
    assert!(location.chunk_id.is_some());

    let st = chunkdb.snapshot();
    assert_eq!(st.seal_calls, 1);
}

#[tokio::test]
async fn chunk_writer_abort() {
    let chunkdb = MockChunkAllocator::new();
    let tmp = test_dirs::tempdir_in_test_data("chunk-client");
    let diskio = Arc::new(LocalFileDiskWriter::new(tmp.path()));
    let ec = ec_4_1();
    let config = test_config(1024 * 1024 * 1024);
    let mut cw = make_writer(chunkdb.clone(), diskio, ec, config);

    let chunk = {
        let pf = crowdb_chunk_client::ChunkPrefetch::new(
            Arc::new(chunkdb.clone()),
            ec,
            test_config(1024 * 1024 * 1024),
            crowdb_protocol::chunk_id::CHUNK_TYPE_REPO,
        );
        pf.on_demand().await.unwrap()
    };
    cw.open(chunk, None).unwrap();

    // Push partial data (2 blocks = partial strip).
    for i in 0..2u8 {
        cw.push(block(i, UNIT_BYTES as usize)).await.unwrap();
    }

    cw.abort().await.unwrap();

    let st = chunkdb.snapshot();
    assert_eq!(st.delete_calls, 1);
}

#[tokio::test]
async fn chunk_writer_empty_seal() {
    let chunkdb = MockChunkAllocator::new();
    let tmp = test_dirs::tempdir_in_test_data("chunk-client");
    let diskio = Arc::new(LocalFileDiskWriter::new(tmp.path()));
    let ec = ec_4_1();
    let config = test_config(1024 * 1024 * 1024);
    let mut cw = make_writer(chunkdb.clone(), diskio, ec, config);

    let chunk = {
        let pf = crowdb_chunk_client::ChunkPrefetch::new(
            Arc::new(chunkdb.clone()),
            ec,
            test_config(1024 * 1024 * 1024),
            crowdb_protocol::chunk_id::CHUNK_TYPE_REPO,
        );
        pf.on_demand().await.unwrap()
    };
    cw.open(chunk, None).unwrap();

    // Immediately seal — no data pushed.
    let location = cw.seal().await.unwrap();
    assert_eq!(location.length, 0);

    let st = chunkdb.snapshot();
    assert_eq!(st.seal_calls, 0);
    assert_eq!(st.delete_calls, 1);
}

#[tokio::test]
async fn chunk_writer_waits_for_parity_before_fsync() {
    let chunkdb = MockChunkAllocator::new();
    let diskio = Arc::new(OrderingDiskWriter::default());
    let ec = ec_4_1();
    let config = test_config(1024 * 1024 * 1024);
    let mut cw = ChunkWriter::new(Arc::new(chunkdb.clone()), diskio.clone(), ec, config);

    let chunk = {
        let pf = crowdb_chunk_client::ChunkPrefetch::new(
            Arc::new(chunkdb),
            ec,
            test_config(1024 * 1024 * 1024),
            crowdb_protocol::chunk_id::CHUNK_TYPE_REPO,
        );
        pf.on_demand().await.unwrap()
    };
    cw.open(chunk, None).unwrap();
    for i in 0..DATA_NUM as u8 {
        cw.push(block(i, UNIT_BYTES as usize)).await.unwrap();
    }
    cw.seal().await.unwrap();

    let events = diskio.events();
    let parity = events.iter().position(|event| event == "write:1004").unwrap();
    let first_fsync = events
        .iter()
        .position(|event| event.starts_with("fsync:"))
        .unwrap();
    assert!(parity < first_fsync, "events = {events:?}");
}

#[tokio::test]
async fn chunk_writer_bounds_cross_strip_parity_tasks() {
    let chunkdb = MockChunkAllocator::new();
    let diskio = Arc::new(OrderingDiskWriter::default());
    let ec = ec_4_1();
    let mut config = (*test_config(1024 * 1024 * 1024)).clone();
    config.parity_depth = 1;
    let mut cw = ChunkWriter::new(Arc::new(chunkdb.clone()), diskio.clone(), ec, Arc::new(config));
    let pf = crowdb_chunk_client::ChunkPrefetch::new(
        Arc::new(chunkdb),
        ec,
        test_config(1024 * 1024 * 1024),
        crowdb_protocol::chunk_id::CHUNK_TYPE_REPO,
    );
    cw.open(pf.on_demand().await.unwrap(), None).unwrap();

    for i in 0..(DATA_NUM * 3) as u8 {
        cw.push(block(i, UNIT_BYTES as usize)).await.unwrap();
    }
    cw.seal().await.unwrap();

    assert_eq!(diskio.parity_max(), 1);
}

#[tokio::test]
async fn chunk_writer_abort_drains_submitted_parity_io() {
    let chunkdb = MockChunkAllocator::new();
    let diskio = Arc::new(OrderingDiskWriter::default());
    let ec = ec_4_1();
    let mut cw = ChunkWriter::new(
        Arc::new(chunkdb.clone()),
        diskio.clone(),
        ec,
        test_config(1024 * 1024 * 1024),
    );
    let pf = crowdb_chunk_client::ChunkPrefetch::new(
        Arc::new(chunkdb.clone()),
        ec,
        test_config(1024 * 1024 * 1024),
        crowdb_protocol::chunk_id::CHUNK_TYPE_REPO,
    );
    cw.open(pf.on_demand().await.unwrap(), None).unwrap();
    for i in 0..=DATA_NUM as u8 {
        cw.push(block(i, UNIT_BYTES as usize)).await.unwrap();
    }

    cw.abort().await.unwrap();

    assert_eq!(diskio.parity_inflight(), 0);
    assert!(diskio.events().iter().any(|event| event == "write:1004"));
    assert_eq!(chunkdb.snapshot().delete_calls, 1);
}
