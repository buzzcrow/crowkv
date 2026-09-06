// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! E2E tests for `LargeObjectWriter` using real kv-server + diskdb +
//! diskio + chunkdb subprocesses.
//!
//! These tests require all four binaries to be built. They are
//! automatically skipped if any binary is missing.

use std::sync::Arc;
use std::time::Duration;

use crowdb_chunk_client::{ChunkClientConfig, DiskWriter, DiskioBlockWriter, LargeAsyncObjectWriter};
use crowdb_chunkdb_client::{ChunkdbClient, ChunkdbRpcTransport, RetryConfig as ChunkdbRetryConfig};
use crowdb_common::ec::EcScheme;
use crowdb_diskio_client::DiskioClient;
use crowdb_protocol::common::DiskId as ProtoDiskId;
use crowdb_rpc_ffi::RpcServer;
use crowdb_test_harness::chunkdb::{self as cdb_harness, ChunkdbProcess};
use crowdb_test_harness::cluster::KvCluster;
use crowdb_test_harness::diskdb::{self as ddb_harness, DiskdbProcess};
use crowdb_test_harness::diskio::{self as dio_harness, DiskioProcess, DiskioStartOpts};
use crowdb_test_harness::hardware::{make_disk_id, seed_hardware, DG_ID, NODE_ID, RACK_ID, UNIT_SIZE_BYTES};

/// 5-disk set for 4+1 EC (5 blocks per strip across 5 disks).
fn standard_disk_ids_5() -> Vec<ProtoDiskId> {
    vec![
        make_disk_id(0, 1),
        make_disk_id(0, 2),
        make_disk_id(0, 3),
        make_disk_id(0, 4),
        make_disk_id(0, 5),
    ]
}

/// Check that all required binaries are available.
fn check_all_binaries() -> bool {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err()
        && crowdb_test_harness::cluster::crowdb_kv_server_bin().is_none()
    {
        eprintln!("skipping: crowdb-kv-server binary not found");
        return false;
    }
    if ddb_harness::crowdb_diskdb_bin().is_none() {
        eprintln!("skipping: crowdb-diskdb binary not found");
        return false;
    }
    if dio_harness::crowdb_diskio_bin().is_none() {
        eprintln!("skipping: crowdb-diskio binary not found");
        return false;
    }
    if cdb_harness::crowdb_chunkdb_bin().is_none() {
        eprintln!("skipping: crowdb-chunkdb binary not found");
        return false;
    }
    true
}

/// EC 4+1: 4 data blocks + 1 parity block per strip.
fn ec_4_1() -> EcScheme {
    EcScheme {
        data_num: 4,
        code_num: 1,
    }
}

/// Set up the full stack: kv cluster + hardware + diskdb + diskio +
/// chunkdb. Returns all the processes + RPC resources + chunkdb client.
struct E2eStack {
    _cluster: KvCluster,
    _diskdb: DiskdbProcess,
    _diskio: DiskioProcess,
    _chunkdb: ChunkdbProcess,
    rpc_server: Arc<RpcServer>,
    conn: crowdb_rpc_ffi::Connection,
    dio_client: Arc<DiskioClient>,
    chunkdb_client: ChunkdbClient,
}

async fn start_e2e_stack() -> E2eStack {
    // 1. Start kv cluster.
    eprintln!("=== starting kv cluster ===");
    let cluster = KvCluster::start().await;
    eprintln!(
        "kv cluster started: group0={}, group1={}",
        cluster.group0_leader_endpoint, cluster.group1_leader_endpoint
    );

    // 2. Seed hardware with 5 disks.
    eprintln!("=== seeding hardware (5 disks) ===");
    let hw = cluster.make_hardware_client();
    let disk_ids = standard_disk_ids_5();
    seed_hardware(&hw, &disk_ids).await;
    eprintln!(
        "hardware seeded: rack={RACK_ID}, node={NODE_ID}, dg={DG_ID}, 5 disks, unit={}KB",
        UNIT_SIZE_BYTES / 1024
    );

    // 3. Start diskdb (block allocator).
    eprintln!("=== starting crowdb-diskdb ===");
    let diskdb = DiskdbProcess::start(&cluster.mgmt_endpoints, false);
    diskdb.wait_for_ready().await;
    eprintln!("crowdb-diskdb ready");

    // 4. Start diskio (block I/O, mem backend).
    eprintln!("=== starting crowdb-diskio (mem) ===");
    let diskio = DiskioProcess::start(&DiskioStartOpts {
        dummy_disk: "mem",
        kv_seeds: &cluster.mgmt_endpoints,
        disks: &[],
        fault_error_rate: 0.0,
        no_o_direct: false,
    });

    let rpc_server = Arc::new(RpcServer::new(None));
    rpc_server.listen("127.0.0.1", 0).expect("listen for rpc client");
    rpc_server.start();
    std::thread::sleep(Duration::from_millis(50));

    let conn = rpc_server
        .connect("127.0.0.1", diskio.port)
        .expect("connect to diskio");
    let dio_client = Arc::new(DiskioClient::new());
    dio_client.attach(&conn);

    diskio.wait_for_disks(&dio_client, &rpc_server, &conn).await;
    eprintln!("crowdb-diskio ready (disks discovered)");

    // 5. Start chunkdb (chunk manager).
    eprintln!("=== starting crowdb-chunkdb ===");
    let chunkdb = ChunkdbProcess::start_with_unsafe_ec(&cluster.mgmt_endpoints, true);
    chunkdb.wait_for_ready().await;
    eprintln!("crowdb-chunkdb ready");

    // 6. Build chunkdb client + refresh endpoints.
    let svc = cluster.make_service_registry_client();
    let transport = std::sync::Arc::new(ChunkdbRpcTransport::new());
    let chunkdb_client = ChunkdbClient::with_retry_config(
        svc,
        ChunkdbRetryConfig {
            max_retries: 5,
            initial_backoff: Duration::from_millis(100),
        },
        transport,
    );
    // Wait for chunkdb to register in the service registry.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if chunkdb_client.refresh_endpoints().await.is_ok() {
            break;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "chunkdb client failed to refresh endpoints within 10s"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    eprintln!("chunkdb client endpoints refreshed");

    // Give the chunkdb server time to discover the diskdb endpoint via
    // the service registry topology refresh (runs every 2s).
    tokio::time::sleep(Duration::from_secs(3)).await;
    eprintln!("chunkdb topology settled");

    E2eStack {
        _cluster: cluster,
        _diskdb: diskdb,
        _diskio: diskio,
        _chunkdb: chunkdb,
        rpc_server,
        conn,
        dio_client,
        chunkdb_client,
    }
}

/// Generate deterministic test data of the given size.
fn make_test_data(size: usize) -> Vec<u8> {
    (0..size)
        .map(|i| u8::try_from((i * 17 + 37) % 256).unwrap())
        .collect()
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn e2e_case1_single_chunk_multi_strip() {
    if !check_all_binaries() {
        return;
    }

    let stack = start_e2e_stack().await;
    let ec = ec_4_1();

    // 12 MB, 1 MB blocks, 4 data blocks per strip → 4 MB per strip.
    // 12 MB / 4 MB = 3 strips. max_chunk_size = 1 GB → all 3 strips
    // in 1 chunk.
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

    let diskio_writer: Arc<dyn DiskWriter> = Arc::new(DiskioBlockWriter::new(
        stack.dio_client.clone(),
        stack.rpc_server.clone(),
        stack.conn.clone(),
    ));

    let mut writer = LargeAsyncObjectWriter::new(Arc::new(stack.chunkdb_client), diskio_writer, ec, config);

    let data = make_test_data(12 * 1024 * 1024);
    eprintln!("=== writing 12 MB ===");
    let locs = writer
        .write_stream(data.as_slice(), Some(12 * 1024 * 1024_u64))
        .await
        .expect("write_stream should succeed");

    // 1 chunk → 1 Location.
    assert_eq!(locs.len(), 1, "Case 1: expected 1 Location (single chunk)");
    assert_eq!(
        locs[0].length,
        12 * 1024 * 1024,
        "Case 1: Location length should be 12 MB"
    );
    eprintln!("Case 1 OK: 1 Location, length = 12 MB");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn e2e_case2_chunk_rotation() {
    if !check_all_binaries() {
        return;
    }

    let stack = start_e2e_stack().await;
    let ec = ec_4_1();

    // 20 MB, 1 MB blocks, 4 data blocks per strip → 4 MB per strip.
    // max_chunk_size = 8 MB → 2 strips per chunk.
    // 20 MB / 4 MB = 5 strips → 3 chunks (2 × 2 strips + 1 × 1 strip).
    let config = Arc::new(ChunkClientConfig {
        max_chunk_size: 8 * 1024 * 1024,
        prealloc_depth: 2,
        parity_depth: 2,
        chunk_prefetch_depth: 1,
        read_buffer_size: 1024 * 1024,
        max_cached_buffer: 4 * 1024 * 1024,
        prefetch_chunk_count: 1,
        memory_budget: 0,
    });

    let diskio_writer: Arc<dyn DiskWriter> = Arc::new(DiskioBlockWriter::new(
        stack.dio_client.clone(),
        stack.rpc_server.clone(),
        stack.conn.clone(),
    ));

    let mut writer = LargeAsyncObjectWriter::new(Arc::new(stack.chunkdb_client), diskio_writer, ec, config);

    let data = make_test_data(20 * 1024 * 1024);
    eprintln!("=== writing 20 MB ===");
    let locs = writer
        .write_stream(data.as_slice(), Some(20 * 1024 * 1024_u64))
        .await
        .expect("write_stream should succeed");

    // 5 strips / 2 strips per chunk = 2.5 → 3 chunks → 3 Locations.
    assert_eq!(locs.len(), 3, "Case 2: expected 3 Locations (rotation)");
    let total: u64 = locs.iter().map(|l| l.length).sum();
    assert_eq!(total, 20 * 1024 * 1024, "Case 2: total length should be 20 MB");

    // Chunks 1-2 = 8 MB each (2 strips), chunk 3 = 4 MB (1 strip).
    for (i, loc) in locs.iter().enumerate() {
        if i < 2 {
            assert_eq!(
                loc.length,
                8 * 1024 * 1024,
                "Case 2: chunk {i} should be 8 MB (2 strips)"
            );
        } else {
            assert_eq!(
                loc.length,
                4 * 1024 * 1024,
                "Case 2: chunk 2 (last) should be 4 MB (1 strip)"
            );
        }
    }
    eprintln!("Case 2 OK: 3 Locations, total = 20 MB");
}
