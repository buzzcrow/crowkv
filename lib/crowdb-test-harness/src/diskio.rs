// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Diskio test harness: subprocess management, IO helpers, and
//! binary discovery for disk-io E2E tests.

use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crowdb_diskio_client::{DiskId as DioDiskId, DiskIoRetCode, DiskioClient, DiskioError};
use crowdb_rpc_ffi::RpcServer;

use crate::hardware::{DG_ID, INSTANCE_ID, NODE_ID, RACK_ID};

// Re-export hardware helpers for convenience.
pub use crate::cluster::crowdb_kv_server_bin;
pub use crate::hardware::{seed_hardware, standard_disk_ids_4};

// ── NullDisk pattern generator (mirrors C++ DummyDiskEngine::fill_pattern) ──

fn xorshift64(mut state: u64) -> u64 {
    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    state
}

fn hash_seed(disk_id: DioDiskId) -> u64 {
    disk_id.high ^ disk_id.low.wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

/// Compute the deterministic pattern `NullDisk` returns for a read at
/// (`disk_id`, `phys_offset`, size). Mirrors the C++ `fill_pattern`.
pub fn null_disk_pattern(disk_id: DioDiskId, phys_offset: u64, size: usize) -> Vec<u8> {
    let mut state = hash_seed(disk_id);
    let skip = phys_offset / 8;
    for _ in 0..skip {
        state = xorshift64(state);
    }
    let mut buf = vec![0u8; size];
    let mut pos = 0;
    while pos < size {
        state = xorshift64(state);
        let val_bytes = state.to_le_bytes();
        let n = std::cmp::min(8, size - pos);
        buf[pos..pos + n].copy_from_slice(&val_bytes[..n]);
        pos += n;
    }
    buf
}

// ── binary discovery ─────────────────────────────────────────────

pub fn crowdb_diskio_bin() -> Option<std::path::PathBuf> {
    if let Ok(p) = std::env::var("CROWDB_DISKIO_BIN") {
        let path = std::path::PathBuf::from(p);
        if path.exists() {
            return Some(path);
        }
    }
    let candidates = [
        "../../app/crowdb-diskio/build/crowdb-diskio",
        "../../../app/crowdb-diskio/build/crowdb-diskio",
        "../../../../app/crowdb-diskio/build/crowdb-diskio",
    ];
    for c in &candidates {
        let p = std::path::PathBuf::from(c);
        if p.exists() {
            return p.canonicalize().ok();
        }
    }
    None
}

pub fn crowdb_lib_dir() -> Option<std::path::PathBuf> {
    let candidates = [
        "../../target/debug",
        "../../target/release",
        "../../../target/debug",
        "../../../target/release",
        "../../../../target/debug",
        "../../../../target/release",
    ];
    for c in &candidates {
        let p = std::path::PathBuf::from(c);
        if p.join("libcrowdb_kv_client.so").exists() {
            return p.canonicalize().ok();
        }
    }
    None
}

// ── diskio subprocess ────────────────────────────────────────────

/// Options for starting a diskio subprocess.
pub struct DiskioStartOpts<'a> {
    /// Dummy disk type ("null" or "mem"). Ignored if `disks` is non-empty.
    pub dummy_disk: &'a str,
    /// KV-server management seeds for group-0 sync. Empty = no sync.
    pub kv_seeds: &'a [String],
    /// Explicit disk list (`--disk` args). Empty = use auto-discover.
    pub disks: &'a [DiskArg],
    /// Fault error rate (0.0 = none). Injects `--fault-error-rate`.
    pub fault_error_rate: f64,
    /// Disable `O_DIRECT` for `BlockDisk`.
    pub no_o_direct: bool,
}

/// A `--disk` argument: hex id + path + zone capacity bytes.
#[derive(Clone)]
pub struct DiskArg {
    pub id_high: u64,
    pub id_low: u64,
    pub path: String,
    pub zone_capacity: i64,
}

/// Monotonic counter for unique log file names when multiple diskio
/// processes start in parallel (tests run concurrently by default).
static DISKIO_INSTANCE_COUNTER: AtomicU64 = AtomicU64::new(0);

pub struct DiskioProcess {
    pub child: std::process::Child,
    pub port: i32,
    pub log_path: std::path::PathBuf,
}

impl DiskioProcess {
    pub fn log_content(&self) -> String {
        std::fs::read_to_string(&self.log_path).unwrap_or_default()
    }

    /// Start crowdb-diskio with the given options.
    pub fn start(opts: &DiskioStartOpts<'_>) -> Self {
        let bin = crowdb_diskio_bin().unwrap_or_else(|| {
            panic!("crowdb-diskio binary not found; set CROWDB_DISKIO_BIN or build app/crowdb-diskio")
        });
        let lib_dir = crowdb_lib_dir().unwrap_or_else(|| {
            panic!(
                "libcrowdb_kv_client.so not found; build with cargo build -p crowdb-kv-client --features ffi"
            )
        });

        let inst = DISKIO_INSTANCE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let log_path = crate::test_dirs::test_log_dir().join(format!(
            "crowdb-diskio-e2e-{}-{}-{}.log",
            opts.dummy_disk,
            std::process::id(),
            inst,
        ));
        let log_file = std::fs::File::create(&log_path).expect("create log file");
        let log_file2 = log_file.try_clone().expect("clone log file");

        let mut cmd = Command::new(&bin);
        cmd.args([
            "--port",
            "0",
            "--bind",
            "127.0.0.1",
            "--dummy-disk",
            opts.dummy_disk,
        ])
        .env("LD_LIBRARY_PATH", lib_dir.to_str().unwrap())
        .stdout(Stdio::from(log_file))
        .stderr(Stdio::from(log_file2));

        if opts.no_o_direct {
            cmd.arg("--no-o-direct");
        }

        if opts.fault_error_rate > 0.0 {
            cmd.args(["--fault-error-rate", &opts.fault_error_rate.to_string()]);
        }

        if !opts.disks.is_empty() {
            for d in opts.disks {
                assert_eq!(
                    d.id_high, 0,
                    "--disk arg only supports id_high=0 (single hex value)"
                );
                let id_str = format!("{:x}", d.id_low);
                let disk_arg = format!("{}:{}:{}", id_str, d.path, d.zone_capacity);
                cmd.args(["--disk", &disk_arg]);
            }
        } else if !opts.kv_seeds.is_empty() {
            let seeds_arg = opts.kv_seeds.join(",");
            cmd.args([
                "--kv-seeds",
                &seeds_arg,
                "--instance-id",
                &INSTANCE_ID.to_string(),
                "--rack-id",
                &RACK_ID.to_string(),
                "--node-id",
                &NODE_ID.to_string(),
                "--dg-id",
                &DG_ID.to_string(),
                "--sync-interval-ms",
                "200",
                "--auto-discover-disks",
            ]);
        }

        let mut child = cmd.spawn().expect("start crowdb-diskio");
        eprintln!("crowdb-diskio ({}) log: {}", opts.dummy_disk, log_path.display());

        let port = {
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            let mut port = None;
            while std::time::Instant::now() < deadline && port.is_none() {
                std::thread::sleep(Duration::from_millis(50));
                if let Ok(content) = std::fs::read_to_string(&log_path) {
                    for line in content.lines() {
                        if let Some(idx) = line.find("listening on ") {
                            let after = &line[idx + "listening on ".len()..];
                            if let Some(colon) = after.find(':') {
                                let rest = &after[colon + 1..];
                                let port_str: String =
                                    rest.chars().take_while(char::is_ascii_digit).collect();
                                if let Ok(p) = port_str.parse::<i32>() {
                                    port = Some(p);
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            port.unwrap_or_else(|| {
                let _ = child.kill();
                let log = std::fs::read_to_string(&log_path).unwrap_or_default();
                panic!("crowdb-diskio did not start. Log:\n{log}");
            })
        };

        eprintln!("crowdb-diskio ({}) started on port {port}", opts.dummy_disk);
        Self {
            child,
            port,
            log_path,
        }
    }

    /// Wait for diskio to discover disks by retrying a write.
    pub async fn wait_for_disks(
        &self,
        dio_client: &DiskioClient,
        server: &RpcServer,
        conn: &crowdb_rpc_ffi::Connection,
    ) {
        let test_disk = DioDiskId::new(0, 1);
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        loop {
            let write_result = dio_client.write(server, conn, test_disk, 0, 0, vec![0xAB; 4096]);
            match write_result {
                Ok(fut) => match DiskioClient::await_write_response(fut).await {
                    Ok(_) => {
                        eprintln!("diskio disks ready");
                        return;
                    }
                    Err(DiskioError::IoError(code)) => {
                        if code == DiskIoRetCode::DiskNotExist {
                            // Disks not yet discovered — keep waiting.
                        } else {
                            eprintln!("diskio disks ready (io error: {code:?})");
                            return;
                        }
                    }
                    Err(e) => eprintln!("diskio write attempt error: {e:?}"),
                },
                Err(e) => eprintln!("diskio write send error: {e:?}"),
            }
            if std::time::Instant::now() > deadline {
                let log = self.log_content();
                panic!("diskio did not discover disks within 15s. Log:\n{log}");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

impl Drop for DiskioProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

// ── IO test helper ───────────────────────────────────────────────

/// Which backend to test.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    Mem,
    Null,
}

impl std::fmt::Display for Backend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

impl Backend {
    pub fn cli_arg(self) -> &'static str {
        match self {
            Self::Mem => "mem",
            Self::Null => "null",
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Mem => "MemDisk",
            Self::Null => "NullDisk",
        }
    }
}

/// Parameters for `test_io_round`, grouped to avoid too-many-arguments.
pub struct IoRoundParams {
    pub backend: Backend,
    pub disk_id: DioDiskId,
    pub zone_index: u32,
    pub zone_offset: u64,
    pub size: usize,
    pub label: &'static str,
}

/// Run a write+read+verify + overwrite cycle at the given size.
///
/// For `MemDisk`: read after write returns the same bytes that were
/// written; read after overwrite returns the second write's bytes.
///
/// For `NullDisk`: read always returns the deterministic pattern
/// regardless of what was written.
#[allow(clippy::too_many_lines)]
pub async fn test_io_round(
    dio_client: &DiskioClient,
    server: &RpcServer,
    conn: &crowdb_rpc_ffi::Connection,
    params: &IoRoundParams,
) {
    let IoRoundParams {
        backend,
        disk_id,
        zone_index,
        zone_offset,
        size,
        label,
    } = *params;
    let zone_size_units = crate::hardware::ZONE_SIZE_UNITS;
    let unit_size_bytes = crate::hardware::UNIT_SIZE_BYTES;
    let phys_offset = u64::from(zone_index) * (zone_size_units * u64::from(unit_size_bytes)) + zone_offset;
    let read_size = u32::try_from(size).unwrap();

    // ── Write + read + verify ──
    let write_data: Vec<u8> = (0..size)
        .map(|i| u8::try_from((i * 7 + 13) % 256).unwrap())
        .collect();
    let write_fut = dio_client
        .write(server, conn, disk_id, zone_index, zone_offset, write_data.clone())
        .expect("write send");
    let write_code = DiskioClient::await_write_response(write_fut)
        .await
        .expect("write IO");
    assert_eq!(
        write_code,
        DiskIoRetCode::Success,
        "{label}: {backend} write should succeed"
    );

    let read_fut = dio_client
        .read(
            server,
            conn,
            disk_id,
            zone_index,
            zone_offset,
            read_size,
            phys_offset,
        )
        .expect("read send");
    let (read_code, read_data) = DiskioClient::await_read_response(read_fut)
        .await
        .expect("read IO");
    assert_eq!(
        read_code,
        DiskIoRetCode::Success,
        "{label}: {backend} read should succeed"
    );
    let read_data = read_data.unwrap_or_default();
    assert_eq!(read_data.len(), size, "{label}: {backend} read length mismatch");

    if size > 0 {
        match backend {
            Backend::Mem => {
                assert_eq!(
                    read_data, write_data,
                    "{label}: {backend} read data must match written data"
                );
            }
            Backend::Null => {
                let expected = null_disk_pattern(disk_id, phys_offset, size);
                assert_eq!(
                    read_data, expected,
                    "{label}: {backend} read data must match computed pattern"
                );
            }
        }
    }

    // ── Overwrite: write different data, read back, verify ──
    if size > 0 {
        let overwrite_data: Vec<u8> = (0..size)
            .map(|i| u8::try_from((i * 3 + 99) % 256).unwrap())
            .collect();
        let ow_fut = dio_client
            .write(
                server,
                conn,
                disk_id,
                zone_index,
                zone_offset,
                overwrite_data.clone(),
            )
            .expect("overwrite send");
        let ow_code = DiskioClient::await_write_response(ow_fut)
            .await
            .expect("overwrite IO");
        assert_eq!(
            ow_code,
            DiskIoRetCode::Success,
            "{label}: {backend} overwrite should succeed"
        );

        let ow_read_fut = dio_client
            .read(
                server,
                conn,
                disk_id,
                zone_index,
                zone_offset,
                read_size,
                phys_offset,
            )
            .expect("overwrite read send");
        let (ow_read_code, ow_read_data) = DiskioClient::await_read_response(ow_read_fut)
            .await
            .expect("overwrite read IO");
        assert_eq!(
            ow_read_code,
            DiskIoRetCode::Success,
            "{label}: {backend} overwrite read should succeed"
        );
        let ow_read_data = ow_read_data.unwrap_or_default();
        assert_eq!(
            ow_read_data.len(),
            size,
            "{label}: {backend} overwrite read length mismatch"
        );
        match backend {
            Backend::Mem => {
                assert_eq!(
                    ow_read_data, overwrite_data,
                    "{label}: {backend} overwrite read must match second write"
                );
            }
            Backend::Null => {
                let expected = null_disk_pattern(disk_id, phys_offset, size);
                assert_eq!(
                    ow_read_data, expected,
                    "{label}: {backend} overwrite read should still match pattern"
                );
            }
        }
    }

    // ── Fsync ──
    let fsync_fut = dio_client.fsync(server, conn, disk_id).expect("fsync send");
    let fsync_code = DiskioClient::await_fsync_response(fsync_fut)
        .await
        .expect("fsync IO");
    assert_eq!(
        fsync_code,
        DiskIoRetCode::Success,
        "{label}: {backend} fsync should succeed"
    );

    eprintln!(
        "  {label}: {backend} write+read+overwrite+fsync OK ({size} bytes, zone {zone_index} offset {zone_offset})"
    );
}

/// Test read-before-write: reading from an uninitialized area of the
/// disk. Uses a fresh disk (not written to by `wait_for_disks`) and a
/// zone-0 offset within the memfd's initial ftruncate size.
///
/// For `MemDisk`: the memfd is zero-filled, so reads return zeros.
/// For `NullDisk`: reads return the deterministic pattern.
pub async fn test_read_before_write(
    dio_client: &DiskioClient,
    server: &RpcServer,
    conn: &crowdb_rpc_ffi::Connection,
    backend: Backend,
    disk_id: DioDiskId,
) {
    let zone_index = 0u32;
    let zone_offset = 1024 * 1024u64; // 1 MB
    let size = 4096usize;
    let zone_size_units = crate::hardware::ZONE_SIZE_UNITS;
    let unit_size_bytes = crate::hardware::UNIT_SIZE_BYTES;
    let phys_offset = u64::from(zone_index) * (zone_size_units * u64::from(unit_size_bytes)) + zone_offset;
    let read_size = u32::try_from(size).unwrap();

    let rf = dio_client
        .read(
            server,
            conn,
            disk_id,
            zone_index,
            zone_offset,
            read_size,
            phys_offset,
        )
        .expect("read-before-write send");
    let (code, data) = DiskioClient::await_read_response(rf)
        .await
        .expect("read-before-write IO");
    assert_eq!(
        code,
        DiskIoRetCode::Success,
        "{backend}: read-before-write should succeed"
    );
    let data = data.expect("read-before-write data should be present");
    assert_eq!(data.len(), size, "{backend}: read-before-write length mismatch");
    match backend {
        Backend::Mem => {
            assert!(
                data.iter().all(|&b| b == 0),
                "{backend}: read-before-write should return zeros (uninitialized memfd)"
            );
        }
        Backend::Null => {
            let expected = null_disk_pattern(disk_id, phys_offset, size);
            assert_eq!(
                data, expected,
                "{backend}: read-before-write should match pattern"
            );
        }
    }
    eprintln!("  {backend} read-before-write OK (4 KB at zone 0 offset 1 MB, uninitialized)");
}

// ── concurrent benchmark ─────────────────────────────────────────

const BENCH_THREADS: usize = 4;
const BENCH_CYCLES: usize = 25;
const BENCH_SIZE: usize = 4096;

/// Run a concurrent write/read benchmark: `BENCH_THREADS` tasks each
/// doing `BENCH_CYCLES` write+read cycles on different offsets of the
/// same disk. Reports throughput and verifies read data content under
/// concurrent load.
#[allow(clippy::cast_precision_loss)]
pub async fn run_concurrent_benchmark(
    dio_client: &Arc<DiskioClient>,
    server: &Arc<RpcServer>,
    conn: &crowdb_rpc_ffi::Connection,
    backend: Backend,
    disk_id: DioDiskId,
) {
    let start = Instant::now();
    let mut handles = Vec::with_capacity(BENCH_THREADS);

    for tid in 0..BENCH_THREADS {
        let client = Arc::clone(dio_client);
        let server = Arc::clone(server);
        let conn = conn.clone();
        handles.push(tokio::spawn(async move {
            let mut ok = 0usize;
            let mut errors = 0usize;
            for i in 0..BENCH_CYCLES {
                let offset = u64::try_from((tid * BENCH_CYCLES + i) * BENCH_SIZE).unwrap();
                let data = vec![u8::try_from((tid + i) % 256).unwrap(); BENCH_SIZE];

                let Ok(wf) = client.write(&server, &conn, disk_id, 0, offset, data.clone()) else {
                    errors += 1;
                    continue;
                };
                let Ok(wc) = DiskioClient::await_write_response(wf).await else {
                    errors += 1;
                    continue;
                };
                if wc != DiskIoRetCode::Success {
                    errors += 1;
                    continue;
                }

                let Ok(rf) = client.read(
                    &server,
                    &conn,
                    disk_id,
                    0,
                    offset,
                    u32::try_from(BENCH_SIZE).unwrap(),
                    offset,
                ) else {
                    errors += 1;
                    continue;
                };
                let Ok((rc, rd)) = DiskioClient::await_read_response(rf).await else {
                    errors += 1;
                    continue;
                };
                if rc != DiskIoRetCode::Success {
                    errors += 1;
                    continue;
                }
                let Some(rd) = rd else {
                    errors += 1;
                    continue;
                };
                if rd.len() != BENCH_SIZE {
                    errors += 1;
                    continue;
                }
                let phys_offset = offset;
                let content_ok = match backend {
                    Backend::Mem => rd == data,
                    Backend::Null => rd == null_disk_pattern(disk_id, phys_offset, BENCH_SIZE),
                };
                if !content_ok {
                    errors += 1;
                    continue;
                }
                ok += 1;
            }
            (ok, errors)
        }));
    }

    let mut total_ok = 0usize;
    let mut total_err = 0usize;
    for h in handles {
        let (ok, err) = h.await.expect("benchmark task panicked");
        total_ok += ok;
        total_err += err;
    }

    let elapsed = start.elapsed();
    let total_ops = total_ok + total_err;
    let secs = elapsed.as_secs_f64();
    let ops_per_sec = if secs > 0.0 { total_ops as f64 / secs } else { 0.0 };
    let data_bytes = u64::try_from(total_ok * BENCH_SIZE * 2).unwrap_or(u64::MAX);
    let data_mb = data_bytes as f64 / (1024.0 * 1024.0);
    let mb_per_sec = if secs > 0.0 { data_mb / secs } else { 0.0 };

    eprintln!(
        "  {backend} benchmark: {BENCH_THREADS} threads × {BENCH_CYCLES} cycles, {BENCH_SIZE}B — {total_ops} ops in {elapsed:.2?} ({ops_per_sec:.0} ops/s, {mb_per_sec:.1} MB/s, {total_ok} ok, {total_err} errors)"
    );
    assert_eq!(total_err, 0, "{backend}: benchmark should have 0 errors");
    assert_eq!(
        total_ok,
        BENCH_THREADS * BENCH_CYCLES,
        "{backend}: all ops should succeed"
    );
}

// IO sizes to test: zero, small, middle, large, max (2 MB).
pub const IO_SIZES: &[(usize, &str)] = &[
    (0, "zero"),
    (100, "small"),
    (4096, "middle"),
    (1024 * 1024, "large"),   // 1 MB
    (2 * 1024 * 1024, "max"), // 2 MB (default max block size)
];

// ── helper: connect to a diskio process ──────────────────────────

/// Connect a `DiskioClient` to a running `DiskioProcess`. Returns
/// (`rpc_server`, `connection`, `dio_client`) ready for I/O.
pub fn connect_to_diskio(
    diskio: &DiskioProcess,
) -> (Arc<RpcServer>, crowdb_rpc_ffi::Connection, Arc<DiskioClient>) {
    let rpc_server = Arc::new(RpcServer::new(None));
    rpc_server.listen("127.0.0.1", 0).expect("listen for rpc client");
    rpc_server.start();
    std::thread::sleep(Duration::from_millis(50));

    let conn = rpc_server
        .connect("127.0.0.1", diskio.port)
        .expect("connect to diskio");
    let dio_client = Arc::new(DiskioClient::new());
    dio_client.attach(&conn);
    (rpc_server, conn, dio_client)
}

/// Check if the test can run (both kv-server and diskio binaries available).
pub fn check_binaries() -> bool {
    let bin = crowdb_diskio_bin();
    crate::hardware::check_binaries(bin.as_deref())
}

/// Check if only the diskio binary is available (for durability test).
pub fn check_diskio_only() -> bool {
    if crowdb_diskio_bin().is_none() {
        eprintln!("skipping: crowdb-diskio binary not found");
        return false;
    }
    true
}
