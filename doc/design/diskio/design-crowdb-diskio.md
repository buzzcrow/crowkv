<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: diskio (Overview)

diskio is the per-node data I/O engine. diskdb allocates disk blocks;
chunkdb manages chunk metadata; diskio reads and writes the block
contents. It is a C++ binary (`crowdb-diskio`) that runs on each storage
node, using io_uring on Linux for asynchronous I/O and a thread-pool
`pwrite`/`pread` fallback on macOS and non-liburing Linux builds. The
server uses the crowdb-rpc C++ engine directly; Rust callers
(chunkdb writers, recovery, rebalance) use a typed client crate that
wraps `crowdb-rpc-ffi`.

The io_uring engine (`DiskIOUring`) is a multi-pipeline io_uring
wrapper that also serves the crowdb-tree btree page store. It lives in
`crowdb-common` and is shared by both consumers.

## Table of Contents

- [1. Overview](#1-overview)
- [2. Non-Goals (Design Envelope)](#2-non-goals-design-envelope)
- [3. Key Design Decisions](#3-key-design-decisions)
  - [3.1 C++ server, Rust client](#31-c-server-rust-client)
  - [3.2 io_uring on Linux; thread-pool pwrite/pread fallback](#32-io_uring-on-linux-thread-pool-pwritepread-fallback)
  - [3.3 Control + data separation in the RPC frame](#33-control--data-separation-in-the-rpc-frame)
  - [3.4 No disk status tracking](#34-no-disk-status-tracking)
  - [3.5 Partial writes are errors, not retried internally](#35-partial-writes-are-errors-not-retried-internally)
  - [3.6 Shared DiskIOUring in crowdb-common](#36-shared-diskiouring-in-crowdb-common)
- [4. Architecture Overview](#4-architecture-overview)
- [5. IoEngine Abstraction](#5-ioengine-abstraction)
  - [5.1 UringEngine](#51-uringengine)
  - [5.2 BlockingEngine](#52-blockingengine)
  - [5.3 DummyDiskEngine](#53-dummydiskengine)
  - [5.4 NullDisk and MemDisk](#54-nulldisk-and-memdisk)
- [6. Disk Model](#6-disk-model)
- [7. DiskIOUring](#7-diskiouring)
  - [7.1 Architecture](#71-architecture)
  - [7.2 fd → Pipeline Routing](#72-fd--pipeline-routing)
  - [7.3 Polling Modes](#73-polling-modes)
  - [7.4 Batched SQE Submission](#74-batched-sqe-submission)
  - [7.5 Pipeline Topology](#75-pipeline-topology)
  - [7.6 Bad-Disk Cancel by fd](#76-bad-disk-cancel-by-fd)
  - [7.7 Lock-Free Design](#77-lock-free-design)
  - [7.8 FFI Multi-Eventfd Pump](#78-ffi-multi-eventfd-pump)
- [8. RPC Service](#8-rpc-service)
- [9. Client Library](#9-client-library)
- [10. Invariants](#10-invariants)
- [11. Configuration](#11-configuration)
- [12. References](#12-references)

---

## 1. Overview

diskio is a **per-node data I/O server**. Each storage node runs one
`crowdb-diskio` process that owns a set of local disks and serves
read/write/fsync RPCs over crowdb-rpc. Callers (chunkdb's large-object
writer, small-object writer, chunk read flow, recovery, rebalance)
send a `Segment`-based address — `{disk_id, zone_index, zone_offset,
size}` — and diskio translates it to a physical offset and performs
the I/O.

**Language:** C++ server, Rust client. **I/O:** io_uring (Linux),
thread-pool `pwrite`/`pread` (macOS, non-liburing Linux).

**Core goals:**
- **Low-latency data path** — io_uring on Linux with no
  `spawn_blocking`, no thread hop, no Rust→C++ round-trip on the data
  path. The entire I/O path is async: RPC receive → DiskIOUring submit →
  CQE completion → RPC response.
- **Cross-platform dev/test** — thread-pool `pwrite`/`pread` fallback
  on macOS gives the same semantics at lower performance, so the test
  suite runs everywhere.
- **Shared DiskIOUring** — the io_uring engine lives in `crowdb-common`
  and is shared by the btree page store and the diskio engine. One
  implementation, two consumers.
- **Testability** — `MemDisk` (drop-write + rule-based read) for
  throughput benches, `SimulatedDisk` (latency + error injection) for
  fault-path tests. Both run without real hardware.

## 2. Non-Goals (Design Envelope)

- **No disk allocation.** diskdb allocates blocks; diskio reads and
  writes their contents. diskio does not track which blocks are busy
  or free.
- **No disk status tracking.** diskio does not maintain disk health
  state. If an I/O fails, the engine returns the error to the caller;
  the top layer (chunkdb/diskdb) handles the failure and stops
  allocating new blocks on that disk.
- **No chunk metadata.** chunkdb manages chunk/strip metadata; diskio
  is unaware of chunks, strips, or redundancy. It reads and writes
  raw block contents at the `Segment` granularity.
- **No internal retry on partial writes.** A short write (fewer bytes
  than requested) is an error, not retried internally. The caller
  decides whether to retry the whole write or fail the operation.
- **No RDMA or SPDK (v1).** v1 uses Linux io_uring on block devices
  and regular files. RDMA and SPDK are future additions.
- **No streaming RPC.** v1 supports request-response only. The data
  payload (up to the max block size, default 2 MB) fits in one frame.

## 3. Key Design Decisions

### 3.1 C++ server, Rust client

The diskio server is C++ because the io_uring engine and the
crowdb-rpc server engine are C++. Running the server in C++ means the
data path has no FFI boundary: RPC receive, frame decode, I/O submit,
CQE completion, and response submit all happen in C++ without a
Rust→C++ round-trip. The Rust client (`crowdb-diskio-client`) wraps
`crowdb-rpc-ffi` with typed `DiskIoClient` methods, following the
existing `crowdb-diskdb-client` / `crowdb-chunkdb-client` pattern.

### 3.2 io_uring on Linux; thread-pool pwrite/pread fallback

io_uring is the Linux asynchronous I/O interface. It provides
submission queues (SQ) and completion queues (CQ) in shared memory,
allowing batched I/O submission and completion polling with minimal
syscalls. For `O_DIRECT` on block devices, I/O completes inline
during SQ processing in most cases — no kernel thread pool needed.

macOS has no io_uring. POSIX `aio` exists but is weak: internally
thread-pool-based, limited queue depth, inconsistent across
filesystems. `libaio` is Linux-only. A dedicated C++ thread pool with
`pwrite`/`pread` and configurable `fdatasync` is the pragmatic
cross-platform fallback. It also serves as the Linux non-liburing
production path (if liburing is not available at build time).

The `IoEngine` virtual base abstracts over both backends so the RPC
layer is backend-agnostic.

### 3.3 Control + data separation in the RPC frame

A diskio RPC frame carries a small flatbuffer control message
(`{disk_id, zone_index, zone_offset, size}`) and a raw data payload
in separate buffers. crowdb-rpc's 12-byte header carries both
`msg_size` (control) and `data_size` (data), so the parser knows the
full frame shape after 12 bytes. The data payload is written directly
to / read directly from the I/O buffer with no intermediate
serialization. This mirrors crowdb-rpc's control+data separation design
(see `design-crowdb-rpc.md` §2).

### 3.4 No disk status tracking

diskio does not track whether a disk is Good, Bad, or Suspicious. It
opens disk handles at startup and serves I/O until a disk fails. When
an I/O fails, the engine returns the error to the caller. The top
layer (chunkdb/diskdb) is responsible for marking a disk as bad and
stopping new allocations on it. After all in-flight I/O to a failing
disk drains, no further I/O is sent to it.

This keeps diskio simple — it does one thing (data I/O) and reports
results. Disk health policy lives in diskdb, where it belongs.

### 3.5 Partial writes are errors, not retried internally

When `pwrite` or io_uring returns fewer bytes than requested, the
engine returns `IoError::PartialWrite` immediately. No internal retry.

For `O_DIRECT` block devices (the primary path), partial writes
essentially do not occur for correctly aligned I/O. If they do, it
usually indicates a hardware error — retrying the same range is
futile. The caller (chunkdb writer) has strip-level retry logic and
can decide whether to retry the whole strip or fail it. Internal
retry would complicate the completion callback path (resubmit for
remaining bytes, track partial progress, handle retry-failure) for no
benefit.

### 3.6 Shared DiskIOUring in crowdb-common

`DiskIOUring` is a multi-pipeline io_uring engine: it maps file
descriptors to pipelines (one `io_uring` instance each), shares
polling threads across CQs, batches SQE submission, and cancels
in-flight I/O by fd via `IORING_ASYNC_CANCEL_FD`. It is not specific
to the btree page store. It lives in `crowdb-common` so both the btree
page store (`crowdb-tree`) and the diskio engine share one
implementation. The WAL's io_uring integration later builds Rust
async submit wrappers over the same C ABI.

## 4. Architecture Overview

```
Caller (Rust)                     crowdb-diskio Server (Node X, C++)
     │                                    │
     │ DiskIoClient::write(seg, data)     │
     │ ──► crowdb-rpc-ffi frame:            │
     │     [hdr][DiskWriteReq][data]      │
     │ ──────────────────────────────────►│
     │                                    │
     │                          crowdb-rpc reader decodes frame
     │                          handler: resolve disk_id → Disk
     │                          compute phys_offset = zone_base + zone_offset
     │                                    │
     │                                    ▼
     │                          IoEngine::write(disk, phys_offset, data, on_complete)
     │                              │
     │                    ┌─────────┴────────────────────┐
     │                    │ UringEngine (Linux)          │ BlockingEngine (macOS)
     │                    │ crowdb::common::DiskIOUring    │ thread pool
     │                    │ fd→pipeline, SQE submit      │ pwrite → callback
     │                    │ → CQE completion             │
     │                    └─────────┬────────────────────┘
     │                              │ (also: DummyDiskEngine wrapper for dummy disks)
     │                                    │
     │                          on_complete → crowdb_rpc_server_submit_response
     │                          crowdb-rpc response: [hdr][DiskWriteResp]
     │ ◄──────────────────────────────────│
     │ write() returns Ok(())             │
```

Components:
- **DiskSet** — holds `HashMap<DiskId, shared_ptr<Disk>>`, opened at
  startup from the node's disk list. Resolves `disk_id` to a `Disk`.
- **Disk** — virtual base with subclasses: `BlockDisk` (real block
  device, `O_DIRECT`), `NullDisk` (memfd, drop-write + pattern read),
  `MemDisk` (memfd, store + read-back).
  Each `Disk` shares the node's `IoEngine` instance; dummy disks wrap
  it with a `DummyDiskEngine` for read-content hack and optional fault
  injection.
- **Zone** — `{zone_index, base_offset, capacity, state}`. The
  physical offset for an I/O is `zone.base_offset + zone_offset`.
- **IoEngine** — virtual base: `submit_write`/`submit_read`/
  `submit_fsync` taking a disk handle, physical offset, buffer, size,
  and a completion callback. Implementations: `UringEngine`,
  `BlockingEngine`, `DummyDiskEngine` (wrapper for dummy disks).
- **DiskIOUring** — the shared multi-pipeline io_uring engine in
  `crowdb-common`. Maps fds to pipelines (one `io_uring` instance per
  pipeline); polling threads drain CQs and submit pending SQEs.
  Lock-free on the hot path (SQE claim via atomic CAS, fd→pipeline
  lookup via direct-indexed array).
- **RPC handler** — dispatches `DiskWriteRequest`/
  `DiskReadRequest`/`DiskFsyncRequest` to the `IoEngine`; the
  completion callback builds the response and submits it via
  `crowdb_rpc_server_submit_response`.
- **DiskIoClient** — Rust client wrapping `crowdb-rpc-ffi` with typed
  methods. Routes to the correct node's diskio server based on
  `segment.node_id`.

## 5. IoEngine Abstraction

`IoEngine` is a C++ virtual base:

```
class IoEngine {
  public:
    virtual ~IoEngine() = default;
    virtual void submit_write(Disk *disk, off_t phys_offset,
                              const uint8_t *data, size_t size,
                              std::function<void(int)> on_complete) = 0;
    virtual void submit_read(Disk *disk, off_t phys_offset,
                             uint8_t *buf, size_t size,
                             std::function<void(int)> on_complete) = 0;
    virtual void submit_fsync(Disk *disk,
                              std::function<void(int)> on_complete) = 0;
};
```

`on_complete` is invoked exactly once with the raw result: `>=0` bytes
transferred, `<0` negative `-errno`. The RPC handler's callback
resolves the request and calls `crowdb_rpc_server_submit_response`.

### 5.1 UringEngine

Linux only (`CROWDB_HAVE_LIBURING`). Owns a `crowdb::common::DiskIOUring`
instance (one engine per diskio server). `O_DIRECT` aligned writes
(configurable, default on for data blocks). The entire I/O path is
async — no `spawn_blocking`, no thread hop.

`UringEngine` is a thin adapter: it validates the disk handle and
`O_DIRECT` alignment, then delegates to `DiskIOUring::submit_*` with
the disk's fd. `cancel_disk` looks up the disk's fd via `DiskSet` and
calls `DiskIOUring::cancel_fd` — one SQE cancels all in-flight I/O on
that fd via `IORING_ASYNC_CANCEL_FD` (kernel 6.0+). No per-disk
in-flight tracking map; the kernel does the cancel lookup. See §7.6.

### 5.2 BlockingEngine

macOS + non-liburing Linux. A dedicated C++ thread pool (configurable
size, default 4 threads per disk) with `pwrite`/`pread` and
configurable `fdatasync`/`fsync`. Each I/O operation is submitted to
the pool; the worker thread performs the blocking syscall and invokes
the completion callback. Correct semantics, lower performance (thread
hop per I/O). Also used for `BlockDisk` without `O_DIRECT` (pwrite to
a block device at an offset).

### 5.3 DummyDiskEngine

The dummy-disk wrapper. `DummyDiskEngine` wraps a real `IoEngine`
(`UringEngine` or `BlockingEngine`) and provides two features:

- **Read-content hack** (NullDisk): after the inner engine completes
  a `pread`, the wrapper overwrites the buffer with deterministic
  pattern data generated from `disk_id` + `phys_offset` (xorshift64
  PRNG). The full uring/blocking `pwrite`/`pread` syscall path
  executes against a `memfd_create` backing — no real disk I/O, but
  the ring mechanics, syscall cost, and DiskIOUring batching all run.
  Used for benchmark tests that measure uring overhead without
  storage.
- **Fault injection** (NullDisk + MemDisk): per-I/O random latency
  (uniform draw from `latency_min_ms`..`latency_max_ms`, sleep before
  completing) and errors (return `-EIO` at the configured
  `error_rate`). Configured via `DiskProperties` on the dummy disk.
  Merged from the former `SimulatedEngine` — no separate wrapper
  engine needed.

### 5.4 NullDisk and MemDisk

Two dummy disk types, both backed by `memfd_create` (tmpfs):

- **NullDisk** — the default dummy disk. Writes go to tmpfs (discarded
  — NullDisk never reads them back). Reads execute the full `pread`
  path, then the `DummyDiskEngine` wrapper overwrites the buffer with
  pattern data. For benchmark tests: measures uring/blocking overhead
  without real disk I/O or storage capacity limits.
- **MemDisk** — stores written data and reads it back. For end-to-end
  correctness tests that verify I/O data integrity. The full
  `pwrite`/`pread` path executes against the memfd; no real disk I/O.

Both dummy disks share the node's `IoEngine` instance (uring or
blocking, auto-detected). Optional `DiskProperties` enable fault
injection on either type.

## 6. Disk Model

`Disk` is a C++ virtual base with subclasses:

- **BlockDisk** — real block device, opened with `O_DIRECT | O_RDWR`
  (Linux). Aligned I/O only. The primary production disk type for
  NVMe/SATA SSDs and HDDs. The device path comes from the
  `device_path` field in `DiskValue` (group-0 sysdata).
- **NullDisk** — memfd-backed dummy disk. Writes discarded; reads
  return deterministic pattern data via `DummyDiskEngine` wrapper.
  Default dummy disk type. For benchmark tests.
- **MemDisk** — memfd-backed dummy disk. Stores written data and
  reads it back. For end-to-end correctness tests.

Each `Disk` shares the node's `IoEngine` instance (auto-detected:
uring on Linux with liburing, blocking otherwise). Dummy disks
(`NullDisk`, `MemDisk`) wrap the shared engine with a
`DummyDiskEngine` for read-content hack and optional fault injection.

`Zone` holds `{zone_index, base_offset, capacity, state}`. The
physical offset for an I/O is `zone.base_offset + zone_offset`. Zone
records come from diskdb's allocation.

`DiskSet` holds `HashMap<DiskId, shared_ptr<Disk>>`, opened at startup
from the node's disk list (via `HardwareClient` / diskdb). `DiskId`
is the 128-bit id from diskdb.

## 7. DiskIOUring

`DiskIOUring` is a multi-pipeline io_uring engine. It maps file
descriptors to pipelines (one `io_uring` instance per pipeline),
shares polling threads across CQs, batches SQE submission, and is
fully lock-free on the hot path. Both diskio and btree use
`DiskIOUring` as their I/O interface.

`DiskIOUring` lives in `crowdb-common` (`crowdb::common::DiskIOUring`),
guarded by `CROWDB_HAVE_LIBURING`. It is Linux-only. On macOS and
non-liburing Linux builds, `DiskIOUring` is absent and
`BlockingEngine` is used instead.

### 7.1 Architecture

```
                    DiskIOUring
                    ┌─────────────────────────────────────────────┐
                    │  fd_table[fd] → pipeline_index (read-only)   │
                    │                                             │
submit_read(fd) ──► │  Pipeline 0        Pipeline 1        ...     │
submit_write(fd) ─► │  ┌──────────┐     ┌──────────┐              │
submit_fsync(fd) ─► │  │ io_uring │     │ io_uring │              │
cancel_fd(fd) ────► │  │ SQ + CQ  │     │ SQ + CQ  │              │
                    │  └────┬─────┘     └────┬─────┘              │
                    │       │                │                     │
                    │  ┌────┴────────────────┘                     │
                    │  │  PollThread 0 (busy-poll or epoll-wait)   │
                    │  │  drains CQ-0 + CQ-1, submits pending SQEs │
                    │  └───────────────────────────────────────────┘
                    └─────────────────────────────────────────────┘
```

Components:
- **Pipeline** — one `io_uring` instance: one SQ + one CQ in shared
  memory. The low-level uring wrapper (lock-free SQE claim, CQE
  drain). Internal to `DiskIOUring`; not exposed to callers.
- **PollThread** — one OS thread that polls a group of pipelines' CQs
  and submits their pending SQEs. Multiple CQs share one polling
  thread (no one-thread-per-CQ requirement). Configurable grouping.
- **fd_table** — direct-indexed `std::vector<size_t>` mapping fd →
  pipeline_index. Populated at `register_fd` time, read-only during
  I/O. No synchronization needed (concurrent reads of an immutable
  array).
- **CallbackEntry** — allocated per submit, freed after CQE dispatch
  via deferred-delete (polling-thread-only). Holds the callback; no
  atomics, no cancel flags, no hazard pointers. Callback suppression
  is client-side (§7.7): the caller wraps the callback with a shared
  cancel flag.

### 7.2 fd → Pipeline Routing

`fd_table` is direct-indexed by fd number. fds are small integers
(typically < 1024). The vector is sized once at construction to
`ulimit -n` (capped at 4096) and never grows — no reallocation, no
data race on concurrent reads. Each slot defaults to `SIZE_MAX`
(sentinel: "not registered"). Looking up `fd_table[fd]` is a single
memory load — no hash, no collision, no atomic, no lock.

Two registration modes:
- **Auto-assignment** (`register_fd(fd)`) — `DiskIOUring` picks the
  pipeline with the lowest in-flight count and sticks the fd to it.
  Used by btree (single pipeline) and uniform-disk diskio topologies.
  Sticky: once assigned, all future submits for that fd go to the
  same pipeline (required for `cancel_fd` correctness — cancel targets
  the ring where the fd's SQEs were submitted).
- **Explicit assignment** (`register_fd(fd, pipeline_index)`) — caller
  pins the fd to a specific pipeline. Used when the caller built a
  disk-class-aware topology (NVMe → pipeline 0, HDD → pipeline 2) and
  needs deterministic routing.

### 7.3 Polling Modes

A `PollingMode` config per pipeline:

- **Classic** — `io_uring_wait_cqe_timeout` every iteration, 50ms
  timeout. Simplest mode; used by crowdb-tree's single-pipeline
  topology.
- **Hybrid** `{ busy_poll_budget }` — busy-poll the CQ ring's shared
  memory via `io_uring_peek_cqe` with no syscall while I/O is active.
  After `busy_poll_budget` consecutive empty peeks, transition to
  `epoll_wait` on the pipeline's eventfd. Any CQE resets the counter,
  returning to busy-poll. Gives sub-µs CQE dispatch during I/O bursts,
  sleeps when idle, no core burned at idle. Default for `UringEngine`.
- **Sqpoll** `{ sq_thread_idle }` — opt-in for sustained high-IOPS:
  `IORING_SETUP_SQPOLL` eliminates submit syscalls via a kernel
  SQ-poll thread. Requires root/CAP_SYS_NOPRIV. Useful for recovery
  scan and bulk rebalance.

When multiple Hybrid-mode pipelines share a poll thread, the thread
uses the **minimum** `busy_poll_budget` across its assigned Hybrid
pipelines — the most conservative budget wins.

### 7.4 Batched SQE Submission

Client threads fill SQE slots (lock-free CAS on shadow tail + per-slot
ready flags) and set a `pending_submit` atomic flag. The polling
thread submits all pending SQEs in one `io_uring_enter` per iteration
— batching N SQEs into one syscall. Clients never call
`io_uring_enter`; only the polling thread does.

In busy-poll mode (I/O active), SQEs are submitted within <1µs of
being filled. In event-wait mode (idle), the first client to fill an
SQE after the thread sleeps writes the pipeline's eventfd (waking the
thread); subsequent clients in the same burst skip the eventfd write
via a `thread_sleeping` CAS — at most one eventfd write per sleep
cycle. For N=32 SQEs in a burst: batching = 2 syscalls, unbatched =
32. **16× reduction.**

### 7.5 Pipeline Topology

io_uring does not support sharing a CQ ring across separate
`io_uring` instances — each `io_uring_queue_init` creates its own SQ +
CQ pair. But one ring can submit I/O for any number of fds, so the
answer is fewer rings, not shared CQs. Topology by disk type:

- **NVMe SSD** (100k+ IOPS/disk): one pipeline per disk. High IOPS
  needs SQ headroom; per-disk isolates SQ/CQ backpressure so one busy
  disk's full SQ doesn't block another.
- **SATA SSD** (10k-100k IOPS/disk): one pipeline per 4-8 disks
  (grouping). Medium IOPS; grouping reduces polling thread count
  while keeping SQ headroom. For 24-30 SSDs this gives 3-8 rings, not
  24-30.
- **HDD** (100-200 IOPS/disk): one shared pipeline for all HDDs. Low
  IOPS; one ring's SQ handles 30 HDDs trivially. `entries = 2048`.

Poll thread grouping:
- **1 thread, all pipelines** (default, simple): one thread
  busy-polls all CQs. Best for low pipeline count.
- **1 thread per pipeline** (max isolation): each CQ has its own
  thread. Best for high-IOPS NVMe where CQ drain latency matters.
- **Grouped by disk class** (balanced): `{ {0,1,2}, {3,4,5} }`. 2
  threads for 6 pipelines. Best for SATA SSD farms.
- **Grouped by NUMA node**: pipelines whose disks are on NUMA node 0
  → thread 0 (pinned to node 0 core); node 1 → thread 1.

`IORING_SETUP_ATTACH_WQ` (Linux 5.18+): when `Topology::attach_wq` is
true and there are ≥2 pipelines, subsequent pipelines share the
kernel's io-wq pool (8 kernel threads total instead of N×8). For
`O_DIRECT` on block devices, I/O almost always completes inline and
io-wq is rarely involved; `attach_wq` matters most for non-`O_DIRECT`
file I/O. Default: `false` (opt-in).

### 7.6 Bad-Disk Cancel by fd

`cancel_fd(fd)` cancels all in-flight I/O for an fd with one SQE:
`IORING_OP_ASYNC_CANCEL` with `flags = IORING_ASYNC_CANCEL_FD |
IORING_ASYNC_CANCEL_ALL` and `addr = fd` (kernel 6.0+). The kernel
cancels all matching ops and posts CQEs (`-ECANCELED` for each
cancelled op, or the original result if already completed). No
per-fd in-flight tracking needed — the kernel does the lookup. Zero
overhead on the hot path.

On a shared pipeline, a bad disk's in-flight I/O holds SQ slots. If
the SQ fills, good disks' I/O is rejected with `-ENOMEM`.
`cancel_fd` frees SQ slots immediately by cancelling all of the bad
fd's in-flight ops. The SQ ring's capacity check is
`tail - head >= entries`, where `head` is the kernel's consumed SQ
head — in-flight I/O is in the block layer, not the SQ ring, so the
SQ has space for the cancel SQE even when all of the fd's I/O is
in-flight.

**Kernels < 6.0** (no `IORING_ASYNC_CANCEL_FD`): `cancel_fd` returns
`-ENOSYS`. The caller (diskio's `cancel_disk`) logs a warning and
falls back to waiting for in-flight I/O to complete naturally (the
bad-disk path has a timeout). macOS never compiles `DiskIOUring`;
`BlockingEngine` is the non-uring path.

CQEs are independent — the polling thread drains all ready CQEs each
iteration, so a slow disk's CQE arriving late doesn't block CQ drain
for others.

### 7.7 Lock-Free Design

The hot path (submit + CQE completion) uses no mutexes, no locks, no
blocking:

- **SQE claim**: atomic CAS on shadow tail + per-slot ready flags.
  Multi-threaded clients claim slots concurrently with no contention
  beyond CAS retries.
- **fd → pipeline lookup**: direct array index, read-only. No
  synchronization.
- **Batch submit coordination**: `pending_submit` atomic flag,
  `pending_count` atomic counter, `thread_sleeping` atomic flag. All
  single-word atomics.
- **CQ drain**: polling thread only (single-threaded per CQ). No
  concurrency, no synchronization.
- **Callback dispatch**: polling thread only. `CallbackEntry` holds
  just the callback — no `cancelled`/`dispatched` atomics. The
  dispatch path calls `entry->cb(res)` unconditionally; if the client
  wants to suppress the callback, it checks a shared cancel flag
  inside the callback itself (client-side cancel). The CQE always
  arrives, the callback always fires, the SQ slot is always freed —
  only the callback body is skipped via the flag. A `shared_ptr` keeps
  the cancel state alive until the CQE arrives (no UAF).
- **CallbackEntry lifetime**: deferred-delete on the polling thread.
  The polling thread is the sole toucher of `CallbackEntry` after
  `submit_*` returns — no concurrent access, no hazard pointers, no
  generation counters.

**Kernel-internal locks** (not controllable from userspace):

| I/O path | Lock | Hold duration | Contention scope |
| --- | --- | --- | --- |
| SQE fill / CQE drain | None | — | — |
| `O_DIRECT` block device (diskio `BlockDisk`) | `blk-mq` per-HW-queue spinlock | µs (queue insertion) | per-HW-queue |
| `O_DIRECT` regular file write (btree block files) | `i_rwsem` exclusive | full I/O latency | per-inode |
| `O_DIRECT` regular file read | `i_rwsem` shared | full I/O latency | per-inode (concurrent reads OK) |

io_uring never blocks the userspace submitter on `i_rwsem` — when an
inline execution would block, io_uring punts to `io_wq` (kernel worker
pool). The design's per-fd pipeline isolation aligns with the kernel's
per-device / per-inode lock scope: if one fd's I/O contends on a
kernel lock, it is on its own pipeline with its own polling thread.
Both consumers avoid same-inode write contention by design: btree
uses one file per extent (different inodes), diskio writes to block
devices (no `i_rwsem`).

### 7.8 FFI Multi-Eventfd Pump

Each pipeline registers its eventfd via
`IORING_REGISTER_EVENTFD`. The Rust FFI layer
(`lib/crowdb-tree/ffi/src/reactor.rs`) spawns one `EventfdPump` per
pipeline eventfd, all sharing one `Arc<Notify>`. Any pipeline's CQE
completion wakes all waiting `drive_ct_future` calls. The C ABI
function `ct_uring_eventfds` returns the pipeline eventfds; the Rust
wrapper spawns one pump per fd.

## 8. RPC Service

The diskio server uses crowdb-rpc's `RpcServer::register_handler` to
handle three message types. Message type IDs are in the diskio range
(3600s) of crowdb-rpc's `msg_type` enum.

Each request's control message is a flatbuffer with
`{disk_id, zone_index, zone_offset, size}` (read also has
`test_pattern_offset`); the write request also carries a raw data
payload of `size` bytes. The handler:
1. Resolves `disk_id` to a `Disk` via `DiskSet`.
2. Computes the physical offset: `zone.base_offset + zone_offset`.
3. Calls `IoEngine::write`/`read`/`fsync` with the disk handle,
   physical offset, buffer, and a completion callback.
4. The completion callback builds the response and calls
   `crowdb_rpc_server_submit_response`.

The data payload is passed from the crowdb-rpc frame decoder directly to
`IoEngine::write` — no copy between RPC receive and I/O submit. The
read response includes the raw data payload.

Flatbuffer schemas (`diskio.fbs`):
- `DiskWriteRequest { disk_id, zone_index, zone_offset, size }`
- `DiskWriteResponse { ret_code }`
- `DiskReadRequest { disk_id, zone_index, zone_offset, size, test_pattern_offset }`
  (`test_pattern_offset` used by NullDisk for deterministic content;
  default 0 = start of disk/object)
- `DiskReadResponse { ret_code }` (data payload follows the control
  message)
- `DiskFsyncRequest { disk_id }`
- `DiskFsyncResponse { ret_code }`

## 9. Client Library

`DiskIoClient` (Rust, in `crowdb-diskio-client`) wraps `crowdb-rpc-ffi`
with typed methods:

```
async fn write(&self, segment: &Segment, data: Bytes) -> Result<(), IoError>
async fn read(&self, segment: &Segment, test_pattern_offset: u64) -> Result<Bytes, IoError>
async fn fsync(&self, disk_id: &DiskId) -> Result<(), IoError>
```

The chunk client combines DiskIO service registrations with hardware disk-group
ownership to build an immutable `disk_id -> endpoint + connection` snapshot.
Every disk group must have exactly one live owner. Missing and duplicate owners
fail discovery; there is no fallback endpoint. Refresh builds the snapshot away
from the write path and publishes it atomically.

`DiskioClient::write_bytes` retains the caller's owned `Bytes` allocation in
the RPC buffer until completion. This avoids a client-side `Bytes` to `Vec`
payload copy. On the server, the decoded payload continues directly into
`IoEngine::write`, so no additional copy is introduced between RPC receive and
I/O submission.

A connection drop during a write is similar to a timeout: the client
does not know the result (the I/O may still complete on the server —
DiskIOUring submission is already in flight). The client treats it as a
failure and retries; idempotent write to the same offset is safe for
the same data.

## 10. Invariants

- **I1 (data integrity)**: A successful `write(segment, data)`
  followed by a `read(segment, None)` on the same `Segment` returns
  the same bytes. Holds for `UringEngine`, `BlockingEngine`, and
  `DummyEngine` (with rule-based read content).
- **I2 (durability)**: A `write` followed by `fsync` followed by
  process restart returns the written data on re-read.
- **I3 (offset correctness)**: The physical offset for an I/O is
  `zone.base_offset + zone_offset`, where `zone` is looked up by
  `zone_index` from the `Disk`'s zone records.
- **I4 (no silent drop)**: `DiskIOUring` does not drop I/O requests.
  If the SQ is full, the lock-free SQE claim enters bounded retry
  waiting for a slot. If the ring is invalid, the completion callback
  is invoked synchronously with `-EIO`.
- **I5 (completion guarantee)**: `on_complete` is invoked exactly
  once per `submit_*` call — either from the polling thread (CQE
  dispatched) or synchronously (submission failure). `cancel_fd`
  replaces the original callback with a `-ECANCELED` CQE.
- **I6 (bad-disk isolation)**: A bad disk's in-flight I/O does not
  permanently block good disks' I/O on a shared pipeline.
  `cancel_fd(fd)` cancels all in-flight ops on the bad fd with one
  SQE, freeing SQ slots immediately.
- **I7 (partial write is error)**: A short write returns
  `IoError::PartialWrite` immediately. No internal retry. The caller
  decides the next action.

## 11. Configuration

- **node_id** — the node's ID (from group-0 sysdata).
- **bind_address** — crowdb-rpc listen address + port.
- **disk_list** — explicit disk list, or auto-discover from group-0
  via `HardwareClient`. Disks with an empty `device_path` are dummy
  disks (`NullDisk` or `MemDisk`).
- **dummy_disk_type** — `null` (default, drop-write + pattern read)
  or `mem` (store + read-back). Used when `device_path` is empty.
- **engine** — auto-detected: `UringEngine` on Linux with liburing,
  `BlockingEngine` otherwise. No user-configurable engine selection.
- **thread_pool_size** — blocking engine thread count (default 4).
- **o_direct** — toggle `O_DIRECT` for `BlockDisk` (default on).
- **polling_mode** — pipeline polling mode: `classic`, `hybrid`,
  `sqpoll` (default `hybrid` for `UringEngine`).
- **busy_poll_budget** — consecutive empty peeks before transitioning
  to event-wait in `Hybrid` mode.
- **sq_thread_idle** — idle timeout for `Sqpoll` mode.
- **sq_entries** — SQ ring size (default 256; 1024+ for high-IOPS SSD
  pipelines; 2048+ for shared HDD pipelines).
- **attach_wq** — share kernel io-wq across pipelines (default
  `false`; opt-in when ≥2 NVMe or ≥2 SATA pipelines).
- **fault_latency** — `min_ms:max_ms` latency injection for dummy
  disks (testing only).
- **fault_error_rate** — `0.0..1.0` error injection rate for dummy
  disks (testing only).

The server registers with the group-0 service registry on startup,
reporting the diskio service is alive. Other services use this for
health detection.

## 12. References

- [`design-crowdb-diskdb.md`](../diskdb/design-crowdb-diskdb.md) §2 (Non-Goals: "No
  data I/O") — diskdb allocates blocks; diskio reads/writes contents.
- [`design-crowdb-chunkdb.md`](../chunkdb/design-crowdb-chunkdb.md) §5.1
  (Disk block — `Segment { node_id, disk_id, zone_index,
  zone_offset, size, tag }`) — the addressing unit for diskio.
- [`../rpc/design-crowdb-rpc.md`](../rpc/design-crowdb-rpc.md) §2 (Key
  Design Decisions: control + data separation, native buffer, C ABI +
  oneshot FFI) — the RPC engine diskio builds on.
- [`design-crowdb-tree.md`](../tree/design-crowdb-tree.md) — the btree
  page store that shares `DiskIOUring`.
