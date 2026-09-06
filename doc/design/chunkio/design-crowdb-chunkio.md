<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Chunk IO Data Path (Overview)

The chunk IO data path is the client-side layer that writes and reads
object data as EC-encoded strips across diskio servers, using chunkdb
for chunk lifecycle management (allocate, append, seal, delete). It
lives in the `crowdb-chunk-client` crate and is consumed by object store
layers and application upload handlers. The chunkdb server design
(chunk lifecycle, placement, EC integration) is in
[`doc/design/chunkdb/design-crowdb-chunkdb.md`](../chunkdb/design-crowdb-chunkdb.md);
the diskio block IO engine is in
[`doc/design/diskio/design-crowdb-diskio.md`](../diskio/design-crowdb-diskio.md).
This doc does not repeat their architecture — it covers the data path
that sits between them: the write pipeline, its backpressure and memory
model, and the design choices that make a 1 TB upload cost the same
~15 MB of RAM as a 50 MB one.

## Table of Contents

- [1. Non-Goals](#1-non-goals)
- [2. Key Design Decisions](#2-key-design-decisions)
- [3. Write Flow](#3-write-flow)
- [4. Backpressure and Memory Budget](#4-backpressure-and-memory-budget)
- [5. EC Integration](#5-ec-integration)
- [6. Chunk Rotation and Location](#6-chunk-rotation-and-location)
- [7. Completion and Error Handling](#7-completion-and-error-handling)
- [8. Interaction with Neighbors](#8-interaction-with-neighbors)
- [9. Tunables and Defaults](#9-tunables-and-defaults)
- [10. Application API and Routing](#10-application-api-and-routing)
- [11. Performance Workload](#11-performance-workload)

## 1. Non-Goals

- **No small-object writer.** Shared-chunk packing for many small
  objects is a separate component. The `ChunkIoWriter` trait and
  `ProtoLocation` type are designed for reuse, but the packing policy
  is not part of this design.
- **No reader.** The read path (location resolution, strip fetch, EC
  decode, range reads) is a separate component.
- **No single-block replacement on write failure.** The error path
  retries whole strips and frees failed segments; in-place single-block
  repair is a future refinement and an integration point, not a v1
  behavior.
- **No GC of leaked partial chunks.** Best-effort cleanup on abort
  leaves Active chunks for a future reaper; this doc does not specify
  the reaper.
- **No object protocol server.** Applications call the client library
  directly. An S3-compatible or other object protocol server can use the same
  API later without owning chunk placement or DiskIO routing.

## 2. Key Design Decisions

- **Block-granularity pipeline, not strip-granularity.** The first disk
  write starts after 1 MB (one data block), not after the full 4 MB
  strip. Three strips stay in flight simultaneously (N parity, N+1
  data, N+2 fetch) without unbounded memory. Strip-granularity would
  double time-to-first-byte and halve steady-state throughput.
- **Push-based "always store" contract.** `ChunkIoWriter::on_data`
  never rejects a buffer; it awaits until internal capacity is free.
  This puts the retry-loop in one place (the writer) instead of every
  caller. A `FeedStatus` (`Continue` / `Pause`) answers "would the next
  push block?" so a dedicated upload task can ignore it and block,
  while a shared handler task can pre-check via the non-async
  `require_data` hint and return 503 instead of stalling.
- **Bounded preallocation, not eager allocation.** A 1 TB object does
  not allocate all 250K strips at once. The prealloc task stays only
  `prealloc_depth` strips (default 2) ahead of the write cursor,
  keeping allocation rate and KV metadata pressure bounded regardless
  of object size while keeping the cursor fed.
- **Shard-based EC, no re-split copy.** The pipeline already holds data
  as separate 1 MB `Bytes` blocks. Re-splitting a contiguous buffer
  just to feed `crowdb_common::ec::encode` would copy 4 MB per strip for
  no benefit. `encode_parity_from_shards` takes pre-split shards
  directly and reuses the existing isa-l FFI path — no new C++ code.
- **Whole-strip retry, not single-block retry.** On a diskio write
  failure for any block of a strip, the writer retries the whole strip
  at a fresh placement. This keeps the strip's data/parity placement
  atomic and avoids degraded-strip bookkeeping in v1; single-block
  replacement is left as a future integration point.
- **Memory budget per pool, not per object.** A `WriterPool` tracks a
  total `memory_budget` and an atomic `in_use` counter; `try_acquire`
  rejects with `MemoryBudgetExhausted` when full, enabling backpressure
  up the call stack. Per-writer footprint is constant (~15 MB peak for
  4+1 EC, 1 MB blocks, defaults), so `max_concurrent = budget /
  per-writer-footprint` — a 1 TB and a 50 MB upload cost the same RAM.
- **Two trait seams for testability.** `LargeObjectWriter` is generic
  over one chunk-lifecycle seam (`ChunkAllocator`) and one block-IO
  seam (`DiskWriter`), so integration tests inject mock impls without
  real servers; E2E tests use real clients. The seams are not a runtime
  polymorphism optimization — they exist so the pipeline can be tested
  in isolation.
- **Drive loop in `ChunkWriter`, not the object layer.** The
  strip-level drive loop (push block → write to disk → auto-rotate
  strips when full → spawn parity) lives inside `ChunkWriter::push`.
  The object layer calls `push` + `is_full` + `seal` — it does not
  track strip boundaries, block indices, or parity handoff. This
  eliminates the `StripPlacement` bridge type and keeps the object
  layer thin.
- **Own flatbuffer types directly.** `ChunkWriter` owns `Arc<Chunk>`,
  `EcStripWriter` holds `Arc<Chunk>` + strip index, and `seal()`
  returns `ProtoLocation` directly. No parallel wrapper structs
  (`StripPlacement`, `Location`) — the flatbuffer types are the canonical
  representation throughout the write path.

## 3. Write Flow

The writer runs three concurrent stages: a fetch stage, a
`ChunkWriter` (which owns the strip-level drive loop + internal strip
prefetch), and a bounded pool of background parity tasks.
`LargeObjectWriter` exposes two driving modes over the same pipeline —
stream mode (`write_stream`, pulling from an `AsyncRead`) and push mode
(the `ChunkIoWriter` trait, §2). In stream mode a fetch stage pulls
from `AsyncRead`; in push mode the caller's bytes go directly to the
block channel. The `ChunkWriter` drive loop is identical in both modes.

```
                    ┌────────────────────┐
                    │  ChunkPrefetch     │  background, bounded:
                    │  1 chunk ahead     │  pre-allocates next Chunk
                    │  (1 strip each)    │  (1 strip) for rotation
                    └─────────────┬──────────┘
                             │ pre-allocated Chunk
                             ▼
  AsyncRead ──► [Fetch] ──► block_buf ──► [ChunkWriter::push] ─────► disk (data)
             read ≤1MB    (1 MB per     │  write 1 data block → 1 disk
             per-block       block)        │  (immediately, no EC wait)
             send to write                  │
             max_cached_buffer              │  when all 4 blocks of strip N written:
             = 4 MB (default)               ├──── spawn parity ────────────────────
             (backpressure if full)         │                     ▼
                                          │   [Parity Task N] (background)
                                          │   EC encode → 1 parity block
                                          │   write parity → 5th disk
                                          │   fsync all 5 disks
                                          │   (no join — handles collected)
                                          ▼
                                     auto-rotate to strip N+1, block 0
                                     (no wait for parity)
                                          │
                                          │  internal strip prefetch:
                                          │   append_chunk ahead of cursor
                                          │   (bounded: prealloc_depth=2)
```

The flow, step by step:

- **Fetch.** Reads from the `AsyncRead` stream in ≤ 1 MB per call (one
  data block). A single socket read may return less (64 KB, 512 KB);
  the fetch stage accumulates to 1 MB, then sends the block to
  `ChunkWriter::push` immediately — it does not wait for the full 4 MB
  strip. The fetch stage sends sequential `Bytes` blocks and does not
  track block indices or strip boundaries; `ChunkWriter` owns indexing.
  On EOF with a partial last block, the partial block is sent.
- **ChunkWriter::push (drive loop).** The central coordinator. It
  owns `Arc<Chunk>` and shares it with `EcStripWriter` by ref count.
  On each `push`, if the current strip is full, it finishes the strip
  (spawns parity writes + fsyncs as background tasks, collects handles,
  no join), checks `is_full()` (chunk-level), and either returns
  `Pause` (chunk full — caller rotates chunks) or opens the next strip
  (from `chunk.strips` if pre-appended by the internal strip prefetch,
  or via inline `append_chunk` RPC). It then pushes the block to the
  new strip's data segment via `DiskWriter::write` — one disk per
  block, no EC wait. Strip N+1's data writes overlap with strip N's
  parity writes + fsyncs.
- **Parity.** One background task per strip. It receives the strip's
  data blocks, EC-encodes via `encode_parity_from_shards` (§5) into
  `code_num` parity blocks, writes them to the remaining segments via
  `DiskWriter::write`, and `fsync`s all disks via `DiskWriter::fsync`.
  Parity handles are collected by `ChunkWriter` and joined at `seal`
  time (not at strip finish) — this decouples parity durability from
  strip rotation, allowing strip N+1's data writes to start before
  strip N's parity completes.
- **Strip prefetch (internal to ChunkWriter).** A background task
  appends strips to the current chunk via `append_chunk` ahead of the
  write cursor, bounded by `prealloc_depth` (default 2). The
  `append_chunk` response replaces `self.chunk` (Arc-swap — old Arc in
  any in-flight `EcStripWriter` stays alive). For known-size objects,
  the prefetch stops after enough strips are allocated; for
  unknown-size objects, it stays `prealloc_depth` ahead.
- **Chunk prefetch (ChunkPrefetch, object layer).** Pre-allocates the
  next `Chunk` (1 strip each) ahead of rotation, up to
  `chunk_prefetch_depth` ahead. On chunk rotation, the object layer
  pulls the next `Chunk` from the prefetch receiver (fast path —
  pre-allocated) or calls `on_demand` (slow path — prefetch fell
  behind). The `Chunk` is passed to `ChunkWriter::open`, which wraps it
  in `Arc` and starts the internal strip prefetch.

### 3.1 Partial Last Strip

Partial strips occur only at EOF, never mid-chunk. When EOF arrives
before all `data_num` blocks of the current strip are filled, the main
write task writes only the filled data blocks, releases the empty ones,
hands the partial set off to parity for partial EC (§5), and records
`sealed_length` for `seal_chunk`.

## 4. Backpressure and Memory Budget

Two independent limits bound the pipeline; neither depends on object
size.

- **`max_cached_buffer`** (default 4 MB = one strip) bounds un-written
  data in the fetch channel — blocks sent to the main write task but
  not yet written to disk. When disk write is slower than network
  receive, the cache fills; once full, the fetch stage blocks and
  throttles the stream to the disk write speed.
- **`parity_depth`** (default 2) bounds in-flight parity tasks. When
  the parity pool is full, the main write task blocks at hand-off —
  backpressure on the write path, decoupled from the fetch cache.

If prealloc falls behind, the main write task awaits on the bounded
strip channel; the fetch stage keeps filling `max_cached_buffer`, then
blocks when full. No data is lost.

### 4.1 Per-Writer Footprint

For 4+1 EC, 1 MB blocks, defaults:

- `max_cached_buffer` — 4 MB un-written data in fetch cache.
- 1 block being written — 1 MB.
- Up to `parity_depth` (2) parity tasks, each holding the strip's data
  blocks (4 × 1 MB = 4 MB, shared via `Bytes` ref count — not copied,
  but resident until EC compute completes) + 1 parity block (1 MB).

Peak: 4 + 1 + 2 × (4 + 1) = **15 MB**. The conservative 15 MB assumes
both in-flight parity tasks hold data refs simultaneously; realistic
steady-state peak is ~11 MB, since EC compute is fast relative to disk
write + fsync and parity tasks stagger.

### 4.2 WriterPool

`WriterPool` tracks a `memory_budget` and an atomic `in_use` counter.
`try_acquire` returns `MemoryBudgetExhausted` when the budget is full;
release decrements `in_use` on `Drop`. `max_concurrent =
memory_budget / per-writer-footprint`. Many concurrent large-object
uploads are thus bounded by available RAM, not by object size — the
pool rejects new writes when the budget is exhausted, propagating
backpressure up the call stack.

## 5. EC Integration

The pipeline holds data as separate 1 MB `Bytes` blocks; the existing
`crowdb_common::ec::encode` re-splits a contiguous buffer into shards,
which would force a 4 MB copy per strip just to re-split. Instead,
`encode_parity_from_shards(scheme, data_shards)` takes pre-split data
shards directly and reuses the existing isa-l FFI path — no new C++
code.

`data_shards.len()` is `data_num` for a full strip, or `< data_num`
for a partial strip. isa-l supports partial EC: missing shards are
treated as zero for the encoding matrix, so no padding is written to
disk — the reader reads only `sealed_length` bytes. The function
returns `code_num` parity shards.

Edge cases:

- Full strip → standard EC.
- Partial strip → parity from present shards; reader reads only
  `sealed_length` bytes.
- Single-block object (< 1 MB) → 1 partial data shard, 1 parity shard.

## 6. Chunk Rotation and Location

Very large objects (`> max_chunk_size`) span multiple chunks. Chunk
size is always a multiple of strip data capacity — the writer only
appends whole strips — so rotation happens at strip boundaries, never
mid-strip. When `ChunkWriter::push` finishes a strip and `is_full()`
returns true, it returns `FeedStatus::Pause` without pushing the
buffer. The object layer then calls `seal()` (joins all in-flight
parity handles, `seal_chunk` RPC, returns `ProtoLocation`), records
the location, pulls the next `Chunk` from `ChunkPrefetch`, and opens a
new `ChunkWriter`. The buffer is re-pushed to the new chunk. The
`ProtoLocation` array accumulates one entry per rotated chunk, ordered
by `logical_offset`.

### 6.1 Location

`ProtoLocation` (from `crowdb-protocol::chunkdb::rpc::Location`) records
which chunk holds a contiguous byte range of an object, the byte range
within that chunk, and the object-level logical offset/length so a
multi-chunk object reads back as one contiguous stream. An object
spanning N chunks has N locations ordered by logical offset, contiguous
and non-overlapping. The within-chunk offset is always 0 for the
large-object writer (dedicated chunks filled from the start); it
exists for future shared-chunk packing and range reads. Serialization
is flatbuffers via the flatbuffer `Message` trait (`encode_to_vec` / `decode`).

Edge cases:

- Empty object (size 0) → `Vec<Location>` is empty; no chunk allocated.
- `logical_length` may be < `length` in future hole-punch scenarios; the
  writer always sets them equal.
- `max_chunk_size` not a multiple of strip data capacity → rotation
  still happens at strip boundaries; the actual chunk size is the
  multiple-of-strip value at or just above the threshold.

## 7. Completion and Error Handling

`write_stream` must seal the final chunk and return the `ProtoLocation`
array on success. On error or abort it must not leak partial chunks and
must return already-sealed `ProtoLocation`s for caller cleanup.

- **Completion** — `ChunkWriter::seal()`: finish the current strip if
  it has data (spawn partial parity, no join), join all in-flight
  parity handles, `seal_chunk` RPC, return `ProtoLocation`. If the
  chunk is empty (0 bytes written), `delete_chunk` instead of
  `seal_chunk`.
- **Error / abort** (`on_error`) — cancel in-flight pipeline
  tasks, `ChunkWriter::abort()`: stop strip prefetch, abort current
  strip, drop parity handles, `delete_chunk` on the partial chunk,
  return `ProtoLocation`s of already-sealed chunks.
- **Whole-strip retry** — on a diskio write failure for any block of a
  strip, retry the whole strip: `append_chunk` a new strip with a fresh
  placement, re-write all data + parity, free the failed strip's
  segments. Up to 3 retries; on exhaustion, `IoError::WriteFailed` with
  the partial `ProtoLocation` array. The abort/cleanup paths are
  integration points for future single-block replacement.
- **Dropped writer** — dropping does not perform async metadata cleanup.
  Applications call `on_error` when abandoning a started push-mode write;
  an Active partial chunk left by process loss remains for future lifecycle
  GC.

Edge cases:

- `on_data` after `on_finish` / `on_error` → `IoError::Finished`.
- `on_finish` twice → `IoError::Finished`.
- `on_error` with no sealed chunks → `Ok(vec![])`.
- EC encode failure → pipeline aborts immediately (no retry — EC encode
  is a CPU/isal error, not a placement issue). A future refinement may
  mark the strip degraded or retry with a fallback encoder.
- `delete_chunk` fails during cleanup → log + continue (best-effort;
  the partial chunk stays Active and is reaped by a future GC task).

## 8. Interaction with Neighbors

- **chunkdb** — the writer calls allocate / append / seal / delete /
  update_chunk_strip / query via `ChunkAllocator`. chunkdb handles
  placement and lifecycle; the writer is unaware of internal placement
  logic — it receives `Segment` placements and writes to them.
- **diskio** — the writer writes data + parity blocks via
  `DiskWriter::write` and flushes via `DiskWriter::fsync`.
  `RoutedDiskWriter` owns discovery and routes every segment by disk ID to the
  unique live DiskIO owner. The fixed-connection `DiskioBlockWriter` remains a
  low-level adapter for focused fixtures.
- **crowdb-common EC** — `encode_parity_from_shards` is the shard-based +
  partial-encode entry point used by parity tasks.
- **crowdb-protocol** — `ChunkId`, `*ChunkRequest` types, `Segment`,
  `Chunk`, `Location` (`ProtoLocation`) message.

## 9. Tunables and Defaults

| Knob | Default | Role |
| --- | --- | --- |
| `max_chunk_size` | 1 GB | Chunk rotation threshold. |
| `prealloc_depth` | 2 strips | Strip preallocation ahead of write cursor. |
| `parity_depth` | 2 tasks | In-flight parity task bound. |
| `chunk_prefetch_depth` | 1 chunk | Chunk prefetch ahead of rotation. |
| fetch granularity | 1 MB | One data block per fetch call. |
| `max_cached_buffer` | 4 MB | Un-written data budget in fetch channel (one strip). |
| `memory_budget` | (pool) | `WriterPool` total; `max_concurrent = budget / per-writer-footprint`. |

## 10. Application API and Routing

`ChunkIoClient` is the application boundary. `connect` takes management seeds
and discovers ChunkDB endpoints, DiskIO registrations, hardware disks, and disk
group ownership. `prepare_large_write` starts bounded chunk preparation when
the object size and `LargeWritePolicy` become known. The single-use
`PreparedLargeWrite::write_stream` consumes an `AsyncRead` and returns
`LargeWriteResult` with locations, logical and EC-expanded physical bytes,
chunk and strip counts, elapsed time, and preparation stalls.

The write hot path reads an immutable disk-ID route snapshot through
`ArcSwap`. Refresh constructs a complete replacement off-path and publishes it
atomically. Missing ownership and duplicate owners are topology errors; the
client never chooses an arbitrary DiskIO endpoint. Connections are reused per
endpoint. The application and CLI do not construct allocators, RPC servers,
connections, chunks, strips, or parity workers.

## 11. Performance Workload

`run_large_write_benchmark` is a library-owned, deterministic, bounded-source
workload. It runs an object-count target with bounded concurrency and returns
application-independent aggregate throughput, latency, error, and preparation
stall fields. `crowdb-cli bench chunkio write` only maps arguments, starts the
standard process metrics collector, and formats the result.

The regression fixture starts three co-located logical nodes: three KV
servers, three DiskDB, three ChunkDB, and three DiskIO processes backed by
`NullDisk`. A 4+1 strip on three racks requires the local-test-only unsafe EC
placement option; disk ownership and routing remain strict. The retained logs
contain `bw_mib` when host PMU counters are available. This is observed host
memory traffic during the workload, not physical DIMM peak bandwidth and not
an application-byte estimate. Loopback TCP, EC expansion, RPC framing, kernel
copies, EC calculation, fsync, and metadata work all keep end-to-end logical
throughput below that hardware envelope.
