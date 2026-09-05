<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# ChunkDB Allocation Flow Analysis

## Scope

This analysis measures one-strip EC 8+4 chunk allocation on three co-located
logical nodes. Each node runs KV, DiskDB, and ChunkDB; the fixture has three
racks and three DiskDB data groups backed by three-replica KV groups. One
successful ChunkDB request allocates and commits twelve physical blocks.

The implementation keeps strips sequential. Within one strip it groups blocks
by data group, sends one DiskDB allocation request for four blocks to each of
the three nodes, and runs those requests concurrently. After they return,
ChunkDB persists one Active chunk record and returns success. Three grouped
block-commit requests run asynchronously after publication.

## Current Allocation Flow

For one EC 8+4 strip, the success path is:

1. ChunkDB selects twelve placements and groups them into three data-group
   requests. Each request asks one DiskDB server for four blocks on four
   different disks. The three requests run concurrently.
2. Each DiskDB server claims four bitmap ranges, constructs four
   `BusyBlockValue` records with `state=Ok` and `commit_state=Tentative`, and
   sends one four-key batch write to that data group's KV leader. After the KV
   write succeeds, DiskDB caches the tentative values by `allocation_ts` and
   returns the four segments to ChunkDB.
3. After all DiskDB responses return, ChunkDB constructs one Active chunk
   containing the twelve segments and writes that chunk record to its KV
   group. This is the allocation publication and durability boundary.
4. Once the Active chunk write succeeds, ChunkDB starts three asynchronous
   commit RPCs, refreshes its chunk cache, and returns the Active chunk to the
   client. The client does not wait for block commit.
5. Each DiskDB commit validates the physical identity, owner, and allocation
   timestamp. A tentative-cache hit supplies the value without a KV read; a
   miss reads it from KV. DiskDB changes only `commit_state=Committed`,
   batch-overwrites the four busy-block values in KV, and removes their cache
   entries after the write succeeds.

At client success, all twelve busy blocks and the referencing Active chunk are
durable. The blocks may remain Tentative until background commit completes. A
future reconciliation scanner can resolve that crash window by asking
ChunkDB whether the Active chunk references the same allocation incarnation,
then committing or freeing the block.

## Environment and Results

- Host: AMD Ryzen 9 5950X, 16 cores / 32 threads, 62 GiB RAM.
- Duration: 20 seconds per row.
- Capacity: four 4-TiB logical disks per DiskDB; 256-GiB zones.
- KV inflight/coalescing: 32/32.
- Command: `pixi run -- bash tools/bench-chunkdb-regression.sh`.

| Workload | Groups | Threads | Strips | EC | Client conn | ChunkDB conn | DiskDB conn | KV conn | Workers | Chunk/s | Block/s | p50 us | p99 us | Errors | Space |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| allocate | 3 | 1 | 1 | 8+4 | 2 | 2 | 2 | 2 | 2 | 799 | 9,588 | 1,275 | 1,561 | 0 | exact |
| allocate | 3 | 16 | 1 | 8+4 | 2 | 2 | 2 | 2 | 2 | 8,800 | 105,600 | 1,719 | 3,481 | 0 | exact |
| allocate | 3 | 128 | 1 | 8+4 | 4 | 4 | 4 | 4 | 4 | 11,914 | 142,968 | 9,945 | 22,922 | 0 | exact |
| allocate | 3 | 256 | 1 | 8+4 | 4 | 4 | 4 | 4 | 4 | 12,685 | 152,220 | 18,427 | 49,050 | 0 | exact |
| allocate | 3 | 512 | 1 | 8+4 | 4 | 4 | 4 | 4 | 4 | 12,734 | 152,808 | 37,131 | 91,641 | 0 | exact |

All rows stopped at the deadline and retained three KV, DiskDB, and ChunkDB
metrics/RPC log pairs plus the CLI pair. Clean artifacts are:

- `bench-log/chunkdb-r98-final2-20260905-223329` (all rows).
- `bench-log/chunkdb-r98-common-latency-20260905-224613` (selected row).
- `bench-log/chunkdb-r98-confirm-2-20260905-224959` (confirmation).
- `bench-log/chunkdb-r98-confirm-3-20260905-225127` (confirmation).

The selected 256-thread confirmation samples are 12,707, 12,460, and
12,634 chunks/s. Their median is 12,634 chunks/s and the maximum deviation is
1.4%, within the 10% stability bound. Every sample has zero errors and exact
space accounting.

## Bottleneck

The peak is 12,734 chunks/s, or 152,808 physical blocks/s, at 512 threads.
Increasing from 256 to 512 threads adds only 0.4% throughput while p99 grows
from 49 ms to 92 ms, so 256 threads remains the better latency/throughput
operating point.

DiskDB now retains each freshly persisted tentative `BusyBlockValue` in a
bounded per-data-group cache keyed by `allocation_ts`. Commit validates the
cached physical identity and owner, changes the state, and overwrites the KV
record without a preceding read. A miss after restart or eviction safely
falls back to KV. At 512 threads, the three DiskDB servers reported 3,153,276
cache hits, exactly 262,773 chunks times twelve blocks, and zero misses.

At 256 threads, weighted ChunkDB metrics show:

- grouped DiskDB allocation: 9.48 ms;
- the single Active metadata KV persistence: 5.08 ms;
- complete request: 18.93 ms;
- asynchronous block commit: 9.30 ms, outside response latency;
- placement: below 0.02 ms.

Each DiskDB server receives exactly one four-block allocation RPC and one
four-block commit RPC per chunk. The earlier flow sent four one-block
allocation RPCs to each node and synchronously waited for commit. Grouping and
asynchronous commit raised the 256-thread row from 8,737 to 12,860 chunks/s
(47.2%) and reduced p50 from 28 ms to 19 ms.

The response-path bottleneck is now the grouped DiskDB allocation round. At
512 threads it grows to 26.16 ms, while the single metadata write is 6.62 ms;
the asynchronous commit averages 23.72 ms. These concurrent writes share the
same KV/Paxos groups, so commit still contributes background queue pressure
even though callers no longer wait for it. Future DiskDB reconciliation must
resolve tentative blocks after a ChunkDB crash by checking whether the Active
chunk still references the exact allocation incarnation, then committing or
freeing it.

### Allocation latency attribution

At 256 threads with four connections and four RPC workers, one three-node
allocation round averages 9.48 ms. The average DiskDB `AllocateBlocks` request
is 6.02 ms, including 4.99 ms in its KV batch write. Bitmap selection is
0.003 ms and response construction is 0.007 ms. Therefore DiskDB's local
allocator is not the bottleneck: about 83% of server request latency is the
KV/Paxos write. The remaining 3.46 ms between average server latency and the
client round includes transport queueing and waiting for the slowest of three
parallel responses.

A 256-thread connection/worker sweep produced:

| Connections/workers | Chunk/s | Allocation round | DiskDB request | DiskDB KV persist | Request latency | Result |
|---:|---:|---:|---:|---:|---:|---|
| 2 | 14,129 | 10.46 ms | 7.26 ms | 6.45 ms | 16.98 ms | exact, zero errors |
| 4 | 12,860 | 9.48 ms | 6.02 ms | 4.99 ms | 18.93 ms | exact, zero errors |
| 8 | 11,252 | 10.49 ms | 6.67 ms | 5.45 ms | 21.39 ms | initial run failed accounting |

Four connections/workers minimize the isolated allocation round, but two
produce better end-to-end throughput because the single metadata write falls
from 5.08 ms to 3.92 ms despite a slower allocation round. Eight add queue
pressure rather than throughput. The first 8/8 run exposed a benchmark result
parser bug: its `busy_delta` lookup could match the suffix of
`expected_busy_delta` and incorrectly print `exact`. The parser now requires a
field boundary. A 120-second diagnostic run also identified the apparent
accounting error as verification overload: two of 256 parallel chunk queries
timed out, so their 24 valid blocks were omitted from expected space. Chunk
verification is now limited to 64 parallel queries and prints the exact failed
ID and reason.
Two subsequent 60-second stress runs were clean: 10,933 chunks/s at 256
threads and 10,882 chunks/s at 512 threads, both with zero errors and exact
space. After limiting verification concurrency, the final 20-second 256-thread
8/8 run reached 11,571 chunks/s with zero errors and exact space. This
indicates shared KV/Paxos contention, not insufficient
ChunkDB-to-DiskDB connections or workers.

### KV load

Each successful EC 8+4 chunk issues three four-key DiskDB allocation writes,
one single-key Active chunk write, and three four-key asynchronous commit
writes. The total is seven logical KV write requests and 25 key mutations per
chunk. Therefore the canonical 512-thread result of 12,734 chunks/s represents
89,138 logical KV requests/s and 318,350 key mutations/s. The latter is above
the direct KV write benchmark peak of 264,130 single-key writes/s; grouped
DiskDB writes and KV coalescing make the request and Paxos-round rates much
lower than the key-mutation rate.

## Metrics

Every ChunkDB RPC exports `request.{method}.lh`, `.inflight.g`, and `.errors.c`.
Allocation also exports placement, DiskDB round/call/retry, asynchronous block
commit/error, record-build, Active KV persistence, rollback, strip/block
count, and inflight metrics. CLI, ChunkDB, DiskDB, KV, and crowdb-rpc logs
share the timestamped regression root.
