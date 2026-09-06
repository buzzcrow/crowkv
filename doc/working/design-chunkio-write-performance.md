<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Chunk IO Write Flow and Performance (R135)

This implementation design realizes
[`R135`](../backlog/R135-chunkio-end-to-end-performance.md) against the
[chunk IO design](../design/chunkio/design-crowdb-chunkio.md), the
[DiskIO design](../design/diskio/design-crowdb-diskio.md), and the landed
system `bw_mib` collector from `2c9c811f`.

## 1. Reviewed Flow and Corrections

The source stage fills a bounded `Bytes` channel while chunk preparation runs
independently. The data stage consumes prepared strips, feeds EC state, and
submits bounded DiskIO writes. Strip completion starts parity writes; fsync for
a disk is submitted only after every data/parity write to that disk completes.
Chunk sealing joins completion work and persists the final length.

The existing implementation has four corrections:

1. `DiskioBlockWriter` converts `Bytes` to `Vec`; the DiskIO client will accept
   an owned `Bytes`/RPC buffer so payload ownership crosses the async response
   without this copy.
2. data blocks are sent serially because `EcStripWriter::push` awaits each RPC;
   a bounded per-strip set of write handles will allow independent segments to
   overlap while retaining buffers until completion.
3. parity writes and fsync tasks are siblings, so fsync can precede parity
   completion; completion will group writes by disk and sequence fsync after
   its writes.
4. chunk/strip preparation can still block transitions. Preparation remains
   bounded, but explicit stall metrics distinguish a miss from steady state.

## 2. Application-Facing Client

`crowdb-chunk-client` gains a facade that takes management seeds and transport
configuration, owns ChunkDB and DiskIO discovery, and creates prepared large
write sessions.

```rust
pub struct ChunkIoClient { /* shared discovery and transports */ }

impl ChunkIoClient {
    pub async fn connect(config: ChunkIoClientConfig) -> Result<Self>;
    pub async fn prepare_large_write(
        &self,
        object_size: Option<u64>,
        policy: LargeWritePolicy,
    ) -> Result<PreparedLargeWrite>;
}

impl PreparedLargeWrite {
    pub async fn write_stream<R: AsyncRead + Unpin + Send>(
        self,
        source: R,
    ) -> Result<LargeWriteResult>;
}
```

`LargeWriteResult` contains locations, logical/physical bytes, chunks, strips,
and preparation-stall observations. Existing low-level writer constructors stay
available for tests. No CLI type enters the library.

## 3. DiskIO Routing

A routing snapshot maps each disk ID to exactly one live DiskIO endpoint using
hardware disk-group ownership plus `diskio` service registrations. Each
endpoint owns a bounded connection pool attached to a shared `DiskioClient`.
Lookup is read-only on the write path. Refresh builds a new immutable snapshot
and publishes it atomically; no write-path lock is introduced.

Unknown or duplicate ownership returns a topology error. A failed send may
refresh discovery, but an acknowledged-or-ambiguous data write is not replayed
by topology code. The existing one-connection writer remains usable only as a
test adapter.

## 4. Simple E2E Fixture

The existing `crowdb-test-harness` fixture stays deliberately small: one KV
server hosting groups 0 and 1, one DiskDB, one ChunkDB, and one DiskIO with
`NullDisk`. Tests construct only `ChunkIoClient`, prepare a known-size write,
and stream deterministic data. They verify locations, chunk state, strip count,
busy-space delta, and completion ordering. The current hang is diagnosed from
the first pending operation; timeouts remain failure guards, not retries.

## 5. Distributed Benchmark Fixture

`local_deploy_combined` adds one DiskIO per node after hardware and DiskDB
provisioning and before ChunkDB readiness is accepted. The topology is three
racks, three nodes, three KV servers, three replicated data groups, three
DiskDB, three ChunkDB, three DiskIO, and one disk group per node. DiskIO uses
`--dummy-disk null`, group-0 discovery, and per-node ownership.

The deploy summary and persisted console configuration include DiskIO
instances so destroy and log validation cover all twelve services.

## 6. Benchmark Ownership and Metrics

Reusable workload types live in `crowdb-chunk-client`; the CLI supplies parsed
values and formats the returned aggregate. A deterministic async source emits
bounded blocks without allocating an object-sized buffer. The initial workload
supports large writes only.

The result reports logical and EC-expanded bytes/s, objects/s, p50/p99,
preparation stalls, writes/fsyncs, errors, stop reason, CPU, RSS, TCP counters,
and `bw_mib`. System collectors already start per process; the sentinel retains
their logs and summarizes workload-window samples. Since the DRAM counter is
system-scoped, multiple processes may report the same host bandwidth; the
summary must not add those values. It reports average/peak from one nominated
collector and keeps all raw samples for diagnosis.

## 7. Regression Sentinel

`tools/bench-chunkio-write-regression.sh` builds the CLI and four service
binaries, deploys a clean fixture per case, runs bounded object-size and
concurrency cases, validates accounting and all service logs, and destroys the
fixture. It preserves later cases after failure and exits non-zero at the end.

## Scope

- `lib/crowdb-chunk-client/`: facade, preparation, routing, results, pipeline
  ordering, copy reduction, metrics, and tests.
- `lib/crowdb-diskio-client/`: owned payload API without `Bytes` to `Vec` copy.
- `lib/crowdb-test-harness/`: simple public-API E2E fixture support.
- `lib/crowdb-console-shared/`: three-node DiskIO lifecycle and configuration.
- `app/crowdb-cli/`: thin benchmark verb and result formatting.
- `tools/bench-chunkio-write-regression.sh`: retained regression workflow.
- `doc/design/chunkio/design-crowdb-chunkio.md`: reviewed permanent flow/API.
- `doc/design/diskio/design-crowdb-diskio.md`: routed client/payload ownership.

## Complexity

High. The work crosses Rust/C++ RPC ownership, async completion ordering,
service discovery, multi-process lifecycle, and performance attribution. The
main risks are buffer lifetime across FFI, duplicate/misdirected writes during
refresh, and preserving bounded memory while increasing concurrency.

## Test Design

- Mock delayed writes and parity -> seal -> assert fsync follows all writes and
  buffers remain bounded. Invariant: durability ordering. Unit test.
- Mock allocation lag -> stream -> assert ordered data and recorded stall.
  Invariant: preparation misses become explicit backpressure. Unit test.
- Missing/duplicate disk owner -> route -> assert topology error without send.
  Invariant: no arbitrary routing. Integration test.
- One-node real stack -> public API writes multi-strip and rotating objects ->
  assert seal, locations, strips, and space. Invariant: simple full flow. E2E.
- Three-node stack -> cross-group EC write -> assert each DiskIO owns its
  requests and accounting is exact. Invariant: distributed routing. E2E.
- Benchmark result/log parsing -> assert throughput, latency, `bw_mib`, errors,
  stop reason, and twelve service logs. Invariant: attributable sample.

## Module Structure

```text
lib/crowdb-chunk-client/src/
├── client.rs                 # application-facing ChunkIoClient
├── client/
│   ├── discovery.rs          # immutable topology refresh
│   └── large_write.rs        # prepared session and result
├── disk_io/
│   ├── disk_writer.rs        # test adapter and trait
│   └── routing.rs            # disk owner snapshot + pools
└── benchmark.rs              # reusable workload runner

app/crowdb-cli/src/commands/bench/
└── chunkio.rs                # argument adapter and formatter
```

## Config Extensions

Client configuration adds management seeds, per-endpoint connections/workers,
refresh interval, and preparation/write concurrency. Combined deployment adds
DiskIO instance count, dummy type fixed to `null` for this sentinel, DiskIO
workers, and metrics interval.

## Server Wiring

DiskIO is launched through console-shared lifecycle on the node owning its disk
group. It registers its RPC endpoint and disk-group ownership in group 0. The
chunk client builds routing only after registrations and hardware topology
agree.
