<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: chunkdb (Overview)

This is the root design document for the chunkdb component area. It
defines **what chunkdb is**, **why key choices were made**, and **how the
component is structured**. Field-level details live in the proto files
and Rust source; this doc covers decisions and architecture only.

---

## Table of Contents

- [1. Overview](#1-overview)
- [2. Non-Goals (Design Envelope)](#2-non-goals-design-envelope)
- [3. Key Design Decisions](#3-key-design-decisions)
  - [3.1 Chunk as logical abstraction over disk blocks](#31-chunk-as-logical-abstraction-over-disk-blocks)
  - [3.2 Strip as atomic redundancy unit](#32-strip-as-atomic-redundancy-unit)
  - [3.3 Rack/node-aware placement for fault tolerance](#33-racknode-aware-placement-for-fault-tolerance)
  - [3.4 Topology cache with group-0 integration](#34-topology-cache-with-group-0-integration)
- [3.4a Watch/notify for real-time updates](#34a-watchnotify-for-real-time-updates)
  - [3.5 EC at strip level via isa-l (crowdb-common module)](#35-ec-at-strip-level-via-isa-l-crowdb-common-module)
  - [3.6 Stateless with KV persistence](#36-stateless-with-kv-persistence)
  - [3.7 Common protocol crate; crowdb-rpc](#37-common-protocol-crate-crowdb-rpc)
  - [3.8 Proto types used directly; no Rust type duplication](#38-proto-types-used-directly-no-rust-type-duplication)
  - [3.9 Chunk types for different use cases](#39-chunk-types-for-different-use-cases)
- [4. Architecture Overview](#4-architecture-overview)
- [5. Data Model](#5-data-model)
  - [5.1 Disk block](#51-disk-block)
  - [5.2 Strip](#52-strip)
  - [5.3 Chunk](#53-chunk)
  - [5.4 Chunk ID structure](#54-chunk-id-structure)
  - [5.4a Logical hash bucket system](#54a-logical-hash-bucket-system)
  - [5.4b Request handling during migration](#54b-request-handling-during-migration)
  - [5.5 Chunk types](#55-chunk-types)
- [6. Topology Management](#6-topology-management)
- [7. Placement Strategy](#7-placement-strategy)
  - [7.1 Mirror placement](#71-mirror-placement)
  - [7.2 EC placement](#72-ec-placement)
- [8. Allocation Flow](#8-allocation-flow)
- [9. Chunk Lifecycle](#9-chunk-lifecycle)
- [10. Per-Chunk-ID Lifecycle Lock + Chunk Cache](#10-per-chunk-id-lifecycle-lock--chunk-cache)
  - [10.1 ChunkLockMap](#101-chunklockmap)
  - [10.2 Lock policy + cache hint](#102-lock-policy--cache-hint)
  - [10.3 ChunkGuard](#103-chunkguard)
  - [10.4 Chunk cache](#104-chunk-cache)
  - [10.5 Sweep task](#105-sweep-task)
  - [10.6 LifecycleHandler integration](#106-lifecyclehandler-integration)
  - [10.7 Error variants + service mapping](#107-error-variants--service-mapping)
  - [10.8 Metrics + HTTP endpoints](#108-metrics--http-endpoints)
  - [10.9 Edge cases](#109-edge-cases)
- [11. EC Encoding/Decoding](#11-ec-encodingdecoding)
- [12. Crate Layout](#12-crate-layout)
- [13. Concurrency Model](#13-concurrency-model)
- [14. Configuration](#14-configuration)
- [15. Implementation Scope](#15-implementation-scope)
- [16. References](#16-references)

## 1. Overview

chunkdb is a **distributed chunk metadata service** that runs on top of
CROWDB's KV cluster and diskdb. It is a **lightweight, stateless server**:
a chunkdb instance manages chunk metadata, orchestrates block allocation
from diskdb, and persists all durable state to CROWDB KV. It holds no
state that cannot be reconstructed from KV — on crash or restart it
rebuilds in-memory structures from the KV records and group-0 metadata.

Multiple chunkdb instances run across a cluster for fast chunk
allocation and lifecycle management. chunkdb does rack/node-aware
placement for fault tolerance, supports both mirror and EC strip types,
and manages chunk state transitions (allocate → seal → delete). All
state changes are durably persisted to CROWDB KV before being acknowledged
to callers.

chunkdb **manages chunk metadata**; it does **not** perform data I/O.
Callers (a future object store, chunkio service) write to the
allocated disk blocks themselves and tell chunkdb when chunks are sealed
or deleted.

**Language:** Rust. **Runtime:** tokio (async everywhere).

**Core goals:**
- **Fast allocation** — rack/node-aware placement with parallel block
  allocation via diskdb, rollback on partial failure.
- **Fault tolerance** — mirror strips across distinct racks, EC strips
  with rack-aware placement, configurable safe/unsafe modes.
- **Durability via CROWDB KV** — the paxos journal is the sole durable
  store; chunkdb has no local WAL and is stateless on disk.
- **Crash safety via KV replay** — chunk metadata in KV is the source of
  truth; in-memory structures (topology cache, allocator state) are rebuilt
  on restart.
- **Flexible EC** — support multiple EC schemes simultaneously, selected
  per-strip based on cluster size and fault tolerance requirements.

**Design philosophy:** "chunkdb is a thin, stateless client of crowdb-kv and
diskdb." All consensus, replication, and durability are delegated to
crowdb-kv; block allocation is delegated to diskdb. chunkdb's job is chunk
lifecycle management, placement policy, and EC coordination, nothing more.

## 2. Non-Goals (Design Envelope)

- **No data I/O.** chunkdb allocates blocks and manages chunk metadata; it
  does not read/write block contents. A future diskio-like component does
  data I/O.
- **No local WAL.** CROWDB KV's WAL is the sole durability mechanism.
- **No consensus code.** chunkdb is a client of crowdb-kv; all interaction
  uses the existing crowdb-kv API.
- **No GC operations in v1.** Garbage collection (reclaim, collapse, merge)
  is deferred to a future requirement. Chunks are allocated and deleted as
  whole units in v1.
- **No mirror-to-EC conversion in v1.** Background conversion of mirror
  strips to EC strips (for shared chunks) is deferred to a future requirement.
- **No EC strip type restrictions.** Chunk type (repo/WAL/btree-page/page-index)
  is independent of strip type (mirror/EC). Any chunk can use either strip type
  based on configuration. v1 supports both mirror and EC strips for all chunk types.
- **No native SMR / zoned-namespace SSD support (v1).** Start with
  conventional HDD/SSD via diskdb; advanced disk features are deferred.

## 3. Key Design Decisions

### 3.1 Chunk as logical abstraction over disk blocks

A **chunk** is a logical container for strips, which in turn contain
physical disk blocks. This abstraction layer allows upper-layer services
to work with chunks without worrying about physical placement details.
chunkdb handles the mapping from logical chunk offsets to physical disk
blocks across multiple nodes and racks.

**Rationale:** Separates logical data management from physical storage
layout. Upper layers (object store, file system) work with chunks; chunkdb
handles the complexity of block allocation, placement, and fault tolerance.

### 3.2 Strip as atomic redundancy unit

A **strip** is the atomic unit of redundancy. Every strip is either a
**mirror strip** (replicated across multiple nodes) or an **EC strip**
(erasure-coded with data + parity blocks). Strip-level operations
(allocation, EC encoding, recovery) are atomic. A strip is either fully
allocated or not at all.

**Rationale:** Simplifies recovery and placement logic. When a disk fails,
recovery operates at strip level (rebuild lost replica or EC parity),
not at individual block level.

### 3.3 Rack/node-aware placement for fault tolerance

Placement decisions are rack-aware and node-aware to survive rack failures
and node failures. Mirror strips place replicas on distinct racks when
possible. EC strips distribute data+parity blocks across multiple racks,
with per-node block limits to ensure no single node failure exceeds the
code_num threshold.

**Rationale:** Rack failures are more common than individual disk failures.
Rack-aware placement ensures the system can survive rack-level failures
without data loss.

### 3.4 Topology cache with group-0 integration

chunkdb maintains a **TopologyCache** with site/rack/node/disk-group
hierarchy, refreshed periodically from group-0 via `HardwareClient`. This
cache is used for placement decisions and is point-in-time consistent for
each allocation operation.

**Rationale:** Placement decisions need up-to-date cluster topology.
Querying group-0 for every allocation would be too slow. Caching with
periodic refresh provides a good balance between freshness and performance.

### 3.4a Watch/notify for real-time updates

chunkdb uses crowdb-kv's watch/notify mechanism for real-time topology updates
in addition to periodic refresh:

- **Watch registration**: chunkdb registers for group-0 watch on disk-group
  and node status keys via the crowdb-kv watch/notify client.
- **Immediate updates**: A notification coalesces into a complete group-0
  topology rebuild. The immutable snapshot is published with one `ArcSwap`
  operation, so allocation never observes a mixture of generations.
- **Fallback mechanism**: Periodic refresh (30s default) continues as a fallback
  for missed notifications and consistency verification.
- **Placement impact**: Real-time updates enable placement decisions to reflect
  current cluster state, avoiding placement on failed/degraded resources.

**Rationale**: Watch/notify provides sub-second responsiveness to critical
cluster state changes (disk failures, maintenance) while periodic refresh
ensures eventual consistency and recovery from missed notifications.

### 3.5 EC at strip level via isa-l (crowdb-common module)

Erasure coding is performed at the strip level using the **isa-l** library
(Intel Storage Acceleration Library). A Rust FFI wrapper in `crowdb-common`
provides safe interfaces to isa-l's encode/decode functions. EC encoding
happens when strips are allocated; EC decoding happens during recovery.

**Rationale:** isa-l is the industry standard for high-performance EC.
Strip-level EC aligns with the strip-as-atomic-redundancy-unit design.
The FFI wrapper isolates unsafe C code from the rest of the Rust codebase.

### 3.6 Stateless with KV persistence

chunkdb is stateless on disk. All chunk metadata is persisted to CROWDB KV.
On restart, chunkdb rebuilds in-memory structures (topology cache,
allocator state) by replaying KV records and fetching fresh topology from
group-0.

**Rationale:** Simplifies operations (no local data to manage), enables
crash recovery without local WAL, and relies on CROWDB KV's durability
guarantees. Stateless design also makes scaling out easier. New instances
can start without data migration.

### 3.7 Common protocol crate; crowdb-rpc

Protocol definitions live in `lib/crowdb-protocol/src/fbs/chunkdb_*.fbs`
and are shared between chunkdb server and chunkdb client. Communication
uses crowdb-rpc (flatbuffer transport), the same engine as the KV
consensus hot path. See sub-design
[`design-crowdb-chunkdb-rpc.md`](design-crowdb-chunkdb-rpc.md).

**Rationale:** Single source of truth for protocol definitions. crowdb-rpc
uses the same engine as the KV consensus hot path for lower latency.

### 3.8 Proto types used directly; no Rust type duplication

Rust code uses the proto-generated types directly from `crowdb-protocol`.
No separate Rust struct definitions mirror the proto types. Conversion
between proto and domain types happens only at boundaries (e.g., KV
persistence).

**Rationale:** Reduces duplication and drift. Proto is the canonical
definition; Rust code works with proto types directly.

### 3.9 Chunk types for different use cases

Four chunk types are defined for CROWDB's storage hierarchy:
- **Repo chunk**: User data storage.
- **WAL chunk**: Write-ahead log entries.
- **BTree page chunk**: B-tree page storage for the crowdb-tree engine.
- **Page index chunk**: Page index metadata.

**Rationale:** Different storage components have different redundancy and
performance requirements. Chunk types allow optimization for each component's
needs. Chunk type is independent of strip type. Any chunk type can use
either mirror or EC strips based on configuration and access patterns.

## 4. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                         chunkdb Server                          │
├─────────────────────────────────────────────────────────────────┤
│  crowdb-rpc Service (ChunkdbService)                              │
│  ├── AllocateChunk                                              │
│  ├── SealChunk                                                   │
│  ├── DeleteChunk                                                 │
│  ├── QueryChunk                                                   │
│  └── ListChunk                                                   │
├─────────────────────────────────────────────────────────────────┤
│  Lifecycle Layer                                                 │
│  ├── allocate_chunk()                                            │
│  ├── seal_chunk()                                                │
│  └── delete_chunk()                                              │
├─────────────────────────────────────────────────────────────────┤
│  Allocation Layer                                                │
│  ├── ChunkAllocator (orchestrate strip allocation)              │
│  ├── MirrorStripAllocator                                        │
│  └── ECStripAllocator                                            │
├─────────────────────────────────────────────────────────────────┤
│  Placement Layer                                                 │
│  ├── RackNodeSelector                                            │
│  ├── MirrorPlacement (distinct racks)                            │
│  └── ECPlacement (rack-aware, safe/unsafe)                       │
├─────────────────────────────────────────────────────────────────┤
│  Topology Layer                                                  │
│  ├── TopologyCache (site/rack/node/disk-group)                  │
│  ├── TopologyRefresh (periodic from group-0)                     │
│  └── WatchNotify (real-time updates from group-0)                │
├─────────────────────────────────────────────────────────────────┤
│  Storage Layer                                                   │
│  ├── KV Persistence (chunk metadata)                            │
│  └── DiskdbClient Pool (block allocation/free)                   │
├─────────────────────────────────────────────────────────────────┤
│  EC Layer                                                        │
│  └── crowdb-common EC (isa-l FFI wrapper)                        │
└─────────────────────────────────────────────────────────────────┘
         │                    │                    │
         ▼                    ▼                    ▼
    ┌─────────┐         ┌─────────┐         ┌─────────┐
    │ crowdb-kv │         │ diskdb  │         │ group-0 │
    │ (KV)    │         │         │         │Hardware │
    └─────────┘         └─────────┘         └─────────┘
```

**Key components:**

- **crowdb-rpc Service**: External API for chunk operations.
- **Lifecycle Layer**: Implements chunk state machine transitions.
- **Allocation Layer**: Orchestrates strip allocation with rollback on failure.
- **Placement Layer**: Rack/node-aware placement algorithms.
- **Topology Layer**: Caches and refreshes cluster topology from group-0.
- **Storage Layer**: KV persistence for chunk metadata, diskdb client pool.
- **EC Layer**: isa-l wrapper for strip-level EC operations.

## 5. Data Model

### 5.1 Disk block

A **disk block** is the smallest fixed-size physical storage unit managed
by diskdb. The default size is **1 MB** (configurable, range 1 MB – 2 MB).
Disk blocks are identified by `Segment { node_id, disk_id, zone_index,
zone_offset, size, tag }` (from diskdb proto).

### 5.2 Strip

A **strip** is the atomic redundancy unit. Two strip types:

**Mirror Strip**: One disk block capacity, replicated across N nodes
(configurable copy count, default 3). Each replica is a full copy on a
different node. Data capacity = 1 × disk_block_size.

**EC Strip**: `data_num` data blocks + `code_num` parity blocks,
distributed across different nodes. Data capacity = `data_num ×
disk_block_size`. For example:
- 6+3 EC with 1 MB blocks → 6 MB data capacity, 9 MB total.
- 8+4 EC with 1 MB blocks → 8 MB data capacity, 12 MB total.

Each strip tracks:
- **Usage bitmap**: Per-strip bitmap tracking which sub-block ranges are
  written (1 bit per granularity, e.g., 16 KB).
- **EC state** (EC strips only): `NotStarted` → `Complete`.
- **Block state**: Per-block health (Good, Suspicious, Bad).

### 5.3 Chunk

A **chunk** is a container for strips. Chunk properties:
- **128-bit ID**: Chunk type (8 bits) + Timestamp (48 bits) + Randomness (72 bits).
- **State**: `Init` → `Active` → `Sealed` → `Deleted`.
- **Type**: Metadata, Shared, Specific.
- **Capacity**: Total data capacity across all strips.
- **Write granularity**: Minimum write alignment (e.g., 4 KB).
- **Strips**: Ordered list of strips (mirror or EC).
- **Logical-to-physical mapping**: Encoded offset arrays for GC (deferred).

Chunk size is variable, determined by the total size of its constituent
strips (1 MB – 4 GB range).

### 5.4 Chunk ID structure

The 128-bit chunk ID uses a custom format optimized for chunkdb:

| Bits    | Width  | Field    | Description                          |
|---------|--------|----------|--------------------------------------|
| 0–7     | 8 bits | Chunk Type | Chunk type identifier (0-255)       |
| 8–55    | 48 bits| Timestamp | Milliseconds since Unix epoch        |
| 56–127  | 72 bits| Randomness | Cryptographically secure random      |

**Layout benefits:**
- **Chunk type prefix**: First byte enables efficient filtering and scanning by chunk type
- **Time ordering**: 48-bit timestamp provides natural ordering for sequential operations
- **Global uniqueness**: 48-bit timestamp + 72-bit randomness provides sufficient uniqueness
- **Compact size**: 128 bits total, same as standard UUID

**Generation**: Chunk IDs are generated using `getrandom` for cryptographically secure randomness combined with system timestamp. The generation function will be added to `crowdb-common` for reuse across components.

**Chunk type values (8 bits, 0-255):**
- 0: Repo chunk (user data)
- 1: WAL chunk
- 2: BTree page chunk
- 3: Page index chunk
- 4-255: Reserved for future use

The chunk ID is hashed to a **logical hash bucket** (16-bit bucket ID, 0-65535),
which is then mapped to a physical KV group via a binding table stored in group-0.
This two-level approach allows KV group topology changes without chunk data migration.

### 5.4a Logical Hash Bucket System

**Two-level routing design:**
1. **Chunk ID → Logical Bucket**: Hash chunk ID (128-bit) to a 16-bit logical bucket ID (0-65535)
2. **Logical Bucket → Physical KV Group**: Consult group-0 binding table to map bucket to KV group

**Benefits:**
- **Predictable migration**: Only chunks in affected bucket ranges need migration when KV groups change
- **Deterministic queries**: Chunk ID always hashes to same logical bucket, then consults current mapping
- **Gradual rebalancing**: Update mappings incrementally to move load between KV groups
- **Independent scaling**: Logical bucket space (65536 buckets) can map to any number of physical KV groups

**Hash function:**
```
logical_bucket = hash(chunk_id) % 65536
```
Using a fast, uniform hash function (e.g., xxHash, FarmHash) on the 128-bit chunk ID.

**Group-0 binding table schema:**
```
LogicalBucketBindingValue:
  bucket_range_start: uint16  # Start of bucket range (inclusive)
  bucket_range_end: uint16    # End of bucket range (exclusive)  
  kv_group_id: uint16         # Target physical KV group
  version: uint64             # Binding version for cache invalidation
```

Stored in group-0 with key pattern: `chunkdb_bucket_binding:<range_start>`

Example mapping:
- Buckets 0-16383 → KV group 1
- Buckets 16384-32767 → KV group 2
- Buckets 32768-49151 → KV group 3
- Buckets 49152-65535 → KV group 4

**Query flow:**
1. Hash chunk ID → logical bucket (e.g., 25000)
2. Consult group-0 binding table (cached locally) → KV group 2
3. Query chunk metadata from KV group 2 using chunk ID as key

**Adding/removing KV groups:**
- To add KV group: Split an existing bucket range, update binding table in group-0
- To remove KV group: Merge its bucket range into neighboring ranges, update binding table
- **Chunk record migration required**: When bucket mappings change, affected chunk
  records must be physically moved from old KV group to new KV group
- Migration strategy: Background task copies chunk records to new KV group,
  then deletes from old KV group after successful copy

**Load rebalancing:**
- Gradually shift bucket range boundaries to balance load across KV groups
- Example: If KV group 2 is overloaded, move some buckets to KV group 3 by updating ranges
- Changes take effect immediately as chunkdb servers refresh their binding cache

**Instance sharding:** The bucket space is also used to shard chunkdb
instances. Each instance owns a range of buckets and rejects requests
for chunks outside its range. See sub-design
[`design-crowdb-chunkdb-range-binding.md`](design-crowdb-chunkdb-range-binding.md).

### 5.4b Request Handling During Migration

When chunk records are being migrated from old KV group to new KV group, requests
must be handled correctly to avoid data loss or inconsistencies.

**Migration phases:**
1. **Pre-migration**: Chunk exists only in old KV group
2. **Copying**: Chunk being copied from old KV group to new KV group
3. **Cutover**: Chunk exists in both KV groups (new location authoritative)
4. **Cleanup**: Old copy deleted from old KV group

**Request handling strategies:**

**Option 1: Dual-write during migration (Recommended)**
- During migration: Write to both old and new KV groups
- Reads: Try new KV group first, fall back to old KV group if not found
- After migration complete: Delete from old KV group, switch to single-location reads
- Pros: No data loss, minimal read latency impact
- Cons: Temporary write amplification during migration

**Option 2: Version-based routing**
- Add migration state to binding table: `in_migration`, `migration_complete`
- During migration: Route all requests to old KV group
- After copy complete: Update binding table to route to new KV group
- Pros: Simple routing logic, no dual writes
- Cons: Longer migration window, old KV group handles all traffic

**Option 3: Background copy with read repair**
- Background task copies to new KV group
- Reads: Always try new KV group first, fall back to old if not found
- When chunk found in old location during migration: trigger copy to new location
- Pros: Eventual consistency, can start serving from new location immediately
- Cons: Read latency spikes during migration, complex coordination

**Recommended approach (Option 1 - Dual-write):**
```rust
fn read_chunk(chunk_id: ChunkId) -> Result<Chunk> {
    let bucket = hash_to_bucket(chunk_id);
    let kv_group = get_binding(bucket);
    
    // Try new location first
    match kv_read(kv_group, chunk_id) {
        Ok(chunk) => return Ok(chunk),
        Err(_) => {
            // Fall back to old location if in migration
            if is_bucket_in_migration(bucket) {
                let old_kv_group = get_old_binding(bucket);
                return kv_read(old_kv_group, chunk_id);
            }
            return Err(NotFound);
        }
    }
}

fn write_chunk(chunk: Chunk) -> Result<()> {
    let bucket = hash_to_bucket(chunk.id);
    let kv_group = get_binding(bucket);
    
    // Write to new location
    kv_write(kv_group, &chunk)?;
    
    // Dual-write during migration
    if is_bucket_in_migration(bucket) {
        let old_kv_group = get_old_binding(bucket);
        kv_write(old_kv_group, &chunk)?;
    }
    
    Ok(())
}
```

**Migration state tracking in group-0:**
```
BucketMigrationState:
  bucket_range_start: uint16
  bucket_range_end: uint16
  old_kv_group: uint16
  new_kv_group: uint16
  state: enum { NotMigrating, Copying, Cutover, Cleanup, Complete }
  version: uint64
```

### 5.5 Chunk types

| Type          | Chunk Type Value | Description                          |
|---------------|------------------|--------------------------------------|
| Repo          | 0                | User data storage                    |
| WAL           | 1                | Write-ahead log entries              |
| BTree page    | 2                | B-tree page storage                  |
| Page index    | 3                | Page index metadata                  |
| Reserved      | 4-255            | Reserved for future use              |

**Note:** Chunk type is independent of strip type. Any chunk type can use
either mirror or EC strips based on configuration and requirements.

**Metadata chunks**: Contain only mirror strips for maximum redundancy and
fast access. Used for critical metadata.

**Repo chunks**: Used for user data storage. Can use either mirror or EC strips
based on configuration and access patterns.

**WAL chunks**: Used for write-ahead log entries. Typically use mirror strips
for durability and fast recovery, but EC is possible for space optimization.

**BTree page chunks**: Used for B-tree page storage in the crowdb-tree engine.
Can use mirror or EC strips based on performance vs space trade-offs.

**Page index chunks**: Used for page index metadata. Typically use mirror strips
for fast lookup, but EC is possible for large indexes.

## 6. Topology Management

chunkdb maintains a **TopologyCache** with the cluster hierarchy:

```
Site (data center)
  └── Rack (physical rack)
      └── Node (server)
          └── Disk-Group (logical disk group)
              └── Disk (physical disk)
```

**TopologySnapshot**: Point-in-time immutable snapshot containing:
- `sites`: Map<site_id, SiteInfo>
- `racks`: Map<rack_id, RackInfo>
- `nodes`: Map<node_uuid, NodeInfo>
- `disk_groups`: Map<disk_group_id, DiskGroupInfo>

**TopologyRefresh**: Hybrid approach combining periodic refresh with watch/notify:
- **Periodic refresh**: Background task that periodically (configurable,
  default 30s) fetches full topology from group-0 via `HardwareClient` for
  consistency and missed notification recovery.
- **Watch/notify integration**: Register for group-0 watch on disk-group and
  node status keys via crowdb-kv watch/notify client. Notifications coalesce
  into a complete refresh; no point mutation is published.
- Fetch all nodes with status and rack assignment
- Fetch all disk-groups with capacity and node assignment
- Fetch all racks with site assignment
- Atomically update the cache

**Rationale**: Placement decisions need consistent topology. Point-in-time
snapshots ensure a single allocation operation sees a consistent view.
Periodic refresh balances freshness and performance.

## 7. Placement Strategy

### 7.1 Mirror placement

Mirror placement aims to place replicas on **distinct racks** to survive
rack failures:

1. Select N nodes (copy count) from online nodes
2. Prefer nodes on different racks
3. If not enough distinct racks, place multiple copies on same rack
4. For each node, select the disk-group with most free space
5. Return placement plan with node/disk-group assignments

**Negative hints**: Nodes can be excluded from placement (e.g., during
recovery to avoid re-using failed nodes).

**Example**: 3-copy mirror on 3-rack cluster → 3 replicas on 3 distinct racks.
On 2-rack cluster → 2 replicas on distinct racks, 1 replica on either rack.

### 7.2 EC placement

EC placement distributes data+parity blocks **across multiple racks**
with per-node block limits:

1. Calculate total blocks = data_num + code_num
2. Select nodes such that:
   - No node has > code_num blocks (safe mode)
   - Blocks are distributed across ≥3 racks when possible
3. For each node, select disk-group with most free space
4. Return placement plan with block assignments

**Safe mode**: Ensures no single node failure exceeds code_num, guaranteeing
recoverability. Requires enough nodes to satisfy constraints.

**Unsafe mode**: Relaxes per-node limits when cluster is too small. It is
disabled by default and requires the explicit `placement.allow_unsafe_ec`
server setting. Insufficient topology otherwise returns a typed placement
error without allocating blocks.

**Example**: 8+4 EC on 12-node cluster → 12 blocks across ≥3 racks, max 4
blocks per node. On 3-node cluster (unsafe mode) → 12 blocks, 4 per node.

## 8. Allocation Flow

**AllocateChunk** operation:

1. Generate 192-bit chunk ID (metadata + UUID + reserved)
2. Fetch topology snapshot from TopologyCache
3. Calculate strip layout based on requested capacity and strip type
4. For each strip:
   - Call placement selector to get node/disk-group assignments
   - Group placements by DiskDB data group and allocate each group in parallel
   - On failure: rollback (free all allocated blocks), return error
5. Persist the complete chunk metadata with state = Active
6. Start grouped DiskDB block commits asynchronously
7. Refresh the chunk cache and return the Active chunk to the caller

**Parallel allocation**: Strips remain sequential. Within a strip, ChunkDB
sends one request per DiskDB data group and runs those requests concurrently.
DiskDB persists each allocated busy block as Tentative before responding.

**Success boundary**: The Active chunk and every referenced Tentative busy
block are durable before ChunkDB returns success. DiskDB commit overwrites
each busy block as Committed after the response. A reconciliation scanner can
later resolve a crash in this interval from the Active chunk reference and the
allocation incarnation.

**Rollback**: If allocation or Active metadata persistence fails, every known
segment from every prior strip is freed through its exact DiskDB group before
returning. A failed rollback is surfaced. Background commit failure is
reported by metrics and logs for later reconciliation; it does not retract an
already durable Active chunk.

## 9. Chunk Lifecycle

```
Init ──> Active ──> Sealed ──> Deleted
```

| State   | Description                                                  |
|---------|--------------------------------------------------------------|
| Init    | Reserved transient state; the allocation path publishes Active directly. |
| Active  | Chunk is open for writes. Strips can be appended. Returned by AllocateChunk. |
| Sealed  | Chunk is read-only. Records final length and seal timestamp. |
| Deleted | Durable cleanup intent; retained segments still need freeing. |

**State transitions**:
- `Active → Sealed`: Via `SealChunk` RPC. Validates state, updates sealed_length.
- `Active → Deleted`: Persist cleanup intent, free segments, persist tombstone.
- `Sealed → Deleted`: Persist cleanup intent, free segments, persist tombstone.
- Invalid transitions (e.g., `Sealed → Active`) return errors.

**Concurrency**: An approved bounded `tokio::Mutex` per chunk serializes the
read-modify-write lifecycle. Locks for different chunks are independent and
tests prove cross-chunk progress. Immutable topology, routing, range, endpoint,
and disk-group reverse-map snapshots remain lock-free on their read paths.

## 10. Per-Chunk-ID Lifecycle Lock + Chunk Cache

`LifecycleHandler`'s six mutating RPCs (allocate/append/seal/delete/
delete-range/update-strip) each perform a read-modify-write cycle on a
chunk record. Without per-chunk serialization, two concurrent
`AppendChunk` RPCs on the same chunk ID both read the chunk, both
append strips, both `put_chunk`. The second overwrite loses the
first's strips. A per-chunk mutex serializes the RMW cycle. A payload
cache avoids the `get_chunk` store round-trip on every mutating RPC
(the latest chunk is known in-process right after the previous
`put_chunk`).

The lock and the payload have different eviction requirements (lock:
evict only when uncontended; payload: evict freely by
recency/frequency), so they are separate structures. See
[`design-crowdb-chunkdb-range-binding.md`](design-crowdb-chunkdb-range-binding.md)
for the range guard that ensures a chunk reaches exactly one chunkdb
instance. The per-chunk lock assumes that one-owner invariant.

### 10.1 ChunkLockMap

`app/crowdb-chunkdb/src/lifecycle.rs`:

```rust
pub struct ChunkLockMap {
    locks: DashMap<ChunkId, Arc<Mutex<()>>>,
    chunks: Arc<quick_cache::Cache<ChunkId, Chunk>>,
    metrics: Arc<LifecycleMetrics>,
    hold_warn_threshold: Duration,
}
```

- `new(cache_capacity, metrics, hold_warn_threshold) -> Self` — creates
  an empty `DashMap` and a `Cache::new(cache_capacity)`.
- `acquire(&self, chunk_id, store, policy, hint) -> Result<ChunkGuard,
  LifecycleError>` — for existing chunks (append/seal/delete). Steps:
  1. `entry().or_default()` to get-or-create the `Arc<Mutex<()>>`.
  2. Record lock-wait start time.
  3. Acquire the mutex per `policy`:
     - `TryLock` → `try_lock_owned()`. On `Err`, increment
       `lock_busy_count`, return `LockBusy`.
     - `Wait(d)` → `lock_owned()` with `tokio::time::timeout(d, ...)`.
       On timeout, increment `lock_timeout_count`, return
       `LockTimeout`.
  4. Record lock-wait time into the histogram.
  5. Serve payload: if `hint == Cache` and `self.chunks.get(&chunk_id)`
     returns `Some(chunk)` → cache hit (increment `cache_hit_count`),
     return guard with that chunk. Otherwise → cache miss (increment
     `cache_miss_count`), `store.get_chunk(chunk_id)`. On
     `StoreError::ChunkNotFound` → return `ChunkNotFound`. On success,
     if `hint == Cache`, `self.chunks.insert(chunk_id, chunk.clone())`.
  6. Return `ChunkGuard` with the chunk, hint, hold_start, metrics.
- `acquire_for_create(&self, chunk_id, policy, hint) -> Result<ChunkGuard,
  LifecycleError>` — for `allocate_chunk` with caller-supplied ID. Same
  lock acquisition as `acquire` but does NOT fetch from store (chunk
  does not exist yet). Returns a guard with `chunk: None`. Caller must
  `refresh()` after creating the chunk.
- `populate_cache(&self, chunk_id, chunk)` — for `allocate_chunk` with
  auto-generated ID (skips the lock; UUID collision negligible).
- `reap_idle(&self)` — iterates `self.locks.retain(|_, arc|
  Arc::strong_count(arc) > 1)`. Entries where only the map holds a
  clone (`strong_count == 1`) are removed. Increments `reap_idle_count`
  and `reap_idle_entries_removed` by the number removed. Payload cache
  is untouched (bounded by its own capacity).
- `invalidate_chunk(&self, chunk_id) -> bool` — calls
  `self.chunks.remove(&chunk_id).is_some()`. Increments
  `invalidate_count`. Used by range migration.
- `invalidate_range(&self, bucket_start, bucket_end) -> u32` — iterates
  cache entries and removes those whose chunk ID hashes to a bucket in
  `[bucket_start, bucket_end]`. Returns the count removed. Increments
  `invalidate_count` by the count. O(n) on cache size — acceptable for
  rare range migrations.
- `cache_len() -> usize`, `metrics_snapshot() -> LifecycleMetricsSnapshot`.

### 10.2 Lock policy + cache hint

```rust
pub enum LockPolicy {
    TryLock,
    Wait(Duration),
}

impl Default for LockPolicy {
    fn default() -> Self { Self::Wait(Duration::from_secs(10)) }
}

pub enum CacheHint {
    Cache,
    NoCache,
}

impl Default for CacheHint {
    fn default() -> Self { Self::Cache }
}
```

- `LockPolicy::TryLock` → fail fast with `LockBusy` on contention.
- `LockPolicy::Wait(d)` → park the task up to `d`, then `LockTimeout`.
  No `WaitForever` variant — a bounded wait prevents indefinite stalls.
- `CacheHint::Cache` (default) → populate cache on miss, write to cache
  on `refresh`.
- `CacheHint::NoCache` → skip cache population; the guard's local copy
  is still updated so the current operation sees the chunk.

All mutating RPCs use `LockPolicy::default()` and `CacheHint::Cache`.
These are internal in v1, not exposed in the RPC API.

### 10.3 ChunkGuard

```rust
pub struct ChunkGuard {
    guard: OwnedMutexGuard<()>,  // held for Drop — releases the lock
    chunk: Option<Chunk>,
    hint: CacheHint,
    chunk_id: ChunkId,
    hold_start: Instant,
    metrics: Arc<LifecycleMetrics>,
    hold_warn_threshold: Duration,
}
```

- `chunk(&self) -> Option<&Chunk>` — returns the latest chunk record.
  `None` for `acquire_for_create` before `refresh`.
- `refresh(&mut self, chunk: Chunk)` — updates the guard's local copy.
  If `hint == Cache`, also writes to the lock map's cache. Caller MUST
  have persisted via `put_chunk` first.
- `Drop` — records lock-hold time into the histogram (from `hold_start`
  to now). If hold time > `hold_warn_threshold`, emits a `warn!` log
  with chunk_id and hold duration.

### 10.4 Chunk cache

`quick-cache = "0.7"` (`app/crowdb-chunkdb/Cargo.toml`). Default capacity
10_000 entries (configurable via `lifecycle.cache_capacity`). The
design supports 100_000+. `quick_cache::Cache::new(capacity)` accepts
any `usize`; the only constraint is memory (~1-2 KB per `Chunk` → 10k
entries ≈ 10-20 MB, 100k entries ≈ 100-200 MB).

Cache operations:
- `Cache::get(&key) -> Option<Chunk>` — cache hit check. O(1).
- `Cache::insert(key, value)` — populate on miss, refresh after
  `put_chunk`. O(1) amortized.
- `Cache::remove(&key) -> Option<(Key, Val)>` — `invalidate_chunk`.
  O(1).
- `Cache::entry_count()` — gauge for metrics snapshot.

### 10.5 Sweep task

The lock map grows unbounded without reaping (one entry per chunk ever
touched). `reap_idle` removes uncontended entries periodically, keeping
the map bounded by concurrent locks, not by chunks-ever-touched.

`main.rs` spawns a background task (`run_sweep_loop`) that calls
`locks.reap_idle()` every `lifecycle.sweep_chunk_lock_interval_secs`
(default 60s). Uses the same `watch::channel(false)` stop signal
pattern as the topology refresh loop. `reap_idle` is a single
`DashMap::retain` call, no allocation, no blocking.

### 10.6 LifecycleHandler integration

`LifecycleHandler` gains a `locks: Arc<ChunkLockMap>` field. Each
mutating RPC acquires the per-chunk lock before its RMW cycle:

- `allocate_chunk` (caller-supplied ID): `check_range` →
  `acquire_for_create` → existence check (`store.get_chunk`; return
  `ChunkAlreadyExists` if taken) → build chunk, allocate strips,
  `put_chunk`, start background segment commit → `guard.refresh(chunk)`.
- `allocate_chunk` (auto-generated ID): skip the lock (UUID collision
  negligible). After `put_chunk`, `populate_cache(id, chunk)` directly.
- `append_chunk`: `check_range` → `acquire` → state check, allocate
  strips, `put_chunk`, `commit_strip_segments` → `guard.refresh(chunk)`.
- `seal_chunk`: same as append but no diskdb calls (fast path).
- `delete_chunk`: `check_range` → `acquire` → state check → persist Deleted
  with segments as cleanup intent → free segments → clear the segment list
  and persist the tombstone → `guard.refresh(chunk)`.
- `delete_chunk_range`: `check_range` → `acquire` → validate a nonzero,
  nonoverflowing range → persist the retained strips → free the removed
  strips' segments → `guard.refresh(chunk)`.
- `update_chunk_strip`: `check_range` → `acquire` → validate state, shape,
  sequence, capacity, and owner → commit the replacement segments → publish
  metadata → free the old strip → `guard.refresh(chunk)`.
- `query_chunk` / `list_chunks` — unchanged (no lock, no cache).

### 10.7 Error variants + service mapping

```rust
#[error("chunk lock busy — retry later")]
LockBusy,
#[error("chunk lock acquire timed out")]
LockTimeout,
```

`map_error` in `service.rs`:
- `LifecycleError::LockBusy => Status::unavailable(e.to_string())`
- `LifecycleError::LockTimeout => Status::unavailable(e.to_string())`

Both map to `UNAVAILABLE`. The client's existing retry logic handles
this (same as `NotLeaderHint` transient errors).

### 10.8 Metrics + HTTP endpoints

`app/crowdb-chunkdb/src/metrics.rs` — `LifecycleMetrics` with
`AtomicU64` counters + `Mutex<PreciseHistogram>` for lock wait/hold
latency. Counters (all `AtomicU64`, `Relaxed` ordering):

- `lock_wait_time` — `PreciseHistogram` (wait duration in acquire).
- `lock_timeout_count` — incremented on `LockTimeout`.
- `lock_busy_count` — incremented on `LockBusy`.
- `lock_hold_time` — `PreciseHistogram` (hold duration in guard Drop).
- `cache_hit_count` — incremented on cache hit in `acquire`.
- `cache_miss_count` — incremented on cache miss in `acquire`.
- `cache_size` — gauge, read from `Cache::entry_count()` at snapshot.
- `reap_idle_count` — incremented each `reap_idle` run.
- `reap_idle_entries_removed` — entries removed per `reap_idle`.
- `invalidate_count` — incremented on `invalidate_chunk`/
  `invalidate_range`.

`snapshot() -> LifecycleMetricsSnapshot` drains counters, reads
histograms, returns a serializable struct (JSON).

HTTP endpoints (`main.rs` HTTP server, alongside `/ready` +
`/health`):

- `GET /metrics` → returns `LifecycleMetricsSnapshot` as JSON.
- `POST /invalidate_chunk` → body `{ "chunk_id": { "high": u64, "low":
  u64 } }` → calls `ChunkLockMap::invalidate_chunk`. Returns
  `{ "invalidated": bool }`.
- `POST /invalidate_range` → body `{ "bucket_start": u16, "bucket_end":
  u16 }` → calls `ChunkLockMap::invalidate_range`. Returns
  `{ "invalidated_count": u32 }`.

All internal (no auth, same as `/ready` and `/health`).

### 10.9 Edge cases

- Lock map entry does not exist → created on first `acquire` via
  `DashMap::entry().or_default()`.
- Lock holder panics → `tokio::sync::Mutex<()>` auto-releases (no
  poisoning for `Mutex<()>`); cache slot may be stale but next
  `acquire` re-fetches on miss.
- `reap_idle` runs while an acquirer holds a clone →
  `Arc::strong_count > 1`, entry is retained. No race:
  `DashMap::retain` holds the shard lock.
- Process crash → all in-memory state lost; KV store is source of
  truth.
- Cache evicts a chunk between two operations → next `acquire` is a
  miss, re-fetches. Correctness unaffected.
- `CacheHint::NoCache` → skip `insert` on miss and `refresh`; the
  guard's local copy is still updated so the current operation sees
  the chunk.
- `delete_chunk` keeps the Deleted-state chunk cached via `refresh` →
  next `delete_chunk` retry gets a cache hit and returns the same tombstone
  successfully without a store round-trip.
- `acquire` returns `ChunkNotFound` (store miss during acquire) → the
  chunk does not exist; `append`/`seal`/`delete` return
  `ChunkNotFound`.
- `LockBusy` / `LockTimeout` → mapped to RPC `UNAVAILABLE`.

## 11. EC Encoding/Decoding

**EC encoding** (strip allocation):
1. Allocate data_num + code_num blocks via diskdb
2. Fill data blocks with actual data (by caller, not chunkdb)
3. Call isa-l encode to generate parity blocks
4. Mark EC state = Complete

**EC decoding** (recovery, deferred):
1. Identify failed blocks (via disk failure notification)
2. Read surviving data+parity blocks
3. Call isa-l decode to reconstruct lost data
4. Write reconstructed data to new blocks
5. Update strip with new block locations

**crowdb-common EC module**: Provides safe Rust interface to isa-l:
- `make_gf_table(data_num, code_num)`: Generate Galois Field table
- `encode(data_num, code_num, segment_len, gf_table, data_ptr, code_ptr)`: Encode
- `decode(...)`: Decode with error recovery
- Buffer management: `make_buffer`, `destroy_buffer`

**Rationale**: isa-l is highly optimized for AVX2/AVX512. Strip-level EC
aligns with redundancy unit design.

## 12. Crate Layout

```
app/crowdb-chunkdb/              # chunkdb server binary
├── Cargo.toml
├── src/
│   ├── main.rs               # CLI entrypoint and server wiring
│   ├── lifecycle.rs          # pure lifecycle index
│   ├── lifecycle/
│   │   ├── handler.rs        # lifecycle orchestration
│   │   ├── lock_map.rs       # per-chunk serialization and payload cache
│   │   └── state.rs          # lifecycle state transitions
│   ├── service.rs            # pure service index
│   ├── service/
│   │   ├── chunkdb_service.rs
│   │   ├── chunkdb_rpc_service.rs
│   │   └── chunkdb_rpc_service/
│   │       ├── service.rs    # state and handler registration
│   │       ├── mutations.rs  # mutating chunk handlers
│   │       ├── queries.rs    # query and list handlers
│   │       └── wire.rs       # FlatBuffer frames and error mapping
│   ├── allocator/
│   │   └── pool.rs           # allocation pool
│   ├── selector/
│   │   ├── mirror.rs         # Mirror placement
│   │   └── ec.rs             # EC placement
│   ├── topology/
│   │   ├── notify.rs         # topology notifications
│   │   └── refresh.rs        # topology refresh task
│   ├── storage.rs            # KV persistence
│   ├── routing.rs            # bucket routing
│   ├── range_guard.rs        # owned-range enforcement
│   └── migration.rs          # range migration

lib/crowdb-chunkdb-client/       # chunkdb client library
├── Cargo.toml
└── src/
    ├── client.rs             # ChunkdbClient
    └── rpc_transport.rs      # crowdb-rpc transport

lib/crowdb-protocol/             # Protocol definitions
└── src/fbs/
    └── chunkdb.fbs          # service and data types
```

The lifecycle index preserves the public `LifecycleHandler`, `ChunkLockMap`,
and state-machine API. The handler owns chunk operations; the lock-map module
owns the approved bounded per-chunk mutex and payload cache. RPC registration,
chunk handlers, and FlatBuffer construction are separate private modules
behind `ChunkdbRpcService`.

## 13. Concurrency Model

- **Async everywhere**: All public APIs are async (`async fn`).
- **Shared state**: `Arc<RwLock<T>>` for topology cache, allocator state.
- **Lock scoping**: Acquire locks in `{}` blocks, drop before `.await`.
- **Parallel allocation**: Use `futures::join_all` for parallel strip/block
  allocation.
- **Background tasks**: `tokio::spawn` for topology refresh, rollback cleanup.

**Rationale**: tokio provides efficient async I/O. Lock scoping prevents
deadlocks with `.await`. Parallel allocation minimizes latency.

## 14. Configuration

Key configuration parameters:

| Parameter                  | Default | Description                          |
|----------------------------|---------|--------------------------------------|
| disk_block_size            | 1 MB    | Size of disk blocks from diskdb       |
| mirror_copy_count          | 3       | Number of replicas for mirror strips  |
| default_ec_scheme          | 6+3     | Default EC scheme (data+parity)       |
| topology_refresh_interval  | 30 s    | Topology cache refresh interval       |
| placement.allow_unsafe_ec  | false   | Permit explicit degraded EC placement |
| max_allocation_parallelism | 10      | Max parallel strip allocations        |
| lifecycle.cache_capacity   | 10_000  | Per-chunk payload cache capacity (§10) |
| lifecycle.sweep_chunk_lock_interval_secs | 60 | Idle lock reap interval (§10) |
| lifecycle.lock_hold_warn_threshold_ms   | 1000 | Lock hold warn threshold (§10) |
| server.keepalive_interval_secs          | 10   | Service-registry heartbeat interval |

Configuration is loaded from CLI args or config file at startup.

## 15. Full-Stack Deployment and Benchmark

`crowdb-cli cluster local-deploy -t combined` creates the canonical local
ChunkDB profile: six KV/storage nodes split evenly across three racks, one
four-disk group and one DiskDB instance per node, one three-replica data
group, three ChunkDB instances, and complete bucket ownership. Services share
one configured log root and are registered before readiness is reported.

`crowdb-cli bench chunkdb allocate` measures mirror or EC allocation with
configurable shape, duration, seed, and concurrency. `bench chunkdb mix` uses
a deterministic allocate/query/append/seal/delete distribution. Both query
every surviving chunk through the public client, validate state, owner tags,
and nonoverlapping physical segments, and require the exact referenced byte
total to equal the DiskDB busy-space delta after compaction. Capacity
exhaustion is a successful stop reason; any correctness error invalidates the
sample.

`tools/bench-chunkdb-regression.sh` builds all four release binaries and uses
a fresh timestamped combined cluster for mirror, EC 4+2, EC 8+4, lifecycle
mix, concurrency, and capacity-exhaustion cases. It retains each case's logs,
destroys each cluster, runs all later cases after a failure, and returns a
nonzero aggregate status.

## 16. Implementation Scope

**v1 (R85)**:
- Basic chunkdb server and client
- Mirror and EC strip allocation
- Rack/node-aware placement
- Topology cache with group-0 integration and watch/notify for real-time updates
- Basic chunk lifecycle (allocate/seal/delete)
- EC wrapper (crowdb-common module)
- Chunk ID generation (crowdb-common): 128-bit format with getrandom
- Repo, WAL, BTree page, Page index chunk types (mirror and EC strips supported)
- E2E tests

**Future work** (separate requirements):
- In-chunk GC operations (reclaim, collapse, merge)
- Mirror-to-EC conversion for shared chunks
- Specific chunk type (direct EC write for large objects)
- Recovery flow (disk failure handling, EC rebuild)
- Metrics and observability
- Console/CLI integration
- Custom RPC for performance

## 17. References

- CROWDB diskdb design: `doc/design/diskdb/design-crowdb-diskdb.md`
