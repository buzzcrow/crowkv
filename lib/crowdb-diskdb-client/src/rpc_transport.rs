// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![allow(clippy::missing_errors_doc)]

//! crowdb-rpc transport for `DiskdbClient` (R115 migration).
//!
//! Builds flatbuffer requests, sends via `RpcClient::call`, awaits
//! `CallFuture`, and parses flatbuffer responses into the existing
//! proto response types. Replaced the legacy transport during the
//! mixed-rollout window; the client selects the transport based on
//! whether `with_rpc_transport` was called.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use flatbuffers::FlatBufferBuilder;

use crowdb_protocol::common::{ChunkId, DiskId};
use crowdb_protocol::diskdb::rpc::{
    AllocateResponse, CompactZoneResponse, DiskInfo, DiskType, FreeResponse, GetDiskGroupInfoResponse,
    GetDiskInfoResponse, GetScanStatusResponse, QueryCapacityStatsResponse, RebuildZoneBitmapResponse,
    RecalcDiskUsageResponse, Segment, TriggerScanResponse, ZoneAllocationState, ZoneUsage,
};
use crowdb_protocol::diskdb_fb::{
    FBAllocateBlocksRequest, FBAllocateBlocksRequestArgs, FBCommitBlocksRequest, FBCommitBlocksRequestArgs,
    FBCompactZoneRequest, FBCompactZoneRequestArgs, FBDiskGroupInfo, FBDiskInfo, FBDiskType, FBDiskdbRetCode,
    FBFreeBlocksRequest, FBFreeBlocksRequestArgs, FBGetDiskGroupInfoRequest, FBGetDiskGroupInfoRequestArgs,
    FBGetDiskInfoRequest, FBGetDiskInfoRequestArgs, FBGetScanStatusRequest, FBGetScanStatusRequestArgs,
    FBHwStatus, FBInt128, FBQueryCapacityStatsRequest, FBQueryCapacityStatsRequestArgs,
    FBRebuildZoneBitmapRequest, FBRebuildZoneBitmapRequestArgs, FBRecalcDiskUsageRequest,
    FBRecalcDiskUsageRequestArgs, FBScanSummary, FBSegment, FBTriggerScanRequest, FBTriggerScanRequestArgs,
    FBZoneAllocationState, FBZoneUsage,
};
use crowdb_protocol::fb::FBMsgType;
use crowdb_protocol::fb_wrappers::diskdb::{
    FBAllocateResponseRef, FBCommitBlocksResponseRef, FBCompactZoneResponseRef, FBFreeResponseRef,
    FBGetDiskGroupInfoResponseRef, FBGetDiskInfoResponseRef, FBGetScanStatusResponseRef,
    FBQueryCapacityStatsResponseRef, FBRebuildZoneBitmapResponseRef, FBRecalcDiskUsageResponseRef,
    FBTriggerScanResponseRef,
};
use crowdb_rpc_ffi::{Buffer, Connection, RpcClient, RpcError, RpcServer};

use crate::{DiskdbClientError, Result};

/// crowdb-rpc transport for diskdb. Holds the client-side `RpcServer`
/// (manages connections), `RpcClient` (request/response correlation),
/// and a connection pool per endpoint.
pub struct DiskdbRpcTransport {
    server: Arc<RpcServer>,
    rpc: RpcClient,
    connections: DashMap<String, Vec<Connection>>,
    pool_size: usize,
    conn_rr: AtomicU64,
    next_req_id: AtomicU64,
}

impl std::fmt::Debug for DiskdbRpcTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiskdbRpcTransport")
            .field("next_req_id", &self.next_req_id.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl DiskdbRpcTransport {
    /// Create a new crowdb-rpc transport with 2 I/O workers (default).
    /// The `RpcServer` is the client-side transport — it does not listen
    /// but is used to establish connections to remote endpoints.
    #[must_use]
    pub fn new() -> Self {
        Self::with_pool_size(1, 2)
    }

    /// Create a new crowdb-rpc transport with `workers` I/O worker threads.
    #[must_use]
    pub fn with_workers(workers: u32) -> Self {
        Self::with_pool_size(1, workers)
    }

    /// Create a transport with `pool_size` connections per endpoint and
    /// `workers` crowdb-rpc I/O workers.
    #[must_use]
    pub fn with_pool_size(pool_size: usize, workers: u32) -> Self {
        let server = Arc::new(RpcServer::with_engines(None, 1, workers));
        server.start();
        server.register_conn_count_gauge("rpc.client.connections");
        let rpc = RpcClient::new();
        rpc.set_completion_pool_size(1024);
        // Disk allocation is not request-idempotent. The distributed KV
        // commit can cross five seconds under a saturated EC allocation
        // workload, so keep the original request alive instead of creating
        // a duplicate allocation through the transient retry path.
        rpc.start_reaper(10_000_000_000, 500_000_000);
        Self {
            server,
            rpc,
            connections: DashMap::new(),
            pool_size: pool_size.max(1),
            conn_rr: AtomicU64::new(0),
            next_req_id: AtomicU64::new(1),
        }
    }

    fn next_id(&self) -> u64 {
        self.next_req_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Get or create a `Connection` for the given rpc endpoint.
    fn conn_for(&self, rpc_endpoint: &str) -> Result<Connection> {
        let normalized = normalize_endpoint(rpc_endpoint);
        if let Some(conns) = self.connections.get(&normalized) {
            if conns.len() == self.pool_size {
                let index = rr_index(&self.conn_rr, conns.len());
                return Ok(conns[index].clone());
            }
        }
        let (host, port) = parse_endpoint(&normalized)?;
        let mut entry = self.connections.entry(normalized).or_default();
        while entry.len() < self.pool_size {
            let conn = self.server.connect(&host, port).map_err(|e| {
                DiskdbClientError::Unreachable(format!("rpc connect to {host}:{port}: {e:?}"))
            })?;
            self.rpc.attach(&conn);
            entry.push(conn);
        }
        let index = rr_index(&self.conn_rr, entry.len());
        Ok(entry[index].clone())
    }

    // ── Public RPC methods ─────────────────────────────────────

    /// Allocate blocks via crowdb-rpc.
    pub async fn allocate_blocks(
        &self,
        rpc_endpoint: &str,
        req: &crowdb_protocol::diskdb::rpc::AllocateBlocksRequest,
    ) -> Result<AllocateResponse> {
        let req_id = self.next_id();
        let conn = self.conn_for(rpc_endpoint)?;
        let control = build_allocate_request(req_id, req);
        let msg_type = FBMsgType::EAllocateBlocksRequest.0 as u16;
        let fut = self
            .rpc
            .call(&self.server, &conn, req_id, control, None, msg_type)
            .map_err(DiskdbClientError::from)?;
        let resp = fut.await.map_err(DiskdbClientError::from)?;
        parse_allocate_response(&resp)
    }

    /// Free blocks via crowdb-rpc.
    pub async fn free_blocks(
        &self,
        rpc_endpoint: &str,
        req: &crowdb_protocol::diskdb::rpc::FreeBlocksRequest,
    ) -> Result<FreeResponse> {
        let req_id = self.next_id();
        let conn = self.conn_for(rpc_endpoint)?;
        let control = build_free_request(req_id, req);
        let msg_type = FBMsgType::EFreeBlocksRequest.0 as u16;
        let fut = self
            .rpc
            .call(&self.server, &conn, req_id, control, None, msg_type)
            .map_err(DiskdbClientError::from)?;
        let resp = fut.await.map_err(DiskdbClientError::from)?;
        parse_free_response(&resp)
    }

    /// Commit blocks via crowdb-rpc.
    pub async fn commit_blocks(
        &self,
        rpc_endpoint: &str,
        req: &crowdb_protocol::diskdb::rpc::CommitBlocksRequest,
    ) -> Result<crowdb_protocol::diskdb::rpc::CommitBlocksResponse> {
        let req_id = self.next_id();
        let conn = self.conn_for(rpc_endpoint)?;
        let control = build_commit_request(req_id, req);
        let msg_type = FBMsgType::ECommitBlocksRequest.0 as u16;
        let fut = self
            .rpc
            .call(&self.server, &conn, req_id, control, None, msg_type)
            .map_err(DiskdbClientError::from)?;
        let resp = fut.await.map_err(DiskdbClientError::from)?;
        parse_commit_response(&resp)
    }

    /// Query capacity stats via crowdb-rpc.
    pub async fn query_capacity_stats(
        &self,
        rpc_endpoint: &str,
        req: &crowdb_protocol::diskdb::rpc::QueryCapacityStatsRequest,
    ) -> Result<QueryCapacityStatsResponse> {
        let req_id = self.next_id();
        let conn = self.conn_for(rpc_endpoint)?;
        let control = build_query_capacity_request(req_id, req);
        let msg_type = FBMsgType::EQueryCapacityStatsRequest.0 as u16;
        let fut = self
            .rpc
            .call(&self.server, &conn, req_id, control, None, msg_type)
            .map_err(DiskdbClientError::from)?;
        let resp = fut.await.map_err(DiskdbClientError::from)?;
        parse_query_capacity_response(&resp)
    }

    /// Get disk-group info via crowdb-rpc.
    pub async fn get_disk_group_info(
        &self,
        rpc_endpoint: &str,
        dg_id: u64,
    ) -> Result<GetDiskGroupInfoResponse> {
        let req_id = self.next_id();
        let conn = self.conn_for(rpc_endpoint)?;
        let control = build_get_disk_group_info_request(req_id, dg_id);
        let msg_type = FBMsgType::EGetDiskGroupInfoRequest.0 as u16;
        let fut = self
            .rpc
            .call(&self.server, &conn, req_id, control, None, msg_type)
            .map_err(DiskdbClientError::from)?;
        let resp = fut.await.map_err(DiskdbClientError::from)?;
        parse_get_disk_group_info_response(&resp)
    }

    /// Get disk info via crowdb-rpc.
    pub async fn get_disk_info(
        &self,
        rpc_endpoint: &str,
        dg_id: u64,
        disk_id: DiskId,
    ) -> Result<GetDiskInfoResponse> {
        let req_id = self.next_id();
        let conn = self.conn_for(rpc_endpoint)?;
        let control = build_get_disk_info_request(req_id, dg_id, disk_id);
        let msg_type = FBMsgType::EGetDiskInfoRequest.0 as u16;
        let fut = self
            .rpc
            .call(&self.server, &conn, req_id, control, None, msg_type)
            .map_err(DiskdbClientError::from)?;
        let resp = fut.await.map_err(DiskdbClientError::from)?;
        parse_get_disk_info_response(&resp)
    }

    /// Rebuild zone bitmap via crowdb-rpc.
    pub async fn rebuild_zone_bitmap(
        &self,
        rpc_endpoint: &str,
        disk_id: DiskId,
        zone_index: u32,
    ) -> Result<RebuildZoneBitmapResponse> {
        let req_id = self.next_id();
        let conn = self.conn_for(rpc_endpoint)?;
        let control = build_rebuild_zone_bitmap_request(req_id, disk_id, zone_index);
        let msg_type = FBMsgType::ERebuildZoneBitmapRequest.0 as u16;
        let fut = self
            .rpc
            .call(&self.server, &conn, req_id, control, None, msg_type)
            .map_err(DiskdbClientError::from)?;
        let resp = fut.await.map_err(DiskdbClientError::from)?;
        parse_rebuild_zone_bitmap_response(&resp)
    }

    /// Recalc disk usage via crowdb-rpc.
    pub async fn recalc_disk_usage(
        &self,
        rpc_endpoint: &str,
        req: &crowdb_protocol::diskdb::rpc::RecalcDiskUsageRequest,
    ) -> Result<RecalcDiskUsageResponse> {
        let req_id = self.next_id();
        let conn = self.conn_for(rpc_endpoint)?;
        let control = build_recalc_request(req_id, req);
        let msg_type = FBMsgType::ERecalcDiskUsageRequest.0 as u16;
        let fut = self
            .rpc
            .call(&self.server, &conn, req_id, control, None, msg_type)
            .map_err(DiskdbClientError::from)?;
        let resp = fut.await.map_err(DiskdbClientError::from)?;
        parse_recalc_response(&resp)
    }

    /// Compact zone via crowdb-rpc.
    pub async fn compact_zone(
        &self,
        rpc_endpoint: &str,
        req: &crowdb_protocol::diskdb::rpc::CompactZoneRequest,
    ) -> Result<CompactZoneResponse> {
        let req_id = self.next_id();
        let conn = self.conn_for(rpc_endpoint)?;
        let control = build_compact_zone_request(req_id, req);
        let msg_type = FBMsgType::ECompactZoneRequest.0 as u16;
        let fut = self
            .rpc
            .call(&self.server, &conn, req_id, control, None, msg_type)
            .map_err(DiskdbClientError::from)?;
        let resp = fut.await.map_err(DiskdbClientError::from)?;
        parse_compact_zone_response(&resp)
    }

    /// Trigger scan via crowdb-rpc.
    pub async fn trigger_scan(&self, rpc_endpoint: &str) -> Result<TriggerScanResponse> {
        let req_id = self.next_id();
        let conn = self.conn_for(rpc_endpoint)?;
        let control = build_trigger_scan_request(req_id);
        let msg_type = FBMsgType::ETriggerScanRequest.0 as u16;
        let fut = self
            .rpc
            .call(&self.server, &conn, req_id, control, None, msg_type)
            .map_err(DiskdbClientError::from)?;
        let resp = fut.await.map_err(DiskdbClientError::from)?;
        parse_trigger_scan_response(&resp)
    }

    /// Get scan status via crowdb-rpc.
    pub async fn get_scan_status(&self, rpc_endpoint: &str) -> Result<GetScanStatusResponse> {
        let req_id = self.next_id();
        let conn = self.conn_for(rpc_endpoint)?;
        let control = build_get_scan_status_request(req_id);
        let msg_type = FBMsgType::EGetScanStatusRequest.0 as u16;
        let fut = self
            .rpc
            .call(&self.server, &conn, req_id, control, None, msg_type)
            .map_err(DiskdbClientError::from)?;
        let resp = fut.await.map_err(DiskdbClientError::from)?;
        parse_get_scan_status_response(&resp)
    }
}

fn rr_index(counter: &AtomicU64, len: usize) -> usize {
    let len_u64 = u64::try_from(len).unwrap_or(u64::MAX);
    usize::try_from(counter.fetch_add(1, Ordering::Relaxed) % len_u64).unwrap_or(0)
}

impl Default for DiskdbRpcTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl From<RpcError> for DiskdbClientError {
    fn from(e: RpcError) -> Self {
        if e.is_retryable() {
            DiskdbClientError::Unreachable(format!("rpc transient: {e:?}"))
        } else {
            DiskdbClientError::Rpc(format!("rpc error: {e:?}"))
        }
    }
}

// ── Request builders ─────────────────────────────────────────────

fn fb_int128(id: DiskId) -> FBInt128 {
    FBInt128::new(id.high, id.low)
}

fn fb_chunk_id(id: ChunkId) -> FBInt128 {
    FBInt128::new(id.high, id.low)
}

fn build_segments<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    segments: &[Segment],
) -> flatbuffers::WIPOffset<flatbuffers::Vector<'a, FBSegment>> {
    let offs: Vec<FBSegment> = segments
        .iter()
        .map(|s| {
            let disk_id = s.disk_id.unwrap_or_default();
            let owner = s.owner_chunk.unwrap_or_default();
            FBSegment::new(
                &FBInt128::new(disk_id.high, disk_id.low),
                &FBInt128::new(owner.high, owner.low),
                s.unit_offset,
                s.allocation_ts,
                s.zone_index,
                s.unit_count,
            )
        })
        .collect();
    fbb.create_vector(&offs)
}

fn build_allocate_request(req_id: u64, req: &crowdb_protocol::diskdb::rpc::AllocateBlocksRequest) -> Buffer {
    let mut fbb = FlatBufferBuilder::new();
    let owner = fb_chunk_id(req.owner_chunk.unwrap_or_default());
    let exclude: Vec<FBInt128> = req.exclude_disk_ids.iter().map(|d| fb_int128(*d)).collect();
    let exclude_off = fbb.create_vector(&exclude);
    let off = FBAllocateBlocksRequest::create(
        &mut fbb,
        &FBAllocateBlocksRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            disk_group_id: req.disk_group_id,
            unit_count: req.unit_count,
            count: req.count,
            owner_chunk: Some(&owner),
            exclude_disk_ids: Some(exclude_off),
        },
    );
    fbb.finish(off, None);
    Buffer::from_bytes(fbb.finished_data())
}

fn build_free_request(req_id: u64, req: &crowdb_protocol::diskdb::rpc::FreeBlocksRequest) -> Buffer {
    let mut fbb = FlatBufferBuilder::new();
    let segs = build_segments(&mut fbb, &req.segments);
    let off = FBFreeBlocksRequest::create(
        &mut fbb,
        &FBFreeBlocksRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            segments: Some(segs),
        },
    );
    fbb.finish(off, None);
    Buffer::from_bytes(fbb.finished_data())
}

fn build_commit_request(req_id: u64, req: &crowdb_protocol::diskdb::rpc::CommitBlocksRequest) -> Buffer {
    let mut fbb = FlatBufferBuilder::new();
    let segs = build_segments(&mut fbb, &req.segments);
    let off = FBCommitBlocksRequest::create(
        &mut fbb,
        &FBCommitBlocksRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            segments: Some(segs),
        },
    );
    fbb.finish(off, None);
    Buffer::from_bytes(fbb.finished_data())
}

fn build_query_capacity_request(
    req_id: u64,
    req: &crowdb_protocol::diskdb::rpc::QueryCapacityStatsRequest,
) -> Buffer {
    let mut fbb = FlatBufferBuilder::new();
    let disk_id_off = req.disk_id.map(fb_int128);
    let off = FBQueryCapacityStatsRequest::create(
        &mut fbb,
        &FBQueryCapacityStatsRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            disk_group_id: req.disk_group_id,
            disk_id: disk_id_off.as_ref(),
            has_zone_index: req.zone_index.is_some(),
            zone_index: req.zone_index.unwrap_or(0),
        },
    );
    fbb.finish(off, None);
    Buffer::from_bytes(fbb.finished_data())
}

fn build_get_disk_group_info_request(req_id: u64, dg_id: u64) -> Buffer {
    let mut fbb = FlatBufferBuilder::new();
    let off = FBGetDiskGroupInfoRequest::create(
        &mut fbb,
        &FBGetDiskGroupInfoRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            disk_group_id: dg_id,
        },
    );
    fbb.finish(off, None);
    Buffer::from_bytes(fbb.finished_data())
}

fn build_get_disk_info_request(req_id: u64, dg_id: u64, disk_id: DiskId) -> Buffer {
    let mut fbb = FlatBufferBuilder::new();
    let fb_disk_id = fb_int128(disk_id);
    let off = FBGetDiskInfoRequest::create(
        &mut fbb,
        &FBGetDiskInfoRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            rack_id: 0,
            node_id: 0,
            disk_group_id: dg_id,
            disk_id: Some(&fb_disk_id),
        },
    );
    fbb.finish(off, None);
    Buffer::from_bytes(fbb.finished_data())
}

fn build_rebuild_zone_bitmap_request(req_id: u64, disk_id: DiskId, zone_index: u32) -> Buffer {
    let mut fbb = FlatBufferBuilder::new();
    let fb_disk_id = fb_int128(disk_id);
    let off = FBRebuildZoneBitmapRequest::create(
        &mut fbb,
        &FBRebuildZoneBitmapRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            disk_id: Some(&fb_disk_id),
            zone_index,
        },
    );
    fbb.finish(off, None);
    Buffer::from_bytes(fbb.finished_data())
}

fn build_recalc_request(req_id: u64, req: &crowdb_protocol::diskdb::rpc::RecalcDiskUsageRequest) -> Buffer {
    let mut fbb = FlatBufferBuilder::new();
    let off = FBRecalcDiskUsageRequest::create(
        &mut fbb,
        &FBRecalcDiskUsageRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            has_disk_group_id: req.disk_group_id.is_some(),
            disk_group_id: req.disk_group_id.unwrap_or(0),
        },
    );
    fbb.finish(off, None);
    Buffer::from_bytes(fbb.finished_data())
}

fn build_compact_zone_request(req_id: u64, req: &crowdb_protocol::diskdb::rpc::CompactZoneRequest) -> Buffer {
    let mut fbb = FlatBufferBuilder::new();
    let fb_disk_id = req.disk_id.map(fb_int128);
    let zone_off = fbb.create_vector(&req.zone_indices);
    let off = FBCompactZoneRequest::create(
        &mut fbb,
        &FBCompactZoneRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            disk_id: fb_disk_id.as_ref(),
            zone_indices: Some(zone_off),
        },
    );
    fbb.finish(off, None);
    Buffer::from_bytes(fbb.finished_data())
}

fn build_trigger_scan_request(req_id: u64) -> Buffer {
    let mut fbb = FlatBufferBuilder::new();
    let off = FBTriggerScanRequest::create(
        &mut fbb,
        &FBTriggerScanRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
        },
    );
    fbb.finish(off, None);
    Buffer::from_bytes(fbb.finished_data())
}

fn build_get_scan_status_request(req_id: u64) -> Buffer {
    let mut fbb = FlatBufferBuilder::new();
    let off = FBGetScanStatusRequest::create(
        &mut fbb,
        &FBGetScanStatusRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
        },
    );
    fbb.finish(off, None);
    Buffer::from_bytes(fbb.finished_data())
}

// ── Response parsers ─────────────────────────────────────────────

/// Check the `ret_code` from a flatbuffer response and return an error
/// if not `Success`.
fn check_ret_code(code: FBDiskdbRetCode, msg: Option<&str>) -> Result<()> {
    if code == FBDiskdbRetCode::Success {
        return Ok(());
    }
    let msg = msg.unwrap_or("unknown error");
    Err(match code {
        FBDiskdbRetCode::NoSpace => DiskdbClientError::NoSpace(msg.to_string()),
        FBDiskdbRetCode::NotOwner => DiskdbClientError::NotOwner(msg.to_string()),
        FBDiskdbRetCode::DiskNotFound => DiskdbClientError::Rpc(format!("disk not found: {msg}")),
        FBDiskdbRetCode::DiskGroupNotFound => DiskdbClientError::Rpc(format!("disk-group not found: {msg}")),
        FBDiskdbRetCode::Degraded => DiskdbClientError::Rpc(format!("degraded: {msg}")),
        FBDiskdbRetCode::Unavailable => DiskdbClientError::Unreachable(format!("unavailable: {msg}")),
        FBDiskdbRetCode::NotFound => DiskdbClientError::Rpc(format!("not found: {msg}")),
        FBDiskdbRetCode::InvalidArgument => DiskdbClientError::Rpc(format!("invalid argument: {msg}")),
        FBDiskdbRetCode::Internal => DiskdbClientError::Rpc(format!("internal: {msg}")),
        _ => DiskdbClientError::Rpc(format!("unknown error: {msg}")),
    })
}

fn parse_allocate_response(resp: &crowdb_rpc_ffi::Response) -> Result<AllocateResponse> {
    let ctrl = resp
        .control
        .as_ref()
        .ok_or_else(|| DiskdbClientError::Rpc("missing control buffer".into()))?;
    let r = FBAllocateResponseRef::new(ctrl.bytes());
    if !r.valid() {
        return Err(DiskdbClientError::Rpc("invalid allocate response".into()));
    }
    check_ret_code(r.ret_code(), r.error_msg())?;
    let segments: Vec<Segment> = r
        .segments()
        .map(|v| {
            v.iter()
                .map(|s| Segment {
                    disk_id: Some(DiskId {
                        high: s.disk_id().high(),
                        low: s.disk_id().low(),
                    }),
                    owner_chunk: Some(ChunkId {
                        high: s.owner_chunk().high(),
                        low: s.owner_chunk().low(),
                    }),
                    unit_offset: s.unit_offset(),
                    zone_index: s.zone_index(),
                    unit_count: s.unit_count(),
                    allocation_ts: s.allocation_ts(),
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(AllocateResponse { segments })
}

fn parse_free_response(resp: &crowdb_rpc_ffi::Response) -> Result<FreeResponse> {
    let ctrl = resp
        .control
        .as_ref()
        .ok_or_else(|| DiskdbClientError::Rpc("missing control buffer".into()))?;
    let r = FBFreeResponseRef::new(ctrl.bytes());
    if !r.valid() {
        return Err(DiskdbClientError::Rpc("invalid free response".into()));
    }
    check_ret_code(r.ret_code(), r.error_msg())?;
    Ok(FreeResponse {
        freed_count: r.freed_count(),
    })
}

fn parse_commit_response(
    resp: &crowdb_rpc_ffi::Response,
) -> Result<crowdb_protocol::diskdb::rpc::CommitBlocksResponse> {
    let ctrl = resp
        .control
        .as_ref()
        .ok_or_else(|| DiskdbClientError::Rpc("missing control buffer".into()))?;
    let r = FBCommitBlocksResponseRef::new(ctrl.bytes());
    if !r.valid() {
        return Err(DiskdbClientError::Rpc("invalid commit response".into()));
    }
    check_ret_code(r.ret_code(), r.error_msg())?;
    Ok(crowdb_protocol::diskdb::rpc::CommitBlocksResponse {
        committed_count: r.committed_count(),
    })
}

fn parse_query_capacity_response(resp: &crowdb_rpc_ffi::Response) -> Result<QueryCapacityStatsResponse> {
    let ctrl = resp
        .control
        .as_ref()
        .ok_or_else(|| DiskdbClientError::Rpc("missing control buffer".into()))?;
    let r = FBQueryCapacityStatsResponseRef::new(ctrl.bytes());
    if !r.valid() {
        return Err(DiskdbClientError::Rpc("invalid query response".into()));
    }
    check_ret_code(r.ret_code(), r.error_msg())?;
    let disk_groups: Vec<crowdb_protocol::diskdb::rpc::DiskGroupInfo> = r
        .disk_groups()
        .map(|v| v.iter().map(parse_disk_group_info).collect())
        .unwrap_or_default();
    Ok(QueryCapacityStatsResponse { disk_groups })
}

fn parse_get_disk_group_info_response(resp: &crowdb_rpc_ffi::Response) -> Result<GetDiskGroupInfoResponse> {
    let ctrl = resp
        .control
        .as_ref()
        .ok_or_else(|| DiskdbClientError::Rpc("missing control buffer".into()))?;
    let r = FBGetDiskGroupInfoResponseRef::new(ctrl.bytes());
    if !r.valid() {
        return Err(DiskdbClientError::Rpc(
            "invalid get_disk_group_info response".into(),
        ));
    }
    check_ret_code(r.ret_code(), r.error_msg())?;
    let group = r.group().map(parse_disk_group_info);
    Ok(GetDiskGroupInfoResponse { group })
}

fn parse_get_disk_info_response(resp: &crowdb_rpc_ffi::Response) -> Result<GetDiskInfoResponse> {
    let ctrl = resp
        .control
        .as_ref()
        .ok_or_else(|| DiskdbClientError::Rpc("missing control buffer".into()))?;
    let r = FBGetDiskInfoResponseRef::new(ctrl.bytes());
    if !r.valid() {
        return Err(DiskdbClientError::Rpc("invalid get_disk_info response".into()));
    }
    check_ret_code(r.ret_code(), r.error_msg())?;
    let disk = r.disk().map(parse_disk_info);
    Ok(GetDiskInfoResponse { disk })
}

fn parse_rebuild_zone_bitmap_response(resp: &crowdb_rpc_ffi::Response) -> Result<RebuildZoneBitmapResponse> {
    let ctrl = resp
        .control
        .as_ref()
        .ok_or_else(|| DiskdbClientError::Rpc("missing control buffer".into()))?;
    let r = FBRebuildZoneBitmapResponseRef::new(ctrl.bytes());
    if !r.valid() {
        return Err(DiskdbClientError::Rpc("invalid rebuild response".into()));
    }
    check_ret_code(r.ret_code(), r.error_msg())?;
    Ok(RebuildZoneBitmapResponse {
        rebuilt_zone_count: r.rebuilt_zone_count(),
        total_busy_units: r.total_busy_units(),
        total_free_units: r.total_free_units(),
    })
}

fn parse_recalc_response(resp: &crowdb_rpc_ffi::Response) -> Result<RecalcDiskUsageResponse> {
    let ctrl = resp
        .control
        .as_ref()
        .ok_or_else(|| DiskdbClientError::Rpc("missing control buffer".into()))?;
    let r = FBRecalcDiskUsageResponseRef::new(ctrl.bytes());
    if !r.valid() {
        return Err(DiskdbClientError::Rpc("invalid recalc response".into()));
    }
    check_ret_code(r.ret_code(), r.error_msg())?;
    let results: Vec<crowdb_protocol::diskdb::rpc::DiskGroupRecalcResult> = r
        .results()
        .map(|v| {
            v.iter()
                .map(|dg_r| {
                    let zone_results: Vec<crowdb_protocol::diskdb::rpc::ZoneRecalcResult> = dg_r
                        .zones()
                        .map(|zv| {
                            zv.iter()
                                .map(|zr| {
                                    let default_id = FBInt128::new(0, 0);
                                    let did = zr.disk_id().unwrap_or(&default_id);
                                    crowdb_protocol::diskdb::rpc::ZoneRecalcResult {
                                        disk_id: Some(DiskId {
                                            high: did.high(),
                                            low: did.low(),
                                        }),
                                        zone_index: zr.zone_index(),
                                        matches: zr.matches(),
                                        drift_detected: zr.drift_detected(),
                                        live_busy_blocks: zr.live_busy_blocks(),
                                        replayed_busy_blocks: zr.replayed_busy_blocks(),
                                        live_snapshot_slot: zr.live_snapshot_slot(),
                                        replayed_snapshot_slot: zr.replayed_snapshot_slot(),
                                        fallback_reason: zr
                                            .fallback_reason()
                                            .map(std::string::ToString::to_string),
                                    }
                                })
                                .collect()
                        })
                        .unwrap_or_default();
                    crowdb_protocol::diskdb::rpc::DiskGroupRecalcResult {
                        disk_group_id: dg_r.disk_group_id(),
                        drift_detected: dg_r.drift_detected(),
                        zones: zone_results,
                    }
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(RecalcDiskUsageResponse { results })
}

fn parse_compact_zone_response(resp: &crowdb_rpc_ffi::Response) -> Result<CompactZoneResponse> {
    let ctrl = resp
        .control
        .as_ref()
        .ok_or_else(|| DiskdbClientError::Rpc("missing control buffer".into()))?;
    let r = FBCompactZoneResponseRef::new(ctrl.bytes());
    if !r.valid() {
        return Err(DiskdbClientError::Rpc("invalid compact response".into()));
    }
    check_ret_code(r.ret_code(), r.error_msg())?;
    let zones: Vec<crowdb_protocol::diskdb::rpc::ZoneCompactionResult> = r
        .zones()
        .map(|v| {
            v.iter()
                .map(|z| crowdb_protocol::diskdb::rpc::ZoneCompactionResult {
                    zone_index: z.zone_index(),
                    success: z.success(),
                    free_records_deleted: z.free_records_deleted(),
                    error: z.error().map(std::string::ToString::to_string),
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(CompactZoneResponse {
        compacted_zone_count: r.compacted_zone_count(),
        total_free_records_deleted: r.total_free_records_deleted(),
        zones,
    })
}

fn parse_trigger_scan_response(resp: &crowdb_rpc_ffi::Response) -> Result<TriggerScanResponse> {
    let ctrl = resp
        .control
        .as_ref()
        .ok_or_else(|| DiskdbClientError::Rpc("missing control buffer".into()))?;
    let r = FBTriggerScanResponseRef::new(ctrl.bytes());
    if !r.valid() {
        return Err(DiskdbClientError::Rpc("invalid trigger_scan response".into()));
    }
    check_ret_code(r.ret_code(), r.error_msg())?;
    let summary = r.summary().map(parse_scan_summary);
    Ok(TriggerScanResponse {
        summary,
        scan_in_progress: r.scan_in_progress(),
    })
}

fn parse_get_scan_status_response(resp: &crowdb_rpc_ffi::Response) -> Result<GetScanStatusResponse> {
    let ctrl = resp
        .control
        .as_ref()
        .ok_or_else(|| DiskdbClientError::Rpc("missing control buffer".into()))?;
    let r = FBGetScanStatusResponseRef::new(ctrl.bytes());
    if !r.valid() {
        return Err(DiskdbClientError::Rpc("invalid get_scan_status response".into()));
    }
    check_ret_code(r.ret_code(), r.error_msg())?;
    let summary = r.summary().map(parse_scan_summary);
    Ok(GetScanStatusResponse {
        summary,
        has_run: r.has_run(),
    })
}

// ── FB → proto converters ────────────────────────────────────────

fn parse_disk_group_info(fb: FBDiskGroupInfo) -> crowdb_protocol::diskdb::rpc::DiskGroupInfo {
    let disk_ids: Vec<DiskId> = fb
        .disk_ids()
        .map(|v| {
            v.iter()
                .map(|id| DiskId {
                    high: id.high(),
                    low: id.low(),
                })
                .collect()
        })
        .unwrap_or_default();
    let disks: Vec<DiskInfo> = fb
        .disks()
        .map(|v| v.iter().map(parse_disk_info).collect())
        .unwrap_or_default();
    crowdb_protocol::diskdb::rpc::DiskGroupInfo {
        rack_id: fb.rack_id(),
        node_id: fb.node_id(),
        disk_group_id: fb.disk_group_id(),
        status: fb_hw_status(fb.status()),
        disk_ids,
        disks,
        capacity_bytes: fb.capacity_bytes(),
        busy_bytes: fb.busy_bytes(),
        free_bytes: fb.free_bytes(),
        allocatable_disk_count: fb.allocatable_disk_count(),
    }
}

fn parse_disk_info(fb: FBDiskInfo) -> DiskInfo {
    let zone_usages: Vec<ZoneUsage> = fb
        .zone_usages()
        .map(|v| v.iter().map(parse_zone_usage).collect())
        .unwrap_or_default();
    let default_id = FBInt128::new(0, 0);
    let id = fb.disk_id().unwrap_or(&default_id);
    let disk_id = Some(DiskId {
        high: id.high(),
        low: id.low(),
    });
    DiskInfo {
        rack_id: fb.rack_id(),
        node_id: fb.node_id(),
        disk_group_id: fb.disk_group_id(),
        disk_id,
        disk_type: fb_disk_type(fb.disk_type()),
        capacity_units: fb.capacity_units(),
        zone_size_units: fb.zone_size_units(),
        unit_size_bytes: fb.unit_size_bytes(),
        zone_count: fb.zone_count(),
        status: fb_hw_status(fb.status()),
        busy_units: fb.busy_units(),
        free_units: fb.free_units(),
        capacity_bytes: fb.capacity_bytes(),
        busy_bytes: fb.busy_bytes(),
        free_bytes: fb.free_bytes(),
        active_zone_count: fb.active_zone_count(),
        zone_usages,
        device_path: fb
            .device_path()
            .map(std::string::ToString::to_string)
            .unwrap_or_default(),
    }
}

fn parse_zone_usage(fb: FBZoneUsage) -> ZoneUsage {
    ZoneUsage {
        zone_index: fb.zone_index(),
        capacity_bytes: fb.capacity_bytes(),
        busy_bytes: fb.busy_bytes(),
        free_bytes: fb.free_bytes(),
        busy_block_count: fb.busy_block_count(),
        free_block_count: fb.free_block_count(),
        alloc_state: fb_zone_alloc_state(fb.alloc_state()),
        usage_bitmap: fb.usage_bitmap().map(|b| b.bytes().to_vec()),
    }
}

fn parse_scan_summary(fb: FBScanSummary) -> crowdb_protocol::diskdb::rpc::ScanSummary {
    crowdb_protocol::diskdb::rpc::ScanSummary {
        started_at_ms: fb.started_at_ms(),
        duration_ms: fb.duration_ms(),
        zones_scanned: fb.zones_scanned(),
        zones_skipped_active: fb.zones_skipped_active(),
        zones_skipped_compacting: fb.zones_skipped_compacting(),
        ghost_busy: fb.ghost_busy(),
        ghost_free: fb.ghost_free(),
        uncompacted_lag: fb.uncompacted_lag(),
        corrupt_snapshots: fb.corrupt_snapshots(),
        corrupt_records: fb.corrupt_records(),
        owner_mismatches: fb.owner_mismatches(),
        leak_status: fb
            .leak_status()
            .map(std::string::ToString::to_string)
            .unwrap_or_default(),
    }
}

fn fb_hw_status(v: FBHwStatus) -> i32 {
    match v {
        FBHwStatus::Up => crowdb_protocol::common::HwStatus::Up as i32,
        FBHwStatus::Maintenance => crowdb_protocol::common::HwStatus::Maintenance as i32,
        FBHwStatus::Suspect => crowdb_protocol::common::HwStatus::Suspect as i32,
        FBHwStatus::Missing => crowdb_protocol::common::HwStatus::Missing as i32,
        FBHwStatus::Bad => crowdb_protocol::common::HwStatus::Bad as i32,
        FBHwStatus::Offline => crowdb_protocol::common::HwStatus::Offline as i32,
        _ => crowdb_protocol::common::HwStatus::Init as i32,
    }
}

fn fb_disk_type(v: FBDiskType) -> i32 {
    match v {
        FBDiskType::BlockSsd => DiskType::BlockSsd as i32,
        FBDiskType::ZoneSsd => DiskType::ZoneSsd as i32,
        FBDiskType::SmrHdd => DiskType::SmrHdd as i32,
        _ => DiskType::BlockHdd as i32,
    }
}

fn fb_zone_alloc_state(v: FBZoneAllocationState) -> i32 {
    match v {
        FBZoneAllocationState::Available => ZoneAllocationState::ZoneAllocAvailable as i32,
        FBZoneAllocationState::Full => ZoneAllocationState::ZoneAllocFull as i32,
        _ => ZoneAllocationState::ZoneAllocActive as i32,
    }
}

// ── Endpoint parsing ─────────────────────────────────────────────

/// Normalize a service-registry endpoint: prepend `http://` if no
/// scheme, rewrite `0.0.0.0` to `127.0.0.1`.
fn normalize_endpoint(endpoint: &str) -> String {
    let with_scheme = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    };
    with_scheme.replacen("://0.0.0.0:", "://127.0.0.1:", 1)
}

/// Parse `http://host:port` into `(host, port)`.
fn parse_endpoint(endpoint: &str) -> Result<(String, i32)> {
    let without_scheme = endpoint
        .strip_prefix("http://")
        .or_else(|| endpoint.strip_prefix("https://"))
        .unwrap_or(endpoint);
    let (host, port_str) = without_scheme
        .rsplit_once(':')
        .ok_or_else(|| DiskdbClientError::Unreachable(format!("invalid endpoint: {endpoint}")))?;
    let port: i32 = port_str
        .parse()
        .map_err(|_| DiskdbClientError::Unreachable(format!("invalid port in endpoint: {endpoint}")))?;
    Ok((host.to_string(), port))
}
