// Copyright 2026-present Gian <crow.db@outlook.com>.

#![allow(clippy::missing_errors_doc)]
#![allow(dead_code)] // Wired in Phase 5 (ChunkdbClient transport selection)

//! crowdb-rpc client transport for `ChunkdbService` (R116 migration).
//! Mirrors `kv_rpc_transport.rs` (R117): builds flatbuffer requests,
//! sends via `RpcClient::call`, awaits `CallFuture`, parses flatbuffer
//! responses via the zero-copy `Ref` wrappers, and maps them to the
//! existing tonic proto response types so the retry/routing logic in
//! `client.rs` is unchanged.
//!
//! Runs alongside the tonic channel pool during the mixed-rollout
//! window; `ChunkdbClient` selects the transport via
//! `with_rpc_transport`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use flatbuffers::FlatBufferBuilder;

use crowdb_protocol::chunkdb::rpc::{
    AllocateChunkResponse, AppendChunkResponse, Chunk, ChunkState as ProtoChunkState, ChunkStrip,
    ChunkType as ProtoChunkType, DeleteChunkRangeResponse, DeleteChunkResponse, ListChunksResponse,
    QueryChunkResponse, SealChunkResponse, StripType as ProtoStripType, UpdateChunkStripResponse,
};
use crowdb_protocol::chunkdb::rpc::{EcState as ProtoEcState, EcStrip, MirrorStrip, Strip as ProtoStrip};
use crowdb_protocol::chunkdb_fb::{
    FBAllocateChunkRequest, FBAllocateChunkRequestArgs, FBAppendChunkRequest, FBAppendChunkRequestArgs,
    FBChunkState, FBChunkStrip, FBChunkType, FBChunkdbRetCode, FBDeleteChunkRangeRequest,
    FBDeleteChunkRangeRequestArgs, FBDeleteChunkRequest, FBDeleteChunkRequestArgs, FBInt128,
    FBListChunksRequest, FBListChunksRequestArgs, FBQueryChunkRequest, FBQueryChunkRequestArgs,
    FBSealChunkRequest, FBSealChunkRequestArgs, FBStripBody, FBStripType, FBUpdateChunkStripRequest,
    FBUpdateChunkStripRequestArgs,
};
use crowdb_protocol::common::{ChunkId, DiskId};
use crowdb_protocol::fb::FBMsgType;
use crowdb_protocol::fb_wrappers::chunkdb::{
    FBAllocateChunkResponseRef, FBDeleteChunkRangeResponseRef, FBListChunksResponseRef,
};
use crowdb_rpc_ffi::{Buffer, Connection, RpcClient, RpcError, RpcServer};

use crate::{ChunkdbClientError, Result};

/// crowdb-rpc transport for `ChunkdbService`. Holds the client-side
/// `RpcServer` (manages connections), `RpcClient` (request/response
/// correlation), and a `Connection` cache per endpoint.
pub struct ChunkdbRpcTransport {
    server: Arc<RpcServer>,
    rpc: Arc<RpcClient>,
    connections: DashMap<String, Vec<Connection>>,
    pool_size: usize,
    conn_rr: AtomicU64,
    next_req_id: AtomicU64,
}

impl std::fmt::Debug for ChunkdbRpcTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChunkdbRpcTransport")
            .field("next_req_id", &self.next_req_id.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl ChunkdbRpcTransport {
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
        let rpc = Arc::new(RpcClient::new());
        rpc.set_completion_pool_size(1024);
        // A chunk allocation spans DiskDB allocate + commit and two KV state
        // writes. Keep the client deadline aligned with DiskDB's 10-second
        // mutation window so a temporary KV tail does not abandon the whole
        // chunk after its physical blocks have already been allocated.
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
        let (host, port) = parse_endpoint(&normalized).map_err(|reason| {
            ChunkdbClientError::Unreachable(format!("invalid endpoint {rpc_endpoint}: {reason}"))
        })?;
        let mut entry = self.connections.entry(normalized).or_default();
        while entry.len() < self.pool_size {
            let conn = self.server.connect(&host, port).map_err(|e| {
                ChunkdbClientError::Unreachable(format!("rpc connect to {host}:{port}: {e:?}"))
            })?;
            self.rpc.attach(&conn);
            entry.push(conn);
        }
        let index = rr_index(&self.conn_rr, entry.len());
        Ok(entry[index].clone())
    }

    // ── AllocateChunk ─────────────────────────────────────────────

    /// Send an `AllocateChunk` request via crowdb-rpc. Returns the tonic
    /// `AllocateChunkResponse` so the caller's retry logic is unchanged.
    pub async fn send_allocate_chunk(
        &self,
        rpc_endpoint: &str,
        req: &crowdb_protocol::chunkdb::rpc::AllocateChunkRequest,
    ) -> Result<AllocateChunkResponse> {
        let req_id = self.next_id();
        let conn = self.conn_for(rpc_endpoint)?;
        let mut builder = FlatBufferBuilder::new();
        let chunk_id_off = req.chunk_id.as_ref().map(|id| FBInt128::new(id.high, id.low));
        let args = FBAllocateChunkRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            chunk_id: chunk_id_off.as_ref(),
            write_granularity: req.write_granularity,
            strip_count: req.strip_count,
            strip_type: strip_type_to_fb(
                ProtoStripType::try_from(req.strip_type).unwrap_or(ProtoStripType::Mirror),
            ),
            data_num: req.data_num,
            code_num: req.code_num,
            copy_count: req.copy_count,
            chunk_type: chunk_type_to_fb(
                ProtoChunkType::try_from(req.chunk_type).unwrap_or(ProtoChunkType::Repo),
            ),
        };
        let fb_req = FBAllocateChunkRequest::create(&mut builder, &args);
        builder.finish(fb_req, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let msg_type = FBMsgType::EAllocateChunkRequest.0 as u16;
        let resp = call_rpc(
            &self.rpc,
            &self.server,
            &conn,
            req_id,
            control,
            msg_type,
            rpc_endpoint,
        )
        .await?;
        let r = FBAllocateChunkResponseRef::new(resp.bytes());
        if !r.valid() {
            return Err(ChunkdbClientError::Rpc(
                "allocate_chunk response malformed".into(),
            ));
        }
        check_ret_code(r.ret_code(), r.error_msg())?;
        Ok(AllocateChunkResponse {
            chunk: r.chunk().map(|fb_chunk| parse_fb_chunk(&fb_chunk)),
        })
    }

    // ── AppendChunk ───────────────────────────────────────────────

    /// Send an `AppendChunk` request via crowdb-rpc.
    pub async fn send_append_chunk(
        &self,
        rpc_endpoint: &str,
        req: &crowdb_protocol::chunkdb::rpc::AppendChunkRequest,
    ) -> Result<AppendChunkResponse> {
        let req_id = self.next_id();
        let conn = self.conn_for(rpc_endpoint)?;
        let mut builder = FlatBufferBuilder::new();
        let chunk_id_off = req.chunk_id.as_ref().map(|id| FBInt128::new(id.high, id.low));
        let args = FBAppendChunkRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            chunk_id: chunk_id_off.as_ref(),
            strip_size: req.strip_size,
            strip_count: req.strip_count,
            strip_type: strip_type_to_fb(
                ProtoStripType::try_from(req.strip_type).unwrap_or(ProtoStripType::Mirror),
            ),
            data_num: req.data_num,
            code_num: req.code_num,
            copy_count: req.copy_count,
        };
        let fb_req = FBAppendChunkRequest::create(&mut builder, &args);
        builder.finish(fb_req, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let msg_type = FBMsgType::EAppendChunkRequest.0 as u16;
        let resp = call_rpc(
            &self.rpc,
            &self.server,
            &conn,
            req_id,
            control,
            msg_type,
            rpc_endpoint,
        )
        .await?;
        // AppendChunk response shares the same shape as AllocateChunkResponse.
        let r = FBAllocateChunkResponseRef::new(resp.bytes());
        if !r.valid() {
            return Err(ChunkdbClientError::Rpc("append_chunk response malformed".into()));
        }
        check_ret_code(r.ret_code(), r.error_msg())?;
        Ok(AppendChunkResponse {
            chunk: r.chunk().map(|fb_chunk| parse_fb_chunk(&fb_chunk)),
        })
    }

    // ── QueryChunk ────────────────────────────────────────────────

    /// Send a `QueryChunk` request via crowdb-rpc.
    pub async fn send_query_chunk(
        &self,
        rpc_endpoint: &str,
        req: &crowdb_protocol::chunkdb::rpc::QueryChunkRequest,
    ) -> Result<QueryChunkResponse> {
        let req_id = self.next_id();
        let conn = self.conn_for(rpc_endpoint)?;
        let mut builder = FlatBufferBuilder::new();
        let chunk_id_off = req.chunk_id.as_ref().map(|id| FBInt128::new(id.high, id.low));
        let args = FBQueryChunkRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            chunk_id: chunk_id_off.as_ref(),
        };
        let fb_req = FBQueryChunkRequest::create(&mut builder, &args);
        builder.finish(fb_req, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let msg_type = FBMsgType::EQueryChunkRequest.0 as u16;
        let resp = call_rpc(
            &self.rpc,
            &self.server,
            &conn,
            req_id,
            control,
            msg_type,
            rpc_endpoint,
        )
        .await?;
        let r = FBAllocateChunkResponseRef::new(resp.bytes());
        if !r.valid() {
            return Err(ChunkdbClientError::Rpc("query_chunk response malformed".into()));
        }
        check_ret_code(r.ret_code(), r.error_msg())?;
        Ok(QueryChunkResponse {
            chunk: r.chunk().map(|fb_chunk| parse_fb_chunk(&fb_chunk)),
        })
    }

    // ── SealChunk ─────────────────────────────────────────────────

    /// Send a `SealChunk` request via crowdb-rpc.
    pub async fn send_seal_chunk(
        &self,
        rpc_endpoint: &str,
        req: &crowdb_protocol::chunkdb::rpc::SealChunkRequest,
    ) -> Result<SealChunkResponse> {
        let req_id = self.next_id();
        let conn = self.conn_for(rpc_endpoint)?;
        let mut builder = FlatBufferBuilder::new();
        let chunk_id_off = req.chunk_id.as_ref().map(|id| FBInt128::new(id.high, id.low));
        let args = FBSealChunkRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            chunk_id: chunk_id_off.as_ref(),
            seal_length: req.seal_length,
        };
        let fb_req = FBSealChunkRequest::create(&mut builder, &args);
        builder.finish(fb_req, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let msg_type = FBMsgType::ESealChunkRequest.0 as u16;
        let resp = call_rpc(
            &self.rpc,
            &self.server,
            &conn,
            req_id,
            control,
            msg_type,
            rpc_endpoint,
        )
        .await?;
        let r = FBAllocateChunkResponseRef::new(resp.bytes());
        if !r.valid() {
            return Err(ChunkdbClientError::Rpc("seal_chunk response malformed".into()));
        }
        check_ret_code(r.ret_code(), r.error_msg())?;
        Ok(SealChunkResponse {
            chunk: r.chunk().map(|fb_chunk| parse_fb_chunk(&fb_chunk)),
        })
    }

    // ── DeleteChunk ───────────────────────────────────────────────

    /// Send a `DeleteChunk` request via crowdb-rpc.
    pub async fn send_delete_chunk(
        &self,
        rpc_endpoint: &str,
        req: &crowdb_protocol::chunkdb::rpc::DeleteChunkRequest,
    ) -> Result<DeleteChunkResponse> {
        let req_id = self.next_id();
        let conn = self.conn_for(rpc_endpoint)?;
        let mut builder = FlatBufferBuilder::new();
        let chunk_id_off = req.chunk_id.as_ref().map(|id| FBInt128::new(id.high, id.low));
        let args = FBDeleteChunkRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            chunk_id: chunk_id_off.as_ref(),
        };
        let fb_req = FBDeleteChunkRequest::create(&mut builder, &args);
        builder.finish(fb_req, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let msg_type = FBMsgType::EDeleteChunkRequest.0 as u16;
        let resp = call_rpc(
            &self.rpc,
            &self.server,
            &conn,
            req_id,
            control,
            msg_type,
            rpc_endpoint,
        )
        .await?;
        let r = FBAllocateChunkResponseRef::new(resp.bytes());
        if !r.valid() {
            return Err(ChunkdbClientError::Rpc("delete_chunk response malformed".into()));
        }
        check_ret_code(r.ret_code(), r.error_msg())?;
        Ok(DeleteChunkResponse {
            chunk: r.chunk().map(|fb_chunk| parse_fb_chunk(&fb_chunk)),
        })
    }

    // ── DeleteChunkRange ──────────────────────────────────────────

    /// Send a `DeleteChunkRange` request via crowdb-rpc.
    pub async fn send_delete_chunk_range(
        &self,
        rpc_endpoint: &str,
        req: &crowdb_protocol::chunkdb::rpc::DeleteChunkRangeRequest,
    ) -> Result<DeleteChunkRangeResponse> {
        let req_id = self.next_id();
        let conn = self.conn_for(rpc_endpoint)?;
        let mut builder = FlatBufferBuilder::new();
        let chunk_id_off = req.chunk_id.as_ref().map(|id| FBInt128::new(id.high, id.low));
        let args = FBDeleteChunkRangeRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            chunk_id: chunk_id_off.as_ref(),
            chunk_offset: req.chunk_offset,
            chunk_size: req.chunk_size,
        };
        let fb_req = FBDeleteChunkRangeRequest::create(&mut builder, &args);
        builder.finish(fb_req, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let msg_type = FBMsgType::EDeleteChunkRangeRequest.0 as u16;
        let resp = call_rpc(
            &self.rpc,
            &self.server,
            &conn,
            req_id,
            control,
            msg_type,
            rpc_endpoint,
        )
        .await?;
        let r = FBDeleteChunkRangeResponseRef::new(resp.bytes());
        if !r.valid() {
            return Err(ChunkdbClientError::Rpc(
                "delete_chunk_range response malformed".into(),
            ));
        }
        check_ret_code(r.ret_code(), r.error_msg())?;
        Ok(DeleteChunkRangeResponse {})
    }

    // ── UpdateChunkStrip ──────────────────────────────────────────

    /// Send an `UpdateChunkStrip` request via crowdb-rpc.
    pub async fn send_update_chunk_strip(
        &self,
        rpc_endpoint: &str,
        req: &crowdb_protocol::chunkdb::rpc::UpdateChunkStripRequest,
    ) -> Result<UpdateChunkStripResponse> {
        let req_id = self.next_id();
        let conn = self.conn_for(rpc_endpoint)?;
        let mut builder = FlatBufferBuilder::new();
        let chunk_id_off = req.chunk_id.as_ref().map(|id| FBInt128::new(id.high, id.low));
        let strip_off = req
            .strip
            .as_ref()
            .map(|s| build_chunk_strip_offset(&mut builder, s));
        let args = FBUpdateChunkStripRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            chunk_id: chunk_id_off.as_ref(),
            strip_index: req.strip_index,
            strip: strip_off,
        };
        let fb_req = FBUpdateChunkStripRequest::create(&mut builder, &args);
        builder.finish(fb_req, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let msg_type = FBMsgType::EUpdateChunkStripRequest.0 as u16;
        let resp = call_rpc(
            &self.rpc,
            &self.server,
            &conn,
            req_id,
            control,
            msg_type,
            rpc_endpoint,
        )
        .await?;
        let r = FBAllocateChunkResponseRef::new(resp.bytes());
        if !r.valid() {
            return Err(ChunkdbClientError::Rpc(
                "update_chunk_strip response malformed".into(),
            ));
        }
        check_ret_code(r.ret_code(), r.error_msg())?;
        Ok(UpdateChunkStripResponse {
            chunk: r.chunk().map(|fb_chunk| parse_fb_chunk(&fb_chunk)),
        })
    }

    // ── ListChunks ────────────────────────────────────────────────

    /// Send a `ListChunks` request via crowdb-rpc.
    pub async fn send_list_chunks(
        &self,
        rpc_endpoint: &str,
        req: &crowdb_protocol::chunkdb::rpc::ListChunksRequest,
    ) -> Result<ListChunksResponse> {
        let req_id = self.next_id();
        let conn = self.conn_for(rpc_endpoint)?;
        let mut builder = FlatBufferBuilder::new();
        let start_token_off = req.start_token.as_ref().map(|id| FBInt128::new(id.high, id.low));
        let args = FBListChunksRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            start_token: start_token_off.as_ref(),
            partition: req.partition,
            max_keys: req.max_keys,
        };
        let fb_req = FBListChunksRequest::create(&mut builder, &args);
        builder.finish(fb_req, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let msg_type = FBMsgType::EListChunksRequest.0 as u16;
        let resp = call_rpc(
            &self.rpc,
            &self.server,
            &conn,
            req_id,
            control,
            msg_type,
            rpc_endpoint,
        )
        .await?;
        let r = FBListChunksResponseRef::new(resp.bytes());
        if !r.valid() {
            return Err(ChunkdbClientError::Rpc("list_chunks response malformed".into()));
        }
        check_ret_code(r.ret_code(), r.error_msg())?;
        let chunks: Vec<Chunk> = r
            .chunks()
            .map(|v| v.iter().map(|fb_chunk| parse_fb_chunk(&fb_chunk)).collect())
            .unwrap_or_default();
        let next_token = r.next_token().map(|id| ChunkId {
            high: id.high(),
            low: id.low(),
        });
        Ok(ListChunksResponse { chunks, next_token })
    }
}

fn rr_index(counter: &AtomicU64, len: usize) -> usize {
    let len_u64 = u64::try_from(len).unwrap_or(u64::MAX);
    usize::try_from(counter.fetch_add(1, Ordering::Relaxed) % len_u64).unwrap_or(0)
}

impl Default for ChunkdbRpcTransport {
    fn default() -> Self {
        Self::new()
    }
}

// ── RPC call helper ───────────────────────────────────────────────

/// Execute a crowdb-rpc call and return the control buffer.
async fn call_rpc(
    rpc: &RpcClient,
    server: &RpcServer,
    conn: &Connection,
    req_id: u64,
    control: Buffer,
    msg_type: u16,
    rpc_endpoint: &str,
) -> Result<Buffer> {
    let fut = rpc
        .call(server, conn, req_id, control, None, msg_type)
        .map_err(rpc_error_to_client)?;
    let resp = fut.await.map_err(rpc_error_to_client)?;
    resp.control.ok_or_else(|| {
        ChunkdbClientError::Rpc(format!("response missing control buffer from {rpc_endpoint}"))
    })
}

// ── Response parsing ──────────────────────────────────────────────

/// Check the `ret_code` and return an error if not Success.
fn check_ret_code(code: FBChunkdbRetCode, error_msg: Option<&str>) -> Result<()> {
    let msg = error_msg.unwrap_or("").to_string();
    match code {
        FBChunkdbRetCode::Success => Ok(()),
        FBChunkdbRetCode::InvalidArgument | FBChunkdbRetCode::StripIndexOutOfRange => {
            Err(ChunkdbClientError::InvalidArgument(msg))
        }
        FBChunkdbRetCode::NotFound => Err(ChunkdbClientError::NotFound(msg)),
        FBChunkdbRetCode::AlreadyExists => Err(ChunkdbClientError::AlreadyExists(msg)),
        FBChunkdbRetCode::FailedPrecondition => Err(ChunkdbClientError::FailedPrecondition(msg)),
        FBChunkdbRetCode::Aborted => Err(ChunkdbClientError::Aborted(msg)),
        FBChunkdbRetCode::Internal => Err(ChunkdbClientError::Internal(msg)),
        FBChunkdbRetCode::Unavailable => Err(ChunkdbClientError::Unavailable(msg)),
        FBChunkdbRetCode::NotMyRange => Err(ChunkdbClientError::NotMyRange(msg)),
        _ => Err(ChunkdbClientError::Internal(format!(
            "unknown ret_code {code:?}: {msg}"
        ))),
    }
}

/// Parse a flatbuffer `FBChunk` into a proto `Chunk`.
fn parse_fb_chunk(fb: &crowdb_protocol::chunkdb_fb::FBChunk<'_>) -> Chunk {
    let id = fb.id().map(|id| ChunkId {
        high: id.high(),
        low: id.low(),
    });
    let state = fb_chunk_state_to_proto(fb.state());
    let chunk_type = fb_chunk_type_to_proto(fb.chunk_type());
    let strips: Vec<ChunkStrip> = fb
        .strips()
        .map(|v| v.iter().map(|s| parse_fb_chunk_strip(&s)).collect())
        .unwrap_or_default();
    Chunk {
        id,
        state: state as i32,
        create_ts_ms: fb.create_ts_ms(),
        sealed_ts_ms: fb.sealed_ts_ms(),
        capacity: fb.capacity(),
        sealed_length: fb.sealed_length(),
        strips,
        chunk_type: chunk_type as i32,
    }
}

/// Parse a flatbuffer `FBChunkStrip` into a proto `ChunkStrip`.
fn parse_fb_chunk_strip(fb: &FBChunkStrip<'_>) -> ChunkStrip {
    use crowdb_protocol::chunkdb_fb::{FBEcStrip, FBMirrorStrip, FBStripBody};

    let strip_type = fb_strip_type_to_proto(fb.strip_type());
    let strip = match fb.strip_body_type() {
        FBStripBody::FBMirrorStrip => {
            let mirror = fb.strip_body_as_fbmirror_strip();
            let segments = mirror
                .map(|m: FBMirrorStrip<'_>| parse_fb_segments(m.segments()))
                .unwrap_or_default();
            Some(ProtoStrip::MirrorStrip(MirrorStrip { segments }))
        }
        FBStripBody::FBEcStrip => {
            let ec = fb.strip_body_as_fbec_strip();
            let segments = ec
                .map(|e: FBEcStrip<'_>| parse_fb_segments(e.segments()))
                .unwrap_or_default();
            let ec_state = ec.map_or(ProtoEcState::NoParity, |e| fb_ec_state_to_proto(e.ec_state()));
            let (data_num, code_num) = ec.map_or((0, 0), |e| (e.data_num(), e.code_num()));
            Some(ProtoStrip::EcStrip(EcStrip {
                data_num,
                code_num,
                ec_state: ec_state as i32,
                segments,
            }))
        }
        _ => None,
    };
    let usage_bitmap = fb
        .usage_bitmap()
        .map(|v| v.iter().collect::<Vec<u8>>())
        .unwrap_or_default();
    ChunkStrip {
        chunk_offset: fb.chunk_offset(),
        strip_sequence: fb.strip_sequence(),
        unit_kb: fb.unit_kb(),
        capacity: fb.capacity(),
        create_ts_ms: fb.create_ts_ms(),
        sealed_ts_ms: fb.sealed_ts_ms(),
        sealed_length: fb.sealed_length(),
        strip_type: strip_type as i32,
        strip,
        usage_bitmap,
    }
}

/// Parse a flatbuffer `FBSegment` vector into proto `Segment`s.
fn parse_fb_segments<'a, V>(fb_segs: Option<V>) -> Vec<crowdb_protocol::diskdb::rpc::Segment>
where
    V: IntoIterator<Item = &'a crowdb_protocol::chunkdb_fb::FBSegment>,
{
    let Some(vec) = fb_segs else {
        return Vec::new();
    };
    vec.into_iter()
        .map(|s| crowdb_protocol::diskdb::rpc::Segment {
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
}

// ── Request building: ChunkStrip offset ───────────────────────────

/// Build a `FBChunkStrip` `WIPOffset` from a proto `ChunkStrip` for
/// request construction (`UpdateChunkStrip`).
fn build_chunk_strip_offset<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    strip: &ChunkStrip,
) -> flatbuffers::WIPOffset<FBChunkStrip<'a>> {
    use crowdb_protocol::chunkdb_fb::FBChunkStripArgs;

    let strip_type = ProtoStripType::try_from(strip.strip_type).unwrap_or(ProtoStripType::Mirror);
    let (body_type, body_off) = build_strip_body_offset(fbb, strip);
    let usage_bitmap_off = if strip.usage_bitmap.is_empty() {
        None
    } else {
        Some(fbb.create_vector(&strip.usage_bitmap))
    };
    FBChunkStrip::create(
        fbb,
        &FBChunkStripArgs {
            chunk_offset: strip.chunk_offset,
            strip_sequence: strip.strip_sequence,
            unit_kb: strip.unit_kb,
            capacity: strip.capacity,
            create_ts_ms: strip.create_ts_ms,
            sealed_ts_ms: strip.sealed_ts_ms,
            sealed_length: strip.sealed_length,
            strip_type: strip_type_to_fb(strip_type),
            strip_body_type: body_type,
            strip_body: body_off,
            usage_bitmap: usage_bitmap_off,
        },
    )
}

/// Build the strip body union offset from a proto `ChunkStrip`.
fn build_strip_body_offset(
    fbb: &mut FlatBufferBuilder<'_>,
    strip: &ChunkStrip,
) -> (
    FBStripBody,
    Option<flatbuffers::WIPOffset<flatbuffers::UnionWIPOffset>>,
) {
    use crowdb_protocol::chunkdb_fb::{
        FBEcStrip, FBEcStripArgs, FBMirrorStrip, FBMirrorStripArgs, FBSegment,
    };

    let Some(ref body) = strip.strip else {
        return (FBStripBody::NONE, None);
    };
    match body {
        ProtoStrip::MirrorStrip(mirror) => {
            let seg_offs: Vec<FBSegment> = mirror
                .segments
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
            let seg_vec = fbb.create_vector(&seg_offs);
            let off = FBMirrorStrip::create(
                fbb,
                &FBMirrorStripArgs {
                    segments: Some(seg_vec),
                },
            );
            (FBStripBody::FBMirrorStrip, Some(off.as_union_value()))
        }
        ProtoStrip::EcStrip(ec) => {
            let seg_offs: Vec<FBSegment> = ec
                .segments
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
            let seg_vec = fbb.create_vector(&seg_offs);
            let ec_state = ProtoEcState::try_from(ec.ec_state).unwrap_or(ProtoEcState::NoParity);
            let off = FBEcStrip::create(
                fbb,
                &FBEcStripArgs {
                    data_num: ec.data_num,
                    code_num: ec.code_num,
                    ec_state: ec_state_to_fb(ec_state),
                    segments: Some(seg_vec),
                },
            );
            (FBStripBody::FBEcStrip, Some(off.as_union_value()))
        }
    }
}

// ── Enum conversion helpers ───────────────────────────────────────

fn strip_type_to_fb(t: ProtoStripType) -> FBStripType {
    match t {
        ProtoStripType::Mirror => FBStripType::Mirror,
        ProtoStripType::Ec => FBStripType::Ec,
    }
}

fn fb_strip_type_to_proto(t: FBStripType) -> ProtoStripType {
    match t {
        FBStripType::Ec => ProtoStripType::Ec,
        _ => ProtoStripType::Mirror,
    }
}

fn chunk_type_to_fb(t: ProtoChunkType) -> FBChunkType {
    match t {
        ProtoChunkType::Repo => FBChunkType::Repo,
        ProtoChunkType::Wal => FBChunkType::Wal,
        ProtoChunkType::BtreePage => FBChunkType::BtreePage,
        ProtoChunkType::PageIndex => FBChunkType::PageIndex,
    }
}

fn fb_chunk_type_to_proto(t: FBChunkType) -> ProtoChunkType {
    match t {
        FBChunkType::Wal => ProtoChunkType::Wal,
        FBChunkType::BtreePage => ProtoChunkType::BtreePage,
        FBChunkType::PageIndex => ProtoChunkType::PageIndex,
        _ => ProtoChunkType::Repo,
    }
}

fn fb_chunk_state_to_proto(s: FBChunkState) -> ProtoChunkState {
    match s {
        FBChunkState::Active => ProtoChunkState::Active,
        FBChunkState::Sealed => ProtoChunkState::Sealed,
        FBChunkState::Deleted => ProtoChunkState::Deleted,
        _ => ProtoChunkState::Init,
    }
}

fn ec_state_to_fb(s: ProtoEcState) -> crowdb_protocol::chunkdb_fb::FBEcState {
    match s {
        ProtoEcState::NoParity => crowdb_protocol::chunkdb_fb::FBEcState::NoParity,
        ProtoEcState::Parity => crowdb_protocol::chunkdb_fb::FBEcState::Parity,
    }
}

fn fb_ec_state_to_proto(s: crowdb_protocol::chunkdb_fb::FBEcState) -> ProtoEcState {
    match s {
        crowdb_protocol::chunkdb_fb::FBEcState::Parity => ProtoEcState::Parity,
        _ => ProtoEcState::NoParity,
    }
}

// ── Endpoint helpers ──────────────────────────────────────────────

fn rpc_error_to_client(e: RpcError) -> ChunkdbClientError {
    ChunkdbClientError::Rpc(format!("rpc error: {e:?}"))
}

fn normalize_endpoint(endpoint: &str) -> String {
    let with_scheme = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    };
    with_scheme.replacen("://0.0.0.0:", "://127.0.0.1:", 1)
}

fn parse_endpoint(endpoint: &str) -> std::result::Result<(String, i32), String> {
    let without_scheme = endpoint
        .strip_prefix("http://")
        .or_else(|| endpoint.strip_prefix("https://"))
        .unwrap_or(endpoint);
    let (host, port_str) = without_scheme
        .rsplit_once(':')
        .ok_or_else(|| format!("invalid endpoint: {endpoint}"))?;
    let port: i32 = port_str
        .parse()
        .map_err(|_| format!("invalid port in endpoint: {endpoint}"))?;
    Ok((host.to_string(), port))
}
