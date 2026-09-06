// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `DiskioClient`: async disk I/O via RPC.

use crowdb_common::RequestIdGen;
use crowdb_protocol::diskio_fb::{
    FBDiskFsyncRequest, FBDiskFsyncRequestArgs, FBDiskFsyncResponse, FBDiskReadRequest,
    FBDiskReadRequestArgs, FBDiskReadResponse, FBDiskWriteRequest, FBDiskWriteRequestArgs,
    FBDiskWriteResponse, FBInt128 as FBDiskInt128,
};
use crowdb_protocol::fb::FBMsgType;
use crowdb_rpc_ffi::{Buffer, CallFuture, Connection, RpcClient, RpcError, RpcServer};
use flatbuffers::FlatBufferBuilder;
use thiserror::Error;

/// 128-bit disk identifier (high + low 64-bit halves).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DiskId {
    pub high: u64,
    pub low: u64,
}

impl DiskId {
    #[must_use]
    pub fn new(high: u64, low: u64) -> Self {
        Self { high, low }
    }

    #[must_use]
    pub fn to_fb(&self) -> FBDiskInt128 {
        let mut bytes = [0u8; 16];
        bytes[0..8].copy_from_slice(&self.high.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.low.to_le_bytes());
        FBDiskInt128(bytes)
    }
}

/// Disk I/O return codes (mirrors `FBDiskIoRetCode`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
pub enum DiskIoRetCode {
    Success = 0,
    DiskNotExist = 1,
    ZoneNotExist = 2,
    IoError = 3,
    PartialWrite = 4,
    InvalidAlignment = 5,
    ConnectionError = 6,
}

#[derive(Clone, Copy)]
struct WriteTarget {
    disk_id: DiskId,
    zone_index: u32,
    zone_offset: u64,
    size: u32,
}

impl From<i16> for DiskIoRetCode {
    fn from(v: i16) -> Self {
        match v {
            0 => Self::Success,
            1 => Self::DiskNotExist,
            2 => Self::ZoneNotExist,
            4 => Self::PartialWrite,
            5 => Self::InvalidAlignment,
            6 => Self::ConnectionError,
            _ => Self::IoError,
        }
    }
}

/// Error type for diskio client operations.
#[derive(Debug, Error)]
pub enum DiskioError {
    #[error("disk I/O error: {0:?}")]
    IoError(DiskIoRetCode),
    #[error("RPC error: {0}")]
    Rpc(String),
}

impl From<RpcError> for DiskioError {
    fn from(e: RpcError) -> Self {
        Self::Rpc(format!("{e:?}"))
    }
}

pub type DiskioResult<T> = std::result::Result<T, DiskioError>;

/// `DiskioClient` sends disk write/read/fsync requests via crowdb-rpc.
pub struct DiskioClient {
    rpc: RpcClient,
    req_id_gen: RequestIdGen,
}

impl std::fmt::Debug for DiskioClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiskioClient")
            .field("req_id_gen", &"RequestIdGen")
            .finish_non_exhaustive()
    }
}

impl DiskioClient {
    /// Create a new `DiskioClient`. Call `attach()` before issuing requests.
    #[must_use]
    pub fn new() -> Self {
        let rpc = RpcClient::new();
        rpc.set_completion_pool_size(1024);
        rpc.start_reaper(5_000_000_000, 500_000_000);
        Self {
            rpc,
            req_id_gen: RequestIdGen::new(),
        }
    }

    /// Attach to a connection (routes responses to this client).
    pub fn attach(&self, conn: &Connection) {
        self.rpc.attach(conn);
    }

    fn next_id(&self) -> u64 {
        self.req_id_gen.next().as_u64()
    }

    /// Send a disk write request. `data` is the payload to write.
    /// Returns a `CallFuture` that resolves to the response.
    ///
    /// # Errors
    ///
    /// Returns `DiskioError::Rpc` if the data is too large or the send fails.
    pub fn write(
        &self,
        server: &RpcServer,
        conn: &Connection,
        disk_id: DiskId,
        zone_index: u32,
        zone_offset: u64,
        data: Vec<u8>,
    ) -> Result<CallFuture, DiskioError> {
        let size = u32::try_from(data.len()).map_err(|_| DiskioError::Rpc("data too large".into()))?;
        self.write_buffer(
            server,
            conn,
            WriteTarget {
                disk_id,
                zone_index,
                zone_offset,
                size,
            },
            Buffer::from_vec(data),
        )
    }

    /// Send a disk write while retaining an owned `Bytes` allocation through
    /// RPC completion, without copying it into a `Vec` or C++ buffer.
    ///
    /// # Errors
    ///
    /// Returns `DiskioError::Rpc` if the data is too large or the send fails.
    pub fn write_bytes(
        &self,
        server: &RpcServer,
        conn: &Connection,
        disk_id: DiskId,
        zone_index: u32,
        zone_offset: u64,
        data: bytes::Bytes,
    ) -> Result<CallFuture, DiskioError> {
        let size = u32::try_from(data.len()).map_err(|_| DiskioError::Rpc("data too large".into()))?;
        self.write_buffer(
            server,
            conn,
            WriteTarget {
                disk_id,
                zone_index,
                zone_offset,
                size,
            },
            Buffer::from_owned_bytes(data),
        )
    }

    fn write_buffer(
        &self,
        server: &RpcServer,
        conn: &Connection,
        target: WriteTarget,
        data_buf: Buffer,
    ) -> Result<CallFuture, DiskioError> {
        let req_id = self.next_id();
        let mut fbb = FlatBufferBuilder::new();
        let fb_disk_id = target.disk_id.to_fb();
        let off = FBDiskWriteRequest::create(
            &mut fbb,
            &FBDiskWriteRequestArgs {
                id: req_id,
                rpc_create_nano: 0,
                disk_id: Some(&fb_disk_id),
                zone_index: target.zone_index,
                zone_offset: target.zone_offset,
                size: target.size,
            },
        );
        fbb.finish(off, None);
        let control = Buffer::from_bytes(fbb.finished_data());
        let msg_type = FBMsgType::EDiskWriteRequest.0 as u16;
        self.rpc
            .call(server, conn, req_id, control, Some(data_buf), msg_type)
            .map_err(DiskioError::from)
    }

    /// Send a disk read request. Returns a `CallFuture` that resolves to
    /// the response (`ret_code` + data).
    ///
    /// `test_pattern_offset` is used by `NullDisk` for deterministic content
    /// generation (testing only); real engines ignore it. Pass the physical
    /// offset (`zone_index * zone_size + zone_offset`) for raw disk reads,
    /// or a logical object offset for object reads.
    ///
    /// # Errors
    ///
    /// Returns `DiskioError::Rpc` if the send fails.
    #[allow(clippy::too_many_arguments)]
    pub fn read(
        &self,
        server: &RpcServer,
        conn: &Connection,
        disk_id: DiskId,
        zone_index: u32,
        zone_offset: u64,
        size: u32,
        test_pattern_offset: u64,
    ) -> Result<CallFuture, DiskioError> {
        let req_id = self.next_id();
        let mut fbb = FlatBufferBuilder::new();
        let fb_disk_id = disk_id.to_fb();
        let off = FBDiskReadRequest::create(
            &mut fbb,
            &FBDiskReadRequestArgs {
                id: req_id,
                rpc_create_nano: 0,
                disk_id: Some(&fb_disk_id),
                zone_index,
                zone_offset,
                size,
                test_pattern_offset,
            },
        );
        fbb.finish(off, None);
        let control = Buffer::from_bytes(fbb.finished_data());
        let msg_type = FBMsgType::EDiskReadRequest.0 as u16;
        self.rpc
            .call(server, conn, req_id, control, None, msg_type)
            .map_err(DiskioError::from)
    }

    /// Send a disk fsync request. Returns a `CallFuture` that resolves to
    /// the response.
    ///
    /// # Errors
    ///
    /// Returns `DiskioError::Rpc` if the send fails.
    pub fn fsync(
        &self,
        server: &RpcServer,
        conn: &Connection,
        disk_id: DiskId,
    ) -> Result<CallFuture, DiskioError> {
        let req_id = self.next_id();
        let mut fbb = FlatBufferBuilder::new();
        let fb_disk_id = disk_id.to_fb();
        let off = FBDiskFsyncRequest::create(
            &mut fbb,
            &FBDiskFsyncRequestArgs {
                id: req_id,
                rpc_create_nano: 0,
                disk_id: Some(&fb_disk_id),
            },
        );
        fbb.finish(off, None);
        let control = Buffer::from_bytes(fbb.finished_data());
        let msg_type = FBMsgType::EDiskFsyncRequest.0 as u16;
        self.rpc
            .call(server, conn, req_id, control, None, msg_type)
            .map_err(DiskioError::from)
    }

    /// Parse a write response from a completed `CallFuture`.
    ///
    /// # Errors
    ///
    /// Returns `DiskioError::Rpc` if the response is missing or invalid.
    pub async fn await_write_response(fut: CallFuture) -> DiskioResult<DiskIoRetCode> {
        let resp = fut.await.map_err(DiskioError::from)?;
        parse_ret_code(&resp)
    }

    /// Parse a read response from a completed `CallFuture`.
    /// Returns (`ret_code`, data) on success.
    ///
    /// # Errors
    ///
    /// Returns `DiskioError::Rpc` if the response is missing or invalid.
    pub async fn await_read_response(fut: CallFuture) -> DiskioResult<(DiskIoRetCode, Option<Vec<u8>>)> {
        let resp = fut.await.map_err(DiskioError::from)?;
        let code = parse_ret_code(&resp)?;
        let data = resp.data.map(|b| b.bytes().to_vec());
        Ok((code, data))
    }

    /// Parse an fsync response from a completed `CallFuture`.
    ///
    /// # Errors
    ///
    /// Returns `DiskioError::Rpc` if the response is missing or invalid.
    pub async fn await_fsync_response(fut: CallFuture) -> DiskioResult<DiskIoRetCode> {
        Self::await_write_response(fut).await
    }
}

impl Default for DiskioClient {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse the `ret_code` from a diskio response control buffer.
fn parse_ret_code(resp: &crowdb_rpc_ffi::Response) -> DiskioResult<DiskIoRetCode> {
    let ctrl = resp
        .control
        .as_ref()
        .ok_or_else(|| DiskioError::Rpc("missing control buffer in response".into()))?;
    let ctrl_bytes = ctrl.bytes();
    let raw = if let Ok(r) = flatbuffers::root::<FBDiskWriteResponse>(ctrl_bytes) {
        r.ret_code().0
    } else if let Ok(r) = flatbuffers::root::<FBDiskReadResponse>(ctrl_bytes) {
        r.ret_code().0
    } else if let Ok(r) = flatbuffers::root::<FBDiskFsyncResponse>(ctrl_bytes) {
        r.ret_code().0
    } else {
        return Err(DiskioError::Rpc("invalid response flatbuffer".into()));
    };
    let code = DiskIoRetCode::from(raw);
    if code == DiskIoRetCode::Success {
        Ok(code)
    } else {
        Err(DiskioError::IoError(code))
    }
}
