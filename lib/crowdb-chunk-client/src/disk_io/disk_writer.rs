// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `DiskWriter` trait + `DiskioBlockWriter` (production impl).
//!
//! `DiskWriter` is the block-IO seam. `write` takes a `Segment`
//! directly (carrying disk_id, zone_index, unit_offset) + `unit_bytes`
//! to compute the byte offset — removing the repeated
//! disk_id/zone/offset extraction from call sites. `fsync` flushes a
//! disk. Production impl wraps `DiskioClient`; test impl
//! (`LocalFileDiskWriter`) writes to local files.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_diskio_client::{DiskId, DiskIoRetCode, DiskioClient};
use crowdb_protocol::diskdb::rpc::Segment;
use crowdb_rpc_ffi::{Connection, RpcServer};

use crate::{IoError, Result};

/// Block-IO seam. Write data blocks to disk + fsync.
#[async_trait]
pub trait DiskWriter: Send + Sync {
    /// Write `data` to the disk/zone/offset described by `seg`.
    /// `unit_bytes` converts `seg.unit_offset` to a byte offset.
    async fn write(&self, seg: &Segment, unit_bytes: u64, data: Bytes) -> Result<()>;

    /// Flush all pending writes on `disk_id` to durable storage.
    async fn fsync(&self, disk_id: DiskId) -> Result<()>;
}

/// Production `DiskWriter` — wraps `DiskioClient` + `RpcServer` +
/// `Connection`. Each `write`/`fsync` sends the RPC and awaits the
/// response, checking the return code.
pub struct DiskioBlockWriter {
    client: Arc<DiskioClient>,
    server: Arc<RpcServer>,
    conn: Connection,
}

impl DiskioBlockWriter {
    /// Construct a new writer. The client must be attached to the
    /// connection before use.
    #[must_use]
    pub fn new(client: Arc<DiskioClient>, server: Arc<RpcServer>, conn: Connection) -> Self {
        Self { client, server, conn }
    }
}

#[async_trait]
impl DiskWriter for DiskioBlockWriter {
    async fn write(&self, seg: &Segment, unit_bytes: u64, data: Bytes) -> Result<()> {
        let disk_id = seg
            .disk_id
            .as_ref()
            .ok_or_else(|| IoError::WriteFailed("segment missing disk_id".into()))?;
        let disk_id = DiskId::new(disk_id.high, disk_id.low);
        let zone_offset = seg.unit_offset * unit_bytes;
        let fut = self
            .client
            .write_bytes(
                &self.server,
                &self.conn,
                disk_id,
                seg.zone_index,
                zone_offset,
                data,
            )
            .map_err(|e| IoError::WriteFailed(e.to_string()))?;
        let code = DiskioClient::await_write_response(fut)
            .await
            .map_err(|e| IoError::WriteFailed(e.to_string()))?;
        if code != DiskIoRetCode::Success {
            return Err(IoError::WriteFailed(format!("disk write returned {code:?}")));
        }
        Ok(())
    }

    async fn fsync(&self, disk_id: DiskId) -> Result<()> {
        let fut = self
            .client
            .fsync(&self.server, &self.conn, disk_id)
            .map_err(|e| IoError::WriteFailed(e.to_string()))?;
        let code = DiskioClient::await_fsync_response(fut)
            .await
            .map_err(|e| IoError::WriteFailed(e.to_string()))?;
        if code != DiskIoRetCode::Success {
            return Err(IoError::WriteFailed(format!("disk fsync returned {code:?}")));
        }
        Ok(())
    }
}
