// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Application-facing chunk IO client and prepared large writes.

use std::sync::Arc;
use std::time::{Duration, Instant};

use crowdb_chunkdb_client::{ChunkdbClient, ChunkdbRpcTransport};
use crowdb_common::ec::EcScheme;
use crowdb_kv_client::{ClientConfig, CrowdbKvClient, HardwareClient, ServiceRegistryClient};
use crowdb_protocol::chunkdb::rpc::Location;

use crate::{ChunkClientConfig, DiskWriter, LargeAsyncObjectWriter, Result, RoutedDiskWriter};

/// Discovery and transport configuration for [`ChunkIoClient`].
#[derive(Debug, Clone)]
pub struct ChunkIoClientConfig {
    /// KV management endpoints used to discover ChunkDB, DiskIO, and disks.
    pub management_seeds: Vec<String>,
}

/// Large-write EC and bounded-buffer policy.
#[derive(Debug, Clone)]
pub struct LargeWritePolicy {
    pub ec_scheme: EcScheme,
    pub client: Arc<ChunkClientConfig>,
}

/// Completed large-write accounting returned to applications.
#[derive(Debug, Clone)]
pub struct LargeWriteResult {
    pub locations: Vec<Location>,
    pub logical_bytes: u64,
    pub physical_bytes: u64,
    pub chunks: usize,
    pub strips: u64,
    pub elapsed: Duration,
    pub preparation_stalls: u64,
    pub preparation_stall_time: Duration,
}

/// A reusable client that owns discovery and transport wiring.
#[derive(Clone)]
pub struct ChunkIoClient {
    allocator: Arc<dyn crate::ChunkAllocator>,
    disk_writer: Arc<dyn DiskWriter>,
}

impl ChunkIoClient {
    /// Discover services and build lock-free DiskIO routing.
    pub async fn connect(config: ChunkIoClientConfig) -> Result<Self> {
        let kv = Arc::new(CrowdbKvClient::new(ClientConfig::new(config.management_seeds)));
        let service = ServiceRegistryClient::from_shared(kv.clone());
        let hardware = HardwareClient::from_shared(kv);
        let chunkdb = Arc::new(ChunkdbClient::new(
            service.clone(),
            Arc::new(ChunkdbRpcTransport::new()),
        ));
        chunkdb.refresh_endpoints().await?;
        let disk_writer = Arc::new(RoutedDiskWriter::connect(&service, &hardware).await?);
        Ok(Self {
            allocator: chunkdb,
            disk_writer,
        })
    }

    /// Construct from low-level seams. Intended for focused tests and embedded fixtures.
    pub fn from_parts(allocator: Arc<dyn crate::ChunkAllocator>, disk_writer: Arc<dyn DiskWriter>) -> Self {
        Self {
            allocator,
            disk_writer,
        }
    }

    /// Start bounded chunk preparation as soon as object metadata is known.
    pub fn prepare_large_write(
        &self,
        object_size: Option<u64>,
        policy: LargeWritePolicy,
    ) -> PreparedLargeWrite {
        let mut writer = LargeAsyncObjectWriter::new(
            self.allocator.clone(),
            self.disk_writer.clone(),
            policy.ec_scheme,
            policy.client.clone(),
        );
        writer.prepare(object_size);
        PreparedLargeWrite {
            writer,
            object_size,
            policy,
            prepared_at: Instant::now(),
        }
    }
}

/// One application-owned, single-use large-write session.
pub struct PreparedLargeWrite {
    writer: LargeAsyncObjectWriter,
    object_size: Option<u64>,
    policy: LargeWritePolicy,
    prepared_at: Instant,
}

impl PreparedLargeWrite {
    /// Stream the object and return durable locations plus accounting.
    pub async fn write_stream(
        mut self,
        source: impl tokio::io::AsyncRead + Unpin + Send,
    ) -> Result<LargeWriteResult> {
        let started = Instant::now();
        let locations = self.writer.write_stream(source, self.object_size).await?;
        let logical_bytes: u64 = locations.iter().map(|location| location.length).sum();
        let block_bytes = self.policy.client.read_buffer_size as u64;
        let data_blocks = logical_bytes.div_ceil(block_bytes);
        let strips = data_blocks.div_ceil(self.policy.ec_scheme.data_num as u64);
        let physical_bytes = logical_bytes + strips * self.policy.ec_scheme.code_num as u64 * block_bytes;
        Ok(LargeWriteResult {
            chunks: locations.len(),
            locations,
            logical_bytes,
            physical_bytes,
            strips,
            elapsed: started.elapsed(),
            preparation_stalls: 0,
            preparation_stall_time: Duration::ZERO,
        })
    }

    /// Time elapsed since the request prepared this write session.
    pub fn preparation_age(&self) -> Duration {
        self.prepared_at.elapsed()
    }
}
