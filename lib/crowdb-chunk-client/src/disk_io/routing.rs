// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Lock-free disk-ID routing for production DiskIO writes.

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use bytes::Bytes;
use crowdb_diskio_client::{DiskId, DiskIoRetCode, DiskioClient};
use crowdb_kv_client::{HardwareClient, ServiceRegistryClient};
use crowdb_protocol::diskdb::rpc::Segment;
use crowdb_rpc_ffi::{Connection, RpcServer};

use crate::{DiskWriter, IoError, Result};

#[derive(Clone)]
struct Route {
    endpoint: Arc<str>,
    connection: Connection,
}

/// Disk writer backed by an atomically published disk-owner snapshot.
pub struct RoutedDiskWriter {
    client: Arc<DiskioClient>,
    server: Arc<RpcServer>,
    routes: ArcSwap<HashMap<DiskId, Route>>,
}

impl RoutedDiskWriter {
    /// Discover live DiskIO owners and connect to their endpoints.
    pub async fn connect(service: &ServiceRegistryClient, hardware: &HardwareClient) -> Result<Self> {
        let server = Arc::new(RpcServer::new(None));
        server
            .listen("127.0.0.1", 0)
            .map_err(|error| IoError::Topology(format!("start RPC client: {error}")))?;
        server.start();
        let client = Arc::new(DiskioClient::new());
        let routes = Self::discover(service, hardware, &server, &client).await?;
        Ok(Self {
            client,
            server,
            routes: ArcSwap::from_pointee(routes),
        })
    }

    /// Refresh topology off the write path, then atomically publish it.
    pub async fn refresh(&self, service: &ServiceRegistryClient, hardware: &HardwareClient) -> Result<()> {
        let routes = Self::discover(service, hardware, &self.server, &self.client).await?;
        self.routes.store(Arc::new(routes));
        Ok(())
    }

    async fn discover(
        service: &ServiceRegistryClient,
        hardware: &HardwareClient,
        server: &RpcServer,
        client: &DiskioClient,
    ) -> Result<HashMap<DiskId, Route>> {
        let instances = service
            .read_all_diskio_instances()
            .await
            .map_err(|error| IoError::Topology(format!("read DiskIO instances: {error}")))?;
        let mut dg_owners = HashMap::<u64, String>::new();
        for (_, instance) in instances {
            let extra = instance.extra.and_then(|extra| extra.diskdb).ok_or_else(|| {
                IoError::Topology(format!(
                    "DiskIO {} has no ownership metadata",
                    instance.rpc_endpoint
                ))
            })?;
            for dg_id in extra.owned_dg_ids {
                if let Some(previous) = dg_owners.insert(dg_id, instance.rpc_endpoint.clone()) {
                    return Err(IoError::Topology(format!(
                        "disk group {dg_id} has duplicate owners {previous} and {}",
                        instance.rpc_endpoint
                    )));
                }
            }
        }

        let disks = hardware
            .list_all_disks()
            .await
            .map_err(|error| IoError::Topology(format!("read disks: {error}")))?;
        let mut connections = HashMap::<String, Connection>::new();
        let mut routes = HashMap::with_capacity(disks.len());
        for disk in disks {
            let endpoint = dg_owners.get(&disk.disk_group_id).ok_or_else(|| {
                IoError::Topology(format!(
                    "disk group {} has no live DiskIO owner",
                    disk.disk_group_id
                ))
            })?;
            if !connections.contains_key(endpoint) {
                let (host, port) = parse_endpoint(endpoint)?;
                let connection = server
                    .connect(host, port)
                    .map_err(|error| IoError::Topology(format!("connect DiskIO {endpoint}: {error}")))?;
                client.attach(&connection);
                connections.insert(endpoint.clone(), connection);
            }
            let id = DiskId::new(disk.disk_id.high, disk.disk_id.low);
            routes.insert(
                id,
                Route {
                    endpoint: Arc::from(endpoint.as_str()),
                    connection: connections[endpoint].clone(),
                },
            );
        }
        Ok(routes)
    }

    fn route(&self, disk_id: DiskId) -> Result<Route> {
        self.routes
            .load()
            .get(&disk_id)
            .cloned()
            .ok_or_else(|| IoError::Topology(format!("disk {}:{} has no route", disk_id.high, disk_id.low)))
    }
}

fn parse_endpoint(endpoint: &str) -> Result<(&str, i32)> {
    let endpoint = endpoint.strip_prefix("http://").unwrap_or(endpoint);
    let (host, port) = endpoint
        .rsplit_once(':')
        .ok_or_else(|| IoError::Topology(format!("invalid DiskIO endpoint {endpoint}")))?;
    let port = port
        .parse::<i32>()
        .map_err(|error| IoError::Topology(format!("invalid DiskIO endpoint {endpoint}: {error}")))?;
    Ok((host, port))
}

#[async_trait]
impl DiskWriter for RoutedDiskWriter {
    async fn write(&self, seg: &Segment, unit_bytes: u64, data: Bytes) -> Result<()> {
        let id = seg
            .disk_id
            .map(|id| DiskId::new(id.high, id.low))
            .ok_or_else(|| IoError::WriteFailed("segment missing disk_id".into()))?;
        let route = self.route(id)?;
        let future = self
            .client
            .write_bytes(
                &self.server,
                &route.connection,
                id,
                seg.zone_index,
                seg.unit_offset * unit_bytes,
                data,
            )
            .map_err(|error| IoError::WriteFailed(format!("{}: {error}", route.endpoint)))?;
        let code = DiskioClient::await_write_response(future)
            .await
            .map_err(|error| IoError::WriteFailed(format!("{}: {error}", route.endpoint)))?;
        if code != DiskIoRetCode::Success {
            return Err(IoError::WriteFailed(format!(
                "{} returned {code:?}",
                route.endpoint
            )));
        }
        Ok(())
    }

    async fn fsync(&self, disk_id: DiskId) -> Result<()> {
        let route = self.route(disk_id)?;
        let future = self
            .client
            .fsync(&self.server, &route.connection, disk_id)
            .map_err(|error| IoError::WriteFailed(format!("{}: {error}", route.endpoint)))?;
        let code = DiskioClient::await_fsync_response(future)
            .await
            .map_err(|error| IoError::WriteFailed(format!("{}: {error}", route.endpoint)))?;
        if code != DiskIoRetCode::Success {
            return Err(IoError::WriteFailed(format!(
                "{} returned {code:?}",
                route.endpoint
            )));
        }
        Ok(())
    }
}
