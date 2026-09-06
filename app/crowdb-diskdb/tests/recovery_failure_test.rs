// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Duration;

use crowdb_diskdb::ddb_kv_client::DdbKvClient;
use crowdb_diskdb::recovery::ZoneLoader;
use crowdb_kv_client::{ClientConfig, CrowdbKvClient, RetryConfig};
use crowdb_protocol::common::DiskId;
use crowdb_protocol::diskdb::rpc::{DiskType, DiskValue};

#[tokio::test]
async fn failed_recovery_does_not_return_writable_disk_group() {
    let mut config = ClientConfig::new(Vec::new());
    config.retry = RetryConfig {
        max_retries: 1,
        unknown_leader_wait: Duration::from_millis(10),
        backoff_base: Duration::from_millis(10),
        backoff_max: Duration::from_millis(10),
    };
    let kv = CrowdbKvClient::new(config);
    let port = crowdb_protocol::port_alloc::alloc_test_port(crowdb_protocol::ServicePort::KvServerListen);
    kv.seed_leader(0, 999, format!("127.0.0.1:{port}"));
    let loader = ZoneLoader::new(Arc::new(DdbKvClient::new(kv)), 1);
    let disk = DiskValue {
        disk_type: DiskType::BlockSsd as i32,
        capacity_units: 128,
        zone_size_units: 128,
        unit_size_bytes: 4096,
        zone_count: 1,
        status: crowdb_protocol::common::HwStatus::Up as i32,
        device_path: String::new(),
    };

    let result = loader
        .load_disk_group(100, 10, 1, (0, 999), &[(DiskId { high: 0, low: 1 }, disk)], 1)
        .await;

    assert!(
        result.is_err(),
        "failed recovery must not synthesize empty Up zones"
    );
}
