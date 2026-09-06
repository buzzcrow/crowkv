// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! KV edge-case keys through group crowdb-rpc KV API.
//!
//! Covers: empty key, large key (1KB), special-bytes key (null,
//! high-UTF8, whitespace), large value (100KB), small value (1 byte),
//! empty value. All verified via `engine_get` on all replicas.

use crate::common::cluster::{start_cluster, TestCluster};
use crate::common::test_client::TestKvClient;
use bytes::Bytes;
use crowdb_kv::rpc::KvSetRequest;

async fn put_raw(client: &mut TestKvClient, key: &[u8], val: &[u8], req_id: u64) {
    let resp = client
        .put(KvSetRequest {
            version: 1,
            key: Bytes::copy_from_slice(key),
            value: Bytes::copy_from_slice(val),
            ttl_ms: 0,
            request_id: req_id,
            request_create_ms: 1000 + req_id,
            client_id: 0,
            seq: 0,
            group_id: 1,
        })
        .await
        .expect("kv put")
        .into_inner();
    assert!(resp.ok, "put failed for key {key:?}");
}

async fn assert_cluster_value(cluster: &TestCluster, key: &[u8], expected: &[u8]) {
    // R65: follower apply is driven by ChosenNotice (async, after quorum
    // confirmation). Poll until all nodes converge, with a bounded timeout.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        let mut all_match = true;
        for node in cluster.nodes() {
            let group = node.get_group(1).expect("group exists");
            let replica = group.local_replica();
            if let Some((_, v)) = replica.learner.engine_get(key).await {
                if v.as_slice() != expected {
                    all_match = false;
                }
            } else {
                all_match = false;
            }
        }
        if all_match {
            return;
        }
        if std::time::Instant::now() >= deadline {
            for node in cluster.nodes() {
                let group = node.get_group(1).expect("group exists");
                let replica = group.local_replica();
                let value = replica.learner.engine_get(key).await.expect("value missing");
                assert_eq!(
                    value.1.as_slice(),
                    expected,
                    "key {key:?} mismatch on replica {}",
                    replica.id
                );
            }
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

#[tokio::test]
async fn empty_value_roundtrips() {
    let cluster = start_cluster(&[0, 1, 2], 0).await;
    let leader = cluster.leader();
    let mut client = cluster.kv_client(leader).await;

    put_raw(&mut client, b"k_empty_val", b"", 300).await;
    assert_cluster_value(&cluster, b"k_empty_val", b"").await;

    cluster.shutdown().await;
}

#[tokio::test]
async fn single_byte_value_roundtrips() {
    let cluster = start_cluster(&[0, 1, 2], 0).await;
    let leader = cluster.leader();
    let mut client = cluster.kv_client(leader).await;

    put_raw(&mut client, b"k_small", b"x", 310).await;
    assert_cluster_value(&cluster, b"k_small", b"x").await;

    cluster.shutdown().await;
}

#[tokio::test]
async fn large_key_1kb_roundtrips() {
    let cluster = start_cluster(&[0, 1, 2], 0).await;
    let leader = cluster.leader();
    let mut client = cluster.kv_client(leader).await;

    let key = vec![0x42u8; 1024];
    put_raw(&mut client, &key, b"big_key_val", 320).await;
    assert_cluster_value(&cluster, &key, b"big_key_val").await;

    cluster.shutdown().await;
}

#[tokio::test]
async fn large_value_100kb_roundtrips() {
    let cluster = start_cluster(&[0, 1, 2], 0).await;
    let leader = cluster.leader();
    let mut client = cluster.kv_client(leader).await;

    let val = vec![0xABu8; 102_400];
    put_raw(&mut client, b"k_big_val", &val, 330).await;
    assert_cluster_value(&cluster, b"k_big_val", &val).await;

    cluster.shutdown().await;
}

#[tokio::test]
async fn special_bytes_key_roundtrips() {
    let cluster = start_cluster(&[0, 1, 2], 0).await;
    let leader = cluster.leader();
    let mut client = cluster.kv_client(leader).await;

    let key = b"\0\xFF\xC0 \t\nkey";
    put_raw(&mut client, key, b"special", 340).await;
    assert_cluster_value(&cluster, key, b"special").await;

    cluster.shutdown().await;
}

#[tokio::test]
async fn whitespace_key_roundtrips() {
    let cluster = start_cluster(&[0, 1, 2], 0).await;
    let leader = cluster.leader();
    let mut client = cluster.kv_client(leader).await;

    put_raw(&mut client, b"   \t\n  ", b"ws_val", 350).await;
    assert_cluster_value(&cluster, b"   \t\n  ", b"ws_val").await;

    cluster.shutdown().await;
}
