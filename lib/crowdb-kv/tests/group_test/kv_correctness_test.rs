// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! KV operation correctness through group `propose` / crowdb-rpc KV API.
//!
//! Covers all op types and orderings: Put, overwrite, Delete,
//! delete non-existent, batch with multiple puts, intra-batch
//! last-wins, put-then-delete, delete-then-put, empty batch,
//! mixed ops across slots. All verified via `engine_get` on all
//! replicas.

use crate::common::cluster::{start_cluster, TestCluster};
use crate::common::test_client::TestKvClient;
use bytes::Bytes;
use crowdb_kv::rpc::{KvBatchItem, KvBatchWriteRequest, KvDeleteRequest, KvSetRequest};

async fn put(client: &mut TestKvClient, key: &[u8], val: &[u8], req_id: u64) {
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

async fn delete(client: &mut TestKvClient, key: &[u8], req_id: u64) {
    let resp = client
        .delete(KvDeleteRequest {
            version: 1,
            key: Bytes::copy_from_slice(key),
            request_id: req_id,
            request_create_ms: 1000 + req_id,
            client_id: 0,
            seq: 0,
            group_id: 1,
        })
        .await
        .expect("kv delete")
        .into_inner();
    assert!(resp.ok, "delete failed for key {key:?}");
}

async fn batch_write(client: &mut TestKvClient, items: Vec<(&[u8], &[u8], bool)>, req_id: u64) {
    let resp = client
        .batch_write(KvBatchWriteRequest {
            version: 1,
            items: items
                .into_iter()
                .map(|(k, v, is_del)| KvBatchItem {
                    key: Bytes::copy_from_slice(k),
                    value: Bytes::copy_from_slice(v),
                    is_delete: is_del,
                })
                .collect(),
            request_id: req_id,
            request_create_ms: 1000 + req_id,
            client_id: 0,
            seq: 0,
            group_id: 1,
        })
        .await
        .expect("kv batch_write")
        .into_inner();
    assert!(resp.ok, "batch_write failed");
}

async fn assert_cluster_value(cluster: &TestCluster, key: &[u8], expected: Option<&[u8]>) {
    // R65: follower apply is driven by ChosenNotice (async, after quorum
    // confirmation). Poll until all nodes converge, with a bounded timeout.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        let mut all_match = true;
        for node in cluster.nodes() {
            let group = node.get_group(1).expect("group exists");
            let replica = group.local_replica();
            let value = replica.learner.engine_get(key).await.map(|(_, v)| v);
            let matches = match expected {
                Some(bytes) => value.is_some_and(|v| v.as_slice() == bytes),
                None => value.is_none(),
            };
            if !matches {
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
                let value = replica.learner.engine_get(key).await.map(|(_, v)| v);
                match expected {
                    Some(bytes) => {
                        let stored = value.expect("value missing");
                        assert_eq!(
                            stored.as_slice(),
                            bytes,
                            "key {key:?} mismatch on replica {}",
                            replica.id
                        );
                    }
                    None => {
                        assert!(
                            value.is_none(),
                            "key {key:?} should be absent on replica {}",
                            replica.id
                        );
                    }
                }
            }
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

/// Put → overwrite → verify latest value wins.
#[tokio::test]
async fn put_overwrite_keeps_latest() {
    let cluster = start_cluster(&[0, 1, 2], 0).await;
    let leader = cluster.leader();
    let mut client = cluster.kv_client(leader).await;

    put(&mut client, b"k", b"v1", 200).await;
    assert_cluster_value(&cluster, b"k", Some(b"v1")).await;

    put(&mut client, b"k", b"v2", 201).await;
    assert_cluster_value(&cluster, b"k", Some(b"v2")).await;

    cluster.shutdown().await;
}

/// Delete non-existent key is a no-op (no panic, no error).
#[tokio::test]
async fn delete_nonexistent_is_noop() {
    let cluster = start_cluster(&[0, 1, 2], 0).await;
    let leader = cluster.leader();
    let mut client = cluster.kv_client(leader).await;

    delete(&mut client, b"never_existed", 210).await;
    assert_cluster_value(&cluster, b"never_existed", None).await;

    cluster.shutdown().await;
}

/// Batch with multiple puts — all keys visible.
#[tokio::test]
async fn batch_multiple_puts_all_visible() {
    let cluster = start_cluster(&[0, 1, 2], 0).await;
    let leader = cluster.leader();
    let mut client = cluster.kv_client(leader).await;

    batch_write(
        &mut client,
        vec![(b"a", b"1", false), (b"b", b"2", false), (b"c", b"3", false)],
        220,
    )
    .await;

    assert_cluster_value(&cluster, b"a", Some(b"1")).await;
    assert_cluster_value(&cluster, b"b", Some(b"2")).await;
    assert_cluster_value(&cluster, b"c", Some(b"3")).await;

    cluster.shutdown().await;
}

/// Intra-batch last occurrence wins: put k=a, delete k, put k=final in one batch.
#[tokio::test]
async fn intra_batch_last_occurrence_wins() {
    let cluster = start_cluster(&[0, 1, 2], 0).await;
    let leader = cluster.leader();
    let mut client = cluster.kv_client(leader).await;

    batch_write(
        &mut client,
        vec![(b"k", b"a", false), (b"k", b"", true), (b"k", b"final", false)],
        230,
    )
    .await;

    assert_cluster_value(&cluster, b"k", Some(b"final")).await;

    cluster.shutdown().await;
}

/// Put then delete — key is absent.
#[tokio::test]
async fn put_then_delete_key_absent() {
    let cluster = start_cluster(&[0, 1, 2], 0).await;
    let leader = cluster.leader();
    let mut client = cluster.kv_client(leader).await;

    put(&mut client, b"k", b"v", 240).await;
    assert_cluster_value(&cluster, b"k", Some(b"v")).await;

    delete(&mut client, b"k", 241).await;
    assert_cluster_value(&cluster, b"k", None).await;

    cluster.shutdown().await;
}

/// Delete then put — key has the new value.
#[tokio::test]
async fn delete_then_put_key_has_new_value() {
    let cluster = start_cluster(&[0, 1, 2], 0).await;
    let leader = cluster.leader();
    let mut client = cluster.kv_client(leader).await;

    put(&mut client, b"k", b"initial", 250).await;
    delete(&mut client, b"k", 251).await;
    put(&mut client, b"k", b"reborn", 252).await;

    assert_cluster_value(&cluster, b"k", Some(b"reborn")).await;

    cluster.shutdown().await;
}

/// Empty batch is a no-op.
#[tokio::test]
async fn empty_batch_is_noop() {
    let cluster = start_cluster(&[0, 1, 2], 0).await;
    let leader = cluster.leader();
    let mut client = cluster.kv_client(leader).await;

    put(&mut client, b"k", b"v", 260).await;
    batch_write(&mut client, vec![], 261).await;
    assert_cluster_value(&cluster, b"k", Some(b"v")).await;

    cluster.shutdown().await;
}

/// Mixed ops across slots: put k1, put k2, delete k1, put k3, overwrite k2.
#[tokio::test]
async fn mixed_ops_across_slots() {
    let cluster = start_cluster(&[0, 1, 2], 0).await;
    let leader = cluster.leader();
    let mut client = cluster.kv_client(leader).await;

    put(&mut client, b"k1", b"v1", 270).await;
    put(&mut client, b"k2", b"v2", 271).await;
    delete(&mut client, b"k1", 272).await;
    put(&mut client, b"k3", b"v3", 273).await;
    put(&mut client, b"k2", b"v2b", 274).await;

    assert_cluster_value(&cluster, b"k1", None).await;
    assert_cluster_value(&cluster, b"k2", Some(b"v2b")).await;
    assert_cluster_value(&cluster, b"k3", Some(b"v3")).await;

    cluster.shutdown().await;
}
