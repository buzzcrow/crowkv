// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::common::cluster::start_cluster_classic;
use bytes::Bytes;
use crowdb_kv::paxos::roles::{PxBallot, PxLogEntry};
use crowdb_kv::rpc::KvSetRequest;

fn encode_put_payload(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&1u16.to_le_bytes());
    payload.push(0);
    payload.extend_from_slice(&u32::try_from(key.len()).unwrap().to_le_bytes());
    payload.extend_from_slice(key);
    payload.extend_from_slice(&u32::try_from(value.len()).unwrap().to_le_bytes());
    payload.extend_from_slice(value);
    payload
}

#[tokio::test]
async fn kv_put_retries_next_slot_when_slot_has_prior_accepted_value() {
    let cluster = start_cluster_classic(&[0, 1, 2], 0).await;

    let stale_payload = encode_put_payload(b"stale", b"value");
    let followers = cluster.followers();
    // Seed ALL followers' slot 1 so the leader cannot reach quorum
    // without discovering the prior accepted value. With only one
    // seeded follower in a 3-node cluster (quorum=2), the leader can
    // reach quorum via the unseeded follower and overwrite slot 1.
    for follower in &followers {
        let follower_group = follower.get_group(1).expect("group exists on follower");
        let follower_replica = follower_group.local_replica();
        let entry = PxLogEntry {
            slot: 1,
            ballot: PxBallot::new(10, 99),
            term: 0,
            payload: bytes::Bytes::from(stale_payload.clone()),
        };
        let reply = follower_replica.on_accept(&entry).await;
        assert!(
            matches!(reply, crowdb_kv::paxos::roles::PxAcceptReply::Accepted { .. }),
            "preload accept should succeed: {reply:?}"
        );
    }

    let leader = cluster.leader();
    let kv = cluster.kv_client(leader).await;
    let put_resp = kv
        .put(KvSetRequest {
            version: 1,
            key: Bytes::from_static(b"my-key"),
            value: Bytes::from_static(b"my-value"),
            ttl_ms: 0,
            request_id: 201,
            request_create_ms: 2001,
            client_id: 0,
            seq: 0,
            group_id: 1,
        })
        .await
        .expect("kv put")
        .into_inner();
    assert!(put_resp.ok, "put should succeed after slot retry");
    assert!(
        put_resp.revision >= 2,
        "client value should be retried on a later slot"
    );

    for node in cluster.nodes() {
        let group = node.get_group(1).expect("group exists");
        let replica = group.local_replica();
        let slot1 = replica.accepted_at(1).await.expect("slot 1 accepted");
        assert_eq!(
            *slot1.payload, stale_payload,
            "slot 1 must preserve pre-existing accepted value"
        );
    }
    // R65: follower apply is driven by ChosenNotice (async). Poll until
    // all nodes have the value, with a bounded timeout.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        let mut all_match = true;
        for node in cluster.nodes() {
            let group = node.get_group(1).expect("group exists");
            let replica = group.local_replica();
            let value = replica
                .learner
                .engine_get(b"my-key".as_slice())
                .await
                .map(|(_, v)| v);
            if value.as_deref() != Some(b"my-value".as_slice()) {
                all_match = false;
            }
        }
        if all_match {
            break;
        }
        if std::time::Instant::now() >= deadline {
            for node in cluster.nodes() {
                let group = node.get_group(1).expect("group exists");
                let replica = group.local_replica();
                let value = replica
                    .learner
                    .engine_get(b"my-key".as_slice())
                    .await
                    .map(|(_, v)| v);
                assert_eq!(value.as_deref(), Some(b"my-value".as_slice()));
            }
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

    drop(kv);
    cluster.shutdown().await;
}
