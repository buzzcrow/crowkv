// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! R45 event-driven proposal coalescing integration tests.

use std::sync::Arc;

use crowdb_kv::cluster::group::{ProposeResult, PxGroup};
use crowdb_kv::cluster::{PxLocalReplica, PxLocalReplicaRole};
use crowdb_kv::common::config::CrowDBConfig;

#[allow(clippy::cast_possible_truncation)]
fn encode_put(key: &[u8], value: &[u8]) -> Vec<u8> {
    encode_puts(&[(key, value)])
}

#[allow(clippy::cast_possible_truncation)]
fn encode_puts(items: &[(&[u8], &[u8])]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(items.len() as u16).to_le_bytes());
    for (key, value) in items {
        buf.push(0u8);
        buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
        buf.extend_from_slice(key);
        buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
        buf.extend_from_slice(value);
    }
    buf
}

fn coalesce_group(max_keys: usize) -> Arc<PxGroup> {
    let local = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);
    let mut group = PxGroup::new(1, local);
    let config = CrowDBConfig {
        wal_early_ack: false,
        async_engine_apply: false,
        paxos: crowdb_kv::common::config::PaxosConfig {
            coalesce_max_keys: max_keys,
            ..crowdb_kv::common::config::PaxosConfig::DEFAULT
        },
        ..CrowDBConfig::default()
    };
    group.set_from_config(&config);
    let arc = Arc::new(group);
    arc.set_self_weak();
    arc
}

#[tokio::test]
async fn coalesce_disabled_when_max_keys_zero() {
    let group = coalesce_group(0);
    let r1 = group.propose(encode_put(b"k1", b"v1"), Some(1), Some(1)).await;
    let r2 = group.propose(encode_put(b"k2", b"v2"), Some(1), Some(2)).await;
    let slot1 = match r1 {
        ProposeResult::Chosen { slot } => slot,
        other => panic!("got {other:?}"),
    };
    let slot2 = match r2 {
        ProposeResult::Chosen { slot } => slot,
        other => panic!("got {other:?}"),
    };
    assert_ne!(slot1, slot2);
}

#[tokio::test]
async fn coalesce_first_op_starts_immediately() {
    let group = coalesce_group(32);
    let r = group.propose(encode_put(b"solo", b"v1"), Some(1), Some(1)).await;
    assert!(matches!(r, ProposeResult::Chosen { .. }), "got {r:?}");
}

#[tokio::test]
async fn coalesce_ops_during_round_share_next_slot() {
    let group = coalesce_group(32);
    let (gate_tx, gate_rx) = tokio::sync::oneshot::channel();
    group.set_coalesce_round_gate_for_tests(gate_rx);
    let g = Arc::clone(&group);
    let h1 = tokio::spawn(async move { g.propose(encode_put(b"k1", b"v1"), Some(10), Some(1)).await });
    while !group.has_coalesce_pending_for_tests() {
        tokio::task::yield_now().await;
    }
    let g = Arc::clone(&group);
    let h2 = tokio::spawn(async move { g.propose(encode_put(b"k2", b"v2"), Some(20), Some(1)).await });
    let g = Arc::clone(&group);
    let h3 = tokio::spawn(async move { g.propose(encode_put(b"k3", b"v3"), Some(30), Some(1)).await });
    while group.coalesce_pending_count_for_tests() < 2 {
        tokio::task::yield_now().await;
    }
    let _ = gate_tx.send(());
    let r1 = h1.await.unwrap();
    let r2 = h2.await.unwrap();
    let r3 = h3.await.unwrap();
    let s1 = match r1 {
        ProposeResult::Chosen { slot } => slot,
        other => panic!("got {other:?}"),
    };
    let s2 = match r2 {
        ProposeResult::Chosen { slot } => slot,
        other => panic!("got {other:?}"),
    };
    let s3 = match r3 {
        ProposeResult::Chosen { slot } => slot,
        other => panic!("got {other:?}"),
    };
    assert_ne!(s1, s2, "first op must get its own slot");
    assert_eq!(s2, s3, "ops arriving during round must share next slot");
}

#[tokio::test]
async fn coalesce_dedup_tags_recorded() {
    let group = coalesce_group(32);
    let (gate_tx, gate_rx) = tokio::sync::oneshot::channel();
    group.set_coalesce_round_gate_for_tests(gate_rx);
    let g = Arc::clone(&group);
    let h1 = tokio::spawn(async move { g.propose(encode_put(b"dk1", b"v1"), Some(100), Some(1)).await });
    while !group.has_coalesce_pending_for_tests() {
        tokio::task::yield_now().await;
    }
    let g = Arc::clone(&group);
    let h2 = tokio::spawn(async move { g.propose(encode_put(b"dk2", b"v2"), Some(200), Some(1)).await });
    while group.coalesce_pending_count_for_tests() < 1 {
        tokio::task::yield_now().await;
    }
    let _ = gate_tx.send(());
    let r1 = h1.await.unwrap();
    let r2 = h2.await.unwrap();
    let s1 = match r1 {
        ProposeResult::Chosen { slot } => slot,
        other => panic!("got {other:?}"),
    };
    let s2 = match r2 {
        ProposeResult::Chosen { slot } => slot,
        other => panic!("got {other:?}"),
    };
    let learner = &group.local_replica().learner;
    assert_eq!(learner.dedup_lookup(100, 1), Some(s1));
    assert_eq!(learner.dedup_lookup(200, 1), Some(s2));
}

#[tokio::test]
async fn coalesce_max_keys_overflow_starts_concurrent_round() {
    let group = coalesce_group(32);
    group.set_coalesce_max_keys_for_tests(2);
    let (gate_tx, gate_rx) = tokio::sync::oneshot::channel();
    group.set_coalesce_round_gate_for_tests(gate_rx);
    let g = Arc::clone(&group);
    let h1 = tokio::spawn(async move { g.propose(encode_put(b"mk1", b"v1"), Some(1), Some(1)).await });
    while !group.has_coalesce_pending_for_tests() {
        tokio::task::yield_now().await;
    }
    let g = Arc::clone(&group);
    let h2 = tokio::spawn(async move { g.propose(encode_put(b"mk2", b"v2"), Some(2), Some(1)).await });
    let g = Arc::clone(&group);
    let h3 = tokio::spawn(async move { g.propose(encode_put(b"mk3", b"v3"), Some(3), Some(1)).await });
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    let _ = gate_tx.send(());
    let r1 = h1.await.unwrap();
    let r2 = h2.await.unwrap();
    let r3 = h3.await.unwrap();
    let s1 = match r1 {
        ProposeResult::Chosen { slot } => slot,
        other => panic!("got {other:?}"),
    };
    let s2 = match r2 {
        ProposeResult::Chosen { slot } => slot,
        other => panic!("got {other:?}"),
    };
    let s3 = match r3 {
        ProposeResult::Chosen { slot } => slot,
        other => panic!("got {other:?}"),
    };
    assert_ne!(s1, s2, "first op must get its own slot");
    assert_eq!(s2, s3, "overflow batch ops must share slot");
}

#[tokio::test]
async fn coalesce_applies_all_keys_to_engine() {
    let group = coalesce_group(32);
    let (gate_tx, gate_rx) = tokio::sync::oneshot::channel();
    group.set_coalesce_round_gate_for_tests(gate_rx);
    let g = Arc::clone(&group);
    let h1 = tokio::spawn(async move { g.propose(encode_put(b"ak1", b"v1"), Some(1), Some(1)).await });
    while !group.has_coalesce_pending_for_tests() {
        tokio::task::yield_now().await;
    }
    let g = Arc::clone(&group);
    let h2 = tokio::spawn(async move { g.propose(encode_put(b"ak2", b"v2"), Some(2), Some(1)).await });
    while group.coalesce_pending_count_for_tests() < 1 {
        tokio::task::yield_now().await;
    }
    let _ = gate_tx.send(());
    let _ = h1.await.unwrap();
    let _ = h2.await.unwrap();
    let learner = &group.local_replica().learner;
    let v1 = learner.engine_get(b"ak1").await.expect("ak1 missing");
    let v2 = learner.engine_get(b"ak2").await.expect("ak2 missing");
    assert_eq!(v1.1, b"v1");
    assert_eq!(v2.1, b"v2");
}

#[tokio::test]
async fn coalesce_preserves_operation_counts_from_batch_requests() {
    let group = coalesce_group(32);
    let (gate_tx, gate_rx) = tokio::sync::oneshot::channel();
    group.set_coalesce_round_gate_for_tests(gate_rx);
    let g = Arc::clone(&group);
    let h1 = tokio::spawn(async move {
        g.propose(
            encode_puts(&[(b"batch-a", b"value-a"), (b"batch-b", b"value-b")]),
            Some(1),
            Some(1),
        )
        .await
    });
    while !group.has_coalesce_pending_for_tests() {
        tokio::task::yield_now().await;
    }
    let g = Arc::clone(&group);
    let h2 = tokio::spawn(async move {
        g.propose(
            encode_puts(&[(b"batch-c", b"value-c"), (b"batch-d", b"value-d")]),
            Some(2),
            Some(1),
        )
        .await
    });
    while group.coalesce_pending_count_for_tests() < 2 {
        tokio::task::yield_now().await;
    }
    let _ = gate_tx.send(());
    assert!(matches!(h1.await.unwrap(), ProposeResult::Chosen { .. }));
    assert!(matches!(h2.await.unwrap(), ProposeResult::Chosen { .. }));

    for (key, value) in [
        (&b"batch-a"[..], &b"value-a"[..]),
        (&b"batch-b"[..], &b"value-b"[..]),
        (&b"batch-c"[..], &b"value-c"[..]),
        (&b"batch-d"[..], &b"value-d"[..]),
    ] {
        let stored = group
            .local_replica()
            .learner
            .engine_get(key)
            .await
            .expect("key missing");
        assert_eq!(stored.1, value);
    }
}

#[tokio::test]
async fn coalesce_sequential_ops_get_increasing_slots() {
    let group = coalesce_group(32);
    let r1 = group.propose(encode_put(b"sb1", b"v1"), Some(1), Some(1)).await;
    let r2 = group.propose(encode_put(b"sb2", b"v2"), Some(2), Some(1)).await;
    let s1 = match r1 {
        ProposeResult::Chosen { slot } => slot,
        other => panic!("got {other:?}"),
    };
    let s2 = match r2 {
        ProposeResult::Chosen { slot } => slot,
        other => panic!("got {other:?}"),
    };
    assert!(s2 > s1, "second op must get a higher slot");
}
