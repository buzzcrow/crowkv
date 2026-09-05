// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![allow(clippy::too_many_lines, clippy::cast_possible_truncation)]

use std::sync::Weak;

use crate::cluster::group::{PendingBatch, ProposeResult, PxGroup};
use crate::paxos::roles::DedupTag;
use tracing::{info_span, Instrument};

/// Coalescer watchdog interval in microseconds. The watchdog sleeps for
/// this duration, then flushes any stuck non-empty batch. Full batches
/// still flush immediately; this bounds the tail when low-rate control
/// traffic keeps arriving after the main workload stops.
const WATCHDOG_US: u64 = 1_000_000;

impl PxGroup {
    /// Current time in microseconds since `UNIX_EPOCH`. Coarse — used only
    /// for watchdog inactivity detection.
    fn now_micros() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_micros() as u64)
    }

    /// Record coalescer activity (enqueue or round completion). The
    /// watchdog uses this to detect stuck batches.
    fn coalesce_touch_activity(&self) {
        self.coalesce_last_activity_us
            .store(Self::now_micros(), std::sync::atomic::Ordering::Relaxed);
    }

    /// Start the single long-running watchdog task if not already running.
    /// Called lazily on first enqueue. The watchdog sleeps for
    /// `WATCHDOG_US`, then flushes any non-empty pending batch.
    fn coalesce_start_watchdog(&self) {
        self.coalesce_watchdog_handle.get_or_init(|| {
            let Some(group) = self.self_weak.get().and_then(Weak::upgrade) else {
                return tokio::spawn(async {});
            };
            let g = group.group_id;
            let replica = group.local_replica().id;
            let span = group.log_store_id().map_or_else(
                || info_span!("coalesce_watchdog", g, replica),
                |s| info_span!("coalesce_watchdog", s, g, replica),
            );
            tokio::spawn(
                async move {
                    loop {
                        tokio::time::sleep(std::time::Duration::from_micros(WATCHDOG_US)).await;
                        // Atomically swap the batch for a fresh empty one so
                        // no op can sneak in and start a 1-op round during
                        // the swap.
                        let batch = {
                            let mut guard = group.coalescer.lock();
                            let taken = guard.take();
                            if taken.is_some() {
                                *guard = Some(PendingBatch::default());
                            }
                            taken
                        };
                        let Some(batch) = batch else { continue };
                        if batch.op_count > 0 {
                            tracing::error!(
                                g = group.group_id,
                                op_count = batch.op_count,
                                "coalescer: watchdog flushing stuck batch — ops delayed >1s"
                            );
                            group.coalesce_touch_activity();
                            group.coalesce_spawn_round(batch);
                        }
                        // Empty batch — just drop it (go idle).
                    }
                }
                .instrument(span),
            )
        });
    }

    /// Spawn a Paxos round for the given batch. The caller must have
    /// already swapped in a fresh `PendingBatch` to the coalescer (so
    /// new ops accumulate for the next round). This function only
    /// builds the payload and spawns the round task.
    fn coalesce_spawn_round(&self, batch: PendingBatch) {
        let mut payload = Vec::with_capacity(2 + batch.op_bodies.len());
        payload.extend_from_slice(&batch.op_count.to_le_bytes());
        payload.extend_from_slice(&batch.op_bodies);
        let payload = bytes::Bytes::from(payload);
        let tags = batch.tags;
        let waiters = batch.waiters;
        let Some(group) = self.self_weak.get().and_then(Weak::upgrade) else {
            return;
        };
        tokio::spawn(
            async move {
                #[cfg(feature = "test-util")]
                group.coalesce_await_round_gate().await;
                let result = group.propose_inner(payload, &tags).await;
                for waiter in waiters {
                    let _ = waiter.send(result.clone());
                }
                group.coalesce_drain_after_round();
            }
            .instrument(tracing::Span::current()),
        );
    }

    /// Enqueue one single-key op into the coalescer.
    ///
    /// - Idle (no batch): start a 1-op round immediately. Open a pending
    ///   batch for concurrent ops.
    /// - Batch exists: append to the pending batch. If the batch fills to
    ///   `max_keys`, flush it as a concurrent round.
    ///
    /// A single activity-based watchdog task runs in the background — it
    /// only fires if there's no coalescer activity for `WATCHDOG_US`.
    #[allow(clippy::type_complexity)]
    pub(crate) async fn coalesce_enqueue(&self, payload: Vec<u8>, tag: Option<DedupTag>) -> ProposeResult {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let request_op_count = payload
            .get(..2)
            .map_or(0, |count| u16::from_le_bytes([count[0], count[1]]));
        let op_body: &[u8] = payload.get(2..).unwrap_or(&[]);
        let max_keys = self.coalesce_max_keys.load(std::sync::atomic::Ordering::Relaxed);
        self.coalesce_touch_activity();
        self.coalesce_start_watchdog();

        // The locked section returns:
        //   Some((payload, tags, waiters)) → start a round now
        //   None → joined a batch, just await the oneshot
        let start_round: Option<(
            Vec<u8>,
            Vec<DedupTag>,
            Vec<tokio::sync::oneshot::Sender<ProposeResult>>,
        )> = {
            let mut guard = self.coalescer.lock();
            match &mut *guard {
                None => {
                    // Idle: start a 1-op round immediately.
                    let batch = PendingBatch::default();
                    *guard = Some(batch);
                    let mut round_payload = Vec::with_capacity(2 + op_body.len());
                    round_payload.extend_from_slice(&request_op_count.to_le_bytes());
                    round_payload.extend_from_slice(op_body);
                    let round_tags: Vec<DedupTag> = tag.into_iter().collect();
                    Some((round_payload, round_tags, vec![tx]))
                }
                Some(batch) => {
                    batch.op_bodies.extend_from_slice(op_body);
                    batch.op_count = batch.op_count.saturating_add(request_op_count);
                    if let Some(t) = tag {
                        batch.tags.push(t);
                    }
                    batch.waiters.push(tx);
                    if batch.op_count >= max_keys {
                        // Batch full: flush as a concurrent round.
                        let taken = std::mem::take(batch);
                        *guard = Some(PendingBatch::default());
                        let mut p = Vec::with_capacity(2 + taken.op_bodies.len());
                        p.extend_from_slice(&taken.op_count.to_le_bytes());
                        p.extend_from_slice(&taken.op_bodies);
                        Some((p, taken.tags, taken.waiters))
                    } else {
                        None
                    }
                }
            }
        };

        // If None, we joined a batch — just await the result.
        let Some((payload, round_tags, round_waiters)) = start_round else {
            return match rx.await {
                Ok(result) => result,
                Err(_) => ProposeResult::Err("coalescer round dropped".to_string()),
            };
        };

        // Start the round. Spawn so the caller is not pinned to the paxos
        // round and can return via the oneshot.
        let payload = bytes::Bytes::from(payload);
        let Some(group) = self.self_weak.get().and_then(Weak::upgrade) else {
            return ProposeResult::Err("group dropped".to_string());
        };
        tokio::spawn(
            async move {
                #[cfg(feature = "test-util")]
                group.coalesce_await_round_gate().await;
                let result = group.propose_inner(payload, &round_tags).await;
                for waiter in round_waiters {
                    let _ = waiter.send(result.clone());
                }
                group.coalesce_drain_after_round();
            }
            .instrument(tracing::Span::current()),
        );

        match rx.await {
            Ok(result) => result,
            Err(_) => ProposeResult::Err("coalescer round dropped".to_string()),
        }
    }

    /// Called after a coalesced round completes. Drains the pending
    /// batch (ops that accumulated during the round). If non-empty,
    /// flushes it as the next round immediately. If empty, goes idle
    /// (coalescer → `None`) so the next op starts a 1-op round — the
    /// zero-latency-floor behavior at low load.
    ///
    /// R45b drain threshold: if the in-flight slot-task count
    /// (`occupied`) is at or above `coalesce_drain_threshold`, skip the
    /// drain — the `max_keys` overflow path handles high load with full
    /// batches, and draining here would fragment the batch (many
    /// slot-tasks racing to take one shared batch). The permit is
    /// already released before this call, so the last finisher always
    /// sees a count below threshold and takes the batch.
    ///
    /// The swap is atomic: the old batch is taken and a fresh empty
    /// batch is put back in a single locked section, so no concurrent
    /// `coalesce_enqueue` can see `None` and start a 1-op round that
    /// would fragment the batch.
    fn coalesce_drain_after_round(&self) {
        self.coalesce_touch_activity();
        let threshold = self.config.paxos.coalesce_drain_threshold;
        if threshold > 0 && self.inflight.occupied() >= u64::try_from(threshold).unwrap_or(u64::MAX) {
            return;
        }
        let batch = {
            let mut guard = self.coalescer.lock();
            let taken = guard.take();
            if taken.is_some() {
                *guard = Some(PendingBatch::default());
            }
            taken
        };
        let Some(batch) = batch else { return };
        if batch.op_count == 0 {
            // No ops arrived during the round — go idle. Reset the
            // coalescer to None so the next op takes the idle path
            // (starts a 1-op round) instead of joining an empty batch
            // and waiting for the watchdog.
            let mut guard = self.coalescer.lock();
            *guard = None;
            return;
        }
        // Flush the accumulated batch as the next round immediately.
        self.coalesce_spawn_round(batch);
    }

    /// Test-only: await the coalesce round gate if set. Consumed by the
    /// first round that runs after the gate is installed.
    #[cfg(feature = "test-util")]
    pub(crate) async fn coalesce_await_round_gate(&self) {
        let gate = self.coalesce_round_gate.lock().take();
        if let Some(gate) = gate {
            let _ = gate.await;
        }
    }
}
