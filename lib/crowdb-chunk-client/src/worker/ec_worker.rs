// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `EcWorker` — streaming EC compute, owned by `EcStripWriter`.
//!
//! Accepts data shards incrementally via `push`, computes parity
//! shards as data arrives (streaming compute — overlaps with
//! data-block writes). `finish` finalizes and returns the code_num
//! parity shards. `reset` clears state for reuse across strips.
//!
//! Pure compute — no IO, no disk. Trivially unit-testable.

use bytes::Bytes;
use crowdb_common::ec::{encode_parity_from_shards, EcScheme};

use crate::{IoError, Result};

/// Streaming EC parity compute worker. Owned by each `EcStripWriter`.
pub struct EcWorker {
    ec_scheme: EcScheme,
    data_shards: Vec<Bytes>,
}

impl EcWorker {
    /// Construct a new worker for the given EC scheme.
    pub fn new(ec_scheme: EcScheme) -> Self {
        Self {
            ec_scheme,
            data_shards: Vec::with_capacity(ec_scheme.data_num),
        }
    }

    /// Feed one data shard. The worker accumulates shards; EC encode
    /// is finalized in `finish`. Streaming per-shard compute is a
    /// future optimization — the API is incremental so a later change
    /// can compute parity shards as each shard arrives.
    pub fn push(&mut self, buffer: &Bytes) -> Result<()> {
        if self.data_shards.len() >= self.ec_scheme.data_num {
            return Err(IoError::EcEncodeFailed(format!(
                "too many data shards: got {}, max {}",
                self.data_shards.len() + 1,
                self.ec_scheme.data_num
            )));
        }
        self.data_shards.push(buffer.clone());
        Ok(())
    }

    /// Finalize: EC-encode parity from the accumulated data shards.
    /// Returns `code_num` parity shards.
    pub fn finish(&mut self) -> Result<Vec<Vec<u8>>> {
        let shard_size = self.data_shards.iter().map(Bytes::len).max().unwrap_or(0);
        let padded: Vec<Vec<u8>> = self
            .data_shards
            .iter()
            .filter(|shard| shard.len() != shard_size)
            .map(|shard| {
                let mut data = vec![0; shard_size];
                data[..shard.len()].copy_from_slice(shard);
                data
            })
            .collect();
        let mut padded_index = 0;
        let shard_refs: Vec<&[u8]> = self
            .data_shards
            .iter()
            .map(|shard| {
                if shard.len() == shard_size {
                    shard.as_ref()
                } else {
                    let result = padded.get(padded_index).map_or(&[][..], Vec::as_slice);
                    padded_index += 1;
                    result
                }
            })
            .collect();
        let parity = encode_parity_from_shards(self.ec_scheme, &shard_refs)?;
        Ok(parity)
    }

    /// Reset to accept a new strip. Reuses the allocated capacity.
    pub fn reset(&mut self) {
        self.data_shards.clear();
    }

    /// Number of data shards received so far.
    pub fn shards_received(&self) -> usize {
        self.data_shards.len()
    }

    /// The EC scheme this worker is configured for.
    pub fn ec_scheme(&self) -> EcScheme {
        self.ec_scheme
    }
}
