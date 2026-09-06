// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `run_fetch_stage` — free function for the async-stream fetch stage.
//!
//! Pure IO glue — reads from `AsyncRead` in ≤ `read_buffer_size`
//! chunks, accumulates to full blocks, sends `Bytes` on the block
//! channel. On EOF, sends any partial last block, then returns (drops
//! the sender → drive loop sees EOF). No state, stays a free function.

use bytes::Bytes;
use bytes::BytesMut;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;

/// Run the fetch stage: reads from `reader` in ≤ `read_buffer_size`
/// chunks, accumulates to full blocks, and sends `Bytes` to the block
/// channel. On EOF, sends any partial last block, then returns (drops
/// the sender → drive loop sees EOF).
pub async fn run_fetch_stage<R>(
    mut reader: R,
    block_tx: mpsc::Sender<Bytes>,
    read_buffer_size: usize,
) -> std::io::Result<()>
where
    R: tokio::io::AsyncRead + Unpin + Send,
{
    let mut buf = BytesMut::with_capacity(read_buffer_size);
    let mut read_buf = vec![0u8; read_buffer_size];

    loop {
        match reader.read(&mut read_buf).await {
            Ok(0) => break,
            Ok(n) => {
                buf.extend_from_slice(&read_buf[..n]);
                while buf.len() >= read_buffer_size {
                    let block = buf.split_to(read_buffer_size);
                    if block_tx.send(block.freeze()).await.is_err() {
                        return Ok(());
                    }
                }
            }
            Err(error) => return Err(error),
        }
    }
    if !buf.is_empty() {
        let _ = block_tx.send(buf.freeze()).await;
    }
    Ok(())
}
