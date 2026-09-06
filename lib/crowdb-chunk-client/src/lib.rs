// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Client library for the CROWDB chunk data path.
//!
//! Owns the chunk data IO layer: the `Location` type, the
//! `ChunkIoWriter` async interface, the large-object writers (R94,
//! refactored into OO stage classes), and the writer pool. Calls into
//! `crowdb-chunkdb-client` (management RPCs) and `crowdb-diskio-client`
//! (block IO).
//!
//! R106 (small-object writer) and R107 (read flow) will add their
//! modules here too.

#![allow(
    clippy::module_name_repetitions,
    clippy::missing_errors_doc,
    clippy::cast_possible_truncation,
    clippy::must_use_candidate,
    clippy::doc_markdown
)]

pub mod chunk;
pub mod client;
pub mod config;
pub mod disk_io;
pub mod error;
pub mod io;
pub mod traits;
pub mod worker;
pub mod writer;

pub use chunk::{ChunkPrefetch, ChunkWriter, EcStripWriter, MirrorStripWriter, StripResult, StripWriter};
pub use client::{
    ChunkIoClient, ChunkIoClientConfig, LargeWritePolicy, LargeWriteResult, PreparedLargeWrite,
};
pub use config::ChunkClientConfig;
pub use disk_io::{DiskWriter, DiskioBlockWriter, RoutedDiskWriter};
pub use error::{IoError, Result};
pub use io::{BackpressurePolicy, ChunkIoWriter, FeedStatus};
pub use traits::ChunkAllocator;
pub use worker::{EcWorker, HashWorker};
pub use writer::{LargeAsyncObjectWriter, LargeObjectWriter, PooledWriter, SmallObjectWriter, WriterPool};

// Re-export key protocol types for convenience.
pub use crowdb_protocol::chunkdb::rpc::Location as ProtoLocation;
pub use crowdb_protocol::common::ChunkId;
