// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Block-IO layer — the `DiskWriter` seam.
//!
//! `DiskWriter` is the single test-injection point for block IO.
//! Production impl (`DiskioBlockWriter`) wraps `DiskioClient`. Test
//! impls live in `tests/common/`. Replaces the old `BlockWriter`
//! trait.

pub mod disk_writer;
pub mod routing;

pub use disk_writer::{DiskWriter, DiskioBlockWriter};
pub use routing::RoutedDiskWriter;
