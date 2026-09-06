// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// DioConfig: configuration for the crowdb-diskio server.
// Parsed from CLI args; validated before startup.
#pragma once

#include "disk/disk_properties.h"
#include "disk/types.h"

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace crowdb::diskio
{

// Disk type for dummy disks (when no real block device is configured).
enum class DummyDiskType {
    Null, // memfd, drop-write + pattern read (default, for benchmarks)
    Mem,  // memfd, store + read-back (for correctness tests)
};

// A disk entry from config: path + zone layout.
struct DiskEntry
{
    DiskId            id;
    std::string       path; // block device path (empty = dummy disk)
    std::vector<Zone> zones;
};

// Server configuration.
struct DioConfig
{
    // RPC listen. Default port 13000 matches DISKIO_RPC_BASE in
    // lib/crowdb-protocol/src/ports.rs — keep in sync.
    std::string bind_address = "127.0.0.1";
    int         listen_port  = 13000;

    // Node identity (64-bit; each diskio service manages one node).
    uint64_t node_id = 0;

    // Dummy disk type (used when disk path is empty).
    DummyDiskType dummy_disk_type = DummyDiskType::Null;

    // Engine tuning (auto-detected: uring if available, blocking otherwise).
    uint32_t thread_pool_size = 4;
    uint32_t sq_entries       = 256;

    // Optional fault injection for dummy disks.
    std::optional<DiskProperties> dummy_props;

    // O_DIRECT for block devices.
    bool o_direct = true;

    // Disk list.
    std::vector<DiskEntry> disks;

    // Group-0 sync (optional). When kv_seeds is non-empty, diskio
    // connects to group-0, fetches its disk list, and heartbeats.
    std::vector<std::string> kv_seeds;
    uint64_t                 instance_id         = 0;
    uint64_t                 rack_id             = 0;
    uint64_t                 dg_id               = 0;
    uint32_t                 sync_interval_ms    = 5000; // keepalive period
    bool                     auto_discover_disks = false;

    // Metrics logging. When metrics_interval_secs > 0, system metrics
    // (CPU, RSS, TCP, DRAM BW) are flushed to a log file every interval.
    std::string metrics_log_dir = "log"; // directory for metrics log files
    uint32_t    metrics_interval_secs = 5; // 0 disables metrics logging

    // Parse CLI args. Returns true on success, false on error (msg in err).
    static bool parse_args(int argc, char *argv[], DioConfig &out, std::string &err);

    // Validate the parsed config. Returns true on success.
    bool validate(std::string &err) const;
};

// Parse a dummy disk type string ("null", "mem").
// Returns true on success.
bool parse_dummy_disk_type(const std::string &s, DummyDiskType &out);

} // namespace crowdb::diskio
