// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "dio_config.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace crowdb::diskio
{

bool parse_dummy_disk_type(const std::string &s, DummyDiskType &out)
{
    if (s == "null") {
        out = DummyDiskType::Null;
    }
    else if (s == "mem") {
        out = DummyDiskType::Mem;
    }
    else {
        return false;
    }
    return true;
}

// Parse a hex DiskId from "high:low" or a single hex value (low only).
static bool parse_disk_id(const char *s, DiskId &out)
{
    out.high          = 0;
    out.low           = 0;
    const char *colon = std::strchr(s, ':');
    if (colon != nullptr) {
        char *end = nullptr;
        out.high  = std::strtoull(s, &end, 16);
        if (end != colon) {
            return false;
        }
        out.low = std::strtoull(colon + 1, &end, 16);
        if (*end != '\0') {
            return false;
        }
    }
    else {
        char *end = nullptr;
        out.low   = std::strtoull(s, &end, 16);
        if (*end != '\0') {
            return false;
        }
    }
    return true;
}

// Parse a u32 from argv.
static bool parse_u32(const char *s, uint32_t &out)
{
    char *end = nullptr;
    long  v   = std::strtol(s, &end, 10);
    if (end == s || *end != '\0' || v < 0 || v > 0xFFFFFFFF) {
        return false;
    }
    out = static_cast<uint32_t>(v);
    return true;
}

// Parse a u64 from argv.
static bool parse_u64(const char *s, uint64_t &out)
{
    char              *end = nullptr;
    unsigned long long v   = std::strtoull(s, &end, 10);
    if (end == s || *end != '\0') {
        return false;
    }
    out = static_cast<uint64_t>(v);
    return true;
}

// Parse a double from argv.
static bool parse_double(const char *s, double &out)
{
    char  *end = nullptr;
    double v   = std::strtod(s, &end);
    if (end == s || *end != '\0') {
        return false;
    }
    out = v;
    return true;
}

bool DioConfig::parse_args(int argc, char *argv[], DioConfig &out, std::string &err)
{
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--bind" && i + 1 < argc) {
            out.bind_address = argv[++i];
        }
        else if (arg == "--port" && i + 1 < argc) {
            uint32_t p;
            if (!parse_u32(argv[++i], p)) {
                err = "invalid --port value";
                return false;
            }
            out.listen_port = static_cast<int>(p);
        }
        else if (arg == "--node-id" && i + 1 < argc) {
            if (!parse_u64(argv[++i], out.node_id)) {
                err = "invalid --node-id value";
                return false;
            }
        }
        else if (arg == "--dummy-disk" && i + 1 < argc) {
            if (!parse_dummy_disk_type(argv[++i], out.dummy_disk_type)) {
                err = "invalid --dummy-disk value (null|mem)";
                return false;
            }
        }
        else if (arg == "--threads" && i + 1 < argc) {
            if (!parse_u32(argv[++i], out.thread_pool_size)) {
                err = "invalid --threads value";
                return false;
            }
        }
        else if (arg == "--sq-entries" && i + 1 < argc) {
            if (!parse_u32(argv[++i], out.sq_entries)) {
                err = "invalid --sq-entries value";
                return false;
            }
        }
        else if (arg == "--fault-latency" && i + 1 < argc) {
            // Format: --fault-latency <min_ms>:<max_ms>
            std::string spec  = argv[++i];
            auto        colon = spec.find(':');
            if (colon == std::string::npos) {
                err = "--fault-latency expects <min_ms>:<max_ms>";
                return false;
            }
            uint32_t lo, hi;
            if (!parse_u32(spec.substr(0, colon).c_str(), lo) || !parse_u32(spec.substr(colon + 1).c_str(), hi)) {
                err = "invalid --fault-latency value";
                return false;
            }
            if (!out.dummy_props.has_value()) {
                out.dummy_props = DiskProperties{};
            }
            out.dummy_props->latency_min_ms = lo;
            out.dummy_props->latency_max_ms = hi;
        }
        else if (arg == "--fault-error-rate" && i + 1 < argc) {
            double rate;
            if (!parse_double(argv[++i], rate) || rate < 0.0 || rate > 1.0) {
                err = "invalid --fault-error-rate value (0.0..1.0)";
                return false;
            }
            if (!out.dummy_props.has_value()) {
                out.dummy_props = DiskProperties{};
            }
            out.dummy_props->error_rate = rate;
        }
        else if (arg == "--no-o-direct") {
            out.o_direct = false;
        }
        else if (arg == "--disk" && i + 1 < argc) {
            // Format: --disk <hex_id>:<path>[:<zone_capacity>]
            // hex_id is either "high:low" or just "low" (hex).
            // Multiple --disk args allowed. Empty path = dummy disk.
            std::string spec        = argv[++i];
            size_t      first_colon = spec.find(':');
            if (first_colon == std::string::npos) {
                err = "--disk expects <hex_id>:<path>[:<capacity>]";
                return false;
            }

            // Determine the ID portion: if the first two colon-separated
            // parts are both valid hex, treat as "high:low"; otherwise
            // treat just the first part as "low".
            std::string id_str;
            std::string rest;
            size_t      second_colon = spec.find(':', first_colon + 1);
            if (second_colon != std::string::npos) {
                // Check if the part between first and second colon is
                // valid hex (potential "low" part of "high:low").
                std::string mid = spec.substr(first_colon + 1, second_colon - first_colon - 1);
                char       *end = nullptr;
                std::strtoull(mid.c_str(), &end, 16);
                if (end == mid.c_str() + mid.size() && !mid.empty()) {
                    // "high:low:path[:capacity]"
                    id_str = spec.substr(0, second_colon);
                    rest   = spec.substr(second_colon + 1);
                }
                else {
                    // "low:path[:capacity]"
                    id_str = spec.substr(0, first_colon);
                    rest   = spec.substr(first_colon + 1);
                }
            }
            else {
                // No second colon — "low:path" (no capacity).
                id_str = spec.substr(0, first_colon);
                rest   = spec.substr(first_colon + 1);
            }

            DiskEntry entry;
            if (!parse_disk_id(id_str.c_str(), entry.id)) {
                err = "invalid disk id in --disk";
                return false;
            }

            // rest = path[:capacity]
            size_t path_colon = rest.find(':');
            if (path_colon == std::string::npos) {
                entry.path = rest;
                Zone z;
                z.zone_index  = 0;
                z.base_offset = 0;
                z.capacity    = 1LL << 40; // default 1 TiB
                entry.zones.push_back(z);
            }
            else {
                entry.path          = rest.substr(0, path_colon);
                std::string cap_str = rest.substr(path_colon + 1);
                char       *end     = nullptr;
                int64_t     cap     = std::strtoll(cap_str.c_str(), &end, 10);
                if (*end != '\0' || cap <= 0) {
                    err = "invalid zone capacity in --disk";
                    return false;
                }
                Zone z;
                z.zone_index  = 0;
                z.base_offset = 0;
                z.capacity    = cap;
                entry.zones.push_back(z);
            }
            out.disks.push_back(std::move(entry));
        }
        else if (arg == "--kv-seeds" && i + 1 < argc) {
            // Comma-separated list of kv-server management endpoints.
            // e.g. --kv-seeds http://127.0.0.1:10000,http://127.0.0.1:10001
            std::string seeds = argv[++i];
            size_t      pos   = 0;
            while (pos < seeds.size()) {
                size_t comma = seeds.find(',', pos);
                if (comma == std::string::npos) {
                    out.kv_seeds.push_back(seeds.substr(pos));
                    break;
                }
                out.kv_seeds.push_back(seeds.substr(pos, comma - pos));
                pos = comma + 1;
            }
        }
        else if (arg == "--instance-id" && i + 1 < argc) {
            out.instance_id = std::strtoull(argv[++i], nullptr, 10);
        }
        else if (arg == "--rack-id" && i + 1 < argc) {
            out.rack_id = std::strtoull(argv[++i], nullptr, 10);
        }
        else if (arg == "--dg-id" && i + 1 < argc) {
            out.dg_id = std::strtoull(argv[++i], nullptr, 10);
        }
        else if (arg == "--sync-interval-ms" && i + 1 < argc) {
            out.sync_interval_ms = static_cast<uint32_t>(std::strtoul(argv[++i], nullptr, 10));
        }
        else if (arg == "--auto-discover-disks") {
            out.auto_discover_disks = true;
        }
        else if (arg == "--metrics-interval" && i + 1 < argc) {
            uint32_t v;
            if (!parse_u32(argv[++i], v)) {
                err = "invalid --metrics-interval value";
                return false;
            }
            out.metrics_interval_secs = v;
        }
        else if (arg == "--log-dir" && i + 1 < argc) {
            out.metrics_log_dir = argv[++i];
        }
        else if (arg == "--help" || arg == "-h") {
            std::printf("usage: crowdb-diskio --port <port> [--bind <addr>] "
                        "[--dummy-disk null|mem] "
                        "[--threads N] [--sq-entries N] [--no-o-direct] "
                        "[--fault-latency <min_ms>:<max_ms>] "
                        "[--fault-error-rate <0.0..1.0>] "
                        "[--disk <hex_id>:<path>[:<capacity>]]... "
                        "  (hex_id = high:low or just low, both hex) "
                        "[--kv-seeds <url1>,<url2>...] "
                        "[--instance-id N] [--rack-id N] [--dg-id N] "
                        "[--sync-interval-ms N] [--auto-discover-disks] "
                        "[--metrics-interval N] [--log-dir <dir>]\n"
                        "  Engine is auto-detected: uring if available, blocking otherwise.\n"
                        "  Empty path in --disk creates a dummy disk (null or mem).\n"
                        "  With --kv-seeds, diskio syncs with group-0 and heartbeats.\n"
                        "  --metrics-interval 0 disables metrics logging (default: 5s).\n");
            std::exit(0);
        }
        else {
            err = "unknown argument: " + arg;
            return false;
        }
    }
    return true;
}

bool DioConfig::validate(std::string &err) const
{
    if (listen_port < 0 || listen_port > 65535) {
        err = "invalid listen port (must be 0-65535, 0 = OS-assigned)";
        return false;
    }
    if (thread_pool_size == 0) {
        err = "thread_pool_size must be > 0";
        return false;
    }
    if (sq_entries == 0) {
        err = "sq_entries must be > 0";
        return false;
    }
    for (const auto &d : disks) {
        if (d.id.is_zero()) {
            err = "disk id is zero";
            return false;
        }
        if (d.zones.empty()) {
            err = "disk has no zones";
            return false;
        }
        // path can be empty (dummy disk)
    }
    return true;
}

} // namespace crowdb::diskio
