// Copyright 2026-present Gian <crow.db@outlook.com>

// OS-level system metrics: CPU time, memory RSS, TCP retransmits, and
// DRAM bandwidth. Mirrors the Rust `crowdb_common::metrics::system`
// module so C++ servers (crowdb-diskio) and Rust servers (crowdb-kv-server)
// report the same `sys.*` metric lines.
//
// On Linux, reads /proc/self/stat for CPU jiffies, /proc/self/status for
// RSS, /proc/net/snmp for TCP counters, and perf_event_open for DRAM
// read+write bandwidth (AMD amd_df or Intel uncore_imc uncore PMU).
// On macOS, CPU and RSS use `ps`; TCP and DRAM BW are stubbed.
#pragma once

#include <cstdint>
#include <cstdio>
#include <optional>

namespace crowdb::common::metrics
{

// Snapshot of system-level metrics at a single point in time.
struct SystemMetricsSnapshot
{
    // User CPU utilization (percent) since the previous snapshot.
    uint64_t cpu_user_pct = 0;
    // System CPU utilization (percent) since the previous snapshot.
    uint64_t cpu_sys_pct = 0;
    // Resident set size in KB.
    uint64_t rss_kb = 0;
    // TCP retransmit count delta since previous snapshot (Linux only).
    uint64_t tcp_retransmits = 0;
    // TCP lost segment count delta since previous snapshot (Linux only).
    uint64_t tcp_lost = 0;
    // Average DRAM read+write bandwidth in MiB/s since the previous
    // snapshot. std::nullopt when the PMU is unavailable.
    std::optional<double> dram_bw_mib;
};

// Collects OS-level metrics by reading /proc (Linux) or using ps (macOS).
// Maintains previous-state to compute deltas for CPU time and TCP
// counters. On Linux, also owns a DRAM bandwidth PMU counter via
// perf_event_open.
class SystemCollector
{
  public:
    SystemCollector();
    ~SystemCollector(); // NOLINT(performance-trivially-destructible) — pImpl, DramBwImpl incomplete

    SystemCollector(const SystemCollector &)            = delete;
    SystemCollector &operator=(const SystemCollector &) = delete;
    SystemCollector(SystemCollector &&) noexcept;
    SystemCollector &operator=(SystemCollector &&) noexcept;

    // Collect a system snapshot, computing deltas from the previous call.
    SystemMetricsSnapshot collect();

  private:
    uint64_t prev_cpu_user_us_     = 0;
    uint64_t prev_cpu_sys_us_      = 0;
    uint64_t prev_tcp_retransmits_ = 0;
    uint64_t prev_tcp_lost_        = 0;
    // Elapsed time tracking (steady_clock microseconds).
    uint64_t prev_time_us_ = 0;

    // Opaque DRAM bandwidth counter (Linux perf_event_open fds).
    struct DramBwImpl;
    DramBwImpl *dram_bw_ = nullptr;
};

// Write a system snapshot to the writer in the "misc" section format,
// matching the Rust flush_system output line-for-line.
void flush_system(FILE *fp, const SystemMetricsSnapshot &snap);

} // namespace crowdb::common::metrics
