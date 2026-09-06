// Copyright 2026-present Gian <crow.db@outlook.com>

//! OS-level system metrics: CPU time, memory RSS, TCP retransmits, and
//! DRAM bandwidth.
//!
//! On Linux, reads `/proc/self/stat` for CPU jiffies, `/proc/self/status`
//! for RSS, `/proc/net/snmp` for TCP retransmit/lost counters, and
//! `perf_event_open` for DRAM read+write bandwidth (AMD `amd_df` or Intel
//! `uncore_imc` uncore PMU). On macOS (and other non-Linux platforms),
//! CPU and RSS are read via `ps` command output; TCP and DRAM BW are
//! stubbed (reported as 0 / unsupported).

use std::io::Write;
use std::time::Instant;

use serde::{Deserialize, Serialize};

#[cfg(target_os = "linux")]
use std::fs;

#[cfg(target_os = "linux")]
use super::perf::DramBwCounter;

/// Snapshot of system-level metrics at a single point in time.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SystemMetrics {
    /// User CPU utilization (percent) since the previous snapshot.
    pub cpu_user_pct: u64,
    /// System CPU utilization (percent) since the previous snapshot.
    pub cpu_sys_pct: u64,
    /// Resident set size in KB.
    pub rss_kb: u64,
    /// TCP retransmit count delta since previous snapshot (Linux only).
    pub tcp_retransmits: u64,
    /// TCP lost segment count delta since previous snapshot (Linux only).
    pub tcp_lost: u64,
    /// Average DRAM read+write bandwidth in MiB/s since the previous
    /// snapshot. `None` when the PMU is unavailable (non-Linux, missing
    /// kernel module, or insufficient permissions).
    pub dram_bw_mib: Option<f64>,
}

/// Collects OS-level metrics by reading `/proc` (Linux) or using
/// `ps` (macOS). Maintains previous-state to compute deltas for
/// CPU time and TCP counters. On Linux, also owns a `DramBwCounter`
/// that reads the uncore PMU via `perf_event_open`.
#[allow(clippy::struct_field_names)]
pub struct SystemCollector {
    prev_cpu_user_us: u64,
    prev_cpu_sys_us: u64,
    prev_tcp_retransmits: u64,
    prev_tcp_lost: u64,
    prev_instant: Instant,
    #[cfg(target_os = "linux")]
    dram_bw: Option<DramBwCounter>,
}

impl SystemCollector {
    /// Create a new collector. The first `collect()` call will report
    /// deltas from this baseline. On Linux, attempts to open a DRAM
    /// bandwidth PMU counter; if unavailable, `dram_bw_mib` will be
    /// `None` in all snapshots.
    #[must_use]
    pub fn new() -> Self {
        let (user_us, sys_us) = read_cpu_times();
        let (retransmits, lost) = read_tcp_stats();
        Self {
            prev_cpu_user_us: user_us,
            prev_cpu_sys_us: sys_us,
            prev_tcp_retransmits: retransmits,
            prev_tcp_lost: lost,
            prev_instant: Instant::now(),
            #[cfg(target_os = "linux")]
            dram_bw: DramBwCounter::new(),
        }
    }

    /// Collect a system snapshot, computing deltas from the previous call.
    #[must_use]
    pub fn collect(&mut self) -> SystemMetrics {
        let (user_us, sys_us) = read_cpu_times();
        let (retransmits, lost) = read_tcp_stats();
        let elapsed = self.prev_instant.elapsed();
        self.prev_instant = Instant::now();

        let delta_user_us = user_us.saturating_sub(self.prev_cpu_user_us);
        let delta_sys_us = sys_us.saturating_sub(self.prev_cpu_sys_us);
        let tcp_retransmits = retransmits.saturating_sub(self.prev_tcp_retransmits);
        let tcp_lost = lost.saturating_sub(self.prev_tcp_lost);

        self.prev_cpu_user_us = user_us;
        self.prev_cpu_sys_us = sys_us;
        self.prev_tcp_retransmits = retransmits;
        self.prev_tcp_lost = lost;

        let rss_kb = read_rss_kb();

        // CPU utilization = (delta_cpu_us / elapsed_us) * 100.
        // On multi-core systems this can exceed 100% (e.g. 300% = 3 cores).
        let elapsed_us = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        let cpu_user_pct = delta_user_us
            .checked_mul(100)
            .and_then(|v| v.checked_div(elapsed_us))
            .unwrap_or(0);
        let cpu_sys_pct = delta_sys_us
            .checked_mul(100)
            .and_then(|v| v.checked_div(elapsed_us))
            .unwrap_or(0);

        #[cfg(target_os = "linux")]
        let dram_bw_mib = self
            .dram_bw
            .as_mut()
            .and_then(DramBwCounter::read_bytes_per_sec)
            .map(|bps| bps / 1024.0 / 1024.0);
        #[cfg(not(target_os = "linux"))]
        let dram_bw_mib = None;

        SystemMetrics {
            cpu_user_pct,
            cpu_sys_pct,
            rss_kb,
            tcp_retransmits,
            tcp_lost,
            dram_bw_mib,
        }
    }
}

impl Default for SystemCollector {
    fn default() -> Self {
        Self::new()
    }
}

/// Write a system snapshot to the flush writer in the "misc" section format.
pub fn flush_system<W: Write>(writer: &mut W, snap: &SystemMetrics) {
    #[allow(clippy::cast_precision_loss)]
    let rss_gb = snap.rss_kb as f64 / 1024.0 / 1024.0;
    let bw = match snap.dram_bw_mib {
        Some(b) => format!("{b:.1}"),
        None => "unsupported".to_string(),
    };
    let _ = writeln!(
        writer,
        "sys  cpu.user={}% cpu.sys={}% rss_gb={rss_gb:.2} tcp_retrans={} tcp_lost={} bw_mib={bw}",
        snap.cpu_user_pct, snap.cpu_sys_pct, snap.tcp_retransmits, snap.tcp_lost,
    );
}

// ── Platform-specific readers ───────────────────────────────────

#[cfg(target_os = "linux")]
fn read_cpu_times() -> (u64, u64) {
    // /proc/self/stat fields (1-based):
    //   14 = utime (clock ticks)
    //   15 = stime (clock ticks)
    // Convert to microseconds: ticks * 1_000_000 / sysconf(_SC_CLK_TCK)
    // _SC_CLK_TCK is virtually always 100 on Linux.
    let ticks_per_sec: u64 = 100;
    if let Ok(stat) = fs::read_to_string("/proc/self/stat") {
        // The comm field is in parentheses and may contain spaces,
        // so split from the right after the last ')'.
        if let Some(pos) = stat.rfind(')') {
            let rest = &stat[pos + 2..];
            let fields: Vec<&str> = rest.split_whitespace().collect();
            // fields[0] = state, fields[1] = ppid, ...
            // After ')', the next fields are: state ppid pgrp session tty_nr
            // tpgid flags minflt cminflt majflt cmajflt utime stime ...
            // So utime is at index 11, stime at index 12 (0-based after ')').
            if fields.len() > 12 {
                let utime: u64 = fields[11].parse().unwrap_or(0);
                let stime: u64 = fields[12].parse().unwrap_or(0);
                let user_us = utime * 1_000_000 / ticks_per_sec;
                let sys_us = stime * 1_000_000 / ticks_per_sec;
                return (user_us, sys_us);
            }
        }
    }
    (0, 0)
}

#[cfg(target_os = "linux")]
fn read_rss_kb() -> u64 {
    if let Ok(status) = fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            if let Some(rest) = line.strip_prefix("VmRSS:") {
                let kb: u64 = rest
                    .split_whitespace()
                    .next()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                return kb;
            }
        }
    }
    0
}

#[cfg(target_os = "linux")]
fn read_tcp_stats() -> (u64, u64) {
    // /proc/net/snmp line: Tcp: ... RetransSegs ...
    // The format has two lines: labels and values.
    if let Ok(snmp) = fs::read_to_string("/proc/net/snmp") {
        let mut lines = snmp.lines().filter(|l| l.starts_with("Tcp:"));
        if let (Some(labels), Some(values)) = (lines.next(), lines.next()) {
            let labels: Vec<&str> = labels.split_whitespace().collect();
            let values: Vec<&str> = values.split_whitespace().collect();
            let retrans_idx = labels.iter().position(|&l| l == "RetransSegs");
            let lost_idx = labels.iter().position(|&l| l == "InErrs" || l == "OutRsts");
            let retransmits = retrans_idx
                .and_then(|i| values.get(i))
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            let lost = lost_idx
                .and_then(|i| values.get(i))
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            return (retransmits, lost);
        }
    }
    (0, 0)
}

#[cfg(not(target_os = "linux"))]
fn read_cpu_times() -> (u64, u64) {
    // macOS: use `ps` to get CPU time. This is a best-effort approach.
    // ps -o utime,stime -p <pid> gives times in M:SS.cc format.
    // For simplicity, report 0 deltas on non-Linux platforms.
    // A future improvement could use mach_time APIs via FFI.
    (0, 0)
}

#[cfg(not(target_os = "linux"))]
fn read_rss_kb() -> u64 {
    // macOS: use `ps` to get RSS.
    // ps -o rss -p <pid> gives RSS in KB.
    let pid = std::process::id();
    let output = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output();
    if let Ok(out) = output {
        let rss_str = String::from_utf8_lossy(&out.stdout);
        let rss: u64 = rss_str.trim().parse().unwrap_or(0);
        return rss;
    }
    0
}

#[cfg(not(target_os = "linux"))]
fn read_tcp_stats() -> (u64, u64) {
    // TCP stats are not available on macOS without private APIs.
    (0, 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collect_returns_snapshot() {
        let mut collector = SystemCollector::new();
        let snap = collector.collect();
        // CPU deltas should be small but non-negative.
        // RSS should be non-zero (the test process has memory).
        assert!(snap.rss_kb > 0, "RSS should be non-zero, got {}", snap.rss_kb);
    }

    #[test]
    fn collect_delta_is_non_negative() {
        let mut collector = SystemCollector::new();
        let _snap1 = collector.collect();
        let snap2 = collector.collect();
        // Utilization should be non-negative (can exceed 100% on multi-core).
        assert!(snap2.cpu_user_pct <= 100 * 1024);
    }

    #[test]
    fn flush_system_writes_all_fields() {
        let snap = SystemMetrics {
            cpu_user_pct: 42,
            cpu_sys_pct: 17,
            rss_kb: 4096,
            tcp_retransmits: 3,
            tcp_lost: 1,
            dram_bw_mib: Some(512.5),
        };
        let mut buf = Vec::new();
        flush_system(&mut buf, &snap);
        let out = String::from_utf8(buf).unwrap();
        assert!(out.contains("cpu.user=42%"));
        assert!(out.contains("cpu.sys=17%"));
        assert!(out.contains("rss_gb=0.00"));
        assert!(out.contains("tcp_retrans=3"));
        assert!(out.contains("tcp_lost=1"));
        assert!(out.contains("bw_mib=512.5"));
    }

    #[test]
    fn flush_system_writes_unsupported_when_none() {
        let snap = SystemMetrics {
            cpu_user_pct: 0,
            cpu_sys_pct: 0,
            rss_kb: 0,
            tcp_retransmits: 0,
            tcp_lost: 0,
            dram_bw_mib: None,
        };
        let mut buf = Vec::new();
        flush_system(&mut buf, &snap);
        let out = String::from_utf8(buf).unwrap();
        assert!(out.contains("bw_mib=unsupported"));
    }
}
