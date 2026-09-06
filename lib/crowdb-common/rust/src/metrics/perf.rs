// Copyright 2026-present Gian <crow.db@outlook.com>

//! DRAM bandwidth counter via `perf_event_open` (Linux only).
//!
//! Opens a system-wide uncore PMU counter at construction time and
//! reads the cumulative byte count on each [`DramBwCounter::read_bytes`]
//! call. The caller computes delta / elapsed to get a window average.
//!
//! Two PMU backends are auto-detected:
//! - **AMD Zen** — `amd_df` PMU, `dram_channel_data_controller_0..7`
//!   events summed, scaled by `6.1e-5 MiB` per tick (the
//!   `nps1_die_to_dram` metric formula).
//! - **Intel** — `uncore_imc` PMU, `cas_count_read` + `cas_count_write`
//!   events, each tick = 64 B.
//!
//! On non-Linux platforms or when the PMU is unavailable (missing
//! kernel module, insufficient permissions), [`DramBwCounter::new`]
//! returns `None` and the caller reports bandwidth as unsupported.

#![allow(unsafe_code)]
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss,
    clippy::doc_markdown,
    clippy::borrow_as_ptr,
    clippy::items_after_statements
)]

#[cfg(target_os = "linux")]
mod imp {
    use std::io::Read;
    use std::os::unix::io::FromRawFd;
    use std::time::Instant;

    // perf_event_attr.size — the kernel uses this to know which fields
    // are valid. 0 means "use default". We set it to the struct size.
    // The kernel ignores fields beyond what it knows.
    const PERF_ATTR_SIZE: u32 = 120; // sizeof(perf_event_attr) on modern kernels

    /// Raw perf_event_attr layout (partial — only fields we need).
    /// The kernel reads up to `.size` bytes, so unused trailing fields
    /// are zero-filled by Default.
    #[repr(C)]
    #[derive(Default)]
    struct PerfEventAttr {
        type_: u32,
        size: u32,
        config: u64,
        sample_period_or_freq: u64,
        sample_type: u64,
        read_format: u64,
        flags: u64,
        wakeup_events_or_watermark: u32,
        bp_type: u32,
        bp_addr_or_config1: u64,
        bp_len_or_config2: u64,
        branch_sample_type: u64,
        sample_regs_user: u64,
        sample_stack_user: u32,
        clockid: i32,
        sample_regs_intr: u64,
        aux_watermark: u32,
        sample_max_stack: u16,
        reserved2: u16,
        aux_sample_size: u32,
        reserved3: u32,
        sig_data: u64,
    }

    // perf_event_attr.flags bits
    const PERF_FLAG_DISABLED: u64 = 1;
    // read_format: just the total counter value (no extra fields).
    const READ_FORMAT_TOTAL: u64 = 0;

    /// A single opened perf counter fd + its previous reading.
    struct PerfFd {
        fd: i32,
        prev_value: u64,
        prev_instant: Instant,
    }

    impl PerfFd {
        /// Open a system-wide raw PMU event.
        /// `config` is the raw event config for the PMU.
        /// `pmu_type` is the type from /sys/bus/event_source/devices/<pmu>/type.
        /// `cpu` is the CPU from the PMU's cpumask (uncore PMUs require
        /// a specific CPU, not -1).
        fn open(pmu_type: u32, config: u64, cpu: i32) -> Option<Self> {
            let attr = PerfEventAttr {
                type_: pmu_type,
                size: PERF_ATTR_SIZE,
                config,
                flags: PERF_FLAG_DISABLED,
                read_format: READ_FORMAT_TOTAL,
                ..Default::default()
            };

            // pid=-1 → system-wide; cpu from cpumask; PERF_FLAG_FD_CLOEXEC=8
            let fd = unsafe {
                libc::syscall(
                    libc::SYS_perf_event_open,
                    &attr as *const PerfEventAttr,
                    -1i32, // pid: system-wide
                    cpu,   // cpu: from PMU cpumask
                    -1i32, // group_fd
                    8u64,  // PERF_FLAG_FD_CLOEXEC
                )
            };
            if fd < 0 {
                return None;
            }
            let fd = fd as i32;

            // Enable the counter.
            const PERF_EVENT_IOC_ENABLE: u64 = 0x2400;
            let _ = unsafe { libc::ioctl(fd, PERF_EVENT_IOC_ENABLE as libc::c_ulong, 0u64) };

            Some(Self {
                fd,
                prev_value: 0,
                prev_instant: Instant::now(),
            })
        }

        /// Read the current cumulative counter value.
        fn read(&mut self) -> u64 {
            // read() on a perf fd returns a u64 (with read_format=0).
            let mut buf = [0u8; 8];
            let mut file = unsafe { std::fs::File::from_raw_fd(self.fd) };
            let _ = file.read(&mut buf);
            // Don't close the fd — from_raw_fd will close on drop.
            // Use into_raw_fd to prevent close.
            let _ = std::os::unix::io::IntoRawFd::into_raw_fd(file);
            u64::from_le_bytes(buf)
        }

        /// Read and compute delta since last call.
        fn read_delta(&mut self) -> (u64, f64) {
            let current = self.read();
            let delta = current.saturating_sub(self.prev_value);
            let now = Instant::now();
            let elapsed = now.duration_since(self.prev_instant).as_secs_f64();
            self.prev_value = current;
            self.prev_instant = now;
            (delta, elapsed)
        }
    }

    impl Drop for PerfFd {
        fn drop(&mut self) {
            if self.fd >= 0 {
                unsafe { libc::close(self.fd) };
            }
        }
    }

    /// Read a PMU type from /sys/bus/event_source/devices/<name>/type.
    fn read_pmu_type(name: &str) -> Option<u32> {
        let path = format!("/sys/bus/event_source/devices/{name}/type");
        std::fs::read_to_string(path)
            .ok()
            .and_then(|s| s.trim().parse().ok())
    }

    /// Read a PMU cpumask from /sys/bus/event_source/devices/<name>/cpumask.
    /// Uncore PMUs (amd_df, uncore_imc) require a specific CPU from this
    /// mask rather than cpu=-1. Returns 0 if the file is missing.
    fn read_pmu_cpumask(name: &str) -> i32 {
        let path = format!("/sys/bus/event_source/devices/{name}/cpumask");
        std::fs::read_to_string(path)
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(0)
    }

    /// Read a PMU event's raw config from
    /// /sys/bus/event_source/devices/<pmu>/events/<event>.
    /// Format: "event=0xNN,umask=0xNN" or just "event=0xNN".
    /// Returns None if the file doesn't exist (some PMUs like amd_df
    /// don't expose events/ in sysfs — their encodings come from perf's
    /// JSON metric database and must be hardcoded by the caller).
    fn read_pmu_event_config(pmu: &str, event: &str) -> Option<u64> {
        let path = format!("/sys/bus/event_source/devices/{pmu}/events/{event}");
        let content = std::fs::read_to_string(path).ok()?;
        let mut event_val: u64 = 0;
        let mut umask_val: u64 = 0;
        for part in content.trim().split(',') {
            if let Some(v) = part.strip_prefix("event=") {
                event_val = u64::from_str_radix(v.trim_start_matches("0x"), 16).ok()?;
            } else if let Some(v) = part.strip_prefix("umask=") {
                umask_val = u64::from_str_radix(v.trim_start_matches("0x"), 16).ok()?;
            }
        }
        // config = umask<<8 | event
        Some((umask_val << 8) | event_val)
    }

    /// Build an AMD DF config value from event + umask, respecting the
    /// bit layout: event occupies config bits 0-7, 32-35, 59-60;
    /// umask occupies config bits 8-15.
    const fn amd_df_config(event: u64, umask: u64) -> u64 {
        // event bits 0-7 → config bits 0-7
        let low = event & 0xFF;
        // event bits 8-11 → config bits 32-35
        let mid = (event >> 8) & 0x0F;
        // event bits 12-13 → config bits 59-60
        let high = (event >> 12) & 0x03;
        (umask << 8) | low | (mid << 32) | (high << 59)
    }

    /// AMD DF dram_channel_data_controller event encodings (from perf's
    /// JSON metric database). umask=0x38 for all channels; event values
    /// are 0x07, 0x47, 0x87, 0xc7, 0x107, 0x147, 0x187, 0x1c7.
    const AMD_DF_CHANNEL_EVENTS: [u64; 8] = [
        amd_df_config(0x07, 0x38),
        amd_df_config(0x47, 0x38),
        amd_df_config(0x87, 0x38),
        amd_df_config(0xc7, 0x38),
        amd_df_config(0x107, 0x38),
        amd_df_config(0x147, 0x38),
        amd_df_config(0x187, 0x38),
        amd_df_config(0x1c7, 0x38),
    ];

    /// DRAM bandwidth counter. Auto-detects AMD vs Intel PMU.
    /// Returns `None` if no suitable PMU is available.
    pub struct DramBwCounter {
        // AMD: 8 channel fds, each tick = 6.1e-5 MiB.
        // Intel: 2 fds (read + write), each tick = 64 B.
        fds: Vec<PerfFd>,
        // Scale factor: multiply raw delta sum by this to get bytes.
        scale: f64,
    }

    impl DramBwCounter {
        /// Try to create a DRAM bandwidth counter.
        /// Detects the platform and opens the appropriate PMU events.
        #[must_use]
        pub fn new() -> Option<Self> {
            // Try AMD first: amd_df with dram_channel_data_controller_0..7
            if let Some(counter) = Self::new_amd() {
                return Some(counter);
            }
            // Try Intel: uncore_imc with cas_count_read + cas_count_write
            if let Some(counter) = Self::new_intel() {
                return Some(counter);
            }
            None
        }

        /// AMD: open 8 dram_channel_data_controller fds.
        /// Zen 3 has only 4 hardware counters, so perf will multiplex.
        /// Each tick ≈ 6.1e-5 MiB = 64 bytes (after scaling).
        /// The 8-channel sum × scale gives total die DRAM bytes.
        /// Event encodings are hardcoded from perf's JSON metric database
        /// (amd_df does not expose events/ in sysfs).
        fn new_amd() -> Option<Self> {
            let pmu_type = read_pmu_type("amd_df")?;
            let cpu = read_pmu_cpumask("amd_df");
            let mut fds = Vec::with_capacity(8);
            for &config in &AMD_DF_CHANNEL_EVENTS {
                let fd = PerfFd::open(pmu_type, config, cpu)?;
                fds.push(fd);
            }
            // Scale: 6.1e-5 MiB per tick = 6.1e-5 * 1048576 bytes ≈ 64 bytes.
            // The nps1_die_to_dram metric uses ScaleUnit=6.1e-5MiB.
            let scale = 6.1e-5 * 1024.0 * 1024.0; // bytes per tick
            Some(Self { fds, scale })
        }

        /// Intel: open cas_count_read + cas_count_write on uncore_imc.
        /// Each tick = 64 bytes (one cache line / DRAM beat).
        /// For multi-socket, perf expands to all uncore_imc instances,
        /// but we open one fd per event (system-wide covers all).
        fn new_intel() -> Option<Self> {
            let pmu_type = read_pmu_type("uncore_imc")?;
            let cpu = read_pmu_cpumask("uncore_imc");
            let read_config = read_pmu_event_config("uncore_imc", "cas_count_read")?;
            let write_config = read_pmu_event_config("uncore_imc", "cas_count_write")?;
            let read_fd = PerfFd::open(pmu_type, read_config, cpu)?;
            let write_fd = PerfFd::open(pmu_type, write_config, cpu)?;
            // Each tick = 64 bytes.
            let scale = 64.0;
            Some(Self {
                fds: vec![read_fd, write_fd],
                scale,
            })
        }

        /// Read the current DRAM bandwidth in bytes/sec since the last call.
        /// Returns `None` if the read fails.
        pub fn read_bytes_per_sec(&mut self) -> Option<f64> {
            if self.fds.is_empty() {
                return None;
            }
            let mut total_delta: u64 = 0;
            let mut max_elapsed: f64 = 0.0;
            for fd in &mut self.fds {
                let (delta, elapsed) = fd.read_delta();
                total_delta = total_delta.saturating_add(delta);
                if elapsed > max_elapsed {
                    max_elapsed = elapsed;
                }
            }
            if max_elapsed <= 0.0 {
                return None;
            }
            let bytes = total_delta as f64 * self.scale;
            Some(bytes / max_elapsed)
        }
    }

    impl Default for DramBwCounter {
        fn default() -> Self {
            Self::new().unwrap_or(Self {
                fds: Vec::new(),
                scale: 0.0,
            })
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod imp {
    /// Stub on non-Linux platforms — DRAM BW is always unsupported.
    pub struct DramBwCounter;

    impl DramBwCounter {
        #[must_use]
        pub fn new() -> Option<Self> {
            None
        }
        pub fn read_bytes_per_sec(&mut self) -> Option<f64> {
            None
        }
    }

    impl Default for DramBwCounter {
        fn default() -> Self {
            Self
        }
    }
}

#[cfg(not(target_os = "linux"))]
pub use imp::DramBwCounter;
#[cfg(target_os = "linux")]
pub use imp::DramBwCounter;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dram_bw_counter_creates_or_none() {
        // On a machine with perf_event_paranoid=-1 and amd_uncore loaded,
        // this should create a counter. Otherwise it returns None.
        // Either outcome is valid — we just check it doesn't panic.
        let _ = DramBwCounter::new();
    }
}
