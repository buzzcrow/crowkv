#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
#
# One-shot perf environment setup for Ubuntu 24.04. Run with sudo:
#   sudo bash doc/dev/setup-perf.sh
#
# Detects AMD vs Intel and configures the right PMU. Each step prints
# a checkmark/cross with the expected value; exits non-zero on the first
# failure. Safe to re-run. See doc/dev/env_setup.md for what each step
# does and why.

set -euo pipefail

# Status icons (emoji badges, self-colored — no ANSI needed):
#   ok   = green square with white check  (U+2705)
#   fail = red square with white X        (U+274C)
#   warn = yellow warning triangle        (U+26A0 U+FE0F)
ok()   { printf '  \xe2\x9c\x85 %s\n' "$1"; }
warn() { printf '  \xe2\x9a\xa0\xef\xb8\x8f  %s\n' "$1"; }
fail() { printf '  \xe2\x9d\x8c %s\n' "$1"; exit 1; }
check() { # check "label" "expected" "actual"
  if [[ "$2" == "$3" ]]; then ok "$1: $3"
  else fail "$1: expected '$2', got '$3'"; fi
}
nonempty() { # nonempty "label" "actual"
  if [[ -n "$2" && "$2" != *"not supported"* && "$2" != *"<not supported>"* ]]; then ok "$1: $2"
  else fail "$1: got '$2'"; fi
}

echo "== perf environment setup ($(date)) =="

# --- 0. root check ---------------------------------------------------------
[[ $EUID -eq 0 ]] || fail "must run as root (use: sudo bash $0)"

# --- 0a. system summary ----------------------------------------------------
echo "[system info]"
# CPU: model, sockets, cores/thread, freq, cache
CPU_MODEL="$(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | sed 's/^ //')"
SOCKETS="$(grep -c '^physical id' /proc/cpuinfo | uniq | head -1)"
[[ -z "$SOCKETS" || "$SOCKETS" -eq 0 ]] && SOCKETS=1
CORES_PER_SOCKET="$(grep -m1 'cpu cores' /proc/cpuinfo | awk '{print $4}')"
THREADS="$(grep -c '^processor' /proc/cpuinfo)"
MIN_FREQ="$(lscpu 2>/dev/null | awk -F: '/CPU min MHz/{gsub(/^ +/,"",$2); print $2}')"
MAX_FREQ="$(lscpu 2>/dev/null | awk -F: '/CPU max MHz/{gsub(/^ +/,"",$2); print $2}')"
L3_CACHE="$(lscpu 2>/dev/null | awk -F: '/L3 cache/{gsub(/^ +/,"",$2); print $2}')"
NUMA_NODES="$(lscpu 2>/dev/null | awk -F: '/NUMA node\(s\)/{gsub(/^ +/,"",$2); print $2}')"
printf '  CPU:          %s\n' "$CPU_MODEL"
printf '  Sockets:      %s\n' "$SOCKETS"
printf '  Cores/socket: %s\n' "$CORES_PER_SOCKET"
printf '  Threads:      %s\n' "$THREADS"
printf '  Frequency:    %s - %s MHz\n' "${MIN_FREQ:-?}" "${MAX_FREQ:-?}"
printf '  L3 cache:     %s\n' "${L3_CACHE:-?}"
printf '  NUMA nodes:   %s\n' "${NUMA_NODES:-1}"

# Memory: total + per-channel DIMM layout.
# Single awk pass over dmidecode -t memory: emits one row per populated
# DIMM, then a summary line "SUMMARY <dimms> <channels> <mt_s>".
MEM_TOTAL="$(awk '/MemTotal/{printf "%.1f GB", $2/1024/1024}' /proc/meminfo)"
printf '  Memory total: %s\n' "$MEM_TOTAL"
DIMM_RAW="$(dmidecode -t memory 2>/dev/null)"
if [[ -n "$DIMM_RAW" ]]; then
  echo "  Memory per channel:"
  printf '    %-28s %-10s %-10s %-12s\n' "Channel/Slot" "Size" "Type" "Speed"
  SUMMARY="$(awk -- '
    function flush() {
      if (size == "") return
      printf "    %-28s %-10s %-10s %-12s\n", loc, size, type, speed
      dimms++
      ch = loc; sub(/-?DIMM.*/, "", ch)
      if (!(ch in seen)) { seen[ch]=1; channels++ }
      if (mt_s == "" && speed ~ /[0-9]+/) {
        mt_s = speed; sub(/[^0-9].*/, "", mt_s)
      }
      loc=""; size=""; type=""; speed=""
    }
    /^Handle / { flush() }
    /^\tLocator:/ { loc=substr($0, index($0,":")+2) }
    /^\tSize:/    { s=substr($0, index($0,":")+2); if (s !~ /No Module/ && s !~ /Unknown/) size=s }
    /^\tType:/    { t=substr($0, index($0,":")+2); if (t !~ /Unknown/) type=t }
    /^\tSpeed:/   { s=substr($0, index($0,":")+2); if (s !~ /Unknown/ && s !~ /0 MHz/) speed=s }
    END { flush(); printf "SUMMARY %d %d %s\n", dimms+0, channels+0, mt_s }
  ' <<< "$DIMM_RAW")"
  # Print DIMM rows (everything except the SUMMARY line), then parse summary.
  echo "$SUMMARY" | grep -v '^SUMMARY ' | sed 's/^  /  /'
  S_LINE="$(echo "$SUMMARY" | grep '^SUMMARY ')"
  DIMM_COUNT="$(echo "$S_LINE" | awk '{print $2}')"
  CHANNELS="$(echo "$S_LINE" | awk '{print $3}')"
  MT_S="$(echo "$S_LINE" | awk '{print $4}')"
  if [[ -n "$MT_S" && "$MT_S" != "0" && "$CHANNELS" -gt 0 ]]; then
    PEAK_GB=$(( MT_S * CHANNELS * 8 / 1000 ))
    printf '  Channels:     %s populated, %s DIMMs total\n' "$CHANNELS" "$DIMM_COUNT"
    printf '  Peak BW:      ~%s GB/s (%s ch x %s MT/s x 8B)\n' "$PEAK_GB" "$CHANNELS" "$MT_S"
  fi
else
  warn "DIMM details unavailable (dmidecode needs root — already root?)"
fi

# Disk: block devices with size, model, rotational
echo "  Disks:"
while IFS=: read -r dev; do
  DEV_NAME="$(basename "$dev")"
  [[ "$DEV_NAME" == loop* || "$DEV_NAME" == ram* || "$DEV_NAME" == zram* ]] && continue
  DEV_SIZE="$(lsblk -bn -o SIZE "/dev/$DEV_NAME" 2>/dev/null | head -1)"
  DEV_ROT="$(lsblk -bn -o ROTA "/dev/$DEV_NAME" 2>/dev/null | head -1)"
  DEV_MODEL="$(lsblk -n -o MODEL "/dev/$DEV_NAME" 2>/dev/null | head -1 | tr -s ' ')"
  if [[ -n "$DEV_SIZE" ]]; then
    if [[ "$DEV_SIZE" -ge 1099511627776 ]]; then
      SIZE_STR="$(awk "BEGIN{printf \"%.1f TB\", $DEV_SIZE/1099511627776}")"
    elif [[ "$DEV_SIZE" -ge 1073741824 ]]; then
      SIZE_STR="$(awk "BEGIN{printf \"%.1f GB\", $DEV_SIZE/1073741824}")"
    else
      SIZE_STR="$(awk "BEGIN{printf \"%.0f MB\", $DEV_SIZE/1048576}")"
    fi
    [[ "$DEV_ROT" == "1" ]] && ROT_STR="HDD" || ROT_STR="SSD/NVMe"
    printf '    %-12s %8s  %-8s  %s\n' "$DEV_NAME" "$SIZE_STR" "$ROT_STR" "${DEV_MODEL:-}"
  fi
done < <(ls -1 /sys/block/)
echo

# --- 1. perf tooling -------------------------------------------------------
echo "[1/5] perf tooling"
if ! command -v perf >/dev/null 2>&1; then
  echo "  perf not found, installing linux-tools-$(uname -r)..."
  apt update && apt install -y "linux-tools-$(uname -r)" linux-tools-generic
fi
PERF_VER="$(perf --version 2>&1 | head -1)"
nonempty "perf --version" "$PERF_VER"

# Stub perf from linux-tools-generic errors out; redirect to the real one.
if ! perf list >/dev/null 2>&1; then
  REAL_PERF="/usr/lib/linux-tools/$(uname -r)/perf"
  [[ -x "$REAL_PERF" ]] || fail "perf stub detected and $REAL_PERF missing"
  ln -sf "$REAL_PERF" /usr/local/bin/perf
  hash -r
  perf --version >/dev/null 2>&1 || fail "perf still broken after symlink"
fi
ok "perf usable"

# --- 2. perf_event_paranoid = -1 (gates ALL CPU counters) ------------------
echo "[2/5] perf_event_paranoid"
echo 'kernel.perf_event_paranoid = -1' > /etc/sysctl.d/99-perf.conf
sysctl -p /etc/sysctl.d/99-perf.conf >/dev/null
check "paranoid" "-1" "$(cat /proc/sys/kernel/perf_event_paranoid)"

# --- 3. vendor detection ---------------------------------------------------
VENDOR="$(grep -m1 '^vendor_id' /proc/cpuinfo | awk '{print $3}')"
echo "  vendor: $VENDOR"

# --- 4a. AMD: amd_uncore -> amd_df -----------------------------------------
if [[ "$VENDOR" == "AuthenticAMD" ]]; then
  echo "[3/5] AMD: amd_uncore module"
  modprobe amd_uncore 2>/dev/null || true
  echo 'amd_uncore' > /etc/modules-load.d/amd-uncore.conf
  [[ -e /sys/bus/event_source/devices/amd_df ]] \
    || fail "amd_df device missing after modprobe (check dmesg | grep -i uncore)"
  ok "amd_df device present"

  echo "[4/5] AMD: nps1_die_to_dram metric"
  perf list 2>/dev/null | grep -q nps1_die_to_dram \
    || fail "nps1_die_to_dram metric not listed by perf"
  ok "metric listed"

  echo "[5/5] AMD: non-zero read"
  # amd_df is a system-wide uncore PMU; -a is required or every channel
  # reads <not supported> (the metric does not auto-fallback to system-wide).
  OUT="$(perf stat -a -M nps1_die_to_dram -- sleep 1 2>&1)"
  echo "$OUT" | grep -qi 'not supported' && fail "metric returned <not supported> (paranoid or missing -a?)"
  # Metric value lives after '#' on the metric line:
  #   0  dram_channel_data_controller_4  #  164.9 MiB  nps1_die_to_dram  (50.02%)
  # Grab "164.9 MiB", not the raw channel count at $1.
  VAL="$(echo "$OUT" | grep -i 'nps1_die_to_dram' | awk -F'#' '{print $2}' | awk '{print $1, $2}')"
  nonempty "nps1_die_to_dram" "$VAL"
  echo "$OUT" | grep -i 'nps1_die_to_dram'
  # Zen 3 exposes 8 dram_channel_data_controller_* events but only 4
  # hardware counters, so perf time-shares them (~50% multiplexing).
  if echo "$OUT" | grep -q '(5[0-9]\.[0-9]%'; then
    warn "counter multiplexing ~50% (8 events, 4 counters) — expected on Zen 3, values are scaled"
  fi

# --- 4b. Intel: uncore_imc -------------------------------------------------
elif [[ "$VENDOR" == "GenuineIntel" ]]; then
  echo "[3/5] Intel: uncore_imc PMU"
  modprobe intel_uncore 2>/dev/null || true   # usually built-in; no-op if so
  [[ -e /sys/bus/event_source/devices/uncore_imc ]] \
    || fail "uncore_imc device missing (check grep INTEL_UNCORE /boot/config-$(uname -r))"
  ok "uncore_imc device present"

  echo "[4/5] Intel: cas_count events"
  perf list 2>/dev/null | grep -q 'uncore_imc/cas_count_read' \
    || fail "uncore_imc/cas_count_read not listed by perf"
  perf list 2>/dev/null | grep -q 'uncore_imc/cas_count_write' \
    || fail "uncore_imc/cas_count_write not listed by perf"
  ok "cas_count_read + cas_count_write listed"

  echo "[5/5] Intel: non-zero read"
  # uncore_imc is system-wide; -a is required.
  OUT="$(perf stat -a -e 'uncore_imc/cas_count_read/' -- sleep 1 2>&1)"
  echo "$OUT" | grep -qi 'not supported' && fail "cas_count_read returned <not supported> (paranoid or missing -a?)"
  VAL="$(echo "$OUT" | grep 'cas_count_read' | awk '{print $1}')"
  nonempty "cas_count_read" "$VAL"
  echo "$OUT" | grep 'cas_count_read'

else
  fail "unknown vendor: $VENDOR"
fi

echo
echo "== all checks passed =="
echo "next: pixi run -- cargo build --release -p crowdb-cli -p crowdb-kv-server"
