#!/usr/bin/env bash
# CROWDB ChunkDB EC allocation regression.
#
# Three co-located logical nodes, each running KV, DiskDB, and ChunkDB.
# The fixture has three racks, three KV data groups, and four 4-TiB logical
# disks per DiskDB. Each operation allocates one EC 8+4 strip. DiskDB requests
# are batched per data group within that strip; strips are not batched together.
#
# Optional environment variables:
#   CHUNKDB_BENCH_DURATION       seconds per case (default: 20)
#   CHUNKDB_BENCH_CASES          space-separated case labels
#   CHUNKDB_BENCH_CONNECTIONS    override all client/service connection counts
#   CHUNKDB_BENCH_RPC_WORKERS    override all RPC worker counts
#   CHUNKDB_BENCH_KV_INFLIGHT    KV proposal window (default: 32)
#   CHUNKDB_BENCH_KV_COALESCE    KV coalescing width (default: 32)
#   CHUNKDB_BENCH_DISK_CAPACITY  bytes per disk (default: 4 TiB)
#   CHUNKDB_BENCH_ZONE_SIZE      bytes per zone (default: 256 GiB)
#   CHUNKDB_BENCH_LOG_ROOT       retained run root
#   CHUNKDB_BENCH_RESULTS        result TSV path
#
# Wl       Grp Thr Strip EC  Cli Cdb Ddb Kv Wkr Win Coal chunk/s block/s p50   p99    Dur Err Stop     Spc
# allocate  3   1   1     8+4 2   2   2   2  2   32  32   436     5232    2386  2777   20s 0   deadline exact
# allocate  3   16  1     8+4 2   2   2   2  2   32  32   5099    61188   3060  4810   20s 0   deadline exact
# allocate  3   128 1     8+4 4   4   4   4  4   32  32   8259    99108   14849 27332  20s 0   deadline exact
# allocate  3   256 1     8+4 4   4   4   4  4   32  32   8737    104844  28112 51656  20s 0   deadline exact
# allocate  3   512 1     8+4 4   4   4   4  4   32  32   8548    102576  57402 103077 20s 0   deadline exact
# Clean artifacts: chunkdb-regression-20260905-181843 (all rows).
set -euo pipefail
cd "$(dirname "$0")/.."

unset CROWDB_ASAN
DURATION="${CHUNKDB_BENCH_DURATION:-20}"
CASES="${CHUNKDB_BENCH_CASES:-}"
CONNECTIONS_OVERRIDE="${CHUNKDB_BENCH_CONNECTIONS:-}"
RPC_WORKERS_OVERRIDE="${CHUNKDB_BENCH_RPC_WORKERS:-}"
KV_INFLIGHT="${CHUNKDB_BENCH_KV_INFLIGHT:-32}"
KV_COALESCE="${CHUNKDB_BENCH_KV_COALESCE:-32}"
DISK_CAPACITY="${CHUNKDB_BENCH_DISK_CAPACITY:-4398046511104}"
ZONE_SIZE="${CHUNKDB_BENCH_ZONE_SIZE:-274877906944}"
RUN_STAMP=$(date +%Y%m%d-%H%M%S)
LOG_ROOT="${CHUNKDB_BENCH_LOG_ROOT:-$(pwd)/bench-log/chunkdb-regression-$RUN_STAMP}"
RESULTS_FILE="${CHUNKDB_BENCH_RESULTS:-$LOG_ROOT/results.tsv}"
CURRENT_CONFIG=""
FAILURES=0
CASE_NUMBER=0

if ! [[ "$DURATION" =~ ^[1-9][0-9]*$ ]] || ! [[ "$DISK_CAPACITY" =~ ^[1-9][0-9]*$ ]] \
    || ! [[ "$ZONE_SIZE" =~ ^[1-9][0-9]*$ ]]; then
    echo "ERROR: duration, disk capacity, and zone size must be positive integers" >&2
    exit 2
fi

cli() {
    ./target/release/crowdb-cli --log-root "$LOG_ROOT" --config "$CURRENT_CONFIG" "$@"
}

destroy_cluster() {
    if [ -n "$CURRENT_CONFIG" ] && [ -f "$CURRENT_CONFIG" ]; then
        cli cluster destroy || true
    fi
    CURRENT_CONFIG=""
}
trap destroy_cluster EXIT

field() {
    local line="$1" name="$2"
    sed -n "s/.*${name}=\([^ ]*\).*/\1/p" <<<"$line"
}

verify_logs() {
    local label="$1" kv_metrics diskdb_metrics chunkdb_metrics cli_metrics
    local kv_rpc diskdb_rpc chunkdb_rpc cli_rpc expected_servers expected_clients
    kv_metrics=$(find "$LOG_ROOT" -path '*/deploy/rack*/node*/log/crowdb-kv-server-metrics-*.log' -type f | wc -l)
    diskdb_metrics=$(find "$LOG_ROOT" -path '*/deploy/rack*/node*/log/crowdb-diskdb-metrics-*.log' -type f | wc -l)
    chunkdb_metrics=$(find "$LOG_ROOT" -path '*/deploy/rack*/node*/log/crowdb-chunkdb-metrics-*.log' -type f | wc -l)
    cli_metrics=$(find "$LOG_ROOT" -path '*/bench-chunkdb-*/crowdb-cli-metrics-*.log' -type f | wc -l)
    kv_rpc=$(find "$LOG_ROOT" -path '*/deploy/rack*/node*/log/crowdb-kv-server-rpc-*.log' -type f | wc -l)
    diskdb_rpc=$(find "$LOG_ROOT" -path '*/deploy/rack*/node*/log/crowdb-diskdb-rpc-*.log' -type f | wc -l)
    chunkdb_rpc=$(find "$LOG_ROOT" -path '*/deploy/rack*/node*/log/crowdb-chunkdb-rpc-*.log' -type f | wc -l)
    cli_rpc=$(find "$LOG_ROOT" -path '*/bench-chunkdb-*/crowdb-cli-rpc-*.log' -type f | wc -l)
    expected_servers=$((CASE_NUMBER * 3))
    expected_clients="$CASE_NUMBER"
    if [ "$kv_metrics" -ne "$expected_servers" ] || [ "$diskdb_metrics" -ne "$expected_servers" ] \
        || [ "$chunkdb_metrics" -ne "$expected_servers" ] || [ "$cli_metrics" -ne "$expected_clients" ] \
        || [ "$kv_rpc" -ne "$expected_servers" ] || [ "$diskdb_rpc" -ne "$expected_servers" ] \
        || [ "$chunkdb_rpc" -ne "$expected_servers" ] || [ "$cli_rpc" -ne "$expected_clients" ]; then
        echo "ERROR: incomplete logs for $label (kv=$kv_metrics/$kv_rpc diskdb=$diskdb_metrics/$diskdb_rpc chunkdb=$chunkdb_metrics/$chunkdb_rpc cli=$cli_metrics/$cli_rpc)" >&2
        return 1
    fi
    echo "    logs: kv=$kv_metrics/$kv_rpc diskdb=$diskdb_metrics/$diskdb_rpc chunkdb=$chunkdb_metrics/$chunkdb_rpc cli=$cli_metrics/$cli_rpc"
}

run_case() {
    local concurrency="$1" label="$2" profile_connections="$3" profile_workers="$4"
    if [ -n "$CASES" ] && [[ " $CASES " != *" $label "* ]]; then
        return
    fi
    local connections="${CONNECTIONS_OVERRIDE:-$profile_connections}"
    local workers="${RPC_WORKERS_OVERRIDE:-$profile_workers}"
    CURRENT_CONFIG="$LOG_ROOT/$label-console.toml"
    CASE_NUMBER=$((CASE_NUMBER + 1))
    echo ">>> $label (EC 8+4, concurrency=$concurrency)"
    cli cluster local-deploy -t combined \
        --kv-backend mem-block --wal-backend mem-block --metrics-interval 1 \
        --event-write --peer-pool-size "$connections" --rpc-workers "$workers" \
        --max-inflight "$KV_INFLIGHT" --coalesce-max-keys "$KV_COALESCE" \
        --data-groups 1,2,3 --disk-groups-per-node 1 --disks-per-group 4 \
        --disk-capacity-bytes "$DISK_CAPACITY" --disk-zone-size-bytes "$ZONE_SIZE" \
        --disk-unit-size-bytes 1048576 --kv-connections "$connections" \
        --kv-client-rpc-workers "$workers" --diskdb-connections "$connections" \
        --diskdb-client-rpc-workers "$workers" --chunkdb-instances 3

    local output status line space busy expected
    set +e
    output=$(timeout --signal=INT --kill-after=10 "$((DURATION + 40))" \
        ./target/release/crowdb-cli --log-root "$LOG_ROOT" --config "$CURRENT_CONFIG" \
        bench chunkdb allocate --duration-secs "$DURATION" --concurrency "$concurrency" \
        --chunkdb-connections "$connections" --chunkdb-client-rpc-workers "$workers" \
        --strip-count 1 --strip-type ec --data-num 8 --code-num 4 \
        --write-granularity-kb 1024 --seed 1 --metrics-interval 1 2>&1)
    status=$?
    set -e
    printf '%s\n' "$output"
    line=$(sed -n '/^chunkdb bench /p' <<<"$output" | tail -n 1)
    if [ -z "$line" ]; then
        printf 'allocate\t3\t%s\t1\t8+4\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t0\t0\t0\t0\t%ss\t1\tunknown\tunknown\n' \
            "$concurrency" "$connections" "$connections" "$connections" "$connections" \
            "$workers" "$KV_INFLIGHT" "$KV_COALESCE" "$DURATION" >>"$RESULTS_FILE"
    else
        busy=$(field "$line" busy_delta)
        expected=$(field "$line" expected_busy_delta)
        space=mismatch
        if [ -n "$busy" ] && [ "$busy" = "$expected" ]; then
            space=exact
        fi
        printf 'allocate\t3\t%s\t1\t8+4\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%ss\t%s\t%s\t%s\n' \
            "$concurrency" "$connections" "$connections" "$connections" "$connections" \
            "$workers" "$KV_INFLIGHT" "$KV_COALESCE" "$(field "$line" ops_per_sec)" \
            "$(field "$line" block_allocs_per_sec)" "$(field "$line" p50_us)" \
            "$(field "$line" p99_us)" "$DURATION" "$(field "$line" errors)" \
            "$(field "$line" stop)" "$space" >>"$RESULTS_FILE"
    fi
    if ! verify_logs "$label"; then
        FAILURES=$((FAILURES + 1))
    fi
    destroy_cluster
    if [ "$status" -ne 0 ]; then
        echo "ERROR: benchmark failed for $label (exit=$status)" >&2
        FAILURES=$((FAILURES + 1))
    fi
}

echo "=== building release binaries ==="
pixi run -- cargo build --release -p crowdb-cli -p crowdb-kv-server -p crowdb-diskdb -p crowdb-chunkdb
mkdir -p "$LOG_ROOT" "$(dirname "$RESULTS_FILE")"
printf 'Wl\tGrp\tThr\tStrip\tEC\tCli\tCdb\tDdb\tKv\tWkr\tWin\tCoal\tchunk/s\tblock/s\tp50\tp99\tDur\tErr\tStop\tSpc\n' >"$RESULTS_FILE"

run_case 1 allocate_ec8_4_1t 2 2
run_case 16 allocate_ec8_4_16t 2 2
run_case 128 allocate_ec8_4_128t 4 4
run_case 256 allocate_ec8_4_256t 4 4
run_case 512 allocate_ec8_4_512t 4 4

echo "=== DONE ==="
echo "Logs and results retained in $LOG_ROOT"
column -t -s$'\t' "$RESULTS_FILE"
if [ "$FAILURES" -ne 0 ]; then
    echo "ERROR: $FAILURES regression case(s) failed" >&2
    exit 1
fi
