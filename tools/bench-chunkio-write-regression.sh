#!/usr/bin/env bash
# CROWDB end-to-end large chunk-write regression using three NullDisk nodes.
#
# Optional environment variables:
#   CHUNKIO_BENCH_CASES       space-separated case labels
#   CHUNKIO_BENCH_LOG_ROOT    retained run root
#   CHUNKIO_BENCH_RESULTS     result TSV path
#   CHUNKIO_BENCH_TIMEOUT     seconds allowed per case (default: 120)
#
# Reference run (2026-09-06): AMD Ryzen 9 5950X, 16c/32t, Linux 6.8,
# three-node loopback deployment, three NullDisk instances, EC 4+1,
# 1 MiB blocks, 1 GiB chunks, two 64 MiB objects per worker.
#
# Case      Obj  Size MiB  C  TPS obj/s  logical MiB/s  physical MiB/s  p50 us   p99 us   errors
# large_1t    2        64  1       0.39           25.0            31.3  2471891  2471891       0
# large_4t    8        64  4       2.20          140.5           175.7  1686185  1873776       0
#
# Memory-counter samples for the same run were 90.4/292.4 MiB/s average/max
# for large_1t and 216.9/411.8 MiB/s for large_4t. These values are retained
# as a diagnostic baseline, not hard thresholds; the sentinel gates complete
# object accounting, zero errors, stop reason, and complete service metrics.
set -euo pipefail
cd "$(dirname "$0")/.."

unset CROWDB_ASAN
CASES="${CHUNKIO_BENCH_CASES:-}"
TIMEOUT_SECS="${CHUNKIO_BENCH_TIMEOUT:-120}"
RUN_STAMP=$(date +%Y%m%d-%H%M%S)
LOG_ROOT="${CHUNKIO_BENCH_LOG_ROOT:-$(pwd)/bench-log/chunkio-write-regression-$RUN_STAMP}"
RESULTS_FILE="${CHUNKIO_BENCH_RESULTS:-$LOG_ROOT/results.tsv}"
CURRENT_CONFIG=""
CURRENT_LOG_ROOT=""
BENCH_LOG_DIR=""
FAILURES=0

if ! [[ "$TIMEOUT_SECS" =~ ^[1-9][0-9]*$ ]]; then
    echo "ERROR: CHUNKIO_BENCH_TIMEOUT must be a positive integer" >&2
    exit 2
fi

cli() {
    ./target/release/crowdb-cli --log-root "$CURRENT_LOG_ROOT" --config "$CURRENT_CONFIG" "$@"
}

destroy_cluster() {
    if [ -n "$CURRENT_CONFIG" ] && [ -f "$CURRENT_CONFIG" ]; then
        timeout 30 ./target/release/crowdb-cli --log-root "$CURRENT_LOG_ROOT" \
            --config "$CURRENT_CONFIG" cluster destroy || true
    fi
    CURRENT_CONFIG=""
    CURRENT_LOG_ROOT=""
}
trap destroy_cluster EXIT

field() {
    local line="$1" name="$2"
    sed -n "s/.*${name}=\([^ ]*\).*/\1/p" <<<"$line"
}

memory_bandwidth() {
    local mode="$1"
    find "$BENCH_LOG_DIR" -type f -name 'crowdb-cli-metrics-*.log' -print0 |
        xargs -0 sed -n 's/.*bw_mib=\([0-9.]*\).*/\1/p' |
        awk -v mode="$mode" '
            NR == 1 { max = $1 }
            { sum += $1; if ($1 > max) max = $1 }
            END {
                if (NR == 0) print "unsupported";
                else if (mode == "max") printf "%.1f", max;
                else printf "%.1f", sum / NR;
            }'
}

verify_logs() {
    local kv diskdb chunkdb diskio cli_metrics
    kv=$(find "$CURRENT_LOG_ROOT" -type f -name 'crowdb-kv-server-metrics-*.log' | wc -l)
    diskdb=$(find "$CURRENT_LOG_ROOT" -type f -name 'crowdb-diskdb-metrics-*.log' | wc -l)
    chunkdb=$(find "$CURRENT_LOG_ROOT" -type f -name 'crowdb-chunkdb-metrics-*.log' | wc -l)
    diskio=$(find "$CURRENT_LOG_ROOT" -type f -name 'crowdb-diskio-metrics-*.log' | wc -l)
    cli_metrics=$(find "$CURRENT_LOG_ROOT" -type f -name 'crowdb-cli-metrics-*.log' | wc -l)
    [ "$kv" -eq 3 ] && [ "$diskdb" -eq 3 ] && [ "$chunkdb" -eq 3 ] \
        && [ "$diskio" -eq 3 ] && [ "$cli_metrics" -eq 1 ]
}

run_case() {
    local label="$1" objects="$2" object_size="$3" concurrency="$4"
    if [ -n "$CASES" ] && [[ " $CASES " != *" $label "* ]]; then
        return
    fi
    CURRENT_LOG_ROOT="$LOG_ROOT/$label-$RUN_STAMP"
    CURRENT_CONFIG="$CURRENT_LOG_ROOT/console.toml"
    echo ">>> $label (objects=$objects size=$object_size concurrency=$concurrency EC=4+1)"
    set +e
    cli cluster local-deploy -t combined --metrics-interval 1 --allow-unsafe-ec
    local deploy_status=$?
    set -e
    if [ "$deploy_status" -ne 0 ]; then
        printf '%s\t%s\t%s\t%s\t%s\t0\t1\t%s\tfailed\t0\t0\t0\t0\t0\tunsupported\tunsupported\n' \
            "$label" "$objects" "$object_size" "$((object_size / 1048576))" \
            "$concurrency" "$objects" >>"$RESULTS_FILE"
        echo "ERROR: deployment failed for $label (exit=$deploy_status)" >&2
        FAILURES=$((FAILURES + 1))
        destroy_cluster
        return 0
    fi

    local output status line avg_bw max_bw
    set +e
    output=$(timeout --signal=INT --kill-after=10 "$TIMEOUT_SECS" \
        ./target/release/crowdb-cli --log-root "$CURRENT_LOG_ROOT" --config "$CURRENT_CONFIG" \
        bench chunkio write --objects "$objects" --object-size "$object_size" \
        --concurrency "$concurrency" --data-num 4 --code-num 1 \
        --block-size 1048576 --chunk-size 1073741824 --seed 1 \
        --metrics-interval 1 2>&1)
    status=$?
    set -e
    printf '%s\n' "$output"
    BENCH_LOG_DIR=$(sed -n 's/^log dir: //p' <<<"$output" | tail -n 1)
    line=$(sed -n '/^chunkio write:/p' <<<"$output" | tail -n 1)
    if [ -n "$BENCH_LOG_DIR" ] && [ -d "$BENCH_LOG_DIR" ]; then
        avg_bw=$(memory_bandwidth avg)
        max_bw=$(memory_bandwidth max)
    else
        avg_bw=unsupported
        max_bw=unsupported
    fi
    if [ -z "$line" ]; then
        printf '%s\t%s\t%s\t%s\t%s\t0\t1\t%s\tfailed\t0\t0\t0\t0\t0\t%s\t%s\n' \
            "$label" "$objects" "$object_size" "$((object_size / 1048576))" \
            "$concurrency" "$objects" "$avg_bw" "$max_bw" >>"$RESULTS_FILE"
    else
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
            "$label" "$objects" "$object_size" "$((object_size / 1048576))" "$concurrency" \
            "$(field "$line" objects)" "$(field "$line" errors)" \
            "$(field "$line" incomplete)" "$(field "$line" stop)" \
            "$(field "$line" objects_s)" "$(field "$line" logical_mib_s)" \
            "$(field "$line" physical_mib_s)" \
            "$(field "$line" p50_us)" "$(field "$line" p99_us)" \
            "$avg_bw" "$max_bw" >>"$RESULTS_FILE"
    fi
    local completed errors incomplete stop objects_s logical physical p50 p99 valid=1
    completed=$(field "$line" objects)
    errors=$(field "$line" errors)
    incomplete=$(field "$line" incomplete)
    stop=$(field "$line" stop)
    objects_s=$(field "$line" objects_s)
    logical=$(field "$line" logical_mib_s)
    physical=$(field "$line" physical_mib_s)
    p50=$(field "$line" p50_us)
    p99=$(field "$line" p99_us)
    if [ -z "$line" ] || [ "$completed" != "$objects" ] || [ "$errors" != 0 ] \
        || [ "$incomplete" != 0 ] || [ "$stop" != complete ] \
        || [ -z "$objects_s" ] || [ -z "$logical" ] || [ -z "$physical" ] \
        || [ -z "$p50" ] || [ -z "$p99" ]; then
        valid=0
    fi
    if [ "$status" -ne 0 ] || [ "$valid" -ne 1 ] || ! verify_logs; then
        echo "ERROR: $label failed or did not retain all service metrics" >&2
        FAILURES=$((FAILURES + 1))
    fi
    destroy_cluster
}

echo "=== building release binaries ==="
pixi run -- cargo build --release -p crowdb-cli -p crowdb-kv-server -p crowdb-diskdb -p crowdb-chunkdb
pixi run build-cpp
mkdir -p "$LOG_ROOT" "$(dirname "$RESULTS_FILE")"
printf 'case\trequested\tsize_bytes\tsize_mib\tconcurrency\tcompleted\terrors\tincomplete\tstop\tobjects_s\tlogical_mib_s\tphysical_mib_s\tp50_us\tp99_us\tmem_bw_avg_mib\tmem_bw_max_mib\n' >"$RESULTS_FILE"

run_case large_1t 2 67108864 1
run_case large_4t 8 67108864 4

echo "=== DONE ==="
echo "Logs and results retained in $LOG_ROOT"
column -t -s$'\t' "$RESULTS_FILE"
if [ "$FAILURES" -ne 0 ]; then
    echo "ERROR: $FAILURES regression case(s) failed" >&2
    exit 1
fi
