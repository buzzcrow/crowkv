#!/usr/bin/env bash
# CROWDB end-to-end large chunk-write regression using three NullDisk nodes.
#
# Optional environment variables:
#   CHUNKIO_BENCH_CASES       space-separated case labels
#   CHUNKIO_BENCH_LOG_ROOT    retained run root
#   CHUNKIO_BENCH_RESULTS     result TSV path
#   CHUNKIO_BENCH_TIMEOUT     seconds allowed per case (default: 120)
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
    find "$CURRENT_LOG_ROOT" -type f -name '*metrics-*.log' -print0 |
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
    [ "$kv" -ge 3 ] && [ "$diskdb" -ge 3 ] && [ "$chunkdb" -ge 3 ] \
        && [ "$diskio" -ge 3 ] && [ "$cli_metrics" -ge 1 ]
}

run_case() {
    local label="$1" objects="$2" object_size="$3" concurrency="$4"
    if [ -n "$CASES" ] && [[ " $CASES " != *" $label "* ]]; then
        return
    fi
    CURRENT_LOG_ROOT="$LOG_ROOT/$label"
    CURRENT_CONFIG="$CURRENT_LOG_ROOT/console.toml"
    echo ">>> $label (objects=$objects size=$object_size concurrency=$concurrency EC=4+1)"
    cli cluster local-deploy -t combined --metrics-interval 1 --allow-unsafe-ec

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
    line=$(sed -n '/^chunkio write:/p' <<<"$output" | tail -n 1)
    avg_bw=$(memory_bandwidth avg)
    max_bw=$(memory_bandwidth max)
    if [ -z "$line" ]; then
        printf '%s\t%s\t%s\t%s\t0\t1\t0\t0\t0\t0\t%s\t%s\n' \
            "$label" "$objects" "$object_size" "$concurrency" "$avg_bw" "$max_bw" >>"$RESULTS_FILE"
    else
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
            "$label" "$objects" "$object_size" "$concurrency" \
            "$(field "$line" objects)" "$(field "$line" errors)" \
            "$(field "$line" logical_mib_s)" "$(field "$line" physical_mib_s)" \
            "$(field "$line" p50_us)" "$(field "$line" p99_us)" \
            "$avg_bw" "$max_bw" >>"$RESULTS_FILE"
    fi
    if [ "$status" -ne 0 ] || ! verify_logs; then
        echo "ERROR: $label failed or did not retain all service metrics" >&2
        FAILURES=$((FAILURES + 1))
    fi
    destroy_cluster
}

echo "=== building release binaries ==="
pixi run -- cargo build --release -p crowdb-cli -p crowdb-kv-server -p crowdb-diskdb -p crowdb-chunkdb
pixi run build-cpp
mkdir -p "$LOG_ROOT" "$(dirname "$RESULTS_FILE")"
printf 'case\trequested\tsize_bytes\tconcurrency\tcompleted\terrors\tlogical_mib_s\tphysical_mib_s\tp50_us\tp99_us\tmem_bw_avg_mib\tmem_bw_max_mib\n' >"$RESULTS_FILE"

run_case large_1t 2 67108864 1
run_case large_4t 8 67108864 4

echo "=== DONE ==="
echo "Logs and results retained in $LOG_ROOT"
column -t -s$'\t' "$RESULTS_FILE"
if [ "$FAILURES" -ne 0 ]; then
    echo "ERROR: $FAILURES regression case(s) failed" >&2
    exit 1
fi
