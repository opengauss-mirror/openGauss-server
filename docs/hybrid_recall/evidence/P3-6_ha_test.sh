#!/usr/bin/env bash
# P3-6: isolated single-node primary/standby read and failover/retry test.
#
# This script never removes an existing path.  It clones a *stopped* fixture
# PGDATA into a new run directory, leaves all evidence there, and stops the
# nodes on exit.  Local TCP sockets are required by physical replication, so
# run it outside a network-restricted sandbox.
set -euo pipefail

WORKSPACE=${P36_WORKSPACE:-/home/qiumc/data/code/cpp}
GAUSSHOME=${P36_GAUSSHOME:-$WORKSPACE/openGauss-p35-release/mppdb_temp_install}
TEMPLATE_PGDATA=${P36_TEMPLATE_PGDATA:-$WORKSPACE/openGauss-p35-release/p35_release_data}
RUN_ID=${P36_RUN_ID:-$(date +%Y%m%d_%H%M%S)_$$}
BASE_DIR=${P36_BASE_DIR:-$WORKSPACE/.runtime/hybrid_recall_p36_$RUN_ID}

PRIMARY_PORT=${P36_PRIMARY_PORT:-55740}
STANDBY_PORT=${P36_STANDBY_PORT:-55750}
PRIMARY_REPL_PORT=${P36_PRIMARY_REPL_PORT:-55742}
STANDBY_REPL_PORT=${P36_STANDBY_REPL_PORT:-55752}
DB_NAME=${P36_DB_NAME:-hybrid_recall_p35_release_20260826}
DB_USER=${P36_DB_USER:-qiumc}
TASK_NAME=${P36_TASK_NAME:-p35_release_task}
TABLE_NAME=${P36_TABLE_NAME:-p35_docs}
EXPECTED_IDS=${P36_EXPECTED_IDS:-245,165,25}

TOOLCHAIN_LIBS=${P36_TOOLCHAIN_LIBS:-$WORKSPACE/binarylibs/buildtools/gcc10.3/gcc/lib64:$WORKSPACE/binarylibs/buildtools/gcc10.3/isl/lib:$WORKSPACE/binarylibs/buildtools/gcc10.3/mpc/lib:$WORKSPACE/binarylibs/buildtools/gcc10.3/mpfr/lib:$WORKSPACE/binarylibs/buildtools/gcc10.3/gmp/lib}
export GAUSSHOME
export LD_LIBRARY_PATH="$GAUSSHOME/lib:$TOOLCHAIN_LIBS"

GS_CTL=$GAUSSHOME/bin/gs_ctl
GS_GUC=$GAUSSHOME/bin/gs_guc
GSQL=$GAUSSHOME/bin/gsql
PRIMARY_DATA=$BASE_DIR/primary
STANDBY_DATA=$BASE_DIR/standby
LOG_DIR=$BASE_DIR/logs

fail()
{
    echo "P3-6 FAIL: $*" >&2
    exit 1
}

for tool in "$GS_CTL" "$GS_GUC" "$GSQL"; do
    [[ -x "$tool" ]] || fail "missing executable: $tool"
done
[[ -f "$TEMPLATE_PGDATA/PG_VERSION" ]] || fail "invalid fixture PGDATA: $TEMPLATE_PGDATA"
if "$GS_CTL" status -D "$TEMPLATE_PGDATA" >/dev/null 2>&1; then
    fail "fixture PGDATA must be stopped before cloning: $TEMPLATE_PGDATA"
fi
[[ ! -e "$BASE_DIR" ]] || fail "refusing to overwrite existing run directory: $BASE_DIR"

mkdir -p "$PRIMARY_DATA" "$STANDBY_DATA" "$LOG_DIR"
cp -a --reflink=auto "$TEMPLATE_PGDATA/." "$PRIMARY_DATA/"
cp -a --reflink=auto "$TEMPLATE_PGDATA/." "$STANDBY_DATA/"

set_guc()
{
    local data_dir=$1
    local setting=$2
    "$GS_GUC" set -D "$data_dir" -c "$setting" >>"$LOG_DIR/gs_guc.log" 2>&1
}

set_guc "$PRIMARY_DATA" "port = $PRIMARY_PORT"
set_guc "$PRIMARY_DATA" "replconninfo1 = 'localhost=127.0.0.1 localport=$PRIMARY_REPL_PORT remotehost=127.0.0.1 remoteport=$STANDBY_REPL_PORT'"
set_guc "$PRIMARY_DATA" "most_available_sync = on"
set_guc "$PRIMARY_DATA" "synchronous_commit = on"
set_guc "$STANDBY_DATA" "port = $STANDBY_PORT"
set_guc "$STANDBY_DATA" "replconninfo1 = 'localhost=127.0.0.1 localport=$STANDBY_REPL_PORT remotehost=127.0.0.1 remoteport=$PRIMARY_REPL_PORT'"
set_guc "$STANDBY_DATA" "most_available_sync = on"
set_guc "$STANDBY_DATA" "synchronous_commit = on"

cleanup()
{
    local data_dir
    for data_dir in "$PRIMARY_DATA" "$STANDBY_DATA"; do
        if "$GS_CTL" status -D "$data_dir" >/dev/null 2>&1; then
            "$GS_CTL" stop -w -t 60 -D "$data_dir" -m fast >/dev/null 2>&1 ||
                "$GS_CTL" stop -w -t 30 -D "$data_dir" -m immediate >/dev/null 2>&1 || true
        fi
    done
}
trap cleanup EXIT

"$GS_CTL" start -w -t 90 -D "$PRIMARY_DATA" -l "$LOG_DIR/primary_start.log" \
    -o "--single_node" -M primary >"$LOG_DIR/primary_start.out" 2>&1
"$GS_CTL" start -w -t 90 -D "$STANDBY_DATA" -l "$LOG_DIR/standby_start.log" \
    -o "--single_node" -M standby >"$LOG_DIR/standby_start.out" 2>&1

streaming=false
for ((attempt = 1; attempt <= 60; attempt++)); do
    "$GS_CTL" query -D "$PRIMARY_DATA" >"$LOG_DIR/pre_primary_ha.out" 2>&1 || true
    "$GS_CTL" query -D "$STANDBY_DATA" >"$LOG_DIR/pre_standby_ha.out" 2>&1 || true
    if grep -q "local_role.*Primary" "$LOG_DIR/pre_primary_ha.out" &&
        grep -q "state.*Streaming" "$LOG_DIR/pre_primary_ha.out" &&
        grep -q "sync_percent.*100%" "$LOG_DIR/pre_primary_ha.out" &&
        grep -q "local_role.*Standby" "$LOG_DIR/pre_standby_ha.out"; then
        streaming=true
        break
    fi
    sleep 1
done
[[ "$streaming" == true ]] || fail "primary/standby did not reach synchronous streaming state"

OPTIONS='{"text_candidates":100,"vector_candidates":100,"rrf_k":60,"weights":[0.5,0.5],"query_vector":[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8],"filters":{"tenant_id":1}}'
SESSION_SQL="SET search_path=ogai,public; SET enable_hybrid_recall=on; SET hybrid_recall_parallel_mode=force;"
HYBRID_SQL="SELECT (result_record->>'id')::int FROM ogai.hybrid_search_v2('$TASK_NAME','database','id,title',3,'','$OPTIONS'::jsonb);"

run_snapshot()
{
    local port=$1
    local output=$2
    "$GSQL" -X -p "$port" -U "$DB_USER" -d "$DB_NAME" -tA -v ON_ERROR_STOP=1 \
        -c "$SESSION_SQL SELECT pg_is_in_recovery()::text || '|' || current_setting('transaction_read_only'); $HYBRID_SQL" \
        >"$output" 2>&1
}

extract_ids()
{
    awk '/^[0-9]+$/{if (seen++) printf ","; printf "%s", $0} END{print ""}' "$1"
}

run_snapshot "$PRIMARY_PORT" "$LOG_DIR/pre_primary_query.out"
run_snapshot "$STANDBY_PORT" "$LOG_DIR/pre_standby_query.out"
grep -q '^false|off$' "$LOG_DIR/pre_primary_query.out" || fail "old primary role/read-only state mismatch"
grep -q '^true|on$' "$LOG_DIR/pre_standby_query.out" || fail "standby role/read-only state mismatch"
[[ "$(extract_ids "$LOG_DIR/pre_primary_query.out")" == "$EXPECTED_IDS" ]] || fail "old primary IDs mismatch"
[[ "$(extract_ids "$LOG_DIR/pre_standby_query.out")" == "$EXPECTED_IDS" ]] || fail "standby IDs mismatch"

# Keep the statement active after HybridRecall materializes but before its first
# result row is delivered.  On connection failure the client must discard the
# entire statement and retry it against the promoted primary.
"$GSQL" -X -p "$PRIMARY_PORT" -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 \
    -c "$SESSION_SQL SELECT pg_sleep(30), result_record FROM ogai.hybrid_search_v2('$TASK_NAME','database','id,title',3,'','$OPTIONS'::jsonb) /* P36_FAILOVER_INFLIGHT */;" \
    >"$LOG_DIR/inflight_client.out" 2>&1 &
inflight_client_pid=$!

active=false
for ((attempt = 1; attempt <= 50; attempt++)); do
    active_count=$("$GSQL" -X -p "$PRIMARY_PORT" -U "$DB_USER" -d "$DB_NAME" -tA -v ON_ERROR_STOP=1 \
        -c "SELECT count(*) FROM pg_stat_activity WHERE query LIKE '%P36_FAILOVER_INFLIGHT%' AND pid <> pg_backend_pid();" 2>/dev/null || true)
    if [[ "$active_count" == 1 ]]; then
        active=true
        break
    fi
    sleep 0.2
done
[[ "$active" == true ]] || fail "fault-injection query did not become active"

"$GS_CTL" stop -w -t 30 -D "$PRIMARY_DATA" -m immediate >"$LOG_DIR/old_primary_immediate_stop.out" 2>&1
set +e
wait "$inflight_client_pid"
inflight_rc=$?
set -e
[[ $inflight_rc -ne 0 ]] || fail "in-flight statement unexpectedly succeeded"
grep -q "connection to server was lost" "$LOG_DIR/inflight_client.out" || fail "missing client connection-loss error"
if grep -q '"id"' "$LOG_DIR/inflight_client.out"; then
    fail "in-flight client received a result row before reporting connection loss"
fi

"$GS_CTL" failover -w -t 60 -D "$STANDBY_DATA" >"$LOG_DIR/failover.out" 2>&1
"$GS_CTL" query -D "$STANDBY_DATA" >"$LOG_DIR/post_failover_ha.out" 2>&1
grep -q "local_role.*Primary" "$LOG_DIR/post_failover_ha.out" || fail "standby was not promoted"
grep -q "db_state.*Normal" "$LOG_DIR/post_failover_ha.out" || fail "promoted primary is not Normal"

run_snapshot "$STANDBY_PORT" "$LOG_DIR/post_failover_query.out"
grep -q '^false|off$' "$LOG_DIR/post_failover_query.out" || fail "promoted primary role/read-only state mismatch"
[[ "$(extract_ids "$LOG_DIR/post_failover_query.out")" == "$EXPECTED_IDS" ]] || fail "post-failover retry IDs mismatch"

if "$GSQL" -X -p "$PRIMARY_PORT" -U "$DB_USER" -d "$DB_NAME" -c "SELECT 1;" \
    >"$LOG_DIR/old_primary_connect.out" 2>&1; then
    fail "old primary unexpectedly remained connectable"
fi

for ((retry = 1; retry <= 20; retry++)); do
    run_snapshot "$STANDBY_PORT" "$LOG_DIR/post_retry_$retry.out"
    [[ "$(extract_ids "$LOG_DIR/post_retry_$retry.out")" == "$EXPECTED_IDS" ]] ||
        fail "post-failover retry $retry IDs mismatch"
done

EXPLAIN_OPTIONS='{"text_candidates":100,"vector_candidates":100,"rrf_k":60,"weights":[0.5,0.5]}'
"$GSQL" -X -p "$STANDBY_PORT" -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 \
    -c "$SESSION_SQL EXPLAIN (ANALYZE,COSTS OFF,TIMING ON) SELECT id,ogai.hybrid_rrf_marker(id,content,ogai_embedding,'database','[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,'$EXPLAIN_OPTIONS'::jsonb) AS score FROM public.$TABLE_NAME WHERE tenant_id=1 ORDER BY score DESC LIMIT 3;" \
    >"$LOG_DIR/post_failover_explain.out" 2>&1
grep -q "Strategy: Parallel RRF" "$LOG_DIR/post_failover_explain.out" || fail "post-failover plan is not parallel RRF"
grep -q "Actual Branch Workers: 2" "$LOG_DIR/post_failover_explain.out" || fail "post-failover plan did not start two workers"
grep -q "Parallel Fallback: false" "$LOG_DIR/post_failover_explain.out" || fail "post-failover plan unexpectedly fell back"

if grep -R -E -i "failed assertion|PANIC:|segmentation fault|addresssanitizer|double free|use-after-free" \
    "$LOG_DIR" "$PRIMARY_DATA/pg_log" "$STANDBY_DATA/pg_log" >"$LOG_DIR/severe_scan.out" 2>&1; then
    fail "severe server log entry found; inspect $LOG_DIR/severe_scan.out"
fi

echo "P3-6 PASS: initial primary/standby IDs=$EXPECTED_IDS; old connection failed before first row; promoted-primary retries=20"
echo "P3-6 evidence directory: $BASE_DIR"
