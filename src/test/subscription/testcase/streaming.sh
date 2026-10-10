#!/bin/bash
source $1/env_utils.sh $1 $2 $3
case_db="streaming_db"
function check_count() {
    expected=$1
    actual=$(exec_sql $case_db $sub_node1_port "SELECT count(*) FROM stream_test")
    if [ "$actual" != "$expected" ]; then echo "$failed_keyword expected $expected rows, got $actual"; exit 1; fi
}
function setup() {
    restart_guc pub_datanode1 "logical_decoding_work_mem=64"
    exec_sql $db $pub_node1_port "CREATE DATABASE $case_db"
    exec_sql $db $sub_node1_port "CREATE DATABASE $case_db"
    exec_sql $case_db $pub_node1_port "CREATE TABLE stream_test(id int PRIMARY KEY, payload text)"
    exec_sql $case_db $sub_node1_port "CREATE TABLE stream_test(id int PRIMARY KEY, payload text)"
    exec_sql $case_db $pub_node1_port "CREATE TABLE stream_skip_conflict(id int PRIMARY KEY)"
    exec_sql $case_db $sub_node1_port "CREATE TABLE stream_skip_conflict(id int PRIMARY KEY)"
    exec_sql $case_db $sub_node1_port "INSERT INTO stream_skip_conflict VALUES (1)"
    exec_sql $case_db $pub_node1_port "CREATE PUBLICATION stream_pub FOR TABLE stream_test, stream_skip_conflict WITH (ddl='table')"
    publisher_connstr="port=$pub_node1_port host=$g_local_ip dbname=$case_db user=$username password=$passwd"
    exec_sql $case_db $sub_node1_port "CREATE SUBSCRIPTION stream_sub CONNECTION '$publisher_connstr' PUBLICATION stream_pub WITH (copy_data=false, streaming=true)"
    wait_for_subscription_sync $case_db $sub_node1_port
}
function test_stream_commit() {
    exec_sql $case_db $pub_node1_port "INSERT INTO stream_test SELECT i, repeat('commit', 40) FROM generate_series(1, 3000) i"
    wait_for_catchup $case_db $pub_node1_port stream_sub
    check_count 3000
}
function test_stream_abort() {
    exec_sql $case_db $pub_node1_port "BEGIN; INSERT INTO stream_test SELECT i, repeat('abort', 40) FROM generate_series(3001, 6000) i; ROLLBACK"
    sleep 2
    check_count 3000
}
function test_subtransaction_abort() {
    exec_sql $case_db $pub_node1_port "BEGIN;
         INSERT INTO stream_test SELECT i, repeat('before', 40)
         FROM generate_series(6001, 7500) i;
         SAVEPOINT s1;
         ALTER TABLE stream_test ADD COLUMN rollback_column int;
         INSERT INTO stream_test SELECT i, repeat('rollback', 40)
         FROM generate_series(7501, 9000) i;
         ROLLBACK TO s1;
         INSERT INTO stream_test SELECT i, repeat('after', 40)
         FROM generate_series(9001, 10000) i;
         COMMIT"
    wait_for_catchup $case_db $pub_node1_port stream_sub
    check_count 5500
    rollback_column_count=$(exec_sql $case_db $sub_node1_port "SELECT count(*) FROM information_schema.columns
         WHERE table_schema = 'public' AND table_name = 'stream_test' AND column_name = 'rollback_column'")
    if [ "$rollback_column_count" != "0" ]; then echo "$failed_keyword rolled back streaming DDL was applied on subscriber"; exit 1; fi
}
function test_mixed_unpublished_ddl() {
    exec_sql $case_db $pub_node1_port "BEGIN;
         INSERT INTO stream_test SELECT i, repeat('ddl', 40)
         FROM generate_series(10001, 11500) i;
         CREATE TABLE stream_ddl_not_published(id int);
         INSERT INTO stream_test SELECT i, repeat('ddl-after', 40)
         FROM generate_series(11501, 12500) i;
         COMMIT"
    wait_for_catchup $case_db $pub_node1_port stream_sub
    check_count 8000
}
function test_stream_truncate() {
    exec_sql $case_db $pub_node1_port "BEGIN;
         TRUNCATE stream_test;
         INSERT INTO stream_test SELECT i, repeat('after truncate', 40)
         FROM generate_series(1, 1500) i;
         COMMIT"
    wait_for_catchup $case_db $pub_node1_port stream_sub
    check_count 1500
}
function test_mixed_published_ddl() {
    exec_sql $case_db $pub_node1_port "BEGIN;
         INSERT INTO stream_test SELECT i, repeat('before-ddl', 40)
         FROM generate_series(12501, 14000) i;
         ALTER TABLE stream_test ADD COLUMN ddl_value int;
         INSERT INTO stream_test(id, payload, ddl_value) SELECT i, repeat('after-ddl', 40), 1
         FROM generate_series(14001, 15000) i;
         COMMIT"
    wait_for_catchup $case_db $pub_node1_port stream_sub
    check_count 10500
    ddl_value_count=$(exec_sql $case_db $sub_node1_port "SELECT count(ddl_value) FROM stream_test")
    if [ "$ddl_value_count" != "1000" ]; then echo "$failed_keyword expected 1000 rows written with the post-DDL schema, got $ddl_value_count"; exit 1; fi
}
function test_reused_subtransaction_state() {
    exec_sql $case_db $pub_node1_port "BEGIN;
         INSERT INTO stream_test SELECT i, repeat('reuse-one', 40)
         FROM generate_series(16001, 16500) i;
         SAVEPOINT s1;
         INSERT INTO stream_test SELECT i, repeat('reuse-one-subxact', 40)
         FROM generate_series(16501, 17000) i;
         RELEASE SAVEPOINT s1;
         COMMIT"
    wait_for_catchup $case_db $pub_node1_port stream_sub
    check_count 11500
    exec_sql $case_db $pub_node1_port "BEGIN;
         INSERT INTO stream_test SELECT i, repeat('reuse-two', 40)
         FROM generate_series(17001, 17500) i;
         SAVEPOINT s2;
         INSERT INTO stream_test SELECT i, repeat('reuse-two-subxact', 40)
         FROM generate_series(17501, 18000) i;
         RELEASE SAVEPOINT s2;
         COMMIT"
    wait_for_catchup $case_db $pub_node1_port stream_sub
    check_count 12500
}
function test_stream_skiplsn() {
    logfile=$(get_log_file "sub_datanode1")
    location=$(awk 'END{print NR}' $logfile)
    exec_sql $case_db $pub_node1_port "BEGIN;
         ALTER TABLE stream_test ADD COLUMN skipped_ddl int;
         TRUNCATE stream_test;
         INSERT INTO stream_skip_conflict VALUES (1);
         COMMIT"
    commitlsn=""
    retry=0
    while [ -z "$commitlsn" ] && [ $retry -lt 60 ]; do
        commitlsn=$(tail -n +$location $logfile | grep -o 'commit_lsn: [0-9A-F/]\+' | tail -1 | awk '{print $2}')
        retry=$((retry + 1))
        sleep 1
    done
    if [ -z "$commitlsn" ]; then echo "$failed_keyword could not find commit LSN for streaming skip test"; exit 1; fi
    exec_sql $case_db $sub_node1_port "ALTER SUBSCRIPTION stream_sub SET (skiplsn = '$commitlsn')"
    wait_for_catchup $case_db $pub_node1_port stream_sub
    check_count 1500
    skipped_ddl_count=$(exec_sql $case_db $sub_node1_port "SELECT count(*) FROM information_schema.columns
         WHERE table_schema = 'public' AND table_name = 'stream_test' AND column_name = 'skipped_ddl'")
    if [ "$skipped_ddl_count" != "0" ]; then echo "$failed_keyword skipped streaming DDL was applied on subscriber"; exit 1; fi
}
function test_stream_error_recovery() {
    logfile=$(get_log_file "sub_datanode1")
    location=$(awk 'END{print NR}' $logfile)
    exec_sql $case_db $sub_node1_port "INSERT INTO stream_test(id, payload) VALUES (19001, 'conflict')"
    exec_sql $case_db $pub_node1_port "INSERT INTO stream_test(id, payload) SELECT i, repeat('retry', 40) FROM generate_series(18001, 21000) i"
    retry=0
    while [ $retry -lt 30 ]; do
        if tail -n +$location $logfile | grep -q "duplicate key"; then
            break
        fi
        retry=$((retry + 1))
        sleep 1
    done
    if [ $retry -eq 30 ]; then echo "$failed_keyword streamed transaction did not reach the expected apply error"; exit 1; fi
    exec_sql $case_db $sub_node1_port "DELETE FROM stream_test WHERE id = 19001"
    wait_for_catchup $case_db $pub_node1_port stream_sub
    check_count 15500
    if tail -n +$location $logfile | grep -qE "nested stream start message|duplicate first segment"; then
        echo "$failed_keyword stale streaming state prevented apply worker recovery"; exit 1; fi
}
function tear_down() {
    exec_sql $case_db $sub_node1_port "DROP SUBSCRIPTION IF EXISTS stream_sub"
    exec_sql $case_db $pub_node1_port "DROP PUBLICATION IF EXISTS stream_pub"
    exec_sql $db $sub_node1_port "DROP DATABASE $case_db"
    exec_sql $db $pub_node1_port "DROP DATABASE $case_db"
    restart_guc pub_datanode1 "logical_decoding_work_mem=65536"
}
setup
test_stream_commit
test_stream_abort
test_subtransaction_abort
test_mixed_unpublished_ddl
test_mixed_published_ddl
test_reused_subtransaction_state
test_stream_error_recovery
test_stream_truncate
test_stream_skiplsn
tear_down
