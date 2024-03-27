#!/bin/sh

source $1/env_utils.sh $1 $2

case_db="bugs_db"

function test_1() {
    echo "create database and tables."
    exec_sql $db $pub_node1_port "CREATE DATABASE $case_db"
    exec_sql $db $sub_node1_port "CREATE DATABASE $case_db"

    # BUG1: coredump when apply null value into not null column.
    # Create some preexisting content on publisher
    exec_sql $case_db $pub_node1_port "CREATE TABLE tab_rep (a int primary key, b text)"
    exec_sql $case_db $pub_node1_port "INSERT INTO tab_rep VALUES (1)"

    # Setup structure on subscriber
    exec_sql $case_db $sub_node1_port "CREATE TABLE tab_rep (a int primary key, b text not null default 0)"

    # Setup logical replication
    echo "create publication and subscription."
    publisher_connstr="port=$pub_node1_port host=$g_local_ip dbname=$case_db user=$username password=$passwd"
    exec_sql $case_db $pub_node1_port "CREATE PUBLICATION tap_pub FOR ALL TABLES"
    exec_sql $case_db $sub_node1_port "CREATE SUBSCRIPTION tap_sub CONNECTION '$publisher_connstr' PUBLICATION tap_pub"

    logfile=$(get_log_file "sub_datanode1")

    location=$(awk 'END{print NR}' $logfile)

    content=$(tail -n +$location $logfile)
    targetstr=$(expr "$content" : '.*\(Failing row contains\).*')

    attempt=0
    while [ -z "$targetstr" ]
    do
        content=$(tail -n +$location $logfile)
        targetstr=$(expr "$content" : '.*\(Failing row contains\).*')
        attempt=`expr $attempt \+ 1`

        sleep 1
        if [ $attempt -eq 5 ]; then
            echo "$failed_keyword when check failing row log"
            exit 1
        fi
    done

    echo "check failing row log success"

    exec_sql $case_db $sub_node1_port "DROP SUBSCRIPTION IF EXISTS tap_sub;TRUNCATE TABLE tab_rep"
    exec_sql $case_db $pub_node1_port "DROP PUBLICATION IF EXISTS tap_pub;TRUNCATE TABLE tab_rep"

    # BUG2: skiplsn does not work occasionally
    # Setup logical replication
    echo "create publication and subscription."
    publisher_connstr="port=$pub_node1_port host=$g_local_ip dbname=$case_db user=$username password=$passwd"
    exec_sql $case_db $pub_node1_port "CREATE PUBLICATION tap_pub FOR ALL TABLES"
    exec_sql $case_db $sub_node1_port "CREATE SUBSCRIPTION tap_sub CONNECTION '$publisher_connstr' PUBLICATION tap_pub with (copy_data=false); ALTER SUBSCRIPTION tap_sub DISABLE"

    exec_sql $case_db $pub_node1_port "INSERT INTO tab_rep VALUES (1, 'pub1'); INSERT INTO tab_rep VALUES (2, 'sub2');"
    dumpfile=$(exec_sql $case_db $pub_node1_port "select gs_xlogdump_xid(xmin) from tab_rep where a = 1;")
    skiplsn=$(grep 'start_lsn' $dumpfile | sed -n '5p' | awk '{print $2}')

    exec_sql $case_db $sub_node1_port "alter subscription tap_sub set (skiplsn = '$skiplsn')"
    exec_sql $case_db $sub_node1_port "alter subscription tap_sub enable"

    wait_for_catchup $case_db $pub_node1_port "tap_sub"

    if [ "$(exec_sql $case_db $sub_node1_port "SELECT * FROM tab_rep")" = "2|sub2" ]; then
        echo "check data skip success"
    else
        echo "$failed_keyword when check data skip"
        exit 1
    fi

    exec_sql $case_db $sub_node1_port "DROP SUBSCRIPTION IF EXISTS tap_sub;DROP TABLE tab_rep"
    exec_sql $case_db $pub_node1_port "DROP PUBLICATION IF EXISTS tap_pub;DROP TABLE tab_rep"

    # BUG3: partition relation not closed when handle conflict
    # Create partition table
    ddl="
create table t_pubsub_0349(
  id int primary key constraint id_nn not null,
  use_filename varchar(20),
  filename varchar2(255)
)partition by range(id)(
  partition p1 values less than(30),
  partition p2 values less than(60),
  partition p3 values less than(90),
  partition p4 values less than(maxvalue));"
    exec_sql $case_db $pub_node1_port "$ddl"
    exec_sql $case_db $sub_node1_port "$ddl"

    echo "create publication and subscription."
    publisher_connstr="port=$pub_node1_port host=$g_local_ip dbname=$case_db user=$username password=$passwd"
    exec_sql $case_db $pub_node1_port "CREATE PUBLICATION tap_pub FOR ALL TABLES"
    exec_sql $case_db $sub_node1_port "CREATE SUBSCRIPTION tap_sub CONNECTION '$publisher_connstr' PUBLICATION tap_pub"

    wait_for_subscription_sync $case_db $sub_node1_port

    exec_sql $case_db $sub_node1_port "ALTER SYSTEM SET subscription_conflict_resolution = apply_remote"

    exec_sql $case_db $sub_node1_port "INSERT INTO t_pubsub_0349 VALUES (1, 'a', 'a');"
    exec_sql $case_db $pub_node1_port "INSERT INTO t_pubsub_0349 VALUES (1, 'a', 'c');"

    wait_for_catchup $case_db $pub_node1_port "tap_sub"

    if [ "$(exec_sql $case_db $sub_node1_port "SELECT * FROM t_pubsub_0349")" = "1|a|c" ]; then
        echo "check insert conflict handle success"
    else
        echo "$failed_keyword when check insert conflict handle"
        exit 1
    fi

    exec_sql $case_db $sub_node1_port "INSERT INTO t_pubsub_0349 VALUES (2, 'a', 'a');"
    exec_sql $case_db $pub_node1_port "UPDATE t_pubsub_0349 SET id = 2 WHERE id = 1;"

    wait_for_catchup $case_db $pub_node1_port "tap_sub"

    if [ "$(exec_sql $case_db $sub_node1_port "SELECT * FROM t_pubsub_0349")" = "2|a|c" ]; then
        echo "check update conflict handle success"
    else
        echo "$failed_keyword when check update conflict handle"
        exit 1
    fi

    logfile=$(get_log_file "sub_datanode1")
    leakstr=$(grep 'partcache reference leak' $logfile -m 1)
    if [ -z "$leakstr" ]; then
        echo "check relation close success"
    else
        echo "$failed_keyword when check relation close"
        exit 1
    fi

    exec_sql $case_db $sub_node1_port "ALTER SYSTEM SET subscription_conflict_resolution = error"

    exec_sql $case_db $sub_node1_port "DROP SUBSCRIPTION IF EXISTS tap_sub;DROP TABLE t_pubsub_0349"
    exec_sql $case_db $pub_node1_port "DROP PUBLICATION IF EXISTS tap_pub;DROP TABLE t_pubsub_0349"

    # BUG4: fix pg_replication_origin_status remain
    exec_sql $case_db $pub_node1_port "create table tab_rep (a int primary key, b int); insert into tab_rep values (1,1)"
    exec_sql $case_db $sub_node1_port "create table tab_rep (a int primary key, b int); insert into tab_rep values (1,1)"

    echo "create publication and subscription."
    publisher_connstr="port=$pub_node1_port host=$g_local_ip dbname=$case_db user=$username password=$passwd"
    exec_sql $case_db $pub_node1_port "CREATE PUBLICATION tap_pub FOR ALL TABLES"
    exec_sql $case_db $sub_node1_port "CREATE SUBSCRIPTION tap_sub CONNECTION '$publisher_connstr' PUBLICATION tap_pub"

    exec_sql $case_db $sub_node1_port "DROP SUBSCRIPTION tap_sub;"

    if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*) FROM pg_replication_origin_status")" = "0" ]; then
        echo "check if pg_replication_origin_status is empty success"
    else
        echo "$failed_keyword when check if pg_replication_origin_status is empty"
        exit 1
    fi

    # BUG5: fix IUD not record mlog
    exec_sql $case_db $sub_node1_port "DROP SUBSCRIPTION IF EXISTS tap_sub;DROP TABLE tab_rep"
    exec_sql $case_db $pub_node1_port "DROP PUBLICATION IF EXISTS tap_pub;DROP TABLE tab_rep"

    exec_sql $case_db $pub_node1_port "create table tab_rep (a int primary key, b int);"
    exec_sql $case_db $sub_node1_port "create table tab_rep (a int primary key, b int);"

    exec_sql $case_db $sub_node1_port "create INCREMENTAL MATERIALIZED VIEW test_mv1 as select * from tab_rep;"

    echo "create publication and subscription."
    publisher_connstr="port=$pub_node1_port host=$g_local_ip dbname=$case_db user=$username password=$passwd"
    exec_sql $case_db $pub_node1_port "CREATE PUBLICATION tap_pub FOR ALL TABLES"
    exec_sql $case_db $sub_node1_port "CREATE SUBSCRIPTION tap_sub CONNECTION '$publisher_connstr' PUBLICATION tap_pub"

    wait_for_subscription_sync $case_db $sub_node1_port

    exec_sql $case_db $pub_node1_port "insert into tab_rep values (1, 1);"
    wait_for_catchup $case_db $pub_node1_port "tap_sub"

    exec_sql $case_db $sub_node1_port "REFRESH INCREMENTAL MATERIALIZED VIEW test_mv1;"
    if [ "$(exec_sql $case_db $sub_node1_port "SELECT * FROM test_mv1")" = "1|1" ]; then
        echo "check if insert into incremental mview success"
    else
        echo "$failed_keyword when check if insert into incremental mview"
        exit 1
    fi

    exec_sql $case_db $pub_node1_port "update tab_rep set b = 2 where a = 1;"
    wait_for_catchup $case_db $pub_node1_port "tap_sub"

    exec_sql $case_db $sub_node1_port "REFRESH INCREMENTAL MATERIALIZED VIEW test_mv1;"
    if [ "$(exec_sql $case_db $sub_node1_port "SELECT * FROM test_mv1")" = "1|2" ]; then
        echo "check if update into incremental mview success"
    else
        echo "$failed_keyword when check if update incremental mview"
        exit 1
    fi

    exec_sql $case_db $pub_node1_port "delete from tab_rep where a = 1;"
    wait_for_catchup $case_db $pub_node1_port "tap_sub"

    exec_sql $case_db $sub_node1_port "REFRESH INCREMENTAL MATERIALIZED VIEW test_mv1;"
    if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*) FROM test_mv1")" = "0" ]; then
        echo "check if delete incremental mview success"
    else
        echo "$failed_keyword when check if delete incremental mview"
        exit 1
    fi

    # BUG6: fix sync insert-data during splitting partition
    exec_sql $case_db $sub_node1_port "DROP SUBSCRIPTION IF EXISTS tap_sub;DROP TABLE tab_rep cascade"
    exec_sql $case_db $pub_node1_port "DROP PUBLICATION IF EXISTS tap_pub;DROP TABLE tab_rep cascade"

    echo "create publication and subscription."
    publisher_connstr="port=$pub_node1_port host=$g_local_ip dbname=$case_db user=$username password=$passwd"
    exec_sql $case_db $pub_node1_port "CREATE PUBLICATION tap_pub FOR ALL TABLES with(publish='insert,update,delete',ddl='all')"
    exec_sql $case_db $sub_node1_port "CREATE SUBSCRIPTION tap_sub CONNECTION '$publisher_connstr' PUBLICATION tap_pub"

    wait_for_subscription_sync $case_db $sub_node1_port

    ddl="create table testTab1
(id number(6,3) not null,
hire_date date not null)
partition by range(hire_date)
(partition p_1998 values less than (to_date('1998-12-31','YYYY-MM-DD')),
partition p_1999 values less than (to_date('1999-12-31','YYYY-MM-DD')),
partition p_default values less than (maxvalue)
);"
    exec_sql $case_db $pub_node1_port "$ddl"
    exec_sql $case_db $pub_node1_port "insert into testTab1 values(30,to_date('1999-5-27','YYYY-MM-DD'));"
    exec_sql $case_db $pub_node1_port "insert into testTab1 values(60.3,to_date('2000-5-27','YYYY-MM-DD'));"
    exec_sql $case_db $pub_node1_port "insert into testTab1 values(403.3,to_date('2001-10-02','YYYY-MM-DD'));"

    exec_sql $case_db $pub_node1_port "ALTER TABLE testTab1 SPLIT PARTITION p_default AT (to_date('2000-12-31','YYYY-MM-DD')) INTO (PARTITION p_2000, PARTITION p_other);"
    wait_for_catchup $case_db $pub_node1_port "tap_sub"

    if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*) FROM testTab1")" = "3" ]; then
        echo "check if not sync insert-data during splitting partition success"
    else
        echo "$failed_keyword when check if not sync insert-data during splitting partition"
        exit 1
    fi

    # BUG7: fix redo ddl logical message core when recovery_max_workers > 1
    exec_sql $case_db $sub_node1_port "DROP SUBSCRIPTION IF EXISTS tap_sub;DROP TABLE testTab1 cascade"
    exec_sql $case_db $pub_node1_port "DROP PUBLICATION IF EXISTS tap_pub;DROP TABLE testTab1 cascade"

    restart_guc "pub_datanode1" "recovery_max_workers = 4"
    restart_guc "pub_datanode2" "recovery_max_workers = 4"

    echo "create publication."
    exec_sql $case_db $pub_node1_port "CREATE PUBLICATION tap_pub FOR ALL TABLES with(publish='insert,update,delete',ddl='all')"
    exec_sql $case_db $pub_node1_port "create table t1 (a int);"

    poll_query_until $case_db $pub_node2_port "SELECT count(*) FROM t1" "0" "Timed out while waiting for pub_node2 to sync"

    restart_guc "pub_datanode1" "recovery_max_workers = 1"
    restart_guc "pub_datanode2" "recovery_max_workers = 1"
}

function tear_down() {
    exec_sql $case_db $sub_node1_port "DROP SUBSCRIPTION IF EXISTS tap_sub"
    exec_sql $case_db $pub_node1_port "DROP PUBLICATION IF EXISTS tap_pub"

    exec_sql $db $sub_node1_port "DROP DATABASE $case_db"
    exec_sql $db $pub_node1_port "DROP DATABASE $case_db"

    echo "tear down"
}

test_1
tear_down