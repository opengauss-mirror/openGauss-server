#!/bin/sh

source $1/env_utils.sh $1 $2

case_db="changel_wal_level"

function test_1() {
    echo "create database and tables."
    exec_sql $db $pub_node1_port "CREATE DATABASE $case_db"
    exec_sql $db $sub_node1_port "CREATE DATABASE $case_db"
    # Create some preexisting content on publisher
    exec_sql $case_db $pub_node1_port "CREATE TABLE tab_rep (a int primary key)"
    exec_sql $case_db $pub_node1_port "INSERT INTO tab_rep SELECT generate_series(1,10)"

    # Setup structure on subscriber
    exec_sql $case_db $sub_node1_port "CREATE TABLE tab_rep (a int primary key)"

    # Setup logical replication
    echo "create publication and subscription."
    publisher_connstr="port=$pub_node1_port host=$g_local_ip dbname=$case_db user=$username password=$passwd"
    exec_sql $case_db $pub_node1_port "CREATE PUBLICATION tap_pub FOR ALL TABLES"
    exec_sql $case_db $sub_node1_port "CREATE SUBSCRIPTION tap_sub CONNECTION '$publisher_connstr' PUBLICATION tap_pub"

    # Wait for initial table sync to finish
    wait_for_subscription_sync $case_db $sub_node1_port

    exec_sql $case_db $pub_node1_port "DELETE FROM tab_rep"
    wait_for_catchup $case_db $pub_node1_port "tap_sub"

    # change wal_level to non-logical
    restart_guc "pub_datanode1" "wal_level = hot_standby"

    # Do IUD operatition, this operation will not record logical xlog, cause wal_level is not logical
    exec_sql $case_db $pub_node1_port "INSERT INTO tab_rep values(1)"
    exec_sql $case_db $pub_node1_port "UPDATE tab_rep SET a = 2 where a = 1"
    exec_sql $case_db $pub_node1_port "DELETE FROM tab_rep"
    exec_sql $case_db $pub_node1_port "INSERT INTO tab_rep values(2)"

    exec_sql $case_db $pub_node1_port "select pg_sleep(5)"

    # change wal_level to logical
    restart_guc "pub_datanode1" "wal_level = logical"
    wait_for_catchup $case_db $pub_node1_port "tap_sub"

    # no data in subscription side
    if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*) FROM tab_rep")" = "0" ]; then
        echo "check data synced for first sub success"
    else
        echo "$failed_keyword when check data synced for first sub"
        exit 1
    fi

    # one row in publication side
    if [ "$(exec_sql $case_db $pub_node1_port "SELECT count(*) FROM tab_rep")" = "1" ]; then
        echo "check data in pub success"
    else
        echo "$failed_keyword when check data in pub"
        exit 1
    fi

    exec_sql $case_db $pub_node1_port "INSERT INTO tab_rep values(10)"
    exec_sql $case_db $pub_node1_port "INSERT INTO tab_rep values(11)"
    exec_sql $case_db $pub_node1_port "UPDATE tab_rep SET a = 20 where a = 10"
    exec_sql $case_db $pub_node1_port "DELETE FROM tab_rep where a = 20"

    if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*) FROM tab_rep")" = "1" ]; then
        echo "check data synced for first sub success"
    else
        echo "$failed_keyword when check data synced for first sub"
        exit 1
    fi
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