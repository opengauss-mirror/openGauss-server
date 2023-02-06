#!/bin/sh

source $1/env_utils.sh $1 $2

case_db="rewrite_db"

function test_1() {
    echo "create database and tables."
	exec_sql $db $pub_node1_port "CREATE DATABASE $case_db"
	exec_sql $db $sub_node1_port "CREATE DATABASE $case_db"

    exec_sql $case_db $pub_node1_port "CREATE TABLE test1 (a int, b text)"
	exec_sql $case_db $sub_node1_port "CREATE TABLE test1 (a int, b text)"

    echo "create publication and subscription."
	publisher_connstr="port=$pub_node1_port host=$g_local_ip dbname=$case_db user=$username password=$passwd"

    exec_sql $case_db $pub_node1_port "CREATE PUBLICATION mypub FOR ALL TABLES"

    exec_sql $case_db $sub_node1_port "CREATE SUBSCRIPTION mysub CONNECTION '$publisher_connstr' PUBLICATION mypub"

    # Wait for initial sync to finish
    wait_for_subscription_sync $case_db $sub_node1_port

    exec_sql $case_db $pub_node1_port "INSERT INTO test1 (a, b) VALUES (1, 'one'), (2, 'two')"

    wait_for_catchup $case_db $pub_node1_port "mysub"

    if [ "$(exec_sql $case_db $sub_node1_port "SELECT a, b FROM test1")" = "1|one
2|two" ]; then
		echo "check initial data replicated to subscriber success"
	else
		echo "$failed_keyword when initial data replicated to subscriber"
		exit 1
	fi

    # DDL that causes a heap rewrite
    exec_sql $case_db $pub_node1_port "ALTER TABLE test1 ADD c int NOT NULL DEFAULT 0"
	exec_sql $case_db $sub_node1_port "ALTER TABLE test1 ADD c int NOT NULL DEFAULT 0"

    wait_for_catchup $case_db $pub_node1_port "mysub"

    exec_sql $case_db $pub_node1_port "INSERT INTO test1 (a, b, c) VALUES (3, 'three', 33)"

    wait_for_catchup $case_db $pub_node1_port "mysub"

    if [ "$(exec_sql $case_db $sub_node1_port "SELECT a, b, c FROM test1")" = "1|one|0
2|two|0
3|three|33" ]; then
		echo "check data replicated to subscriber success"
	else
		echo "$failed_keyword when data replicated to subscriber"
		exit 1
	fi
}

function tear_down() {
	exec_sql $case_db $sub_node1_port "DROP SUBSCRIPTION IF EXISTS mysub"
	exec_sql $case_db $pub_node1_port "DROP PUBLICATION IF EXISTS mypub"

	exec_sql $db $sub_node1_port "DROP DATABASE $case_db"
	exec_sql $db $pub_node1_port "DROP DATABASE $case_db"

	echo "tear down"
}

test_1
tear_down