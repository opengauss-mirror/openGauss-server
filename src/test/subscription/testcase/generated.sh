#!/bin/sh

source $1/env_utils.sh $1 $2

case_db="generated_db"

function test_1() {
    # setup
    echo "create database and tables."
	exec_sql $db $pub_node1_port "CREATE DATABASE $case_db"
	exec_sql $db $sub_node1_port "CREATE DATABASE $case_db"

    exec_sql $case_db $pub_node1_port "CREATE TABLE tab1 (a int PRIMARY KEY, b int GENERATED ALWAYS AS (a * 2) STORED)"
	exec_sql $case_db $pub_node1_port "CREATE TABLE tab2 (a int PRIMARY KEY, b int default 5)"
	exec_sql $case_db $sub_node1_port "CREATE TABLE tab1 (a int PRIMARY KEY, b int GENERATED ALWAYS AS (a * 22) STORED)"
	exec_sql $case_db $sub_node1_port "CREATE TABLE tab2 (a int PRIMARY KEY, b int default 10)"

    # data for initial sync
    exec_sql $case_db $pub_node1_port "INSERT INTO tab1 (a) VALUES (1), (2), (3)"
	exec_sql $case_db $pub_node1_port "INSERT INTO tab2 (a) VALUES (1), (2), (3)"

    echo "create publication and subscription."
    publisher_connstr="port=$pub_node1_port host=$g_local_ip dbname=$case_db user=$username password=$passwd"

    exec_sql $case_db $pub_node1_port "CREATE PUBLICATION pub1 FOR ALL TABLES"
    
	exec_sql $case_db $sub_node1_port "CREATE SUBSCRIPTION sub1 CONNECTION '$publisher_connstr' PUBLICATION pub1"

    # Wait for initial sync of all subscriptions
    echo "$(exec_sql $case_db $sub_node1_port "SELECT * FROM pg_replication_slots")"

    wait_for_subscription_sync $case_db $sub_node1_port

    if [ "$(exec_sql $case_db $sub_node1_port "SELECT a, b FROM tab1")" = "1|22
2|44
3|66" ]; then
		echo "check generated columns initial sync success"
	else
		echo "$failed_keyword when generated columns initial sync"
		exit 1
	fi

	if [ "$(exec_sql $case_db $sub_node1_port "SELECT a, b FROM tab2")" = "1|5
2|5
3|5" ]; then
		echo "check default columns initial sync success"
	else
		echo "$failed_keyword when default columns initial sync"
		exit 1
	fi

    # data to replicate
    exec_sql $case_db $pub_node1_port "INSERT INTO tab1 VALUES (4), (5)"
    exec_sql $case_db $pub_node1_port "UPDATE tab1 SET a = 6 WHERE a = 5"
	exec_sql $case_db $pub_node1_port "INSERT INTO tab2 VALUES (4), (5)"

    wait_for_catchup $case_db $pub_node1_port "sub1"

    if [ "$(exec_sql $case_db $sub_node1_port "SELECT a, b FROM tab1")" = "1|22
2|44
3|66
4|88
6|132" ]; then
		echo "check generated columns replicated success"
	else
		echo "$failed_keyword when generated columns replicated"
		exit 1
	fi

	if [ "$(exec_sql $case_db $sub_node1_port "SELECT a, b FROM tab2")" = "1|5
2|5
3|5
4|5
5|5" ]; then
		echo "check default columns replicated success"
	else
		echo "$failed_keyword when default columns replicated"
		exit 1
	fi
}

function tear_down() {
	exec_sql $case_db $sub_node1_port "DROP SUBSCRIPTION IF EXISTS sub1"
	exec_sql $case_db $pub_node1_port "DROP PUBLICATION IF EXISTS pub1"

	exec_sql $db $sub_node1_port "DROP DATABASE $case_db"
	exec_sql $db $pub_node1_port "DROP DATABASE $case_db"

	echo "tear down"
}

test_1
tear_down