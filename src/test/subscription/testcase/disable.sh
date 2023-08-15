#!/bin/sh

source $1/env_utils.sh $1 $2

case_db="disable_db"

function test_1() {
    echo "create database and tables."
    exec_sql $db $pub_node1_port "CREATE DATABASE $case_db"
	exec_sql $db $sub_node1_port "CREATE DATABASE $case_db"
    # Create some preexisting content on publisher
    exec_sql $case_db $pub_node1_port "CREATE TABLE tab_rep (a int primary key, b int)"

    # Setup structure on subscriber
    exec_sql $case_db $sub_node1_port "CREATE TABLE tab_rep (a int primary key, b int)"

    # Setup logical replication
    echo "create publication and subscription."
    publisher_connstr="port=$pub_node1_port host=$g_local_ip dbname=$case_db user=$username password=$passwd"
    exec_sql $case_db $pub_node1_port "CREATE PUBLICATION tap_pub FOR ALL TABLES"
    exec_sql $case_db $sub_node1_port "CREATE SUBSCRIPTION tap_sub CONNECTION '$publisher_connstr' PUBLICATION tap_pub"

    # Wait for initial table sync to finish
	wait_for_subscription_sync $case_db $sub_node1_port

	exec_sql $case_db $pub_node1_port "insert into tab_rep values (1,1)"

	wait_for_catchup $case_db $pub_node1_port "tap_sub"

	exec_sql $case_db $sub_node1_port "alter subscription tap_sub disable"

	exec_sql $case_db $pub_node1_port "insert into tab_rep values (2,2)"

    if [ "$(exec_sql $case_db $sub_node1_port "SELECT * FROM tab_rep")" = "1|1" ]; then
		echo "check data not sync after disable subscription success"
	else
		echo "$failed_keyword when check data not sync after disable subscription"
		exit 1
	fi

	exec_sql $case_db $sub_node1_port "alter subscription tap_sub enable"

	wait_for_catchup $case_db $pub_node1_port "tap_sub"

	if [ "$(exec_sql $case_db $sub_node1_port "SELECT * FROM tab_rep")" = "1|1
2|2" ]; then
		echo "check data not sync after enable subscription success"
	else
		echo "$failed_keyword when check data not sync after enable subscription"
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