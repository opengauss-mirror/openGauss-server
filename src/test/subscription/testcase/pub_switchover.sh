#!/bin/sh

source $1/env_utils.sh $1 $2

case_db="sw_db"

function test_1() {
	echo "create database and tables."
	exec_sql $db $pub_node1_port "CREATE DATABASE $case_db"
	exec_sql $db $sub_node1_port "CREATE DATABASE $case_db"
	# Create some preexisting content on publisher
	exec_sql $case_db $pub_node1_port "CREATE TABLE tab_rep AS SELECT generate_series(1,10) AS a"

	# Setup structure on subscriber
	exec_sql $case_db $sub_node1_port "CREATE TABLE tab_rep (a int)"

	echo "create publication and subscription."
	# Setup logical replication
	publisher_connstr="port=$pub_node1_port host=$g_local_ip dbname=$case_db user=$username password=$passwd"
	exec_sql $case_db $pub_node1_port "CREATE PUBLICATION tap_pub for all tables"
	exec_sql $case_db $sub_node1_port "CREATE SUBSCRIPTION tap_sub CONNECTION '$publisher_connstr' PUBLICATION tap_pub"

	# Wait for initial table sync to finish
	wait_for_subscription_sync $case_db $sub_node1_port

	if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*) FROM tab_rep")" = "10" ]; then
		echo "check initial data of table is replicated success"
	else
		echo "$failed_keyword when check initial data of table is replicated"
		exit 1
	
	fi

	# test incremental synchronous
	exec_sql $case_db $pub_node1_port "INSERT INTO tab_rep VALUES (11)"

	wait_for_catchup $case_db $pub_node1_port "tap_sub"

	if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*) FROM tab_rep")" = "11" ]; then
		echo "check incremental data of table is replicated success"
	else
		echo "$failed_keyword when check incremental data of table is replicated"
		exit 1
	fi

	echo "switchover pub_node2 to primary"
	switchover_to_primary "pub_datanode2"

	exec_sql $case_db $pub_node2_port "INSERT INTO tab_rep VALUES (12)"

	wait_for_catchup $case_db $pub_node2_port "tap_sub"

	if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*) FROM tab_rep")" = "12" ]; then
		echo "check incremental data of table is replicated after switchover success"
	else
		echo "$failed_keyword when check incremental data of table is replicated after switchover"
		exit 1
	fi
}

function tear_down(){
	switchover_to_primary "pub_datanode1"

	exec_sql $case_db $sub_node1_port "DROP SUBSCRIPTION tap_sub"
	exec_sql $case_db $pub_node1_port "DROP PUBLICATION tap_pub"

	exec_sql $db $sub_node1_port "DROP DATABASE $case_db"
	exec_sql $db $pub_node1_port "DROP DATABASE $case_db"

	echo "tear down"
}

test_1
tear_down