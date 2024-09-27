#!/bin/sh

source $1/env_utils.sh $1 $2

case_db="sw_db"

function test_1() {

	local conninfo = ""

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
	exec_sql $case_db $pub_node1_port "CREATE PUBLICATION sw_tap_pub for all tables"
	exec_sql $case_db $sub_node1_port "CREATE SUBSCRIPTION sw_tap_sub CONNECTION '$publisher_connstr' PUBLICATION sw_tap_pub"

	conninfo = "$(exec_sql $case_db $sub_node1_port "SELECT subconninfo FROM pg_subscription WHERE subname = 'sw_tap_sub'")"

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

	wait_for_catchup $case_db $pub_node1_port "sw_tap_sub"

	if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*) FROM tab_rep")" = "11" ]; then
		echo "check incremental data of table is replicated success"
	else
		echo "$failed_keyword when check incremental data of table is replicated"
		exit 1
	fi

	echo "switchover pub_node2 to primary"
	switchover_to_primary "pub_datanode2"

	exec_sql $case_db $pub_node2_port "INSERT INTO tab_rep VALUES (12)"

	wait_for_catchup $case_db $pub_node2_port "sw_tap_sub"

	if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*) FROM tab_rep")" = "12" ]; then
		echo "check incremental data of table is replicated after switchover success"
	else
		echo "$failed_keyword when check incremental data of table is replicated after switchover"
		exit 1
	fi

	# default sync conntion info
	if [ "$conninfo" = "$(exec_sql $case_db $sub_node1_port "SELECT subconninfo FROM pg_subscription where subname = 'sw_tap_sub'")" ]; then
		echo "publication node switchover, subsciption node do sync connection info failed"
		exit 1
	else
		echo "publication node do switchover, subsciption node do sync connection info success"
	fi

	# alter syncconninfo to false, disable sync conntion info after publication switchover
	exec_sql $case_db $sub_node1_port "alter subscription sw_tap_sub set (syncconninfo = false)"
	conninfo = "$(exec_sql $case_db $sub_node1_port "SELECT subconninfo FROM pg_subscription WHERE subname = 'sw_tap_sub'")"

	echo "switchover pub_node1 to primary"
	switchover_to_primary "pub_datanode1"

	exec_sql $case_db $pub_node1_port "INSERT INTO tab_rep VALUES (13)"

	wait_for_catchup $case_db $pub_node1_port "sw_tap_sub"

	if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*) FROM tab_rep")" = "13" ]; then
		echo "check incremental data of table is replicated after switchover success"
	else
		echo "$failed_keyword when check incremental data of table is replicated after switchover"
		exit 1
	fi

	# syncconninfo = false
	if [ "$conninfo" = "$(exec_sql $case_db $sub_node1_port "SELECT subconninfo FROM pg_subscription where subname = 'sw_tap_sub'")" ]; then
		echo "publication node switchover, syncconninfo is false, subsciption node don't sync connection info , success."
	else
		echo "publication node do switchover, syncconninfo is false, subsciption node still do sync connection info, false."
		exit 1
	fi
}

function tear_down(){
	exec_sql $case_db $sub_node1_port "DROP SUBSCRIPTION sw_tap_sub"
	exec_sql $case_db $pub_node1_port "DROP PUBLICATION sw_tap_pub"

	exec_sql $db $sub_node1_port "DROP DATABASE $case_db"
	exec_sql $db $pub_node1_port "DROP DATABASE $case_db"

	echo "tear down"
}

test_1
tear_down