#!/bin/sh

source $1/env_utils.sh $1 $2

case_db="skiplsn_db"

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

	exec_sql $case_db $sub_node1_port "insert into tab_rep values (1,1)"

	logfile=$(get_log_file "sub_datanode1")

	location=$(awk 'END{print NR}' $logfile)

	exec_sql $case_db $pub_node1_port "insert into tab_rep values (1,2)"

	content=$(tail -n +$location $logfile)
	commitlsn=$(expr "$content" : '.*commit_lsn:\s\([0-9|/|ABCDEF]*\).*')

	while [ -z $commitlsn ]
	do
		content=$(tail -n +$location $logfile)
		commitlsn=$(expr "$content" : '.*commit_lsn:\s\([0-9|/|ABCDEF]*\).*')
	done

	exec_sql $case_db $sub_node1_port "alter subscription tap_sub set (skiplsn = '$commitlsn')"

	exec_sql $case_db $pub_node1_port "insert into tab_rep values (2, 2)"

	wait_for_catchup $case_db $pub_node1_port "tap_sub"

    if [ "$(exec_sql $case_db $sub_node1_port "SELECT * FROM tab_rep")" = "1|1
2|2" ]; then
		echo "check data sync after skip conflict success"
	else
		echo "$failed_keyword when check data sync after skip conflict"
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