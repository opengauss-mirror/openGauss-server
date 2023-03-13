#!/bin/sh

source $1/env_utils.sh $1 $2

case_db="encoding_db"

# Test replication between databases with different encodings
function test_1() {
	echo "create database and tables."
	exec_sql $db $pub_node1_port "CREATE DATABASE $case_db WITH encoding 'UTF8'"
	exec_sql $db $sub_node1_port "CREATE DATABASE $case_db WITH LC_CTYPE 'en_US' encoding 'LATIN1' lc_collate 'en_US'"

	exec_sql $case_db $pub_node1_port "CREATE TABLE test1 (a int, b text);"
	exec_sql $case_db $sub_node1_port "CREATE TABLE test1 (a int, b text);"

	# Setup logical replication
	echo "create publication and subscription."
	publisher_connstr="port=$pub_node1_port host=$g_local_ip dbname=$case_db user=$username password=$passwd"
	exec_sql $case_db $pub_node1_port "CREATE PUBLICATION mypub FOR ALL TABLES;"
	exec_sql $case_db $sub_node1_port "CREATE SUBSCRIPTION mysub CONNECTION '$publisher_connstr' PUBLICATION mypub"

	# Wait for initial sync to finish
	wait_for_subscription_sync $case_db $sub_node1_port "mysub"

	exec_sql $case_db $pub_node1_port "INSERT INTO test1 VALUES (1, E'Mot\xc3\xb6rhead')"

	wait_for_catchup $case_db $pub_node1_port "mysub"

	if [ "$(exec_sql $case_db $sub_node1_port "SELECT a FROM test1 WHERE b = E'Mot\xf6rhead'")" = "1" ]; then
		echo "check data replicated to subscriber success"
	else
		echo "$failed_keyword when check data replicated to subscriber"
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