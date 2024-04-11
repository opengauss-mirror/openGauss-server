#!/bin/sh

source $1/env_utils.sh $1 $2

case_db="matviews_db"

# Test materialized views behavior
function test_1() {
	echo "create database and tables."
	exec_sql $db $pub_node1_port "CREATE DATABASE $case_db"
	exec_sql $db $sub_node1_port "CREATE DATABASE $case_db"

	exec_sql $case_db $pub_node1_port "CREATE TABLE test1 (a int PRIMARY KEY, b text)"
	exec_sql $case_db $pub_node1_port "INSERT INTO test1 (a, b) VALUES (1, 'one'), (2, 'two')"
	exec_sql $case_db $sub_node1_port "CREATE TABLE test1 (a int PRIMARY KEY, b text)"

	# Setup logical replication
	echo "create publication and subscription."
	publisher_connstr="port=$pub_node1_port host=$g_local_ip dbname=$case_db user=$username password=$passwd"
	exec_sql $case_db $pub_node1_port "CREATE PUBLICATION mypub FOR ALL TABLES;"
	exec_sql $case_db $sub_node1_port "CREATE SUBSCRIPTION mysub CONNECTION '$publisher_connstr' PUBLICATION mypub"

	# Wait for initial sync to finish
	wait_for_subscription_sync $case_db $sub_node1_port "mysub"

	# Materialized views are not supported by logical replication, but
	# logical decoding does produce change information for them, so we
	# need to make sure they are properly ignored.

	# create a MV with some data
	exec_sql $case_db $pub_node1_port "CREATE MATERIALIZED VIEW testmv1 AS SELECT * FROM test1;"
	wait_for_catchup $case_db $pub_node1_port "mysub"

	# There is no equivalent relation on the subscriber, but MV data is
	# not replicated, so this does not hang.

	echo "materialized view data not replicated";

	# create a MV on the subscriber
	exec_sql $case_db $sub_node1_port "CREATE MATERIALIZED VIEW testmv1 AS SELECT * FROM test1;"
	exec_sql $case_db $pub_node1_port "INSERT INTO test1 (a, b) VALUES (3, 'three')"
	wait_for_catchup $case_db $pub_node1_port "mysub"

	exec_sql $case_db $sub_node1_port "REFRESH MATERIALIZED VIEW testmv1"

	if [ "$(exec_sql $case_db $sub_node1_port "SELECT * FROM testmv1")" = "1|one
2|two
3|three" ]; then
		echo "check if refresh materialized view success"
	else
		echo "$failed_keyword when check if refresh materialized view"
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