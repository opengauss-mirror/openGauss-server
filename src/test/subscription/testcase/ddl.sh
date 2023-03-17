#!/bin/sh

source $1/env_utils.sh $1 $2

case_db="ddl_db"

# Test some logical replication DDL behavior
function test_1() {
	echo "create database and tables."
	exec_sql $db $pub_node1_port "CREATE DATABASE $case_db"
	exec_sql $db $sub_node1_port "CREATE DATABASE $case_db"

	exec_sql $case_db $pub_node1_port "CREATE TABLE test1 (a int, b text);"
	exec_sql $case_db $sub_node1_port "CREATE TABLE test1 (a int, b text);"

	# Setup logical replication
	echo "create publication and subscription."
	publisher_connstr="port=$pub_node1_port host=$g_local_ip dbname=$case_db user=$username password=$passwd"
	exec_sql $case_db $pub_node1_port "CREATE PUBLICATION mypub FOR ALL TABLES;"

	# One of the specified publications exists.
	exec_sql $case_db $sub_node1_port "CREATE SUBSCRIPTION mysub CONNECTION '$publisher_connstr' PUBLICATION mypub, non_existent_pub" 2> exec.out
	if [ "$(cat exec.out)" = "ERROR:  There are some publications not exist on the publisher." ]; then
		echo "check create subscription throws warning for non-existent publication success"
	else
		echo "$failed_keyword when check create subscription throws warning for non-existent publication"
		exit 1
	fi
	rm exec.out

	exec_sql $case_db $sub_node1_port "CREATE SUBSCRIPTION mysub CONNECTION '$publisher_connstr' PUBLICATION mypub"

	wait_for_subscription_sync $case_db $sub_node1_port "mysub"

	# Specifying non-existent publication along with set publication.
	exec_sql $case_db $sub_node1_port "ALTER SUBSCRIPTION mysub SET PUBLICATION non_existent_pub" 2> exec.out
	if [ "$(cat exec.out)" = "ERROR:  There are some publications not exist on the publisher." ]; then
		echo "check alter subscription set publication throws error for non-existent publications success"
	else
		echo "$failed_keyword when check alter subscription set publication throws error for non-existent publications"
		exit 1
	fi
	rm exec.out

	exec_sql $case_db $sub_node1_port "ALTER SUBSCRIPTION mysub RENAME to mysub" 2> exec.out
	if [ "$(cat exec.out)" = "ERROR:  subscription \"mysub\" already exists" ]; then
		echo "check alter subscription rename throws error for existent name success"
	else
		echo "$failed_keyword when check alter subscription rename throws error for existent name"
		exit 1
	fi
	rm exec.out

	exec_sql $case_db $pub_node1_port "ALTER PUBLICATION mypub RENAME to mypub" 2> exec.out
	if [ "$(cat exec.out)" = "ERROR:  publication \"mypub\" already exists" ]; then
		echo "check alter publication rename throws error for existent name success"
	else
		echo "$failed_keyword when check alter publication rename throws error for existent name"
		exit 1
	fi
	rm exec.out
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