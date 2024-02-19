#!/bin/sh

source $1/env_utils.sh $1 $2 $3

case_db="dump_db"
subscription_dir=$1
dump_expected_dir="${subscription_dir}/testcase/dump_expected"
results_dir="${subscription_dir}/results"
dump_result_dir="${results_dir}/dump_results"
mkdir -p $dump_result_dir

function test_1() {
	pub_ddl=$1
    echo "create database and tables."
	exec_sql $db $pub_node1_port "CREATE DATABASE $case_db"
	exec_sql $db $sub_node1_port "CREATE DATABASE $case_db"

    echo "create publication and subscription."
	publisher_connstr="port=$pub_node1_port host=$g_local_ip dbname=$case_db user=$username password=$passwd"

	if [ "${pub_ddl}" = "all" ]; then
		exec_sql $case_db $pub_node1_port "CREATE PUBLICATION mypub FOR ALL TABLES WITH (ddl='all')"
	else
		exec_sql $case_db $pub_node1_port "CREATE PUBLICATION mypub FOR ALL TABLES WITH (ddl='table')"
	fi

    exec_sql $case_db $sub_node1_port "CREATE SUBSCRIPTION mysub CONNECTION '$publisher_connstr' PUBLICATION mypub"

    # Wait for initial sync to finish
    wait_for_subscription_sync $case_db $sub_node1_port

	exec_sql $case_db $pub_node1_port "CREATE TABLE test1 (a int, b text)"

    exec_sql $case_db $pub_node1_port "INSERT INTO test1 (a, b) VALUES (1, 'one'), (2, 'two')"

    wait_for_catchup $case_db $pub_node1_port "mysub"

    if [ "$(exec_sql $case_db $sub_node1_port "SELECT a, b FROM test1")" = "1|one
2|two" ]; then
		echo "check initial data replicated to subscriber success"
	else
		echo "$failed_keyword when initial data replicated to subscriber"
		exit 1
	fi

	exec_dump_db $case_db $pub_node1_port "$dump_result_dir/dump_db_pub${pub_ddl}.pub" "all"
	sedcmd="sed -i -e s/gauss/${g_username}/g $dump_expected_dir/dump_db_pub${pub_ddl}.pub"
	$sedcmd
	diff $dump_result_dir/dump_db_pub${pub_ddl}.pub $dump_expected_dir/dump_db_pub${pub_ddl}.pub > ${dump_result_dir}/dump_pub${pub_ddl}_pub.diff
	if [ -s ${dump_result_dir}/dump_puball_pub.diff ]; then
		echo "$failed_keyword when dump publication"
		exit 1
	else
		echo "check publication dump data success"
	fi

    exec_dump_db $case_db $sub_node1_port "$dump_result_dir/dump_db_pub${pub_ddl}.sub" "all"
	sedcmd="sed -i -e s/gauss/${g_username}/g $dump_expected_dir/dump_db_pub${pub_ddl}.sub"
	$sedcmd
	diff $dump_result_dir/dump_db_pub${pub_ddl}.sub $dump_expected_dir/dump_db_pub${pub_ddl}.sub > ${dump_result_dir}/dump_pub${pub_ddl}_sub.diff --ignore-matching-lines='password=encryptOpt'
	if [ -s ${dump_result_dir}/dump_pub${pub_ddl}_sub.diff ]; then
		echo "$failed_keyword when dump subscription"
		exit 1
	else
		echo "check subscription dump data success"
	fi
}

function tear_down() {
	exec_sql $case_db $sub_node1_port "DROP SUBSCRIPTION IF EXISTS mysub"
	exec_sql $case_db $pub_node1_port "DROP PUBLICATION IF EXISTS mypub"

	exec_sql $db $sub_node1_port "DROP DATABASE $case_db"
	exec_sql $db $pub_node1_port "DROP DATABASE $case_db"

	echo "tear down"
}

test_1 'all'
tear_down
test_1 'table'
tear_down