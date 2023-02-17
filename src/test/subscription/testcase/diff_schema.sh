#!/bin/sh

source $1/env_utils.sh $1 $2

case_db="rep_db"

function test_1() {
    echo "create database and tables."
	exec_sql $db $pub_node1_port "CREATE DATABASE $case_db"
	exec_sql $db $sub_node1_port "CREATE DATABASE $case_db"

    # Create some preexisting content on publisher
    exec_sql $case_db $pub_node1_port "CREATE TABLE test_tab (a int primary key, b varchar)"
    exec_sql $case_db $pub_node1_port "INSERT INTO test_tab VALUES (1, 'foo'), (2, 'bar')"

    # Setup structure on subscriber
    exec_sql $case_db $sub_node1_port "CREATE TABLE test_tab (a int primary key, b text, c timestamptz DEFAULT now(), d bigint DEFAULT 999)"

    # Setup logical replication
    echo "create publication and subscription."
	publisher_connstr="port=$pub_node1_port host=$g_local_ip dbname=$case_db user=$username password=$passwd"

    exec_sql $case_db $pub_node1_port "CREATE PUBLICATION tap_pub FOR ALL TABLES"
    exec_sql $case_db $sub_node1_port "CREATE SUBSCRIPTION tap_sub CONNECTION '$publisher_connstr' PUBLICATION tap_pub"

    # Wait for initial table sync to finish
    wait_for_subscription_sync $case_db $sub_node1_port

    if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*), count(c), count(d = 999) FROM test_tab")" = "2|2|2" ]; then
		echo "check initial data was copied to subscriber success"
	else
		echo "$failed_keyword when check initial data was copied to subscriber"
		exit 1
	fi

    # Update the rows on the publisher and check the additional columns on
    # subscriber didn't change
    exec_sql $case_db $pub_node1_port "UPDATE test_tab SET b = md5(b)"
    wait_for_catchup $case_db $pub_node1_port "tap_sub"

    if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*), count(c), count(d = 999) FROM test_tab")" = "2|2|2" ]; then
		echo "check extra columns contain local defaults after copy success"
	else
		echo "$failed_keyword when check extra columns contain local defaults after copy"
		exit 1
	fi

    # Change the local values of the extra columns on the subscriber,
    # update publisher, and check that subscriber retains the expected
    # values
    exec_sql $case_db $sub_node1_port "UPDATE test_tab SET c = 'epoch'::timestamptz + 987654321 * interval '1s'"
    exec_sql $case_db $pub_node1_port "UPDATE test_tab SET b = md5(a::text)"
    wait_for_catchup $case_db $pub_node1_port "tap_sub"

    if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*), count(extract(epoch from c) = 987654321), count(d = 999) FROM test_tab")" = "2|2|2" ]; then
		echo "check extra columns contain locally changed data success"
	else
		echo "$failed_keyword when check extra columns contain locally changed data"
		exit 1
	fi

    # Another insert
    exec_sql $case_db $pub_node1_port "INSERT INTO test_tab VALUES (3, 'baz')"
    wait_for_catchup $case_db $pub_node1_port "tap_sub"
    
    if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*), count(c), count(d = 999) FROM test_tab")" = "3|3|3" ]; then
		echo "check extra columns contain local defaults after apply success"
	else
		echo "$failed_keyword when check extra columns contain local defaults after apply"
		exit 1
	fi

    # Check a bug about adding a replica identity column on the subscriber
    # that was not yet mapped to a column on the publisher.  This would
    # result in errors on the subscriber and replication thus not
    # progressing.
    exec_sql $case_db $pub_node1_port "CREATE TABLE test_tab2 (a int)"
    exec_sql $case_db $sub_node1_port "CREATE TABLE test_tab2 (a int)"
    exec_sql $case_db $sub_node1_port "ALTER SUBSCRIPTION tap_sub REFRESH PUBLICATION"

    wait_for_subscription_sync $case_db $sub_node1_port

    # Add replica identity column.  (The serial is not necessary, but it's
    # a convenient way to get a default on the new column so that rows
    # from the publisher that don't have the column yet can be inserted.)
    # it's not supported to alter table add serial column, use default instead, 
    # can only insert 1 column
    exec_sql $case_db $sub_node1_port "ALTER TABLE test_tab2 ADD COLUMN b int DEFAULT 1 PRIMARY KEY"
    exec_sql $case_db $pub_node1_port "INSERT INTO test_tab2 VALUES (1)"
    wait_for_catchup $case_db $pub_node1_port "tap_sub"

    if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*), min(a), max(a) FROM test_tab2")" = "1|1|1" ]; then
		echo "check replicated inserts on subscriber success"
	else
		echo "$failed_keyword when check replicated inserts on subscriber"
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