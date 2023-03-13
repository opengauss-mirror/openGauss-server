#!/bin/sh

source $1/env_utils.sh $1 $2

case_db="sync_db"

function test_1() {
    echo "create database and tables."
    exec_sql $db $pub_node1_port "CREATE DATABASE $case_db"
	exec_sql $db $sub_node1_port "CREATE DATABASE $case_db"
    # Create some preexisting content on publisher
    exec_sql $case_db $pub_node1_port "CREATE TABLE tab_rep (a int primary key)"
    exec_sql $case_db $pub_node1_port "INSERT INTO tab_rep SELECT generate_series(1,10)"

    # Setup structure on subscriber
    exec_sql $case_db $sub_node1_port "CREATE TABLE tab_rep (a int primary key)"

    # Setup logical replication
    echo "create publication and subscription."
    publisher_connstr="port=$pub_node1_port host=$g_local_ip dbname=$case_db user=$username password=$passwd"
    exec_sql $case_db $pub_node1_port "CREATE PUBLICATION tap_pub FOR ALL TABLES"
    exec_sql $case_db $sub_node1_port "CREATE SUBSCRIPTION tap_sub CONNECTION '$publisher_connstr' PUBLICATION tap_pub"

    # Wait for initial table sync to finish
	wait_for_subscription_sync $case_db $sub_node1_port

    if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*) FROM tab_rep")" = "10" ]; then
		echo "check initial data synced for first sub success"
	else
		echo "$failed_keyword when check initial data synced for first sub"
		exit 1
	fi

    # drop subscription so that there is unreplicated data
    exec_sql $case_db $sub_node1_port "DROP SUBSCRIPTION tap_sub"
    exec_sql $case_db $pub_node1_port "INSERT INTO tab_rep SELECT generate_series(11,20)"

    # recreate the subscription, it will try to do initial copy
    exec_sql $case_db $sub_node1_port "CREATE SUBSCRIPTION tap_sub CONNECTION '$publisher_connstr' PUBLICATION tap_pub"

    # but it will be stuck on data copy as it will fail on constraint
    poll_query_until $case_db $sub_node1_port "SELECT srsubstate FROM pg_subscription_rel" "d" "Timed out while waiting for subscriber to start sync"

    # remove the conflicting data
    exec_sql $case_db $sub_node1_port "DELETE FROM tab_rep"

    # wait for sync to finish this time
    wait_for_subscription_sync $case_db $sub_node1_port

    # check that all data is synced
    if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*) FROM tab_rep")" = "20" ]; then
		echo "check initial data synced for second sub success"
	else
		echo "$failed_keyword when initial data synced for second sub"
		exit 1
	fi

    # now check another subscription for the same node pair
    exec_sql $case_db $sub_node1_port "CREATE SUBSCRIPTION tap_sub2 CONNECTION '$publisher_connstr' PUBLICATION tap_pub WITH (copy_data = false)"

    # wait for it to start
    poll_query_until $case_db $sub_node1_port "SELECT count(*) FROM pg_stat_subscription WHERE subname = 'tap_sub2' AND relid IS NULL AND pid IS NOT NULL" "1" "Timed out while waiting for subscriber to start"

    # and drop both subscriptions
    exec_sql $case_db $sub_node1_port "DROP SUBSCRIPTION tap_sub"
    exec_sql $case_db $sub_node1_port "DROP SUBSCRIPTION tap_sub2"

    # check subscriptions are removed
    if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*) FROM pg_subscription")" = "0" ]; then
		echo "check second and third sub are dropped success"
	else
		echo "$failed_keyword when second and third sub are dropped"
		exit 1
	fi

    # remove the conflicting data
    exec_sql $case_db $sub_node1_port "DELETE FROM tab_rep"

    # recreate the subscription again
    exec_sql $case_db $sub_node1_port "CREATE SUBSCRIPTION tap_sub CONNECTION '$publisher_connstr' PUBLICATION tap_pub"

    # and wait for data sync to finish again
    wait_for_subscription_sync $case_db $sub_node1_port

    # check that all data is synced
    if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*) FROM tab_rep")" = "20" ]; then
		echo "check initial data synced for fourth sub success"
	else
		echo "$failed_keyword when initial data synced for fourth sub"
		exit 1
	fi

    # add new table on subscriber
    exec_sql $case_db $sub_node1_port "CREATE TABLE tab_rep_next (a int)"

    # setup structure with existing data on publisher
    exec_sql $case_db $pub_node1_port "CREATE TABLE tab_rep_next (a) AS SELECT generate_series(1,10)"

    wait_for_catchup $case_db $pub_node1_port "tap_sub"

    if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*) FROM tab_rep_next")" = "0" ]; then
		echo "check no data for table added after subscription initialized success"
	else
		echo "$failed_keyword when no data for table added after subscription initialized"
		exit 1
	fi

    # ask for data sync
    exec_sql $case_db $sub_node1_port "ALTER SUBSCRIPTION tap_sub REFRESH PUBLICATION"

    # wait for sync to finish
    wait_for_subscription_sync $case_db $sub_node1_port

    if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*) FROM tab_rep_next")" = "10" ]; then
		echo "check data for table added after subscription initialized are now synced success"
	else
		echo "$failed_keyword when data for table added after subscription initialized are now synced"
		exit 1
	fi

    # Add some data
    exec_sql $case_db $pub_node1_port "INSERT INTO tab_rep_next SELECT generate_series(1,10)"

    wait_for_catchup $case_db $pub_node1_port "tap_sub"

    if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*) FROM tab_rep_next")" = "20" ]; then
		echo "check data for table added after subscription initialized are now synced success"
	else
		echo "$failed_keyword when changes for table added after subscription initialized replicated"
		exit 1
	fi

    # clean up
    exec_sql $case_db $pub_node1_port "DROP TABLE tab_rep_next"
    exec_sql $case_db $sub_node1_port "DROP TABLE tab_rep_next"
    exec_sql $case_db $sub_node1_port "DROP SUBSCRIPTION tap_sub"

    # Table tap_rep already has the same records on both publisher and subscriber
    # at this time. Recreate the subscription which will do the initial copy of
    # the table again and fails due to unique constraint violation.
    exec_sql $case_db $sub_node1_port "CREATE SUBSCRIPTION tap_sub CONNECTION '$publisher_connstr' PUBLICATION tap_pub"

    poll_query_until $case_db $sub_node1_port "SELECT srsubstate FROM pg_subscription_rel" "d" "Timed out while waiting for subscriber to start sync"

    # DROP SUBSCRIPTION must clean up slots on the publisher side when the
    # subscriber is stuck on data copy for constraint violation.
    exec_sql $case_db $sub_node1_port "DROP SUBSCRIPTION tap_sub"

    if [ "$(exec_sql $case_db $pub_node1_port "SELECT count(*) FROM pg_replication_slots")" = "2" ]; then
		echo "check DROP SUBSCRIPTION during error can clean up the slots on the publisher success"
	else
		echo "$failed_keyword when DROP SUBSCRIPTION during error can clean up the slots on the publisher"
		exit 1
	fi
}

function tear_down() {
	exec_sql $case_db $sub_node1_port "DROP SUBSCRIPTION IF EXISTS tap_sub"
    exec_sql $case_db $sub_node1_port "DROP SUBSCRIPTION IF EXISTS tap_sub2"
	exec_sql $case_db $pub_node1_port "DROP PUBLICATION IF EXISTS tap_pub"

	exec_sql $db $sub_node1_port "DROP DATABASE $case_db"
	exec_sql $db $pub_node1_port "DROP DATABASE $case_db"

	echo "tear down"
}

test_1
tear_down