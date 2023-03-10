#!/bin/sh

source $1/env_utils.sh $1 $2

case_db="binary_db"

function test_1() {
    # Create and initialize node
    echo "create database and tables."
	exec_sql $db $pub_node1_port "CREATE DATABASE $case_db"
	exec_sql $db $sub_node1_port "CREATE DATABASE $case_db"

    # Create tables on both sides of the replication
    ddl="CREATE TABLE public.test_numerical (
		a INTEGER PRIMARY KEY,
		b NUMERIC(10,2),
		c FLOAT,
		d BIGINT
		);
	CREATE TABLE public.test_arrays (
		a INTEGER[] PRIMARY KEY,
		b NUMERIC(10,2)[],
		c TEXT[]
		)"

    exec_sql $case_db $pub_node1_port "$ddl"
    exec_sql $case_db $sub_node1_port "$ddl"

    # Configure logical replication
    echo "create publication and subscription."
    publisher_connstr="port=$pub_node1_port host=$g_local_ip dbname=$case_db user=$username password=$passwd"

    exec_sql $case_db $pub_node1_port "CREATE PUBLICATION tpub FOR ALL TABLES"

    exec_sql $case_db $sub_node1_port "CREATE SUBSCRIPTION tsub CONNECTION '$publisher_connstr' PUBLICATION tpub"

    # Ensure nodes are in sync with each other
    wait_for_subscription_sync $case_db $sub_node1_port

    # Insert some content and make sure it's replicated across
    exec_sql $case_db $pub_node1_port "INSERT INTO public.test_arrays (a, b, c) VALUES
		('{1,2,3}', '{1.1, 1.2, 1.3}', '{"one", "two", "three"}'),
		('{3,1,2}', '{1.3, 1.1, 1.2}', '{"three", "one", "two"}')"
    exec_sql $case_db $pub_node1_port "INSERT INTO public.test_numerical (a, b, c, d) VALUES
		(1, 1.2, 1.3, 10),
		(2, 2.2, 2.3, 20),
		(3, 3.2, 3.3, 30)"

    wait_for_catchup $case_db $pub_node1_port "tsub"

    if [ "$(exec_sql $case_db $sub_node1_port "SELECT a, b, c, d FROM test_numerical ORDER BY a")" = "1|1.20|1.3|10
2|2.20|2.3|20
3|3.20|3.3|30" ]; then
		echo "check replicated data on subscriber success"
	else
		echo "$failed_keyword when check replicated data on subscriber"
		exit 1
	fi

    # Test updates as well
    exec_sql $case_db $pub_node1_port "UPDATE public.test_arrays SET b[1] = 42, c = NULL"
    exec_sql $case_db $pub_node1_port "UPDATE public.test_numerical SET b = 42, c = NULL"

    wait_for_catchup $case_db $pub_node1_port "tsub"

    if [ "$(exec_sql $case_db $sub_node1_port "SELECT a, b, c FROM test_arrays ORDER BY a")" = "{1,2,3}|{42.00,1.20,1.30}|
{3,1,2}|{42.00,1.10,1.20}|" ]; then
		echo "check updated replicated data on subscriber success"
	else
		echo "$failed_keyword when check updated replicated data on subscriber"
		exit 1
	fi

    if [ "$(exec_sql $case_db $sub_node1_port "SELECT a, b, c, d FROM test_numerical ORDER BY a")" = "1|42.00||10
2|42.00||20
3|42.00||30" ]; then
		echo "check updated replicated data on subscriber success"
	else
		echo "$failed_keyword when check updated replicated data on subscriber"
		exit 1
	fi

    # Test to reset back to text formatting, and then to binary again
    exec_sql $case_db $sub_node1_port "ALTER SUBSCRIPTION tsub SET (binary = false)"

    exec_sql $case_db $pub_node1_port "INSERT INTO public.test_numerical (a, b, c, d) VALUES(4, 4.2, 4.3, 40)"

    wait_for_catchup $case_db $pub_node1_port "tsub"

    if [ "$(exec_sql $case_db $sub_node1_port "SELECT a, b, c, d FROM test_numerical ORDER BY a")" = "1|42.00||10
2|42.00||20
3|42.00||30
4|4.20|4.3|40" ]; then
		echo "check replicated data on subscriber success"
	else
		echo "$failed_keyword when check replicated data on subscriber"
		exit 1
	fi

    exec_sql $case_db $sub_node1_port "ALTER SUBSCRIPTION tsub SET (binary = true)"

    exec_sql $case_db $pub_node1_port "INSERT INTO public.test_arrays (a, b, c) VALUES
		('{2,3,1}', '{1.2, 1.3, 1.1}', '{"two", "three", "one"}')"
    
    wait_for_catchup $case_db $pub_node1_port "tsub"

    if [ "$(exec_sql $case_db $sub_node1_port "SELECT a, b, c FROM test_arrays ORDER BY a")" = "{1,2,3}|{42.00,1.20,1.30}|
{2,3,1}|{1.20,1.30,1.10}|{two,three,one}
{3,1,2}|{42.00,1.10,1.20}|" ]; then
		echo "check replicated data on subscriber success"
	else
		echo "$failed_keyword when check replicated data on subscriber"
		exit 1
	fi
}

function tear_down() {
	exec_sql $case_db $sub_node1_port "DROP SUBSCRIPTION IF EXISTS tsub"
	exec_sql $case_db $pub_node1_port "DROP PUBLICATION IF EXISTS tpub"

	exec_sql $db $sub_node1_port "DROP DATABASE $case_db"
	exec_sql $db $pub_node1_port "DROP DATABASE $case_db"

	echo "tear down"
}

test_1
tear_down