
#!/bin/sh

source $1/env_utils.sh $1 $2

case_db="conflict_db"

function test_1() {
    echo "create database and tables."
    exec_sql $db $pub_node1_port "CREATE DATABASE $case_db"
	exec_sql $db $sub_node1_port "CREATE DATABASE $case_db"
    # Create some preexisting content on publisher
    # => keep_local
    exec_sql $case_db $sub_node1_port "ALTER SYSTEM SET subscription_conflict_resolution = keep_local"

    # Setup structure on subscriber
    exec_sql $case_db $pub_node1_port "CREATE TABLE tab_keep (a int primary key, b char)"
    exec_sql $case_db $sub_node1_port "CREATE TABLE tab_keep (a int primary key, b char)"

    # Setup logical replication
    echo "create publication and subscription. (=> keep_local)"
    publisher_connstr="port=$pub_node1_port host=$g_local_ip dbname=$case_db user=$username password=$passwd"

    exec_sql $case_db $pub_node1_port "CREATE PUBLICATION tap_pub1 FOR TABLE tab_keep"
    exec_sql $case_db $sub_node1_port "CREATE SUBSCRIPTION tap_sub1 CONNECTION '$publisher_connstr' PUBLICATION tap_pub1"

    # Wait for initial table sync to finish
    wait_for_subscription_sync $case_db $sub_node1_port
    
    exec_sql $case_db $sub_node1_port "INSERT INTO tab_keep VALUES(1, 'a')"
    exec_sql $case_db $pub_node1_port "INSERT INTO tab_keep VALUES(1, 'b')"

    # Wait for catchup 
    wait_for_catchup $case_db $pub_node1_port "tap_sub1"

    if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*) FROM tab_keep where b = 'a'")" = "1" ]; then
		echo "check pub_sub conflict for 1st sub success"
	else
		echo "$failed_keyword when check pub_sub conflict for 1st sub"
		exit 1
	fi
    
    exec_sql $case_db $sub_node1_port "DROP SUBSCRIPTION tap_sub1"
	exec_sql $case_db $pub_node1_port "DROP PUBLICATION tap_pub1"

    # => apply_remote
    echo "create publication and subscription. (=> apply_remote)"
    exec_sql $case_db $sub_node1_port "ALTER SYSTEM SET subscription_conflict_resolution = apply_remote"

    exec_sql $case_db $pub_node1_port "CREATE TABLE tab_apply (a int primary key, b char)"
    exec_sql $case_db $sub_node1_port "CREATE TABLE tab_apply (a int primary key, b char)"

    exec_sql $case_db $pub_node1_port "CREATE PUBLICATION tap_pub2 FOR TABLE tab_apply"
    exec_sql $case_db $sub_node1_port "CREATE SUBSCRIPTION tap_sub2 CONNECTION '$publisher_connstr' PUBLICATION tap_pub2"

    # Wait for initial table sync to finish
    wait_for_subscription_sync $case_db $sub_node1_port

    exec_sql $case_db $sub_node1_port "INSERT INTO tab_apply VALUES(1, 'k')"
    exec_sql $case_db $pub_node1_port "INSERT INTO tab_apply VALUES(1, 'a')"

    # Wait for catchup 
    wait_for_catchup $case_db $pub_node1_port "tap_sub2"


    if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*) FROM tab_apply where b = 'a'")" = "1" ]; then
		echo "check pub_sub conflict for 2nd sub success"
	else
		echo "$failed_keyword when check pub_sub conflict for 2nd sub"
		exit 1
	fi

    exec_sql $case_db $sub_node1_port "DROP SUBSCRIPTION tap_sub2"
	exec_sql $case_db $pub_node1_port "DROP PUBLICATION tap_pub2"

    # update apply_remote
    echo "create publication and subscription. (update apply_remote)"
    exec_sql $case_db $pub_node1_port "CREATE TABLE tab_updateApply (a int primary key, b char)"
    exec_sql $case_db $sub_node1_port "CREATE TABLE tab_updateApply (a int primary key, b char)"

    exec_sql $case_db $pub_node1_port "CREATE PUBLICATION tap_pub3 FOR TABLE tab_updateApply"
    exec_sql $case_db $sub_node1_port "CREATE SUBSCRIPTION tap_sub3 CONNECTION '$publisher_connstr' PUBLICATION tap_pub3"

    exec_sql $case_db $pub_node1_port "INSERT INTO tab_updateApply VALUES(1, 'a')"

    # Wait for catchup 
    wait_for_catchup $case_db $pub_node1_port "tap_sub3"

    exec_sql $case_db $sub_node1_port "INSERT INTO tab_updateApply VALUES(2, 'b')"
    exec_sql $case_db $pub_node1_port "UPDATE tab_updateApply SET a = 2 where a = 1"

    # Wait for catchup 
    wait_for_catchup $case_db $pub_node1_port "tap_sub3"

    if [ "$(exec_sql $case_db $sub_node1_port "SELECT * FROM tab_updateApply")" = "2|a" ]; then
        echo "check pub_sub conflict for 3rd sub success"
    else
        echo "$failed_keyword when check pub_sub conflict for 3rd sub"
        exit 1
    fi

    exec_sql $case_db $sub_node1_port "DROP SUBSCRIPTION tap_sub3"
	exec_sql $case_db $pub_node1_port "DROP PUBLICATION tap_pub3"

    #unique conflict
    echo "create publication and subscription. (unique conflict)"
    exec_sql $case_db $pub_node1_port "CREATE TABLE tab_unique (a int primary key, b char, c int unique)"
    exec_sql $case_db $sub_node1_port "CREATE TABLE tab_unique (a int primary key, b char, c int unique)"

    exec_sql $case_db $pub_node1_port "CREATE PUBLICATION tap_pub4 FOR TABLE tab_unique"
    exec_sql $case_db $sub_node1_port "CREATE SUBSCRIPTION tap_sub4 CONNECTION '$publisher_connstr' PUBLICATION tap_pub4"

    exec_sql $case_db $sub_node1_port "INSERT INTO tab_unique VALUES (1, 'a', 1), (2, 'c', 2), (3, 'c', 3)"

    # Wait for catchup 
    wait_for_catchup $case_db $pub_node1_port "tap_sub4"

    exec_sql $case_db $pub_node1_port "INSERT INTO tab_unique VALUES(3, 'k', 4)"

    wait_for_catchup $case_db $pub_node1_port "tap_sub4"

    exec_sql $case_db $pub_node1_port "INSERT INTO tab_unique VALUES(1, 'b', 2)"

    if [ "$(exec_sql $case_db $sub_node1_port "SELECT * FROM tab_unique")" = "1|a|1
2|c|2
3|k|4" ]; then
        echo "check pub_sub conflict for 4th sub success"
    else
        echo "$failed_keyword when check pub_sub conflict for 4th sub"
        exit 1
    fi

    exec_sql $case_db $sub_node1_port "DROP SUBSCRIPTION tap_sub4"
	exec_sql $case_db $pub_node1_port "DROP PUBLICATION tap_pub4"
}

function tear_down(){
    exec_sql $case_db $sub_node1_port "ALTER SYSTEM SET subscription_conflict_resolution = error"

	exec_sql $db $sub_node1_port "DROP DATABASE $case_db"
	exec_sql $db $pub_node1_port "DROP DATABASE $case_db"

	echo "tear down"
}

test_1
tear_down