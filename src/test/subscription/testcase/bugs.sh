#!/bin/sh

source $1/env_utils.sh $1 $2

case_db="bugs_db"

function test_1() {
    echo "create database and tables."
    exec_sql $db $pub_node1_port "CREATE DATABASE $case_db"
    exec_sql $db $sub_node1_port "CREATE DATABASE $case_db"
    # Create some preexisting content on publisher
    exec_sql $case_db $pub_node1_port "CREATE TABLE tab_rep (a int primary key, b text)"
    exec_sql $case_db $pub_node1_port "INSERT INTO tab_rep VALUES (1)"

    # Setup structure on subscriber
    exec_sql $case_db $sub_node1_port "CREATE TABLE tab_rep (a int primary key, b text not null default 0)"

    # Setup logical replication
    echo "create publication and subscription."
    publisher_connstr="port=$pub_node1_port host=$g_local_ip dbname=$case_db user=$username password=$passwd"
    exec_sql $case_db $pub_node1_port "CREATE PUBLICATION tap_pub FOR ALL TABLES"
    exec_sql $case_db $sub_node1_port "CREATE SUBSCRIPTION tap_sub CONNECTION '$publisher_connstr' PUBLICATION tap_pub"

    logfile=$(get_log_file "sub_datanode1")

    location=$(awk 'END{print NR}' $logfile)

    content=$(tail -n +$location $logfile)
    targetstr=$(expr "$content" : '.*\(Failing row contains\).*')

    attempt=0
    while [ -z "$targetstr" ]
    do
        content=$(tail -n +$location $logfile)
        targetstr=$(expr "$content" : '.*\(Failing row contains\).*')
        attempt=`expr $attempt \+ 1`

        sleep 1
        if [ $attempt -eq 5 ]; then
            echo "$failed_keyword when check failing row log"
            exit 1
        fi
    done

    echo "check failing row log success"
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