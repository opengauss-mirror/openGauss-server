#!/bin/sh
source ./standby_env.sh

function test_1() 
{
    gsql -d $db -p $primary_port -U $USER  -W Gauss@123 -c "DROP DATABASE if exists db_uwal_test;"
    gsql -d $db -p $primary_port -U $USER  -W Gauss@123 -c "CREATE DATABASE db_uwal_test;"
    gsql -d db_uwal_test -p $primary_port -U $USER  -W Gauss@123 -c "CREATE TABLE foo(id int);"
    gsql -d db_uwal_test -p $primary_port -U $USER  -W Gauss@123 -c "select gs_walwriter_flush_stat(0);"

    count=3
    for((i=1; i<=$count; i++))
    do
        gsql -d db_uwal_test -p $primary_port -U $USER  -W Gauss@123 -c "insert into foo (id) values (123); select * from foo;"
    done
    sleep 3
    result=$(gsql -d db_uwal_test -p $primary_port -U $USER  -W Gauss@123 -c "SELECT * from foo;")
    if [ $(echo "$result" | grep "123" | wc -l) -eq $count ]; then
        echo "success on uwal replication"
else
        echo "$failed_keyword on uwal replication"
        exit 1
fi
}


function tear_down()
{
    sleep 1
    gsql -d $db -p $primary_port -U $USER  -W Gauss@123 -c "DROP DATABASE if exists db_uwal_test;"
    rm -rf $data_dir/archive_nas
}

test_1
tear_down