#!/bin/sh
#three test
#test_1: standby(origin primary) failover, and crc consistency check with secondary standby succeed.
#expected: failover succeed.
#test_2: standby(origin primary) failover, and no wal segments in secondary standby.
#expected: failover succeed.
#test_3: standby(origin primary) failover, and there wal segments in secondary standby, but crc consistency check failed.
#expected: failover timeout.
source ./standby_env.sh

function test_1()
{
check_instance

#kill standby
kill_standby

#create table
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test1; CREATE TABLE mpp_test1(id INT,name VARCHAR(15) NOT NULL);"

#kill primary
kill_primary

#start origin primary as standby and do failover
start_primary_as_standby

#failover standby to primary
gs_ctl failover -w -t $gsctl_wait_time -D  $data_dir/datanode1
if [ $? -eq 0 ]; then
    echo "test_1 failover succeed"
else
	echo "$failed_keyword, test_1 failover failed"
	exit 1
fi

#start standby
start_standby
check_replication_setup
}

function test_2()
{
check_instance

#kill primary
kill_primary

#start origin primary as standby and do failover
start_primary_as_standby

#failover standby to primary
gs_ctl failover -w -t $gsctl_wait_time -D  $data_dir/datanode1
if [ $? -eq 0 ]; then
    echo "test_2 failover succeed"
else
	echo "$failed_keyword, test_2 failover failed"
	exit 1
fi

check_replication_setup
}

function test_3()
{
check_instance

#create table
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test3; CREATE TABLE mpp_test3(id INT,name VARCHAR(15) NOT NULL);"

#stop secondary and standby
stop_dummystandby
stop_standby

gsql -d $db -p $dn1_primary_port -c "checkpoint;checkpoint;checkpoint;"
sleep 2

#kill primary and start secondary and standby as primary
kill_primary
start_dummystandby
start_standby_as_primary

gsql -d $db -p $dn1_standby_port -c "checkpoint;checkpoint;checkpoint;"

kill_standby
start_primary_as_standby
sleep 2

gs_ctl failover -w -t 10 -D  $data_dir/datanode1 1>"./results/secondary/standby_failover_consistency_check.result" 2>&1 &
sleep 10

if [ $(query_primary | grep -E "Promoting" | wc -l) -eq 1 ]; then
    echo "test_3 failover timeout as expected"
else
    if [ $(query_primary | grep -E "Secondary" | wc -l) -eq 1 ]; then
        echo "$failed_keyword, test_3 failover succeed unexpected"
        gs_ctl build -Z single_node -D $data_dir/datanode1_standby
        check_replication_setup
        exit 1
    fi
fi

stop_primary
start_standby_as_primary
# waiting for dummystandby connect to primary
check_dummy2_setup
gs_ctl build -Z single_node -D $data_dir/datanode1

check_replication_setup_for_primary
wait_primarycatchup_finish
gs_ctl switchover -D $data_dir/datanode1
check_replication_setup
}

function tear_down()
{
sleep 1
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test1;"
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test3;"
}

test_1
test_2
test_3
tear_down
