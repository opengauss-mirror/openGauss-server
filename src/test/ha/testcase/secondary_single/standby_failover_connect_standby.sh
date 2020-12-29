#!/bin/sh

source ./standby_env.sh

function test_1()
{
check_instance

gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test1; CREATE TABLE mpp_test1(id INT,name VARCHAR(15) NOT NULL);"

stop_standby
stop_dummystandby

gsql -d $db -p $dn1_primary_port -c "checkpoint;checkpoint;checkpoint;"

kill_primary

start_primary_as_standby
start_standby

# failover standby with less xlog
gs_ctl failover -w -t 20 -D  $data_dir/datanode1_standby

if [ $? -eq 1 ]; then
	echo "test_1 failover timeout as expected"
else
	echo "$failed_keyword, test_1 failover success but not expected!"
	exit 1
fi

# failover standby with more xlog
gs_ctl failover -w -t $gsctl_wait_time -D  $data_dir/datanode1
if [ $? -eq 0 ]; then
	echo "test_1 failover succeed"
else
	echo "$failed_keyword, test_1 failover failed!"
	exit 1
fi

start_dummystandby

sleep 15 # wait for standby finish catchup

check_replication_setup
}

# xlog of primary and standby separate with each other, neither of them could failover
function test_2()
{
# failover standby with more xlog
gs_ctl failover -w -t $gsctl_wait_time -D  $data_dir/datanode1_standby

if [ $? -eq 0 ]; then
	echo "test_2 failover succeed"
else
	echo "$failed_keyword, test_2 failover failed!"
	exit 1
fi

gsql -d $db -p $dn1_primary_port -c "checkpoint;checkpoint;checkpoint;"

gsql -d $db -p $dn1_standby_port -m -c "insert into mpp_test1 values(1, 'aaa');"

stop_dummystandby
stop_standby
kill_primary

start_primary_as_standby
start_standby

gs_ctl failover -w -t 20 -D  $data_dir/datanode1_standby

if [ $? -ne 0 ]; then
	echo "test_2 failover timeout as expected"
else
	echo "$failed_keyword, test_2 failover success but not expected!"
	exit 1
fi

gs_ctl failover -w -t 20 -D  $data_dir/datanode1

if [ $? -ne 0 ]; then
	echo "test_2 failover timeout as expected"
else
	echo "test_2 $failed_keyword"
	exit 1
fi

start_dummystandby
# failover primary will be failed
gs_ctl failover -w -t 20 -D  $data_dir/datanode1

if [ $? -ne 0 ]; then
	echo "test_2 failover timeout as expected"
else
	echo "$failed_keyword, test_2 failover success but not expected!"
	exit 1
fi

gs_ctl failover -w -t $gsctl_wait_time -D  $data_dir/datanode1_standby
if [ $? -eq 0 ]; then
	echo "test_2 failover succeed"
else
	echo "$failed_keyword, test_2 failover failed!"
	exit 1
fi
# waiting for dummystandby connect to primary
check_standby_setup
gs_ctl build -Z single_node -D $data_dir/datanode1

stop_standby

gs_ctl failover -w -t $gsctl_wait_time -D  $data_dir/datanode1
if [ $? -eq 0 ]; then
	echo "test_2 failover succeed"
else
	echo "$failed_keyword, test_2 failover failed!"
	exit 1
fi

start_standby

check_replication_setup
}

function tear_down()
{
sleep 1
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test1;"
}

test_1
#test_2
tear_down

