#!/bin/sh

source ./standby_env.sh

function test_1()
{
check_instance

kill_standby

gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test1; CREATE TABLE mpp_test1(id INT, name VARCHAR(15) NOT NULL);"

stop_primary

start_primary_as_standby

# dummystandby should not connect to standby because of crc mismatch
start_standby_as_primary

query_standby

gsql -d $db -p $dn1_standby_port -c "checkpoint;checkpoint;checkpoint;"

stop_standby
start_standby

gs_ctl failover -w -t 20 -D  $data_dir/datanode1_standby

if [ $? -ne 0 ]; then
	echo "test_1 failover timeout as expected"
else
	echo "$failed_keyword, test_1 failover success but not expected!"
	exit 1
fi

stop_primary
start_primary

# waiting for dummystandby connect to primary
check_dummy_setup
gs_ctl build -Z single_node -D $data_dir/datanode1_standby

check_replication_setup
}

function test_2()
{
check_instance
gs_guc reload -Z datanode -D $data_dir/datanode1 -c "enable_incremental_catchup=off"
gs_guc reload -Z datanode -D $data_dir/datanode1 -c "checkpoint_segments=5"
gs_guc reload -Z datanode -D $data_dir/datanode1_standby -c "checkpoint_segments=5"
gs_guc reload -Z datanode -D $data_dir/datanode1 -c "wal_keep_segments=6"
gs_guc reload -Z datanode -D $data_dir/datanode1_standby -c "wal_keep_segments=6"

stop_standby

# make some xlog on dummy standby
gsql -d $db -p $dn1_primary_port -c "checkpoint;"
sleep 2
gsql -d $db -p $dn1_primary_port -c "checkpoint;"
sleep 2

stop_dummystandby
start_standby

stop_primary
start_primary_as_standby

gs_ctl failover -w -t $gsctl_wait_time -D  $data_dir/datanode1_standby
if [ $? -eq 0 ]; then
        echo "test_2 failover succeed"
else
        echo "$failed_keyword, test_2 failover failed!"
        exit 1
fi

# make some xlog on primary and standby, and truncate the xlog
gsql -d $db -p $dn1_standby_port -c "select pg_switch_xlog();select pg_switch_xlog();select pg_switch_xlog();select pg_switch_xlog();select pg_switch_xlog();select pg_switch_xlog();select pg_switch_xlog();select pg_switch_xlog();select pg_switch_xlog();select pg_switch_xlog();checkpoint;"

stop_primary

start_dummystandby

# check the dummystandby has connected to standby(new primary). When gs_rewind, primary must be connected to dummystandby.
gs_ctl build -D $data_dir/datanode1 -b incremental -Z single_node

if [ $? -eq 0 ]; then
        echo "test_2: gs_rewind success as expected"
else
        echo "$failed_keyword, test_2: gs_rewind failed!"
        exit 1
fi

start_primary_as_standby
stop_standby
stop_primary
start_primary
start_standby

check_replication_setup
}

function tear_down()
{
sleep 1
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test1;"
gs_guc reload -Z datanode -D $data_dir/datanode1 -c "enable_incremental_catchup=on"
gs_guc reload -Z datanode -D $data_dir/datanode1 -c "checkpoint_segments=64"
gs_guc reload -Z datanode -D $data_dir/datanode1_standby -c "checkpoint_segments=64"
gs_guc reload -Z datanode -D $data_dir/datanode1 -c "wal_keep_segments=65"
gs_guc reload -Z datanode -D $data_dir/datanode1_standby -c "wal_keep_segments=65"
}

test_1
#test_2
tear_down

