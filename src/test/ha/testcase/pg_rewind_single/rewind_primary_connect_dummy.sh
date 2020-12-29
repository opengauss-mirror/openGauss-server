#!/bin/sh

source ./standby_env.sh

function test_1()
{
check_instance

# do a checkpoint to force primary flush the replication slot to disk
gsql -d $db -p $dn1_primary_port -c "checkpoint;"

stop_dummystandby

stop_standby

# build standby incremental will fail
#gs_rewind -D $data_dir/datanode1_standby --source-server='hostaddr=127.0.0.1 port='$dn1_primary_port' dbname=postgres application_name=gs_rewind user='$username'' -P --debug
gs_ctl build -D $data_dir/datanode1_standby -b incremental -Z single_node

if [ $? -eq 0 ]; then
	echo "$failed_keyword, test_1: gs_rewind success but not expected!"
	exit 1
else
	echo "test_1: gs_rewind failed as expected."
fi

start_dummystandby

gs_ctl build -D $data_dir/datanode1_standby -b incremental -Z single_node 

if [ $? -eq 0 ]; then
	echo "test_1: gs_rewind success as expected"
else
	echo "$failed_keyword, test_1: gs_rewind failed!"
	exit 1
fi

start_standby

check_replication_setup
}

function test_2()
{
check_instance

gs_ctl failover -w -t $gsctl_wait_time -D  $data_dir/datanode1_standby
if [ $? -eq 0 ]; then
    echo "test_2 failover succeed"
else
	echo "test_2 $failed_keyword"
	exit 1
fi

gsql -d $db -p $dn1_primary_port -c "checkpoint;checkpoint;checkpoint;"

gsql -d $db -p $dn1_standby_port -c "checkpoint;checkpoint;checkpoint;"

stop_standby

# xlog separate between primary and standby/dummystandby, build standby will fail
#gs_rewind -D $data_dir/datanode1_standby --source-server='hostaddr=127.0.0.1 port='$dn1_primary_port' dbname=postgres application_name=gs_rewind user='$username'' -P
gs_ctl build  -D $data_dir/datanode1_standby -b incremental -Z single_node

if [ $? -eq 0 ]; then
	echo "$failed_keyword, test_2: gs_rewind success but not expected!"
	exit 1
else
	echo "test_2: gs_rewind failed as expected."
fi

stop_standby
start_standby_as_primary
kill_primary

#gs_rewind -D $data_dir/datanode1 --source-server='hostaddr=127.0.0.1 port='$dn1_standby_port' dbname=postgres application_name=gs_rewind user='$username'' -P
gs_ctl build -D $data_dir/datanode1 -b incremental -Z single_node

if [ $? -eq 0 ]; then
	echo "test_2: gs_rewind success as expected"
else
	echo "$failed_keyword, test_2: gs_rewind failed!"
	exit 1
fi

check_replication_setup_for_primary
wait_primarycatchup_finish
gs_ctl switchover -D $data_dir/datanode1

check_replication_setup
check_instance
}

test_1
test_2

