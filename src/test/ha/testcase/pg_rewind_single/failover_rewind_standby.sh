#!/bin/sh

source ./standby_env.sh

function test_1()
{
check_instance

#check and insure they have no relationship now
gs_ctl query -D $data_dir/datanode1
gs_ctl query -D $data_dir/datanode1_standby

stop_standby
kill_primary
start_primary_as_standby

gs_ctl failover -D $data_dir/datanode1

# wait dummy connect finish
check_dummy_setup

kill_primary
start_primary_as_standby

gs_ctl failover -D $data_dir/datanode1

# wait dummy connect finish
check_dummy_setup

#use gs_rewind to mirror the primary on standby, sync the diferent part between them.
gs_ctl build -D $data_dir/datanode1_standby -Z single_node -b incremental

#check if rewind is ok.
#start_standby
check_replication_setup
}

function tear_down()
{
sleep 1
}

test_1
tear_down
