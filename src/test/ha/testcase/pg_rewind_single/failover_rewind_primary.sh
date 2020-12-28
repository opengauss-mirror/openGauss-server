#!/bin/sh

source ./standby_env.sh

function test_1()
{
check_instance

#failover the standby, so we have two diferent primary
gs_ctl failover -D $data_dir/datanode1_standby

sleep 5

#check and insure they have no relationship now
gs_ctl query -D $data_dir/datanode1
gs_ctl query -D $data_dir/datanode1_standby

kill_primary

#use incremental to mirror the primary on standby, sync the diferent part between them.
gs_ctl build -D $data_dir/datanode1 -Z single_node -b incemental

#check if rewind is ok.
#start_primary_as_standby
check_replication_setup_for_primary
}

function tear_down()
{
sleep 1
gs_ctl switchover -D $data_dir/datanode1
check_replication_setup
}

test_1
tear_down
