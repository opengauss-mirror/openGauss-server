#!/bin/sh
#failover failed without standby and primary

source ./standby_env.sh

function test_1()
{
check_instance

stop_primary

stop_dummystandby

#failover standby to primary failed
gs_ctl failover -w -t 30 -D  $data_dir/datanode1_standby

start_dummystandby

sleep 2

start_primary_as_standby
}

function tear_down()
{
sleep 5
#gs_ctl build -D $data_dir/datanode1
gs_ctl switchover -D  $data_dir/datanode1
}

test_1
tear_down