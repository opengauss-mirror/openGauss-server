#!/bin/sh

source ./standby_env.sh

function test_1()
{
check_instance

#ensure that code here can be processed 
#gs_rewind --help

#failover the standby, so we have two diferent primary
#gs_ctl failover -D $data_dir/datanode1_standby

stop_standby

check_dummy_setup

#test the case of connstr_source is null.
gs_rewind -D $data_dir/datanode1_standby  -P

#test the case of datadir_target is null.
#gs_rewind --source-server='hostaddr=127.0.0.1 port='$dn1_primary_port' dbname=postgres application_name=gs_rewind user='$username'' -P

#test dry_run.
#gs_rewind -D $data_dir/datanode1_standby --source-server='hostaddr=127.0.0.1 port='$dn1_primary_port' dbname=postgres application_name=gs_rewind user='$username'' -P -n

#run real gs_rewind 
#gs_rewind -D $data_dir/datanode1_standby --source-server='hostaddr=127.0.0.1 port='$dn1_primary_port' dbname=postgres application_name=gs_rewind user='$username'' -P

gs_ctl build -D $data_dir/datanode1_standby -Z single_node -b incremental
#check if rewind is ok.
start_standby
}

function tear_down()
{
sleep 1
}

test_1
tear_down