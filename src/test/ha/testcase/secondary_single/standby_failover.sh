#!/bin/sh

source ./standby_env.sh

function test_1()
{

    check_instance

	gs_guc reload -Z datanode -D $data_dir/datanode1 -c "enable_incremental_catchup=off"
	
	# Create Table
	gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists standby_failover_test; create table standby_failover_test(ID int);"

	gsql -d $db -p $dn1_primary_port -c "insert into standby_failover_test values(1); insert into standby_failover_test values(2); insert into standby_failover_test values(3);"
	
	stop_standby
	
	gsql -d $db -p $dn1_primary_port -c "insert into standby_failover_test select * from standby_failover_test;"
	
	gs_ctl stop -D $data_dir/datanode1_dummystandby
	
	start_standby
	
	gsql -d $db -p $dn1_primary_port -c "insert into standby_failover_test select * from standby_failover_test;" 
	
	stop_primary
	
	stop_standby
	
	start_primary_as_standby
	
	start_dummystandby
	
	# Failover standby to primary
	gs_ctl failover -w -t $gsctl_wait_time -D  $data_dir/datanode1 
	
	start_standby
	
	if [ $(gsql -d $db -p $dn1_primary_port -t -c "select count(*) from standby_failover_test;") -eq 12 ]; then
		echo "success implemented failover when xlog file in standby is more then dummystandby one."
	else
		echo "faiover $failed_keyword."
		exit 1
	fi
}

function tear_down()
{
	sleep 1
	gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists standby_failover_test;"
	gs_guc reload -Z datanode -D $data_dir/datanode1 -c "enable_incremental_catchup=on"
}

test_1
tear_down