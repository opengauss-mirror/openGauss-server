#!/bin/sh

source ./standby_env.sh

function test_1()
{
	check_instance
	stop_standby
	gs_guc set -D $standby_data_dir -c "archive_mode = on"
	gs_guc set -D $standby_data_dir -c "archive_dest = $standby_data_dir/archive_directory"
	start_standby
	#create table1
	gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test1; CREATE TABLE mpp_test1(id INT);"
	for i in $(seq 1 10000)
	do
		gsql -d $db -p $dn1_primary_port -c "insert into mpp_test1 values($i);"
	done

	stop_primary
	stop_standby

	cp $standby_data_dir/archive_directory/* $primary_data_dir/pg_xlog
	cp $standby_data_dir/archive_directory/* $standby_data_dir/pg_xlog
	start_primary
	start_standby
	check_standby_startup
}