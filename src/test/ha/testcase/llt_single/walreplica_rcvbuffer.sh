#!/bin/sh

source ./standby_env.sh

function test_1()
{
check_instance

#create table1
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test1; CREATE TABLE mpp_test1(id INT,name VARCHAR(15) NOT NULL);"


if [ $(gsql -d $db -p $dn1_primary_port -c "show enable_mix_replication;" | grep "off" |wc -l) -eq 1 ]; then
	stop_primary
	stop_standby

	gs_guc set -Z datanode -D $data_dir/datanode1 -c "walsender_max_send_size=64MB"
	gs_guc set -Z datanode -D $data_dir/datanode1 -c "data_replicate_buffer_size=128MB"
	gs_guc set -Z datanode -D $data_dir/datanode1_standby -c "walsender_max_send_size=64MB"
	gs_guc set -Z datanode -D $data_dir/datanode1_standby -c "data_replicate_buffer_size=128MB"
	gs_guc set -Z datanode -D $data_dir/datanode1_standby -c "wal_receiver_buffer_size=4MB"

	start_primary

	gsql -d $db -p $dn1_primary_port -c "insert into mpp_test1 values (generate_series(1,20000), 'ABCDEFGH');"

	sleep 5

	start_standby
	check_replication_setup
else
	gsql -d $db -p $dn1_primary_port -c "insert into mpp_test1 values (generate_series(1,20000), 'ABCDEFGH');"
fi

gs_ctl query -D $data_dir/datanode1_standby
gs_ctl query -D $data_dir/datanode1_standby -U $username -P $passwd
gs_ctl query -D $data_dir/datanode1_standby -U $username
gs_ctl query -D $data_dir/datanode1_standby -P $passwd
}

function tear_down()
{
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test1;"
sleep 1
stop_primary
stop_standby

gs_guc set -Z datanode -D $data_dir/datanode1 -c "walsender_max_send_size=8MB"
gs_guc set -Z datanode -D $data_dir/datanode1 -c "data_replicate_buffer_size=16MB"
gs_guc set -Z datanode -D $data_dir/datanode1_standby -c "walsender_max_send_size=8MB"
gs_guc set -Z datanode -D $data_dir/datanode1_standby -c "data_replicate_buffer_size=16MB"
gs_guc set -Z datanode -D $data_dir/datanode1_standby -c "wal_receiver_buffer_size=64MB"

start_standby
start_primary
}

test_1
tear_down
