#!/bin/sh

source ./standby_env.sh

function test_1()
{
check_instance

#failover the standby, so we have two diferent primary
stop_primary
gs_ctl failover -D $data_dir/datanode1_standby

#check and insure they have no relationship now
gs_ctl query -D $data_dir/datanode1
gs_ctl query -D $data_dir/datanode1_standby

stop_standby
start_standby
#do failover twice
gs_ctl failover -D $data_dir/datanode1_standby

#check and insure they have no relationship now
gs_ctl query -D $data_dir/datanode1
gs_ctl query -D $data_dir/datanode1_standby
#use gs_rewind to mirror the standby on primary, sync the diferent part between them.
gs_ctl build -D $data_dir/datanode1 -Z single_node -b incremental

check_replication_setup_for_primary
wait_primarycatchup_finish
#restore enviroment0
gs_ctl switchover -D $data_dir/datanode1

#check if rewind is ok.
check_replication_setup
}

function test_2()
{
check_instance

#failover the standby, so we have two diferent primary
stop_primary
gs_ctl failover -D $data_dir/datanode1_standby

#check and insure they have no relationship now
gs_ctl query -D $data_dir/datanode1
gs_ctl query -D $data_dir/datanode1_standby

stop_standby
start_standby
#do failover twice
gs_ctl failover -D $data_dir/datanode1_standby
#check and insure they have no relationship now
gs_ctl query -D $data_dir/datanode1
gs_ctl query -D $data_dir/datanode1_standby

check_dummy_setup
#use gs_rewind to mirror the standby on primary, sync the diferent part between them.
gs_ctl build -D $data_dir/datanode1 -Z single_node -b incremental

check_replication_setup_for_primary
#wait catup
wait_primarycatchup_finish
#restore enviroment0
gs_ctl switchover -D $data_dir/datanode1

#check if rewind is ok.
check_replication_setup
}
function tear_down()
{
sleep 1
}

#create db test
function test_3()
{
    check_instance

	gs_guc reload -Z datanode -D $data_dir/datanode1 -c "enable_incremental_catchup=off"
	stop_primary
	stop_standby
	#set the sender/writer queue small, so the data replication would be slow.
	gs_guc set -Z datanode -D $data_dir/datanode1 -c "walsender_max_send_size=16MB"
	gs_guc set -Z datanode -D $data_dir/datanode1 -c "data_replicate_buffer_size=8MB"
	gs_guc set -Z datanode -D $data_dir/datanode1_standby -c "walsender_max_send_size=16MB"
	gs_guc set -Z datanode -D $data_dir/datanode1_standby -c "data_replicate_buffer_size=8MB"
	gs_guc set -Z datanode -D $data_dir/datanode1_dummystandby -c "walsender_max_send_size=16MB"
	gs_guc set -Z datanode -D $data_dir/datanode1_dummystandby -c "data_replicate_buffer_size=8MB"
	cstore_rawdata_lines=15000
	dbnum=2
	start_primary
	start_standby

    check_primary_startup

    for((loopdb=1;loopdb<=$dbnum;loopdb++))
	do
		dbcur="hatest$loopdb"
		gsql -d $db -p $dn1_primary_port -c "drop database $dbcur; create database $dbcur;"
		#create table
		gsql -d $dbcur -p $dn1_primary_port -c "DROP TABLE if exists big_cu_table; create table big_cu_table (c_id bigint NOT NULL,c_d_id int NOT NULL,c_w_id int NOT NULL,c_first varchar(16) NOT NULL,c_middle varchar NOT NULL,c_last TEXT NOT NULL, c_street_1 varchar(20) NOT NULL,c_street_2 varchar(20) NOT NULL,c_city varchar(20) NOT NULL,c_state char(2) NOT NULL,c_zip char(9) NOT NULL,c_phone char(16) NOT NULL, c_since timestamp NOT NULL,c_credit char(2) NOT NULL, c_credit_lim numeric(12,2) NOT NULL, c_discount numeric(4,4) NOT NULL,c_balance numeric(12,2) NOT NULL,c_ytd_payment numeric(12,2) NOT NULL,c_payment_cnt int NOT NULL,c_delivery_cnt int NOT NULL, c_data varchar(500) NOT NULL , partial cluster key(c_id)) with (orientation=column) distribute by hash (c_d_id);"
		gsql -d $dbcur -p $dn1_primary_port -c "set enable_data_replicate=on; copy big_cu_table from '$scripts_dir/data/customer0_0' with csv null '';"
		
		kill_primary
		#failover the standby, so we have two diferent primary
		gs_ctl failover -D $data_dir/datanode1_standby -w -t $gsctl_wait_time
		gs_ctl query -D $data_dir/datanode1
        gs_ctl query -D $data_dir/datanode1_standby

        check_dummy_setup
		#use gs_rewind to mirror the standby on primary, sync the diferent part between them.
		gs_ctl build -D $data_dir/datanode1 -Z single_node -b incremental
		#check data
		gsql -d $dbcur -p $dn1_standby_port -c "select pgxc_pool_reload();select count(1) from big_cu_table;"
		if [ $(gsql -d $dbcur -p $dn1_standby_port -c "select pgxc_pool_reload();select count(1) from big_cu_table;" | grep `expr 1 \* $cstore_rawdata_lines` |wc -l) -eq 1 ]; then
			echo "check success on dn1_primary big_cu_table"
		else
			echo "check $failed_keyword on dn1_primary big_cu_table"
			exit 1
		fi
		check_replication_setup_for_primary
		#wait catup
		wait_primarycatchup_finish
		gs_ctl switchover -D $data_dir/datanode1 -w -t $gsctl_wait_time

	done
	stop_primary
	stop_standby
	#restore original options
	gs_guc set -Z datanode -D $data_dir/datanode1 -c "walsender_max_send_size=8MB"
	gs_guc set -Z datanode -D $data_dir/datanode1 -c "data_replicate_buffer_size=256MB"
	gs_guc set -Z datanode -D $data_dir/datanode1_standby -c "walsender_max_send_size=8MB"
	gs_guc set -Z datanode -D $data_dir/datanode1_standby -c "data_replicate_buffer_size=256MB"
	gs_guc set -Z datanode -D $data_dir/datanode1_dummystandby -c "walsender_max_send_size=8MB"
	gs_guc set -Z datanode -D $data_dir/datanode1_dummystandby -c "data_replicate_buffer_size=256MB"
	start_primary
	start_standby
	#check if rewind is ok.
	check_replication_setup
	gs_guc reload -Z datanode -D $data_dir/datanode1 -c "enable_incremental_catchup=on"
}
function tear_down()
{
sleep 1
}
test_1
test_3
tear_down
