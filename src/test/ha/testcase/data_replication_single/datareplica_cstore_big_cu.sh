#!/bin/sh

source ./standby_env.sh

function test_1()
{
check_instance

if [ $(gsql -d $db -p $dn1_primary_port -c "show enable_mix_replication;" | grep "off" |wc -l) -eq 1 ]; then
	stop_primary
	stop_standby

	#set the sender/writer queue small, so the data replication would be slow.
	gs_guc set -Z datanode -D $data_dir/datanode1 -c "walsender_max_send_size=128MB"
	gs_guc set -Z datanode -D $data_dir/datanode1 -c "data_replicate_buffer_size=8MB"
	gs_guc set -Z datanode -D $data_dir/datanode1_standby -c "walsender_max_send_size=128MB"
	gs_guc set -Z datanode -D $data_dir/datanode1_standby -c "data_replicate_buffer_size=8MB"
	gs_guc set -Z datanode -D $data_dir/datanode1_dummystandby -c "walsender_max_send_size=128MB"
	gs_guc set -Z datanode -D $data_dir/datanode1_dummystandby -c "data_replicate_buffer_size=8MB"

	start_standby
	start_primary
fi

cstore_rawdata_lines=15000

#create table
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists big_cu_table; create table big_cu_table (c_id bigint NOT NULL,c_d_id int NOT NULL,c_w_id int NOT NULL,c_first varchar(16) NOT NULL,c_middle varchar NOT NULL,c_last TEXT NOT NULL, c_street_1 varchar(20) NOT NULL,c_street_2 varchar(20) NOT NULL,c_city varchar(20) NOT NULL,c_state char(2) NOT NULL,c_zip char(9) NOT NULL,c_phone char(16) NOT NULL, c_since timestamp NOT NULL,c_credit char(2) NOT NULL, c_credit_lim numeric(12,2) NOT NULL, c_discount numeric(4,4) NOT NULL,c_balance numeric(12,2) NOT NULL,c_ytd_payment numeric(12,2) NOT NULL,c_payment_cnt int NOT NULL,c_delivery_cnt int NOT NULL, c_data varchar(500) NOT NULL , partial cluster key(c_id)) with (orientation=column) distribute by hash (c_d_id);"

gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy big_cu_table from '$scripts_dir/data/customer0_0' with csv null '';"

sleep 15

#test the copy results on dn1_primary
if [ $(gsql -d $db -p $dn1_primary_port -c "select pgxc_pool_reload();select count(1) from big_cu_table;" | grep `expr 1 \* $cstore_rawdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_primary big_cu_table"
else
	echo "copy $failed_keyword on dn1_primary big_cu_table"
	exit 1
fi

#test the copy results on dn1_standby
if [ $(gsql -d $db -p $dn1_standby_port -m -c "select count(1) from big_cu_table;" | grep `expr 1 \* $cstore_rawdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_standby big_cu_table"
else
	echo "copy $failed_keyword on dn1_standby big_cu_table"
	exit 1
fi
}

function test_2()
{
check_instance

if [ $(gsql -d $db -p $dn1_primary_port -c "show enable_mix_replication;" | grep "off" |wc -l) -eq 1 ]; then

	stop_primary
	stop_standby

	#set the sender/writer queue small, so the data replication would be slow.
	gs_guc set -Z datanode -D $data_dir/datanode1 -c "walsender_max_send_size=8MB"
	gs_guc set -Z datanode -D $data_dir/datanode1 -c "data_replicate_buffer_size=128MB"
	gs_guc set -Z datanode -D $data_dir/datanode1_standby -c "walsender_max_send_size=8MB"
	gs_guc set -Z datanode -D $data_dir/datanode1_standby -c "data_replicate_buffer_size=128MB"
	gs_guc set -Z datanode -D $data_dir/datanode1_dummystandby -c "walsender_max_send_size=8MB"
	gs_guc set -Z datanode -D $data_dir/datanode1_dummystandby -c "data_replicate_buffer_size=128MB"

	start_standby
	start_primary
fi

cstore_rawdata_lines=15000

#create table
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists big_cu_table; create table big_cu_table (c_id bigint NOT NULL,c_d_id int NOT NULL,c_w_id int NOT NULL,c_first varchar(16) NOT NULL,c_middle varchar NOT NULL,c_last TEXT NOT NULL, c_street_1 varchar(20) NOT NULL,c_street_2 varchar(20) NOT NULL,c_city varchar(20) NOT NULL,c_state char(2) NOT NULL,c_zip char(9) NOT NULL,c_phone char(16) NOT NULL, c_since timestamp NOT NULL,c_credit char(2) NOT NULL, c_credit_lim numeric(12,2) NOT NULL, c_discount numeric(4,4) NOT NULL,c_balance numeric(12,2) NOT NULL,c_ytd_payment numeric(12,2) NOT NULL,c_payment_cnt int NOT NULL,c_delivery_cnt int NOT NULL, c_data varchar(500) NOT NULL , partial cluster key(c_id)) with (orientation=column) distribute by hash (c_d_id);"

gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy big_cu_table from '$scripts_dir/data/customer0_0' with csv null '';"

sleep 3

#test the copy results on dn1_primary
if [ $(gsql -d $db -p $dn1_primary_port -c "select pgxc_pool_reload();select count(1) from big_cu_table;" | grep `expr 1 \* $cstore_rawdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_primary big_cu_table"
else
	echo "copy $failed_keyword on dn1_primary big_cu_table"
	exit 1
fi

#test the copy results on dn1_standby
if [ $(gsql -d $db -p $dn1_standby_port -m -c "select count(1) from big_cu_table;" | grep `expr 1 \* $cstore_rawdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_standby big_cu_table"
else
	echo "copy $failed_keyword on dn1_standby big_cu_table"
	exit 1
fi
}

function tear_down()
{
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists big_cu_table;"

if [ $(gsql -d $db -p $dn1_primary_port -c "show enable_mix_replication;" | grep "off" |wc -l) -eq 1 ]; then
	stop_primary
	stop_standby

	#set the sender/writer queue small, so the data replication would be slow.
	gs_guc set -Z datanode -D $data_dir/datanode1 -c "walsender_max_send_size=8MB"
	gs_guc set -Z datanode -D $data_dir/datanode1 -c "data_replicate_buffer_size=256MB"
	gs_guc set -Z datanode -D $data_dir/datanode1_standby -c "walsender_max_send_size=8MB"
	gs_guc set -Z datanode -D $data_dir/datanode1_standby -c "data_replicate_buffer_size=256MB"
	gs_guc set -Z datanode -D $data_dir/datanode1_dummystandby -c "walsender_max_send_size=8MB"
	gs_guc set -Z datanode -D $data_dir/datanode1_dummystandby -c "data_replicate_buffer_size=256MB"

	start_standby
	start_primary
fi
check_replication_setup
}

test_1
test_2
tear_down
