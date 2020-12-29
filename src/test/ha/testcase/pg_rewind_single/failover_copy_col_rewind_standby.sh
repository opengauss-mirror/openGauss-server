#!/bin/sh

source ./standby_env.sh

function test_1()
{
check_instance

cstore_rawdata_lines=15000

#create table on cn, dn_primary, dn_standby
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists big_cu_table; create table big_cu_table (c_id bigint NOT NULL,c_d_id int NOT NULL,c_w_id int NOT NULL,c_first varchar(16) NOT NULL,c_middle varchar NOT NULL,c_last TEXT NOT NULL, c_street_1 varchar(20) NOT NULL,c_street_2 varchar(20) NOT NULL,c_city varchar(20) NOT NULL,c_state char(2) NOT NULL,c_zip char(9) NOT NULL,c_phone char(16) NOT NULL, c_since timestamp NOT NULL,c_credit char(2) NOT NULL, c_credit_lim numeric(12,2) NOT NULL, c_discount numeric(4,4) NOT NULL,c_balance numeric(12,2) NOT NULL,c_ytd_payment numeric(12,2) NOT NULL,c_payment_cnt int NOT NULL,c_delivery_cnt int NOT NULL, c_data varchar(500) NOT NULL , partial cluster key(c_id)) with (orientation=column) distribute by hash (c_d_id);"

#copy data to primary
stop_primary

#failover the standby, so we have two diferent primary
gs_ctl failover -D $data_dir/datanode1_standby

#wait for dummystandy connect to new primary(datanode1_standby)
check_standby_setup

#check and insure they have no relationship now
gs_ctl query -D $data_dir/datanode1
gs_ctl query -D $data_dir/datanode1_standby

gsql -d $db -p $dn1_standby_port -c "set enable_data_replicate=on; copy big_cu_table from '$scripts_dir/data/customer0_0' with csv null '';"
gsql -d $db -p $dn1_standby_port -c "set enable_data_replicate=on; copy big_cu_table from '$scripts_dir/data/customer0_0' with csv null '';"

stop_standby

start_primary_as_standby
gs_ctl failover -D  $data_dir/datanode1 -Z single_node
#wait dummy standby connect to primary
check_dummy_setup
#use incemental build to mirror the primary on standby, sync the diferent part between them.
gs_ctl build -D $data_dir/datanode1_standby -Z single_node -b incemental

#start_standby
check_replication_setup

#test the copy results on dn1_primary
if [ $(gsql -d $db -p $dn1_primary_port -m -c "select count(1) from big_cu_table;" | grep `expr 2 \* $cstore_rawdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_primary big_cu_table"
else
	echo "copy $failed_keyword on dn1_primary big_cu_table"
	exit 1
fi

wait_catchup_finish

#test the copy results on dn1_standby
if [ $(gsql -d $db -p $dn1_standby_port -m -c "select count(1) from big_cu_table;" | grep `expr 2 \* $cstore_rawdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_standby big_cu_table"
else
	echo "copy $failed_keyword on dn1_standby big_cu_table"
	exit 1
fi
}

function tear_down()
{
sleep 1
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists big_cu_table;"
}

test_1
tear_down
