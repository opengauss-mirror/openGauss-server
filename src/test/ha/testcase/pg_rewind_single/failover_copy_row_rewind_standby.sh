#!/bin/sh

source ./standby_env.sh

function test_1()
{
check_instance

#create table on cn, dn_primary, dn_standby
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test1; CREATE TABLE mpp_test1(id INT,name VARCHAR(15) NOT NULL);"

#stop the primary, failover the standby
stop_primary
gs_ctl failover -D $data_dir/datanode1_standby

#check and insure they have no relationship now
gs_ctl query -D $data_dir/datanode1
gs_ctl query -D $data_dir/datanode1_standby

#copy data to primary
stop_standby
start_primary_as_standby
rm -f $data_dir/datanode1_dummystandby/pg_xlog/000*
rm -f $data_dir/datanode1_dummystandby/base/dummy_standby/*
gs_ctl failover -D $data_dir/datanode1

gsql -d $db -p $dn1_primary_port -m -c "set enable_data_replicate=on; copy mpp_test1 from '$scripts_dir/data/data5';"
gsql -d $db -p $dn1_primary_port -m -c "set enable_data_replicate=on; copy mpp_test1 from '$scripts_dir/data/data5';"

check_dummy_setup

#use incremental to mirror the primary on standby, sync the diferent part between them.
gs_ctl build -D $data_dir/datanode1_standby -Z single_node -b incemental

#check if rewind is ok.
#start_standby
check_replication_setup

#test the copy results on dn1_primary
if [ $(gsql -d $db -p $dn1_primary_port -m -c "select count(1) from mpp_test1;" | grep `expr 2 \* $rawdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_primary"
else
	echo "copy $failed_keyword on dn1_primary"
	exit 1
fi

wait_catchup_finish

#test the copy results on dn1_standby
if [ $(gsql -d $db -p $dn1_standby_port -m -c "select count(1) from mpp_test1;" | grep `expr 2 \* $rawdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_standby"
else
	echo "copy $failed_keyword on dn1_standby"
	exit 1
fi
}

function tear_down()
{
sleep 1
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test1;"
}

test_1
tear_down
