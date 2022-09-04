#!/bin/sh

source ./standby_env.sh

function test_1()
{
check_instance

#create table on cn, dn_primary, dn_standby
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test1; CREATE TABLE mpp_test1(id INT,name VARCHAR(15) NOT NULL);"

#stop the primary, failover the standby
gs_ctl failover -D $data_dir/datanode1_standby
kill_primary

#copy data to standby (new primary)
gsql -d $db -p $dn1_standby_port -c "set enable_data_replicate=on; copy mpp_test1 from '$scripts_dir/data/data_30_lines';"
gsql -d $db -p $dn1_standby_port -c "set enable_data_replicate=on; copy mpp_test1 from '$scripts_dir/data/data_30_lines';"
 
kill_standby
start_primary_as_standby
rm -f $data_dir/datanode1_dummystandby/pg_xlog/000*
rm -f $data_dir/datanode1_dummystandby/base/dummy_standby/*
gs_ctl failover -D $data_dir/datanode1

#copy data to primary
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test1 from '$scripts_dir/data/data_30_lines';"
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; delete from mpp_test1;"
gsql -d $db -p $dn1_primary_port -c "vacuum full mpp_test1;"
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test1 from '$scripts_dir/data/data_30_lines';"

start_standby
#test the copy results on dn1_standby
if [ $(gsql -d $db -p $dn1_standby_port -m -c "select count(1) from mpp_test1;" | grep `expr 2 \* $lessdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_standby"
else
	echo "copy $failed_keyword on dn1_standby"
	exit 1
fi

stop_standby

check_dummy_setup

#force a checkpoint just to avoid rewind failure when checkpoint on primary is smaller
gsql -d $db -p $dn1_primary_port -c "checkpoint;"

#use incremental to mirror the primary on standby, sync the diferent part between them.
gs_ctl build -D $data_dir/datanode1_standby -Z single_node -b incemental

#check if rewind is ok.
#start_standby
check_replication_setup

#test the copy results on dn1_primary
if [ $(gsql -d $db -p $dn1_primary_port -c "select count(1) from mpp_test1;" | grep `expr 1 \* $lessdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_primary"
else
	echo "copy $failed_keyword on dn1_primary"
	exit 1
fi

wait_catchup_finish

#test the copy results on dn1_standby
if [ $(gsql -d $db -p $dn1_standby_port -m -c "select count(1) from mpp_test1;" | grep `expr 1 \* $lessdata_lines` |wc -l) -eq 1 ]; then
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
