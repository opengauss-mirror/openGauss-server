#!/bin/sh

source ./standby_env.sh

function test_1()
{
check_instance

cstore_rawdata_lines=8

#create table
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists cstore_copy_t1; create table cstore_copy_t1(c1 int2, c2 int4, c3 int8, c4 char(10), c5 varchar(12),c6 numeric(10,2)) with (orientation = column);"

gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy cstore_copy_t1 from '$scripts_dir/data/cstore_copy_t1.data' delimiter '|';"

#kill the standby_node_name
kill_standby

#copy data(25M) to dummy standby
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy cstore_copy_t1 from '$scripts_dir/data/cstore_copy_t1.data' delimiter '|';"

#kill the primary_node_name
kill_primary

#start the standby_node_name
start_standby

echo after start standby
ls -l $data_dir/datanode1_dummystandby/pg_xlog
ls -l $data_dir/datanode1_dummystandby/base/dummy_standby
lsof | grep $USER | grep datanode1_dummystandby | grep pg_xlog


#failover standby to primary
gs_ctl failover -w -t $gsctl_wait_time -D  $data_dir/datanode1_standby

if [ $? -eq 0 ]; then
    echo "all of success!"
else
	echo "$failed_keyword"
	exit 1
fi

#test the copy results on dn1_standby
if [ $(gsql -d $db -p $dn1_standby_port -c "select pgxc_pool_reload();select count(1) from cstore_copy_t1;" | grep `expr 2 \* $cstore_rawdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_standby cstore_copy_t1"
else
	echo "copy $failed_keyword on dn1_standby cstore_copy_t1"
	exit 1
fi

start_primary_as_standby
sleep 10
gs_ctl switchover -w -t $gsctl_wait_time -D  $data_dir/datanode1
if [ $? -eq 0 ]; then
    echo "all of success!"
else
	echo "$failed_keyword"
	exit 1
fi

sleep 5

#test the copy results on dn1_primary
if [ $(gsql -d $db -p $dn1_primary_port -c "select pgxc_pool_reload();select count(1) from cstore_copy_t1;" | grep `expr 2 \* $cstore_rawdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_primary cstore_copy_t1"
else
	echo "copy $failed_keyword on dn1_primary cstore_copy_t1"
	exit 1
fi

#test the copy results on dn1_standby
if [ $(gsql -d $db -p $dn1_standby_port -m -c "select count(1) from cstore_copy_t1;" | grep `expr 2 \* $cstore_rawdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_standby cstore_copy_t1"
else
	echo "copy $failed_keyword on dn1_standby cstore_copy_t1"
	exit 1
fi
}

function tear_down()
{
sleep 1
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists cstore_copy_t1;"
}

test_1
tear_down