#!/bin/sh

source ./standby_env.sh

function test_1()
{
check_instance

cstore_rawdata_lines=80

#create table
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists cstore_copy_t1; create table cstore_copy_t1(c1 int2, c2 int4, c3 int8, c4 char(10), c5 varchar(12),c6 numeric(10,2)) with (orientation = column);"

gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy cstore_copy_t1 from '$scripts_dir/data/cstore_copy_t1.data' delimiter '|'; copy cstore_copy_t1 from '$scripts_dir/data/cstore_copy_t1.data' delimiter '|'; copy cstore_copy_t1 from '$scripts_dir/data/cstore_copy_t1.data' delimiter '|'; copy cstore_copy_t1 from '$scripts_dir/data/cstore_copy_t1.data' delimiter '|'; copy cstore_copy_t1 from '$scripts_dir/data/cstore_copy_t1.data' delimiter '|'; checkpoint;"

#kill the standby_node_name
kill_standby

#copy data(25M) to dummy standby
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy cstore_copy_t1 from '$scripts_dir/data/cstore_copy_t1.data' delimiter '|';copy cstore_copy_t1 from '$scripts_dir/data/cstore_copy_t1.data' delimiter '|';copy cstore_copy_t1 from '$scripts_dir/data/cstore_copy_t1.data' delimiter '|';copy cstore_copy_t1 from '$scripts_dir/data/cstore_copy_t1.data' delimiter '|';copy cstore_copy_t1 from '$scripts_dir/data/cstore_copy_t1.data' delimiter '|'; checkpoint;"

#catchup failed because of bcm
chmod 000 $data_dir/datanode1/*bcm
#start the standby_node_name
start_standby
sleep 20

kill_standby
sleep 10

chmod 600 $data_dir/datanode1/*bcm

#start the standby_node_name
start_standby

check_replication_setup
wait_catchup_finish

#test the copy results on dn1_standby
if [ $(gsql -d $db -p $dn1_standby_port -m -c "select count(1) from cstore_copy_t1;" | grep `expr 1 \* $cstore_rawdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_standby cstore_copy_t1"
else
	echo "copy $failed_keyword on dn1_standby cstore_copy_t1"
	exit 1
fi
}

function tear_down()
{
#let llt to collect the data 
stop_primary
start_primary
sleep 10
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists cstore_copy_t1;"
}

test_1
tear_down