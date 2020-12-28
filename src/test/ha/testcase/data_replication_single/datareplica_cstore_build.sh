#!/bin/sh

source ./standby_env.sh

function test_1()
{
check_instance

cstore_rawdata_lines=8

#create table
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists cstore_copy_t1; create table cstore_copy_t1(c1 int2, c2 int4, c3 int8, c4 char(10), c5 varchar(12),c6 numeric(10,2)) with (orientation = column);"

#stop_standby
stop_standby

gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy cstore_copy_t1 from '$scripts_dir/data/cstore_copy_t1.data' delimiter '|';"

#build standby
gs_ctl build -Z single_node -D $data_dir/datanode1_standby

sleep 10

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
sleep 1
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists cstore_copy_t1;"
}

test_1
tear_down