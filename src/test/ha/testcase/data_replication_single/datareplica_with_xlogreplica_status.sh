#!/bin/sh

source ./standby_env.sh

function test_1()
{
check_instance

cstore_rawdata_lines=8

#create table

gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists cstore_copy_t1; create table cstore_copy_t1(c1 int2, c2 int4, c3 int8, c4 char(10), c5 varchar(12),c6 numeric(10,2)) with (orientation = column);"

gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=off; copy cstore_copy_t1 from '$scripts_dir/data/cstore_copy_t1.data' delimiter '|';"

#test the copy results on dn1_primary
if [ $(gsql -d $db -p $dn1_primary_port -c "select pgxc_pool_reload();select count(1) from cstore_copy_t1;" | grep `expr 1 \* $cstore_rawdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_primary cstore_copy_t1"
else
	echo "copy $failed_keyword on dn1_primary cstore_copy_t1"
	exit 1
fi

#test the copy results on dn1_standby
if [ $(gsql -d $db -p $dn1_standby_port -m -c "select count(1) from cstore_copy_t1;" | grep `expr 1 \* $cstore_rawdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_standby cstore_copy_t1"
else
	echo "copy $failed_keyword on dn1_standby cstore_copy_t1"
	exit 1
fi
# stop primary 
stop_primary
# sleep
sleep 10

#test the copy status on dn1_standby
if [ $(gsql -d $db -p $dn1_standby_port -m -c "select  gs_get_recv_locations();" | awk -F'|'  'NR==3 {print $4}') == '0/0' ]; then
	echo " dn1_standby recv status is wrong."
	exit 1
else
	echo " dn1_standby recv status is normal."
fi

stop_standby

sleep 10

start_standby

 #test the copy status on dn1_standby
if [ $(gsql -d $db -p $dn1_standby_port -m -c "select  gs_get_recv_locations();" | awk -F'|'  'NR==3 {print $4}') == '0/0' ]; then
	echo " dn1_standby recv status is wrong."
	exit 1
else
	echo " dn1_standby recv status is normal."
fi

}

function tear_down()
{
sleep 1
start_primary
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists cstore_copy_t1;"
}

test_1
tear_down