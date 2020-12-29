#!/bin/sh

source ./util.sh

function test_1()
{
set_default
check_instance_multi_standby
cstore_rawdata_lines=8

query_primary
#create table
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists cstore_copy_t1; create table cstore_copy_t1(c1 int2, c2 int4, c3 int8, c4 char(10), c5 varchar(12),c6 numeric(10,2));"

gsql -d $db -p $dn1_primary_port -c "copy cstore_copy_t1 from '$scripts_dir/data/cstore_copy_t1.data' delimiter '|';"

#test the copy results on dn1_primary
if [ $(gsql -d $db -p $dn1_primary_port -c "select count(1) from cstore_copy_t1;" | grep `expr 1 \* $cstore_rawdata_lines` |wc -l) -eq 1 ]; then
  echo "copy success on dn1_primary cstore_copy_t1"
else
  echo "copy $failed_keyword on dn1_primary cstore_copy_t1"
  exit 1
fi


#kill the standby_node_name
kill_standby


#copy data(25M) to primary
gsql -d $db -p $dn1_primary_port -c "copy cstore_copy_t1 from '$scripts_dir/data/cstore_copy_t1.data' delimiter '|';"


#kill the primary_node_name
kill_primary
echo "kill primary success"

echo `date`

#failover dummystandby to primary
#gs_ctl failover -w -t $gsctl_wait_time -D $data_dir/datanode1_dummystandby
echo "begin to failover to standby2"
failover_to_standby2
echo "failover to standby2 success"

echo `date`

sleep 5

echo "query standby2 status"
query_standby2
#start the standby_node_name
start_standby
echo "start standby success"
#notify_standby_as_standby
query_standby

sleep 2

echo `date`

#sh testcase/multi_standby/datareplica_cstore_failover.sh
gs_ctl build -D $data_dir/datanode1_standby -Z single_node

echo "after build----"

sleep 5

query_standby

echo "build success"


#test the copy results on dn1_standby
if [ $(gsql -d $db -p $dn1_standby_port -m -c "select count(1) from cstore_copy_t1;" | grep `expr 2 \* $cstore_rawdata_lines` |wc -l) -eq 1 ]; then
  echo "copy success on dn1_standby cstore_copy_t1"
else
  echo "copy $failed_keyword on dn1_standby cstore_copy_t1"
  exit 1
fi

start_primary_as_standby
build_primary_as_standby
sleep 10
#gs_ctl switchover -w -t $gsctl_wait_time -D $data_dir/datanode1
switchover_to_primary

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
  set_default
  sleep 1
  gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists cstore_copy_t1;"
}

test_1
tear_down
