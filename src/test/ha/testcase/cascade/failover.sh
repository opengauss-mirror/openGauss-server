#!/bin/sh

source ./util.sh

function test_1()
{
  set_cascade_default
  check_instance_cascade_standby
  cstore_rawdata_lines=8

  echo "cluster setup completed----------------"
  #create table
  #gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy cstore_copy_t1 from '$scripts_dir/data/cstore_copy_t1.data' delimiter '|';"
  #gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists cstore_copy_t1; create table cstore_copy_t1(c1 int2, c2 int4, c3 int8, c4 char(10), c5 varchar(12),c6 numeric(10,2)) with (orientation = column);"
  gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists cstore_copy_t1; create table cstore_copy_t1(c1 int2, c2 int4, c3 int8, c4 char(10), c5 varchar(12),c6 numeric(10,2));"

  #copy data(25M) to primary
  cat $scripts_dir'/data/cstore_copy_t1.data' | python tools.py cstore_copy_t1 '|' > $scripts_dir'/data/cstore_copy_t1.data.sql'
  gsql -d $db -p $dn1_primary_port < $scripts_dir'/data/cstore_copy_t1.data.sql'
  echo "copy success"

  echo "begin to kill primary"
  #kill the primary_node_name
  kill_primary

  #failover standby to primary
  failover_to_standby

  echo  "begin to query standby"
  query_standby

  #test the copy results on dn1_standby
  if [ $(gsql -d $db -p $dn1_standby_port -c "select pgxc_pool_reload();select count(1) from cstore_copy_t1;" | grep `expr 1 \* $cstore_rawdata_lines` |wc -l) -eq 1 ]; then
    echo "copy success on dn1_standby cstore_copy_t1"
  else
    echo "copy $failed_keyword on dn1_standby cstore_copy_t1"
    exit 1
  fi

  #failover cascade_standby to standby
  failover_to_cascade_standby

  echo  "begin to query cascade_standby"
  query_cascade_standby

  #test the copy results on dn1_standby
  if [ $(gsql -d $db -p $standby2_port -c "select pgxc_pool_reload();select count(1) from cstore_copy_t1;" | grep `expr 1 \* $cstore_rawdata_lines` |wc -l) -eq 1 ]; then
    echo "copy success on cascade_standby cstore_copy_t1"
  else
    echo "copy $failed_keyword on dn1_standby cstore_copy_t1"
    exit 1
  fi




  start_primary_as_cascade_standby
  sleep 5
  switchover_to_primary

  #test the copy results on dn1_primary
  if [ $(gsql -d $db -p $dn1_primary_port -c "select pgxc_pool_reload();select count(1) from cstore_copy_t1;" | grep `expr 1 \* $cstore_rawdata_lines` |wc -l) -eq 1 ]; then
    echo "copy success on dn1_primary cstore_copy_t1"
  else
    echo "copy $failed_keyword on dn1_primary cstore_copy_t1"
    exit 1
  fi

  #test the copy results on cascade_standby
  if [ $(gsql -d $db -p $standby2_port -m -c "select count(1) from cstore_copy_t1;" | grep `expr 1 \* $cstore_rawdata_lines` |wc -l) -eq 1 ]; then
    echo "copy success on cascade_standby cstore_copy_t1"
  else
    echo "copy $failed_keyword on dn1_standby cstore_copy_t1"
    exit 1
  fi
}

function tear_down()
{
  set_cascade_default
  sleep 1
  gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists cstore_copy_t1;"
}

test_1
tear_down
