#!/bin/sh
# cluster failover

source ./util.sh

function test_1()
{
  kill_primary_cluster
  kill_standby_cluster
  start_primary_cluster
  start_standby_cluster
  sleep 30
  check_detailed_instance 3 
  cstore_rawdata_lines=0

  echo "cluster setup completed----------------"
  #create table
  gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists cstore_copy_t1; create table cstore_copy_t1(c1 int2, c2 int4, c3 int8, c4 char(10), c5 varchar(12),c6 numeric(10,2));"

  echo "begin to kill primary_cluster"
  stop_primary_cluster
  cp ${data_dir}/shared_disk ${data_dir}/shared_disk_copy
  stop_standby_cluster
  sleep 5

  start_primary_cluster
  #copy data(25M) to primary
  cat $scripts_dir'/data/cstore_copy_t1.data' | python tools.py cstore_copy_t1 '|' > $scripts_dir'/data/cstore_copy_t1.data.sql'
  gsql -d $db -p $dn1_primary_port < $scripts_dir'/data/cstore_copy_t1.data.sql' &> /dev/null
  sleep 20
  kill_primary_cluster
  mv ${data_dir}/shared_disk_copy ${data_dir}/shared_disk

  #failover standby cluster to primary cluster
  echo "begin to start standby_cluster as primary cluster"
  stop_standby_cluster
  start_standby_cluster_as_primary
  
  #test the copy results on dn1_standby
  if [ $(gsql -d $db -p $standby2_port -c "select pgxc_pool_reload();select count(1) from cstore_copy_t1;" | grep `expr 1 \* $cstore_rawdata_lines` |wc -l) -eq 1 ]; then
    echo "copy success on dn2_standby cstore_copy_t1"
  else
    echo "copy $failed_keyword on dn2_standby cstore_copy_t1"
    exit 1
  fi
  
  # restore primary cluster as standby cluster
  echo "begin to start primary_cluster as standby cluster" 
  start_primary_cluster_as_standby
  check_dn_state "datanode1" "db_state" "Need repair" 1
  check_dn_state "datanode1_standby" "db_state" "Need repair" 1
  
  stop_primary_cluster
  cross_build_primary_as_standby
  cross_build_standby_as_standby
  sleep 30
  check_detailed_instance 3
}

function tear_down()
{
  gsql -d $db -p $standby2_port -c "DROP TABLE if exists cstore_copy_t1;"
  stop_standby_cluster
  stop_primary_cluster
  sleep 5
}

test_1
tear_down
