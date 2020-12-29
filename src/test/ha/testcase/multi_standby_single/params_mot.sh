#!/bin/sh

source ./util.sh

function test_1()
{
  set_default 
  check_detailed_instance

  echo "n sync standby"
  kill_cluster
  gs_guc set -Z datanode -D $primary_data_dir -c "synchronous_commit = on"
  gs_guc set -Z datanode -D $primary_data_dir -c "synchronous_standby_names = '333(*)'"
  gs_guc set -Z datanode -D $primary_data_dir -c "most_available_sync = on"
  start_cluster

  check_synchronous_commit "datanode1" 4
  check_most_available "datanode1" "On" 4
  echo "start 1master-nslaves param check success"

  timeout 5 gsql -d $db -p $dn1_primary_port -c "DROP FOREIGN TABLE if exists test1; create FOREIGN table test1(id int) SERVER mot_server;"
  if [ $? -gt 0 ]; then
    echo "ddl block success!"
  else
    echo "ddl block failure $failed_keyword"
    exit 1
  fi

  echo "1 sync standby, 1 potential"
  kill_cluster
  gs_guc set -Z datanode -D $primary_data_dir -c "synchronous_commit = on"
  gs_guc set -Z datanode -D $primary_data_dir -c "synchronous_standby_names = '1(*)'"
  gs_guc set -Z datanode -D $primary_data_dir -c "most_available_sync = on"
  start_cluster

  check_synchronous_commit "datanode1" 1
  check_most_available "datanode1" "On" 4
  echo "start 1master-2slaves param check success"

  echo "random error config"
  kill_cluster
  gs_guc set -Z datanode -D $primary_data_dir -c "most_available_sync = doe"
  if [ $? -gt 0 ]; then
    echo "most_available_sync error tested!"
  else
    echo "most_available_sync error not tested failure $failed_keyword"
    exit 1
  fi
  gs_guc set -Z datanode -D $primary_data_dir -c "synchronous_commit = john"
  if [ $? -gt 0 ]; then
    echo "synchronous_commit error tested!"
  else
    echo "synchronous_commit error not tested failure $failed_keyword"
    exit 1
  fi
  gs_guc set -Z datanode -D $primary_data_dir -c "synchronous_standby_names = 'jane'"
  start_cluster
  check_asynchronous_commit "datanode1" 4
}

function tear_down() {
  sleep 1
  kill_cluster
  gs_guc set -Z datanode -D $primary_data_dir -c "synchronous_commit = on"
  gs_guc set -Z datanode -D $primary_data_dir -c "synchronous_standby_names = '*'"
  gs_guc set -Z datanode -D $primary_data_dir -c "most_available_sync = off"
  start_cluster
  gsql -d $db -p $dn1_primary_port -c "DROP FOREIGN TABLE if exists test1;"
}

test_1
tear_down
