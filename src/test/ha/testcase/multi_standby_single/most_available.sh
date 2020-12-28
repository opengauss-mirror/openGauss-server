#!/bin/sh

source ./util.sh

function test_1()
{
  set_default
  check_detailed_instance

  kill_cluster
  gs_guc set -Z datanode -D $primary_data_dir -c "synchronous_commit = on"
  gs_guc set -Z datanode -D $primary_data_dir -c "synchronous_standby_names = '2(*)'"
  gs_guc set -Z datanode -D $primary_data_dir -c "most_available_sync = on"
  start_cluster

  echo "start cluter success!"
  echo "begin to check sync status"
  check_synchronous_commit "datanode1" 2
  check_most_available "datanode1" "On" 4
  echo "start 1master-2slaves param check success"

  timeout 5 gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists test1; create table test1(id int);"
  if [ $? -eq 0 ]; then
    echo "ddl success!"
  else
    echo "ddl $failed_keyword"
    exit 1
  fi

  kill_standby
  echo "standy killed"
  timeout 5 gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists test1; create table test1(id int);"
  if [ $? -gt 0 ]; then
    echo "ddl block success!"
  else
    echo "ddl block $failed_keyword"
    exit 1
  fi

  kill_standby2
  echo "standby2 killed => most_available take effect"
  timeout 5 gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists test1; create table test1(id int);"
  if [ $? -eq 0 ]; then
    echo "ddl success!"
  else
    echo "ddl $failed_keyword"
    exit 1
  fi

  echo "set most_available off"
  kill_cluster
  gs_guc set -Z datanode -D $primary_data_dir -c "most_available_sync = off"
  start_cluster
  check_most_available "datanode1" "Off" 4
  kill_standby
  kill_standby2
  kill_standby3

  timeout 5 gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists test1; create table test1(id int);"
  if [ $? -gt 0 ]; then
    echo "ddl block success!"
  else
    echo "ddl block $failed_keyword"
    exit 1
  fi

  start_standby
  start_standby2
  start_standby3
}

function tear_down() {
  sleep 1
  set_default
  gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists test1;"
}

test_1
tear_down
