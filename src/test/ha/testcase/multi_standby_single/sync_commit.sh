#!/bin/sh

source ./util.sh

function test_1()
{
  set_default
  check_detailed_instance

  kill_cluster
  gs_guc set -Z datanode -D $primary_data_dir -c "synchronous_commit = on"
  gs_guc set -Z datanode -D $primary_data_dir -c "synchronous_standby_names = '2(*)'"
  gs_guc set -Z datanode -D $primary_data_dir -c "most_available_sync = off"
  start_cluster

  echo "check 2-sync slaves"
  check_synchronous_commit "datanode1" 2

  echo "2 sync standbys alive => ddl success"
  timeout 5 gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists test1; create table test1(id int);"
  if [ $? -eq 0 ]; then
    echo "ddl success!"
  else
    echo "ddl $failed_keyword"
    exit 1
  fi

  echo "standby down => ddl hang"
  kill_standby
  kill_standby2
  timeout 5 gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists test1; create table test1(id int);"
  if [ $? -gt 0 ]; then
    echo "ddl block success!"
  else
    echo "ddl block $failed_keyword"
    #exit 1
  fi

  echo "2 sync standbys alive => ddl success"
  start_standby
  timeout 5 gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists test1; create table test1(id int);"
  if [ $? -eq 0 ]; then
    echo "ddl success!"
  else
    echo "ddl $failed_keyword"
    #exit 1
  fi

  kill_cluster
  gs_guc set -Z datanode -D $primary_data_dir -c "synchronous_commit = on"
  gs_guc set -Z datanode -D $primary_data_dir -c "synchronous_standby_names = 'ANY 2(*)'"
  gs_guc set -Z datanode -D $primary_data_dir -c "most_available_sync = off"
  start_cluster

  sleep 5
  echo "check 2-sync slaves"
  check_synchronous_commit "datanode1" 0
  check_asynchronous_commit "datanode1" 0

  kill_cluster
  gs_guc set -Z datanode -D $primary_data_dir -c "synchronous_commit = off"
  gs_guc set -Z datanode -D $primary_data_dir -c "synchronous_standby_names = '2(*)'"
  start_cluster

  sleep 5
  check_asynchronous_commit "datanode1" 4

  echo "kill standby => ddl success"
  kill_standby
  timeout 5 gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists test1; create table test1(id int);"
  if [ $? -eq 0 ]; then
    echo "ddl success!"
  else
    echo "ddl $failed_keyword"
    #exit 1
  fi

  echo "kill standby2 => ddl success"
  kill_standby2
  timeout 5 gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists test1; create table test1(id int);"
  if [ $? -eq 0 ]; then
    echo "ddl success!"
  else
    echo "ddl $failed_keyword"
    #exit 1
  fi

   kill_cluster
  gs_guc set -Z datanode -D $primary_data_dir -c "synchronous_commit = on"
  gs_guc set -Z datanode -D $primary_data_dir -c "synchronous_standby_names = '2(standbynode1,standbynode2)'"
  start_cluster

  sleep 5
  check_asynchronous_commit "datanode1" 4

}

function tear_down() {
  sleep 1
  set_default
  gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists test1;"
}

test_1
tear_down
