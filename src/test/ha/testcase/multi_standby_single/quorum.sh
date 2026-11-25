#!/bin/sh
# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# openGauss-server is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.


source ./util.sh

function test_1()
{
  set_default
  check_detailed_instance

  kill_cluster
  gs_guc set -Z datanode -D $primary_data_dir -c "synchronous_commit = on"
  gs_guc set -Z datanode -D $primary_data_dir -c "synchronous_standby_names = '2(*)'"
  gs_guc set -Z datanode -D $primary_data_dir -c "most_available_sync = off"

  #gs_guc set -Z datanode -D $primary_data_dir -c "application_name = 'datanode1'"

  start_cluster

  echo "check 2-sync slaves"
  check_synchronous_commit "datanode1" 2


  kill_cluster
  gs_guc set -Z datanode -D $primary_data_dir -c "synchronous_commit = on"
  gs_guc set -Z datanode -D $primary_data_dir -c "synchronous_standby_names = 'ANY 1(standby1,standby2)'"
  gs_guc set -Z datanode -D $primary_data_dir -c "most_available_sync = off"

  start_cluster

  echo "check 2-quorum slaves"
  check_quorum_synchronous_commit "datanode1" 2


  echo "execute ddl statement"
  timeout 5 gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists test1; create table test1(id int);"
  if [ $? -eq 0 ]; then
    echo "ddl success!"
  else
    echo "ddl $failed_keyword"
    exit 1
  fi
 
  #echo "check 2-quorum slaves"
  kill_cluster
  gs_guc set -Z datanode -D $primary_data_dir -c "synchronous_commit = on"
  gs_guc set -Z datanode -D $primary_data_dir -c "synchronous_standby_names = 'ANY 2(standby1,standby2,standby3)'"
  gs_guc set -Z datanode -D $primary_data_dir -c "most_available_sync = off"
  #gs_guc set -Z datanode -D $primary_data_dir -c "application_name = 'datanode1'"
  start_cluster

  echo "check 3-quorum slaves"
  check_quorum_synchronous_commit "datanode1" 3

  echo "check 2-sync slaves"
  kill_cluster
  gs_guc set -Z datanode -D $primary_data_dir -c "synchronous_commit = on"
  gs_guc set -Z datanode -D $primary_data_dir -c "synchronous_standby_names = 'FIRST 2(standby1,standby2,standby3)'"
  gs_guc set -Z datanode -D $primary_data_dir -c "most_available_sync = off"
  #gs_guc set -Z datanode -D $primary_data_dir -c "application_name = 'datanode1'"
  start_cluster

  echo "check 2-sync slaves"
  check_synchronous_commit "datanode1" 2

  kill_cluster
  gs_guc set -Z datanode -D $primary_data_dir -c "synchronous_commit = on"
  gs_guc set -Z datanode -D $primary_data_dir -c "synchronous_standby_names = 'ANY 1(*)'"
  gs_guc set -Z datanode -D $primary_data_dir -c "most_available_sync = off"
  #gs_guc set -Z datanode -D $primary_data_dir -c "application_name = 'datanode1'"
  start_cluster

  echo "check 4-quorum slaves"
  check_quorum_synchronous_commit "datanode1" 4

  kill_cluster
  gs_guc set -Z datanode -D $primary_data_dir -c "synchronous_commit = on"
  gs_guc set -Z datanode -D $primary_data_dir -c "synchronous_standby_names = 'ANY 1(standby1,standby2,standby3)'"
  gs_guc set -Z datanode -D $primary_data_dir -c "most_available_sync = off"
  #gs_guc set -Z datanode -D $primary_data_dir -c "application_name = 'datanode1'"
  start_cluster

  echo "check 3-quorum slaves"
  check_quorum_synchronous_commit "datanode1" 3


  kill_cluster
  gs_guc set -Z datanode -D $primary_data_dir -c "synchronous_commit = on"
  gs_guc set -Z datanode -D $primary_data_dir -c "synchronous_standby_names = 'FIRST 1(*)'"
  gs_guc set -Z datanode -D $primary_data_dir -c "most_available_sync = off"
  #gs_guc set -Z datanode -D $primary_data_dir -c "application_name = 'datanode1'"
  #gs_guc set -Z datanode -D $standby1_data_dir -c "application_name = 'standby1'"
  #gs_guc set -Z datanode -D $standby2_data_dir -c "application_name = 'standby2'"
  start_cluster

  echo "check 1-sync slaves"
  check_synchronous_commit "datanode1" 1

  #below case is invalid and will make the testing pending
  #kill_cluster
  #gs_guc set -Z datanode -D $primary_data_dir -c "synchronous_commit = on"
  #gs_guc set -Z datanode -D $primary_data_dir -c "synchronous_standby_names = 'ANY (standby1)'"
  #gs_guc set -Z datanode -D $primary_data_dir -c "most_available_sync = off"
  #gs_guc set -Z datanode -D $primary_data_dir -c "application_name = 'datanode1'"
  #gs_guc set -Z datanode -D $standby_data_dir -c "application_name = 'standby1'"
  #gs_guc set -Z datanode -D $standby2_data_dir -c "application_name = 'standby2'"
  #gs_guc set -Z datanode -D $standby3_data_dir -c "application_name = 'standby3'"
  #start_cluster

  #echo "check 0-quorum slaves--invalid settings"
  #check_quorum_synchronous_commit "datanode1" 0

  #kill_cluster
  #gs_guc set -Z datanode -D $primary_data_dir -c "synchronous_commit = on"
  #gs_guc set -Z datanode -D $primary_data_dir -c "synchronous_standby_names = 'FIRST (standby1)'"
  #gs_guc set -Z datanode -D $primary_data_dir -c "most_available_sync = off"
  #gs_guc set -Z datanode -D $primary_data_dir -c "application_name = 'datanode1'"
  #gs_guc set -Z datanode -D $standby_data_dir -c "application_name = 'standby1'"
  #gs_guc set -Z datanode -D $standby2_data_dir -c "application_name = 'standby2'"
  #gs_guc set -Z datanode -D $standby3_data_dir -c "application_name = 'standby3'"
  #start_cluster

  
  #echo "check 0-sync slaves--invalid settings"
  #check_synchronous_commit "datanode1" 0


  kill_cluster
  gs_guc set -Z datanode -D $primary_data_dir -c "synchronous_commit = off"
  gs_guc set -Z datanode -D $primary_data_dir -c "synchronous_standby_names = '2(*)'"
  start_cluster

  check_asynchronous_commit "datanode1" 4
  
  
  #echo "kill standby => ddl success"
  #kill_standby
  #timeout 5 gsql -d $db -p $cn1_port -c "DROP TABLE if exists test1; create table test1(id int);"
  #if [ $? -eq 0 ]; then
  #  echo "ddl success!"
  #else
   # echo "ddl $failed_keyword"
   # exit 1
  #fi

  #echo "kill standby2 => ddl success"
  #kill_standby2
  #timeout 5 gsql -d $db -p $cn1_port -c "DROP TABLE if exists test1; create table test1(id int);"
  #if [ $? -eq 0 ]; then
  #  echo "ddl success!"
  #else
  #  echo "ddl $failed_keyword"
  #  exit 1
  #fi

}

function tear_down() {
  sleep 1
  set_default
  gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists test1;"
}

test_1
tear_down
