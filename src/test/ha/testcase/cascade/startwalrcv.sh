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
  set_cascade_default
  kill_cascade_cluster
  start_cascade_cluster

  receiver_keyword="receiver_replay_location"
  echo "query primary"
  if [ $(query_primary | grep $receiver_keyword | wc -l) -ne 1 ]; then
    echo "$failed_keyword when query primary"
  fi

  gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists testapplydelay;"
  gs_guc reload -D $standby_data_dir -c "recovery_min_apply_delay=900s"

  gsql -d $db -p $dn1_primary_port -c "start transaction;
    create table testapplydelay(id integer);
    insert into testapplydelay values(1);
    commit;"

  kill_primary
  echo "primary killed"
  sleep 60
  start_primary
  echo "primary started"
  if [ $(query_primary | grep $receiver_keyword | wc -l) -ne 0 ]; then
    echo "$failed_keyword when query primary"
  fi

  echo "startwalrcv standby"
  startwalrcv_standby
  if [ $(query_primary | grep $receiver_keyword | wc -l) -ne 1 ]; then
    echo "$failed_keyword when query primary"
  fi
}

function tear_down()
{
  sleep 1
  gs_guc reload -D $standby_data_dir -c "recovery_min_apply_delay=0"
  gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists testapplydelay;"
}

test_1
tear_down
