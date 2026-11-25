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
  kill_cluster
  start_streaming_cluster
  echo "start cluter success!"
  kill_standby3
  echo "hadr main standby killed"
  gs_ctl failover -Z single_node -D $standby4_data_dir
  echo "failover_to_cascade_standby"
  sleep 5
  check_dn_state "datanode4_standby" "db_state" "Normal" 1
}

function tear_down() {
  stop_streaming_cluster
}

test_1
tear_down
