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
  gs_guc set -D $primary_data_dir -Z datanode -c "synchronous_standby_names='ANY 1(dn_s1, dn_s2)'"
  start_streaming_cluster
  echo "start cluter success!"
  kill_standby
  kill_standby2
  echo "standby and standby2 have been killed"
  echo "check main standby status"
  check_dn_state "datanode3_standby" "db_state" "Need repair" 1
  gsql -d postgres -p ${standby3_port} -c "select * from pg_stat_get_wal_receiver();"
}

function tear_down() {
  stop_streaming_cluster
}

test_1
tear_down
