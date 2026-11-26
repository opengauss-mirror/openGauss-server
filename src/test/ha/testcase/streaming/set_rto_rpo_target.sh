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
  gs_guc set -D $primary_data_dir -Z datanode -c "hadr_recovery_time_target=30"
  gs_guc set -D $primary_data_dir -Z datanode -c "hadr_recovery_point_target=30"
  start_streaming_cluster
  echo "start cluter success!" 
  echo "match guc parameter"
  gsql -d postgres -p ${dn1_primary_port} -c "select * from gs_hadr_local_rto_and_rpo_stat();"
}

function tear_down() {
  stop_streaming_cluster
}

test_1
tear_down
