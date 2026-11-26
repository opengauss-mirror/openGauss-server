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

# 1 standby3 request standby2 full build.
source ./util.sh

function test_1()
{
  set_default
  kill_cluster
  start_cluster

  echo "start cluter success!"
  inc_build_pattern="waiting for server to start..."
  echo "standby2_build_standby3"
  build_result=`gs_ctl build -b standby_full -D ${standby3_data_dir} -C "localhost=127.0.0.1 localport=$standby3_port remotehost=127.0.0.1 remoteport=$standby2_port"`
  if [[ $build_result =~ $inc_build_pattern ]]
  then
    echo "standby build success"
  else
    echo "standby build $failed_keyword"
  fi
}

function tear_down() {
  sleep 1
  kill_cluster
  start_cluster
}

test_1
tear_down
