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
  start_cluster
  build_pattern="build completed"
  echo "start main cluter success!"
  kill_standby3
  kill_standby4  
  echo "start hadr_main_standby"
  build_result=`gs_ctl build -Z single_node -D ${standby3_data_dir} -b full -M hadr_main_standby`
  echo "build and start hadr_main_standby"
  if [[ $build_result =~ $build_pattern ]]
  then
    echo "build hadr_main_standby success!"
  else
    echo "$failed_keyword, build standby3 failed!"
    exit 1
  fi
  echo "start cascade_standby for main_standby"
  build1_result=`gs_ctl build -Z single_node -D ${standby4_data_dir} -b standby_full -M cascade_standby`
  echo "build and start cascade_standby for main_standby"
  if [[ $build1_result =~ $build_pattern ]]
  then
    echo "build cascade_standby success!"
  else
    echo "$failed_keyword, build standby4 failed!"
    exit 1
  fi
}

function tear_down() {
  stop_streaming_cluster
}

test_1
tear_down
