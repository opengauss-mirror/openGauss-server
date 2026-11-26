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
  build_pattern="build completed"
  echo "start main cluster success!"
  echo "start standby cluster"
  build_result=`gs_ctl build -Z single_node -D ${standby4_data_dir} -b standby_full -M cascade_standby`
  echo "repair cascade_standby"
  if [[ $build_result =~ $build_pattern ]]
  then
    echo "repair standby4 success!"
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
