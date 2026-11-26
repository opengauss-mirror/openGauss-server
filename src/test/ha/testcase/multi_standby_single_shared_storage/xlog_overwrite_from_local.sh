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

# to make sure that standby will use inc build after down and reconnect
source ./util.sh

function test_1()
{
  kill_primary_cluster
  kill_standby_cluster
  sleep 5
  overwrite_pattern="overwrite xlog: success"

  echo "overwrite xlog when local is greater than share"
  echo "truncate file to 100M"
  truncate -s 100M ${data_dir}/shared_disk
  overwrite_result1=`gs_ctl copy -Q copy_from_local -D ${primary_data_dir}`
  if [[ $overwrite_result1 =~ $overwrite_pattern ]]
  then
    echo "xlog copy from local success"
  else
    echo "xlog copy from local $failed_keyword"
  fi

  echo "force overwrite xlog from local to shared storage"
  gs_initdb -D ${data_dir}/tmp --nodename=datanode_tmp -w Gauss@123 -g ${data_dir}/shared_disk_tmp
  rm -rf ${data_dir}/tmp
  sleep 20
  mv ${data_dir}/shared_disk_tmp ${data_dir}/shared_disk
  overwrite_result2=`gs_ctl copy -Q force_copy_from_local -D ${primary_data_dir}`
  if [[ $overwrite_result2 =~ $overwrite_pattern ]]
  then
    echo "xlog force copy from local success"
  else
    echo "xlog force copy from local $failed_keyword"
  fi
  
  echo "overwrite xlog when local is smaller than share"
  start_primary_as_primary
  sleep 30
  stop_primary
  cp ${data_dir}/shared_disk ${data_dir}/shared_disk_copy
  overwrite_result3=`gs_ctl copy -Q force_copy_from_local -D ${standby_data_dir}`
  if [[ $overwrite_result3 =~ $overwrite_pattern ]]
  then
    echo "xlog copy from local success"
  else
    echo "xlog copy from local $failed_keyword"
  fi
  mv ${data_dir}/shared_disk_copy ${data_dir}/shared_disk
}

function tear_down() {
  sleep 5
}

test_1
tear_down
