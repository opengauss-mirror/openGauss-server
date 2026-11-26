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

# switchover when primary-standby-dummystandby all ready

source ./util.sh

function test_1()
{
  set_default
  check_detailed_instance
  #create table
  gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test1; CREATE TABLE mpp_test1(id INT,name VARCHAR(15) NOT NULL);"
  echo "drop table success"

  #prepare insert sql
  cat $scripts_dir'/data/data5_head_100' | ./run_python.sh tools.py mpp_test1 '|' > $scripts_dir'/data/data5_head_100_sql'
  gsql -d $db -p $dn1_primary_port < $scripts_dir'/data/data5_head_100_sql' &> /dev/null

  #test the insert results
  b=`wc $scripts_dir'/data/data5_head_100_sql' | awk '{print $1}'`
  echo "b=" $b
  if [ $(gsql -d $db -p $dn1_primary_port -c "select count(1) from mpp_test1;" | grep $b | wc -l) -eq 1 ]; then
    echo "insert into table success!"
  else
    echo "insert into table failure $failed_keyword!"
    exit 1
  fi

  sleep 1

  echo "begin to switch to standby1 with timeout 1s"
  #switchover
  switchover_to_standby 1
  echo "end of switch to standby1 with timeout 1s"

  sleep 60

  echo "begin to switch to standby2 with timeout 2s"
  #switchover
  switchover_to_standby2 2
  echo "end of switch to standby2"

  sleep 60

  echo "begin to switch to standby3 with timeout 3s"
  #switchover
  switchover_to_standby3 3
  echo "end of switch to standby3"

  sleep 60

  #test the insert results
  b=`wc $scripts_dir'/data/data5_head_100_sql' | awk '{print $1}'`
  if [ $(gsql -d $db -p $standby3_port -c "select count(1) from mpp_test1;" | grep $b | wc -l) -eq 1 ]; then
    echo "test insert result success!"
  else
    echo "test insert result, $failed_keyword!"
    exit 1
  fi
}

function tear_down()
{
  set_default
  sleep 3
  gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test1;"
}

test_1
tear_down
