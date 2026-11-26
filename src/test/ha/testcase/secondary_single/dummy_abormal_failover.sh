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

#failover failed without standby and primary

source ./standby_env.sh

function test_1()
{
check_instance

stop_primary

stop_dummystandby

#failover standby to primary failed
gs_ctl failover -w -t 30 -D  $data_dir/datanode1_standby

start_dummystandby

sleep 2

start_primary_as_standby
}

function tear_down()
{
sleep 5
#gs_ctl build -D $data_dir/datanode1
gs_ctl switchover -D  $data_dir/datanode1
}

test_1
tear_down