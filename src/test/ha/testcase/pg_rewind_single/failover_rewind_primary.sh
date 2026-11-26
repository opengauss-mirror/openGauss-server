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


source ./standby_env.sh

function test_1()
{
check_instance

#failover the standby, so we have two diferent primary
gs_ctl failover -D $data_dir/datanode1_standby

sleep 5

#check and insure they have no relationship now
gs_ctl query -D $data_dir/datanode1
gs_ctl query -D $data_dir/datanode1_standby

kill_primary

#use incremental to mirror the primary on standby, sync the diferent part between them.
gs_ctl build -D $data_dir/datanode1 -Z single_node -b incemental

#check if rewind is ok.
#start_primary_as_standby
check_replication_setup_for_primary
}

function tear_down()
{
sleep 1
gs_ctl switchover -D $data_dir/datanode1
check_replication_setup
}

test_1
tear_down
