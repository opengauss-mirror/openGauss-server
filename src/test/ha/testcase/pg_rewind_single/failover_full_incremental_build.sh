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
#1.test the default build mode
check_instance

#failover the standby, so we have two diferent primary
gs_ctl failover -D $data_dir/datanode1_standby

#wait for dummystandy connect to new primary(datanode1_standby)
check_standby_setup

#check and insure they have no relationship now
gs_ctl query -D $data_dir/datanode1
gs_ctl query -D $data_dir/datanode1_standby

stop_primary

#use gs_rewind to mirror the stadnby to primary, sync the different part between them.
gs_ctl build -D $data_dir/datanode1 -Z single_node

#check if rewind is ok.
check_standby_as_primary_setup

#2.test the full build mode
#check_instance

#failover the primary, so we have two diferent primary
gs_ctl failover -D $data_dir/datanode1

#wait for dummystandy connect to new primary(datanode1)
check_dummy_setup

#check and insure they have no relationship now
gs_ctl query -D $data_dir/datanode1
gs_ctl query -D $data_dir/datanode1_standby

kill_standby

check_dummy_setup

#use full build to mirror the primary on standby, sync the different part between them.
gs_ctl build -D $data_dir/datanode1_standby -Z single_node -b full

#check if rewind is ok.
check_replication_setup

#3.test the incremental build mode
check_instance

#failover the standby, so we have two diferent primary
gs_ctl failover -D $data_dir/datanode1_standby

#wait for dummystandy connect to new primary(datanode1_standby)
check_standby_setup

#check and insure they have no relationship now
gs_ctl query -D $data_dir/datanode1
gs_ctl query -D $data_dir/datanode1_standby

stop_standby
rm -f $data_dir/datanode1_dummystandby/pg_xlog/0000*

kill_primary
start_primary_as_standby
gs_ctl failover -D $data_dir/datanode1 -Z single_node

check_dummy_setup

#use incremental build to mirror the primary on standby, sync the different part between them.
gs_ctl build -D $data_dir/datanode1_standby -Z single_node -b incremental

#check if rewind is ok.
check_replication_setup
}

function tear_down()
{
sleep 1
}

test_1
tear_down