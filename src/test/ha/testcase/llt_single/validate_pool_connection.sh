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

#prepare
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test1; CREATE TABLE mpp_test1(id INT,name VARCHAR(15) NOT NULL);"
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test2; CREATE TABLE mpp_test2(id INT,name VARCHAR(15) NOT NULL) with (orientation = column);"

gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test1 from '$scripts_dir/data/data5';"
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test2 from '$scripts_dir/data/data5';"

gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test1 from '$scripts_dir/data/data5';" &
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test2 from '$scripts_dir/data/data5';" &
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test1 from '$scripts_dir/data/data5';" &
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test2 from '$scripts_dir/data/data5';" &

sleep 1

#update ha relation in pgxc_node
gsql -d $db -p $dn1_primary_port -c "update pgxc_node set hostis_primary=false where node_type = 'D'"
#check validate connection
gsql -d $db -p $dn1_primary_port -c "select * from pg_get_invalid_backends;"
#validate connection
gsql -d $db -p $dn1_primary_port -c "select * from pg_pool_validate(true, ' ');"
#llt test
gsql -d $db -p $dn1_primary_port -c "start transaction; select * from pg_pool_validate(false, ' ');"
gsql -d $db -p $dn1_primary_port -c "select * from pg_pool_validate(false, '');"


#test the copy results on cn
#if [ $(gsql -d $db -p $dn1_primary_port -c "select count(1) from mpp_test1;" | grep `expr 3 \* $rawdata_lines`  | wc -l) -eq 1 ]; then
#	echo "copy success on dn1_primary mpp_test1"
#else
#	echo "copy $failed_keyword on dn1_primary mpp_test1"
#	exit 1
#fi

#if [ $(gsql -d $db -p $dn1_primary_port -c "select count(1) from mpp_test2;" | grep `expr 3 \* $rawdata_lines` |wc -l) -eq 1 ]; then
#	echo "copy success on dn1_primary mpp_test2"
#else
#	echo "copy $failed_keyword on dn1_primary mpp_test2"
#	exit 1
#fi
}

function tear_down()
{
sleep 1
gsql -d $db -p $dn1_primary_port -c "update pgxc_node set hostis_primary=true where node_type = 'D'"
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test1;"
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test2;"
}

test_1
tear_down
