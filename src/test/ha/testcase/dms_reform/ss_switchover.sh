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


source ./../ss/ss_database_build_env.sh

# switchover test
test_1()
{
    sleep 5
   
    # switchover
    ${GAUSSHOME}/bin/gs_ctl switchover -D ${SS_DATA}/dn1
    if [ $? -eq 0 ]; then
        echo "all of success!"
    else
	echo "$failed_keyword"
	exit 1
    fi

    sleep 10
}

tear_down()
{
    sleep 10
    #switchover
    ${GAUSSHOME}/bin/gs_ctl switchover -D  ${SS_DATA}/dn0
    if [ $? -eq 0 ]; then
        echo "all of success!"
    else
        echo "$failed_keyword"
        exit 1
    fi 
}

test_1
tear_down

