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

# stop database
test_1()
{
    sleep 10

    # stop database
    stop_gaussdb ${SS_DATA}/dn0
    if [ $? -eq 0 ]; then
        echo "all of success!"
    else
        echo "$failed_keyword"
        exit 1
    fi

    sleep 15
    
    # start database
    start_gaussdb ${SS_DATA}/dn0
    if [ $? -eq 0 ]; then
        echo "all of success!"
    else
        echo "$failed_keyword"
        exit 1
    fi

    sleep 5
}

tear_down()
{
    sleep 2
}

test_1
tear_down