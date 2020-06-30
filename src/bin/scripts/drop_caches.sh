#!/bin/bash
#Copyright (c) 2020 Huawei Technologies Co.,Ltd.
#
#openGauss is licensed under Mulan PSL v2.
#You can use this software according to the terms and conditions of the Mulan PSL v2.
#You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
#-------------------------------------------------------------------------
#
# drop_caches.sh
#    script to drop system caches
#
# IDENTIFICATION
#    src/bin/scripts/drop_caches.sh
#
#-------------------------------------------------------------------------
set -e

total=$(free -m |grep Mem|awk '{print $2}');
threshold=$(expr $total \* 20 \/ 100);  #20%

while [ 1 ]
do
    free=$(free -m |grep Mem |awk '{print$4}');
    if [ "$free" -lt "$threshold" ]
    then
        logger -t drop_cache "free memory:"$free" threshold:"$threshold
        sync
        /sbin/sysctl -w vm.drop_caches=3
    fi

    sleep 15
    date 
done
