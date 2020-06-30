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
# setArmOptimization.sh
#    script to drop system caches
#
# IDENTIFICATION
#    src/bin/scripts/setArmOptimization.sh
#
#-------------------------------------------------------------------------
set -e

#systemctl disable irqbalance
#systemctl stop irqbalance
SUDO_FOLDER=$(dirname $(readlink -f "$0"))

if [ ! -f $SUDO_FOLDER/gs_checkos ] ; then
    exit 1
fi
python $SUDO_FOLDER/gs_checkos -i B5

bash $SUDO_FOLDER/bind_net_irq.sh
