#!/bin/bash
# Copyright (c) 2020 Huawei Technologies Co.,Ltd.
#
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
#         http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
#-------------------------------------------------------------------------
#
# initialize_certificate.sh
#    initialize certificate
#
# IDENTIFICATION
#    src/gausskernel/dbmind/tools/A-Detection/shell/initialize_certificate.sh
#
#-------------------------------------------------------------------------
source ./common.sh

SERVER="certificate/server"
AGENT="certificate/agent"
CA="certificate/ca"
PW_FILE="certificate/pwf"

if [ ! -d ${CURRENT_DIR}/${SERVER} ]; then
    mkdir -p  ${CURRENT_DIR}/${SERVER}
fi

if [ ! -d ${CURRENT_DIR}/${AGENT} ]; then
    mkdir -p ${CURRENT_DIR}/${AGENT}
fi

if [ ! -d ${CURRENT_DIR}/${CA} ]; then
    mkdir -p ${CURRENT_DIR}/${CA}
fi

