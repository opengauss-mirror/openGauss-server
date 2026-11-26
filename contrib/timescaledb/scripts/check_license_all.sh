#!/bin/bash
# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# openGauss-server is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
# Copyright (c) 2017-2022 Timescale, Inc. All Rights Reserved.
# Copyright (c) 2016-2017 440 Labs Inc. dba Timescale. All Rights Reserved

SCRIPT_DIR=$(dirname $0)
BASE_DIR=$(dirname ${SCRIPT_DIR})

SRC_DIR=$BASE_DIR ${SCRIPT_DIR}/check_license.sh -c ${BASE_DIR}/src -s ${BASE_DIR}/sql -c ${BASE_DIR}/test -t ${BASE_DIR}/test
exit_apache=$?
SRC_DIR=$BASE_DIR ${SCRIPT_DIR}/check_license.sh -e ${BASE_DIR}/tsl/src -u ${BASE_DIR}/tsl/test/sql -e ${BASE_DIR}/tsl/test/src
exit_tsl=$?

exit ${exit_apache} || ${exit_tsl}
