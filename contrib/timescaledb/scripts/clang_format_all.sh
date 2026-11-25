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


# we need to convert script dir to an absolute path
SCRIPT_DIR=$(cd $(dirname $0); pwd)
BASE_DIR=$(dirname $SCRIPT_DIR)

find ${BASE_DIR} \( -path "${BASE_DIR}/src/*" -or -path "${BASE_DIR}/test/*" -or -path "${BASE_DIR}/tsl/*" \) \
    -and -not \( -path "*/.*" -or -path "*CMake*" \) \
    -and \( -name '*.c' -or -name '*.h' \) -print | xargs ${SCRIPT_DIR}/clang_format_wrapper.sh -style=file -i
