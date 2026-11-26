#!/usr/bin/env bash
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


set -e
set -o pipefail

SCRIPT_DIR=$(dirname $0)

TAGS="0.7.0-pg10 0.7.1-pg10 0.8.0-pg10 0.9.0-pg10 0.9.1-pg10 0.9.2-pg10 0.10.0-pg10 0.10.1-pg10 0.11.0-pg10 0.12.0-pg10 1.0.0-pg10 1.0.1-pg10 1.1.0-pg10 1.1.1-pg10 1.2.0-pg10 1.2.1-pg10 1.2.2-pg10"
TEST_VERSION="v2"

TAGS=$TAGS TEST_VERSION=$TEST_VERSION bash ${SCRIPT_DIR}/test_updates.sh
EXIT_CODE=$?
if [ $EXIT_CODE -ne 0 ]; then
  exit $EXIT_CODE
fi


TAGS="1.3.0-pg10 1.3.1-pg10 1.3.2-pg10 1.4.0-pg10 1.4.1-pg10 1.4.2-pg10"
TEST_VERSION="v3"

TAGS=$TAGS TEST_VERSION=$TEST_VERSION bash ${SCRIPT_DIR}/test_updates.sh
EXIT_CODE=$?
if [ $EXIT_CODE -ne 0 ]; then
  exit $EXIT_CODE
fi

TAGS="1.5.0-pg10 1.5.1-pg10 1.6.0-pg10 1.6.1-pg10"
TEST_VERSION="v5-pg10"

TAGS=$TAGS TEST_VERSION=$TEST_VERSION bash ${SCRIPT_DIR}/test_updates.sh
EXIT_CODE=$?
if [ $EXIT_CODE -ne 0 ]; then
  exit $EXIT_CODE
fi

TAGS="1.7.0-pg10 1.7.1-pg10 1.7.2-pg10"
TEST_VERSION="v6-pg10"

TAGS=$TAGS TEST_VERSION=$TEST_VERSION bash ${SCRIPT_DIR}/test_updates.sh
EXIT_CODE=$?
if [ $EXIT_CODE -ne 0 ]; then
  exit $EXIT_CODE
fi
