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
# date: 2021-12-22
# version: 1.0

if ! source ~/.bashrc; then
  echo "Error: check ~/.bashrc first"
  exit 1
fi
CURRENT_DIR=$(
  cd $(dirname $0)
  pwd
)

source ${CURRENT_DIR}/upgrade_config.sh
source ${CURRENT_DIR}/upgrade_common.sh

function main() {
  check_and_init
  parse_cmd_line $@
  case "${action}" in
  upgrade_pre)
    upgrade_pre
    exit 0
    ;;
  upgrade_bin)
    upgrade_bin
    exit 0
    ;;
  upgrade_post)
    upgrade_post
    exit 0
    ;;
  rollback_pre)
    rollback_pre
    exit 0
    ;;
  rollback_bin)
    rollback_bin
    exit 0
    ;;
  rollback_post)
    rollback_post
    exit 0
    ;;
  upgrade_commit)
    upgrade_commit
    exit 0
    ;;
  switch_over)
    switch_over
    exit 0
    ;;
  *)
    log "please input right parameter, the following command may help you"
    log "sh upgrade_GAUSSV5.sh --help or sh upgrade_GAUSSV5.sh -h"
    die "Must input parameter -t action" ${err_parameter}
    ;;
  esac
}
main $@
