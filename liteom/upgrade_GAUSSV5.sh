#!/bin/bash
# Copyright (c) Huawei Technologies Co., Ltd. 2010-2022. All rights reserved.
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
