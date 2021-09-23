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
# stop.sh
#    stop script of A-Detection
#
#-------------------------------------------------------------------------
source ./common.sh


function usage()
{
    echo "usage: $0 [option]
         --help
         --stop_local_service [role, {agent,server,monitor}]
         --stop_remote_service [host] [user] [project_path] [role, {agent,server,monitor}]
         "
}


function stop_local_service()
{
    local role=$1

    cd ${CURRENT_DIR}
    python main.py stop --role ${role}
    return $?
}


function main()
{
    if [ $# -eq 0 ]; then
        usage
        exit 1
    fi

    case "$1" in
        --help)
            usage
            exit 0
            ;;
        --stop_local_service)
            stop_local_service $2
            exit $?
            ;;
        --stop_remote_service)
            send_ssh_command $2 $3 $SSH_PORT $4/${PROJECT_NAME} "python main.py stop --role $5"
            exit $?
            ;;
        *)
            echo "Unknown arguments"
            exit 1
            ;;
    esac
}


main $@
