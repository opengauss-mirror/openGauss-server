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
# IDENTIFICATION
#    src/gausskernel/dbmind/tools/A-Detection/tools/stop.sh
#
#-------------------------------------------------------------------------
source ./common.sh


function usage()
{
    echo "usage: $0 [option]
         --help
         --stop_local_service [role, {agent,server,monitor}]
         --stop_remote_service [user] [host] [password] [project_path] [role, {agent,server,monitor}]
         "
}


function stop_local_service()
{
    local role=$1
    cd ${CURRENT_DIR}
    python main.py stop --role ${role}
    return 0
}


function stop_remote_service()
{
    local user=$1
    local host=$2
    local password=$3
    local project_path=$4
    local role=$5
    local port=22

expect <<-EOF
    spawn ssh ${host} -p ${port} -l ${user}
    expect {
       "(yes/no)?" {
           send "yes\r"
           expect "*assword:"
           send "${password}\r"
       }
       "*assword:" {
           send "${password}\r"
       }
       "Last login:" {
           send "\r"
       }

    }
    send "\r"
    expect "*]*"
    send "cd ${project_path}\r"
    expect "*]*"
    send "python main.py stop --role ${role}\r"
    expect "*]*"
    send "exit\r"
    expect eof
EOF
    return 0
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
            break
            ;;
        --stop_local_service)
            stop_local_service $2
            break
            ;;
        --stop_remote_service)
            stop_remote_service $2 $3 $4 $5 $6
            break
            ;;
        *)
            echo "unknown arguments"
            ;;
    esac
}


main $@
