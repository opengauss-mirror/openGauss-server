#!/bin/bash
#############################################################################
# Copyright (c): 2012-2021, Huawei Tech. Co., Ltd.
# FileName     : execute.sh
# Version      : GaussDB Kernel V500R001
# Date         : 2021-03-01
# Description  : Remote execute script
#############################################################################

function execute_remote_cmd()
{
    local host=$1
    local user=$2
    local cmd_str=`echo ${@:3}`
    read -p "password: " password
    local port=22
    local timer=120
expect <<-EOF
    set timeout ${timer}
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
       "*]*" {
           send "\r"
       }
    }
    send "\r"
    expect "*]*"
    send "${cmd_str}"
    send "\r"
    expect "*]*"
    send "exit\r"
    expect eof
EOF
}

execute_remote_cmd $@