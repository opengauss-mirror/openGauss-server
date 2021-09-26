#!/bin/bash
#############################################################################
# Copyright (c): 2012-2021, Huawei Tech. Co., Ltd.
# FileName     : deploy.sh
# Version      : V1.0.0
# Date         : 2021-03-01
# Description  : Remote deploy script
#############################################################################

function deploy_code()
{
    local host=$1
    local user=$2
    local project_from=$3
    local project_to=$4
    read -p "password: " password
    local timer=60
expect <<-EOF
    set timeout ${timer}
    spawn scp -r ${project_from} ${user}@${host}:${project_to}
    expect {
        "(yes/no)?" {
            send "yes\r"
            expect "*assword:"
            send "${password}\r"
        }
        "*assword" {
            send "${password}\r"
        }
        "*]*" {
           send "\r"
       }
}
    expect eof
EOF
    return 0
}

deploy_code $@