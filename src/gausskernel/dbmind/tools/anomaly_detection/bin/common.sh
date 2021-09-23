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
# common.sh
#    common info of A-Detection
#
#-------------------------------------------------------------------------

CURRENT_DIR=$(cd ../$(dirname $0); pwd)
PROJECT_NAME=$(basename ${CURRENT_DIR})
SSH_PORT=22


function send_ssh_command_without_pwd()
{
    local host=$1
    local user=$2
    local password=$3
    local port=$4
    local path=$5
    local cmd=$6

expect <<-EOF
    spawn ssh -o StrictHostKeyChecking=no ${host} -p ${port} -l ${user}
    expect {
        "*assword:" {
            send "${password}\r";
            expect {
                "*denied*" {exit 2;}
                eof
            }
        }
        eof {exit 1;}
    }
    send "\r"
    expect "*]*"
    send "cd ${path}\r"
    expect "*]*"
    send "${cmd}\r"
    expect "*]*"
    send "exit $?\r"
    expect eof

    catch wait result;
    exit [lindex \$result 3]
EOF

    return $?
}


function send_scp_command_without_pwd()
{
    local host=$1
    local user=$2
    local password=$3
    local dest_path=$4

expect <<-EOF
    spawn scp -o StrictHostKeyChecking=no -r ${CURRENT_DIR} ${user}@${host}:${dest_path}
    expect {
        "*assword:" {
            send "${password}\r";
            expect {
                "*denied*" {exit 2;}
                eof
            }
        }
    }

    catch wait result;
    exit [lindex \$result 3]
EOF
    return $?
}

function send_ssh_command() {
    local host=$1
    local user=$2
    local port=$3
    local path=$4
    local cmd=$5

    ssh -o StrictHostKeyChecking=no ${host} -p ${port} -l ${user} <<-EOF
        cd ${path};
        ${cmd}
EOF

    return $?
}

function send_scp_command() {
    local host=$1
    local user=$2
    local dest_path=$3

    scp -o StrictHostKeyChecking=no -r ${CURRENT_DIR} ${user}@${host}:${dest_path}

    return $?
}
