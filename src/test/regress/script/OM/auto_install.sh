#!/usr/bin/expect -f
# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# openGauss-server is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.

set timeout -1
set password [lindex $argv 0]
set xmlpath [lindex $argv 1]
#set scriptpath [lindex $argv 2]

spawn gs_install -X $xmlpath --dn-guc=modify_initial_password=false
expect "password"
send "${password}\n"
expect "Please repeat for"
send "${password}\n"
expect eof
interact

