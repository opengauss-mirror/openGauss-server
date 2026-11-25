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
set username [lindex $argv 0]
set usergroup [lindex $argv 1]
set xmlpath [lindex $argv 2]
set envpath [lindex $argv 3]
set scriptpath [lindex $argv 4]
set password [lindex $argv 5]
#spawn passwd $username
#expect "password:"
#send "Gauss_234\n"
#expect "password:"
#send "Gauss_234\n"


spawn $scriptpath/gs_preinstall -U $username -G $usergroup -X $xmlpath --sep-env-file=$envpath 
expect "yes/no"
send "yes\n"
expect "Password:"
send "${password}\n"
expect "yes/no"
send "yes\n"
expect "*assw*"
send "${password}\n"
expect "*assw*"
send "${password}\n"
expect eof
interact

