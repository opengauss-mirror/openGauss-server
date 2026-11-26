#!/usr/bin/expect
# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# openGauss-server is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.


if { $argc != 4 } {
    send_user "Usage:cmd username password path\n"
    exit 1
}

set cmd [lindex $argv 0]
set principal [lindex $argv 1]
set passwd [lindex $argv 2]
set path [lindex $argv 3]

set timeout 3
spawn ${cmd} -c "${path}" "${principal}"

expect "*@HADOOP.COM:"
send "${passwd}\r"

expect eof

