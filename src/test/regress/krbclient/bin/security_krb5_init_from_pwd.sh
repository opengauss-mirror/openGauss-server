#!/usr/bin/expect

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

