#!/usr/bin/expect -f
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

