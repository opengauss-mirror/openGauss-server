#!/usr/bin/expect -f
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

