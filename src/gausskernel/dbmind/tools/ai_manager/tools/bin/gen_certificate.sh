#!/bin/bash
#############################################################################
# Copyright (c): 2012-2021, Huawei Tech. Co., Ltd.
# FileName     : remote_commander
# Version      : GaussDB Kernel V500R001
# Date         : 2021-03-01
# Description  : Remote execute script
#############################################################################

CA_CRT=$1
CA_KEY=$2

crt=$3
key=$4
req=$5
local_host=$6
file_name=$7

ca_password=""
ssl_password=""

read -s -p "please input the password of ca and ssl separated by space: " ca_password ssl_password



expect <<-EOF
    spawn /bin/openssl genrsa -aes256 -out ${key} 2048
    expect "Enter pass phrase for"
    send "${ssl_password}\r"
    expect "Verifying - Enter pass phrase for"
    send "${ssl_password}\r"
    expect eof
EOF

expect <<-EOF
    spawn /bin/openssl req -new -out ${req} -key ${key} -subj "/C=CN/ST=Some-State/O=${file_name}/CN=${local_host}"
    expect "Enter pass phrase for"
    send "${ssl_password}\r"
    expect eof
EOF

expect <<-EOF
    spawn /bin/openssl x509 -req -in ${req} -out ${crt} -sha256 -CAcreateserial -days 7000 -CA ${CA_CRT} -CAkey ${CA_KEY}
    expect "Enter pass phrase for"
    send "${ca_password}\r"
    expect eof
EOF

rm ${req}

chmod 600 ${key}
chmod 600 ${crt}
