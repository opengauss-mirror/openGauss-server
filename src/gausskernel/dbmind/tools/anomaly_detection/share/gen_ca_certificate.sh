#!/bin/bash
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
# gen_ca_certificate.sh
#    generate certificate
#
# IDENTIFICATION
#    src/gausskernel/dbmind/tools/A-Detection/shell/gen_ca_certificate.sh
#
#-------------------------------------------------------------------------
source ./initialize_certificate.sh

ca_crt="ca.crt"
ca_key="ca.key"
ca_password=""

read -s -p "please input the password of ca: " ca_password

cat > ca.conf <<-EOF
[req]
distinguished_name = req_distinguished_name
prompt = no

[req_distinguished_name]
O = $PROJECT_NAME Certificate Authority
EOF

expect <<-EOF
    spawn /bin/openssl genrsa -aes256 -out ${ca_key} 2048
    expect "Enter pass phrase for"
    send "${ca_password}\r"
    expect "Verifying - Enter pass phrase for"
    send "${ca_password}\r" 
    expect eof
EOF

expect <<-EOF
    spawn /bin/openssl req -new -out ca.req -key ${ca_key} -config ca.conf 
    expect "Enter pass phrase for"
    send "${ca_password}\r"
    expect eof
EOF

expect <<-EOF
    spawn /bin/openssl x509 -req -in ca.req -signkey ${ca_key} -days 7300 -out ${ca_crt}
    expect "Enter pass phrase for"
    send "${ca_password}\r"
    expect eof
EOF

mv ${ca_crt} ${ca_key} ${CURRENT_DIR}/${CA}
rm ca.req ca.conf
chmod 600 `find ${CURRENT_DIR}/${CA} -type f`


