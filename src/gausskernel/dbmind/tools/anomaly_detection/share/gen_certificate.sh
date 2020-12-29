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
# gen_certificate.sh
#    generate certificate
#
# IDENTIFICATION
#    src/gausskernel/dbmind/tools/A-Detection/shell/gen_certificate.sh
#
#-------------------------------------------------------------------------

source ./initialize_certificate.sh

CA_CRT="${CURRENT_DIR}/${CA}/ca.crt"
CA_KEY="${CURRENT_DIR}/${CA}/ca.key"
pwf="${CURRENT_DIR}/${PW_FILE}"
local_host=""
ca_password=""
ssl_password=""
base_dir=""
file_name=""

if [ ! -f ${CA_CRT} ]; then
    echo "not found ${CA_CRT}."
    exit 0
fi

if [ ! -f ${CA_KEY} ]; then
    echo "not found ${CA_KEY}."
    exit 0
fi

read -p "please input the basename of ssl certificate: " base_dir
read -p "please input the filename of ssl certificate: " file_name
read -p "please input the local host: " local_host
read -s -p "please input the password of ca and ssl separated by space: " ca_password ssl_password

if [ ! -d ${base_dir}/ ]; then
    mkdir -p ${base_dir}
fi

key="${base_dir}/${file_name}.key"
crt="${base_dir}/${file_name}.crt"
req="${base_dir}/${file_name}.req"

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

echo "${ssl_password}">${pwf}
chmod 600 ${key}
chmod 600 ${crt}
chmod 600 ${pwf}
