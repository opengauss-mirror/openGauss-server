#!/bin/bash
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
rm ca.req
chmod 600 `find ${CURRENT_DIR}/${CA} -type f`


