"""
 openGauss is licensed under Mulan PSL v2.
 You can use this software according to the terms and conditions of the Mulan PSL v2.
 You may obtain a copy of Mulan PSL v2 at:

 http://license.coscl.org.cn/MulanPSL2

 THIS SOFTWARE IS PROVIDED ON AN 'AS IS' BASIS, WITHOUT WARRANTIES OF ANY KIND,
 EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 See the Mulan PSL v2 for more details.
 
 Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 Description: The routine of generating ssl certificates for AiEngine.
"""

if [ ! $GAUSSHOME ];then
  echo "PATH GAUSSHOME NOT EXISTS. FAILED to generate SSL certificates."
  exit 0
fi

if [ ! -f "$GAUSSHOME/openssl.cnf" ]; then
  echo "Can not find openssl.cnf file under $GAUSSHOME, please check!"
  echo "FAILED to generate SSL certificates!"
  exit 0
fi


if [ ! -d "$GAUSSHOME/CA" ]; then
  mkdir -m 700 $GAUSSHOME/CA
else
  read -p "The CA file exits, please make sure to remove it or not: [y/n] " answer
  if [ "$answer" == "y" ]; then
    rm -r $GAUSSHOME/CA
  elif [ "$answer" == "n" ]; then
    echo "FAILED to generate SSL certificates!"
    exit 0
  else
    exit 0
  fi  
fi

mkdir -m 700 $GAUSSHOME/CA
cd $GAUSSHOME/CA
cp $GAUSSHOME/bin/dbmind/predictor/install/ca_ext.txt ./
cp $GAUSSHOME/openssl.cnf ./
mkdir -m 700 ./demoCA ./demoCA/newcerts ./demoCA/private
echo '01'>./demoCA/serial
touch ./demoCA/index.txt
chmod 700 ./demoCA/index.txt

read -s -p "Please enter your password for SSL key files: " pass
openssl genrsa -aes256 -passout pass:$pass -out demoCA/private/cakey.pem 2048

openssl req -config openssl.cnf -passin pass:$pass -new -key demoCA/private/cakey.pem -out demoCA/careq.pem -subj "/C=CN/ST=SZ/O=HW/OU=GS/CN=CA/"

openssl ca -config openssl.cnf -out demoCA/cacert.pem -keyfile demoCA/private/cakey.pem -passin pass:$pass -extensions ca_ext -extfile ca_ext.txt -selfsign -infiles demoCA/careq.pem


openssl genrsa -aes256 -passout pass:$pass -out server.key 2048
read -p "Please enter your aiEngine IP: " AIIP

openssl req -config openssl.cnf -passin pass:$pass -new -key server.key -out server.req -subj "/C=CN/ST=SZ/O=HW/OU=GS/CN=$AIIP"
sed -i "s/yes/no/g" demoCA/index.txt.attr

openssl ca -config openssl.cnf -in server.req -out server.crt -passin pass:$pass -extensions some_ext -extfile ca_ext.txt -days 3650 -md sha256 


openssl genrsa -aes256 -passout pass:$pass -out client.key 2048
read -p "Please enter your gaussdb IP: " DBIP

openssl req -config openssl.cnf -passin pass:$pass -new -key client.key -out client.req -subj "/C=CN/ST=SZ/O=HW/OU=GS/CN=$DBIP"

openssl ca -config openssl.cnf -in client.req -out client.crt -passin pass:$pass -extensions some_ext -extfile ca_ext.txt -days 3650 -md sha256 

gs_guc encrypt -M server -K $pass -D ./ 
gs_guc encrypt -M client -K $pass -D ./ 
