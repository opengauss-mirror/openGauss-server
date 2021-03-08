#!/bin/bash
# create master and slave
# Copyright (c) Huawei Technologies Co., Ltd. 2020-2028. All rights reserved.
#
#openGauss is licensed under Mulan PSL v2.
#You can use this software according to the terms and conditions of the Mulan PSL v2.
#You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
#-------------------------------------------------------------------------
#
# create_master_slave.sh
#    create master and slave
#
# IDENTIFICATION
#    GaussDBKernel/server/docker/dockerfiles/create_master_slave.sh
#
#-------------------------------------------------------------------------

#set OG_SUBNET,GS_PASSWORD,MASTER_IP,SLAVE_1_IP,MASTER_HOST_PORT,MASTER_LOCAL_PORT,SLAVE_1_HOST_PORT,SLAVE_1_LOCAL_PORT,MASTER_NODENAME,SLAVE_NODENAME

read -p "Please input OG_SUBNET (容器所在网段) [172.11.0.0/24]: " OG_SUBNET
OG_SUBNET=${OG_SUBNET:-172.11.0.0/24}
echo "OG_SUBNET set $OG_SUBNET"

read -p "Please input GS_PASSWORD (定义数据库密码)[Enmo@123]: " GS_PASSWORD
GS_PASSWORD=${GS_PASSWORD:-Enmo@123}
echo "GS_PASSWORD set $GS_PASSWORD"

read -p "Please input MASTER_IP (主库IP)[172.11.0.101]: " MASTER_IP
MASTER_IP=${MASTER_IP:-172.11.0.101}
echo "MASTER_IP set $MASTER_IP"

read -p "Please input SLAVE_1_IP (备库IP)[172.11.0.102]: " SLAVE_1_IP
SLAVE_1_IP=${SLAVE_1_IP:-172.11.0.102}
echo "SLAVE_1_IP set $SLAVE_1_IP"

read -p "Please input MASTER_HOST_PORT (主库数据库服务端口)[5432]: " MASTER_HOST_PORT
MASTER_HOST_PORT=${MASTER_HOST_PORT:-5432}
echo "MASTER_HOST_PORT set $MASTER_HOST_PORT"

read -p "Please input MASTER_LOCAL_PORT (主库通信端口)[5434]: " MASTER_LOCAL_PORT
MASTER_LOCAL_PORT=${MASTER_LOCAL_PORT:-5434}
echo "MASTER_LOCAL_PORT set $MASTER_LOCAL_PORT"

read -p "Please input SLAVE_1_HOST_PORT (备库数据库服务端口)[6432]: " SLAVE_1_HOST_PORT
SLAVE_1_HOST_PORT=${SLAVE_1_HOST_PORT:-6432}
echo "SLAVE_1_HOST_PORT set $SLAVE_1_HOST_PORT"

read -p "Please input SLAVE_1_LOCAL_PORT (备库通信端口)[6434]: " SLAVE_1_LOCAL_PORT
SLAVE_1_LOCAL_PORT=${SLAVE_1_LOCAL_PORT:-6434}
echo "SLAVE_1_LOCAL_PORT set $SLAVE_1_LOCAL_PORT"

read -p "Please input MASTER_NODENAME [opengauss_master]: " MASTER_NODENAME
MASTER_NODENAME=${MASTER_NODENAME:-opengauss_master}
echo "MASTER_NODENAME set $MASTER_NODENAME"

read -p "Please input SLAVE_NODENAME [opengauss_slave1]: " SLAVE_NODENAME
SLAVE_NODENAME=${SLAVE_NODENAME:-opengauss_slave1}
echo "SLAVE_NODENAME set $SLAVE_NODENAME"

read -p "Please input openGauss VERSION [1.0.1]: " VERSION
VERSION=${VERSION:-1.0.1}
echo "openGauss VERSION set $VERSION"

echo "starting  "

docker network create --subnet=$OG_SUBNET opengaussnetwork \
|| {
  echo ""
  echo "ERROR: OpenGauss Database Network was NOT successfully created."
  echo "HINT: opengaussnetwork Maybe Already Exsist Please Execute 'docker network rm opengaussnetwork' "
  exit 1
}
echo "OpenGauss Database Network Created."

docker run --network opengaussnetwork --ip $MASTER_IP --privileged=true \
--name $MASTER_NODENAME -h $MASTER_NODENAME -p $MASTER_HOST_PORT:$MASTER_HOST_PORT -d \
-e GS_PORT=$MASTER_HOST_PORT \
-e OG_SUBNET=$OG_SUBNET \
-e GS_PASSWORD=$GS_PASSWORD \
-e NODE_NAME=$MASTER_NODENAME \
-e REPL_CONN_INFO="replconninfo1 = 'localhost=$MASTER_IP localport=$MASTER_LOCAL_PORT localservice=$MASTER_HOST_PORT remotehost=$SLAVE_1_IP remoteport=$SLAVE_1_LOCAL_PORT remoteservice=$SLAVE_1_HOST_PORT'\n" \
opengauss:$VERSION -M primary \
|| {
  echo ""
  echo "ERROR: OpenGauss Database Master Docker Container was NOT successfully created."
  exit 1
}
echo "OpenGauss Database Master Docker Container created."

sleep 30s

docker run --network opengaussnetwork --ip $SLAVE_1_IP --privileged=true \
--name $SLAVE_NODENAME -h $SLAVE_NODENAME -p $SLAVE_1_HOST_PORT:$SLAVE_1_HOST_PORT -d \
-e GS_PORT=$SLAVE_1_HOST_PORT \
-e OG_SUBNET=$OG_SUBNET \
-e GS_PASSWORD=$GS_PASSWORD \
-e NODE_NAME=$SLAVE_NODENAME \
-e REPL_CONN_INFO="replconninfo1 = 'localhost=$SLAVE_1_IP localport=$SLAVE_1_LOCAL_PORT localservice=$SLAVE_1_HOST_PORT remotehost=$MASTER_IP remoteport=$MASTER_LOCAL_PORT remoteservice=$MASTER_HOST_PORT'\n" \
opengauss:$VERSION -M standby \
|| {
  echo ""
  echo "ERROR: OpenGauss Database Slave1 Docker Container was NOT successfully created."
  exit 1
}
echo "OpenGauss Database Slave1 Docker Container created."
