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

# Define default values
NETWORK_NAME="opengaussnetwork"
OG_SUBNET="172.11.0.0/24"
GS_PASSWORD="Enmo@123"
MASTER_IP="172.11.0.101"
MASTER_HOST_PORT="5432"
MASTER_NODENAME="dn_6001"
VERSION="5.0.0"
# Define default values for slaves
SLAVE_IP=("172.11.0.102" "172.11.0.103" "172.11.0.104" "172.11.0.105" "172.11.0.106" "172.11.0.107" "172.11.0.108" "172.11.0.109")
SLAVE_HOST_PORT=("6432" "7432" "8432" "9432" "10432" "11432" "12432" "13432")
SLAVE_NODENAME=("dn_6002" "dn_6003" "dn_6004" "dn_6005" "dn_6006" "dn_6007" "dn_6008" "dn_6009")
SLAVE_COUNT=1

ARGS=$(getopt -o h --long OG_SUBNET:,GS_PASSWORD:,MASTER_IP:,MASTER_HOST_PORT:,MASTER_LOCAL_PORT:,MASTER_NODENAME:,VERSION:,SLAVE_COUNT:,NETWORK_NAME: -- "$@")
if [ $? != 0 ]; then
    echo "参数解析错误"
    exit 1
fi
eval set -- "$ARGS"

# Use getopts to process command line arguments
while true; do
    case "$1" in
        -h)
            echo "Usage: $0 [--OG_SUBNET value] [--GS_PASSWORD value] [--MASTER_IP value] [--MASTER_HOST_PORT value] [--MASTER_NODENAME value] [--VERSION value] [--SLAVE_COUNT value] [--SLAVE_NODENAME value] [--SLAVE_IP value] [--SLAVE_HOST_PORT value] [--NETWORK_NAME value]"
            shift
            ;;
        --OG_SUBNET)
            OG_SUBNET="$2"
            shift 2
            ;;
        --GS_PASSWORD)
            GS_PASSWORD="$2"
            shift 2
            ;;
        --MASTER_IP)
            MASTER_IP="$2"
            shift 2
            ;;
        --MASTER_HOST_PORT)
            MASTER_HOST_PORT="$2"
            shift 2
            ;;
        --MASTER_LOCAL_PORT)
            MASTER_LOCAL_PORT="$2"
            shift 2
            ;;
        --MASTER_NODENAME)
            MASTER_NODENAME="$2"
            shift 2
            ;;
        --VERSION)
            VERSION="$2"
            shift 2
            ;;
        --SLAVE_COUNT)
            SLAVE_COUNT="$2"
            shift 2
            ;;
        --NETWORK_NAME)
            NETWORK_NAME="$2"
            shift 2
            ;;
        --)
            shift
            break
            ;;
        *)
            echo "Invalid option: -$OPTARG" >&2
            exit 1
            ;;
    esac
done

# Output the set values
echo "OG_SUBNET set $OG_SUBNET"
echo "GS_PASSWORD set $GS_PASSWORD"
echo "MASTER_IP set $MASTER_IP"
echo "MASTER_HOST_PORT set $MASTER_HOST_PORT"
echo "MASTER_NODENAME set $MASTER_NODENAME"
echo "openGauss VERSION set $VERSION"
echo "SLAVE_COUNT set $SLAVE_COUNT"
echo "SLAVE_NODENAME set $SLAVE_NODENAME"
echo "SLAVE_IP set $SLAVE_IP"
echo "SLAVE_HOST_PORT set $SLAVE_HOST_PORT"
echo "NETWORK_NAME set $NETWORK_NAME"

# Loop through and process each slave's information
for (( i=0; i<SLAVE_COUNT; i++ )); do
    echo "SLAVE_${i}_IP set${SLAVE_IP[$i]}"
    echo "SLAVE_${i}_HOST_PORT set${SLAVE_HOST_PORT[$i]}"
    echo "SLAVE_${i}_NODENAME set${SLAVE_NODENAME[$i]}"
done


echo "starting  "

# Create the network
docker network create --subnet=$OG_SUBNET $NETWORK_NAME \
|| {
  echo ""
  echo "ERROR: OpenGauss Database Network was NOT successfully created."
  echo "HINT: opengaussnetwork Maybe Already Exsist Please Execute 'docker network rm opengaussnetwork' "
  exit 1
}
echo "OpenGauss Database Network Created."

# Create the master container
REPL_CONN_INFO_MASTER=""
local_info="localhost=$MASTER_IP localport=$((MASTER_HOST_PORT+1)) localservice=$((MASTER_HOST_PORT+4)) localheartbeatport=$((MASTER_HOST_PORT+5))"
for (( i=0; i<SLAVE_COUNT; i++ )); do
    remote_port=${SLAVE_HOST_PORT[$i]}
    remote_info="remotehost=${SLAVE_IP[$i]} remoteport=$((remote_port+1)) remoteservice=$((remote_port+4)) remoteheartbeatport=$((remote_port+5))"
    REPL_CONN_INFO_MASTER+="replconninfo$((i+1)) = '$local_info $remote_info'\n"
done
docker run --network $NETWORK_NAME --ip $MASTER_IP --privileged=true \
--name $MASTER_NODENAME -h $MASTER_NODENAME -p $MASTER_HOST_PORT:$MASTER_HOST_PORT -d \
-e GS_PORT=$MASTER_HOST_PORT \
-e OG_SUBNET=$OG_SUBNET \
-e GS_PASSWORD="$GS_PASSWORD" \
-e NODE_NAME="$MASTER_NODENAME" \
-e REPL_CONN_INFO="$REPL_CONN_INFO_MASTER" \
opengauss:$VERSION -M primary \
|| {
  echo ""
  echo "ERROR: OpenGauss Database Master Docker Container was NOT successfully created."
  exit 1
}
echo "OpenGauss Database Master Docker Container created."

sleep 30s

# Create the slave containers
for (( i=0; i<SLAVE_COUNT; i++ )); do
    # Get the slave's information
    REPL_CONN_INFO_SLAVE=""
    local_port=${SLAVE_HOST_PORT[$i]}
    local_info="localhost=${SLAVE_IP[$i]} localport=$((local_port+1)) localservice=$((local_port+4)) localheartbeatport=$((local_port+5))"
    remote_master_info="remotehost=$MASTER_IP remoteport=$((MASTER_HOST_PORT+1)) remoteservice=$((MASTER_HOST_PORT+4)) remoteheartbeatport=$((MASTER_HOST_PORT+5))"
    k=1
    REPL_CONN_INFO_SLAVE="replconninfo${k} = '$local_info $remote_master_info'\n"
    for (( j=0; j<SLAVE_COUNT; j++ )); do
        if [[ $i -eq $j ]]; then
            continue
        fi
        k=$((k+1))
        remote_port=${SLAVE_HOST_PORT[$j]}
        remote_info="remotehost=${SLAVE_IP[$j]} remoteport=$((remote_port+1)) remoteservice=$((remote_port+4)) remoteheartbeatport=$((remote_port+5))"
        REPL_CONN_INFO_SLAVE+="replconninfo${k} = '$local_info $remote_info'\n"
    done
    echo "REPL_CONN_INFO_SLAVE=$REPL_CONN_INFO_SLAVE"

    # Create the slave container
    docker run --network $NETWORK_NAME --ip ${SLAVE_IP[$i]} --privileged=true \
    --name ${SLAVE_NODENAME[$i]} -h ${SLAVE_NODENAME[$i]} -p $local_port:$local_port -d \
    -e GS_PORT=$local_port \
    -e OG_SUBNET=$OG_SUBNET \
    -e GS_PASSWORD="$GS_PASSWORD" \
    -e NODE_NAME="${SLAVE_NODENAME[$i]}" \
    -e REPL_CONN_INFO="$REPL_CONN_INFO_SLAVE" \
    opengauss:$VERSION -M standby \
    || {
      echo ""
      echo "ERROR: OpenGauss Database ${SLAVE_NODENAME[$i]} Docker Container was NOT successfully created."
      exit 1
    }
    echo "OpenGauss Database ${SLAVE_NODENAME[$i]} Docker Container created."
    sleep 30s
done
