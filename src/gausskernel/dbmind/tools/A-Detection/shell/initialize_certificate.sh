source ./common.sh

SERVER="certificate/server"
AGENT="certificate/agent"
CA="certificate/ca"
PW_FILE="certificate/pwf"

if [ ! -d ${CURRENT_DIR}/${SERVER} ]; then
    mkdir -p  ${CURRENT_DIR}/${SERVER}
fi

if [ ! -d ${CURRENT_DIR}/${AGENT} ]; then
    mkdir -p ${CURRENT_DIR}/${AGENT}
fi

if [ ! -d ${CURRENT_DIR}/${CA} ]; then
    mkdir -p ${CURRENT_DIR}/${CA}
fi

