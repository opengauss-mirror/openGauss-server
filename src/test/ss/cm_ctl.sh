#!/bin/bash
# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# openGauss-server is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.

CMD=$(echo ${1}|tr a-z A-Z)
KEY=$(echo ${2}|tr a-z A-Z)
VALUE=${3}

function show_help()
{
    echo "Usage: $0 [cmd] [key] [value]"
    echo "cmd:"
    echo "    set:          set [key]=[value]"
    echo "    show:         show key"
    echo "    help:         show help"
    echo "key&value:"
    echo "    REFORMER_ID   [0, 63]"
    echo "    BITMAP_ONLINE [0, UINT64_MAX]"
}

function show_param()
{
    echo "The parameters are as follows:"
    cat ${CM_CONFIG_PATH}
}

function set_param()
{
    if [ "$KEY" == "REFORMER_ID" ] || [ "$KEY" == "BITMAP_ONLINE" ]; then
        PARAMS=$(cat ${CM_CONFIG_PATH}|grep -v ${KEY}|sed '/^$/d')
        cat /dev/null > ${CM_CONFIG_PATH}
        echo "${PARAMS}" >> ${CM_CONFIG_PATH}
        echo "${KEY} = ${VALUE}" >> ${CM_CONFIG_PATH}
        echo "set ${KEY} = ${VALUE} success"
    else
        echo "invalid parameter"
    fi
}

function main()
{
    if [[ -z "${CM_CONFIG_PATH}" ]]
    then
        echo "CM_CONFIG_PATH is NULL"
        exit 0
    else
        echo "CM_CONFIG_PATH=${CM_CONFIG_PATH}"
    fi

    if [[ ! -e "${CM_CONFIG_PATH}" ]]
    then
        touch ${CM_CONFIG_PATH}
    fi

    if [ "$CMD" == "SET" ];then
        set_param
        exit 0
    elif [ "$CMD" == "SHOW"]; then
        show_param
        exit 0
    else
        show_help
        exit 0
    fi
}

main
