#!/bin/bash
# -*- coding:utf-8 -*-
#############################################################################
# Copyright (c): 2021, Huawei Tech. Co., Ltd.
# FileName     : uninstall.sh
# Version      : V1.0.0
# Date         : 2021-04-17
# Description  : the script used to uninstall the single cluster on one machine
#########################################

declare user=$(whoami)
if [ X"$user" = X"root" ]; then
	echo "error: can not uninstall gauss with root"
	exit 1
fi
declare delete_data="false"
declare root_path=$(cd $(dirname $0);pwd)

declare log_file="${root_path}/uninstall.log"
if [ -e "${log_file}" ]
then
    cat /dev/null >  ${log_file}
else
    touch ${log_file}
fi
env_file=$(echo $GAUSSENV)
if [ "${env_file}" = "" ]
then
    env_file=~/.bashrc
fi
source ${env_file}

function usage()
{
    echo "
Usage: $0 [OPTION]
Arguments:
   --delete-data               delete data path and program path
   -h|--help                   show this help, then exit
   "
}

function info()
{
    echo "$1" >> ${log_file}
    echo -e "\033[32minfo:\033[0m$1"
}

function log()
{
    echo "$1" >> ${log_file}
    echo "$1"
}

function die()
{
    echo "$1" >> ${log_file}
    echo -e "\033[31m$1\033[0m"
	exit 1
}

while [ $# -gt 0 ]
do
case "$1" in
    -h|--help)
            usage
            exit 1
            ;;
	--delete-data)
            delete_data="true"
            shift 1
            ;;
    *)
            echo "internal error: option processing error" 1>&2
            echo "please input right paramtenter, the following command may help you"
            echo "sh uninstall.sh --help or sh uninstall.sh -h"
            exit 1
esac
done

log "delete-data is ${delete_data}"

function uninstall() {
    log "cleaning up related processes"
    if [ "$GAUSSHOME" != "" ]
    then
        pids=$(ps -ef | grep "$user" | grep "$GAUSSHOME/bin/gaussdb" | grep -v grep | awk '{print $2}')
        if [ "${pids}" != "" ]
        then
            kill -9 $pids
            log "clean up related processes $pids"
        fi
    fi
    log "clean up related processes success"
}

function set_environment() {
	data_path=$(echo $GAUSSDATA)
	if [[ -n "${data_path}" && -e "${data_path}" ]]
	then
		rm -rf ${data_path}
	fi

	app_path=$(echo $GAUSSHOME)
	if [[ -n "${app_path}" && -e "${app_path}" ]]
	then
	    chmod -R 700 ${app_path}
		rm -rf ${app_path}
	fi

	log_path=$(echo $GAUSSLOG)
	if [[ -n "${log_path}" && -e "${log_path}" ]]
	then
		rm -rf ${log_path}
	fi

	# set GAUSSHOME and GAUSSDATA
    sed -i "/.*export\\s*GAUSSHOME=/d" ${env_file}
	sed -i "/.*export\\s*GAUSSDATA=/d" ${env_file}
	sed -i "/.*export\\s*GAUSSLOG=/d" ${env_file}
    sed -i "/.*export\\s*GAUSSENV=/d" ${env_file}
    sed -i "/.*ulimit\\s*-SHn\\s*1000000/d" ${env_file}
    # set PATH and LD_LIBRARY_PATH  GS_CLUSTER_NAME
	sed -i "/.*export\\s*PATH=/d" ${env_file}
	sed -i "/.*export\\s*LD_LIBRARY_PATH=/d" ${env_file}
}

uninstall

if [ "${delete_data}" = "true" ]
then
	set_environment
fi
exit 0