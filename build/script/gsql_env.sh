#!/bin/bash

#-----------------------------------------------------
#Copyright (c): 2020, Huawei Tech. Co., Ltd.
#FileName     : gsql_env.sh
#Version      : V500R001C10
#Date         : 2020-08-06
#Description  : This file is to configure environment variables of gsql
#-----------------------------------------------------

#find the absolute path of this script
LOCAL_PATH=${0}
if [ x${LOCAL_PATH:0:1} = "x-" ] || [ "x${LOCAL_PATH}" = "x/bin/bash" ] || [ "x${LOCAL_PATH}" = "x/bin/sh" ]; then
    LOCAL_PATH="$(pwd)"
elif [ x${LOCAL_PATH:0:1} != "x/" ]; then
    LOCAL_PATH="$(pwd)/$(dirname ${LOCAL_PATH})";
fi

function logerr()
{
    printf "ERROR: $* \n" >&2
}

function loghint()
{
    printf "HINT: $* \n" >&2
}

function logwarning()
{
    printf "WARNING: $* \n" >&2
}

function doing()
{
    length_of_line=60
    printf "$1 ";
    for ((i=${#1};i<$length_of_line;i++)); do 
        printf '.';
    done;
    printf " "
}

#------------------------------
#       gsql things
#------------------------------
function cofig_gsql()
{
    doing 'Configuring LD_LIBRARY_PATH and PATH for gsql...'
    LIB_PATH="${LOCAL_PATH}/lib"
    BIN_PATH="${LOCAL_PATH}/bin"
    if [ ! -f "${LOCAL_PATH}/bin/gsql" ]; then
        logerr "failed to locate ./bin/gsql, please source this file at the path where it is. "
        return 1;
    fi;
    export LD_LIBRARY_PATH=${LIB_PATH}:${LD_LIBRARY_PATH}
    export PATH=${BIN_PATH}:${PATH}
    echo 'done'
    return 0
}

if [ ! -z "$1" ]; then
    echo "Usage:"
    echo "    source $0"
else
    cofig_gsql
    if [ 0 -eq $? ]; then
        echo 'All things done.'
    fi
fi
