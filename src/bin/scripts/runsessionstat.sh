#!/bin/bash
#Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
# runsessionstat.sh
#    script to run session stat 
#
# IDENTIFICATION
#    src/bin/scripts/runsessionstat.sh
#
#-------------------------------------------------------------------------
set -e

# script exit and print infomation
die()
{
    echo "$*" 1>&2
    exit 1
}

#-d database name
MPP_DB_NAME=""
#-p port number
MPP_PORT=""
#-f removed flag
DATA_REMOVED_FLAG=1

n=$(ps ux | grep -v grep | grep runsessionstat | wc -l)
if [ $n -gt 2 ] ; then
	echo $n
	echo "session stat is running"
	exit 0
fi

date

#######################################################################
## print help information
#######################################################################
function print_help()
{
	echo "Usage: $0 [OPTION]
	-h			show help information
	-d			database name
	-p 			database port
	-f			0: removed cache data only 1: dump data into the table
        "
}
if [ $# = 0 ] ; then 
	echo "missing option"
        print_help 
	exit 1
fi

while getopts "d:f:hp:" arg 
do
        case $arg in
             d) MPP_DB_NAME=$OPTARG
                echo "MPP_DB_NAME: $MPP_DB_NAME"
                ;;
             f) DATA_REMOVED_FLAG=$OPTARG
                echo "DATA_REMOVED_FLAG: $DATA_REMOVED_FLAG"
                ;;
             h)
                print_help   
            	exit 1  
                ;;
             p) MPP_PORT=$OPTARG
                echo "MPP_PORT: $MPP_PORT"
                ;;
             ?)
                echo "unkonw argument"
                exit 1
                ;;
        esac
done

if [ -z "$MPP_DB_NAME" ]; then
	die "database name is null"
fi

if [ -z "$MPP_PORT" ]; then
	die "database port is null"
fi

echo "============ Session Info Dump Start ========================="

$GAUSSHOME/bin/gsql -d $MPP_DB_NAME -p $MPP_PORT -c "
select create_wlm_session_info($DATA_REMOVED_FLAG);
"

echo "============ Session Info Dump End ========================="
