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
# runmemprofile.sh
#    script to run mem profile 
#
# IDENTIFICATION
#    src/bin/scripts/runmemprofile.sh
#
#-------------------------------------------------------------------------
set -e

# 脚本退出，并进行打印。该脚本不返回1
die()
{
    echo "$*" 1>&2
    exit 1
}

# 获取参数
#参数1  数据库名称
MPP_DB_NAME=""
#参数2  数据库端口号
MPP_PORT=""
#参数3  数据名称1
EXPLAIN_DATA_NAME1=""
#参数4  数据名称2
EXPLAIN_DATA_NAME2=""


#######################################################################
## print help information
#######################################################################
function print_help()
{
	echo "Usage: $0 [OPTION]
	-h			show help information
	-d			database name
	-p 			database port
	-f			memory_context_tid_timestamp.log
        "
}
if [ $# = 0 ] ; then 
	echo "missing option"
        print_help 
	exit 1
fi

while [ $# -gt 0 ]; do   
    case $1 in   
        -h|--help)   
            	print_help   
            	exit 1  
            	;;       
	-d)
		if [ "$2"X = X ];then
			echo "no given database name"
			exit 1
		fi
		MPP_DB_NAME=$2
		echo "new dbname=$MPP_DB_NAME"
		shift 2
		;;
	-p)
		if [ "$2"X = X ];then
			echo "no given database port"
			exit 1
		fi
		MPP_PORT=$2
		echo "port=$MPP_PORT"
		shift 2
		;;
	-f)
		if [  ];then
			echo "no given data file"
			exit 1
		fi
		EXPLAIN_DATA_NAME1=$2
		if [ "$3"X = X ];then
			echo "name1=$EXPLAIN_DATA_NAME1"
			shift 2
		else
			EXPLAIN_DATA_NAME2=$3
			echo "name2=$EXPLAIN_DATA_NAME2"
			shift 3
		fi	
		;;
	*)
		echo "Internal Error: option processing error: $1" 1>&2  
		echo "please input right paramtenter, the following command may help you"
		echo "./runprofile.sh --help or ./runprofile.sh -h"
		exit 1
    esac
done 


if [ -z "$MPP_DB_NAME" ]; then
	die "database name is null"
fi

if [ -z "$MPP_PORT" ]; then
	die "database port is null"
fi

if [ -z "$EXPLAIN_DATA_NAME1" ]; then
	die "data file is null"
fi

echo "============ Profile Analyze Start ========================="

gsql -d $MPP_DB_NAME -p $MPP_PORT -c "
drop table if exists memctx_tid_time;
create table memctx_tid_time
(
	position         text,
	size             bigint,
	requested_size   bigint
);
COPY memctx_tid_time FROM '$EXPLAIN_DATA_NAME1' WITH csv;
"

echo "=========== summary by size ======="
gsql -d $MPP_DB_NAME -p $MPP_PORT <<EOF
select position, sum(size) as size, sum(requested_size) as requested_size, count(position) from memctx_tid_time group by position order by sum(size) desc;
EOF

echo "=========== summary by count ======="
gsql -d $MPP_DB_NAME -p $MPP_PORT <<EOF
select position, sum(size) as size, sum(requested_size) as requested_size, count(position) from memctx_tid_time group by position order by count(position) desc;
EOF

gsql -d $MPP_DB_NAME -p $MPP_PORT -c "
drop table if exists memctx_tid_time;
"
echo "============ Profile Analyze End ========================="
