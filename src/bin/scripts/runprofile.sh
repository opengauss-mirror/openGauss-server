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
# runprofile.sh
#
# IDENTIFICATION
#    src/bin/scripts/runprofile.sh
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
	-f			profiling data name, values is name1 name2
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
			echo "no given profiling data name"
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
	die "explain profiling data name is null"
fi

echo "============ Profile Analyze Start ========================="

gsql -d $MPP_DB_NAME -p $MPP_PORT -c "
drop table if exists explain1;
create table explain1
(
	plan_id int,
	parent_plan_id int,
	plan_node_id int,
	plan_type int,
	is_dn bool,
	node_name text,
	plan_name text,
	start_time float8,
	total_time float8,
	oper_time float8,
	plan_rows bigint,
	loops int,
	ex_cyc bigint,
	inc_cyc bigint,
	ex_cyc_rows bigint,
	memory bigint,
	peak_memory bigint,
	shared_hit bigint,
	shared_read bigint,
	shared_dirtied bigint,
	shared_written bigint,
	local_hit bigint,
	local_read bigint,
	local_dirtied bigint,
	local_written bigint,
	temp_read bigint,
	temp_written bigint,
	blk_read_time float8,
	blk_write_time float8,
	cu_none bigint,
	cu_some bigint,
	sort_method text,
	sort_type text,
	sort_space bigint,
	hash_batch int,
	hash_batch_orignal int,
	hash_bucket int,
	hash_space bigint,
	hash_file_number int,
	query_net_work bigint,
	network_poll_time float8,
	llvm_opti  bool
);
COPY explain1 FROM '$EXPLAIN_DATA_NAME1' WITH csv;
"

echo "=========== Top 3-time consumering node in each node ======="

gsql -d $MPP_DB_NAME -p $MPP_PORT <<EOF
select * from(
select oper_time, CAST(100 * (oper_time / (select total_time from explain1 where plan_node_id = 1)) AS numeric) 
as time_percentage, plan_name, plan_node_id, node_name, plan_rows,rank() over(partition by node_name order by oper_time desc) as topnode from explain1 
where plan_name not like '%BROADCAST%' and plan_name not like '%REDISTRIBUTE%') where topnode <=3;
EOF

echo "============ Top 3-memory consuming node in each node ======="
gsql -d $MPP_DB_NAME -p $MPP_PORT <<EOF
select memory, plan_name, plan_node_id, node_name, plan_rows from(
select memory, plan_name, plan_node_id, node_name, plan_rows, 
rank() over(partition by topnode order by memory desc, node_name asc ) as result 
from(
select memory, plan_name, plan_node_id, node_name, plan_rows,rank() over(partition by node_name order by memory desc) as topnode from explain1 
where plan_name not like '%BROADCAST%' and plan_name not like '%REDISTRIBUTE%') where topnode <=3) where node_name not like '%coordinator%' order by 1 desc;
EOF
echo "============ Profile Analyze End ========================="

if [ -z "$EXPLAIN_DATA_NAME2" ]; then
	exit
fi

gsql -d $MPP_DB_NAME -p $MPP_PORT -c "
drop table if exists explain2;
create table explain2
(
	plan_id int,
	parent_plan_id int,
	plan_node_id int,
	plan_type int,
	is_dn bool,
	node_name text,
	plan_name text,
	start_time float8,
	total_time float8,
	oper_time float8,
	plan_rows bigint,
	loops int,
	ex_cyc bigint,
	inc_cyc bigint,
	ex_cyc_rows bigint,
	memory bigint,
	peak_memory bigint,
	shared_hit bigint,
	shared_read bigint,
	shared_dirtied bigint,
	shared_written bigint,
	local_hit bigint,
	local_read bigint,
	local_dirtied bigint,
	local_written bigint,
	temp_read bigint,
	temp_written bigint,
	blk_read_time float8,
	blk_write_time float8,
	cu_none bigint,
	cu_some bigint,
	sort_method text,
	sort_type text,
	sort_space bigint,
	hash_batch int,
	hash_batch_orignal int,
	hash_bucket int,
	hash_space bigint,
	hash_file_number int,
	query_net_work bigint,
	network_poll_time float8,
	llvm_opti  bool
	
);
COPY explain2 FROM '$EXPLAIN_DATA_NAME2' WITH csv;
"

echo "============ Profile Compare Start ========================="

echo "========= First: Whether the plan is consistent ============"

result=`gsql -d "$MPP_DB_NAME" -p "$MPP_PORT" --tuples-only -c "select a.plan_name
	from explain1 a, explain2 b where a.plan_node_id = b.plan_node_id and a.node_name = b.node_name and (a.plan_name <> b.plan_name or a.plan_rows != b.plan_rows); "`

gsql -d $MPP_DB_NAME -p $MPP_PORT <<EOF
select a.plan_name as old_operator, b.plan_name as new_operator, a.plan_rows, b.plan_rows, a.plan_id, b.plan_id, a.plan_node_id,b.plan_node_id 
from explain1 a, explain2 b where a.plan_node_id = b.plan_node_id and a.node_name = b.node_name and (a.plan_name <> b.plan_name or a.plan_rows != b.plan_rows);
EOF

if [ -n "$result" ]; then
	echo "the plan is not consistent"
	exit 1
fi

echo "========= Second: when plan is consistent, whether the result is consistent ============"

result=`gsql -d "$MPP_DB_NAME" -p "$MPP_PORT" --tuples-only -c "select a.plan_name 
from explain1 a, explain2 b where a.plan_node_id = b.plan_node_id and a.node_name = b.node_name and a.plan_name = b.plan_name and a.plan_rows != b.plan_rows;"`

gsql -d $MPP_DB_NAME -p $MPP_PORT <<EOF
select a.plan_name as old_operator, b.plan_name as new_operator, a.plan_rows, b.plan_rows, a.plan_id, b.plan_id, a.plan_node_id,b.plan_node_id 
from explain1 a, explain2 b where a.plan_node_id = b.plan_node_id and a.node_name = b.node_name and a.plan_name = b.plan_name and a.plan_rows != b.plan_rows;
EOF

if [ -n "$result" ]; then
	echo "the result is not consistent"
	exit 1
fi

echo "========= Third: when plan and result are consistent, show the max difference operator op_time ============"
gsql -d $MPP_DB_NAME -p $MPP_PORT <<EOF
select 
a.oper_time , 
b.oper_time, 
case when a.oper_time > b.oper_time then a.oper_time - b.oper_time
else b.oper_time - a.oper_time end as oper_time_minus,
a.plan_name ,
a.node_name, 
a.plan_rows, 
a.plan_node_id
from explain1 a, explain2 b 
where a.plan_node_id = b.plan_node_id and a.node_name = b.node_name 
and a.plan_name = b.plan_name and a.plan_rows = b.plan_rows order by oper_time_minus desc limit 10;
EOF
echo "============ Profile Compare End ========================="


