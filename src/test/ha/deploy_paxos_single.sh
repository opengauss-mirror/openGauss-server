#!/bin/bash
# Copyright (c) Huawei Technologies Co., Ltd. 2010-2018. All rights reserved.

# deploy paxos

node_num=4
source ./standby_env.sh
dcf_port=13001
dcf_ports=()
function deploy_paxos()
{
    #stop the database
    python $scripts_dir/pgxc_paxos_single.py -o

    sleep 2
    #init the database
    if [ $# -gt 0 ]; then
        dcf_port=$1
    fi
    python $scripts_dir/pgxc_paxos_single.py -c 1 -d $node_num -p ${dcf_port}
}

function stop_datanodes()
{
    gs_ctl stop -D $primary_data_dir -M standby -Z single_node
    for((i=1; i<=$node_num; i++))
    do
        datanode_dir=$data_dir/datanode$i
        datanode_dir=$datanode_dir"_standby"
        gs_ctl stop -D $datanode_dir -M standby -Z single_node
    done
}

function set_paxos_config()
{
    dcf_port=$1
    dcf_ports[0]=$1
    dcf_config="{\"stream_id\":1,\"node_id\":1,\"ip\":\"127.0.0.1\",\"port\":${dcf_ports[0]},\"role\":\"LEADER\"}"
    for((i=1; i<=${node_num}; i++))
    do
        dcf_ports[$i]=`expr ${dcf_port} + $i`
        dcf_config=${dcf_config}",{\"stream_id\":1,\"node_id\":`expr $i + 1`,\"ip\":\"127.0.0.1\",\"port\":${dcf_ports[$i]},\"role\":\"FOLLOWER\"}"
    done
    dcf_config="'[${dcf_config}]'"
    #set the primary postgresql.conf file
    gs_guc set -Z datanode -D $primary_data_dir -c "enable_dcf = on"
    gs_guc set -Z datanode -D $primary_data_dir -c "dcf_log_level = 'RUN_ERR|RUN_WAR|RUN_INF|DEBUG_ERR|DEBUG_WAR|DEBUG_INF|TRACE|PROFILE|OPER'"
    gs_guc set -Z datanode -D $primary_data_dir -c "dcf_max_workers = 10"
    gs_guc set -Z datanode -D $primary_data_dir -c "dcf_data_path = '$primary_data_dir/dcf_data'"
    gs_guc set -Z datanode -D $primary_data_dir -c "dcf_log_path = '$primary_data_dir/dcf_log'"
    gs_guc set -Z datanode -D $primary_data_dir -c "dcf_node_id = 1"
    gs_guc set -Z datanode -D $primary_data_dir -c "dcf_config =  ${dcf_config}"

    for((i=1; i<=$node_num; i++))
    do
        datanode_dir=$data_dir/datanode$i
        datanode_dir=$datanode_dir"_standby"
        gs_guc set -Z datanode -D $datanode_dir -c "enable_dcf = on"
        gs_guc set -Z datanode -D $datanode_dir -c "dcf_log_level = 'RUN_ERR|RUN_WAR|RUN_INF|DEBUG_ERR|DEBUG_WAR|DEBUG_INF|TRACE|PROFILE|OPER'"
        gs_guc set -Z datanode -D $datanode_dir -c "dcf_max_workers = 10"
        gs_guc set -Z datanode -D $datanode_dir -c "dcf_data_path = '$datanode_dir/dcf_data'"
        gs_guc set -Z datanode -D $datanode_dir -c "dcf_log_path = '$datanode_dir/dcf_log'"
        gs_guc set -Z datanode -D $datanode_dir -c "dcf_node_id = "`expr $i \+ 1`
        gs_guc set -Z datanode -D $datanode_dir -c "dcf_config = ${dcf_config}"
    done
}

function turn_paxos()
{
    gs_guc set -Z datanode -D $primary_data_dir -c "enable_dcf = "$1
    
    for((i=1; i<=$node_num; i++))
    do
        datanode_dir=$data_dir/datanode$i
        datanode_dir=$datanode_dir"_standby"
        gs_guc set -Z datanode -D $datanode_dir -c "enable_dcf = "$1
    done
}

function build_datanodes()
{
    stop_datanodes
    turn_paxos off
    sleep 2
    rm -rf $primary_data_dir/PaxosNode
    gs_ctl start -D $primary_data_dir -M primary -Z single_node
    for((i=1; i<=$node_num; i++))
    do
        datanode_dir=$data_dir/datanode$i
        datanode_dir=$datanode_dir"_standby"
        rm -rf $datanode_dir/PaxosNode
        gs_ctl build -D $datanode_dir -Z single_node
    done
}

function start_paxos()
{
    if [ ! -z $1 ]; then
        dcf_port=$1
    fi
    stop_datanodes
    set_paxos_config ${dcf_port}
    sleep 2
    gs_ctl start -D $primary_data_dir -M standby -Z single_node
    for((i=1; i<=$node_num; i++))
    do
        datanode_dir=$data_dir/datanode$i
        datanode_dir=$datanode_dir"_standby"
        gs_ctl start -D $datanode_dir -M standby -Z single_node
    done
}

function check_paxos()
{
    gs_ctl query -D $primary_data_dir -Z single_node
    for((i=1; i<=$node_num; i++))
    do
        datanode_dir=$data_dir/datanode$i
        datanode_dir=$datanode_dir"_standby"
        gs_ctl query -D $datanode_dir -Z single_node
    done
}

function check_primary()
{
    primary_port=""
    nodes=($primary_data_dir $standby_data_dir $standby2_data_dir $standby3_data_dir $standby4_data_dir)
    for((i=0;i<=${node_num};i++))
    do
        gs_ctl query -D ${nodes[$i]} | grep -o -E "Primary"
        if [ $? -eq 0 ];then
           num=`expr ${i} \+ 1`
           primary_port=`expr $g_base_port \+ 3 \* ${num}`
           echo "primary port: "${primary_port}
           break
        fi
    done
}

function check_old_primary_dir()
{
    current_primary=""
    nodes=($primary_data_dir $standby_data_dir $standby2_data_dir $standby3_data_dir $standby4_data_dir)
    for((i=0;i<=${node_num};i++))
    do
        gs_ctl query -D ${nodes[$i]} | grep -o -E "Primary"
        if [ $? -eq 0 ];then
	   if [ $i == 0 ];then
		current_primary=$primary_data_dir
		break
	   fi
	   if [ $i == 1 ];then
		current_primary=$standby_data_dir
		break
           fi
	   
	   if [ $i == 2 ];then
		current_primary=$standby2_data_dir
		break
	   fi 
	
	   if [ $i == 3 ];then
		current_primary=$standby3_data_dir
		break
	   fi
	  
           if [ $i == 4 ];then
                current_primary=$standby4_data_dir
                break
           fi 
        fi
    done
    echo "old primary dir:"${current_primary}
}



