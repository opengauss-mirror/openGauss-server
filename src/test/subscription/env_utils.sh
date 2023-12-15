#!/bin/sh
#some enviroment vars

export g_base_port=$2
export prefix=${GAUSSHOME}
export install_path="$prefix"
export GAUSSHOME="$prefix"
export LD_LIBRARY_PATH=$prefix/lib:$prefix/lib/libobs:$LD_LIBRARY_PATH
export PATH="$prefix/bin":$PATH
export g_local_ip="127.0.0.1"
export dbcompatibility=$3

db=postgres
scripts_dir="$1"
username=`whoami`
passwd="Gauss@123"

export g_data_path="$scripts_dir/tmp_check"
export g_username="$username"
export pub_node1_port=`expr $g_base_port`
export pub_node2_port=`expr $g_base_port \+ 3`
export pub_node3_port=`expr $g_base_port \+ 6`
export sub_node1_port=`expr $g_base_port \+ 9`
export sub_node2_port=`expr $g_base_port \+ 12`
export sub_node3_port=`expr $g_base_port \+ 15`

failed_keyword="testcase_failed"
gsctl_wait_time=3600
data_dir=$g_data_path

function exec_sql(){
	result=$(gsql -d $1 -p $2 -Atq -c "$3")
	if [ "$result" != "" ]; then
		echo "$result"
	fi
}

function exec_sql_with_user() {
	local sql_user=$username
	if [ -n "$test_username" ]; then
		sql_user=$test_username
	fi
	result=$(gsql -U $sql_user -W $passwd -d $1 -p $2 -Atq -c "$3")
	if [ "$result" != "" ]; then
		echo "$result"
	fi
}

function exec_sql_file() {
	local sql_user=$username
	if [ -n "$test_username" ]; then
		sql_user=$test_username
	fi

	gsql -U $sql_user -W $passwd -d $1 -p $2 -Atq -f "$3"
}

function exec_dump_db() {
	echo "dump $1 to $3.."
	if [ $# -eq 4 ]; then
		gs_dump $1 -f $3 -p $2
	else 
		gs_dump $1 -f $3 -p $2 --no-publications --no-subscriptions
	fi
}

function wait_for_subscription_sync(){
	max_attempts=20
	attempt=0
	query="SELECT count(1) = 0 FROM pg_subscription_rel WHERE srsubstate NOT IN ('r', 's');";
	while (($attempt < $max_attempts))
	do
		if [ "$(exec_sql $1 $2 "$query")" = "t" ]; then
			echo "initial data for subscription has been already synchronized"
			break
		fi
		sleep 1
		attempt=`expr $attempt \+ 1`
	done

	if [ $attempt -eq $max_attempts ]; then
		echo "$failed_keyword, timed out waiting for subscriber to synchronize data."
		exit 1
	fi
}

function wait_for_catchup(){
	$(exec_sql $1 $2 "checkpoint")
	target_lsn=$(exec_sql $1 $2 "SELECT pg_current_xlog_location()")
	max_attempts=20
	attempt=0
	query="SELECT '$target_lsn' <= confirmed_flush FROM pg_replication_slots WHERE slot_name = '$3'";
	while (($attempt < $max_attempts))
	do
		if [ "$(exec_sql $1 $2 "$query")" = "t" ]; then
			echo "subscriber has been caught up"
			break
		fi
		sleep 1
		attempt=`expr $attempt \+ 1`
	done

	if [ $attempt -eq $max_attempts ]; then
		echo "$failed_keyword, timed out waiting for catchup."
		exit 1
	fi
}

function switchover_to_primary() {
	gs_ctl switchover -w -t $gsctl_wait_time -D $data_dir/$1
	if [ $? -eq 0 ]; then
		echo "switchover to primary success!"
	else
		echo "$failed_keyword, switchover to pirmary fail!"
		exit 1
	fi
}

function get_log_file(){
	logfile=$(ls -rtl $data_dir/$1/pg_log/ | tail -n 1 | awk '{print $9}')
	echo "$data_dir/$1/pg_log/$logfile"
}

function restart_guc(){
	gs_guc set -D $data_dir/$1 -c "$2"
	gs_ctl restart -D $data_dir/$1
	if [ $? -eq 0 ]; then
		echo "restart $2 success!"
	else
		echo "$failed_keyword, restart $2 fail!"
		exit 1
	fi
}

function poll_query_until(){
	max_attempts=20
	attempt=0
	while (($attempt < $max_attempts))
	do
		if [ "$(exec_sql $1 $2 "$3")" = "$4" ]; then
			break
		fi
		sleep 1
		attempt=`expr $attempt \+ 1`
	done

	if [ $attempt -eq $max_attempts ]; then
		echo "$failed_keyword, $5"
		exit 1
	fi
}
