#!/bin/bash
#
# Copyright (c) 2020 Huawei Technologies Co.,Ltd.
# 
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
# 
#          http://license.coscl.org.cn/MulanPSL2
# 
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
# ---------------------------------------------------------------------------------------
# 
# gs_plan_simulator.sh
#        This script is used to collect information and reproduce the environment
#        during the plan reproduction process.
# 
# IDENTIFICATION
#        src/bin/gs_plan_simulator/gs_plan_simulator.sh
# 
# ---------------------------------------------------------------------------------------
#parameters
progname="gs_plan_simulator"
dbname=""
node_group=""
mode=""
port=$PGPORT
user=""
dir=""
tables=""
ishdfs="no"

enable_extended_statistic="yes"
enable_node_group="no"

#global variables
sql_conect=""
sql_conect_pg=""
dump_conect=""
data_node_prefix="datanode"
node_group_name="group1"

#file name
files_path=""
clean_temp_info="/dependences/clean_temp.sql"
store_class_stats_info="/dependences/store_pg_class_stats.sql"
store_statistic_stats_info="/dependences/store_pg_statistic_stats.sql"
store_statistic_ext_stats_info="/dependences/store_pg_statistic_ext_stats.sql"
store_pgxc_class_info="/dependences/store_pgxc_class.sql"

log_file_info="/gs_plan_simulator.log"
init_script="/dependences/initdb.py"
restore_temp_info="/dependences/restore_temp.sql"
object_defination="/allObjetctInfo.sql"
pg_class_stats_info="/pgClassStats.sql"
pg_statistic_stats_info="/pgStatisticStats.sql"
pg_statistic_ext_stats_info="/pgStatisticExtStats.sql"
pgxc_class_ng_info="/pgxcClassCG.sql"
node_group_info="/NodeGroup.sql"
pg_setting_info="/pgSetting.sql"
pg_database_info="/pgDatabaseInfo.sql"
fake_node_group_info="/FakeNodeGroup.sql"
dump_log="/dumplog.log"

#progressbar var
progress_total=5
progress_done=0

#folder path
data_dir=""
temp_dir="/temp/"

#numbers of dumped files when enable_extended_statistic is no
files_number1=8
#numbers of dumped files when enable_extended_statistic is yes
files_number2=9

function logo()
{
	echo "--------------------------------------------------------------------------"
	echo "  _____  _                _____ _                 _       _               " 
	echo " |  __ \| |              / ____(_)               | |     | |              " 
	echo " | |__) | | __ _ _ __   | (___  _ _ __ ___  _   _| | __ _| |_ ___  _ __   " 
	echo " |  ___/| |/ _\` | |_  | |___  \| | |_ \` _ \| | | | |/ _\` | __/ _ \| |__|  " 
	echo " | |    | | (_| | | | |  ____) | | | | | | | |_| | | (_| | || (_) | |     " 
	echo " |_|    |_|\__,_|_| |_| |_____/|_|_| |_| |_|\__,_|_|\__,_|\__|___/|_|     "
	echo "                                                                          "
	echo "                                                 Version 3.0.0"
	echo "--------------------------------------------------------------------------"
}

function help()
{
	echo " "
	echo "Script includs 3 mandatory parameters (-m -p -A)"
	echo "-m: Script execution mode, there are three values, "
	echo "	  restore to restore from dumped database information,"
	echo "	  off 	  to shut down the running databases"
	echo "	  start   to start the restored database"
	echo "-p: Port number, the value is not specified when to environmental variables (when restore)"
	echo "-A: Directory path to restore"
	echo "Example"
	echo " (1)restore statistics info "
	echo '       $GAUSSHOME/bin/gs_plan_simulator.sh -m restore -p 5500 -A ../data/planSimulatorfiles/'
	echo " (2)shut down the running database "
	echo '       $GAUSSHOME/bin/gs_plan_simulator.sh -m off'
	echo " (3)start the restored database "
	echo '       $GAUSSHOME/bin/gs_plan_simulator.sh -m start'
	echo ""
	echo ""
}

# error type
# -1 parameter error
#  1 sql execution error
#  2 init db error
#  3 function usage error
function report_error()
{
	echo -e "\033[31m\033[01m[ERROR] $1 \033[0m"
	
	if [[ ! -z $3 ]]; then
		cd $data_dir".."
		python $init_script -o
	fi
	exit $2
}

function report_error_with_usage()
{
	echo -e "\033[31m\033[01m[ERROR] $1 \033[0m"
	help

	if [[ ! -z $3 ]]; then
		cd $data_dir".."
		python $init_script -o
	fi
	exit $2
}

function report_warning()
{
	echo -e "\033[33m\033[01m [WARNING] $1 \033[0m"
}

# function to handle gsql execution
function gsql_c_run()
{
	# $1 SQLConnect in the form of gsql -d <database> -p <portnumber> -U <username> 
	# $2 sql command
	# $3 0: >, 1: >>
	# $4 file to dump results. 
    if [[ ($# -eq 1) || ($# -eq 3) ]]; then
        report_error "Invalid usage of gsql_c_run" 3 off
    elif [ $# -eq 2 ]; then
    	echo "[SQL] $1 -c $2" >> $log_file_location 2>&1
    	$1 -c "$2" >> $log_file_location 2>&1
    elif [ $# -eq 4 ]; then
        echo "$1 -c '$2'" >> $log_file_location 2>&1
        if [ $3 -eq 0 ]; then
            $1 -c "$2" > $4
        else
            $1 -c "$2" >> $4
        fi
    else
        report_error "Invalid usage of gsql_c_run function" 3 off
    fi

    if [ $? != "0" ]; then
    	error_info="Error occured when executing:"
        if [ $# -eq 2 ]; then
    		error_info+="	$1 -c '$2'"
        elif [ $3 -eq 0 ]; then
        	error_info+="	$1 -c '$2' > $4"
        else
        	error_info+="	$1 -c '$2' >> $4"    
    	fi
    	error_info+="Please check gs_plan_simulator.log for details."
    	report_error $error_info 1 off
    fi
}

function gsql_f_run()
{
	# $1 SQLConnect in the form of gsql -d <database> -p <portnumber> -U <username> 
	# $2 sql file
	# $3 0: >, 1: >>
	# $4 file to dump results. 
    if [[ ($# -eq 1) || ($# -eq 3) ]]; then
        report_error "Invalid usage of gsql_f_run" 3 off
    elif [ $# -eq 2 ]; then
    	echo "[SQL] $1 -f $2" >> $log_file_location 2>&1
    	$1 -f "$2" >> $log_file_location 2>&1
    elif [ $# -eq 4 ]; then
        echo "$1 -f '$2'" >> $log_file_location 2>&1
        if [ $3 -eq 0 ]; then
            $1 -f "$2" > $4
        else
            $1 -f "$2" >> $4
        fi
    else
        report_error "Invalid usage of gsql_c_run function" 3 off
    fi

    if [ $? != "0" ]; then
    	error_info="Error occured when executing:"
        if [ $# -eq 2 ]; then
    		error_info+="	$1 -f '$2'"
        elif [ $3 -eq 0 ]; then
        	error_info+="	$1 -f '$2' > $4"
        else
        	error_info+="	$1 -f '$2' >> $4"
    	fi
    	error_info+="Please check gs_plan_simulator.log for details."
	    report_error $error_info 1 off
    fi
}

# restoring query related GUC configurations
# gs_clean_timeout=0 is essential when some of the datanodes are "fake"
# configurations are set by writing to postgresql.conf of the coordinator node.
# therefore, it is important to restart the database to load these configuration.
function restore_settings()
{
	echo "gs_clean_timeout = 0" >> "$data_dir""/coordinator1/postgresql.conf"
	cat "$pg_setting_info_location" | grep -E 'work_mem |max_process_memory|max_datanodes|query_mem' > temp.txt
	cat "$pg_setting_info_location" | grep -E 'enable_mergejoin|enable_nestloop|enable_indexonlyscan|enable_indexscan|enable_seqscan|enable_sort|enable_vector_engine' >> temp.txt
	sed -i 's/|/=/g' temp.txt
	cat temp.txt >> "$data_dir""/coordinator1/postgresql.conf"
	echo "enable_dynamic_workload = off" >> "$data_dir""/coordinator1/postgresql.conf"
	rm temp.txt

	cd $data_dir".."

	"python" $init_script -s > temp.log

	while [ 1 ] ; do
		NUM_SUCCESS=$(cat temp.log | grep "start ok" | wc -l)
		if [ $NUM_SUCCESS -eq 1 ] ; then
			break
		fi
		NUM_ERROR=$(cat $log_file_location |grep "Error" |wc -l)
		if [[ $NUM_ERROR -ge 1 ]]; then
			echo "Error occured when starting gaussdb, check gs_plan_simulator.log for more information."
			exit 1
		fi
		sleep 10
	done
	cd $prev_dir
}

function is_multiple_nodegroups()
{
	NODEGROUP_NUM=""
	NODEGROUP_NUM=$(cat $node_group_info_location | grep "CREATE NODE GROUP"| wc -l)
	if [ "$NODEGROUP_NUM"x != "1"x ] ; then
		report_error "Does not support multiple nodegroups when restoring $dbname." 3 off
		exit 1
	fi
}

function is_exist_gausshome()
{
	if [ x"$GAUSSHOME" == x ] ; then
		echo "Env variable \$GAUSSHOME not found."
		exit 1
	fi
}

function is_exist_python()
{
	python_path=$(which python)
	if [ -z "$python_path" ] ; then
		echo "python not found, check if python is installed"
		exit 1
	fi
}

# initialize database.
# if -N no, only initialize a 1-c-1-d local cluster and create many "fake" data node to simulate the cluster state.
# if -N yes, initialize according to nodegroup information.
# database is initialized by calling the pgxc.py script
# the script is modified to have compact port distribution and the base port can be changed
function init_database()
{

	if [[ !(-d $data_dir) ]]; then
		mkdir -p $data_dir
	fi

	DN_NUM=""

	if [ "$enable_node_group"x == "yes"x ] ; then
		DN_NUM=$(cat $node_group_info_location | grep "UPDATE pgxc_node"| wc -l)
	else
		DN_NUM=1
	fi

	prev_dir="$files_path"
	cd $data_dir
	"python" $init_script -c 1 -d $DN_NUM>>$log_file_location 2>&1 &
	cd $prev_dir
	data_dir=$data_dir"/data/"
	total_nodes=$((DN_NUM+2))
	((progress_total += total_nodes))
	while [ 1 ] ; do
		NUM_SUCCESS=$(cat $log_file_location | grep "Success" | wc -l)
		echo -ne "Initializing Database...          \r"
		NUM_ERROR=$(cat $log_file_location |grep "Error" |wc -l)
		if [ $NUM_SUCCESS -eq $((DN_NUM+2)) ] ; then
			break
		fi
		if [[ $NUM_ERROR -ge 1 ]]; then
			report_error "Error occured when initializing gaussdb, check gs_plan_simulator.log for more information." 2
		fi
		sleep 10
	done
	((progress_done = total_nodes))
	sleep 10

	for logfile in $(ls $data_dir/../log*)
	do
		cat "$logfile" | grep "Address already in use" > /dev/null
		if [ $? == 0 ]; then
			report_error "Port occupied by another process." 2
		fi
	done

	cd $data_dir".."
	"python" $init_script -o>>$log_file_location
	while [ 1 ] ; do
		NUM_SUCCESS=$(cat $log_file_location | grep "stop ok" | wc -l)
		if [ $NUM_SUCCESS -eq 1 ] ; then
			break
		fi
		NUM_ERROR=$(cat $log_file_location |grep "Error" |wc -l)
		if [[ $NUM_ERROR -ge 1 ]]; then
			report_error "Error occured when stoping gaussdb, check gs_plan_simulator.log for more information." 2
		fi
		sleep 10
	done
	((progress_done++))
	cd $prev_dir


}


# Generating datanode information in the form of SQL sentences.
# Information includes node name, node group name, member names for each group.
function generate_datanodeinfo()
{
	echo "DELETE FROM pgxc_group;"
	
	# Node
	node_names=$($sql_conect -t -A -c "select node_name from pgxc_node where node_type = 'D';")
	CNT=1
	for node_name in $node_names
	do
		
		node_define="UPDATE pgxc_node SET node_name = '$node_name' WHERE node_name = 'datanode$CNT';"
		
		CNT=$((CNT+1))
		 
		echo $node_define
	done
		
	
	# NodeGroup
	node_group_names=$($sql_conect -t -A  -c "select group_name from pgxc_group order by group_name;")
	for node_group_name in $node_group_names
	do
		 node_group_define="CREATE NODE GROUP "$node_group_name" with ( "
		 i=1
		 for group_member_oid in $($sql_conect -t -A  -c "select group_members from pgxc_group where group_name = '$node_group_name';")
		 do
				group_member_name=$($sql_conect -t -A  -c "select node_name from pgxc_node where  oid = $group_member_oid;")
				
				if [ $i -eq 1 ] ; then
					node_group_define=$node_group_define"  $group_member_name "
				else
					node_group_define=$node_group_define", $group_member_name "
				fi
				i=$((i+1))
		 done
		 node_group_define=$node_group_define");"
		echo $node_group_define
	done
}


# Creating fake datanode information for 1c1d setting when -N = no
# Fake info is generated according to the actual datanode information
# But all ports for datanodes are fake.
function fake_data_node_info()
{
	data_node_num=$(cat $node_group_info_location | grep "UPDATE pgxc_node"| wc -l)
    node_group_define="CREATE NODE GROUP "$node_group_name" with ("

   for (( step = 1; step <= $data_node_num; step++))
    do
        data_node_name=$data_node_prefix$step
        data_node_define=$data_node_define"CREATE NODE "$data_node_name" WITH (HOST = 'localhost', type = 'datanode', PORT = 42100);"
        data_node_define=$data_node_define"\n"
        node_group_define=$node_group_define$data_node_name 
        if [ $step -ne $data_node_num ]; then
            node_group_define=$node_group_define", "
        fi
    done

    node_group_define=$node_group_define");\n"
    
    replace_define="delete from pgxc_node where node_type = 'D';\n"
    replace_define=$replace_define"delete from pgxc_group;\n"
    replace_define=$replace_define$data_node_define
    replace_define=$replace_define$node_group_define

    echo -e $replace_define > $fake_node_group_info_location
}


function construct_command()
{
	sql_conect="gsql"
	dump_conect="gs_dump"

	if [ -n "$port" ]; then
		sql_conect+=" -p ""$port"
		dump_conect+=" -p ""$port"
	fi

	if [ -n "$user" ]; then
		sql_conect+=" -U ""$user"
		dump_conect+=" -U ""$user"
	fi

	sql_conect_pg="$sql_conect"" -d postgres"

	if [ -n "$dbname" ]; then
		sql_conect+=" -d ""$dbname"
		dump_conect+=" $dbname "
	fi
}

function parse_para()
{
	while getopts ':d:m:p:U:W:I:N:E:D:C:T:H:A:' arg
	do
		case $arg in
			d)
				dbname="$OPTARG"
				;;
			m)
				mode="$OPTARG"
				;;
			p)
				port="$OPTARG"
				;;
			U)
				user="$OPTARG"
				;;
			E)
				enable_extended_statistic="$OPTARG"
				;;
			N)
				enable_node_group="$OPTARG"
				;;
			D)
				dir="$OPTARG""/stats/"
				;;
			C)
				data_dir="$OPTARG"
				;;
			T)
				tables="$OPTARG"
				;;
			H)
				ishdfs="$OPTARG"
				;;
			A)
				all_db_dir="$OPTARG"
				;;
			?)
				report_error "Invalid Input paramater" -1
		esac
	done

	if [ -z "$mode" ]; then
		report_error_with_usage "Lack of Mode, usage is below:" -1
	elif [ "$mode"x == "dump"x ]; then
		files_path=$(which gs_plan_simulator.sh)
		files_path=${files_path%/*}
		echo "mode = "$mode
		if [ "x"$dir != "x" ] ; then
			echo "dir = "$dir
		else
			dir="$files_path""/${dbname}_"$(date "+%Y-%m-%d-%H%M%S"/stats)
        	report_warning "Directory to store dumped information not specified, using $dir as default."
        	echo "dir = "$dir
		fi
	elif [ "$mode"x == "restore"x ]; then
		is_exist_gausshome
		is_exist_python
		files_path="$GAUSSHOME""/bin"
		init_script="$files_path""$init_script"
		if [ "x"$data_dir == "x" ]; then
			data_dir="$GAUSSHOME""/bin/database/"
		fi
		if [ "x"$port == "x" ]; then
			report_error "Lack of Port Number, as well as environment default PGPORT value, $progname --help for more info" -1
		else
			lsof -i:$port > /dev/null
			if [[ $? == 0 ]]; then
				report_error "Port already occupied by an established database. Please check." 2
			fi
			base_port=$(cat $init_script|grep "base_port = "|awk -F '=' '{print $2}')

			sed -i 's/base_port ='"$base_port"'/base_port = '"$port"'/g' $init_script
		fi

		if [[ -z $dir  &&  -z $all_db_dir ]]; then
			report_error "Lack of target information folder, $progname --help for more info" -1
		fi
		if [[ -n $dir  &&  -n $all_db_dir ]]; then
			report_error "-d -A can't exist together" -1
		fi
	elif [ "$mode"x == "off"x ] ; then
		is_exist_python
		files_path="$GAUSSHOME""/bin"
		init_script="$files_path""$init_script"
		data_dir="$GAUSSHOME""/bin/database/"
		cd $data_dir
		python $init_script -o
		exit 0
	elif [ "$mode"x == "start"x ]; then
		is_exist_python
		files_path="$GAUSSHOME""/bin"
		init_script="$files_path""$init_script"
		data_dir="$GAUSSHOME""/bin/database/"
		cd $data_dir
		python $init_script -s
		exit 0
	else
		report_error "Invalid Mode, $progname --help for more info" -1 
	fi

	if [ ! -d $dir ] ; then
		mkdir -p $dir
	fi
}

# Dumping table creation sql sentences, table information, statistical information,
# extended statistical information, node group information and configuration settings.
function dump_stats()
{
	clean_temp_info="$files_path""$clean_temp_info"
	store_class_stats_info="$files_path""$store_class_stats_info"
	store_statistic_stats_info="$files_path""$store_statistic_stats_info"
	store_statistic_ext_stats_info="$files_path""$store_statistic_ext_stats_info"
	store_pgxc_class_info="$files_path""$store_pgxc_class_info"
    
	statsinfolation
	echo -e "\n***************Dump Version Info***************" >>$log_info_location 2>&1 
	$sql_conect -A -t -c "select version();" >>$log_info_location 2>&1
	if [[ $? != 0 ]]; then
		report_error "Unable to connect to server." 1
	fi

	temp_sql_pg_class_stats=$(cat $store_class_stats_info_location)
	temp_sql_statistic_stats=$(cat $store_statistic_stats_info_location)
	temp_sql_statistic_ext_stats=$(cat $store_statistic_ext_stats_info_location)
	temp_sql_pgxc_class=$(cat $store_pgxc_class_info_location)

	if [[ ! -z ${tables} ]]; then
		tables=$(echo $tables|sed 's/,/ /g')
		all_tables=($tables)
		echo "------------ $tables"
		echo -e "\n***************Dump Object Defination Info***************" 
		temp_sql_pg_class_stats+=" AND c.relname IN ("
		temp_sql_statistic_stats+=" AND c.relname IN ("
		temp_sql_statistic_ext_stats+=" AND c.relname IN ("
		temp_sql_pgxc_class+=" AND c.relname IN ("

		for t in ${all_tables[@]}
		do
			echo "+++++++++++ $t"
			$dump_conect -s -x -O -t "$t" >> $object_info_location
			if [[ $? != 0 ]]; then
				report_error "Error occured when dumping table info" 1
			fi
			temp_sql_pg_class_stats+=" '"$t"',"
			temp_sql_statistic_stats+=" '"$t"',"
			temp_sql_statistic_ext_stats+=" '"$t"',"
			temp_sql_pgxc_class+=" '"$t"',"
		done
		temp_sql_pg_class_stats="${temp_sql_pg_class_stats%?});"
		temp_sql_statistic_stats="${temp_sql_statistic_stats%?});"
		temp_sql_statistic_ext_stats="${temp_sql_statistic_ext_stats%?});"
		temp_sql_pgxc_class="${temp_sql_pgxc_class%?});"

		sed -i '/gs_dump\[port/d' $object_info_location
		echo "dump "$(cat $object_info_location | wc -l)" lines"

		echo -e "\n***************Dump pg_class stats Info***************"
		$sql_conect -c "${temp_sql_pg_class_stats}"
		$dump_conect -a -t pg_class_stats > $pg_class_stats_info_location 
		if [[ $? != 0 ]]; then
			report_error "Error occured when dumping class_stats info" 1
		fi
		sed -i '/gs_dump\[port/d' $pg_class_stats_info_location
		echo "dump "$(cat $pg_class_stats_info_location | wc -l)" lines"

		echo -e "\n***************Dump pg_statistic stats Info***************" 
		$sql_conect -c "${temp_sql_statistic_stats}"
		$dump_conect -a -t pg_statistic_stats > $pg_statistic_stats_info_location 
		if [[ $? != 0 ]]; then
			report_error "Error occured when dumping statistical info" 1
		fi
		sed -i '/gs_dump\[port/d' $pg_statistic_stats_info_location	
		echo "dump "$(cat $pg_statistic_stats_info_location | wc -l)" lines"

		if [ "$enable_extended_statistic"x == "yes"x ] ; then	
			echo -e "\n***************Dump pg_statistic_ext stats Info***************" 
			$sql_conect -c "${temp_sql_statistic_ext_stats}"
			$dump_conect -a -t pg_statistic_ext_stats > $pg_statistic_ext_stats_info_location
			if [[ $? != 0 ]]; then
				report_error "Error occured when dumping extended statistical info" 1
			fi
			sed -i '/gs_dump\[port/d' $pg_statistic_ext_stats_info_location
			echo "dump "$(cat $pg_statistic_stats_info_location | wc -l)" lines"
		fi

		echo -e "\n***************Dump NodeGroup Info***************" 
		generate_datanodeinfo > $node_group_info_location
		echo "dump "$(cat $node_group_info_location | wc -l)" lines"
		$sql_conect -c "${temp_sql_pgxc_class}"
		$dump_conect -a -t pgxc_class_ng > $pgxc_class_ng_info_location
		if [[ $? != 0 ]]; then
			report_error "Error occured when dumping table distribution info" 1
		fi
		sed -i '/gs_dump\[port/d' $pgxc_class_ng_info_location
	else
		echo -e "\n***************Dump Object Defination Info***************" >>$log_info_location 2>&1
		$dump_conect -s -x -O > $object_info_location
		if [[ $? != 0 ]]; then
			report_error "Error occured when dumping table info" 1
		fi
		sed -i '/gs_dump\[port/d' $object_info_location
		echo "dump "$(cat $object_info_location | wc -l)" lines" >>$log_info_location 2>&1

		echo -e "\n***************Dump pg_class stats Info***************" >>$log_info_location 2>&1
		$sql_conect --no-align --field-separator $'\t' --pset tuples_only -c "${temp_sql_pg_class_stats};" > $pg_class_stats_info_location
		if [[ $? != 0 ]]; then
			echo "Error occured when dumping class_stats info." >> $log_info_location 2>&1
			report_error "Error occured when dumping class_stats info." 1
		fi
		echo "dump "$(cat $pg_class_stats_info_location | wc -l)" lines">>$log_info_location 2>&1

		echo -e "\n***************Dump pg_statistic stats Info***************" >>$log_info_location 2>&1
		$sql_conect --no-align --field-separator $'\t' --pset tuples_only -c "${temp_sql_statistic_stats};" > $pg_statistic_stats_info_location
		if [[ $? != 0 ]]; then
			echo "Error occured when dumping statistical info." >> $log_info_location 2>&1
			report_error "Error occured when dumping statistical info." 1
		fi
		echo "dump "$(cat $pg_statistic_stats_info_location | wc -l)" lines">>$log_info_location 2>&1

		if [ "$enable_extended_statistic"x == "yes"x ] ; then	
			echo -e "\n***************Dump pg_statistic_ext stats Info***************" >>$log_info_location 2>&1
			$sql_conect --no-align --field-separator $'\t' --pset tuples_only -c "${temp_sql_statistic_ext_stats};" > $pg_statistic_ext_stats_info_location
			if [[ $? != 0 ]]; then
				echo "Error occured when dumping extended statistical info." >> $log_info_location 2>&1
				report_error "Error occured when dumping extended statistical info." 1
			fi
			echo "dump "$(cat $pg_statistic_stats_info_location | wc -l)" lines">>$log_info_location 2>&1
		fi

		echo -e "\n***************Dump NodeGroup Info***************" >>$log_info_location 2>&1
		generate_datanodeinfo > $node_group_info_location
		echo "dump "$(cat $node_group_info_location | wc -l)" lines">>$log_info_location 2>&1
		$sql_conect --no-align --field-separator $'\t' --pset tuples_only -c "${temp_sql_pgxc_class};" > $pgxc_class_ng_info_location
		if [[ $? != 0 ]]; then
			echo "Error occured when dumping table distribution info." >> $log_info_location 2>&1
			report_error "Error occured when dumping table distribution info." 1
		fi
		echo "dump "$(cat $pgxc_class_ng_info_location | wc -l)" lines" >> $log_info_location 2>&1
	fi

	echo -e "\n***************Dump Settings Info***************" >>$log_info_location 2>&1
	$sql_conect -t -A -c "select version();" > $pg_setting_info_location
	if [[ $? != 0 ]]; then
		echo "Error occured when dumping version info" >> $log_info_location 2>&1
		report_error "Error occured when dumping version info" 1
	fi
	$sql_conect -c "select name, setting from pg_settings order by name;" >> $pg_setting_info_location
	if [[ $? != 0 ]]; then
		echo "Error occured when dumping configuration info" >> $log_info_location 2>&1
		report_error "Error occured when dumping configuration info" 1
	fi
	$sql_conect -c "select * from pg_database order by datname;" >> $pg_setting_info_location
	if [[ $? != 0 ]]; then
		echo "Error occured when dumping pg_database info" >> $log_info_location 2>&1
		report_error "Error occured when dumping pg_database info" 1
	fi
	echo "dump "$(cat $pg_setting_info_location | wc -l)" lines">>$log_info_location 2>&1

	echo -e "\n***************Dump Database Info***************" >> $log_info_location 2>&1
	$sql_conect -c "select * from pg_database order by datname;" > $pg_database_info_location
	if [[ $? != 0 ]]; then
		echo "Error occured when dumping pg_database info" >> $log_info_location 2>&1
		report_error "Error occured when dumping pg_database info" 1
	fi
	echo "dump "$(cat $pg_database_info_location | wc -l)" lines">>$log_info_location 2>&1
}

function restore_alldb_stats()
{
	log_file_location="$files_path"$log_file_info
	if [ -f $log_file_location ] ; then
		rm $log_file_location
	fi
	restore_temp_info="$files_path""$restore_temp_info"
	temp_dir="$files_path""$temp_dir"
	sql_conect_pg="$files_path""/$sql_conect_pg"
	sql_conect="$files_path""/$sql_conect"
	connect="$files_path""/$connect"
	cmdpath=$(pwd)
	database_is_setting="false"
	for dir in $(ls $all_db_dir)
	do
		dbname=$dir
		if [[ $all_db_dir = /* ]]; then
			dir="$all_db_dir$dir""/stats"
		else
			dir="$cmdpath""/$all_db_dir$dir""/stats"
		fi
		n_files=$(ls $dir | wc -w)
		if [[ $? != 0 ]]; then
			echo "data_dir not found, please check." >>$log_file_location 2>&1
			echo -e "restore $dbname Failed.\n"
			continue
		elif [[ $n_files < $files_number1 ]]; then
			echo "Empty or incomplete information in $dir. Please check" >>$log_file_location 2>&1
			echo -e "restore $dbname Failed.\n"
			continue
		elif [[ $enable_extended_statistic == "yes" && $n_files < $files_number2 ]]; then
			echo "Extended statistical information not found in $dir. Please check" >>$log_file_location 2>&1
			echo -e "restore $dbname Failed.\n"
			continue
		fi

		statsinfolation
		is_multiple_nodegroups
		if [ "$database_is_setting" == "false" ];then
			echo -e "\n***************Initializing Database***************" >>$log_file_location 2>&1
			init_database
			restore_settings
			database_is_setting="true"
		fi
		echo "$sql_conect_pg"" -c CREATE DATABASE $dbname;"	>>$log_file_location 2>&1
		dbcom=$(cat $pg_database_info_location | grep -w $dbname | grep -n $dbname | awk '{print $24}')
		if [ "$dbname" != "postgres" ];then
			gsql_c_run "$sql_conect_pg" "CREATE DATABASE $dbname dbcompatibility='$dbcom';"
		fi

		db=$dbname
		connect="$sql_conect"" -d ""$dbname"
		std_path="$dir""/stats/"

		echo -e "\nRestore NodeGroup Information...\n\n"				>>$log_file_location 2>&1
		if [ "$enable_node_group"x == "yes"x ] ; then
			gsql_f_run "$connect" "$node_group_info_location"
		else
			if [[ !(-d $temp_dir) ]]; then
				mkdir -p $temp_dir
			fi
			cp $object_info_location $temp_object_info_location
			object_info_location=$temp_object_info_location
			cat $object_info_location |grep 'TO GROUP'|awk -F ';' '{print $1}'|sort --unique | while read line
			do
				sed -i "s/$line//g" $object_info_location
			done

			if [[ $ishdfs == 'yes' ]]; then
				tmp_dir="$files_path""/tablespace"$RANDOM
				conf="/opt/config"
				tmp_string=$(cat $object_info_location|grep 'create tablespace'|awk -F "'" '{print $2}')
				sed -i "s|$tmp_string|$tmp_dir|g" $object_info_location
				tmp_string=$(cat $object_info_location|grep 'cfgpath = '|awk -F "'" '{print $2}')
				sed -i "s|$tmp_string|$conf|g" $object_info_location
				tmp_string=$(cat $object_info_location|grep 'storepath = '|awk -F "'" '{print $2}')
				sed -i "s|$tmp_string|$tmp_dir|g" $object_info_location
			fi
		fi

		echo -e "\n\n\n"					>>$log_file_location 2>&1
		connect_c="$connect"" -c "
		connect_f="$connect"" -f "

		${connect_f}"$restore_temp_info_location" >> $log_file_location 2>&1
		if [[ $? != 0 ]]; then
			echo -e "restore $dbname Failed.\n"
			continue
		fi
		${connect_f}"$object_info_location" >> $log_file_location 2>&1
		if [[ $? != 0 ]]; then
			echo -e "restore $dbname Failed.\n"
			continue
		fi

		${connect_c}"\copy pg_class_stats from '$pg_class_stats_info_location';" >> $log_file_location 2>&1
		if [[ $? != 0 ]]; then
			echo -e "restore $dbname Failed.\n"
			continue
		fi
		${connect_c}"CALL restore_pg_class_stats(true);" >> $log_file_location 2>&1
		if [[ $? != 0 ]]; then
			echo -e "restore $dbname Failed.\n"
			continue
		fi

		${connect_c}"\copy pg_statistic_stats from '$pg_statistic_stats_info_location';" >> $log_file_location 2>&1
		if [[ $? != 0 ]]; then
			echo -e "restore $dbname Failed.\n"
			continue
		fi
		${connect_c}"CALL restore_pg_statistic_stats(true);" >> $log_file_location 2>&1
		if [[ $? != 0 ]]; then
			echo -e "restore $dbname Failed.\n"
			continue
		fi

		${connect_c}"\copy pg_statistic_ext_stats from '$pg_statistic_ext_stats_info_location';" >> $log_file_location 2>&1
		if [[ $? != 0 ]]; then
			echo -e "restore $dbname Failed.\n"
			continue
		fi
		${connect_c}"CALL restore_pg_statistic_ext_stats(true);" >> $log_file_location 2>&1
		if [[ $? != 0 ]]; then
			echo -e "restore $dbname Failed.\n"
			continue
		fi

		${connect_c}"\copy pgxc_class_ng from '$pgxc_class_ng_info_location';" >> $log_file_location 2>&1
		if [[ $? != 0 ]]; then
			echo -e "restore $dbname Failed.\n"
			continue
		fi
		${connect_c}"CALL restore_pgxc_class_ng(true);" >> $log_file_location 2>&1
		if [[ $? != 0 ]]; then
			echo -e "restore $dbname Failed.\n"
			continue
		fi
		
		echo -e "restoring $dbname Success.\n"
	done
	if [ "$enable_node_group"x == "no"x ] ; then
		echo -e "\n\n\n"			>>$log_file_location 2>&1
		fake_data_node_info
		gsql_f_run "$connect" "$fake_node_group_info_location"
		gsql_f_run "$connect" "$node_group_info_location"
	fi
	cd $cmdpath
	for dir in $(ls $all_db_dir)
	do
		dbname=$dir
		if [[ $all_db_dir = /* ]]; then
			dir="$all_db_dir$dir""/stats"
		else
			dir="$cmdpath""/$all_db_dir$dir""/stats"
		fi
		n_files=$(ls $dir | wc -w)
		if [[ $? != 0 ]]; then
			continue
		elif [[ $n_files < $files_number1 ]]; then
			continue
		elif [[ $enable_extended_statistic == "yes" && $n_files < $files_number2 ]]; then
			continue
		fi
		connect="$sql_conect"" -d ""$dbname"
		gsql_c_run "$connect" "CALL restore_nodeoids(true);"
	done

	rm $temp_object_info_location
	rm $fake_node_group_info_location
}

function statsinfolation()
{
	object_info_location=$dir$object_defination
	pg_class_stats_info_location=$dir$pg_class_stats_info
	pg_statistic_stats_info_location=$dir$pg_statistic_stats_info
	pg_statistic_ext_stats_info_location=$dir$pg_statistic_ext_stats_info
	node_group_info_location=$dir$node_group_info
	temp_object_info_location=$temp_dir$object_defination
	pgxc_class_ng_info_location=$dir$pgxc_class_ng_info
	fake_node_group_info_location=$temp_dir$fake_node_group_info
	pg_setting_info_location=$dir$pg_setting_info
	pg_database_info_location=$dir$pg_database_info
	log_info_location=$dir$dump_log
	restore_temp_info_location=$restore_temp_info
	clean_temp_info_location=$clean_temp_info
	store_class_stats_info_location=$store_class_stats_info
	store_statistic_stats_info_location=$store_statistic_stats_info
	store_statistic_ext_stats_info_location=$store_statistic_ext_stats_info
	store_pgxc_class_info_location=$store_pgxc_class_info
}



# return
#	success:			 0
#	parameter error:   	-1
#	gsql failure:		 1
#	port occupied:		 2
function main()
{
	if [ $# -eq 1 ]; then
		if [ $1 == "--help" -o $1 == "-help" ]; then
			help
			exit 0
		elif [ $1 == "-V" -o $1 == "-v" ]; then
			version
			exit 0
		fi
	fi

	parse_para $@ 
	construct_command 
	
	if [ "$mode"x == "dump"x ]; then
		dump_stats
	elif [ "$mode"x == "restore"x ]; then
		logo
		restore_alldb_stats
		echo "The database can accessed by using"
		echo '$GAUSSHOME/bin'"/gsql -p ${port} postgres -r"
		echo 'To shut down the database, use $GAUSSHOME/bin/gs_plan_simulator.sh -m off'
		echo 'To restart the datbase, use $GAUSSHOME/bin/gs_plan_simulator.sh -m start'
	fi
	return 0
}

main $@

