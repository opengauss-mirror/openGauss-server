#!/bin/sh

source ./standby_env.sh
#node_num=4

function check_dn_state() {
  sleep 1
  echo "query $1 $2 $3 is $4"
  if [ $(gs_ctl query -D $data_dir/$1 | grep "$2" | grep "$3" | wc -l) -eq $4 ]; then
    echo "query $1 $2 $3 is $4 success!"
  else
	sleep 3
	if [ $(gs_ctl query -D $data_dir/$1 | grep "$2" | grep "$3" | wc -l) -eq $4 ]; then
	  echo "query $1 $2 $3 is $4 success!"	
	else
	  echo "$failed_keyword, check_dn_state $1 failed, the query result is display as follows: "
	  gs_ctl query -D $data_dir/$1
	  exit 1
	fi
  fi
}

function check_dn_no_need_repair() {
  check_dn_state $1 "" "Need repair" 0
}

function check_detailed_instance(){
  sleep 2
  #date
  #can grep datanode1 datanode1_standby datanode1_dummystandby gtm
  #ps -ef | grep $data_dir | grep -v grep
  echo query ps check
  if [ $(ps -ef | grep $data_dir | grep -v grep | wc -l) -eq 5 ]; then
    echo "ps check process success!"
  else
    sleep 2
    if [ $(ps -ef | grep $data_dir | grep -v grep | wc -l) -eq 5 ]; then
	echo "ps check process success!"
    else
	echo "$failed_keyword, ps check process failure, process info is displayed as: "
	ps -ef | grep $data_dir | grep -v grep
    	exit 2
    fi
  fi
  sleep 1

  check_dn_no_need_repair "datanode1"
  check_dn_no_need_repair "datanode1_standby"
  check_dn_no_need_repair "datanode2_standby"
  check_dn_no_need_repair "datanode3_standby"
  check_dn_no_need_repair "datanode4_standby"

  check_dn_state "datanode1" "db_state" "Normal" 1
  check_dn_state "datanode1_standby" "db_state" "Normal" 1
  check_dn_state "datanode2_standby" "db_state" "Normal" 1
  check_dn_state "datanode3_standby" "db_state" "Normal" 1
  check_dn_state "datanode4_standby" "db_state" "Normal" 1
}

function check_primary() {
  check_dn_no_need_repair $1
  check_dn_state $1 "db_state" "Normal" 1
  check_dn_state $1 peer_role "Standby" $2
  check_dn_state $1 peer_state "Normal" $2
}

function check_synchronous_commit() {
  check_dn_state $1 "sync_state" "Sync" $2
}

function check_quorum_synchronous_commit() {
  check_dn_state $1 "sync_state" "Quorum" $2
}

function check_asynchronous_commit() {
  check_dn_state $1 "sync_state" "Async" $2
}

function check_most_available() {
  check_dn_state $1 "sync_most_available" $2 $3
}

function build_primary_as_standby() {
  echo "start building primary as standby"
  gs_ctl build -D $data_dir/datanode1 -Z single_node -b full
  if [ $? -eq 0 ]; then
    echo "standby built"
  else
    echo "$failed_keyword, build primary as standby failed"
    exit 1
  fi
}

function build_standby_as_standby() {
  echo "start building standby as standby"
  gs_ctl build -D $data_dir/datanode1_standby -Z single_node -b full
  if [ $? -eq 0 ]; then
    echo "standby built"
  else
    echo "$failed_keyword, build standby as standby failed"
    exit 1
  fi
}

function build_standby2_as_standby() {
  echo "start building standby2 as standby"
  gs_ctl build -D $data_dir/datanode2_standby -Z single_node -b full
  if [ $? -eq 0 ]; then
    echo "standby built"
  else
    echo "$failed_keyword, build standby2 as standby failed"
    exit 1
  fi
}

function build_standby3_as_standby() {
  echo "start building standby3 as standby"
  gs_ctl build -D $data_dir/datanode3_standby -Z single_node -b full
  if [ $? -eq 0 ]; then
    echo "standby built"
  else
    echo "$failed_keyword, build standby3 as standby failed"
    exit 1
  fi
}

function build_standby4_as_standby() {
  echo "start building standby4 as standby"
  gs_ctl build -D $data_dir/datanode4_standby -Z single_node -b full
  if [ $? -eq 0 ]; then
    echo "standby built"
  else
    echo "$failed_keyword, build standby4 as standby failed"
    exit 1
  fi
}

function build_standbys() {
	build_standby_as_standby
	build_standby2_as_standby
	build_standby3_as_standby
	build_standby4_as_standby
}

function kill_cluster() {
  kill_primary
  kill_standby
  kill_standby2
  kill_standby3
  kill_standby4
}

function start_cluster() {
  start_primary
  start_standby
  start_standby2
  start_standby3
  start_standby4
}

function set_data_replicate() {
  kill_cluster
  set_data_replicate_helper
  start_cluster
}

function set_most_available_sync() {
  kill_cluster
  set_most_available_sync_helper
  start_cluster
}

function set_default() {
  kill_cluster
  set_default_helper
  start_cluster
  switchover_to_primary
  build_standbys
}

function set_default_paxos() 
{
   kill_cluster
   set_default_helper
   start_paxos $1
}

function set_default_helper() {
  cluster_dns=($primary_data_dir $standby_data_dir $standby2_data_dir $standby3_data_dir)
  for element in ${cluster_dns[@]}
  do
    gs_guc set -Z datanode -D $element -c "synchronous_commit = on"
    gs_guc set -Z datanode -D $element -c "synchronous_standby_names = '*'"
    gs_guc set -Z datanode -D $element -c "most_available_sync = on"
    #gs_guc set -Z datanode -D $element -c "enable_mix_replication = off"
  done
}

function set_enable_mix_replication() {
  cluster_dns=($primary_data_dir $standby_data_dir $standby2_data_dir $standby3_data_dir $standby4_data_dir)
  #for element in ${cluster_dns[@]}
  #do
    #gs_guc set -Z datanode -D $element -c "enable_mix_replication = $1"
  #done
}


#function set_data_replicate_helper() {
  #gs_guc set -Z datanode -D $primary_data_dir -c "enable_data_replicate = on"
  #gs_guc set -Z datanode -D $standby_data_dir -c "enable_data_replicate = on"
  #gs_guc set -Z datanode -D $dummystandby_data_dir -c "enable_data_replicate = on"
  #gs_guc set -Z datanode -D $standby2_data_dir -c "enable_data_replicate = off"
#}

function set_most_available_sync_helper() {
  gs_guc set -Z datanode -D $primary_data_dir -c "most_available_sync = on"
  gs_guc set -Z datanode -D $standby_data_dir -c "most_available_sync = on"
  for((i=2;i<$node_num+1;i++))
  do
      tmp_data_dir="datanode"$i"_standby"
      gs_guc set -Z datanode -D $tmp_data_dir -c "most_available_sync = on"  
  done  
  
}

function start_node() {
  gs_ctl start -D $1 -M standby
  sleep 2s
  gs_ctl query -D $1
  if [ ! $? -eq 0 ]; then
    echo "$failed_keyword, start $1 failed!"
    exit 1
  fi
}

function switchover_to_primary() {
  check_old_primary_dir
  if [ -z ${current_primary} ];then
	  echo "get old primary dir failed!"
	  exit 1
  fi
  gs_ctl switchover -w -t $gsctl_wait_time -D $data_dir/datanode1
  if [ $? -eq 0 ]; then
    echo "switchover to primary success!"
    if [ "$data_dir/datanode1" != ${current_primary} ]; then
      start_node ${current_primary}
    fi
  else
    echo "$failed_keyword, switchover to primary fail!"
    exit 1
  fi
}

function switchover_to_standby() {
  check_old_primary_dir
  if [ -z ${current_primary} ];then
	  echo "get old primary dir failed!"
	  exit 1
  fi
  gs_ctl switchover -w -t $gsctl_wait_time -D $data_dir/datanode1_standby
  if [ $? -eq 0 ]; then
    echo "switchover to standby1 success!"
    # start old primary
    if [ "$data_dir/datanode1_standby" != ${current_primary} ]; then
      start_node ${current_primary}
    fi
  else
    echo "$failed_keyword, switchover to standby1 fail!"
    exit 1
  fi
}

function switchover_to_standby2() {
  check_old_primary_dir
  if [ -z ${current_primary} ];then
	  echo "get old primary dir failed!"
	  exit 1
  fi
  gs_ctl switchover -w -t $gsctl_wait_time -D $data_dir/datanode2_standby
  if [ $? -eq 0 ]; then
    echo "switchover to standby2 success!"
    # start old primary
    if [ "$data_dir/datanode2_standby" != ${current_primary} ]; then
      start_node ${current_primary}
    fi
  else
    echo "$failed_keyword, switchover to standby2 fail!"
    exit 1
  fi
}

function switchover_to_standby3() {
  check_old_primary_dir
  if [ -z ${current_primary} ];then
	  echo "get old primary dir failed!"
	  exit 1
  fi
  gs_ctl switchover -w -t $gsctl_wait_time -D $data_dir/datanode3_standby
  if [ $? -eq 0 ]; then
    echo "switchover to standby3 success!"
    # start old primary
    if [ "$data_dir/datanode3_standby" != ${current_primary} ]; then
      start_node ${current_primary}
    fi
  else
    echo "$failed_keyword, switchover to standby3 fail!"
    exit 1
  fi
}

function switchover_to_standby4() {
  check_old_primary_dir
  if [ -z ${current_primary} ];then
	  echo "get old primary dir failed!"
	  exit 1
  fi
  gs_ctl switchover -w -t $gsctl_wait_time -D $data_dir/datanode4_standby
  if [ $? -eq 0 ]; then
    echo "switchover to standby4 success!"
    # start old primary
    if [ "$data_dir/datanode4_standby" != ${current_primary} ]; then
      start_node ${current_primary}
    fi
  else
    echo "$failed_keyword, switchover to standby4 fail!"
    exit 1
  fi
}


function kill_current_primary() {
    nodes=($primary_data_dir $standby_data_dir $standby2_data_dir $standby3_data_dir $standby4_data_dir)
    for((i = 0;i<=${node_num};i++))
    do
      gs_ctl query -D ${nodes[$i]} | grep -o -E "Primary"
        if [ $? -eq 0 ];then
           if [ $i == 0 ];then
                current_primary=$primary_data_dir
		kill_primary
                break
           fi
           if [ $i == 1 ];then
                current_primary=$standby_data_dir
		kill_standby
                break
           fi
           if [ $i == 2 ];then
		current_primary=$standby2_data_dir
    		kill_standby2
                break
           fi
           if [ $i == 3 ];then
		current_primary=$standby3_data_dir
    		kill_standby3
                break
           fi
           if [ $i == 4 ];then
		current_primary=$standby4_data_dir
		kill_standby4
                break
           fi
        fi
    done
    # sleep for a while
    sleep 5
    if [ -z ${current_primary} ];then
        echo "get old primary dir failed!"
        exit 1
    fi
    # start old primary
    gs_ctl start -D ${current_primary} -M standby -Z single_node
}


function print_time() {
  cur_time=`date +"%F %T.%N" | cut -c1-23`
  echo -n "[${cur_time}] "
}

function wait_recovery_done() {
  # wait for recovery to complete on primary node
  # this function is incomplete as it does not find the correct log line (still need to support minimum timstamp for search)
  recovery_done=0
  node_data_dir=$1
  wait_time_seconds=$2
  last_log=`ls -ltr ${node_data_dir}/pg_log/postgresql-* | tail -1 | awk '{print $9}'`
  for i in `seq 1 $wait_time_seconds`;
  do
    recovery_done=`grep "database system is ready to accept read only connections" $last_log | wc -l`
    if [ $recovery_done -eq 1 ]; then
      print_time
      echo "Recovery done on node detected after $i seconds at: $node_data_dir"
      break
    fi
    sleep 1
  done

  if [ $recovery_done -eq 0 ]; then
    print_time
    echo "Failed to find recovery done message after $wait_time_seconds seconds in node log at: $node_data_dir"
  fi
}

#check_synchronous_commit "datanode1" 1
#check_detailed_instance
#check_primary "datanode1" 2

# encapsulate change role cmd
function run_change_role() {
  # timeout is 10s
  role=$1
  tmp_data_dir=$2
  timeout=10
  gs_ctl changerole -R ${role} -D ${tmp_data_dir} -t ${timeout}
}

# encapsulate change run mode cmd
function set_run_mode_cmd() {
  retry=3
  runmode=$1
  tmp_data_dir=$2
  for((i=0; i<${retry}; i++))
  do
    echo "begin to set ${runmode} mode for ${tmp_data_dir}"
    if [ "${runmode}" = "minority" ]; then
      votenum=$3
      gs_ctl setrunmode -x ${runmode} -v ${votenum} -D ${tmp_data_dir}
    elif [ "${runmode}" = "normal" ]; then
      gs_ctl setrunmode -x ${runmode} -D ${tmp_data_dir}
    else
      echo "${failed_keyword}, Run mode is not supported!"
      exit 1
    fi
    if [ $? != 0 ]; then
      echo "Change run mode of ${tmp_data_dir} to ${runmode} failed and retry!"
      continue
    fi
    return 0
  done
  echo "${failed_keyword}, change run mode of ${tmp_data_dir} to ${runmode} failed after three times of retry!"
  exit 1
}

# encapsulate remove member
function remove_member() {
  node_id=$1
  gs_ctl member --operation=remove --nodeid=${node_id} -D ${current_primary}
  if [ ! $? -eq 0 ]; then
    echo "${failed_keyword}, Run remove member cmd failed"
    exit 1
  fi
  sleep 3s
  # still exist
  gs_ctl query -D ${current_primary} | grep "\"node_id\":${node_id}"
  if [ $? -eq 0 ]; then
    echo "${failed_keyword}, remove member failed"
    exit 1
  fi
}
# encapsulate add member
function add_member() {
  node_id=$1
  ip=$2
  port=$3
  gs_ctl member --operation=add --nodeid=${node_id} --ip=${ip} --port=${port} -D ${current_primary}
  if [ ! $? -eq 0 ]; then
    echo "${failed_keyword}, Run add member cmd failed"
    exit 1
  fi
  gs_ctl query -D ${current_primary} | grep "\"node_id\":${node_id}"
  # expect existing nodeid
  if [ ! $? -eq 0 ]; then
    echo "${failed_keyword}, add member failed"
    exit 1
  fi
}
