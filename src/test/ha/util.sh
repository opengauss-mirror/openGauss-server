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
  nodes=$1
  if [ -z $1 ]; then
    nodes=4
  fi
  #date
  #can grep datanode1 datanode1_standby datanode1_dummystandby gtm
  #ps -ef | grep $data_dir | grep -v grep
  echo query ps check
  if [ $(ps -ef | grep $data_dir | grep -v grep | wc -l) -eq $[$nodes+1] ]; then
    echo "ps check process success!"
  else
    sleep 2
	if [ $(ps -ef | grep $data_dir | grep -v grep | wc -l) -eq $[$nodes+1] ]; then
	  echo "ps check process success!"
	else
	  echo "$failed_keyword, ps check process failure, process info is displayed as: "
	  ps -ef | grep $data_dir | grep -v grep
      exit 1
	fi
  fi
  sleep 1

  check_dn_no_need_repair "datanode1"
  check_dn_no_need_repair "datanode1_standby"
  check_dn_no_need_repair "datanode2_standby"
  check_dn_no_need_repair "datanode3_standby"

  check_dn_state "datanode1" "db_state" "Normal" 1
  check_dn_state "datanode1_standby" "db_state" "Normal" 1
  check_dn_state "datanode2_standby" "db_state" "Normal" 1
  check_dn_state "datanode3_standby" "db_state" "Normal" 1
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
  gs_ctl build -D $data_dir/datanode1 -Z single_node
  if [ $? -eq 0 ]; then
    echo "standby built"
  else
    echo "$failed_keyword, build primary as standby failed"
    exit 1
  fi
}

function build_standby_as_standby() {
  echo "start building standby as standby"
  gs_ctl build -D $data_dir/datanode1_standby -Z single_node
  if [ $? -eq 0 ]; then
    echo "standby built"
  else
    echo "$failed_keyword, build standby as standby failed"
    exit 1
  fi
}

function build_standby2_as_standby() {
  echo "start building standby2 as standby"
  gs_ctl build -D $data_dir/datanode2_standby -Z single_node
  if [ $? -eq 0 ]; then
    echo "standby built"
  else
    echo "$failed_keyword, build standby2 as standby failed"
    exit 1
  fi
}

function build_standby3_as_standby() {
  echo "start building standby3 as standby"
  gs_ctl build -D $data_dir/datanode3_standby -Z single_node
  if [ $? -eq 0 ]; then
    echo "standby built"
  else
    echo "$failed_keyword, build standby3 as standby failed"
    exit 1
  fi
}

function build_standby4_as_standby() {
  echo "start building standby4 as standby"
  gs_ctl build -D $data_dir/datanode4_standby -Z single_node
  if [ $? -eq 0 ]; then
    echo "standby built"
  else
    echo "$failed_keyword, build standby4 as standby failed"
    exit 1
  fi
}

function cross_build_primary_as_standby() {
  echo "start building primary as standby"
  gs_ctl build -b cross_cluster_full -D $data_dir/datanode1 -Z single_node
  if [ $? -eq 0 ]; then
    echo "standby built"
  else
    echo "$failed_keyword, build primary as standby failed"
    exit 1
  fi
}

function cross_build_standby_as_standby() {
  echo "start building standby as standby"
  gs_ctl build -b cross_cluster_full -D $data_dir/datanode1_standby -Z single_node
  if [ $? -eq 0 ]; then
    echo "standby built"
  else
    echo "$failed_keyword, build standby as standby failed"
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

function kill_primary_cluster() {
  kill_primary
  kill_standby
}

function kill_standby_cluster() {
  kill_standby2
  kill_standby3
}

function start_primary_cluster() {
  cluster_dns=($primary_data_dir $standby_data_dir)
  for element in ${cluster_dns[@]}
  do
    gs_guc set -Z datanode -D $element -c "cluster_run_mode=cluster_primary"
  done
  start_primary_as_primary
  start_standby
}

function start_standby_cluster() {
  cluster_dns=($standby2_data_dir $standby3_data_dir)
  for element in ${cluster_dns[@]}
  do
    gs_guc set -Z datanode -D $element -c "cluster_run_mode=cluster_standby"
  done
  start_standby2
  start_standby3
}

function start_primary_cluster_as_standby() {
  cluster_dns=($primary_data_dir $standby_data_dir)
  for element in ${cluster_dns[@]}
  do
    gs_guc set -Z datanode -D $element -c "cluster_run_mode=cluster_standby"
  done
  start_primary_as_standby
  start_standby
}

function start_standby_cluster_as_primary() {
  cluster_dns=($standby2_data_dir $standby3_data_dir)
  for element in ${cluster_dns[@]}
  do
    gs_guc set -Z datanode -D $element -c "cluster_run_mode=cluster_primary"
  done
  start_standby2_as_primary
  start_standby3
}

function stop_primary_cluster() {
  stop_primary
  stop_standby
}

function stop_standby_cluster() {
  stop_standby2
  stop_standby3
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
      data_dir="datanode"$i"_standby"
      gs_guc set -Z datanode -D $data_dir -c "most_available_sync = on"  
  done  
  
}

function switchover_to_primary() {
  gs_ctl switchover -w -t $gsctl_wait_time -D $data_dir/datanode1
  if [ $? -eq 0 ]; then
    echo "switchover to primary success!"
  else
    echo "$failed_keyword, switchover to pirmary fail!"
    exit 1
  fi
}

function switchover_to_standby() {
  gs_ctl switchover -w -t $gsctl_wait_time -D $data_dir/datanode1_standby
  if [ $? -eq 0 ]; then
    echo "switchover to standby1 success!"
  else
    echo "$failed_keyword, switchover to standby1 fail!"
    exit 1
  fi
}

function switchover_to_standby2() {
  gs_ctl switchover -w -t $gsctl_wait_time -D $data_dir/datanode2_standby
  if [ $? -eq 0 ]; then
    echo "switchover to standby2 success!"
  else
    echo "$failed_keyword, switchover to standby2 fail!"
    exit 1
  fi
}

function switchover_to_standby3() {
  gs_ctl switchover -w -t $gsctl_wait_time -D $data_dir/datanode3_standby -f
  if [ $? -eq 0 ]; then
    echo "switchover to standby3 success!"
  else
    echo "$failed_keyword, switchover to standby3 fail!"
    exit 1
  fi
}

function switchover_to_standby4() {
  gs_ctl switchover -w -t $gsctl_wait_time -D $data_dir/datanode4_standby -f
  if [ $? -eq 0 ]; then
    echo "switchover to standby4 success!"
  else
    echo "$failed_keyword, switchover to standby4 fail!"
    exit 1
  fi
}


function failover_to_primary() {
  gs_ctl failover -w -t $gsctl_wait_time -D $data_dir/datanode1
  if [ $? -eq 0 ]; then
    echo "failover to primary success!"
  else
    echo "$failed_keyword, failover to primary fail!"
    exit 1
  fi
}

function failover_to_standby() {
  gs_ctl failover -w -t $gsctl_wait_time -D $data_dir/datanode1_standby
  if [ $? -eq 0 ]; then
    echo "failover to standby success!"
  else
    echo "$failed_keyword, failover to standby fail!"
    exit 1
  fi
}

function failover_to_standby2() {
  gs_ctl failover -w -t $gsctl_wait_time -D $data_dir/datanode2_standby
  if [ $? -eq 0 ]; then
    echo "failover to standby2 success!"
  else
    echo "$failed_keyword, failover to standby2 fail!"
    exit 1
  fi
}

function failover_to_standby3() {
  gs_ctl failover -w -t $gsctl_wait_time -D $data_dir/datanode3_standby
  if [ $? -eq 0 ]; then
    echo "failover to standby3 success!"
  else
    echo "$failed_keyword, failover to standby3 fail!"
    exit 1
  fi
}

function failover_to_standby4() {
  gs_ctl failover -w -t $gsctl_wait_time -D $data_dir/datanode4_standby
  if [ $? -eq 0 ]; then
    echo "failover to standby4 success!"
  else
    echo "$failed_keyword, failover to standby4 fail!"
    exit 1
  fi
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

###################################################################################################################
#cascade standby
#hacheck
#cascade_data_dir=$standby2_data_dir
function build_cascade_standby_as_cascade_standby() {
  echo "start building cascade standby as cascade standby"
  gs_ctl build -D $data_dir/datanode2_standby -Z single_node -M cascade_standby
  if [ $? -eq 0 ]; then
    echo "cascade standby built"
  else
    echo "$failed_keyword, build cascade standby as cascade_standby failed"
    exit 1
  fi
}
function build_cascade_standbys() {
        build_standby_as_standby
        build_cascade_standby_as_cascade_standby
}
function kill_cascade_cluster() {
  kill_primary
  kill_standby
  kill_cascade_standby
}
function start_cascade_cluster() {
  start_primary
  start_standby
  start_cascade_standby
}
function set_cascade_default() {
  kill_cascade_cluster
  set_cascade_default_helper
  start_cascade_cluster
  switchover_to_primary
  build_cascade_standbys
}
function set_cascade_default_helper() {
  cluster_dns=($primary_data_dir $standby_data_dir $standby2_data_dir)
  for element in ${cluster_dns[@]}
  do
    gs_guc set -Z datanode -D $element -c "synchronous_commit = on"
    gs_guc set -Z datanode -D $element -c "synchronous_standby_names = '*'"
    gs_guc set -Z datanode -D $element -c "most_available_sync = on"
    #gs_guc set -Z datanode -D $element -c "enable_mix_replication = off"
  done
}
function failover_to_cascade_standby() {
  gs_ctl failover -w -t $gsctl_wait_time -D $data_dir/datanode2_standby
  if [ $? -eq 0 ]; then
    echo "failover to cascade standby success!"
  else
    echo "$failed_keyword, failover to cascade standby fail!"
    exit 1
  fi
}
function switchover_to_cascade_standby() {
  gs_ctl switchover -w -t $gsctl_wait_time -D $data_dir/datanode2_standby
  if [ $? -eq 0 ]; then
    echo "switchover to cascade standby success!"
  else
    echo "$failed_keyword, switchover to cascade standby fail!"
    exit 1
  fi
}
function check_cascade_detailed_instance(){
  sleep 2
  #date
  #can grep datanode1 datanode1_standby datanode1_dummystandby gtm
  #ps -ef | grep $data_dir | grep -v grep
  echo query ps check cascade
  if [ $(ps -ef | grep $data_dir | grep -v grep | wc -l) -eq 3 ]; then
    echo "ps check cascade process success!"
  else
    sleep 2
        if [ $(ps -ef | grep $data_dir | grep -v grep | wc -l) -eq 5 ]; then
          echo "ps check cascade process success!"
        else
          echo "$failed_keyword, ps check cascade process failure, process info is displayed as: "
          ps -ef | grep $data_dir | grep -v grep
      exit 1
        fi
  fi
  sleep 1
  check_dn_no_need_repair "datanode1"
  check_dn_no_need_repair "datanode1_standby"
  check_dn_no_need_repair "datanode2_standby"
  check_dn_state "datanode1" "db_state" "Normal" 1
  check_dn_state "datanode1_standby" "db_state" "Normal" 1
  check_dn_state "datanode2_standby" "db_state" "Normal" 1
}

function start_streaming_cluster(){
  start_primary
  start_standby
  start_standby2
  echo "start hadr main standby $standby3_data_dir"
  $bin_dir/gaussdb --single_node -M hadr_main_standby -p $standby3_port -D $standby3_data_dir > ./results/gaussdb.log 2>&1 &
  echo "start cascade standby $standby4_data_dir"
  $bin_dir/gaussdb --single_node -M cascade_standby -p $standby4_port -D $standby4_data_dir > ./results/gaussdb.log 2>&1 &
}

function stop_streaming_cluster(){
  stop_primary
  stop_standby
  stop_standby2
  stop_standby3
  stop_standby4
}
