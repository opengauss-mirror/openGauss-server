#!/bin/sh
# to make sure that standby will use inc build after down and reconnect
source ./util.sh

function test_1()
{
  kill_primary_cluster
  kill_standby_cluster

  # update xlog file size
  gs_guc set -Z datanode -D $primary_data_dir -c "xlog_file_size = 107374182400"
  gs_guc set -Z datanode -D $standby_data_dir -c "xlog_file_size = 107374182400"
  gs_guc set -Z datanode -D $standby2_data_dir -c "xlog_file_size = 107374182400"
  gs_guc set -Z datanode -D $standby3_data_dir -c "xlog_file_size = 107374182400"  

  start_primary_cluster
  start_standby_cluster

  echo "start cluter success!"
  build_pattern="server started"

  kill_standby
  echo "standy of primary cluster is killed"
  build_result=`gs_ctl build -b standby_full -Z single_node -D ${standby_data_dir}`
  echo $build_result
  if [[ $build_result =~ $build_pattern ]]
  then
    echo "inc build success"
  else
    echo "inc build $failed_keyword"
  fi

  kill_standby3
  echo "standy of standby cluster is killed"
  build_result=`gs_ctl build -b standby_full -Z single_node -D ${standby3_data_dir}`
  echo $build_result
  if [[ $build_result =~ $build_pattern ]]
  then
    echo "inc build success"
  else
    echo "inc build $failed_keyword"
  fi
}

function tear_down() {
  stop_primary_cluster
  stop_standby_cluster
  sleep 5
}

test_1
tear_down
