#!/bin/sh
# to make sure that standby will use inc build after down and reconnect
source ./util.sh

function test_1()
{
  kill_primary_cluster
  kill_standby_cluster
  start_primary_cluster
  start_standby_cluster

  echo "start cluter success!"
  inc_build_pattern="server started"
  kill_standby3
  echo "standy killed"
  build_result=`gs_ctl build -b cross_cluster_incremental -Z single_node -D ${standby3_data_dir}`
  echo $build_result
  if [[ $build_result =~ $inc_build_pattern ]]
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
