#!/bin/sh
# to make sure that standby will use inc build after down and reconnect
source ./util.sh

function test_1()
{
  set_default
  kill_cluster
  start_cluster

  echo "start cluter success!"
  inc_build_pattern="dn incremental build completed"
  kill_standby
  echo "standy killed"
  build_result=`gs_ctl build -Z single_node -D ${standby_data_dir}`
  if [[ $build_result =~ $inc_build_pattern ]]
  then
    echo "inc build success"
  else
    echo "inc build $failed_keyword"
  fi
}

function tear_down() {
  sleep 1
}

test_1
tear_down
