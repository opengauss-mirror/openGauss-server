#!/bin/sh
# 1 standby3 request standby2 full build.
source ./util.sh

function test_1()
{
  set_default
  kill_cluster
  start_cluster

  echo "start cluter success!"
  inc_build_pattern="waiting for server to start..."
  echo "standby2_build_standby3"
  build_result=`gs_ctl build -b standby_full -D ${standby3_data_dir} -C "localhost=127.0.0.1 localport=$standby3_port remotehost=127.0.0.1 remoteport=$standby2_port"`
  if [[ $build_result =~ $inc_build_pattern ]]
  then
    echo "standby build success"
  else
    echo "standby build $failed_keyword"
  fi
}

function tear_down() {
  sleep 1
  kill_cluster
  start_cluster
}

test_1
tear_down
