#!/bin/sh
source ./util.sh

function test_1()
{
  kill_cluster
  start_streaming_cluster
  build_pattern="build completed"
  echo "start main cluster success!"
  echo "start standby cluster"
  build_result=`gs_ctl build -Z single_node -D ${standby4_data_dir} -b standby_full -M cascade_standby`
  echo "repair cascade_standby"
  if [[ $build_result =~ $build_pattern ]]
  then
    echo "repair standby4 success!"
  else
    echo "$failed_keyword, build standby4 failed!"
    exit 1
  fi
}

function tear_down() {
  stop_streaming_cluster
}

test_1
tear_down
