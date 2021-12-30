#!/bin/sh
source ./util.sh

function test_1()
{
  kill_cluster
  start_streaming_cluster
  echo "start cluter success!"
  kill_standby3
  echo "hadr main standby killed"
  gs_ctl failover -Z single_node -D $standby4_data_dir
  echo "failover_to_cascade_standby"
  sleep 5
  check_dn_state "datanode4_standby" "db_state" "Normal" 1
}

function tear_down() {
  stop_streaming_cluster
}

test_1
tear_down
