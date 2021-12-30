#!/bin/sh
source ./util.sh

function test_1()
{
  kill_cluster
  start_streaming_cluster
  echo "start cluster success!"
  kill_primary
  echo "primary has been killed"
  gs_ctl failover -Z single_node -D $standby_data_dir
  echo "failover to standby"
  check_dn_state "datanode1_standby" "db_state" "Normal" 1
  sleep 20
  echo "check hadr main standby"
  check_dn_state "datanode3_standby" "db_state" "Normal" 1
}

function tear_down() {
  stop_streaming_cluster
}

test_1
tear_down
