#!/bin/sh
source ./util.sh

function test_1()
{
  kill_cluster
  gs_guc set -D $primary_data_dir -Z datanode -c "hadr_recovery_time_target=30"
  gs_guc set -D $primary_data_dir -Z datanode -c "hadr_recovery_point_target=30"
  start_streaming_cluster
  echo "start cluter success!" 
  echo "match guc parameter"
  gsql -d postgres -p ${dn1_primary_port} -c "select * from gs_hadr_local_rto_and_rpo_stat();"
}

function tear_down() {
  stop_streaming_cluster
}

test_1
tear_down
