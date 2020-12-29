#!/bin/sh
# 1 old primary down 
# 2 new primary is working
# 3 old primary start as standby ,then should inc build not full build;
source ./util.sh

function test_1()
{
  set_default
  kill_cluster
  start_cluster

  echo "start cluter success!"
  inc_build_pattern="dn incremental build completed"
  kill_primary
  echo "primary killed"
  failover_to_standby
  echo "failover_to_standby"
  build_result=`gs_ctl build -Z single_node -D ${primary_data_dir}`
  if [[ $build_result =~ $inc_build_pattern ]]
  then
    echo "inc build success"
  else
    echo "inc build $failed_keyword"
  fi
}

function tear_down() {
  sleep 1
  failover_to_primary
  gsql -d $db -p $dn1_standby_port -m -c " select * from pg_drop_replication_slot('dn_s2');"
  gsql -d $db -p $dn1_standby_port -m -c " select * from pg_drop_replication_slot('dn_s3');"
  gsql -d $db -p $dn1_standby_port -m -c " select * from pg_drop_replication_slot('dn_s4');"
  gsql -d $db -p $dn1_standby_port -m -c " select * from pg_drop_replication_slot('dn_p1');"
}

test_1
tear_down
