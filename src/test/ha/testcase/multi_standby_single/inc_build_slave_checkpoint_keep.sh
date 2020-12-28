#!/bin/sh
# we mock a scene that primary do a normal checkpoint but checkpoint xlog send to standby 
# suggest standby keep all xlog, we check this scene to make sure the feature that checkpoint xlog from primary can
# contronl xlog recyle in standby is working
source ./util.sh

function test_1()
{
  set_default
  kill_cluster
  switch_xlog_num=250
  #gs_guc set -Z datanode -D $standby_data_dir -c "distribute_test_param='0,DN_SLAVE_CHECKPOINT_KEEPXLOG,NOTICE,1'"
  #gs_guc set -Z datanode -D $primary_data_dir -c "distribute_test_param='0,DN_SLAVE_CHECKPOINT_KEEPXLOG,NOTICE,1'"
  gs_guc set -Z datanode -D  $primary_data_dir -c "wal_keep_segments = 16"
  gs_guc set -Z datanode -D  $standby_data_dir -c "wal_keep_segments = 16"
  start_cluster
  echo "start cluter success!"
  for((integer = 1; integer <= $switch_xlog_num; integer++))
  do
    gsql -d $db -p $dn1_primary_port -c "select pg_switch_xlog();"
  done
  xlog_num=`ls ${primary_data_dir}/pg_xlog/ | wc -l`
  echo " ${primary_data_dir}/pg_xlog/  xlog num is ${xlog_num} pg_switch_xlog exec num $switch_xlog_num"
  
  if [[ $((xlog_num)) -lt $switch_xlog_num ]]
  then
    echo "primary xlog keep success"
  else
    echo "primary xlog keep $failed_keyword"
  fi

  xlog_num=`ls ${standby_data_dir}/pg_xlog/ | wc -l`
  echo " ${standby_data_dir}/pg_xlog/  xlog num is ${xlog_num} pg_switch_xlog exec num $switch_xlog_num"
  
  if [[ $((xlog_num)) -gt $switch_xlog_num ]]
  then
    echo "xlog keep xuccess"
  else
    echo "xlog keep $failed_keyword"
  fi
}

function tear_down() {
  #gs_guc reload -Z datanode -D $standby_data_dir -c "distribute_test_param='0,NOT_EXIST,NOTICE,1'"
  #gs_guc reload -Z datanode -D $primary_data_dir -c "distribute_test_param='0,NOT_EXIST,NOTICE,1'"
  sleep 1
}

test_1
tear_down
