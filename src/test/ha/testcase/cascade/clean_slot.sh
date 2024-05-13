#!/bin/sh
 
source ./util.sh
 
function check_select_result()
{
  if [ $(echo $result | grep "${1}" | wc -l) -eq 1 ]; then
    echo "remote read successful"
  else
    echo "remote read failed $failed_keyword with [$result]"
    exit 1
  fi
}
 
function check_select_no_result()
{
  if [ $(echo $result | grep "${1}" | wc -l) -eq 0 ]; then
    echo "remote read successful"
  else
    echo "remote read failed $failed_keyword with [$result]"
    exit 1
  fi
}
 
function test_cascade_standby_clean_slot_func()
{
  set_default
 
  echo "base"
  result=`gsql -d $db -p $dn1_primary_port -c "select * from pg_get_replication_slots();"`
  check_select_result "dn_s2"
 
  kill_cascade_cluster
  start_cascade_cluster
  
 
  result=`gsql -d $db -p $dn1_primary_port -c "select * from pg_get_replication_slots();"`
  check_select_no_result "dn_s2"
 
  switchover_to_cascade_standby
 
  result=`gsql -d $db -p $dn1_primary_port -c "select * from pg_get_replication_slots();"`
  check_select_no_result "dn_s1"
 
}
 
function tear_down() {
  set_cascade_default
}
 
test_cascade_standby_clean_slot_func
tear_down