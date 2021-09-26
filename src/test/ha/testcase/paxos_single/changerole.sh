#!/bin/sh

source ./util_paxos.sh
source ./deploy_paxos_single.sh
function change_leader_role_case() {
  # Change leader to follow. Failure result was excepted
  check_old_primary_dir
  if [ -z ${current_primary} ];then
    echo "${failed_keyword}, get old primary dir failed!"
    exit 1
  fi
  run_change_role follower ${current_primary}
  if [ $? -eq 0 ]; then
    echo "${failed_keyword}, it's not allower to change leader to follower!"
    exit 1
  elif [ $? -eq 2 ]; then
    echo "${failed_keyword}, change role timeout!"
    exit 1
  elif [ $? -eq 1 ]; then
    echo "Can't change leader to follower......pass"
  else
    echo "${failed_keyword}, unknown error!"
    exit 1
  fi
}

function change_follower_role_case() {
  standby_dir=""
  data_dir=$g_data_path
  str_arr=(`check_paxos | grep -o -E "Standby|Primary"`)
  for((i=0;i<=${node_num};i++))
  do
    if [ "${str_arr[$i]}" != "Primary" ];then
      if [ $i == 0 ]; then
        standby_dir=$primary_data_dir
        break
      fi
      if [ $i == 1 ]; then
        standby_dir=$standby_data_dir
        break
      fi
      if [ $i == 1 ]; then
        standby_dir=$standby_data_dir
        break
      fi
      if [ $i == 2 ]; then
              standby_dir=$standby2_data_dir
        break
      fi
      if [ $i == 3 ]; then
              standby_dir=$standby3_data_dir
        break
      fi
      if [ $i == 4 ]; then
              standby_dir=$standby4_data_dir
        break
      fi
    fi
  done
  if [ ${node_num} -gt 3 ]; then
    run_change_role passive ${standby_dir}
    if [ $? != 0 ]; then
      echo "${failed_keyword}, change role failed!"
      exit 1
    else
      # Resume status to change passive to follower
      run_change_role follower ${standby_dir}
      echo "Change role successfully...........pass"
    fi
  fi
}

function test_1()
{
  check_primary
  if [ -z $primary_port ];then
    echo "${failed_keyword}, get primary port failed"
    exit 1
  fi
  echo "cluster setup completed----------------"
  #create table
  #gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy cstore_copy_t1 from '$scripts_dir/data/cstore_copy_t1.data' delimiter '|';"
  #gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists cstore_copy_t1; create table cstore_copy_t1(c1 int2, c2 int4, c3 int8, c4 char(10), c5 varchar(12),c6 numeric(10,2)) with (orientation = column);"
  gsql -d $db -p $primary_port -c "DROP TABLE if exists cstore_copy_t1; create table cstore_copy_t1(c1 int2, c2 int4, c3 int8, c4 char(10), c5 varchar(12),c6 numeric(10,2));"

  #copy data(25M) to primary
  cat $scripts_dir'/data/cstore_copy_t1.data' | python tools.py cstore_copy_t1 '|' > $scripts_dir'/data/cstore_copy_t1.data.sql'
  gsql -d $db -p $primary_port < $scripts_dir'/data/cstore_copy_t1.data.sql'
  echo "copy success"

  echo "begin to change leader role"
  change_leader_role_case

  echo "Begin to change follower role"
  change_follower_role_case
  #test the insert results
  b=`wc $scripts_dir'/data/cstore_copy_t1.data.sql' | awk '{print $1}'`
  if [ $(gsql -d $db -p $standby3_port -c "select count(1) from cstore_copy_t1;" | grep $b | wc -l) -eq 1 ]; then
    echo "test insert result success!"
  else
    echo "test insert result, ${failed_keyword}!"
    exit 1
  fi
}
test_1
