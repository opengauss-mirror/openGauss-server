#!/bin/sh

source ./util_paxos.sh
source ./deploy_paxos_single.sh
function set_run_mode_case() {
  vote_num=`expr ${node_num} / 2`
  kill_num=`expr ${node_num} + 1 - ${vote_num}`
  echo "Kill three nodes"
  kill_primary
  kill_standby
  kill_standby2
  sleep 10s
  gs_ctl query -D $standby3_data_dir
  if [ ! $? -eq 0 ]; then
    gs_ctl start -D $standby3_data_dir -M standby
  fi
  gs_ctl query -D $standby4_data_dir
  if [ ! $? -eq 0 ]; then
    gs_ctl start -D $standby4_data_dir -M standby
  fi
  echo "There should be no primary"
  current_primary=""
  check_old_primary_dir
  if [ -z ${current_primary} ];then
    echo "get old primary dir failed.....pass!"
  else
    echo "${failed_keyword}, primary still existed!"
    exit 1
  fi
  set_run_mode_cmd minority ${standby3_data_dir} ${vote_num}
  set_run_mode_cmd minority ${standby4_data_dir} ${vote_num}
  sleep 10s
  echo "There should be a primary"
  check_primary
  if [ -z $primary_port ];then
    echo "${failed_keyword}, get primary port failed"
    exit 1
  fi
  echo "Check if we can insert data"
  gsql -d $db -p $primary_port -c "DROP TABLE if exists cstore_copy_t1; create table cstore_copy_t1(c1 int2, c2 int4, c3 int8, c4 char(10), c5 varchar(12),c6 numeric(10,2));"
  if [ ! $? -eq 0 ]; then
    echo "${failed_keyword}, create table failed!"
    exit 1
  fi
  #copy data(25M) to primary
  cat $scripts_dir'/data/cstore_copy_t1.data' | python tools.py cstore_copy_t1 '|' > $scripts_dir'/data/cstore_copy_t1.data.sql'
  gsql -d $db -p $primary_port < $scripts_dir'/data/cstore_copy_t1.data.sql'
  if [ ! $? -eq 0 ]; then
    echo "${failed_keyword}, insert data failed!"
    exit 1
  fi
  echo "copy success"
  sleep 5s
  #test the insert results
  test_port=0
  if [ ! ${standby3_port} -eq ${primary_port} ]; then
    test_port=${standby3_port}
  else
    test_port=${standby4_port}
  fi
  b=`wc $scripts_dir'/data/cstore_copy_t1.data.sql' | awk '{print $1}'`
  if [ $(gsql -d $db -p ${test_port} -c "select count(1) from cstore_copy_t1;" | grep $b | wc -l) -eq 1 ]; then
    echo "test insert result success!"
  else
    echo "test insert result, ${failed_keyword}!"
    exit 1
  fi
  echo "Change minority mode success"
  echo "Begin to change normal mode"
  echo "start killed nodes and change to normal mode"
  kill_nodes=(${primary_data_dir} ${standby_data_dir} ${standby2_data_dir})
  for((i=0;i<${kill_num};i++))
  do
    gs_ctl start -D ${kill_nodes[${i}]} -M standby
    sleep 2s
    gs_ctl query -D ${kill_nodes[${i}]}
    if [ $? -eq 1 ]; then
      echo "${failed_keyword}, start DN failed when recovering the env in change run mode"
      exit 1
    fi
  done
  set_run_mode_cmd normal ${standby3_data_dir}
  set_run_mode_cmd normal ${standby4_data_dir}
}
function test_1()
{
  #set_default_paxos
  #check_instance_multi_standby
  #cstore_rawdata_lines=8
  check_primary
  if [ -z $primary_port ];then 
    echo "get primary port failed"
    exit 1
  fi
  echo "cluster setup completed----------------"
  set_run_mode_case
}
test_1
