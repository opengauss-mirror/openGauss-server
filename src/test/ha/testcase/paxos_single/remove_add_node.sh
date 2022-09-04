#!/bin/sh

source ./util_paxos.sh
source ./deploy_paxos_single.sh
function test_change_majority_groups() {
  check_primary
  if [ -z $primary_port ];then
    echo "${failed_keyword}, get primary port failed"
    exit 1
  fi
  
  # Check reload guc param dcf_majority_groups
  gs_guc reload -D ${data_dir}/datanode1 -c "dcf_majority_groups = '0'"
  gsql -d $db -p $primary_port -c "show dcf_majority_groups;"
  gs_guc reload -D ${data_dir}/datanode1 -c "dcf_majority_groups = ''"
  gsql -d $db -p $primary_port -c "show dcf_majority_groups;"

}

#replace node case
function remove_and_add_member() {
  dcf_port=$1
  # Check we can remove member
  check_primary
  if [ -z $primary_port ];then 
    echo "${failed_keyword}, get primary port failed"
    exit 1
  fi
  gsql -d $db -p $primary_port -c "DROP TABLE if exists cstore_copy_t1;"
  check_old_primary_dir
  gs_ctl switchover -D $primary_data_dir
  if [ ! $? -eq 0 ]; then
    echo "${failed_keyword}, Switchover to primary data dir failed!"
    exit 1
  fi
  if [ "$primary_data_dir" != "${current_primary}" ]; then
    gs_ctl start -D ${current_primary} -M standby
    gs_ctl query -D ${current_primary}
    if [ ! $? -eq 0 ]; then
      echo "${failed_keyword}, start previous data node failed!"
      exit 1
    fi
  fi
  node_id=5
  check_old_primary_dir
  remove_member ${node_id}
  # Stop the removed node
  gs_ctl stop -D $standby4_data_dir
  sleep 2s
  # add member
  new_node_port=`expr $standby4_port + 3`
  new_dcf_port=`expr $dcf_port + 5`
  add_member ${node_id} 127.0.0.1 ${new_dcf_port}
  python ./pgxc_paxos_single.py -c 1 -d 5 -n ${node_id} -a -p ${dcf_port} -q ${new_dcf_port}
  new_data_dir=${data_dir}"/datanode5_standby"
  gs_ctl start -D ${new_data_dir} -M standby
  sleep 5s
  gs_ctl query -D ${new_data_dir}
  if [ $? -eq 1 ]; then
    echo "${failed_keyword}, start adding member failed"
    exit 1
  fi
  gs_ctl build -b full -D ${new_data_dir}
  sleep 5
  gs_ctl query -D ${new_data_dir}
  if [ ! $? -eq 0 ]; then
    echo "${failed_keyword}, build member added failed"
    exit 1
  fi
  # Check if adding member can receive data
  check_primary
  if [ -z $primary_port ];then 
    echo "${failed_keyword}, get primary port failed"
    exit 1
  fi
  gsql -d $db -p $primary_port -c "DROP TABLE if exists cstore_copy_t1; create table cstore_copy_t1(c1 int2, c2 int4, c3 int8, c4 char(10), c5 varchar(12),c6 numeric(10,2));"
  #copy data(25M) to primary
  cat $scripts_dir'/data/cstore_copy_t1.data' | python tools.py cstore_copy_t1 '|' > $scripts_dir'/data/cstore_copy_t1.data.sql'
  gsql -d $db -p $primary_port < $scripts_dir'/data/cstore_copy_t1.data.sql'
  echo "copy success"
  sleep 5s
  #test the insert results
  echo "new node port is $new_node_port"
  b=`wc $scripts_dir'/data/cstore_copy_t1.data' | awk '{print $1}'`
  if [ $(gsql -d $db -p $new_node_port  -c "select count(1) from cstore_copy_t1;" | grep $b | wc -l) -eq 1 ]; then
    echo "test insert result success!"
  else
    echo "test insert result, $failed_keyword!"
    exit 1
  fi
  #resume env
  echo "Begin to resume env"
  remove_member ${node_id}
  gs_ctl stop -D ${new_data_dir}
  gs_ctl query -D ${new_data_dir}
  if [ $? -eq 0 ]; then
    echo "$failed_keyword, stop new node failed!"
  fi
  add_member ${node_id} 127.0.0.1 `expr ${dcf_port} + 4`
  start_node $standby4_data_dir
  run_change_role follower $standby4_data_dir
  rm -rf ${new_data_dir}
  echo "resume env end"
}
function test_1()
{
  set_default_paxos $1
  sleep 5s
  echo "cluster setup completed----------------"
  remove_and_add_member $1
  test_change_majority_groups
}
test_1 $1
