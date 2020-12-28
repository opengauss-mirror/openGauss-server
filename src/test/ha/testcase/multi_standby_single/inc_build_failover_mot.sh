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

  #create mot data
  gsql -d $db -p $dn1_primary_port -c "DROP FOREIGN TABLE if exists mot_switch1; CREATE FOREIGN TABLE mot_switch1(id INT,name VARCHAR(15) NOT NULL) SERVER mot_server;"
  gsql -d $db -p $dn1_primary_port -c "copy mot_switch1 from '$scripts_dir/data/data5';"

  print_time
  echo "start cluter success!"

  inc_build_pattern="dn incremental build completed"
  print_time
  echo "killing primary"
  kill_primary

  print_time
  echo "primary killed"

  print_time
  echo "failing over to standby"
  failover_to_standby

  print_time
  echo "failover_to_standby DONE"

  print_time
  echo build
  #sleep 5
  build_result=`gs_ctl build -Z single_node -D ${primary_data_dir}`
  #build_result=`gs_ctl build -Z single_node -D ${primary_data_dir} -b full`
  echo $build_result
  print_time
  if [[ $build_result =~ $inc_build_pattern ]]
  then
    echo "inc build success"
  else
    echo "inc build $failed_keyword"
  fi

  # wait for recovery to complete on primary node
  sleep 30

  gsql -d $db -p $dn1_primary_port -m -c "select count(1) from mot_switch1;"
  print_time
  if [ $(gsql -d $db -p $dn1_primary_port -m -c "select count(1) from mot_switch1;" | grep `expr 1 \* $rawdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_primary"
  else
	echo "copy $failed_keyword on dn1_primary"
  fi

  gsql -d $db -p $dn1_standby_port -m -c "select count(1) from mot_switch1;"
  print_time
  if [ $(gsql -d $db -p $dn1_standby_port -m -c "select count(1) from mot_switch1;" | grep `expr 1 \* $rawdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_standby"
  else
	echo "copy $failed_keyword on dn1_standby"
  fi
}

function tear_down() {
  sleep 1
  print_time
  echo "Test done. Tearing down cluter. Failing over to primary after test"
  failover_to_primary
  gsql -d $db -p $dn1_standby_port -m -c " select * from pg_drop_replication_slot('dn_s2');"
  gsql -d $db -p $dn1_standby_port -m -c " select * from pg_drop_replication_slot('dn_s3');"
  gsql -d $db -p $dn1_standby_port -m -c " select * from pg_drop_replication_slot('dn_s4');"
  gsql -d $db -p $dn1_standby_port -m -c " select * from pg_drop_replication_slot('dn_p1');"
  gsql -d $db -p $dn1_primary_port -c "DROP FOREIGN TABLE if exists mpp_test1;"
}

test_1
tear_down
