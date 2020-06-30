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
  gsql -d $db -p $dn1_primary_port -c "copy mot_switch1 from '$g_data_path/datanode1/pg_copydir/data5';"
  
  echo "start cluter success!"
  inc_build_pattern="dn incremental build completed"
  kill_primary
  echo "primary killed"
  failover_to_standby
  echo "failover_to_standby"
  echo build
  #sleep 5
  build_result=`gs_ctl build -D ${primary_data_dir}`
  #build_result=`gs_ctl build -D ${primary_data_dir} -b full`
  echo $build_result
  if [[ $build_result =~ $inc_build_pattern ]]
  then
    echo "inc build success"
  else
    echo "inc build $failed_keyword"
  fi

  sleep 5

  gsql -d $db -p $dn1_primary_port -m -c "select count(1) from mot_switch1;"
  if [ $(gsql -d $db -p $dn1_primary_port -m -c "select count(1) from mot_switch1;" | grep `expr 1 \* $rawdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_primary"
  else
	echo "copy $failed_keyword on dn1_primary"
  fi

  gsql -d $db -p $dn1_standby_port -m -c "select count(1) from mot_switch1;"
  if [ $(gsql -d $db -p $dn1_standby_port -m -c "select count(1) from mot_switch1;" | grep `expr 1 \* $rawdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_standby"
  else
	echo "copy $failed_keyword on dn1_standby"
  fi
}

function tear_down() {
  sleep 1
  failover_to_primary
  gsql -d $db -p $dn1_standby_port -m -c " select * from pg_drop_replication_slot('dn_s2');"
  gsql -d $db -p $dn1_standby_port -m -c " select * from pg_drop_replication_slot('dn_s3');"
  gsql -d $db -p $dn1_standby_port -m -c " select * from pg_drop_replication_slot('dn_s4');"
  gsql -d $db -p $dn1_standby_port -m -c " select * from pg_drop_replication_slot('dn_p1');"
  gsql -d $db -p $dn1_primary_port -c "DROP FOREIGN TABLE if exists mpp_test1;"
}

test_1
tear_down
