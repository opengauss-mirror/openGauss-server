#!/bin/sh
# 1 standyby1 down
# 2 old primary down 
# 2 standby2 become new primary and working
# 3 standyby1 start quickly,then should inc build not full build;
source ./util.sh

function test_1()
{
  set_default
  kill_cluster
  start_cluster
  echo "start cluter success!"
  
  #create mot data
  gsql -d $db -p $dn1_primary_port -c "DROP FOREIGN TABLE if exists mot_switch1; CREATE FOREIGN TABLE mot_switch1(id INT,name VARCHAR(15) NOT NULL) SERVER mot_server;"
  gsql -d $db -p $dn1_primary_port -c "copy mot_switch1 from '$scripts_dir/data/data5';"
  
  inc_build_pattern="dn incremental build completed"
  kill_standby2
  kill_primary
  echo "primary killed"
  failover_to_standby
  echo "failover_to_standby"
  build_result=`gs_ctl build -Z single_node -D ${standby2_data_dir}`
  if [[ $build_result =~ $inc_build_pattern ]]
  then
    echo "inc build success"
  else
    echo "inc build $failed_keyword"
  fi
  
  gsql -d $db -p $dn1_standby_port  -c "select count(1) from mot_switch1;"
  if [ $(gsql -d $db -p $dn1_standby_port -c "select count(1) from mot_switch1;" | grep `expr 1 \* $rawdata_lines` |wc -l) -eq 1 ]; then
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
  gsql -d $db -p $dn1_standby_port -c "DROP FOREIGN TABLE if exists mpp_test1;"
}

test_1
tear_down
