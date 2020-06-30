#!/bin/sh
# to make sure that standby will use inc build after down and reconnect
source ./util.sh

function test_1()
{
  set_default
  kill_cluster
  start_cluster

  echo "start cluter success!"
  #create mot data
  gsql -d $db -p $dn1_primary_port -c "DROP FOREIGN TABLE if exists mot_switch1; CREATE FOREIGN TABLE mot_switch1(id INT,name VARCHAR(15) NOT NULL) SERVER mot_server;"
  gsql -d $db -p $dn1_primary_port -c "copy mot_switch1 from '$g_data_path/datanode1/pg_copydir/data5';"
  
  inc_build_pattern="dn incremental build completed"
  kill_standby
  echo "standy killed"
  build_result=`gs_ctl build -D ${standby_data_dir}`
  if [[ $build_result =~ $inc_build_pattern ]]
  then
    echo "inc build success"
  else
    echo "inc build $failed_keyword"
  fi

  sleep 5

  gsql -d $db -p $dn1_primary_port -c "select count(1) from mot_switch1;"
  if [ $(gsql -d $db -p $dn1_primary_port -c "select count(1) from mot_switch1;" | grep `expr 1 \* $rawdata_lines` |wc -l) -eq 1 ]; then
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
  gsql -d $db -p $dn1_primary_port -c "DROP FOREIGN TABLE if exists mpp_test1;"
}

test_1
tear_down
