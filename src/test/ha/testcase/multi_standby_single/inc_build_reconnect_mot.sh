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
  gsql -d $db -p $dn1_primary_port -c "copy mot_switch1 from '$scripts_dir/data/data5';"
  
  inc_build_pattern="dn incremental build completed"
  print_time
  echo "Killing stadnby..."
  kill_standby
  print_time
  echo "standy killed, building standby..."
  build_result=`gs_ctl build -Z single_node -D ${standby_data_dir}`
  print_time
  if [[ $build_result =~ $inc_build_pattern ]]
  then
    echo "inc build success"
  else
    echo "inc build $failed_keyword"
  fi

  # wait for recovery on standby to complete
  sleep 30

  print_time
  echo "Querying primary..."
  gsql -d $db -p $dn1_primary_port -c "select count(1) from mot_switch1;"
  print_time
  if [ $(gsql -d $db -p $dn1_primary_port -c "select count(1) from mot_switch1;" | grep `expr 1 \* $rawdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_primary"
  else
	echo "copy $failed_keyword on dn1_primary"
  fi

  print_time
  echo "Attempting access to standby"
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
  gsql -d $db -p $dn1_primary_port -c "DROP FOREIGN TABLE if exists mpp_test1;"
}

test_1
tear_down
