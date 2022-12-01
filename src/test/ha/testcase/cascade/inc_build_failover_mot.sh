#!/bin/sh
# 1 old primary down
# 2 new primary is working
# 3 old primary start as standby ,then should inc build not full build;
source ./util.sh

function test_1()
{
  set_cascade_default
  kill_cascade_cluster
  start_cascade_cluster

  echo "start cluter success!"

  gsql -d $db -p $dn1_primary_port -c "DROP FOREIGN TABLE if exists mot_switch1; CREATE FOREIGN TABLE mot_switch1(id INT,name VARCHAR(15) NOT NULL) SERVER mot_server;"
  gsql -d $db -p $dn1_primary_port -c "copy mot_switch1 from '$scripts_dir/data/data5';"

  echo "mot data loaded"

  inc_build_pattern="dn incremental build completed"
  kill_primary
  echo "primary killed"
  failover_to_standby
  echo "failover_to_standby"
  build_result=`gs_ctl build -Z single_node -D ${standby2_data_dir} -M cascade_standby`
  if [[ $build_result =~ $inc_build_pattern ]]
  then
    echo "inc build success"
  else
    echo "inc build $failed_keyword"
  fi

  failover_to_cascade_standby
  sleep 1
  kill_standby
  sleep 1
  failover_to_cascade_standby
  build_result=`gs_ctl build -Z single_node -D ${standby_data_dir} -M cascade_standby`
  if [[ $build_result =~ $inc_build_pattern ]]
  then
    echo "inc build success"
  else
    echo "inc build $failed_keyword"
  fi
  build_result=`gs_ctl build -Z single_node -D ${primary_data_dir}`
  if [[ $build_result =~ $inc_build_pattern ]]
  then
    echo "inc build success"
  else
    echo "inc build $failed_keyword"
  fi

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

  gsql -d $db -p $standby2_port -m -c "select count(1) from mot_switch1;"
  print_time
  if [ $(gsql -d $db -p $standby2_port -m -c "select count(1) from mot_switch1;" | grep `expr 1 \* $rawdata_lines` |wc -l) -eq 1 ]; then
        echo "copy success on cascade standby"
  else
        echo "copy $failed_keyword on cascade standby"
  fi

}

function tear_down() {
  sleep 1
  failover_to_primary
  sleep 1
  failover_to_standby
  gsql -d $db -p $dn1_standby_port -m -c " select * from pg_drop_replication_slot('dn_c2');"
  gsql -d $db -p $dn1_standby_port -m -c " select * from pg_drop_replication_slot('dn_p1');"
}

test_1
tear_down
