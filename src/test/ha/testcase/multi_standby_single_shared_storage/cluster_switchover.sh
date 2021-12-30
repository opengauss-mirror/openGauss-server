#!/bin/sh
# cluster switchover

source ./util.sh

function test_1()
{
  kill_primary_cluster
  kill_standby_cluster
  start_primary_cluster
  start_standby_cluster
  sleep 30
  check_detailed_instance 3
  #create table
  gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test1; CREATE TABLE mpp_test1(id INT,name VARCHAR(15) NOT NULL);"
  echo "drop table success"

  #prepare insert sql
  cat $scripts_dir'/data/data5_head_100' | python tools.py mpp_test1 '|' > $scripts_dir'/data/data5_head_100_sql'
  gsql -d $db -p $dn1_primary_port < $scripts_dir'/data/data5_head_100_sql' &> /dev/null

  #test the insert results
  b=`wc $scripts_dir'/data/data5_head_100_sql' | awk '{print $1}'`
  echo "b=" $b
  if [ $(gsql -d $db -p $dn1_primary_port -c "select count(1) from mpp_test1;" | grep $b | wc -l) -eq 1 ]; then
    echo "insert into table success!"
  else
    echo "insert into table failure $failed_keyword!"
    exit 1
  fi

  sleep 1

  echo "begin to switch cluster"
  stop_primary_cluster
  stop_standby_cluster
  sleep 10
  start_standby_cluster_as_primary
  start_primary_cluster_as_standby
  sleep 10
  echo "end of swtich to standby1"

  #test the insert results
  b=`wc $scripts_dir'/data/data5_head_100_sql' | awk '{print $1}'`
  if [ $(gsql -d $db -p $standby2_port -c "select count(1) from mpp_test1;" | grep $b | wc -l) -eq 1 ]; then
    echo "test insert result success!"
  else
    echo "test insert result, $failed_keyword!"
    exit 1
  fi
}

function tear_down()
{
  gsql -d $db -p $standby2_port -c "DROP TABLE if exists mpp_test1;"
  stop_standby_cluster
  stop_primary_cluster
  sleep 5
}

test_1
tear_down
