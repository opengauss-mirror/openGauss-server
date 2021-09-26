#!/bin/sh
# switchover when primary-standby-dummystandby all ready

source ./util.sh
source ./deploy_paxos_single.sh
function test_1()
{
  set_default_paxos $1
  echo "begin to switch to primary"
  check_detailed_instance
  check_primary
  #create table
  gsql -d $db -p $primary_port -c "DROP TABLE if exists mpp_test1; CREATE TABLE mpp_test1(id INT,name VARCHAR(15) NOT NULL);"
  echo "drop table success"

  #copy error, use insert to by-pass
  #copy data(25M) to standby 1 times
  #gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test1 from '$scripts_dir/data/data5';"

  #prepare insert sql
  cat $scripts_dir'/data/data5_head_100' | python tools.py mpp_test1 '|' > $scripts_dir'/data/data5_head_100_sql'
  gsql -d $db -p $primary_port < $scripts_dir'/data/data5_head_100_sql' &> /dev/null

  #test the insert results
  b=`wc $scripts_dir'/data/data5_head_100_sql' | awk '{print $1}'`
  echo "b=" $b
  if [ $(gsql -d $db -p $primary_port -c "select count(1) from mpp_test1;" | grep $b | wc -l) -eq 1 ]; then
    echo "insert into table success!"
  else
    echo "insert into table failure $failed_keyword!"
    exit 1
  fi

  sleep 1
  echo "begin to switch to primary"
  #switchover
  switchover_to_primary
  echo "end of swtich to primary"

  sleep 10

  echo "begin to switch to standby1"
  #switchover
  switchover_to_standby
  echo "end of swtich to standby1"

  sleep 10

  echo "begin to switch to standby2"
  #switchover
  switchover_to_standby2
  echo "end of swtich to standby2"

  sleep 10

  echo "begin to switch to standby3"
  #switchover
  switchover_to_standby3
  echo "end of swtich to standby3"

  #echo "begin to switch to standby4"
  #switchover
  #switchover_to_standby4
  #echo "end of swtich to standby4"


  #test the insert results
  b=`wc $scripts_dir'/data/data5_head_100_sql' | awk '{print $1}'`
  if [ $(gsql -d $db -p $standby3_port -c "select count(1) from mpp_test1;" | grep $b | wc -l) -eq 1 ]; then
    echo "test insert result success!"
  else
    echo "test insert result, $failed_keyword!"
    exit 1
  fi
}
test_1 $1
