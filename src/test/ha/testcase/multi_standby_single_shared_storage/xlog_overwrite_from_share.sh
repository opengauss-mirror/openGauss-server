#!/bin/sh
# to make sure that standby will use inc build after down and reconnect
source ./util.sh

function test_1()
{
  kill_primary_cluster
  kill_standby_cluster
  cstore_rawdata_lines=8
  sleep 5

  echo "begin to start primary dn"
  start_primary_as_primary
  #create table
  gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists cstore_copy_t1; create table cstore_copy_t1(c1 int2, c2 int4, c3 int8, c4 char(10), c5 varchar(12),c6 numeric(10,2));"
  #copy data(25M) to primary
  cat $scripts_dir'/data/cstore_copy_t1.data' | python tools.py cstore_copy_t1 '|' > $scripts_dir'/data/cstore_copy_t1.data.sql'
  gsql -d $db -p $dn1_primary_port < $scripts_dir'/data/cstore_copy_t1.data.sql'
  echo "copy success"
 
  # overwrite xlog from shared storage to local
  overwrite_result2=`gs_ctl copy -Q copy_from_share -D ${standby_data_dir}`
  if [[ $overwrite_result2 =~ $overwrite_pattern ]]
  then
    echo "xlog copy from share success"
  else
    echo "xlog copy from share $failed_keyword"
  fi
  
  echo "begin to start standby"
  start_standby
}

function tear_down() {
  stop_primary_cluster
  sleep 5
}

test_1
tear_down
