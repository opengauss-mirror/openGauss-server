#!/bin/sh
# logical decoding on standby

source ./util.sh

function test_1()
{
  set_default

  echo "set logical decoding parameters"
  kill_cluster
  gs_guc set -Z datanode -D $primary_data_dir -c "wal_level = logical"
  gs_guc set -Z datanode -D $primary_data_dir -c "enable_slot_log = on"
  gs_guc set -Z datanode -D $primary_data_dir -h "local    replication     $USER                                trust"
  gs_guc set -Z datanode -D $standby_data_dir -c "wal_level = logical"
  gs_guc set -Z datanode -D $standby_data_dir -c "hot_standby = on"
  gs_guc set -Z datanode -D $standby_data_dir -h "local    replication     $USER                                trust"
  start_cluster

  #create table
  gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists decode_test; CREATE TABLE decode_test(id INT,name VARCHAR(15) NOT NULL);"
  echo "drop table success"

  #create logical replication slot
  pg_recvlogical -d $db -p $dn1_primary_port -S test --create
  if [ $? -eq 0 ]; then
    echo "create replication slot success"
  else
    echo "$failed_keyword: create replication slot failed."
    exit 1
  fi

  sleep 1

  #start logical decoding on standby
  echo "begin to decode"
  nohup pg_recvlogical -d $db -p $dn1_standby_port -S test --start -f - &
  if [ $? -eq 0 ]; then
    echo "logical decoding on standby success"
  else
    echo "$failed_keyword: logical decoding on standby failed."
    exit 1
  fi

  #insert
  gsql -d $db -p $dn1_primary_port -c "insert into decode_test values(1, 'gaussdb');"
  gsql -d $db -p $dn1_primary_port -c "insert into decode_test values(2, 'opengauss');"
  sleep 30
}

function tear_down()
{
  set_default
  sleep 3
  #kill pg_recvlogical
  ps -ef | grep pg_recvlogical | grep -v grep | awk '{print $2}' | xargs kill -9

  #drop table
  gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists decode_test;"

  #drop logical replication slot
  pg_recvlogical -d $db -p $dn1_primary_port -S test --drop
  if [ $? -eq 0 ]; then
    echo "drop replication slot success"
  else
    echo "$failed_keyword: drop replication slot failed."
    exit 1
  fi
}

test_1
tear_down
