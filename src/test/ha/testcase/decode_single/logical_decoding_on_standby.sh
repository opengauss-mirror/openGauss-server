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
  gs_guc set -Z datanode -D $primary_data_dir -c "undo_zone_count = 16384"
  gs_guc set -Z datanode -D $primary_data_dir -h "local    replication     $USER                                trust"
  gs_guc set -Z datanode -D $standby_data_dir -c "wal_level = logical"
  gs_guc set -Z datanode -D $standby_data_dir -c "hot_standby = on"
  gs_guc set -Z datanode -D $standby_data_dir -c "undo_zone_count = 16384"
  gs_guc set -Z datanode -D $standby_data_dir -h "local    replication     $USER                                trust"
  gs_guc set -Z datanode -D $standby2_data_dir -c "undo_zone_count = 16384"
  gs_guc set -Z datanode -D $standby3_data_dir -c "undo_zone_count = 16384"
  gs_guc set -Z datanode -D $standby4_data_dir -c "undo_zone_count = 16384"
  start_cluster

  #create logical replication slots
  pg_recvlogical -d $db -p $dn1_primary_port -S slot1 --create
  if [ $? -eq 0 ]; then
    echo "create replication slot slot1 success"
  else
    echo "$failed_keyword: create replication slot slot1 failed."
    exit 1
  fi

  sleep 1

  pg_recvlogical -d $db -p $dn1_primary_port -S slot2 --create
  if [ $? -eq 0 ]; then
    echo "create replication slot slot2 success"
  else
    echo "$failed_keyword: create replication slot slot2 failed."
    exit 1
  fi

  sleep 1

  pg_recvlogical -d $db -p $dn1_primary_port -S slot3 --create
  if [ $? -eq 0 ]; then
    echo "create replication slot3 success"
  else
    echo "$failed_keyword: create replication slot slot3 failed."
    exit 1
  fi

  sleep 1

  pg_recvlogical -d $db -p $dn1_primary_port -S slot4 --create
  if [ $? -eq 0 ]; then
    echo "create replication slot4 success"
  else
    echo "$failed_keyword: create replication slot slot4 failed."
    exit 1
  fi

  sleep 1

  #start logical decoding on standby
  echo "begin to decode"
  nohup pg_recvlogical -d $db -p $dn1_standby_port -o include-xids=false -o include-timestamp=true -o skip-empty-xacts=true -o only-local=true -o white-table-list='public.*' -o parallel-decode-num=2 -o parallel-queue-size=256 -o sender-timeout='60s' -o standby-connection=false -o decode-style='j' -S slot1 --start -s 2 -f $scripts_dir/data/test1.log &
  if [ $? -eq 0 ]; then
    echo "parallel decoding with type \'j\' start on standby success"
  else
    echo "$failed_keyword: parallel decoding with type \'j\' start on standby failed."
    exit 1
  fi

  nohup pg_recvlogical -d $db -p $dn1_standby_port -o parallel-decode-num=2 -o standby-connection=true -o decode-style='t' -o max-txn-in-memory=0 -o max-reorderbuffer-in-memory=0 -o white-table-list='public.t4_decode,*.t1_decode' -S slot2 --start -s 2 -f $scripts_dir/data/test2.log &
  if [ $? -eq 0 ]; then
    echo "parallel decoding with type \'t\' start on standby success"
  else
    echo "$failed_keyword: parallel decoding with type \'t\' start on standby failed."
    exit 1
  fi

  nohup pg_recvlogical -d $db -p $dn1_standby_port -o parallel-decode-num=2 -o standby-connection=true -o decode-style='b' -o max-txn-in-memory=1 -o max-reorderbuffer-in-memory=1 -o white-table-list='public.t2_decode,public.t3_decode' -S slot3 --start -s 2 -f $scripts_dir/data/test3.log &
  if [ $? -eq 0 ]; then
    echo "parallel decoding with type \'b\' start on standby success"
  else
    echo "$failed_keyword: parallel decoding with type \'b\' start on standby failed."
    exit 1
  fi

  nohup pg_recvlogical -d $db -p $dn1_standby_port -o standby-connection=true -o max-txn-in-memory=1 -o max-reorderbuffer-in-memory=1 -S slot4 --start -s 2 -f $scripts_dir/data/test4.log &
  if [ $? -eq 0 ]; then
    echo "logical decoding start on standby success"
  else
    echo "$failed_keyword: parallel decoding with type \'b\' start on standby failed."
    exit 1
  fi

  #run sql for parallel decoding
  gsql -d $db -p $dn1_primary_port -f $scripts_dir/data/parallel_decode_xact.sql
  gsql -d $db -p $dn1_primary_port -c "copy t1_decode from '$scripts_dir/data/parallel_decode_data';"
  gsql -d $db -p $dn1_primary_port -c "copy t2_decode from '$scripts_dir/data/parallel_decode_data';"
  gsql -d $db -p $dn1_primary_port -c "copy t3_decode from '$scripts_dir/data/parallel_decode_data';"
  gsql -d $db -p $dn1_primary_port -c "copy t4_decode from '$scripts_dir/data/parallel_decode_data';"
  gsql -d $db -p $dn1_standby_port -c "select * from gs_get_parallel_decode_status();"
  sleep 60
  #kill pg_recvlogical
  ps -ef | grep pg_recvlogical | grep -v grep | awk '{print $2}' | xargs kill -9

  #drop table
  gsql -d $db -p $dn1_primary_port -c "DROP TABLE IF EXISTS t1_decode; DROP TABLE IF EXISTS t2_decode; DROP TABLE IF EXISTS t3_decode; DROP TABLE IF EXISTS t4_decode; DROP TABLE IF EXISTS t5_decode;"

  #drop logical replication slot
  pg_recvlogical -d $db -p $dn1_primary_port -S slot1 --drop
  if [ $? -eq 0 ]; then
    echo "drop replication slot success"
  else
    echo "$failed_keyword: drop replication slot failed."
    exit 1
  fi
  pg_recvlogical -d $db -p $dn1_primary_port -S slot2 --drop
  if [ $? -eq 0 ]; then
    echo "drop replication slot success"
  else
    echo "$failed_keyword: drop replication slot failed."
    exit 1
  fi
  pg_recvlogical -d $db -p $dn1_primary_port -S slot3 --drop
  if [ $? -eq 0 ]; then
    echo "drop replication slot success"
  else
    echo "$failed_keyword: drop replication slot failed."
    exit 1
  fi

  rm $scripts_dir/data/test1.log
  rm $scripts_dir/data/test2.log
  rm $scripts_dir/data/test3.log
  rm $scripts_dir/data/test4.log
}

test_1
