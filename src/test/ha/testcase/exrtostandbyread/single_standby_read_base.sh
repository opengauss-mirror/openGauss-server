#!/bin/sh

source ./util.sh

function check_select_result()
{
  if [ $(echo $result | grep "${1}" | wc -l) -eq 1 ]; then
    echo "remote read successful"
  else
    echo "remote read failed $failed_keyword with [$result]"
    exit 1
  fi
}

function start_standby_read_cluster()
{
  start_primary_as_primary
  start_standby
}

function stop_standby_read_cluster()
{
  stop_primary
  stop_standby
}

function restart_standby_read_cluster()
{
  stop_standby_read_cluster
  start_standby_read_cluster
}

function test_base_sql_func()
{
  gsql -d test_standby_read_base -p $dn1_primary_port -c "DROP TABLE if exists test1; CREATE TABLE test1(contentId VARCHAR(128) NOT NULL, commentId VARCHAR(128) NOT NULL, appId VARCHAR(128) NOT NULL, PRIMARY KEY (contentId, commentId)) with(parallel_workers=8,storage_type=aSTORE);"
  gsql -d test_standby_read_base -p $dn1_primary_port -c "DROP TABLE if exists test2; CREATE TABLE test2(contentId VARCHAR(128) NOT NULL, commentId VARCHAR(128) NOT NULL, appId VARCHAR(128) NOT NULL, PRIMARY KEY (contentId, commentId)) with(storage_type=aSTORE,fillfactor=80) partition by hash(contentId);"
  gsql -d test_standby_read_base -p $dn1_primary_port -c "DROP TABLE if exists test3; CREATE TABLE test3(contentId VARCHAR(128) NOT NULL, commentId VARCHAR(128) NOT NULL, appId VARCHAR(128) NOT NULL, PRIMARY KEY (contentId, commentId)) with(storage_type=aSTORE,fillfactor=40) partition by list(contentId)  (partition p1 values ('1') ,partition p2 values ('2') ,partition p3 values ('3') ,partition p4 values (default));"
  
  gsql -d test_standby_read_base -p $dn1_primary_port -c "insert into test1 select generate_series(1,20), generate_series(1,20), generate_series(1,20);"
  gsql -d test_standby_read_base -p $dn1_primary_port -c "insert into test2 select generate_series(1,300), generate_series(1,300), generate_series(1,300);"
  gsql -d test_standby_read_base -p $dn1_primary_port -c "insert into test3 select generate_series(1,20), generate_series(1,20), generate_series(1,20);"

  gsql -d test_standby_read_base -p $dn1_primary_port -c "delete test1 where contentId = 8;"
  gsql -d test_standby_read_base -p $dn1_primary_port -c "delete test2 where contentId = 8;"
  gsql -d test_standby_read_base -p $dn1_primary_port -c "delete test3 where contentId = 8;"

  gsql -d test_standby_read_base -p $dn1_primary_port -c "update test1 set appId = 2 where contentId = 1;"
  gsql -d test_standby_read_base -p $dn1_primary_port -c "update test2 set appId = 2 where contentId = 1;"
  gsql -d test_standby_read_base -p $dn1_primary_port -c "update test2 set appId = 2 where contentId = 1;"

  sleep 5
  
  echo "execute light-proxy plan"
  result=`gsql -d test_standby_read_base -p $dn1_standby_port -c "select * from test1 where appId = 10;"`
  check_select_result "10 | 10 | 10 (1 row)"

  echo "execute remote-query plan"
  result=`gsql -d test_standby_read_base -p $dn1_standby_port -c "select count(*) from test1, test2 where test1.appId = test2.appId;"`
  check_select_result "21 (1 row)"

  echo "execute pbe fqs plan"
  result=`gsql -d test_standby_read_base -p $dn1_standby_port -c 'prepare a as select * from test3 where appId = $1;execute a (10);'`
  check_select_result "10 | 10 | 10 (1 row)"


  echo "execute pbe remote-query plan"
  result=`gsql -d test_standby_read_base -p $dn1_standby_port -c 'prepare b as select count(*) from test3, test1 where test3.appId = $1;execute b (10);'`
  check_select_result "19 (1 row)"

  echo "execute cursor"
  result=`gsql -d test_standby_read_base -p $dn1_standby_port -c "START TRANSACTION;CURSOR cursor1 FOR SELECT * FROM test1 order by 1;FETCH 1 IN cursor1;CLOSE cursor1;END;"`
  check_select_result "1 | 1 | 2 (1 row) CLOSE CURSOR COMMIT"

  echo "execute transaction"
  gsql -d test_standby_read_base -p $dn1_primary_port -c 'begin; update test2 set test2.appId = test2.contentId + 10 where test2.contentId = 10; select pg_sleep(2); commit;' &
  result=`gsql -d test_standby_read_base -p $dn1_standby_port -c "select appId from test2 where test2.contentId = 10;"`
  check_select_result "10 (1 row)"
  sleep 5
  result=`gsql -d test_standby_read_base -p $dn1_standby_port -c "select appId from test2 where test2.contentId = 10;"`
  check_select_result "20 (1 row)"
  gsql -d test_standby_read_base -p $dn1_primary_port -c 'update test2 set test2.appId = 10 where test2.contentId = 10; select pg_sleep(3);'
}

function test_standby_read_base_func()
{
  set_default
  kill_cluster

  echo "set guc"
  gs_guc set -Z datanode -D $primary_data_dir -c "recovery_parse_workers = 1"
  gs_guc set -Z datanode -D $primary_data_dir -c "recovery_redo_workers = 1"
  gs_guc set -Z datanode -D $primary_data_dir -c "hot_standby = on"

  gs_guc set -Z datanode -D $standby_data_dir -c "recovery_parse_workers = 1"
  gs_guc set -Z datanode -D $standby_data_dir -c "recovery_redo_workers = 1"
  gs_guc set -Z datanode -D $standby_data_dir -c "hot_standby = on"

  start_standby_read_cluster
  echo "start cluster success"
  sleep 2

  echo "prepare data"
  gsql -d $db -p $dn1_primary_port -c "create database test_standby_read_base with encoding='UTF8' template=template0;"

  echo "test serial redo"
 
  test_base_sql_func

  sleep 2

  echo "test paraller redo"
  
  gs_guc set -Z datanode -D $primary_data_dir -c "recovery_parse_workers = 1"
  gs_guc set -Z datanode -D $standby_data_dir -c "recovery_parse_workers = 1"
  
  gs_guc set -Z datanode -D $primary_data_dir -c " recovery_max_workers = 4"
  gs_guc set -Z datanode -D $standby_data_dir -c " recovery_max_workers = 4"

  restart_standby_read_cluster

  test_base_sql_func
  
  sleep 2

  echo "test exrto redo"
  
  gs_guc set -Z datanode -D $primary_data_dir -c "recovery_parse_workers = 2"
  gs_guc set -Z datanode -D $standby_data_dir -c "recovery_parse_workers = 2"
  
  gs_guc set -Z datanode -D $primary_data_dir -c " recovery_redo_workers = 4"
  gs_guc set -Z datanode -D $standby_data_dir -c " recovery_redo_workers = 4"

  restart_standby_read_cluster

  test_base_sql_func
}

function tear_down() {
  sleep 1
  gsql -d $db -p $cn1_port -c "DROP DATABASE if exists test_standby_read_base;"

  stop_streaming_cluster

  echo "reset guc"

  gs_guc set -Z datanode -D $primary_data_dir -c "recovery_parse_workers=1"
  gs_guc set -Z datanode -D $standby_data_dir -c "recovery_parse_workers=1"
  
  gs_guc set -Z datanode -D $primary_data_dir -c " recovery_redo_workers = 1"
  gs_guc set -Z datanode -D $standby_data_dir -c " recovery_redo_workers = 1"
  
  gs_guc set -Z datanode -D $primary_data_dir -c " recovery_max_workers = 1"
  gs_guc set -Z datanode -D $standby_data_dir -c " recovery_max_workers = 1"

  gs_guc set -Z datanode -D $primary_data_dir -c "hot_standby = on"
  gs_guc set -Z datanode -D $standby_data_dir -c "hot_standby = on"
}

test_standby_read_base_func
tear_down