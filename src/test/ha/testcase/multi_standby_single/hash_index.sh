#!/bin/sh

# hash index xlog
# 1. parallel recovery mode
# 2. extreme rto mode

source ./util.sh

function hash_index_test()
{
    db_name=$1
    echo "begin test hash index in database $db_name"

    gsql -d $db -p $dn1_primary_port -c "create database $db_name;"

    gsql -d $db_name -p $dn1_primary_port -c "create table hash_table_1 (id int, num int, sex varchar default 'male');"
    gsql -d $db_name -p $dn1_primary_port -c "create index hash_t1_id1 on hash_table_1 using hash (id);"
    gsql -d $db_name -p $dn1_primary_port -c "insert into hash_table_1 select random()*10, random()*10, 'XXX' from generate_series(1,5000);"
    gsql -d $db_name -p $dn1_primary_port -c "delete from hash_table_1 where id = 7 and num = 1;"
    gsql -d $db_name -p $dn1_primary_port -c "insert into hash_table_1 select 7, random()*3, 'XXX' from generate_series(1,500);"
    gsql -d $db_name -p $dn1_primary_port -c "delete from hash_table_1 where id = 5;"
    gsql -d $db_name -p $dn1_primary_port -c "vacuum hash_table_1;"
    gsql -d $db_name -p $dn1_primary_port -c "insert into hash_table_1 select random()*50, random()*3, 'XXX' from generate_series(1,50000);"
    gsql -d $db_name -p $dn1_primary_port -c "delete from hash_table_1 where num = 2;"
    gsql -d $db_name -p $dn1_primary_port -c "vacuum hash_table_1;"

    gsql -d $db_name -p $dn1_primary_port -c "create table hash_table_2(id int, name varchar, sex varchar default 'male');"
    gsql -d $db_name -p $dn1_primary_port -c "insert into hash_table_2 select random()*100, 'XXX', 'XXX' from generate_series(1,50000);"
    gsql -d $db_name -p $dn1_primary_port -c "create or replace procedure hash_proc_9(sid in integer)
is
begin
set enable_indexscan = on;
set enable_bitmapscan = off;
delete from hash_table_9 where id = sid;
perform * from hash_table_9 where id = sid;
insert into hash_table_9 select sid, random() * 10, 'xxx' from generate_series(1,5000);
end;
/"
    gsql -d $db_name -p $dn1_primary_port -c "call hash_proc_9(1);"
    gsql -d $db_name -p $dn1_primary_port -c "call hash_proc_9(1);"
    gsql -d $db_name -p $dn1_primary_port -c "call hash_proc_9(1);"
    gsql -d $db_name -p $dn1_primary_port -c "call hash_proc_9(1);"

    sleep 3;

    gsql -d $db -p $dn1_primary_port -c "drop database $db_name;"
}

function test_1()
{
    set_default
    gs_guc set -Z datanode -D  $primary_data_dir -c "autovacuum = off"

    # parallel recovery
    echo "begin to kill primary"
    kill_cluster
    echo "begin to set parallel recovery param"
    gs_guc set -Z datanode -D  $primary_data_dir -c "recovery_max_workers = 2"
    gs_guc set -Z datanode -D  $primary_data_dir -c "recovery_parse_workers = 0"
    gs_guc set -Z datanode -D  $primary_data_dir -c "recovery_redo_workers = 0"
    gs_guc set -Z datanode -D  $primary_data_dir -c "hot_standby = on"
    start_cluster
    echo "start cluter success!"
    hash_index_test "hash_db_1"
    echo "begin to query primary"
    query_primary
    echo "begin to query standby"
    query_standby

    # extreme rto
    echo "begin to kill primary"
    kill_cluster
    echo "begin to set extreme rto param"
    gs_guc set -Z datanode -D  $primary_data_dir -c "recovery_max_workers = 0"
    gs_guc set -Z datanode -D  $primary_data_dir -c "recovery_parse_workers = 2"
    gs_guc set -Z datanode -D  $primary_data_dir -c "recovery_redo_workers = 1"
    gs_guc set -Z datanode -D  $primary_data_dir -c "hot_standby = off"
    start_cluster
    echo "start cluter success!"
    hash_index_test "hash_db_2"
    echo "begin to query primary"
    query_primary
    echo "begin to query standby"
    query_standby
}

function tear_down()
{   
    sleep 1
    set_default
    kill_cluster
    gs_guc set -Z datanode -D  $primary_data_dir -c "recovery_max_workers = 4"
    gs_guc set -Z datanode -D  $primary_data_dir -c "recovery_parse_workers  = 1"
    gs_guc set -Z datanode -D  $primary_data_dir -c "recovery_redo_workers = 1"
    gs_guc set -Z datanode -D  $primary_data_dir -c "hot_standby = on"
    start_cluster
}

test_1
tear_down