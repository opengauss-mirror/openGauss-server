#!/bin/sh
source ./standby_env.sh


function produce_enough_wal()
{
#create table wal1
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_wal1; CREATE TABLE mpp_wal1(id INT,name VARCHAR(15) NOT NULL);"

#produce enough wal segment files
gsql -d $db -p $dn1_primary_port -c "INSERT INTO mpp_wal1 VALUES(311, 'sp');"
gsql -d $db -p $dn1_primary_port -c "select pg_switch_xlog();"

gsql -d $db -p $dn1_primary_port -c "INSERT INTO mpp_wal1 VALUES(311, 'sp');"
gsql -d $db -p $dn1_primary_port -c "select pg_switch_xlog();"

gsql -d $db -p $dn1_primary_port -c "INSERT INTO mpp_wal1 VALUES(311, 'sp');"
gsql -d $db -p $dn1_primary_port -c "select pg_switch_xlog();"

gsql -d $db -p $dn1_primary_port -c "INSERT INTO mpp_wal1 VALUES(311, 'sp');"
gsql -d $db -p $dn1_primary_port -c "select pg_switch_xlog();"

gsql -d $db -p $dn1_primary_port -c "INSERT INTO mpp_wal1 VALUES(311, 'sp');"
gsql -d $db -p $dn1_primary_port -c "select pg_switch_xlog();"

gsql -d $db -p $dn1_primary_port -c "INSERT INTO mpp_wal1 VALUES(311, 'sp');"
gsql -d $db -p $dn1_primary_port -c "select pg_switch_xlog();"

gsql -d $db -p $dn1_primary_port -c "INSERT INTO mpp_wal1 VALUES(311, 'sp');"
gsql -d $db -p $dn1_primary_port -c "select pg_switch_xlog();"
}

function test_1()
{
check_instance

#set wal_keep_segments to 2
gs_guc reload -Z datanode -D $data_dir/datanode1 -c "wal_keep_segments=2"
sleep 2

#stop all
stop_primary
stop_standby
stop_dummystandby
sleep 2

start_primary
start_dummystandby
sleep 2

produce_enough_wal
sleep 1

stop_dummystandby
start_standby
sleep 2

produce_enough_wal
#this checkpoint flush slot(standby && dummy) to disk
gsql -d $db -p $dn1_primary_port -c "checkpoint;"
start_dummystandby
sleep 1

produce_enough_wal
#this checkpoint only flush slot(standby) to disk
gsql -d $db -p $dn1_primary_port -c "checkpoint;"
stop_primary
stop_standby

start_primary
check_dummy_setup
if [ $? -eq 0 ]; then
	echo "dummy slot choose startpoint success."
else
	echo "$failed_keyword: failed to choose dummy slot startpoint!"
	exit 1
fi
}

function tear_down()
{
start_standby

#set wal_keep_segments back to 16
gs_guc reload -Z datanode -D $data_dir/datanode1 -c "wal_keep_segments=16"
#drop table
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_wal1; "
sleep 1
}

test_1
tear_down