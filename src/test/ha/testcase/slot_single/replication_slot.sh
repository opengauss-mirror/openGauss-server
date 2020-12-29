#!/bin/sh
# keep wal segment files when standby is offline using replication slot on primary

source ./standby_env.sh

function test_1()
{
check_instance

#create table slot1
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_slot1; CREATE TABLE mpp_slot1(id INT,name VARCHAR(15) NOT NULL);"

#set wal_keep_segments to 2
gs_guc reload -Z datanode -D $data_dir/datanode1 -c "wal_keep_segments=2"

#kill standby, so data will replicate to dummystandby
kill_standby

#produce enough wal segment files
gsql -d $db -p $dn1_primary_port -c "INSERT INTO mpp_slot1 VALUES(311, 'sp');"
gsql -d $db -p $dn1_primary_port -c "checkpoint;"
gsql -d $db -p $dn1_primary_port -c "select pg_switch_xlog();"

gsql -d $db -p $dn1_primary_port -c "INSERT INTO mpp_slot1 VALUES(311, 'sp');"
gsql -d $db -p $dn1_primary_port -c "checkpoint;"
gsql -d $db -p $dn1_primary_port -c "select pg_switch_xlog();"

gsql -d $db -p $dn1_primary_port -c "INSERT INTO mpp_slot1 VALUES(311, 'sp');"
gsql -d $db -p $dn1_primary_port -c "checkpoint;"
gsql -d $db -p $dn1_primary_port -c "select pg_switch_xlog();"

gsql -d $db -p $dn1_primary_port -c "INSERT INTO mpp_slot1 VALUES(311, 'sp');"
gsql -d $db -p $dn1_primary_port -c "checkpoint;"
gsql -d $db -p $dn1_primary_port -c "select pg_switch_xlog();"

gsql -d $db -p $dn1_primary_port -c "INSERT INTO mpp_slot1 VALUES(311, 'sp');"
gsql -d $db -p $dn1_primary_port -c "checkpoint;"
gsql -d $db -p $dn1_primary_port -c "select pg_switch_xlog();"

gsql -d $db -p $dn1_primary_port -c "INSERT INTO mpp_slot1 VALUES(311, 'sp');"
gsql -d $db -p $dn1_primary_port -c "checkpoint;"
gsql -d $db -p $dn1_primary_port -c "select pg_switch_xlog();"

gsql -d $db -p $dn1_primary_port -c "INSERT INTO mpp_slot1 VALUES(311, 'sp');"
gsql -d $db -p $dn1_primary_port -c "checkpoint;"
gsql -d $db -p $dn1_primary_port -c "select pg_switch_xlog();"

start_standby

check_replication_setup
if [ $? -eq 0 ]; then
	echo "all of success"
else
	echo "$failed_keyword: replication slot failed."
	exit 1
fi
}

function test_2()
{
check_instance

#create table slot2
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_slot2; CREATE TABLE mpp_slot2(id INT,name VARCHAR(15) NOT NULL);"

#set wal_keep_segments to 2
gs_guc reload -Z datanode -D $data_dir/datanode1 -c "wal_keep_segments=2"

#kill standby, so data will replicate to dummystandby
kill_standby

#drop standby replication slot on primary
gsql -d $db -p $dn1_primary_port -c "SELECT * FROM pg_drop_replication_slot('datanode1');"

#produce enough wal segment files
gsql -d $db -p $dn1_primary_port -c "INSERT INTO mpp_slot2 VALUES(311, 'sp');"
gsql -d $db -p $dn1_primary_port -c "checkpoint;"
gsql -d $db -p $dn1_primary_port -c "select pg_switch_xlog();"

gsql -d $db -p $dn1_primary_port -c "INSERT INTO mpp_slot2 VALUES(311, 'sp');"
gsql -d $db -p $dn1_primary_port -c "checkpoint;"
gsql -d $db -p $dn1_primary_port -c "select pg_switch_xlog();"

gsql -d $db -p $dn1_primary_port -c "INSERT INTO mpp_slot2 VALUES(311, 'sp');"
gsql -d $db -p $dn1_primary_port -c "checkpoint;"
gsql -d $db -p $dn1_primary_port -c "select pg_switch_xlog();"

gsql -d $db -p $dn1_primary_port -c "INSERT INTO mpp_slot2 VALUES(311, 'sp');"
gsql -d $db -p $dn1_primary_port -c "checkpoint;"
gsql -d $db -p $dn1_primary_port -c "select pg_switch_xlog();"

gsql -d $db -p $dn1_primary_port -c "INSERT INTO mpp_slot2 VALUES(311, 'sp');"
gsql -d $db -p $dn1_primary_port -c "checkpoint;"
gsql -d $db -p $dn1_primary_port -c "select pg_switch_xlog();"

gsql -d $db -p $dn1_primary_port -c "INSERT INTO mpp_slot2 VALUES(311, 'sp');"
gsql -d $db -p $dn1_primary_port -c "checkpoint;"
gsql -d $db -p $dn1_primary_port -c "select pg_switch_xlog();"

gsql -d $db -p $dn1_primary_port -c "INSERT INTO mpp_slot2 VALUES(311, 'sp');"
gsql -d $db -p $dn1_primary_port -c "checkpoint;"
gsql -d $db -p $dn1_primary_port -c "select pg_switch_xlog();"

start_standby

check_walkeepsegment
if [ $? -eq 0 ]; then
	#build the standby
	gs_ctl build -D $data_dir/datanode1_standby -Z single_node -b full
	echo "all of success"
else
	echo "$failed_keyword: no replication slot failed."
	exit 1
fi

check_standby_startup
}

function tear_down()
{
#set wal_keep_segments back to 16
gs_guc reload -Z datanode -D $data_dir/datanode1 -c "wal_keep_segments=16"
sleep 1
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_slot1;"
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_slot2;"
}

test_1
#now if the standby is offline, we hold all the segments, so the test_2 is now unnecessary.
#test_2
tear_down