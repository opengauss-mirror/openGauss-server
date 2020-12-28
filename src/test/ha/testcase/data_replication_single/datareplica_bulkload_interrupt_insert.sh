#!/bin/sh
#relation create and data input in the same transaction, interrupt during copy data, rollback the transaction
#standby will request the data page which has been deleted in the primary, which was not correct, but, we just
#want to test this scenario.
source ./standby_env.sh

function test_bulkload()
{
check_instance

stop_primary
gs_guc set -Z datanode -D $data_dir/datanode1 -c "shared_buffers=512MB"
start_primary

#prepare data
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_bulkload1; CREATE TABLE mpp_bulkload1(id INT,name VARCHAR(15) NOT NULL);"
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_bulkload2; CREATE TABLE mpp_bulkload2(id INT,name VARCHAR(15) NOT NULL);"

#copy
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on;copy mpp_bulkload1 from '$scripts_dir/data/data5';checkpoint;"

sleep 2

#bulkload data
gsql -d $db -p $dn1_primary_port -c "
                            start transaction;
                            set enable_data_replicate=on;
                            insert into mpp_bulkload2 select * from mpp_bulkload1;
                            insert into mpp_bulkload2 values (1, 'a');
                            commit;"
kill_primary

#maybe take a long time to replay logical record.
start_primary
sleep 2

check_replication_setup
if [ $? -eq 0 ]; then
	echo "all of success"
else
	echo "$failed_keyword: bulkload interrupt insert failed."
	exit 1
fi
}

function tear_down()
{
stop_primary
gs_guc set -Z datanode -D $data_dir/datanode1 -c "shared_buffers=2GB"
start_primary
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_bulkload1;"
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_bulkload2;"
}

test_bulkload
tear_down
