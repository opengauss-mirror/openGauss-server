#!/bin/sh
# the shell is to test failover in the clusters of primary, standby, dummy standby

source ./standby_env.sh

function test_1()
{
check_instance

#create table
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test1; CREATE TABLE mpp_test1(id INT,name VARCHAR(15) NOT NULL);"

#copy data(25M) to standby 3 times
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test1 from '$scripts_dir/data/data5';"
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test1 from '$scripts_dir/data/data5';"
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test1 from '$scripts_dir/data/data5';checkpoint;"

#kill the standby_node_name
kill_standby

#copy data(25M) to dummy standby
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test1 from '$scripts_dir/data/data5';checkpoint;"

#test the data of dummy standby
ls -lh $dummystandby_data_dir/base/dummy_standby

#kill the primary_node_name
kill_primary

#start the standby_node_name
start_standby

#failover standby to primary
gs_ctl failover -w -t $gsctl_wait_time -D  $data_dir/datanode1_standby

if [ $? -eq 0 ]; then
    echo "all of success!"
else
	echo "$failed_keyword"
	exit 1
fi

#test the copy results
if [ $(gsql -d $db -p $dn1_standby_port -c "select pgxc_pool_reload(); select count(1) from mpp_test1;" | grep `expr 4 \* $rawdata_lines` |wc -l) -eq 1 ]; then
	echo "all of success!"
else
	echo "copy $failed_keyword"
	exit 1
fi
}

function tear_down()
{
start_primary_as_standby
sleep 10
gs_ctl switchover -w -t $gsctl_wait_time -D  $data_dir/datanode1
if [ $? -eq 0 ]; then
    echo "all of success!"
else
	echo "$failed_keyword"
	exit 1
fi

check_replication_setup
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test1;"
}


test_1
tear_down
