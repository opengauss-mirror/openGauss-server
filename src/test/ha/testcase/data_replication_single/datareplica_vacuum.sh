#!/bin/sh

source ./standby_env.sh

function test_1()
{
check_instance

#create table
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test1; CREATE TABLE mpp_test1(id INT,name VARCHAR(15) NOT NULL);"

#copy data(25M) to dummy standby
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test1 from '$scripts_dir/data/data5';"
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; delete from mpp_test1;"
gsql -d $db -p $dn1_primary_port -c "vacuum mpp_test1;"
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test1 from '$scripts_dir/data/data5';"

# master occupy 25M * 2 storage space, but the first 25M is aborted 
kill_primary
kill_standby

start_primary
start_standby

check_replication_setup
wait_catchup_finish

#gsql -d $db -p $dn1_primary_port -c "select txid_current();select txid_current();"

#test the copy results on dn1_primary
if [ $(gsql -d $db -p $dn1_primary_port -c "select pgxc_pool_reload();select count(1) from mpp_test1;" | grep `expr 1 \* $rawdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_primary mpp_test1"
else
	echo "copy $failed_keyword on dn1_primary mpp_test1"
	exit 1
fi

#test the copy results on dn1_standby
if [ $(gsql -d $db -p $dn1_standby_port -m -c "select count(1) from mpp_test1;" | grep `expr 1 \* $rawdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_standby mpp_test1"
else
	echo "copy $failed_keyword on dn1_standby mpp_test1"
	exit 1
fi
}

function tear_down()
{
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test1;"
}

test_1
tear_down