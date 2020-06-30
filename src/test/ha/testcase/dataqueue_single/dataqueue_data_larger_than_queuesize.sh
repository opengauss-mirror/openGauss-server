#!/bin/sh
#the shell is to test the dataqueue of data replicate
#copy 268M data while dataqueue_size=256M

source ./standby_env.sh

function test_1()
{
check_instance

#create table
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test1; CREATE TABLE mpp_test1(id INT,name VARCHAR(15) NOT NULL);"

#copy data(25M) to standby 4 times
gsql -d $db -p $dn1_primary_port -c "start transaction;
							set enable_data_replicate=on;
							copy mpp_test1 from '$scripts_dir/data/data5';
							copy mpp_test1 from '$scripts_dir/data/data5';
							copy mpp_test1 from '$scripts_dir/data/data5';
							copy mpp_test1 from '$scripts_dir/data/data5';
							commit;"

gsql -d $db -p $dn1_primary_port -c "checkpoint;"

#test the copy results on dn1_primary
if [ $(gsql -d $db -p $dn1_primary_port -c "select count(1) from mpp_test1;" | grep `expr 4 \* $rawdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_primary"
else
	echo "copy $failed_keyword on dn1_primary"
	exit 1
fi

#test the copy results on dn1_standby
if [ $(gsql -d $db -p $dn1_standby_port -m -c "select count(1) from mpp_test1;" | grep `expr 4 \* $rawdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_standby"
else
	echo "copy $failed_keyword on dn1_standby"
	exit 1
fi
}

function tear_down()
{
sleep 1
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test1;"
#let llt to collect the data 
stop_primary
start_primary

check_replication_setup
wait_catchup_finish
}

test_1
tear_down