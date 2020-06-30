#!/bin/sh
# switchover when primary-standby-dummystandby all ready

source ./standby_env.sh

function test_1()
{
check_instance

#create table
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test1; CREATE TABLE mpp_test1(id INT,name VARCHAR(15) NOT NULL);"

#copy data(25M) to standby 1 times
gsql -d $db -p $dn1_primary_port -c "start transaction; set enable_data_replicate=on; copy mpp_test1 from '$scripts_dir/data/data5'; checkpoint; rollback;"

kill_primary
sleep 3
start_primary_as_pending
sleep 3
notify_primary_as_primary
sleep 3

#test the copy results
b=`expr $rawdata_lines \* 0`
if [ $(gsql -d $db -p $dn1_primary_port -c "select count(1) from mpp_test1;" | grep $b |wc -l) -eq 1 ]; then
	echo "all of success"
else
	echo "copy to primary $failed_keyword 1"
fi
sleep 1

#copy data(25M) to standby 1 times
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test1 from '$scripts_dir/data/data5';checkpoint;"

#test the copy results
b=`expr $rawdata_lines \* 1`
if [ $(gsql -d $db -p $dn1_primary_port -c "select count(1) from mpp_test1;" | grep $b |wc -l) -eq 1 ]; then
		gsql -d $db -p $dn1_primary_port -c "select count(1) from mpp_test1;" 
        echo "all of success"
else
        echo "copy to primary $failed_keyword,after start dummystandby"
fi
#test the copy results
b=`expr $rawdata_lines \* 1`
if [ $(gsql -d $db -p $dn1_standby_port  -m -c "select count(1) from mpp_test1;" | grep $b |wc -l) -eq 1 ]; then
        echo "all of success when query standby"
else
        echo "copy to standby $failed_keyword,after start dummystandby"
		gsql -d $db -p $dn1_standby_port  -m -c "select count(1) from mpp_test1;"
		exit 1
fi

sleep 1

#query
gs_ctl query -D  $data_dir/datanode1
gs_ctl query -D  $data_dir/datanode1_standby
}

function tear_down()
{
sleep 1
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test1;"
}

test_1
tear_down
