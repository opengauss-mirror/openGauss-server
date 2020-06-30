#!/bin/sh
# switchover when primary-standby-dummystandby all ready

source ./standby_env.sh

function test_1()
{
check_instance

#create table
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test1; CREATE TABLE mpp_test1(id INT,name VARCHAR(15) NOT NULL);"

#kill standby
kill_standby

#copy data(25M) to standby 1 times
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test1 from '$scripts_dir/data/data5';checkpoint;" &

sleep 1

#kill dummystandby when copy
kill_dummystandby

sleep 3

#start dummystandby
start_dummystandby

#wait copy end
wait %1

#test the copy results
b=`expr $rawdata_lines \* 1`
if [ $(gsql -d $db -p $dn1_primary_port -c "select count(1) from mpp_test1;" | grep $b |wc -l) -eq 1 ]; then
		gsql -d $db -p $dn1_primary_port -c "select count(1) from mpp_test1;" 
        echo "all of success 2"
else
        echo "copy to primary $failed_keyword 2,after start dummystandby"
		exit 1
fi

#copy data(25M) to standby 1 times
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test1 from '$scripts_dir/data/data5';checkpoint;"

#test the copy results
b=`expr $rawdata_lines \* 2`
if [ $(gsql -d $db -p $dn1_primary_port -c "select count(1) from mpp_test1;" | grep $b |wc -l) -eq 1 ]; then
		gsql -d $db -p $dn1_primary_port -c "select count(1) from mpp_test1;" 
        echo "all of success 2"
else
        echo "copy to primary $failed_keyword 2,after start dummystandby"
		exit 1
fi

sleep 1

#query
gs_ctl query -D  $data_dir/datanode1
}

function tear_down()
{
start_standby
sleep 1
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test1;"
}

test_1
tear_down
