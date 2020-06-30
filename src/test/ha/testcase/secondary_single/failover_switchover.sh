#!/bin/sh
# the shell is to test the dataqueue of data replicate

source ./standby_env.sh

function test_1()
{
check_instance

#create table
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test1; CREATE TABLE mpp_test1(id INT,name VARCHAR(15) NOT NULL);"

#copy data(25M) to standby 1 times
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test1 from '$scripts_dir/data/data5';"

#test the copy results
b=`expr $rawdata_lines \* 1`
if [ $(gsql -d $db -p $dn1_primary_port -c "select count(1) from mpp_test1;" | grep $b |wc -l) -eq 1 ]; then
	echo "all of success"
else
	echo "copy $failed_keyword 1"
	exit 1
fi

#kill the standby_node_name
kill_standby

#copy data(25M) to dummy standby
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test1 from '$scripts_dir/data/data5';"

#test the copy results
b=`expr $rawdata_lines \* 2`
if [ $(gsql -d $db -p $dn1_primary_port -c "select count(1) from mpp_test1;" | grep $b |wc -l) -eq 1 ]; then
	echo "all of success"
else
	echo "copy $failed_keyword 2"
	exit 1
fi

#kill the primary_node_name
kill_primary

#start the standby_node_name
start_standby

sleep 5
#failover standby to primary
gs_ctl failover -w -t $gsctl_wait_time -D  $data_dir/datanode1_standby
if [ $? -eq 0 ]; then
    echo "all of success!"
else
	echo "$failed_keyword"
	exit 1
fi

#test the copy results
b=`expr $rawdata_lines \* 2`
if [ $(gsql -d $db -p $dn1_standby_port -c "select count(1) from mpp_test1;" | grep $b |wc -l) -eq 1 ]; then
	echo "all of success!"
else
	echo "copy $failed_keyword 2 when failover"
	exit 1
fi

#copy data(25M) to standby 1 times
gsql -d $db -p $dn1_standby_port -c "set enable_data_replicate=on; copy mpp_test1 from '$scripts_dir/data/data5';"

#test the copy results
b=`expr $rawdata_lines \* 3`
if [ $(gsql -d $db -p $dn1_standby_port -c "select count(1) from mpp_test1;" | grep $b |wc -l) -eq 1 ]; then
	echo "all of success"
else
	echo "copy $failed_keyword 3"
	exit 1
fi

#start the standby_node_name
start_primary_as_standby
sleep 5

#query
query_primary
query_standby

sleep 5
#switchover
gs_ctl switchover -w -t $gsctl_wait_time -D  $data_dir/datanode1
if [ $? -eq 0 ]; then
    echo "all of success!"
else
	echo "$failed_keyword"
	exit 1
fi
sleep 5

#query
echo ----after switchover----
echo datanode1
gs_ctl query -D  $data_dir/datanode1
echo datanode1_standby
gs_ctl query -D  $data_dir/datanode1_standby
}

function tear_down()
{
sleep 1
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test1;"
}

test_1
tear_down
