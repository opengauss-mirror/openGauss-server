#!/bin/sh
source ./standby_env.sh

#test_1用例测试主备连续发生多次catchup的场景
function test_1()
{
check_instance

#set ha_module_debug
gs_guc reload -Z datanode -D $data_dir/datanode1 -c "ha_module_debug=on"

#create table
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test1; CREATE TABLE mpp_test1(id INT,name VARCHAR(15) NOT NULL) with (orientation = column);" 

#kill the standby
kill_standby

#copy 50MB data
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test1 from '$scripts_dir/data/data5';"
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test1 from '$scripts_dir/data/data5';"

#start standby - catchup occured
start_standby
#wait catchup down
sleep 5

#reset ha_module_debug
gs_guc reload -Z datanode -D $data_dir/datanode1 -c "ha_module_debug=off"

#kill the standby again
kill_standby

#copy 50MB data
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test1 from '$scripts_dir/data/data5';"
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test1 from '$scripts_dir/data/data5';"

#start standby - catchup occured
start_standby

#wait catchup down
check_replication_setup
wait_catchup_finish

#test the copy results on dn1_primary
if [ $(gsql -d $db -p $dn1_primary_port -c "select count(1) from mpp_test1;" | grep `expr 4 \* $rawdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_primary mpp_test1"
else
	echo "copy $failed_keyword on dn1_primary mpp_test1"
	exit 1
fi
#test the copy results on dn1_standby
if [ $(gsql -d $db -p $dn1_standby_port -m -c "select count(1) from mpp_test1;" | grep `expr 4 \* $rawdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_standby mpp_test1"
else
	echo "copy $failed_keyword on dn1_standby mpp_test1"
	exit 1
fi
}

#test_2用例测试主备间隔发生多次catchup的场景
function test_2()
{
check_instance

#create table
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test2; CREATE TABLE mpp_test2(id INT,name VARCHAR(15) NOT NULL) with (orientation = column);" 

#kill the standby
kill_standby

#copy 25MB data
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test2 from '$scripts_dir/data/data5';"

#start standby - catchup occured
start_standby

#copy 50MB data immediately
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test2 from '$scripts_dir/data/data5';"
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test2 from '$scripts_dir/data/data5';"

#wait catchup down
check_replication_setup
wait_catchup_finish

#kill the standby again
kill_standby

#copy 25MB data
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test2 from '$scripts_dir/data/data5';"

#start standby - catchup occured
start_standby
#wait catchup down
check_replication_setup
wait_catchup_finish

#test the copy results on dn1_primary
if [ $(gsql -d $db -p $dn1_primary_port -c "select count(1) from mpp_test2;" | grep `expr 4 \* $rawdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_primary mpp_test2"
else
	echo "copy $failed_keyword on dn1_primary mpp_test2"
	exit 1
fi
#test the copy results on dn1_standby
if [ $(gsql -d $db -p $dn1_standby_port -m -c "select count(1) from mpp_test2;" | grep `expr 4 \* $rawdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_standby mpp_test2"
else
	echo "copy $failed_keyword on dn1_standby mpp_test2"
	exit 1
fi
}

function tear_down()
{
sleep 1
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test1;"
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test2;"
}

test_1
test_2
tear_down