#!/bin/sh
source ./standby_env.sh

#test_1用例测试主备在小发送buffer场景下的catchup与导入的并发
#主要测试catchup对正在导入数据所修改的BCM的处理。
function test_1()
{
check_instance

#create table
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test1; CREATE TABLE mpp_test1(id INT,name VARCHAR(15) NOT NULL) with (orientation = column);" 
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test2; CREATE TABLE mpp_test2(id INT,name VARCHAR(15) NOT NULL) with (orientation = column);" 
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test3; CREATE TABLE mpp_test3(id INT,name VARCHAR(15) NOT NULL) with (orientation = column);" 
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test4; CREATE TABLE mpp_test4(id INT,name VARCHAR(15) NOT NULL) with (orientation = column);" 

#kill the standby
kill_standby

#copy data
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test1 from '$scripts_dir/data/data5';" 1>"./results/datareplica_cstore_slow_catchup.result" 2>&1 &
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test1 from '$scripts_dir/data/data5';" 1>"./results/datareplica_cstore_slow_catchup.result" 2>&1 &
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test2 from '$scripts_dir/data/data5';" 1>"./results/datareplica_cstore_slow_catchup.result" 2>&1 &
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test2 from '$scripts_dir/data/data5';" 1>"./results/datareplica_cstore_slow_catchup.result" 2>&1 &
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test3 from '$scripts_dir/data/data5';" 1>"./results/datareplica_cstore_slow_catchup.result" 2>&1 &
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test3 from '$scripts_dir/data/data5';" 1>"./results/datareplica_cstore_slow_catchup.result" 2>&1 &
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test4 from '$scripts_dir/data/data5';" 1>"./results/datareplica_cstore_slow_catchup.result" 2>&1 &
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test4 from '$scripts_dir/data/data5';" 1>"./results/datareplica_cstore_slow_catchup.result" 2>&1 &
sleep 5
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test1 from '$scripts_dir/data/data5';"
sleep 20

if [ $(gsql -d $db -p $dn1_primary_port -c "show enable_mix_replication;" | grep "off" |wc -l) -eq 1 ]; then
	stop_primary
	#take the data replication slow.
	gs_guc set -Z datanode -D $data_dir/datanode1 -c "walsender_max_send_size=1MB"
	gs_guc set -Z datanode -D $data_dir/datanode1 -c "data_replicate_buffer_size=4MB"
	gs_guc set -Z datanode -D $data_dir/datanode1_standby -c "data_replicate_buffer_size=4MB"
	start_primary
fi

#we expect that when we do copy, the catchup is online
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test4 from '$scripts_dir/data/data5';" 1>"./results/datareplica_cstore_slow_catchup.result" 2>&1 &
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test3 from '$scripts_dir/data/data5';" 1>"./results/datareplica_cstore_slow_catchup.result" 2>&1 &
#start standby - catchup occured
start_standby
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test2 from '$scripts_dir/data/data5';" 1>"./results/datareplica_cstore_slow_catchup.result" 2>&1 &
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test1 from '$scripts_dir/data/data5';"
sleep 3

check_replication_setup
wait_catchup_finish

#test the copy results on dn1_primary
if [ $(gsql -d $db -p $dn1_primary_port -c "select count(1) from mpp_test1;" | grep `expr 4 \* $rawdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_primary mpp_test1"
else
	echo "copy $failed_keyword on dn1_primary mpp_test1"
	exit 1
fi
if [ $(gsql -d $db -p $dn1_primary_port -c "select count(1) from mpp_test2" | grep `expr 3 \* $rawdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_primary mpp_test2"
else
	echo "copy $failed_keyword on dn1_primary mpp_test2"
	exit 1
fi
if [ $(gsql -d $db -p $dn1_primary_port -c "select count(1) from mpp_test3;" | grep `expr 3 \* $rawdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_primary mpp_test3"
else
	echo "copy $failed_keyword on dn1_primary mpp_test3"
	exit 1
fi
if [ $(gsql -d $db -p $dn1_primary_port -c "select count(1) from mpp_test4;" | grep `expr 3 \* $rawdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_primary mpp_test4"
else
	echo "copy $failed_keyword on dn1_primary mpp_test4"
	exit 1
fi

#test the copy results on dn1_standby
if [ $(gsql -d $db -p $dn1_standby_port -m -c "select count(1) from mpp_test1;" | grep `expr 4 \* $rawdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_standby mpp_test1"
else
	echo "copy $failed_keyword on dn1_standby mpp_test1"
	exit 1
fi
if [ $(gsql -d $db -p $dn1_standby_port -m -c "select count(1) from mpp_test2;" | grep `expr 3 \* $rawdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_standby mpp_test2"
else
	echo "copy $failed_keyword on dn1_standby mpp_test2"
	exit 1
fi
if [ $(gsql -d $db -p $dn1_standby_port -m -c "select count(1) from mpp_test3;" | grep `expr 3 \* $rawdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_standby mpp_test3"
else
	echo "copy $failed_keyword on dn1_standby mpp_test3"
	exit 1
fi
if [ $(gsql -d $db -p $dn1_standby_port -m -c "select count(1) from mpp_test4;" | grep `expr 3 \* $rawdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_standby mpp_test4"
else
	echo "copy $failed_keyword on dn1_standby mpp_test4"
	exit 1
fi
}

function tear_down()
{
sleep 1
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test1;"
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test2;"
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test3;"
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test4;"

if [ $(gsql -d $db -p $dn1_primary_port -c "show enable_mix_replication;" | grep "off" |wc -l) -eq 1 ]; then
	stop_primary
	stop_standby
	gs_guc set -Z datanode -D $data_dir/datanode1 -c "walsender_max_send_size=8MB"
	gs_guc set -Z datanode -D $data_dir/datanode1 -c "data_replicate_buffer_size=16MB"
	gs_guc set -Z datanode -D $data_dir/datanode1_standby -c "data_replicate_buffer_size=16MB"
	start_standby
	start_primary
fi
}

test_1
tear_down
