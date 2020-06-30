#!/bin/sh
# When standby connect primary, primary will startup a catchup thread to scan bcm files and put heap pages with bcm bit 1 to dataqueue and send to standby;
# but dummystandby not needed, when dummy standby connect primary, primary do not need to start catch thread.

source ./standby_env.sh

function test_1()
{
check_instance

#create table
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test1; CREATE TABLE mpp_test1(id INT,name VARCHAR(15) NOT NULL);"

kill_standby

#copy data(25M) to dummy standby
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test1 from '$scripts_dir/data/data5';"

kill_dummystandby

sleep 1

#copy will abort, and queue has data which not send to dummystandby, now master occupy 25M storage space
kill_primary

#start primary, dummy stanby will connect to it, but no need to start catchup thread
start_primary
start_dummystandby

sleep 5

#gsql -d $db -p $dn1_primary_port -c "select txid_current();select txid_current();"

gsql -d $db -p $dn1_primary_port -c "select pgxc_pool_reload(); set enable_data_replicate=on; copy mpp_test1 from '$scripts_dir/data/data5';"

# master occupy 25M * 2 storage space, but the first 25M is aborted 
kill_primary

start_standby

sleep 5
# standby will extend the first 25M pages and fetch last 25M pages from dummy standby
gs_ctl failover -w -t $gsctl_wait_time -D  $data_dir/datanode1_standby

# reset primary and standby
start_primary_as_standby

sleep 10

query_primary
query_standby

sleep 5
gs_ctl switchover -w -t $gsctl_wait_time -D  $data_dir/datanode1

#test the copy results on dn1_primary
if [ $(gsql -d $db -p $dn1_primary_port -c "select count(1) from mpp_test1;" | grep `expr 2 \* $rawdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_primary mpp_test1"
else
	echo "copy $failed_keyword on dn1_primary mpp_test1"
	exit 1
fi

#test the copy results on dn1_standby
if [ $(gsql -d $db -p $dn1_standby_port -m -c "select count(1) from mpp_test1;" | grep `expr 2 \* $rawdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_standby mpp_test1"
else
	echo "copy $failed_keyword on dn1_standby mpp_test1"
	exit 1
fi
}

function tear_down()
{
sleep 1
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test1;"
}

test_1
tear_down
