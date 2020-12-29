#!/bin/sh
# test case: after a datareplication we update/insert/..the page which is logical especially these operations are after a checkpoint
#            hopefully, the update/insert operation would be logged with force page write

source ./standby_env.sh

function test_insert()
{
check_instance

#create table1
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test1; CREATE TABLE mpp_test1(id INT,name VARCHAR(15) NOT NULL);"

#checkpoint the cluster
gsql -d $db -p $dn1_primary_port -c "checkpoint;"

#stop standby, so the data will stream to dummy standby.
stop_standby

#copy 30 lines and insert
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test1 from '$scripts_dir/data/data_30_lines';"
gsql -d $db -p $dn1_primary_port -c "insert into mpp_test1 values (0, 'ha'); commit;"

#set the replication test so standby would neglect the received data pages

#start standby for receive and redo the missing data.
start_standby
sleep 5
#stop standby
stop_standby

#start standby as primary #####
start_standby_as_primary
}

function test_vacuum()
{
#create table1
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test1; CREATE TABLE mpp_test1(id INT,name VARCHAR(15) NOT NULL);"

#checkpoint the cluster
gsql -d $db -p $dn1_primary_port -c "checkpoint;"

#stop standby, so the data will stream to dummy standby.
stop_standby

#copy 30 lines and vacuum
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test1 from '$scripts_dir/data/data_30_lines';"
gsql -d $db -p $dn1_primary_port -c "vacuum mpp_test1;"

#start standby for receive and redo the missing data.
start_standby
sleep 5
#stop standby
stop_standby

#start standby as primary #####
start_standby_as_primary
}

function tear_down()
{
stop_standby
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test1;"

gs_ctl build -Z single_node -D $data_dir/datanode1_standby

sleep 3
}


test_insert
tear_down
test_vacuum
tear_down
