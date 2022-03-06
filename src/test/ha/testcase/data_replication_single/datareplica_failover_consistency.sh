#!/bin/sh
#If there is no data changed after the restart checkpoint LSN, and during failover
#we received the data of that page

source ./standby_env.sh

test_data=102

function test_1()
{
check_instance

stop_standby

#create table1
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test1; CREATE TABLE mpp_test1(id INT);"
#create table2
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test2; CREATE TABLE mpp_test2(id INT);"

#generate data
gsql -d $db -p $dn1_primary_port -c "insert into mpp_test1 values(generate_series(1, 100));"
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on;insert into mpp_test2 select * from mpp_test1;"

#no data change in mpp_test2 after the checkpoint
gsql -d $db -p $dn1_primary_port -c "insert into mpp_test1 values (4);"
gsql -d $db -p $dn1_primary_port -c "insert into mpp_test2 values (4);"
gsql -d $db -p $dn1_primary_port -c "insert into mpp_test2 values (5);"
gsql -d $db -p $dn1_primary_port -c "checkpoint;"
gsql -d $db -p $dn1_primary_port -c "insert into mpp_test1 values (5);"

kill_primary
#simulate the cm management
start_primary_as_pending
notify_primary_as_standby

sleep 5

#failover standby to primary failed
gs_ctl failover -w -t 30 -D  $data_dir/datanode1

if [ $(gsql -d $db -p $dn1_primary_port -c "select count(1) from mpp_test1;" | grep $test_data |wc -l) -eq 1 ]; then
	echo "check data consistency success on mpp_test1 during failover"
else
	echo "check data consistency $failed_keyword on mpp_test1 during failover"
	exit 1
fi
if [ $(gsql -d $db -p $dn1_primary_port -c "select count(1) from mpp_test2;" | grep $test_data |wc -l) -eq 1 ]; then
	echo "check data consistency success on mpp_test2 during failover"
else
	echo "check data consistency $failed_keyword on mpp_test2 during failover"
	exit 1
fi
}

function tear_down()
{
sleep 1
start_standby
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test1;"
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test2;"
}

test_1
tear_down
