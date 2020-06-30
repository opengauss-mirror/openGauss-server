#!/bin/sh

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
	echo "all of success!"
else
	echo "copy to primary $failed_keyword!"
	exit 1
fi

#switchover very fast
gs_ctl switchover -w -t 30 -D  $data_dir/datanode1_standby
if [ $? -eq 0 ]; then
    echo "all of success!"
else
	echo "$failed_keyword"
	exit 1
fi
sleep 10
#test the copy results
b=`expr $rawdata_lines \* 1`
if [ $(gsql -d $db -p $dn1_standby_port -c "select count(1) from mpp_test1;" | grep $b |wc -l) -eq 1 ]; then
        echo "all of success!"
else
        echo "standby data does not match with primary, $failed_keyword!"
		exit 1
fi

#query
gs_ctl query -D  $data_dir/datanode1_standby
}

function tear_down()
{
gs_ctl query -D  $data_dir/datanode1
#switchover
gs_ctl switchover -w -t 30 -D  $data_dir/datanode1
if [ $? -eq 0 ]; then
    echo "all of success!"
else
	echo "$failed_keyword"
	exit 1
fi

gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test1;"
}

test_1
tear_down
