#!/bin/sh
# switchover when primary-standby-dummystandby all ready

source ./standby_env.sh

function test_1()
{
check_instance

#create table
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on;DROP TABLE if exists mpp_test1; CREATE TABLE mpp_test1(id INT,name VARCHAR(15) NOT NULL);"

#copy data(25M) to standby 1 times
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test1 from '$scripts_dir/data/data_30_lines';checkpoint;"
#create index, data replicate
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; CREATE INDEX id_idx ON mpp_test1(id);checkpoint;"


#test the copy results
echo %2=0:
gsql -d $db -p $dn1_primary_port -c "set enable_seqscan=off;select sum(id) from mpp_test1 where id % 2 = 0;" 
echo %2=1:
gsql -d $db -p $dn1_primary_port -c "set enable_seqscan=off;select sum(id) from mpp_test1 where id % 2 = 1;"
if [ $(gsql -d $db -p $dn1_primary_port -c "set enable_seqscan=off;select sum(id) from mpp_test1 where id % 2 = 0;" | grep 13414806 | wc -l) -eq 1 ]; then
	echo "all of success"
else
	echo "copy to primary %0 $failed_keyword 1/index before vacuum"
fi
#test the copy results
if [ $(gsql -d $db -p $dn1_primary_port -c "set enable_seqscan=off;select sum(id) from mpp_test1 where id % 2 = 1;" | grep 13414841 | wc -l) -eq 1 ]; then
	echo "all of success"
else
	echo "copy to primary %1 $failed_keyword 1/index before vacuum"
	exit
fi

#test the copy results
if [ $(gsql -d $db -p $dn1_standby_port -m -c "set enable_seqscan=off;select sum(id) from mpp_test1 where id % 2 = 0;" | grep 13414806 |wc -l) -eq 1 ]; then
	echo "all of success"
else
	echo "copy to standby $failed_keyword 1/index before vacuum"
	exit
fi
if [ $(gsql -d $db -p $dn1_standby_port -m -c "set enable_seqscan=off;select sum(id) from mpp_test1 where id % 2 = 1;" | grep 13414841 | wc -l) -eq 1 ]; then
	echo "all of success"
else
	echo "copy to standby $failed_keyword 1/index before vacuum"
	exit
fi


#insert, xlog replicate
gsql -d $db -p $dn1_primary_port -c "insert into mpp_test1 values(1,'1');"
gsql -d $db -p $dn1_primary_port -c "insert into mpp_test1 values(2,'2');"
gsql -d $db -p $dn1_primary_port -c "insert into mpp_test1 values(3,'3');"
gsql -d $db -p $dn1_primary_port -c "insert into mpp_test1 values(4,'4');"
#delete
gsql -d $db -p $dn1_primary_port -c "delete from mpp_test1 where id = 2;vacuum mpp_test1;checkpoint"

echo %2=0:
gsql -d $db -p $dn1_primary_port -c "set enable_seqscan=off;select sum(id) from mpp_test1 where id % 2 = 0;"
echo %2=1:
gsql -d $db -p $dn1_primary_port -c "set enable_seqscan=off;select sum(id) from mpp_test1 where id % 2 = 1;"
#test the copy results
if [ $(gsql -d $db -p $dn1_primary_port -c "set enable_seqscan=off;select sum(id) from mpp_test1 where id % 2 = 0;" | grep 13414810 | wc -l) -eq 1 ]; then
	echo "all of success"
else
	echo "copy to primary $failed_keyword 0/index after vacuum"
	exit
fi
if [ $(gsql -d $db -p $dn1_primary_port -c "set enable_seqscan=off;select sum(id) from mpp_test1 where id % 2 = 1;" | grep 13414845 | wc -l) -eq 1 ]; then
	echo "all of success"
else
	echo "copy to primary $failed_keyword 1/index after vacuum"
	exit
fi
#test the copy results
if [ $(gsql -d $db -p $dn1_standby_port -m -c "set enable_seqscan=off;select sum(id) from mpp_test1 where id % 2 = 0;" | grep 13414810 | wc -l) -eq 1 ]; then
	echo "all of success"
else
	echo "copy to standby $failed_keyword 0/index after vacuum"
	exit
fi
if [ $(gsql -d $db -p $dn1_standby_port -m -c "set enable_seqscan=off;select sum(id) from mpp_test1 where id % 2 = 1;" | grep 13414845 | wc -l) -eq 1 ]; then
	echo "all of success"
else
	echo "copy to standby $failed_keyword 1/index after vacuum"
	exit
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
