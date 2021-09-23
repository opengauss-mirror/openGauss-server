#!/bin/sh
#the shell is to test complicated Performance of the dataqueue
#3 gsql to copy data while dataqueue_size=256M

source ./standby_env.sh

function test_1()
{
set_default
check_detailed_instance

gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists testapplydelay"
gs_guc reload -D $standby3_data_dir -c "recovery_min_apply_delay=5s"

# create table  at one transaction
gsql -d $db -p $dn1_primary_port -c "start transaction; 
						create table testapplydelay(id integer);
						insert into testapplydelay values(1);
						commit;"

if [ $(gsql -d $db -p $standby3_port -c "select count(*) from testapplydelay;"|wc -l) -eq 0 ]; then
	echo "apply delay step1 success!"
else
	echo "apply delay step1 $failed_keyword"
	exit 1
fi

sleep 8
if [ $(gsql -d $db -p $standby3_port -c "select count(*) from testapplydelay;"|wc -l) -eq 5 ]; then
	echo "apply delay step2 success!"
else
	echo "apply delay step2 $failed_keyword"
	exit 1
fi
}

function tear_down()
{
sleep 1
gs_guc reload -D $standby3_data_dir -c "recovery_min_apply_delay=0"
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists testapplydelay"
}

test_1
tear_down
