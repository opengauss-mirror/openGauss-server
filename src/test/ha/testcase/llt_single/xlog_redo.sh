#!/bin/sh
#the shell is to test complicated Performance of the dataqueue
#3 gsql to copy data while dataqueue_size=256M

source ./standby_env.sh

function test_1()
{
check_instance

gs_guc reload -Z datanode -D $data_dir/datanode1 -c "wal_keep_segments=128"

#test create table and truncate at one transaction
gsql -d $db -p $dn1_primary_port -c "start transaction; 
							create table testtrunk(id integer,id2 integer);
							insert into testtrunk values(1,1);
							insert into testtrunk values(2,2);
							insert into testtrunk select * from testtrunk;
							truncate testtrunk;
							commit;"

#test unlogged table
gsql -d $db -p $dn1_primary_port -c "CREATE unlogged TABLE table_replication_12 
							(
								R_REGIONKEY  INT NOT NULL
							  , R_NAME       CHAR(25) NOT NULL
							  , R_COMMENT    VARCHAR(152)
							)
							with (orientation = column)
							distribute by hash(R_REGIONKEY)
							;
							vacuum full table_replication_12;"

gsql -d $db -p $dn1_primary_port -c "checkpoint;"

#test gin index
gsql -d $db -p $dn1_primary_port -f $scripts_dir/data/gin_test.sql

#test gist/spgist index
#gsql -d $db -p $dn1_primary_port -f $scripts_dir/data/gist_spgist.sql
}

function test_2()
{
ls $primary_data_dir/pg_xlog | grep -v archive_status > xlog.log
for line in `cat xlog.log`
do
	printf "pg_xlogdump %s\n" $line
	pg_xlogdump $primary_data_dir/pg_xlog/$line > /dev/null 2>&1
done
rm xlog.log
}

function tear_down()
{
sleep 1
gs_guc reload -Z datanode -D $data_dir/datanode1 -c "wal_keep_segments=16"
gsql -d $db -p $dn1_primary_port -c "checkpoint;"
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists testtrunk; DROP TABLE if exists table_replication_12;"
}

test_1
test_2
tear_down
