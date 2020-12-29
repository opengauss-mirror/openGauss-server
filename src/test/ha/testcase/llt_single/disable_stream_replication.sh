#!/bin/sh
#disable stream replication test
#test wal stream replication and data stream replication with enable_stream_replication = off
#test catchup and recopy after enable stream replication

source ./standby_env.sh

function test_replication()
{
check_instance

cstore_rawdata_lines=10000

#prepare
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test1; CREATE TABLE mpp_test1(id INT,name VARCHAR(15) NOT NULL);"
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test2; CREATE TABLE mpp_test2
															(
																L_ORDERKEY    BIGINT NOT NULL
															  , L_PARTKEY     BIGINT NOT NULL
															  , L_SUPPKEY     BIGINT NOT NULL
															  , L_LINENUMBER  BIGINT NOT NULL
															  , L_QUANTITY    DECIMAL(15,2) NOT NULL
															  , L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL
															  , L_DISCOUNT    DECIMAL(15,2) NOT NULL
															  , L_TAX         DECIMAL(15,2) NOT NULL
															  , L_RETURNFLAG  CHAR(1) NOT NULL
															  , L_LINESTATUS  CHAR(1) NOT NULL
															  , L_SHIPDATE    DATE NOT NULL
															  , L_COMMITDATE  DATE NOT NULL
															  , L_RECEIPTDATE DATE NOT NULL
															  , L_SHIPINSTRUCT CHAR(25) NOT NULL
															  , L_SHIPMODE     CHAR(10)
															  , L_COMMENT      VARCHAR(44)
															)
															with (orientation = column)
															distribute by hash(L_ORDERKEY);"
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test3;"

#data replication test for row store
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test1 from '$scripts_dir/data/data5';"
#data replication test for column store
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test2 from '$scripts_dir/data/lineitem.data' delimiter '|';"

#disable stream replication to standby or dummy, cluster is running under high performance mode
gs_guc reload -Z datanode -D $data_dir/datanode1 -c "enable_stream_replication=false"

#wait param change reload to standby
while [ $(gsql -d $db -p $dn1_standby_port -m -c "show enable_stream_replication;" | grep "off" | wc -l) -eq 0 ]
do
	echo "sleep 2 seconds waiting for parameter sync"
	sleep 2
done

#DDL test
gsql -d $db -p $dn1_primary_port -c "CREATE TABLE mpp_test3(id INT,name VARCHAR(15) NOT NULL);"	
#DML test
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=off; copy mpp_test1 from '$scripts_dir/data/data5';"
#data replication test for row store
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test1 from '$scripts_dir/data/data5';"
#data replication test for column store
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test2 from '$scripts_dir/data/lineitem.data' delimiter '|';"

#check proc function
gsql -d $db -p $dn1_primary_port -c "select * from pg_stat_get_data_senders();"

gsql -d $db -p $dn1_primary_port -c "checkpoint;"


#test the copy results on dn1_primary
if [ $(gsql -d $db -p $dn1_primary_port -c "select count(1) from mpp_test1;" | grep `expr 3 \* $rawdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_primary mpp_test1"
else
	echo "copy $failed_keyword on dn1_primary mpp_test1"
	exit 1
fi

if [ $(gsql -d $db -p $dn1_primary_port -c "select count(1) from mpp_test2;" | grep `expr 2 \* $cstore_rawdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_primary mpp_test2"
else
	echo "copy $failed_keyword on dn1_primary mpp_test2"
	exit 1
fi

#test the copy results on dn1_standby
if [ $(gsql -d $db -p $dn1_standby_port -m -c "select count(1) from mpp_test1;" | grep $rawdata_lines |wc -l) -eq 1 ]; then
	echo "copy success on dn1_standby mpp_test1"
else
	echo "copy $failed_keyword on dn1_standby mpp_test1"
	exit 1
fi

if [ $(gsql -d $db -p $dn1_standby_port -m -c "select count(1) from mpp_test2;" | grep $cstore_rawdata_lines |wc -l) -eq 1 ]; then
	echo "copy success on dn1_standby mpp_test2"
else
	echo "copy $failed_keyword on dn1_standby mpp_test2"
	exit 1
fi

if [ $(gsql -d $db -p $dn1_standby_port -m -c "select count(1) from pg_class where relname = 'mpp_test3';" | grep 0 |wc -l) -eq 1 ]; then
	echo "no mpp_test3 on standby"
else
	echo "$failed_keyword mpp_test3 exists on standby"
	exit 1
fi
}

function test_catchup()
{
gs_guc reload -Z datanode -D $data_dir/datanode1 -c "enable_incremental_catchup=off"

#enable stream replication to standby or dummy, cluster is running under high performance mode
gs_guc reload -Z datanode -D $data_dir/datanode1 -c "enable_stream_replication=true"

#DML test after enable stream replication
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=off; copy mpp_test1 from '$scripts_dir/data/data5';"
#data replication test for column store
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test2 from '$scripts_dir/data/lineitem.data' delimiter '|';"

wait_catchup_finish

#test the copy results on dn1_primary
if [ $(gsql -d $db -p $dn1_primary_port -c "select count(1) from mpp_test1;" | grep `expr 4 \* $rawdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_primary mpp_test1"
else
	echo "copy $failed_keyword on dn1_primary mpp_test1"
	exit 1
fi

if [ $(gsql -d $db -p $dn1_primary_port -c "select count(1) from mpp_test2;" | grep `expr 3 \* $cstore_rawdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_primary mpp_test2"
else
	echo "copy $failed_keyword on dn1_primary mpp_test2"
	exit 1
fi

#test the copy results on dn1_standby
if [ $(gsql -d $db -p $dn1_standby_port -m -c "select count(1) from mpp_test1;" | grep `expr 4 \* $rawdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_standby mpp_test1"
else
	echo "copy $failed_keyword on dn1_standby mpp_test1"
	exit 1
fi

if [ $(gsql -d $db -p $dn1_standby_port -m -c "select count(1) from mpp_test2;" | grep `expr 3 \* $cstore_rawdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_standby mpp_test2"
else
	echo "copy $failed_keyword on dn1_standby mpp_test2"
	exit 1
fi

if [ $(gsql -d $db -p $dn1_standby_port -m -c "select count(1) from pg_class where relname = 'mpp_test3';" | grep 1 |wc -l) -eq 2 ]; then
	echo "mpp_test3 exists on standby"
else
	echo "$failed_keyword mpp_test3 doesnot exist on standby"
	exit 1
fi
}

function tear_down()
{
sleep 1
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test1;"
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test2;"
gsql -d $db -p $dn1_primary_port -c "DROP TABLE if exists mpp_test3;"
gs_guc reload -Z datanode -D $data_dir/datanode1 -c "enable_incremental_catchup=on"
}

test_replication
test_catchup
tear_down
