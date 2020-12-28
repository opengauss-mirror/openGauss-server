#!/bin/sh

source ./standby_env.sh

function test_1()
{
check_instance

cstore_rawdata_lines=10000

#create table
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

#copy data(25M) to standby 4 times
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test1 from '$scripts_dir/data/data5';" &
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test2 from '$scripts_dir/data/lineitem.data' delimiter '|';" &
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test1 from '$scripts_dir/data/data5';" &
gsql -d $db -p $dn1_primary_port -c "set enable_data_replicate=on; copy mpp_test2 from '$scripts_dir/data/lineitem.data' delimiter '|';" &

# wait done
for((i=0;i<4;i++)); do 
    j=$(echo "$i+1" | bc -l)   
    wait %$j  
    echo $?
done

gsql -d $db -p $dn1_primary_port -c "checkpoint;"

#test the copy results on dn1_primary
if [ $(gsql -d $db -p $dn1_primary_port -c "select count(1) from mpp_test1;" | grep `expr 2 \* $rawdata_lines` |wc -l) -eq 1 ]; then
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
if [ $(gsql -d $db -p $dn1_standby_port -m -c "select count(1) from mpp_test1;" | grep `expr 2 \* $rawdata_lines` |wc -l) -eq 1 ]; then
	echo "copy success on dn1_standby mpp_test1"
else
	echo "copy $failed_keyword on dn1_standby mpp_test1"
	exit 1
fi

if [ $(gsql -d $db -p $dn1_standby_port -m -c "select count(1) from mpp_test2;" | grep `expr 2 \* $cstore_rawdata_lines` |wc -l) -eq 1 ]; then
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
tear_down