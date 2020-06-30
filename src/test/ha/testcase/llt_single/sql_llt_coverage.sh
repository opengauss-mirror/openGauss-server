#!/bin/sh

source ./standby_env.sh

function test_1()
{
check_instance

#create table
gsql -d $db -p $dn1_primary_port -f $scripts_dir/data/sql_llt_init.sql

#run test
gsql -d $db -p $dn1_primary_port -f $scripts_dir/data/sql_llt_test01.sql
gsql -d $db -p $dn1_primary_port -f $scripts_dir/data/sql_llt_test02.sql
}

function test_2()
{

gsql -d $db -p $dn1_primary_port -c "CREATE FUNCTION vec_int4add_3(int,int,int) RETURNS int4 AS '$scripts_dir/../regress/regress.so', 'vec_int4add_0' LANGUAGE C IMMUTABLE;" 

if [ $(gsql -d $db -p $dn1_primary_port -c "select 1 from pg_proc where proname = 'vec_int4add_3';"|wc -l) -eq 5 ]; then
	echo "all of success!"
else
	echo "create function vec_int4add_3 $failed_keyword"
	exit 1
fi
}

function test_3()
{
gsql -d $db -p $dn1_primary_port -f $scripts_dir/data/sql_llt_test03.sql
}

function tear_down()
{
sleep 1
gsql -d $db -p $dn1_primary_port -c "DROP TABLE IF EXISTS PDLGD_OF_TA351307;DROP TABLE IF EXISTS PDLGD_OF_TA351307_NEW;DROP TABLE IF EXISTS PROVI_AMT_OF_TA490461;DROP SCHEMA EDISUM CASCADE;DROP SCHEMA EDIODS CASCADE;"

#test pool_sendpids and pool_recvpids for llt data. not supported in single-node
#gsql -d $db -p $dn1_primary_port -c "CLEAN CONNECTION TO ALL FORCE FOR DATABASE postgres;"
#stop_coordinator
#start_coordinator
}

test_1
#test_2
test_3
tear_down
