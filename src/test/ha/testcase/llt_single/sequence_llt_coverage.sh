#!/bin/sh

source ./standby_env.sh

function test_1()
{
check_instance

#create table
gsql -d $db -p $dn1_primary_port -f $scripts_dir/data/sequence_llt_init.sql

#run test
gsql -d $db -p $dn1_primary_port -f $scripts_dir/data/sequence_llt_test.sql
}

function test_2()
{
gtm_ctl switchover -w -t $gsctl_wait_time -D  $gtm_standby_data_dir  
sleep 3
gtm_ctl switchover -w -t $gsctl_wait_time -D  $gtm_primary_data_dir
}
function tear_down()
{
sleep 3
gsql -d $db -p $dn1_primary_port -f $scripts_dir/data/sequence_llt_drop.sql
}

test_1
#don't have gtm in single-node mode
#test_2
tear_down