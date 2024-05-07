#!/bin/sh

source ./util.sh

function test_max_size_xlog_force_prune()
{
    #1. original max_size_xlog_force_prune is 0
    query_result=`gsql -d $db -p $dn1_primary_port -t -c "show max_size_xlog_force_prune;"`
    if [ -z "`echo $query_result | grep 0`" ]; then
        echo "max_size_xlog_force_prune not 0: $failed_keyword"
    fi

    #2. set max_size_xlog_force_prune to 1GB
    gs_guc reload -D $primary_data_dir -c "max_size_xlog_force_prune=1GB"
    if [ "$?" -ne 0 ]; then
        echo "gs_guc reload max_size_xlog_force_prune error: $failed_keyword"
    fi

    #3. trigger gs_xlog_keepers() 1st time
    gsql -d $db -p $dn1_primary_port -c "select * from gs_xlog_keepers();"

    #4. kill standby
    kill_standby

    #5. original enable_xlog_prune is on
    query_result=`gsql -d $db -p $dn1_primary_port -t -c "show enable_xlog_prune;"`
    if [ -z "`echo $query_result | grep on`" ]; then
        echo "enable_xlog_prune not on: $failed_keyword"
    fi

    #6. trigger gs_xlog_keepers() 2nd time
    gsql -d $db -p $dn1_primary_port -c "select * from gs_xlog_keepers();"

    #7. set enable_xlog_prune to off
    gs_guc reload -D $primary_data_dir -c "enable_xlog_prune=off"
    if [ "$?" -ne 0 ]; then
        echo "gs_guc reload enable_xlog_prune error: $failed_keyword"
    fi

    #8. trigger gs_xlog_keepers() 3rd time
    gsql -d $db -p $dn1_primary_port -c "select * from gs_xlog_keepers();"
}

function tear_down()
{
    #1. start standby
    start_standby

    #2. restore parameter
    gs_guc reload -D $primary_data_dir -c "max_size_xlog_force_prune=0"
    gs_guc reload -D $primary_data_dir -c "enable_xlog_prune=on"
}

test_max_size_xlog_force_prune
tear_down
