#!/bin/sh

source ./../ss/ss_database_build_env.sh

# failover test
test_1()
{
    sleep 10

    # switchover at first
    ${GAUSSHOME}/bin/gs_ctl switchover -D ${SS_DATA}/dn1
    if [ $? -eq 0 ]; then
        echo "all of success!"
    else
        echo "$failed_keyword"
        exit 1
    fi

    sleep 10
    
    # stop database
    stop_gaussdb ${SS_DATA}/dn0
    if [ $? -eq 0 ]; then
        echo "all of success!"
    else
        echo "$failed_keyword"
        exit 1
    fi
    sleep 5
    stop_gaussdb ${SS_DATA}/dn1 
    if [ $? -eq 0 ]; then
        echo "all of success!"
    else
        echo "$failed_keyword"
        exit 1
    fi
    sleep 10
   
    # start database0 to simulate failover
    start_gaussdb ${SS_DATA}/dn0 ${PGPORT0}
    if [ $? -eq 0 ]; then
        echo "all of success!"
    else
        echo "$failed_keyword"
        exit 1
    fi
    sleep 5
    
    start_gaussdb ${SS_DATA}/dn1 ${PGPORT1}
    if [ $? -eq 0 ]; then
        echo "all of success!"
    else
        echo "$failed_keyword"
        exit 1
    fi

    sleep 5
}

tear_down()
{
    sleep 5
}

test_1
tear_down
