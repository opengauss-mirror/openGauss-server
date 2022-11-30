#!/bin/sh

source ./../ss/ss_database_build_env.sh

# stop database
test_1()
{
    sleep 10

    # stop database
    stop_gaussdb ${SS_DATA}/dn0
    if [ $? -eq 0 ]; then
        echo "all of success!"
    else
        echo "$failed_keyword"
        exit 1
    fi

    sleep 15
    
    # start database
    start_gaussdb ${SS_DATA}/dn0
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
    sleep 2
}

test_1
tear_down