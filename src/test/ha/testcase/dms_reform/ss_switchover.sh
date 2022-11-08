#!/bin/sh

source ./../ss/ss_database_build_env.sh

# switchover test
test_1()
{
    sleep 5
   
    # switchover
    ${GAUSSHOME}/bin/gs_ctl switchover -D ${SS_DATA}/dn1
    if [ $? -eq 0 ]; then
        echo "all of success!"
    else
	echo "$failed_keyword"
	exit 1
    fi

    sleep 10
}

tear_down()
{
    sleep 10
    #switchover
    ${GAUSSHOME}/bin/gs_ctl switchover -D  ${SS_DATA}/dn0
    if [ $? -eq 0 ]; then
        echo "all of success!"
    else
        echo "$failed_keyword"
        exit 1
    fi 
}

test_1
tear_down

