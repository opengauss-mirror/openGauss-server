#!/bin/bash
# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# openGauss-server is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
# Portions Copyright (c) 2013-2020, Euler Taveira de Oliveira


function delete()
{
    rm -rf test_wal2json
    rm -rf $PGTMPDATA
}

function database_install()
{
    echo "init gaussdb database"
    gs_initdb -D $PGTMPDATA --nodename=single_node1 -w Test@123 > init.log 2>&1
    if [ $? -ne 0 ]
    then
        echo "init failed, see init.log for detail information"
    delete
        exit 1
    else
        echo "init success, begin to start"
    fi
    if [ $PGTEMPPORT ];then
        echo "PGTEMPPORT = $PGTEMPPORT, IF THE PORT BE OCCUPIED, PLEASE EXPORT PGTEMPPORT DIFFERENT PORT!"
    else
        echo "PGTEMPPORT IS NOT EXISTS, PLEASE SET PGTEMPPORT, IT IS REGRESSION TEST PORT"
    delete
        exit 1
    fi
    echo "port = $PGTEMPPORT" >> $PGTMPDATA/postgresql.conf
    echo "most_available_sync = on" >> $PGTMPDATA/postgresql.conf
    echo "replconninfo1 = 'localhost=127.0.0.1 localport=12001 localheartbeatport=12004 remotehost=127.0.0.1 remoteport=12002 remoteheartbeatport=12005'" >> $PGTMPDATA/postgresql.conf
    sed -i 's/wal_level = hot_standby/wal_level = logical/g' $PGTMPDATA/postgresql.conf
    sed -i 's/max_replication_slots = 8/max_replication_slots = 10/g' $PGTMPDATA/postgresql.conf
    sed -i 's/max_wal_senders = 4/max_wal_senders = 10/g' $PGTMPDATA/postgresql.conf
    gs_ctl start -D $PGTMPDATA -M primary > start.log 2>&1
    if [ $? -ne 0 ]
    then
        echo "start failed，see start.log for detail information"
    delete
        exit 1
    else
        echo "OpenGauss start success，the port is $PGTEMPPORT"
    fi
}

function run_check()
{
    gsql -d postgres -p $PGTEMPPORT -c "create database regression;"
    if [ $? -ne 0 ]
    then
        echo "create database failed"
	delete
        exit 1
    fi
    if [ ! -d "result" ]; then    
        mkdir result
    fi

    for sql_flle in $( ls sql/|awk -F '.' '{print $1;}')
    do
        gsql -d regression -p $PGTEMPPORT -a <  sql/$sql_flle.sql > result/$sql_flle.out 2>&1
    done

    if [ -e "diff.log" ]; then
        rm diff.log
    fi

    for out_file in $(ls expected)
    do
        diff -u -w -B --ignore-matching-lines="WARNING:  Due to DDL in the same top transaction, partial modification may be lost for <XID, FIRST_LSN, FINAL_LSN, SKIP_START_LSN>*" expected/$out_file result/$out_file >> diff.log
    done

    if [[ `cat diff.log |wc -l` -eq 0 ]]
    then
        echo -e "\033[32m OK \033[0m"
    else
        echo -e "\033[31m FAILED \033[0m"
    fi
    pid=$(ps ux | grep "$PGTMPDATA" | grep -v "grep" | tr -s ' ' | cut -d ' ' -f 2)
    echo "The gaussdb server pid is $pid"
    kill -9 $pid
    sleep 0.5
    pid_check=$(ps ux | grep "gaussdb" | grep -v "grep" | tr -s ' ' | cut -d ' ' -f 2)
    if [ $pid_check ]
    then
        echo "The gaussdb server $pid has not been killed"
    else
        echo "The gaussdb server $pid has been killed"
    fi
    delete
    rm init.log
    rm start.log
}

function main()
{
    database_install
    run_check
}

main $@
