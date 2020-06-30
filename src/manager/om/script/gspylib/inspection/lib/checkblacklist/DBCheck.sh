#!/bin/bash
#Copyright (c) 2020 Huawei Technologies Co.,Ltd.
#
#openGauss is licensed under Mulan PSL v2.
#You can use this software according to the terms and conditions of the Mulan PSL v2.
#You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
#-------------------------------------------------------------------------
#
# DBCheck.sh
#
# IDENTIFICATION
#    src/manager/om/script/gspylib/inspection/lib/checkblacklist/DBCheck.sh
#
#-------------------------------------------------------------------------

GSPORT=''

function usage()
{
    echo "***********************************************************************"
    echo "*                         DBCheck.sh usage                            *"
    echo "* -p: coordinator port number                                         *"
    echo "* example:  ./DBCheck.sh -p 25308                                     *"
    echo "***********************************************************************"
}

function parse_para()
{
    while getopts "p:h" opt
    do
        case $opt in
            p)
                if [ -z $GSPORT ]; then
                    let GSPORT=$OPTARG
                else
                    echo "ERROR: duplicate port number"
                    usage
                    exit 1
                fi
                ;;
            h)
                usage
                exit 1
                ;;
            ?)
                echo "ERROR: unkonw argument"
                usage
                exit 1
                ;;
        esac
    done

    if [ -z $GSPORT ]; then
        echo "ERROR: must designate -p"
        usage
        exit 1
    fi
}

function blacklist_check()
{
    blacklist_sql="blacklist_check-"$date_flag".sql"
    #only used from v1r5 to v1r6
    version=$(gaussdb -V | awk -F 'Gauss200 OLAP' '{print $2}' | awk -F ' ' '{print $1}')
    sed s/OMVersion/$version/g blacklist_check.sql >$blacklist_sql
    
    
    echo "==========================================================================================================================="
    echo "==                                                                                                                       =="
    echo "==                                            Check Blacklist                                                            =="
    echo "==                                                                                                                       =="
    echo "==========================================================================================================================="

    for db in $(gsql -d postgres -p $GSPORT -c "select datname||'  GAUSSDB' from pg_database where datname != 'template0'" | grep GAUSSDB | awk '{print $1}')
    do
        echo "Blacklist Check for DataBase: "$db
        check_log="checklog-"$db"-"$date_flag".log"
        gsql -d $db -p $GSPORT -f $blacklist_sql > $check_log
		if [ $(cat $check_log | grep FAILED | wc -l) -gt 0 ]; then
			echo "NOTICE: Violation of blacklist rule"
			cat $check_log
		else
			echo "NOTICE: Comply with the blacklist rule"
		fi

        for sqlfile in $(cat $check_log | grep FAILED | awk -F '|' '{print $NF}')
        do
            sqlfilelog=$(echo $log_path/$sqlfile|awk -F '.' '{print $1}')-$db.log
            gsql -d $db -p $GSPORT -f $sqlfile > $sqlfilelog
            cat $sqlfilelog
        done
    done
}

function dropped_column_table_check()
{
    echo "==========================================================================================================================="
    echo "==                                                                                                                       =="
    echo "==                                        Check DroppedColumnTable                                                       =="
    echo "==                                                                                                                       =="
    echo "==========================================================================================================================="

    dropped_column_table_log=$log_path/GetDroppedColumnTable.log
    ./ExecuteSQLOnAllDB.sh -p $GSPORT -f GetDroppedColumnTable.sql > $dropped_column_table_log
    cat $dropped_column_table_log
}

function recurrent_grant_check()
{
    echo "==========================================================================================================================="
    echo "==                                                                                                                       =="
    echo "==                                         Check RecurrentGrant                                                          =="
    echo "==                                                                                                                       =="
    echo "==========================================================================================================================="

    table_recurrent_grant_log=$log_path/GetTableRecurrentGrant.log
    ./ExecuteSQLOnAllDB.sh -p $GSPORT -f GetTableRecurrentGrant.sql > $table_recurrent_grant_log
    cat $table_recurrent_grant_log
}

function main()
{
    date_flag=$(date "+%Y%m%d-%H%M%S")
    if [ ! -d ./log ]; then mkdir ./log ;fi
    if [ ! -d ./log/"checkBlack-"$date_flag ]; then mkdir ./log/"checkBlack-"$date_flag ;fi
    log_path=$(pwd)"/log/checkBlack-"$date_flag
    source /opt/huawei/Bigdata/mppdb/.mppdbgs_profile 
    parse_para $*

    blacklist_check
    dropped_column_table_check
    recurrent_grant_check
}

main $*
