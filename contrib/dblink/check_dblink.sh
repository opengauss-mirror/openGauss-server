#!/bin/bash


function fn_print_help()
{
    echo "Usage: $0 [OPTION]
    -?|--help                         show help information
    -U|--user_name                    cluster user
    -p|--port                         database server port
    "
}


function fn_prase_input_param()
{
    while [ $# -gt 0 ]; do
        case $1 in
            -\?|--help )
                fn_print_help
                exit 1
                ;;
            -U|--user_name )
                fn_check_param user_name $2
                user_name=$2
                shift 2
                ;;
            -p|--port )
                fn_check_param port $2
                host_port=$2
                shift 2
                ;;
            * )
                echo "Please input right paramtenter, the following command may help you"
                echo "sh check_dblink.sh --help or sh check_dblink.sh -?"
                exit 1
        esac
    done
}


function fn_check_param()
{
    if [ "$2"X = X ]
    then
        echo "no given $1, the following command may help you"
        echo "sh check_dblink.sh --help or sh check_dblink.sh -?"
        exit 1
    fi
}


function fn_check_input()
{
    if [ ! "$user_name" -o ! "$host_port" ]
    then
        echo "Usage: sh check_dblink.sh -U user_name -p port"
        echo "The following command may help you"
        echo "sh check_dblink.sh --help or sh check_dblink.sh -?"
        return 1
    fi
    if [ "`netstat -an | grep -w $host_port`" ]
    then 
        echo "port $host_port occupied, please choose another."
        return 1
    fi
    return 0
}


function database_install()
{
    echo "init openGauss database"
    gs_initdb -D test_dblink/dn1 --nodename=single_node1 -w Test@123 > init.log 2>&1
    if [ $? -ne 0 ]
    then
        echo "init failed，see init.log for detail information"
	delete
        exit 1
    else
        echo "init success, begin to start"
    fi
    echo "port = $host_port" >> test_dblink/dn1/postgresql.conf
    gs_ctl start -D test_dblink/dn1 > start.log 2>&1
    if [ $? -ne 0 ]
    then
        echo "start failed，see start.log for detail information"
	delete
        exit 1
    else
        echo "openGauss start success，the port is $host_port"
    fi
}


function create_sql()
{
    cp sql/dblink.tmp sql/dblink.sql
    sed -i "s/portIp/$host_port/g" sql/dblink.sql
    sed -i "s/userName/$user_name/g" sql/dblink.sql
    cp expected/dblink.tmp expected/dblink.out
    sed -i "s/portIp/$host_port/g" expected/dblink.out
    sed -i "s/userName/$user_name/g" expected/dblink.out
}


function run_check()
{
    gsql -d postgres -p "$host_port" -c "create database regression;"
    if [ $? -ne 0 ]
    then
        echo "create database failed"
	delete
        exit 1
    fi

    create_sql
    if [ $? -ne 0 ]
    then
        echo "generate sql file failed"
	delete
        exit 1
    fi
    gsql -d regression -p $host_port -a <  sql/dblink.sql > result/dblink.out 2>&1
    diff -u result/dblink.out expected/dblink.out > diff.log
    if [[ `cat diff.log |wc -l` -eq 0 ]]
    then
        echo -e "\033[32m OK \033[0m"
    else
        echo -e "\033[31m FAILED \033[0m"
    fi
    pid=$(ps ux | grep "test_dblink" | grep -v "grep" | tr -s ' ' | cut -d ' ' -f 2)
    kill -9 $pid
    delete
}


function delete()
{
    rm -rf test_dblink
    rm -rf sql/dblink.sql
    rm -rf expected/dblink.out
}


function main()
{
    fn_prase_input_param $@
    fn_check_input
    if [ $? -ne 0 ]
    then
        exit 1
    fi
    database_install
    run_check
}


main $@