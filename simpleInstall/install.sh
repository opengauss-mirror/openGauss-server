#!/bin/bash
# -*- coding:utf-8 -*-
#############################################################################
# Copyright (c): 2021, Huawei Tech. Co., Ltd.
# FileName     : install.py
# Version      : V1.0.0
# Date         : 2021-03-15
# Description  : the scripy used to install the single cluster on one machine
#########################################

function usage()
{
    echo "
Usage: sh $0 -w password
Arguments:
   -w                   login password
   -p                   datanode port, default 5432
   --multinode          if specify, will install master_slave cluster. default install single node.
   -h, --help           Show this help, then exit
   "
}

function info()
{
    echo -e "\033[32m$1\033[0m"
}

function error() {
    echo -e "\033[31m$1\033[0m"
}

function check_param() {
    if [ X$password == X'' ]; then
        error "ERROR: The parameter '-w' can not be empty\n"
        usage
        exit 1
    fi

    local -i num=0
    if [ -n "$(echo $password | grep -E --color '^(.*[a-z]+).*$')" ]; then
      let num=$num+1
    fi
    if [ -n "$(echo $password | grep -E --color '^(.*[A-Z]).*$')" ]; then
      let num=$num+1
    fi
    if [ -n "$(echo $password | grep -E --color '^(.*\W).*$')" ]; then
      let num=$num+1
    fi
    if [ -n "$(echo $password | grep -E --color '^(.*[0-9]).*$')" ]; then
      let num=$num+1
    fi
    if [ ${#password} -lt 8 ] || [ $num -lt 3 ]
    then
        error "password must be at least 8 character and at least three kinds"
        exit 1
    fi

    if [ X$user == X"root" ]; then
        error "Error: can not install openGauss with root"
        exit 1
    fi
    if [ X$port == X"" ]; then
        port=$default_port
    fi
    if [ X$mode == X"master_standby" ]; then
        let slave_port=$port+200
    fi
}

function check_install_env() {
    # check pgxc path
    if [ X$mode == X"single" ]
    then
        if [ -d "$app/data/single_node" ] && [ X"$(ls -A $app/data/single_node)" != X"" ]
            then
                error "ERROR: the directory $app/data/single_node must be dir and empty"
                exit 1
        fi
    else
        for pgxc in $(echo "master slave" | awk '{print $1,$2}')
        do
            if [ -d "$app/data/$pgxc" ] && [ X"$(ls -A $app/data/$pgxc)" != X"" ]
            then
                error "ERROR: the directory $app/data/master or $app/data/slave must be dir and empty"
                exit 1
            fi
        done
    fi

    # check pid port and lock file
    port_occupied=$(netstat -ntul | grep $port)
    if [ X"$port_occupied" != X"" ]; then
        error "Error: The port $port has been occupied, please use -p to set a new port."
        exit 1
    fi

    local delete_slave_lock=""
    local delete_master_lock=""
    if [ X$slave_port != X"0" ]; then
        slave_occupied=$(netstat -ntul | grep $slave_port)
        delete_slave_lock=$(rm -rf /tmp/.s.PGSQL.$slvae_port* 2>&1)
        if [ X"$slave_occupied" != X"" ]; then
            error "Error: The slave port $slave_port has been occupied, please use -p to set a new port."
            exit 1
        fi
    fi
    delete_master_lock=$(rm -rf /tmp/.s.PGSQL.$port* 2>&1)
    if [[ $delete_master_lock == *"Operation not permitted"* ]] || [[ $delete_slave_lock == *"Operation not permitted"* ]]; then
        error "Error: not have permitted to delete /tmp/.s.PGSQL.* file, you should delete it or change port."
        exit 1
    fi
}

function check_os() {
    # check shm
    local shared_buffers=1073741824  # 1G
    local shmmax=$(cat /proc/sys/kernel/shmmax)
    env test $shared_buffers -gt $shmmax && echo "Shared_buffers must be less than shmmax. Please check it." && exit 1

    local shmall=$(cat /proc/sys/kernel/shmall)
    local pagesize=$(getconf PAGESIZE)
    if [ $(($shmall/1024/1024/1024*$pagesize)) -ne 0 ]; then
      if [ $(($shared_buffers/1024/1024/1024-$shmall/1024/1024/1024*$pagesize)) -gt 0 ]; then
        echo "The usage of the device [Shared_buffers] space cannot be greater than shmall*PAGESIZE." && exit 1
      fi
    fi

    # check sem
    local -a sem
    local -i index=0
    local max_connection=5000
    local conn_floor
    for line in $(cat /proc/sys/kernel/sem)
    do
      sem[index]=$line
      let index=$index+1
    done
    if [ ${sem[0]} -lt 17 ]
        then
            info "On systemwide basis, the maximum number of SEMMSL is not correct. the current SEMMSL value is: ${sem[0]}. Please check it."
            info "The required value should be greater than 17. You can modify it in file '/etc/sysctl.conf'."
            exit 1
    fi
    let conn_floor=($max_connection+150)/16
    if [ ${sem[3]} -lt $conn_floor ]
    then
      info "On systemwide basis, the maximum number of SEMMNI is not correct. the current SEMMNI value is: ${sem[3]}. Please check it."
      info "The required value should be greater than ${conn_floor}. You can modify it in file '/etc/sysctl.conf'."
      exit 1
    fi

    let conn_floor=$conn_floor*17
    if [ ${sem[1]} -lt $conn_floor ]
    then
      info "On systemwide basis, the maximum number of SEMMNS is not correct. the current SEMMNS value is: ${sem[1]}. Please check it."
      info "The required value should be greater than ${conn_floor}. You can modify it in file '/etc/sysctl.conf'."
      exit 1
    fi
    # check cpu instruction
    CPU_BIT=$(uname -m)
    if [ X"$CPU_BIT" == X"x86_64" ]; then
        if [ X"$(cat /proc/cpuinfo | grep rdtscp | uniq)" == X"" ]; then
            echo "The cpu instruction rdtscp is missing." && exit 1
        fi
    fi
}

function change_gausshome_owner() {
  mkdir_file=$(chown $user:$group $app 2>&1)
  if [[ $mkdir_file == *"Permission denied"* ]]; then
      error "Error: $user not have permission to change $app owner and group. please fix the permission manually."
      exit 1
  fi
  chmod 700  $app
}

function set_environment() {
    local path_env='export PATH=$GAUSSHOME/bin:$PATH'
    local ld_env='export LD_LIBRARY_PATH=$GAUSSHOME/lib:$LD_LIBRARY_PATH'
    local insert_line=2
    sed -i "/^\\s*export\\s*GAUSSHOME=/d" ~/.bashrc
    # set PATH and LD_LIBRARY_PATH
    if [ X"$(grep 'export PATH=$GAUSSHOME/bin:$PATH' ~/.bashrc)" == X"" ]
    then
        echo $path_env >> ~/.bashrc
    fi
    if [ X"$(grep 'export LD_LIBRARY_PATH=$GAUSSHOME/lib:$LD_LIBRARY_PATH' ~/.bashrc)" == X"" ]
    then
        echo $ld_env >> ~/.bashrc
    fi
    if [ X"$(grep 'export GS_CLUSTER_NAME=dbCluster' ~/.bashrc)" == X"" ]
    then
        echo 'export GS_CLUSTER_NAME=dbCluster' >> ~/.bashrc
    fi
    if [ X"$(grep 'ulimit -n 1000000' ~/.bashrc)" == X"" ]
    then
        echo 'ulimit -n 1000000' >> ~/.bashrc
    fi
    # set GAUSSHOME
    path_env_line=$(cat ~/.bashrc | grep -n 'export PATH=$GAUSSHOME/bin:$PATH' | awk -F ':' '{print $1}')
    ld_env_line=$(grep -n 'export LD_LIBRARY_PATH=$GAUSSHOME/lib:$LD_LIBRARY_PATH' ~/.bashrc | awk -F ':' '{print $1}')
    echo
    if [ $path_env_line -gt $ld_env_line ]
    then
        let insert_line=$ld_env_line
    else
        let insert_line=$path_env_line
    fi
    sed -i "$insert_line i\export GAUSSHOME=$app" ~/.bashrc
    source ~/.bashrc
}

function single_install() {
    info "[step 6]: init datanode"
    gs_initdb -w $password -D $app/data/single_node --nodename "sgnode" --locale="en_US.UTF-8"
    if [ X$port != X$default_port  ]
    then
        sed -i "/^#port =/c\port = $port" $app/data/single_node/postgresql.conf
    fi
    info "[step 7]: start datanode"
    gs_ctl start -D $app/data/single_node -Z single_node
}

function init_db() {
    info "[init primary datanode.]"
    gs_initdb -D $app/data/master --nodename=datanode1 -E UTF-8 --locale=en_US.UTF-8 -U $user  -w $password
    info "[init slave datanode.]"
    gs_initdb -D $app/data/slave --nodename=datanode2 -E UTF-8 --locale=en_US.UTF-8 -U $user  -w $password
}

function config_db() {
    info "[config datanode.]"
    local -a ip_arr
    local -i index=0
    for line in $(/sbin/ifconfig -a|grep inet|grep -v 127.0.0.1|grep -v inet6|awk '{print $2}'|tr -d "addr:")
    do
        ip_arr[index]=$line
        let index=$index+1
    done
    sed -i "/^#listen_addresses/c\listen_addresses = 'localhost,${ip_arr[0]}'"  $app/data/master/postgresql.conf
    sed -i "/^#listen_addresses/c\listen_addresses = 'localhost,${ip_arr[0]}'"  $app/data/slave/postgresql.conf
    sed -i "/^#port/c\port = $port"  $app/data/master/postgresql.conf
    sed -i "/^#port/c\port = $slave_port"  $app/data/slave/postgresql.conf
    sed -i "/^#replconninfo1/c\replconninfo1 = 'localhost=${ip_arr[0]} localport=$(($port+1)) localheartbeatport=$(($port+5)) localservice=$(($port+4)) remotehost=${ip_arr[0]} remoteport=$(($slave_port+1)) remoteheartbeatport=$(($slave_port+5)) remoteservice=$(($slave_port+4))'"  $app/data/master/postgresql.conf
    sed -i "/^#replconninfo1/c\replconninfo1 = 'localhost=${ip_arr[0]} localport=$(($slave_port+1)) localheartbeatport=$(($slave_port+5)) localservice=$(($slave_port+4)) remotehost=${ip_arr[0]} remoteport=$(($port+1)) remoteheartbeatport=$(($port+5)) remoteservice=$(($port+4))'"  $app/data/slave/postgresql.conf
    echo "remote_read_mode = non_authentication" | tee -a $app/data/master/postgresql.conf $app/data/slave/postgresql.conf
    echo "host    all             all             ${ip_arr[0]}/32            trust" | tee -a $app/data/master/pg_hba.conf $app/data/slave/pg_hba.conf
}

function start_db() {
    info "[start primary datanode.]"
    gs_ctl start -D $app/data/master -M primary
    info "[build and start slave datanode.]"
    gs_ctl build -D $app/data/slave  -b full
}

function master_standby_install() {
    init_db
    config_db
    start_db
}

declare default_port=5432
declare user=$(whoami)
declare group=$(id -gn $user)
declare shell_path=$(cd `dirname $0`;pwd)
declare app=$(dirname $shell_path)
declare mode="single"
declare -i port
declare -i slave_port=0

function get_param() {
    ARGS=$(getopt -a -o w:p:h -l multinode,help -- "$@")
    [ $? -ne 0 ] && usage
    eval set -- "${ARGS}"
    while [ $# -gt 0 ]
    do
        case "$1" in
        -w)
            password="$2"
            shift
            ;;
        -p)
            port="$2"
            shift
            ;;
        --multinode)
            mode="master_standby"
            ;;
        -h|--help)
            usage
            exit
            ;;
        --)
            shift
            break
            ;;
        esac
    shift
    done
}

function end_help() {
    if [ X$mode == X"single" ]
    then
        echo -e '[complete successfully]: You can start or stop the database server using:
    gs_ctl start|stop|restart -D $GAUSSHOME/data/single_node -Z single_node\n'
    else
        echo -e '[complete successfully]: You can start or stop the database server using:
    primary: gs_ctl start|stop|restart -D $GAUSSHOME/data/master -M primary
    standby: gs_ctl start|stop|restart -D $GAUSSHOME/data/slave -M standby\n'
    fi
}



function fn_load_demoDB()
{
    cd $shell_path
    gsql -d postgres -p $port -f school.sql
    gsql -d postgres -p $port -f finance.sql
}

function fn_check_demoDB()
{
    cd $shell_path
    if [ "`cat load.log | grep ROLLBACK`" != "" ]
    then
        return 1
    elif [ "`cat load.log | grep '\[GAUSS-[0-9]*\]'`" != "" ]
    then
        return 1
    elif [ "`cat load.log | grep ERROR`" != "" ]
    then
        return 1
    elif [ "`cat load.log | grep Unknown`" != "" ]
    then
        return 1
    fi
    return 0
}

function fn_install_demoDB()
{
    input=$1
    if [ "$input"X = X ]
    then
        read -p "Would you like to create a demo database (yes/no)? " input
    fi
    if [ $input == "yes" ]
    then
        fn_load_demoDB 1>load.log 2>&1
        fn_check_demoDB
    elif [ $input == "no" ]
    then
        return 2
    else
        read -p "Please type 'yes' or 'no': " input
        fn_install_demoDB $input
    fi
    return $?
}

function import_sql() {
    fn_install_demoDB
    local returnFlag=$?
    if [ $returnFlag -eq 0 ]
    then
        info "Load demoDB [school,finance] success."
        return 0
    elif [ $returnFlag -eq 1 ]
    then
        error "Load demoDB failed, you can check load.log for more details."
    else
        info "Input no, operation skip."
    fi
    return 1
}

function main() {
    get_param $@
    info "[step 1]: check parameter"
    check_param
    info "[step 2]: check install env and os setting"
    check_install_env
    check_os
    info "[step 3]: change_gausshome_owner"
    change_gausshome_owner
    info "[step 4]: set environment variables"
    set_environment
    if [ X$mode == X"single" ]
    then
        single_install
    else
        master_standby_install
    fi
    info "import sql file"
    import_sql
    if [ $? -eq 0 ]
    then
        end_help
        exit 0
    else
        exit 1
    fi
}
main $@

