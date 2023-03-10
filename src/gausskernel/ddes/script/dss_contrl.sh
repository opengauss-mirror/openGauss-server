#!/bin/bash
export PATH=${GAUSSHOME}/bin:$PATH
export LD_LIBRARY_PATH=${GAUSSHOME}/lib:${GAUSSHOME}/add-ons:$LD_LIBRARY_PATH

curr_path=`dirname $(readlink -f $0)`
curr_filename=`basename $(readlink -f $0)`
os_user=`whoami`
file_user=`ls -l ${curr_path}"/${curr_filename}" | awk '{print $3}'`

if [ ${file_user} != ${os_user} ]; then
    echo "Can't run ${curr_filename}, because it does not belong to the current user!"
    exit 1
fi 

GSDB_BIN=${GAUSSHOME}/bin/gaussdb
BIN_PATH=${GAUSSHOME}/bin
SCRIPT_NAME=$0

usage()
{
    echo "Usage: $0 [cmd] [dssserver_id] [DSS_HOME] [GSDB_HOME]"
    echo "cmd:"
    echo "    -start: start dssserver"
    echo "    -stop: stop dssserver&create dn_stop_flag_file"
    echo "    -check: check dssserver"
    echo "    -clean: clean dssserver&${GSDB_BIN}"
    echo "    -reg: register dssserver"
    echo "    -unreg: unregister dssserver"
    echo "    -isreg: check whether dssserver is registered"
    echo "dssserver_id:"
    echo "    dssserver id"
    echo "DSS_HOME:"
    echo "    dssserver data path"
    echo "GSDB_HOME:"
    echo "    ${GSDB_BIN} data path"
}

if [ $# -lt 4 ]
then
    echo  "parameter numbers not meet, num=$#."
    usage
    exit 1
fi

log()
{
    time=`date "+%Y-%m-%d %H:%M:%S"`
    echo "$time $1"
}

assert_empty()
{
    return
}

assert_nonempty()
{
    if [[ -z ${2} ]]
    then
        log "The ${1} parameter is empty."
        exit 1
    fi
}

program_pid()
{
    pid=`ps -f f -u \`whoami\` | grep ${1} | grep ${2} | grep -v grep | grep -v ${SCRIPT_NAME} | awk '{print $2}'`
    echo ${pid}
}

kill_program()
{
    assert_nonempty 1 ${1}
    assert_nonempty 2 ${2}
    pid=`program_pid $1 $2`
    if [[ -z ${pid} ]]
    then
        log "${1} is already dead."
        return
    fi

    kill -9 ${pid}
    sleep 3
    ps -f -p "${pid}" | grep ${1}
    if [ $? = 0 ]
    then
        log "ERROR! ${1} with pid:${pid} is not killed..."
        exit 0
    fi
}

check_dss_start()
{
    started=0
    for (( i=1; i<30; i++ ))
    do
        pid=`program_pid dssserver ${1}`
        if [[ ! -z ${pid} ]]
        then
            started=1
            break
        fi
        sleep 1
    done

    if [[ ${started} -eq 0 ]]
    then
        log "ERROR! start dssserver in dir ${1} failed"
        exit 1
    fi
}

function clear_script_log
{
    local _log_dir=$1
    local _log_name=$2
    local _max_log_backup=$3

    if [ -L ${_log_dir} ]; then
        typeset log_num=`find -L "${_log_dir}" -maxdepth 1 -type f -name "${_log_name}*" | wc -l`
        if [ ${log_num} -ge ${_max_log_backup} ];then
            find -L "${_log_dir}" -maxdepth 1 -type f -name "${_log_name}*" | xargs ls -t {} 2>/dev/null | tail -n $(expr ${log_num} - ${_max_log_backup}) | xargs -i rm -f {}
        fi
    else
        typeset log_num=$(find "${_log_dir}" -maxdepth 1 -type f -name "${_log_name}*" | wc -l)
        if [ ${log_num} -ge ${_max_log_backup} ];then
            find "${_log_dir}" -maxdepth 1 -type f -name "${_log_name}*" | xargs ls -t {} 2>/dev/null | tail -n $(expr ${log_num} - ${_max_log_backup}) | xargs -i rm -f {}
        fi
    fi
}

check_log_file()
{
    log_path=$1
    log_file=$2
    operation=$3
    # max log file size 16 * 1024 * 1024
    MAX_LOG_SIZE=16777216
    MAX_LOG_BACKUP=10
    log_file_size=$(ls -l ${log_file} |awk '{print $5}')
    if [ -f ${log_file} ];then
        if [ ${log_file_size} -ge ${MAX_LOG_SIZE} ];then
            mv -f ${log_file} "${log_path}/${operation}-`date +%Y-%m-%d_%H%M%S`.log" 2>/dev/null
            clear_script_log "${log_path}" "${operation}-" $MAX_LOG_BACKUP
        fi
    fi
}

touch_logfile()
{
    log_file=$1
    if [ ! -f $log_file ]
    then
        touch $log_file
    fi
}

assert_nonempty 1 ${1}
assert_nonempty 2 ${2}
assert_nonempty 3 ${3}
assert_nonempty 4 ${4}

CMD=${1}
INSTANCE_ID=${2}
export DSS_HOME=${3}
GSDB_HOME=${4}
CONN_PATH=UDS:${DSS_HOME}/.dss_unix_d_socket

function check_dss_config()
{
    log "Checking dss_inst.ini before start dss..."
    if [[ ! -e ${DSS_HOME}/cfg/dss_inst.ini ]]
    then
        log "${DSS_HOME}/cfg/dss_inst.ini must exist"
        exit 1
    fi

    log "Checking dss_vg_conf.ini before start dss..."
    if [[ ! -e ${DSS_HOME}/cfg/dss_vg_conf.ini ]]
    then
        log "${DSS_HOME}/cfg/dss_vg_conf.ini must exist"
        exit 1
    fi

    LSNR_PATH=`awk '/LSNR_PATH/{print}' ${DSS_HOME}/cfg/dss_inst.ini | awk -F= '{print $2}' | xargs`
    if [[ -z ${LSNR_PATH} ]]
    then
        log "can't find lsnr path. Aborting."
        exit 1
    fi
    CONN_PATH=UDS:${LSNR_PATH}/.dss_unix_d_socket
}

function ScandCheck()
{
    groups=`groups`
    echo $groups
    array=(${groups// / })
    for var in ${array[@]}
    do
        echo $var
        nohup dsscmd scandisk -t block -p /dev/sd -u $os_user -g $var >> /dev/null 2>&1
        if [[ $? != 0 ]]
        then
            log "[SCAND]dsscmd scandisk -t block -p /dev/sd -u $os_user -g $var fail."
            exit 1
        fi
        log "[SCAND]dsscmd scandisk."
    done
}

# 1st step: if database exists, kill it
# 2nd step: if dssserver no exists, start it
function Start()
{
    check_dss_config

    startdss_log=${DSS_HOME}/startdss.log
    db_start_log=${GSDB_HOME}/DBstart.log
    check_log_file ${DSS_HOME} $startdss_log startdss
    check_log_file ${GSDB_HOME} $db_start_log DBstart
    if [[ -z "${DSS_HOME}" ]]
    then
        startdss_log=/dev/null
    else
        touch_logfile $startdss_log
        chmod 600 $startdss_log
    fi

    if [[ -z "${GSDB_HOME}" ]]
    then
        db_start_log=/dev/null
    else
        touch_logfile $db_start_log
        chmod 600 $db_start_log
    fi

    pid=`program_pid dssserver ${DSS_HOME}`
    if [[ ! -z ${pid} ]]
    then
        log "dssserver already started in dir ${DSS_HOME}..."
    else
        log "Starting dssserver..."
        pid=`program_pid ${GSDB_BIN} ${GSDB_HOME}`
        if [[ ! -z ${pid} ]]
        then
            kill_program ${GSDB_BIN} ${GSDB_HOME}
        else
            log "${GSDB_BIN} is offline in dir ${GSDB_HOME}..."
        fi
        ScandCheck
        nohup dssserver -D ${DSS_HOME} >> ${startdss_log} 2>&1  &
        check_dss_start ${DSS_HOME}
        log "start dss in ${DSS_HOME} success."
    fi
}

# 1st Whether there is a dn stop tag file
# 2st stop tag file need to be created when there is no dn stop tag file
# 3st step: kill database
# 4nd step: stop dssserver by using dsscmd
# 5rd step: if fail to stop dssserver in 2nd step, then kill dssserver
function Stop()
{
    log "stop ${GSDB_BIN}..."
    db_flag_file=instance_manual_start_$(expr $INSTANCE_ID + 6001)
    echo "db_flag_file=$db_flag_file"

    if [[ -f $GAUSSHOME/bin/$db_flag_file ]];
    then
        log "$GAUSSHOME/bin/$db_flag_file is exist"
    else
        touch $GAUSSHOME/bin/$db_flag_file
    fi

    pid=$(program_pid ${GSDB_BIN} ${GSDB_HOME})
    if [[ -z ${pid} ]]
    then
        log "stop dssserver if running..."
        nohup dsscmd stopdss -U ${CONN_PATH} >> /dev/null 2>&1
        sleep 2

        pid=`program_pid dssserver ${DSS_HOME}`
        if [[ -z ${pid} ]]
        then
            log "dssserver stopped in dir ${DSS_HOME}..."
            exit 0
        fi
        log "Killing dssserver if running..."
        kill_program dssserver ${DSS_HOME}
    else
        log "stop ${GSDB_BIN}..."
        ${BIN_PATH}/gs_ctl stop -D ${GSDB_HOME}
        sleep 5

        pid=`program_pid ${GSDB_BIN} ${GSDB_HOME}`
        if [[ -z ${pid} ]]
        then
            log "${GSDB_BIN} stopped in dir ${GSDB_HOME}..."
        else
            log "Killing ${GSDB_BIN} if running..."
            kill_program ${GSDB_BIN} ${GSDB_HOME}
        fi

        log "stop dssserver if running..."
        nohup dsscmd stopdss -U ${CONN_PATH} >> /dev/null 2>&1
        sleep 2
        pid=`program_pid dssserver ${DSS_HOME}`
        if [[ -z ${pid} ]]
        then
            log "dssserver stopped in dir ${DSS_HOME}..."
            exit 0
        fi
        log "Killing dssserver if running..."
        kill_program dssserver ${DSS_HOME}
    fi
}

# 1st step: check dssserver if exists
function Check()
{
    pid=$(program_pid dssserver ${DSS_HOME})
    if [[ -z ${pid} ]]
    then
        log "check dssserver in ${DSS_HOME} fail."
        exit 1
    fi

    log "check dss in ${DSS_HOME} success."
}

# 1st step: kill database
# 2nd step: stop dssserver by using dsscmd
# 3rd step: if fail to stop dssserver in 2nd step, then kill dssserver
function Clean()
{
    log "stop ${GSDB_BIN}..."
    kill_program ${GSDB_BIN} ${GSDB_HOME}
    sleep 3

    log "stop dssserver if running..."
    nohup dsscmd stopdss -U ${CONN_PATH} >> /dev/null 2>&1
    sleep 2

    pid=`program_pid dssserver ${DSS_HOME}`
    if [[ -z ${pid} ]]
    then
        log "dssserver stopped in dir ${DSS_HOME}..."
        exit 0
    fi
    log "Killing dssserver if running..."
    kill_program dssserver ${DSS_HOME}
    dsscmd clean_vglock -D ${DSS_HOME} >> /dev/null 2>&1
}

function Reg()
{
    ScandCheck
    LOCAL_INSTANCE_ID=`awk '/INST_ID/{print}' ${DSS_HOME}/cfg/dss_inst.ini | awk -F= '{print $2}' | xargs`
    if [[ -z ${LOCAL_INSTANCE_ID} ]]
    then
        log "can't find inst id. Aborting."
        exit 1
    fi
    dsscmd reghl -D ${DSS_HOME} >> /dev/null 2>&1
    if [[ $? != 0 ]]
    then
        log "dsscmd reghl -D ${DSS_HOME} fail."
        exit 1
    fi
    log "register success."
}

function Unreg()
{
    LOCAL_INSTANCE_ID=`awk '/INST_ID/{print}' ${DSS_HOME}/cfg/dss_inst.ini | awk -F= '{print $2}' | xargs`
    if [[ -z ${LOCAL_INSTANCE_ID} ]]
    then
        log "can't find inst id. Aborting."
        exit 1
    fi
    if [[ ${LOCAL_INSTANCE_ID} == ${INSTANCE_ID} ]]
    then
        dsscmd unreghl -D ${DSS_HOME} >> /dev/null 2>&1
    else
        pid=$(program_pid dssserver ${DSS_HOME})
        if [[ -z ${pid} ]]
        then
            log "dssserver is not running."
            exit 1
        fi
        dsscmd kickh -i ${INSTANCE_ID} -D ${DSS_HOME} >> /dev/null 2>&1
    fi

    if [[ $? != 0 ]]
    then
        log "dsscmd kickh -i ${INSTANCE_ID} -D ${DSS_HOME} fail, or dsscmd unreghl -D ${DSS_HOME} fail."
        exit 1
    fi
    log "unregister ${INSTANCE_ID} success."
}

function Isreg()
{
    dsscmd inq_reg -i ${INSTANCE_ID} -D ${DSS_HOME} >> /dev/null 2>&1
    result=$?
    if [[ ${result} == 255 ]]
    then
        log "dsscmd inq_reg -i ${INSTANCE_ID} -D ${DSS_HOME} fail."
        exit -1
    fi
    exit ${result}
}

function Main()
{
    if [ "$CMD" == "-start" ]; then
        Start
        exit 0
    elif [ "$CMD" == "-stop" ]; then
        Stop
        exit 0
    elif [ "$CMD" == "-check" ]; then
        Check
        exit 0
    elif [ "$CMD" == "-clean" ]; then
        Clean
        exit 0
    elif [ "$CMD" == "-reg" ]; then
        Reg
        exit 0
    elif [ "$CMD" == "-unreg" ]; then
        Unreg
        exit 0
    elif [ "$CMD" == "-isreg" ]; then
        Isreg
        exit 0
    else
        echo "Please confirm the input parameters."
        exit 1
    fi
}

Main