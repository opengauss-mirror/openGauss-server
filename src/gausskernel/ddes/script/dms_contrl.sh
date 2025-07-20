#!/bin/bash
export PATH=${GAUSSHOME}/bin:$PATH
export LD_LIBRARY_PATH=${GAUSSHOME}/lib:${GAUSSHOME}/add-ons:$LD_LIBRARY_PATH

curr_path=`dirname $(readlink -f $0)`
curr_filename=`basename $(readlink -f $0)`
os_user=`whoami`
file_user=`ls -l ${curr_path}"/${curr_filename}" | awk '{print $3}'`
dms_log=/dev/null

if [ ${file_user} != ${os_user} ]; then
    echo "Can't run ${curr_filename}, because it does not belong to the current user!"
    exit 1
fi 

GSDB_BIN=${GAUSSHOME}/bin/gaussdb
BIN_PATH=${GAUSSHOME}/bin
SCRIPT_NAME=$0

usage()
{
    echo "Usage: $0 [cmd] [gaussdb_id] [GSDB_HOME] [DSS_HOME]"
    echo "cmd:"
    echo "    -start: start ${GSDB_BIN}&delete dss_stop_flag_file"
    echo "    -stop: stop ${GSDB_BIN}"
    echo "    -check: check ${GSDB_BIN}"
    echo "    -clean: clean ${GSDB_BIN}"
    echo "    -reg: register gaussdb"
    echo "    -unreg: unregister gaussdb"
    echo "    -isreg: check whether gaussdb is registered"
    echo "gaussdb_id:"
    echo "    gaussdb id"
    echo "GSDB_HOME:"
    echo "    ${GSDB_BIN} data path"
    echo "DSS_HOME:"
    echo "    dssserver data path"
}

if [ $# -lt 3 ]
then
    echo  "parameter numbers not meet, num=$#."
    usage
    exit 1
fi

log()
{
    time=`date "+%Y-%m-%d %H:%M:%S"`
    echo "$time $1"
    echo "$time $1" >> $dms_log
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

function check_local_dn_disk()
{
    test_file=${GSDB_HOME}/disk_readwrite_test
    timeout 5 touch ${test_file}
    if [[ $? != 0 ]]
    then
        log "could not wrtie on local disk, ERRNO:$?."
        exit 3
    fi

    timeout 5 cat ${test_file}
    if [[ $? != 0 ]]
    then
        log "could not read on local disk, ERRNO:$?."
        rm -f ${test_file}
        exit 3
    fi

    rm -f ${test_file}
}

touch_logfile()
{
    log_file=$1
    if [ ! -f $log_file ]
    then
        touch $log_file
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
    if [ -f ${log_file} ];then
        log_file_size=$(ls -l ${log_file} | awk '{print $5}')
        if [ ${log_file_size} -ge ${MAX_LOG_SIZE} ];then
            mv -f ${log_file} "${log_path}/${operation}-`date +%Y-%m-%d_%H%M%S`.log" 2>/dev/null
            clear_script_log "${log_path}" "${operation}-" $MAX_LOG_BACKUP
        fi
    fi
    touch_logfile $log_file
    chmod 600 $log_file
}

assert_nonempty 1 ${1}
assert_nonempty 2 ${2}
assert_nonempty 3 ${3}

CMD=${1}
INSTANCE_ID=${2}
GSDB_HOME=${3}
CMD_PARAM=${4}

dms_log=${GSDB_HOME}/dms_control.log
check_log_file ${GSDB_HOME} $dms_log dms_control

if [ X${DSS_HOME} == X"" ]
then
    log "ERROR! DSS_HOME cannot be null!"
fi

# 1st step: if dss_flag_file exists, delete it
# 2nd step: if dssserver exists, start database
# 3nd step: if dssserver no exists, exit
function Start()
{
    db_start_log=${GSDB_HOME}/DBstart.log
    check_log_file ${GSDB_HOME} $db_start_log DBstart

    dss_flag_file=instance_manual_start_$(expr $INSTANCE_ID + 20001 - 6001)
    if [[ -f $GAUSSHOME/bin/$dss_flag_file ]];
    then
        rm $GAUSSHOME/bin/$dss_flag_file
    fi

    pid=`program_pid dssserver ${DSS_HOME}`
    if [[ -z ${pid} ]]
    then
        log "dssserver not exist in dir ${DSS_HOME}..."
        exit 6
    else
        log "Starting dn..."
        nohup ${GSDB_BIN} -D ${GSDB_HOME} ${CMD_PARAM} >> $db_start_log 2>&1 &
        sleep 3
        log "start dn in ${DSS_HOME} success."
    fi
}

# 1st step: kill database
function Stop()
{
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
}

# 1st step: check database if exists
function Check()
{
    pid=$(program_pid ${GSDB_BIN} ${GSDB_HOME})
    if [[ -z ${pid} ]]
    then
        log "check ${GSDB_BIN} in ${GSDB_HOME} fail."
        exit 1
    fi
    check_local_dn_disk   
}

# 1st step: kill database
function Clean()
{
    log "stop ${GSDB_BIN}..."
    ${BIN_PATH}/gs_ctl stop -D ${GSDB_HOME}

    if [ $? -eq 0 ]
    then
        log "${GSDB_BIN} stopped in dir ${GSDB_HOME}..."
    else
        log "Killing ${GSDB_BIN} if running..."
        kill_program ${GSDB_BIN} ${GSDB_HOME}
    fi
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
        # IO FENCE
        exit 0
    elif [ "$CMD" == "-unreg" ]; then
        # IO FENCE
        exit 0
    elif [ "$CMD" == "-isreg" ]; then
        # IO FENCE
        exit 11
    else
        echo "Please confirm the input parameters."
        exit 1
    fi
}

Main