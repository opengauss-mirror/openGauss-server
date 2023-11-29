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

GSDB_BIN=gaussdb
GSDB_BIN_FULL=${GAUSSHOME}/bin/gaussdb
DSS_BIN=dssserver
DSS_BIN_FULL=${GAUSSHOME}/bin/dssserver
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
    echo "[$time][DSS]$1" >> ${startdss_log} 2>&1
}

assert_empty()
{
    return
}

assert_nonempty()
{
    if [[ -z ${2} ]]
    then
        log "[SCRIPT]The ${1} parameter is empty."
        exit 1
    fi
}

program_pid()
{
    pid=`ps -f f -u \`whoami\` | grep -w ${1} | grep ${2} | grep -v grep | grep -v ${SCRIPT_NAME} | awk '{print $2}' | tail -1`
    echo ${pid}
}

program_pid2()
{
    pid=`ps -f f -u \`whoami\` | grep -w ${1} | grep -v grep | grep -v ${SCRIPT_NAME} | awk '{print $2}'`
    echo ${pid}
}

program_status()
{
    pid=`program_pid $1 $2`
    if [[ -z ${pid} ]]; then
        echo ""
        return
    fi

    pstatus_file="/proc/"${pid}"/status"
    cat ${pstatus_file} | while read line
    do
        if [[ "${line}" =~ ^State.* ]]; then
            echo ${line} | awk -F ":" '{print $2}' | awk -F " " '{print $1}'
            return
        fi
    done

    echo ""
}

kill_program()
{
    assert_nonempty 1 ${1}
    assert_nonempty 2 ${2}
    pid=`program_pid $1 $2`
    if [[ -z ${pid} ]]
    then
        log "[KILL]${1} is already dead."
        return
    fi

    kill -9 ${pid}
    if [ $? -ne 0 ]
    then
        log "[KILL]ERROR! ${1} with pid:${pid} is not killed..."
        exit 1
    fi
    for ((i=0; i < 30; i++))
    do
        ps -f -p "${pid}" | grep ${1}
        if [ $? -eq 0 ]
        then
            sleep 0.1
        else
            log "[KILL]SUCCESS!"
            return
        fi
    done

    log "[KILL]ERROR! ${1} with pid:${pid} is not killed..."
    exit 1
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
        typeset log_num=`find -L "${_log_dir}" -maxdepth 1 -type f -name "${_log_name}*" | wc -l`
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
            mv -f ${log_file} "${log_path}/${operation} - `date +%Y-%m-%d_%H%M%S`.log" 2>/dev/null
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
        chmod 600 $log_file
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
startdss_log=${DSS_HOME}/startdss.log

function check_dss_config()
{
    log "[START]Checking dss_inst.ini before start dss..."
    if [[ ! -e ${DSS_HOME}/cfg/dss_inst.ini ]]
    then
        log "[START]${DSS_HOME}/cfg/dss_inst.ini must exist"
        exit 1
    fi

    log "[START]Checking dss_vg_conf.ini before start dss..."
    if [[ ! -e ${DSS_HOME}/cfg/dss_vg_conf.ini ]]
    then
        log "[START]${DSS_HOME}/cfg/dss_vg_conf.ini must exist"
        exit 1
    fi

    LSNR_PATH=`cat ${DSS_HOME}/cfg/dss_inst.ini | sed s/[[:space:]]//g |grep -Eo "^LSNR_PATH=.*" | awk -F '=' '{print $2}'`
    if [[ -z ${LSNR_PATH} ]]
    then
        log "[START]can't find lsnr path. Aborting."
        exit 1
    fi
    CONN_PATH=UDS:${LSNR_PATH}/.dss_unix_d_socket
}

get_startdss_log()
{
    LOG_HOME=`cat ${DSS_HOME}/cfg/dss_inst.ini | sed s/[[:space:]]//g |grep -Eo "^LOG_HOME=.*" | awk -F '=' '{print $2}'`
    if [[ ! -z ${LOG_HOME} ]]
    then
        startdss_log=${LOG_HOME}/startdss.log
    fi

    if [[ -z ${DSS_HOME} ]]
    then
        startdss_log=/dev/null
    else
        touch_logfile $startdss_log
    fi
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

kill_dss_and_perctrl()
{
    pid=$(program_pid dssserver ${DSS_HOME})
    if [[ -z ${pid} ]]
    then
        log "[${1}]dssserver not exist."
    fi
    kill_program dssserver ${DSS_HOME}
    log "[${1}]Success to kill dssserver."

    pid=$(program_pid2 perctrl)
    for perctrl_pid in ${pid}
    do
        if [[ -z ${perctrl_pid} ]]
        then
            log "[${1}]perctrl not exist."
        fi
        kill_program ${perctrl_pid} perctrl
        log "[${1}]kill perctrl ${perctrl_pid} success."
    done
}

# 1st step: if database exists, kill it
# 2nd step: if dssserver no exists, start it
function Start()
{
    check_dss_config
    check_log_file ${DSS_HOME} $startdss_log startdss
    pid=`program_pid ${DSS_BIN_FULL} ${DSS_HOME}`
    if [[ ! -z ${pid} ]]
    then
        log "[START]dssserver already started in dir ${DSS_HOME}..."
    else
        log "[START]Starting dssserver..."
        pid=`program_pid ${GSDB_BIN_FULL} ${GSDB_HOME}`
        if [[ ! -z ${pid} ]]
        then
            log "[START]kill ${GSDB_BIN} before start dssserver"
            kill_program ${GSDB_BIN_FULL} ${GSDB_HOME}
        else
            log "[START]${GSDB_BIN} is offline in dir ${GSDB_HOME}..."
        fi
        log "[START]dssserver"
        ScandCheck
        nohup ${DSS_BIN_FULL} -D ${DSS_HOME} >> ${startdss_log} 2>&1  &
        log "[START]start dssserver in ${DSS_HOME} is starting."
    fi
}

# 1st Whether there is a dn stop tag file
# 2st stop tag file need to be created when there is no dn stop tag file
# 3st step: kill database
# 4nd step: stop dssserver by using dsscmd
# 5rd step: if fail to stop dssserver in 2nd step, then kill dssserver
function Stop()
{
    check_log_file ${DSS_HOME} $startdss_log startdss

    log "[STOP]stop ${GSDB_BIN}..."
    db_flag_file=instance_manual_start_$(expr $INSTANCE_ID + 6001)
    log "[STOP]db_flag_file=$db_flag_file"

    if [[ -f $GAUSSHOME/bin/$db_flag_file ]];
    then
        log "[STOP]$GAUSSHOME/bin/$db_flag_file is exist"
    else
        touch $GAUSSHOME/bin/$db_flag_file
    fi

    pid=$(program_pid ${GSDB_BIN_FULL} ${GSDB_HOME})
    if [[ ! -z ${pid} ]]
    then
        log "[STOP] kill ${GSDB_BIN} before stop dssserver"
        kill_program ${GSDB_BIN_FULL} ${GSDB_HOME}
    fi

    kill_dss_and_perctrl "STOP"
}

# 1st step: check dssserver if exists



function Check()
{
    dss_status=$(program_status dssserver ${DSS_HOME})
    if [[ -z ${dss_status} ]]
    then
        log "[CHECK]dssserver is offline."
        exit 1
    fi
    if [[ "${dss_status}" == "D" || "${dss_status}" == "T" || "${dss_status}" == "Z" ]]
    then
        log "[CHECK]dssserver is dead."
        exit 3
    fi

    pid=$(program_pid2 perctrl)
    for perctrl_pid in ${pid}
    do
        perctrl_status=$(program_status ${perctrl_pid} perctrl)
        if [[ "${perctrl_status}" == "D" || "${perctrl_status}" == "T" || "${perctrl_status}" == "Z" ]]
        then
            log "[CHECK]perctrl is dead."
            exit 3
        fi
    done
}
# 1st step: kill database
# 2nd step: stop dssserver by using dsscmd
# 3rd step: if fail to stop dssserver in 2nd step, then kill dssserver
function Clean()
{
    check_log_file ${DSS_HOME} $startdss_log startdss
    pid=$(program_pid ${GSDB_BIN_FULL} ${GSDB_HOME})
    if [[ ! -z ${pid} ]]
    then
        log "[CLEAN]kill ${GSDB_BIN} before kill dssserver"
        kill_program ${GSDB_BIN_FULL} ${GSDB_HOME}
    fi
    kill_dss_and_perctrl "CLEAN"
    dsscmd clean_vglock -D ${DSS_HOME} >> /dev/null 2>&1
}

function Reg()
{
    ScandCheck
    LOCAL_INSTANCE_ID=`awk '/INST_ID/{print}' ${DSS_HOME}/cfg/dss_inst.ini | awk -F= '{print $2}' | xargs`
    if [[ -z ${LOCAL_INSTANCE_ID} ]]
    then
        log "[REG]can't find inst id. Aborting."
        exit 1
    fi
    dsscmd reghl -D ${DSS_HOME} >> /dev/null 2>&1
    if [[ $? != 0 ]]
    then
        log "[REG]dsscmd reghl -D ${DSS_HOME} fail."
        exit 1
    fi
    log "[REG]register success."
}

function Unreg()
{
    LOCAL_INSTANCE_ID=`awk '/INST_ID/{print}' ${DSS_HOME}/cfg/dss_inst.ini | awk -F= '{print $2}' | xargs`
    if [[ -z ${LOCAL_INSTANCE_ID} ]]
    then
        log "[UNREG]can't find inst id. Aborting."
        exit 1
    fi
    if [[ ${LOCAL_INSTANCE_ID} == ${INSTANCE_ID} ]]
    then
        dsscmd unreghl -D ${DSS_HOME} >> /dev/null 2>&1
    else
        pid=$(program_pid dssserver ${DSS_HOME})
        if [[ -z ${pid} ]]
        then
            log "[UNREG]dssserver is not running."
            exit 1
        fi
        dsscmd kickh -i ${INSTANCE_ID} -D ${DSS_HOME} >> /dev/null 2>&1
    fi

    if [[ $? != 0 ]]
    then
        log "[UNREG]dsscmd kickh -i ${INSTANCE_ID} -D ${DSS_HOME} fail, or dsscmd unreghl -D ${DSS_HOME} fail."
        exit 1
    fi
    log "[UNREG]unregister ${INSTANCE_ID} success."
}

function Isreg()
{
    dsscmd inq_reg -i ${INSTANCE_ID} -D ${DSS_HOME} >> /dev/null 2>&1
    result=$?
    if [[ ${result} == 255 ]]
    then
        log "[ISREG]dsscmd inq_reg -i ${INSTANCE_ID} -D ${DSS_HOME} fail."
        exit -1
    fi
    if [[ ${result} != 2 ]]
    then
        log "[ISREG]result: ${result}"
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
        echo "[SCRIPT]Please confirm the input parameters."
        exit 1
    fi
}

Main