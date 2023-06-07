#!/bin/sh
##need set
IPS=$1
ENVFILE_FILE=$2
TPCC_PATH=/data3/script/benchmarksql/run
TPCC_P=("0-primary.gs" "1-primary.gs" "2-primary.gs")
TPCC_S=("0-standby.gs" "1-standby.gs" "2-standby.gs")

##no need set
CURPATH=$(dirname $(readlink -f $0))
OUTPUT_LOG=${CURPATH}/results/run_cm.log
IPS_ARR=()
NODE_ID=()
ID_ARR=()
DB_ROLE=()
declare DB_CONFIG_HOME
declare REFORMER_LOCK_STR
declare PGPORT
declare NODE_NUM
declare cluster_ready
PRIMARY_ID=-1
PROMOTE_ID=-1
DELETED_ID=-1
REFORMER_ID=-1

function print
{
    echo `date "+[%Y-%m-%d %H:%M:%S] $1."`
    echo `date "+[%Y-%m-%d %H:%M:%S] $1."` >> ${OUTPUT_LOG}
}

reset_global_variable()
{
    PRIMARY_ID=-1
    PROMOTE_ID=-1
    DELETED_ID=-1
    REFORMER_ID=-1
}


function get_port()
{
    res=`cm_ctl query -Cvd`
    for out in $res
    do
        result=$(echo $out | grep -w dn1)
        if [[ $result != '' ]]; then
        DB_CONFIG_HOME=$result
        break
    fi

    done
    config_file=${DB_CONFIG_HOME}/postgresql.conf
    PGPORT=`cat $config_file | grep -w  "port =" | awk '{print $3}'` 
}

function get_reformer_lock_str()
{
    res=`cm_ctl ddb --get / --prefix`
    for out in $res
    do
        result=$(echo $out | grep -w dms_reformer_lock)
        if [[ $result != '' ]]; then
            REFORMER_LOCK_STR=$result
            break
        fi

    done
}

function init_global_variable()
{
    echo "" > $OUTPUT_LOG
    source $ENVFILE_FILE

    i=0
    for ip in $IPS
    do
        IPS_ARR[$i]=$ip
        i=`expr $i + 1`
    done
    NODE_NUM=$i

    DMS_ID_BASE=6001
    for(( i=0;i<${#IPS_ARR[@]};i++ ))
    do
        NODE_ID[i]=`expr $i + 1`
        ID_ARR[i]=`expr $i + $DMS_ID_BASE`
        DB_ROLE[i]='unknown'
    done

    get_reformer_lock_str
    get_port

    rm -rf ${CURPATH}/results
    mkdir -p ${CURPATH}/results
    touch ${CURPATH}/results/run_cm.log
    print_global_variable
}

function print_global_variable()
{
    print "ENVFILE_FILE:$ENVFILE_FILE"

    for(( i=0;i<${#IPS_ARR[@]};i++ ))
    do
        print "IPS[$i]:${IPS_ARR[$i]}"
    done

    for(( i=0;i<${#IPS_ARR[@]};i++ ))
    do
        print "NODE_ID[$i]:${NODE_ID[$i]}"
    done

    print "REFORMER_LOCK_STR:$REFORMER_LOCK_STR"
    print "PGPORT:$PGPORT"

}

function scan_role()
{
    for(( i=0;i<${#DB_ROLE[@]};i++ ))
    do
        echo ${DB_ROLE[$i]}
    done
}

function get_reformer()
{
    DMS_ID=`cm_ctl ddb --get $REFORMER_LOCK_STR | grep 60 | awk '{print $2}'`
    if [[ $DMS_ID == '' ]];then
        echo "get reformer error"
    fi
    REFORMER_ID=`expr $DMS_ID - 6001`
    print "NODE_ID:$REFORMER_ID got reformer lock"
}

function kick()
{
    #cmd=`ssh $USER@${IPS_ARR[$PRIMARY_ID]} "source $ENVFILE_FILE && cm_ctl stop -n ${NODE_ID[$PRIMARY_ID]} -I ${ID_ARR[$PRIMARY_ID]}"`
    NODE_IDX=$1
    cmd=`cm_ctl stop -n ${NODE_ID[NODE_IDX]} -I ${ID_ARR[NODE_IDX]}`
    print "cm_ctl stop -n ${NODE_ID[NODE_IDX]} -I ${ID_ARR[NODE_IDX]}"
    DELETED_ID=$NODE_IDX
}

function db_start()
{
    NODE_IDX=$1
    #cmd=`ssh $USER@${IPS_ARR[$PRIMARY_ID]} "source $ENVFILE_FILE && cm_ctl start -n ${NODE_ID[$NODE_IDX]}"`
    cmd=`cm_ctl start -n ${NODE_ID[$NODE_IDX]}`
}

function db_kill()
{
    DB_CONFIG_HOME=$1
    ps ux | grep gaussdb | grep ${DB_CONFIG_HOME} | grep -v grep | awk '{print $2}' |xargs kill -9 > /dev/null 2>&1
    pid=`ps ux | grep gaussdb | grep ${DB_CONFIG_HOME} | grep -v grep`
    while [[ ! -z $pid ]]; do
        sleep 1
        pid=`ps ux | grep gaussdb | grep ${DB_CONFIG_HOME} | grep -v grep`
    done
    sleep 1
}

function ssh_db_kill()
{
    print "kill node $1"
    NODE_IDX=$1
    ssh $USER@${IPS_ARR[$NODE_IDX]}  "$(typeset -f db_kill);db_kill $DB_CONFIG_HOME"
}


function create_user()
{
    gsql -d postgres -p ${PGPORT} -c "create user hatest with password \"Huawei@123\"; grant all privileges to hatest;"
}

function db_role_get_allnode()
{
    for(( i=0;i<$NODE_NUM;i++ ))
    do
        role=`timeout 10 gsql -d postgres -p ${PGPORT} -U hatest -W "Huawei@123" -c "select local_role from pg_stat_get_stream_replications();" -h ${IPS_ARR[$i]} | grep -E 'Primary|Standby' | sed s/[[:space:]]//g`
        echo "role: $role"
        if [ $role = "" ]; then
            cluster_ready=0
            return
        fi

        if [ $role = "Primary" ]; then
            PRIMARY_ID=$i
        fi
        DB_ROLE[$i]=$role
        echo "DB_ROLE $i:${DB_ROLE[$i]}"
    done
    cluster_ready=1
}

function db_wait_all_node_ready()
{
    while true
    do
        db_role_get_allnode
        if (( $cluster_ready == 1)); then
            break;
        fi
    done
}

function db_role_get()
{
    NODE_IDX=$1
    DB_ROLE[$1]=`timeout 5 gsql -d postgres -p ${PGPORT} -U hatest -W "Huawei@123" -c "select local_role from pg_stat_get_stream_replications();" -h ${IPS_ARR[$NODE_IDX]} | grep -E 'Primary|Standby' | sed s/[[:space:]]//g`
}

function db_role_check()
{
    role_result="${DB_ROLE[$1]}"
    role_expect="$2"

    if [ "$role_result" != "$role_expect" ]; then
        print "node $1 role check fail, should be $role_expect, but $role_result"
        exit 1
    fi
}

function function_enter()
{
    echo ""
    print "===================TestCaseBegin:${FUNCNAME[1]}====================="
    db_role_get 0
    db_role_get 1
    db_role_check 0 Primary
    db_role_check 1 Standby
}

function function_leave()
{
    db_role_get 0
    db_role_get 1
    db_role_check 0 Primary
    db_role_check 1 Standby
    print "===================TestCaseEnd:${FUNCNAME[1]}====================="
}


function function_enter_failover()
{
    echo ""
    print "===================TestCaseBegin:${FUNCNAME[1]}====================="
}

function function_leave_failover()
{
    print "===================TestCaseEnd:${FUNCNAME[1]}====================="
}


function db_start_wait()
{
    wait_time=0
    NODE_IDX=$1
    db_role_get $NODE_IDX
    cur_mode=${DB_ROLE[$NODE_IDX]}
    print "wait node $NODE_IDX start"
    while [[ $cur_mode != "Primary" ]] && [[ $cur_mode != "Standby" ]]; do
        sleep 5
        db_role_get $NODE_IDX
        cur_mode=${DB_ROLE[$NODE_IDX]}
        wait_time=`expr ${wait_time} + 5`
        temp=`expr ${wait_time} % 30`
        if [ ${temp} -eq 0 ]; then
            print "wait node $NODE_IDX start ${wait_time}s"
        fi
        if [ ${wait_time} -gt 600 ]; then
            print "wait node $NODE_IDX start ${wait_time}s, db start fail"
            exit 1
        fi
    done
    sleep 5
}

function restart_standby() {
    function_enter
    ssh_db_kill 1
    db_start_wait 1
    function_leave
}

function restart_primary() {
    function_enter
    ssh_db_kill 0
    db_start_wait 0
    function_leave
}

function switchover()
{
    db_start_wait 0
    db_start_wait 1
    cm_ctl switchover -n ${NODE_ID[$1]} -D $DB_CONFIG_HOME
    sleep 5
    db_start_wait 0
    db_start_wait 1
}

function switchover_4times()
{
    function_enter
    switchover 1
    db_role_get 0
    db_role_get 1
    db_role_check 0 Standby
    db_role_check 1 Primary
    switchover 0
    db_role_get 0
    db_role_get 1
    db_role_check 0 Primary
    db_role_check 1 Standby
    switchover 1
    db_role_get 0
    db_role_get 1
    db_role_check 0 Standby
    db_role_check 1 Primary
    switchover 0
    db_role_get 0
    db_role_get 1
    db_role_check 0 Primary
    db_role_check 1 Standby
    function_leave
}

function failover_alive() {
    function_enter
    get_reformer
    PRIMARY_ID=$REFORMER_ID
    db_start_wait $PRIMARY_ID
    db_role_check $PRIMARY_ID Primary
    kick $PRIMARY_ID
    while true
    do
        get_reformer
        if [ $REFORMER_ID != $DELETED_ID ]; then
            PROMOTE_ID=$REFORMER_ID
            break;
        fi
        print "need wait anthor node get lock"
        sleep 5
    done
    db_start_wait $PROMOTE_ID
    db_role_check $PROMOTE_ID Primary
    db_start $DELETED_ID
    db_start_wait $DELETED_ID
    db_role_check $DELETED_ID Standby
    switchover 0
    function_leave
}

function twonode_testcase()
{
    restart_standby
    restart_primary
    failover_alive
    switchover_4times
}

function create_user_tpcc()
{
    gsql -d postgres -p ${PGPORT} -c "create user tpcc with password \"Huawei@123\"; grant all privileges to tpcc;"
}

function tpcc_init() {
    cd ${TPCC_PATH}
    create_user_tpcc
    sh runDatabaseBuild.sh 0-primary.gs
    print "sh runDatabaseBuild.sh 0"
}

function runbenchmark()
{
    is_primary=$1
    NODE_IDX=$2
    cd ${TPCC_PATH}
    if [[ $is_primary = "Primary" ]]; then
        print "runbenchmark Primary $NODE_IDX"
        sh runBenchmark.sh ${TPCC_P[$NODE_IDX]} > /dev/null 2>&1 &
    else
        print "runbenchmark Standby $NODE_IDX"
        sh runBenchmark.sh ${TPCC_S[$NODE_IDX]} > /dev/null 2>&1 &
    fi
}

function restart_primary_tpcc() {

    function_enter
    runbenchmark Primary 0
    runbenchmark Standby 1
    sleep 60
    ssh_db_kill 0
    db_start_wait 0
    function_leave

}

function failover_alive_tpcc() {
    function_enter_failover
    get_reformer
    db_wait_all_node_ready
    for(( i=0;i<${#DB_ROLE[@]};i++ ))
    do
        if [ ${DB_ROLE[$i]} = 'Primary' ]; then
            runbenchmark Primary $i
        else
            runbenchmark Standby $i
        fi
    done

    sleep 60
    kick $PRIMARY_ID
    while true
    do
        get_reformer
        if [ $REFORMER_ID != $DELETED_ID ]; then
            PROMOTE_ID=$REFORMER_ID
            break;
        fi
        print "need wait anthor node get lock"
        sleep 10
    done
    db_start_wait $PROMOTE_ID
    db_role_check $PROMOTE_ID Primary
    db_start $DELETED_ID
    db_start_wait $DELETED_ID
    db_role_check $DELETED_ID Standby
    function_leave_failover
}

function muti_execute()
{
    times=$1
    for(( time=0;time<$times;time++ ))
    do
        print "times:$time"
        ## run function
        $2
    done
}


init_global_variable
db_wait_all_node_ready
switchover 0
create_user
twonode_testcase
#tpcc_init
#muti_execute 10 restart_primary_tpcc
#muti_execute 10 failover_alive_tpcc
