CURPATH=$(dirname $(readlink -f $0))
echo "check env var"
if [ ${GAUSSHOME} ] && [ -d ${GAUSSHOME}/bin ];then
    echo "GAUSSHOME: ${GAUSSHOME}"
else
    echo "GAUSSHOME NOT EXIST"
    exit 1;
fi

BIN_PATH=${GAUSSHOME}/bin
LIB_PATH=${GAUSSHOME}/lib

declare -a DB_ROLE
declare -a DB_STATUS
declare -a DB_HOME

PGPORT=(6600 6700)
STANDBY_PGPORT=(9600 9700)
SS_DATA=${HOME}/ss_hatest
SS_DATA_STANDBY=${HOME}/ss_hatest1
nodedata_cfg="0:127.0.0.1:6611,1:127.0.0.1:6711"
standby_nodedata_cfg="0:127.0.0.1:9611,1:127.0.0.1:9711"
export CM_CONFIG_PATH=${CURPATH}/cm_config.ini
export TPCC_PATH=~/benchmarksql/run
TPCC_P=("0-primary.gs" "1-primary.gs" "2-primary.gs")
TPCC_S=("0-standby.gs" "1-standby.gs" "2-standby.gs")
GAUSSLOG_TMP=("${SS_DATA}/gausslog0" "${SS_DATA}/gausslog1" "${SS_DATA}/gausslog2")
declare -a NEXT_REFORMER_ID

function ha_test_init()
{
    rm -rf ${CURPATH}/results
    mkdir -p ${CURPATH}/results
    touch ${CURPATH}/results/run.log

    echo "CURPATH:$CURPATH"
    echo "REFORMER_ID = 0" > $CM_CONFIG_PATH
    echo "BITMAP_ONLINE = 3" >> $CM_CONFIG_PATH

    DB_HOME[0]=$SS_DATA/dn0
    DB_HOME[1]=$SS_DATA/dn1
    echo "DB_HOME[0]=${DB_HOME[0]}"
    echo "DB_HOME[1]=${DB_HOME[1]}"
}

function print
{
    echo `date "+[%Y-%m-%d %H:%M:%S] $1."`
    echo `date "+[%Y-%m-%d %H:%M:%S] $1."` >> ${CURPATH}/results/run.log
}

function db_role_get
{
    DB_ROLE[$1]=`$BIN_PATH/gsql -d postgres -p ${PGPORT[$1]} -c "select local_role from pg_stat_get_stream_replications();"| grep -E 'Primary|Standby'|sed s/[[:space:]]//g`
}

function db_role_check
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

function db_shutdown
{
    print "shutdown node $1"
    ${BIN_PATH}/gsql -d postgres -p ${PGPORT[$1]} -c "shutdown;"

    pid=`ps ux|grep gaussdb|grep ${DB_HOME[$1]}|grep -v grep`
    while [[ ! -z $pid ]]; do
        sleep 1
        pid=`ps ux|grep gaussdb|grep ${DB_HOME[$1]}|grep -v grep`
    done
    sleep 1
}

function db_kill
{
    print "kill node $1"
    ps ux | grep gaussdb | grep ${DB_HOME[$1]} | grep -v grep | awk '{print $2}' | xargs kill -9 > /dev/null 2>&1
    pid=`ps ux | grep gaussdb | grep ${DB_HOME[$1]} | grep -v grep`
    while [[ ! -z $pid ]]; do
        sleep 1
        pid=`ps ux | grep gaussdb | grep ${DB_HOME[$1]} | grep -v grep`
    done
    sleep 1
}

function db_start
{
    print "node $1 start"
    export GAUSSLOG=${GAUSSLOG_TMP[$1]}
    print "export GAUSSLOG=${GAUSSLOG_TMP[$1]}"
    ${BIN_PATH}/gaussdb -D ${DB_HOME[$1]} > /dev/null 2>&1 &
    print "${BIN_PATH}/gaussdb -D ${DB_HOME[$1]} > /dev/null 2>&1 &"
}

function db_start_wait
{
    wait_time=0
    cur_mode=`timeout 5 $BIN_PATH/gsql -d postgres -p ${PGPORT[$1]} -c "select local_role from pg_stat_get_stream_replications();"| grep -E 'Primary|Standby'|sed s/[[:space:]]//g` >/dev/null 2>&1
    print "wait node $1 start"
    while [[ $cur_mode != "Primary" ]] && [[ $cur_mode != "Standby" ]]; do
        sleep 5
        cur_mode=`timeout 5 $BIN_PATH/gsql -d postgres -p ${PGPORT[$1]} -c "select local_role from pg_stat_get_stream_replications();"| grep -E 'Primary|Standby'|sed s/[[:space:]]//g` >/dev/null 2>&1
        wait_time=`expr ${wait_time} + 5`
        temp=`expr ${wait_time} % 30`
        if [ ${temp} -eq 0 ]; then
            print "wait node $1 start ${wait_time}s"
        fi
        if [ ${wait_time} -gt 300 ]; then
            print "wait node $1 start ${wait_time}s, db start failed"
            exit 1
        fi
    done
    sleep 5
}

function switchover()
{
    db_start_wait 0
    db_start_wait 1
    $BIN_PATH/gs_ctl switchover -D ${DB_HOME[$1]}
    sleep 5
    db_start_wait 0
    db_start_wait 1
}

function restart_primary
{
    function_enter
    db_shutdown 0
    db_start 0
    db_start_wait 0
    function_leave
}

function restart_standby
{
    function_enter
    db_shutdown 1
    db_start 1
    db_start_wait 1
    function_leave
}

function restart_all
{
    function_enter
    db_shutdown 1
    db_shutdown 0
    db_start 0
    db_start 1
    db_start_wait 0
    db_start_wait 1
    function_leave
}

function delete_add_standby
{
    function_enter
    db_kill 1
    sh ${CURPATH}/cm_ctl.sh set BITMAP_ONLINE 1
    sleep 20
    db_role_get 0
    db_role_check 0 Primary
    sh ${CURPATH}/cm_ctl.sh set BITMAP_ONLINE 3
    db_start 1
    db_start_wait 1
    function_leave
}

function delete_add_primary
{
    function_enter
    db_kill 0
    sh ${CURPATH}/cm_ctl.sh set BITMAP_ONLINE 2
    sleep 5
    sh ${CURPATH}/cm_ctl.sh set BITMAP_ONLINE 3
    db_start 0
    db_start_wait 0
    function_leave
}

function failover_alive
{
    function_enter
    db_kill 0
    sh ${CURPATH}/cm_ctl.sh set BITMAP_ONLINE 2
    sh ${CURPATH}/cm_ctl.sh set REFORMER_ID 1
    sleep 30
    db_start_wait 1
    db_role_get 1
    db_role_check 1 Primary

    sh ${CURPATH}/cm_ctl.sh set BITMAP_ONLINE 3
    db_start 0
    sleep 10
    db_start_wait 0
    db_role_get 0
    db_role_check 0 Standby
    switchover 0
    function_leave
}

function failover_restart
{
    function_enter
    db_kill 0
    db_kill 1
    sh ${CURPATH}/cm_ctl.sh set BITMAP_ONLINE 2
    sh ${CURPATH}/cm_ctl.sh set REFORMER_ID 1
    db_start 1
    sleep 20
    db_start_wait 1
    db_role_get 1
    db_role_check 1 Primary

    sh ${CURPATH}/cm_ctl.sh set BITMAP_ONLINE 3
    db_start 0
    db_start_wait 0
    db_role_get 0
    db_role_check 0 Standby
    switchover 0
    function_leave
}

function switchover_4times
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

function full_clean
{
    function_enter
    db_kill 1
    sh ${CURPATH}/cm_ctl.sh set BITMAP_ONLINE 1
    db_start_wait 0
    db_role_get 0
    db_role_check 0 Primary
    print 'cluster now has one node'

    sh ${CURPATH}/cm_ctl.sh set BITMAP_ONLINE 3
    db_start 1
    sleep 2
    db_kill 1
    db_start 1
    sleep 2
    db_kill 1

    print 'expect perform full_clean reform'
    sh ${CURPATH}/cm_ctl.sh set BITMAP_ONLINE 1
    db_start_wait 0
    db_role_get 0 
    db_role_check 0 Primary

    sh ${CURPATH}/cm_ctl.sh set BITMAP_ONLINE 3
    db_start 1
    db_start_wait 1
    function_leave
}

function change_pg_log
{
    dn_home=$1
    pg_log_path=$2
    echo "log_directory = '${pg_log_path}'" >> ${dn_home}/postgresql.conf
}

function deploy_two_inst
{
    source ${CURPATH}/build_ss_database_common.sh
    kill_gaussdb
    clean_database_env ${SS_DATA}

    sh ${CURPATH}/conf_start_dss_inst.sh 2 ${SS_DATA} ${SS_DATA}/dss_disk
    init_gaussdb 0 ${SS_DATA}/dss_home0 $SS_DATA $nodedata_cfg
    init_gaussdb 1 ${SS_DATA}/dss_home1 $SS_DATA $nodedata_cfg

    set_gaussdb_port ${SS_DATA}/dn0 ${PGPORT[0]}
    set_gaussdb_port ${SS_DATA}/dn1 ${PGPORT[1]}

    assign_hatest_parameter ${SS_DATA}/dn0 ${SS_DATA}/dn1
    ha_test_init
    change_pg_log ${SS_DATA}/dn0 ${GAUSSLOG_TMP[0]}/pg_log/dn_6001
    change_pg_log ${SS_DATA}/dn1 ${GAUSSLOG_TMP[1]}/pg_log/dn_6002

    export DSS_HOME=${SS_DATA}/dss_home0
    db_start 0 
    export DSS_HOME=${SS_DATA}/dss_home1
    db_start 1
    db_start_wait 0
    db_start_wait 1
}

function deploy_dual_cluster
{   
    echo "deploy master cluster"
    source ${CURPATH}/build_ss_database_common.sh
    kill_gaussdb
    clean_database_env ${SS_DATA}
    DORADO_SHARED_DISK=${HOME}/ss_hatest/dorado_shared_disk

    sh ${CURPATH}/conf_start_dss_inst.sh 2 ${SS_DATA} ${SS_DATA}/dss_disk 
    init_gaussdb 0 ${SS_DATA}/dss_home0 $SS_DATA $nodedata_cfg $DORADO_SHARED_DISK
    init_gaussdb 1 ${SS_DATA}/dss_home1 $SS_DATA $nodedata_cfg

    set_gaussdb_port ${SS_DATA}/dn0 ${PGPORT[0]}
    set_gaussdb_port ${SS_DATA}/dn1 ${PGPORT[1]} 

    assign_hatest_parameter ${SS_DATA}/dn0 ${SS_DATA}/dn1
    assign_dorado_parameter ${SS_DATA} ${SS_DATA}/dn0

    export DSS_HOME=${SS_DATA}/dss_home0
    start_gaussdb ${SS_DATA}/dn0
    export DSS_HOME=${SS_DATA}/dss_home1
    start_gaussdb ${SS_DATA}/dn1

    echo "deploy standby cluster"
    clean_database_env ${SS_DATA_STANDBY}
    sh ${CURPATH}/conf_start_dss_inst.sh 2 ${SS_DATA_STANDBY} ${SS_DATA_STANDBY}/dss_disk standby_cluster
    init_gaussdb 0 ${SS_DATA_STANDBY}/dss_home0 $SS_DATA_STANDBY $standby_nodedata_cfg
    init_gaussdb 1 ${SS_DATA_STANDBY}/dss_home1 $SS_DATA_STANDBY $standby_nodedata_cfg

    set_gaussdb_port ${SS_DATA_STANDBY}/dn0 ${STANDBY_PGPORT[0]}
    set_gaussdb_port ${SS_DATA_STANDBY}/dn1 ${STANDBY_PGPORT[1]} 
    
    assign_hatest_parameter ${SS_DATA_STANDBY}/dn0 ${SS_DATA_STANDBY}/dn1

    cp ${CURPATH}/cm_config.ini ${CURPATH}/cm_config_standby.ini
    export CM_CONFIG_PATH=${CURPATH}/cm_config_standby.ini
    export DSS_HOME=${SS_DATA_STANDBY}/dss_home0
    start_gaussdb ${SS_DATA_STANDBY}/dn0
    export DSS_HOME=${SS_DATA_STANDBY}/dss_home1
    start_gaussdb ${SS_DATA_STANDBY}/dn1

}

function testcase()
{
    restart_primary
    restart_standby
    restart_all
    delete_add_standby
    delete_add_primary
    # full_clean
    failover_alive
    failover_restart
    switchover_4times
}

function check_user()
{
    if [[ $USER = "root" ]]; then
        print "current user is root, do not use root"
        exit 1
    fi
}

function create_user_tpcc()
{
    gsql -d postgres -p ${PGPORT} -c "create user tpcc with password \"Huawei@123\"; grant all privileges to tpcc;"
}


function runDatabaseBuild()
{
    NODE_IDX=$1
    cd ${TPCC_PATH}
    sh runDatabaseBuild.sh ${TPCC_P[$NODE_IDX]}
    print "sh runDatabaseBuild.sh ${TPCC_P[$NODE_IDX]}"
}

function runbenchmark()
{
    is_primary=$1
    NODE_IDX=$2
    cd ${TPCC_PATH}
    if [[ $is_primary = "Primary" ]]; then
        print "runbenchmark Primary $NODE_IDX"
        sh runBenchmark.sh ${TPCC_P[$NODE_IDX]}  > /dev/null 2>&1 &
    else
        print "runbenchmark Standby $NODE_IDX"
        sh runBenchmark.sh ${TPCC_S[$NODE_IDX]}  > /dev/null 2>&1 &
    fi
}

function tpcc_init()
{
    create_user_tpcc
    runDatabaseBuild 0
}

function restart_primary_tpcc()
{
    function_enter
    runbenchmark Primary 0
    runbenchmark Standby 1
    sleep 60
    db_kill 0
    db_start 0
    sleep 30
    db_start_wait 0
    function_leave
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


function kick_node_offline()
{
    NODE_MASK=3
    NODE_ID=$1
    db_kill $NODE_ID
    NEXT_REFORMER_ID=$(( (${NODE_ID} + 1)%2 ))
    NEXT_BITMAP_ONLINE=$(( ~(1 << ${NODE_ID})  & $NODE_MASK ))
    sh ${CURPATH}/cm_ctl.sh set BITMAP_ONLINE $NEXT_BITMAP_ONLINE
    sh ${CURPATH}/cm_ctl.sh set REFORMER_ID $NEXT_REFORMER_ID
    print "node$NODE_ID is kick off , node:${NEXT_REFORMER_ID} will become reformer, bitmap_online:$NEXT_BITMAP_ONLINE"
}

function kick_node_online()
{
    NODE_MASK=3
    NODE_ID=$1
    db_kill $NODE_ID
    NEXT_REFORMER_ID=$(( (${NODE_ID} + 1)%2 ))
    sh ${CURPATH}/cm_ctl.sh set REFORMER_ID $NEXT_REFORMER_ID
    db_start $NODE_ID
    print "node$NODE_ID is kick off , node:${NEXT_REFORMER_ID} will become reformer, bitmap_online:$NEXT_BITMAP_ONLINE"
}

function failover_alive_tpcc_old_offline()
{
    function_enter_failover
    db_start_wait 0
    db_start_wait 1
    db_role_get 0
    db_role_get 1
    for (( i=0;i<${#DB_ROLE[@]};i++ ))
    do
        if [ ${DB_ROLE[$i]} = 'Primary' ]; then
            runbenchmark Primary $i
            PRIMARY_ID=$i
        else
            runbenchmark Standby $i
        fi
    done

    sleep 120
    kick_node_offline $PRIMARY_ID
    DELETED_ID=$PRIMARY_ID

    pid=`ps ux | grep gaussdb | grep ${DB_HOME[$NEXT_REFORMER_ID]} | grep -v grep`
    wait_time=0
    while [[ ! -z $pid ]]; do
        sleep 20
        db_role_get $NEXT_REFORMER_ID
        if [[ ${DB_ROLE[$NEXT_REFORMER_ID]} == 'Primary' ]] ||  [[ ${DB_ROLE[$NEXT_REFORMER_ID]} == 'Standby' ]]; then
            break;
        fi
        wait_time=`expr ${wait_time} + 20`
        if [ ${wait_time} -gt 1200 ]; then
            print "failover wait too long."
            exit 0
        fi
        pid=`ps ux | grep gaussdb | grep ${DB_HOME[$NEXT_REFORMER_ID]} | grep -v grep`
    done

    if [[ -z $pid ]]; then
        print "failover failed, maybe caused by backend cannot exit, please check"
        db_start $NEXT_REFORMER_ID
    fi

    db_start_wait $NEXT_REFORMER_ID
    db_role_get $NEXT_REFORMER_ID
    db_role_check $NEXT_REFORMER_ID Primary

    db_start $DELETED_ID
    sh ${CURPATH}/cm_ctl.sh set BITMAP_ONLINE 3
    db_start_wait $DELETED_ID
    db_role_get $DELETED_ID
    db_role_check $DELETED_ID Standby   
    function_leave_failover
}

function failover_alive_tpcc_old_online()
{
    function_enter_failover
    db_start_wait 0
    db_start_wait 1
    db_role_get 0
    db_role_get 1
    for (( i=0;i<${#DB_ROLE[@]};i++ ))
    do
        if [ ${DB_ROLE[$i]} = 'Primary' ]; then
            runbenchmark Primary $i
            PRIMARY_ID=$i
        else
            runbenchmark Standby $i
        fi
    done

    sleep 120
    kick_node_online $PRIMARY_ID
    DELETED_ID=$PRIMARY_ID

    pid=`ps ux | grep gaussdb | grep ${DB_HOME[$NEXT_REFORMER_ID]} | grep -v grep`
    wait_time=0
    while [[ ! -z $pid ]]; do
        sleep 20
        db_role_get $NEXT_REFORMER_ID
        if [[ ${DB_ROLE[$NEXT_REFORMER_ID]} == 'Primary' ]] ||  [[ ${DB_ROLE[$NEXT_REFORMER_ID]} == 'Standby' ]]; then
            break;
        fi
        wait_time=`expr ${wait_time} + 20`
        if [ ${wait_time} -gt 1200 ]; then
            print "failover wait too long."
            exit 0
        fi
        pid=`ps ux | grep gaussdb | grep ${DB_HOME[$NEXT_REFORMER_ID]} | grep -v grep`
    done

    if [[ -z $pid ]]; then
        print "failover failed, maybe caused by backend cannot exit, please check"
        db_start $NEXT_REFORMER_ID
    fi    
    db_start_wait $DELETED_ID
    db_role_get $DELETED_ID
    db_role_check $DELETED_ID Standby

    db_start_wait $NEXT_REFORMER_ID
    db_role_get $NEXT_REFORMER_ID
    db_role_check $NEXT_REFORMER_ID Primary
    function_leave_failover
}

function muti_execute()
{
    times=$1
    for(( time=0;time<$times;time++ ))
    do
        print "=====times:$time===="
        ## run function
        $2
    done
}

check_user
if [ "$1" == "dual_cluster" ]; then
    echo "starting dual cluster"
    deploy_dual_cluster
else
    echo "starting single cluster"
    deploy_two_inst
fi
ha_test_init
testcase
## if you want use tpcc
#tpcc_init
#muti_execute 10 restart_primary_tpcc
#muti_execute 10 failover_alive_tpcc_old_offline
#muti_execute 10 failover_alive_tpcc_old_online
exit 0