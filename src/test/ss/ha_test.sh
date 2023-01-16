CURPATH=$(dirname $(readlink -f $0))
echo "check env var"
if [ ${GAUSSHOME} ] && [ -d ${GAUSSHOME}/bin ];then
    echo "GAUSSHOME :${GAUSSHOME}"
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
SS_DATA=${HOME}/ss_hatest
nodedata_cfg="0:127.0.0.1:6611,1:127.0.0.1:6711"
export CM_CONFIG_PATH=${CURPATH}/cm_config.ini

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
    print "shutdown node$1"
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
    ps ux | grep gaussdb | grep ${DB_HOME[$1]} | grep -v grep |awk '{print $2}' | xargs kill -9 > /dev/null 2>&1
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
    ${BIN_PATH}/gaussdb -D ${DB_HOME[$1]} > /dev/null 2>&1 &
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
        if [ ${wait_time} -gt 600 ]; then
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

    export DSS_HOME=${SS_DATA}/dss_home0
    start_gaussdb ${SS_DATA}/dn0
    export DSS_HOME=${SS_DATA}/dss_home1
    start_gaussdb ${SS_DATA}/dn1
}

function testcase()
{
    restart_primary
    restart_standby
    restart_all
    delete_add_standby
    delete_add_primary
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

check_user
deploy_two_inst
ha_test_init
testcase
exit 0