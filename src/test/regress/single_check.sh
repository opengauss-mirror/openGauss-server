#!/bin/bash

#OPENGS=${openGauss}

function export_ldpath()
{
    export LD_LIBRARY_PATH=$2:$LD_LIBRARY_PATH
}

function runscript()
{
    if [ ! "X$1" = "X" ]; then
        eval $1
        echo "$1" >> $CMAKE_BUILD_PATH/runscript.log
    fi
    if [ ! "X$2" = "X" ]; then
        eval $2
        echo "$2" >> $CMAKE_BUILD_PATH/runscript.log
    fi
    if [ ! "X$3" = "X" ]; then
        eval $3
        echo "$3" >> $CMAKE_BUILD_PATH/runscript.log
    fi
    if [ ! "X$4" = "X" ]; then
        eval $4
        echo "$4" >> $CMAKE_BUILD_PATH/runscript.log
    fi
}

function before_regresscheck()
{
    rm -rf $1/testtablespace
    mkdir $1/testtablespace
    test -d $1/memcheck/asan/runlog || mkdir -p $1/memcheck/asan/runlog
    cp $1/make_fastcheck_postgresql.conf $1/make_fastcheck_postgresql_tmp.conf
    mkdir -p $1/tmp
    mkdir -p $1/tmp/gs_profile
    sed -i "s@unix_socket_directory = '/tmp@unix_socket_directory = '$1/tmp@g" $1/make_fastcheck_postgresql_tmp.conf
    export PGHOST=$1/tmp
    if [[ ! ${LD_LIBRARY_PATH} =~ ${INSTALLED_PATH}/lib ]]; then
        export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${INSTALLED_PATH}/lib
    fi
    if [[ ! ${PATH} =~ ${INSTALLED_PATH}/bin ]]; then
        export PATH=$PATH:${INSTALLED_PATH}/bin
    fi
}

function set_hotpatch_env()
{
   if [ "XX$part" != "XX" ]; then
       return
   fi

   if [ "${SUPPORT_HOTPATCH}" = "no" ]; then
       sed -i '/hotpatch/s/test: hotpatch/#test: Hotpatch/g' $ROOT_CODE_PATH/distribute/test/regress/parallel_schedule
       return
   fi
   HOTPATCHDIR="$ROOT_CODE_PATH/distribute/lib/hotpatch"
   if [ ! -f "${HOTPATCHDIR}"/tool/fastcheck/patch_tool_llt.sh ]; then
       echo "Can not find hotpatch fastcheck tool"
       echo "Please clone hotpatch repo and create soft link:"
       echo "git clone ssh://git@lfg-y.codehub.huawei.com:2222/Gauss/GaussDBKernel/GaussDBKernel-hotpatch.git"
       echo "ln -s <hotpatch-code>/GaussDBKernel-hotpatch/samples/ ${HOTPATCHDIR}/sample"
       echo "ln -s <hotpatch-code>/GaussDBKernel-hotpatch/tool/ ${HOTPATCHDIR}/tool"
       echo "Can not find hotpatch fastcheck tool"
       exit 1
   fi
   export SERVER_CODE_BASE=$ROOT_CODE_PATH
   export PATCHLIB_HOME=$THIRD_PATH/kernel/platform/hotpatch
}

function exception_arm_cases()
{
    if [ "${CPU_ARCH}_X" = "aarch64_X" ];then
        sed -i '/distribute_setop_1/s/distribute_setop_1/distribute_setop_ARM_1/g' ${1}/${2}
        sed -i '/nodegroup_setop_smp_test/s/nodegroup_setop_smp_test/nodegroup_setop_smp_ARM_test/g' ${1}/${2}
        sed -i '/nodegroup_setop_test/s/nodegroup_setop_test/nodegroup_setop_ARM_test/g' ${1}/${2}
        sed -i '/random_plan/s/random_plan/random_ARM_plan/g' ${1}/${2}
        sed -i '/shipping_no_from_list/s/shipping_no_from_list/shipping_no_from_ARM_list/g' ${1}/${2}
        sed -i '/test_regex/s/test_regex/test_ARM_regex/g' ${1}/${2}
        sed -i '/vec_expression1/s/vec_expression1/vec_expression_ARM1/g' ${1}/${2}
        sed -i '/vec_expression2/s/vec_expression2/vec_expression_ARM2/g' ${1}/${2}
        sed -i '/xc_copy/s/xc_copy/xc_ARM_copy/g' ${1}/${2}
    fi
}

function real_regresscheck_old()
{
    set_hotpatch_env
    set_common_env $1 $2
    echo "regresscheck_old: $pg_regress_check --dlpath=$DL_PATH $EXTRA_REGRESS_OPTS $3 -b $TEMP_INSTALL --abs_gausshome=\'$PREFIX_HOME\' --schedule=$SCHEDULE -w --keep_last_data=$keep_last_data  --temp-config=$TEMP_CONFIG --seprate_unix_socket_dir $MAXCONNOPT --regconf=$REGCONF"
    $pg_regress_check --dlpath=$DL_PATH $EXTRA_REGRESS_OPTS $3 -b $TEMP_INSTALL --abs_gausshome=\'$PREFIX_HOME\' --schedule=$SCHEDULE -w --keep_last_data=$keep_last_data  --temp-config=$TEMP_CONFIG --seprate_unix_socket_dir $MAXCONNOPT --regconf=$REGCONF
}

function real_upgradecheck_single()
{
    set_hotpatch_env
    set_common_env $1 $2
    echo "regresscheck_single: $pg_regress_check --dlpath=$DL_PATH $EXTRA_REGRESS_OPTS $3 -b $TEMP_INSTALL --abs_gausshome=\'$PREFIX_HOME\' --single_node --schedule=$SCHEDULE -w --keep_last_data=$keep_last_data --temp-config=$TEMP_CONFIG $MAXCONNOPT --regconf=$REGCONF --data_base_dir=$DATA_BASE_DIR --upgrade_script_dir=$UPGRADE_SCRIPT_DIR --old_bin_dir=\'$OLD_BIN_DIR\' --grayscale_full_mode --upgrade_schedule=$UPGRADE_SCHEDULE --upgrade_from=$UPGRADE_FROM"
    $pg_regress_check --dlpath=$DL_PATH $EXTRA_REGRESS_OPTS $3 -b $TEMP_INSTALL --abs_gausshome=\'$PREFIX_HOME\' --single_node --schedule=$SCHEDULE -w --keep_last_data=$keep_last_data --temp-config=$TEMP_CONFIG $MAXCONNOPT --regconf=$REGCONF --data_base_dir=$DATA_BASE_DIR --upgrade_script_dir=$UPGRADE_SCRIPT_DIR --old_bin_dir=\'$OLD_BIN_DIR\' --grayscale_full_mode --upgrade_schedule=$UPGRADE_SCHEDULE --upgrade_from=$UPGRADE_FROM
}

function real_regresscheck_single()
{
    set_hotpatch_env
    set_common_env $1 $2
    echo "regresscheck_single: $pg_regress_check --dlpath=$DL_PATH $EXTRA_REGRESS_OPTS $3 -b $TEMP_INSTALL --abs_gausshome=\'$PREFIX_HOME\' --single_node --schedule=$SCHEDULE -w --keep_last_data=$keep_last_data --temp-config=$TEMP_CONFIG $MAXCONNOPT --regconf=$REGCONF"
    $pg_regress_check --dlpath=$DL_PATH $EXTRA_REGRESS_OPTS $3 -b $TEMP_INSTALL --abs_gausshome=\'$PREFIX_HOME\' --single_node --schedule=$SCHEDULE -w --keep_last_data=$keep_last_data --temp-config=$TEMP_CONFIG $MAXCONNOPT --regconf=$REGCONF
}

function real_regresscheck_plugin()
{
    set_common_env_plugin $1 $2
    echo ..........
    echo "$pg_regress_check --dlpath=$DL_PATH $EXTRA_REGRESS_OPTS $3 -b $TEMP_INSTALL --abs_gausshome=\'$PREFIX_HOME\' --single_node --schedule=$SCHEDULE -w --keep_last_data=$keep_last_data --temp-config=$TEMP_CONFIG $MAXCONNOPT --regconf=$REGCONF --dbname=contrib_regression --dbcmpt=B dummy"
    $pg_regress_check --dlpath=$DL_PATH $EXTRA_REGRESS_OPTS $3    --abs_gausshome=\'$PREFIX_HOME\' --single_node --schedule=$SCHEDULE -w --keep_last_data=$keep_last_data --temp-config=$TEMP_CONFIG $MAXCONNOPT --regconf=$REGCONF --dbname=contrib_regression --dbcmpt=B
}

function real_regresscheck_single_audit()
{
    set_hotpatch_env
    set_common_env $1 $2
    echo "regresscheck_single: $pg_regress_check --dlpath=$DL_PATH $EXTRA_REGRESS_OPTS $3 -b $TEMP_INSTALL --abs_gausshome=\'$PREFIX_HOME\' --single_node --schedule=$SCHEDULE -w --keep_last_data=$keep_last_data --temp-config=$TEMP_CONFIG $MAXCONNOPT --regconf=$REGCONF"
    $pg_regress_check --dlpath=$DL_PATH $EXTRA_REGRESS_OPTS $3 -b $TEMP_INSTALL --abs_gausshome=\'$PREFIX_HOME\' --single_node --schedule=$SCHEDULE -w --keep_last_data=$keep_last_data --temp-config=$TEMP_CONFIG $MAXCONNOPT --regconf=$REGCONF
}

function real_regresscheck_single_ss()
{
    set_hotpatch_env
    set_common_env $1 $2

    if [ -d $TEMP_INSTALL ];then
        rm -rf $TEMP_INSTALL
    fi
    sh ${TOP_DIR}/src/test/ss/conf_start_dss_inst.sh 1 $TEMP_INSTALL ${HOME}/ss_fastcheck_disk
    echo "regresscheck_ss_single: $pg_regress_check --dlpath=$DL_PATH $EXTRA_REGRESS_OPTS $3 -b $TEMP_INSTALL --abs_gausshome=\'$PREFIX_HOME\' --single_node --schedule=$SCHEDULE -w --keep_last_data=$keep_last_data --temp-config=$TEMP_CONFIG $MAXCONNOPT --regconf=$REGCONF --enable_ss"
    $pg_regress_check --dlpath=$DL_PATH $EXTRA_REGRESS_OPTS $3 -b $TEMP_INSTALL --abs_gausshome=\'$PREFIX_HOME\' --single_node --schedule=$SCHEDULE -w --keep_last_data=$keep_last_data --temp-config=$TEMP_CONFIG $MAXCONNOPT --regconf=$REGCONF --enable_ss --enable-segment
}

function real_regresscheck_ss()
{
    set_hotpatch_env
    set_common_env $1 $2

    if [ -d $TEMP_INSTALL ];then
        rm -rf $TEMP_INSTALL
    fi
    sh ${TOP_DIR}/src/test/ss/conf_start_dss_inst.sh 2 $TEMP_INSTALL ${HOME}/ss_fastcheck_disk
    echo "regresscheck_ss: $pg_regress_check --dlpath=$DL_PATH $EXTRA_REGRESS_OPTS $3 -b $TEMP_INSTALL --abs_gausshome=\'$PREFIX_HOME\' --single_node --schedule=$SCHEDULE -w --keep_last_data=$keep_last_data --temp-config=$TEMP_CONFIG $MAXCONNOPT --regconf=$REGCONF --ss_standby_read"
    $pg_regress_check --dlpath=$DL_PATH $EXTRA_REGRESS_OPTS $3 -b $TEMP_INSTALL --abs_gausshome=\'$PREFIX_HOME\' --single_node --schedule=$SCHEDULE -w --keep_last_data=$keep_last_data --temp-config=$TEMP_CONFIG $MAXCONNOPT --regconf=$REGCONF --ss_standby_read --enable-segment
}

function real_regresscheck_single_mot()
{
    set_common_env $1 $2
    echo "regresscheck_single_mot: $pg_regress_check --dlpath=$DL_PATH $EXTRA_REGRESS_OPTS $3 -b $TEMP_INSTALL --abs_gausshome=\'$PREFIX_HOME\' --single_node --schedule=$SCHEDULE -w --keep_last_data=$keep_last_data --temp-config=$TEMP_CONFIG $MAXCONNOPT --regconf=$REGCONF"
    $pg_regress_check --dlpath=$DL_PATH $EXTRA_REGRESS_OPTS $3 -b $TEMP_INSTALL --abs_gausshome=\'$PREFIX_HOME\' --single_node --schedule=$SCHEDULE -w --keep_last_data=$keep_last_data --temp-config=$TEMP_CONFIG $MAXCONNOPT --regconf=$REGCONF
}

function real_regresscheck_ledger()
{
    set_common_env $1 $2
    echo "regresscheck_ledger: $pg_regress_check --dlpath=$DL_PATH $EXTRA_REGRESS_OPTS $3 -b $TEMP_INSTALL --abs_gausshome=\'$PREFIX_HOME\' --schedule=$SCHEDULE -w --keep_last_data=$keep_last_data  --temp-config=$TEMP_CONFIG --seprate_unix_socket_dir $MAXCONNOPT --regconf=$REGCONF"
    $pg_regress_check --dlpath=$DL_PATH $EXTRA_REGRESS_OPTS $3 -b $TEMP_INSTALL --abs_gausshome=\'$PREFIX_HOME\' --schedule=$SCHEDULE -w --keep_last_data=$keep_last_data  --temp-config=$TEMP_CONFIG --seprate_unix_socket_dir $MAXCONNOPT --regconf=$REGCONF
}

function real_regresscheck_ce()
{
    set_hotpatch_env
    set_common_env $1 $2
    echo "regresscheck_ce: $pg_regress_check --dlpath=$DL_PATH $EXTRA_REGRESS_OPTS $3 -b $TEMP_INSTALL --abs_gausshome=\'$PREFIX_HOME\' --single_node --schedule=$SCHEDULE -w --keep_last_data=$keep_last_data  --temp-config=$TEMP_CONFIG  $MAXCONNOPT --regconf=$REGCONF"
    $pg_regress_check --dlpath=$DL_PATH $EXTRA_REGRESS_OPTS $3 -b $TEMP_INSTALL --abs_gausshome=\'$PREFIX_HOME\' --single_node --schedule=$SCHEDULE -w --keep_last_data=$keep_last_data  --temp-config=$TEMP_CONFIG $MAXCONNOPT --regconf=$REGCONF
}

function real_regresscheck_ce_jdbc()
{
    set_hotpatch_env
    set_common_env $1 $2
    echo "regresscheck_ce_jdbc: $pg_regress_check --dlpath=$DL_PATH $EXTRA_REGRESS_OPTS $3 -b $TEMP_INSTALL --abs_gausshome=\'$PREFIX_HOME\' --schedule=$SCHEDULE -w --keep_last_data=$keep_last_data  --temp-config=$TEMP_CONFIG $MAXCONNOPT --regconf=$REGCONF"
    $pg_regress_check --dlpath=$DL_PATH $EXTRA_REGRESS_OPTS $3 -b $TEMP_INSTALL --abs_gausshome=\'$PREFIX_HOME\' --single_node --schedule=$SCHEDULE -w --keep_last_data=$keep_last_data  --temp-config=$TEMP_CONFIG $MAXCONNOPT --regconf=$REGCONF --jdbc
}

function real_regresscheck_policy_plugin()
{
    set_hotpatch_env
    set_common_env $1 $2
    echo "regresscheck_policy_plugin: $pg_regress_check --dlpath=$DL_PATH $EXTRA_REGRESS_OPTS $PARALLEL_INITDB $3 -b $TEMP_INSTALL --abs_gausshome=\'$PREFIX_HOME\' --schedule=$SCHEDULE -w --keep_last_data=$keep_last_data  --temp-config=$TEMP_CONFIG --seprate_unix_socket_dir $MAXCONNOPT --regconf=$REGCONF"
    $pg_regress_check --dlpath=$DL_PATH $EXTRA_REGRESS_OPTS $PARALLEL_INITDB $3 -b $TEMP_INSTALL --abs_gausshome=\'$PREFIX_HOME\' --schedule=$SCHEDULE -w --keep_last_data=$keep_last_data  --temp-config=$TEMP_CONFIG --seprate_unix_socket_dir $MAXCONNOPT --regconf=$REGCONF
}

function real_regresscheck_decode()
{
    set_hotpatch_env
    set_common_env $1 $2
    echo "regresscheck_decode: $pg_regress_check --dlpath=$DL_PATH $EXTRA_REGRESS_OPTS $PARALLEL_INITDB $3 -b $TEMP_INSTALL --abs_gausshome=\'$PREFIX_HOME\' --single_node --schedule=$SCHEDULE -w --keep_last_data=$keep_last_data  --temp-config=$TEMP_CONFIG $MAXCONNOPT --regconf=$REGCONF"
    $pg_regress_check --dlpath=$DL_PATH $EXTRA_REGRESS_OPTS $PARALLEL_INITDB $3 -b $TEMP_INSTALL --abs_gausshome=\'$PREFIX_HOME\' --single_node --schedule=$SCHEDULE --keep_last_data=$keep_last_data -w --temp-config=$TEMP_CONFIG $MAXCONNOPT --regconf=$REGCONF
}

function real_redischeck()
{
    set_common_env $1 $2
    echo "redischeck: $pg_regress_check --dlpath=$DL_PATH $EXTRA_REGRESS_OPTS $3 -b $TEMP_INSTALL --schedule=$SCHEDULE --keep_last_data=$keep_last_data --temp-config=$TEMP_CONFIG --regconf=$REGCONF"
    $pg_regress_check --dlpath=$DL_PATH $EXTRA_REGRESS_OPTS $3 -b $TEMP_INSTALL --schedule=$SCHEDULE --keep_last_data=$keep_last_data --temp-config=$TEMP_CONFIG --regconf=$REGCONF
}

function real_wmlcheck()
{
    set_common_env $1 $2
    export TMP_USR=$(whoami)
    cp /home/$(whoami)/etc/gscgroup_$(whoami).cfg $GAUSSHOME/etc/
    if [ $(whoami)x == 'CodeTestCov'x ];then
        export WLM_TEST=${GAUSSHOME}
    else
        export WLM_TEST=${TEMP_INSTALL}
    fi
    echo "wmlcheck: $pg_regress_check --dlpath=$DL_PATH $EXTRA_REGRESS_OPTS $3 -b $TEMP_INSTALL --schedule=$SCHEDULE --keep_last_data=$keep_last_data --temp-config=$TEMP_CONFIG --regconf=$REGCONF"
    $pg_regress_check --dlpath=$DL_PATH $EXTRA_REGRESS_OPTS $3 -b $TEMP_INSTALL --schedule=$SCHEDULE --keep_last_data=$keep_last_data --temp-config=$TEMP_CONFIG --regconf=$REGCONF
}

function real_2pccheck()
{
    set_common_env $1 $2
    echo "wmlcheck: $pg_regress_check --dlpath=$DL_PATH $EXTRA_REGRESS_OPTS $3 -b $TEMP_INSTALL --schedule=$SCHEDULE --temp-config=$TEMP_CONFIG --regconf=$REGCONF"
    $pg_regress_check --dlpath=$DL_PATH $EXTRA_REGRESS_OPTS $3 -b $TEMP_INSTALL --schedule=$SCHEDULE --temp-config=$TEMP_CONFIG --regconf=$REGCONF
}

function real_hacheck()
{
    REGRESS_PATH=$ROOT_CODE_PATH/${OPENGS}/src/test/regress
    PGREGRESS_BIN=./${OPENGS}/src/test/regress/pg_regress_single
    if [ "${ENABLE_MEMORY_CHECK}" = "ON" ];then
        EXTRA_REGRESS_OPTS="--ignore-exitcode --keep-run"
    fi
    PSQLDIR=${PREFIX_HOME}/bin
    REGRESS_OPTS="--dlpath=$PREFIX_HOME/lib ${EXTRA_REGRESS_OPTS}"
    REG_CONF="--regconf=$INPUT_DIR/regress.conf"
    cd ${REGRESS_PATH}/../ha
    sed -i "/export g_base_port=8888/s/export g_base_port=8888/export g_base_port=${p}/g" ${REGRESS_PATH}/../ha/standby_env.sh
    ip_list=$(ifconfig | grep 'inet ' | grep -v '127.0.0.1' | cut -d: -f2 | awk '{print $2}')
    eth0ip=$(echo $ip_list | awk -F' ' '{print $1}')
    eth1ip=$(echo $ip_list | awk -F' ' '{print $2}')
    export prefix=$PREFIX_HOME
    module=$1
    case $module in
        hacheck_all)
            sh ./run_ha.sh
            sh ./run_ha_multi.sh ;;
        hacheck_multi_standby)
            sh ./run_ha_multi.sh ;;
        hacheck_single_all)
            sh ./run_ha_single.sh
            sh ./run_ha_multi_single.sh
            sh ./run_ha_multi_cascade.sh ;;
        hacheck_single)
            sh ./run_ha_single.sh 1 ${part} ;;
        hacheck_decode)
            sh ./run_ha_decode_single.sh ;;
        hacheck_multi_single)
            sh ./run_ha_multi_single.sh 1 ${part}
            sh ./run_ha_multi_cascade.sh ;;
        hacheck_multi_single_mot)
            sh ./run_ha_multi_single_mot.sh 1 ${part}
            sh ./run_ha_multi_cascade_mot.sh ;;
        hacheck_single_paxos)
            sh ./run_paxos_single.sh ;;
        hacheck_ss_all)
            sh ./run_ha_single_ss.sh ;;
        hacheck_single_standby_read)
            sh ./run_ha_single_standby_read.sh ;;
        *)
            echo "module $module is not valid" ;;
    esac
}

function check_gs_probackup()
{
    REGRESS_PATH=$ROOT_CODE_PATH/${OPENGS}/src/test/regress
    cd ${REGRESS_PATH}/
    sh ${REGRESS_PATH}/gs_probackup.sh;
}

#These only used for *check cmd.
DO_CMD=$1
TRUNK_CODE_PATH=${2}
CMAKE_BUILD_PATH=${3}
INSTALLED_PATH=${5}
ENABLE_MEMORY_CHECK=${7}
MAKE_PROGRAM=${8}
SUPPORT_HOTPATCH=${9}
OPENGS=${10}

d=4
c=2
p=25632
runtest=1
level=basic
n=2002
s=1
part=""
PART=""
ENCODE=""
MAX_CONNECTIONS=""
keep_last_data=false
hdfshostname='10.185.178.239:25000,10.185.178.241'
PARALLEL_INITDB=""
CPU_ARCH=$(uname -p)

function check_enum()
{
    old_path=`pwd`
    cd $CODE_BASE
    python $REGRESS_PATH/check_enum.py
    if [ $? != 0 ]; then
        exit 1
    fi
    cd $old_path
}

function set_common_env()
{
    export CODE_BASE=$ROOT_CODE_PATH/${OPENGS}
    export CODE_BASE_SRC=$CODE_BASE/src
    export GAUSSHOME=$PREFIX_HOME
    REGRESS_PATH=$ROOT_CODE_PATH/${OPENGS}/src/test/regress
    check_enum
    REGRESS_PATH=$(cd ${REGRESS_PATH}/; pwd)
    PGREGRESS_BIN=./${OPENGS}/src/test/regress/pg_regress_single
    if [ "${ENABLE_MEMORY_CHECK}" = "ON" ];then
        export ASAN_OPTIONS="unmap_shadow_on_exit=1:abort_on_error=1:detect_leaks=1:force_check_leak=1:halt_on_error=0:alloc_dealloc_mismatch=0:fast_unwind_on_fatal=1:log_path=/home/$(whoami)/memchk/asan/runlog"
        export LSAN_OPTIONS="exitcode=0:suppressions=${CODE_BASE}/Tools/memory_check/memleak_ignore"
        EXTRA_REGRESS_OPTS="--ignore-exitcode --keep-run"
    fi
    MAXCONNOPT=""
    if [ ! "${MAX_CONNECTIONS}" = "" ];then
        MAXCONNOPT="--max-connections=$(MAX_CONNECTIONS)"
    fi
    echo ..........$REGRESS_PATH
    cp $THIRD_PATH/kernel/dependency/event/comm/lib/* $PREFIX_HOME/lib
    cp -r $REGRESS_PATH/expected .
    cp -r $REGRESS_PATH/data .
    cp -r $REGRESS_PATH/jdbc_test .
    cp -r $REGRESS_PATH/jdbc_ce_test .
    cp -r $REGRESS_PATH/jdbc_client .
    cp -r $REGRESS_PATH/jdbc_client_lib .
    cp -r $REGRESS_PATH/jdbc_client_lib ./opengauss/src/test/regress/
    cp -r $REGRESS_PATH/security_scripts .
    cp ./lib/regress.so $PREFIX_HOME/lib
    cp ./lib/refint.so $PREFIX_HOME/lib
    cp ./lib/autoinc.so $PREFIX_HOME/lib
    rm $PREFIX_HOME/lib/libstdc++.so.6
    DL_PATH=${GAUSSHOME}/lib
    TOP_DIR=$ROOT_CODE_PATH
    INPUT_DIR=${REGRESS_PATH}
    TEMP_INSTALL=${REGRESS_PATH}/tmp_check

    REGCONF=$INPUT_DIR/regress.conf
    SCHEDULE=$INPUT_DIR/$1
    TEMP_CONFIG=$INPUT_DIR/$2

    DATA_BASE_DIR=$INPUT_DIR/../../../../privategauss/test/grayscale_upgrade
    UPGRADE_SCRIPT_DIR=$INPUT_DIR/../grayscale_upgrade
    OLD_BIN_DIR=$TEMP_INSTALL/bin
    UPGRADE_SCHEDULE=$INPUT_DIR/upgrade_schedule
    exception_arm_cases ${ROOT_CODE_PATH} parallel_schedule

    pg_regress_locale_flags=""
    pg_regress_check="${PGREGRESS_BIN} --inputdir=${INPUT_DIR} --temp-install=${TEMP_INSTALL} --top-builddir=${TOP_DIR} ${pg_regress_locale_flags}"
    chmod +x ${REGRESS_PATH}/smartmatch.pl
    rm -rf ./testtablespace && mkdir ./testtablespace
    test -d /home/$(whoami)/memchk/asan || mkdir -p /home/$(whoami)/memchk/asan
    #端口
    if [ "X$p" = "X" ]; then
        p=35000
    fi

    echo "--------------------LD_LIBRARY_PATH----------------------------------------------------"
    echo $LD_LIBRARY_PATH
    export
    UPGRADE_FROM=92497
}

function set_common_env_plugin()
{
    export CODE_BASE=$ROOT_CODE_PATH/${OPENGS}
    export CODE_BASE_SRC=$CODE_BASE/src
    export GAUSSHOME=$PREFIX_HOME
    REGRESS_PATH=$ROOT_CODE_PATH/contrib/dolphin
    cp $ROOT_CODE_PATH/src/test/regress/check_enum.py $REGRESS_PATH
    check_enum
    REGRESS_PATH=$(cd ${REGRESS_PATH}/; pwd)
    PGREGRESS_BIN=./${OPENGS}/src/test/regress/pg_regress_single
    if [ "${ENABLE_MEMORY_CHECK}" = "ON" ];then
        export ASAN_OPTIONS="unmap_shadow_on_exit=1:abort_on_error=1:detect_leaks=1:force_check_leak=1:halt_on_error=0:alloc_dealloc_mismatch=0:fast_unwind_on_fatal=1:log_path=/home/$(whoami)/memchk/asan/runlog"
        export LSAN_OPTIONS="exitcode=0:suppressions=${CODE_BASE}/Tools/memory_check/memleak_ignore"
        EXTRA_REGRESS_OPTS="--ignore-exitcode --keep-run"
    fi
    MAXCONNOPT=""
    if [ ! "${MAX_CONNECTIONS}" = "" ];then
        MAXCONNOPT="--max-connections=$(MAX_CONNECTIONS)"
    fi
    echo ..........$REGRESS_PATH
    cp $THIRD_PATH/kernel/dependency/event/comm/lib/* $PREFIX_HOME/lib
    cp -r $REGRESS_PATH/expected .
    cp -r $REGRESS_PATH/data .
    cp ./lib/regress.so $PREFIX_HOME/lib
    cp ./lib/refint.so $PREFIX_HOME/lib
    cp ./lib/autoinc.so $PREFIX_HOME/lib

    rm $PREFIX_HOME/lib/libstdc++.so.6
    DL_PATH=${GAUSSHOME}/lib
    TOP_DIR=$ROOT_CODE_PATH
    INPUT_DIR=${REGRESS_PATH}
    TEMP_INSTALL=${REGRESS_PATH}/tmp_check

    REGCONF=$INPUT_DIR/regress.conf
    SCHEDULE=$INPUT_DIR/$1
    TEMP_CONFIG=$INPUT_DIR/$2

    DATA_BASE_DIR=$INPUT_DIR/../../../../privategauss/test/grayscale_upgrade
    UPGRADE_SCRIPT_DIR=$INPUT_DIR/../grayscale_upgrade
    OLD_BIN_DIR=$TEMP_INSTALL/bin
    UPGRADE_SCHEDULE=$INPUT_DIR/upgrade_schedule
    exception_arm_cases ${ROOT_CODE_PATH} parallel_schedule

    pg_regress_locale_flags=""
    pg_regress_check="${PGREGRESS_BIN} --inputdir=${INPUT_DIR} --temp-install=${TEMP_INSTALL} --top-builddir=${TOP_DIR} ${pg_regress_locale_flags}"
    chmod +x ${REGRESS_PATH}/smartmatch.pl
    rm -rf ./testtablespace && mkdir ./testtablespace
    test -d /home/$(whoami)/memchk/asan || mkdir -p /home/$(whoami)/memchk/asan
    #端口
    if [ "X$p" = "X" ]; then
        p=35000
    fi

    echo "--------------------LD_LIBRARY_PATH----------------------------------------------------"
    echo $LD_LIBRARY_PATH
    export
    UPGRADE_FROM=92497
}

function parse_args()
{
    #the args start from $8, only aacept args flags is *=[0-9]*|*=true|*=false
    for((i=1;i<10;i++))
    do
        shift
    done

    while (($#))
    do
        case $1 in
            *=[[:print:]]*) eval $1; shift;;
            *) shift;;
        esac
    done
}


if [ "X$DO_CMD" = "X--cmd-withargs" ]; then
    DO_CMD=$6
    parse_args $@

    export ROOT_CODE_PATH=$2
    export EXEC_PATH=$3
    export THIRD_PATH=$4
    export INSTALL_PATH=$5
    echo $THIRD_PATH
fi
DEPLOY_OPTS="-d $d -c $c -p $p -r ${runtest} -b tmp_check"
DEPLOY_OPTS_N="-d $d -c $c -p $p -r ${runtest} -b tmp_check -n $n"
DEPLOY_OPTS_ORC="-d 2 -c 2 -p $p -r ${runtest} -b tmp_check -n $n"



case $DO_CMD in
    --runscript*|runscript)
        runscript "$4" "$5" "$6" "$7" ;;
    --build-package*|package*|--p)
        cd $TRUNK_CODE_PATH/../Build/Script && ./mpp_package_cmake.sh -i ${INSTALLED_PATH} -m ;;

    --check|--runcheck|runcheck|check)
        real_regresscheck parallel_schedule make_check_postgresql.conf "$DEPLOY_OPTS_N" ;;
    --fastcheck_parallel_initdb_single|fastcheck_parallel_initdb_single)
        PARALLEL_INITDB="--parallel_initdb"
        args_val="$PARALLEL_INITDB -d 6 -c 3 -p $p -r 1 -n 2002"
        real_regresscheck_old parallel_schedule$part make_fastcheck_postgresql.conf "${args_val}" ;;
    --fastcheck_single|fastcheck_single)
        args_val="-d 1 -c 0 -p $p -r 1 "
        real_regresscheck_single parallel_schedule0$part make_fastcheck_postgresql.conf "${args_val}" ;;
    --plugin_check|plugin_check)
        args_val="-d 1 -c 0 -p $p -r 1 "
        real_regresscheck_plugin parallel_schedule_dolphin$part make_check_postgresql.conf "${args_val}" ;;
    --fastcheck_single_audit|fastcheck_single_audit)
        args_val="-d 1 -c 0 -p $p -r 1 "
        real_regresscheck_single_audit security_audit_schedule0$part make_fastcheck_audit_postgresql.conf "${args_val}" ;;
    --fastcheck_lite|fastcheck_lite)
        args_val="-d 1 -c 0 -p $p -r 1 "
        real_regresscheck_single parallel_schedule.lite$part make_fastcheck_postgresql.conf "${args_val}" ;;
    --fastcheck_single_ss|fastcheck_single_ss)
        args_val="-d 1 -c 0 -p $p -r 1 "
        real_regresscheck_single_ss parallel_scheduleSS make_fastcheck_ss_postgresql.conf "${args_val}" ;;
    --fastcheck_ss|fastcheck_ss)
        args_val="-d 2 -c 0 -p $p -r 1 "
        real_regresscheck_ss parallel_schedule_ss_read$part make_fastcheck_ss_postgresql.conf "${args_val}" ;;
    --fastcheck_gs_probackup|fastcheck_gs_probackup)
        args_val=$(echo $DO_CMD | sed 's\--\\g')
        check_gs_probackup;;

    --upgradecheck_single|upgradecheck_single)
        args_val="-d 1 -c 0 -p $p -r 1 "
        real_upgradecheck_single parallel_schedule0$part make_fastcheck_postgresql.conf "${args_val}" ;;

    --fastcheck_single_mot|fastcheck_single_mot)
        args_val="-d 1 -c 0 -p $p -r 1 "
        real_regresscheck_single_mot parallel_schedule20 make_fastcheck_single_mot_postgresql.conf "${args_val}" ;;
    --2pccheck_single|2pccheck_single)
        args_val="-d 4 -c 2 -p $p -r ${runtest}"
        real_2pccheck parallel_schedule_2pc make_check_postgresql_2pc.conf "${args_val}" ;;
    --redischeck_single|redischeck_single)
        args_val="-d 8 -c 1 -p $p -r ${runtest}"
        real_redischeck parallel_schedule${part}.redistribute make_check_postgresql.conf "${args_val}" ;;
    --wlmcheck_single|wlmcheck_single)
        args_val="-d 6 -c 3 -p $p -r ${runtest}"
        real_wmlcheck parallel_schedule${part}.wlm make_wlmcheck_postgresql.conf "${args_val}" ;;
    --hacheck_single_all|hacheck_single_all|--hacheck_single|hacheck_single|--hacheck_multi_single|hacheck_multi_single|--hacheck_multi_single_mot|hacheck_multi_single_mot|--hacheck_decode|hacheck_decode|--hacheck_single_paxos|hacheck_single_paxos|--hacheck_ss_all|hacheck_ss_all|--hacheck_single_standby_read|hacheck_single_standby_read)
        args_val=$(echo $DO_CMD | sed 's\--\\g')
        real_hacheck "${args_val}";;
    --fastcheck_ledger_single|fastcheck_ledger_single)
        args_val="-d 6 -c 3 -p $p -r ${runtest} -n 2002"
        real_regresscheck_ledger ledger_schedule make_fastcheck_postgresql.conf "${args_val}" ;;
    --fastcheck_ce_single|fastcheck_ce_single)
        args_val="-d 1 -c 0 -p $p -r ${runtest} -n 2002"
        real_regresscheck_ce ce_sched make_fastcheck_postgresql.conf "${args_val}" ;;
    --execute_fastcheck_ce_single_jdbc|execute_fastcheck_ce_single_jdbc)
        args_val="-d 1 -c 0 -p $p -r ${runtest} -n 2002"
        real_regresscheck_ce_jdbc ce_sched_jdbc make_fastcheck_postgresql.conf "${args_val}" ;;
    --fastcheck_policy_plugin_single|fastcheck_policy_plugin_single)
        args_val="-d 6 -c 3 -p $p -r ${runtest} -n 2002"
        real_regresscheck_policy_plugin security_plugin_schedule make_fastcheck_postgresql.conf "${args_val}" ;;
    --decodecheck_single|decodecheck_single)
        args_val="-d 1 -c 0 -p $p -r ${runtest}"
        real_regresscheck_decode parallel_schedule.decode make_fastcheck_postgresql.conf "${args_val}" ;;
    --utest*|utest*|ut|--u)
        echo "now ctest: this is CMake test work, test driver develop" ;;
    --test*|test)
        echo "now test services" ;;
    *)
        echo "no args: after you need add cmd in cmake" ;;
esac

