
# 获取当前执行脚本绝对路径
path=$(dirname "${BASH_SOURCE-$0}")
real_path=$(cd "$path">/dev/null;pwd)

# 定义函数，功能：引入配置文件 及 function目录下定义的函数
function source_file()
{
    [ ! -f "${g_conf_file}" ] && echo "Can not find config file ${g_conf_file}" && exit 1

    # 去除配置文件中的\r字符
    config_str="$(cat "$g_conf_file" | grep -v "^[[:space:]]*$" | \
        grep -v "^#" | tr -d '\r')"

    config_str_new="$config_str"
    #printf "$config_str_new\n"
    # 引入配置文件
    eval "$config_str"
    if [ $? -ne 0 ]; then
        echo "source ${g_conf_file} failed"
        return 1
    fi

    # source function目录下定义的函数
    while read line
    do
        source "$line"
        if [ $? -ne 0 ]; then
            echo "source ${line} failed"
            return 1
        fi
    done << _EOF_
$(find "${real_path}/function/" -mindepth 1 -maxdepth 1 \
         -name "[0-9][0-9].*.sh" -type f | sort)
_EOF_

    return 0
}

# 展示环境信息
function show_env_detail
{
    # get OS version.
    os_name="$(get_osname)"
    if [ "${os_name}" == "" ]; then
        error "openGauss" "Can not get OS type on Current node"
        return 1
    else
        log "openGauss" "OS Type: ${os_name}"
    fi

    # show env var
    log "openGauss" "Work  Dir: ${g_boot_dir}"
    log "openGauss" "Code  Dir: ${g_src_dir}"
    log "openGauss" "Log   Dir: ${g_log_file}"
    log "openGauss" "Build Dir: ${g_build_dir}"
    log "openGauss" "Current       User: ${USER}"
    log "openGauss" "Configure Pkg Type: ${g_release_mode}"
    log "openGauss" "User for HAcheck: ${g_execute_user}"
    log "openGauss" "Port for HAcheck: ${g_check_port}"

    log "openGauss" "ENV VAR -- MPPHOME: ${MPPHOME}"
    log "openGauss" "ENV VAR -- GAUSSHOME: ${GAUSSHOME}"
    log "openGauss" "ENV VAR -- MPPDB_CODE: ${MPPDB_CODE}"
    log "openGauss" "ENV VAR -- CC: ${CC}"
    log "openGauss" "ENV VAR -- CXX: ${CXX}"
    log "openGauss" "ENV VAR -- LD_LIBRARY_PATH: ${LD_LIBRARY_PATH}"
    log "openGauss" "ENV VAR -- PATH: ${PATH}"

    log "openGauss" "OPTION -- Retrans_enable: ${g_retrans_enable}"
    log "openGauss" "OPTION -- g_master_user: ${g_master_user}"
    log "openGauss" "OPTION -- g_master_log_path: ${g_master_log_path}"
    log "openGauss" "OPTION -- g_top_build_id: ${g_top_build_id}"
    log "openGauss" "OPTION -- g_cur_build_id: ${g_cur_build_id}"
}

# mk_hacheck
function mk_hacheck
{

    # get OS version.
    os_name="$(get_osname)"
    #change current work dir to src
    log "openGauss" "Change current dir to src dir"
    log "openGauss" "Command: cd ${g_src_dir}"
    cd ${g_src_dir} >> ${g_log_file} 2>&1
    if [ $? -ne 0 ]; then
        error "openGauss" "Change dir failed"
        return 1
    fi

    #configure
    configure_start_time=$(date +%s.%N)
    log "openGauss" "configure_start_time="${configure_start_time}""
    log "openGauss" "Begin configure..."
    chmod 755 configure

    if [ "${g_release_mode}" == "release" ]; then
        log "openGauss" "Command: ./configure --prefix="${g_build_dir}" CC=g++  CFLAGS='-O2' --enable-thread-safety --with-readline --without-zlib --disable-debug --gcc-version=8.2.0 "
        ./configure --prefix="${g_build_dir}" CC=g++  CFLAGS='-O2' --enable-thread-safety --with-readline --without-zlib --disable-debug --gcc-version=8.2.0 >> ${g_log_file} 2>&1
    else
        log "openGauss" "Command: ./configure --prefix="${g_build_dir}" --enable-debug --enable-cassert CC=g++ CFLAGS='-g3 -O2 -w' --without-zlib --gcc-version=8.2.0 --3rd="${BINARYLIBS}""
        ./configure --prefix="${g_build_dir}" --enable-debug --enable-cassert CC=g++ CFLAGS='-g3 -O2 -w' --without-zlib --gcc-version=8.2.0 --3rd="${BINARYLIBS}">> ${g_log_file} 2>&1
    fi

    configure_stop_time=$(date +%s.%N)
    log "openGauss" "configure_stop_time="${configure_stop_time}""
    if [ $? -ne 0 ]; then
        error "openGauss" "Configure failed."
        log "openGauss" "Configure detail:"
        sed -n '/configure --prefix/,$'p ${g_log_file}|sed 1d|sed '$d'|sed '$d'
        flag_configure_fail=1
        return 1
    else
        log "openGauss" "Configure succeed."
        flag_configure_fail=0
    fi

    #make clean
    makeclean_start_time=$(date +%s.%N)
    log "openGauss" "makeclean_start_time="${makeclean_start_time}""
    log "openGauss" "Begin make clean..."
    log "openGauss" "Command: make clean -sj 8"
    make clean -sj 8 >> ${g_log_file} 2>&1
    makeclean_stop_time=$(date +%s.%N)
    log "openGauss" "makeclean_stop_time="${makeclean_stop_time}""
    if [ $? -ne 0 ]; then
        error "openGauss" "Make clean failed"
        flag_makeclean_fail=1
        return 1
    else
        log "openGauss" "Make clean succeed."
        flag_makeclean_fail=0
    fi

    #make install
    makeinstall_start_time=$(date +%s.%N)
    log "openGauss" "makeinstall_start_time="${makeinstall_start_time}""
    log "openGauss" "Begin make install..."
    for((i=1;i<=2;i++))
    do
        log "openGauss" "Try $i time ..."
        log "openGauss" "Command: make install -sj 8"
        cd ${g_src_dir} && make install -sj 8 >> ${g_log_file} 2>&1
        if [ $? -eq 0 ]; then
            log "openGauss" "Make install succeed."
            flag_makeinstall_fail=0
            break
        else
            if [ $i -eq 2 ];then
                error "openGauss" "Make install failed."
                log "openGauss" "Make install detail:"
                sed -n '/make install -sj 8/,$'p ${g_log_file}|sed 1d|sed '$d'|sed '$d'
                [ -f "${g_src_dir}/src/test/regress/log/install.log" ] && cat ${g_src_dir}/src/test/regress/log/install.log
                flag_makeinstall_fail=1
                return 1
            else
                error "openGauss" "Make install failed first time."
                log "openGauss" "Execute make clean -sj 8 for make install again."
                cd ${g_src_dir} && make clean -sj 8 >> ${g_log_file} 2>&1
            fi
        fi
    done
    makeinstall_stop_time=$(date +%s.%N)
    log "openGauss" "makeinstall_stop_time="${makeinstall_stop_time}""
    #make hacheck
    makehacheck_start_time=$(date +%s.%N)
    log "openGauss" "makehacheck_start_time="${makehacheck_start_time}""
    log "openGauss" "Begin make hacheck..."
    CUR_USER=`whoami`
    if [ "${CUR_USER}" == "root" ]; then
        log "openGauss" "Current user is root."
        #create user
        #add_user
        chown -R ${g_execute_user} ${g_boot_dir} >> ${g_log_file} 2>&1
        chmod 777 ${g_boot_dir} >> ${g_log_file} 2>&1
        #change port
        sed -i "s/export g_base_port=8888/export g_base_port=${g_check_port}/g" ${g_src_dir}/src/test/ha/standby_env.sh
        #change user to make check
        log "openGauss" "Begin do the ${check_mode} ..."
        log "openGauss" "Command: su - ${g_execute_user} -c \"source $g_conf_file && export && cd \"${g_src_dir}/src/test/ha\" && make ${check_mode} PART=${check_part}\""
        result=`su - ${g_execute_user} -c "source $g_conf_file && export && cd \"${g_src_dir}/src/test/ha\" && make ${check_mode} PART=${check_part}" >> ${g_log_file} 2>&1`

        if [ $? -ne 0 ]; then
            makehacheck_stop_time=$(date +%s.%N)
            log "openGauss" "makehacheck_stop_time="${makehacheck_stop_time}""
            error "openGauss" "Make ${check_mode} failed."
            cat ${g_src_dir}/src/test/ha/deploy_standby_multi_single.log 2>&1 | tee -a ${g_log_file}
            cat ${g_src_dir}/src/test/ha/deploy_standby_single.log 2>&1 | tee -a ${g_log_file}
            cat ${g_src_dir}/src/test/ha/gaussdb.log 2>&1 | tee -a ${g_log_file}
            cat ${g_src_dir}/src/test/ha/gs_ctl.log 2>&1 | tee -a ${g_log_file}
            cat ${g_src_dir}/src/test/ha/stop_database_multi_single.log 2>&1 | tee -a ${g_log_file}
            cat ${g_src_dir}/src/test/ha/stop_database_single.log 2>&1 | tee -a ${g_log_file}
            flag_makehacheck_fail=1
            return 1
        else
            makehacheck_stop_time=$(date +%s.%N)
            log "openGauss" "makehacheck_stop_time="${makehacheck_stop_time}""
            flag_makehacheck_fail=0
            log "openGauss" "Make ${check_mode} succeed."
        fi
    else
        error "openGauss" "Need current user to be root"
        return 1
    fi
}

function transport_info
{
    export g_aes_rand_key="$(date +%s%N)"
    g_trans_log_path=${g_master_log_path}/$(date +'%Y%m%d')_TID_${g_top_build_id}
    if [ -z "$g_master_ip" -o \
         -z "$g_master_user" -o \
         -z "$g_master_password" -o \
         -z "$g_master_log_path" -o \
         -z "$g_top_build_id" -o \
         -z "$g_cur_build_id" \
       ];then
        error "openGauss" "Fail transport logs. Option Retrans parameters have error(s),please check"
        return 1
    fi
    res=0
    sh ${real_path}/function/remote.sh -i $g_master_ip -u $g_master_user -p "$g_master_password" -m "ssh-cmd" -t 100000 -c "mkdir -p $g_trans_log_path" 2>&1 >> ${g_log_file}
    res=$(expr $res \| $?)
    for i in $(ls ${g_log_file%.log}*)
    do
        sh ${real_path}/function/remote.sh -i $g_master_ip -u $g_master_user -p "$g_master_password" -m "scp-out" -t 100000 -s $i -d $g_trans_log_path 2>&1 >> ${g_log_file}
        res=$(expr $res \| $?)
        #echo "DETAIL: ftp://mpp:mpp@$g_master_ip/log_fastcheck/${g_trans_log_path##*/}/${i##*/}" | tee -a ${g_log_file}
    done
    if [ $res -ne 0 ];then
        error "openGauss" "Fail transport logs"
    fi
}

function cal_interval_time
{
    if [ $# -ne 2 ];then
        printf "        "
        return 1
    fi
    local t=$(awk 'BEGIN{print '$2' - '$1'}')
    local z=${t%.*}
    printf "%.2d:%.2d:%.2d" $((z/60/60)) $((z/60%60)) $((z%60))
}
function judge_time
{
    local display="$1"
    local flag_fail=$2
    local start_time=$3
    local stop_time=$4
    if [ -z "$flag_fail" -o x"$flag_fail" = x"1" ];then
        flag_str="Failure"
    else
        flag_str="Success"
    fi
    interval_time="$(cal_interval_time $start_time $stop_time)"
    [ -z "$start_time" ] && start_time="                   " || start_time=$(date -d @$start_time '+%Y-%m-%d %T')
    [ -z "$stop_time" ] && stop_time="                   " || stop_time=$(date -d @$stop_time '+%Y-%m-%d %T')
    printf "|$display| $flag_str     | $start_time | $stop_time |  $interval_time   |\n"
}
function gen_report
{
    if [ "${check_mode}" = "hacheck_single" -o "${check_mode}" = "hacheck_multi_single" ];then
        printf "\
==============================================================================================
| Work Dir           | ${g_boot_dir}
| llt part           | ${check_part}
----------------------------------------------------------------------------------------------
| Stage              | Status      | Start Time          | Stop Time           |  Exec Time  |
|--------------------------------------------------------------------------------------------|
"
        judge_time " configure          " "${flag_configure_fail}"     "$configure_start_time"     "$configure_stop_time"
        judge_time " make clean         " "${flag_makeclean_fail}"     "$makeclean_start_time"     "$makeclean_stop_time"
        judge_time " make install       " "${flag_makeinstall_fail}"   "$makeinstall_start_time"   "$makeinstall_stop_time"
        judge_time " make ${check_mode} " "${flag_makehacheck_fail}" "$makehacheck_start_time" "$makehacheck_stop_time"
        interval_total_time="$(cal_interval_time $total_start_time $total_stop_time)"
        echo ----------------------------------------------------------------------------------------------
        echo "HACHECK TOTAL TIME: $interval_total_time"
        echo
    fi
}
function clean_env
{
    for u in $(echo $g_clean_user|sed 's/,/ /g')
    do
        cleanEnv_ipcs $u
    done

    ps -ef|grep ${g_src_dir}/src/test/ha|grep -v grep|awk '{print $2}'|xargs kill -9 > /dev/null 2>&1 
    # 清理环境
    rm -rf ${g_src_dir}/src/test/ha/results

    echo 1 > /proc/sys/vm/drop_caches
    echo 3 > /proc/sys/vm/drop_caches
    umask 022
}

# main function
function main
{
    total_start_time=$(date +%s.%N)

    echo ========================================================================================== | tee -a ${g_log_file}
    show_env_detail
    echo | tee -a ${g_log_file}

    echo ========================================================================================== | tee -a ${g_log_file}
    clean_env 2>&1 | tee -a ${g_log_file}
    echo | tee -a ${g_log_file}

    echo ========================================================================================== | tee -a ${g_log_file}
    mk_hacheck
    if [ $? -ne 0 ]; then
        error "openGauss" "Fail make hacheck"
    fi
    echo | tee -a ${g_log_file}

    total_stop_time=$(date +%s.%N)

    gen_report 2>&1 | tee -a ${g_log_file}

    reportfile="regression.out${check_part}"
    if [ -f $reportfile ];then
        cp ${g_src_dir}/src/test/regress/regression.out ${g_src_dir}/src/test/regress/${reportfile}
    fi

    if [ $g_retrans_enable -eq 1 ]; then
        transport_info
    fi

    if [ "${flag_makeinstall_fail}" == 1 ] || [ "${flag_makehacheck_fail}" == 1 ] || [ "${flag_makeclean_fail}" == 1 ];then
        error "openGauss" "Exit the make hacheck whith error."
        exit 1
    fi
}

# 帮助
function show_help
{
    echo
    echo "Usage: `basename $0` [option]"
    echo "options:"
    echo "-m                    Notes: check mode"
    echo "-p                    Notes: check part"
    echo "-c                    Notes: hdfs_conf_path"
    echo "-d                    Notes: top mission build id"
    echo "-h                    Notes: help"
    echo
    echo "Example:"
    echo "`basename $0`"
    echo
    return 0
}

# 获取入参
while getopts "m:p:c:d:h" options;
do
    case ${options} in
        m) check_mode="${OPTARG}"
        ;;
        p) check_part="${OPTARG}"
        ;;
        c) log_file="${OPTARG}"
        ;;
        d) top_build_id="${OPTARG}"
        ;;
        h) show_help
           exit 1
        ;;
        *) show_help
           exit 1
        ;;
    esac
done

if [ -z "$check_mode" ] ; then
    echo "ERROR: need -m"
    show_help
    exit 1
fi

arch=$(uname -m)

# 引入配置文件 及 function目录下定义的函数

g_conf_file=${real_path}/config.ini
chmod 644 $g_conf_file
source_file

# 定义日志目录
#g_log_dir=${real_path}/log
[ -z "${g_cur_build_id}" ] && g_cur_build_id=xx
[ -z "${g_top_build_id}" ] && g_top_build_id=xx
#g_log_file=${g_log_dir}/opengauss_ID_${g_cur_build_id}_$(date +'%Y%m%d_%H%M%S').log
#g_log_file=${g_log_dir}/opengauss_llt_$(printf "%.2d" ${check_part})_$(date +'%Y%m%d_%H%M%S').log
g_log_file=${log_file}

# 当前ip
g_agent_ip=$(echo "${SSH_CONNECTION}"|awk '{print $3}')

# 主程序入口
main
