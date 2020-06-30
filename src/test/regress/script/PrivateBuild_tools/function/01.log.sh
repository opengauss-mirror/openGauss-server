
###################################################
# 把内容写入日志文件
###################################################
function _write_logfile()
{
    # g_log_file 为空,则默认设置为 /tmp/fi-preinstall.log
    if [ -z "$g_log_file" ]; then
        echo "ERROR: need global parameter g_log_file"
        exit 1
    fi

    #如果日志文件已经产生,可以直接写入(绝大部分情况符合此场景)
    if [ -f "${g_log_file}" ]; then
        echo "$*" >> "${g_log_file}"
        return 0
    fi

    #日志文件不存在,日志文件所在的目录已经存在,可以直接写入
    if [ -d "$(dirname ${g_log_file})" ]; then
        echo "$*" >> "${g_log_file}"
        return 0
    fi

    #日志文件所在的目录不存在
    mkdir -p "$(dirname ${g_log_file})" > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        #目录创建成功,可以写入日志
        echo "$*" >> "${g_log_file}"
        return 0
    fi

    return 1
}

###################################################
# 检查日志文件的权限
###################################################
function check_logfile()
{
    # g_log_file 为空,则默认设置为 /tmp/fi-preinstall.log
    if [ -z "$g_log_file" ]; then
        g_log_file="./log_privateBuildCheck"
    fi

    if [ ! -e "${g_log_file}" ]; then
        return 0
    fi

    #如果日志文件已经产生,可以直接写入(绝大部分情况符合此场景)
    if [ -f "${g_log_file}" -a -r "${g_log_file}" -a -w "${g_log_file}" ]; then
        return 0
    fi

    #日志文件存在且没有可写权限，返回失败
    echo -e "\033[31mERROR: ${g_log_file}: Permission denied, Please check your permission or clean /tmp/fi-preinstall.log and /tmp/diskmgt/\033[0m"
    return 1
}

###################################################
# 记录错误日志
###################################################
function error()
{
    if [ $# -lt 2 ] ;then
        echo "ERROR: error function need at least 2 parameter !!!"
        exit 1
    fi
    module="$1"
    shift
    msg="[$(date '+%Y-%m-%d %T')] [${module}] ERROR: $@"
    # 在界面上输出
    echo -e "\033[31m${msg}\033[0m"

    # 记录日志
    _write_logfile "${msg}"

}

###################################################
# 记录警告日志
###################################################
function warn()
{
    if [ $# -lt 2 ] ;then
        echo "ERROR: warn function need at least 2 parameter !!!"
        exit 1
    fi
    module="$1"
    shift
    msg="[$(date '+%Y-%m-%d %T')] [${module}] WARNING: $@"
    # 在界面上输出
    echo -e "\033[33m${msg}\033[0m"

    # 记录日志
    _write_logfile "${msg}"

}

###################################################
# 记录提示日志
###################################################
function log()
{
    if [ $# -lt 2 ] ;then
        echo "ERROR: log function need at least 2 parameter !!!"
        exit 1
    fi
    module="$1"
    shift
    msg="[$(date '+%Y-%m-%d %T')] [${module}] INFO: $@"
    # 在界面上输出
    echo "${msg}"

    # 记录日志
    _write_logfile "${msg}"
}

###################################################
# 记录一般日志(不往界面输出)
###################################################
function info()
{
    if [ $# -lt 2 ] ;then
        echo "ERROR: log function need at least 2 parameter !!!"
        exit 1
    fi
    module="$1"
    shift
    msg="[$(date '+%Y-%m-%d %T')] [${module}] INFO: $@"

    # 记录日志
    _write_logfile "${msg}"
}

###################################################
# 记录debug(不往界面输出)
###################################################
function debug()
{
    #如果 g_debug 没有定义,则直接返回
    if [ -z "${g_debug}" ]; then
        return
    fi

    if [ $# -lt 2 ] ;then
        echo "ERROR: debug function need at least 2 parameter !!!"
        exit 1
    fi
    module="$1"
    shift
    msg="[$(date '+%Y-%m-%d %T')] [${module}] DEBUG: $@"

    # 记录日志
    if [ "${g_debug}" -eq 1 ]; then
        _write_logfile "${msg}"
    fi
}

