#!/bin/bash
# Copyright (c) Huawei Technologies Co., Ltd. 2020-2025. All rights reserved.
# descript: Compile and pack openGauss

declare LOG_FILE="${SCRIPT_DIR}/makemppdb_pkg.log"
declare product_name='openGauss'
declare PLATFORM_ARCH=$(uname -p)
declare package_path=${ROOT_DIR}/output
declare install_package_format="tar"
declare PLATFORM=32
bit=$(getconf LONG_BIT)
if [ "$bit" -eq 64 ]; then
    declare PLATFORM=64
fi

# 公共方法
#######################################################################
##putout the version of gaussdb
#######################################################################
function print_version()
{
    echo "$version_number"
}

#######################################################################
#  Print log.
#######################################################################
function log()
{
    echo "[makegaussdb] $(date +%y-%m-%d' '%T): $@"
    echo "[makegaussdb] $(date +%y-%m-%d' '%T): $@" >> "$LOG_FILE" 2>&1
}

#######################################################################
#  print log and exit.
#######################################################################
function die()
{
    log "$@"
    echo "$@"
    exit $ERR_FAILED
}

#######################################################################
##select package command accroding to install_package_format
#######################################################################
function select_package_command()
{
    case "$install_package_format" in
        tar)
            tar='tar'
            option=' -zcvf'
            package_command="$tar$option"
            ;;
    esac
}
select_package_command

#######################################################################
##get os dist version
#######################################################################
if [[ -f "/etc/openEuler-release" ]]; then
    os_name="openEuler"
elif [[ -f "/etc/euleros-release" ]]; then
    os_name="EulerOS"
elif [[ -f "/etc/centos-release" ]]; then
    os_name="CentOS"
elif [[ -f "/etc/openEuler-release" ]]; then
    os_name="openEuler"
elif [[ -f "/etc/FusionOS-release" ]]; then
    os_name="FusionOS"
elif [[ -f "/etc/kylin-release" ]]; then
    os_name="Kylin"
elif [[ -f "/etc/asianux-release" ]]; then
    os_name="Asianux"
elif [[ -f "/etc/CSIOS-release" ]]; then
    os_name="CSIOS"
else
    os_name=$(lsb_release -d | awk -F ' ' '{print $2}'| tr A-Z a-z | sed 's/.*/\L&/; s/[a-z]*/\u&/g')
fi

##add platform architecture information
if [ "$PLATFORM_ARCH"X == "aarch64"X ] ; then
    if [ "$os_name" != "openEuler" ] && [ "$os_name" != "EulerOS" ] && [ "$os_name" != "FusionOS" ] && [ "$os_name" != "Kylin" ] && [ "$dist_version" != "Asianux" ] && [ "$os_name" != "CSIOS" ]; then
        echo "We only support NUMA on openEuler(aarch64), EulerOS(aarch64), FusionOS(aarch64), Kylin(aarch64), Asianux, CSIOS(aarch64) platform."
        exit 1
    fi
    GAUSSDB_EXTRA_FLAGS=" -D__USE_NUMA"
fi

if [ "${binarylib_dir}" != 'None' ] && [ -d "${binarylib_dir}" ]; then
    BUILD_TOOLS_PATH="${binarylib_dir}/buildtools"
    PLATFORM_PATH="${binarylib_dir}/kernel/platform"
    BINARYLIBS_PATH="${binarylib_dir}/kernel/dependency"
else
    die "${binarylib_dir} doesn't exist."
fi

declare INSTALL_TOOLS_DIR=${binarylib_dir}/install_tools
declare UNIX_ODBC="${BINARYLIBS_PATH}/unixodbc"

# Comment 编译相关
if [ -d "${USE_CCACHE}$BUILD_TOOLS_PATH/gcc10.3" ]; then
    gcc_version="10.3"
else
    gcc_version="7.3"
fi

if [ "$PLATFORM_ARCH"X == "loongarch64"X ];then
    gcc_version="8.3"
fi

if [ "$PLATFORM_ARCH"X == "aarch64"X ] && [ "$gcc_version" == "10.3" ]; then
    gcc_sub_version="1"
else
    gcc_sub_version="0"
fi
ccache -V >/dev/null 2>&1 && USE_CCACHE="ccache " ENABLE_CCACHE="--enable-ccache"
export CC="${USE_CCACHE}$BUILD_TOOLS_PATH/gcc$gcc_version/gcc/bin/gcc"
export CXX="${USE_CCACHE}$BUILD_TOOLS_PATH/gcc$gcc_version/gcc/bin/g++"
export LD_LIBRARY_PATH=$BUILD_TOOLS_PATH/gcc$gcc_version/gcc/lib64:$BUILD_TOOLS_PATH/gcc$gcc_version/isl/lib:$BUILD_TOOLS_PATH/gcc$gcc_version/mpc/lib/:$BUILD_TOOLS_PATH/gcc$gcc_version/mpfr/lib/:$BUILD_TOOLS_PATH/gcc$gcc_version/gmp/lib/:$LD_LIBRARY_PATH
export LD_LIBRARY_PATH=$BINARYLIBS_PATH/zstd/lib:$LD_LIBRARY_PATH
export PATH=$BUILD_TOOLS_PATH/gcc$gcc_version/gcc/bin:$PATH
export JAVA_HOME=${PLATFORM_PATH}/huaweijdk8/${PLATFORM_ARCH}/jdk

declare ERR_FAILED=1
declare ERR_OK=0

gaussdb_200_file="${BUILD_TOOLS_PATH}/license_control/gaussdb.version.GaussDB200"
gaussdb_300_file="${BUILD_TOOLS_PATH}/license_control/gaussdb.version.GaussDB300"
gaussdb_200_standard_file="${BUILD_TOOLS_PATH}/license_control/gaussdb.license.GaussDB200_Standard"
gaussdb_version_file="${ROOT_DIR}/src/gausskernel/process/postmaster/gaussdb_version.cpp"


if [ -f "$SCRIPT_DIR/gaussdb.ver" ];then
    declare version_number=$(cat ${SCRIPT_DIR}/gaussdb.ver | grep 'VERSION' | awk -F "=" '{print $2}')
else
    echo "gaussdb.ver not found!"
    exit 1
fi

declare release_file_list="${PLATFORM_ARCH}_${product_mode}_list"

#######################################################################
## declare all package name
#######################################################################
declare version_string="${product_name}-${version_number}"
declare package_pre_name="${version_string}-${os_name}-${PLATFORM}bit"
declare libpq_package_name="${package_pre_name}-Libpq.tar.gz"
declare tools_package_name="${package_pre_name}-tools.tar.gz"
declare kernel_package_name="${package_pre_name}.tar.bz2"
declare symbol_package_name="${package_pre_name}-symbol.tar.gz"
declare sha256_name="${package_pre_name}.sha256"
