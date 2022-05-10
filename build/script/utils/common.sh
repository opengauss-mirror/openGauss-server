#!/bin/bash
#######################################################################
# Copyright (c): 2020-2025, Huawei Tech. Co., Ltd.
# descript: Compile and pack openGauss
#           Return 0 means OK.
#           Return 1 means failed.
# version:  2.0
# date:     2020-08-08
#######################################################################

declare LOG_FILE="${SCRIPT_DIR}/makemppdb_pkg.log"
declare gaussdb_version='openGauss'
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
    exit $ERR_MKGS_FAILED
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
export PLAT_FORM_STR=$(sh "${ROOT_DIR}/src/get_PlatForm_str.sh")
if [ "${PLAT_FORM_STR}"x == "Failed"x -o "${PLAT_FORM_STR}"x == ""x ]
then
    echo "We only support openEuler(aarch64), EulerOS(aarch64), CentOS, Kylin(aarch64), Asianux platform."
    exit 1;
fi

if [[ "$PLAT_FORM_STR" =~ "euleros" ]]; then
    dist_version="EulerOS"
    if [ "$PLATFORM_ARCH"X == "aarch64"X ];then 
        GAUSSDB_EXTRA_FLAGS=" -D__USE_NUMA"
    fi
elif [[ "$PLAT_FORM_STR" =~ "centos" ]]; then
    dist_version="CentOS"
    if [ "$PLATFORM_ARCH"X == "aarch64"X ];then 
        GAUSSDB_EXTRA_FLAGS=" -D__USE_NUMA"
    fi
elif [[ "$PLAT_FORM_STR" =~ "openeuler" ]]; then
    dist_version="openEuler"
    if [ "$PLATFORM_ARCH"X == "aarch64"X ];then 
        GAUSSDB_EXTRA_FLAGS=" -D__USE_NUMA -D__ARM_LSE"
    fi
elif [[ "$PLAT_FORM_STR" =~ "kylin" ]]; then
    dist_version="Kylin"
    if [ "$PLATFORM_ARCH"X == "aarch64"X ];then 
        GAUSSDB_EXTRA_FLAGS=" -D__USE_NUMA"
    fi
elif [[ "$PLAT_FORM_STR" =~ "asianux" ]]; then
    dist_version="Asianux"
    if [ "$PLATFORM_ARCH"X == "aarch64"X ];then
        GAUSSDB_EXTRA_FLAGS=" -D__USE_NUMA"
    fi
else
    echo "We only support openEuler(aarch64), EulerOS(aarch64), CentOS, Kylin(aarch64), Asianux platform."
    echo "Kernel is $kernel"
    exit 1
fi

##add platform architecture information
if [ "$PLATFORM_ARCH"X == "aarch64"X ] ; then
    if [ "$dist_version" != "openEuler" ] && [ "$dist_version" != "EulerOS" ] && [ "$dist_version" != "Kylin" ] && [ "$dist_version" != "Asianux" ]; then
        echo "We only support NUMA on openEuler(aarch64), EulerOS(aarch64), Kylin(aarch64), Asianux platform."
        exit 1
    fi
fi

if [ "${binarylib_dir}" != 'None' ] && [ -d "${binarylib_dir}" ]; then
    BUILD_TOOLS_PATH="${binarylib_dir}/buildtools/${PLAT_FORM_STR}"
    PLATFORM_PATH="${binarylib_dir}/platform/${PLAT_FORM_STR}"
    BINARYLIBS_PATH="${binarylib_dir}/dependency"
else
    die "${binarylib_dir} not exist"
fi

declare INSTALL_TOOLS_DIR=${BINARYLIBS_PATH}/install_tools_${PLAT_FORM_STR}
declare UNIX_ODBC="${BINARYLIBS_PATH}/${PLAT_FORM_STR}/unixodbc"

# Comment 编译相关 
gcc_version="7.3" 
ccache -V >/dev/null 2>&1 && USE_CCACHE="ccache " ENABLE_CCACHE="--enable-ccache"
export CC="${USE_CCACHE}$BUILD_TOOLS_PATH/gcc$gcc_version/gcc/bin/gcc"
export CXX="${USE_CCACHE}$BUILD_TOOLS_PATH/gcc$gcc_version/gcc/bin/g++"
export LD_LIBRARY_PATH=$BUILD_TOOLS_PATH/gcc$gcc_version/gcc/lib64:$BUILD_TOOLS_PATH/gcc$gcc_version/isl/lib:$BUILD_TOOLS_PATH/gcc$gcc_version/mpc/lib/:$BUILD_TOOLS_PATH/gcc$gcc_version/mpfr/lib/:$BUILD_TOOLS_PATH/gcc$gcc_version/gmp/lib/:$LD_LIBRARY_PATH
export PATH=$BUILD_TOOLS_PATH/gcc$gcc_version/gcc/bin:$PATH
export JAVA_HOME=${binarylib_dir}/platform/huaweijdk8/${PLATFORM_ARCH}/jdk

declare ERR_MKGS_FAILED=1
declare MKGS_OK=0


gaussdb_200_file="${binarylib_dir}/buildtools/license_control/gaussdb.version.GaussDB200"
gaussdb_300_file="${binarylib_dir}/buildtools/license_control/gaussdb.version.GaussDB300"
gaussdb_200_standard_file="${binarylib_dir}/buildtools/license_control/gaussdb.license.GaussDB200_Standard"
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
declare gaussdb_name_for_package="$(echo ${gaussdb_version} | sed 's/ /-/g')"
declare version_string="${gaussdb_name_for_package}-${version_number}"
declare package_pre_name="${version_string}-${dist_version}-${PLATFORM}bit"
declare libpq_package_name="${package_pre_name}-Libpq.tar.gz"
declare tools_package_name="${package_pre_name}-tools.tar.gz"
declare kernel_package_name="${package_pre_name}.tar.bz2"
declare symbol_package_name="${package_pre_name}-symbol.tar.gz"
declare sha256_name="${package_pre_name}.sha256"


