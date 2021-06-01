#!/bin/bash
#############################################################################
# Copyright (c) 2020 Huawei Technologies Co.,Ltd.
#
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms
# and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
# ----------------------------------------------------------------------------
# Description  : gs_backup is a utility to back up or restore binary files and parameter files.
#############################################################################

declare version_mode='release'
declare binarylib_dir='None'

#detect platform information.
PLATFORM=32
bit=$(getconf LONG_BIT)
if [ "$bit" -eq 64 ]; then
    PLATFORM=64
fi

#get OS distributed version.
kernel=""
version=""
if [ -f "/etc/openEuler-release" ]
then
    kernel=$(cat /etc/openEuler-release | awk -F ' ' '{print $1}' | tr A-Z a-z)
    version=$(cat /etc/openEuler-release | awk -F '(' '{print $2}'| awk -F ')' '{print $1}' | tr A-Z a-z)
elif [ -f "/etc/centos-release" ]
then
    kernel=$(cat /etc/centos-release | awk -F ' ' '{print $1}' | tr A-Z a-z)
    version=$(cat /etc/centos-release | awk -F '(' '{print $2}'| awk -F ')' '{print $1}' | tr A-Z a-z)
elif [ -f "/etc/euleros-release" ]
then
    kernel=$(cat /etc/centos-release | awk -F ' ' '{print $1}' | tr A-Z a-z)
    version=$(cat /etc/centos-release | awk -F '(' '{print $2}'| awk -F ')' '{print $1}' | tr A-Z a-z)
else
    kernel=$(lsb_release -d | awk -F ' ' '{print $2}'| tr A-Z a-z)
    version=$(lsb_release -r | awk -F ' ' '{print $2}')
fi

## to solve kernel="name=openeuler"
if echo $kernel | grep -q 'openeuler'
then
    kernel="openeuler"
fi

if [ X"$kernel" == X"centos" ]; then
    dist_version="CentOS"
elif [ X"$kernel" == X"openeuler" ]; then
    dist_version="openEuler"
elif [ X"$kernel" == X"euleros" ]; then
    dist_version="EulerOS"
elif [ X"$kernel" == X"kylin" ]; then
    dist_version="Kylin"
else
    echo "We only support openEuler(aarch64), EulerOS(aarch64), CentOS, Kylin(aarch64) platform."
    echo "Kernel is $kernel"
    exit 1
fi

declare release_file_list="opengauss_release_list_${kernel}_single"
declare dest_list=""

##add platform architecture information
PLATFORM_ARCH=$(uname -p)
if [ "$PLATFORM_ARCH"X == "aarch64"X ] ; then
    if [ "$dist_version" != "openEuler" ] && [ "$dist_version" != "EulerOS" ] && [ "$dist_version" != "Kylin" ] ; then
        echo "We only support NUMA on openEuler(aarch64), EulerOS(aarch64), Kylin(aarch64) platform."
        exit 1
    fi

    release_file_list="opengauss_release_list_${kernel}_aarch64_single"
fi

##default install version storage path
declare server_version='openGauss'
declare server_name_for_package="$(echo ${server_version} | sed 's/ /-/g')" # replace blank with '-' for package name.
declare version_number=''

#######################################################################
##putout the version of server
#######################################################################
function print_version()
{
    echo "$version_number"
}

#######################################################################
## print help information
#######################################################################
function print_help()
{
    echo "Usage: $0 [OPTION]
    -h|--help                   show help information
    -V|--version                show version information
    -m|--version_mode           this values of paramenter is debug, release or memcheck, the default value is release
    -3rd|--binarylibs_dir       the parent directory of binarylibs
"
}

#######################################################################
##version 2.0.1
#######################################################################
function read_srv_version()
{
    cd $SCRIPT_DIR
    version_number=$(grep 'VERSION' opengauss.spec | awk -F "=" '{print $2}')
    echo "${server_name_for_package}-${version_number}">version.cfg
}

###################################
# get version number from globals.cpp
##################################
function read_srv_number()
{
    global_kernal="${ROOT_DIR}/src/common/backend/utils/init/globals.cpp"
    version_name="GRAND_VERSION_NUM"
    version_num=""
    line=$(cat $global_kernal | grep ^const* | grep $version_name)
    version_num1=${line#*=}
    #remove the symbol;
    version_num=$(echo $version_num1 | tr -d ";")
    #remove the blank
    version_num=$(echo $version_num)

    if echo $version_num | grep -qE '^92[0-9]+$'
    then
        # get the last three number
        latter=${version_num:2}
        echo "92.${latter}" >>${SCRIPT_DIR}/version.cfg
    else
        echo "Cannot get the version number from globals.cpp."
        exit 1
    fi
}

SCRIPT_DIR=$(cd $(dirname $0) && pwd)

test -d ${SCRIPT_DIR}/../../output || mkdir -p ${SCRIPT_DIR}/../../output && rm -fr ${SCRIPT_DIR}/../../output/*
output_path=$(cd ${SCRIPT_DIR}/../../output && pwd)

#########################################################################
##read command line paramenters
#######################################################################
while [ $# -gt 0 ]; do
    case "$1" in
        -h|--help)
            print_help
            exit 1
            ;;
        -v|--version)
            print_version
            exit 1
            ;;
        -m|--version_mode)
            if [ "$2"X = X ]; then
                echo "no given version number values"
                exit 1
            fi
            version_mode=$2
            shift 2
            ;;
        -3rd|--binarylibs_dir)
            if [ "$2"X = X ]; then
                echo "no given binarylib directory values"
                exit 1
            fi
            binarylib_dir=$2
            shift 2
            ;;
         *)
            echo "Internal Error: option processing error: $1" 1>&2
            echo "please input right paramtenter, the following command may help you"
            echo "./package.sh --help or ./package.sh -h"
            exit 1
    esac
done

read_srv_version

#######################################################################
## declare all package name
#######################################################################
declare version_string="${server_name_for_package}-${version_number}"
declare package_pre_name="${version_string}-${dist_version}-${PLATFORM}bit"
declare libpq_package_name="${package_pre_name}-Libpq.tar.gz"
declare tools_package_name="${package_pre_name}-tools.tar.gz"
declare kernel_package_name="${package_pre_name}.tar.bz2"
declare kernel_symbol_package_name="${package_pre_name}-symbol.tar.gz"
declare sha256_name="${package_pre_name}.sha256"

echo "[make single db] $(date +%y-%m-%d' '%T): script dir : ${SCRIPT_DIR}"
ROOT_DIR=$(dirname "$SCRIPT_DIR")
ROOT_DIR=$(dirname "$ROOT_DIR")
PLAT_FORM_STR=$(sh "${ROOT_DIR}/src/get_PlatForm_str.sh")
if [ "${PLAT_FORM_STR}"x == "Failed"x ]
then
    echo "We only support openEuler(aarch64), EulerOS(aarch64), CentOS, Kylin(aarch64) platform."
    exit 1;
fi

PG_REG_TEST_ROOT="${ROOT_DIR}/"
PMK_SCHEMA="${ROOT_DIR}/script/pmk_schema.sql"
declare LOG_FILE="${SCRIPT_DIR}/make_package.log" 
declare BUILD_DIR="${ROOT_DIR}/mppdb_temp_install"
BUILD_TOOLS_PATH="${ROOT_DIR}/binarylibs/buildtools/${PLAT_FORM_STR}"
BINARYLIBS_PATH="${ROOT_DIR}/binarylibs/dependency/${PLAT_FORM_STR}"
declare UPGRADE_SQL_DIR="${ROOT_DIR}/src/include/catalog/upgrade_sql"
if [ "${binarylib_dir}"x != "None"x ]
then
    echo "binarylib dir : ${binarylib_dir}"
    BUILD_TOOLS_PATH="${binarylib_dir}/buildtools/${PLAT_FORM_STR}"
    BINARYLIBS_PATH="${binarylib_dir}/dependency/${PLAT_FORM_STR}"
fi

gcc_version="7.3"

export CC=$BUILD_TOOLS_PATH/gcc$gcc_version/gcc/bin/gcc
export CXX=$BUILD_TOOLS_PATH/gcc$gcc_version/gcc/bin/g++
export LD_LIBRARY_PATH=$BUILD_TOOLS_PATH/gcc$gcc_version/gcc/lib64:$BUILD_TOOLS_PATH/gcc$gcc_version/isl/lib:$BUILD_TOOLS_PATH/gcc$gcc_version/mpc/lib/:$BUILD_TOOLS_PATH/gcc$gcc_version/mpfr/lib/:$BUILD_TOOLS_PATH/gcc$gcc_version/gmp/lib/:$LD_LIBRARY_PATH
export PATH=$BUILD_TOOLS_PATH/gcc$gcc_version/gcc/bin:$PATH

read_srv_number

#######################################################################
#  move pkgs to output directory
#######################################################################
function deploy_pkgs()
{
    for pkg in $@; do
        if [ -f $pkg ]; then
            mv $pkg $output_path/
        fi
    done
}

#######################################################################
#  Print log.
#######################################################################
log()
{
    echo "[make single db] $(date +%y-%m-%d' '%T): $@"
    echo "[make single db] $(date +%y-%m-%d' '%T): $@" >> "$LOG_FILE" 2>&1
}

#######################################################################
#  print log and exit.
#######################################################################
die()
{
    log "$@"
    echo "$@"
    exit 1
}

#######################################################################
##install gaussdb database contained server
#######################################################################
function install_gaussdb()
{
    cd $SCRIPT_DIR
    if [ "$version_mode" = "release"  ]; then
        chmod +x ./separate_debug_information.sh
        ./separate_debug_information.sh
        cd $SCRIPT_DIR
        mv symbols.tar.gz $kernel_symbol_package_name
        deploy_pkgs $kernel_symbol_package_name
    fi

    #insert the commitid to version.cfg as the upgrade app path specification
    export PATH=${BUILD_DIR}:$PATH
    export LD_LIBRARY_PATH=${BUILD_DIR}/lib:$LD_LIBRARY_PATH

    commitid=$(LD_PRELOAD='' ${BUILD_DIR}/bin/gaussdb -V | awk '{print $5}' | cut -d ")" -f 1)
    if [ -z "$commitid" ]
    then
        commitid=$(date "+%Y%m%d%H%M%S")
        commitid=${commitid:4:8}
    fi
    echo "${commitid}" >>${SCRIPT_DIR}/version.cfg
    echo "End insert commitid into version.cfg" >> "$LOG_FILE" 2>&1
}

#######################################################################
# copy directory's files list to $2
#######################################################################
function copy_files_list()
{
    for file in $(echo $1)
    do
        tar -cpf - $file  | ( cd $2; tar -xpf -  )
    done
}

#######################################################################
##copy target file into temporary directory temp
#######################################################################
function target_file_copy()
{
    cd ${BUILD_DIR}
    copy_files_list "$1" $2

    cp ${SCRIPT_DIR}/version.cfg ${BUILD_DIR}/temp
    cp -rf ${SCRIPT_DIR}/../../simpleInstall ${BUILD_DIR}/temp
    if [ $? -ne 0 ]; then
        die "copy ${SCRIPT_DIR}/version.cfg to ${BUILD_DIR}/temp failed"
    fi
        
    sed -i '/^process_cpu_affinity|/d'  $2/bin/cluster_guc.conf

    #generate tar file
    echo  "Begin generate ${kernel_package_name} tar file..."  >> "$LOG_FILE" 2>&1
    cd $2
    tar -jcvpf "${kernel_package_name}" ./* >> "$LOG_FILE" 2>&1
    cd '-'
    mv $2/"${kernel_package_name}" ./
    if [ $? -ne 0 ]; then
        die "generate ${kernel_package_name} failed."
    fi
    echo "End generate ${kernel_package_name}  tar file"  >> "$LOG_FILE" 2>&1

    #generate sha256 file
    sha256_name="${package_pre_name}.sha256"
    echo  "Begin generate ${sha256_name} sha256 file..."  >> "$LOG_FILE" 2>&1
    sha256sum "${kernel_package_name}" | awk -F" " '{print $1}' > "$sha256_name"
    if [ $? -ne 0 ]; then
        die "generate sha256 file failed."
    fi
    echo "End generate ${sha256_name} sha256 file"  >> "$LOG_FILE" 2>&1

    ###################################################
    # make server package
    ###################################################
    if [ -d "${2}" ]; then
        rm -rf ${2}
    fi
}

function target_file_copy_for_non_server()
{
    cd ${BUILD_DIR}
    copy_files_list "$1" $2
}

#######################################################################
##function make_package_prep have two actions
##1.parse release_file_list variable represent file
##2.copy target file into a newly created temporary directory temp
#######################################################################
function prep_dest_list()
{
    cd $SCRIPT_DIR
    releasefile=$1
    pkgname=$2

    local head=$(cat $releasefile | grep "\[$pkgname\]" -n | awk -F: '{print $1}')
    if [ ! -n "$head" ]; then
        die "error: ono find $pkgname in the $releasefile file "
    fi

    local tail=$(cat $releasefile | sed "1,$head d" | grep "^\[" -n | sed -n "1p" |  awk -F: '{print $1}')
    if [ ! -n "$tail" ]; then
        local all=$(cat $releasefile | wc -l)
        let tail=$all+1-$head
    fi

    dest_list=$(cat $releasefile | awk "NR==$head+1,NR==$tail+$head-1")
}

function make_package_srv()
{
    echo "Begin package server"
    cd $SCRIPT_DIR
    prep_dest_list $release_file_list 'server'

    rm -rf ${BUILD_DIR}/temp
    mkdir -p ${BUILD_DIR}/temp/etc
    target_file_copy "$dest_list" ${BUILD_DIR}/temp

    deploy_pkgs ${sha256_name} ${kernel_package_name}
    echo "make server(all) package success!"
}

#######################################################################
# Install all SQL files from src/distribute/include/catalog/upgrade_sql
# to INSTALL_DIR/bin/script/upgrade_sql.
# Package all SQL files and then verify them with SHA256.
#######################################################################
function make_package_upgrade_sql()
{
    echo "Begin to install upgrade_sql files..."
    UPGRADE_SQL_TAR="upgrade_sql.tar.gz"
    UPGRADE_SQL_SHA256="upgrade_sql.sha256"

    cd $SCRIPT_DIR
    mkdir -p ${BUILD_DIR}
    cd ${BUILD_DIR}
    rm -rf temp
    mkdir temp
    cd ${BUILD_DIR}/temp
    cp -r "${UPGRADE_SQL_DIR}" ./upgrade_sql
    [ $? -ne 0 ] && die "Failed to cp upgrade_sql files"
    tar -czf ${UPGRADE_SQL_TAR} upgrade_sql
    [ $? -ne 0 ] && die "Failed to package ${UPGRADE_SQL_TAR}"
    rm -rf ./upgrade_sql > /dev/null 2>&1

    sha256sum ${UPGRADE_SQL_TAR} | awk -F" " '{print $1}' > "${UPGRADE_SQL_SHA256}"
    [ $? -ne 0 ] && die "Failed to generate sha256 sum file for ${UPGRADE_SQL_TAR}"

    chmod 600 ${UPGRADE_SQL_TAR}
    chmod 600 ${UPGRADE_SQL_SHA256}

    deploy_pkgs ${UPGRADE_SQL_TAR} ${UPGRADE_SQL_SHA256}

    echo "Successfully packaged upgrade_sql files."
}

function make_package_libpq()
{
    cd $SCRIPT_DIR
    prep_dest_list $release_file_list 'libpq'

    rm -rf ${BUILD_DIR}/temp
    mkdir -p ${BUILD_DIR}/temp

    target_file_copy_for_non_server "$dest_list" ${BUILD_DIR}/temp

    cd ${BUILD_DIR}/temp
    echo "packaging libpq..."
    tar -zvcf "${libpq_package_name}" ./* >>"$LOG_FILE" 2>&1
    if [ $? -ne 0 ]; then
        die "$package_command ${libpq_package_name} failed"
    fi

    deploy_pkgs ${libpq_package_name}
    echo "install $pkgname tools is ${libpq_package_name} of ${output_path} directory " >> "$LOG_FILE" 2>&1
    echo "success!"
}

function make_package_tools()
{
    cd $SCRIPT_DIR
    prep_dest_list $release_file_list 'client'

    rm -rf ${BUILD_DIR}/temp
    mkdir -p ${BUILD_DIR}/temp

    cd ${BUILD_DIR}/

    target_file_copy_for_non_server "$dest_list" ${BUILD_DIR}/temp

    cd ${BUILD_DIR}/temp
    echo "packaging tools..."
    tar -zvcf "${tools_package_name}" ./* >>"$LOG_FILE" 2>&1
    if [ $? -ne 0 ]; then
        die "$package_command ${tools_package_name} failed"
    fi

    deploy_pkgs ${tools_package_name}
    echo "install $pkgname tools is ${tools_package_name} of ${output_path} directory " >> "$LOG_FILE" 2>&1
    echo "success!"
}

function spec_prep()
{
    cp opengauss.spec gauss.spec
}

#######################################################################
##  Check the installation package production environment
#######################################################################
function srv_pkg_bld()
{
    install_gaussdb
}

function srv_pkg_make()
{
    echo "Start package opengauss."
    make_package_srv
    make_package_libpq
    make_package_tools
    make_package_upgrade_sql
    echo "End package opengauss."
}

#############################################################
# main function
#############################################################
# 0. prepare spec file
spec_prep

# 1. build server
srv_pkg_bld

# 2. make package
srv_pkg_make

echo "now, all packages has finished!"
exit 0
