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
elif [ -f "/etc/os-release" ]
then
        kernel=$(source /etc/os-release; echo $ID)
        version=$(source /etc/os-release; echo $VERSION_ID)
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
elif [ X"$kernel" == X"neokylin" ]; then
    dist_version="neokylin"
elif [ X"$kernel" == X"kylin" ]; then
    dist_version="kylin"
else
    echo "We only support openEuler(aarch64), neokylin(aarch64), kylin(aarch64), CentOS platform."
    echo "Kernel is $kernel"
    exit 1
fi

gcc_version="8.2"
##add platform architecture information
PLATFORM_ARCH=$(uname -p)
if [ "$PLATFORM_ARCH"X == "aarch64"X ] ; then
    if [ "$dist_version" != "openEuler" ] && [ "$dist_version" != "neokylin" ] && [ "$dist_version" != "kylin" ] ; then
        echo "We only support NUMA on openEuler(aarch64), neokylin(aarch64), kylin(aarch64) platform."
        exit 1
    fi
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


SCRIPT_PATH=${0}
FIRST_CHAR=$(expr substr "$SCRIPT_PATH" 1 1)
if [ "$FIRST_CHAR" = "/" ]; then
    SCRIPT_PATH=${0}
else
    SCRIPT_PATH="$(pwd)/${SCRIPT_PATH}"
fi

SCRIPT_DIR=$(cd $(dirname $SCRIPT_PATH) && pwd)

package_path=$SCRIPT_DIR

#######################################################################
##version 1.0.0
#######################################################################
function read_srv_version()
{
    cd $SCRIPT_DIR
    version_number='1.0.0'
    echo "${server_name_for_package}-${version_number}">version.cfg
    #auto read the number from kernal globals.cpp, no need to change it here
}

read_srv_version


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


#######################################################################
## declare all package name
#######################################################################
declare version_string="${server_name_for_package}-${version_number}"
declare package_pre_name="${version_string}-${dist_version}-${PLATFORM}bit"
declare server_package_name="${package_pre_name}.tar.gz"
declare symbol_package_name="${package_pre_name}-symbol.tar.gz"
declare libpq_package_name="${package_pre_name}-Libpq.tar.gz"

echo "[make single db] $(date +%y-%m-%d' '%T): script dir : ${SCRIPT_DIR}"
ROOT_DIR=$(dirname "$SCRIPT_DIR")
PLAT_FORM_STR=$(sh "${ROOT_DIR}/src/get_PlatForm_str.sh")
if [ "${PLAT_FORM_STR}"x == "Failed"x ]
then
    echo "We only support openEuler(aarch64), CentOS platform."
    exit 1;
fi

PG_REG_TEST_ROOT="${ROOT_DIR}/"
PMK_SCHEMA="${ROOT_DIR}/script/pmk_schema.sql"
declare LOG_FILE="${ROOT_DIR}/package/make_package.log"
declare BUILD_DIR="${ROOT_DIR}/dest"
BUILD_TOOLS_PATH="${ROOT_DIR}/binarylibs/buildtools/${PLAT_FORM_STR}"
BINARYLIBS_PATH="${ROOT_DIR}/binarylibs/dependency/${PLAT_FORM_STR}"
if [ "${binarylib_dir}"x != "None"x ]
then
    echo "binarylib dir : ${binarylib_dir}"
    BUILD_TOOLS_PATH="${binarylib_dir}/buildtools/${PLAT_FORM_STR}"
    BINARYLIBS_PATH="${binarylib_dir}/dependency/${PLAT_FORM_STR}"
fi

export CC=$BUILD_TOOLS_PATH/gcc$gcc_version/gcc/bin/gcc
export CXX=$BUILD_TOOLS_PATH/gcc$gcc_version/gcc/bin/g++
export LD_LIBRARY_PATH=$BUILD_TOOLS_PATH/gcc$gcc_version/gcc/lib64:$BUILD_TOOLS_PATH/gcc$gcc_version/isl/lib:$BUILD_TOOLS_PATH/gcc$gcc_version/mpc/lib/:$BUILD_TOOLS_PATH/gcc$gcc_version/mpfr/lib/:$BUILD_TOOLS_PATH/gcc$gcc_version/gmp/lib/:$LD_LIBRARY_PATH
export PATH=$BUILD_TOOLS_PATH/gcc$gcc_version/gcc/bin:$PATH

###################################
# get version number from globals.cpp
##################################
function read_svr_number()
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
read_svr_number

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
##copy all file of script directory to target directory
#######################################################################
function copy_script_file()
{
    target_file=$1
    target_dir=$2
    cp -rf $target_file/script/ $target_dir/ &&
        cp -f ${BINARYLIBS_PATH}/openssl/comm/bin/openssl $target_dir/ &&

    find $target_dir/script/ -type f -print0 | xargs -0 -n 10 -r dos2unix > /dev/null 2>&1 &&
    find $target_dir/script/gspylib/inspection/ -name d2utmp* -print0 | xargs -0 rm -rf &&
    cp -rf $target_file/script/gspylib/inspection/lib/checknetspeed/speed_test* $target_dir/script/gspylib/inspection/lib/checknetspeed/ &&
    cp -rf $target_file/script/gspylib/inspection/lib/*.png $target_dir/script/gspylib/inspection/lib/ &&

    if [ $? -ne 0 ]; then
        die "cp -r $target_file $target_dir failed "
    fi
}


#######################################################################
##install gaussdb database contained server
#######################################################################
function install_gaussdb()
{
    cd $ROOT_DIR
    copy_script_file "$script_dir/" ${BUILD_DIR}/bin/

    cp ${BUILD_DIR}/bin/script/gspylib/etc/sql/pmk_schema.sql ${BUILD_DIR}/share/postgresql/
    if [ -f ${BUILD_DIR}/bin/script/gspylib/etc/sql/pmk_schema_single_inst.sql ]; then
        cp ${BUILD_DIR}/bin/script/gspylib/etc/sql/pmk_schema_single_inst.sql ${BUILD_DIR}/share/postgresql/
    fi

    cd $ROOT_DIR
    cp -f ${script_dir}/other/transfer.py ${BUILD_DIR}/bin
    if [ $? -ne 0 ]; then
       die "cp -f ${script_dir}/script/transfer.py ${BUILD_DIR}/bin failed."
    fi
    dos2unix ${BUILD_DIR}/bin/transfer.py > /dev/null 2>&1

    cd $SCRIPT_DIR
    if [ "$version_mode" = "release"  ]; then
       chmod +x ./separate_debug_information.sh
       ./separate_debug_information.sh
       cd $SCRIPT_DIR
       mv symbols.tar.gz $symbol_package_name
    fi

    #insert the commitid to version.cfg as the upgrade app path specification
    export PATH=${BUILD_DIR}:$PATH
    export LD_LIBRARY_PATH=$GAUSSHOME/lib:$LD_LIBRARY_PATH

    commitid=$(LD_PRELOAD=''  ${BUILD_DIR}/bin/gaussdb -V | awk '{print $5}' | cut -d ")" -f 1)
    echo "${commitid}" >>${SCRIPT_DIR}/version.cfg
    echo "End insert commitid into version.cfg" >> "$LOG_FILE" 2>&1
}

function read_script_file()
{
    cd $SCRIPT_DIR
    if [ -e script_file ]
    then
        rm -rf script_file
    fi
    touch script_file
    for element in `ls ${BUILD_DIR}/script`
    do
        dir_or_file=${BUILD_DIR}/script"/"$element
        if [ ! -d $dir_or_file ]
        then
            echo "./script/"$(basename $dir_or_file) >> script_file
        fi
    done
}

function copy_script_file()
{
    target_file=$1
    target_dir=$2

    cp -rf $target_file/script/ $target_dir/ &&
    cp -f ${BINARYLIBS_PATH}/openssl/comm/bin/openssl $target_dir/ &&

    find $target_dir/script/ -type f -print0 | xargs -0 -n 10 -r dos2unix > /dev/null 2>&1 &&
    find $target_dir/script/gspylib/inspection/ -name d2utmp* -print0 | xargs -0 rm -rf &&
    cp -rf $target_file/script/gspylib/inspection/lib/checknetspeed/speed_test* $target_dir/script/gspylib/inspection/lib/checknetspeed/ &&
    cp -rf $target_file/script/gspylib/inspection/lib/*.png $target_dir/script/gspylib/inspection/lib/ &&

    if [ $? -ne 0 ]; then
        die "cp -r $target_file $target_dir failed "
    fi
}

declare tar_name="${package_pre_name}.tar.bz2"
declare sha256_name=''
declare script_dir="${ROOT_DIR}/src/manager/om"
declare root_script=''
declare bin_script=''

#######################################################################
# copy directory's files list to $2
#######################################################################
function copy_files_list()
{
    for element in `ls $1`
    do
        dir_or_file=$1"/"$element
        if [ -d $dir_or_file ]
        then
            copy_files_list $dir_or_file $2
        else
            tar -cpf - $file  | ( cd $2; tar -xpf -  )
        fi
    done
}
#######################################################################
##copy target file into temporary directory temp
#######################################################################
function target_file_copy()
{
    cd ${BUILD_DIR}
    for file in $(echo $1)
    do
        copy_files_list $file $2
    done
    read_script_file
    cd $SCRIPT_DIR
    sed 's/^\./\.\/bin/' script_file >binfile
    root_script=$(cat script_file)
    sed -i '/gs_backup/d' binfile
    sed -i '/gs_check/d' binfile
    sed -i '/gs_checkos/d' binfile
    sed -i '/gs_checkperf/d' binfile
    sed -i '/gs_collector/d' binfile
    sed -i '/gs_expand/d' binfile
    sed -i '/gs_install/d' binfile
    sed -i '/gs_om/d' binfile
    sed -i '/gs_postuninstall/d' binfile
    sed -i '/gs_preinstall/d' binfile
    sed -i '/gs_replace/d' binfile
    sed -i '/gs_shrink/d' binfile
    sed -i '/gs_ssh/d' binfile
    sed -i '/gs_sshexkey/d' binfile
    sed -i '/gs_uninstall/d' binfile
    sed -i '/gs_upgradectl/d' binfile
    sed -i '/gs_lcctl/d' binfile
    sed -i '/gs_wsr/d' binfile
    sed -i '/gs_gucZenith/d' binfile
    sed -i '/gs_expansion/d' binfile
    bin_script=$(cat binfile)
    rm binfile script_file
    cd $BUILD_DIR
    for file in $(echo $bin_script)
    do
        tar -cpf - $file  | ( cd $2; tar -xpf -  )
    done

    # create script/gspylib/clib, put file encrypt, libcrypto.so.1.1,libssl.so.1.1
    rm -rf $BUILD_DIR/script/gspylib/clib
    mkdir -p $BUILD_DIR/script/gspylib/clib
    cp $BUILD_DIR/lib/libstdc++.so.6 $BUILD_DIR/script/gspylib/clib
    cp $BUILD_DIR/lib/libssl.so.1.1 $BUILD_DIR/script/gspylib/clib
    cp $BUILD_DIR/lib/libcrypto.so.1.1 $BUILD_DIR/script/gspylib/clib
    cp $BUILD_DIR/bin/encrypt $BUILD_DIR/script/gspylib/clib

    # copy script dir to temp path
    cp -rf $BUILD_DIR/script/gspylib/ $2/bin/script/
    cp -rf $BUILD_DIR/script/impl/ $2/bin/script/
    cp -rf $BUILD_DIR/script/local/ $2/bin/script/

    # clean the files which under bin/script/ is not be used
    for x in $(ls $2/bin/script/)
    do
        filename="$2/bin/script/$x"
        if [[ "$filename" = *"__init__.py" ]];then
            continue
        elif [ -d "$filename" ];then
            continue
        elif [ -f "$filename" ];then
            rm -f "$filename"
        fi
    done

    chmod -R +x $2/bin/script/
    sed -i '/^process_cpu_affinity|/d'  $2/bin/cluster_guc.conf

    #generate tar file
    echo  "Begin generate ${tar_name} tar file..."  >> "$LOG_FILE" 2>&1
    cd $2
    tar -jcvpf "${tar_name}" ./* >> "$LOG_FILE" 2>&1
    cd '-'
    mv $2/"${tar_name}" ./
    if [ $? -ne 0 ]; then
        die "generate ${tar_name} failed."
    fi
    echo "End generate ${tar_name}  tar file"  >> "$LOG_FILE" 2>&1

    #generate sha256 file
    sha256_name="${package_pre_name}.sha256"
    echo  "Begin generate ${sha256_name} sha256 file..."  >> "$LOG_FILE" 2>&1
    sha256sum "${tar_name}" | awk -F" " '{print $1}' > "$sha256_name"
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
    mkdir -p ${2}
    mv ${tar_name} ${sha256_name} $2
    for file in $(echo $root_script)
    do
        tar -cpf - $file  | ( cd $2; tar -xpf -  )
    done

    # copy script dir to temp path
    cp -rf $BUILD_DIR/script/gspylib/ $2/script/
    cp -rf $BUILD_DIR/script/impl/ $2/script/
    cp -rf $BUILD_DIR/script/local/ $2/script/
    chmod -R +x $2/script/
}

function replace_omtools_version()
{
    gs_version=$(grep DEF_GS_VERSION ${PG_REG_TEST_ROOT}/src/include/pg_config.h | awk -F '"' '{print $2}')
    echo $gs_version | grep -e "${server_version}.*build.*compiled.*"  > /dev/null 2>&1
    if [ $? -ne 0 ]; then
        die "Failed to get gs_version from pg_config.h."
    fi

    if [ -f "$1"/script/gspylib/common/VersionInfo.py ] ; then
        sed -i -e "s/COMMON_VERSION = \"Gauss200 OM VERSION\"/COMMON_VERSION = \"$(echo ${gs_version})\"/g" -e "s/__GAUSS_PRODUCT_STRING__/$server_version/g" $1/script/gspylib/common/VersionInfo.py
        if [ $? -ne 0 ]; then
            die "Failed to replace OM tools version number."
        fi
    else
        sed -i "s/COMMON_VERSION = \"Gauss200 OM VERSION\"/COMMON_VERSION = \"$(echo ${gs_version})\"/g" $1/script/gspylib/os/gsOSlib.py
        if [ $? -ne 0 ]; then
            die "Failed to replace OM tools version number."
        fi
    fi

    grep 'CATALOG_VERSION_NO' ${PG_REG_TEST_ROOT}/src/include/catalog/catversion.h >/dev/null  2>&1
    if [ $? -ne 0 ]; then
        die "Failed to get catalog_version from catversion.h."
    fi

    catalog_version=$(grep 'CATALOG_VERSION_NO' ${PG_REG_TEST_ROOT}/src/include/catalog/catversion.h | uniq | awk -F ' ' '{print $NF}')
    if [ x"$catalog_version" == x"" ]; then
        die "Failed to get catalog_version from catversion.h."
    fi

    sed -i "s/TABLESPACE_VERSION_DIRECTORY = .*/TABLESPACE_VERSION_DIRECTORY = \"PG_9.2_$(echo ${catalog_version})\"/g" $1/script/gspylib/common/Common.py
    if [ $? -ne 0 ]; then
        die "Failed to replacecatalog_version number."
    fi

}


function make_package_srv()
{
    cd $SCRIPT_DIR
    copydest="./bin
        ./etc
        ./share
        ./lib
        ./include"
    mkdir -p ${BUILD_DIR}
    cd ${BUILD_DIR}
    rm -rf temp
    mkdir temp
    copy_script_file "$script_dir/" ${BUILD_DIR}
    mkdir -p ${BUILD_DIR}/temp/etc
    target_file_copy "$copydest" ${BUILD_DIR}/temp
    cd ${BUILD_DIR}/temp
    cp ${SCRIPT_DIR}/version.cfg ${BUILD_DIR}/temp
    if [ $? -ne 0 ]; then
        die "copy ${SCRIPT_DIR}/version.cfg to ${BUILD_DIR}/temp failed"
    fi

    replace_omtools_version ${BUILD_DIR}/temp/
    #copy inspection lib
    mkdir -p ${BUILD_DIR}/temp/script/gspylib/inspection/output/log/ &&
    mkdir -p ${BUILD_DIR}/temp/script/gspylib/inspection/output/nodes/ &&
    mkdir -p ${BUILD_DIR}/temp/script/gspylib/inspection/lib/asn1crypto/ &&
    mkdir -p ${BUILD_DIR}/temp/script/gspylib/inspection/lib/bcrypt/ &&
    mkdir -p ${BUILD_DIR}/temp/script/gspylib/inspection/lib/cffi/ &&
    mkdir -p ${BUILD_DIR}/temp/script/gspylib/inspection/lib/cryptography/ &&
    mkdir -p ${BUILD_DIR}/temp/script/gspylib/inspection/lib/idna/ &&
    mkdir -p ${BUILD_DIR}/temp/script/gspylib/inspection/lib/nacl/ &&
    mkdir -p ${BUILD_DIR}/temp/script/gspylib/inspection/lib/pyasn1/ &&
    mkdir -p ${BUILD_DIR}/temp/script/gspylib/inspection/lib/pycparser/ &&
    mkdir -p ${BUILD_DIR}/temp/script/gspylib/inspection/lib/OpenSSL/ &&
    mkdir -p ${BUILD_DIR}/temp/script/gspylib/inspection/lib/psutil/ &&
    mkdir -p ${BUILD_DIR}/temp/script/gspylib/inspection/lib/netifaces/ &&
    mkdir -p ${BUILD_DIR}/temp/script/gspylib/inspection/lib/paramiko/ &&

    cp -rf ${BINARYLIBS_PATH}/install_tools/asn1crypto/ ${BUILD_DIR}/temp/script/gspylib/inspection/lib/
    cp -rf ${BINARYLIBS_PATH}/install_tools/bcrypt/ ${BUILD_DIR}/temp/script/gspylib/inspection/lib/
    cp -rf ${BINARYLIBS_PATH}/install_tools/cffi/ ${BUILD_DIR}/temp/script/gspylib/inspection/lib/
    cp -rf ${BINARYLIBS_PATH}/install_tools/cryptography/ ${BUILD_DIR}/temp/script/gspylib/inspection/lib/
    cp -rf ${BINARYLIBS_PATH}/install_tools/idna/ ${BUILD_DIR}/temp/script/gspylib/inspection/lib/
    cp -rf ${BINARYLIBS_PATH}/install_tools/nacl/ ${BUILD_DIR}/temp/script/gspylib/inspection/lib/
    cp -rf ${BINARYLIBS_PATH}/install_tools/pyasn1/ ${BUILD_DIR}/temp/script/gspylib/inspection/lib/
    cp -rf ${BINARYLIBS_PATH}/install_tools/pycparser/ ${BUILD_DIR}/temp/script/gspylib/inspection/lib/
    cp -rf ${BINARYLIBS_PATH}/install_tools/OpenSSL/ ${BUILD_DIR}/temp/script/gspylib/inspection/lib/
    cp -rf ${BINARYLIBS_PATH}/install_tools/ipaddress.py ${BUILD_DIR}/temp/script/gspylib/inspection/lib/
    cp -rf ${BINARYLIBS_PATH}/install_tools/six.py ${BUILD_DIR}/temp/script/gspylib/inspection/lib/
    cp -rf ${BINARYLIBS_PATH}/install_tools/_cffi_backend.py ${BUILD_DIR}/temp/script/gspylib/inspection/lib/
    cp -rf ${BINARYLIBS_PATH}/install_tools/_cffi_backend.so_UCS2 ${BUILD_DIR}/temp/script/gspylib/inspection/lib/
    cp -rf ${BINARYLIBS_PATH}/install_tools/_cffi_backend.so_UCS4 ${BUILD_DIR}/temp/script/gspylib/inspection/lib/
    cp -rf ${BINARYLIBS_PATH}/install_tools/psutil/ ${BUILD_DIR}/temp/script/gspylib/inspection/lib/
    cp -rf ${BINARYLIBS_PATH}/install_tools/netifaces/ ${BUILD_DIR}/temp/script/gspylib/inspection/lib/
    cp -rf ${BINARYLIBS_PATH}/install_tools/paramiko/ ${BUILD_DIR}/temp/script/gspylib/inspection/lib/
    cp -r ${BINARYLIBS_PATH}/install_tools ./install_tools

    mkdir -p ./lib

    mv ./install_tools/asn1crypto               ./lib
    mv ./install_tools/bcrypt                   ./lib
    mv ./install_tools/cffi                     ./lib
    mv ./install_tools/cryptography             ./lib
    mv ./install_tools/idna                     ./lib
    mv ./install_tools/nacl                     ./lib
    mv ./install_tools/pyasn1                   ./lib
    mv ./install_tools/pycparser                ./lib
    mv ./install_tools/OpenSSL                  ./lib
    mv ./install_tools/ipaddress.py             ./lib
    mv ./install_tools/six.py                   ./lib
    mv ./install_tools/_cffi_backend.py         ./lib
    mv ./install_tools/_cffi_backend.so_UCS2    ./lib
    mv ./install_tools/_cffi_backend.so_UCS4    ./lib
    mv ./install_tools/paramiko                 ./lib
    mv ./install_tools/psutil                   ./lib
    mv ./install_tools/netifaces                ./lib

    rm -r ./install_tools
        tar -zvcf "${server_package_name}" ./* >>"$LOG_FILE" 2>&1
    if [ $? -ne 0 ]; then
        die "$package_command ${server_package_name} failed"
    fi
    mv ${server_package_name} ${package_path}
    echo "install $pkgname tools is ${server_package_name} of ${package_path} directory " >> "$LOG_FILE" 2>&1
    echo "success!"
}

function target_file_copy_for_non_server()
{
    for file in $(echo $1)
    do
        tar -cpf - $file  | ( cd $2; tar -xpf -  )
    done
}

function make_package_libpq()
{
    cd $SCRIPT_DIR
    dest="./lib/libpq.a
./lib/libpq.so
./lib/libpq.so.5
./lib/libpq.so.5.5
./lib/libconfig.so
./lib/libconfig.so.4
./lib/libcrypto.so
./lib/libcrypto.so.1.1
./lib/libssl.so
./lib/libssl.so.1.1
./lib/libpgport_tool.so
./lib/libpgport_tool.so.1
./lib/libgssapi_krb5_gauss.so
./lib/libgssapi_krb5_gauss.so.2
./lib/libgssapi_krb5_gauss.so.2.2
./lib/libgssrpc_gauss.so
./lib/libgssrpc_gauss.so.4
./lib/libgssrpc_gauss.so.4.2
./lib/libk5crypto_gauss.so
./lib/libk5crypto_gauss.so.3
./lib/libk5crypto_gauss.so.3.1
./lib/libkrb5support_gauss.so
./lib/libkrb5support_gauss.so.0
./lib/libkrb5support_gauss.so.0.1
./lib/libkrb5_gauss.so
./lib/libkrb5_gauss.so.3
./lib/libkrb5_gauss.so.3.3
./lib/libcom_err_gauss.so
./lib/libcom_err_gauss.so.3
./lib/libcom_err_gauss.so.3.0
./include/gs_thread.h
./include/gs_threadlocal.h
./include/postgres_ext.h
./include/libpq-fe.h
./include/libpq-events.h
./include/libpq/libpq-fs.h"

    mkdir -p ${BUILD_DIR}
    cd ${BUILD_DIR}
    rm -rf temp
    mkdir temp
    target_file_copy_for_non_server "$dest" ${BUILD_DIR}/temp

    cd ${BUILD_DIR}/temp
    echo "packaging libpq..."
    tar -zvcf "${libpq_package_name}" ./* >>"$LOG_FILE" 2>&1
    if [ $? -ne 0 ]; then
        die "$package_command ${libpq_package_name} failed"
    fi
    mv ${libpq_package_name} ${package_path}
    echo "install $pkgname tools is ${libpq_package_name} of ${package_path} directory " >> "$LOG_FILE" 2>&1
    echo "success!"
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
        echo "Start package gaussdb."
        make_package_srv
        make_package_libpq
        echo "End package gaussdb."
}

#############################################################
# main function
#############################################################

# 1. build server
srv_pkg_bld

# 2. make package
srv_pkg_make

#clean install directory
echo "clean enviroment"
echo "[make single db] $(date +%y-%m-%d' '%T): remove ${BUILD_DIR}" >>"$LOG_FILE" 2>&1
if [ -d "${BUILD_DIR}" ]; then
    rm -rf ${BUILD_DIR} >>"$LOG_FILE" 2>&1
fi
echo "now, all packages has finished!"
exit 0