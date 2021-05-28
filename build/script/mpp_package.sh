#!/bin/bash
#######################################################################
# Copyright (c): 2020-2025, Huawei Tech. Co., Ltd.
# descript: Compile and pack MPPDB
#           Return 0 means OK.
#           Return 1 means failed.
# version:  2.0
# date:     2020-08-08
#######################################################################

##default package type is all
declare package_type='all'
declare install_package_format='tar'
declare optimized='true'

declare product_mode='multiple'

##default version mode is relase
declare version_mode='release'
declare binarylib_dir='None'
declare separate_symbol='on'
#detect platform information.
PLATFORM=32
bit=$(getconf LONG_BIT)
if [ "$bit" -eq 64 ]; then
    PLATFORM=64
fi

#get OS distributed version.
kernel=""
version=""
if [ -f "/etc/euleros-release" ]; then
    kernel=$(cat /etc/euleros-release | awk -F ' ' '{print $1}' | tr A-Z a-z)
    version=$(cat /etc/euleros-release | awk -F '(' '{print $2}'| awk -F ')' '{print $1}' | tr A-Z a-z)
elif [ -f "/etc/openEuler-release" ]; then
    kernel=$(cat /etc/openEuler-release | awk -F ' ' '{print $1}' | tr A-Z a-z)
    version=$(cat /etc/openEuler-release | awk -F '(' '{print $2}'| awk -F ')' '{print $1}' | tr A-Z a-z)
elif [ -f "/etc/centos-release" ]; then
    kernel=$(cat /etc/centos-release | awk -F ' ' '{print $1}' | tr A-Z a-z)
    version=$(cat /etc/centos-release | awk -F '(' '{print $2}'| awk -F ')' '{print $1}' | tr A-Z a-z)
else
    kernel=$(lsb_release -d | awk -F ' ' '{print $2}'| tr A-Z a-z)
    version=$(lsb_release -r | awk -F ' ' '{print $2}')
fi

if [ X"$kernel" == X"euleros" ]; then
    dist_version="EULER"
elif [ X"$kernel" == X"centos" ]; then
    dist_version="CENTOS"
elif [ X"$kernel" == X"openeuler" ]; then
    dist_version="OPENEULER"
elif [ X"$kernel" == X"kylin" ]; then
    dist_version="KYLIN"
else
    echo "Only support EulerOS, OPENEULER(aarch64), CentOS and Kylin(aarch64) platform."
    echo "Kernel is $kernel"
    exit 1
fi

show_package=false

gcc_version="7.3.0"
##add platform architecture information
PLATFORM_ARCH=$(uname -p)
if [ "$PLATFORM_ARCH"X == "aarch64"X ] ; then
    if [ "$dist_version" == "EULER" ]; then
        ARCHITECTURE_EXTRA_FLAG=_euleros2.0_sp8_$PLATFORM_ARCH
        GAUSSDB_EXTRA_FLAGS=" -D__USE_NUMA"
    elif [ "$dist_version" == "CENTOS" ]; then
        ARCHITECTURE_EXTRA_FLAG=_centos_7.5_$PLATFORM_ARCH
    elif [ "$dist_version" == "OPENEULER" ]; then
        ARCHITECTURE_EXTRA_FLAG=_openeuler_$PLATFORM_ARCH
        # it may be risk to enable 'ARM_LSE' for all ARM CPU, but we bid our CPUs are not elder than ARMv8.1
        GAUSSDB_EXTRA_FLAGS=" -D__USE_NUMA -D__ARM_LSE"
    elif [ "$dist_version" == "KYLIN" ]; then
        ARCHITECTURE_EXTRA_FLAG=_kylin_$PLATFORM_ARCH
    else
        ARCHITECTURE_EXTRA_FLAG=_$PLATFORM_ARCH
    fi
    release_file_list="mpp_release_list_${kernel}_${PLATFORM_ARCH}"
else
    ARCHITECTURE_EXTRA_FLAG=_euleros2.0_sp5_$PLATFORM_ARCH
    release_file_list="mpp_release_list_${kernel}"
fi

##default install version storage path
declare mppdb_version='GaussDB Kernel'
declare mppdb_name_for_package="$(echo ${mppdb_version} | sed 's/ /-/g')"
declare package_path='./'
declare version_number=''
declare make_check='off'
declare zip_package='on'
declare extra_config_opt=''

#######################################################################
##putout the version of mppdb
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
    -h|--help              show help information.
    -V|--version           show version information.
    -f|--file              provide the file list released.
    -3rd|--binarylib_dir   the directory of third party binarylibs.
    -pkg|--package         provode type of installation packages, values parameter is all, server, jdbc, odbc, agent.
    -pm                    product mode, values parameter is single, multiple or opengauss, default value is multiple.
    -p|--path              generation package storage path.
    -t                     packaging format, values parameter is tar or rpm, the default value is tar.
    -m|--version_mode      this values of paramenter is debug, release, memcheck, the default value is release.
    -mc|--make_check       this values of paramenter is on or off, the default value is on.
    -s|--symbol_mode       whether separate symbol in debug mode, the default value is on.
    -cv|--gcc_version      gcc-version option: 7.3.0.
    -nopt|--not_optimized  on kunpeng platform , like 1616 version, without LSE optimized.
    -nopkg|--no_package    don't zip binaries into packages
    -co|--config_opt       more config options
    -S|--show_pkg          show server package name and Bin name base on current configuration.
"
}

if [ $# = 0 ] ; then
    echo "missing option"
    print_help
    exit 1
fi

SCRIPT_PATH=${0}
FIRST_CHAR=$(expr substr "$SCRIPT_PATH" 1 1)
if [ "$FIRST_CHAR" = "/" ]; then
    SCRIPT_PATH=${0}
else
    SCRIPT_PATH="$(pwd)/${SCRIPT_PATH}"
fi

SCRIPT_DIR=$(dirname "${SCRIPT_PATH}")
SCRIPT_DIR=$(dirname "$SCRIPT_DIR")

if [ ! -f "$SCRIPT_DIR"/mpp_package.sh ] ; then
    SCRIPT_DIR=$SCRIPT_DIR/script
fi

package_path=$SCRIPT_DIR
#######################################################################
##read version from $release_file_list
#######################################################################
function read_mpp_version()
{
    cd $SCRIPT_DIR
    local head=$(cat $release_file_list | grep "\[version\]" -n | awk -F: '{print $1}')
    if [ ! -n "$head" ]; then
        echo "error: no find version in the $release_file_list file "
        exit 1
    fi
    local tail=$(cat $release_file_list | sed "1,$head d" | grep "^\[" -n | sed -n "1p" |  awk -F: '{print $1}')
    if [ ! -n "$tail" ]; then
        local all=$(cat $release_file_list | wc -l)
        let tail=$all+1-$head
    fi
    version_number=$(cat $release_file_list | awk "NR==$head+1,NR==$tail+$head-1")
    echo "${mppdb_name_for_package}-${version_number}">version.cfg
    #auto read the number from kernal globals.cpp, no need to change it here
}
#######################################################################
##first read mppdb version
#######################################################################

#########################################################################
##read command line paramenters
#######################################################################
while [ $# -gt 0 ]; do
    case "$1" in
        -h|--help)
            print_help
            exit 1
            ;;
        -V|--version)
            print_version
            exit 1
            ;;
        -f|--file)
            if [ "$2"X = X ]; then
                echo "no given file name"
                exit 1
            fi
            release_file_list=$2
            shift 2
            ;;
        -3rd|--binarylib_dir)
            if [ "$2"X = X ]; then
                echo "no given binarylib directory values"
                exit 1
            fi
            binarylib_dir=$2
            shift 2
            ;;
        -p|--path)
            if [ "$2"X = X ]; then
                echo "no given generration package path"
                exit 1
            fi
            package_path=$2
            if [ ! -d "$package_path" ]; then
                mkdir -p $package_path
            fi
            shift 2
            ;;
        -pkg)
            if [ "$2"X = X ]; then
                echo "no given package type name"
                exit 1
            fi
            package_type=$2
            shift 2
            ;;
        -pm)
            if [ "$2"X = X ]; then
                echo "no given product mode"
                exit 1
            fi
            product_mode=$2
            shift 2
            ;;
        -s|--symbol_mode)
            if [ "$2"X = X ]; then
                echo "no given symbol parameter"
                exit 1
            fi
            separate_symbol=$2
            shift 2
            ;;
        -t)
            if [ "$2"X = X ]; then
                echo "no given installation package format values"
                exit 1
            fi
            if [ "$2" = rpm  ]; then
                echo "error: do not suport rpm package now!"
                exit 1
            fi
            install_package_format=$2
            shift 1
            ;;
        -m|--version_mode)
            if [ "$2"X = X ]; then
                echo "no given version number values"
                exit 1
            fi
            version_mode=$2
            shift 2
            ;;
        -mc|--make_check)
            if [ "$2"X = X ]; then
                echo "no given make check  values"
                exit 1
            fi
            make_check=$2
            shift 2
            ;;
        -cv|--gcc_version)
            if [ "$2"X = X ]; then
                echo "no given gcc version"
                exit 1
            fi
            gcc_version=$2
            shift 2
            ;;
        -nopt|--not_optimized)
            optimized='false'
            shift 1
            ;;
        -nopkg|--no_package)
            zip_package='off'
            shift 1
            ;;
        -co|--config_opt)
            if [ "$2"X = X ]; then
                echo "no extra configure options provided"
                exit 1
            fi
            extra_config_opt=$2
            shift 2
            ;;
        -S|--show_pkg)
            show_package=true
            shift
            ;;
         *)
            echo "Internal Error: option processing error: $1" 1>&2
            echo "please input right paramtenter, the following command may help you"
            echo "./mpp_package.sh --help or ./mpp_package.sh -h"
            exit 1
    esac
done

if [ "$product_mode"x == "single"x ]; then
    release_file_list="${release_file_list}_${product_mode}"
fi

if [ "$product_mode"x == "opengauss"x ]; then
    release_file_list=$(echo ${release_file_list}_single | sed -e 's/mpp_release/opengauss_release/')
fi

read_mpp_version

if [ "$gcc_version" = "7.3.0" ]; then
    gcc_version=${gcc_version:0:3}
else
    echo "Unknown gcc version $gcc_version"
    exit 1
fi

#######################################################################
## declare all package name
#######################################################################
declare version_string="${mppdb_name_for_package}-${version_number}"
declare package_pre_name="${version_string}-${dist_version}-${PLATFORM}bit"
declare server_package_name="${package_pre_name}.${install_package_format}.gz"
declare agent_package_name="${package_pre_name}-AGENT.${install_package_format}.gz"
declare gsql_package_name="${mppdb_name_for_package}-${version_number}-${dist_version}-${PLATFORM}bit-gsql.${install_package_format}.gz"
declare client_package_name="${package_pre_name}-ClientTools.${install_package_format}.gz"
declare libpq_package_name="${package_pre_name}-Libpq.${install_package_format}.gz"
declare gds_package_name="${package_pre_name}-Gds.${install_package_format}.gz"
declare symbol_package_name="${package_pre_name}-symbol.${install_package_format}.gz"
declare inspection_package_name="${version_string}-Inspection.tar.gz"

echo "[makemppdb] $(date +%y-%m-%d' '%T): script dir : ${SCRIPT_DIR}"
ROOT_DIR=$(dirname "$SCRIPT_DIR")
ROOT_DIR=$(dirname "$ROOT_DIR")
PLAT_FORM_STR=$(sh "${ROOT_DIR}/src/get_PlatForm_str.sh")
if [ "${PLAT_FORM_STR}"x == "Failed"x ]
then
    echo "Only support EulerOS openEuler and Centros platform."
    exit 1;
fi
PG_REG_TEST_ROOT="${ROOT_DIR}"
ROACH_DIR="${ROOT_DIR}/src/distribute/bin/roach"
MPPDB_DECODING_DIR="${ROOT_DIR}/contrib/mppdb_decoding"
PMK_SCHEMA="${ROOT_DIR}/script/pmk_schema.sql"
declare LOG_FILE="${ROOT_DIR}/build/script/makemppdb_pkg.log"
declare BUILD_DIR="${ROOT_DIR}/mppdb_temp_install"
declare ERR_MKGS_FAILED=1
declare MKGS_OK=0
if [ "${binarylib_dir}" != 'None' ] && [ -d "${binarylib_dir}" ]; then
    BUILD_TOOLS_PATH="${binarylib_dir}/buildtools/${PLAT_FORM_STR}"
    BINARYLIBS_PATH="${binarylib_dir}/dependency"
else
    BUILD_TOOLS_PATH="${ROOT_DIR}/buildtools/${PLAT_FORM_STR}"
    BINARYLIBS_PATH="${ROOT_DIR}/binarylibs"
fi

if [ "$product_mode"x == "single"x ] || [ "$product_mode"x == "opengauss"x ]; then
    declare UPGRADE_SQL_DIR="${ROOT_DIR}/src/include/catalog/upgrade_sql"
else
    declare UPGRADE_SQL_DIR="${ROOT_DIR}/src/distribute/include/catalog/upgrade_sql"
fi

gaussdb_200_file="${binarylib_dir}/buildtools/license_control/gaussdb.version.GaussDB200"
gaussdb_300_file="${binarylib_dir}/buildtools/license_control/gaussdb.version.GaussDB300"
gaussdb_200_standard_file="${binarylib_dir}/buildtools/license_control/gaussdb.license.GaussDB200_Standard"
gaussdb_version_file="${ROOT_DIR}/src/gausskernel/process/postmaster/gaussdb_version.cpp"

ccache -V >/dev/null 2>&1 && USE_CCACHE="ccache " ENABLE_CCACHE="--enable-ccache"
export CC="${USE_CCACHE}$BUILD_TOOLS_PATH/gcc$gcc_version/gcc/bin/gcc"
export CXX="${USE_CCACHE}$BUILD_TOOLS_PATH/gcc$gcc_version/gcc/bin/g++"
export LD_LIBRARY_PATH=$BUILD_TOOLS_PATH/gcc$gcc_version/gcc/lib64:$BUILD_TOOLS_PATH/gcc$gcc_version/isl/lib:$BUILD_TOOLS_PATH/gcc$gcc_version/mpc/lib/:$BUILD_TOOLS_PATH/gcc$gcc_version/mpfr/lib/:$BUILD_TOOLS_PATH/gcc$gcc_version/gmp/lib/:$LD_LIBRARY_PATH
export PATH=$BUILD_TOOLS_PATH/gcc$gcc_version/gcc/bin:$PATH

declare p7zpath="${BUILD_TOOLS_PATH}/p7z/bin"
###################################
# build parameter about enable-llt
##################################
COMPLIE_TYPE="comm"
echo "[makemppdb] $(date +%y-%m-%d' '%T): Work root dir : ${ROOT_DIR}"
###################################
# get version number from globals.cpp
##################################
function read_mpp_number()
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
read_mpp_number

#######################################################################
#  Print log.
#######################################################################
log()
{
    echo "[makegaussdb] $(date +%y-%m-%d' '%T): $@"
    echo "[makegaussdb] $(date +%y-%m-%d' '%T): $@" >> "$LOG_FILE" 2>&1
}

#######################################################################
#  print log and exit.
#######################################################################
die()
{
    log "$@"
    echo "$@"
    exit $ERR_MKGS_FAILED
}

#######################################################################
##  Check the installation package production environment
#######################################################################
function mpp_pkg_pre_check()
{
    if [ -d "$BUILD_DIR" ]; then
        rm -rf $BUILD_DIR
    fi
    if [ -d "$LOG_FILE" ]; then
        rm -rf $LOG_FILE
    fi

    if [  X"$package_type" == X"server" -o X"$package_type" == X"all" ] && [ X"$zip_package" = X"on" ] && [ ! -d "${ROOT_DIR}"/script/script/gspylib/ ]; then
        printf "\033[31mCan not found OM script directory. solution steps:\n\033[0m"
        echo "  1) git clone git@isource-dg.huawei.com:2222/GaussDB_Kernel/GaussDB_Kernel_OM.git -b $(git branch | grep '*' | sed -e 's/*//g' -e 's/^ //g')"
        echo "  2) if you do not have the permission to git it, please call CMO "
        echo "  3) rm -rf ${ROOT_DIR}/script && ln -s <om_code_path>/GaussDB_Kernel_OM ${ROOT_DIR}/script"
        echo "and then try again!"
        exit 1
    fi
}

#######################################################################
# Install all SQL files from src/distribute/include/catalog/upgrade_sql
# to INSTALL_DIR/bin/script/upgrade_sql.
# Package all SQL files and then verify them with SHA256.
#######################################################################
function package_upgrade_sql()
{
    echo "Begin to install upgrade_sql files..."
    UPGRADE_SQL_TAR="upgrade_sql.tar.gz"
    UPGRADE_SQL_SHA256="upgrade_sql.sha256"
    MULTIP_IGNORE_VERSION=(289 294 296)
    cp -r "${UPGRADE_SQL_DIR}" .
    [ $? -ne 0 ] && die "Failed to cp upgrade_sql files"
    if [ "$product_mode"x == "multiple"x ]; then
        for version_num in ${MULTIP_IGNORE_VERSION[*]}
        do
            find ./upgrade_sql -name *${version_num}* | xargs rm -rf
        done
    fi
    tar -czf ${UPGRADE_SQL_TAR} upgrade_sql
    [ $? -ne 0 ] && die "Failed to package ${UPGRADE_SQL_TAR}"
    rm -rf ./upgrade_sql > /dev/null 2>&1

    sha256sum ${UPGRADE_SQL_TAR} | awk -F" " '{print $1}' > "${UPGRADE_SQL_SHA256}"
    [ $? -ne 0 ] && die "Failed to generate sha256 sum file for ${UPGRADE_SQL_TAR}"

    chmod 600 ${UPGRADE_SQL_TAR}
    chmod 600 ${UPGRADE_SQL_SHA256}

    echo "Successfully packaged upgrade_sql files."
}
#######################################################################
# get cluster version from src/include/pg_config.h by 'DEF_GS_VERSION '
# then replace OM tools version
#######################################################################
function replace_omtools_version()
{
    local gs_version=$(grep DEF_GS_VERSION ${PG_REG_TEST_ROOT}/src/include/pg_config.h | awk -F '"' '{print $2}')
    echo $gs_version | grep -e "${mppdb_version}.*build.*compiled.*"  > /dev/null 2>&1
    if [ $? -ne 0 ]; then
        die "Failed to get gs_version from pg_config.h."
    fi

    if [ -f "$1"/script/gspylib/common/VersionInfo.py ] ; then
        sed -i -e "s/COMMON_VERSION = \"Gauss200 OM VERSION\"/COMMON_VERSION = \"$(echo ${gs_version})\"/g" -e "s/__GAUSS_PRODUCT_STRING__/$mppdb_version/g" $1/script/gspylib/common/VersionInfo.py
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
#######################################################################
# get cluster version from src/include/pg_config.h by 'DEF_GS_VERSION '
# then replace ODBC version
#######################################################################
function replace_odbc_version()
{
    local gs_version=$(grep DEF_GS_VERSION ${PG_REG_TEST_ROOT}/src/include/pg_config.h | awk -F '"' '{print $2}')
    echo $gs_version | grep -e "${mppdb_version:x}.*build.*compiled.*"  > /dev/null 2>&1

    if [ $? -ne 0 ]; then
        die "Failed to get gs_version from pg_config.h."
    fi

    if [ -f "$1"/config.h ] ; then
        sed -i "/^\\s*#define\\s*DEF_GS_VERSION.*$/d" $1/config.h
        echo "#define DEF_GS_VERSION \"$(echo ${gs_version})\"">>$1/config.h
        if [ $? -ne 0 ]; then
            die "Failed to replace odbc tools version number."
        fi
    else
        echo "Failed to replace odbc tools: can not find file $1/config.h."
    fi
}
#######################################################################
##install gaussdb database and others
##select to install something according to variables package_type need
#######################################################################
function mpp_pkg_bld()
{
    case "$package_type" in
        all)
            echo "Install all"
            install_gaussdb
            install_inspection
            echo "Install all success"
            ;;
        server)
            install_gaussdb
            ;;
        gsql)
            install_gaussdb
            ;;
        libpq)
            install_gaussdb
            ;;
        gds)
            install_gaussdb
            ;;
        inspection)
            install_inspection
            ;;
        *)
            echo "Internal Error: option processing error: $package_type"
            echo "please input right paramenter values all, server, libpq, gds or gsql "
            exit 1
    esac
}
#######################################################################
##install inspection tool scripts
#######################################################################
function install_inspection()
{
    echo "packaging inspection..."
    rm -rf ${package_path}/inspection &&
    mkdir -p ${package_path}/inspection &&

    cp -f ${script_dir}/script/gs_check ${package_path}/inspection/ &&
    cp -rf ${script_dir}/script/gspylib/ ${package_path}/inspection/ &&

    mkdir -p ${package_path}/inspection/gspylib/inspection/output/log/ &&
    mkdir -p ${package_path}/inspection/gspylib/inspection/output/nodes/ &&
    mkdir -p ${package_path}/inspection/gspylib/inspection/lib/asn1crypto/ &&
    mkdir -p ${package_path}/inspection/gspylib/inspection/lib/bcrypt/ &&
    mkdir -p ${package_path}/inspection/gspylib/inspection/lib/cryptography/ &&
    mkdir -p ${package_path}/inspection/gspylib/inspection/lib/cffi/ &&
    mkdir -p ${package_path}/inspection/gspylib/inspection/lib/enum/ &&
    mkdir -p ${package_path}/inspection/gspylib/inspection/lib/idna/ &&
    mkdir -p ${package_path}/inspection/gspylib/inspection/lib/nacl/ &&
    mkdir -p ${package_path}/inspection/gspylib/inspection/lib/pyasn1/ &&
    mkdir -p ${package_path}/inspection/gspylib/inspection/lib/kafka-python/ &&
    mkdir -p ${package_path}/inspection/gspylib/inspection/lib/pycparser/ &&
    mkdir -p ${package_path}/inspection/gspylib/inspection/lib/OpenSSL/ &&
    mkdir -p ${package_path}/inspection/gspylib/inspection/lib/psutil/ &&
    mkdir -p ${package_path}/inspection/gspylib/inspection/lib/netifaces/ &&
    mkdir -p ${package_path}/inspection/gspylib/inspection/lib/paramiko/ &&
    mkdir -p ${package_path}/inspection/gspylib/inspection/lib/paste/ &&
    mkdir -p ${package_path}/inspection/gspylib/inspection/lib/bottle/ &&

    cp -rf ${BINARYLIBS_PATH}/install_tools${ARCHITECTURE_EXTRA_FLAG}/asn1crypto/ ${package_path}/inspection/gspylib/inspection/lib/
    cp -rf ${BINARYLIBS_PATH}/install_tools${ARCHITECTURE_EXTRA_FLAG}/bcrypt/ ${package_path}/inspection/gspylib/inspection/lib/
    cp -rf ${BINARYLIBS_PATH}/install_tools${ARCHITECTURE_EXTRA_FLAG}/cffi/ ${package_path}/inspection/gspylib/inspection/lib/
    cp -rf ${BINARYLIBS_PATH}/install_tools${ARCHITECTURE_EXTRA_FLAG}/cryptography/ ${package_path}/inspection/gspylib/inspection/lib/
    cp -rf ${BINARYLIBS_PATH}/install_tools${ARCHITECTURE_EXTRA_FLAG}/enum/ ${package_path}/inspection/gspylib/inspection/lib/
    cp -rf ${BINARYLIBS_PATH}/install_tools${ARCHITECTURE_EXTRA_FLAG}/idna/ ${package_path}/inspection/gspylib/inspection/lib/
    cp -rf ${BINARYLIBS_PATH}/install_tools${ARCHITECTURE_EXTRA_FLAG}/nacl/ ${package_path}/inspection/gspylib/inspection/lib/
    cp -rf ${BINARYLIBS_PATH}/install_tools${ARCHITECTURE_EXTRA_FLAG}/pyasn1/ ${package_path}/inspection/gspylib/inspection/lib/
    cp -rf ${BINARYLIBS_PATH}/install_tools${ARCHITECTURE_EXTRA_FLAG}/kafka-python/ ${package_path}/inspection/gspylib/inspection/lib/
    cp -rf ${BINARYLIBS_PATH}/install_tools${ARCHITECTURE_EXTRA_FLAG}/pycparser/ ${package_path}/inspection/gspylib/inspection/lib/
    cp -rf ${BINARYLIBS_PATH}/install_tools${ARCHITECTURE_EXTRA_FLAG}/OpenSSL/ ${package_path}/inspection/gspylib/inspection/lib/
    cp -rf ${BINARYLIBS_PATH}/install_tools${ARCHITECTURE_EXTRA_FLAG}/ipaddress.py ${package_path}/inspection/gspylib/inspection/lib/
    cp -rf ${BINARYLIBS_PATH}/install_tools${ARCHITECTURE_EXTRA_FLAG}/six.py ${package_path}/inspection/gspylib/inspection/lib/
    cp -rf ${BINARYLIBS_PATH}/install_tools${ARCHITECTURE_EXTRA_FLAG}/_cffi_backend.py ${package_path}/inspection/gspylib/inspection/lib/
    cp -rf ${BINARYLIBS_PATH}/install_tools${ARCHITECTURE_EXTRA_FLAG}/_cffi_backend.so_UCS2 ${package_path}/inspection/gspylib/inspection/lib/
    cp -rf ${BINARYLIBS_PATH}/install_tools${ARCHITECTURE_EXTRA_FLAG}/_cffi_backend.so_UCS4 ${package_path}/inspection/gspylib/inspection/lib/
    cp -rf ${BINARYLIBS_PATH}/install_tools${ARCHITECTURE_EXTRA_FLAG}/psutil/ ${package_path}/inspection/gspylib/inspection/lib/
    cp -rf ${BINARYLIBS_PATH}/install_tools${ARCHITECTURE_EXTRA_FLAG}/netifaces/ ${package_path}/inspection/gspylib/inspection/lib/
    cp -rf ${BINARYLIBS_PATH}/install_tools${ARCHITECTURE_EXTRA_FLAG}/paramiko/ ${package_path}/inspection/gspylib/inspection/lib/
    cp -rf ${BINARYLIBS_PATH}/install_tools${ARCHITECTURE_EXTRA_FLAG}/paste/ ${package_path}/inspection/gspylib/inspection/lib/
    cp -rf ${BINARYLIBS_PATH}/install_tools${ARCHITECTURE_EXTRA_FLAG}/bottle/ ${package_path}/inspection/gspylib/inspection/lib/

    mv -f ${package_path}/inspection/gspylib/inspection/readme.txt ${package_path}/inspection/

    if [ $? -ne 0 ]; then
        die "cp -rf ${script_dir}/script/inspection/* ${package_path}/inspection/inspection/ failed"
    fi

    find ${package_path}/inspection/ -name .svn -type d -print0 | xargs -0 rm -rf
    find ${package_path}/inspection/ -name d2utmp* -print0 | xargs -0 rm -rf
    chmod -R +x ${package_path}/inspection/

    cd ${package_path}/inspection
    select_package_command
    $package_command "${inspection_package_name}" ./* >>"$LOG_FILE" 2>&1
    if [ $? -ne 0 ]; then
        die "$package_command ${inspection_package_name} failed"
    fi
    mv ${inspection_package_name} ${package_path}
    rm -rf ${package_path}/inspection/
    echo "install $pkgname tools is ${inspection_package_name} of ${package_path} directory " >> "$LOG_FILE" 2>&1
    echo "success!"
}
#######################################################################
##install gaussdb database contained server,client and libpq
#######################################################################
function install_gaussdb()
{
    # Generate the license control file, and set md5sum string to the code.
    echo "Modify gaussdb_version.cpp file." >> "$LOG_FILE" 2>&1
    make_license_control
    echo "Modify gaussdb_version.cpp file success." >> "$LOG_FILE" 2>&1
    #putinto to Code dir
    cd "$ROOT_DIR"
    #echo "$ROOT_DIR/Code"
    if [ $? -ne 0 ]; then
        die "change dir to $SRC_DIR failed."
    fi

    if [ "$version_mode" = "debug" -a "$separate_symbol" = "on" ]; then
        echo "WARNING: do not separate symbol in debug mode!"
    fi

    if [ "$product_mode" != "single" ] && [ "$product_mode" != "multiple" ] && [ "$product_mode" != "opengauss" ]; then
        die "the product mode can only be multiple, single, or opengauss!"
    fi

    binarylibs_path=${ROOT_DIR}/binarylibs
    if [ "${binarylib_dir}"x != "None"x ]; then
        binarylibs_path=${binarylib_dir}
    fi

    #configure
    make distclean -sj >> "$LOG_FILE" 2>&1

    echo "Begin configure." >> "$LOG_FILE" 2>&1
    chmod 755 configure

    shared_opt="--gcc-version=${gcc_version}.0 --prefix="${BUILD_DIR}" --3rd=${binarylibs_path} --enable-thread-safety --with-readline --without-zlib"
    if [ "$product_mode"x == "multiple"x ]; then
        if [ "$version_mode"x == "release"x ]; then
            ./configure $shared_opt CFLAGS="-O2 -g3 ${GAUSSDB_EXTRA_FLAGS}"  CC="${USE_CCACHE}g++" ${ENABLE_CCACHE} --enable-multiple-nodes $extra_config_opt >> "$LOG_FILE" 2>&1
        elif [ "$version_mode"x == "memcheck"x ]; then
            ./configure $shared_opt CFLAGS='-O0' --enable-debug --enable-cassert --enable-memory-check CC=g++  --enable-multiple-nodes $extra_config_opt >> "$LOG_FILE" 2>&1
        elif [ "$version_mode"x == "threadcheck"x ] && [ "$PLATFORM_ARCH"X == "aarch64"X ]; then
            ./configure $shared_opt CFLAGS='-O2 -g3' --disable-jemalloc --enable-thread-check CC=g++ --enable-multiple-nodes $extra_config_opt >> "$LOG_FILE" 2>&1
        elif [ "$version_mode"x == "fiurelease"x ]; then
            ./configure $shared_opt CFLAGS="-O2 -g3 ${GAUSSDB_EXTRA_FLAGS}" CC=g++ --enable-multiple-nodes --disable-jemalloc  $extra_config_opt >> "$LOG_FILE" 2>&1
        elif [ "$version_mode"x == "fiudebug"x ]; then
            ./configure $shareed_opt CFLAGS="-O0 ${GAUSSDB_EXTRA_FLAGS}" --enable-debug --enable-cassert CC=g++  --enable-multiple-nodes --disable-jemalloc $extra_config_opt >> "$LOG_FILE" 2>&1
        else
            ./configure $shared_opt CFLAGS="-O0 ${GAUSSDB_EXTRA_FLAGS}" --enable-debug --enable-cassert CC="${USE_CCACHE}g++" ${ENABLE_CCACHE} --enable-multiple-nodes $extra_config_opt >> "$LOG_FILE" 2>&1
        fi
    elif [ "$product_mode"x == "single"x ]; then
        if [ "$version_mode"x == "release"x ]; then
            # configure -D__USE_NUMA -D__ARM_LSE with arm single mode
            if [ "$PLATFORM_ARCH"X == "aarch64"X ] ; then
                echo "configure -D__USE_NUMA -D__ARM_LSE with arm single mode"
                GAUSSDB_EXTRA_FLAGS=" -D__USE_NUMA -D__ARM_LSE"
            fi

            ./configure $shared_opt CFLAGS="-O2 -g3 ${GAUSSDB_EXTRA_FLAGS}" CC=g++ --enable-privategauss $extra_config_opt >> "$LOG_FILE" 2>&1
        elif [ "$version_mode"x == "memcheck"x ]; then
            ./configure $shared_opt CFLAGS='-O0' --enable-debug --enable-cassert --enable-memory-check CC=g++ --enable-privategauss $extra_config_opt >> "$LOG_FILE" 2>&1
        elif [ "$version_mode"x == "fiurelease"x ]; then
            ./configure $shared_opt CFLAGS="-O2 -g3 ${GAUSSDB_EXTRA_FLAGS}" --disable-jemalloc CC=g++ --enable-privategauss $extra_config_opt >> "$LOG_FILE" 2>&1
        elif [ "$version_mode"x == "fiudebug"x ]; then
            ./configure $shared_opt CFLAGS="-O0 ${GAUSSDB_EXTRA_FLAGS}" --enable-debug --enable-cassert --disable-jemalloc CC=g++ --enable-privategauss $extra_config_opt >> "$LOG_FILE" 2>&1
        else
            ./configure $shared_opt CFLAGS="-O0 ${GAUSSDB_EXTRA_FLAGS}" --enable-debug --enable-cassert CC=g++ --enable-privategauss $extra_config_opt >> "$LOG_FILE" 2>&1
        fi
    else # product_mode == opengauss
        if [ "$version_mode"x == "release"x ]; then
            # configure -D__USE_NUMA -D__ARM_LSE with arm opengauss mode
            if [ "$PLATFORM_ARCH"X == "aarch64"X ] ; then
                echo "configure -D__USE_NUMA -D__ARM_LSE with arm opengauss mode"
                GAUSSDB_EXTRA_FLAGS=" -D__USE_NUMA -D__ARM_LSE"
            fi

            ./configure $shared_opt CFLAGS="-O2 -g3 ${GAUSSDB_EXTRA_FLAGS}" --enable-mot CC=g++ $extra_config_opt >> "$LOG_FILE" 2>&1
        elif [ "$version_mode"x == "memcheck"x ]; then
            ./configure $shared_opt CFLAGS="-O0" --enable-mot --enable-debug --enable-cassert --enable-memory-check CC=g++ $extra_config_opt >> "$LOG_FILE" 2>&1
        elif [ "$version_mode"x == "fiurelease"x ]; then
            ./configure $shared_opt CFLAGS="-O2 -g3 ${GAUSSDB_EXTRA_FLAGS}" --enable-mot --disable-jemalloc CC=g++ $extra_config_opt >> "$LOG_FILE" 2>&1
        elif [ "$version_mode"x == "fiudebug"x ]; then
            ./configure $shared_opt CFLAGS="-O0 ${GAUSSDB_EXTRA_FLAGS}" --enable-mot --enable-debug --enable-cassert --disable-jemalloc CC=g++ $extra_config_opt >> "$LOG_FILE" 2>&1
        else
            ./configure $shared_opt CFLAGS="-O0 ${GAUSSDB_EXTRA_FLAGS}" --enable-mot --enable-debug --enable-cassert CC=g++ $extra_config_opt >> "$LOG_FILE" 2>&1
        fi
    fi

    if [ $? -ne 0 ]; then
        die "configure failed."
    fi
    echo "End configure" >> "$LOG_FILE" 2>&1

    echo "Begin make install MPPDB server" >> "$LOG_FILE" 2>&1
    make clean >> "$LOG_FILE" 2>&1

    export GAUSSHOME=${BUILD_DIR}
    export LD_LIBRARY_PATH=${BUILD_DIR}/lib:${BUILD_DIR}/lib/postgresql:${LD_LIBRARY_PATH}
    make -sj 8>> "$LOG_FILE" 2>&1
    make install -sj 8>> "$LOG_FILE" 2>&1
    if [ $? -ne 0 ]; then
        make install -sj 8>> "$LOG_FILE" 2>&1
        if [ $? -ne 0 ]; then
            make install -sj 8>> "$LOG_FILE" 2>&1
            if [ $? -ne 0 ]; then
                die "make install failed."
            fi
        fi
    fi

    cd "$ROOT_DIR/contrib/pg_upgrade_support"
    make clean >> "$LOG_FILE" 2>&1
    make -sj >> "$LOG_FILE" 2>&1
    make install -sj >> "$LOG_FILE" 2>&1
    echo "End make install MPPDB" >> "$LOG_FILE" 2>&1


    cd "$ROOT_DIR"
    if [ "${make_check}" = 'on' ]; then
        echo "Begin make check MPPDB..." >> "$LOG_FILE" 2>&1
        cd ${PG_REG_TEST_ROOT}
        make check -sj >> "$LOG_FILE" 2>&1
        if [ $? -ne 0 ]; then
            die "make check MPPDB failed."
        fi
        echo "End make check MPPDB success." >> "$LOG_FILE" 2>&1
    fi

    ## check build specification
    spec="gaussdbkernel"
    if ( cat $SCRIPT_DIR/gauss.spec | grep 'PRODUCT' | grep 'GaussDB Kernel' >/dev/null 2>&1 ); then
        spec="gaussdbkernel"
    elif ( cat $SCRIPT_DIR/gauss.spec | grep 'PRODUCT' | grep 'openGauss' >/dev/null 2>&1 ); then
        spec="opengauss"
    fi

    if [ "${spec}" = "gaussdbkernel" ]; then
        echo "Begin make install Roach..." >> "$LOG_FILE" 2>&1
        #copy gs_roach form clienttools to bin
        if [ "$version_mode"x == "release"x ]; then
            cd "$ROACH_DIR/src"
            make clean >> "$LOG_FILE" 2>&1
            make release gcc_version="$gcc_version" >> "$LOG_FILE" 2>&1
            if [ $? -ne 0 ]; then
                die "make install(release) Roach failed."
            fi
            echo "End make install(release) Roach success." >> "$LOG_FILE" 2>&1
            echo "Begin pack Roach..." >> "$LOG_FILE" 2>&1
            cp ${ROACH_DIR}/bin/package/${PLAT_FORM_STR}/release/bin/gs_roach ${BUILD_DIR}/bin/gs_roach
            cp ${ROACH_DIR}/bin/package/${PLAT_FORM_STR}/release/lib/postgresql/roach_api.so ${BUILD_DIR}/lib/postgresql/roach_api.so
            cp ${ROACH_DIR}/bin/package/${PLAT_FORM_STR}/release/lib/lib_roach_show.so ${BUILD_DIR}/lib/lib_roach_show.so
            cp ${ROACH_DIR}/bin/package/${PLAT_FORM_STR}/release/lib/roach_contrib.so ${BUILD_DIR}/lib/roach_contrib.so
            cp ${ROACH_DIR}/bin/package/${PLAT_FORM_STR}/release/share/postgresql/extension/roach_api.control ${BUILD_DIR}/share/postgresql/extension/roach_api.control
            cp ${ROACH_DIR}/bin/package/${PLAT_FORM_STR}/release/share/postgresql/extension/roach_api--1.0.sql ${BUILD_DIR}/share/postgresql/extension/roach_api-1.0.sql
        elif [ "$version_mode"x == "memcheck"x ]; then
            cd "$ROACH_DIR/src"
            make clean >> "$LOG_FILE" 2>&1
            make debug enable_memory_check=yes gcc_version="$gcc_version" >> "$LOG_FILE" 2>&1
            if [ $? -ne 0 ]; then
                die "make install(debug) Roach failed."
            fi
            echo "End make install(debug) Roach success." >> "$LOG_FILE" 2>&1
            echo "Begin pack Roach..." >> "$LOG_FILE" 2>&1
            cp ${ROACH_DIR}/bin/package/${PLAT_FORM_STR}/debug/bin/gs_roach ${BUILD_DIR}/bin/gs_roach
            cp ${ROACH_DIR}/bin/package/${PLAT_FORM_STR}/debug/lib/postgresql/roach_api.so ${BUILD_DIR}/lib/postgresql/roach_api.so
            cp ${ROACH_DIR}/bin/package/${PLAT_FORM_STR}/debug/lib/lib_roach_show.so ${BUILD_DIR}/lib/lib_roach_show.so
            cp ${ROACH_DIR}/bin/package/${PLAT_FORM_STR}/debug/lib/roach_contrib.so ${BUILD_DIR}/lib/roach_contrib.so
            cp ${ROACH_DIR}/bin/package/${PLAT_FORM_STR}/debug/share/postgresql/extension/roach_api.control ${BUILD_DIR}/share/postgresql/extension/roach_api.control
            cp ${ROACH_DIR}/bin/package/${PLAT_FORM_STR}/debug/share/postgresql/extension/roach_api--1.0.sql ${BUILD_DIR}/share/postgresql/extension/roach_api--1.0.sql
        else
            cd "$ROACH_DIR/src"
            make clean >> "$LOG_FILE" 2>&1
            make debug gcc_version="$gcc_version" >> "$LOG_FILE" 2>&1
            if [ $? -ne 0 ]; then
                die "make install(debug) Roach failed."
            fi
            echo "End make install(debug) Roach success." >> "$LOG_FILE" 2>&1
            echo "Begin pack Roach..." >> "$LOG_FILE" 2>&1
            cp ${ROACH_DIR}/bin/package/${PLAT_FORM_STR}/debug/bin/gs_roach ${BUILD_DIR}/bin/gs_roach
            cp ${ROACH_DIR}/bin/package/${PLAT_FORM_STR}/debug/lib/postgresql/roach_api.so ${BUILD_DIR}/lib/postgresql/roach_api.so
            cp ${ROACH_DIR}/bin/package/${PLAT_FORM_STR}/debug/lib/lib_roach_show.so ${BUILD_DIR}/lib/lib_roach_show.so
            cp ${ROACH_DIR}/bin/package/${PLAT_FORM_STR}/debug/lib/roach_contrib.so ${BUILD_DIR}/lib/roach_contrib.so
            cp ${ROACH_DIR}/bin/package/${PLAT_FORM_STR}/debug/share/postgresql/extension/roach_api.control ${BUILD_DIR}/share/postgresql/extension/roach_api.control
            cp ${ROACH_DIR}/bin/package/${PLAT_FORM_STR}/debug/share/postgresql/extension/roach_api--1.0.sql ${BUILD_DIR}/share/postgresql/extension/roach_api--1.0.sql
        fi
        if [ $? -ne 0 ]; then
            if [ "$version_mode"x == "release"x ]; then
                die "cp ${ROACH_DIR}/bin/package/${PLAT_FORM_STR}/release/bin/gs_roach ${BUILD_DIR}/bin/gs_roach failed"
            else
                die "cp ${ROACH_DIR}/bin/package/${PLAT_FORM_STR}/debug/bin/gs_roach ${BUILD_DIR}/bin/gs_roach failed"
            fi

            chmod +x ${BUILD_DIR}/bin/gs_roach
        fi
    fi

    echo "Begin make install mpp_decoding..." >> "$LOG_FILE" 2>&1
    #copy mppdb_decoding form clienttools to bin
    if [ "$version_mode"x == "release"x ]; then
        cd "$MPPDB_DECODING_DIR"
        make  >> "$LOG_FILE" 2>&1
        if [ $? -ne 0 ]; then
            die "make install mppdb_decoding failed."
        fi
        echo "End make install mppdb_decoding success." >> "$LOG_FILE" 2>&1
        echo "Begin pack mppdb_decoding..." >> "$LOG_FILE" 2>&1
        cp ${MPPDB_DECODING_DIR}/mppdb_decoding.so ${BUILD_DIR}/lib/postgresql/mppdb_decoding.so
    elif [ "$version_mode"x == "memcheck"x ]; then
        cd "$MPPDB_DECODING_DIR"
        make  >> "$LOG_FILE" 2>&1
        if [ $? -ne 0 ]; then
            die "make install mppdb_decoding failed."
        fi
        echo "End make install mppdb_decoding success." >> "$LOG_FILE" 2>&1
        echo "Begin pack mppdb_decoding..." >> "$LOG_FILE" 2>&1
        cp ${MPPDB_DECODING_DIR}/mppdb_decoding.so ${BUILD_DIR}/lib/postgresql/mppdb_decoding.so
    else
        cd "$MPPDB_DECODING_DIR"
        make  >> "$LOG_FILE" 2>&1
        if [ $? -ne 0 ]; then
            die "make install mppdb_decoding failed."
        fi
        echo "End make install mppdb_decoding success." >> "$LOG_FILE" 2>&1
        echo "Begin pack mppdb_decoding..." >> "$LOG_FILE" 2>&1
        cp ${MPPDB_DECODING_DIR}/mppdb_decoding.so ${BUILD_DIR}/lib/postgresql/mppdb_decoding.so
    fi
    if [ $? -ne 0 ]; then
        if [ "$version_mode"x == "release"x ]; then
            die "cp ${MPPDB_DECODING_DIR}/mppdb_decoding ${MPPDB_DECODING_DIR}/bin/mppdb_decoding failed"
        else
            die "cp ${MPPDB_DECODING_DIR}/mppdb_decoding ${MPPDB_DECODING_DIR}/bin/mppdb_decoding failed"
        fi
    fi

    # copy gs_plan_simulator.sh to /bin dir. it's used in gs_collector tool.
    cp ${ROOT_DIR}/src/bin/gs_plan_simulator/gs_plan_simulator.sh ${BUILD_DIR}/bin/

    chmod 444 ${BUILD_DIR}/bin/cluster_guc.conf
    dos2unix ${BUILD_DIR}/bin/cluster_guc.conf > /dev/null 2>&1

    #back to separate_debug_symbol.sh dir
    cd $SCRIPT_DIR
    if [ "$version_mode" = "release" -a "$separate_symbol" = "on" -a "$zip_package" = "on" ]; then
       chmod +x ./separate_debug_information.sh
       ./separate_debug_information.sh
        cd $SCRIPT_DIR
        mv symbols.tar.gz $symbol_package_name
    fi

    #back to root dir
    cd $ROOT_DIR

    # om scripts may be package alone
    if [ "${zip_package}" = "on" ]; then
        copy_script_file "$script_dir/" ${BUILD_DIR}/bin/
        cp ${BUILD_DIR}/bin/script/gspylib/etc/sql/pmk_schema.sql ${BUILD_DIR}/share/postgresql/
        if [ -f "${BUILD_DIR}"/bin/script/gspylib/etc/sql/pmk_schema_single_inst.sql ]; then
            cp ${BUILD_DIR}/bin/script/gspylib/etc/sql/pmk_schema_single_inst.sql ${BUILD_DIR}/share/postgresql/
        fi

        #copy transfer.py to bin
        cd $ROOT_DIR
        cp -f ${script_dir}/other/transfer.py ${BUILD_DIR}/bin
        if [ $? -ne 0 ]; then
            die "cp -f ${script_dir}/script/transfer.py ${BUILD_DIR}/bin failed."
        fi
        dos2unix ${BUILD_DIR}/bin/transfer.py > /dev/null 2>&1

        # generate gaussdb.version file.
        echo "Copy gaussdb.version file." >> "$LOG_FILE" 2>&1
        cd $ROOT_DIR
        copy_license_file "${BUILD_DIR}/bin/"
        echo "Copy gaussdb.version file success." >> "$LOG_FILE" 2>&1


        #copy getDEK.jar to bin
        cp -f ${binarylib_dir}/platform/common/JdbcDek/getDEK.jar ${BUILD_DIR}/bin &&
            chmod +x ${BUILD_DIR}/bin/getDEK.jar
        if [ $? -ne 0 ]; then
            die "copy getDEK.jar to bin failed."
        fi

        #insert the commitid to version.cfg as the upgrade app path specification
        export PATH=${BUILD_DIR}:$PATH
        export LD_LIBRARY_PATH=$GAUSSHOME/lib:$LD_LIBRARY_PATH

        commitid=$(LD_PRELOAD=''  ${BUILD_DIR}/bin/gaussdb -V | awk '{print $6}' | cut -d ")" -f 1)
        echo "${commitid}" >>${SCRIPT_DIR}/version.cfg
        echo "End insert commitid into version.cfg" >> "$LOG_FILE" 2>&1
        # Remove the license control file, and restore the source code.
        echo "Restore gaussdb_version.cpp file." >> "$LOG_FILE" 2>&1
        restore_license_control
        echo "Restore gaussdb_version.cpp file success." >> "$LOG_FILE" 2>&1

        cp ${BINARYLIBS_PATH}/${PLAT_FORM_STR}/iperf/comm/bin/iperf3 ${BUILD_DIR}/bin
        if [ $? -ne 0 ]; then
            die "cp ${BINARYLIBS_PATH}/${PLAT_FORM_STR}/iperf/comm/bin/iperf3 ${BUILD_DIR}/bin failed"
        fi

        cp ${BINARYLIBS_PATH}/${PLAT_FORM_STR}/iperf/comm/lib/libiperf.so.0 ${BUILD_DIR}/lib
        if [ $? -ne 0 ]; then
            die "cp ${BINARYLIBS_PATH}/${PLAT_FORM_STR}/iperf/comm/lib/libiperf.so.0 ${BUILD_DIR}/lib failed"
        fi

        cp ${BINARYLIBS_PATH}/${PLAT_FORM_STR}/fio/comm/bin/fio ${BUILD_DIR}/bin
        if [ $? -ne 0 ]; then
            die "cp ${BINARYLIBS_PATH}/${PLAT_FORM_STR}/fio/comm/bin/fio ${BUILD_DIR}/bin failed"
        fi
    fi
}

#######################################################################

#######################################################################
# make package for gsql
#######################################################################
function make_package_gsql()
{
    # mkdir temp directory
    mkdir -p gsql
    mkdir -p gsql/bin
    mkdir -p gsql/lib
    mkdir -p gsql/gs_ktool_file

    # copy gsql and depend *.so
    cp ${BUILD_DIR}/bin/gsql gsql/bin
    if [ $? -ne 0 ]; then
        die "copy gsql failed."
    fi

    cp ${BUILD_DIR}/bin/gs_ktool gsql/bin
    if [ $? -ne 0 ]; then
        die "copy gsql failed."
    fi

    cp -r ${BUILD_DIR}/etc/gs_ktool_file/gs_ktool_conf.ini gsql/gs_ktool_file
    if [ $? -ne 0 ]; then
        die "copy gs_ktool_con.ini failed."
    fi

    cd gsql
    tar -xvf ${package_path}/${libpq_package_name}
    if [ $? -ne 0 ]; then
        die "unpack libpq failed."
    fi
    rm -f *.docx
    chmod 700 ./lib/*.so*
    cd ..

    cp $SCRIPT_DIR/gsql_env.sh gsql/gsql_env.sh
    if [ $? -ne 0 ]; then
        die "copy gsql_env.sh failed."
    fi
    chmod +x gsql/gsql_env.sh

    # make package
    cd gsql
    echo "packaging gsql..."
    tar -zcf "${gsql_package_name}" ./* >>"$LOG_FILE" 2>&1
    if [ $? -ne 0 ]; then
        die "tar ${gsql_package_name} failed"
    fi
    mv ${gsql_package_name} ${package_path}

    # clean tmp directory
    cd .. && rm -rf gsql

    echo "install $pkgname tools is ${gsql_package_name} of ${package_path} directory " >> "$LOG_FILE" 2>&1
    echo "success!"
}

#######################################################################
##select package type according to variable package_type
#######################################################################
function mpp_pkg_make()
{
    case "$package_type" in
        server)
            echo "file list: $release_file_list"
            make_package $release_file_list  'server'
            make_package $release_file_list  'libpq'
            make_package_gsql
            make_package $release_file_list  'gds'
            ;;
        jdbc)
            make_package $release_file_list  'jdbc'
            ;;
        odbc)
            make_package $release_file_list  'odbc'
            ;;
        libpq)
            make_package $release_file_list  'libpq'
            ;;
        gsql)
            make_package $release_file_list  'libpq'
            make_package_gsql
            ;;
        gds)
            make_package $release_file_list  'gds'
            ;;
    esac
}
declare package_command
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
        rpm)
            rpm='rpm'
            option=' -i'
            package_command="$rpm$option"
            ;;
    esac
}

###############################################################
##  client tools                                       package
##   Roach                                               yes
##   sslcert                                             no
##   Data Studio                                         no
##   Database Manager                                    no
##   Migration Toolkit                                   no
##   Cluster Configuration Assistant (CCA)               no
##   CAT                                                 no
###############################################################
function target_file_copy_for_non_server()
{
    for file in $(echo $1)
    do
        tar -cpf - $file  | ( cd $2; tar -xpf -  )
    done

    if [ "$3"x = "client"x ]; then
        # copy Roach tools
        mkdir -p $2/gs_roach
        mkdir -p $2/gs_roach/script/util
        if [ "$version_mode"x == "release"x ]; then
            cp -r ${ROACH_DIR}/bin/package/${PLAT_FORM_STR}/release/bin ${ROACH_DIR}/bin/package/${PLAT_FORM_STR}/release/lib ${ROACH_DIR}/bin/package/${PLAT_FORM_STR}/release/share ${ROACH_DIR}/bin/package/${PLAT_FORM_STR}/release/symbols $2/gs_roach/
            cp -f ${script_dir}/other/roach/util/GSroach* $2/gs_roach/script/util
        else
            cp -r ${ROACH_DIR}/bin/package/${PLAT_FORM_STR}/debug/bin ${ROACH_DIR}/bin/package/${PLAT_FORM_STR}/debug/lib ${ROACH_DIR}/bin/package/${PLAT_FORM_STR}/debug/share $2/gs_roach/
            cp -f ${script_dir}/other/roach/util/GSroach* $2/gs_roach/script/util
        fi
        if [ $? -ne 0 ]; then
            if [ "$version_mode"x == "release"x ]; then
                die "cp -r ${ROACH_DIR}/bin/package/${PLAT_FORM_STR}/release/* $2/gs_roach/ failed"
            else
                die "cp -r ${ROACH_DIR}/bin/package/${PLAT_FORM_STR}/debug/* $2/gs_roach/ failed"
            fi
        fi
    fi
}

declare bin_name="${package_pre_name}.bin"
declare sha256_name=''
declare script_dir="${ROOT_DIR}/script"
declare root_script=''
declare bin_script=''
#######################################################################
##copy target file into temporary directory temp
#######################################################################
function target_file_copy()
{
    ###################################################
    # make bin package
    ###################################################
    for file in $(echo $1)
    do
        tar -cpf - $file  | ( cd $2; tar -xpf -  )
    done

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

    # copy script which gs_roach depends from ${script_dir}/ to $2/bin/script
    cp -rf ${script_dir}/other/roach/util/ $2/bin/script/
    cp -f ${script_dir}/other/roach/local/* $2/bin/script/local/

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

    if [ "$PLATFORM_ARCH"X == "aarch64"X ] ; then
        # do nothing in current version
    echo ""
    else
        sed -i '/^process_cpu_affinity|/d'  $2/bin/cluster_guc.conf
    fi

    #generate bin file
    echo  "Begin generate ${bin_name}  bin file..."  >> "$LOG_FILE" 2>&1
    ${p7zpath}/7z a -t7z -sfx "${bin_name}" "$2/*" >> "$LOG_FILE" 2>&1
    if [ $? -ne 0 ]; then
        echo "Please check and makesure '7z' exist. "
        die "generate ${bin_name} failed."
    fi
    echo "End generate ${bin_name}  bin file"  >> "$LOG_FILE" 2>&1

    #generate sha256 file
    sha256_name="${package_pre_name}.sha256"
    echo  "Begin generate ${sha256_name} sha256 file..."  >> "$LOG_FILE" 2>&1
    sha256sum "${bin_name}" | awk -F" " '{print $1}' > "$sha256_name"
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
    mv ${bin_name} ${sha256_name} $2
    for file in $(echo $root_script)
    do
        tar -cpf - $file  | ( cd $2; tar -xpf -  )
    done

    # copy script dir to temp path
    cp -rf $BUILD_DIR/script/gspylib/ $2/script/
    cp -rf $BUILD_DIR/script/impl/ $2/script/
    cp -rf $BUILD_DIR/script/local/ $2/script/

    # copy GaussRoach.py to script dir
    cp -f ${script_dir}/other/roach/GaussRoach.py $2/script/
    cp -f ${script_dir}/other/roach/SyncDataToStby.py $2/script/
    cp -f ${script_dir}/other/roach/CSVInfo.py $2/script/
    cp -f ${script_dir}/other/roach/HADR.py $2/script/
    cp -f ${script_dir}/other/roach/local/* $2/script/local/
    cp -rf ${script_dir}/other/roach/util/ $2/script/

    # copy agent tool to temp path
    res=$(cp -rf ${script_dir}/agent/ $2/ 2>&1)
    if [ $? -ne 0 ]; then
        die "copy ${script_dir}/agent to $2 failed. $res"
    fi
    res=$(cp -f ${script_dir}/agent/common/cmd_sender.py $2/script/ 2>&1)
    if [ $? -ne 0 ]; then
        die "copy ${script_dir}/agent/common/cmd_sender.py to $2/script/ failed. $res"
    fi
    res=$(cp -f ${script_dir}/agent/common/uploader.py $2/script/ 2>&1)
    if [ $? -ne 0 ]; then
        die "copy ${script_dir}/agent/common/uploader.py to $2/script/ failed. $res"
    fi
    res=$(cp -f ${script_dir}/agent/common/py_pstree.py $2/script/ 2>&1)
    if [ $? -ne 0 ]; then
        die "copy ${script_dir}/agent/common/py_pstree.py to $2/script/ failed. $res"
    fi
    # copy the default xml to temp path
    res=$(cp -f ${script_dir}/build/cluster_default_agent.xml $2/ 2>&1)
    if [ $? -ne 0 ]; then
        die "copy ${script_dir}/build/cluster_default_agent.xml to $2 failed. $res"
    fi
    # copy CBG shell tools to temp path
    res=$(cp -rf ${script_dir}/build/bin/ $2/ 2>&1)
    if [ $? -ne 0 ]; then
        die "copy ${script_dir}/build/bin to $2 failed. $res"
    fi
    # copy the CBG config template to temp path
    res=$(cp -rf ${script_dir}/build/configtemplate/ $2/ 2>&1)
    if [ $? -ne 0 ]; then
        die "copy ${script_dir}/build/configtemplate/ to $2 failed. $res"
    fi
    res=$(cp -f ${script_dir}/agent/om_agent.conf $2/configtemplate/ 2>&1)
    if [ $? -ne 0 ]; then
        die "copy ${script_dir}/agent/om_agent.conf to $2/configtemplate/ failed. $res"
    fi

    find $2/bin/ -type f -print0 | xargs -0 -n 10 -r dos2unix > /dev/null 2>&1 &&
    chmod -R +x $2/bin/ &&
    chmod -R +x $2/script/
}

#######################################################################
# read script file list from mpp_release_list
#######################################################################
function read_script_file()
{
    cd $SCRIPT_DIR
    local head=$(cat $releasefile | grep "\[script\]" -n | awk -F: '{print $1}')
    if [ ! -n "$head" ]; then
        die "error: ono find $pkgname in the $releasefile file "
    fi

    local tail=$(cat $releasefile | sed "1,$head d" | grep "^\[" -n | sed -n "1p" |  awk -F: '{print $1}')
    if [ ! -n "$tail" ]; then
        local all=$(cat $releasefile | wc -l)
        let tail=$all+1-$head
    fi

    touch script_file
    cat $releasefile | awk "NR==$head+1,NR==$tail+$head-1" >script_file
}

#######################################################################
##function make_package have three actions
##1.parse release_file_list variable represent file
##2.copy target file into a newly created temporary directory temp
##3.package all file in the temp directory and renome to destination package_path
#######################################################################
function make_package()
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

    dest=$(cat $releasefile | awk "NR==$head+1,NR==$tail+$head-1")
    if [ "$pkgname"x = "libpq"x  -a "$version_mode" = "debug" ]; then
        # copy include file
        head=$(cat $releasefile | grep "\[header\]" -n | awk -F: '{print $1}')
        if [ ! -n "$head" ]; then
            die "error: ono find header in the $releasefile file "
        fi

        tail=$(cat $releasefile | sed "1,$head d" | grep "^\[" -n | sed -n "1p" |  awk -F: '{print $1}')
        if [ ! -n "$tail" ]; then
            all=$(cat $releasefile | wc -l)
            let tail=$all+1-$head
        fi

        dest1=$(cat $releasefile | awk "NR==$head+1,NR==$tail+$head-1")
        # copy cm depend library file
        head=$(cat $releasefile | grep "\[cmlibrary\]" -n | awk -F: '{print $1}')
        if [ ! -n "$head" ]; then
            die "error: ono find cmlibrary in the $releasefile file "
        fi

        tail=$(cat $releasefile | sed "1,$head d" | grep "^\[" -n | sed -n "1p" |  awk -F: '{print $1}')
        if [ ! -n "$tail" ]; then
            all=$(cat $releasefile | wc -l)
            let tail=$all+1-$head
        fi

        dest2=$(cat $releasefile | awk "NR==$head+1,NR==$tail+$head-1")
        dest=$(echo "$dest";echo "$dest1";echo "$dest2")
    fi

    if [ "$pkgname"x = "server"x ]; then
        read_script_file
    fi

    mkdir -p ${BUILD_DIR}
    cd ${BUILD_DIR}
    rm -rf temp
    mkdir temp
    if [ "$pkgname"x = "server"x ]; then
        copy_script_file "$script_dir/" ${BUILD_DIR}
    fi

    case "$pkgname" in
        server)
        mkdir -p ${BUILD_DIR}/temp/etc
            target_file_copy "$dest" ${BUILD_DIR}/temp
            ;;
        *)
            target_file_copy_for_non_server "$dest" ${BUILD_DIR}/temp $pkgname
            ;;
    esac

    cd ${BUILD_DIR}/temp
    select_package_command

    case "$pkgname" in
        client)
            echo "packaging client..."
            $package_command "${client_package_name}" ./*  >>"$LOG_FILE" 2>&1
            if [ $? -ne 0 ]; then
                die "$package_command ${client_package_name} failed"
            fi

            mv ${client_package_name} ${package_path}
            echo "install $pkgname tools is ${client_package_name} of the current directory  " >> "$LOG_FILE" 2>&1
            echo "success!"
            ;;
        server)
            echo "packaging server..."
            cp ${SCRIPT_DIR}/version.cfg ${BUILD_DIR}/temp
            if [ $? -ne 0 ]; then
                die "copy ${SCRIPT_DIR}/version.cfg to ${BUILD_DIR}/temp failed"
            fi

            # copy script which gs_roach depends from ${script_dir}/script/util to ${BUILD_DIR}/script/util
            cp -rf ${script_dir}/other/roach/util/ ${BUILD_DIR}/temp/script/
            replace_omtools_version ${BUILD_DIR}/temp/

            #copy inspection lib
            mkdir -p ${BUILD_DIR}/temp/script/gspylib/inspection/output/log/ &&
            mkdir -p ${BUILD_DIR}/temp/script/gspylib/inspection/output/nodes/ &&
            mkdir -p ${BUILD_DIR}/temp/script/gspylib/inspection/lib/asn1crypto/ &&
            mkdir -p ${BUILD_DIR}/temp/script/gspylib/inspection/lib/bcrypt/ &&
            mkdir -p ${BUILD_DIR}/temp/script/gspylib/inspection/lib/cffi/ &&
            mkdir -p ${BUILD_DIR}/temp/script/gspylib/inspection/lib/cryptography/ &&
            mkdir -p ${BUILD_DIR}/temp/script/gspylib/inspection/lib/enum/ &&
            mkdir -p ${BUILD_DIR}/temp/script/gspylib/inspection/lib/idna/ &&
            mkdir -p ${BUILD_DIR}/temp/script/gspylib/inspection/lib/nacl/ &&
            mkdir -p ${BUILD_DIR}/temp/script/gspylib/inspection/lib/pyasn1/ &&
            mkdir -p ${BUILD_DIR}/temp/script/gspylib/inspection/lib/kafka-python/ &&
            mkdir -p ${BUILD_DIR}/temp/script/gspylib/inspection/lib/pycparser/ &&
            mkdir -p ${BUILD_DIR}/temp/script/gspylib/inspection/lib/OpenSSL/ &&
            mkdir -p ${BUILD_DIR}/temp/script/gspylib/inspection/lib/psutil/ &&
            mkdir -p ${BUILD_DIR}/temp/script/gspylib/inspection/lib/netifaces/ &&
            mkdir -p ${BUILD_DIR}/temp/script/gspylib/inspection/lib/paramiko/ &&
            mkdir -p ${BUILD_DIR}/temp/script/gspylib/inspection/lib/bottle/ &&
            mkdir -p ${BUILD_DIR}/temp/script/gspylib/inspection/lib/paste/ &&

            cp -rf ${BINARYLIBS_PATH}/install_tools${ARCHITECTURE_EXTRA_FLAG}/asn1crypto/ ${BUILD_DIR}/temp/script/gspylib/inspection/lib/
            cp -rf ${BINARYLIBS_PATH}/install_tools${ARCHITECTURE_EXTRA_FLAG}/bcrypt/ ${BUILD_DIR}/temp/script/gspylib/inspection/lib/
            cp -rf ${BINARYLIBS_PATH}/install_tools${ARCHITECTURE_EXTRA_FLAG}/cffi/ ${BUILD_DIR}/temp/script/gspylib/inspection/lib/
            cp -rf ${BINARYLIBS_PATH}/install_tools${ARCHITECTURE_EXTRA_FLAG}/cryptography/ ${BUILD_DIR}/temp/script/gspylib/inspection/lib/
            cp -rf ${BINARYLIBS_PATH}/install_tools${ARCHITECTURE_EXTRA_FLAG}/enum/ ${BUILD_DIR}/temp/script/gspylib/inspection/lib/
            cp -rf ${BINARYLIBS_PATH}/install_tools${ARCHITECTURE_EXTRA_FLAG}/idna/ ${BUILD_DIR}/temp/script/gspylib/inspection/lib/
            cp -rf ${BINARYLIBS_PATH}/install_tools${ARCHITECTURE_EXTRA_FLAG}/nacl/ ${BUILD_DIR}/temp/script/gspylib/inspection/lib/
            cp -rf ${BINARYLIBS_PATH}/install_tools${ARCHITECTURE_EXTRA_FLAG}/pyasn1/ ${BUILD_DIR}/temp/script/gspylib/inspection/lib/
            cp -rf ${BINARYLIBS_PATH}/install_tools${ARCHITECTURE_EXTRA_FLAG}/kafka-python/ ${BUILD_DIR}/temp/script/gspylib/inspection/lib/
            cp -rf ${BINARYLIBS_PATH}/install_tools${ARCHITECTURE_EXTRA_FLAG}/pycparser/ ${BUILD_DIR}/temp/script/gspylib/inspection/lib/
            cp -rf ${BINARYLIBS_PATH}/install_tools${ARCHITECTURE_EXTRA_FLAG}/OpenSSL/ ${BUILD_DIR}/temp/script/gspylib/inspection/lib/
            cp -rf ${BINARYLIBS_PATH}/install_tools${ARCHITECTURE_EXTRA_FLAG}/ipaddress.py ${BUILD_DIR}/temp/script/gspylib/inspection/lib/
            cp -rf ${BINARYLIBS_PATH}/install_tools${ARCHITECTURE_EXTRA_FLAG}/six.py ${BUILD_DIR}/temp/script/gspylib/inspection/lib/
            cp -rf ${BINARYLIBS_PATH}/install_tools${ARCHITECTURE_EXTRA_FLAG}/_cffi_backend.py ${BUILD_DIR}/temp/script/gspylib/inspection/lib/
            cp -rf ${BINARYLIBS_PATH}/install_tools${ARCHITECTURE_EXTRA_FLAG}/_cffi_backend.so_UCS2 ${BUILD_DIR}/temp/script/gspylib/inspection/lib/
            cp -rf ${BINARYLIBS_PATH}/install_tools${ARCHITECTURE_EXTRA_FLAG}/_cffi_backend.so_UCS4 ${BUILD_DIR}/temp/script/gspylib/inspection/lib/
            cp -rf ${BINARYLIBS_PATH}/install_tools${ARCHITECTURE_EXTRA_FLAG}/psutil/ ${BUILD_DIR}/temp/script/gspylib/inspection/lib/
            cp -rf ${BINARYLIBS_PATH}/install_tools${ARCHITECTURE_EXTRA_FLAG}/netifaces/ ${BUILD_DIR}/temp/script/gspylib/inspection/lib/
            cp -rf ${BINARYLIBS_PATH}/install_tools${ARCHITECTURE_EXTRA_FLAG}/paramiko/ ${BUILD_DIR}/temp/script/gspylib/inspection/lib/
            cp -rf ${BINARYLIBS_PATH}/install_tools${ARCHITECTURE_EXTRA_FLAG}/bottle/ ${BUILD_DIR}/temp/script/gspylib/inspection/lib/
            cp -rf ${BINARYLIBS_PATH}/install_tools${ARCHITECTURE_EXTRA_FLAG}/paste/ ${BUILD_DIR}/temp/script/gspylib/inspection/lib/

            if [ $? -ne 0 ]; then
                die "remove svn info failed."
            fi

            cp -r ${BINARYLIBS_PATH}/install_tools${ARCHITECTURE_EXTRA_FLAG} ./install_tools
            find ./install_tools -name .svn -type d -print0 | xargs -0 rm -rf
            if [ $? -ne 0 ]; then
                die "remove svn info failed."
            fi

            mkdir -p ./lib

            mv ./install_tools/asn1crypto               ./lib
            mv ./install_tools/bcrypt                   ./lib
            mv ./install_tools/cffi                     ./lib
            mv ./install_tools/cryptography             ./lib
            mv ./install_tools/enum                     ./lib
            mv ./install_tools/idna                     ./lib
            mv ./install_tools/nacl                     ./lib
            mv ./install_tools/pyasn1                   ./lib
            mv ./install_tools/kafka-python             ./lib
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
            mv ./install_tools/unixodbc .
            mv ./install_tools/paste                    ./lib
            mv ./install_tools/bottle                   ./lib

            #Not package unixodbc/bin/odbc_config, so delete it
            rm -f ./unixodbc/bin/odbc_config

            rm -r ./install_tools

	    if [ "$product_mode"x == "multiple"x ]
            then
                mkdir -p ./libcgroup/bin
                if [ $? -ne 0 ]; then
                        die "mkdir -p ./libcgroup/bin failed"
                fi

                cp ${BUILD_DIR}/bin/gs_cgroup ./libcgroup/bin
                if [ $? -ne 0 ]; then
                        die "cp ${BUILD_DIR}/bin/gs_cgroup ./libcgroup/bin failed"
                fi
                mkdir -p ./libcgroup/lib
                if [ $? -ne 0 ]; then
                        die "mkdir -p ./libcgroup/lib failed"
                fi
                cp ${BINARYLIBS_PATH}/${PLAT_FORM_STR}/libcgroup/${COMPLIE_TYPE}/lib/libcgroup.so ./libcgroup/lib
                if [ $? -ne 0 ]; then
                        die "cp ${BINARYLIBS_PATH}/${PLAT_FORM_STR}/libcgroup/${COMPLIE_TYPE}/lib/libcgroup.so ./libcgroup/lib failed"
                fi
            fi


            #compress the agent package
            echo "Agent package is starting."
            cp ./lib/_cffi_backend.so_UCS4 ./lib/_cffi_backend.so
            cp -r ./script/gspylib/pssh/bin ./agent/
            cp -r ./script/gspylib/clib ./agent/
            if [ "$product_mode"x == "single"x ]
            then
                if [ ! -e ./agent/centralized_cluster ]
                then
                        touch ./agent/centralized_cluster
                        echo "This file is used only to distinguish cluster types (generated by the packaging script)." >> ./agent/centralized_cluster
                else
                        echo "This file is used only to distinguish cluster types (generated by the packaging script)." > ./agent/centralized_cluster
                fi
            fi
            $package_command "${agent_package_name}" ./agent ./lib ./cluster_default_agent.xml ./version.cfg >>"$LOG_FILE" 2>&1
            if [ $? -ne 0 ]; then
                die "$package_command ${agent_package_name} failed"
            fi
            mv ${agent_package_name} ${package_path}
            echo "Agent package has been finished."

            #remove the agent path which only needed by agent before compress server package
            echo "Server package is starting."
            rm -rf ./agent
            cp -a ${BUILD_TOOLS_PATH}/secbox .
            if [ -e secbox/libso ] ; then rm -rf secbox/libso ; fi
            mkdir -p secbox/libso/
            cp -a ${BUILD_DIR}/lib/libcgroup.so secbox/libso/libcgroup.so.1
            cp -a ${BUILD_DIR}/lib/libsecurec.so secbox/libso/libsecurec.so

            # install upgrade_sql.* files.
            package_upgrade_sql

            $package_command "${server_package_name}" ./* >>"$LOG_FILE" 2>&1
            if [ $? -ne 0 ]; then
                die "$package_command ${server_package_name} failed"
            fi
            mv ${server_package_name} ${package_path}
            echo "install $pkgname tools is ${server_package_name} of ${package_path} directory " >> "$LOG_FILE" 2>&1
            echo "success!"
            ;;
        libpq)
            echo "packaging libpq..."
            $package_command "${libpq_package_name}" ./* >>"$LOG_FILE" 2>&1
            if [ $? -ne 0 ]; then
                die "$package_command ${libpq_package_name} failed"
            fi
            mv ${libpq_package_name} ${package_path}
            echo "install $pkgname tools is ${libpq_package_name} of ${package_path} directory " >> "$LOG_FILE" 2>&1
            echo "success!"
            ;;
        gds)
            mkdir gds config lib_tmp
            cp ./bin/* ./gds
            cp ./share/postgresql/* ./config
            cp ./lib/libcjson* ./lib_tmp
            mv  config ./gds/
            rm -rf ./bin ./share ./lib
            mv ./lib_tmp ./lib
            echo "packaging gds..."
            $package_command "${gds_package_name}" ./* >>"$LOG_FILE" 2>&1
            if [ $? -ne 0 ]; then
                die "$package_command ${gds_package_name} failed"
            fi
            mv ${gds_package_name} ${package_path}
            echo "install $pkgname tools is ${gds_package_name} of ${package_path} directory " >> "$LOG_FILE" 2>&1
            echo "success!"
            ;;
    esac
}

#######################################################################
##copy all file of script directory to target directory
#######################################################################
function copy_script_file()
{
    target_file=$1
    local target_dir=$2

    cp -rf $target_file/script/ $target_dir/ &&
    find $target_dir/script/ -type f -print0 | xargs -0 -n 10 -r dos2unix > /dev/null 2>&1 &&
    find $target_dir/script/gspylib/inspection/ -name d2utmp* -print0 | xargs -0 rm -rf &&
    cp -rf $target_file/script/gspylib/inspection/lib/checknetspeed/speed_test* $target_dir/script/gspylib/inspection/lib/checknetspeed/ &&
    cp -rf $target_file/script/gspylib/inspection/lib/*.png $target_dir/script/gspylib/inspection/lib/ &&

    if [ $? -ne 0 ]; then
        die "cp -r $target_file $target_dir failed "
    fi
}
#######################################################################
## generate the version file.
#######################################################################
function make_license_control()
{
    local target_dir=$1
    python_exec=$(which python 2>/dev/null)

    if [ -x "$python_exec" ]; then
        $python_exec ${binarylib_dir}/buildtools/license_control/encrypted_version_file.py >> "$LOG_FILE" 2>&1
    fi

    if [ $? -ne 0 ]; then
        die "create ${binarylib_dir}/buildtools/license_control license file failed."
    fi

    if [ -f "$gaussdb_200_file" ] && [ -f "$gaussdb_300_file" ]; then
        # Get the md5sum.
        gaussdb_200_sha256sum=$(sha256sum $gaussdb_200_file | awk '{print $1}')
        gaussdb_300_sha256sum=$(sha256sum $gaussdb_300_file | awk '{print $1}')
        # Modify the source code.
        sed -i "s/^[ \t]*const[ \t]\+char[ \t]*\*[ \t]*sha256_digests[ \t]*\[[ \t]*SHA256_DIGESTS_COUNT[ \t]*\][ \t]*=[ \t]*{[ \t]*NULL[ \t]*,[ \t]*NULL[ \t]*}[ \t]*;[ \t]*$/const char \*sha256_digests\[SHA256_DIGESTS_COUNT\] = {\"$gaussdb_200_sha256sum\", \"$gaussdb_300_sha256sum\"};/g" $gaussdb_version_file
    fi

    if [ $? -ne 0 ]; then
        die "modify '$gaussdb_version_file' failed."
    fi
}
#######################################################################
## copy the version file to target directory.
#######################################################################
function copy_license_file()
{
    local target_dir=$1

    # Copy the version file to bin path.
    if [ -f "$gaussdb_200_file" ] && [ -f "$gaussdb_200_file" ] && [ -f "$gaussdb_200_standard_file" ]; then
        cp -f $gaussdb_200_file $target_dir &&
        cp -f $gaussdb_300_file $target_dir &&
        cp -f $gaussdb_200_standard_file $target_dir
    fi

    if [ $? -ne 0 ]; then
        die "cp -r ${binarylib_dir}/buildtools/license_control $target_dir failed."
    fi
}
#######################################################################
## restore the gaussdb_version.cpp content.
#######################################################################
function restore_license_control()
{
    # Generate license control file.
    make_license_control

    # Restore the gaussdb_version.cpp content.
    if [ -f "$gaussdb_200_file" ] && [ -f "$gaussdb_300_file" ] && [ -f "$gaussdb_200_standard_file" ]; then
        sed -i "s/^[ \t]*const[ \t]\+char[ \t]*\*[ \t]*sha256_digests[ \t]*\[[ \t]*SHA256_DIGESTS_COUNT[ \t]*\][ \t]*=[ \t]*{[ \t]*[a-zA-Z0-9\"]\+[ \t]*,[ \t]*[a-zA-Z0-9\"]\+[ \t]*}[ \t]*;[ \t]*$/const char \*sha256_digests\[SHA256_DIGESTS_COUNT\] = {NULL, NULL};/g" $gaussdb_version_file &&

        # Remove the gaussdb.version file.
        rm -f $gaussdb_200_file &&
        rm -f $gaussdb_300_file &&
        rm -f $gaussdb_200_standard_file
    fi

    if [ $? -ne 0 ]; then
        die "restore '$gaussdb_version_file' failed, remove ${binarylib_dir}/buildtools/license_control file failed."
    fi
}

#############################################################
# show package for hotpatch sdv.
#############################################################
if [ "$show_package" = true ]; then
    echo "package: "$server_package_name
    echo "bin: "$bin_name
    exit 0
fi

#############################################################
# main function
#############################################################
# 1. clean install path and log file
mpp_pkg_pre_check

# 2. chose action
mpp_pkg_bld
if [ "$zip_package" = "off" ]; then
    echo "The option 'nopkg' is on, no package will be zipped."
    exit 0
fi

# 3. make package
mpp_pkg_make

#clean mpp_install directory
echo "clean enviroment"
echo "[makemppdb] $(date +%y-%m-%d' '%T): remove ${BUILD_DIR}" >>"$LOG_FILE" 2>&1

mkdir ${ROOT_DIR}/output
mv ${ROOT_DIR}/build/script/*.tar.gz ${ROOT_DIR}/output/
echo "now, all packages has finished!"

exit 0
