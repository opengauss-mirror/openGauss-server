#!/bin/bash
#######################################################################
# Copyright (c): 2020-2021, Huawei Tech. Co., Ltd.
# descript: Compile and pack MPPDB
#           Return 0 means OK.
#           Return 1 means failed.
# version:  2.0
# date:     2021-12-12
#######################################################################

##default package type is server
declare package_type='server'
declare install_package_format='tar'
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
ext_version=""
if [ -f "/etc/euleros-release" ]; then
    kernel=$(cat /etc/euleros-release | awk -F ' ' '{print $1}' | tr A-Z a-z)
    version=$(cat /etc/euleros-release | awk -F '(' '{print $2}'| awk -F ')' '{print $1}' | tr A-Z a-z)
    ext_version=$version
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
    dist_version="CentOS"
elif [ X"$kernel" == X"openeuler" ]; then
    dist_version="openEuler"
elif [ X"$kernel" == X"kylin" ]; then
    dist_version="kylin"
else
    dist_version="Platform"
fi

show_package=false
gcc_version="10.3.1"

##add platform architecture information
cpus_num=$(grep -w processor /proc/cpuinfo|wc -l)
PLATFORM_ARCH=$(uname -p)
if [ "$PLATFORM_ARCH"X == "aarch64"X ] ; then
    ARCHITECTURE_EXTRA_FLAG=_euleros2.0_${ext_version}_$PLATFORM_ARCH
    release_file_list="aarch64_lite_list"
else
    ARCHITECTURE_EXTRA_FLAG=_euleros2.0_sp5_${PLATFORM_ARCH}
    release_file_list="x86_64_lite_list"
fi

##default install version storage path
declare mppdb_version='openGauss Lite'
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
SCRIPT_NAME=$(basename $SCRIPT_PATH)
SCRIPT_DIR=$(dirname "${SCRIPT_PATH}")
SCRIPT_DIR=$(dirname "$SCRIPT_DIR")

if [ ! -f "$SCRIPT_DIR/$SCRIPT_NAME" ] ; then
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
            echo "./cmake_package_internal.sh --help or ./cmake_package_internal.sh -h"
            exit 1
    esac
done

read_mpp_version

if [ "$gcc_version" == "7.3.0" ]; then
    gcc_version=${gcc_version:0:3}
elif [ "$gcc_version" == "8.3.0" ]; then
    if [ $PLATFORM_ARCH == "loongarch64" ];then 
        gcc_version=${gcc_version:0:3}
    else
        echo "Only loongarch model use gcc8.3"
        exit 1
    fi 
elif [ "$gcc_version" == "10.3.0" ] || [ "$gcc_version" == "10.3.1" ]; then
    gcc_version=${gcc_version:0:4}
else
    echo "Unknown gcc version $gcc_version"
    exit 1
fi

#######################################################################
## declare all package name
#######################################################################
declare version_string="${mppdb_name_for_package}-${version_number}"
declare package_pre_name="${version_string}-${dist_version}-${PLATFORM_ARCH}"
declare server_package_name="${package_pre_name}.${install_package_format}.gz"

declare libpq_package_name="${package_pre_name}-Libpq.${install_package_format}.gz"
declare symbol_package_name="${package_pre_name}-symbol.${install_package_format}.gz"

echo "[makemppdb] $(date +%y-%m-%d' '%T): script dir : ${SCRIPT_DIR}"
ROOT_DIR=$(dirname "$SCRIPT_DIR")
ROOT_DIR=$(dirname "$ROOT_DIR")

CMAKE_BUILD_DIR=${ROOT_DIR}/tmp_build
declare LOG_FILE="${ROOT_DIR}/build/script/makemppdb_pkg.log"
declare BUILD_DIR="${ROOT_DIR}/mppdb_temp_install"
declare ERR_MKGS_FAILED=1
declare MKGS_OK=0
if [ "${binarylib_dir}" != 'None' ] && [ -d "${binarylib_dir}" ]; then
    BUILD_TOOLS_PATH="${binarylib_dir}/buildtools/"
    PLATFORM_PATH="${binarylib_dir}/kernel/platform/"
    BINARYLIBS_PATH="${binarylib_dir}/kernel/dependency"
else
    BUILD_TOOLS_PATH="${ROOT_DIR}/buildtools/"
    PLATFORM_PATH="${ROOT_DIR}/kernel/platform/"
    BINARYLIBS_PATH="${ROOT_DIR}/binarylibs"
fi

declare UPGRADE_SQL_DIR="${ROOT_DIR}/src/include/catalog/upgrade_sql"

export CC="$BUILD_TOOLS_PATH/gcc$gcc_version/gcc/bin/gcc"
export CXX="$BUILD_TOOLS_PATH/gcc$gcc_version/gcc/bin/g++"
export LD_LIBRARY_PATH=$BUILD_TOOLS_PATH/gcc$gcc_version/gcc/lib64:$BUILD_TOOLS_PATH/gcc$gcc_version/isl/lib:$BUILD_TOOLS_PATH/gcc$gcc_version/mpc/lib/:$BUILD_TOOLS_PATH/gcc$gcc_version/mpfr/lib/:$BUILD_TOOLS_PATH/gcc$gcc_version/gmp/lib/:$LD_LIBRARY_PATH
export PATH=$BUILD_TOOLS_PATH/gcc$gcc_version/gcc/bin:$PATH
jdkpath=${binarylib_dir}/kernel/platform/huaweijdk8/${PLATFORM_ARCH}/jdk
if [ ! -d "${jdkpath}" ]; then
    jdkpath=${binarylib_dir}/kernel/platform/openjdk8/${PLATFORM_ARCH}/jdk
fi
export JAVA_HOME=${jdkpath}

declare p7zpath="${BUILD_TOOLS_PATH}/p7z/bin"

###################################
# build parameter about enable-llt
##################################
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
}

#######################################################################
# Install all SQL files from distribute/include/catalog/upgrade_sql
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
##install gaussdb database and others
##select to install something according to variables package_type need
#######################################################################
function mpp_pkg_bld()
{
    install_gaussdb
}

#######################################################################
##install gaussdb database contained server,client and libpq
#######################################################################
function install_gaussdb()
{
    # Generate the license control file, and set md5sum string to the code.
    echo "Modify gaussdb_version.cpp file." >> "$LOG_FILE" 2>&1
    echo "Modify gaussdb_version.cpp file success." >> "$LOG_FILE" 2>&1
    cd "$ROOT_DIR/"
    if [ $? -ne 0 ]; then
        die "change dir to $SRC_DIR failed."
    fi

    if [ "$version_mode" = "debug" -a "$separate_symbol" = "on" ]; then
        echo "WARNING: do not separate symbol in debug mode!"
    fi

    binarylibs_path=${ROOT_DIR}/binarylibs
    if [ "${binarylib_dir}"x != "None"x ]; then
        binarylibs_path=${binarylib_dir}
    fi

    export BUILD_TUPLE=${PLATFORM_ARCH}
    export THIRD_BIN_PATH="${binarylibs_path}"
    export PREFIX_HOME="${BUILD_DIR}"
    export ENABLE_LITE_MODE=ON

    export WITH_TASSL="${build_with_tassl}"

    if [ "$version_mode"x == "release"x ]; then
    	CMAKE_OPT="-DENABLE_MULTIPLE_NODES=OFF -DENABLE_PRIVATEGAUSS=OFF -DENABLE_THREAD_SAFETY=ON -DENABLE_LITE_MODE=ON"
    	export DEBUG_TYPE=release
    elif [ "$version_mode"x == "memcheck"x ]; then
    	CMAKE_OPT="-DENABLE_MULTIPLE_NODES=OFF -DENABLE_PRIVATEGAUSS=OFF -DENABLE_THREAD_SAFETY=ON -DENABLE_LITE_MODE=ON"
    	export DEBUG_TYPE=memcheck
    else
    	CMAKE_OPT="-DENABLE_MULTIPLE_NODES=OFF -DENABLE_PRIVATEGAUSS=OFF -DENABLE_THREAD_SAFETY=ON -DENABLE_LITE_MODE=ON"
    	export DEBUG_TYPE=debug
    fi

    if [[ -e "/etc/openEuler-release" && "$(cat /etc/openEuler-release | awk '{print $3}')" == "22.03" ]]; then
        CMAKE_OPT="$CMAKE_OPT -DENABLE_OPENEULER_MAJOR=ON"
    fi

    if [ "${PLATFORM_ARCH}"x == "loongarch64"x ]; then
       CMAKE_OPT="$CMAKE_OPT -DENABLE_BBOX=OFF -DENABLE_JEMALLOC=OFF"
    fi
    
    echo "CMAKE_OPT----> $CMAKE_OPT"
    echo "Begin run cmake for gaussdb server" >> "$LOG_FILE" 2>&1
    echo "CMake options: ${CMAKE_OPT}" >> "$LOG_FILE" 2>&1
    echo "CMake release: ${DEBUG_TYPE}" >> "$LOG_FILE" 2>&1

    export GAUSSHOME=${BUILD_DIR}
    export LD_LIBRARY_PATH=${BUILD_DIR}/lib:${BUILD_DIR}/lib/postgresql:${LD_LIBRARY_PATH}

    cd ${ROOT_DIR}
    [ -d "${CMAKE_BUILD_DIR}" ] && rm -rf ${CMAKE_BUILD_DIR}
    [ -d "${BUILD_DIR}" ] && rm -rf ${BUILD_DIR}
    mkdir -p ${CMAKE_BUILD_DIR}
    cd ${CMAKE_BUILD_DIR}
    cmake .. ${CMAKE_OPT}
    echo "Begin make and install gaussdb server" >> "$LOG_FILE" 2>&1
    make VERBOSE=1 -sj ${cpus_num}
    if [ $? -ne 0 ]; then
        die "make failed."
    fi
    make install -sj ${cpus_num}
    if [ $? -ne 0 ]; then
        die "make install failed."
    fi
    ## check build specification
    spec="gaussdbkernel"
    if ( cat $SCRIPT_DIR/gauss.spec | grep 'PRODUCT' | grep 'GaussDB Kernel' >/dev/null 2>&1 ); then
        spec="gaussdbkernel"
    elif ( cat $SCRIPT_DIR/gauss.spec | grep 'PRODUCT' | grep 'openGauss' >/dev/null 2>&1 ); then
        spec="opengauss"
    fi

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

    #insert the commitid to version.cfg as the upgrade app path specification
    export PATH=${BUILD_DIR}:$PATH
    export LD_LIBRARY_PATH=$GAUSSHOME/lib:$LD_LIBRARY_PATH

    commitid=$(LD_PRELOAD=''  ${BUILD_DIR}/bin/gaussdb -V | cut -d ")" -f 1 | awk '{print $NF}')
    echo "${commitid}" >>${SCRIPT_DIR}/version.cfg
    echo "End insert commitid into version.cfg" >> "$LOG_FILE" 2>&1
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
            ;;
        libpq)
            make_package $release_file_list  'libpq'
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
##   Roach                                               no
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
}

declare bin_name="${package_pre_name}.bin"
declare sha256_name=''
declare script_dir="${ROOT_DIR}/script"

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
    cd $BUILD_DIR
    if [ "$PLATFORM_ARCH"X == "aarch64"X ] ; then
        # do nothing in current version
    echo ""
    else
        sed -i '/^process_cpu_affinity|/d'  $2/bin/cluster_guc.conf
    fi
    if [ "$(ls -A /lib64/libaio.so*)" != "" ]
    then
        cp /lib64/libaio.so* $2/lib/
    elif [ "$(ls -A /lib/libaio.so*)" != "" ]
    then
    	cp /lib/libaio.so* $2/lib/
    fi

    if [ "$(ls -A /lib64/libnuma.so*)" != "" ]
    then
        cp /lib64/libnuma.so* $2/lib/
    elif [ "$(ls -A /lib/libnuma.so*)" != "" ]
    then
    	cp /lib/libnuma.so* $2/lib/
    fi

    #generate bin file
    echo  "Begin generate ${bin_name}  bin file..."  >> "$LOG_FILE" 2>&1
    curpath=$(pwd)
    cd $2
    tar -zcf ${curpath}/${bin_name} .  >> "$LOG_FILE" 2>&1
    cd ${curpath}
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

    cp $2/lib/libstdc++.so.6 ./

    ###################################################
    # make server package
    ###################################################
    if [ -d "${2}" ]; then
        rm -rf ${2}
    fi
    mkdir -p ${2}
    mkdir -p $2/dependency
    cp libstdc++.so.6 $2/dependency
    mv ${bin_name} ${sha256_name} $2
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
    if [ "$pkgname"x = "libpq"x -a \( "$version_mode" = "debug" -o "$version_mode" = "release" \) ]; then
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
        dest=$(echo "$dest";echo "$dest1")
    fi

    mkdir -p ${BUILD_DIR}
    cd ${BUILD_DIR}
    rm -rf temp
    mkdir temp

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
        server)
            echo "packaging server..."
            cp ${SCRIPT_DIR}/version.cfg ${BUILD_DIR}/temp
            if [ $? -ne 0 ]; then
                die "copy ${SCRIPT_DIR}/version.cfg to ${BUILD_DIR}/temp failed"
            fi
            cp ${ROOT_DIR}/${open_gauss}/liteom/install.sh  ./
            if [ $? -ne 0 ]
            then
                die "copy ${ROOT_DIR}/${open_gauss}/liteom/install.sh to ${BUILD_DIR}/temp failed"
            fi

            cp ${ROOT_DIR}/${open_gauss}/liteom/uninstall.sh ./
            if [ $? -ne 0 ]
            then
                die "copy ${ROOT_DIR}/${open_gauss}/liteom/uninstall.sh to ${BUILD_DIR}/temp failed"
            fi

            cp ${ROOT_DIR}/${open_gauss}/liteom/opengauss_lite.conf ./
            if [ $? -ne 0 ]
            then
                die "copy ${ROOT_DIR}/${open_gauss}/liteom/opengauss_lite.conf to ${BUILD_DIR}/temp failed"
            fi

            # pkg upgrade scripts:upgrade_GAUSSV5.sh, upgrade_common.sh, upgrade_config.sh, upgrade_errorcode.sh
            for filename in upgrade_GAUSSV5.sh upgrade_common.sh upgrade_config.sh upgrade_errorcode.sh
            do
                if ! cp ${ROOT_DIR}/${open_gauss}/liteom/${filename} ./ ; then
                    die "copy ${ROOT_DIR}/${open_gauss}/liteom/${filename} to ${BUILD_DIR}/temp failed"
                fi
            done
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
    esac
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