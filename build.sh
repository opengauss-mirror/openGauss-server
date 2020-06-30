#!/bin/bash

declare build_version_mode='release'
declare build_binarylib_dir='None'
declare to_package='NO'
declare optimized='true'
#########################################################################
##read command line paramenters
#######################################################################

function print_help()
{
    echo "Usage: $0 [OPTION]
    -h|--help                         show help information
    -m|--version_mode                 this values of paramenter is debug, release or memcheck, the default value is release
    -3rd|--binarylib_dir              the parent directory of binarylibs
    -pkg|--package                    package the project,by default, only compile the project
    -nopt|--not_optimized             on kunpeng platform, like 1616 version, without LSE optimized
    "
}

while [ $# -gt 0 ]; do
    case "$1" in
        -h|--help)
            print_help
            exit 1
            ;;
        -m|--version_mode)
            if [ "$2"X = X ]; then
                echo "no given correct version information, such as: debug/release/memcheck"
                exit 1
            fi
            build_version_mode=$2
            shift 2
            ;;
        -pkg|--package)
            to_package='YES'
                        shift 1
            ;;
        -3rd|--binarylib_dir)
            if [ "$2"X = X ]; then
                echo "no given binarylib directory values"
                exit 1
            fi
            build_binarylib_dir=$2
            shift 2
            ;;

        -nopt|--not_optimized)
            optimized='false'
            shift 1
            ;;
         *)
            echo "Internal Error: option processing error: $1" 1>&2
            echo "please input right paramtenter, the following command may help you"
            echo "./package.sh --help or ./package.sh -h"
            exit 1
    esac
done

if [ "$build_version_mode" != "debug" ] && [ "$build_version_mode" != "release" ] && [ "$build_version_mode" != "memcheck" ]; then
    echo "no given correct version information, such as: debug/release/memcheck"
    exit 1
fi

ROOT_DIR=$(cd $(dirname "${BASH_SOURCE[0]}") && pwd)
echo "ROOT_DIR : $ROOT_DIR"

declare ERR_MKGS_FAILED=1
declare LOG_FILE="${ROOT_DIR}/make_compile.log"
declare BUILD_DIR="${ROOT_DIR}/dest"

PLAT_FORM_STR=$(sh "${ROOT_DIR}/src/get_PlatForm_str.sh")
if [ "${PLAT_FORM_STR}"x == "Failed"x ]
then
    echo "We only support OPENEULER(aarch64), CentOS(x86-64) platform."
    exit 1;
fi

##add platform architecture information
PLATFORM_ARCH=$(uname -p)
if [ "$PLATFORM_ARCH"X == "aarch64"X ] ; then
   GAUSSDB_EXTRA_FLAGS=" -D__USE_NUMA -D__ARM_LSE"
fi

gcc_version="8.2"

if [ ${build_binarylib_dir} != 'None' ] && [ -d ${build_binarylib_dir} ]; then
    BUILD_TOOLS_PATH="${build_binarylib_dir}/buildtools/${PLAT_FORM_STR}"
else
    BUILD_TOOLS_PATH="${ROOT_DIR}/binarylibs/buildtools/${PLAT_FORM_STR}"
fi

export CC=$BUILD_TOOLS_PATH/gcc$gcc_version/gcc/bin/gcc
export CXX=$BUILD_TOOLS_PATH/gcc$gcc_version/gcc/bin/g++
export LD_LIBRARY_PATH=$BUILD_TOOLS_PATH/gcc$gcc_version/gcc/lib64:$BUILD_TOOLS_PATH/gcc$gcc_version/isl/lib:$BUILD_TOOLS_PATH/gcc$gcc_version/mpc/lib/:$BUILD_TOOLS_PATH/gcc$gcc_version/mpfr/lib/:$BUILD_TOOLS_PATH/gcc$gcc_version/gmp/lib/:$LD_LIBRARY_PATH
export PATH=$BUILD_TOOLS_PATH/gcc$gcc_version/gcc/bin:$PATH

log()
{
    echo "[makegaussdb] $(date +%y-%m-%d' '%T): $@"
    echo "[makegaussdb] $(date +%y-%m-%d' '%T): $@" >> "$LOG_FILE" 2>&1
}

die()
{
    log "$@"
    echo "$@"
    exit $ERR_MKGS_FAILED
}

function srv_pkg_pre_check()
{
    if [ -d "$BUILD_DIR" ]; then
        rm -rf $BUILD_DIR
    fi
    if [ -d "$LOG_FILE" ]; then
        rm -rf $LOG_FILE
    fi
    if [ $? -eq 0 ]; then
        echo "Everything is ready."
    else
        echo "clean enviroment failed."
        exit 1
    fi
}
# 1. clean install path and log file
srv_pkg_pre_check

function getExtraFlags()
{
    if [ "$PLATFORM_ARCH"X == "aarch64"X ] ; then
        if [ "$dist_version" == "openEuler" ]; then
            GAUSSDB_EXTRA_FLAGS=" -D__USE_NUMA"
            if [ "${optimized}"x == "true"x ] ; then
                GAUSSDB_EXTRA_FLAGS=" -D__USE_NUMA -D__ARM_LSE"
                echo "Attention: Make sure your target platforms support LSE."
            fi
        fi
    fi
}


function compile_gaussdb()
{
    cd "$ROOT_DIR"
    echo "begin to make distclean"
    if [ $? -ne 0 ]; then
        die "change dir to $SRC_DIR failed."
    fi
    with_3rd=${ROOT_DIR}/binarylibs
    if [ "${build_binarylib_dir}"x != "None"x ]; then
        with_3rd=${build_binarylib_dir}
    fi
    getExtraFlags
    #configure
    make distclean -sj >> "$LOG_FILE" 2>&1
    echo "Begin configure, Please wait a few minutes..."
    chmod 755 configure

    if [ "${build_version_mode}"x == "release"x ]; then
        ./configure --prefix="${BUILD_DIR}" --3rd=${with_3rd} CFLAGS="-O2 -g3 ${GAUSSDB_EXTRA_FLAGS}" --enable-thread-safety --without-readline --without-zlib CC=g++ >> "$LOG_FILE" 2>&1
    elif [ "${build_version_mode}"x == "memcheck"x ]; then
        ./configure --prefix="${BUILD_DIR}" --3rd=${with_3rd} CFLAGS='-O0' --enable-debug --enable-cassert --enable-thread-safety --without-readline --without-zlib --enable-memory-check CC=g++  >> "$LOG_FILE" 2>&1
    else
        ./configure --prefix="${BUILD_DIR}" --3rd=${with_3rd} CFLAGS="-O0 ${GAUSSDB_EXTRA_FLAGS}" --enable-debug --enable-cassert --enable-thread-safety --without-readline --without-zlib CC=g++ >> "$LOG_FILE" 2>&1
    fi

    if [ $? -ne 0 ]; then
        die "configure failed."
    fi
    echo "End configure"

    echo "Begin make compile database, Please wait a few minutes..."
    export GAUSSHOME=${BUILD_DIR}
    export LD_LIBRARY_PATH=${BUILD_DIR}/lib:${BUILD_DIR}/lib/postgresql:${LD_LIBRARY_PATH}
    make -sj >> "$LOG_FILE" 2>&1
    make install -sj>> "$LOG_FILE" 2>&1

    if [ $? -ne 0 ]; then
        die "make compile failed."
    fi
    echo "make compile sucessfully!"
}
compile_gaussdb

if [ "${to_package}"X = "YES"X ]
then
    chmod +x ${ROOT_DIR}/package/package.sh
    if [ "${build_binarylib_dir}"x != "None"x ]
    then
        ${ROOT_DIR}/package/package.sh -m ${build_version_mode} --binarylibs_dir ${build_binarylib_dir}
    else
        ${ROOT_DIR}/package/package.sh -m ${build_version_mode}
    fi
fi
exit 0
