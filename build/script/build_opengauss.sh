#!/bin/bash
#######################################################################
# Copyright (c): 2020-2021, Huawei Tech. Co., Ltd.
# descript: Compile opengauss
#           Return 0 means OK.
#           Return 1 means failed.
# version:  1.0
# date:     2020-11-28
#######################################################################

# It is just a wrapper to package_internal.sh
# Example: ./build_opengauss.sh -3rd /path/to/your/third_party_binarylibs/

# change it to "N", if you want to build with original build system based on solely Makefiles
declare CMAKE_PKG="N"
declare SCRIPT_DIR=$(cd $(dirname "${BASH_SOURCE[0]}"); pwd)
declare ROOT_DIR=$(dirname "${SCRIPT_DIR}")
declare ROOT_DIR=$(dirname "${ROOT_DIR}")
declare package_type='server'
declare product_mode='opengauss'
declare version_mode='release'
declare binarylib_dir='None'
declare make_check='off'
declare separate_symbol='on'

function print_help()
{
    echo "Usage: $0 [OPTION]
    -h|--help              show help information.
    -V|--version           show version information.
    -3rd|--binarylib_dir   the directory of third party binarylibs.
    -pkg|--package         provode type of installation packages, values parameter is server.
    -m|--version_mode      this values of paramenter is debug, release, memcheck, the default value is release.
    -pm                    product mode, values parameter is  opengauss.
    -mc|--make_check       this values of paramenter is on or off, the default value is on.   
    -s|--symbol_mode       whether separate symbol in debug mode, the default value is on.
    -co|--cmake_opt        more cmake options
"
}

function print_version()
{
    echo $(cat ${SCRIPT_DIR}/gaussdb.ver | grep 'VERSION' | awk -F "=" '{print $2}')
}

if [ $# = 0 ] ; then
    echo "missing option"
    print_help
    exit 1
fi

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
        -3rd|--binarylib_dir)
            if [ "$2"X = X ]; then
                echo "no given binarylib directory values"
                exit 1
            fi
            binarylib_dir=$2
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
        -m|--version_mode)
            if [ "$2"X = X ]; then
                echo "no given version number values"
                exit 1
            fi
            version_mode=$2
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
        -mc|--make_check)
            if [ "$2"X = X ]; then
                echo "no given make check  values"
                exit 1
            fi
            make_check=$2
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
        --cmake_opt)
            if [ "$2"X = X ]; then
                echo "no extra configure options provided"
                exit 1
            fi
            extra_cmake_opt=$2
            shift 2
            ;;
        --config_opt)
            if [ "$2"X = X ]; then
                echo "no extra configure options provided"
                exit 1
            fi
            extra_config_opt=$2
            shift 2
            ;;  
         *)
            echo "Internal Error: option processing error: $1" 1>&2
            echo "please input right paramtenter, the following command may help you"
            echo "${0} --help or ${0} -h"
            exit 1
    esac
done

if [ -e "$SCRIPT_DIR/utils/common.sh" ];then
    source $SCRIPT_DIR/utils/common.sh
else
    exit 1
fi


#(1) invoke package_internal.sh
if [ "$CMAKE_PKG" == "N" ]; then
    declare BUILD_DIR="${ROOT_DIR}/mppdb_temp_install"
    source $SCRIPT_DIR/utils/make_compile.sh || exit 1
else
    echo "begin config cmake options:" >> "$LOG_FILE" 2>&1
    declare BUILD_DIR="${ROOT_DIR}/mppdb_temp_install"
    declare CMAKE_BUILD_DIR=${ROOT_DIR}/tmp_build
    declare CMAKE_OPT="-DENABLE_MULTIPLE_NODES=OFF -DENABLE_THREAD_SAFETY=ON -DENABLE_MOT=ON ${extra_cmake_opt}"
    echo "[cmake options] cmake options is:${CMAKE_OPT}" >> "$LOG_FILE" 2>&1
    source $SCRIPT_DIR/utils/cmake_compile.sh || exit 1
fi

function main()
{
    echo "[makegaussdb] $(date +%y-%m-%d' '%T): script dir : ${SCRIPT_DIR}"
    echo "[makegaussdb] $(date +%y-%m-%d' '%T): Work root dir : ${ROOT_DIR}"
    read_gaussdb_version
    read_gaussdb_number
    gaussdb_pkg_pre_clean
    gaussdb_build
}
main

echo "now, all build has finished!"
exit 0
