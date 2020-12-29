#!/bin/bash

declare build_version_mode='release'
declare build_binarylib_dir='None'
declare wrap_binaries='NO'
declare not_optimized=''
#########################################################################
##read command line paramenters
#######################################################################

function print_help()
{
    echo "Usage: $0 [OPTION]
    -h|--help                         show help information
    -m|--version_mode                 this values of paramenter is debug, release or memcheck, the default value is release
    -3rd|--binarylib_dir              the parent directory of binarylibs
    -pkg|--package                    (deprecated option)package the project,by default, only compile the project
    -wrap|--wrap_binaries             wrop up the project binaries. By default, only compile the project
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
        -pkg|--package|-wrap|--wrap_binaries)
            wrap_binaries='YES'
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
            not_optimized='-nopt'
            shift 1
            ;;
         *)
            echo "Internal Error: option processing error: $1" 1>&2
            echo "please input right paramtenter, the following command may help you"
            echo "./build.sh --help or ./build.sh -h"
            exit 1
    esac
done


ROOT_DIR=$(cd $(dirname "${BASH_SOURCE[0]}") && pwd)
echo "ROOT_DIR : $ROOT_DIR"
cd build/script
chmod a+x build_opengauss.sh
sh build_opengauss.sh -m ${build_version_mode} -3rd ${build_binarylib_dir} ${not_optimized} -pkg server -mc off
if [ "${wrap_binaries}"X = "YES"X ]
then
    chmod a+x build_opengauss.sh
    sh package_opengauss.sh -3rd ${build_binarylib_dir}
fi
exit 0
