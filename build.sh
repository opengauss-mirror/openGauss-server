#!/bin/bash
#######################################################################
# Copyright (c): 2020-2025, Huawei Tech. Co., Ltd.
# descript: Compile and pack openGauss
#           Return 0 means OK.
#           Return 1 means failed.
# version:  2.0
# date:     2020-08-08
#######################################################################
declare build_version_mode='release'
declare build_binarylib_dir='None'
declare wrap_binaries='NO'
declare not_optimized=''
declare config_file=''
declare product_mode='opengauss'
declare extra_config_opt=''

declare -a DEPENDENCIES_YUM=(
    "libedit-devel"
    "libxml2-devel"
    "lz4-devel"
    "numactl-devel"
    "unixODBC-devel"
    "java-1.8.0-openjdk-devel"
)

declare missing_deps=()
declare pkg_manager=""
#########################################################################
##read command line paramenters
#######################################################################

function print_help()
{
    echo "Usage: $0 [OPTION]
    -h|--help                         show help information
    -m|--version_mode                 this values of paramenter is debug, release, memcheck or mini, the default value is release
    -3rd|--binarylib_dir              the parent directory of binarylibs
    -pkg|--package                    (deprecated option)package the project,by default, only compile the project
    -wrap|--wrap_binaries             wrop up the project binaries. By default, only compile the project
    -nopt|--not_optimized             on kunpeng platform, like 1616 version, without LSE optimized
    -f|--config_file                  set postgresql.conf.sample from config_file when packing
    -T|--tassl                        build with tassl
    -pm|--product_mode                this values of paramenter is opengauss or lite or finance, the default value is opengauss.
    -nls|--enable_nls                 enable Native Language Support
    --relocation                      generate gaussdb.map with relocation(GCC >=10.3).
    --cmake                           use cmake to build openGauss, which is faster than traditional configure/autoconf
    "
}

detect_pkg_manager() {
    if command -v yum >/dev/null 2>&1; then
        pkg_manager="yum"
    elif command -v apt-get >/dev/null 2>&1; then
        pkg_manager="apt"
    else
        echo "Error: Unsupported package manager. Only yum and apt are supported."
        exit 1
    fi
}

check_dependencies() {
    detect_pkg_manager

    if [ "$pkg_manager" == "yum" ]; then
        for dep in "${DEPENDENCIES_YUM[@]}"; do
            if ! rpm -q "$dep" >/dev/null 2>&1; then
                missing_deps+=("$dep")
            fi
        done
    fi

    if [ ${#missing_deps[@]} -gt 0 ]; then
        echo "Error: Missing required dependencies:"
        printf "  %s\n" "${missing_deps[@]}"
        
        if [ "$pkg_manager" == "yum" ]; then
            echo -e "\nPlease install the missing dependencies using:"
            echo "sudo yum install -y ${missing_deps[*]}"
        fi
        
        exit 1
    else
        echo "All required dependencies are installed."
    fi
}

while [ $# -gt 0 ]; do
    case "$1" in
        -h|--help)
            print_help
            exit 1
            ;;
        -m|--version_mode)
            if [ "$2"X = X ]; then
                echo "no given correct version information, such as: debug/release/memcheck/mini"
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
        -f|--config_file)
            if [[ ! -f "$2" ]]
            then
                echo "config_file does not exist"
                exit 1
            fi
            config_file=$(realpath "$2")
            shift 2
            ;;
        -pm|--product_mode)
            product_mode=$2
            shift 2
            ;;
        -T|--tassl)
            build_with_tassl='-T'
            shift 1
            ;;
        -nls|--enable_nls)
            extra_config_opt="$extra_config_opt --config_opt --enable-nls=zh_CN "
            shift 1
            ;;
        --relocation)
            extra_config_opt="$extra_config_opt --config_opt --enable-relocation "
            shift 1
            ;;
        -C|--cmake)
            build_by_cmake='--cmake'
            shift 1
            ;;
         *)
            echo "Internal Error: option processing error: $1" 1>&2
            echo "please input right paramtenter, the following command may help you"
            echo "./build.sh --help or ./build.sh -h"
            exit 1
    esac
done

check_dependencies

ROOT_DIR=$(cd $(dirname "${BASH_SOURCE[0]}") && pwd)
echo "ROOT_DIR : $ROOT_DIR"
cd build/script
chmod a+x build_opengauss.sh
./build_opengauss.sh -m ${build_version_mode} -3rd ${build_binarylib_dir} ${not_optimized} -pkg server ${build_with_tassl} -pm ${product_mode} ${extra_config_opt} ${build_by_cmake}
if [ $? -ne 0 ]; then
    echo "build_opengauss.sh failed, aborting."
    exit 1
fi

if [ "${wrap_binaries}"X = "YES"X ]; then
    chmod a+x package_opengauss.sh
    if [ X$config_file = "X" ];then
        ./package_opengauss.sh -3rd ${build_binarylib_dir} -m ${build_version_mode} -pm ${product_mode}
    else
        ./package_opengauss.sh -3rd ${build_binarylib_dir} -m ${build_version_mode} -f ${config_file} -pm ${product_mode}
    fi
fi
exit 0
