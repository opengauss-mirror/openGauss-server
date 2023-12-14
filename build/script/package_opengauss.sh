#!/bin/bash
#######################################################################
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
# descript: Compile and pack GaussDB
#           Return 0 means OK.
#           Return 1 means failed.
# version:  2.0
# date:     2021-02-28
#######################################################################

declare SCRIPT_DIR=$(cd $(dirname "${BASH_SOURCE[0]}"); pwd)
declare ROOT_DIR=$(dirname "${SCRIPT_DIR}")
declare ROOT_DIR=$(dirname "${ROOT_DIR}")
declare package_type='server'
declare product_mode='opengauss'
declare version_mode='release'
declare binarylib_dir='None'
declare om_dir='None'
declare cm_dir='None'
declare show_package='false'
declare install_package_format='tar'

function print_help()
{
    echo "Usage: $0 [OPTION]
    -h|--help              show help information.
    -3rd|--binarylib_dir   the directory of third party binarylibs.
    -pkg|--package         provode type of installation packages, values parameter is server.
    -m|--version_mode      this values of paramenter is debug, release, memcheck, the default value is release.
    -pm|--product_mode     this values of paramenter is opengauss or lite or finance, the default value is opengauss.
"
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
        -pm)
            if [ "$2"X = X ]; then
                echo "no given product mode"
                exit 1
            fi
            product_mode=$2
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
        -S|--show_pkg)
            show_package=true
            shift
            ;;
         *)
            echo "Internal Error: option processing error: $1" 1>&2
            echo "please input right paramtenter, the following command may help you"
            echo "./package.sh --help or ./package.sh -h"
            exit 1
    esac
done

if [ -e "$SCRIPT_DIR/utils/common.sh" ];then
    source $SCRIPT_DIR/utils/common.sh
else
    exit 1
fi

#############################################################
# show package for hotpatch sdv.
#############################################################
if [ "$show_package" = true ]; then
    echo "package: "$server_package_name
    echo "bin: "$bin_name
    exit 0
fi

declare BUILD_DIR="${ROOT_DIR}/mppdb_temp_install"
declare PKG_TMP_DIR="${BUILD_DIR}/temp"

if [ "${product_mode}" == "lite" ]; then
    if [ -e "$SCRIPT_DIR/utils/internal_packages_lite.sh" ];then
        source $SCRIPT_DIR/utils/internal_packages_lite.sh
    else
        exit 1
    fi
else
    if [ -e "$SCRIPT_DIR/utils/internal_packages.sh" ];then
        source $SCRIPT_DIR/utils/internal_packages.sh
    else
        exit 1
    fi
fi

function main()
{
    echo "[makegaussdb] $(date +%y-%m-%d' '%T): script dir : ${SCRIPT_DIR}"
    echo "[makegaussdb] $(date +%y-%m-%d' '%T): Work root dir : ${ROOT_DIR}"
    gaussdb_pkg
}
main

echo "now, all packages has finished!"
exit 0