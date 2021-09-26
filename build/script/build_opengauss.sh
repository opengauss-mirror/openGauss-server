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
CMAKE_PKG="N"

#(0) pre-check 
if [ ! -f opengauss.spec ] || [ ! -f package_internal.sh ]; then
    echo "ERROR: there is no opengauss.spec/mpp_package.sh"
    exit 1
fi

#(1) prepare
cp opengauss.spec gauss.spec

#(2) invoke package_internal.sh
if [ "$CMAKE_PKG" == "N" ]; then
    chmod a+x package_internal.sh
    echo "package_internal.sh $@ -nopkg -pm opengauss"
    ./package_internal.sh $@ -nopkg -pm opengauss
    if [ $? != "0" ]; then
        echo "failed in build opengauss"
    fi
else
    chmod a+x cmake_package_internal.sh
    echo "cmake_package_internal.sh $@ -nopkg -pm opengauss"
    ./cmake_package_internal.sh $@ -nopkg -pm opengauss
    if [ $? != "0" ]; then
        echo "failed in build opengauss"
    fi
fi

#(3) remove files which are not necessary
BUILD_DIR="${ROOT_DIR}/mppdb_temp_install"
