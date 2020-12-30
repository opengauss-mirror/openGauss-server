#!/bin/bash
#######################################################################
# Copyright (c): 2020-2025, Huawei Tech. Co., Ltd.
# descript: Compile opengauss
#           Return 0 means OK.
#           Return 1 means failed.
# version:  1.0
# date:     2020-11-28
#######################################################################

# It is just a wrapper to mpp_package.sh
# Example: ./build_opengauss.sh -3rd /path/to/your/third_party_binarylibs/

ROOT_DIR=$(cd ../../;pwd)

#(0) pre-check 
if [ ! -f opengauss.spec ] || [ ! -f mpp_package.sh ]; then
    echo "ERROR: there is no opengauss.spec/mpp_package.sh"
    exit 1
fi

#(1) remove files that will be released as part of opengauss
now=$(date +%Y%m%d.%H%M%S)
test -d $ROOT_DIR/src/distribute && mv $ROOT_DIR/src/distribute $ROOT_DIR/src/distribute.$now
test -d $ROOT_DIR/src/bin/gds && mv $ROOT_DIR/src/bin/gds $ROOT_DIR/src/bin/gds.$now
test -d $ROOT_DIR/contrib/secbox && mv $ROOT_DIR/contrib/secbox $ROOT_DIR/contrib/secbox.$now
test -d $ROOT_DIR/contrib/carbondata && mv $ROOT_DIR/contrib/carbondata $ROOT_DIR/contrib/carbondata.$now

#(2) prepare
cp opengauss.spec gauss.spec

#(3) invoke mpp_package
chmod a+x mpp_package.sh
echo "mpp_package.sh $@ -nopkg -pm opengauss"
./mpp_package.sh $@ -nopkg -pm opengauss
if [ $? != "0" ]; then
    echo "failed in build opengauss"
fi

#(4) remove files which are not necessary
BUILD_DIR="${ROOT_DIR}/mppdb_temp_install"

rm -f ${BUILD_DIR}/bin/gs_roach
rm -f ${BUILD_DIR}/lib/postgresql/roach_api.so
rm -f ${BUILD_DIR}/lib/lib_roach_show.so
rm -f ${BUILD_DIR}/lib/roach_contrib.so
rm -f ${BUILD_DIR}/lib/postgresql/roach_api_stub.so
rm -f ${BUILD_DIR}/share/postgresql/extension/roach_api.control
rm -f ${BUILD_DIR}/share/postgresql/extension/roach_api--1.0.sql
rm -f ${BUILD_DIR}/share/postgresql/extension/roach_api_stub--1.0.sql
rm -f ${BUILD_DIR}/share/postgresql/extension/roach_api_stub.control
