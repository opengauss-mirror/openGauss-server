#!/bin/bash
# Copyright (c) Huawei Technologies Co., Ltd. 2020-2025. All rights reserved.
# descript: Compile and pack openGauss
#######################################################################
##  Check the installation package production environment
#######################################################################
function gaussdb_pkg_pre_clean()
{
    if [ -d "$BUILD_DIR" ]; then
        rm -rf $BUILD_DIR
    fi
    if [ -d "$LOG_FILE" ]; then
        rm -rf $LOG_FILE
    fi
}
###################################

#######################################################################
##read version from gaussdb.ver
#######################################################################
function read_gaussdb_version()
{
    cd ${SCRIPT_DIR}
    echo "${product_name}-Server-${version_number}" > version.cfg
    #auto read the number from kernal globals.cpp, no need to change it here
}

###################################
# get version number from globals.cpp
##################################
function read_gaussdb_number()
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

#######################################################################
##insert the commitid to version.cfg as the upgrade app path specification
#######################################################################
function get_kernel_commitid()
{
    export PATH=${BUILD_DIR}:$PATH
    export LD_LIBRARY_PATH=$GAUSSHOME/lib:$LD_LIBRARY_PATH
    commitid=$(LD_PRELOAD=''  ${BUILD_DIR}/bin/gaussdb -V | awk '{print $5}' | cut -d ")" -f 1)
    echo "${commitid}" >>${SCRIPT_DIR}/version.cfg
    echo "End insert commitid into version.cfg" >> "$LOG_FILE" 2>&1
}

#######################################################################
##insert the version mode to version.cfg
#######################################################################
function get_version_mode()
{
    echo "$version_mode" >> ${SCRIPT_DIR}/version.cfg
    echo "End insert version mode into version cfg" >> "$LOG_FILE" 2>&1
}

#######################################################################
## generate the version file.
#######################################################################
function make_license_control()
{
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

function make_gaussdb_kernel()
{
    export BUILD_TUPLE=${PLATFORM_ARCH}
    export THIRD_BIN_PATH="${binarylib_dir}"
    export PREFIX_HOME="${BUILD_DIR}"
    export DEBUG_TYPE=${version_mode}

    export WITH_TASSL="${build_with_tassl}"

    echo "Begin make install gaussdb server" >> "$LOG_FILE" 2>&1

    export GAUSSHOME=${BUILD_DIR}
    export LD_LIBRARY_PATH=${BUILD_DIR}/lib:${BUILD_DIR}/lib/postgresql:${LD_LIBRARY_PATH}

    [ -d "${CMAKE_BUILD_DIR}" ] && rm -rf ${CMAKE_BUILD_DIR}
    [ -d "${BUILD_DIR}" ] && rm -rf ${BUILD_DIR}
    mkdir -p ${CMAKE_BUILD_DIR}
    cd ${CMAKE_BUILD_DIR}
    cmake .. ${CMAKE_OPT}
    if [ $? -ne 0 ]; then
        die "cmake failed."
    fi
    cpus_num=$(grep -w processor /proc/cpuinfo|wc -l)
    make -sj ${cpus_num}
    if [ $? -ne 0 ]; then
        die "make failed."
    fi
    make install -sj ${cpus_num}
    if [ $? -ne 0 ]; then
        die "make install failed."
    fi

    echo "End make install gaussdb server" >> "$LOG_FILE" 2>&1
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
    cd "$ROOT_DIR/"
    if [ $? -ne 0 ]; then
        die "change dir to $ROOT_DIR failed."
    fi

    if [ "$version_mode" = "debug" -a "$separate_symbol" = "on" ]; then
        echo "WARNING: do not separate symbol in debug mode!"
    fi

    if [ "$product_mode" != "opengauss" -a "$product_mode" != "lite" -a "$product_mode" != "finance" ]; then
        die "the product mode can only be opengauss, lite, finance!"
    fi

    echo "build gaussdb kernel." >> "$LOG_FILE" 2>&1
    make_gaussdb_kernel
    echo "build gaussdb kernel success." >> "$LOG_FILE" 2>&1

    chmod 444 ${BUILD_DIR}/bin/cluster_guc.conf
    dos2unix ${BUILD_DIR}/bin/cluster_guc.conf > /dev/null 2>&1

    #insert the commitid to version.cfg as the upgrade app path specification
    get_kernel_commitid

    #insert the version mode to version.cfg
    get_version_mode
}

function spq_build() {
    if [ -z "${SPQ_ROOT}" ]; then
        SPQ_ROOT="${ROOT_DIR}/../spq"
        echo "INFO: Auto set SPQ_ROOT to ${SPQ_ROOT}"
    fi

    if [ ! -d "${SPQ_ROOT}" ]; then
        echo "ERROR: Invalid SPQ_ROOT path '${SPQ_ROOT}'"
        return
    fi

    echo "Building in ${SPQ_ROOT}..."
    (
        cd "${SPQ_ROOT}" && \
        rm -rf build && \
        mkdir build && \
        cd build
        ../configure && \
        make -sj && \
        make install
    ) || {
        echo "WARNING: Build spq failed, continuing other building steps..."
    }

    cd -
}

#######################################################################
##install gaussdb database and others
##select to install something according to variables package_type need
#######################################################################
function gaussdb_build()
{
    case "$package_type" in
        server)
            install_gaussdb
            ;;
        libpq)
            install_gaussdb
            ;;
        *)
            echo "Internal Error: option processing error: $package_type"
            echo "please input right paramenter values server or libpq "
            exit 1
    esac
}
