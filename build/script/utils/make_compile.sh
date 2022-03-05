#!/bin/bash
#######################################################################
# Copyright (c): 2020-2025, Huawei Tech. Co., Ltd.
# descript: Compile and pack openGauss
#           Return 0 means OK.
#           Return 1 means failed.
# version:  2.0
# date:     2020-08-08
#######################################################################
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
    echo "${gaussdb_name_for_package}-${version_number}" > version.cfg
    #auto read the number from kernal globals.cpp, no need to change it here
}


PG_REG_TEST_ROOT="${ROOT_DIR}"
ROACH_DIR="${ROOT_DIR}/distribute/bin/roach"
MPPDB_DECODING_DIR="${ROOT_DIR}/contrib/mppdb_decoding"


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


#######################################################################
##back to separate_debug_symbol.sh dir
#######################################################################
function separate_symbol()
{
    cd $SCRIPT_DIR
    if [ "$version_mode" = "release" -a "$separate_symbol" = "on" ]; then
       chmod +x ./separate_debug_information.sh
       ./separate_debug_information.sh
        cd $SCRIPT_DIR
        mkdir -p $package_path
        mv symbols.tar.gz $package_path/$symbol_package_name

    fi
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
        die "change dir to $ROOT_DIR failed."
    fi

    if [ "$version_mode" = "debug" -a "$separate_symbol" = "on" ]; then
        echo "WARNING: do not separate symbol in debug mode!"
    fi

    if [ "$product_mode" != "opengauss" ]; then
        die "the product mode can only be opengauss!"
    fi

    #configure
    make distclean -sj >> "$LOG_FILE" 2>&1

    echo "Begin configure." >> "$LOG_FILE" 2>&1
    chmod 755 configure

    if [ "$product_mode"x == "opengauss"x ]; then
        enable_readline="--with-readline"
    else
        enable_readline="--without-readline"
    fi
    shared_opt="--gcc-version=${gcc_version}.0 --prefix="${BUILD_DIR}" --3rd=${binarylib_dir} --enable-thread-safety ${enable_readline} --without-zlib"
    if [ "$product_mode"x == "opengauss"x ]; then
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
    make -sj 20 >> "$LOG_FILE" 2>&1
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

    chmod 444 ${BUILD_DIR}/bin/cluster_guc.conf
    dos2unix ${BUILD_DIR}/bin/cluster_guc.conf > /dev/null 2>&1

    separate_symbol
    
    get_kernel_commitid
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
