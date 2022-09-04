#!/bin/bash
# Copyright (c) Huawei Technologies Co., Ltd. 2020-2025. All rights reserved.
# descript: package opengauss lite

declare UPGRADE_SQL_DIR="${ROOT_DIR}/src/include/catalog/upgrade_sql"

#######################################################################
#  move pkgs to output directory
#######################################################################
function deploy_pkgs()
{
    mkdir -p $package_path
    for pkg in $@; do
        if [ -f "$pkg" ]; then
            mv $pkg $package_path/
        fi
    done
}

#######################################################################
# copy directory's files list to $2
#######################################################################
function copy_files_list()
{
    for file in $(echo $1)
    do
        test -e $file && tar -cpf - $file | ( cd $2; tar -xpf -  )
    done
}

#######################################################################
##copy target file into temporary directory temp
#######################################################################
function target_file_copy()
{
    cd ${BUILD_DIR}
    copy_files_list "$1" $2

    sed -i '/^process_cpu_affinity|/d'  $2/bin/cluster_guc.conf

    #generate tar file
    echo  "Begin generate ${kernel_package_name} tar file..."  >> "$LOG_FILE" 2>&1
    cd $2
    ${BUILD_TOOLS_PATH}/p7z/bin/7z a -t7z -sfx "${kernel_package_name}" ./* >> "$LOG_FILE" 2>&1
    cd '-'
    mv $2/"${kernel_package_name}" ./
    if [ $? -ne 0 ]; then
        die "generate ${kernel_package_name} failed."
    fi
    echo "End generate ${kernel_package_name}  tar file"  >> "$LOG_FILE" 2>&1

    #generate sha256 file
    sha256_name="${sha256_name}"
    echo  "Begin generate ${sha256_name} sha256 file..."  >> "$LOG_FILE" 2>&1
    sha256sum "${kernel_package_name}" | awk -F" " '{print $1}' > "${sha256_name}"
    if [ $? -ne 0 ]; then
        die "generate sha256 file failed."
    fi
    echo "End generate ${sha256_name} sha256 file"  >> "$LOG_FILE" 2>&1
    if [ -d "${2}" ]; then
        rm -rf ${2}
    fi
}

function target_file_copy_for_non_server()
{
    cd ${BUILD_DIR}
    copy_files_list "$1" $2
}

#######################################################################
##function prep_dest_list parse release_file_list variable represent file
#######################################################################
function prep_dest_list()
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

    dest_list=$(cat $releasefile | awk "NR==$head+1,NR==$tail+$head-1")
}

#######################################################################
##back to separate_debug_symbol.sh dir
#######################################################################
function separate_symbol()
{
    cd $SCRIPT_DIR
    if [ "$version_mode" = "release" ]; then
       chmod +x ./separate_debug_information.sh
       ./separate_debug_information.sh
        cd $SCRIPT_DIR
        mv symbols.tar.gz $symbol_package_name
        deploy_pkgs $symbol_package_name
    fi
}

function make_package_srv()
{
    echo "Begin package server"
    cd $SCRIPT_DIR
    prep_dest_list $release_file_list 'server'

    rm -rf ${BUILD_DIR}/temp
    mkdir -p ${BUILD_DIR}/temp/etc
    target_file_copy "$dest_list" ${BUILD_DIR}/temp
    make_package_upgrade_sql
    cp ${SCRIPT_DIR}/version.cfg ${BUILD_DIR}/temp
    if [ $? -ne 0 ]; then
        die "copy ${SCRIPT_DIR}/version.cfg to ${BUILD_DIR}/temp failed"
    fi

    # pkg install uninstall scripts:install.sh, uninstall.sh, opengauss_lite.conf, upgrade_errorcode.sh
    for filename in install.sh uninstall.sh opengauss_lite.conf
    do
        if ! cp ${ROOT_DIR}/liteom/${filename} ${BUILD_DIR}/temp ; then
            die "copy ${ROOT_DIR}/liteom/${filename} to ${BUILD_DIR}/temp failed"
        fi
    done
    chmod 500 ./install.sh ./uninstall.sh
    # pkg upgrade scripts:upgrade_GAUSSV5.sh, upgrade_common.sh, upgrade_config.sh, upgrade_errorcode.sh
    for filename in upgrade_GAUSSV5.sh upgrade_common.sh upgrade_config.sh upgrade_errorcode.sh
    do
        if ! cp ${ROOT_DIR}/liteom/${filename} ${BUILD_DIR}/temp ; then
            die "copy ${ROOT_DIR}/liteom/${filename} to ${BUILD_DIR}/temp failed"
        fi
    done
    chmod 500 ./upgrade_GAUSSV5.sh
    chmod 400 ./upgrade_common.sh upgrade_errorcode.sh
    chmod 600 ./opengauss_lite.conf upgrade_config.sh
    mkdir -p ${BUILD_DIR}/temp/dependency
    cp ${BUILD_DIR}/lib/libstdc++.so.6 ${BUILD_DIR}/temp/dependency
    cd ${BUILD_DIR}/temp
    cp ${BUILD_DIR}/"${kernel_package_name}" ./
    cp ${BUILD_DIR}/"${sha256_name}" ./
    tar -czf "${package_pre_name}.tar.gz" ./*
    deploy_pkgs "${package_pre_name}.tar.gz"
    echo "make server(all) package success!"
}

#######################################################################
# Install all SQL files from src/distribute/include/catalog/upgrade_sql
# to INSTALL_DIR/bin/script/upgrade_sql.
# Package all SQL files and then verify them with SHA256.
#######################################################################
function make_package_upgrade_sql()
{
    echo "Begin to install upgrade_sql files..."
    UPGRADE_SQL_TAR="upgrade_sql.tar.gz"
    UPGRADE_SQL_SHA256="upgrade_sql.sha256"
    cd $SCRIPT_DIR
    mkdir -p ${BUILD_DIR}
    cd ${BUILD_DIR}
    rm -rf temp
    mkdir temp
    cd ${BUILD_DIR}/temp
    cp -r "${UPGRADE_SQL_DIR}" ./upgrade_sql
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

function make_package_libpq()
{
    cd $SCRIPT_DIR
    prep_dest_list $release_file_list 'libpq'

    rm -rf ${BUILD_DIR}/temp
    mkdir -p ${BUILD_DIR}/temp

    target_file_copy_for_non_server "$dest_list" ${BUILD_DIR}/temp

    if [ "$version_mode" != "memcheck" ]; then
        # copy include file
        prep_dest_list $release_file_list 'header'
        target_file_copy_for_non_server "$dest_list" ${BUILD_DIR}/temp
    fi

    cd ${BUILD_DIR}/temp
    echo "packaging libpq..."
    tar -zvcf "${libpq_package_name}" ./* >>"$LOG_FILE" 2>&1
    if [ $? -ne 0 ]; then
        die "$package_command ${libpq_package_name} failed"
    fi

    deploy_pkgs ${libpq_package_name}
    echo "install $pkgname tools is ${libpq_package_name} of ${package_path} directory " >> "$LOG_FILE" 2>&1
    echo "success!"
}

function gaussdb_pkg()
{
    echo "Start package opengauss."
    separate_symbol
    make_package_srv
    make_package_libpq
    echo "End package opengauss."
}