#!/bin/bash
# Copyright (c) Huawei Technologies Co., Ltd. 2020-2025. All rights reserved.
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
# Description  : gs_backup is a utility to back up or restore binary files and parameter files.

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

    cp ${SCRIPT_DIR}/version.cfg ${BUILD_DIR}/temp

    # package simpleInstall dir 
    cp -rf ${SCRIPT_DIR}/../../simpleInstall ${BUILD_DIR}/temp
    if [ $? -ne 0 ]; then
        die "copy ${SCRIPT_DIR}/version.cfg to ${BUILD_DIR}/temp failed"
    fi
        
    sed -i '/^process_cpu_affinity|/d'  $2/bin/cluster_guc.conf

    #generate tar file
    echo  "Begin generate ${kernel_package_name} tar file..."  >> "$LOG_FILE" 2>&1
    cd $2
    tar -jcvpf "${kernel_package_name}" ./* >> "$LOG_FILE" 2>&1
    cd '-'
    mv $2/"${kernel_package_name}" ./
    if [ $? -ne 0 ]; then
        die "generate ${kernel_package_name} failed."
    fi
    echo "End generate ${kernel_package_name}  tar file"  >> "$LOG_FILE" 2>&1

    #generate sha256 file
    sha256_name="${package_pre_name}.sha256"
    echo  "Begin generate ${sha256_name} sha256 file..."  >> "$LOG_FILE" 2>&1
    sha256sum "${kernel_package_name}" | awk -F" " '{print $1}' > "$sha256_name"
    if [ $? -ne 0 ]; then
        die "generate sha256 file failed."
    fi
    echo "End generate ${sha256_name} sha256 file"  >> "$LOG_FILE" 2>&1

    ###################################################
    # make server package
    ###################################################
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
##function make_package_prep have two actions
##1.parse release_file_list variable represent file
##2.copy target file into a newly created temporary directory temp
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

    deploy_pkgs ${sha256_name} ${kernel_package_name}
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

    deploy_pkgs ${UPGRADE_SQL_TAR} ${UPGRADE_SQL_SHA256}

    echo "Successfully packaged upgrade_sql files."
}

function make_package_libpq()
{
    cd $SCRIPT_DIR
    prep_dest_list $release_file_list 'libpq'

    rm -rf ${BUILD_DIR}/temp
    mkdir -p ${BUILD_DIR}/temp

    target_file_copy_for_non_server "$dest_list" ${BUILD_DIR}/temp

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

function make_package_tools()
{
    cd $SCRIPT_DIR
    prep_dest_list $release_file_list 'client'

    rm -rf ${BUILD_DIR}/temp
    mkdir -p ${BUILD_DIR}/temp

    cd ${BUILD_DIR}/

    target_file_copy_for_non_server "$dest_list" ${BUILD_DIR}/temp

    cd ${BUILD_DIR}/temp
    echo "packaging tools..."
    tar -zvcf "${tools_package_name}" ./* >>"$LOG_FILE" 2>&1
    if [ $? -ne 0 ]; then
        die "$package_command ${tools_package_name} failed"
    fi

    deploy_pkgs ${tools_package_name}
    echo "install $pkgname tools is ${tools_package_name} of ${package_path} directory " >> "$LOG_FILE" 2>&1
    echo "success!"
}


function gaussdb_pkg()
{
    echo "Start package opengauss."
    separate_symbol
    make_package_srv
    make_package_libpq
    make_package_tools
    make_package_upgrade_sql
    echo "End package opengauss."
}
