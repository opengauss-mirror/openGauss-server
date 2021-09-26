#!/bin/bash
#######################################################################
# Copyright (c): 2020-2025, Huawei Tech. Co., Ltd.
# descript: recompress package
# version:  2.0
# date:     2021-05-19
#######################################################################

declare server_package_path=""
declare agent_package_path=""
declare product_mode="multiple"
declare unpack_server="unpack_server"
declare unpack_agent="unpack_agent"
declare unpack_psycopg2="unpack_psycopg2"
declare compress_command="tar -zcf"
declare decompress_command="tar -zxf"

function print_help()
{
    echo "Usage: $0 [OPTION]
    -h|--help              show help information.
    -pm                    product mode, values parameter is single, multiple or opengauss, default value is multiple.
    --server-pacakge       the server pacakge path.
    --agent-package        the agent package path, only -pm is single or multiple need.
"
}

function log() {
    echo "[makegaussdb] $(date +%y-%m-%d' '%T): $@"
}

function error() {
    echo -e "\033[31m[makegaussdb] $(date +%y-%m-%d' '%T) Error: $@\033[0m"
}

while [ $# -gt 0 ]; do
    case "$1" in
        -h|--help)
            print_help
            exit 1
            ;;
        -pm)
            if [ X$2 == X"" ]; then
                error "no given pm product mode."
                exit 1
            fi
            product_mode=$2
            shift 2
            ;;
        --server-package)
            if [ X$2 == X"" ]; then
                error "no given server compress path"
                exit 1
            fi
            server_package_path=$2
            shift 2
            ;;
         --agent-package)
            if [ X$2 == X"" ]; then
                error "no given agent compress path"
                exit 1
            fi
            agent_package_path=$2
            shift 2
            ;;
         *)
            echo "Internal Error: option processing error: $1" 1>&2
            echo "please input right paramtenter, the following command may help you"
            echo "sh reconstruct.sh --help or sh reconstruct.sh -h"
            exit 1
    esac
done

function standard_path() {
    local package_path=$1
    local first_char=$(expr substr "${package_path}" 1 1)
    if [ "${first_char}" != "/" ]; then
          package_path="$(pwd)/${package_path}"
    fi
    echo "${package_path}"
}

function check_path() {
    local package_type=$1
    local package_path=$2
    if [ X${package_path} = X ]; then
      error "the paramtenter --${package_type} can not be empty."
      exit 1
    fi
    if [ ! -f "${package_path}" ]; then
        error "the file ${package_path} not exist, please check."
        exit 1
    fi
}

function check_parameter() {
    check_path "server-package" ${server_package_path}
    server_package_path=$(standard_path ${server_package_path})
    if [ X${product_mode} != X"opengauss" ]; then
        check_path "agent-pacakge" ${agent_package_path}
        agent_package_path=$(standard_path ${agent_package_path})
    fi
}

function backup_compress() {
    local compress_name=$1
    local bak_package_name="${compress_name%%.*}_old.${compress_name#*.}"
    if [ -d "${bak_package_name}" ]; then
        rm -rf ${bak_package_name}
    fi
    cp ${compress_name} ${bak_package_name}
}

function delete_backup_package() {
    local compress_name=$1
    local bak_package_name="${compress_name%%.*}_old.${compress_name#*.}"
    if [ -d "${bak_package_name}" ]; then
        rm -rf ${bak_package_name}
    fi
}

function final_compress() {
    local compress_file=$1
    if [ X"${compress_file##*.}" == X"zip" ]; then
      zip -q -r ${compress_file} ./*
    else
      ${compress_command} ${compress_file} ./*
    fi
}

function begin_decompress() {
    local decompress_file=$1
    local decompress_dir=$2
    if [ X"${decompress_file##*.}" == X"zip" ]; then
      unzip -q ${decompress_file} -d ${decompress_dir}
    else
      ${decompress_command} ${decompress_file} -C ${decompress_dir}
    fi
}

function distribute_compress() {
    server_dir=$(dirname "${server_package_path}")
    server_name=$(basename "${server_package_path}")

    agent_dir=$(dirname "${agent_package_path}")
    agent_name=$(basename "${agent_package_path}")

    log "server_name: ${server_name}, agent_name: ${agent_name}"
    # decompress server package and copy psycopg2 to lib
    cd ${server_dir}
    backup_compress ${server_name}
    if [ -e "${unpack_server}" ]; then
        rm -rf ${unpack_server}
    fi
    mkdir ${unpack_server}
    begin_decompress ${server_name} ${unpack_server}
    cd ${unpack_server} && mkdir ${unpack_server} ${unpack_psycopg2}

    euler_name=$(basename "$(find . -name "GaussDB-Kernel-V500R00*-64bit.tar.gz")")
    psycopg2_name=$(basename "$(find . -name "GaussDB-Kernel-V500R00*-64bit-Python.tar.gz")")
    log "euler_name: ${euler_name}, psycopg2_name: ${psycopg2_name}"

    ${decompress_command} ${euler_name} -C ${unpack_server}
    ${decompress_command} ${psycopg2_name} -C ${unpack_psycopg2}
    chmod -R 700 ${unpack_psycopg2}/psycopg2
    cp -r ${unpack_psycopg2}/psycopg2 ${unpack_server}/lib
    cp -r ${unpack_psycopg2}/psycopg2 ${unpack_server}/script/gspylib/inspection/lib
    log "complete copy psycopg2 to server package."

    # decompress agent package and copy psycopg2 to lib, then compress
    cd ${agent_dir}
    backup_compress ${agent_name}

    if [ -e "${unpack_agent}" ]; then
        rm -rf ${unpack_agent}
    fi
    mkdir ${unpack_agent}
    begin_decompress ${agent_name} ${unpack_agent}
    cd ${unpack_agent} && mkdir ${unpack_agent}
    agent_tar_name=$(basename "$(find . -name "GaussDB-Kernel-V500R00*-64bit-AGENT.tar.gz")")
    ${decompress_command} ${agent_tar_name} -C ${unpack_agent}

    cd ${unpack_agent}
    cp -r ${server_dir}/${unpack_server}/${unpack_psycopg2}/psycopg2 lib/
    ${compress_command} ${agent_tar_name} ./*
    rm -rf ../${agent_tar_name} && mv ${agent_tar_name} ../ && cd ../ && rm -rf ${unpack_agent}
    final_compress ${agent_name}
    rm -rf ../${agent_name} && mv ${agent_name} ../ && cd ../ && rm -rf ${unpack_agent}

    cd ${agent_dir}
    delete_backup_package ${agent_name}
    log "complete copy psycopg2 to agent package and compress agent package."

    # compress server package
    log "begin to compress server package ......"
    cd ${server_dir}/${unpack_server}/${unpack_server}
    ${compress_command} ${euler_name} ./*
    rm -rf ../${euler_name} && mv ${euler_name} ../ && cd ../ && rm -rf ${unpack_server}
    if [ -d "${unpack_psycopg2}" ]; then
        rm -rf ${unpack_psycopg2}
    fi
    final_compress ${server_name}
    rm -rf ../${server_name} && mv ${server_name} ../ && cd ../ && rm -rf ${unpack_server}

    cd ${server_dir}
    delete_backup_package ${server_name}
    log "complete compress server package."
}

function opengauss_compress() {
    server_dir=$(dirname "${server_package_path}")
    server_name=$(basename "${server_package_path}")

    cd ${server_dir}
    backup_compress ${server_name}
    if [ -e "${unpack_server}" ]; then
        rm -rf ${unpack_server}
    fi
    mkdir ${unpack_server}
    ${decompress_command} ${server_name} -C ${unpack_server}
    cd ${unpack_server} && mkdir ${unpack_agent} ${unpack_psycopg2}

    psycopg2_name=$(basename "$(find . -name "openGauss-*-Python.tar.gz")")
    agent_name=$(basename "$(find . -name "openGauss-*-om.tar.gz")")
    log "agent_name: ${agent_name}, psycopg2_name: ${psycopg2_name}"

    ${decompress_command} ${agent_name} -C ${unpack_agent}
    ${decompress_command} ${psycopg2_name} -C ${unpack_psycopg2}
    chmod -R 700 ${unpack_psycopg2}/psycopg2
    cp -r ${unpack_psycopg2}/psycopg2 ${unpack_agent}/lib
    cp -r ${unpack_psycopg2}/psycopg2 ${unpack_agent}/script/gspylib/inspection/lib
    log "complete copy psycopg2 to agent package."

    # compress agent package
    cd ${unpack_agent}
    ${compress_command} ${agent_name} ./*
    rm -rf ../${agent_name} && mv ${agent_name} ../ && cd ../ && rm -rf ${unpack_agent}
    log "complete compress agent package."

    # recover om sha256
    sha256_name="$(echo ${agent_name} | sed 's/\.tar\.gz//').sha256"
    if [ -d "${sha256_name}" ]; then
        rm -rf ${sha256_name}
    fi
    sha256sum "${agent_name}" | awk -F" " '{print $1}' > "${sha256_name}"
    if [ $? -ne 0 ]; then
        die "generate sha256 file failed."
    fi

    if [ -d "${unpack_psycopg2}" ]; then
        rm -rf ${unpack_psycopg2}
    fi
    ${compress_command} ${server_name} ./*
    rm -rf ../${server_name} && mv ${server_name} ../ && cd ../ && rm -rf ${unpack_server}

    delete_backup_package ${server_name}
    log "complete compress server package."
}

check_parameter
if [ X${product_mode} == X"opengauss" ]; then
  opengauss_compress
else
  distribute_compress
fi
