# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# openGauss-server is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.

project_dir=$1
source_dir=$2
# clean
if [[ -e ${source_dir}/checksum_impl.cpp ]];then
  rm ${source_dir}/checksum_impl.cpp
fi
if [[ -e ${source_dir}/pg_lzcompress.cpp ]];then
  rm ${source_dir}/pg_lzcompress.cpp
fi
if [[ -e ${source_dir}/cfs_tools.cpp ]];then
  rm ${source_dir}/cfs_tools.cpp
fi
if [[ -e ${source_dir}/pgsleep.cpp ]];then
  rm ${source_dir}/pgsleep.cpp
fi
if [[ -e ${source_dir}/pg_lzcompress.h ]];then
  rm ${source_dir}/pg_lzcompress.h
fi
rm -rf ${source_dir}/storage
rm -rf ${source_dir}/utils
if [[ -e ${source_dir}/PageCompression.cpp ]] && [[ -L ${source_dir}/PageCompression.cpp ]];then
  rm ${source_dir}/PageCompression.cpp
fi


# setup file
mkdir -p ${source_dir}/storage
mkdir -p ${source_dir}/utils

ln -fs ${project_dir}/gausskernel/storage/page/checksum_impl.cpp ${source_dir}/checksum_impl.cpp
ln -fs ${project_dir}/common/backend/utils/adt/pg_lzcompress.cpp ${source_dir}/pg_lzcompress.cpp
ln -fs ${project_dir}/common/port/pgsleep.cpp ${source_dir}/pgsleep.cpp
ln -fs ${project_dir}/gausskernel/storage/smgr/cfs/cfs_tools.cpp ${source_dir}/cfs_tools.cpp
ln -fs ${project_dir}/include/utils/pg_lzcompress.h ${source_dir}/pg_lzcompress.h
if [[ ! -e ${source_dir}/PageCompression.cpp ]]; then
    ln -fs ${project_dir}/lib/page_compression/PageCompression.cpp ${source_dir}/PageCompression.cpp
fi
echo '' > ${source_dir}/utils/errcodes.h
#link: pg_lzcompress.cpp->knl_variable.h->knl_instance.h->double_write_basic.h->lwlock.h->lwlocknames.h
echo "#define NUM_INDIVIDUAL_LWLOCKS 0" > ${source_dir}/storage/lwlocknames.h
