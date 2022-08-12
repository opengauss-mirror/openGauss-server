/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 *
 * nvm.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/nvm/nvm.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "storage/nvm/nvm.h"

#include "postgres.h"
#include "knl/knl_variable.h"
#include "storage/buf/bufmgr.h"

void nvm_init(void)
{
    LWLockAcquire(ShmemIndexLock, LW_EXCLUSIVE);

    if (NVM_BUFFER_NUM == 0) {
        LWLockRelease(ShmemIndexLock);
        ereport(FATAL, (errmsg("nvm_buffers is not set.\n")));
    }

    if (g_instance.attr.attr_storage.nvm_attr.nvmBlocks == NULL) {
        int nvmBufFd = open(g_instance.attr.attr_storage.nvm_attr.nvm_file_path, O_RDWR);
        if (nvmBufFd < 0) {
            LWLockRelease(ShmemIndexLock);
            ereport(FATAL, (errmsg("can not open nvm file.\n")));
        }
        size_t nvmBufferSize = (NVM_BUFFER_NUM) * (Size)BLCKSZ;

        g_instance.attr.attr_storage.nvm_attr.nvmBlocks = (char *)mmap(NULL, nvmBufferSize,
            PROT_READ | PROT_WRITE, MAP_SHARED, nvmBufFd, 0);
        if (g_instance.attr.attr_storage.nvm_attr.nvmBlocks == NULL) {
            LWLockRelease(ShmemIndexLock);
            ereport(FATAL, (errmsg("mmap nvm buffer failed.\n")));
        }
    }

    t_thrd.storage_cxt.NvmBufferBlocks = g_instance.attr.attr_storage.nvm_attr.nvmBlocks;
    LWLockRelease(ShmemIndexLock);
}
