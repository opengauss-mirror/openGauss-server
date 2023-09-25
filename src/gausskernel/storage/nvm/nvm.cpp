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
#include "utils/guc.h"
#include "knl/knl_variable.h"
#include "storage/buf/bufmgr.h"

extern bool check_special_character(char c);

bool check_nvm_path(char** newval, void** extra, GucSource source)
{
    char *absPath;

    absPath = pstrdup(*newval);

    int len = strlen(absPath);
    for (int i = 0; i < len; i++) {
        if (!check_special_character(absPath[i]) || isspace(absPath[i])) {
            return false;
        }
    }

    return true;
}

static bool LockNvmFile(int fd)
{
    struct flock lock;
    lock.l_type = F_WRLCK;
    lock.l_start = 0;
    lock.l_whence = SEEK_SET;
    lock.l_len = 0;
    lock.l_pid = getpid();

    if (fcntl(fd, F_SETLK, &lock) == 0) {
        return false;
    }
    return true;
}

void nvm_init(void)
{
    LWLockAcquire(ShmemIndexLock, LW_EXCLUSIVE);

    if (NVM_BUFFER_NUM == 0) {
        LWLockRelease(ShmemIndexLock);
        ereport(WARNING, (errmsg("nvm_buffers is not set.")));
        return;
    }

    if (g_instance.attr.attr_storage.nvm_attr.nvmBlocks == NULL) {
        int nvmBufFd = open(g_instance.attr.attr_storage.nvm_attr.nvm_file_path, O_RDWR);
        if (nvmBufFd < 0) {
            LWLockRelease(ShmemIndexLock);
            ereport(FATAL, (errmsg("can not open nvm file.")));
        }

        if (LockNvmFile(nvmBufFd)) {
            close(nvmBufFd);
            LWLockRelease(ShmemIndexLock);
            ereport(FATAL, (errmsg("can not lock nvm file.")));
        }

        size_t nvmBufferSize = (NVM_BUFFER_NUM) * (Size)BLCKSZ;

        g_instance.attr.attr_storage.nvm_attr.nvmBlocks = (char *)mmap(NULL, nvmBufferSize,
            PROT_READ | PROT_WRITE, MAP_SHARED, nvmBufFd, 0);
        close(nvmBufFd);
        if (g_instance.attr.attr_storage.nvm_attr.nvmBlocks == NULL) {
            LWLockRelease(ShmemIndexLock);
            ereport(FATAL,
                (errmsg("could not create nvm buffer"),
                    errdetail("Failed system call was mmap(size=%lu)", nvmBufferSize),
                    errhint("This error usually means that openGauss's request for a"
                            "nvm shared buffer exceeded your nvm's file size. you can either"
                            "reduce the request the request size or resize the nvm file.")));
        }
    }

    t_thrd.storage_cxt.NvmBufferBlocks = g_instance.attr.attr_storage.nvm_attr.nvmBlocks;
    LWLockRelease(ShmemIndexLock);
}
