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
 * -------------------------------------------------------------------------
 *
 * flock.cpp
 *
 * IDENTIFICATION
 *    src/common/port/flock.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "c.h"
#include "flock.h"
#include "securec.h"
#include "securec_check.h"

#undef flock

#include <fcntl.h>

#ifdef WIN32
#define _WINSOCKAPI_
#include <windows.h>
#include <io.h>
#endif

#ifdef WIN32
/*
 * lock and unlock file for windows
 * operation : LOCK_SH, LOCK_EX, LOCK_UN, LOCK_NB
 * location  : START_LOCATION,CURRENT_LOCATION,END_LOCATION
 * The lock area is determined by offset,location and len.
 */
int pgut_flock(int fd, int operation, int64 offset, int location, int64 len)
{
    BOOL ret = false;
    HANDLE handle = (HANDLE)_get_osfhandle(fd);
    DWORD lo = len;
    const DWORD hi = 0;
    errno_t rc = EOK;

    OVERLAPPED Oapped;
    rc = memset_s(&Oapped, sizeof(OVERLAPPED), 0, sizeof(OVERLAPPED));
    securec_check_c(rc, "\0", "\0");
    Oapped.Offset = offset;

    /*  UNlOCK*/
    if (operation & LOCK_UN) {
        ret = UnlockFileEx(handle, 0, lo, hi, &Oapped);
    } else {
        DWORD flags = 0;
        if (operation & LOCK_EX)
            flags |= LOCKFILE_EXCLUSIVE_LOCK;
        if (operation & LOCK_NB)
            flags |= LOCKFILE_FAIL_IMMEDIATELY;
        ret = LockFileEx(handle, flags, 0, lo, hi, &Oapped);
    }

    if (!ret) {
        fprintf(stdout, "Get lock Faild\n");
        return -1;
    }

    return 0;
}

#else
/*
 * lock and unlock file for linux
 * operation : LOCK_SH, LOCK_EX, LOCK_UN, LOCK_NB
 * location  : START_LOCATION,CURRENT_LOCATION,END_LOCATION
 * The lock area is determined by offset,location and len.
 */
int pgut_flock(int fd, int operation, int64 offset, int location, int64 len)
{
    struct flock lck;
    int cmd;
    errno_t rc = EOK;

    rc = memset_s(&lck, sizeof(lck), 0, sizeof(lck));
    securec_check_c(rc, "\0", "\0");

    /*Get start location of lock area*/
    if ((unsigned int)location & START_LOCATION) {
        lck.l_whence = SEEK_SET;
    }
    if ((unsigned int)location & CURRENT_LOCATION) {
        lck.l_whence = SEEK_CUR;
    }
    if ((unsigned int)location & END_LOCATION) {
        lck.l_whence = SEEK_END;
    }

    lck.l_start = offset;
    lck.l_len = len;
    lck.l_pid = getpid();

    if ((unsigned int)operation & LOCK_UN) {
        lck.l_type = F_UNLCK;
    } else if ((unsigned int)operation & LOCK_EX) {
        lck.l_type = F_WRLCK;
    } else {
        lck.l_type = F_RDLCK;
    }

    if ((unsigned int)operation & LOCK_NB) {
        cmd = F_SETLK;
    } else {
        cmd = F_SETLKW;
    }

    return fcntl(fd, cmd, &lck);
}
#endif
