/*-------------------------------------------------------------------------
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-server is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *  PL/Python init processes
 *  src/gausskernel/storage/ipc/plpython_init.cpp
 */

#include "storage/plpython_init.h"
#include "storage/shmem.h"
#include "knl/knl_thread.h"
#include "storage/lock/lock.h"

PlPythonState* plpython_state = NULL;

Size PlpythonShmemSize(void)
{
    return sizeof(PlPythonState);
}

void PlpythonShmemInit(void)
{
    bool found;
    plpython_state = (PlPythonState*)ShmemInitStruct("plpython_state", PlpythonShmemSize(), &found);
    if (found) {
        return;
    }
    errno_t rc = memset_s(plpython_state, sizeof(PlPythonState), 0, sizeof(PlPythonState));
    securec_check(rc, "\0", "\0");
}

extern void PlPyGilAcquire(void)
{
    LOCKTAG tag;
    LockAcquireResult result;
    uint64 self = u_sess->session_id;

    SET_LOCKTAG_PLPY_GIL(tag);

    result = LockAcquire(&tag, AccessExclusiveLock, false, false);
    if (result == LOCKACQUIRE_OK || result == LOCKACQUIRE_ALREADY_HELD) {
        pg_atomic_write_u64(&plpython_state->granted_session_id, self);
    } else {
        elog(ERROR, "cant not hold pypython GIL");
    }
}

