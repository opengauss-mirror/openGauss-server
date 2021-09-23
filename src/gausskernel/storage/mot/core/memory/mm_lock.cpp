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
 * mm_lock.cpp
 *    Memory management buffer lock implementation.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_lock.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "mm_lock.h"
#include "utilities.h"

namespace MOT {
DECLARE_LOGGER(Lock, Memory)

extern int MemLockReportError(int rc, MemLockOp lockOp)
{
    int result = MOT_ERROR_SYSTEM_FAILURE;

    switch (lockOp) {
        case MEM_LOCK_OP_INIT:
            MOT_REPORT_SYSTEM_ERROR_CODE(rc, pthread_spin_init, "N/A", "Failed to initialize lock");
            break;

        case MEM_LOCK_OP_DESTROY:
            MOT_REPORT_SYSTEM_ERROR_CODE(rc, pthread_spin_destroy, "N/A", "Failed to destroy lock");
            break;

        case MEM_LOCK_OP_TRY_ACQUIRE:
            MOT_REPORT_SYSTEM_ERROR_CODE(rc, pthread_spin_trylock, "N/A", "Failed to try-acquire lock");
            break;

        default:
            MOT_REPORT_ERROR(MOT_ERROR_RESOURCE_UNAVAILABLE, "N/A", "Unexpected failed lock operation %d", (int)lockOp);
            result = MOT_ERROR_INTERNAL;
            break;
    }

    SetLastError(result, MOT_SEVERITY_ERROR);
    return result;
}
}  // namespace MOT
