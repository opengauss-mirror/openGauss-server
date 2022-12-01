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
 * sentinel.cpp
 *    Base Index Sentinel
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/storage/sentinel.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "sentinel.h"
#include "row.h"

namespace MOT {

IMPLEMENT_CLASS_LOGGER(Sentinel, Storage);

RC Sentinel::RefCountUpdate(AccessType type)
{
    LockRefCount();
    RC rc = RefCountUpdateNoLock(type);
    UnlockRefCount();
    return rc;
}

RC Sentinel::RefCountUpdateNoLock(AccessType type)
{
    RC rc = RC_OK;

    if (type == INC) {
        if (GetCounter() == 0) {
            rc = RC::RC_INDEX_RETRY_INSERT;
        } else {
            IncCounter();
        }
    } else {
        MOT_ASSERT(GetCounter() != 0);
        DecCounter();
        if (GetCounter() == 0) {
            rc = RC::RC_INDEX_DELETE;
        }
    }

    return rc;
}

RC Sentinel::RollBackUnique()
{
    RC rc = RC_OK;
    LockRefCount();
    DecCounter();
    if (GetCounter() == 0) {
        IncCounter();
        rc = RC::RC_INDEX_DELETE;
    }

    UnlockRefCount();
    return rc;
}
}  // namespace MOT
