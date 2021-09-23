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
 *    Primary/Secondary index sentinel.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/storage/sentinel.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "sentinel.h"
#include "row.h"

namespace MOT {
RC Sentinel::RefCountUpdate(AccessType type, uint64_t tid)
{
    RC rc = RC_OK;

    LockRefCount();
    if (type == INC) {
        if (GetCounter() == 0) {
            ReleaseRefCount();
            return RC::RC_INDEX_RETRY_INSERT;
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
    ReleaseRefCount();

    return rc;
}
}  // namespace MOT
