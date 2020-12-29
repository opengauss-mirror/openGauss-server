/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * transam.cpp
 *    common function for transam
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/redo/transam.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/transam.h"
#include "access/redo_common.h"

/*
 * TransactionIdLatest --- get latest XID among a main xact and its children
 */
TransactionId TransactionIdLatest(TransactionId mainxid, int nxids, const TransactionId *xids)
{
    TransactionId result;

    /*
     * In practice it is highly likely that the xids[] array is sorted, and so
     * we could save some cycles by just taking the last child XID, but this
     * probably isn't so performance-critical that it's worth depending on
     * that assumption.  But just to show we're not totally stupid, scan the
     * array back-to-front to avoid useless assignments.
     */
    result = mainxid;
    while (--nxids >= 0) {
        if (TransactionIdPrecedes(result, xids[nxids]))
            result = xids[nxids];
    }
    return result;
}
