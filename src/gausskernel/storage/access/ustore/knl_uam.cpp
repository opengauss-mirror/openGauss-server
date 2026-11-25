/* -------------------------------------------------------------------------
 *
 * knl_uam.cpp
 *     uheap page pruning
 *
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-server is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * src/gausskernel/storage/access/ustore/knl_uam.cpp
 * --------------------------------------------------------------------------------
 */
#include "access/ustore/knl_uam.h"

bool UHeapFetchRowVersion(TidScanState *node, Relation relation, ItemPointer tid, Snapshot snapshot,
    TupleTableSlot *slot)
{
    Buffer buffer = InvalidBuffer;
    UHeapTuple utuple = &node->tss_uhtup;

    /* Must set a private data buffer for TidScan. (same as HeapFetchRowVersion) */
    union {
        UHeapDiskTupleData hdr;
        char data[MaxPossibleUHeapTupleSize];
    } tbuf;
    utuple->disk_tuple = &tbuf.hdr;

    ExecClearTuple(slot);

    if (UHeapFetch(relation, snapshot, tid, utuple, &buffer, false, false,
                   &node->ss.ps.state->have_current_xact_date)) {
        ExecStoreTuple(UHeapCopyTuple(utuple), slot, InvalidBuffer, true);
        ReleaseBuffer(buffer);

        return true;
    }

    return false;
}
