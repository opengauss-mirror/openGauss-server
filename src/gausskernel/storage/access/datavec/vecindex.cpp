/*
 * Copyright (c) 2024 Huawei Technologies Co.,Ltd.
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
 * vecindex.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/vecindex.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "access/transam.h"
#include "access/datavec/hnsw.h"
#include "storage/procarray.h"
#include "access/datavec/vecindex.h"

VectorScanData *VecGetScanData(IndexScanDesc scan)
{
    switch (scan->indexRelation->rd_rel->relam) {
        case HNSW_AM_OID:
            return &((HnswScanOpaque)scan->opaque)->vs;
        default:
            break;
    }
    return NULL;
}

size_t VecDefaultMaxItemSize(IndexScanDesc scan)
{
    switch (scan->indexRelation->rd_rel->relam) {
        case HNSW_AM_OID:
            return HnswDefaultMaxItemSize;
        default:
            break;
    }
    return (size_t)-1;
}

TransactionIdStatus HnswCheckXid(TransactionId xid)
{
    TransactionIdStatus ts = TransactionIdGetStatus(xid);
    /* Please refer to HeapTupleSatisfiesVaccum */
    if (ts == XID_INPROGRESS) {
        if (TransactionIdIsInProgress(xid)) {
            /* Inprogress */
        } else if (TransactionIdDidCommit(xid)) {
            ts = XID_COMMITTED;
        } else {
            ts = XID_ABORTED;
        }
    }
    return ts;
}

bool VecItupGetXminXmax(Page page, OffsetNumber offnum, TransactionId oldest_xmin, TransactionId *xmin,
                        TransactionId *xmax, bool *xminCommitted, bool *xmaxCommitted, bool isToast)
{
    ItemId iid = PageGetItemId(page, offnum);
    HnswElementTuple itup = (HnswElementTuple)PageGetItem(page, iid);
    IndexTransInfo *idxXid = (IndexTransInfo *)VecIndexTupleGetXid(itup);
    bool isDead = false;
    bool needCheckXmin = true;

    *xminCommitted = *xmaxCommitted = false;

    if (ItemIdIsDead(iid)) {
        *xmin = InvalidTransactionId;
        *xmax = InvalidTransactionId;
        return true;
    }

    *xmin = idxXid->xmin;
    *xmax = idxXid->xmax;

    /* examine xmax */
    if (TransactionIdIsValid(*xmax)) {
        TransactionIdStatus ts = HnswCheckXid(*xmax);
        switch (ts) {
            case XID_INPROGRESS:
                if (TransactionIdEquals(*xmin, *xmax)) {
                    needCheckXmin = false;
                }
                break;
            case XID_COMMITTED:
                *xminCommitted = *xmaxCommitted = true;
                needCheckXmin = false;
                break;
            case XID_ABORTED:
                idxXid->xmax = InvalidTransactionId;
                *xmax = InvalidTransactionId;
                if (TransactionIdEquals(*xmin, *xmax)) {
                    /* xmin xmax aborted */
                    idxXid->xmin = InvalidTransactionId;
                    *xmin = InvalidTransactionId;
                    needCheckXmin = false;
                }
                break;
        }
    }

    /* examine xmin */
    if (needCheckXmin) {
        if (IndexItemIdIsFrozen(iid)) {
            *xminCommitted = true;
        } else if (TransactionIdIsValid(*xmin)) {
            TransactionIdStatus ts = HnswCheckXid(*xmin);
            switch (ts) {
                case XID_INPROGRESS:
                    break;
                case XID_COMMITTED:
                    *xminCommitted = true;
                    break;
                case XID_ABORTED:
                    idxXid->xmin = InvalidTransactionId;
                    *xmin = InvalidTransactionId;
                    break;
            }
        }
    }

    /* if there is no passed oldest_xmin, we will ues the current oldest_xmin */
    if (!TransactionIdIsValid(oldest_xmin)) {
        if (isToast) {
            GetOldestXminForUndo(&oldest_xmin);
        } else {
            oldest_xmin = u_sess->utils_cxt.RecentGlobalDataXmin;
        }
    }
    /* we can't do bypass in hotstandby read mode, or there will be different between index scan and seq scan */
    if (RecoveryInProgress()) {
        oldest_xmin = InvalidTransactionId;
    }

    if (!TransactionIdIsValid(*xmin)) {
        isDead = true;
    }
    /* before we mark the tuple as DEAD because of xmax, must comfirm that xmax has committed */
    if (*xmaxCommitted && TransactionIdPrecedes(*xmax, oldest_xmin)) {
        isDead = true;
    }

    /* before we mark the tuple as FROZEN, must comfirm that xmin has committed */
    if (IndexItemIdIsFrozen(iid)) {
        *xmin = FrozenTransactionId;
    } else if (*xminCommitted && TransactionIdPrecedes(*xmin, oldest_xmin)) {
        IndexItemIdSetFrozen(iid);
        *xmin = FrozenTransactionId;
    }

    if (isDead) {
        ItemIdMarkDead(iid);
        *xmin = InvalidTransactionId;
        *xmax = InvalidTransactionId;
        *xminCommitted = *xmaxCommitted = false;
    }

    return isDead;
}

static bool VecItupEquals(IndexTuple itup1, IndexTuple itup2)
{
    if (itup1 == NULL || itup2 == NULL) {
        return false;
    }
    if (IndexTupleSize(itup1) == 0 || IndexTupleSize(itup2) == 0) {
        return false;
    }
    /*
     * compare the binary directly. If these index tuples are formed from the
     * same uheap tuple, they should be exactly the same.
     */
    return memcmp(itup1, itup2, IndexTupleSize(itup1)) == 0;
}

static bool VecVisibilityCheckCid(IndexScanDesc scan, IndexTuple itup, bool *needRecheck)
{
    VectorScanData *vs = VecGetScanData(scan);
    Assert(vs != NULL);

    if (VecItupEquals((IndexTuple)vs->lastSelfModifiedItup, itup)) {
        *needRecheck = false;
        return false; /* tuples with same key and TID will only returned once */
    }

    /* save this index tuple as lastSelfModifiedItup */
    /* Step1: Check that the buffer space is large enough. */
    size_t maxItemSize = VecDefaultMaxItemSize(scan);
    uint newSize = 0;
    int multiSize = 2;
    if (vs->lastSelfModifiedItup == NULL) {
        newSize = IndexTupleSize(itup);
    } else if (vs->lastSelfModifiedItupBufferSize < IndexTupleSize(itup)) {
        newSize = MAX(vs->lastSelfModifiedItupBufferSize * multiSize, IndexTupleSize(itup));
        newSize = MIN(newSize, maxItemSize);
        pfree(vs->lastSelfModifiedItup);
    }
    /* Step2: Extend when necessary. */
    if (newSize != 0) {
        vs->lastSelfModifiedItup = (char *)palloc(newSize);
        vs->lastSelfModifiedItupBufferSize = newSize;
    }
    /* Step3: Save the current IndexTuple. */
    errno_t rc = 0;
    rc = memcpy_s(vs->lastSelfModifiedItup, maxItemSize, itup, IndexTupleSize(itup));
    securec_check(rc, "\0", "\0");

    *needRecheck = true;
    return true; /* treat as visible, but need recheck */
}

static bool VecXidSatisfiesMVCC(TransactionId xid, bool committed, Snapshot snapshot, Buffer buffer)
{
    TransactionIdStatus ignore;

    if (!TransactionIdIsValid(xid)) {
        return false; /* invisible */
    }
    if (xid == FrozenTransactionId) {
        return true; /* frozen */
    }

    /*
     * We can use snapshot's xmin/xmax as fast bypass after they become valid again.
     * Currently, snapshot's csn and xmin/xmax may be inconsistent. The reavsn is
     * that there is a problem with the cooperation of committing and subtransaction.
     */

    /* we can't tell visibility by snapshot's xmin/xmax alone, check snapshot */
    return XidVisibleInSnapshot(xid, snapshot, &ignore, (RecoveryInProgress() ? buffer : InvalidBuffer), NULL);
}

static bool VecVisibilityCheckXid(TransactionId xmin, TransactionId xmax, bool xminCommitted, bool xmaxCommitted,
                                  Snapshot snapshot, Buffer buffer, bool isUpsert)
{
    if (snapshot->satisfies == SNAPSHOT_DIRTY && isUpsert) {
        bool xmaxVisible = xmaxCommitted || TransactionIdIsCurrentTransactionId(xmax);
        if (xmaxVisible) {
            return false;
        }
        return true;
    }

    /* only support MVCC and NOW, ereport used to locate bug */
    if (snapshot->satisfies != SNAPSHOT_VERSION_MVCC && snapshot->satisfies != SNAPSHOT_MVCC &&
        snapshot->satisfies != SNAPSHOT_NOW) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("unsupported snapshot type %u for UBTree index.", snapshot->satisfies),
                        errhint("This kind of operation may not supported.")));
    }

    /* handle snapshot MVCC */
    if (snapshot->satisfies == SNAPSHOT_VERSION_MVCC || snapshot->satisfies == SNAPSHOT_MVCC) {
        if (VecXidSatisfiesMVCC(xmax, xmaxCommitted, snapshot, buffer)) {
            return false; /* already deleted */
        }
        if (!VecXidSatisfiesMVCC(xmin, xminCommitted, snapshot, buffer)) {
            return false; /* have not inserted yet */
        }
    }

    /* handle snapshot NOW */
    if (snapshot->satisfies == SNAPSHOT_NOW) {
        return xminCommitted && !xmaxCommitted;
    }

    return true;
}

bool VecVisibilityCheck(IndexScanDesc scan, Page page, OffsetNumber offnum, bool *needRecheck)
{
    bool needVisibilityCheck =
        scan->xs_snapshot->satisfies != SNAPSHOT_ANY && scan->xs_snapshot->satisfies != SNAPSHOT_TOAST;
    TransactionId xmin, xmax;
    bool xminCommitted = false;
    bool xmaxCommitted = false;
    bool isDead = VecItupGetXminXmax(page, offnum, InvalidTransactionId, &xmin, &xmax, &xminCommitted, &xmaxCommitted,
                                     RelationGetNamespace(scan->indexRelation) == PG_TOAST_NAMESPACE);

    if (needRecheck == NULL) {
        *needRecheck = false;
    }

    bool isVisible = !isDead;
    if (needVisibilityCheck && !isDead) {
        /*
         * If this IndexTuple is not visible to the current Snapshot, try to get the next one.
         * We're not going to tell heap to skip visibility check, because it doesn't cost a lot and we need heap
         * to check the visibility with CID when snapshot's xid equals to xmin or xmax.
         */
        if (scan->xs_snapshot->satisfies == SNAPSHOT_MVCC &&
            (TransactionIdIsCurrentTransactionId(xmin) || TransactionIdIsCurrentTransactionId(xmax))) {
            ItemId iid = PageGetItemId(page, offnum);
            IndexTuple tuple = (IndexTuple)PageGetItem(page, iid);
            isVisible = VecVisibilityCheckCid(scan, tuple, needRecheck); /* need check cid */
        } else {
            VectorScanData *vs = VecGetScanData(scan);
            isVisible = VecVisibilityCheckXid(xmin, xmax, xminCommitted, xmaxCommitted, scan->xs_snapshot, vs->buf,
                                              scan->isUpsert);
        }
    }

    return isVisible;
}
