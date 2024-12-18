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
 * hnswscan.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/hnswscan.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/relscan.h"
#include "access/datavec/hnsw.h"
#include "pgstat.h"
#include "storage/buf/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/memutils.h"

/*
 * Algorithm 5 from paper
 */
static List *GetScanItems(IndexScanDesc scan, Datum q)
{
    HnswScanOpaque so = (HnswScanOpaque)scan->opaque;
    Relation index = scan->indexRelation;
    FmgrInfo *procinfo = so->procinfo;
    Oid collation = so->collation;
    List *ep;
    List *w;
    int m;
    HnswElement entryPoint;
    char *base = NULL;
    PQParams *params = &so->params;
    bool enablePQ = so->enablePQ;
    int hnswEfSearch = so->length;
    /* Get m and entry point */
    HnswGetMetaPageInfo(index, &m, &entryPoint);

    if (entryPoint == NULL)
        return NIL;

    if (enablePQ) {
        uint8* qPQCode;
        PQSearchInfo pqinfo;
        float *query = DatumGetVector(q)->x;

        pqinfo.params = *params;
        if (params->pqMode == HNSW_PQMODE_SDC) {
            qPQCode = (uint8 *)palloc(params->pqM * sizeof(uint8));
            ComputeVectorPQCode(query, params, qPQCode);
            pqinfo.qPQCode = qPQCode;
            pqinfo.pqDistanceTable = index->pqDistanceTable;
        } else {
            pqinfo.qPQCode = NULL;
            pqinfo.pqDistanceTable = (float*) palloc(params->pqM * params->pqKsub * sizeof(float));
            GetPQDistanceTableAdc(query, params, pqinfo.pqDistanceTable);
        }

        pqinfo.lc = entryPoint->level;
        ep = list_make1(HnswEntryCandidate(
                        base, entryPoint, q, index, procinfo, collation, false, NULL, enablePQ, &pqinfo));
        for (int lc = entryPoint->level; lc >= 1; lc--) {
            pqinfo.lc = lc;
            w = HnswSearchLayer(base, q, ep, 1, lc, index, procinfo, collation, m, false, NULL, NULL, enablePQ, &pqinfo);
            ep = w;
        }
        pqinfo.lc = 0;
        w = HnswSearchLayer(base, q, ep, hnswEfSearch, 0, index, procinfo, collation, m,
                            false, NULL, NULL, enablePQ, &pqinfo);
    } else {
        ep = list_make1(HnswEntryCandidate(base, entryPoint, q, index, procinfo, collation, false));
        for (int lc = entryPoint->level; lc >= 1; lc--) {
            w = HnswSearchLayer(base, q, ep, 1, lc, index, procinfo, collation, m, false, NULL);
            ep = w;
        }
        w = HnswSearchLayer(base, q, ep, hnswEfSearch, 0, index, procinfo, collation, m, false, NULL);
    }
    return w;
}

/*
 * Get scan value
 */
static Datum GetScanValue(IndexScanDesc scan)
{
    HnswScanOpaque so = (HnswScanOpaque)scan->opaque;
    Datum value;

    if (scan->orderByData->sk_flags & SK_ISNULL) {
        value = PointerGetDatum(NULL);
    } else {
        value = scan->orderByData->sk_argument;

        /* Value should not be compressed or toasted */
        Assert(!VARATT_IS_COMPRESSED(DatumGetPointer(value)));
        Assert(!VARATT_IS_EXTENDED(DatumGetPointer(value)));

        /* Normalize if needed */
        if (so->normprocinfo != NULL) {
            value = HnswNormValue(so->typeInfo, so->collation, value);
        }
    }

    return value;
}

/*
 * Prepare for an index scan
 */
IndexScanDesc hnswbeginscan_internal(Relation index, int nkeys, int norderbys)
{
    IndexScanDesc scan;
    HnswScanOpaque so;
    PQParams params;
    int dim;

    scan = RelationGetIndexScan(index, nkeys, norderbys);

    so = (HnswScanOpaque)palloc(sizeof(HnswScanOpaqueData));
    so->typeInfo = HnswGetTypeInfo(index);
    so->first = true;
    so->tmpCtx = AllocSetContextCreate(CurrentMemoryContext, "Hnsw scan temporary context", ALLOCSET_DEFAULT_SIZES);

    so->vs.buf = InvalidBuffer;
    so->vs.lastSelfModifiedItup = NULL;
    so->vs.lastSelfModifiedItupBufferSize = 0;

    /* Set support functions */
    so->procinfo = index_getprocinfo(index, 1, HNSW_DISTANCE_PROC);
    so->normprocinfo = HnswOptionalProcInfo(index, HNSW_NORM_PROC);
    so->collation = index->rd_indcollation[0];

    dim = TupleDescAttr(index->rd_att, 0)->atttypmod;
    InitPQParamsOnDisk(&params, index, so->procinfo, dim, &so->enablePQ);
    so->params = params;

    scan->opaque = so;

    return scan;
}

/*
 * Start or restart an index scan
 */
void hnswrescan_internal(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys)
{
    HnswScanOpaque so = (HnswScanOpaque)scan->opaque;
    errno_t rc = EOK;

    if (so->vs.lastSelfModifiedItup) {
        IndexTupleSetSize(((IndexTuple)(so->vs.lastSelfModifiedItup)), 0); /* clear */
    }

    so->first = true;
    MemoryContextReset(so->tmpCtx);

    if (keys && scan->numberOfKeys > 0) {
        rc = memmove_s(scan->keyData, scan->numberOfKeys * sizeof(ScanKeyData), keys, scan->numberOfKeys * sizeof(ScanKeyData));
        securec_check(rc, "\0", "\0");
    }

    if (orderbys && scan->numberOfOrderBys > 0) {
        rc = memmove_s(scan->orderByData, scan->numberOfOrderBys * sizeof(ScanKeyData), orderbys, scan->numberOfOrderBys * sizeof(ScanKeyData));
        securec_check(rc, "\0", "\0");
    }
}

void check_length(HnswScanOpaque so, IndexScanDesc scan)
{
    if (list_length(so->w) == 0) {
        LockPage(scan->indexRelation, HNSW_SCAN_LOCK, ShareLock);
        so->length = so->length * 2;
        so->w = GetScanItems(scan, so->value);

        /* Release shared lock */
        UnlockPage(scan->indexRelation, HNSW_SCAN_LOCK, ShareLock);
        for (int i = 0; i < so->currentLoc; i++) {
            so->w = list_delete_first(so->w);
        }
    }
}
/*
 * Fetch the next tuple in the given scan
 */
bool hnswgettuple_internal(IndexScanDesc scan, ScanDirection dir)
{
    HnswScanOpaque so = (HnswScanOpaque)scan->opaque;
    MemoryContext oldCtx = MemoryContextSwitchTo(so->tmpCtx);

    /*
     * Index can be used to scan backward, but Postgres doesn't support
     * backward scan on operators
     */
    Assert(ScanDirectionIsForward(dir));

    if (so->first) {
        Datum value;
        so->length = u_sess->datavec_ctx.hnsw_ef_search;
        so->currentLoc = 0;
        /* Count index scan for stats */
        pgstat_count_index_scan(scan->indexRelation);

        /* Safety check */
        if (scan->orderByData == NULL)
            elog(ERROR, "cannot scan hnsw index without order");

        /* Requires MVCC-compliant snapshot as not able to maintain a pin */
        /* https://www.postgresql.org/docs/current/index-locking.html */
        if (!IsMVCCSnapshot(scan->xs_snapshot))
            elog(ERROR, "non-MVCC snapshots are not supported with hnsw");

        /* Get scan value */
        value = GetScanValue(scan);
        so->value = value;
        /*
         * Get a shared lock. This allows vacuum to ensure no in-flight scans
         * before marking tuples as deleted.
         */
        LockPage(scan->indexRelation, HNSW_SCAN_LOCK, ShareLock);

        so->w = GetScanItems(scan, value);

        /* Release shared lock */
        UnlockPage(scan->indexRelation, HNSW_SCAN_LOCK, ShareLock);

        so->first = false;

    }
    check_length(so, scan);
    while (list_length(so->w) > 0) {
        char *base = NULL;
        HnswCandidate *hc = (HnswCandidate *)linitial(so->w);
        HnswElement element = (HnswElement)HnswPtrAccess(base, hc->element);
        ItemPointer heaptid;

        /* Move to next element if no valid heap TIDs */
        if (element->heaptidsLength == 0) {
            so->w = list_delete_first(so->w);
            if (list_length(so->w) !=0) {
                continue;
            }
        }
        check_length(so, scan);
        if (list_length(so->w) == 0) {
            continue;
        }
        hc = (HnswCandidate *)linitial(so->w);
        element = (HnswElement)HnswPtrAccess(base, hc->element);
        heaptid = &element->heaptids[--element->heaptidsLength];

        MemoryContextSwitchTo(oldCtx);

        scan->xs_ctup.t_self = *heaptid;
        scan->xs_recheck = false;
        so->currentLoc = so->currentLoc + 1;
        return true;
    }

    MemoryContextSwitchTo(oldCtx);
    return false;
}

/*
 * End a scan and release resources
 */
void hnswendscan_internal(IndexScanDesc scan)
{
    HnswScanOpaque so = (HnswScanOpaque)scan->opaque;

    FREE_POINTER(so->vs.lastSelfModifiedItup);

    MemoryContextDelete(so->tmpCtx);

    pfree(so);
    scan->opaque = NULL;
}
