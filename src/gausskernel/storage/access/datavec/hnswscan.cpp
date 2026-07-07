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

#include <cmath>

#include "access/amapi.h"
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
    int pqMode = so->pqMode;
    /* Get m and entry point */
    GetMMapMetaPageInfo(index, &m, (void**)(&entryPoint));

    so->q = q;
    so->m = m;

    if (entryPoint == NULL)
        return NIL;

    if (enablePQ) {
        uint8* qPQCode;
        PQSearchInfo pqinfo = {};
        float *query = DatumGetVector(q)->x;

        pqinfo.params = *params;
        if (pqMode == HNSW_PQMODE_SDC) {
            size_t pqCodeSize = params->pqM * sizeof(uint8);
            qPQCode = (uint8 *)palloc(pqCodeSize);
            if (ComputeVectorPQCode(query, params, qPQCode, pqCodeSize) != 0) {
                ereport(ERROR, (errmsg("failed to compute PQ query code")));
            }
            pqinfo.qPQCode = qPQCode;
            pqinfo.pqDistanceTable = index->pqDistanceTable;
            pqinfo.pqDistanceTableSize = (size_t)params->pqM * params->pqKsub * params->pqKsub * sizeof(float);
        } else {
            pqinfo.qPQCode = NULL;
            size_t pqDistTblSize = (size_t)params->pqM * params->pqKsub * params->pqKsub * sizeof(float);
            pqinfo.pqDistanceTable = (float*) palloc(pqDistTblSize);
            pqinfo.pqDistanceTableSize = pqDistTblSize;
            if (GetPQDistanceTableAdc(query, params, pqinfo.pqDistanceTable, pqDistTblSize) != 0) {
                ereport(ERROR, (errmsg("failed to compute PQ ADC distance table")));
            }
        }

        pqinfo.pqMode = pqMode;
        pqinfo.lc = entryPoint->level;
        ep = list_make1(MMapEntryCandidate(
                        base, entryPoint, q, index, procinfo, collation, false, false, NULL, NULL, NULL, enablePQ, &pqinfo));
        for (int lc = entryPoint->level; lc >= 1; lc--) {
            pqinfo.lc = lc;
            w = HnswSearchLayer(base, q, ep, 1, lc, index, procinfo, collation, m, false, NULL,
                                NULL, NULL, true, NULL, false, NULL, NULL, true, NULL, enablePQ, &pqinfo);
            ep = w;
        }
        pqinfo.lc = 0;
        w = HnswSearchLayer(base, q, ep, hnswEfSearch, 0, index, procinfo, collation, m, false, NULL, &so->v,
                            u_sess->datavec_ctx.hnsw_iterative_scan != HNSW_ITERATIVE_SCAN_OFF ? &so->discarded : NULL,
                            true, &so->tuples, false, NULL, NULL, true, NULL, enablePQ, &pqinfo);
    } else {
        ep = list_make1(MMapEntryCandidate(base, entryPoint, q, index, procinfo, collation, false, so->enableRabitQ, so->rbqParams, NULL));
        for (int lc = entryPoint->level; lc >= 1; lc--) {
            w = HnswSearchLayer(base, q, ep, 1, lc, index, procinfo, collation, m, false, NULL,
                                NULL, NULL, true, NULL, so->enableRabitQ, so->rbqParams, NULL, true);
            ep = w;
        }
        w = HnswSearchLayer(base, q, ep, hnswEfSearch, 0, index, procinfo, collation, m, false, NULL, &so->v,
                            u_sess->datavec_ctx.hnsw_iterative_scan != HNSW_ITERATIVE_SCAN_OFF ? &so->discarded : NULL,
                            true, &so->tuples, so->enableRabitQ, so->rbqParams, NULL, true);
    }
    return w;
}

/*
 * Resume scan at ground level with discarded candidates
 */
static List *ResumeScanItems(IndexScanDesc scan)
{
    HnswScanOpaque so = (HnswScanOpaque) scan->opaque;
    Relation index = scan->indexRelation;
    List *ep = NIL;
    char *base = NULL;
    int batchSize = u_sess->datavec_ctx.hnsw_ef_search;

    if (pairingheap_is_empty(so->discarded)) {
        return NIL;
    }

    /* Get next batch of candidates */
    for (int i = 0; i < batchSize; i++) {
        HnswCandidate *hc;

        if (pairingheap_is_empty(so->discarded)) {
            break;
        }

        hc = HnswGetPairingHeapCandidate(w_node, pairingheap_remove_first(so->discarded));
        ep = lappend(ep, hc);
    }

    return HnswSearchLayer(base, so->q, ep, batchSize, 0, index, so->procinfo, so->collation,
                           so->m, false, NULL, &so->v, &so->discarded, false, &so->tuples,
                           so->enableRabitQ, so->rbqParams, NULL, true);
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
    so->pqMode = HNSW_PQMODE_DEFAULT;
    InitPQParamsOnDisk(&params, index, so->procinfo, dim, &so->enablePQ, true);
    so->params = params;

    so->rbqParams = (RabitqQueryParams *)palloc0(sizeof(RabitqQueryParams));
    so->rbqParams->dim = dim;
    so->rbqParams->rbqConfig = InitRbqConfigOnDisk(index, &so->enableRabitQ, &so->rbqParams->centroid, dim);
    if (so->rbqParams->rbqConfig != NULL) {
        so->rbqParams->rbqConfig->rbqQueryBits = u_sess->datavec_ctx.rbq_query_bits;
    }
    so->rbqParams->funcType = so->enableRabitQ ? GetFunctionType(so->procinfo, so->normprocinfo) : 0;
    so->rbqParams->qrbqVec = NULL;

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
    so->v.tids = NULL;
    so->discarded = NULL;
    so->tuples = 0;
    so->previousDistance = -INFINITY;
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

bool isMulOverflowInt(int64 a, int64 b)
{
    if (a == 0 || b == 0) {
        return false;
    }
    if (a > INT_MAX || a < INT_MIN || b > INT_MAX || b < INT_MIN) {
        return true;
    }
    return (a > INT_MAX / b);
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

        if (so->enableRabitQ) {
            Datum vecVal = value;
            if (t_thrd.proc->workingVersionNum < RABITQ_VERSION_NUM) {
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Before RABITQ_VERSION_NUM VERSION NUM %u, we do not support rabitq.", RABITQ_VERSION_NUM)));
            }
            if (CanUseMmap(scan->indexRelation)) {
                ereport(ERROR, (errmsg("HNSW_RABITQ dose not support mmap.")));
            }
            if (IS_HALFVEC(so->procinfo->fn_oid)) {
                vecVal = (Datum)Halfvec2Vector(value);
            }
            RabitqQueryParams *rbqParams = so->rbqParams;
            rbqParams->heap = scan->heapRelation;
            rbqParams->normprocinfo = so->normprocinfo;
            rbqParams->collation = so->collation;
            if (rbqParams->rbqConfig->reType != NotRefine) {
                rbqParams->originQueryVec = vecVal;
                if (scan->limitk == -1) {
                    rbqParams->rbqConfig->kreorder = 0;
                } else {
                    bool overflow = isMulOverflowInt(u_sess->datavec_ctx.rbq_refinek, scan->limitk);
                    rbqParams->rbqConfig->kreorder = overflow ? HNSW_RABITQ_MAX_REORDER :
                                            (int64)ceil(u_sess->datavec_ctx.rbq_refinek * scan->limitk);
                }
                so->length = rbqParams->rbqConfig->kreorder > so->length ?
                             rbqParams->rbqConfig->kreorder : so->length;
            }
            /* Transform scan value */
            VectorTransform *vtrans = rbqParams->rbqConfig->vtrans;
            Vector *transValue = InitVector(rbqParams->dim);
            if (vtrans->type == RANDOM_ORTHOGONAL) {
                RomTransform(vtrans, ((Vector *)DatumGetPointer(vecVal))->x, transValue->x);
            } else {
                FhtTransform(vtrans, ((Vector *)DatumGetPointer(vecVal))->x, transValue->x);
            }

            int qb = rbqParams->rbqConfig->rbqQueryBits;
            /* Encode query and compute factor */
            rbqParams->qrbqVec = (QueryRabitqVector *)palloc0(rbqQuerySize(rbqParams->dim, qb));
            SetRBQQuery(rbqParams->dim, qb, transValue->x,  rbqParams->qrbqVec, rbqParams->centroid, rbqParams->funcType);
        }
        so->value = value;
        /*
         * Get a shared lock. This allows vacuum to ensure no in-flight scans
         * before marking tuples as deleted.
         */
        LockPage(scan->indexRelation, HNSW_SCAN_LOCK, ShareLock);

        so->w = GetScanItems(scan, value);

        UnlockPage(scan->indexRelation, HNSW_SCAN_LOCK, ShareLock);

        so->first = false;

    }

    for (;;) {
        char *base = NULL;
        HnswCandidate *hc;
        HnswElement element;
        ItemPointer heaptid;

        if (list_length(so->w) == 0) {
            if (u_sess->datavec_ctx.hnsw_iterative_scan == HNSW_ITERATIVE_SCAN_OFF) {
                break;
            }

            /* Empty index */
            if (so->discarded == NULL) {
                break;
            }

            /* Reached max number of tuples */
            if (u_sess->datavec_ctx.hnsw_max_scan_tuples > 0 &&
                so->tuples >= u_sess->datavec_ctx.hnsw_max_scan_tuples) {
                if (pairingheap_is_empty(so->discarded)) {
                    break;
                }

                /* Return remaining tuples */
                so->w = lcons(HnswGetPairingHeapCandidate(w_node,
                    pairingheap_remove_first(so->discarded)), so->w);
            } else {
                /*
                * Locking ensures when neighbors are read, the elements they
                * reference will not be deleted (and replaced) during the
                * iteration.
                *
                * Elements loaded into memory on previous iterations may have
                * been deleted (and replaced), so when reading neighbors, the
                * element version must be checked.
                */
                LockPage(scan->indexRelation, HNSW_SCAN_LOCK, ShareLock);
                so->w = ResumeScanItems(scan);
                UnlockPage(scan->indexRelation, HNSW_SCAN_LOCK, ShareLock);
            }

            if (list_length(so->w) == 0) {
                break;
            }
        }

        hc = (HnswCandidate *)linitial(so->w);
        element = (HnswElement)HnswPtrAccess(base, hc->element);

        /* Move to next element if no valid heap TIDs */
        if (element->heaptidsLength == 0) {
            so->w = list_delete_first(so->w);

            /* Mark memory as free for next iteration */
            if (u_sess->datavec_ctx.hnsw_iterative_scan != HNSW_ITERATIVE_SCAN_OFF) {
                pfree(element);
                pfree(hc);
            }

            continue;
        }

        heaptid = &element->heaptids[--element->heaptidsLength];

        if (u_sess->datavec_ctx.hnsw_iterative_scan == HNSW_ITERATIVE_SCAN_STRICT) {
            if (hc->distance < so->previousDistance) {
                continue;
            }

            so->previousDistance = hc->distance;
        }

        MemoryContextSwitchTo(oldCtx);

        scan->xs_ctup.t_self = *heaptid;
        scan->xs_recheck = false;
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

    if (so->rbqParams) {
        if (so->rbqParams->rbqConfig) {
            if (so->rbqParams->rbqConfig->vtrans) {
                pfree(so->rbqParams->rbqConfig->vtrans);
            }
            if (so->rbqParams->rbqConfig->sq) {
                pfree(so->rbqParams->rbqConfig->sq);
            }
            pfree(so->rbqParams->rbqConfig);
        }
        pfree(so->rbqParams);
    }
    pfree(so);
    scan->opaque = NULL;
}

IndexAmRoutine *get_index_amroutine_for_hnsw()
{
    IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);
    amroutine->ambuild = hnswbuild_internal;
    amroutine->ambuildempty = hnswbuildempty_internal;
    amroutine->aminsert = nullptr;
    amroutine->ambulkdelete = nullptr;
    amroutine->amvacuumcleanup = hnswvacuumcleanup_internal;
    amroutine->amcanreturn = nullptr;
    amroutine->ambeginscan = hnswbeginscan_internal;
    amroutine->amrescan = nullptr;
    amroutine->amgettuple = hnswgettuple_internal;
    amroutine->amgetbitmap= nullptr;
    amroutine->amendscan = hnswendscan_internal;
    amroutine->ammarkpos = nullptr;
    amroutine->amrestrpos = nullptr;
    amroutine->ammerge = nullptr;
    return amroutine;
}
