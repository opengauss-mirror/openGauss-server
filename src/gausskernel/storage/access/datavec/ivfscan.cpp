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
 * ivfscan.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/ivfscan.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include <cfloat>

#include "access/tableam.h"
#include "access/relscan.h"
#include "catalog/index.h"
#include "lib/pairingheap.h"
#include "access/datavec/ivfflat.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/buf/bufmgr.h"

/*
 * Compare list distances
 */
static int CompareLists(const pairingheap_node *a, const pairingheap_node *b, void *arg)
{
    if (((const IvfflatScanList *)a)->distance < ((const IvfflatScanList *)b)->distance) {
        return 1;
    }

    if (((const IvfflatScanList *)a)->distance > ((const IvfflatScanList *)b)->distance) {
        return -1;
    }

    return 0;
}

/*
 * Get lists and sort by distance
 */
static void GetScanLists(IndexScanDesc scan, Datum value)
{
    IvfflatScanOpaque so = (IvfflatScanOpaque)scan->opaque;
    uint16 pqTableNblk;
    uint32 pqDisTableNblk;
    uint16 matrixNblk;
    uint16 otherNblk;
    errno_t rc = EOK;

    IvfGetPQInfoFromMetaPage(scan->indexRelation, &pqTableNblk, NULL, &pqDisTableNblk, NULL);
    IvfflatGetRbqInfoFromMetaPage(scan->indexRelation, NULL, NULL, NULL, NULL, &matrixNblk, NULL,
                            &otherNblk, NULL, NULL, NULL);
    BlockNumber nextblkno = IVFFLAT_CHUNK_START_BLKNO + pqTableNblk + pqDisTableNblk + matrixNblk + otherNblk;
    int listId = 0;

    /* Search all list pages */
    while (BlockNumberIsValid(nextblkno)) {
        Buffer cbuf;
        Page cpage;
        OffsetNumber maxoffno;

        cbuf = ReadBuffer(scan->indexRelation, nextblkno);
        LockBuffer(cbuf, BUFFER_LOCK_SHARE);
        cpage = BufferGetPage(cbuf);

        maxoffno = PageGetMaxOffsetNumber(cpage);

        for (OffsetNumber offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno)) {
            IvfflatList list = (IvfflatList)PageGetItem(cpage, PageGetItemId(cpage, offno));
            double distance;
            size_t copy_size = sizeof(float) * list->center.dim;

            /* Use procinfo from the index instead of scan key for performance */
            distance = DatumGetFloat8(so->distfunc(so->procinfo, so->collation, PointerGetDatum(&list->center), value));

            if (listId < so->listCount) {
                IvfflatScanList *scanlist;

                scanlist = &so->lists[listId];
                scanlist->startPage = list->startPage;
                scanlist->distance = distance;
                scanlist->key = listId;
                scanlist->center = InitVector(list->center.dim);
                rc = memcpy_s(scanlist->center->x, copy_size, list->center.x, copy_size);
                securec_check(rc, "\0", "\0");

                listId++;
                if (so->funcType == DIS_COSINE && so->byResidual) {
                    Vector *vd = (Vector *)DatumGetPointer(value);
                    scanlist->pqDistance = VectorL2SquaredDistance(so->dimensions, list->center.x, vd->x);
                } else {
                    scanlist->pqDistance = distance;
                }
                /* Add to heap */
                pairingheap_add(so->listQueue, &scanlist->ph_node);
            }
        }

        nextblkno = IvfflatPageGetOpaque(cpage)->nextblkno;

        UnlockReleaseBuffer(cbuf);
    }
}

/*
 * Get items
 */
static void GetScanItems(IndexScanDesc scan, Datum value)
{
    IvfflatScanOpaque so = (IvfflatScanOpaque)scan->opaque;
    TupleDesc tupdesc = RelationGetDescr(scan->indexRelation);
    double tuples = 0;
    TupleTableSlot *slot = MakeSingleTupleTableSlot(so->tupdesc);

    /*
     * Reuse same set of shared buffers for scan
     *
     * See postgres/src/backend/storage/buffer/README for description
     */
    BufferAccessStrategy bas = GetAccessStrategy(BAS_BULKREAD);

    /* Search closest probes lists */
    int listCount = 0;
    while (!pairingheap_is_empty(so->listQueue)) {
        BlockNumber searchPage = ((IvfflatScanList *)pairingheap_remove_first(so->listQueue))->startPage;
        /* Search all entry pages for list */
        bool isEmptyList = false;
        bool isFirstPage = true;
        while (BlockNumberIsValid(searchPage)) {
            Buffer buf;
            Page page;
            OffsetNumber maxoffno;

            buf = ReadBufferExtended(scan->indexRelation, MAIN_FORKNUM, searchPage, RBM_NORMAL, bas);
            LockBuffer(buf, BUFFER_LOCK_SHARE);
            page = BufferGetPage(buf);
            maxoffno = PageGetMaxOffsetNumber(page);

            isEmptyList = (isFirstPage && maxoffno <= 0 && !BlockNumberIsValid(IvfflatPageGetOpaque(page)->nextblkno));
            isFirstPage = false;
            if (isEmptyList) {
                UnlockReleaseBuffer(buf);
                break;
            }

            for (OffsetNumber offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno)) {
                IndexTuple itup;
                Datum datum;
                bool isnull;
                ItemId itemid = PageGetItemId(page, offno);

                itup = (IndexTuple)PageGetItem(page, itemid);
                datum = index_getattr(itup, 1, tupdesc, &isnull);

                /*
                 * Add virtual tuple
                 *
                 * Use procinfo from the index instead of scan key for
                 * performance
                 */
                ExecClearTuple(slot);
                slot->tts_values[0] = so->distfunc(so->procinfo, so->collation, datum, value);
                slot->tts_isnull[0] = false;
                slot->tts_values[1] = PointerGetDatum(&itup->t_tid);
                slot->tts_isnull[1] = false;
                ExecStoreVirtualTuple(slot);

                tuplesort_puttupleslot(so->sortstate, slot);

                tuples++;
            }
            
            searchPage = IvfflatPageGetOpaque(page)->nextblkno;

            UnlockReleaseBuffer(buf);
        }

        if (!isEmptyList) {
            ++listCount;
            if (listCount >= so->probes) {
                break;
            }
        }
    }

    FreeAccessStrategy(bas);

    if (tuples < 100)
        ereport(DEBUG1,
                (errmsg("index scan found few tuples"), errdetail("Index may have been created with little data."),
                 errhint("Recreate the index and possibly decrease lists.")));

    tuplesort_performsort(so->sortstate);
}

/*
 * Compare candidate distances
 */
static inline int CompareFurthestCandidates(const pairingheap_node *a, const pairingheap_node *b, void *arg)
{
    if (((const IvfpqPairingHeapNode *)a)->distance < ((const IvfpqPairingHeapNode *)b)->distance) {
        return -1;
    }
    if (((const IvfpqPairingHeapNode *)a)->distance > ((const IvfpqPairingHeapNode *)b)->distance) {
        return 1;
    }

    return 0;
}

/*
 * Compare candidate blocknumber
 */
static inline int CompareBlknoCandidates(const pairingheap_node *a, const pairingheap_node *b, void *arg)
{
    if (((const IvfpqPairingHeapNode *)a)->indexBlk < ((const IvfpqPairingHeapNode *)b)->indexBlk) {
        return -1;
    }
    if (((const IvfpqPairingHeapNode *)a)->indexBlk > ((const IvfpqPairingHeapNode *)b)->indexBlk) {
        return 1;
    }

    return 0;
}

/*
 * Get items PQ
 */
static void GetScanItemsPQ(IndexScanDesc scan, Datum value, float *simTable)
{
    IvfflatScanOpaque so = (IvfflatScanOpaque)scan->opaque;
    TupleDesc tupdesc = RelationGetDescr(scan->indexRelation);
    double tuples = 0;
    TupleTableSlot *slot = MakeSingleTupleTableSlot(so->tupdesc);
    Relation index = scan->indexRelation;
    int pqM = so->pqM;
    int pqKsub = so->pqKsub;
    int kreorder = so->kreorder;
    bool l2CosResidual = so->funcType != DIS_IP && so->byResidual;
    pairingheap *reOrderCandidate = pairingheap_allocate(CompareFurthestCandidates, NULL);
    int canLen = 0;

    /*
     * Reuse same set of shared buffers for scan
     *
     * See postgres/src/backend/storage/buffer/README for description
     */
    BufferAccessStrategy bas = GetAccessStrategy(BAS_BULKREAD);

    /* Search closest probes lists */
    int listCount = 0;
    while (!pairingheap_is_empty(so->listQueue)) {
        IvfflatScanList *scanlist = (IvfflatScanList *)pairingheap_remove_first(so->listQueue);
        double dis0 = so->byResidual ? scanlist->pqDistance : 0;
        BlockNumber searchPage = scanlist->startPage;
        int key = scanlist->key;
        float *simTable2;
        /* Search all entry pages for list */
        bool isEmptyList = false;
        bool isFirstPage = true;

        if (l2CosResidual) {
            /* L2 or Cosine */
            float *preComputeDisTable = (float *)index->pqDistanceTable + key * pqM * pqKsub;
            simTable2 = (float *)palloc(pqM * pqKsub * sizeof(float));
            VectorMadd(pqM * pqKsub, preComputeDisTable, -2.0, simTable, simTable2);
        }

        while (BlockNumberIsValid(searchPage)) {
            Buffer buf;
            Page page;
            OffsetNumber maxoffno;

            buf = ReadBufferExtended(scan->indexRelation, MAIN_FORKNUM, searchPage, RBM_NORMAL, bas);
            LockBuffer(buf, BUFFER_LOCK_SHARE);
            page = BufferGetPage(buf);
            maxoffno = PageGetMaxOffsetNumber(page);

            isEmptyList = (isFirstPage && maxoffno <= 0 && !BlockNumberIsValid(IvfflatPageGetOpaque(page)->nextblkno));
            isFirstPage = false;
            if (isEmptyList) {
                UnlockReleaseBuffer(buf);
                break;
            }

            for (OffsetNumber offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno)) {
                IndexTuple itup;
                Datum datum;
                bool isnull;
                uint8 *code;
                double distance;
                double maxDistance = DBL_MAX;

                ItemId itemid = PageGetItemId(page, offno);

                itup = (IndexTuple)PageGetItem(page, itemid);
                datum = index_getattr(itup, 1, tupdesc, &isnull);
                code = LoadPQCode(itup);
                if (l2CosResidual) {
                    distance = GetPQDistance(simTable2, code, dis0, pqM, pqKsub, false);
                } else {
                    distance = GetPQDistance(simTable, code, dis0, pqM, pqKsub, so->funcType == DIS_IP);
                }

                if (kreorder == 0) {
                    /*
                     * Add virtual tuple
                     *
                     * Use procinfo from the index instead of scan key for
                     * performance
                     */
                    ExecClearTuple(slot);
                    slot->tts_values[0] = Float8GetDatum(distance);
                    slot->tts_isnull[0] = false;
                    slot->tts_values[1] = PointerGetDatum(&itup->t_tid);
                    slot->tts_isnull[1] = false;
                    ExecStoreVirtualTuple(slot);

                    tuplesort_puttupleslot(so->sortstate, slot);
                } else {
                    /* need reorder, add to pairingheap */
                    if (canLen < kreorder) {
                        IvfpqPairingHeapNode *e = IvfpqCreatePairingHeapNode(distance, &itup->t_tid, searchPage, offno);
                        pairingheap_add(reOrderCandidate, &e->ph_node);
                        canLen++;
                        if (canLen == kreorder) {
                            maxDistance = ((IvfpqPairingHeapNode *)pairingheap_first(reOrderCandidate))->distance;
                        }
                    } else if (distance < maxDistance) {
                        IvfpqPairingHeapNode *e = (IvfpqPairingHeapNode *)pairingheap_remove_first(reOrderCandidate);
                        e->distance = distance;
                        ItemPointerCopy(&itup->t_tid, &e->heapTid);
                        e->indexBlk = searchPage;
                        e->indexOff = offno;
                        pairingheap_add(reOrderCandidate, &e->ph_node);
                        maxDistance = ((IvfpqPairingHeapNode *)pairingheap_first(reOrderCandidate))->distance;
                    }
                }
                tuples++;
            }

            searchPage = IvfflatPageGetOpaque(page)->nextblkno;

            UnlockReleaseBuffer(buf);
        }

        if (!isEmptyList) {
            ++listCount;
            if (listCount >= so->probes) {
                break;
            }
        }
    }

    if (tuples < 100)
        ereport(DEBUG1,
                (errmsg("index scan found few tuples"), errdetail("Index may have been created with little data."),
                 errhint("Recreate the index and possibly decrease lists.")));

    if (kreorder != 0) {
        pairingheap *blkOrderCandidate = pairingheap_allocate(CompareBlknoCandidates, NULL);
        BlockNumber blkno = InvalidBlockNumber;
        Buffer buf;
        Page page;

        while (!pairingheap_is_empty(reOrderCandidate)) {
            pairingheap_add(blkOrderCandidate, pairingheap_remove_first(reOrderCandidate));
        }

        while (!pairingheap_is_empty(blkOrderCandidate)) {
            bool isnull;
            IvfpqPairingHeapNode *node = (IvfpqPairingHeapNode *)pairingheap_remove_first(blkOrderCandidate);

            if (blkno != node->indexBlk) {
                if (BlockNumberIsValid(blkno)) {
                    UnlockReleaseBuffer(buf);
                }
                blkno = node->indexBlk;
                buf = ReadBufferExtended(scan->indexRelation, MAIN_FORKNUM, node->indexBlk, RBM_NORMAL, bas);
                LockBuffer(buf, BUFFER_LOCK_SHARE);
                page = BufferGetPage(buf);
            }

            ItemId itemid = PageGetItemId(page, node->indexOff);
            IndexTuple itup = (IndexTuple)PageGetItem(page, itemid);
            Datum datum = index_getattr(itup, 1, tupdesc, &isnull);

            /* Add virtual tuple */
            ExecClearTuple(slot);
            slot->tts_values[0] = so->distfunc(so->procinfo, so->collation, datum, value);
            slot->tts_isnull[0] = false;
            slot->tts_values[1] = PointerGetDatum(&node->heapTid);
            slot->tts_isnull[1] = false;
            ExecStoreVirtualTuple(slot);

            tuplesort_puttupleslot(so->sortstate, slot);
        }

        if (BlockNumberIsValid(blkno)) {
            UnlockReleaseBuffer(buf);
        }
    }

    FreeAccessStrategy(bas);
    tuplesort_performsort(so->sortstate);
}

float *IvfflatGetVectorFromHeapRefine(Relation heap, ItemPointer tid, IndexInfo *indexInfo,
    VectorTransform* vtrans, HeapTuple tuple)
{
    if (indexInfo->ii_NumIndexAttrs != 1) {
        ereport(ERROR, (errmsg("Supports vector indexing exclusively for a single column.")));
    }
    GetTupleFromHeap(heap, tid, tuple);

    TupleDesc relTupleDesc = heap->rd_att;
    Datum *val = (Datum *)palloc(sizeof(Datum) * (relTupleDesc->natts + 1));
    bool *isnull = (bool *)palloc(sizeof(bool) * (relTupleDesc->natts + 1));

    tableam_tops_deform_tuple(tuple, relTupleDesc, val, isnull);
    Vector *originVec;

    for (int i = 0; i < indexInfo->ii_NumIndexAttrs; i++) {
        int keycol = indexInfo->ii_KeyAttrNumbers[i];
        if (keycol != 0) {
            originVec = DatumGetVector(val[keycol - 1]);
        } else {
            pfree(val);
            pfree(isnull);
            ereport(ERROR, (errmsg("Failed to get origin vector from heap.")));
        }
    }

    int dim = originVec->dim;
    float *resData;

    if (vtrans != NULL && vtrans->type == FAST_HTRANSFORM) {
        resData = (float *)palloc(dim * sizeof(float));
        FhtTransform(vtrans, originVec->x, resData);
        pfree(val);
    } else {
        resData = originVec->x;
    }

    pfree(isnull);
    return resData;
}

typedef struct RbqScanBatch {
    RabitqVector *vectors[VECTOR_RBQ_BATCH_SIZE];
    float distances[VECTOR_RBQ_BATCH_SIZE];
    int count;
    int next;
} RbqScanBatch;

/* Called with the index page locked; consume candidates in their original order. */
static float GetRbqBatchDistance(Page page, OffsetNumber offno, TupleDesc rbqTupdesc,
    const RabitqQueryParams *params, RbqScanBatch *batch)
{
    if (batch->next < batch->count) {
        return batch->distances[batch->next++];
    }

    batch->count = 0;
    batch->next = 0;
    OffsetNumber maxoffno = PageGetMaxOffsetNumber(page);
    bool refineSQ8 = params->rbqConfig->reType == SQ8;
    for (OffsetNumber look = offno; look <= maxoffno && batch->count < VECTOR_RBQ_BATCH_SIZE;
         look = OffsetNumberNext(look)) {
        ItemId candidate = PageGetItemId(page, look);
        IndexTuple tup = (IndexTuple)PageGetItem(page, candidate);
        bool isnull;
        Datum code = index_getattr(tup, 1, rbqTupdesc, &isnull);
        errno_t rc = memcpy_s(batch->vectors[batch->count]->data, rbqDataSize(params->dim, refineSQ8),
            VARDATA((bytea *)DatumGetPointer(code)), rbqDataSize(params->dim, refineSQ8));
        if (rc != EOK) {
            securec_check(rc, "\0", "\0");
        }
        batch->vectors[batch->count]->fac = *LoadRbqData(tup);
        batch->count++;
    }

    if (batch->count == VECTOR_RBQ_BATCH_SIZE) {
        ComputeRbqDistanceBatch4(params, batch->vectors, batch->distances);
    } else {
        for (int j = 0; j < batch->count; j++) {
            batch->distances[j] = ComputeRbqDistance(params->dim, params->rbqConfig->rbqQueryBits,
                batch->vectors[j], params->qrbqVec, params->funcType);
        }
    }
    return batch->distances[batch->next++];
}

/*
 * Get items by RabitQ
 */
static void GetScanItemsRabitQ(IndexScanDesc scan, Datum value)
{
    IvfflatScanOpaque so = (IvfflatScanOpaque)scan->opaque;
    TupleDesc tupdesc = RelationGetDescr(scan->indexRelation);
    Oid attrelid = tupdesc->attrs[0].attrelid;

    TupleDesc rbqTupdesc = CreateTemplateTupleDesc(1, false);
    TupleDescInitEntry(rbqTupdesc, (AttrNumber)1, "rbqdata", BYTEAOID, -1, 0);
    rbqTupdesc->attrs[0].attrelid = attrelid;
    rbqTupdesc->attrs[0].attstorage = 'p';

    double tuples = 0;
    TupleTableSlot *slot = MakeSingleTupleTableSlot(so->tupdesc);
    int kreorder = so->rbqParams->rbqConfig->kreorder;
    pairingheap *reOrderCandidate = pairingheap_allocate(CompareFurthestCandidates, NULL);
    int canLen = 0;

    /*
     * Reuse same set of shared buffers for scan
     *
     * See postgres/src/backend/storage/buffer/README for description
     */
    BufferAccessStrategy bas = GetAccessStrategy(BAS_BULKREAD);

    /* Search closest probes lists */
    int listCount = 0;

    /* Compute rabitq auxiliary factor */
    int qb = so->rbqParams->rbqConfig->rbqQueryBits;
    Vector *transVec = (Vector *)DatumGetPointer(value);
    so->rbqParams->qrbqVec = (QueryRabitqVector *)palloc0(rbqQuerySize(so->rbqParams->dim, qb));
    /* Distance computation consumes the code synchronously; reuse scan-local storage. */
    bool refineSQ8 = so->rbqParams->rbqConfig->reType == SQ8;
    RabitqVector *rbqVec = (RabitqVector *)palloc0(rbqCodeSize(so->rbqParams->dim, refineSQ8));

    RbqScanBatch batch;
    for (int j = 0; j < VECTOR_RBQ_BATCH_SIZE; j++) {
        batch.vectors[j] = (RabitqVector *)palloc0(rbqCodeSize(so->rbqParams->dim, refineSQ8));
    }

    while (!pairingheap_is_empty(so->listQueue)) {
        IvfflatScanList *scanList = (IvfflatScanList *)pairingheap_remove_first(so->listQueue);
        BlockNumber searchPage = scanList->startPage;

        so->rbqParams->centroid = scanList->center->x;
        SetRBQQuery(so->rbqParams->dim, qb, transVec->x, so->rbqParams->qrbqVec, so->rbqParams->centroid,
            so->rbqParams->funcType);

        /* Search all entry pages for list */
        bool isEmptyList = false;
        bool isFirstPage = true;
        while (BlockNumberIsValid(searchPage)) {
            Buffer buf;
            Page page;
            OffsetNumber maxoffno;

            buf = ReadBufferExtended(scan->indexRelation, MAIN_FORKNUM, searchPage, RBM_NORMAL, bas);
            LockBuffer(buf, BUFFER_LOCK_SHARE);
            page = BufferGetPage(buf);
            maxoffno = PageGetMaxOffsetNumber(page);

            isEmptyList = (isFirstPage && maxoffno <= 0 && !BlockNumberIsValid(IvfflatPageGetOpaque(page)->nextblkno));
            isFirstPage = false;
            if (isEmptyList) {
                UnlockReleaseBuffer(buf);
                break;
            }

            batch.count = 0;
            batch.next = 0;
            for (OffsetNumber offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno)) {
                IndexTuple itup;
                ItemId itemid = PageGetItemId(page, offno);
                double maxDistance = DBL_MAX;

                itup = (IndexTuple)PageGetItem(page, itemid);
                double distance = (double)GetRbqBatchDistance(page, offno, rbqTupdesc, so->rbqParams, &batch);

                if (kreorder == 0) {
                    /*
                     * Add virtual tuple
                     *
                     * Use procinfo from the index instead of scan key for
                     * performance
                     */
                    ExecClearTuple(slot);
                    slot->tts_values[0] = Float8GetDatum(distance);
                    slot->tts_isnull[0] = false;
                    slot->tts_values[1] = PointerGetDatum(&itup->t_tid);
                    slot->tts_isnull[1] = false;
                    ExecStoreVirtualTuple(slot);

                    tuplesort_puttupleslot(so->sortstate, slot);
                } else {
                    /* need reorder, add to pairingheap */
                    if (canLen < kreorder) {
                        IvfpqPairingHeapNode *e = IvfpqCreatePairingHeapNode(distance, &itup->t_tid, searchPage, offno);
                        pairingheap_add(reOrderCandidate, &e->ph_node);
                        canLen++;
                        if (canLen == kreorder) {
                            maxDistance = ((IvfpqPairingHeapNode *)pairingheap_first(reOrderCandidate))->distance;
                        }
                    } else if (distance < maxDistance) {
                        IvfpqPairingHeapNode *e = (IvfpqPairingHeapNode *)pairingheap_remove_first(reOrderCandidate);
                        e->distance = distance;
                        ItemPointerCopy(&itup->t_tid, &e->heapTid);
                        e->indexBlk = searchPage;
                        e->indexOff = offno;
                        pairingheap_add(reOrderCandidate, &e->ph_node);
                        maxDistance = ((IvfpqPairingHeapNode *)pairingheap_first(reOrderCandidate))->distance;
                    }
                }

                tuples++;
            }
            
            searchPage = IvfflatPageGetOpaque(page)->nextblkno;

            UnlockReleaseBuffer(buf);
        }

        if (!isEmptyList) {
            ++listCount;
            if (listCount >= so->probes) {
                break;
            }
        }
    }

    if (tuples < TUPLE_NUM)
        ereport(DEBUG1,
                (errmsg("index scan found few tuples"), errdetail("Index may have been created with little data."),
                 errhint("Recreate the index and possibly decrease lists.")));

    if (kreorder != 0) {
        pairingheap *blkOrderCandidate = pairingheap_allocate(CompareBlknoCandidates, NULL);
        BlockNumber blkno = InvalidBlockNumber;
        Buffer buf;
        Page page;
        double refineDis;
        RabitQConfig *rbqConfig = so->rbqParams->rbqConfig;
        HeapTuple heapTuple;
        IndexInfo* indexInfo;
        float square;
        errno_t rc = EOK;
        if (so->rbqParams->rbqConfig->reType == FP32) {
            indexInfo = BuildIndexInfo(scan->indexRelation);
            heapTuple = (HeapTupleData *)heaptup_alloc(BLCKSZ);
        }

        while (!pairingheap_is_empty(reOrderCandidate)) {
            pairingheap_add(blkOrderCandidate, pairingheap_remove_first(reOrderCandidate));
        }

        while (!pairingheap_is_empty(blkOrderCandidate)) {
            bool isnull;
            IvfpqPairingHeapNode *node = (IvfpqPairingHeapNode *)pairingheap_remove_first(blkOrderCandidate);

            if (blkno != node->indexBlk && BlockNumberIsValid(blkno)) {
                UnlockReleaseBuffer(buf);
            }
            if (blkno != node->indexBlk) {
                blkno = node->indexBlk;
                buf = ReadBufferExtended(scan->indexRelation, MAIN_FORKNUM, node->indexBlk, RBM_NORMAL, bas);
                LockBuffer(buf, BUFFER_LOCK_SHARE);
                page = BufferGetPage(buf);
            }

            ItemId itemid = PageGetItemId(page, node->indexOff);
            IndexTuple itup = (IndexTuple)PageGetItem(page, itemid);
            Datum datum = index_getattr(itup, 1, rbqTupdesc, &isnull);

            bytea *rbqdata = (bytea *)DatumGetPointer(datum);
            rc = memcpy_s(rbqVec->data, rbqDataSize(so->rbqParams->dim, refineSQ8),
                VARDATA(rbqdata), rbqDataSize(so->rbqParams->dim, refineSQ8));
            securec_check(rc, "\0", "\0");

            if (rbqConfig->reType == SQ8) {
                uint8 *refineCode = getRefineCode(rbqVec, rbqConfig->reOffset);
                ScalarQuantizer *sq = rbqConfig->sq;
                int dim = sq->dim;
                VectorDecodeSQ(dim, sq->trained, sq->trained + dim, sq->decodeVec->x, refineCode);
                refineDis = (float)DatumGetFloat8(FunctionCall2Coll(
                            so->procinfo, so->collation, so->rbqParams->originQueryVec,
                            PointerGetDatum(sq->decodeVec)));
            } else if (rbqConfig->reType == FP32) {
                float *eRbqDiskData = IvfflatGetVectorFromHeapRefine(scan->heapRelation,
                    &node->heapTid, indexInfo, NULL, heapTuple);
                Vector *qVec = (Vector *)DatumGetPointer(so->rbqParams->originQueryVec);
                if (so->rbqParams->funcType == DIS_L2) {
                    refineDis = VectorL2SquaredDistance(qVec->dim, qVec->x, eRbqDiskData);
                } else {
                    refineDis = -VectorInnerProduct(qVec->dim, qVec->x, eRbqDiskData);
                }
                if (so->normprocinfo != NULL) {
                    square = (float)vector_square(eRbqDiskData, qVec->dim);
                    if (square == 0) {
                        continue;
                    }
                    refineDis = -refineDis * refineDis / square;
                }
            } else {
                UnlockReleaseBuffer(buf);
                ereport(ERROR, (errmsg("IVFFLAT RabitQ rerank type error!")));
            }
            /* Add virtual tuple */
            ExecClearTuple(slot);
            slot->tts_values[0] = Float8GetDatum(refineDis);
            slot->tts_isnull[0] = false;
            slot->tts_values[1] = PointerGetDatum(&node->heapTid);
            slot->tts_isnull[1] = false;
            ExecStoreVirtualTuple(slot);

            tuplesort_puttupleslot(so->sortstate, slot);
        }

        if (BlockNumberIsValid(blkno)) {
            UnlockReleaseBuffer(buf);
        }

        if (so->rbqParams->rbqConfig->reType == FP32) {
            pfree(indexInfo);
            pfree(heapTuple);
        }
    }
    for (int j = 0; j < VECTOR_RBQ_BATCH_SIZE; j++) {
        pfree(batch.vectors[j]);
    }
    pfree(rbqVec);
    FreeAccessStrategy(bas);
    tuplesort_performsort(so->sortstate);
}

/*
 * Zero distance
 */
static Datum ZeroDistance(FmgrInfo *flinfo, Oid collation, Datum arg1, Datum arg2)
{
    return Float8GetDatum(0.0);
}

/*
 * Get scan value
 */
static Datum GetScanValue(IndexScanDesc scan)
{
    IvfflatScanOpaque so = (IvfflatScanOpaque)scan->opaque;
    Datum value;

    if (scan->orderByData->sk_flags & SK_ISNULL) {
        value = PointerGetDatum(NULL);
        so->distfunc = ZeroDistance;
    } else {
        value = scan->orderByData->sk_argument;
        so->distfunc = FunctionCall2Coll;

        /* Value should not be compressed or toasted */
        Assert(!VARATT_IS_COMPRESSED(DatumGetPointer(value)));
        Assert(!VARATT_IS_EXTENDED(DatumGetPointer(value)));

        /* Normalize if needed */
        if (so->normprocinfo != NULL)
            value = IvfflatNormValue(so->typeInfo, so->collation, value);
    }

    return value;
}

/*
 * Prepare for an index scan
 */
IndexScanDesc ivfflatbeginscan_internal(Relation index, int nkeys, int norderbys)
{
    IndexScanDesc scan;
    IvfflatScanOpaque so;
    int lists;
    int dimensions;
    AttrNumber attNums[] = {1};
    Oid sortOperators[] = {FLOAT8LTOID};
    Oid sortCollations[] = {InvalidOid};
    bool nullsFirstFlags[] = {false};
    int probes = u_sess->datavec_ctx.ivfflat_probes;
    int natts = 2;
    int attDistance = 1;
    int attHeaptid = 2;

    scan = RelationGetIndexScan(index, nkeys, norderbys);

    /* Get lists and dimensions from metapage */
    IvfflatGetMetaPageInfo(index, &lists, &dimensions);

    if (probes > lists) {
        probes = lists;
    }

    so = (IvfflatScanOpaque)palloc(offsetof(IvfflatScanOpaqueData, lists) + lists * sizeof(IvfflatScanList));
    so->typeInfo = IvfflatGetTypeInfo(index);
    so->first = true;
    so->listCount = lists;
    so->probes = probes;
    so->dimensions = dimensions;
    so->kreorder = u_sess->datavec_ctx.ivfpq_kreorder;

    /* Set support functions */
    so->procinfo = index_getprocinfo(index, 1, IVFFLAT_DISTANCE_PROC);
    so->normprocinfo = IvfflatOptionalProcInfo(index, IVFFLAT_NORM_PROC);
    so->collation = index->rd_indcollation[0];

    /* Create tuple description for sorting */
    so->tupdesc = CreateTemplateTupleDesc(natts, false);
    TupleDescInitEntry(so->tupdesc, (AttrNumber)attDistance, "distance", FLOAT8OID, -1, 0);
    TupleDescInitEntry(so->tupdesc, (AttrNumber)attHeaptid, "heaptid", TIDOID, -1, 0);

    /* Prep sort */
    so->sortstate = tuplesort_begin_heap(so->tupdesc, 1, attNums, sortOperators, sortCollations, nullsFirstFlags,
                                         u_sess->attr.attr_memory.work_mem, NULL, false);

    so->slot = MakeSingleTupleTableSlot(so->tupdesc);

    so->listQueue = pairingheap_allocate(CompareLists, scan);

    GetPQInfoOnDisk(so, index);
    so->pqCtx = AllocSetContextCreate(CurrentMemoryContext, "IVFPQ scan temporary context", ALLOCSET_DEFAULT_SIZES);
    
    so->RabitqCtx = AllocSetContextCreate(CurrentMemoryContext,
        "IVFRabitQ scan temporary context", ALLOCSET_DEFAULT_SIZES);
    so->rbqParams = (RabitqQueryParams *)palloc0(sizeof(RabitqQueryParams));
    so->rbqParams->dim = dimensions;
    so->rbqParams->rbqConfig = IvfInitRbqConfigOnDisk(index, &so->enableRabitQ, dimensions);
    so->rbqParams->rbqConfig->rbqQueryBits = u_sess->datavec_ctx.rbq_query_bits;
    so->rbqParams->rbqConfig->kreorder = 0;
    so->rbqParams->funcType = so->enableRabitQ ? GetFunctionType(so->procinfo, so->normprocinfo) : 0;
    so->rbqParams->qrbqVec = NULL;

    scan->opaque = so;

    return scan;
}

/*
 * Start or restart an index scan
 */
void ivfflatrescan_internal(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys)
{
    IvfflatScanOpaque so = (IvfflatScanOpaque)scan->opaque;
    errno_t rc = EOK;

    so->first = true;
    pairingheap_reset(so->listQueue);

    if (keys && scan->numberOfKeys > 0) {
        rc = memmove_s(scan->keyData, scan->numberOfKeys * sizeof(ScanKeyData), keys, scan->numberOfKeys * sizeof(ScanKeyData));
        securec_check(rc, "\0", "\0");
    }

    if (orderbys && scan->numberOfOrderBys > 0) {
        rc = memmove_s(scan->orderByData, scan->numberOfOrderBys * sizeof(ScanKeyData), orderbys, scan->numberOfOrderBys * sizeof(ScanKeyData));
        securec_check(rc, "\0", "\0");
    }
}

/*
 * Fetch the next tuple in the given scan
 */
bool ivfflatgettuple_internal(IndexScanDesc scan, ScanDirection dir)
{
    IvfflatScanOpaque so = (IvfflatScanOpaque)scan->opaque;

    /*
     * Index can be used to scan backward, but Postgres doesn't support
     * backward scan on operators
     */
    Assert(ScanDirectionIsForward(dir));

    if (so->first) {
        Datum value;

        /* Count index scan for stats */
        pgstat_count_index_scan(scan->indexRelation);

        /* Safety check */
        if (scan->orderByData == NULL)
            elog(ERROR, "cannot scan ivfflat index without order");

        /* Requires MVCC-compliant snapshot as not able to pin during sorting */
        if (!IsMVCCSnapshot(scan->xs_snapshot))
            elog(ERROR, "non-MVCC snapshots are not supported with ivfflat");

        value = GetScanValue(scan);

        Vector *transValue = NULL;
        if (so->enableRabitQ) {
            if (t_thrd.proc->workingVersionNum < RABITQ_VERSION_NUM) {
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Before RABITQ_VERSION_NUM VERSION NUM %u, we do not support rabitq.", RABITQ_VERSION_NUM)));
            }
            RabitqQueryParams *rbqParams = so->rbqParams;
            if (rbqParams->rbqConfig->reType != NotRefine) {
                rbqParams->originQueryVec = value;
                rbqParams->rbqConfig->kreorder = (scan->limitk == -1) ? 0 :
                    (int64)ceil(u_sess->datavec_ctx.rbq_refinek * scan->limitk);
            }
            /* Transform scan value */
            VectorTransform* vtrans = rbqParams->rbqConfig->vtrans;
            transValue = InitVector(rbqParams->dim);
            if (vtrans->type == RANDOM_ORTHOGONAL) {
                RomTransform(vtrans, ((Vector *)DatumGetPointer(value))->x, transValue->x);
            } else {
                FhtTransform(vtrans, ((Vector *)DatumGetPointer(value))->x, transValue->x);
            }
            IvfflatBench("GetScanLists", GetScanLists(scan, (Datum)transValue));
        } else {
            IvfflatBench("GetScanLists", GetScanLists(scan, value));
        }

        if (so->enablePQ) {
            MemoryContext oldCxt = MemoryContextSwitchTo(so->pqCtx);

            float *simTable = (float *)palloc0(so->pqM * so->pqKsub * sizeof(float));
            IvfpqComputeQueryRelTables(so, scan->indexRelation, value, simTable);
            IvfflatBench("GetScanItemsPQ", GetScanItemsPQ(scan, value, simTable));

            MemoryContextSwitchTo(oldCxt);
        } else if (so->enableRabitQ) {
            MemoryContext oldCxt = MemoryContextSwitchTo(so->RabitqCtx);
            IvfflatBench("GetScanItemsRabitQ", GetScanItemsRabitQ(scan, (Datum)transValue));

            MemoryContextSwitchTo(oldCxt);
        } else {
            IvfflatBench("GetScanItems", GetScanItems(scan, value));
        }
        so->first = false;

        /* Clean up if we allocated a new value */
        if (value != scan->orderByData->sk_argument)
            pfree(DatumGetPointer(value));
    }

    bool isDone = tuplesort_gettupleslot(so->sortstate, true, so->slot, NULL);
    if (!isDone && !pairingheap_is_empty(so->listQueue)) {
        /* End prev  tuplesort of ivfflat lists group */
        tuplesort_end(so->sortstate);

        /* Reinitialize a new tuplesort of ivfflat lists group */
        AttrNumber attNums[] = {1};
        Oid sortOperators[] = {FLOAT8LTOID};
        Oid sortCollations[] = {InvalidOid};
        bool nullsFirstFlags[] = {false};
        so->sortstate = tuplesort_begin_heap(so->tupdesc, 1, attNums, sortOperators, sortCollations, nullsFirstFlags,
                                                    u_sess->attr.attr_memory.work_mem, NULL, false);
        Datum value = GetScanValue(scan);
        if (so->enablePQ) {
            MemoryContext oldCxt = MemoryContextSwitchTo(so->pqCtx);

            float *simTable = (float *)palloc0(so->pqM * so->pqKsub * sizeof(float));
            IvfpqComputeQueryRelTables(so, scan->indexRelation, value, simTable);
            IvfflatBench("GetScanItemsPQ", GetScanItemsPQ(scan, value, simTable));

            MemoryContextSwitchTo(oldCxt);
        } else if (so->enableRabitQ) {
            MemoryContext oldCxt = MemoryContextSwitchTo(so->RabitqCtx);
            VectorTransform *vtrans = so->rbqParams->rbqConfig->vtrans;
            Vector *transValue = InitVector(so->rbqParams->dim);
            if (vtrans->type == RANDOM_ORTHOGONAL) {
                RomTransform(vtrans, ((Vector *)DatumGetPointer(value))->x, transValue->x);
            } else {
                FhtTransform(vtrans, ((Vector *)DatumGetPointer(value))->x, transValue->x);
            }
            IvfflatBench("GetScanItemsRabitQ", GetScanItemsRabitQ(scan, (Datum)transValue));

            MemoryContextSwitchTo(oldCxt);
        } else {
            IvfflatBench("GetScanItems", GetScanItems(scan, value));
        }
        isDone = tuplesort_gettupleslot(so->sortstate, true, so->slot, NULL);
        
        /* Clean up if we allocated a new value */
        if (value != scan->orderByData->sk_argument) {
            pfree(DatumGetPointer(value));
        }
    }

    if (isDone) {
        ItemPointer heaptid = (ItemPointer)DatumGetPointer(heap_slot_getattr(so->slot, 2, &so->isnull));

        scan->xs_ctup.t_self = *heaptid;
        scan->xs_recheck = false;
        return true;
    }

    return false;
}

/*
 * End a scan and release resources
 */
void ivfflatendscan_internal(IndexScanDesc scan)
{
    IvfflatScanOpaque so = (IvfflatScanOpaque)scan->opaque;

    MemoryContextDelete(so->pqCtx);
    pairingheap_free(so->listQueue);
    tuplesort_end(so->sortstate);
    MemoryContextDelete(so->RabitqCtx);

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
        so->rbqParams = NULL;
    }

    pfree(so);
    scan->opaque = NULL;
}
