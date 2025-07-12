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

#include "access/relscan.h"
#include "lib/pairingheap.h"
#include "access/datavec/ivfflat.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/buf/bufmgr.h"
#include "access/datavec/ivfnpuadaptor.h"

#define IvfflatNPUGetListInfo(i) (((IvfListInfo *)g_instance.npu_cxt.ivf_lists_info)[i])

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
    IvfGetPQInfoFromMetaPage(scan->indexRelation, &pqTableNblk, NULL, &pqDisTableNblk, NULL);
    BlockNumber nextblkno = IVFPQTABLE_START_BLKNO + pqTableNblk + pqDisTableNblk;
    int listId = 0;

    double maxDistance = DBL_MAX;
    int listCount = 0;

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

            /* Use procinfo from the index instead of scan key for performance */
            distance = DatumGetFloat8(so->distfunc(so->procinfo, so->collation, PointerGetDatum(&list->center), value));

            if (listId < so->listCount) {
                IvfflatScanList *scanlist;

                scanlist = &so->lists[listId];
                scanlist->startPage = list->startPage;
                scanlist->distance = distance;
                scanlist->key = listId;
                scanlist->listId = list->listId;
                scanlist->tupleNum = list->tupleNum;
                listId++;
                listCount++;
                if (so->funcType == PQ_DIS_COSINE && so->byResidual) {
                    Vector *vd = (Vector *)DatumGetPointer(value);
                    scanlist->pqDistance = VectorL2SquaredDistance(so->dimensions, list->center.x, vd->x);
                } else {
                    scanlist->pqDistance = distance;
                }
                /* Add to heap */
                pairingheap_add(so->listQueue, &scanlist->ph_node);

                /* Calculate max distance */
                if (listCount == so->listCount)
                    maxDistance = ((IvfflatScanList *)pairingheap_first(so->listQueue))->distance;
            } else if (distance < maxDistance) {
                IvfflatScanList *scanlist = (IvfflatScanList *)pairingheap_remove_first(so->listQueue);

                scanlist->startPage = list->startPage;
                scanlist->distance = distance;
                scanlist->listId = list->listId;
                scanlist->tupleNum = list->tupleNum;
                pairingheap_add(so->listQueue, &scanlist->ph_node);

                maxDistance = ((IvfflatScanList *)pairingheap_first(so->listQueue))->distance;
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


bool isL2Dis(FmgrInfo *procinfo)
{
    return procinfo->fn_oid == L2_FUNC_OID;
}

void addSlots(IvfflatScanOpaque so, float *tupleNorms, ItemPointerData *tupleTids, int num, bool isL2, float *resMatrix)
{
    /*
     * Add virtual tuple
     *
     * Use procinfo from the index instead of scan key for
     * performance
     */
    TupleTableSlot *slot = MakeSingleTupleTableSlot(so->tupdesc);
    for (int i = 0; i < num; i++) {
        ExecClearTuple(slot);
        slot->tts_values[0] =
            Float8GetDatum((double)(isL2 ? tupleNorms[i] - 2 * resMatrix[i] :
            -resMatrix[i]));
        slot->tts_isnull[0] = false;
        slot->tts_values[1] = PointerGetDatum(&tupleTids[i]);
        slot->tts_isnull[1] = false;
        ExecStoreVirtualTuple(slot);

        tuplesort_puttupleslot(so->sortstate, slot);
    }
}

static inline void InitNPUCxt()
{
    pthread_rwlock_wrlock(&g_instance.npu_cxt.context_mutex);
    if (g_instance.npu_cxt.ivf_list_context == NULL) {
        g_instance.npu_cxt.ivf_list_context = AllocSetContextCreate(g_instance.instance_context,
            "ivfnpu scan tempoary context", ALLOCSET_DEFAULT_SIZES, SHARED_CONTEXT);
    }
    pthread_rwlock_unlock(&g_instance.npu_cxt.context_mutex);
}

/*
 * Get items on NPU
 */
static void GetScanItemsNPUWithoutCache(IndexScanDesc scan, Datum value)
{
    IvfflatScanOpaque so = (IvfflatScanOpaque)scan->opaque;
    TupleDesc tupdesc = RelationGetDescr(scan->indexRelation);
    double tuples = 0;
    TupleTableSlot *slot = MakeSingleTupleTableSlot(so->tupdesc);
    BufferAccessStrategy bas = GetAccessStrategy(BAS_BULKREAD);
    float *queryMatrix = DatumGetVector(value)->x;
    bool isL2 = isL2Dis(so->procinfo);
    errno_t rc = EOK;

    /* Search closest probes lists */
    int listCount = 0;
    while (!pairingheap_is_empty(so->listQueue)) {
        IvfflatScanList *list = (IvfflatScanList *)pairingheap_remove_first(so->listQueue);
        if (list->tupleNum == 0) {
            continue;
        }
        BlockNumber searchPage = list->startPage;
        int listId = list->listId;
        float *resMatrix = (float *)palloc(sizeof(float) * list->tupleNum);
        float *norms = NULL;
        if (isL2) {
            norms = (float *)palloc(sizeof(float) * list->tupleNum);
        }
        ItemPointerData *tids =
            (ItemPointerData *)palloc((sizeof(ItemPointerData) * list->tupleNum / 16 + 1) * 16);
        float *tupleMatrix =
            (float *)palloc0_huge(CurrentMemoryContext, sizeof(float) * list->tupleNum * so->dimensions);
        int curTupleNum = 0;
        uint8_t *tupleDevice = nullptr;

        while (BlockNumberIsValid(searchPage)) {
            Buffer buf;
            Page page;
            OffsetNumber maxoffno;

            buf = ReadBufferExtended(scan->indexRelation, MAIN_FORKNUM, searchPage, RBM_NORMAL, bas);
            LockBuffer(buf, BUFFER_LOCK_SHARE);
            page = BufferGetPage(buf);
            maxoffno = PageGetMaxOffsetNumber(page);

            for (OffsetNumber offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno)) {
                bool isnull;
                ItemId itemid = PageGetItemId(page, offno);
                IndexTuple itup = (IndexTuple)PageGetItem(page, itemid);
                Datum datum = index_getattr(itup, 1, tupdesc, &isnull);
                float *vec = DatumGetVector(datum)->x;
                int vecLen = so->dimensions * sizeof(float);
                tids[curTupleNum] = itup->t_tid;
                if (isL2Dis(so->procinfo)) {
                    norms[curTupleNum] = VectorSquareNorm(vec, so->dimensions);
                }
                rc = memcpy_s(tupleMatrix + curTupleNum * so->dimensions, vecLen, vec, vecLen);
                securec_check(rc, "\0", "\0");
                curTupleNum++;
            }
            searchPage = IvfflatPageGetOpaque(page)->nextblkno;
            UnlockReleaseBuffer(buf);
        }
        int ret = MatrixMulOnNPU(tupleMatrix, queryMatrix, resMatrix, list->tupleNum, 1, so->dimensions,
            &tupleDevice, listId, false);
        if (ret != 0) {
            pfree(tupleMatrix);
            pfree(tids);
            if (norms != NULL) {
                pfree(norms);
            }
            pfree_ext(resMatrix);
            FreeAccessStrategy(bas);
            ereport(ERROR, (errmsg("matrix mul failed on npu, errCode: %d.", ret)));
        }
        addSlots(so, norms, tids, list->tupleNum, isL2, resMatrix);
        pfree(tupleMatrix);
        pfree(tids);
        if (norms != NULL) {
            pfree(norms);
        }
        pfree_ext(resMatrix);

        tuples += list->tupleNum;
        ++listCount;
        if (listCount >= so->probes) {
            break;
        }
    }

    FreeAccessStrategy(bas);

    if (tuples < 100)
        ereport(DEBUG1,
                (errmsg("index scan found few tuples"), errdetail("Index may have been created with little data."),
                 errhint("Recreate the index and possibly decrease lists.")));

    tuplesort_performsort(so->sortstate);
}
static void GetScanItemsNPUWithCache(IndexScanDesc scan, Datum value)
{
    IvfflatScanOpaque so = (IvfflatScanOpaque)scan->opaque;
    TupleDesc tupdesc = RelationGetDescr(scan->indexRelation);
    double tuples = 0;
    TupleTableSlot *slot = MakeSingleTupleTableSlot(so->tupdesc);
    BufferAccessStrategy bas = GetAccessStrategy(BAS_BULKREAD);
    float *queryMatrix = DatumGetVector(value)->x;
    bool isL2 = isL2Dis(so->procinfo);
    errno_t rc = EOK;

    /* Search closest probes lists */
    int listCount = 0;
    while (!pairingheap_is_empty(so->listQueue)) {
        IvfflatScanList *list = (IvfflatScanList *)pairingheap_remove_first(so->listQueue);
        if (list->tupleNum == 0) {
            continue;
        }
        BlockNumber searchPage = list->startPage;
        int listId = list->listId;
        float *resMatrix = (float *)palloc(sizeof(float) * list->tupleNum);

        if (IvfflatNPUGetListInfo(listId).initialized == false) {
            pthread_rwlock_wrlock(&g_instance.npu_cxt.ivf_lists_mutex[listId]);
            if (IvfflatNPUGetListInfo(listId).initialized == false) {
                if (g_instance.npu_cxt.ivf_list_context == NULL) {
                    InitNPUCxt();
                }
                MemoryContext oldcxt = MemoryContextSwitchTo(g_instance.npu_cxt.ivf_list_context);
                float *norms = NULL;
                if (isL2) {
                    norms = (float *)palloc(sizeof(float) * list->tupleNum);
                }
                ItemPointerData *tids =
                    (ItemPointerData *)palloc((sizeof(ItemPointerData) * list->tupleNum / 16 + 1) * 16);
                MemoryContextSwitchTo(oldcxt);
                float *tupleMatrix =
                    (float *)palloc0_huge(CurrentMemoryContext, sizeof(float) * list->tupleNum * so->dimensions);
                int curTupleNum = 0;
                uint8_t *tupleDevice = nullptr;

                while (BlockNumberIsValid(searchPage)) {
                    Buffer buf;
                    Page page;
                    OffsetNumber maxoffno;

                    buf = ReadBufferExtended(scan->indexRelation, MAIN_FORKNUM, searchPage, RBM_NORMAL, bas);
                    LockBuffer(buf, BUFFER_LOCK_SHARE);
                    page = BufferGetPage(buf);
                    maxoffno = PageGetMaxOffsetNumber(page);

                    for (OffsetNumber offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno)) {
                        bool isnull;
                        ItemId itemid = PageGetItemId(page, offno);
                        IndexTuple itup = (IndexTuple)PageGetItem(page, itemid);
                        Datum datum = index_getattr(itup, 1, tupdesc, &isnull);
                        float *vec = DatumGetVector(datum)->x;
                        int vecLen = so->dimensions * sizeof(float);
                        tids[curTupleNum] = itup->t_tid;
                        if (isL2Dis(so->procinfo)) {
                            norms[curTupleNum] = VectorSquareNorm(vec, so->dimensions);
                        }
                        rc = memcpy_s(tupleMatrix + curTupleNum * so->dimensions, vecLen, vec, vecLen);
                        securec_check(rc, "\0", "\0");
                        curTupleNum++;
                    }
                    searchPage = IvfflatPageGetOpaque(page)->nextblkno;
                    UnlockReleaseBuffer(buf);
                }
                int ret = MatrixMulOnNPU(tupleMatrix, queryMatrix, resMatrix, list->tupleNum, 1, so->dimensions,
                    &tupleDevice, listId, true);
                if (ret != 0) {
                    pthread_rwlock_unlock(&g_instance.npu_cxt.ivf_lists_mutex[listId]);
                    pfree(tupleMatrix);
                    pfree_ext(resMatrix);
                    ereport(ERROR, (errmsg("matrix mul failed on npu, errCode: %d.", ret)));
                }
                IvfflatNPUGetListInfo(listId).tupleTids = tids;
                IvfflatNPUGetListInfo(listId).tupleNorms = norms;
                IvfflatNPUGetListInfo(listId).deviceVecs = tupleDevice;
                IvfflatNPUGetListInfo(listId).initialized = true;
                addSlots(so, IvfflatNPUGetListInfo(listId).tupleNorms, IvfflatNPUGetListInfo(listId).tupleTids,
                    list->tupleNum, isL2, resMatrix);
                pthread_rwlock_unlock(&g_instance.npu_cxt.ivf_lists_mutex[listId]);
                pfree(tupleMatrix);
            } else {
                uint8_t *tupleDevice = IvfflatNPUGetListInfo(listId).deviceVecs;
                int ret = MatrixMulOnNPU(NULL, queryMatrix, resMatrix, list->tupleNum, 1, so->dimensions, &tupleDevice,
                    listId, true);
                if (ret != 0) {
                    pthread_rwlock_unlock(&g_instance.npu_cxt.ivf_lists_mutex[listId]);
                    pfree_ext(resMatrix);
                    ereport(ERROR, (errmsg("matrix mul failed on npu, errCode: %d.", ret)));
                }
                addSlots(so, IvfflatNPUGetListInfo(listId).tupleNorms, IvfflatNPUGetListInfo(listId).tupleTids,
                    list->tupleNum, isL2, resMatrix);
                pthread_rwlock_unlock(&g_instance.npu_cxt.ivf_lists_mutex[listId]);
            }
        } else {
            pthread_rwlock_rdlock(&g_instance.npu_cxt.ivf_lists_mutex[listId]);
            uint8_t *tupleDevice = IvfflatNPUGetListInfo(listId).deviceVecs;
            int ret = MatrixMulOnNPU(NULL, queryMatrix, resMatrix, list->tupleNum, 1, so->dimensions, &tupleDevice,
                listId, true);
            if (ret != 0) {
                pthread_rwlock_unlock(&g_instance.npu_cxt.ivf_lists_mutex[listId]);
                pfree_ext(resMatrix);
                ereport(ERROR, (errmsg("matrix mul failed on npu, errCode: %d.", ret)));
            }
            addSlots(so, IvfflatNPUGetListInfo(listId).tupleNorms, IvfflatNPUGetListInfo(listId).tupleTids,
                list->tupleNum, isL2, resMatrix);
            pthread_rwlock_unlock(&g_instance.npu_cxt.ivf_lists_mutex[listId]);
        }
        pfree_ext(resMatrix);
        tuples += list->tupleNum;
        ++listCount;
        if (listCount >= so->probes) {
            break;
        }
    }

    FreeAccessStrategy(bas);

    if (tuples < 100)
        ereport(DEBUG1,
                (errmsg("index scan found few tuples"), errdetail("Index may have been created with little data."),
                 errhint("Recreate the index and possibly decrease lists.")));

    tuplesort_performsort(so->sortstate);
}

static void GetScanItemsNPU(IndexScanDesc scan, Datum value)
{
    if (g_instance.attr.attr_storage.cache_data_on_npu) {
        GetScanItemsNPUWithCache(scan, value);
    } else {
        GetScanItemsNPUWithoutCache(scan, value);
    }
    return;
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
    bool l2CosResidual = so->funcType != PQ_DIS_IP && so->byResidual;
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
                    distance = GetPQDistance(simTable, code, dis0, pqM, pqKsub, so->funcType == PQ_DIS_IP);
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
                        e->heapTid = &itup->t_tid;
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

    FreeAccessStrategy(bas);

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
            slot->tts_values[1] = PointerGetDatum(node->heapTid);
            slot->tts_isnull[1] = false;
            ExecStoreVirtualTuple(slot);

            tuplesort_puttupleslot(so->sortstate, slot);
        }

        if (BlockNumberIsValid(blkno)) {
            UnlockReleaseBuffer(buf);
        }
    }

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

void initIvfNpuContext(Oid scanOid, int nlists)
{
    pthread_rwlock_wrlock(&g_instance.npu_cxt.context_mutex);
    if (scanOid != g_instance.npu_cxt.index_oid) {
        int oldLists = g_instance.npu_cxt.ivf_lists_num;
        if (g_instance.npu_cxt.index_oid != -1) {
            for (int i = 0; i < oldLists; i++) {
                MemoryContext oldcxt = MemoryContextSwitchTo(g_instance.npu_cxt.ivf_list_context);
                if (IvfflatNPUGetListInfo(i).tupleNorms != NULL) {
                    pfree(IvfflatNPUGetListInfo(i).tupleNorms);
                    IvfflatNPUGetListInfo(i).tupleNorms = NULL;
                }
                if (IvfflatNPUGetListInfo(i).tupleTids != NULL) {
                    pfree(IvfflatNPUGetListInfo(i).tupleTids);
                    IvfflatNPUGetListInfo(i).tupleTids = NULL;
                }
                MemoryContextSwitchTo(oldcxt);

                pthread_rwlock_destroy(&(g_instance.npu_cxt.ivf_lists_mutex[i]));
                // 删除device侧分区i矩阵的内存q
                ReleaseNPUCache(&(IvfflatNPUGetListInfo(i).deviceVecs), i);
            }
            delete [] static_cast<IvfListInfo *>(g_instance.npu_cxt.ivf_lists_info);
            g_instance.npu_cxt.ivf_lists_info = NULL;
            delete [] (g_instance.npu_cxt.ivf_lists_mutex);
            g_instance.npu_cxt.ivf_lists_mutex = NULL;
        }
        g_instance.npu_cxt.ivf_lists_mutex = new pthread_rwlock_t[nlists];
        g_instance.npu_cxt.index_oid = scanOid;
        g_instance.npu_cxt.ivf_lists_info = new IvfListInfo[nlists];
        if (g_instance.npu_cxt.ivf_list_context != NULL) {
            MemoryContextDelete(g_instance.npu_cxt.ivf_list_context);
            g_instance.npu_cxt.ivf_list_context = NULL;
        }
        for (int i = 0; i < nlists; i++) {
            g_instance.npu_cxt.ivf_lists_mutex[i] = PTHREAD_RWLOCK_INITIALIZER;
            g_instance.npu_cxt.ivf_lists_num = nlists;
            IvfflatNPUGetListInfo(i).tupleNorms = NULL;
            IvfflatNPUGetListInfo(i).tupleTids = NULL;
            IvfflatNPUGetListInfo(i).deviceVecs = NULL;
            IvfflatNPUGetListInfo(i).initialized = false;
        }
    }
    pthread_rwlock_unlock(&g_instance.npu_cxt.context_mutex);
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

    scan->opaque = so;

    if (u_sess->datavec_ctx.enable_npu && so->typeInfo->supportNPU) {
        initIvfNpuContext(index->rd_id, lists);
    }

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

    if (u_sess->datavec_ctx.enable_npu && !so->typeInfo->supportNPU) {
        ereport(ERROR, (errmsg("this data type cannot support ivfnpu.")));
    }
    if (u_sess->datavec_ctx.enable_npu && !g_npu_func.inited) {
        ereport(ERROR, (errmsg("this instance has not currently loaded the ivfflatnpu dynamic library.")));
    }

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

        IvfflatBench("GetScanLists", GetScanLists(scan, value));

        if (so->enablePQ) {
            MemoryContext oldCxt = MemoryContextSwitchTo(so->pqCtx);

            float *simTable = (float *)palloc0(so->pqM * so->pqKsub * sizeof(float));
            IvfpqComputeQueryRelTables(so, scan->indexRelation, value, simTable);
            IvfflatBench("GetScanItemsPQ", GetScanItemsPQ(scan, value, simTable));

            MemoryContextSwitchTo(oldCxt);
        } else {
            if (u_sess->datavec_ctx.enable_npu && so->typeInfo->supportNPU) {
                IvfflatBench("GetScanItemsNPU", GetScanItemsNPU(scan, value));
            } else {
                IvfflatBench("GetScanItems", GetScanItems(scan, value));
            }
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
        } else {
            if (u_sess->datavec_ctx.enable_npu && so->typeInfo->supportNPU) {
                if (g_instance.npu_cxt.ivf_list_context == NULL) {
                    g_instance.npu_cxt.ivf_list_context = AllocSetContextCreate(g_instance.instance_context,
                        "ivfnpu scan tempoary context", ALLOCSET_DEFAULT_SIZES);
                }
                MemoryContext oldcxt = MemoryContextSwitchTo(g_instance.npu_cxt.ivf_list_context);
                IvfflatBench("GetScanItemsNPU", GetScanItemsNPU(scan, value));
                MemoryContextSwitchTo(oldcxt);
            } else {
                IvfflatBench("GetScanItems", GetScanItems(scan, value));
            }
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

    pfree(so);
    scan->opaque = NULL;
}
