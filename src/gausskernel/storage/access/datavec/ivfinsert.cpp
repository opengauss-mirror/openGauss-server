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
 * ivfinsert.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/ivfinsert.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include <cfloat>

#include "access/generic_xlog.h"
#include "access/datavec/ivfflat.h"
#include "storage/buf/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/memutils.h"
#include "access/datavec/ivfnpuadaptor.h"

#define IvfflatNPUListInfo(i) (((IvfListInfo *)g_instance.npu_cxt.ivf_lists_info)[i])

/*
 * Find the list that minimizes the distance function
 */
static void FindInsertPage(Relation index, Datum *values, BlockNumber *insertPage, ListInfo *listInfo)
{
    double minDistance = DBL_MAX;
    uint16 pqTableNblk;
    uint32 pqDisTableNblk;
    IvfGetPQInfoFromMetaPage(index, &pqTableNblk, NULL, &pqDisTableNblk, NULL);
    BlockNumber nextblkno = IVFPQTABLE_START_BLKNO + pqTableNblk + pqDisTableNblk;
    FmgrInfo *procinfo;
    Oid collation;

    /* Avoid compiler warning */
    listInfo->blkno = nextblkno;
    listInfo->offno = FirstOffsetNumber;

    procinfo = index_getprocinfo(index, 1, IVFFLAT_DISTANCE_PROC);
    collation = index->rd_indcollation[0];

    /* Search all list pages */
    while (BlockNumberIsValid(nextblkno)) {
        Buffer cbuf;
        Page cpage;
        OffsetNumber maxoffno;

        cbuf = ReadBuffer(index, nextblkno);
        LockBuffer(cbuf, BUFFER_LOCK_SHARE);
        cpage = BufferGetPage(cbuf);
        maxoffno = PageGetMaxOffsetNumber(cpage);

        for (OffsetNumber offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno)) {
            IvfflatList list;
            double distance;

            list = (IvfflatList)PageGetItem(cpage, PageGetItemId(cpage, offno));
            distance =
                DatumGetFloat8(FunctionCall2Coll(procinfo, collation, values[0], PointerGetDatum(&list->center)));
            if (distance < minDistance || !BlockNumberIsValid(*insertPage)) {
                *insertPage = list->insertPage;
                listInfo->blkno = nextblkno;
                listInfo->offno = offno;
                minDistance = distance;
            }
        }

        nextblkno = IvfflatPageGetOpaque(cpage)->nextblkno;

        UnlockReleaseBuffer(cbuf);
    }
}

static void InitPQParamsOnDisk(Relation index, PQParams *params, int dim, bool *enablePQ,
                               bool *byResidual, uint16 *pqcodeSize)
{
    Buffer buf;
    Page page;
    IvfflatMetaPage metap;
    const IvfflatTypeInfo *typeInfo = IvfflatGetTypeInfo(index);

    buf = ReadBuffer(index, IVFFLAT_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    metap = IvfflatPageGetMeta(page);
    if (unlikely(metap->magicNumber != IVFFLAT_MAGIC_NUMBER)) {
        UnlockReleaseBuffer(buf);
        elog(ERROR, "ivfflat index is not valid");
    }

    *enablePQ = metap->enablePQ;
    params->pqM = metap->pqM;
    params->pqKsub = metap->pqKsub;
    *byResidual = metap->byResidual;
    *pqcodeSize = metap->pqcodeSize;
    UnlockReleaseBuffer(buf);

    if (*enablePQ) {
        if (!g_instance.pq_inited) {
            ereport(ERROR, (errmsg("the SQL involves operations related to IVFPQ, "
                                    "but this instance has not currently loaded the PQ dynamic library.")));
        }
        FmgrInfo *procinfo = index_getprocinfo(index, 1, IVFFLAT_DISTANCE_PROC);
        FmgrInfo *normprocinfo = IvfflatOptionalProcInfo(index, IVFFLAT_NORM_PROC);
        params->funcType = getIVFPQfunctionType(procinfo, normprocinfo);
        params->dim = dim;
        Size subItemsize = typeInfo->itemSize(dim / params->pqM);
        params->subItemSize = MAXALIGN(subItemsize);

        /* Now save pqTable in the relcache entry. */
        if (index->pqTable == NULL) {
            MemoryContext oldcxt = MemoryContextSwitchTo(index->rd_indexcxt);
            index->pqTable = IVFPQLoadPQtable(index);
            (void)MemoryContextSwitchTo(oldcxt);
        }
        params->pqTable = index->pqTable;
    } else {
        params->pqTable = NULL;
    }
}

void ReleaseIvfNpuContext(Oid current_index_oid)
{
    pthread_rwlock_wrlock(&g_instance.npu_cxt.context_mutex);
    if (current_index_oid == g_instance.npu_cxt.index_oid &&
        g_instance.npu_cxt.index_oid != -1) {
            for (int i = 0; i < g_instance.npu_cxt.ivf_lists_num; i++) {
                pthread_rwlock_wrlock(&g_instance.npu_cxt.ivf_lists_mutex[i]);
                MemoryContext oldcxt = MemoryContextSwitchTo(g_instance.npu_cxt.ivf_list_context);
                if (IvfflatNPUListInfo(i).tupleNorms != NULL) {
                    pfree(IvfflatNPUListInfo(i).tupleNorms);
                    IvfflatNPUListInfo(i).tupleNorms = NULL;
                }
                if (IvfflatNPUListInfo(i).tupleTids != NULL) {
                    pfree(IvfflatNPUListInfo(i).tupleTids);
                    IvfflatNPUListInfo(i).tupleTids = NULL;
                }
                MemoryContextSwitchTo(oldcxt);

                ReleaseNPUCache(&(IvfflatNPUListInfo(i).deviceVecs), i);
                IvfflatNPUListInfo(i).initialized = false;
                pthread_rwlock_unlock(&g_instance.npu_cxt.ivf_lists_mutex[i]);
                pthread_rwlock_destroy(&(g_instance.npu_cxt.ivf_lists_mutex[i]));
            }
            delete [] static_cast<IvfListInfo *>(g_instance.npu_cxt.ivf_lists_info);
            g_instance.npu_cxt.ivf_lists_info = NULL;
            delete [] (g_instance.npu_cxt.ivf_lists_mutex);
            g_instance.npu_cxt.ivf_lists_mutex = NULL;

            g_instance.npu_cxt.index_oid = -1;
            g_instance.npu_cxt.ivf_lists_num = 0;

        if (g_instance.npu_cxt.ivf_list_context != NULL) {
            MemoryContextDelete(g_instance.npu_cxt.ivf_list_context);
            g_instance.npu_cxt.ivf_list_context = NULL;
        }
    }
    pthread_rwlock_unlock(&g_instance.npu_cxt.context_mutex);
}

/*
 * Insert a tuple into the index
 */
static void InsertTuple(Relation index, Datum *values, const bool *isnull, ItemPointer heap_tid, Relation heapRel)
{
    const IvfflatTypeInfo *typeInfo = IvfflatGetTypeInfo(index);
    IndexTuple itup;
    Datum value;
    FmgrInfo *normprocinfo;
    Buffer buf;
    Page page;
    GenericXLogState *state;
    Size itemsz;
    BlockNumber insertPage = InvalidBlockNumber;
    ListInfo listInfo;
    BlockNumber originalInsertPage;
    PQParams params;
    bool enablePQ;
    bool byResidual;
    uint16 pqcodeSize;
    int dim = TupleDescAttr(index->rd_att, 0)->atttypmod;

    /* Detoast once for all calls */
    value = PointerGetDatum(PG_DETOAST_DATUM(values[0]));

    /* Normalize if needed */
    normprocinfo = IvfflatOptionalProcInfo(index, IVFFLAT_NORM_PROC);
    if (normprocinfo != NULL) {
        Oid collation = index->rd_indcollation[0];

        if (!IvfflatCheckNorm(normprocinfo, collation, value)) {
            return;
        }

        value = IvfflatNormValue(typeInfo, collation, value);
    }

    /* Ensure index is valid */
    IvfflatGetMetaPageInfo(index, NULL, NULL);

    InitPQParamsOnDisk(index, &params, dim, &enablePQ, &byResidual, &pqcodeSize);

    /* Find the insert page - sets the page and list info */
    FindInsertPage(index, values, &insertPage, &listInfo);
    Assert(BlockNumberIsValid(insertPage));
    originalInsertPage = insertPage;

    /* Form tuple */
    itup = index_form_tuple(RelationGetDescr(index), &value, isnull);
    itup->t_tid = *heap_tid;

    /* Get tuple size */
    itemsz = MAXALIGN(IndexTupleSize(itup));
    Assert(itemsz <=
           BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - MAXALIGN(sizeof(IvfflatPageOpaqueData)) - sizeof(ItemIdData));

    /* Find a page to insert the item */
    for (;;) {
        buf = ReadBuffer(index, insertPage);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

        state = GenericXLogStart(index);
        page = GenericXLogRegisterBuffer(state, buf, 0);
        if (PageGetFreeSpace(page) >= itemsz + MAXALIGN(pqcodeSize)) {
            break;
        }

        insertPage = IvfflatPageGetOpaque(page)->nextblkno;
        if (BlockNumberIsValid(insertPage)) {
            /* Move to next page */
            GenericXLogAbort(state);
            UnlockReleaseBuffer(buf);
        } else {
            Buffer newbuf;
            Page newpage;

            /* Add a new page */
            LockRelationForExtension(index, ExclusiveLock);
            newbuf = IvfflatNewBuffer(index, MAIN_FORKNUM);
            UnlockRelationForExtension(index, ExclusiveLock);

            /* Init new page */
            newpage = GenericXLogRegisterBuffer(state, newbuf, GENERIC_XLOG_FULL_IMAGE);
            IvfflatInitPage(newbuf, newpage);

            /* Update insert page */
            insertPage = BufferGetBlockNumber(newbuf);

            /* Update previous buffer */
            IvfflatPageGetOpaque(page)->nextblkno = insertPage;

            /* Commit */
            GenericXLogFinish(state);

            /* Unlock previous buffer */
            UnlockReleaseBuffer(buf);

            /* Prepare new buffer */
            state = GenericXLogStart(index);
            buf = newbuf;
            page = GenericXLogRegisterBuffer(state, buf, 0);
            break;
        }
    }

    if (enablePQ) {
        uint8 *pqcode = (uint8 *)palloc(pqcodeSize);
        float *vec = ((Vector *)value)->x;
        if (byResidual) {
            float *resVec = (float *)palloc(dim * sizeof(float));
            Buffer cbuf = ReadBuffer(index, listInfo.blkno);
            LockBuffer(cbuf, BUFFER_LOCK_SHARE);
            Page cpage = BufferGetPage(cbuf);
            IvfflatList list = (IvfflatList)PageGetItem(cpage, PageGetItemId(cpage, listInfo.offno));

            for (int i = 0; i < dim; i++) {
                resVec[i] = vec[i] - list->center.x[i];
            }
            vec = resVec;
            UnlockReleaseBuffer(cbuf);
        }
        IvfComputeVectorPQCode(vec, &params, pqcode);
        ((PageHeader)page)->pd_upper -= MAXALIGN(pqcodeSize);
        errno_t rc = memcpy_s(
            ((char *)page) + ((PageHeader)page)->pd_upper, pqcodeSize, (char *)pqcode, pqcodeSize);
        securec_check_c(rc, "\0", "\0");
    }

    /* Add to next offset */
    if (PageAddItem(page, (Item)itup, itemsz, InvalidOffsetNumber, false, false) == InvalidOffsetNumber)
        elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));

    IvfflatCommitBuffer(buf, state);

    /* Update the insert page */
    bool isSupportNPU = (u_sess->datavec_ctx.enable_npu || g_instance.attr.attr_storage.enable_ivfflat_npu);
    if (isSupportNPU) {
        IvfflatUpdateList(index, listInfo, insertPage, originalInsertPage, InvalidBlockNumber, MAIN_FORKNUM, 1);

        /* Release Npu Cache */
        Oid current_index_oid = RelationGetRelid(index);
        ReleaseIvfNpuContext(current_index_oid);
    } else if (!isSupportNPU && (insertPage != originalInsertPage)) {
        IvfflatUpdateList(index, listInfo, insertPage, originalInsertPage, InvalidBlockNumber, MAIN_FORKNUM, 0);
    }
}

/*
 * Insert a tuple into the index
 */
bool ivfflatinsert_internal(Relation index, Datum *values, const bool *isnull, ItemPointer heap_tid, Relation heap,
                            IndexUniqueCheck checkUnique)
{
    MemoryContext oldCtx;
    MemoryContext insertCtx;

    /* Skip nulls */
    if (isnull[0]) {
        return false;
    }

    /*
     * Use memory context since detoast, IvfflatNormValue, and
     * index_form_tuple can allocate
     */
    insertCtx = AllocSetContextCreate(CurrentMemoryContext, "Ivfflat insert temporary context", ALLOCSET_DEFAULT_SIZES);
    oldCtx = MemoryContextSwitchTo(insertCtx);

    /* Insert tuple */
    InsertTuple(index, values, isnull, heap_tid, heap);

    /* Delete memory context */
    MemoryContextSwitchTo(oldCtx);
    MemoryContextDelete(insertCtx);

    return false;
}
