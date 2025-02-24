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
 * ivfutils.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/ivfutils.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/generic_xlog.h"
#include "access/datavec/bitvec.h"
#include "catalog/pg_type.h"
#include "fmgr.h"
#include "access/datavec/halfutils.h"
#include "access/datavec/halfvec.h"
#include "access/datavec/ivfflat.h"
#include "access/datavec/utils.h"
#include "storage/buf/bufmgr.h"

/*
 * Get the number of lists in the index
 */
int IvfflatGetLists(Relation index)
{
    IvfflatOptions *opts = (IvfflatOptions *)index->rd_options;

    if (opts)
        return opts->lists;

    return IVFFLAT_DEFAULT_LISTS;
}

/*
 * Get proc
 */
FmgrInfo *IvfflatOptionalProcInfo(Relation index, uint16 procnum)
{
    if (!OidIsValid(index_getprocid(index, 1, procnum)))
        return NULL;

    return index_getprocinfo(index, 1, procnum);
}

/*
 * Normalize value
 */
Datum IvfflatNormValue(const IvfflatTypeInfo *typeInfo, Oid collation, Datum value)
{
    return DirectFunctionCall1Coll(typeInfo->normalize, collation, value);
}

/*
 * Check if non-zero norm
 */
bool IvfflatCheckNorm(FmgrInfo *procinfo, Oid collation, Datum value)
{
    return DatumGetFloat8(FunctionCall1Coll(procinfo, collation, value)) > 0;
}

/*
 * New buffer
 */
Buffer IvfflatNewBuffer(Relation index, ForkNumber forkNum)
{
    Buffer buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);

    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    return buf;
}

/*
 * Init page
 */
void IvfflatInitPage(Buffer buf, Page page)
{
    PageInit(page, BufferGetPageSize(buf), sizeof(IvfflatPageOpaqueData));
    IvfflatPageGetOpaque(page)->nextblkno = InvalidBlockNumber;
    IvfflatPageGetOpaque(page)->page_id = IVFFLAT_PAGE_ID;
}

/*
 * Init and register page
 */
void IvfflatInitRegisterPage(Relation index, Buffer *buf, Page *page, GenericXLogState **state)
{
    *state = GenericXLogStart(index);
    *page = GenericXLogRegisterBuffer(*state, *buf, GENERIC_XLOG_FULL_IMAGE);
    IvfflatInitPage(*buf, *page);
}

/*
 * Commit buffer
 */
void IvfflatCommitBuffer(Buffer buf, GenericXLogState *state)
{
    GenericXLogFinish(state);
    UnlockReleaseBuffer(buf);
}

/*
 * Add a new page
 *
 * The order is very important!!
 */
void IvfflatAppendPage(Relation index, Buffer *buf, Page *page, GenericXLogState **state, ForkNumber forkNum)
{
    /* Get new buffer */
    Buffer newbuf = IvfflatNewBuffer(index, forkNum);
    Page newpage = GenericXLogRegisterBuffer(*state, newbuf, GENERIC_XLOG_FULL_IMAGE);

    /* Update the previous buffer */
    IvfflatPageGetOpaque(*page)->nextblkno = BufferGetBlockNumber(newbuf);

    /* Init new page */
    IvfflatInitPage(newbuf, newpage);

    /* Commit */
    GenericXLogFinish(*state);

    /* Unlock */
    UnlockReleaseBuffer(*buf);

    *state = GenericXLogStart(index);
    *page = GenericXLogRegisterBuffer(*state, newbuf, GENERIC_XLOG_FULL_IMAGE);
    *buf = newbuf;
}

/*
 * Get the metapage info
 */
void IvfflatGetMetaPageInfo(Relation index, int *lists, int *dimensions)
{
    Buffer buf;
    Page page;
    IvfflatMetaPage metap;

    buf = ReadBuffer(index, IVFFLAT_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    metap = IvfflatPageGetMeta(page);
    if (unlikely(metap->magicNumber != IVFFLAT_MAGIC_NUMBER))
        elog(ERROR, "ivfflat index is not valid");

    if (lists != NULL)
        *lists = metap->lists;

    if (dimensions != NULL)
        *dimensions = metap->dimensions;

    UnlockReleaseBuffer(buf);
}

/*
 * Update the start or insert page of a list
 */
void IvfflatUpdateList(Relation index, ListInfo listInfo, BlockNumber insertPage, BlockNumber originalInsertPage,
                       BlockNumber startPage, ForkNumber forkNum)
{
    Buffer buf;
    Page page;
    GenericXLogState *state;
    IvfflatList list;
    bool changed = false;

    buf = ReadBufferExtended(index, forkNum, listInfo.blkno, RBM_NORMAL, NULL);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    state = GenericXLogStart(index);
    page = GenericXLogRegisterBuffer(state, buf, 0);
    list = (IvfflatList)PageGetItem(page, PageGetItemId(page, listInfo.offno));
    if (BlockNumberIsValid(insertPage) && insertPage != list->insertPage) {
        /* Skip update if insert page is lower than original insert page  */
        /* This is needed to prevent insert from overwriting vacuum */
        if (!BlockNumberIsValid(originalInsertPage) || insertPage >= originalInsertPage) {
            list->insertPage = insertPage;
            changed = true;
        }
    }

    if (BlockNumberIsValid(startPage) && startPage != list->startPage) {
        list->startPage = startPage;
        changed = true;
    }

    /* Only commit if changed */
    if (changed) {
        IvfflatCommitBuffer(buf, state);
    } else {
        GenericXLogAbort(state);
        UnlockReleaseBuffer(buf);
    }
}

char* IVFPQLoadPQtable(Relation index)
{
    Buffer buf;
    Page page;
    uint16 nblks;
    uint32 curFlushSize;
    uint32 pqTableSize;
    char* pqTable;

    IvfGetPQInfoFromMetaPage(index, &nblks, &pqTableSize, NULL, NULL);
    pqTable = (char*)palloc0(pqTableSize);

    for (uint16 i = 0; i < nblks; i++) {
        curFlushSize = (i == nblks - 1) ? (pqTableSize - i * IVFPQTABLE_STORAGE_SIZE) : IVFPQTABLE_STORAGE_SIZE;
        buf = ReadBuffer(index, IVFPQTABLE_START_BLKNO + i);
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buf);
        errno_t err = memcpy_s(pqTable + i * IVFPQTABLE_STORAGE_SIZE, curFlushSize,
                               PageGetContents(page), curFlushSize);
        securec_check(err, "\0", "\0");
        UnlockReleaseBuffer(buf);
    }
    return pqTable;
}

float* IVFPQLoadPQDisTable(Relation index)
{
    Buffer buf;
    Page page;
    uint16 pqTableNblk;
    uint32 nblks;
    uint32 curFlushSize;
    uint64 pqDisTableSize;
    float* disTable;

    IvfGetPQInfoFromMetaPage(index, &pqTableNblk, NULL, &nblks, &pqDisTableSize);
    disTable = (float*)palloc0(pqDisTableSize);

    BlockNumber startBlkno = IVFPQTABLE_START_BLKNO + pqTableNblk;
    for (uint32 i = 0; i < nblks; i++) {
        curFlushSize = (i == nblks - 1) ? (pqDisTableSize - i * IVFPQTABLE_STORAGE_SIZE) : IVFPQTABLE_STORAGE_SIZE;
        buf = ReadBuffer(index, startBlkno + i);
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buf);
        errno_t err = memcpy_s((char*)disTable + i * IVFPQTABLE_STORAGE_SIZE, curFlushSize,
                               PageGetContents(page), curFlushSize);
        securec_check(err, "\0", "\0");
        UnlockReleaseBuffer(buf);
    }
    return disTable;
}

/*
 * Get Ivfflat PQ info
 */
void GetPQInfoOnDisk(IvfflatScanOpaque so, Relation index)
{
    Buffer buf;
    Page page;
    IvfflatMetaPage metap;

    buf = ReadBuffer(index, IVFFLAT_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    metap = IvfflatPageGetMeta(page);
    if (unlikely(metap->magicNumber != IVFFLAT_MAGIC_NUMBER)) {
        UnlockReleaseBuffer(buf);
        elog(ERROR, "ivfflat index is not valid");
    }

    so->enablePQ = metap->enablePQ;
    so->pqM = metap->pqM;
    so->pqKsub = metap->pqKsub;
    so->byResidual = metap->byResidual;
    UnlockReleaseBuffer(buf);

    if (so->enablePQ) {
        so->funcType = getIVFPQfunctionType(so->procinfo, so->normprocinfo);
        /* Now save pqTable and pqDistanceTable in the relcache entry. */
        if (index->pqTable == NULL) {
            MemoryContext oldcxt = MemoryContextSwitchTo(index->rd_indexcxt);
            index->pqTable = IVFPQLoadPQtable(index);
            (void)MemoryContextSwitchTo(oldcxt);
        }
        if (index->pqDistanceTable == NULL && so->byResidual && so->funcType != IVFPQ_DIS_IP) {
            MemoryContext oldcxt = MemoryContextSwitchTo(index->rd_indexcxt);
            index->pqDistanceTable = IVFPQLoadPQDisTable(index);
            (void)MemoryContextSwitchTo(oldcxt);
        }
    }
}

void IvfpqComputeQueryRelTablesInternal(IvfflatScanOpaque so, float *q, char *pqTable, bool innerPro, float *simTable)
{
    int pqM = so->pqM;
    int pqKsub = so->pqKsub;
    int dim = so->dimensions;
    int dsub = dim / pqM;
    Size subSize = MAXALIGN(so->typeInfo->itemSize(dsub));

    for (int m = 0; m < pqM; m++) {
        int offset = m * pqKsub;
        float *qsubVector = q + m * dsub;
        float *dis = simTable + offset;
        /* one-to-many computation */
        if (innerPro) {
            /* negate when GetPQDistance  */
            VectorInnerProductNY(dsub, pqKsub, qsubVector, pqTable, subSize, offset, dis);
        } else {
            VectorL2SquaredDistanceNY(dsub, pqKsub, qsubVector, pqTable, subSize, offset, dis);
        }
    }
}

/*
 * Precompute some tables specific to query q, r is cluster center of PQ.
 */
void IvfpqComputeQueryRelTables(IvfflatScanOpaque so, Relation index, Datum q, float *simTable)
{
    if (so->funcType == IVFPQ_DIS_IP) {
        /* compute q*r */
        IvfpqComputeQueryRelTablesInternal(so, DatumGetVector(q)->x, index->pqTable, true, simTable);
    } else {
        /* funcType is cosine or l2 */
        if (so->byResidual) {
            /* compute q*r */
            IvfpqComputeQueryRelTablesInternal(so, DatumGetVector(q)->x, index->pqTable, true, simTable);
        } else {
            /* compute (q-r)^2 */
            IvfpqComputeQueryRelTablesInternal(so, DatumGetVector(q)->x, index->pqTable, false, simTable);
        }
    }
}

uint8 *LoadPQCode(IndexTuple itup)
{
    return (uint8 *)((char *)itup + MAXALIGN(IndexTupleSize(itup)));
}

float GetPQDistance(float *pqDistanceTable, uint8 *code, double dis0, int pqM, int pqKsub, bool innerPro)
{
    float resDistance = 0.0;
    for (int i = 0; i < pqM; i++) {
        int offset = i * pqKsub + code[i];
        resDistance += pqDistanceTable[offset];
    }
    return innerPro ? (dis0 - resDistance) : (dis0 + resDistance);
}

IvfpqPairingHeapNode * IvfpqCreatePairingHeapNode(float distance, ItemPointer heapTid,
                                                  BlockNumber indexBlk, OffsetNumber indexOff)
{
    IvfpqPairingHeapNode *n = (IvfpqPairingHeapNode *)palloc(sizeof(IvfpqPairingHeapNode));
    n->distance = distance;
    n->heapTid = heapTid;
    n->indexBlk = indexBlk;
    n->indexOff = indexOff;
    return n;
}

/*
 * Get type info
 */
const IvfflatTypeInfo *IvfflatGetTypeInfo(Relation index)
{
    FmgrInfo *procinfo = IvfflatOptionalProcInfo(index, IVFFLAT_TYPE_INFO_PROC);

    if (procinfo == NULL) {
        static const IvfflatTypeInfo typeInfo = {.maxDimensions = IVFFLAT_MAX_DIM,
                                                 .supportPQ = true,
                                                 .normalize = l2_normalize,
                                                 .itemSize = VectorItemSize,
                                                 .updateCenter = VectorUpdateCenter,
                                                 .sumCenter = VectorSumCenter};

        return (&typeInfo);
    } else {
        return (const IvfflatTypeInfo *)DatumGetPointer(OidFunctionCall0Coll(procinfo->fn_oid, InvalidOid));
    }
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(ivfflat_halfvec_support);
Datum ivfflat_halfvec_support(PG_FUNCTION_ARGS)
{
    static const IvfflatTypeInfo typeInfo = {.maxDimensions = IVFFLAT_MAX_DIM * 2,
                                             .supportPQ = false,
                                             .normalize = halfvec_l2_normalize,
                                             .itemSize = HalfvecItemSize,
                                             .updateCenter = HalfvecUpdateCenter,
                                             .sumCenter = HalfvecSumCenter};

    PG_RETURN_POINTER(&typeInfo);
};

PGDLLEXPORT PG_FUNCTION_INFO_V1(ivfflat_bit_support);
Datum ivfflat_bit_support(PG_FUNCTION_ARGS)
{
    static const IvfflatTypeInfo typeInfo = {.maxDimensions = IVFFLAT_MAX_DIM * 32,
                                             .supportPQ = false,
                                             .normalize = NULL,
                                             .itemSize = BitItemSize,
                                             .updateCenter = BitUpdateCenter,
                                             .sumCenter = BitSumCenter};

    PG_RETURN_POINTER(&typeInfo);
};

int getIVFPQfunctionType(FmgrInfo *procinfo, FmgrInfo *normprocinfo)
{
    if (procinfo->fn_oid == 8431) {
        return IVF_PQ_DIS_L2;
    } else if (procinfo->fn_oid == 8434) {
        if (normprocinfo == NULL) {
            return IVF_PQ_DIS_IP;
        } else {
            return IVF_PQ_DIS_COSINE;
        }
    } else {
        ereport(ERROR, (errmsg("current data type or distance type can't support IVFPQ.")));
        return -1;
    }
}

/*
* Get the info related to pqTable in metapage
*/
void IvfGetPQInfoFromMetaPage(Relation index, uint16 *pqTableNblk, uint32 *pqTableSize,
                              uint32 *pqPreComputeTableNblk, uint64 *pqPreComputeTableSize)
{
    Buffer buf;
    Page page;
    IvfflatMetaPage metap;

    buf = ReadBuffer(index, IVFFLAT_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    metap = IvfflatPageGetMeta(page);

    PG_TRY();
    {
        if (unlikely(metap->magicNumber != IVFFLAT_MAGIC_NUMBER)) {
            elog(ERROR, "ivfflat index is not valid");
        }
    }
    PG_CATCH();
    {
        UnlockReleaseBuffer(buf);
        PG_RE_THROW();
    }
    PG_END_TRY();

    if (pqTableNblk != NULL) {
        *pqTableNblk = metap->pqTableNblk;
    }
    if (pqTableSize != NULL) {
        *pqTableSize = metap->pqTableSize;
    }
    if (pqPreComputeTableNblk != NULL) {
        *pqPreComputeTableNblk = metap->pqPreComputeTableNblk;
    }
    if (pqPreComputeTableSize != NULL) {
        *pqPreComputeTableSize = metap->pqPreComputeTableSize;
    }

    UnlockReleaseBuffer(buf);
}

/*
 * Get whether to enable PQ
 */
bool IvfGetEnablePQ(Relation index)
{
    IvfflatOptions *opts = (IvfflatOptions *)index->rd_options;

    if (opts) {
        return opts->enablePQ;
    }

    return GENERIC_DEFAULT_ENABLE_PQ;
}

/*
 * Get the number of subquantizer
 */
int IvfGetPqM(Relation index)
{
    IvfflatOptions *opts = (IvfflatOptions *)index->rd_options;

    if (opts) {
        return opts->pqM;
    }

    return GENERIC_DEFAULT_PQ_M;
}

/*
 * Get the number of centroids for each subquantizer
 */
int IvfGetPqKsub(Relation index)
{
    IvfflatOptions *opts = (IvfflatOptions *)index->rd_options;

    if (opts) {
        return opts->pqKsub;
    }

    return GENERIC_DEFAULT_PQ_KSUB;
}

/*
 * Get whether to use residual
 */
int IvfGetByResidual(Relation index)
{
    IvfflatOptions *opts = (IvfflatOptions *)index->rd_options;

    if (opts) {
        return opts->byResidual;
    }

    return IVFPQ_DEFAULT_RESIDUAL;
}

void IvfFlushPQInfoInternal(Relation index, char* table, BlockNumber startBlkno, uint32 nblks, uint64 totalSize)
{
    Buffer buf;
    Page page;
    uint32 curFlushSize;
    GenericXLogState *state;

    for (uint32 i = 0; i < nblks; i++) {
        curFlushSize = (i == nblks - 1) ?
                        (totalSize - i * IVF_PQTABLE_STORAGE_SIZE) : IVF_PQTABLE_STORAGE_SIZE;
        buf = ReadBufferExtended(index, MAIN_FORKNUM, startBlkno + i, RBM_NORMAL, NULL);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        state = GenericXLogStart(index);
        page = GenericXLogRegisterBuffer(state, buf, 0);
        errno_t err = memcpy_s(PageGetContents(page), curFlushSize,
                               table + i * IVF_PQTABLE_STORAGE_SIZE, curFlushSize);
        securec_check(err, "\0", "\0");
        MarkBufferDirty(buf);
        IvfflatCommitBuffer(buf, state);
    }
}

/*
* Flush PQ table into page during index building
*/
void IvfFlushPQInfo(IvfflatBuildState *buildstate)
{
    Relation index = buildstate->index;
    char* pqTable = buildstate->pqTable;
    float* preComputeTable = buildstate->preComputeTable;
    uint16 pqTableNblk;
    uint32 pqTableSize;
    uint32 pqPrecomputeTableNblk;
    uint64 pqPrecomputeTableSize;

    IvfGetPQInfoFromMetaPage(index, &pqTableNblk, &pqTableSize, &pqPrecomputeTableNblk, &pqPrecomputeTableSize);

    /* Flush pq table */
    IvfFlushPQInfoInternal(index, pqTable, IVF_PQTABLE_START_BLKNO, pqTableNblk, pqTableSize);
    if (buildstate->byResidual && buildstate->params->funcType != IVF_PQ_DIS_IP) {
        /* Flush pq distance table */
        IvfFlushPQInfoInternal(index, (char*)preComputeTable,
                               IVF_PQTABLE_START_BLKNO + pqTableNblk, pqPrecomputeTableNblk, pqPrecomputeTableSize);
    }
}
