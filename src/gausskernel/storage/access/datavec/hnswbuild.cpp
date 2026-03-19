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
 * hnswbuild.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/hnswbuild.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include <cmath>

#include "access/tableam.h"
#include "access/xact.h"
#include "access/xloginsert.h"
#include "postmaster/bgworker.h"
#include "catalog/index.h"
#include "access/datavec/hnsw.h"
#include "miscadmin.h"
#include "storage/buf/bufmgr.h"
#include "storage/procarray.h"
#include "tcop/tcopprot.h"
#include "utils/datum.h"
#include "utils/memutils.h"
#include "commands/vacuum.h"

#include "pgstat.h"

#define CALLBACK_ITEM_POINTER HeapTuple hup

#define PARALLEL_KEY_HNSW_SHARED UINT64CONST(0xA000000000000001)
#define PARALLEL_KEY_HNSW_AREA UINT64CONST(0xA000000000000002)
#define PARALLEL_KEY_QUERY_TEXT UINT64CONST(0xA000000000000003)
#define PROGRESS_CREATEIDX_TUPLES_DONE 0

#define GENERATIONCHUNK_RAWSIZE (SIZEOF_SIZE_T + SIZEOF_VOID_P * 2)

/*
 * Add sample
 */
static void AddSample(Datum *values, HnswBuildState *buildstate)
{
    VectorArray samples = buildstate->samples;
    int targsamples = samples->maxlen;

    /* Detoast once for all calls */
    Datum value = PointerGetDatum(PG_DETOAST_DATUM(values[0]));

    if (buildstate->kmeansnormprocinfo != NULL) {
        if (!HnswCheckNorm(buildstate->kmeansnormprocinfo, buildstate->collation, value)) {
            return;
        }

        value = HnswNormValue(buildstate->typeInfo, buildstate->collation, value);
    }

    if (samples->length < targsamples) {
        VectorArraySet(samples, samples->length, DatumGetPointer(value));
        samples->length++;
    } else {
        if (buildstate->rowstoskip < 0) {
            buildstate->rowstoskip = anl_get_next_S(samples->length, targsamples, &buildstate->rstate);
        }

        if (buildstate->rowstoskip <= 0) {
            int k = (int) (targsamples * anl_random_fract());
            Assert(k >= 0 && k < targsamples);
            VectorArraySet(samples, k, DatumGetPointer(value));
        }

        buildstate->rowstoskip -= 1;
    }
}

/*
 * Sum samples and get the minmax of each dim
 */
static void SumAndMixMaxSamples(Datum *values, HnswBuildState *buildstate)
{
    VectorArray samples = buildstate->samples;
    int targsamples = samples->maxlen;

    /* Detoast once for all calls */
    Datum value = PointerGetDatum(PG_DETOAST_DATUM(values[0]));

    if (buildstate->normprocinfo != NULL) {
        if (!HnswCheckNorm(buildstate->normprocinfo, buildstate->collation, value)) {
            return;
        }

        value = HnswNormValue(buildstate->typeInfo, buildstate->collation, value);
    }

    Vector *vec;
    if (IS_HALFVEC(buildstate->procinfo->fn_oid)) {
        vec = Halfvec2Vector(value);
    } else {
        vec = (Vector *)value;
    }
    float *centroid = buildstate->centroid;
    for (int i = 0; i < vec->dim; i++) {
        centroid[i] += vec->x[i];
    }
    if (buildstate->rbqConfig->reType == SQ8) {
        ScalarQuantizer *sq = buildstate->rbqConfig->sq;
        float *vmin = sq->trained;
        float *vmax = vmin + sq->dim;
        for (int i = 0; i < sq->dim; i++) {
            vmin[i] = vec->x[i] < vmin[i] ? vec->x[i] : vmin[i];
            vmax[i] = vec->x[i] > vmax[i] ? vec->x[i] : vmax[i];
        }
    }
}

/*
 * Callback for sampling
 */
static void SampleCallback(Relation index, CALLBACK_ITEM_POINTER, Datum *values,
                           const bool *isnull, bool tupleIsAlive, void *state)
{
    HnswBuildState *buildstate = (HnswBuildState *) state;
    MemoryContext oldCtx;

    /* Skip nulls */
    if (isnull[0]) {
        return;
    }

    /* Use memory context since detoast can allocate */
    oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);

    if (buildstate->enableRabitQ) {
        /* Sum samples and get the minmax of each dim */
        SumAndMixMaxSamples(values, buildstate);
    } else {
        /* Add sample */
        AddSample(values, buildstate);
    }

    /* Reset memory context */
    MemoryContextSwitchTo(oldCtx);
    MemoryContextReset(buildstate->tmpCtx);
}

/*
 * Sample rows with same logic as ANALYZE
 */
static void SampleRows(HnswBuildState *buildstate)
{
    int targsamples = buildstate->samples->maxlen;
    BlockNumber totalblocks = RelationGetNumberOfBlocks(buildstate->heap);

    buildstate->rowstoskip = -1;

    BlockSampler_Init(&buildstate->bs, totalblocks, targsamples);

    buildstate->rstate = anl_init_selection_state(targsamples);
    while (BlockSampler_HasMore(&buildstate->bs)) {
        BlockNumber targblock = BlockSampler_Next(&buildstate->bs);

        tableam_index_build_scan(buildstate->heap, buildstate->index, buildstate->indexInfo,
                                 false, SampleCallback, (void *) buildstate, NULL, targblock, 1);
    }
}

PQParams *InitPQParamsInMemory(HnswBuildState *buildstate)
{
    PQParams *params = (PQParams*)palloc(sizeof(PQParams));
    params->pqM = buildstate->pqM;
    params->pqKsub = buildstate->pqKsub;
    params->funcType = GetFunctionType(buildstate->procinfo, buildstate->normprocinfo);
    params->dim = buildstate->dimensions;
    Size subItemsize = buildstate->typeInfo->itemSize(buildstate->dimensions / buildstate->pqM);
    params->subItemSize = MAXALIGN(subItemsize);
    params->pqTable = buildstate->pqTable;
    return params;
}

static int ComputeHnswPQ(HnswBuildState *buildstate)
{
    MemoryContext pqCtx = AllocSetContextCreate(CurrentMemoryContext,
                                                "Hnsw PQ temporary context",
                                                ALLOCSET_DEFAULT_SIZES);
    MemoryContext oldCtx = MemoryContextSwitchTo(pqCtx);

    int res = ComputePQTable(buildstate->samples, buildstate->params);
    MemoryContextSwitchTo(oldCtx);
    MemoryContextDelete(pqCtx);
    return res;
}

/*
 * Build PQ table
 */
static void BuildPQtable(HnswBuildState *buildstate)
{
    int numSamples;
    Relation index = buildstate->index;

    /* Skip samples for unlogged table */
    if (buildstate->heap == NULL) {
        numSamples = 1;
    } else {
        double num;
        EstimateRows(buildstate->heap, &num);
        numSamples = (int)num;
    }
    PG_TRY();
    {
        /* Sample rows */
        ereport(LOG, (errmsg("HNSWPQ start sample rows.")));
        buildstate->samples = VectorArrayInit(numSamples, buildstate->dimensions,
                                              buildstate->typeInfo->itemSize(buildstate->dimensions));
    }
    PG_CATCH();
    {
        ereport(ERROR, (errmsg("memory alloc failed during PQtable sampling, suggest using hnsw without PQ.")));
        PG_RE_THROW();
    }
    PG_END_TRY();
    if (buildstate->heap != NULL) {
        SampleRows(buildstate);
        if (buildstate->samples->length < buildstate->pqKsub) {
            ereport(NOTICE,
                    (errmsg("hnsw PQ table created with little data"),
                            errdetail("This will cause low recall."),
                            errhint("Drop the index until the table has more data.")));
        }
    }
    ereport(LOG, (errmsg("HNSWPQ start to train codebook.")));
    int result = ComputeHnswPQ(buildstate);
    VectorArrayFree(buildstate->samples);
    if (result == -1) {
        ereport(ERROR, (errmsg("HNSWPQ training codebook is failed.")));
    } else {
        ereport(LOG, (errmsg("HNSWPQ finish to train codebook.")));
    }
}


/*
 * Create the metapage
 */
static void CreateMetaPage(HnswBuildState *buildstate)
{
    Relation index = buildstate->index;
    ForkNumber forkNum = buildstate->forkNum;
    Buffer buf;
    Page page;
    HnswMetaPage metap;

    buf = HnswNewBuffer(index, forkNum);
    page = BufferGetPage(buf);
    HnswInitPage(buf, page);

    if (buildstate->isUStore) {
        HnswPageGetOpaque(page)->pageType = HNSW_USTORE_PAGE_TYPE;
    }

    /* Set metapage data */
    metap = HnswPageGetMeta(page);
    metap->magicNumber = HNSW_MAGIC_NUMBER;
    metap->version = HNSW_VERSION;
    metap->dimensions = buildstate->dimensions;
    metap->m = buildstate->m;
    metap->efConstruction = buildstate->efConstruction;
    metap->entryBlkno = InvalidBlockNumber;
    metap->entryOffno = InvalidOffsetNumber;
    metap->entryLevel = -1;
    metap->insertPage = InvalidBlockNumber;

    /* set PQ info */
    metap->enablePQ = buildstate->enablePQ;
    metap->pqM = buildstate->pqM;
    metap->pqKsub = buildstate->pqKsub;
    metap->pqcodeSize = buildstate->pqcodeSize;
    metap->pqDisTableSize = 0;
    metap->pqDisTableNblk = 0;
    if (buildstate->enablePQ) {
        metap->pqTableSize = (uint32)buildstate->pqTableSize;
        metap->pqTableNblk = (uint16)(
            (metap->pqTableSize + CHUNK_STORAGE_SIZE - 1) / CHUNK_STORAGE_SIZE);
        if (buildstate->pqMode == HNSW_PQMODE_SDC) {
            uint32 disTableLen = buildstate->pqM * buildstate->pqKsub * buildstate->pqKsub;
            metap->pqDisTableSize = (uint32)disTableLen * sizeof(float);
            metap->pqDisTableNblk = (uint16)(
                (metap->pqDisTableSize + CHUNK_STORAGE_SIZE - 1) / CHUNK_STORAGE_SIZE);
        }
    } else {
        metap->pqTableSize = 0;
        metap->pqTableNblk = 0;
    }

    /* set RabitQ info */
    metap->enableRabitQ = buildstate->enableRabitQ;
    metap->rbqDelayState = buildstate->rbqDelayState;
    metap->rbqInsertRows = 0;
    if (buildstate->enableRabitQ) {
        metap->useFHT = buildstate->rbqConfig->FHT;
        metap->reOffset = buildstate->rbqConfig->reOffset;
        int dim = buildstate->dimensions;
        Size matrixSize;
        if (buildstate->rbqConfig->FHT) {
            int outputDim = FhtOutputDim(dim);
            matrixSize = FhtSerializeSize(outputDim);
        } else {
            matrixSize = dim * dim * sizeof(float);
        }
        metap->matrixSize = matrixSize;
        metap->matrixNblk = (uint16)(
                (matrixSize + CHUNK_STORAGE_SIZE - 1) / CHUNK_STORAGE_SIZE);

        metap->reType = buildstate->rbqConfig->reType;
        Size otherSize = dim * sizeof(float);
        if (metap->reType == SQ8) {
            otherSize *= 3;
        }
        metap->otherSize = otherSize;
        metap->otherNblk = (uint16)(
                (otherSize + CHUNK_STORAGE_SIZE - 1) / CHUNK_STORAGE_SIZE);
    } else {
        metap->useFHT = false;
        metap->reOffset = 0;
        metap->matrixNblk = 0;
        metap->matrixSize = 0;
        metap->reType = NotRefine;
        metap->otherSize = 0;
        metap->otherNblk = 0;
    }

    if (buildstate->enableLsg) {
        metap->enableLsg = buildstate->enableLsg;
        metap->lsgSampleSize = buildstate->lsgSampleSize;
        metap->lsgCodeBookSize = metap->lsgSampleSize * buildstate->lsgDim * sizeof(float);
        metap->lsgSampleNblk = (metap->lsgCodeBookSize + LSGSAMPLE_STORAGE_SIZE - 1) / LSGSAMPLE_STORAGE_SIZE;
    } else {
        metap->lsgSampleSize = 0;
    }

    ((PageHeader)page)->pd_lower = ((char *)metap + sizeof(HnswMetaPageData)) - (char *)page;

    MarkBufferDirty(buf);
    UnlockReleaseBuffer(buf);
}

/*
 * Create PQ-related pages
 */
static void CreatePQPages(HnswBuildState *buildstate)
{
    uint16 nblks;
    Relation index = buildstate->index;
    ForkNumber forkNum = buildstate->forkNum;
    Buffer buf;
    Page page;
    uint16 pqTableNblk;
    uint16 pqDisTableNblk;

    HnswGetPQInfoFromMetaPage(index, &pqTableNblk, NULL, &pqDisTableNblk, NULL);

    /* create pq table page */
    for (uint16 i = 0; i < pqTableNblk; i++) {
        buf = HnswNewBuffer(index, forkNum);
        page = BufferGetPage(buf);
        HnswInitPage(buf, page);
        MarkBufferDirty(buf);
        UnlockReleaseBuffer(buf);
    }

    /* create pq distance table page */
    for (uint16 i = 0; i < pqDisTableNblk; i++) {
        buf = HnswNewBuffer(index, forkNum);
        page = BufferGetPage(buf);
        HnswInitPage(buf, page);
        MarkBufferDirty(buf);
        UnlockReleaseBuffer(buf);
    }
}

/*
 * Create RabitQ-matrix pages
 */
static void CreateRbqMatrixPages(HnswBuildState *buildstate)
{
    uint16 nblks;
    Relation index = buildstate->index;
    ForkNumber forkNum = buildstate->forkNum;
    Buffer buf;
    Page page;
    uint16 matrixNblk;
    uint32 matrixSize;
    void *matrix;

    HnswGetRbqInfoFromMetaPage(index, NULL, NULL, NULL, NULL, &matrixNblk,
                               &matrixSize, NULL, NULL, NULL, NULL);

    /* create matrix page */
    for (uint16 i = 0; i < matrixNblk; i++) {
        buf = HnswNewBuffer(index, forkNum);
        page = BufferGetPage(buf);
        HnswInitPage(buf, page);
        MarkBufferDirty(buf);
        UnlockReleaseBuffer(buf);
    }

    VectorTransform* vtrans = buildstate->rbqConfig->vtrans;
    if (vtrans->type == RANDOM_ORTHOGONAL) {
        matrix = RomGetMatrix(vtrans);
    } else {
        matrix = FhtGetMatrix(vtrans);
    }

    FlushChunkInfoInternal(index, (char *)matrix, HNSW_CHUNK_START_BLKNO, matrixNblk, matrixSize);
    if (vtrans->type == FAST_HTRANSFORM) {
        pfree(matrix);
    }
}

/*
 * Create RabitQ-other pages, including centroid and min+diff if refine_type is SQ8
 */
static void CreateRbqOtherPages(HnswBuildState *buildstate)
{
    uint16 nblks;
    Relation index = buildstate->index;
    ForkNumber forkNum = buildstate->forkNum;
    RabitQConfig *rbqConfig = buildstate->rbqConfig;
    Buffer buf;
    Page page;
    uint16 matrixNblk;
    uint16 otherNblk;
    uint32 otherSize;
    uint32 oneSize = buildstate->dimensions * sizeof(float);
    void *other;
    errno_t rc;

    HnswGetRbqInfoFromMetaPage(index, NULL, NULL, NULL, NULL, &matrixNblk, NULL,
                               &otherNblk, &otherSize, NULL, NULL);

    /* create ohter page */
    for (uint16 i = 0; i < otherNblk; i++) {
        buf = HnswNewBuffer(index, forkNum);
        page = BufferGetPage(buf);
        HnswInitPage(buf, page);
        MarkBufferDirty(buf);
        UnlockReleaseBuffer(buf);
    }

    if (rbqConfig->reType == SQ8) {
        other = (void *)palloc(oneSize * 3);
        rc = memcpy_s((char*)other, oneSize, buildstate->centroid, oneSize);
        securec_check(rc, "\0", "\0");
        uint32 sq8Size = oneSize + oneSize;
        rc = memcpy_s(((char*)other + oneSize), sq8Size, rbqConfig->sq->trained, sq8Size);
        securec_check(rc, "\0", "\0");
    } else {
        other = (void *)palloc(oneSize);
        rc = memcpy_s((char*)other, oneSize, buildstate->centroid, oneSize);
        securec_check(rc, "\0", "\0");
    }

    FlushChunkInfoInternal(index, (char *)other, HNSW_CHUNK_START_BLKNO + matrixNblk, otherNblk, otherSize);
}

/*
 *  Create LSG-related pages
 */
static void CreateLsgSamplePages(HnswBuildState *buildstate)
{
    Buffer buf;
    Page page;
    Relation index = buildstate->index;
    ForkNumber forkNum = buildstate->forkNum;
    uint16 nBlks;
    HnswGetLsgInfoFromMetaPage(buildstate->index, NULL, &nBlks, NULL, NULL, NULL);

    /* create lsg codebook page */
    for (int i = 0; i < nBlks; i++) {
        buf = HnswNewBuffer(index, forkNum);
        page = BufferGetPage(buf);
        HnswInitPage(buf, page);
        MarkBufferDirty(buf);
        UnlockReleaseBuffer(buf);
    }
}

/*
 * Add a new page
 */
static void HnswBuildAppendPage(Relation index, Buffer *buf, Page *page, ForkNumber forkNum)
{
    /* Add a new page */
    Buffer newbuf = HnswNewBuffer(index, forkNum);

    /* Update previous page */
    HnswPageGetOpaque(*page)->nextblkno = BufferGetBlockNumber(newbuf);

    /* Commit */
    MarkBufferDirty(*buf);
    UnlockReleaseBuffer(*buf);

    /* Can take a while, so ensure we can interrupt */
    /* Needs to be called when no buffer locks are held */
    LockBuffer(newbuf, BUFFER_LOCK_UNLOCK);
    CHECK_FOR_INTERRUPTS();
    LockBuffer(newbuf, BUFFER_LOCK_EXCLUSIVE);

    /* Prepare new page */
    *buf = newbuf;
    *page = BufferGetPage(*buf);
    HnswInitPage(*buf, *page);
}

/*
 * Create graph pages
 */
static void CreateGraphPages(HnswBuildState *buildstate)
{
    Relation index = buildstate->index;
    ForkNumber forkNum = buildstate->forkNum;
    Size maxSize;
    HnswElementTuple etup;
    HnswNeighborTuple ntup;
    BlockNumber insertPage;
    HnswElement entryPoint;
    Buffer buf;
    Page page;
    HnswElementPtr iter = buildstate->graph->head;
    char *base = buildstate->hnswarea;
    IndexTransInfo *idxXid;
    Size pqcodesSize = buildstate->pqcodeSize;
    bool refineSQ8;
    Size rbqcodesSize = 0;
    Size rbqEtupSize = 0;

    if (buildstate->enableRabitQ) {
        refineSQ8 = buildstate->rbqConfig->reType == SQ8;
        rbqcodesSize = rbqCodeSize(buildstate->dimensions, refineSQ8);
        rbqEtupSize = MAXALIGN(offsetof(HnswElementTupleData, data) + rbqcodesSize);
    }

    /* Calculate sizes */
    maxSize = HNSW_MAX_SIZE;

    /* Allocate once */
    etup = (HnswElementTuple)palloc0(HNSW_TUPLE_ALLOC_SIZE);
    ntup = (HnswNeighborTuple)palloc0(HNSW_TUPLE_ALLOC_SIZE);

    /* Prepare first page */
    buf = HnswNewBuffer(index, forkNum);
    page = BufferGetPage(buf);
    HnswInitPage(buf, page);

    /* Check vector and pqcode can be on the same page */
    if (!HnswPtrIsNull(base, buildstate->graph->head)) {
        HnswElement head = (HnswElement)HnswPtrAccess(base, buildstate->graph->head);
        Size elementSize;
        if (buildstate->enableRabitQ) {
            elementSize = rbqEtupSize;
            if (PageGetFreeSpace(page) < elementSize) {
                ereport(ERROR, (errmsg("One rabitqcode must be stored within a single page")));
            }
        } else {
            elementSize = HNSW_ELEMENT_TUPLE_SIZE(VARSIZE_ANY((Pointer)HnswPtrAccess(base, head->value)));
            if (PageGetFreeSpace(page) < elementSize + MAXALIGN(pqcodesSize)) {
                int maxPQcodeSize = ((PageGetFreeSpace(page) - elementSize) / 8) * 8;
                ereport(ERROR, (errmsg("vector and pqcode must be on the same page, max pq_m is %d", maxPQcodeSize)));
            }
        }
    }

    if (buildstate->isUStore) {
        HnswPageGetOpaque(page)->pageType = HNSW_USTORE_PAGE_TYPE;
    }

    while (!HnswPtrIsNull(base, iter)) {
        HnswElement element = (HnswElement)HnswPtrAccess(base, iter);
        Size etupSize;
        Size ntupSize;
        Size combinedSize;
        Pointer valuePtr = (Pointer)HnswPtrAccess(base, element->value);

        /* Update iterator */
        iter = element->next;

        /* Zero memory for each element */
        MemSet(etup, 0, HNSW_TUPLE_ALLOC_SIZE);

        /* Calculate sizes */
        etupSize = buildstate->enableRabitQ ? rbqEtupSize
                    : HNSW_ELEMENT_TUPLE_SIZE(VARSIZE_ANY(valuePtr));
        ntupSize = HNSW_NEIGHBOR_TUPLE_SIZE(element->level, buildstate->m);
        combinedSize = etupSize + MAXALIGN(pqcodesSize) + ntupSize + sizeof(ItemIdData);

        if (buildstate->isUStore) {
            combinedSize += sizeof(IndexTransInfo);
        }

        /* Initial size check */
        if (etupSize > HNSW_TUPLE_ALLOC_SIZE) {
            elog(ERROR, "index tuple too large");
        }

        HnswSetElementTuple(base, etup, element, rbqcodesSize);

        /* Keep element and neighbors on the same page if possible */
        if (PageGetFreeSpace(page) < etupSize + MAXALIGN(pqcodesSize) ||
            (combinedSize <= maxSize && PageGetFreeSpace(page) < combinedSize)) {
            HnswBuildAppendPage(index, &buf, &page, forkNum);
            if (buildstate->isUStore) {
                HnswPageGetOpaque(page)->pageType = HNSW_USTORE_PAGE_TYPE;
            }
        }

        /* Calculate offsets */
        element->blkno = BufferGetBlockNumber(buf);
        element->offno = OffsetNumberNext(PageGetMaxOffsetNumber(page));
        if (combinedSize <= maxSize) {
            element->neighborPage = element->blkno;
            element->neighborOffno = OffsetNumberNext(element->offno);
        } else {
            element->neighborPage = element->blkno + 1;
            element->neighborOffno = FirstOffsetNumber;
        }

        ItemPointerSet(&etup->neighbortid, element->neighborPage, element->neighborOffno);

        if (buildstate->enablePQ) {
            ((PageHeader)page)->pd_upper -= MAXALIGN(pqcodesSize);
            Pointer codePtr = (Pointer) HnswPtrAccess(base, element->pqcodes);
            errno_t rc = memcpy_s(
                ((char*)page) + ((PageHeader)page)->pd_upper, pqcodesSize, codePtr, pqcodesSize);
            securec_check_c(rc, "\0", "\0");
        }

        if (buildstate->isUStore) {
            ((PageHeader)page)->pd_upper -= sizeof(IndexTransInfo);
            idxXid = (IndexTransInfo *)(((char *)page) + ((PageHeader)page)->pd_upper);
            idxXid->xmin = FrozenTransactionId;
            idxXid->xmax = InvalidTransactionId;
        }

        /* Add element */
        if (PageAddItem(page, (Item)etup, etupSize, InvalidOffsetNumber, false, false) != element->offno) {
            elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));
        }

        /* Add new page if needed */
        if (PageGetFreeSpace(page) < ntupSize) {
            HnswBuildAppendPage(index, &buf, &page, forkNum);
            if (buildstate->isUStore) {
                HnswPageGetOpaque(page)->pageType = HNSW_USTORE_PAGE_TYPE;
            }
        }
        /* Add placeholder for neighbors */
        if (PageAddItem(page, (Item)ntup, ntupSize, InvalidOffsetNumber, false, false) != element->neighborOffno) {
            elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));
        }
    }

    insertPage = BufferGetBlockNumber(buf);

    /* Commit */
    MarkBufferDirty(buf);
    UnlockReleaseBuffer(buf);

    entryPoint = (HnswElement)HnswPtrAccess(base, buildstate->graph->entryPoint);
    HnswUpdateMetaPage(index, HNSW_UPDATE_ENTRY_ALWAYS, entryPoint, insertPage, forkNum, true);

    pfree(etup);
    pfree(ntup);
}

/*
 * Write neighbor tuples
 */
static void WriteNeighborTuples(HnswBuildState *buildstate)
{
    Relation index = buildstate->index;
    ForkNumber forkNum = buildstate->forkNum;
    int m = buildstate->m;
    HnswElementPtr iter = buildstate->graph->head;
    char *base = buildstate->hnswarea;
    HnswNeighborTuple ntup;

    /* Allocate once */
    ntup = (HnswNeighborTuple)palloc0(HNSW_TUPLE_ALLOC_SIZE);

    while (!HnswPtrIsNull(base, iter)) {
        HnswElement element = (HnswElement)HnswPtrAccess(base, iter);
        Buffer buf;
        Page page;
        Size ntupSize = HNSW_NEIGHBOR_TUPLE_SIZE(element->level, m);

        /* Update iterator */
        iter = element->next;

        /* Zero memory for each element */
        MemSet(ntup, 0, HNSW_TUPLE_ALLOC_SIZE);

        /* Can take a while, so ensure we can interrupt */
        /* Needs to be called when no buffer locks are held */
        CHECK_FOR_INTERRUPTS();

        buf = ReadBufferExtended(index, forkNum, element->neighborPage, RBM_NORMAL, NULL);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        page = BufferGetPage(buf);

        HnswSetNeighborTuple(base, ntup, element, m);

        if (!page_index_tuple_overwrite(page, element->neighborOffno, (Item)ntup, ntupSize))
            elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));

        /* Commit */
        MarkBufferDirty(buf);
        UnlockReleaseBuffer(buf);
    }

    pfree(ntup);
}

/*
 * Flush pages
 */
static void FlushPages(HnswBuildState *buildstate)
{
#ifdef HNSW_MEMORY
    elog(INFO, "memory: %zu MB", buildstate->graph->memoryUsed / (1024 * 1024));
#endif
    BlockNumber numPages = RelationGetNumberOfBlocks(buildstate->index);
    /* 
     * When numPages != 0, it means there was no data in the table when hnsw rabitq
     * called "create index", so a Metapage is created to record basic information.
     * After inserting sufficient data, the index is built in a delayed manner here,
     * and there is no need to recreate the Metapage.
     */
    if (numPages == 0) {
        CreateMetaPage(buildstate);
    }
    if (buildstate->enablePQ) {
        CreatePQPages(buildstate);
        /* Save PQ table and distance table */
        FlushPQInfo(buildstate);
    }
    if (buildstate->enableRabitQ) {
        /* Create pages and flush matrix */
        CreateRbqMatrixPages(buildstate);
        /* Create pages and flush centroid (min+diff if refine type is SQ8) */
        CreateRbqOtherPages(buildstate);
    }
    if (buildstate->enableLsg) {
        CreateLsgSamplePages(buildstate);
        FlushLsgSamples(buildstate);
    }
    CreateGraphPages(buildstate);
    WriteNeighborTuples(buildstate);

    buildstate->graph->flushed = true;
    MemoryContextReset(buildstate->graphCtx);
}

/*
 * Add a heap TID to an existing element
 */
static bool AddDuplicateInMemory(HnswElement element, HnswElement dup)
{
    LWLockAcquire(&dup->lock, LW_EXCLUSIVE);

    if (dup->heaptidsLength == HNSW_HEAPTIDS) {
        LWLockRelease(&dup->lock);
        return false;
    }

    HnswAddHeapTid(dup, &element->heaptids[0]);

    LWLockRelease(&dup->lock);

    return true;
}

/*
 * Find duplicate element
 */
static bool FindDuplicateInMemory(char *base, HnswElement element)
{
    HnswNeighborArray *neighbors = HnswGetNeighbors(base, element, 0);
    Datum value = HnswGetValue(base, element);

    for (int i = 0; i < neighbors->length; i++) {
        HnswCandidate *neighbor = &neighbors->items[i];
        HnswElement neighborElement = (HnswElement)HnswPtrAccess(base, neighbor->element);
        Datum neighborValue = HnswGetValue(base, neighborElement);
        /* Exit early since ordered by distance */
        if (!datumIsEqual(value, neighborValue, false, -1))
            return false;

        /* Check for space */
        if (AddDuplicateInMemory(element, neighborElement))
            return true;
    }

    return false;
}

/*
 * Add to element list
 */
static void AddElementInMemory(char *base, HnswGraph *graph, HnswElement element)
{
    SpinLockAcquire(&graph->lock);
    element->next = graph->head;
    HnswPtrStore(base, graph->head, element);
    SpinLockRelease(&graph->lock);
}

/*
 * Update neighbors
 */
static void UpdateNeighborsInMemory(HnswBuildState *buildstate, HnswElement element)
{
    char *base = buildstate->hnswarea;
    FmgrInfo *procinfo = buildstate->procinfo;
    Oid collation = buildstate->collation;
    int m = buildstate->m;

    for (int lc = element->level; lc >= 0; lc--) {
        int lm = HnswGetLayerM(m, lc);
        HnswNeighborArray *neighbors = HnswGetNeighbors(base, element, lc);

        for (int i = 0; i < neighbors->length; i++) {
            HnswCandidate *hc = &neighbors->items[i];
            HnswElement neighborElement = (HnswElement)HnswPtrAccess(base, hc->element);

            if (neighborElement == NULL) {
                continue;
            }

            /* Use element for lock instead of hc since hc can be replaced */
            LWLockAcquire(&neighborElement->lock, LW_EXCLUSIVE);
            HnswUpdateConnection(base, element, hc, lm, lc, NULL, NULL, procinfo, collation,
                                 buildstate->enableRabitQ, NULL, buildstate->enableLsg);
            LWLockRelease(&neighborElement->lock);
        }
    }
}

/*
 * Update graph in memory
 */
static void UpdateGraphInMemory(HnswElement element, HnswElement entryPoint, HnswBuildState *buildstate)
{
    HnswGraph *graph = buildstate->graph;
    char *base = buildstate->hnswarea;

    /* Look for duplicate */
    if (FindDuplicateInMemory(base, element)) {
        return;
    }

    /* Add element */
    AddElementInMemory(base, graph, element);

    /* Update neighbors */
    UpdateNeighborsInMemory(buildstate, element);

    /* Update entry point if needed (already have lock) */
    if (entryPoint == NULL || element->level > entryPoint->level) {
        HnswPtrStore(base, graph->entryPoint, element);
    }
}

/*
 * Insert tuple in memory
 */
static void InsertTupleInMemory(HnswBuildState *buildstate, HnswElement element, Vector *transformedVec)
{
    FmgrInfo *procinfo = buildstate->procinfo;
    Oid collation = buildstate->collation;
    HnswGraph *graph = buildstate->graph;
    HnswElement entryPoint;
    LWLock *entryLock = &graph->entryLock;
    LWLock *entryWaitLock = &graph->entryWaitLock;
    int efConstruction = buildstate->efConstruction;
    int m = buildstate->m;
    char *base = buildstate->hnswarea;
    if (buildstate->enableRabitQ) {
        int funcType = GetFunctionType(procinfo, buildstate->normprocinfo);
        HnswComputeVectorRBQCode(element, transformedVec, buildstate->centroid, funcType, base);
    }

    /* Wait if another process needs exclusive lock on entry lock */
    LWLockAcquire(entryWaitLock, LW_EXCLUSIVE);
    LWLockRelease(entryWaitLock);

    /* Get entry point */
    LWLockAcquire(entryLock, LW_SHARED);
    entryPoint = (HnswElement)HnswPtrAccess(base, graph->entryPoint);
    /* Prevent concurrent inserts when likely updating entry point */
    if (entryPoint == NULL || element->level > entryPoint->level) {
        /* Release shared lock */
        LWLockRelease(entryLock);

        /* Tell other processes to wait and get exclusive lock */
        LWLockAcquire(entryWaitLock, LW_EXCLUSIVE);
        LWLockAcquire(entryLock, LW_EXCLUSIVE);
        LWLockRelease(entryWaitLock);

        /* Get latest entry point after lock is acquired */
        entryPoint = (HnswElement)HnswPtrAccess(base, graph->entryPoint);
    }

    /* Find neighbors for element */
    HnswFindElementNeighbors(base, element, entryPoint, NULL, procinfo, collation, m, efConstruction, false,
                             buildstate->enablePQ, buildstate->params, buildstate->enableRabitQ, NULL,
                             buildstate->enableLsg);

    /* Update graph in memory */
    UpdateGraphInMemory(element, entryPoint, buildstate);

    /* Release entry lock */
    LWLockRelease(entryLock);
}

/*
 * Insert tuple
 */
static bool InsertTuple(Relation index, Datum *values, const bool *isnull, ItemPointer heaptid,
                        HnswBuildState *buildstate)
{
    const HnswTypeInfo *typeInfo = buildstate->typeInfo;
    HnswGraph *graph = buildstate->graph;
    HnswElement element;
    HnswAllocator *allocator = &buildstate->allocator;
    Size valueSize;
    Pointer valuePtr;
    Pointer codePtr = NULL;
    Pointer rbqPtr = NULL;
    Vector *transValue = NULL;
    LWLock *flushLock = &graph->flushLock;
    char *base = buildstate->hnswarea;

    /* Detoast once for all calls */
    Datum value = PointerGetDatum(PG_DETOAST_DATUM(values[0]));

    /* Check value */
    if (typeInfo->checkValue != NULL) {
        typeInfo->checkValue(DatumGetPointer(value));
    }

    /* Normalize if needed */
    if (buildstate->normprocinfo != NULL) {
        if (!HnswCheckNorm(buildstate->normprocinfo, buildstate->collation, value)) {
            return false;
        }

        value = HnswNormValue(typeInfo, buildstate->collation, value);
    }

    /* Ensure graph not flushed when inserting */
    LWLockAcquire(flushLock, LW_SHARED);

    /* Are we in the on-disk phase? */
    if (graph->flushed) {
        LWLockRelease(flushLock);

        return HnswInsertTupleOnDisk(index, value, isnull, heaptid, true, buildstate->heap);
    }

    /*
     * In a parallel build, the HnswElement is allocated from the shared
     * memory area, so we need to coordinate with other processes.
     */
    LWLockAcquire(&graph->allocatorLock, LW_EXCLUSIVE);

    /*
     * Check that we have enough memory available for the new element now that
     * we have the allocator lock, and flush pages if needed.
     */
    if (graph->memoryUsed >= graph->memoryTotal) {
        LWLockRelease(&graph->allocatorLock);

        LWLockRelease(flushLock);
        LWLockAcquire(flushLock, LW_EXCLUSIVE);

        if (!graph->flushed) {
            ereport(NOTICE, (errmsg("hnsw graph no longer fits into maintenance_work_mem after " INT64_FORMAT " tuples",
                                    (int64)graph->indtuples),
                             errdetail("Building will take significantly more time."),
                             errhint("Increase maintenance_work_mem to speed up builds.")));

            FlushPages(buildstate);
        }

        LWLockRelease(flushLock);

        return HnswInsertTupleOnDisk(index, value, isnull, heaptid, true, buildstate->heap);
    }

    if (buildstate->enableRabitQ) {
        Datum vecVal = value;
        if (IS_HALFVEC(buildstate->procinfo->fn_oid)) {
            vecVal = (Datum)Halfvec2Vector(value);
        }
        RabitQConfig *rbqConfig = buildstate->rbqConfig;
        if (rbqConfig->reType == SQ8) {
            /* Calculate origin vector's SQ8 */
            rbqPtr = (Pointer)HnswAlloc(allocator, rbqCodeSize(buildstate->dimensions, true));
            ScalarQuantizer *sq = rbqConfig->sq;
            int dim = sq->dim;
            VectorEncodeSQ(dim, sq->trained, sq->trained + dim, ((Vector *)DatumGetPointer(vecVal))->x,
                                getRefineCode(rbqPtr, rbqConfig->reOffset));
        } else {
            rbqPtr = (Pointer)HnswAlloc(allocator, rbqCodeSize(buildstate->dimensions, false));
        }
        /* Transform vector in rabitq */
        VectorTransform* vtrans = rbqConfig->vtrans;
        transValue = InitVector(buildstate->dimensions);
        if (vtrans->type == RANDOM_ORTHOGONAL) {
            RomTransform(vtrans, ((Vector *)DatumGetPointer(vecVal))->x, transValue->x);
        } else {
            FhtTransform(vtrans, ((Vector *)DatumGetPointer(vecVal))->x, transValue->x);
        }

        if (IS_HALFVEC(buildstate->procinfo->fn_oid)) {
            pfree((Vector *)vecVal);
        }
    }

    /* Get datum size */
    valueSize = VARSIZE_ANY(DatumGetPointer(value));

    /* Ok, we can proceed to allocate the element */
    element = HnswInitElement(base, heaptid, buildstate->m, buildstate->ml, buildstate->maxLevel, allocator);
    valuePtr = (Pointer)HnswAlloc(allocator, valueSize);
    if (buildstate->enablePQ) {
        Size codesize = buildstate->pqM * sizeof(uint8);
        codePtr = (Pointer)HnswAlloc(allocator, codesize);
    }

    /*
     * We have now allocated the space needed for the element, so we don't
     * need the allocator lock anymore. Release it and initialize the rest of
     * the element.
     */
    LWLockRelease(&graph->allocatorLock);

    /* Copy the datum */
    errno_t rc = memcpy_s(valuePtr, valueSize, DatumGetPointer(value), valueSize);
    securec_check(rc, "\0", "\0");
    HnswPtrStore(base, element->value, valuePtr);
    HnswPtrStore(base, element->pqcodes, codePtr);
    HnswPtrStore(base, element->rbqcodes, rbqPtr);

    if (!IS_SPARSEVEC(buildstate->procinfo->fn_oid) && !IS_BITVEC(buildstate->procinfo->fn_oid)) {
        Vector* currentVec = (Vector*)HnswGetValue(base, element);
        if (buildstate->enableLsg) {
            currentVec->isoValue = CalcIsoVal((float *)currentVec->x, buildstate->LocScalingParam);
        } else {
            currentVec->isoValue = Float32ToFloat16(1.0f);
        }
    }
    /* Create a lock for the element */
    LWLockInitialize(&element->lock, hnsw_lock_tranche_id);

    /* Insert tuple */
    InsertTupleInMemory(buildstate, element, transValue);

    if (buildstate->enableRabitQ) {
        pfree(transValue);
    }

    /* Release flush lock */
    LWLockRelease(flushLock);

    return true;
}

/*
 * Callback for table_index_build_scan
 */
static void BuildCallback(Relation index, CALLBACK_ITEM_POINTER, Datum *values, const bool *isnull, bool tupleIsAlive,
                          void *state)
{
    HnswBuildState *buildstate = (HnswBuildState *)state;
    HnswGraph *graph = buildstate->graph;
    MemoryContext oldCtx;

    ItemPointer tid = &hup->t_self;

    /* Skip nulls */
    if (isnull[0]) {
        return;
    }

    /* RabitQ delay build, avoid "insert into select from" sql from inserting repeatedly. */
    if (buildstate->enableRabitQ && buildstate->rbqDelayState == RBQ_BUILD_AFTER_DELAY) {
        buildstate->rbqDelayBuildRows++;
        int64 insertedRows;
        HnswGetRbqInfoFromMetaPage(index, NULL, NULL, NULL, NULL, NULL, NULL,
                                   NULL, NULL, NULL, &insertedRows);
        if (buildstate->rbqDelayBuildRows > insertedRows) {
            return;
        }
    }

    /* Use memory context */
    oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);

    /* Insert tuple */
    if (InsertTuple(index, values, isnull, tid, buildstate)) {
        /* Update progress */
        SpinLockAcquire(&graph->lock);
        UpdateProgress(PROGRESS_CREATEIDX_TUPLES_DONE, ++graph->indtuples);
        SpinLockRelease(&graph->lock);
    }

    /* Reset memory context */
    MemoryContextSwitchTo(oldCtx);
    MemoryContextReset(buildstate->tmpCtx);
}

/*
 * Initialize the graph
 */
static void InitGraph(HnswGraph *graph, char *base, long memoryTotal)
{
    HnswPtrStore(base, graph->head, (HnswElement)NULL);
    HnswPtrStore(base, graph->entryPoint, (HnswElement)NULL);
    graph->memoryUsed = 0;
    graph->memoryTotal = memoryTotal;
    graph->flushed = false;
    graph->indtuples = 0;
    SpinLockInit(&graph->lock);
    LWLockInitialize(&graph->entryLock, hnsw_lock_tranche_id);
    LWLockInitialize(&graph->entryWaitLock, hnsw_lock_tranche_id);
    LWLockInitialize(&graph->allocatorLock, hnsw_lock_tranche_id);
    LWLockInitialize(&graph->flushLock, hnsw_lock_tranche_id);
}

/*
 * Initialize an allocator
 */
static void InitAllocator(HnswAllocator *allocator, void *(*alloc)(Size size, void *state), void *state)
{
    allocator->alloc = alloc;
    allocator->state = state;
}

/*
 * Memory context allocator
 */
static void *HnswMemoryContextAlloc(Size size, void *state)
{
    HnswBuildState *buildstate = (HnswBuildState *)state;
    void *chunk = MemoryContextAlloc(buildstate->graphCtx, size);

    buildstate->graphData.memoryUsed += MAXALIGN(size);

    return chunk;
}

/*
 * Shared memory allocator
 */
static void *HnswSharedMemoryAlloc(Size size, void *state)
{
    HnswBuildState *buildstate = (HnswBuildState *)state;
    void *chunk = buildstate->hnswarea + buildstate->graph->memoryUsed;

    buildstate->graph->memoryUsed += MAXALIGN(size);
    return chunk;
}

/*
 * Initialize the build state
 */
static void InitBuildState(HnswBuildState *buildstate, Relation heap, Relation index, IndexInfo *indexInfo,
                           ForkNumber forkNum, bool parallel)
{
    buildstate->heap = heap;
    buildstate->index = index;
    buildstate->indexInfo = indexInfo;
    buildstate->forkNum = forkNum;
    buildstate->typeInfo = HnswGetTypeInfo(index);

    buildstate->m = HnswGetM(index);
    buildstate->efConstruction = HnswGetEfConstruction(index);
    buildstate->dimensions = TupleDescAttr(index->rd_att, 0)->atttypmod;

    /* Disallow varbit since require fixed dimensions */
    if (TupleDescAttr(index->rd_att, 0)->atttypid == VARBITOID) {
        elog(ERROR, "type not supported for hnsw index");
    }

    /* Require column to have dimensions to be indexed */
    if (buildstate->dimensions < 0) {
        elog(ERROR, "column does not have dimensions");
    }

    if (buildstate->dimensions > buildstate->typeInfo->maxDimensions) {
        elog(ERROR, "column cannot have more than %d dimensions for hnsw index", buildstate->typeInfo->maxDimensions);
    }

    if (buildstate->efConstruction < 2 * buildstate->m) {
        elog(ERROR, "ef_construction must be greater than or equal to 2 * m");
    }

    buildstate->reltuples = 0;
    buildstate->indtuples = 0;

    /* Get support functions */
    buildstate->procinfo = index_getprocinfo(index, 1, HNSW_DISTANCE_PROC);
    buildstate->normprocinfo = HnswOptionalProcInfo(index, HNSW_NORM_PROC);
    buildstate->kmeansnormprocinfo = HnswOptionalProcInfo(index, HNSW_KMEANS_NORMAL_PROC);
    buildstate->collation = index->rd_indcollation[0];

    InitGraph(&buildstate->graphData, NULL, u_sess->attr.attr_memory.maintenance_work_mem * 1024L);
    buildstate->graph = &buildstate->graphData;
    buildstate->ml = HnswGetMl(buildstate->m);
    buildstate->maxLevel = HnswGetMaxLevel(buildstate->m);

    buildstate->graphCtx =
        AllocSetContextCreate(CurrentMemoryContext, "Hnsw build graph context", ALLOCSET_DEFAULT_SIZES);
    buildstate->tmpCtx =
        AllocSetContextCreate(CurrentMemoryContext, "Hnsw build temporary context", ALLOCSET_DEFAULT_SIZES);

    InitAllocator(&buildstate->allocator, &HnswMemoryContextAlloc, buildstate);

    buildstate->hnswleader = NULL;
    buildstate->hnswshared = NULL;
    buildstate->hnswarea = NULL;

    buildstate->enablePQ = HnswGetEnablePQ(index);
    buildstate->enableLsg = HnswGetEnableLsg(index);
    LsgCalculator* LocScalingParam;
    if (buildstate->enablePQ && !buildstate->typeInfo->supportPQ) {
        ereport(ERROR, (errmsg("this data type cannot support hnswpq.")));
    }
    if (buildstate->enablePQ && !g_instance.pq_inited) {
        ereport(ERROR, (errmsg("this instance has not currently loaded the pq dynamic library.")));
    }

    buildstate->pqM = HnswGetPqM(index);
    buildstate->pqKsub = HnswGetPqKsub(index);

    buildstate->enableRabitQ = HnswGetEnableRabitQ(index);
    buildstate->rbqDelayBuildRows = 0;
    buildstate->rbqDelayState = RBQ_BUILD_NORMAL;
    if (buildstate->enablePQ && buildstate->enableRabitQ) {
        ereport(ERROR, (errmsg("hnsw does not support the mixed use of the two quantization methods: PQ and RabitQ.")));
    }
    if (buildstate->enableLsg && (buildstate->enablePQ || buildstate->enableRabitQ)) {
        ereport(ERROR, (errmsg("hnsw does not support the mixed use of the LSG and quantization methods.")));
    }

    if (buildstate->enablePQ) {
        if (buildstate->kmeansnormprocinfo != NULL && buildstate->dimensions == 1) {
            ereport(ERROR, (errmsg("dimensions must be greater than one for this opclass.")));
        }
        if (buildstate->dimensions % buildstate->pqM != 0) {
            ereport(ERROR, (errmsg("dimensions must be divisible by pq_M, please reset pq_M.")));
        }
        Size subItemsize = buildstate->typeInfo->itemSize(buildstate->dimensions / buildstate->pqM);
        subItemsize = MAXALIGN(subItemsize);
        buildstate->pqTableSize = buildstate->pqM * buildstate->pqKsub * subItemsize;
        buildstate->pqTable = parallel ? NULL : (char*)palloc0(buildstate->pqTableSize);
        buildstate->pqcodeSize = buildstate->pqM * sizeof(uint8);
        buildstate->params = InitPQParamsInMemory(buildstate);
    } else {
        buildstate->pqTable = NULL;
        buildstate->pqTableSize = 0;
        buildstate->pqcodeSize = 0;
        buildstate->params = NULL;
    }
    buildstate->pqMode = HNSW_PQMODE_DEFAULT;
    buildstate->pqDistanceTable = NULL;

    if (buildstate->enableRabitQ && !buildstate->typeInfo->supportRabitQ) {
        ereport(ERROR, (errmsg("this data type cannot support hnsw_rabitq.")));
    }
    if (buildstate->enableRabitQ && !parallel) {
        RabitQConfig *rbqConfig = (RabitQConfig *)palloc(sizeof(RabitQConfig));
        rbqConfig->FHT = HnswGetUseFHT(index);
        buildstate->rbqConfig = rbqConfig;
        rbqConfig->reType = HnswGetRefineType(index);
        rbqConfig->reOffset = (buildstate->dimensions + 7) / 8;
        if (rbqConfig->reType == SQ8) {
            rbqConfig->sq = InitScalarQuantizer(buildstate->dimensions);
        } else {
            rbqConfig->sq = NULL;
        }
        VectorTransform *vt = (VectorTransform *)palloc(sizeof(VectorTransform));
        rbqConfig->vtrans = vt;
        vt->dim = buildstate->dimensions;
        vt->type = rbqConfig->FHT ? FAST_HTRANSFORM : RANDOM_ORTHOGONAL;
        vt->matrix = NULL;
        vt->fastRotation = NULL;
    } else {
        buildstate->rbqConfig = NULL;
    }
    buildstate->centroid = NULL;
    if (buildstate->enableLsg) {
        buildstate->lsgSample = NULL;
        buildstate->lsgSampleSize = 0;
        buildstate->lsgIndexCounter = 0;
        buildstate->lsgDim = 0;
        buildstate->LocScalingParam = NULL;
    }

    buildstate->isUStore = buildstate->heap ? RelationIsUstoreFormat(buildstate->heap) : false;
}

/*
 * Free resources
 */
static void FreeBuildState(HnswBuildState *buildstate, bool parallel)
{
    MemoryContextDelete(buildstate->graphCtx);
    MemoryContextDelete(buildstate->tmpCtx);
    if (buildstate->enablePQ && !parallel) {
        pfree(buildstate->pqTable);
        if (buildstate->pqMode == HNSW_PQMODE_SDC) {
            pfree(buildstate->pqDistanceTable);
        }
        pfree(buildstate->params);
    }
    if (buildstate->enableRabitQ && !parallel) {
        if (buildstate->centroid != NULL) {
            pfree(buildstate->centroid);
        }
        FreeTransformer(buildstate->rbqConfig->vtrans);
        if (buildstate->rbqConfig->sq != NULL){
            FreeScalarQuantizer(buildstate->rbqConfig->sq);
        }
        pfree(buildstate->rbqConfig);
    }
    if (buildstate->enableLsg && !parallel) {
        if (buildstate->LocScalingParam != NULL && buildstate->LocScalingParam->sampleVecs != NULL) {
            pfree(buildstate->LocScalingParam->sampleVecs);
            buildstate->LocScalingParam->sampleVecs = NULL;
        }
        if (buildstate->LocScalingParam) {
            pfree(buildstate->LocScalingParam);
            buildstate->LocScalingParam = NULL;
        }
        if (buildstate->lsgSample) {
            pfree(buildstate->lsgSample);
            buildstate->lsgSample = NULL;
        }
    }
}

static double ParallelHeapScan(HnswBuildState *buildstate, int *nparticipanttuplesorts)
{
    HnswShared *hnswshared = buildstate->hnswleader->hnswshared;
    double reltuples;

    BgworkerListWaitFinish(&buildstate->hnswleader->nparticipanttuplesorts);
    pg_memory_barrier();

    *nparticipanttuplesorts = buildstate->hnswleader->nparticipanttuplesorts;
    buildstate->graph = &hnswshared->graphData;
    buildstate->hnswarea = hnswshared->hnswarea;
    reltuples = hnswshared->reltuples;

    return reltuples;
}

/*
 * Perform a worker's portion of a parallel insert
 */
static void HnswParallelScanAndInsert(Relation heapRel, Relation indexRel, HnswShared *hnswshared, char *hnswarea)
{
    HnswBuildState buildstate;
    TableScanDesc scan;
    double reltuples;
    IndexInfo *indexInfo;

    /* Join parallel scan */
    indexInfo = BuildIndexInfo(indexRel);
    InitBuildState(&buildstate, heapRel, indexRel, indexInfo, MAIN_FORKNUM, true);
    buildstate.graph = &hnswshared->graphData;
    buildstate.hnswarea = hnswarea;
    buildstate.pqTable = hnswshared->pqTable;
    if (buildstate.enablePQ) {
        buildstate.params->pqTable = hnswshared->pqTable;
    }
    buildstate.pqDistanceTable = hnswshared->pqDistanceTable;
    if (buildstate.enableRabitQ) {
        buildstate.centroid = hnswshared->centroid;
        buildstate.rbqConfig = hnswshared->rbqConfig;
    }
    if (buildstate.enableLsg) {
        buildstate.LocScalingParam = hnswshared->lsgCalc;
        buildstate.lsgDim = buildstate.LocScalingParam->dim;
        buildstate.lsgSampleSize = buildstate.LocScalingParam->sampleSize;
    }
    InitAllocator(&buildstate.allocator, &HnswSharedMemoryAlloc, &buildstate);
    scan = tableam_scan_begin_parallel(heapRel, &hnswshared->heapdesc);
    reltuples = tableam_index_build_scan(heapRel, indexRel, indexInfo, true, BuildCallback, (void *)&buildstate, scan);

    /* Record statistics */
    SpinLockAcquire(&hnswshared->mutex);
    hnswshared->nparticipantsdone++;
    hnswshared->reltuples += reltuples;
    SpinLockRelease(&hnswshared->mutex);

    FreeBuildState(&buildstate, true);
}

/*
 * Perform work within a launched parallel process
 */
void HnswParallelBuildMain(const BgWorkerContext *bwc)
{
    HnswShared *hnswshared;
    char *hnswarea;
    Relation heapRel;
    Relation indexRel;

    /* Look up shared state */
    hnswshared = (HnswShared *)bwc->bgshared;

    /* Open relations within worker */
    heapRel = heap_open(hnswshared->heaprelid, NoLock);
    indexRel = index_open(hnswshared->indexrelid, NoLock);

    hnswarea = hnswshared->hnswarea;

    /* Perform inserts */
    HnswParallelScanAndInsert(heapRel, indexRel, hnswshared, hnswarea);

    /* Close relations within worker */
    index_close(indexRel, NoLock);
    heap_close(heapRel, NoLock);
}

/*
 * End parallel build
 */
static void HnswEndParallel(HnswLeader *hnswleader)
{
    HnswShared *hnswshared = hnswleader->hnswshared;
    if (hnswshared) {
        if (hnswshared->pqTable) {
            pfree_ext(hnswshared->pqTable);
        }
        if (hnswshared->pqDistanceTable) {
            pfree_ext(hnswshared->pqDistanceTable);
        }
        if (hnswshared->hnswarea) {
            pfree_ext(hnswshared->hnswarea);
        }
    }
    pfree_ext(hnswleader);
    BgworkerListSyncQuit();
}

static HnswShared *HnswParallelInitshared(HnswBuildState *buildstate)
{
    HnswShared *hnswshared;
    char *hnswarea;
    Size esthnswarea;
    Size estother;
    char *pqTable;
    float *pqDistanceTable;
    errno_t rc;
    LsgCalculator* lsgCalc;
    uint32 pqDistanceTableSize = buildstate->pqM * buildstate->pqKsub * buildstate->pqKsub * sizeof(float);

    /* Store shared build state, for which we reserved space */
    hnswshared =
        (HnswShared *)MemoryContextAllocZero(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), sizeof(HnswShared));

    /* Initialize immutable state */
    hnswshared->heaprelid = RelationGetRelid(buildstate->heap);
    hnswshared->indexrelid = RelationGetRelid(buildstate->index);
    hnswshared->pqDistanceTable = NULL;
    if (buildstate->enablePQ) {
        pqTable = (char *) MemoryContextAllocZero(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE),
                                                  buildstate->pqTableSize);
        rc = memcpy_s(pqTable, buildstate->pqTableSize, buildstate->pqTable, buildstate->pqTableSize);
        securec_check_c(rc, "\0", "\0");
        hnswshared->pqTable = pqTable;
        if (buildstate->pqMode == HNSW_PQMODE_SDC) {
            pqDistanceTable = (float *) MemoryContextAllocZero(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE),
                                                               pqDistanceTableSize);
            rc = memcpy_s(pqDistanceTable, pqDistanceTableSize, buildstate->pqDistanceTable, pqDistanceTableSize);
            securec_check_c(rc, "\0", "\0");
            hnswshared->pqDistanceTable = pqDistanceTable;
        }
    } else {
        hnswshared->pqTable = NULL;
    }
    if (buildstate->enableRabitQ) {
        hnswshared->centroid = buildstate->centroid;
        hnswshared->rbqConfig = buildstate->rbqConfig;
    } else {
        hnswshared->centroid = NULL;
        hnswshared->rbqConfig = NULL;
    }
    if (buildstate->enableLsg) {
        lsgCalc = (LsgCalculator *) MemoryContextAllocZero(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE),
                                                               sizeof(LsgCalculator));
        rc = memcpy_s(lsgCalc, sizeof(LsgCalculator), buildstate->LocScalingParam, sizeof(LsgCalculator));
        securec_check_c(rc, "\0", "\0");
        hnswshared->lsgCalc = lsgCalc;
    } else {
        hnswshared->lsgCalc = NULL;
    }
    SpinLockInit(&hnswshared->mutex);
    /* Initialize mutable state */
    hnswshared->nparticipantsdone = 0;
    hnswshared->reltuples = 0;
    HeapParallelscanInitialize(&hnswshared->heapdesc, buildstate->heap);

    /* Leave space for other objects in shared memory */
    /* Docker has a default limit of 64 MB for shm_size */
    /* which happens to be the default value of maintenance_work_mem */
    esthnswarea = u_sess->attr.attr_memory.maintenance_work_mem * 1024L;
    estother = 3 * 1024 * 1024;
    if (esthnswarea > estother)
        esthnswarea -= estother;

    hnswarea = (char *)palloc0_huge(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), esthnswarea);
    /* Report less than allocated so never fails */
    InitGraph(&hnswshared->graphData, hnswarea, esthnswarea - 1024 * 1024);

    hnswshared->graphData.memoryUsed += MAXALIGN(1);

    hnswshared->hnswarea = hnswarea;
    return hnswshared;
}

/*
 * Begin parallel build
 */
static void HnswBeginParallel(HnswBuildState *buildstate, int request)
{
    HnswShared *hnswshared;
    HnswLeader *hnswleader = (HnswLeader *)palloc0(sizeof(HnswLeader));

    Assert(request > 0);

    hnswshared = HnswParallelInitshared(buildstate);
    /* Launch workers, saving status for leader/caller */
    hnswleader->nparticipanttuplesorts = LaunchBackgroundWorkers(request, hnswshared, HnswParallelBuildMain, NULL);
    hnswleader->hnswshared = hnswshared;

    /* If no workers were successfully launched, back out (do serial build) */
    if (hnswleader->nparticipanttuplesorts == 0) {
        HnswEndParallel(hnswleader);
        return;
    }

    /* Log participants */
    ereport(DEBUG1, (errmsg("using %d parallel workers", hnswleader->nparticipanttuplesorts)));

    /* Save leader state now that it's clear build will be parallel */
    buildstate->hnswleader = hnswleader;
}

/*
 * Build graph
 */
static void BuildGraph(HnswBuildState *buildstate, ForkNumber forkNum)
{
    int parallel_workers = 0;

    /* Calculate parallel workers */
    if (buildstate->heap != NULL) {
        parallel_workers = PlanCreateIndexWorkers(buildstate->heap, buildstate->indexInfo);
    }

    bool singleThreadBuild = (buildstate->enableRabitQ && buildstate->rbqDelayState == RBQ_BUILD_AFTER_DELAY);

    /* Attempt to launch parallel worker scan when required */
    if (parallel_workers > 0 && !singleThreadBuild) {
        HnswBeginParallel(buildstate, parallel_workers);
    }

    /* Add tuples to graph */
    if (buildstate->heap != NULL) {
        if (!buildstate->hnswleader) {
        serial_build:
            buildstate->reltuples = tableam_index_build_scan(buildstate->heap, buildstate->index, buildstate->indexInfo,
                                                             true, BuildCallback, (void *)buildstate, NULL);
        } else {
            int nruns;
            buildstate->reltuples = ParallelHeapScan(buildstate, &nruns);
            if (nruns == 0) {
                /* failed to startup any bgworker, retry to do serial build */
                goto serial_build;
            }
        }

        buildstate->indtuples = buildstate->graph->indtuples;
    }

    /* Flush pages */
    if (!buildstate->graph->flushed) {
        FlushPages(buildstate);
    }

    /* End parallel build */
    if (buildstate->hnswleader) {
        HnswEndParallel(buildstate->hnswleader);
    }
}

void ComputeCenterAndTrainRefine(HnswBuildState *buildstate)
{
    if (buildstate->heap == NULL) {
        return;
    }
    double num;
    EstimateRows(buildstate->heap, &num);
    int numSamples = (int)num;
    if (buildstate->rbqDelayState == RBQ_BUILD_AFTER_DELAY) {
        numSamples = u_sess->datavec_ctx.rbq_sample_rows;
    } else if (numSamples == 0) {
        buildstate->rbqDelayState = RBQ_BUILD_DELAY;
        ereport(LOG, (errmsg("If there is no data in the table, RabitQ cannot be trained,"
            "and the index will not be built for the time being.")));
        return;
    }

    PG_TRY();
    {
        /* Sample rows */
        ereport(LOG, (errmsg("HNSW RabitQ start sample rows.")));
        buildstate->samples = VectorArrayInit(numSamples, buildstate->dimensions,
                                              buildstate->typeInfo->itemSize(buildstate->dimensions));
    }
    PG_CATCH();
    {
        PG_RE_THROW();
    }
    PG_END_TRY();

    BlockNumber totalblocks = RelationGetNumberOfBlocks(buildstate->heap);
    buildstate->rowstoskip = -1;
    BlockSampler_Init(&buildstate->bs, totalblocks, numSamples);

    buildstate->rstate = anl_init_selection_state(numSamples);
    while (BlockSampler_HasMore(&buildstate->bs)) {
        BlockNumber targblock = BlockSampler_Next(&buildstate->bs);

        tableam_index_build_scan(buildstate->heap, buildstate->index, buildstate->indexInfo,
                                 false, SampleCallback, (void *) buildstate, NULL, targblock, 1);
    }
    float *centroid = buildstate->centroid;
    for (int i = 0; i < buildstate->dimensions; i++) {
        centroid[i] = centroid[i] / numSamples;
    }
    ereport(LOG, (errmsg("HNSW RabitQ compute center successfully.")));

    if (buildstate->rbqConfig->reType == SQ8) {
        ScalarQuantizer *sq = buildstate->rbqConfig->sq;
        float *vmin = sq->trained;
        float *vdiff = vmin + sq->dim;
        for (int i = 0; i < sq->dim; i++) {
            vdiff[i] -= vmin[i];
        }
        ereport(LOG, (errmsg("HNSW RabitQ train SQ8 successfully for refine.")));
    }
}

static void LsgSampleCallBack(Relation index, CALLBACK_ITEM_POINTER, Datum* values, const bool* isnull,
                              bool tupleIsAlive, void* state)
{
    HnswBuildState *buildstate = (HnswBuildState*) state;
    char *base = buildstate->hnswarea;
    const HnswTypeInfo *typeInfo = buildstate->typeInfo;
    Datum value = PointerGetDatum(PG_DETOAST_DATUM(values[0]));

    if (buildstate->normprocinfo != NULL) {
        value = HnswNormValue(typeInfo, buildstate->collation, value);
    }

    Vector* currentVec = (Vector*)DatumGetPointer(value);

    buildstate->lsgDim = currentVec->dim;
    if ((buildstate->lsgIndexCounter % LSG_SAMPLE_INTERVAL == 0) && (buildstate->lsgSampleSize < MAX_LSG_SAMPLE_SIZE)) {
        memcpy_s(buildstate->lsgSample + buildstate->lsgSampleSize * currentVec->dim, sizeof(float) * (currentVec->dim),
                 currentVec->x, sizeof(float) * (currentVec->dim));
        buildstate->lsgSampleSize += 1;
    }

    buildstate->lsgIndexCounter += 1;
}

void GetLsgSample(HnswBuildState *buildstate)
{
    buildstate->lsgSampleSize = 0;
    buildstate->lsgIndexCounter = 0;
    buildstate->lsgSample = (float*)palloc0(MAX_LSG_SAMPLE_SIZE * buildstate->dimensions * sizeof(float));
    (void)tableam_index_build_scan(buildstate->heap, buildstate->index, buildstate->indexInfo, false, LsgSampleCallBack,
                             (void*)buildstate, NULL);
    if (buildstate->lsgSampleSize == 0 || buildstate->lsgIndexCounter < 2) {
        ereport(ERROR, (errmsg("create an HNSW LSG index requires inserting data first")));
    }
    buildstate->LocScalingParam = (LsgCalculator*)palloc0(sizeof(LsgCalculator));
    InitScalingParam(buildstate->LocScalingParam, buildstate->lsgSampleSize, buildstate->lsgDim, buildstate->lsgSample,
                     GetLsgfunctionType(buildstate->procinfo, buildstate->normprocinfo),
                     HnswGetLsgDegree(buildstate->index), HnswGetLsgAlpha(buildstate->index));
}


/*
 * Build the index
 */
void BuildIndex(Relation heap, Relation index, IndexInfo *indexInfo, HnswBuildState *buildstate,
                       ForkNumber forkNum, bool insert)
{
    Oid toastRelOid = InvalidOid;
    Relation toastRel = NULL;

#ifdef HNSW_MEMORY
    SeedRandom(42);
#endif

    InitBuildState(buildstate, heap, index, indexInfo, forkNum, false);

    /*
     * Lock the TOAST table to prevent VACUUM from cleaning up TOAST data
     * during index build. This is necessary because:
     * 1. Index build uses SnapshotAny which may see RECENTLY_DEAD tuples
     * 2. VACUUM may use a newer OldestXmin and consider those tuples as DEAD
     * 3. If VACUUM cleans TOAST data while we're building, detoast will fail
     *
     * We use ShareLock which conflicts with ShareUpdateExclusiveLock used by VACUUM.
     */
    if (heap != NULL && OidIsValid(heap->rd_rel->reltoastrelid)) {
        toastRelOid = heap->rd_rel->reltoastrelid;
        toastRel = heap_open(toastRelOid, ShareLock);
        ereport(DEBUG1, (errmsg("HNSW build: locked TOAST table %u to prevent VACUUM interference", toastRelOid)));
    }

    if (buildstate->isUStore) {
        if (toastRel != NULL) {
            heap_close(toastRel, ShareLock);
        }
        ereport(ERROR, (errmsg("ustore table cannot support hnsw.")));
    }
    if (HnswGetEnableMMap(index)) {
        ereport(LOG, (errmsg("Table MMap is on")));
    }

    if (buildstate->enablePQ) {
        BuildPQtable(buildstate);
        if (buildstate->pqMode == HNSW_PQMODE_SDC) {
            int pqM = buildstate->pqM;
            int pqKsub = buildstate->pqKsub;
            buildstate->pqDistanceTable = (float *)palloc(pqM * pqKsub * pqKsub * sizeof(float));
            GetPQDistanceTableSdc(buildstate->params, buildstate->pqDistanceTable);
        }
    }

    if (buildstate->enableRabitQ) {
        if (t_thrd.proc->workingVersionNum < RABITQ_VERSION_NUM) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("Before RABITQ_VERSION_NUM VERSION NUM %u, we do not support rabitq.", RABITQ_VERSION_NUM)));
        }
        buildstate->rbqDelayState = insert ? RBQ_BUILD_AFTER_DELAY : RBQ_BUILD_NORMAL;
        float *centroid = (float *)palloc0(sizeof(float) * buildstate->dimensions);
        buildstate-> centroid = centroid;
        ComputeCenterAndTrainRefine(buildstate);
        if (buildstate->rbqDelayState == RBQ_BUILD_DELAY) {
            buildstate-> centroid = NULL;
        } else {
            float *transCentroid = (float *)palloc(buildstate->dimensions * sizeof(float));
            VectorTransform* vtrans = buildstate->rbqConfig->vtrans;
            if (vtrans->type == RANDOM_ORTHOGONAL) {
                RomTrain(vtrans);
                RomTransform(vtrans, centroid, transCentroid);
            } else {
                FhtTrain(vtrans);
                FhtTransform(vtrans, centroid, transCentroid);
            }
            buildstate->centroid = transCentroid;
        }
        pfree(centroid);
    }

    if (buildstate->enableLsg) {
        if (t_thrd.proc->workingVersionNum < LSG_VERSION_NUM) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("Before LSG_VERSION_NUM VERSION NUM %u, we do not support lsg.", LSG_VERSION_NUM)));
        }
        if (buildstate->procinfo->fn_oid != L2_FUNC_OID && buildstate->procinfo->fn_oid != IP_FUNC_OID) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("HNSW LSG only supports L2 distance, IP distance, and cosine distance calculations "
                                   "for vector data types, and does not support the halfvector type.")));
        }
        GetLsgSample(buildstate);
    }

    if (buildstate->rbqDelayState == RBQ_BUILD_DELAY) {
        CreateMetaPage(buildstate);
    } else {
        BuildGraph(buildstate, forkNum);
    }

    if (RelationNeedsWAL(index) || forkNum == INIT_FORKNUM)
        LogNewpageRange(index, forkNum, 0, RelationGetNumberOfBlocksInFork(index, forkNum), true);

    FreeBuildState(buildstate, false);

    /* Release the TOAST table lock after index build is complete */
    if (toastRel != NULL) {
        heap_close(toastRel, ShareLock);
        ereport(DEBUG1, (errmsg("HNSW build: released TOAST table lock")));
    }
}

/*
 * Build the index for a logged table
 */
IndexBuildResult *hnswbuild_internal(Relation heap, Relation index, IndexInfo *indexInfo)
{
    IndexBuildResult *result;
    HnswBuildState buildstate;

    BuildIndex(heap, index, indexInfo, &buildstate, MAIN_FORKNUM, false);

    result = (IndexBuildResult *)palloc(sizeof(IndexBuildResult));
    result->heap_tuples = buildstate.reltuples;
    result->index_tuples = buildstate.indtuples;

    return result;
}

/*
 * Build the index for an unlogged table
 */
void hnswbuildempty_internal(Relation index)
{
    IndexInfo *indexInfo = BuildIndexInfo(index);
    HnswBuildState buildstate;

    BuildIndex(NULL, index, indexInfo, &buildstate, INIT_FORKNUM, false);
}
