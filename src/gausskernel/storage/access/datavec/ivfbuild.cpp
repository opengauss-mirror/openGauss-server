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
 * ivfbuild.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/ivfbuild.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include <cfloat>
#include <mutex>

#include "access/tableam.h"
#include "access/xact.h"
#include "access/datavec/bitvec.h"
#include "catalog/index.h"
#include "access/datavec/halfvec.h"
#include "access/datavec/ivfflat.h"
#include "miscadmin.h"
#include "storage/buf/bufmgr.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"
#include "access/datavec/vector.h"
#include "postmaster/bgworker.h"
#include "commands/vacuum.h"
#include "access/datavec/ivfnpuadaptor.h"

#include "pgstat.h"

#define CALLBACK_ITEM_POINTER HeapTuple hup

#define PARALLEL_KEY_IVFFLAT_SHARED UINT64CONST(0xA000000000000001)
#define PARALLEL_KEY_TUPLESORT UINT64CONST(0xA000000000000002)
#define PARALLEL_KEY_IVFFLAT_CENTERS UINT64CONST(0xA000000000000003)
#define PARALLEL_KEY_QUERY_TEXT UINT64CONST(0xA000000000000004)

#define MAX_HBM 1.0

static int g_deviceNum = 0;

/*
 * Create PQ-related pages
 */
static void CreatePQPages(IvfflatBuildState *buildstate, ForkNumber fNum)
{
    uint16 nblks;
    Relation index = buildstate->index;
    ForkNumber forkNum = fNum;
    Buffer buf;
    Page page;
    uint16 pqTableNblk;
    uint32 pqPreComputeTableNblk;
    GenericXLogState *state;

    IvfGetPQInfoFromMetaPage(index, &pqTableNblk, NULL, &pqPreComputeTableNblk, NULL);

    /* create pq table page */
    for (uint16 i = 0; i < pqTableNblk; i++) {
        buf = IvfflatNewBuffer(index, forkNum);
        IvfflatInitRegisterPage(index, &buf, &page, &state);
        MarkBufferDirty(buf);
        IvfflatCommitBuffer(buf, state);
    }

    /* create pq distance table page */
    for (uint32 i = 0; i < pqPreComputeTableNblk; i++) {
        buf = IvfflatNewBuffer(index, forkNum);
        IvfflatInitRegisterPage(index, &buf, &page, &state);
        MarkBufferDirty(buf);
        IvfflatCommitBuffer(buf, state);
    }
}

/*
 * Caculate Residual
 */
static void ComputeResidual(IvfflatBuildState *buildstate, Vector* sample, int list)
{
    Vector *vec = (Vector *)lfirst(buildstate->rlist->tail);
    Vector *center = (Vector *)VectorArrayGet(buildstate->centers, list);

    if (buildstate->byResidual) {
        for (int i = 0; i < buildstate->dimensions; i++) {
            vec->x[i] = sample->x[i] -center->x[i];
        }
    } else {
        for (int i = 0; i < buildstate->dimensions; i++) {
            vec->x[i] = sample->x[i];
        }
    }
}

/*
 * Caculate square of L2 normalform
 */
static float ComputeNormL2sqr(float *x, int dsub)
{
    float res = 0.0f;
    for (int i = 0; i < dsub; i++) {
        res += x[i] * x[i];
    }
    return res;
}

static void ComputeInnerProdAndSum(IvfflatBuildState *buildstate, float * l2Norm, float *center, float * tab, int dsub)
{
    Size itemSize = MAXALIGN(buildstate->typeInfo->itemSize(dsub));
    const float MULTIPLIER = 2.0;

    for (int i = 0; i < buildstate->pqM; i++) {
        for (int j = 0; j < buildstate->pqKsub; j++) {
            float *x = DatumGetVector(buildstate->pqTable + ((i * buildstate->pqKsub + j) * itemSize))->x;
            tab[i * buildstate->pqKsub + j] = VectorInnerProduct(dsub, x, center + i * dsub);
            float *pretable = &tab[i * buildstate->pqKsub + j];
            VectorMadd(1, l2Norm + (i * buildstate->pqKsub + j), MULTIPLIER, pretable, pretable);
        }
    }
}

/*
 * Compute precalculated table
 */
static void ComputePreTable(IvfflatBuildState *buildstate)
{
    Size size = buildstate->pqKsub * buildstate->pqM * sizeof(float);
    float *l2Norm = (float *)palloc0(size);

    int dsub =  buildstate->dimensions / buildstate->pqM;
    Size itemSize = MAXALIGN(buildstate->typeInfo->itemSize(dsub));

    for (int m = 0; m < buildstate->pqM; m++) {
        for (int j = 0; j < buildstate->pqKsub; j++) {
            float *x = DatumGetVector(buildstate->pqTable + (m * buildstate->pqKsub + j) * itemSize)->x;
            l2Norm[m * buildstate->pqKsub + j] = ComputeNormL2sqr(x, dsub);
        }
    }

    for (int n = 0; n < buildstate->lists; n++) {
        float *tab = buildstate->preComputeTable + n * buildstate->pqM * buildstate->pqKsub;
        Vector *center = (Vector *)VectorArrayGet(buildstate->centers, n);
        ComputeInnerProdAndSum(buildstate, l2Norm, center->x, tab, dsub);
    }

    pfree(l2Norm);
}

/*
 * Compute PQTable
 */
static int ComputeIvfPQ(IvfflatBuildState *buildstate)
{
    MemoryContext pqCtx = AllocSetContextCreate(CurrentMemoryContext,
                                                "Ivfflat PQ temporary context",
                                                ALLOCSET_DEFAULT_SIZES);
    MemoryContext oldCtx = MemoryContextSwitchTo(pqCtx);

    int res = IvfComputePQTable(buildstate->residuals, buildstate->params);
    MemoryContextSwitchTo(oldCtx);
    MemoryContextDelete(pqCtx);
    return res;
}

/*
 *  Get all sample vector or residual vector to vector array
 */
static void CopyResidaulFromList(IvfflatBuildState *buildstate)
{
    if (buildstate->rlist == NIL) {
        ereport(ERROR, (errmsg("when enable_pq = on, at least one vector needs to be include")));
    }

    ListCell *lc;
    buildstate->residuals = VectorArrayInit(
        buildstate->rlist->length,
        buildstate->dimensions,
        buildstate->typeInfo->itemSize(buildstate->dimensions)
    );

    foreach (lc, buildstate->rlist) {
        Vector *vec = (Vector *)lfirst(lc);
        Datum value = PointerGetDatum(vec);
        value = PointerGetDatum(PG_DETOAST_DATUM(value));
        VectorArraySet(buildstate->residuals, buildstate->residuals->length, DatumGetPointer(value));
        buildstate->residuals->length++;
    }
    list_free_deep(buildstate->rlist);
}

/*
 * Init PQParam
 */
PQParams *InitIVFPQParamsInMemory(IvfflatBuildState *buildstate)
{
    PQParams *params = (PQParams*)palloc(sizeof(PQParams));
    params->pqM = buildstate->pqM;
    params->pqKsub = buildstate->pqKsub;
    params->funcType = GetPQfunctionType(buildstate->procinfo, buildstate->normprocinfo);
    params->dim = buildstate->dimensions;
    Size subItemsize = buildstate->typeInfo->itemSize(buildstate->dimensions / buildstate->pqM);
    params->subItemSize = MAXALIGN(subItemsize);
    params->pqTable = buildstate->pqTable;
    return params;
}

/*
 * Add sample
 */
static void AddSample(Datum *values, IvfflatBuildState *buildstate)
{
    VectorArray samples = buildstate->samples;
    int targsamples = samples->maxlen;

    /* Detoast once for all calls */
    Datum value = PointerGetDatum(PG_DETOAST_DATUM(values[0]));

    /*
     * Normalize with KMEANS_NORM_PROC since spherical distance function
     * expects unit vectors
     */
    if (buildstate->kmeansnormprocinfo != NULL) {
        if (!IvfflatCheckNorm(buildstate->kmeansnormprocinfo, buildstate->collation, value)) {
            return;
        }

        value = IvfflatNormValue(buildstate->typeInfo, buildstate->collation, value);
    }

    if (samples->length < targsamples) {
        VectorArraySet(samples, samples->length, DatumGetPointer(value));
        samples->length++;
    } else {
        if (buildstate->rowstoskip < 0) {
            buildstate->rowstoskip = anl_get_next_S(samples->length, targsamples, &buildstate->rstate);
        }

        if (buildstate->rowstoskip <= 0) {
            int k = static_cast<int>(targsamples * anl_random_fract());
            Assert(k >= 0 && k < targsamples);
            VectorArraySet(samples, k, DatumGetPointer(value));
        }

        buildstate->rowstoskip -= 1;
    }
}

/*
 * Callback for sampling
 */
static void SampleCallback(Relation index, CALLBACK_ITEM_POINTER, Datum *values, const bool *isnull, bool tupleIsAlive,
                           void *state)
{
    IvfflatBuildState *buildstate = (IvfflatBuildState *)state;
    MemoryContext oldCtx;

    /* Skip nulls */
    if (isnull[0]) {
        return;
    }

    /* Use memory context since detoast can allocate */
    oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);

    /* Add sample */
    AddSample(values, buildstate);

    /* Reset memory context */
    MemoryContextSwitchTo(oldCtx);
    MemoryContextReset(buildstate->tmpCtx);
}

/*
 * Sample rows with same logic as ANALYZE
 */
static void SampleRows(IvfflatBuildState *buildstate)
{
    int targsamples = buildstate->samples->maxlen;
    BlockNumber totalblocks = RelationGetNumberOfBlocks(buildstate->heap);

    buildstate->rowstoskip = -1;

    BlockSampler_Init(&buildstate->bs, totalblocks, targsamples);

    buildstate->rstate = anl_init_selection_state(targsamples);
    while (BlockSampler_HasMore(&buildstate->bs)) {
        BlockNumber targblock = BlockSampler_Next(&buildstate->bs);

        tableam_index_build_scan(buildstate->heap, buildstate->index, buildstate->indexInfo, false, SampleCallback,
                                 (void *)buildstate, NULL, targblock, 1);
    }
}

/*
 * Add tuple to sort
 */
static void AddTupleToSort(Relation index, ItemPointer tid, Datum *values, IvfflatBuildState *buildstate)
{
    double distance;
    double minDistance = DBL_MAX;
    int closestCenter = 0;
    VectorArray centers = buildstate->centers;
    TupleTableSlot *slot = buildstate->slot;

    /* Detoast once for all calls */
    Datum value = PointerGetDatum(PG_DETOAST_DATUM(values[0]));

    /* Normalize if needed */
    if (buildstate->normprocinfo != NULL) {
        if (!IvfflatCheckNorm(buildstate->normprocinfo, buildstate->collation, value)) {
            return;
        }

        value = IvfflatNormValue(buildstate->typeInfo, buildstate->collation, value);
    }

    /* Find the list that minimizes the distance */
    for (int i = 0; i < centers->length; i++) {
        distance = DatumGetFloat8(FunctionCall2Coll(buildstate->procinfo, buildstate->collation, value,
                                                    PointerGetDatum(VectorArrayGet(centers, i))));
        if (distance < minDistance) {
            minDistance = distance;
            closestCenter = i;
        }
    }

    Vector* residual = NULL;
    if (buildstate->enablePQ) {
        ComputeResidual(buildstate, DatumGetVector(value), closestCenter);
        if (buildstate->byResidual) {
            residual = (Vector *)lfirst(buildstate->rlist->tail);
        }
    }

#ifdef IVFFLAT_KMEANS_DEBUG
    buildstate->inertia += minDistance;
    buildstate->listSums[closestCenter] += minDistance;
    buildstate->listCounts[closestCenter]++;
#endif

    /* Create a virtual tuple */
    ExecClearTuple(slot);
    slot->tts_values[IVF_LISTID - 1] = Int32GetDatum(closestCenter);
    slot->tts_isnull[IVF_LISTID - 1] = false;
    slot->tts_values[IVF_TID - 1] = PointerGetDatum(tid);
    slot->tts_isnull[IVF_TID - 1] = false;
    slot->tts_values[IVF_VECTOR - 1] = value;
    slot->tts_isnull[IVF_VECTOR - 1] = false;
    slot->tts_values[IVF_RESIDUAL - 1] = residual == NULL ? NULL : PointerGetDatum(residual);
    slot->tts_isnull[IVF_RESIDUAL - 1] = residual == NULL ? true : false;
    ExecStoreVirtualTuple(slot);

    /*
     * Add tuple to sort
     *
     * tuplesort_puttupleslot comment: Input data is always copied; the caller
     * need not save it.
     */
    tuplesort_puttupleslot(buildstate->sortstate, slot);

    buildstate->indtuples++;
}

/*
 * Callback for table_index_build_scan
 */
static void BuildCallback(Relation index, CALLBACK_ITEM_POINTER, Datum *values, const bool *isnull, bool tupleIsAlive,
                          void *state)
{
    IvfflatBuildState *buildstate = (IvfflatBuildState *)state;
    MemoryContext oldCtx;
    errno_t rc = EOK;

    ItemPointer tid = &hup->t_self;

    /* Skip nulls */
    if (isnull[0]) {
        return;
    }

    Vector *vec = InitVector(buildstate->dimensions);
    buildstate->rlist = lappend(buildstate->rlist, vec);

    /* Use memory context since detoast can allocate */
    oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);

    if (buildstate->enableNPU) {
        Datum value = PointerGetDatum(PG_DETOAST_DATUM(values[0]));

         /* Normalize if needed */
        if (buildstate->normprocinfo != NULL) {
            if (!IvfflatCheckNorm(buildstate->normprocinfo, buildstate->collation, value)) {
                return;
            }

            value = IvfflatNormValue(buildstate->typeInfo, buildstate->collation, value);
        }
        Vector *vector = DatumGetVector(value);
        buildstate->tupleslist = lappend(buildstate->tupleslist, vector);

        ItemPointer tmptid = (ItemPointer)palloc(sizeof(ItemPointerData));
        rc = memcpy_s(tmptid, sizeof(ItemPointerData), tid, sizeof(ItemPointerData));
        securec_check(rc, "\0", "\0");

        buildstate->tidslist = lappend(buildstate->tidslist, tmptid);
        buildstate->indtuples++;
    } else {
        AddTupleToSort(index, tid, values, buildstate);
    }

    /* Reset memory context */
    MemoryContextSwitchTo(oldCtx);
    if (!buildstate->enableNPU) {
        MemoryContextReset(buildstate->tmpCtx);
    }
}

/*
 * Get index tuple from sort state
 */
static inline void GetNextTuple(Tuplesortstate *sortstate, TupleDesc tupdesc, TupleTableSlot *slot, IndexTuple *itup,
                                int *list)
{
    Datum value;
    bool isnull;

    if (tuplesort_gettupleslot(sortstate, true, slot, NULL)) {
        *list = DatumGetInt32(heap_slot_getattr(slot, 1, &isnull));
        value = heap_slot_getattr(slot, 3, &isnull);

        /* Form the index tuple */
        *itup = index_form_tuple(tupdesc, &value, &isnull);
        (*itup)->t_tid = *((ItemPointer)DatumGetPointer(heap_slot_getattr(slot, 2, &isnull)));
    } else {
        *list = -1;
    }
}

/*
 * Create initial entry pages
 */
static void InsertTuples(Relation index, IvfflatBuildState *buildstate, ForkNumber forkNum)
{
    int list;
    IndexTuple itup = NULL; /* silence compiler warning */
    int64 inserted = 0;

    TupleTableSlot *slot = MakeSingleTupleTableSlot(buildstate->tupdesc);
    TupleDesc tupdesc = RelationGetDescr(index);
    Size pqcodesSize = buildstate->pqcodeSize;

    GetNextTuple(buildstate->sortstate, tupdesc, slot, &itup, &list);

    /* Check vector and pqcode can be on the same page */
    if (list != -1) {
        Size itemsize = MAXALIGN(IndexTupleSize(itup));
        Size emptyFreeSize = BLCKSZ - sizeof(IvfflatPageOpaqueData) - SizeOfPageHeaderData - sizeof(ItemIdData);
        if (emptyFreeSize < itemsize + MAXALIGN(pqcodesSize)) {
            int maxPQcodeSize = ((emptyFreeSize - itemsize) / 8) * 8;
            ereport(ERROR, (errmsg("vector and pqcode must be on the same page, max pq_m is %d", maxPQcodeSize)));
        }
    }

    for (int i = 0; i < buildstate->centers->length; i++) {
        Buffer buf;
        Page page;
        GenericXLogState *state;
        BlockNumber startPage;
        BlockNumber insertPage;
        int tuplePerList = 0;

        /* Can take a while, so ensure we can interrupt */
        /* Needs to be called when no buffer locks are held */
        CHECK_FOR_INTERRUPTS();

        buf = IvfflatNewBuffer(index, forkNum);
        IvfflatInitRegisterPage(index, &buf, &page, &state);

        startPage = BufferGetBlockNumber(buf);

        /* Get all tuples for list */
        while (list == i) {
            /* Check for free space */
            Size itemsz = MAXALIGN(IndexTupleSize(itup));
            if (PageGetFreeSpace(page) < itemsz + MAXALIGN(pqcodesSize))
                IvfflatAppendPage(index, &buf, &page, &state, forkNum);

            if (buildstate->enablePQ) {
                bool isnull;
                Size codesize = buildstate->params->pqM * sizeof(uint8);
                uint8 *pqcode = (uint8 *)palloc(codesize);
                Datum datum = buildstate->byResidual ? heap_slot_getattr(slot, 4, &isnull) : index_getattr(itup, 1, tupdesc, &isnull);

                IvfComputeVectorPQCode(DatumGetVector(datum)->x, buildstate->params, pqcode);
                ((PageHeader)page)->pd_upper -= MAXALIGN(pqcodesSize);
                errno_t rc = memcpy_s(
                    ((char *)page) + ((PageHeader)page)->pd_upper, pqcodesSize, (char *)pqcode, pqcodesSize);
                securec_check_c(rc, "\0", "\0");
            }

            /* Add the item */
            if (PageAddItem(page, (Item)itup, itemsz, InvalidOffsetNumber, false, false) == InvalidOffsetNumber)
                elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));

            pfree(itup);

            tuplePerList++;

            UpdateProgress(PROGRESS_CREATEIDX_TUPLES_DONE, ++inserted);

            GetNextTuple(buildstate->sortstate, tupdesc, slot, &itup, &list);
        }

        insertPage = BufferGetBlockNumber(buf);

        IvfflatCommitBuffer(buf, state);

        /* Set the start and insert pages */
        IvfflatUpdateList(index, buildstate->listInfo[i], insertPage, InvalidBlockNumber, startPage, forkNum,
            tuplePerList);
    }
}

/*
 * Initialize the build state
 */
static void InitBuildState(IvfflatBuildState *buildstate, Relation heap, Relation index, IndexInfo *indexInfo)
{
    buildstate->heap = heap;
    buildstate->index = index;
    buildstate->indexInfo = indexInfo;
    buildstate->typeInfo = IvfflatGetTypeInfo(index);

    buildstate->lists = IvfflatGetLists(index);
    buildstate->dimensions = TupleDescAttr(index->rd_att, 0)->atttypmod;

    /* Disallow varbit since require fixed dimensions */
    if (TupleDescAttr(index->rd_att, 0)->atttypid == VARBITOID)
        elog(ERROR, "type not supported for ivfflat index");

    /* Require column to have dimensions to be indexed */
    if (buildstate->dimensions < 0)
        elog(ERROR, "column does not have dimensions");

    if (buildstate->dimensions > buildstate->typeInfo->maxDimensions)
        elog(ERROR, "column cannot have more than %d dimensions for ivfflat index",
             buildstate->typeInfo->maxDimensions);

    buildstate->reltuples = 0;
    buildstate->indtuples = 0;

    /* Get support functions */
    buildstate->procinfo = index_getprocinfo(index, 1, IVFFLAT_DISTANCE_PROC);
    buildstate->normprocinfo = IvfflatOptionalProcInfo(index, IVFFLAT_NORM_PROC);
    buildstate->kmeansnormprocinfo = IvfflatOptionalProcInfo(index, IVFFLAT_KMEANS_NORM_PROC);
    buildstate->collation = index->rd_indcollation[0];

    /* Require more than one dimension for spherical k-means */
    if (buildstate->kmeansnormprocinfo != NULL && buildstate->dimensions == 1)
        elog(ERROR, "dimensions must be greater than one for this opclass");

    /* Create tuple description for sorting */
    buildstate->tupdesc = CreateTemplateTupleDesc(IVF_NUM_COLUMNS, false);
    TupleDescInitEntry(buildstate->tupdesc, (AttrNumber)IVF_LISTID, "list", INT4OID, -1, 0);
    TupleDescInitEntry(buildstate->tupdesc, (AttrNumber)IVF_TID, "tid", TIDOID, -1, 0);
    TupleDescInitEntry(buildstate->tupdesc, (AttrNumber)IVF_VECTOR, "vector", RelationGetDescr(index)->attrs[0].atttypid, -1, 0);
    TupleDescInitEntry(buildstate->tupdesc, (AttrNumber)IVF_RESIDUAL, "residual", VECTOROID, -1, 0);

    buildstate->slot = MakeSingleTupleTableSlot(buildstate->tupdesc);

    buildstate->centers = VectorArrayInit(buildstate->lists, buildstate->dimensions,
                                          buildstate->typeInfo->itemSize(buildstate->dimensions));
    buildstate->listInfo = (ListInfo *)palloc(sizeof(ListInfo) * buildstate->lists);

    buildstate->tmpCtx =
        AllocSetContextCreate(CurrentMemoryContext, "Ivfflat build temporary context", ALLOCSET_DEFAULT_SIZES);

#ifdef IVFFLAT_KMEANS_DEBUG
    buildstate->inertia = 0;
    buildstate->listSums = palloc0(sizeof(double) * buildstate->lists);
    buildstate->listCounts = palloc0(sizeof(int) * buildstate->lists);
#endif
    buildstate->ivfleader = NULL;

    buildstate->enablePQ = IvfGetEnablePQ(index);
    if (buildstate->enablePQ && !buildstate->typeInfo->supportPQ) {
        ereport(ERROR, (errmsg("this data type cannot support ivfpq.")));
    }
    if (buildstate->enablePQ && !g_instance.pq_inited) {
        ereport(ERROR, (errmsg("this instance has not currently loaded the pq dynamic library.")));
    }

    buildstate->pqM = IvfGetPqM(index);
    buildstate->pqKsub = IvfGetPqKsub(index);
    buildstate->byResidual = IvfGetByResidual(index);
    buildstate->rlist = NIL;
    buildstate->residuals = NULL;

    if (buildstate->enablePQ) {
        if (buildstate->dimensions % buildstate->pqM != 0) {
            ereport(ERROR, (errmsg("dimensions must be divisible by pq_m, please reset pq_m.")));
        }
        Size subItemsize = buildstate->typeInfo->itemSize(buildstate->dimensions / buildstate->pqM);
        subItemsize = MAXALIGN(subItemsize);
        buildstate->pqTableSize = buildstate->pqM * buildstate->pqKsub * subItemsize;
        buildstate->pqTable = (char*)palloc0(buildstate->pqTableSize);
        buildstate->pqcodeSize = buildstate->pqM * sizeof(uint8);
        buildstate->params = InitIVFPQParamsInMemory(buildstate);

        if (buildstate->byResidual &&
            (buildstate->params->funcType == PQ_DIS_L2 || buildstate->params->funcType == PQ_DIS_COSINE)) {
            buildstate->preComputeTableSize = buildstate->lists * buildstate->pqM * buildstate->pqKsub;
            buildstate->preComputeTable = (float*)palloc0(buildstate->preComputeTableSize * sizeof(float));
        } else {
            buildstate->preComputeTableSize = 0;
            buildstate->preComputeTable = NULL;
        }
    } else {
        buildstate->pqTable = NULL;
        buildstate->pqTableSize = 0;
        buildstate->pqcodeSize = 0;
        buildstate->params = NULL;
        buildstate->preComputeTableSize = 0;
        buildstate->preComputeTable = NULL;
    }
    buildstate->pqDistanceTable = NULL;

    buildstate->ivfclosestCentersIndexs = NULL;
    buildstate->ivfclosestCentersDistances = NULL;
    buildstate->tupleslist = NIL;
    buildstate->tidslist = NIL;
    buildstate->curtuple = 0;
    buildstate->enableNPU = u_sess->datavec_ctx.enable_npu;
    if (buildstate->enableNPU && !buildstate->typeInfo->supportNPU) {
        ereport(ERROR, (errmsg("this data type cannot support ivfnpu.")));
    }
    if (buildstate->enableNPU && !g_npu_func.inited) {
        ereport(ERROR, (errmsg("this instance has not currently loaded the ivfflatnpu dynamic library.")));
    }
}

/*
 * Free resources
 */
static void FreeBuildState(IvfflatBuildState *buildstate)
{
    VectorArrayFree(buildstate->centers);
    if (buildstate->residuals) {
        VectorArrayFree(buildstate->residuals);
    }
    pfree(buildstate->listInfo);

#ifdef IVFFLAT_KMEANS_DEBUG
    pfree(buildstate->listSums);
    pfree(buildstate->listCounts);
#endif

    MemoryContextDelete(buildstate->tmpCtx);
}

/*
 * Compute centers
 */
static void ComputeCenters(IvfflatBuildState *buildstate)
{
    int numSamples;

    /* Target 50 samples per list, with at least 10000 samples */
    /* The number of samples has a large effect on index build time */
    numSamples = buildstate->lists * 50;
    if (numSamples < 10000) {
        numSamples = 10000;
    }

    /* Skip samples for unlogged table */
    if (buildstate->heap == NULL) {
        numSamples = 1;
    }

    /* Sample rows */
    /* TODO Ensure within maintenance_work_mem */
    buildstate->samples = VectorArrayInit(numSamples, buildstate->dimensions, buildstate->centers->itemsize);
    if (buildstate->heap != NULL) {
        SampleRows(buildstate);
        if (buildstate->samples->length < buildstate->lists) {
            ereport(NOTICE, (errmsg("ivfflat index created with little data"), errdetail("This will cause low recall."),
                             errhint("Drop the index until the table has more data.")));
        }
    }

    /* Calculate centers */
    IvfflatBench("k-means",
                 IvfflatKmeans(buildstate->index, buildstate->samples, buildstate->centers, buildstate->typeInfo));

    /* Free samples before we allocate more memory */
    VectorArrayFree(buildstate->samples);
}

/*
 * Create the metapage
 */
static void CreateMetaPage(Relation index, IvfflatBuildState *buildstate, ForkNumber forkNum)
{
    Buffer buf;
    Page page;
    GenericXLogState *state;
    IvfflatMetaPage metap;

    buf = IvfflatNewBuffer(index, forkNum);
    IvfflatInitRegisterPage(index, &buf, &page, &state);

    /* Set metapage data */
    metap = IvfflatPageGetMeta(page);
    metap->magicNumber = IVFFLAT_MAGIC_NUMBER;
    metap->version = IVFFLAT_VERSION;
    metap->dimensions = buildstate->dimensions;
    metap->lists = buildstate->lists;

     /* set PQ info */
    metap->enablePQ = buildstate->enablePQ;
    metap->pqM = buildstate->pqM;
    metap->byResidual = buildstate->byResidual;
    metap->pqKsub = buildstate->pqKsub;
    metap->pqcodeSize = buildstate->pqcodeSize;
    metap->pqPreComputeTableSize = 0;
    metap->pqPreComputeTableNblk = 0;
    metap->pqTableSize = (uint32)buildstate->pqTableSize;
    metap->pqTableNblk =
        buildstate->enablePQ
            ? (uint16)((metap->pqTableSize + PQTABLE_STORAGE_SIZE - 1) / PQTABLE_STORAGE_SIZE)
            : 0;
    if (buildstate->enablePQ) {
        metap->pqTableNblk = (uint16)(
            (metap->pqTableSize + PQTABLE_STORAGE_SIZE - 1) / PQTABLE_STORAGE_SIZE);
        if (buildstate->byResidual &&
            (buildstate->params->funcType == PQ_DIS_L2 || buildstate->params->funcType == PQ_DIS_COSINE)) {
            uint64 TableLen = buildstate->lists * buildstate->pqM * buildstate->pqKsub;
            metap->pqPreComputeTableSize = (uint64)TableLen * sizeof(float);
            metap->pqPreComputeTableNblk = (uint32)(
                (metap->pqPreComputeTableSize + PQTABLE_STORAGE_SIZE - 1) / PQTABLE_STORAGE_SIZE);
        }
    } else {
        metap->pqTableNblk = 0;
    }

    ((PageHeader)page)->pd_lower = ((char *)metap + sizeof(IvfflatMetaPageData)) - (char *)page;

    IvfflatCommitBuffer(buf, state);
}

/*
 * Create list pages
 */
static void CreateListPages(Relation index, VectorArray centers, int dimensions, int lists, ForkNumber forkNum,
                            ListInfo **listInfo)
{
    Buffer buf;
    Page page;
    GenericXLogState *state;
    Size listSize;
    IvfflatList list;
    errno_t rc = EOK;

    listSize = MAXALIGN(IVFFLAT_LIST_SIZE(centers->itemsize));
    list = (IvfflatList)palloc0(listSize);

    buf = IvfflatNewBuffer(index, forkNum);
    IvfflatInitRegisterPage(index, &buf, &page, &state);

    for (int i = 0; i < lists; i++) {
        OffsetNumber offno;

        /* Zero memory for each list */
        MemSet(list, 0, listSize);

        /* Load list */
        list->startPage = InvalidBlockNumber;
        list->insertPage = InvalidBlockNumber;
        list->listId = i;
        list->tupleNum = 0;
        rc = memcpy_s(&list->center, VARSIZE_ANY(VectorArrayGet(centers, i)), VectorArrayGet(centers, i), VARSIZE_ANY(VectorArrayGet(centers, i)));
        securec_check(rc, "\0", "\0");

        /* Ensure free space */
        if (PageGetFreeSpace(page) < listSize)
            IvfflatAppendPage(index, &buf, &page, &state, forkNum);

        /* Add the item */
        offno = PageAddItem(page, (Item)list, listSize, InvalidOffsetNumber, false, false);
        if (offno == InvalidOffsetNumber)
            elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));

        /* Save location info */
        (*listInfo)[i].blkno = BufferGetBlockNumber(buf);
        (*listInfo)[i].offno = offno;
    }

    IvfflatCommitBuffer(buf, state);

    pfree(list);
}

#ifdef IVFFLAT_KMEANS_DEBUG
/*
 * Print k-means metrics
 */
static void PrintKmeansMetrics(IvfflatBuildState *buildstate)
{
    elog(INFO, "inertia: %.3e", buildstate->inertia);

    /* Calculate Davies-Bouldin index */
    if (buildstate->lists > 1) {
        double db = 0.0;

        /* Calculate average distance */
        for (int i = 0; i < buildstate->lists; i++) {
            if (buildstate->listCounts[i] > 0)
                buildstate->listSums[i] /= buildstate->listCounts[i];
        }

        for (int i = 0; i < buildstate->lists; i++) {
            double max = 0.0;
            double distance;

            for (int j = 0; j < buildstate->lists; j++) {
                if (j == i)
                    continue;

                distance = DatumGetFloat8(FunctionCall2Coll(buildstate->procinfo, buildstate->collation,
                                                            PointerGetDatum(VectorArrayGet(buildstate->centers, i)),
                                                            PointerGetDatum(VectorArrayGet(buildstate->centers, j))));
                distance = (buildstate->listSums[i] + buildstate->listSums[j]) / distance;

                if (distance > max)
                    max = distance;
            }
            db += max;
        }
        db /= buildstate->lists;
        elog(INFO, "davies-bouldin: %.3f", db);
    }
}
#endif

bool isL2Op(FmgrInfo *procinfo)
{
    return procinfo->fn_oid == L2_FUNC_OID;
}

static void CountBuildCallback(Relation index, CALLBACK_ITEM_POINTER, Datum *values, const bool *isnull,
    bool tupleIsAlive, void *state)
{
    IvfflatBuildState *buildstate = (IvfflatBuildState *)state;
    MemoryContext oldCtx;

    /* Skip nulls */
    if (isnull[0]) {
        return;
    }

    /* Use memory context since detoast can allocate */
    oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);

    /* Add tuple to sort */
    buildstate->curtuple++;

    /* Reset memory context */
    MemoryContextSwitchTo(oldCtx);
    MemoryContextReset(buildstate->tmpCtx);
}

static void GetClosestCenterOnNPU(int *closestCenterIndexs, float *closestCenterDistances, float *samples,
    float *tCentersMatrix, float *centersNorm, VectorArray centers, int dimensions, int centersNum, int numSamples,
    FmgrInfo *procinfo)
{
    float *resMatrix = (float *)palloc0_huge(CurrentMemoryContext, sizeof(float) * numSamples * centersNum);
    uint8_t *tupleDevice = nullptr;

    {
        std::mutex mDevice;
        std::lock_guard<std::mutex> lock(mDevice);
        g_deviceNum++;
    }

    size_t totalMem = (((double)(numSamples + centersNum) * dimensions + (int64)numSamples * (int64)centersNum) *
        sizeof(float)) / (1024 * 1024);  // MB
    int iterNum = std::max((int)(std::ceil((double)(totalMem) / (MAX_HBM * 1024))), 1);
    int batchSize = (int)std::ceil((double)numSamples / iterNum);
    int actualBatchSize = batchSize;
    int offset = 0;

    for (int i = 0; i < iterNum; i++) {
        actualBatchSize = std::min(batchSize, numSamples - offset);
        int ret = MatrixMulOnNPU(samples + offset * dimensions, tCentersMatrix, resMatrix + offset * centersNum,
            actualBatchSize, centersNum, dimensions, &tupleDevice, g_deviceNum - 1, false);
        if (ret != 0) {
            pfree(resMatrix);
            ereport(ERROR, (errmsg("matrix mul on npu failed, errCode: %d.", ret)));
        }
        offset += actualBatchSize;
    }

    #pragma omp parallel for schedule(dynamic)
    for (int i = 0; i < numSamples; i++) {
        int curMinIndex = -1;
        float curMinDistance = FLT_MAX;
        for (int j = 0; j < centersNum; j++) {
            float temp = isL2Op(procinfo) ? centersNorm[j] - 2 * resMatrix[j + i * centersNum]
                    : -resMatrix[j + i * centersNum];
            if (curMinDistance > temp) {
                curMinIndex = j;
                curMinDistance = temp;
            }
        }
        closestCenterIndexs[i] = curMinIndex;
        closestCenterDistances[i] = curMinDistance;
    }
    
    pfree(resMatrix);
}

static void ParallelAddTupleToSortOnNPU(IvfflatBuildState *buildstate)
{
    int batchsize = (int)buildstate->indtuples;
    float *centersMatrix = nullptr;
    float *centersNorm = nullptr;
    VectorArray centers = buildstate->centers;
    TupleTableSlot *slot = buildstate->slot;
    int dimensions = buildstate->dimensions;

    centersMatrix = (float *)palloc0_huge(CurrentMemoryContext, sizeof(float) * centers->length * dimensions);
    centersNorm = (float *)palloc0(sizeof(float) * centers->length);
    
    for (int i = 0; i < centers->length; i++) {
        float *centerData = (float *)PointerGetDatum(VectorArrayGet(centers, i));
        for (int j = 0; j < dimensions; j++) {
            centersMatrix[i * dimensions + j] = centerData[j + 2];
            if (isL2Op(buildstate->procinfo)) {
                centersNorm[i] += (centerData[j + 2] * centerData[j + 2]);
            }
        }
    }

    float *tCentersMatrix = (float *)palloc(sizeof(float) * dimensions * centers->length);
    for (int i = 0; i < centers->length * dimensions; i++) {
        tCentersMatrix[(i % dimensions) * centers->length + (i / dimensions)] =
             centersMatrix[(i / dimensions) * dimensions + (i % dimensions)];
    }

    float *tupleMatrix = (float *)palloc_huge(CurrentMemoryContext, batchsize * dimensions * sizeof(float));

    ListCell *cell = NULL;
    int i = 0;
    foreach (cell, buildstate->tupleslist) {
        Vector* tuple = (Vector *)lfirst(cell);
        for (int j = 0; j < dimensions; j++) {
            tupleMatrix[j + i * dimensions] = tuple->x[j];
        }
        i++;
    }

    buildstate->ivfclosestCentersIndexs = (int *)palloc0(sizeof(int) * batchsize);
    buildstate->ivfclosestCentersDistances = (float *)palloc0(sizeof(float) * batchsize);
    GetClosestCenterOnNPU(buildstate->ivfclosestCentersIndexs, buildstate->ivfclosestCentersDistances, tupleMatrix,
        tCentersMatrix, centersNorm, centers, dimensions, centers->length, batchsize, buildstate->procinfo);

    pfree(tupleMatrix);
    pfree(tCentersMatrix);
    
    for (int k = 0; k < batchsize; k++) {
        Vector *tmp_tuple = (Vector *)linitial(buildstate->tupleslist);
        buildstate->tupleslist = list_delete_first(buildstate->tupleslist);
        Datum value = PointerGetDatum(tmp_tuple);

        ItemPointer tmp_tid = (ItemPointer)linitial(buildstate->tidslist);
        buildstate->tidslist = list_delete_first(buildstate->tidslist);

#ifdef IVFFLAT_KMEANS_DEBUG
        buildstate->inertia += buildstate->ivfclosestCentersDistances[k];
        buildstate->listSums[buildstate->ivfclosestCentersIndexs[k]] += buildstate->ivfclosestCentersDistances[k];
        buildstate->listCounts[buildstate->ivfclosestCentersIndexs[k]]++;
#endif

        /* Create a virtual tuple */
        ExecClearTuple(slot);
        slot->tts_values[IVF_LISTID - 1] = Int32GetDatum(buildstate->ivfclosestCentersIndexs[k]);
        slot->tts_isnull[IVF_LISTID - 1] = false;
        slot->tts_values[IVF_TID - 1] = PointerGetDatum(tmp_tid);
        slot->tts_isnull[IVF_TID - 1] = false;
        slot->tts_values[IVF_VECTOR - 1] = value;
        slot->tts_isnull[IVF_VECTOR - 1] = false;
        slot->tts_values[IVF_RESIDUAL - 1] = NULL;
        slot->tts_isnull[IVF_RESIDUAL - 1] = true;
        ExecStoreVirtualTuple(slot);

        /*
        * Add tuple to sort
        *
        * tuplesort_puttupleslot comment: Input data is always copied; the caller
        * need not save it.
        */
        tuplesort_puttupleslot(buildstate->sortstate, slot);
    }
    pfree(buildstate->ivfclosestCentersIndexs);
    pfree(buildstate->ivfclosestCentersDistances);

    list_free(buildstate->tupleslist);
    list_free(buildstate->tidslist);

    pfree(centersMatrix);
    pfree(centersNorm);
}

static void SingleAddTupleToSortOnNPU(Relation index, ItemPointer tid, Datum *values, IvfflatBuildState *buildstate)
{
    TupleTableSlot *slot = buildstate->slot;
    double curTupleDis = buildstate->ivfclosestCentersDistances[buildstate->curtuple];
    int curTupleInd = buildstate->ivfclosestCentersIndexs[buildstate->curtuple];

    /* Detoast once for all calls */
    Datum value = PointerGetDatum(PG_DETOAST_DATUM(values[0]));

    /* Normalize if needed */
    if (buildstate->normprocinfo != NULL) {
        if (!IvfflatCheckNorm(buildstate->normprocinfo, buildstate->collation, value)) {
            return;
        }

        value = IvfflatNormValue(buildstate->typeInfo, buildstate->collation, value);
    }

    #ifdef IVFFLAT_KMEANS_DEBUG
    buildstate->inertia += curTupleDis;
    buildstate->listSums[curTupleInd] += curTupleDis;
    buildstate->listCounts[curTupleInd]++;
    #endif

    /* Create a virtual tuple */
    ExecClearTuple(slot);
    slot->tts_values[IVF_LISTID - 1] = Int32GetDatum(curTupleInd);
    slot->tts_isnull[IVF_LISTID - 1] = false;
    slot->tts_values[IVF_TID - 1] = PointerGetDatum(tid);
    slot->tts_isnull[IVF_TID - 1] = false;
    slot->tts_values[IVF_VECTOR - 1] = value;
    slot->tts_isnull[IVF_VECTOR - 1] = false;
    slot->tts_values[IVF_RESIDUAL - 1] = NULL;
    slot->tts_isnull[IVF_RESIDUAL - 1] = true;
    ExecStoreVirtualTuple(slot);

    /*
     * Add tuple to sort
     *
     * tuplesort_puttupleslot comment: Input data is always copied; the caller
     * need not save it.
     */
    tuplesort_puttupleslot(buildstate->sortstate, slot);

    buildstate->indtuples++;
    buildstate->curtuple++;
}

/*
 * Callback for table_index_build_scan for npu
 */
static void BuildCallbackOnNPU(Relation index, CALLBACK_ITEM_POINTER, Datum *values, const bool *isnull,
    bool tupleIsAlive, void *state)
{
    IvfflatBuildState *buildstate = (IvfflatBuildState *)state;
    MemoryContext oldCtx;

    ItemPointer tid = &hup->t_self;

    /* Skip nulls */
    if (isnull[0]) {
        return;
    }

    /* Use memory context since detoast can allocate */
    oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);

    /* Add tuple to sort */
    SingleAddTupleToSortOnNPU(index, tid, values, buildstate);

    /* Reset memory context */
    MemoryContextSwitchTo(oldCtx);
    MemoryContextReset(buildstate->tmpCtx);
}

static double SingleTableAmIndexBuildOnNPU(IvfflatBuildState *buildstate)
{
    Relation heap = buildstate->heap;
    Relation index = buildstate->index;
    IndexInfo *indexInfo = buildstate->indexInfo;

    float *centersMatrix = nullptr;
    float *centersNorm = nullptr;
    VectorArray centers = buildstate->centers;
    TupleTableSlot *slot = buildstate->slot;
    int dimensions = buildstate->dimensions;

    BlockNumber startblock = 0;
    BlockNumber totalblocks = 0;

    centersMatrix = (float *)palloc0_huge(CurrentMemoryContext, sizeof(float) * centers->length * dimensions);
    centersNorm = (float *)palloc0(sizeof(float) * centers->length);
    for (int i = 0; i < centers->length; i++) {
        float *centerData = (float *)PointerGetDatum(VectorArrayGet(centers, i));
        for (int j = 0; j < dimensions; j++) {
            centersMatrix[i * dimensions + j] = centerData[j + 2];
            if (isL2Op(buildstate->procinfo)) {
                centersNorm[i] += (centerData[j + 2] * centerData[j + 2]);
            }
        }
    }

    float *tCentersMatrix = (float *)palloc(sizeof(float) * dimensions * centers->length);
    for (int i = 0; i < centers->length * dimensions; i++) {
        tCentersMatrix[(i % dimensions) * centers->length + (i / dimensions)] =
             centersMatrix[(i / dimensions) * dimensions + (i % dimensions)];
    }

    if (buildstate->heap != NULL) {
        buildstate->reltuples =
            tableam_index_build_scan(heap, index, indexInfo, true, CountBuildCallback, (void *)buildstate, NULL);
        totalblocks = RelationGetNumberOfBlocks(buildstate->heap);
    }

    size_t totalMem = ((double)((centers->length + buildstate->reltuples) * dimensions +
                       (int64)centers->length * (int64)buildstate->reltuples) * sizeof(float)) / (1024 * 1024);  // MB
    int numbatch = std::max((int)(std::ceil((double)(totalMem) / (MAX_HBM * 1024))), 1);
    BlockNumber numblocks = (BlockNumber)(std::max((int)((int)totalblocks / numbatch), 1));
    int numSamples = (int)(std::ceil(buildstate->reltuples / (int)totalblocks)) * (int)numblocks + 2000;

    buildstate->reltuples = 0;
    while (startblock < totalblocks) {
        int realblocks = std::min(numblocks, totalblocks - startblock);
        buildstate->curtuple = 0;
        buildstate->samples = VectorArrayInit(numSamples, buildstate->dimensions, buildstate->centers->itemsize);
        tableam_index_build_scan(heap, index, indexInfo, false, SampleCallback, (void *)buildstate, NULL,
            startblock, realblocks);

        int realNumSamples = buildstate->samples->length;

        float *tupleMatrix = (float *)palloc0_huge(CurrentMemoryContext, sizeof(float) * realNumSamples * dimensions);
        for (int i = 0; i < realNumSamples; i++) {
            float *tupleData = (float *)PointerGetDatum(VectorArrayGet(buildstate->samples, i));
            for (int j = 0; j < dimensions; j++) {
                tupleMatrix[i * dimensions + j] = tupleData[j + 2];
            }
        }

        buildstate->ivfclosestCentersIndexs = (int *)palloc0(sizeof(int) * realNumSamples);
        buildstate->ivfclosestCentersDistances = (float *)palloc0(sizeof(float) * realNumSamples);

        GetClosestCenterOnNPU(buildstate->ivfclosestCentersIndexs, buildstate->ivfclosestCentersDistances, tupleMatrix,
            tCentersMatrix, centersNorm, centers, dimensions, centers->length, realNumSamples, buildstate->procinfo);

        buildstate->reltuples += tableam_index_build_scan(heap, index, indexInfo, false, BuildCallbackOnNPU,
            (void *)buildstate, NULL, startblock, realblocks);
        VectorArrayFree(buildstate->samples);
        startblock += numblocks;
        
        pfree(buildstate->ivfclosestCentersIndexs);
        pfree(buildstate->ivfclosestCentersDistances);
        pfree(tupleMatrix);
    }
    pfree(centersMatrix);
    pfree(centersNorm);
    pfree(tCentersMatrix);

    return buildstate->reltuples;
}

/*
 * Within leader, wait for end of heap scan
 */
static double ParallelHeapScan(IvfflatBuildState *buildstate)
{
    IvfflatShared *ivfshared = buildstate->ivfleader->ivfshared;
    double reltuples;

    BgworkerListWaitFinish(&buildstate->ivfleader->nparticipanttuplesorts);
    pg_memory_barrier();

    /* all done, update to the actual number of participants */
    if (ivfshared->sharedsort != NULL) {
        ivfshared->sharedsort->actualParticipants = buildstate->ivfleader->nparticipanttuplesorts;
    }

    buildstate->indtuples = ivfshared->indtuples;
    reltuples = ivfshared->reltuples;
    buildstate->rlist =list_copy(ivfshared->rlist);
    list_free(ivfshared->rlist);
#ifdef IVFFLAT_KMEANS_DEBUG
    buildstate->inertia = ivfshared->inertia;
#endif

    return reltuples;
}

/*
 * Perform a worker's portion of a parallel sort
 */
static void IvfflatParallelScanAndSort(IvfflatSpool *ivfspool, IvfflatShared *ivfshared, Vector *ivfcenters)
{
    SortCoordinate coordinate;
    IvfflatBuildState buildstate;
    TableScanDesc scan;
    double reltuples;
    IndexInfo *indexInfo;
    errno_t rc = EOK;

    /* Sort options, which must match AssignTuples */
    AttrNumber attNums[] = {1};
    Oid sortOperators[] = {INT4LTOID};
    Oid sortCollations[] = {InvalidOid};
    bool nullsFirstFlags[] = {false};

    /* Initialize local tuplesort coordination state */
    coordinate = (SortCoordinate)palloc0(sizeof(SortCoordinateData));
    coordinate->isWorker = true;
    coordinate->nParticipants = -1;
    coordinate->sharedsort = ivfshared->sharedsort;

    int sortmem = ivfshared->workmem / ivfshared->scantuplesortstates;

    /* Join parallel scan */
    indexInfo = BuildIndexInfo(ivfspool->index);
    indexInfo->ii_Concurrent = false;
    InitBuildState(&buildstate, ivfspool->heap, ivfspool->index, indexInfo);
    Size centersSize = buildstate.centers->itemsize * buildstate.centers->maxlen;
    rc = memcpy_s(buildstate.centers->items, centersSize, ivfcenters, centersSize);
    securec_check(rc, "\0", "\0");
    buildstate.centers->length = buildstate.centers->maxlen;
    ivfspool->sortstate = tuplesort_begin_heap(buildstate.tupdesc, 1, attNums, sortOperators, sortCollations,
                                               nullsFirstFlags, sortmem, false, 0, 0, 1, coordinate);
    buildstate.sortstate = ivfspool->sortstate;
    buildstate.enableNPU = ivfshared->enablenpu;

    scan = tableam_scan_begin_parallel(ivfspool->heap, &ivfshared->heapdesc);

    if (buildstate.enableNPU) {
        reltuples = tableam_index_build_scan(ivfspool->heap, ivfspool->index, indexInfo, true, BuildCallback,
            (void *)&buildstate, scan);
        if (reltuples != 0) {
            MemoryContext oldCtx = MemoryContextSwitchTo(buildstate.tmpCtx);

            ParallelAddTupleToSortOnNPU(&buildstate);

            MemoryContextSwitchTo(oldCtx);
            MemoryContextReset(buildstate.tmpCtx);
        }
    } else {
        reltuples = tableam_index_build_scan(ivfspool->heap, ivfspool->index, indexInfo, true, BuildCallback,
            (void *)&buildstate, scan);
    }

    /* Execute this worker's part of the sort */
    tuplesort_performsort(ivfspool->sortstate);

    /* Record statistics */
    SpinLockAcquire(&ivfshared->mutex);

    MemoryContext oldCtx = MemoryContextSwitchTo(ivfshared->tmpCtx);
    ListCell *lc;
    foreach (lc, buildstate.rlist) {
        Vector *vec = InitVector(buildstate.dimensions);
        int size = VECTOR_SIZE(buildstate.dimensions);
        error_t rc = memcpy_s(vec, size, lc->data.ptr_value, size);
        securec_check_c(rc, "\0", "\0");
        ivfshared->rlist = lappend(ivfshared->rlist, vec);
    }
    MemoryContextSwitchTo(oldCtx);
    list_free_deep(buildstate.rlist);

    ivfshared->nparticipantsdone++;
    ivfshared->reltuples += reltuples;
    ivfshared->indtuples += buildstate.indtuples;
#ifdef IVFFLAT_KMEANS_DEBUG
    ivfshared->inertia += buildstate.inertia;
#endif
    SpinLockRelease(&ivfshared->mutex);

    /* We can end tuplesorts immediately */
    tuplesort_end(ivfspool->sortstate);

    FreeBuildState(&buildstate);
}

/*
 * Perform work within a launched parallel process
 */
void IvfflatParallelBuildMain(const BgWorkerContext *bwc)
{
    IvfflatSpool *ivfspool;
    IvfflatShared *ivfshared;
    Relation heapRel;
    Relation indexRel;

    ivfshared = (IvfflatShared *)bwc->bgshared;

    /* Open relations within worker */
    heapRel = heap_open(ivfshared->heaprelid, NoLock);
    indexRel = index_open(ivfshared->indexrelid, NoLock);

    /* Initialize worker's own spool */
    ivfspool = (IvfflatSpool *)palloc0(sizeof(IvfflatSpool));
    ivfspool->heap = heapRel;
    ivfspool->index = indexRel;

    IvfflatParallelScanAndSort(ivfspool, ivfshared, ivfshared->ivfcenters);

    /* Close relations within worker */
    index_close(indexRel, NoLock);
    heap_close(heapRel, NoLock);
}

/*
 * End parallel build
 */
static void IvfflatParallelCleanup(const BgWorkerContext *bwc)
{
    IvfflatShared *ivfshared = (IvfflatShared *)bwc->bgshared;

    /* delete shared fileset */
    Assert(ivfshared->sharedsort);
    SharedFileSetDeleteAll(&ivfshared->sharedsort->fileset);
    pfree_ext(ivfshared->sharedsort);

    pfree_ext(ivfshared->ivfcenters);
    MemoryContextDelete(ivfshared->tmpCtx);
}

static IvfflatShared *IvfflatParallelInitshared(IvfflatBuildState *buildstate, int workmem, int scantuplesortstates)
{
    IvfflatShared *ivfshared;
    Sharedsort *sharedsort;
    Size estsort;
    Size estcenters;
    char *ivfcenters;

    /* Store shared build state, for which we reserved space */
    ivfshared = (IvfflatShared *)MemoryContextAllocZero(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE),
                                                        sizeof(IvfflatShared));

    /* Initialize immutable state */
    ivfshared->heaprelid = RelationGetRelid(buildstate->heap);
    ivfshared->indexrelid = RelationGetRelid(buildstate->index);
    ivfshared->scantuplesortstates = scantuplesortstates;
    SpinLockInit(&ivfshared->mutex);

    /* Initialize mutable state */
    ivfshared->nparticipantsdone = 0;
    ivfshared->reltuples = 0;
    ivfshared->indtuples = 0;
    ivfshared->workmem = workmem;
#ifdef IVFFLAT_KMEANS_DEBUG
    ivfshared->inertia = 0;
#endif
    HeapParallelscanInitialize(&ivfshared->heapdesc, buildstate->heap);

    /* Store shared tuplesort-private state, for which we reserved space */
    estsort = tuplesort_estimate_shared(scantuplesortstates);
    sharedsort = (Sharedsort *)MemoryContextAllocZero(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), estsort);
    tuplesort_initialize_shared(sharedsort, scantuplesortstates);
    ivfshared->sharedsort = sharedsort;

    estcenters = buildstate->centers->itemsize * buildstate->lists;
    ivfcenters = (char *)MemoryContextAllocZero(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), estcenters);
    errno_t rc = memcpy_s(ivfcenters, estcenters, buildstate->centers->items, estcenters);
    securec_check(rc, "\0", "\0");
    ivfshared->ivfcenters = (Vector *)ivfcenters;
    ivfshared->enablenpu = u_sess->datavec_ctx.enable_npu;

    ivfshared->tmpCtx =
        AllocSetContextCreate(CurrentMemoryContext, "Ivfflat build temporary context", ALLOCSET_DEFAULT_SIZES);
    return ivfshared;
}

/*
 * Shut down workers, destory parallel context, and end parallel mode.
 */
void IvfflatEndParallel(IvfflatLeader *ivfleader)
{
    BgworkerListSyncQuit();
    pfree_ext(ivfleader);
}

/*
 * Begin parallel build
 */
static void IvfflatBeginParallel(IvfflatBuildState *buildstate, int request, int workmem)
{
    IvfflatShared *ivfshared;
    IvfflatLeader *ivfleader = (IvfflatLeader *)palloc0(sizeof(IvfflatLeader));

    Assert(request > 0);
    ivfshared = IvfflatParallelInitshared(buildstate, workmem, request);

    /* Launch workers, saving status for leader/caller */
    ivfleader->nparticipanttuplesorts =
        LaunchBackgroundWorkers(request, ivfshared, IvfflatParallelBuildMain, IvfflatParallelCleanup);

    /* If no workers were successfully launched, back out (do serial build) */
    if (ivfleader->nparticipanttuplesorts == 0) {
        IvfflatEndParallel(ivfleader);
        return;
    }

    /* Log participants */
    ereport(DEBUG1, (errmsg("using %d parallel workers", ivfleader->nparticipanttuplesorts)));

    ivfleader->ivfshared = ivfshared;
    /* Save leader state now that it's clear build will be parallel */
    buildstate->ivfleader = ivfleader;
}

static double AssignTupleUtility(IvfflatBuildState *buildstate)
{
    Relation heap = buildstate->heap;
    Relation index = buildstate->index;
    IndexInfo *indexInfo = buildstate->indexInfo;
    double reltuples = 0;

    /* Fill spool using either serial or parallel heap scan */
    if (!buildstate->ivfleader) {
    serial_build:
        if (u_sess->datavec_ctx.enable_npu) {
            reltuples = SingleTableAmIndexBuildOnNPU(buildstate);
        } else {
            reltuples = tableam_index_build_scan(heap, index, indexInfo, true, BuildCallback, (void *)buildstate, NULL);
        }
    } else {
        reltuples = ParallelHeapScan(buildstate);
        IvfflatShared *ivfshared = buildstate->ivfleader->ivfshared;
        int nruns = ivfshared->sharedsort->actualParticipants;
        if (nruns == 0) {
            /* failed to startup any bgworker, retry to do serial build */
            goto serial_build;
        }
    }
    return reltuples;
}

/*
 * Scan table for tuples to index
 */
static void AssignTuples(IvfflatBuildState *buildstate)
{
    SortCoordinate coordinate = NULL;
    int parallel_workers = 0;
    IndexInfo *indexInfo = buildstate->indexInfo;
    UtilityDesc *desc = &indexInfo->ii_desc;
    int workmem;

    /* Sort options, which must match IvfflatParallelScanAndSort */
    AttrNumber attNums[] = {1};
    Oid sortOperators[] = {INT4LTOID};
    Oid sortCollations[] = {InvalidOid};
    bool nullsFirstFlags[] = {false};

    workmem = (desc->query_mem[0] > 0) ? (desc->query_mem[0] - SIMPLE_THRESHOLD)
                                       : u_sess->attr.attr_memory.maintenance_work_mem;

    /* Calculate parallel workers */
    if (buildstate->heap != NULL)
        parallel_workers = PlanCreateIndexWorkers(buildstate->heap, indexInfo);

    /* Attempt to launch parallel worker scan when required */
    if (parallel_workers > 0) {
        Assert(!indexInfo->ii_Concurrent);
        IvfflatBeginParallel(buildstate, parallel_workers, workmem);
    }

    /* Set up coordination state if at least one worker launched */
    if (buildstate->ivfleader) {
        coordinate = (SortCoordinate)palloc0(sizeof(SortCoordinateData));
        coordinate->isWorker = false;
        coordinate->nParticipants = buildstate->ivfleader->nparticipanttuplesorts;
        coordinate->sharedsort = buildstate->ivfleader->ivfshared->sharedsort;
    }

    /* Begin serial/leader tuplesort */
    buildstate->sortstate =
        tuplesort_begin_heap(buildstate->tupdesc, 1, attNums, sortOperators, sortCollations, nullsFirstFlags,
                             u_sess->attr.attr_memory.maintenance_work_mem, false, 0, 0, 1, coordinate);

    /* Add tuples to sort */
    if (buildstate->heap != NULL) {
        buildstate->reltuples = AssignTupleUtility(buildstate);

#ifdef IVFFLAT_KMEANS_DEBUG
        PrintKmeansMetrics(buildstate);
#endif
    }
}

/*
 * Create entry pages
 */
static void CreateEntryPages(IvfflatBuildState *buildstate, ForkNumber forkNum)
{
    /* Assign */
    IvfflatBench("assign tuples", AssignTuples(buildstate));

    /* Sort */
    IvfflatBench("sort tuples", tuplesort_performsort(buildstate->sortstate));
    /* Build PQTable by residusal */
    if (buildstate->enablePQ) {
        CopyResidaulFromList(buildstate);
        ereport(LOG, (errmsg("IVFPQ start to train codebook.")));
        int result = ComputeIvfPQ(buildstate);
        if (result == -1) {
            ereport(ERROR, (errmsg("IVFPQ training codebook is failed.")));
        } else {
            ereport(LOG, (errmsg("IVFPQ finish to train codebook.")));
        }
        if (buildstate->byResidual &&
            (buildstate->params->funcType == PQ_DIS_L2 || buildstate->params->funcType == PQ_DIS_COSINE))
            ComputePreTable(buildstate);
    }

    /* Load */
    IvfflatBench("load tuples", InsertTuples(buildstate->index, buildstate, forkNum));

    /* End sort */
    tuplesort_end(buildstate->sortstate);

    /* End parallel build */
    if (buildstate->ivfleader) {
        IvfflatEndParallel(buildstate->ivfleader);
    }
}

/*
 * Build the index
 */
static void BuildIndex(Relation heap, Relation index, IndexInfo *indexInfo, IvfflatBuildState *buildstate,
                       ForkNumber forkNum)
{
    InitBuildState(buildstate, heap, index, indexInfo);

    ComputeCenters(buildstate);

    /* Create pages */
    CreateMetaPage(index, buildstate, forkNum);

    if (buildstate->enablePQ) {
        CreatePQPages(buildstate, forkNum);
    }

    CreateListPages(index, buildstate->centers, buildstate->dimensions, buildstate->lists, forkNum,
                    &buildstate->listInfo);
    CreateEntryPages(buildstate, forkNum);

    if (buildstate->enablePQ) {
        IvfFlushPQInfo(buildstate);
    }

    /* Write WAL for initialization fork since GenericXLog functions do not */
    if (forkNum == INIT_FORKNUM)
        LogNewpageRange(index, forkNum, 0, RelationGetNumberOfBlocksInFork(index, forkNum), true);

    FreeBuildState(buildstate);
}

/*
 * Build the index for a logged table
 */
IndexBuildResult *ivfflatbuild_internal(Relation heap, Relation index, IndexInfo *indexInfo)
{
    IndexBuildResult *result;
    IvfflatBuildState buildstate;

    BuildIndex(heap, index, indexInfo, &buildstate, MAIN_FORKNUM);

    result = (IndexBuildResult *)palloc(sizeof(IndexBuildResult));
    result->heap_tuples = buildstate.reltuples;
    result->index_tuples = buildstate.indtuples;

    return result;
}

/*
 * Build the index for an unlogged table
 */
void ivfflatbuildempty_internal(Relation index)
{
    IndexInfo *indexInfo = BuildIndexInfo(index);
    IvfflatBuildState buildstate;

    BuildIndex(NULL, index, indexInfo, &buildstate, INIT_FORKNUM);
}
