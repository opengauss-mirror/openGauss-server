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

#include "pgstat.h"

#define CALLBACK_ITEM_POINTER HeapTuple hup

#define PARALLEL_KEY_IVFFLAT_SHARED UINT64CONST(0xA000000000000001)
#define PARALLEL_KEY_TUPLESORT UINT64CONST(0xA000000000000002)
#define PARALLEL_KEY_IVFFLAT_CENTERS UINT64CONST(0xA000000000000003)
#define PARALLEL_KEY_QUERY_TEXT UINT64CONST(0xA000000000000004)

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
    buildstate->rlist = NIL;
}

/*
 * Init PQParam
 */
PQParams *InitIVFPQParamsInMemory(IvfflatBuildState *buildstate)
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
 * get the minmax of each dim
 */
static void MixMaxSamples(Datum *values, IvfflatBuildState *buildstate)
{
    VectorArray samples = buildstate->samples;
    int targsamples = samples->maxlen;

    /* Detoast once for all calls */
    Datum value = PointerGetDatum(PG_DETOAST_DATUM(values[0]));

    if (buildstate->kmeansnormprocinfo != NULL) {
        if (!IvfflatCheckNorm(buildstate->kmeansnormprocinfo, buildstate->collation, value)) {
            return;
        }

        value = IvfflatNormValue(buildstate->typeInfo, buildstate->collation, value);
    }

    Vector *vec = (Vector *)value;
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
static void SQ8SampleCallback(Relation index, CALLBACK_ITEM_POINTER, Datum *values, const bool *isnull,
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

    /* get min/max in sample */
    MixMaxSamples(values, buildstate);

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

    Vector *transValue = NULL;
    if (buildstate->enableRabitQ) {
        RabitQConfig *rbqConfig = buildstate->rbqConfig;
        VectorTransform* vtrans = rbqConfig->vtrans;
        transValue = InitVector(buildstate->dimensions);
        if (vtrans->type == RANDOM_ORTHOGONAL) {
            RomTransform(vtrans, ((Vector *)DatumGetPointer(value))->x, transValue->x);
        } else {
            FhtTransform(vtrans, ((Vector *)DatumGetPointer(value))->x, transValue->x);
        }
    }

    /* Find the list that minimizes the distance */
    for (int i = 0; i < centers->length; i++) {
        if (buildstate->enableRabitQ) {
            distance = DatumGetFloat8(FunctionCall2Coll(buildstate->procinfo, buildstate->collation, (Datum)transValue,
                                                        PointerGetDatum(VectorArrayGet(buildstate->centroid, i))));
        } else {
            distance = DatumGetFloat8(FunctionCall2Coll(buildstate->procinfo, buildstate->collation, value,
                                                        PointerGetDatum(VectorArrayGet(centers, i))));
        }

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

    RabitqVector *rbqVec = NULL;
    bytea *rbqdata = NULL;
    if (buildstate->enableRabitQ) {
        RabitQConfig *rbqConfig = buildstate->rbqConfig;
        bool refineSQ8 = rbqConfig->reType == SQ8;
        if (refineSQ8) {
            /* Calculate origin vector's SQ8 */
            rbqVec = (RabitqVector *)palloc(rbqCodeSize(buildstate->dimensions, true));
            ScalarQuantizer *sq = rbqConfig->sq;
            int dim = sq->dim;
            VectorEncodeSQ(dim, sq->trained, sq->trained + dim, ((Vector *)DatumGetPointer(value))->x,
                                getRefineCode(rbqVec, rbqConfig->reOffset));
        } else {
            rbqVec = (RabitqVector *)palloc(rbqCodeSize(buildstate->dimensions, false));
        }
        
        Vector *centroid = (Vector *)VectorArrayGet(buildstate->centroid, closestCenter);
        int funcType = GetFunctionType(buildstate->procinfo, buildstate->normprocinfo);
        ComputeVectorRBQCode(transValue->dim, transValue->x, rbqVec, centroid->x, funcType);

        rbqdata = (bytea *)palloc(rbqDataSize(transValue->dim, refineSQ8) + VARHDRSZ);
        SET_VARSIZE(rbqdata, rbqDataSize(transValue->dim, refineSQ8) + VARHDRSZ);
        
        errno_t rc = memcpy_s(VARDATA(rbqdata), rbqDataSize(transValue->dim, refineSQ8),
            rbqVec->data, rbqDataSize(transValue->dim, refineSQ8));
        securec_check(rc, "\0", "\0");
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

    slot->tts_values[IVF_RBQ_DATA - 1] = rbqVec == NULL ? NULL : PointerGetDatum(rbqdata);
    slot->tts_isnull[IVF_RBQ_DATA - 1] = rbqVec == NULL ? true : false;
    slot->tts_values[IVF_OR_MINUS_CL2_SQR - 1] = rbqVec == NULL ? NULL : Float4GetDatum(rbqVec->fac.orMinusCL2Sqr);
    slot->tts_isnull[IVF_OR_MINUS_CL2_SQR - 1] = rbqVec == NULL ? true : false;
    slot->tts_values[IVF_XB_SUM - 1] = rbqVec == NULL ? NULL : Float4GetDatum(rbqVec->fac.xbSum);
    slot->tts_isnull[IVF_XB_SUM - 1] = rbqVec == NULL ? true : false;
    slot->tts_values[IVF_DP_MULTIPLIER - 1] = rbqVec == NULL ? NULL : Float4GetDatum(rbqVec->fac.dpMultiplier);
    slot->tts_isnull[IVF_DP_MULTIPLIER - 1] = rbqVec == NULL ? true : false;
    slot->tts_values[IVF_UNUSED - 1] = rbqVec == NULL ? NULL : UInt32GetDatum(rbqVec->fac.unused);
    slot->tts_isnull[IVF_UNUSED - 1] = rbqVec == NULL ? true : false;
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

    ItemPointer tid = &hup->t_self;

    /* Skip nulls */
    if (isnull[0]) {
        return;
    }

    if (buildstate->enablePQ) {
        Vector *vec = InitVector(buildstate->dimensions);
        buildstate->rlist = lappend(buildstate->rlist, vec);
    }

    /* RabitQ delay build, avoid "insert into select from" sql from inserting repeatedly. */
    if (buildstate->enableRabitQ && buildstate->rbqDelayState == RBQ_BUILD_AFTER_DELAY) {
        buildstate->rbqDelayBuildRows++;
        int64 insertedRows;
        IvfflatGetRbqInfoFromMetaPage(index, NULL, NULL, NULL, NULL, NULL, NULL,
                                   NULL, NULL, NULL, &insertedRows);
        if (buildstate->rbqDelayBuildRows > insertedRows) {
            return;
        }
    }

    /* Use memory context since detoast can allocate */
    oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);

    /* Add tuple to sort */
    AddTupleToSort(index, tid, values, buildstate);

    /* Reset memory context */
    MemoryContextSwitchTo(oldCtx);
    MemoryContextReset(buildstate->tmpCtx);
}

/*
 * Get index tuple from sort state
 */
static inline void GetNextTuple(Tuplesortstate *sortstate, TupleDesc tupdesc, TupleTableSlot *slot, IndexTuple *itup,
                                int *list, bool enableRabitQ)
{
    Datum value;
    bool isnull;
    Oid attrelid = tupdesc->attrs[0].attrelid;

    if (tuplesort_gettupleslot(sortstate, true, slot, NULL)) {
        *list = DatumGetInt32(heap_slot_getattr(slot, 1, &isnull));
        TupleDesc rbqTupdesc = NULL;
        if (enableRabitQ) {
            value = heap_slot_getattr(slot, 5, &isnull);

            rbqTupdesc = CreateTemplateTupleDesc(1, false);
            TupleDescInitEntry(rbqTupdesc, (AttrNumber)1, "rbqdata", BYTEAOID, -1, 0);
            rbqTupdesc->attrs[0].attrelid = attrelid;
            rbqTupdesc->attrs[0].attstorage = 'p';
        } else {
            value = heap_slot_getattr(slot, 3, &isnull);
        }

        /* Form the index tuple */
        if (enableRabitQ) {
            *itup = index_form_tuple(rbqTupdesc, &value, &isnull);
        } else {
            *itup = index_form_tuple(tupdesc, &value, &isnull);
        }
        
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

    GetNextTuple(buildstate->sortstate, tupdesc, slot, &itup, &list, buildstate->enableRabitQ);

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
            if (PageGetFreeSpace(page) < itemsz + MAXALIGN(pqcodesSize) + MAXALIGN(sizeof(FactorData)))
                IvfflatAppendPage(index, &buf, &page, &state, forkNum);

            if (buildstate->enablePQ) {
                bool isnull;
                Size codesize = buildstate->params->pqM * sizeof(uint8);
                uint8 *pqcode = (uint8 *)palloc(codesize);
                Datum datum = buildstate->byResidual ? heap_slot_getattr(slot, 4, &isnull) : index_getattr(itup, 1, tupdesc, &isnull);
                if (IvfComputeVectorPQCode(DatumGetVector(datum)->x, buildstate->params, pqcode, codesize) != 0) {
                    ereport(ERROR, (errmsg("failed to compute IVFPQ vector code")));
                }
                ((PageHeader)page)->pd_upper -= MAXALIGN(pqcodesSize);
                errno_t rc = memcpy_s(
                    ((char *)page) + ((PageHeader)page)->pd_upper, pqcodesSize, (char *)pqcode, pqcodesSize);
                securec_check_c(rc, "\0", "\0");
            }

            if (buildstate->enableRabitQ) {
                bool isnull;
                Size rbqvecsize = MAXALIGN(sizeof(FactorData));
                FactorData *fac = (FactorData *)palloc(rbqvecsize);
                fac->orMinusCL2Sqr = DatumGetFloat4(heap_slot_getattr(slot, 6, &isnull));
                fac->xbSum = DatumGetFloat4(heap_slot_getattr(slot, 7, &isnull));
                fac->dpMultiplier = DatumGetFloat4(heap_slot_getattr(slot, 8, &isnull));
                fac->unused = DatumGetUInt32(heap_slot_getattr(slot, 9, &isnull));
                ((PageHeader)page)->pd_upper -= rbqvecsize;
                errno_t rc = memcpy_s(
                    ((char *)page) + ((PageHeader)page)->pd_upper, rbqvecsize, (char *)fac, rbqvecsize);
                securec_check_c(rc, "\0", "\0");
            }

            /* Add the item */
            if (PageAddItem(page, (Item)itup, itemsz, InvalidOffsetNumber, false, false) == InvalidOffsetNumber)
                elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));

            pfree(itup);

            UpdateProgress(PROGRESS_CREATEIDX_TUPLES_DONE, ++inserted);

            GetNextTuple(buildstate->sortstate, tupdesc, slot, &itup, &list, buildstate->enableRabitQ);
        }

        insertPage = BufferGetBlockNumber(buf);

        IvfflatCommitBuffer(buf, state);

        /* Set the start and insert pages */
        IvfflatUpdateList(index, buildstate->listInfo[i], insertPage, InvalidBlockNumber, startPage, forkNum);
    }
}

/*
 * Initialize the build state
 */
static void InitBuildState(IvfflatBuildState *buildstate, Relation heap, Relation index,
    IndexInfo *indexInfo, bool parallel)
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
    
    /* RabitQ info */
    buildstate->enableRabitQ = IvfGetEnableRabitQ(index);
    buildstate->rbqDelayBuildRows = 0;
    buildstate->rbqDelayState = RBQ_BUILD_NORMAL;
    if (buildstate->enableRabitQ && !buildstate->typeInfo->supportRabitQ) {
        ereport(ERROR, (errmsg("this data type cannot support ivfflat_rabitq.")));
    }
    if (buildstate->enableRabitQ && !parallel) {
        RabitQConfig *rbqConfig = (RabitQConfig *)palloc(sizeof(RabitQConfig));
        rbqConfig->FHT = IvfGetUseFHT(index);
        buildstate->rbqConfig = rbqConfig;
        rbqConfig->reType = IvfGetRefineType(index);
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
    buildstate->centroid = VectorArrayInit(buildstate->lists, buildstate->dimensions,
                                           buildstate->typeInfo->itemSize(buildstate->dimensions));

    /* Create tuple description for sorting */
    buildstate->tupdesc = CreateTemplateTupleDesc(IVF_NUM_COLUMNS, false);
    TupleDescInitEntry(buildstate->tupdesc, (AttrNumber)IVF_LISTID, "list", INT4OID, -1, 0);
    TupleDescInitEntry(buildstate->tupdesc, (AttrNumber)IVF_TID, "tid", TIDOID, -1, 0);
    TupleDescInitEntry(buildstate->tupdesc, (AttrNumber)IVF_VECTOR, "vector", RelationGetDescr(index)->attrs[0].atttypid, -1, 0);
    TupleDescInitEntry(buildstate->tupdesc, (AttrNumber)IVF_RESIDUAL, "residual", VECTOROID, -1, 0);

    TupleDescInitEntry(buildstate->tupdesc, (AttrNumber)IVF_RBQ_DATA, "rbqdata", BYTEAOID, -1, 0);
    TupleDescInitEntry(buildstate->tupdesc, (AttrNumber)IVF_OR_MINUS_CL2_SQR, "orMinusCL2Sqr", FLOAT4OID, -1, 0);
    TupleDescInitEntry(buildstate->tupdesc, (AttrNumber)IVF_XB_SUM, "xbSum", FLOAT4OID, -1, 0);
    TupleDescInitEntry(buildstate->tupdesc, (AttrNumber)IVF_DP_MULTIPLIER, "dpMultiplier", FLOAT4OID, -1, 0);
    TupleDescInitEntry(buildstate->tupdesc, (AttrNumber)IVF_UNUSED, "unused", INT4OID, -1, 0);

    buildstate->tupdesc->attrs[IVF_RBQ_DATA - 1].attstorage = 'p';

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
            (buildstate->params->funcType == DIS_L2 || buildstate->params->funcType == DIS_COSINE)) {
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
    if (buildstate->enablePQ && buildstate->enableRabitQ) {
        ereport(ERROR, (\
            errmsg("ivfflat does not support the mixed use of the two quantization methods: PQ and RabitQ.")));
    }
}

/*
 * Free resources
 */
static void FreeBuildState(IvfflatBuildState *buildstate, bool parallel)
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

    if (buildstate->rlist != NIL) {
        list_free_deep(buildstate->rlist);
        buildstate->rlist = NIL;
    }

    if (buildstate->enableRabitQ && !parallel) {
        if (buildstate->centroid != NULL) {
            VectorArrayFree(buildstate->centroid);
        }
        
        FreeTransformer(buildstate->rbqConfig->vtrans);
        if (buildstate->rbqConfig->sq != NULL) {
            FreeScalarQuantizer(buildstate->rbqConfig->sq);
        }
        pfree(buildstate->rbqConfig);
    }

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

    if (buildstate->enablePQ) {
        metap->pqTableSize = (uint32)buildstate->pqTableSize;
        metap->pqTableNblk = (uint16)(
            (metap->pqTableSize + CHUNK_STORAGE_SIZE - 1) / CHUNK_STORAGE_SIZE);
        if (buildstate->byResidual &&
            (buildstate->params->funcType == DIS_L2 || buildstate->params->funcType == DIS_COSINE)) {
            uint64 TableLen = buildstate->lists * buildstate->pqM * buildstate->pqKsub;
            metap->pqPreComputeTableSize = (uint64)TableLen * sizeof(float);
            metap->pqPreComputeTableNblk = (uint32)(
                (metap->pqPreComputeTableSize + CHUNK_STORAGE_SIZE - 1) / CHUNK_STORAGE_SIZE);
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
        Size otherSize = 0;
        if (metap->reType == SQ8) {
            otherSize = 2 * dim * sizeof(float);
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

    ((PageHeader)page)->pd_lower = ((char *)metap + sizeof(IvfflatMetaPageData)) - (char *)page;

    IvfflatCommitBuffer(buf, state);
}

/*
 * Create list pages
 */
static void CreateListPages(Relation index, VectorArray centers, VectorArray centroid, int dimensions, int lists,
                            ForkNumber forkNum, ListInfo **listInfo, bool enableRabitQ)
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
        if (enableRabitQ) {
            rc = memcpy_s(&list->center, VARSIZE_ANY(VectorArrayGet(centroid, i)), VectorArrayGet(centroid, i),
                VARSIZE_ANY(VectorArrayGet(centroid, i)));
        } else {
            rc = memcpy_s(&list->center, VARSIZE_ANY(VectorArrayGet(centers, i)), VectorArrayGet(centers, i), VARSIZE_ANY(VectorArrayGet(centers, i)));
        }
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

/*
 * Create RabitQ-matrix pages
 */
static void CreateRbqMatrixPages(IvfflatBuildState *buildstate, ForkNumber fNum)
{
    uint16 nblks;
    Relation index = buildstate->index;
    ForkNumber forkNum = fNum;
    Buffer buf;
    Page page;
    uint16 matrixNblk;
    uint32 matrixSize;
    void *matrix;

    IvfflatGetRbqInfoFromMetaPage(index, NULL, NULL, NULL, NULL, &matrixNblk,
                               &matrixSize, NULL, NULL, NULL, NULL);

    /* create matrix page */
    for (uint16 i = 0; i < matrixNblk; i++) {
        buf = IvfflatNewBuffer(index, forkNum);
        page = BufferGetPage(buf);
        IvfflatInitPage(buf, page);
        MarkBufferDirty(buf);
        UnlockReleaseBuffer(buf);
    }

    VectorTransform* vtrans = buildstate->rbqConfig->vtrans;
    if (vtrans->type == RANDOM_ORTHOGONAL) {
        matrix = RomGetMatrix(vtrans);
    } else {
        matrix = FhtGetMatrix(vtrans);
    }

    IvfFlushChunkInfoInternal(index, (char *)matrix, IVFFLAT_CHUNK_START_BLKNO, matrixNblk, matrixSize);
    if (vtrans->type == FAST_HTRANSFORM) {
        pfree(matrix);
    }
}

/*
 * Create RabitQ-other pages, including centroid and min+diff if refine_type is SQ8
 */
static void CreateRbqOtherPages(IvfflatBuildState *buildstate, ForkNumber fNum)
{
    uint16 nblks;
    Relation index = buildstate->index;
    ForkNumber forkNum = fNum;
    RabitQConfig *rbqConfig = buildstate->rbqConfig;
    Buffer buf;
    Page page;
    uint16 matrixNblk;
    uint16 otherNblk;
    uint32 otherSize;
    uint32 oneSize = buildstate->dimensions * sizeof(float);
    void *other;
    errno_t rc;

    IvfflatGetRbqInfoFromMetaPage(index, NULL, NULL, NULL, NULL, &matrixNblk, NULL,
                               &otherNblk, &otherSize, NULL, NULL);

    /* create ohter page */
    for (uint16 i = 0; i < otherNblk; i++) {
        buf = IvfflatNewBuffer(index, forkNum);
        page = BufferGetPage(buf);
        IvfflatInitPage(buf, page);
        MarkBufferDirty(buf);
        UnlockReleaseBuffer(buf);
    }

    other = (void *)palloc(oneSize * 2);
    rc = memcpy_s((char*)other, oneSize * 2, rbqConfig->sq->trained, oneSize * 2);
    securec_check(rc, "\0", "\0");

    IvfFlushChunkInfoInternal(index, (char *)other, IVFFLAT_CHUNK_START_BLKNO + matrixNblk, otherNblk, otherSize);
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
    if (ivfshared->rlist != NIL) {
        buildstate->rlist = list_copy(ivfshared->rlist);
        list_free(ivfshared->rlist);
    }
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
    InitBuildState(&buildstate, ivfspool->heap, ivfspool->index, indexInfo, true);
    Size centersSize = buildstate.centers->itemsize * buildstate.centers->maxlen;
    rc = memcpy_s(buildstate.centers->items, centersSize, ivfcenters, centersSize);
    securec_check(rc, "\0", "\0");
    buildstate.centers->length = buildstate.centers->maxlen;
    ivfspool->sortstate = tuplesort_begin_heap(buildstate.tupdesc, 1, attNums, sortOperators, sortCollations,
                                               nullsFirstFlags, sortmem, false, 0, 0, 1, coordinate);
    buildstate.sortstate = ivfspool->sortstate;

    if (buildstate.enableRabitQ) {
        Size centroidSize = buildstate.centroid->itemsize * buildstate.centroid->maxlen;
        rc = memcpy_s(buildstate.centroid->items, centroidSize, ivfshared->centroid, centroidSize);
        securec_check(rc, "\0", "\0");
        buildstate.centroid->length = buildstate.centroid->maxlen;

        buildstate.rbqConfig = ivfshared->rbqConfig;
    }

    scan = tableam_scan_begin_parallel(ivfspool->heap, &ivfshared->heapdesc);
    reltuples = tableam_index_build_scan(ivfspool->heap, ivfspool->index, indexInfo, true, BuildCallback,
                                         (void *)&buildstate, scan);

    /* Execute this worker's part of the sort */
    tuplesort_performsort(ivfspool->sortstate);

    /* Record statistics */
    SpinLockAcquire(&ivfshared->mutex);

    if (buildstate.rlist != NIL) {
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
        buildstate.rlist = NIL;
    }

    ivfshared->nparticipantsdone++;
    ivfshared->reltuples += reltuples;
    ivfshared->indtuples += buildstate.indtuples;
#ifdef IVFFLAT_KMEANS_DEBUG
    ivfshared->inertia += buildstate.inertia;
#endif
    SpinLockRelease(&ivfshared->mutex);

    /* We can end tuplesorts immediately */
    tuplesort_end(ivfspool->sortstate);

    FreeBuildState(&buildstate, true);
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

    if (buildstate->enableRabitQ) {
        char *ivfcentroid = (char *)MemoryContextAllocZero(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE),
            estcenters);
        errno_t rc = memcpy_s(ivfcentroid, estcenters, buildstate->centroid->items, estcenters);
        securec_check(rc, "\0", "\0");
        ivfshared->centroid = (Vector *)ivfcentroid;
        ivfshared->rbqConfig = buildstate->rbqConfig;
    } else {
        ivfshared->centroid = NULL;
        ivfshared->rbqConfig = NULL;
    }

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
        reltuples = tableam_index_build_scan(heap, index, indexInfo, true, BuildCallback, (void *)buildstate, NULL);
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

    bool singleThreadBuild = (buildstate->enableRabitQ && buildstate->rbqDelayState == RBQ_BUILD_AFTER_DELAY);
    /* Attempt to launch parallel worker scan when required */
    if (parallel_workers > 0 && !singleThreadBuild) {
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
        if (buildstate->residuals->length < buildstate->pqKsub) {
            ereport(ERROR,
                    (errmsg("not enough samples to build IVFPQ table"),
                            errdetail("PQ requires at least pq_ksub samples, but got %d samples for pq_ksub=%d.",
                                      buildstate->residuals->length, buildstate->pqKsub),
                            errhint("Insert more rows or reduce pq_ksub.")));
        }
        ereport(LOG, (errmsg("IVFPQ start to train codebook.")));
        int success = ComputeIvfPQ(buildstate);
        if (success == -1) {
            ereport(ERROR, (errmsg("IVFPQ training codebook is failed.")));
        } else {
            ereport(LOG, (errmsg("IVFPQ finish to train codebook.")));
        }
        if (buildstate->byResidual &&
            (buildstate->params->funcType == DIS_L2 || buildstate->params->funcType == DIS_COSINE))
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

void TrainRefine(IvfflatBuildState *buildstate)
{
    if (buildstate->heap == NULL) {
        return;
    }
    double num;
    EstimateRows(buildstate->heap, &num);
    int numSamples = (int)num;
    if (numSamples == 0) {
        buildstate->rbqDelayState = RBQ_BUILD_DELAY;
        ereport(LOG, (errmsg("If there is no data in the table, RabitQ cannot be trained,"
            "and the index will not be built for the time being.")));
        return;
    }

    PG_TRY();
    {
        /* Sample rows */
        ereport(LOG, (errmsg("IVFFLAT RabitQ start sample rows.")));
        buildstate->samples = VectorArrayInit(numSamples, buildstate->dimensions,
                                              buildstate->typeInfo->itemSize(buildstate->dimensions));
    }
    PG_CATCH();
    {
        ereport(ERROR, (errmsg("memory alloc failed during IVFFLAT RabitQ sampling,"
            "suggest using ivfflat without RabitQ.")));
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
                                 false, SQ8SampleCallback, (void *) buildstate, NULL, targblock, 1);
    }

    if (buildstate->rbqConfig->reType == SQ8) {
        ScalarQuantizer *sq = buildstate->rbqConfig->sq;
        float *vmin = sq->trained;
        float *vdiff = vmin + sq->dim;
        for (int i = 0; i < sq->dim; i++) {
            vdiff[i] -= vmin[i];
        }
        ereport(LOG, (errmsg("IVFFLAT RabitQ train SQ8 successfully for refine.")));
    }
}


/*
 * Build the index
 */
void BuildIndex(Relation heap, Relation index, IndexInfo *indexInfo, IvfflatBuildState *buildstate,
                       ForkNumber forkNum, bool insert)
{
    InitBuildState(buildstate, heap, index, indexInfo, false);

    ComputeCenters(buildstate);

    if (buildstate->enableRabitQ) {
        if (t_thrd.proc->workingVersionNum < RABITQ_VERSION_NUM) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("Before RABITQ_VERSION_NUM VERSION NUM %u, we do not support rabitq.", RABITQ_VERSION_NUM)));
        }
        buildstate->rbqDelayState = insert ? RBQ_BUILD_AFTER_DELAY : RBQ_BUILD_NORMAL;
        int dim = buildstate->dimensions;
        TrainRefine(buildstate);
        if (buildstate->rbqDelayState == RBQ_BUILD_DELAY) {
            buildstate->centroid = NULL;
        } else {
            for (int i = 0; i < buildstate->centers->length; i++) {
                Vector *centerData = (Vector *)VectorArrayGet(buildstate->centers, i);
                float *transCentroid = (float *)palloc(dim * sizeof(float));
                VectorTransform* vtrans = buildstate->rbqConfig->vtrans;
                if (vtrans->type == RANDOM_ORTHOGONAL) {
                    RomTrain(vtrans);
                    RomTransform(vtrans, centerData->x, transCentroid);
                } else {
                    FhtTrain(vtrans);
                    FhtTransform(vtrans, centerData->x, transCentroid);
                }
                Vector *transValue = InitVector(dim);
                errno_t rc = memcpy_s(&transValue->x[0], sizeof(float) * dim, transCentroid, sizeof(float) * dim);
                securec_check_c(rc, "\0", "\0");

                VectorArraySet(buildstate->centroid, i, (Pointer)transValue);

                pfree(transCentroid);
            }
        }
    }

    if (buildstate->rbqDelayState == RBQ_BUILD_DELAY) {
        /* Create pages */
        CreateMetaPage(index, buildstate, forkNum);
    } else {
        BlockNumber numPages = RelationGetNumberOfBlocks(buildstate->index);
        if (numPages == 0) {
            CreateMetaPage(index, buildstate, forkNum);
        }

        if (buildstate->enablePQ) {
            CreatePQPages(buildstate, forkNum);
        }

        if (buildstate->enableRabitQ) {
            /* Create pages and flush matrix */
            CreateRbqMatrixPages(buildstate, forkNum);
            /* Create pages and flush centroid (min+diff if refine type is SQ8) */
            if (buildstate->rbqConfig->reType == SQ8) {
                CreateRbqOtherPages(buildstate, forkNum);
            }
        }

        CreateListPages(index, buildstate->centers, buildstate->centroid, buildstate->dimensions,
            buildstate->lists, forkNum, &buildstate->listInfo, buildstate->enableRabitQ);
        CreateEntryPages(buildstate, forkNum);
    }

    if (buildstate->enablePQ) {
        IvfFlushPQInfo(buildstate);
    }

    /* Write WAL for initialization fork since GenericXLog functions do not */
    if (forkNum == INIT_FORKNUM)
        LogNewpageRange(index, forkNum, 0, RelationGetNumberOfBlocksInFork(index, forkNum), true);

    FreeBuildState(buildstate, false);
}

/*
 * Build the index for a logged table
 */
IndexBuildResult *ivfflatbuild_internal(Relation heap, Relation index, IndexInfo *indexInfo)
{
    IndexBuildResult *result;
    IvfflatBuildState buildstate;

    BuildIndex(heap, index, indexInfo, &buildstate, MAIN_FORKNUM, false);

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

    BuildIndex(NULL, index, indexInfo, &buildstate, INIT_FORKNUM, false);
}
