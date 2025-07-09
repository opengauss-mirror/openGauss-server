/*
 * Copyright (c) 2025 Huawei Technologies Co.,Ltd.
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
 * diskannbuild.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/diskannbuild.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/datavec/diskann.h"
#include "access/tableam.h"

#define CALLBACK_ITEM_POINTER HeapTuple hup

/*
 * Initialize the build state
 */
static void InitBuildState(DiskAnnBuildState* buildstate, Relation heap, Relation index, IndexInfo* indexInfo)
{
    buildstate->heap = heap;
    buildstate->index = index;
    buildstate->indexInfo = indexInfo;
    buildstate->typeInfo = DiskAnnGetTypeInfo(index);

    DiskAnnOptions* opts = (DiskAnnOptions*)index->rd_options;

    buildstate->dimensions = TupleDescAttr(index->rd_att, 0)->atttypmod;

    /* Disallow varbit since require fixed dimensions */
    if (TupleDescAttr(index->rd_att, 0)->atttypid == VARBITOID) {
        elog(ERROR, "type not supported for diskann index");
    }

    /* Require column to have dimensions to be indexed */
    if (buildstate->dimensions < 0) {
        elog(ERROR, "column does not have dimensions");
    }

    if (buildstate->dimensions > buildstate->typeInfo->maxDimensions) {
        elog(ERROR, "column cannot have more than %d dimensions for diskann index",
             buildstate->typeInfo->maxDimensions);
    }

    if (opts) {
        buildstate->maxDegree = opts->maxDegree;
        buildstate->maxAlpha = opts->maxAlpha;
        buildstate->indexSize = opts->indexSize;
    } else {
        buildstate->maxDegree = DISKANN_DEFAULT_MAX_DEGREE;
        buildstate->maxAlpha = DISKANN_DEFAULT_MAX_ALPHA;
        buildstate->indexSize = DISKANN_DEFAULT_INDEX_SIZE;
    }

    buildstate->reltuples = 0;
    buildstate->indtuples = 0;

    /* Get support functions */
    buildstate->procinfo = index_getprocinfo(index, 1, DISKANN_DISTANCE_PROC);
    buildstate->normprocinfo = DiskAnnOptionalProcInfo(index, DISKANN_NORM_PROC);
    buildstate->kmeansnormprocinfo = DiskAnnOptionalProcInfo(index, DISKANN_KMEANS_NORMAL_PROC);
    buildstate->collation = index->rd_indcollation[0];

    /* Require more than one dimension for spherical k-means */
    if (buildstate->kmeansnormprocinfo != NULL && buildstate->dimensions == 1) {
        elog(ERROR, "dimensions must be greater than one for this opclass");
    }

    buildstate->nodeSize = sizeof(DiskAnnNodePageData);
    buildstate->edgeSize = sizeof(DiskAnnEdgePageData);
    buildstate->itemSize = buildstate->nodeSize + buildstate->edgeSize;
    buildstate->graphStore = New(CurrentMemoryContext) DiskAnnGraphStore(index);
    buildstate->tmpCtx =
        AllocSetContextCreate(CurrentMemoryContext, "diskann build temporary context", ALLOCSET_DEFAULT_SIZES);
}

/*
 * Free resources
 */
static void FreeBuildState(DiskAnnBuildState* buildstate)
{
    if (buildstate->graphStore) {
        delete buildstate->graphStore;
        buildstate->graphStore = nullptr;
    }
    MemoryContextDelete(buildstate->tmpCtx);
}

static void CreateMetaPage(Relation index, DiskAnnBuildState* buildstate, ForkNumber forkNum)
{
    Buffer buf;
    Page page;
    DiskAnnMetaPage metap;

    buf = ReadBufferExtended(index, MAIN_FORKNUM, P_NEW, RBM_NORMAL, NULL);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    page = DiskAnnInitRegisterPage(index, buf);

    /* Set metapage data */
    metap = DiskAnnPageGetMeta(page);
    metap->magicNumber = DISKANN_MAGIC_NUMBER;
    metap->version = DISKANN_VERSION;
    metap->dimensions = buildstate->dimensions;
    metap->maxDegree = buildstate->maxDegree;
    metap->maxAlpha = buildstate->maxAlpha;
    metap->nodeSize = buildstate->nodeSize;
    metap->edgeSize = buildstate->edgeSize;
    metap->itemSize = metap->nodeSize + metap->edgeSize;
    metap->insertPage = InvalidBlockNumber;

    ((PageHeader)page)->pd_lower = ((char*)metap + sizeof(DiskAnnMetaPageData)) - (char*)page;

    MarkBufferDirty(buf);
    UnlockReleaseBuffer(buf);
}

static BlockNumber GetInsertPage(Relation index)
{
    DiskAnnMetaPageData metap;
    DiskANNGetMetaPageInfo(index, &metap);
    BlockNumber insertPage = metap.insertPage;
    return insertPage;
}

static char* ReserveSpace(Page page, Size size)
{
    PageHeader ph = (PageHeader)page;
    Assert((ph->pd_upper > ph->pd_lower));
    Assert((Size)(uint32)(ph->pd_upper - ph->pd_lower) >= size);
    ph->pd_upper -= (uint16)size;
    char* dst = ((char*)ph) + ph->pd_upper;
    return dst;
}

/*
 * Insert vector into page, reserve and initialize node & edge page
 *
 */
static BlockNumber InsertVectorIntoPage(Relation index, Vector* vec, double sqrSum, ItemPointer heaptid,
                                        DiskAnnBuildState* buildstate)
{
    Buffer buf;
    Page page;
    IndexTuple itup;
    Datum value = PointerGetDatum(vec);
    bool isnull[1] = {false};

    /* form index tuple */
    itup = index_form_tuple(RelationGetDescr(index), &value, isnull);
    itup->t_tid = *heaptid;

    Size itemsz = MAXALIGN(IndexTupleSize(itup));
    Assert(itemsz <=
           BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - MAXALIGN(sizeof(DiskAnnPageOpaqueData)) - sizeof(ItemIdData));

    /* initialize page */
    buf = ReadBufferExtended(index, MAIN_FORKNUM, P_NEW, RBM_NORMAL, NULL);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    page = DiskAnnInitRegisterPage(index, buf);

    /* reserve space for node & edge page and add item into page */
    ReserveSpace(page, buildstate->itemSize);
    if (PageAddItem(page, (Item)itup, itemsz, InvalidOffsetNumber, false, false) == InvalidOffsetNumber) {
        UnlockReleaseBuffer(buf);
        pfree(itup);
        elog(ERROR, "Failed to add index item to \"%s\"", RelationGetRelationName(index));
    }

    /* initialize node page */
    IndexTuple ctup = (IndexTuple)PageGetItem(page, PageGetItemId(page, FirstOffsetNumber));
    DiskAnnNodePage tup = DiskAnnPageGetNode(ctup);
    tup->sqrSum = (sqrSum >= 0) ? sqrSum : VectorSquareNorm(vec->x, vec->dim);

    for (int pos = 0; pos < DISKANN_HEAPTIDS; pos++) {
        ItemPointerSetInvalid(&tup->heaptids[pos]);
    }
    tup->heaptidsLength = (ItemPointerIsValid(heaptid) ? 1 : 0);
    if (ItemPointerIsValid(heaptid)) {
        tup->heaptids[0] = *heaptid;
    }

    /*  initialize edge page */
    DiskAnnEdgePage etup = (DiskAnnEdgePage)((uint8_t*)tup + buildstate->nodeSize);
    for (uint16_t pos = 0; pos < OUTDEGREE; pos++) {
        etup->nexts[pos] = InvalidBlockNumber;
        etup->distance[pos] = FLT_MAX;
    }
    etup->count = 0;

    BlockNumber blkno = BufferGetBlockNumber(buf);
    MarkBufferDirty(buf);
    UnlockReleaseBuffer(buf);

    pfree(itup);
    return blkno;
}

static BlockNumber InsertTuple(Relation index, Datum* values, ItemPointer heaptid,
                               DiskAnnBuildState* buildstate)
{
    Datum value = PointerGetDatum(PG_DETOAST_DATUM(values[0]));
    /* Normalize if needed */
    if (buildstate->normprocinfo != NULL) {
        if (!DiskAnnCheckNorm(buildstate->normprocinfo, buildstate->collation, value)) {
            return InvalidBlockNumber;
        }

        value = DiskAnnNormValue(buildstate->typeInfo, buildstate->collation, value);
    }

    /* form vector */
    Vector* vec = InitVector(buildstate->dimensions);
    errno_t rc = memcpy_s(&vec->x[0], buildstate->dimensions * sizeof(float), ((Vector*)DatumGetPointer(value))->x,
                          buildstate->dimensions * sizeof(float));
    securec_check_c(rc, "\0", "\0");

    /* insert into page */
    BlockNumber blkno = InsertVectorIntoPage(index, vec,
                                             -1,  // calculate sqrSum by VectorSquareNorm
                                             heaptid, buildstate);

    BlockNumber currentPage = GetInsertPage(index);
    if (BlockNumberIsValid(blkno) && blkno != currentPage) {
        DiskAnnUpdateMetaPage(index, blkno, MAIN_FORKNUM);
    }

    pfree(vec);
    return blkno;
}

static void AddTupleToSort(Relation index, ItemPointer tid, Datum* values, DiskAnnBuildState* buildstate)
{
    BlockNumber blkno;
    Datum dst = PointerGetDatum(PG_DETOAST_DATUM(values[0]));
    Vector* value = (Vector*)DatumGetPointer(dst);
    float* data = value->x;

    Datum insertValue[1];
    int dim = buildstate->dimensions;
    Vector* insertVector = InitVector(dim);
    errno_t rc = memcpy_s(&insertVector->x[0], dim * sizeof(float), data, dim * sizeof(float));
    securec_check_c(rc, "\0", "\0");
    insertValue[0] = PointerGetDatum(insertVector);

    blkno = InsertTuple(index, insertValue, tid, buildstate);
    buildstate->blocksList.push_back(blkno);
    buildstate->indtuples++;
    pfree(insertVector);
}

static void BuildCallback(Relation index, CALLBACK_ITEM_POINTER, Datum* values, const bool* isnull, bool tupleIsAlive,
                          void* state)
{
    DiskAnnBuildState* buildstate = (DiskAnnBuildState*)state;
    MemoryContext oldCtx;

    ItemPointer tid = &hup->t_self;

    /* Skip nulls */
    if (isnull[0]) {
        return;
    }

    /* Use memory context since detoast can allocate */
    oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);

    /* Add tuple to sort */
    AddTupleToSort(index, tid, values, buildstate);

    /* Reset memory context */
    MemoryContextSwitchTo(oldCtx);
    MemoryContextReset(buildstate->tmpCtx);
}

static double AssignTuples(DiskAnnBuildState* buildstate)
{
    if (buildstate->heap != NULL) {
        Relation heap = buildstate->heap;
        Relation index = buildstate->index;
        IndexInfo* indexInfo = buildstate->indexInfo;
        buildstate->reltuples =
            tableam_index_build_scan(heap, index, indexInfo, true, BuildCallback, (void*)buildstate, NULL);
        return buildstate->reltuples;
    }
    return 0.0;  // default value
}

static void CreateEntryPages(DiskAnnBuildState* buildstate, ForkNumber forkNum)
{
    /* Assign */
    DiskAnnBench("DiskAnn assign tuples", AssignTuples(buildstate));
}

static BlockNumber GenerateFrozenPoint(DiskAnnBuildState* buildstate)
{
    /* select frozen point randomly */
    int pos = gs_random() % buildstate->blocksList.size();
    BlockNumber frozen = buildstate->blocksList[pos];

    float* vec = (float*)palloc(buildstate->dimensions * sizeof(float));
    double sqrSum = 0;
    ItemPointerData hctid;
    buildstate->graphStore->GetVector(frozen, vec, &(sqrSum), &hctid);

    /* form vector */
    Vector* vector = InitVector(buildstate->dimensions);
    errno_t rc =
        memcpy_s(&vector->x[0], buildstate->dimensions * sizeof(float), vec, buildstate->dimensions * sizeof(float));
    securec_check_c(rc, "\0", "\0");

    /* insert into page */
    BlockNumber blkno = InsertVectorIntoPage(buildstate->index, vector,
                                             sqrSum,  // use vec sqrsum
                                             &hctid, buildstate);

    pfree(vector);
    pfree(vec);

    return blkno;
}

static void BuildVamanaIndex(DiskAnnBuildState* buildstate)
{
    BlockNumber frozen = GenerateFrozenPoint(buildstate);

    InsertFrozenPoint(buildstate->index, frozen);

    // todo: graph link
}

void InsertFrozenPoint(Relation index, BlockNumber frozen)
{
    Buffer buf;
    Page page;
    buf = ReadBuffer(index, DISKANN_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    page = BufferGetPage(buf);
    DiskAnnMetaPage metaPage = DiskAnnPageGetMeta(page);
    if (metaPage->nfrozen < FROZEN_POINT_SIZE) {
        metaPage->frozenBlkno[metaPage->nfrozen] = frozen;
        metaPage->nfrozen++;
        MarkBufferDirty(buf);
    }
    UnlockReleaseBuffer(buf);
}

/*
 * Build DiskANN Index
 * 1. Initialize the build state
 * 2. Create Meta page,  MetaPageBlkno = 0
 * 3. Create Entry page, insert vector into index page
 * and reserve node page & edge page for further Vamana build process
 * 4. Build Vamana Index:
 *  Build a frozen point as the starting point for search.
 *  Iterate through vectors stored in the index page,
 *  compute and compare distances between nodes,
 *  and store neighboring nodes' block numbers and distance values in each node's edge page.
 */
static void BuildIndex(Relation heap, Relation index, IndexInfo* indexInfo, DiskAnnBuildState* buildstate,
                       ForkNumber forkNum)
{
    InitBuildState(buildstate, heap, index, indexInfo);

    /* Create pages */
    CreateMetaPage(index, buildstate, forkNum);

    CreateEntryPages(buildstate, forkNum);

    BuildVamanaIndex(buildstate);

    FreeBuildState(buildstate);
}

IndexBuildResult* diskannbuild_internal(Relation heap, Relation index, IndexInfo* indexInfo)
{
    IndexBuildResult* result;
    DiskAnnBuildState buildstate;

    BuildIndex(heap, index, indexInfo, &buildstate, MAIN_FORKNUM);

    result = (IndexBuildResult*)palloc(sizeof(IndexBuildResult));
    result->heap_tuples = buildstate.reltuples;
    result->index_tuples = buildstate.indtuples;

    return result;
}

void diskannbuildempty_internal(Relation index)
{
    IndexInfo* indexInfo = BuildIndexInfo(index);
    DiskAnnBuildState buildstate;

    BuildIndex(NULL, index, indexInfo, &buildstate, INIT_FORKNUM);
}
