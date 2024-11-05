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
 * ivfflat.h
 *
 * IDENTIFICATION
 *        src/include/access/datavec/ivfflat.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef IVFFLAT_H
#define IVFFLAT_H

#include "postgres.h"

#include "access/genam.h"
#include "access/generic_xlog.h"
#include "catalog/pg_operator.h"
#include "lib/pairingheap.h"
#include "nodes/execnodes.h"
#include "port.h" /* for random() */
#include "sampling.h"
#include "utils/tuplesort.h"
#include "access/datavec/vector.h"
#include "postmaster/bgworker.h"

#ifdef IVFFLAT_BENCH
#include "portability/instr_time.h"
#endif

#define IVFFLAT_MAX_DIM 2000

/* Support functions */
#define IVFFLAT_DISTANCE_PROC 1
#define IVFFLAT_NORM_PROC 2
#define IVFFLAT_KMEANS_DISTANCE_PROC 3
#define IVFFLAT_KMEANS_NORM_PROC 4
#define IVFFLAT_TYPE_INFO_PROC 5

#define IVFFLAT_VERSION 1
#define IVFFLAT_MAGIC_NUMBER 0x14FF1A7
#define IVFFLAT_PAGE_ID 0xFF84

/* Preserved page numbers */
#define IVFFLAT_METAPAGE_BLKNO 0
#define IVFFLAT_HEAD_BLKNO 1 /* first list page */

/* IVFFlat parameters */
#define IVFFLAT_DEFAULT_LISTS 100
#define IVFFLAT_MIN_LISTS 1
#define IVFFLAT_MAX_LISTS 32768
#define IVFFLAT_DEFAULT_PROBES 1

/* Build phases */
/* PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE is 1 */
#define PROGRESS_IVFFLAT_PHASE_KMEANS 2
#define PROGRESS_IVFFLAT_PHASE_ASSIGN 3
#define PROGRESS_IVFFLAT_PHASE_LOAD 4

#define IVFFLAT_LIST_SIZE(size) (offsetof(IvfflatListData, center) + (size))

#define IvfflatPageGetOpaque(page) ((IvfflatPageOpaque)PageGetSpecialPointer(page))
#define IvfflatPageGetMeta(page) ((IvfflatMetaPageData *)PageGetContents(page))

#ifdef IVFFLAT_BENCH
#define IvfflatBench(name, code)                                            \
    do {                                                                    \
        instr_time start;                                                   \
        instr_time duration;                                                \
        INSTR_TIME_SET_CURRENT(start);                                      \
        (code);                                                             \
        INSTR_TIME_SET_CURRENT(duration);                                   \
        INSTR_TIME_SUBTRACT(duration, start);                               \
        elog(INFO, "%s: %.3f ms", name, INSTR_TIME_GET_MILLISEC(duration)); \
    } while (0)
#else
#define IvfflatBench(name, code) (code)
#endif

#define RandomDouble() (((double)random()) / MAX_RANDOM_VALUE)
#define RandomInt() random()

typedef struct VectorArrayData {
    int length;
    int maxlen;
    int dim;
    Size itemsize;
    char *items;
} VectorArrayData;

typedef VectorArrayData *VectorArray;

typedef struct ListInfo {
    BlockNumber blkno;
    OffsetNumber offno;
} ListInfo;

/* IVFFlat index options */
typedef struct IvfflatOptions {
    int32 vl_len_; /* varlena header (do not touch directly!) */
    int lists;     /* number of lists */
} IvfflatOptions;

typedef struct IvfflatSpool {
    Tuplesortstate *sortstate;
    Relation heap;
    Relation index;
} IvfflatSpool;

typedef struct IvfflatShared {
    /* Immutable state */
    Oid heaprelid;
    Oid indexrelid;
    int scantuplesortstates;

    /* Mutex for mutable state */
    slock_t mutex;

    /* Mutable state */
    int nparticipantsdone;
    double reltuples;
    double indtuples;

    Sharedsort *sharedsort;
    Vector *ivfcenters;
    int workmem;

#ifdef IVFFLAT_KMEANS_DEBUG
    double inertia;
#endif
    ParallelHeapScanDescData heapdesc;  // must come last
} IvfflatShared;

#define ParallelTableScanFromIvfflatShared(shared) \
    (ParallelTableScanDesc)((char *)(shared) + BUFFERALIGN(sizeof(IvfflatShared)))

typedef struct IvfflatLeader {
    int nparticipanttuplesorts;
    IvfflatShared *ivfshared;
} IvfflatLeader;

typedef struct IvfflatTypeInfo {
    int maxDimensions;
    Datum (*normalize)(PG_FUNCTION_ARGS);
    Size (*itemSize)(int dimensions);
    void (*updateCenter)(Pointer v, int dimensions, float *x);
    void (*sumCenter)(Pointer v, float *x);
} IvfflatTypeInfo;

typedef struct IvfflatBuildState {
    /* Info */
    Relation heap;
    Relation index;
    IndexInfo *indexInfo;
    const IvfflatTypeInfo *typeInfo;

    /* Settings */
    int dimensions;
    int lists;

    /* Statistics */
    double indtuples;
    double reltuples;

    /* Support functions */
    FmgrInfo *procinfo;
    FmgrInfo *normprocinfo;
    FmgrInfo *kmeansnormprocinfo;
    Oid collation;

    /* Variables */
    VectorArray samples;
    VectorArray centers;
    ListInfo *listInfo;

#ifdef IVFFLAT_KMEANS_DEBUG
    double inertia;
    double *listSums;
    int *listCounts;
#endif

    /* Sampling */
    BlockSamplerData bs;
    double rstate;
    int rowstoskip;

    /* Sorting */
    Tuplesortstate *sortstate;
    TupleDesc tupdesc;
    TupleTableSlot *slot;

    /* Memory */
    MemoryContext tmpCtx;

    /* Parallel builds */
    IvfflatLeader *ivfleader;
} IvfflatBuildState;

typedef struct IvfflatMetaPageData {
    uint32 magicNumber;
    uint32 version;
    uint16 dimensions;
    uint16 lists;
} IvfflatMetaPageData;

typedef IvfflatMetaPageData *IvfflatMetaPage;

typedef struct IvfflatPageOpaqueData {
    BlockNumber nextblkno;
    uint16 unused;
    uint16 page_id; /* for identification of IVFFlat indexes */
} IvfflatPageOpaqueData;

typedef IvfflatPageOpaqueData *IvfflatPageOpaque;

typedef struct IvfflatListData {
    BlockNumber startPage;
    BlockNumber insertPage;
    Vector center;
} IvfflatListData;

typedef IvfflatListData *IvfflatList;

typedef struct IvfflatScanList {
    pairingheap_node ph_node;
    BlockNumber startPage;
    double distance;
} IvfflatScanList;

typedef struct IvfflatScanOpaqueData {
    const IvfflatTypeInfo *typeInfo;
    int probes;
    int dimensions;
    bool first;

    /* Sorting */
    Tuplesortstate *sortstate;
    TupleDesc tupdesc;
    TupleTableSlot *slot;
    bool isnull;

    /* Support functions */
    FmgrInfo *procinfo;
    FmgrInfo *normprocinfo;
    Oid collation;
    Datum (*distfunc)(FmgrInfo *flinfo, Oid collation, Datum arg1, Datum arg2);

    /* Lists */
    pairingheap *listQueue;
    IvfflatScanList lists[FLEXIBLE_ARRAY_MEMBER]; /* must come last */
} IvfflatScanOpaqueData;

typedef IvfflatScanOpaqueData *IvfflatScanOpaque;

#define VECTOR_ARRAY_SIZE(_length, _size) (sizeof(VectorArrayData) + (_length) * MAXALIGN(_size))

/* Use functions instead of macros to avoid double evaluation */

static inline Pointer VectorArrayGet(VectorArray arr, int offset)
{
    return ((char *)arr->items) + (offset * arr->itemsize);
}

static inline void VectorArraySet(VectorArray arr, int offset, Pointer val)
{
    errno_t rc = memcpy_s(VectorArrayGet(arr, offset),  VARSIZE_ANY(val), val, VARSIZE_ANY(val));
    securec_check(rc, "\0", "\0");
}

/* Methods */
VectorArray VectorArrayInit(int maxlen, int dimensions, Size itemsize);
void VectorArrayFree(VectorArray arr);
void IvfflatKmeans(Relation index, VectorArray samples, VectorArray centers, const IvfflatTypeInfo *typeInfo);
FmgrInfo *IvfflatOptionalProcInfo(Relation index, uint16 procnum);
Datum IvfflatNormValue(const IvfflatTypeInfo *typeInfo, Oid collation, Datum value);
bool IvfflatCheckNorm(FmgrInfo *procinfo, Oid collation, Datum value);
int IvfflatGetLists(Relation index);
void IvfflatGetMetaPageInfo(Relation index, int *lists, int *dimensions);
void IvfflatUpdateList(Relation index, ListInfo listInfo, BlockNumber insertPage, BlockNumber originalInsertPage,
                       BlockNumber startPage, ForkNumber forkNum);
void IvfflatCommitBuffer(Buffer buf, GenericXLogState *state);
void IvfflatAppendPage(Relation index, Buffer *buf, Page *page, GenericXLogState **state, ForkNumber forkNum);
Buffer IvfflatNewBuffer(Relation index, ForkNumber forkNum);
void IvfflatInitPage(Buffer buf, Page page);
void IvfflatInitRegisterPage(Relation index, Buffer *buf, Page *page, GenericXLogState **state);
PGDLLEXPORT void IvfflatParallelBuildMain(const BgWorkerContext *bwc);
void IvfflatInit(void);
const IvfflatTypeInfo *IvfflatGetTypeInfo(Relation index);

Datum ivfflathandler(PG_FUNCTION_ARGS);
Datum ivfflatbuild(PG_FUNCTION_ARGS);
Datum ivfflatbuildempty(PG_FUNCTION_ARGS);
Datum ivfflatinsert(PG_FUNCTION_ARGS);
Datum ivfflatbulkdelete(PG_FUNCTION_ARGS);
Datum ivfflatvacuumcleanup(PG_FUNCTION_ARGS);
Datum ivfflatcostestimate(PG_FUNCTION_ARGS);
Datum ivfflatoptions(PG_FUNCTION_ARGS);
Datum ivfflatvalidate(PG_FUNCTION_ARGS);
Datum ivfflatbeginscan(PG_FUNCTION_ARGS);
Datum ivfflatrescan(PG_FUNCTION_ARGS);
Datum ivfflatgettuple(PG_FUNCTION_ARGS);
Datum ivfflatendscan(PG_FUNCTION_ARGS);
Datum ivfflat_halfvec_support(PG_FUNCTION_ARGS);
Datum ivfflat_bit_support(PG_FUNCTION_ARGS);

/* Index access methods */
IndexBuildResult *ivfflatbuild_internal(Relation heap, Relation index, IndexInfo *indexInfo);
void ivfflatbuildempty_internal(Relation index);
bool ivfflatinsert_internal(Relation index, Datum *values, const bool *isnull, ItemPointer heap_tid, Relation heap,
                            IndexUniqueCheck checkUnique);
IndexBulkDeleteResult *ivfflatbulkdelete_internal(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
                                                  IndexBulkDeleteCallback callback, void *callbackState);
IndexBulkDeleteResult *ivfflatvacuumcleanup_internal(IndexVacuumInfo *info, IndexBulkDeleteResult *stats);
IndexScanDesc ivfflatbeginscan_internal(Relation index, int nkeys, int norderbys);
void ivfflatrescan_internal(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys);
bool ivfflatgettuple_internal(IndexScanDesc scan, ScanDirection dir);
void ivfflatendscan_internal(IndexScanDesc scan);

#endif
