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
 * hnsw.h
 *
 * IDENTIFICATION
 *        src/include/access/datavec/hnsw.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef HNSW_H
#define HNSW_H

#include "postgres.h"

#include "access/genam.h"
#include "lib/pairingheap.h"
#include "nodes/execnodes.h"
#include "port.h" /* for random() */
#include "access/datavec/vector.h"
#include "access/datavec/vecindex.h"
#include "access/datavec/utils.h"

#define HNSW_MAX_DIM 2000
#define HNSW_MAX_NNZ 1000

/* Support functions */
#define HNSW_DISTANCE_PROC 1
#define HNSW_NORM_PROC 2
#define HNSW_TYPE_INFO_PROC 3
#define HNSW_KMEANS_NORMAL_PROC 4

#define HNSW_VERSION 1
#define HNSW_MAGIC_NUMBER 0xA953A953
#define HNSW_PAGE_ID 0xFF90

/* Preserved page numbers */
#define HNSW_METAPAGE_BLKNO 0
#define HNSW_HEAD_BLKNO 1                            /* first element page */
#define HNSW_PQTABLE_START_BLKNO 1                   /* pqtable start page */
#define HNSW_PQTABLE_STORAGE_SIZE (uint16)(6 * 1024) /* pqtable storage size in each page */

/* Append page slot info */
#define HNSW_DEFAULT_NPAGES_PER_SLOT 50
#define HNSW_BUFFER_THRESHOLD 4

/* Must correspond to page numbers since page lock is used */
#define HNSW_UPDATE_LOCK 0
#define HNSW_SCAN_LOCK 1

/* HNSW parameters */
#define HNSW_FUNC_NUM 4
#define HNSW_DEFAULT_M 16
#define HNSW_MIN_M 2
#define HNSW_MAX_M 100
#define HNSW_DEFAULT_EF_CONSTRUCTION 64
#define HNSW_MIN_EF_CONSTRUCTION 4
#define HNSW_MAX_EF_CONSTRUCTION 1000
#define HNSW_DEFAULT_EF_SEARCH 40
#define HNSW_MIN_EF_SEARCH 1
#define HNSW_DEFAULT_THRESHOLD INT32_MAX
#define HNSW_MIN_THRESHOLD 160
#define HNSW_MAX_THRESHOLD INT32_MAX
#define HNSW_MAX_EF_SEARCH 1000000

#define HNSW_PQMODE_ADC 1
#define HNSW_PQMODE_SDC 2
#define HNSW_PQMODE_DEFAULT HNSW_PQMODE_ADC
#define HNSW_PQ_DIS_L2 1
#define HNSW_PQ_DIS_IP 2
#define HNSW_PQ_DIS_COSINE 3

/* Tuple types */
#define HNSW_ELEMENT_TUPLE_TYPE 1
#define HNSW_NEIGHBOR_TUPLE_TYPE 2

/* page types */
#define HNSW_DEFAULT_PAGE_TYPE 0
#define HNSW_ELEMENT_PAGE_TYPE 1
#define HNSW_NEIGHBOR_PAGE_TYPE 2
#define HNSW_USTORE_PAGE_TYPE 3

/* Make graph robust against non-HOT updates */
#define HNSW_HEAPTIDS 10

#define HNSW_UPDATE_ENTRY_GREATER 1
#define HNSW_UPDATE_ENTRY_ALWAYS 2

/* Build phases */
/* PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE is 1 */
#define PROGRESS_HNSW_PHASE_LOAD 2

#define PQ_SUCCESS 0
#define PQ_ERROR (-1)

#define HNSWPQ_MAX_PATH_LEN 4096
#ifndef MAX_PATH_LEN
#define MAX_PATH_LEN UWAL_MAX_PATH_LEN
#endif

#define HNSWPQ_DEFAULT_TARGET_ROWS 300

#define PQ_ENV_PATH "DATAVEC_PQ_LIB_PATH"
#define PQ_SO_NAME "libkvecturbo.so"

#define HNSW_MAX_SIZE \
    (BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - MAXALIGN(sizeof(HnswPageOpaqueData)) - sizeof(ItemIdData))
#define HNSW_TUPLE_ALLOC_SIZE BLCKSZ

#define HNSW_ELEMENT_TUPLE_SIZE(size) MAXALIGN(offsetof(HnswElementTupleData, data) + (size))
#define HNSW_NEIGHBOR_TUPLE_SIZE(level, m) \
    MAXALIGN(offsetof(HnswNeighborTupleData, indextids) + ((level) + 2) * (m) * sizeof(ItemPointerData))

#define HNSW_NEIGHBOR_ARRAY_SIZE(lm) (offsetof(HnswNeighborArray, items) + sizeof(HnswCandidate) * (lm))

#define HnswPageGetOpaque(page) ((HnswPageOpaque)PageGetSpecialPointer(page))
#define HnswPageGetMeta(page) ((HnswMetaPageData *)PageGetContents(page))
#define HnswPageGetAppendMeta(page) ((HnswAppendMetaPageData *)PageGetContents(page))

#define HnswDefaultMaxItemSize                                                                              \
    MAXALIGN_DOWN((BLCKSZ - MAXALIGN(SizeOfPageHeaderData + sizeof(ItemIdData) + sizeof(ItemPointerData)) - \
                   MAXALIGN(sizeof(HnswPageOpaqueData))))

#define RandomDouble() (((double)random()) / MAX_RANDOM_VALUE)
#define SeedRandom(seed) srandom(seed)

#define list_delete_last(list) list_truncate(list, list_length(list) - 1)
#define list_sort(list, cmp)                                      \
    do {                                                          \
        ListCell *cell;                                           \
        int i;                                                    \
        int len = list_length(list);                              \
        ListCell **list_arr;                                      \
        List *new_list;                                           \
                                                                  \
        if (len == 0) {                                           \
            list = NIL;                                           \
            return list;                                          \
        }                                                         \
        i = 0;                                                    \
        list_arr = (ListCell **)palloc(sizeof(ListCell *) * len); \
        foreach (cell, list)                                      \
            list_arr[i++] = cell;                                 \
                                                                  \
        qsort(list_arr, len, sizeof(ListCell *), cmp);            \
                                                                  \
        new_list = (List *)palloc(sizeof(List));                  \
        new_list->type = (list->type);                              \
        new_list->length = len;                                   \
        new_list->head = list_arr[len - 1];                       \
        new_list->tail = list_arr[0];                             \
                                                                  \
        for (i = len - 1; i > 0; i--)                             \
            list_arr[i]->next = list_arr[i - 1];                  \
                                                                  \
        list_arr[0]->next = NULL;                                 \
        pfree(list_arr);                                          \
        list = new_list;                                          \
    } while (0)

#define HnswIsElementTuple(tup) ((tup)->type == HNSW_ELEMENT_TUPLE_TYPE)
#define HnswIsNeighborTuple(tup) ((tup)->type == HNSW_NEIGHBOR_TUPLE_TYPE)

/* 2 * M connections for ground layer */
#define HnswGetLayerM(m, layer) ((layer == 0) ? (m) * 2 : (m))

/* Optimal ML from paper */
#define HnswGetMl(m) (1 / log(m))

/* Ensure fits on page and in uint8 */
#define HnswGetMaxLevel(m)                                                                 \
    Min(((BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - MAXALIGN(sizeof(HnswPageOpaqueData)) - \
          offsetof(HnswNeighborTupleData, indextids) - sizeof(ItemIdData)) /               \
         (sizeof(ItemPointerData)) / (m)) -                                                \
            2,                                                                             \
        255)

#define HnswGetValue(base, element) PointerGetDatum(HnswPtrAccess(base, (element)->value))

#define relptr_offset(rp) ((rp).relptr_off - 1)

/* Pointer macros */
#define HnswPtrAccess(base, hp) ((base) == NULL ? (hp).ptr : relptr_access(base, (hp).relptr))
#define HnswPtrStore(base, hp, value) \
    ((base) == NULL ? (void)((hp).ptr = (value)) : (void)relptr_store(base, (hp).relptr, value))
#define HnswPtrIsNull(base, hp) ((base) == NULL ? (hp).ptr == NULL : relptr_is_null((hp).relptr))
#define HnswPtrEqual(base, hp1, hp2) \
    ((base) == NULL ? (hp1).ptr == (hp2).ptr : relptr_offset((hp1).relptr) == relptr_offset((hp2).relptr))

/* For code paths dedicated to each type */
#define HnswPtrPointer(hp) (hp).ptr
#define HnswPtrOffset(hp) relptr_offset((hp).relptr)

/* Variables */
extern int hnsw_lock_tranche_id;

typedef struct HnswElementData HnswElementData;
typedef struct HnswNeighborArray HnswNeighborArray;

#define relptr(type)       \
    union {                \
        type *relptr_type; \
        Size relptr_off;   \
    }

#define relptr_declare(type, relptrtype) typedef relptr(type) (relptrtype)

#ifdef HAVE__BUILTIN_TYPES_COMPATIBLE_P
#define relptr_access(base, rp)                 \
    (AssertVariableIsOfTypeMacro(base, char *), \
     (__typeof__((rp).relptr_type))((rp).relptr_off == 0 ? NULL : (base) + (rp).relptr_off - 1))
#else
/*
 * If we don't have __builtin_types_compatible_p, assume we might not have
 * __typeof__ either.
 */
#define relptr_access(base, rp) \
    (AssertVariableIsOfTypeMacro(base, char *), (void *)((rp).relptr_off == 0 ? NULL : (base) + (rp).relptr_off - 1))
#endif

#define relptr_is_null(rp) ((rp).relptr_off == 0)

#define relptr_offset(rp) ((rp).relptr_off - 1)

/* We use this inline to avoid double eval of "val" in relptr_store */
static inline Size relptr_store_eval(char *base, char *val)
{
    if (val == NULL) {
        return 0;
    } else {
        Assert(val >= base);
        return val - base + 1;
    }
}

#ifdef HAVE__BUILTIN_TYPES_COMPATIBLE_P
#define relptr_store(base, rp, val)                                                                             \
    (AssertVariableIsOfTypeMacro(base, char *), AssertVariableIsOfTypeMacro(val, __typeof__((rp).relptr_type)), \
     (rp).relptr_off = relptr_store_eval((base), (char *)(val)))
#else
/*
 * If we don't have __builtin_types_compatible_p, assume we might not have
 * __typeof__ either.
 */
#define relptr_store(base, rp, val) \
    (AssertVariableIsOfTypeMacro(base, char *), (rp).relptr_off = relptr_store_eval((base), (char *)(val)))
#endif

#define HnswPtrDeclare(type, relptrtype, ptrtype) \
    relptr_declare(type, relptrtype);             \
    typedef union {                               \
        type *ptr;                                \
        relptrtype relptr;                        \
    } (ptrtype);

/* Pointers that can be absolute or relative */
/* Use char for HnswDatumPtr so works with Pointer */
HnswPtrDeclare(HnswElementData, HnswElementRelptr, HnswElementPtr);
HnswPtrDeclare(HnswNeighborArray, HnswNeighborArrayRelptr, HnswNeighborArrayPtr);
HnswPtrDeclare(HnswNeighborArrayPtr, HnswNeighborsRelptr, HnswNeighborsPtr);
HnswPtrDeclare(char, DatumRelptr, HnswDatumPtr);

struct HnswElementData {
    HnswElementPtr next;
    ItemPointerData heaptids[HNSW_HEAPTIDS];
    uint8 heaptidsLength;
    uint8 level;
    uint8 deleted;
    uint32 hash;
    HnswNeighborsPtr neighbors;
    BlockNumber blkno;
    OffsetNumber offno;
    OffsetNumber neighborOffno;
    BlockNumber neighborPage;
    HnswDatumPtr value;
    HnswDatumPtr pqcodes;
    LWLock lock;
    bool fromMmap;
};

typedef HnswElementData *HnswElement;

typedef struct HnswCandidate {
    HnswElementPtr element;
    float distance;
    bool closer;
} HnswCandidate;

struct HnswNeighborArray {
    int length;
    bool closerSet;
    HnswCandidate items[FLEXIBLE_ARRAY_MEMBER];
};

typedef struct HnswPairingHeapNode {
    HnswCandidate *inner;
    pairingheap_node c_node;
    pairingheap_node w_node;
} HnswPairingHeapNode;

/* HNSW index options */
typedef struct HnswOptions {
    int32 vl_len_;      /* varlena header (do not touch directly!) */
    int m;              /* number of connections */
    int efConstruction; /* size of dynamic candidate list */
    bool enablePQ;
    int pqM;            /* number of subquantizer */
    int pqKsub;         /* number of centroids for each subquantizer */
    char *storage_type; /* table access method kind */
} HnswOptions;

typedef struct HnswGraph {
    /* Graph state */
    slock_t lock;
    HnswElementPtr head;
    double indtuples;

    /* Entry state */
    LWLock entryLock;
    LWLock entryWaitLock;
    HnswElementPtr entryPoint;

    /* Allocations state */
    LWLock allocatorLock;
    long memoryUsed;
    long memoryTotal;

    /* Flushed state */
    LWLock flushLock;
    bool flushed;
} HnswGraph;

typedef struct HnswShared {
    /* Immutable state */
    Oid heaprelid;
    Oid indexrelid;
    char *pqTable;
    float *pqDistanceTable;

    /* Mutex for mutable state */
    slock_t mutex;

    /* Mutable state */
    int nparticipantsdone;
    double reltuples;
    HnswGraph graphData;

    char *hnswarea;
    ParallelHeapScanDescData heapdesc;
} HnswShared;

typedef struct HnswLeader {
    int nparticipanttuplesorts;
    HnswShared *hnswshared;
} HnswLeader;

typedef struct HnswAllocator {
    void *(*alloc)(Size size, void *state);
    void *state;
} HnswAllocator;

typedef struct HnswTypeInfo {
    int maxDimensions;
    bool supportPQ;
    Size (*itemSize) (int dimensions);
    Datum (*normalize)(PG_FUNCTION_ARGS);
    void (*checkValue)(Pointer v);
} HnswTypeInfo;

typedef struct HnswBuildState {
    /* Info */
    Relation heap;
    Relation index;
    IndexInfo *indexInfo;
    ForkNumber forkNum;
    const HnswTypeInfo *typeInfo;

    /* Settings */
    int dimensions;
    int m;
    int efConstruction;

    /* Statistics */
    double indtuples;
    double reltuples;

    /* Support functions */
    FmgrInfo *procinfo;
    FmgrInfo *normprocinfo;
    FmgrInfo *kmeansnormprocinfo;
    Oid collation;

    /* Variables */
    HnswGraph graphData;
    HnswGraph *graph;
    double ml;
    int maxLevel;

    /* Memory */
    MemoryContext graphCtx;
    MemoryContext tmpCtx;
    HnswAllocator allocator;

    /* Parallel builds */
    HnswLeader *hnswleader;
    HnswShared *hnswshared;
    char *hnswarea;

    /* PQ info */
    bool enablePQ;
    int pqM;
    int pqKsub;
    char *pqTable;
    Size pqTableSize;
    float *pqDistanceTable;
    uint16 pqcodeSize;
    PQParams *params;
    int pqMode;

    VectorArray samples;
    BlockSamplerData bs;
    double rstate;
    int rowstoskip;

    /* storage page info */
    bool isUStore; /* false means astore */
} HnswBuildState;

typedef struct HnswMetaPageData {
    uint32 magicNumber;
    uint32 version;
    uint32 dimensions;
    uint16 m;
    uint16 efConstruction;
    BlockNumber entryBlkno;
    OffsetNumber entryOffno;
    int16 entryLevel;
    BlockNumber insertPage;

    /* PQ info */
    bool enablePQ;
    uint16 pqM;
    uint16 pqKsub;
    uint16 pqcodeSize;
    uint32 pqTableSize;
    uint16 pqTableNblk;
    uint32 pqDisTableSize; /* SDC */
    uint16 pqDisTableNblk;
} HnswMetaPageData;

typedef HnswMetaPageData *HnswMetaPage;

typedef struct HnswAppendMetaPageData {
    uint32 magicNumber;
    uint32 version;
    uint32 dimensions;
    uint16 m;
    uint16 efConstruction;
    BlockNumber entryBlkno;
    OffsetNumber entryOffno;
    int16 entryLevel;

    /* PQ info */
    bool enablePQ;
    uint16 pqM;             /* number of subquantizer */
    uint16 pqKsub;          /* number of centroids for each subquantizer */
    uint16 pqcodeSize;      /* number of bits per quantization index */
    uint32 pqTableSize;     /* dim * pqKsub * sizeof(float) */
    uint16 pqTableNblk;     /* total number of blks pqtable */
    uint32 pqDisTableSize;  /* SDC */
    uint16 pqDisTableNblk;

    /* slot info */
    int npages; /* number of pages per slot */
    BlockNumber slotStartBlkno;
    BlockNumber elementInsertSlot;  /* the first page of the element type to be inserted into the slot */
    BlockNumber neighborInsertSlot; /* the first page of the neighbor type to be inserted into the slot */
} HnswAppendMetaPageData;

typedef HnswAppendMetaPageData *HnswAppendMetaPage;

typedef struct HnswPageOpaqueData {
    BlockNumber nextblkno;
    uint8 pageType; /* element or neighbor page */
    uint8 unused;
    uint16 page_id; /* for identification of HNSW indexes */
} HnswPageOpaqueData;

typedef HnswPageOpaqueData *HnswPageOpaque;

typedef struct HnswElementTupleData {
    uint8 type;
    uint8 level;
    uint8 deleted;
    uint8 unused;
    ItemPointerData heaptids[HNSW_HEAPTIDS];
    ItemPointerData neighbortid;
    uint16 unused2;
    Vector data;
} HnswElementTupleData;

typedef HnswElementTupleData *HnswElementTuple;

typedef struct HnswNeighborTupleData {
    uint8 type;
    uint8 unused;
    uint16 count;
    ItemPointerData indextids[FLEXIBLE_ARRAY_MEMBER];
} HnswNeighborTupleData;

typedef HnswNeighborTupleData *HnswNeighborTuple;

typedef struct HnswBuildParams {
    /* build params */
    char *base;
    FmgrInfo *procinfo;
    Oid collation;
    int m;
    int ef;
    bool existing;

    /* PQ params */
    bool enablePQ;
    int pqM;
    int pqKsub;
    char *pqTable;
    Size pqTableSize;
    float *pqDistanceTable;
    HnswAllocator *allocator;
} HnswBuildParams;

typedef struct HnswScanOpaqueData {
    const HnswTypeInfo *typeInfo;
    bool first;
    List *w;
    MemoryContext tmpCtx;

    /* Support functions */
    FmgrInfo *procinfo;
    FmgrInfo *normprocinfo;
    Oid collation;

    bool enablePQ;
    PQParams params;
    int pqMode;

    /* used in ustore only */
    VectorScanData vs;
    int length;
    int currentLoc;
    Datum value;
} HnswScanOpaqueData;

typedef HnswScanOpaqueData *HnswScanOpaque;

typedef struct HnswVacuumState {
    /* Info */
    Relation index;
    IndexBulkDeleteResult *stats;
    IndexBulkDeleteCallback callback;
    void *callbackState;
    BlockNumber hnswHeadBlkno;

    /* Settings */
    int m;
    int efConstruction;

    /* Support functions */
    FmgrInfo *procinfo;
    Oid collation;

    /* Variables */
    struct tidhash_hash *deleted;
    BufferAccessStrategy bas;
    HnswNeighborTuple ntup;
    HnswElementData highestPoint;

    /* Memory */
    MemoryContext tmpCtx;
} HnswVacuumState;

typedef struct PQSearchInfo {
    PQParams params;
    int lc;
    int pqMode;
    uint8 *qPQCode;
    float *pqDistanceTable;
} PQSearchInfo;

typedef struct Candidate {
    float *vector;
    float distance;
    void *heaptids;
    uint8 heaptidsLength;
} Candidate;

/* Methods */
int HnswGetM(Relation index);
int HnswGetEfConstruction(Relation index);
bool HnswGetEnablePQ(Relation index);
int HnswGetPqM(Relation index);
int HnswGetPqKsub(Relation index);
FmgrInfo *HnswOptionalProcInfo(Relation index, uint16 procnum);
Datum HnswNormValue(const HnswTypeInfo *typeInfo, Oid collation, Datum value);
bool HnswCheckNorm(FmgrInfo *procinfo, Oid collation, Datum value);
Buffer HnswNewBuffer(Relation index, ForkNumber forkNum);
void HnswInitPage(Buffer buf, Page page);
List *HnswSearchLayer(char *base, Datum q, List *ep, int ef, int lc, Relation index, FmgrInfo *procinfo, Oid collation,
                      int m, bool inserting, HnswElement skipElement, bool tryMmap = false, IndexScanDesc scan = NULL,
                      bool enablePQ = false, PQSearchInfo *pqinfo = NULL);
HnswElement HnswGetEntryPoint(Relation index);
void HnswGetMetaPageInfo(Relation index, int *m, HnswElement *entryPoint);
void *HnswAlloc(HnswAllocator *allocator, Size size);
HnswElement HnswInitElement(char *base, ItemPointer tid, int m, double ml, int maxLevel, HnswAllocator *alloc);
HnswElement HnswInitElementFromBlock(BlockNumber blkno, OffsetNumber offno);
void HnswFindElementNeighbors(char *base, HnswElement element, HnswElement entryPoint, Relation index,
                              FmgrInfo *procinfo, Oid collation, int m, int efConstruction, bool existing,
                              bool enablePQ, PQParams *params);
HnswCandidate *HnswEntryCandidate(char *base, HnswElement em, Datum q, Relation rel, FmgrInfo *procinfo, Oid collation,
                                  bool loadVec, IndexScanDesc scan = NULL, bool enablePQ = false,
                                  PQSearchInfo *pqinfo = NULL);
void HnswUpdateMetaPage(Relation index, int updateEntry, HnswElement entryPoint, BlockNumber insertPage,
                        ForkNumber forkNum, bool building);
void HnswSetNeighborTuple(char *base, HnswNeighborTuple ntup, HnswElement e, int m);
void HnswAddHeapTid(HnswElement element, ItemPointer heaptid);
void HnswInitNeighbors(char *base, HnswElement element, int m, HnswAllocator *alloc);
bool HnswInsertTupleOnDisk(Relation index, Datum value, Datum *values, const bool *isnull, ItemPointer heap_tid,
                           bool building);
void HnswUpdateNeighborsOnDisk(Relation index, FmgrInfo *procinfo, Oid collation, HnswElement e, int m,
                               bool checkExisting, bool building);
void HnswLoadElementFromTuple(HnswElement element, HnswElementTuple etup, bool loadHeaptids, bool loadVec);
bool HnswLoadElement(HnswElement element, float *distance, Datum *q, Relation index, FmgrInfo *procinfo, Oid collation,
                     bool loadVec, float *maxDistance, IndexScanDesc scan = NULL, bool enablePQ = false,
                     PQSearchInfo *pqinfo = NULL);
void HnswSetElementTuple(char *base, HnswElementTuple etup, HnswElement element);
void HnswUpdateConnection(char *base, HnswElement element, HnswCandidate *hc, int lm, int lc, int *updateIdx,
                          Relation index, FmgrInfo *procinfo, Oid collation);
void HnswLoadNeighbors(HnswElement element, Relation index, int m);
const HnswTypeInfo *HnswGetTypeInfo(Relation index);
bool HnswDelete(Relation index, Datum *values, const bool *isnull, ItemPointer heapTCtid, bool isRollbackIndex);

void HnswUpdateAppendMetaPage(Relation index, int updateEntry, HnswElement entryPoint, BlockNumber eleInsertPage,
                              BlockNumber neiInsertPage, ForkNumber forkNum, bool building);
void FlushPQInfo(HnswBuildState *buildstate);
void HnswGetPQInfoFromMetaPage(Relation index, uint16 *pqTableNblk, uint32 *pqTableSize,
                               uint16 *pqDisTableNblk, uint32 *pqDisTableSize);

int ComputePQTable(VectorArray samples, PQParams *params);
int ComputeVectorPQCode(float *vector, const PQParams *params, uint8 *pqCode);
int GetPQDistanceTableSdc(const PQParams *params, float *pqDistanceTable);
int GetPQDistanceTableAdc(float *vector, const PQParams *params, float *pqDistanceTable);
int GetPQDistance(const uint8 *basecode, const uint8 *querycode, const PQParams *params,
                  const float *pqDistanceTable, float *pqDistance);
int getPQfunctionType(FmgrInfo *procinfo, FmgrInfo *normprocinfo);
void InitPQParamsOnDisk(PQParams *params, Relation index, FmgrInfo *procinfo, int dim, bool *enablePQ, bool trymmap);

Datum hnswhandler(PG_FUNCTION_ARGS);
Datum hnswbuild(PG_FUNCTION_ARGS);
Datum hnswbuildempty(PG_FUNCTION_ARGS);
Datum hnswinsert(PG_FUNCTION_ARGS);
Datum hnswbulkdelete(PG_FUNCTION_ARGS);
Datum hnswvacuumcleanup(PG_FUNCTION_ARGS);
Datum hnswcostestimate(PG_FUNCTION_ARGS);
Datum hnswoptions(PG_FUNCTION_ARGS);
Datum hnswvalidate(PG_FUNCTION_ARGS);
Datum hnswbeginscan(PG_FUNCTION_ARGS);
Datum hnswrescan(PG_FUNCTION_ARGS);
Datum hnswgettuple(PG_FUNCTION_ARGS);
Datum hnswendscan(PG_FUNCTION_ARGS);
Datum hnswdelete(PG_FUNCTION_ARGS);
Datum hnsw_halfvec_support(PG_FUNCTION_ARGS);
Datum hnsw_bit_support(PG_FUNCTION_ARGS);
Datum hnsw_sparsevec_support(PG_FUNCTION_ARGS);

/* Index access methods */
IndexBuildResult *hnswbuild_internal(Relation heap, Relation index, IndexInfo *indexInfo);
void hnswbuildempty_internal(Relation index);
bool hnswinsert_internal(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid, Relation heap,
                         IndexUniqueCheck checkUnique);
IndexBulkDeleteResult *hnswbulkdelete_internal(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
                                               IndexBulkDeleteCallback callback, void *callbackState);
IndexBulkDeleteResult *hnswvacuumcleanup_internal(IndexVacuumInfo *info, IndexBulkDeleteResult *stats);
IndexScanDesc hnswbeginscan_internal(Relation index, int nkeys, int norderbys);
void hnswrescan_internal(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys);
bool hnswgettuple_internal(IndexScanDesc scan, ScanDirection dir);
void hnswendscan_internal(IndexScanDesc scan);
bool hnswdelete_internal(Relation index, Datum *values, const bool *isnull, ItemPointer heapTCtid,
                         bool isRollbackIndex);

static inline HnswNeighborArray *HnswGetNeighbors(char *base, HnswElement element, int lc)
{
    HnswNeighborArrayPtr *neighborList = (HnswNeighborArrayPtr *)HnswPtrAccess(base, element->neighbors);

    Assert(element->level >= lc);

    return (HnswNeighborArray *)HnswPtrAccess(base, neighborList[lc]);
}

/* Hash tables */
typedef struct TidHashEntry {
    ItemPointerData tid;
    char status;
} TidHashEntry;

#define SH_PREFIX tidhash
#define SH_ELEMENT_TYPE TidHashEntry
#define SH_KEY_TYPE ItemPointerData
#define SH_SCOPE extern
#define SH_DECLARE
#include "lib/simplehash.h"

typedef struct PointerHashEntry {
    uintptr_t ptr;
    char status;
} PointerHashEntry;

#define SH_PREFIX pointerhash
#define SH_ELEMENT_TYPE PointerHashEntry
#define SH_KEY_TYPE uintptr_t
#define SH_SCOPE extern
#define SH_DECLARE
#include "lib/simplehash.h"

typedef struct OffsetHashEntry {
    Size offset;
    char status;
} OffsetHashEntry;

#define SH_PREFIX offsethash
#define SH_ELEMENT_TYPE OffsetHashEntry
#define SH_KEY_TYPE Size
#define SH_SCOPE extern
#define SH_DECLARE
#include "lib/simplehash.h"
typedef union {
    pointerhash_hash *pointers;
    offsethash_hash *offsets;
    tidhash_hash *tids;
} VisitedHash;

HnswCandidate *MMapEntryCandidate(char *base, HnswElement entryPoint, Datum q, Relation index, FmgrInfo *procinfo, Oid collation,
                                    bool loadVec, IndexScanDesc scan = NULL, bool enablePQ = false, PQSearchInfo *pqinfo = NULL);

uint8* LoadPQcode(HnswElementTuple tuple);
bool MmapLoadElement(HnswElement element, float *distance, Datum *q, Relation index, FmgrInfo *procinfo, Oid collation,
                     bool loadVec, float *maxDistance, IndexScanDesc scan, bool enablePQ, PQSearchInfo *pqinfo);
void HnswLoadUnvisitedFromMmap(HnswElement element, HnswElement *unvisited, int *unvisitedLength,
                          VisitedHash *v, Relation index, int m, int lm, int lc);
void HnswLoadUnvisitedFromDisk(HnswElement element, HnswElement *unvisited, int *unvisitedLength,
                          VisitedHash *v, Relation index, int m, int lm, int lc);
#endif
