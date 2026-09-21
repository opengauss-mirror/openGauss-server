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
 * hnswutils.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/hnswutils.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include <cmath>

#include "access/tableam.h"
#include "access/generic_xlog.h"
#include "catalog/pg_type.h"
#include "catalog/index.h"
#include "fmgr.h"
#include "access/datavec/hnsw.h"
#include "access/datavec/hnsw_vector_storage.h"
#include "access/datavec/vector_buffer.h"
#include "access/datavec/vector_storage.h"
#include "access/relscan.h"
#include "lib/pairingheap.h"
#include "access/datavec/halfvec.h"
#include "access/datavec/sparsevec.h"
#include "access/datavec/utils.h"
#include "storage/buf/bufmgr.h"
#include "catalog/pg_partition.h"
#include "catalog/pg_partition_fn.h"
#include "utils/datum.h"
#include "utils/partcache.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"

#include "utils/hashutils.h"
#include "access/datavec/hnswlsg.h"

static inline uint64 murmurhash64(uint64 data)
{
    uint64 h = data;

    h ^= h >> 33;
    h *= 0xff51afd7ed558ccd;
    h ^= h >> 33;
    h *= 0xc4ceb9fe1a85ec53;
    h ^= h >> 33;

    return h;
}

/* TID hash table */
static uint32 hash_tid(ItemPointerData tid)
{
    union {
        uint64 i;
        ItemPointerData tid;
    } x;

    /* Initialize unused bytes */
    x.i = 0;
    x.tid = tid;

    return murmurhash64(x.i);
}

#define VALGRIND_MAKE_MEM_DEFINED(addr, size) \
    do {                                      \
    } while (0)

#define SH_PREFIX tidhash
#define SH_ELEMENT_TYPE TidHashEntry
#define SH_KEY_TYPE ItemPointerData
#define SH_KEY tid
#define SH_HASH_KEY(tb, key) hash_tid(key)
#define SH_EQUAL(tb, a, b) ItemPointerEquals(&(a), &(b))
#define SH_SCOPE extern
#define SH_DEFINE
#include "lib/simplehash.h"

/* Pointer hash table */
static uint32 hash_pointer(uintptr_t ptr)
{
#if SIZEOF_VOID_P == 8
    return murmurhash64((uint64)ptr);
#else
    return murmurhash32((uint32)ptr);
#endif
}

#define SH_PREFIX pointerhash
#define SH_ELEMENT_TYPE PointerHashEntry
#define SH_KEY_TYPE uintptr_t
#define SH_KEY ptr
#define SH_HASH_KEY(tb, key) hash_pointer(key)
#define SH_EQUAL(tb, a, b) ((a) == (b))
#define SH_SCOPE extern
#define SH_DEFINE
#include "lib/simplehash.h"

/* Offset hash table */
static uint32 hash_offset(Size offset)
{
#if SIZEOF_SIZE_T == 8
    return murmurhash64((uint64)offset);
#else
    return murmurhash32((uint32)offset);
#endif
}

#define SH_PREFIX offsethash
#define SH_ELEMENT_TYPE OffsetHashEntry
#define SH_KEY_TYPE Size
#define SH_KEY offset
#define SH_HASH_KEY(tb, key) hash_offset(key)
#define SH_EQUAL(tb, a, b) ((a) == (b))
#define SH_SCOPE extern
#define SH_DEFINE
#include "lib/simplehash.h"

/*
 * Get the max number of connections in an upper layer for each element in the index
 */
int HnswGetM(Relation index)
{
    HnswOptions *opts = (HnswOptions *)index->rd_options;

    if (opts)
        return opts->m;

    return HNSW_DEFAULT_M;
}

/*
 * Get the size of the dynamic candidate list in the index
 */
int HnswGetEfConstruction(Relation index)
{
    HnswOptions *opts = (HnswOptions *)index->rd_options;

    if (opts)
        return opts->efConstruction;

    return HNSW_DEFAULT_EF_CONSTRUCTION;
}

/*
 * Get whether to enable PQ
 */
bool HnswGetEnablePQ(Relation index)
{
    HnswOptions *opts = (HnswOptions *)index->rd_options;

    if (opts) {
        return opts->enablePQ;
    }

    return GENERIC_DEFAULT_ENABLE_PQ;
}

bool HnswGetEnableLsg(Relation index)
{
    HnswOptions *opts = (HnswOptions *)index->rd_options;

    if (opts) {
        return opts->enableLsg;
    }

    return GENERIC_DEFAULT_USE_LSG;
}

int HnswGetLsgDegree(Relation index)
{
    HnswOptions *opts = (HnswOptions *)index->rd_options;

    if (opts) {
        return opts->lsgDegree;
    }

    return GENERIC_DEFAULT_LSG_DEGREE;
}

double HnswGetLsgAlpha(Relation index)
{
    HnswOptions *opts = (HnswOptions *)index->rd_options;

    if (opts) {
        return opts->lsgAlpha;
    }

    return GENERIC_DEFAULT_LSG_ALPHA;
}

bool HnswGetEnableMMap(Relation index)
{
    HnswOptions *opts = (HnswOptions *)index->rd_options;

    if (opts) {
        return opts->useMmap;
    }

    return GENERIC_DEFAULT_USE_MMAP;
}

/*
 * Get the number of subquantizer
 */
int HnswGetPqM(Relation index)
{
    HnswOptions *opts = (HnswOptions *)index->rd_options;

    if (opts) {
        return opts->pqM;
    }

    return GENERIC_DEFAULT_PQ_M;
}

/*
 * Get the number of centroids for each subquantizer
 */
int HnswGetPqKsub(Relation index)
{
    HnswOptions *opts = (HnswOptions *)index->rd_options;

    if (opts) {
        return opts->pqKsub;
    }

    return GENERIC_DEFAULT_PQ_KSUB;
}

/*
 * Get whether to enable RabitQ
 */
bool HnswGetEnableRabitQ(Relation index)
{
    HnswOptions *opts = (HnswOptions *)index->rd_options;

    if (opts) {
        return opts->enableRabitQ;
    }

    return GENERIC_DEFAULT_ENABLE_RABITQ;
}

/*
 * Get whether to enable FHT Matrix
 */
bool HnswGetUseFHT(Relation index)
{
    HnswOptions *opts = (HnswOptions *)index->rd_options;

    if (opts) {
        return opts->rabitqFHT;
    }

    return GENERIC_DEFAULT_USE_FHT;
}

/*
 * Get refine type
 */
RefineType HnswGetRefineType(Relation index)
{
    HnswOptions *opts = (HnswOptions *)index->rd_options;

    if (opts) {
        char *rrt = (char *)HnswOptionsGetStringData(opts, rabitqRT, RABITQ_REFINE_TYPE_SQ8);
        if (pg_strcasecmp(rrt, RABITQ_REFINE_TYPE_SQ8) == 0) {
            return SQ8;
        } else if (pg_strcasecmp(rrt, RABITQ_REFINE_TYPE_FP32) == 0) {
            return FP32;
        }else if (pg_strcasecmp(rrt, RABITQ_REFINE_TYPE_NONE) == 0) {
            return NotRefine;
        }
    }

    return GENERIC_DEFAULT_REFINE_TYPE;
}

/*
 * Get proc
 */
FmgrInfo *HnswOptionalProcInfo(Relation index, uint16 procnum)
{
    if (!OidIsValid(index_getprocid(index, 1, procnum)))
        return NULL;

    return index_getprocinfo(index, 1, procnum);
}

/*
 * Normalize value
 */
Datum HnswNormValue(const HnswTypeInfo *typeInfo, Oid collation, Datum value)
{
    return DirectFunctionCall1Coll(typeInfo->normalize, collation, value);
}

/*
 * Check if non-zero norm
 */
bool HnswCheckNorm(FmgrInfo *procinfo, Oid collation, Datum value)
{
    return DatumGetFloat8(FunctionCall1Coll(procinfo, collation, value)) > 0;
}

/*
 * New buffer
 */
Buffer HnswNewBuffer(Relation index, ForkNumber forkNum, LWLock *buildExtensionLock)
{
    Buffer volatile buf = InvalidBuffer;
    bool volatile lockHeld = false;

    PG_TRY();
    {
        if (buildExtensionLock != NULL) {
            LWLockAcquire(buildExtensionLock, LW_EXCLUSIVE);
            lockHeld = true;
        }
        buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        if (lockHeld) {
            LWLockRelease(buildExtensionLock);
            lockHeld = false;
        }
    }
    PG_CATCH();
    {
        if (lockHeld) {
            LWLockRelease(buildExtensionLock);
        }
        if (BufferIsValid(buf)) {
            ReleaseBuffer(buf);
        }
        PG_RE_THROW();
    }
    PG_END_TRY();

    return buf;
}

/*
 * Init page
 */
void HnswInitPage(Buffer buf, Page page)
{
    PageInit(page, BufferGetPageSize(buf), sizeof(HnswPageOpaqueData));
    HnswPageGetOpaque(page)->nextblkno = InvalidBlockNumber;
    HnswPageGetOpaque(page)->pageType = HNSW_DEFAULT_PAGE_TYPE;
    HnswPageGetOpaque(page)->page_id = HNSW_PAGE_ID;
}

/*
 * Allocate a neighbor array
 */
static HnswNeighborArray *HnswInitNeighborArray(int lm, HnswAllocator *allocator)
{
    HnswNeighborArray *a = (HnswNeighborArray *)HnswAlloc(allocator, HNSW_NEIGHBOR_ARRAY_SIZE(lm));

    a->length = 0;
    a->closerSet = false;
    return a;
}

/*
 * Allocate neighbors
 */
void HnswInitNeighbors(char *base, HnswElement element, int m, HnswAllocator *allocator)
{
    int level = element->level;
    HnswNeighborArrayPtr *neighborList =
        (HnswNeighborArrayPtr *)HnswAlloc(allocator, sizeof(HnswNeighborArrayPtr) * (level + 1));

    HnswPtrStore(base, element->neighbors, neighborList);

    for (int lc = 0; lc <= level; lc++)
        HnswPtrStore(base, neighborList[lc], HnswInitNeighborArray(HnswGetLayerM(m, lc), allocator));
}

/*
 * Allocate memory from the allocator
 */
void *HnswAlloc(HnswAllocator *allocator, Size size)
{
    if (allocator)
        return (*(allocator)->alloc)(size, (allocator)->state);

    return palloc(size);
}

/*
 * Allocate an element
 */
int HnswChooseLevel(double ml, int maxLevel)
{
    int level = static_cast<int>(-log(RandomDouble()) * ml);
    /* Cap level */
    if (level > maxLevel) {
        level = maxLevel;
    }

    return level;
}

HnswElement HnswInitElementWithLevel(char *base, ItemPointer heaptid, int m, int level, HnswAllocator *allocator)
{
    HnswElement element = (HnswElement)HnswAlloc(allocator, sizeof(HnswElementData));

    MemSet(element, 0, sizeof(HnswElementData));
    element->heaptidsLength = 0;
    HnswAddHeapTid(element, heaptid);

    element->level = level;
    element->deleted = 0;
    /* Start at one to make it easier to find issues */
    element->version = 1;

    HnswInitNeighbors(base, element, m, allocator);

    HnswPtrStore(base, element->value, (Pointer)NULL);
    HnswPtrStore(base, element->pqcodes, (Pointer)NULL);
    HnswPtrStore(base, element->rbqcodes, (Pointer)NULL);
    element->fromMmap = false;

    return element;
}

HnswElement HnswInitElement(char *base, ItemPointer heaptid, int m, double ml, int maxLevel, HnswAllocator *allocator)
{
    int level = HnswChooseLevel(ml, maxLevel);

    return HnswInitElementWithLevel(base, heaptid, m, level, allocator);
}

/*
 * Add a heap TID to an element
 */
void HnswAddHeapTid(HnswElement element, const ItemPointerData *heaptid)
{
    element->heaptids[element->heaptidsLength++] = *heaptid;
}

/*
 * Allocate an element from block and offset numbers
 */
HnswElement HnswInitElementFromBlock(BlockNumber blkno, OffsetNumber offno)
{
    HnswElement element = (HnswElement)palloc0(sizeof(HnswElementData));
    char *base = NULL;

    element->blkno = blkno;
    element->offno = offno;
    HnswPtrStore(base, element->neighbors, (HnswNeighborArrayPtr *)NULL);
    HnswPtrStore(base, element->value, (Pointer)NULL);
    HnswPtrStore(base, element->pqcodes, (Pointer)NULL);
    HnswPtrStore(base, element->rbqcodes, (Pointer)NULL);
    element->fromMmap = false;
    return element;
}

/*
 * True only for the parent local partitioned index catalog entry in pg_class,
 * which has no on-disk HNSW metapage.  Index partition dummy Relations from
 * partitionGetRelation() map rd_node to part->pd_node and must read metapage.
 */
static bool HnswIndexRelationSkipsPhysicalMetapage(Relation index)
{
    return RelationIsIndex(index) &&
           RelationIsPartitioned(index) &&
           !RelationIsGlobalIndex(index);
}

bool HnswRelationHasVectorPayloadStorage(
    Relation index, BlockNumber *payloadInsertBlkno, uint32 *payloadLen)
{
    Buffer buf;
    Page page;
    HnswMetaPage metap;
    HnswVectorStorageMetaLayout layout;
    uint32 attestedPayloadLen = 0;

    if (index == NULL || HnswIndexRelationSkipsPhysicalMetapage(index)) {
        return false;
    }

    buf = ReadBuffer(index, HNSW_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    metap = HnswPageGetMeta(page);
    if (unlikely(metap->magicNumber != HNSW_MAGIC_NUMBER)) {
        UnlockReleaseBuffer(buf);
        elog(ERROR, "hnsw index is not valid");
    }

    layout = HnswClassifyVectorStorageMeta(metap, &attestedPayloadLen);
    if (layout == HNSW_VECTOR_STORAGE_META_INVALID) {
        UnlockReleaseBuffer(buf);
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("hnsw vector storage metapage for relation \"%s\" is malformed",
                   RelationGetRelationName(index))));
    }
    if (layout == HNSW_VECTOR_STORAGE_META_V2) {
        if (!BlockNumberIsValid(metap->payloadInsertBlkno)) {
            UnlockReleaseBuffer(buf);
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("hnsw vector storage metapage for relation \"%s\" has no payload insert page",
                    RelationGetRelationName(index))));
        }
        if (payloadInsertBlkno != NULL) {
            *payloadInsertBlkno = metap->payloadInsertBlkno;
        }
        if (payloadLen != NULL) {
            *payloadLen = attestedPayloadLen;
        }
    }
    UnlockReleaseBuffer(buf);
    return layout == HNSW_VECTOR_STORAGE_META_V2;
}

static const char *HnswGraphRelationName(Relation index)
{
    return index != NULL ? RelationGetRelationName(index) : "<unknown>";
}

static bool HnswGraphPageGeometryIsValid(Page page)
{
    PageHeader header;

    if (page == NULL) {
        return false;
    }
    header = (PageHeader)page;
    if (!PageHeaderIsValid(header) || PageGetPageSize(page) != BLCKSZ) {
        return false;
    }
    if (PageGetSpecialSize(page) != MAXALIGN(sizeof(HnswPageOpaqueData))) {
        return false;
    }
    return header->pd_lower >= SizeOfPageHeaderData &&
        (header->pd_lower - SizeOfPageHeaderData) % sizeof(ItemIdData) == 0 &&
        header->pd_lower <= header->pd_upper &&
        header->pd_upper <= header->pd_special &&
        header->pd_special <= BLCKSZ &&
        header->pd_special >= SizeOfPageHeaderData &&
        header->pd_special == MAXALIGN(header->pd_special);
}

static void HnswCheckVectorStorageGraphPage(Relation index, Page page, BlockNumber blkno)
{
    if (!HnswGraphPageGeometryIsValid(page)) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("hnsw graph page for relation \"%s\" at block %u has invalid physical layout",
                   HnswGraphRelationName(index), blkno)));
    }
    if (HnswPageGetRole(page) == HNSW_PAGE_ROLE_GRAPH) {
        return;
    }

    ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
        errmsg("hnsw graph traversal for relation \"%s\" reached payload page at block %u",
               HnswGraphRelationName(index), blkno)));
}

static bool HnswGraphTupleInvalid(bool raiseError, Relation index, BlockNumber blkno,
    OffsetNumber offno, const char *reason)
{
    if (raiseError) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("invalid HNSW graph element tuple in relation \"%s\" at block %u offset %u: %s",
                   HnswGraphRelationName(index), blkno, (uint32)offno, reason)));
    }
    return false;
}

typedef struct HnswGraphTupleLocation {
    Relation index;
    BlockNumber blkno;
    OffsetNumber offno;
    bool raiseError;
} HnswGraphTupleLocation;

static bool HnswLocateGraphElementTupleInternal(Page page, const HnswGraphTupleLocation *location,
    HnswElementTuple *tuple)
{
    Relation index = location->index;
    BlockNumber blkno = location->blkno;
    OffsetNumber offno = location->offno;
    bool raiseError = location->raiseError;
    PageHeader header = NULL;
    ItemId itemId = NULL;
    HnswElementTuple elementTuple = NULL;
    Size itemLen;
    LocationIndex itemOffset;

    if (tuple != NULL) {
        *tuple = NULL;
    }
    if (tuple == NULL) {
        return HnswGraphTupleInvalid(raiseError, index, blkno, offno, "missing tuple output");
    }
    if (!HnswGraphPageGeometryIsValid(page) || HnswPageGetRole(page) != HNSW_PAGE_ROLE_GRAPH) {
        return HnswGraphTupleInvalid(raiseError, index, blkno, offno, "invalid graph page layout or role");
    }
    header = (PageHeader)page;
    if (!OffsetNumberIsValid(offno) || offno > PageGetMaxOffsetNumber(page)) {
        return HnswGraphTupleInvalid(raiseError, index, blkno, offno, "line pointer offset is out of range");
    }
    itemId = PageGetItemId(page, offno);
    if (!ItemIdIsNormal(itemId)) {
        return HnswGraphTupleInvalid(raiseError, index, blkno, offno, "line pointer is not normal");
    }
    itemOffset = ItemIdGetOffset(itemId);
    itemLen = ItemIdGetLength(itemId);
    if (itemOffset < header->pd_upper || itemOffset >= header->pd_special) {
        return HnswGraphTupleInvalid(raiseError, index, blkno, offno, "line pointer storage offset is out of range");
    }
    if (itemLen > (Size)(header->pd_special - itemOffset)) {
        return HnswGraphTupleInvalid(raiseError, index, blkno, offno, "tuple extends past page data");
    }
    if (itemLen < offsetof(HnswElementTupleData, data)) {
        return HnswGraphTupleInvalid(raiseError, index, blkno, offno, "tuple header is truncated");
    }

    elementTuple = (HnswElementTuple)PageGetItem(page, itemId);
    if (!HnswIsElementTuple(elementTuple)) {
        return HnswGraphTupleInvalid(raiseError, index, blkno, offno, "tuple type is not element");
    }
    if ((elementTuple->unused & HNSW_ELEMENT_FLAG_VECTOR_STORAGE) != 0 &&
        itemLen != HNSW_ELEMENT_TUPLE_V2_SIZE) {
        return HnswGraphTupleInvalid(raiseError, index, blkno, offno, "V2 tuple physical length mismatch");
    }
    *tuple = elementTuple;

    return true;
}


/*
 * Get the metapage info
 */
void HnswGetMetaPageInfo(Relation index, int *m, HnswElement *entryPoint)
{
    Buffer buf;
    Page page;
    HnswMetaPage metap;

    if (HnswIndexRelationSkipsPhysicalMetapage(index)) {
        if (m != NULL) {
            *m = HNSW_DEFAULT_M;
        }
        if (entryPoint != NULL) {
            *entryPoint = NULL;
        }
        return;
    }

    buf = ReadBuffer(index, HNSW_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    metap = HnswPageGetMeta(page);
    PG_TRY();
    {
        if (unlikely(metap->magicNumber != HNSW_MAGIC_NUMBER)) {
            elog(ERROR, "hnsw index is not valid");
        }

        if (m != NULL)
            *m = metap->m;

        if (entryPoint != NULL) {
            if (BlockNumberIsValid(metap->entryBlkno)) {
                *entryPoint = HnswInitElementFromBlock(metap->entryBlkno, metap->entryOffno);
                (*entryPoint)->level = metap->entryLevel;
            } else {
                *entryPoint = NULL;
            }
        }
    }
    PG_CATCH();
    {
        UnlockReleaseBuffer(buf);
        PG_RE_THROW();
    }
    PG_END_TRY();

    UnlockReleaseBuffer(buf);
}

BlockNumber HnswGetGraphHeadBlkno(Relation index)
{
    Buffer buf;
    Page page;
    HnswMetaPage metap;
    BlockNumber graphHeadBlkno;
    HnswVectorStorageMetaLayout layout;

    if (HnswIndexRelationSkipsPhysicalMetapage(index)) {
        return HNSW_HEAD_BLKNO;
    }

    buf = ReadBuffer(index, HNSW_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    metap = HnswPageGetMeta(page);
    if (unlikely(metap->magicNumber != HNSW_MAGIC_NUMBER)) {
        UnlockReleaseBuffer(buf);
        elog(ERROR, "hnsw index is not valid");
    }

    layout = HnswClassifyVectorStorageMeta(metap, NULL);
    if (layout == HNSW_VECTOR_STORAGE_META_INVALID) {
        UnlockReleaseBuffer(buf);
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("hnsw vector storage metapage for relation \"%s\" is malformed",
                   RelationGetRelationName(index))));
    }

    if (layout == HNSW_VECTOR_STORAGE_META_V2) {
        if (!HnswGetVectorStorageGraphHeadBlkno(metap, &graphHeadBlkno)) {
            UnlockReleaseBuffer(buf);
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("hnsw vector storage index has invalid graph head block")));
        }
    } else {
        graphHeadBlkno = HNSW_CHUNK_START_BLKNO + metap->pqTableNblk + metap->pqDisTableNblk +
                         metap->matrixNblk + metap->otherNblk + metap->lsgSampleNblk;
    }

    UnlockReleaseBuffer(buf);
    return graphHeadBlkno;
}

/*
 * Get the entry point
 */
HnswElement HnswGetEntryPoint(Relation index)
{
    HnswElement entryPoint;

    HnswGetMetaPageInfo(index, NULL, &entryPoint);

    return entryPoint;
}

/*
 * Update the metapage info
 */
static void HnswUpdateMetaPageInfo(Page page, int updateEntry, HnswElement entryPoint, BlockNumber insertPage)
{
    HnswMetaPage metap = HnswPageGetMeta(page);

    if (updateEntry) {
        if (entryPoint == NULL) {
            metap->entryBlkno = InvalidBlockNumber;
            metap->entryOffno = InvalidOffsetNumber;
            metap->entryLevel = -1;
        } else if (entryPoint->level > metap->entryLevel || updateEntry == HNSW_UPDATE_ENTRY_ALWAYS) {
            metap->entryBlkno = entryPoint->blkno;
            metap->entryOffno = entryPoint->offno;
            metap->entryLevel = entryPoint->level;
        }
    }

    if (BlockNumberIsValid(insertPage))
        metap->insertPage = insertPage;
}

static void HnswUpdateMetaPageVectorStorageInfo(Page page, BlockNumber graphHeadBlkno,
                                                BlockNumber payloadInsertBlkno, uint32 payloadLen,
                                                uint64 livePayloads)
{
    HnswMetaPage metap = HnswPageGetMeta(page);

    metap->version = HNSW_VECTOR_STORAGE_VERSION;
    metap->flags |= HNSW_META_HAS_VECTOR_STORAGE;
    metap->payloadFormatVersion = HNSW_PAYLOAD_FORMAT_VERSION;
    metap->reservedFlags = 0;
    metap->payloadLen = payloadLen;
    metap->graphHeadBlkno = graphHeadBlkno;
    metap->payloadInsertBlkno = payloadInsertBlkno;
    ItemPointerSetInvalid(&metap->payloadFreeHead);
    metap->livePayloads = livePayloads;
    metap->deadPayloads = 0;
}

/*
 * Update the metapage info about RabitQ
 */
static void HnswUpdateMetaPageInfoRbq(Page page, bool updateDelay)
{
    HnswMetaPage metap = HnswPageGetMeta(page);
    metap->rbqInsertRows += 1;
    if (updateDelay) {
        metap->rbqDelayState = RBQ_BUILD_AFTER_DELAY;
    }
}

/*
 * Update the append metapage info
 */
static void HnswUpdateAppendMetaPageInfo(Page page, int updateEntry, HnswElement entryPoint,
                                         BlockNumber eleInsertSlotStartPage, BlockNumber neiInsertSlotStartPage)
{
    HnswAppendMetaPage metap = HnswPageGetAppendMeta(page);

    if (updateEntry) {
        if (entryPoint == NULL) {
            metap->entryBlkno = InvalidBlockNumber;
            metap->entryOffno = InvalidOffsetNumber;
            metap->entryLevel = -1;
        } else if (entryPoint->level > metap->entryLevel || updateEntry == HNSW_UPDATE_ENTRY_ALWAYS) {
            metap->entryBlkno = entryPoint->blkno;
            metap->entryOffno = entryPoint->offno;
            metap->entryLevel = entryPoint->level;
        }
    }

    if (BlockNumberIsValid(eleInsertSlotStartPage)) {
        metap->elementInsertSlot = eleInsertSlotStartPage;
    }

    if (BlockNumberIsValid(neiInsertSlotStartPage)) {
        metap->neighborInsertSlot = neiInsertSlotStartPage;
    }
}

/*
 * Update the metapage
 */
void HnswUpdateMetaPage(Relation index, int updateEntry, HnswElement entryPoint, BlockNumber insertPage,
                        ForkNumber forkNum, bool building)
{
    Buffer buf;
    Page page;
    GenericXLogState *state;

    buf = ReadBufferExtended(index, forkNum, HNSW_METAPAGE_BLKNO, RBM_NORMAL, NULL);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    if (building) {
        state = NULL;
        page = BufferGetPage(buf);
    } else {
        state = GenericXLogStart(index);
        page = GenericXLogRegisterBuffer(state, buf, 0);
    }

    HnswUpdateMetaPageInfo(page, updateEntry, entryPoint, insertPage);

    if (building)
        MarkBufferDirty(buf);
    else
        GenericXLogFinish(state);
    UnlockReleaseBuffer(buf);
}

void HnswUpdateMetaPageVectorStorage(Relation index, const HnswVectorStorageMetaUpdate *update,
                                     ForkNumber forkNum, bool building)
{
    Buffer buf;
    Page page;
    GenericXLogState *state;

    buf = ReadBufferExtended(index, forkNum, HNSW_METAPAGE_BLKNO, RBM_NORMAL, NULL);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    if (building) {
        state = NULL;
        page = BufferGetPage(buf);
    } else {
        state = GenericXLogStart(index);
        page = GenericXLogRegisterBuffer(state, buf, 0);
    }

    HnswUpdateMetaPageVectorStorageInfo(page, update->graphHeadBlkno, update->payloadInsertBlkno,
                                        update->payloadLen, update->livePayloads);

    if (building)
        MarkBufferDirty(buf);
    else
        GenericXLogFinish(state);
    UnlockReleaseBuffer(buf);
}

/*
 * Update the metapage about RabitQ
 */
void HnswUpdateMetaPageRbq(Relation index, ForkNumber forkNum, bool updateDelay)
{
    Buffer buf;
    Page page;
    GenericXLogState *state;

    buf = ReadBufferExtended(index, forkNum, HNSW_METAPAGE_BLKNO, RBM_NORMAL, NULL);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    state = GenericXLogStart(index);
    page = GenericXLogRegisterBuffer(state, buf, 0);

    HnswUpdateMetaPageInfoRbq(page, updateDelay);

    GenericXLogFinish(state);
    UnlockReleaseBuffer(buf);
}

/*
 * Update the append metapage
 */
void HnswUpdateAppendMetaPage(Relation index, int updateEntry, HnswElement entryPoint, BlockNumber eleInsertPage,
                              BlockNumber neiInsertPage, ForkNumber forkNum, bool building)
{
    Buffer buf;
    Page page;
    GenericXLogState *state;

    buf = ReadBufferExtended(index, forkNum, HNSW_METAPAGE_BLKNO, RBM_NORMAL, NULL);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    if (building) {
        state = NULL;
        page = BufferGetPage(buf);
    } else {
        state = GenericXLogStart(index);
        page = GenericXLogRegisterBuffer(state, buf, 0);
    }

    HnswUpdateAppendMetaPageInfo(page, updateEntry, entryPoint, eleInsertPage, neiInsertPage);

    if (building) {
        MarkBufferDirty(buf);
    } else {
        GenericXLogFinish(state);
    }
    UnlockReleaseBuffer(buf);
}

void FlushChunkInfoInternal(Relation index, char* table, BlockNumber startBlkno, uint16 nblks, uint32 totalSize)
{
    Buffer buf;
    Page page;
    PageHeader p;
    uint32 curFlushSize;
    for (uint16 i = 0; i < nblks; i++) {
        curFlushSize = (i == nblks - 1) ?
                        (totalSize - i * CHUNK_STORAGE_SIZE) : CHUNK_STORAGE_SIZE;
        buf = ReadBufferExtended(index, MAIN_FORKNUM, startBlkno + i, RBM_NORMAL, NULL);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        page = BufferGetPage(buf);
        errno_t err = memcpy_s(PageGetContents(page), curFlushSize, table + i * CHUNK_STORAGE_SIZE, curFlushSize);
        securec_check(err, "\0", "\0");
        p = (PageHeader)page;
        p->pd_lower += curFlushSize;
        MarkBufferDirty(buf);
        UnlockReleaseBuffer(buf);
    }
}

/*
* Flush PQ table into page during index building
*/
void FlushPQInfo(HnswBuildState * buildstate)
{
    Relation index = buildstate->index;
    char* pqTable = buildstate->pqTable;
    float* pqDistanceTable = buildstate->pqDistanceTable;
    uint16 pqTableNblk;
    uint16 pqDisTableNblk;
    uint32 pqTableSize;
    uint32 pqDisTableSize;

    HnswGetPQInfoFromMetaPage(index, &pqTableNblk, &pqTableSize, &pqDisTableNblk, &pqDisTableSize);

    /* Flush pq table */
    FlushChunkInfoInternal(index, pqTable, HNSW_CHUNK_START_BLKNO, pqTableNblk, pqTableSize);
    if (buildstate->pqMode == HNSW_PQMODE_SDC) {
        /* Flush pq distance table */
        FlushChunkInfoInternal(index, (char*)pqDistanceTable,
                            HNSW_CHUNK_START_BLKNO + pqTableNblk, pqDisTableNblk, pqDisTableSize);
    }
}

char* LoadPQtable(Relation index)
{
    Buffer buf;
    Page page;
    uint16 nblks;
    uint32 curFlushSize;
    uint32 pqTableSize;
    char* pqTable;

    HnswGetPQInfoFromMetaPage(index, &nblks, &pqTableSize, NULL, NULL);
    pqTable = (char*)palloc0(pqTableSize);

    for (uint16 i = 0; i < nblks; i++) {
        curFlushSize = (i == nblks - 1) ? (pqTableSize - i * CHUNK_STORAGE_SIZE) : CHUNK_STORAGE_SIZE;
        buf = ReadBuffer(index, HNSW_CHUNK_START_BLKNO + i);
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buf);
        errno_t err = memcpy_s(pqTable + i * CHUNK_STORAGE_SIZE, curFlushSize,
                               PageGetContents(page), curFlushSize);
        securec_check(err, "\0", "\0");
        UnlockReleaseBuffer(buf);
    }
    return pqTable;
}

float* LoadPQDisTable(Relation index)
{
    Buffer buf;
    Page page;
    uint16 pqTableNblk;
    uint16 nblks;
    uint32 curFlushSize;
    uint32 pqDisTableSize;
    float* disTable;

    HnswGetPQInfoFromMetaPage(index, &pqTableNblk, NULL, &nblks, &pqDisTableSize);
    disTable = (float*)palloc0(pqDisTableSize);

    BlockNumber startBlkno = HNSW_CHUNK_START_BLKNO + pqTableNblk;
    for (uint16 i = 0; i < nblks; i++) {
        curFlushSize = (i == nblks - 1) ? (pqDisTableSize - i * CHUNK_STORAGE_SIZE) : CHUNK_STORAGE_SIZE;
        buf = ReadBuffer(index, startBlkno + i);
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buf);
        errno_t err = memcpy_s((char*)disTable + i * CHUNK_STORAGE_SIZE, curFlushSize,
                                PageGetContents(page), curFlushSize);
        securec_check(err, "\0", "\0");
        UnlockReleaseBuffer(buf);
    }
    return disTable;
}

uint8*
LoadPQcode(HnswElementTuple tuple)
{
    return (uint8*)(((char*)(tuple)) + HNSW_ELEMENT_TUPLE_SIZE(VARSIZE_ANY(&tuple->data)));
}

void FlushLsgSamples(HnswBuildState *buildstate)
{
    uint32 lsgCodeBookSize;
    uint16 nBlks;
    char* codeBookPtr = (char*)buildstate->LocScalingParam->sampleVecs;
    Relation index = buildstate->index;
    HnswGetLsgInfoFromMetaPage(index, &lsgCodeBookSize, &nBlks, NULL, NULL, NULL);

    Buffer buf;
    Page page;
    PageHeader p;
    uint32 curFlushSize;
    for (int i = 0; i < nBlks; i++) {
        curFlushSize = (i == nBlks - 1) ? (lsgCodeBookSize - i * LSGSAMPLE_STORAGE_SIZE) : LSGSAMPLE_STORAGE_SIZE;

        buf = ReadBufferExtended(index, MAIN_FORKNUM, HNSW_LSG_SAMPLE_START_BLKNO + i, RBM_ZERO, NULL);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        page = BufferGetPage(buf);
        errno_t err =
            memcpy_s(PageGetContents(page), curFlushSize, codeBookPtr + i * LSGSAMPLE_STORAGE_SIZE, curFlushSize);
        securec_check(err, "\0", "\0");

        p = (PageHeader)page;
        p->pd_lower += curFlushSize;
        MarkBufferDirty(buf);
        UnlockReleaseBuffer(buf);
    }
}

/*
 * Set element tuple, except for neighbor info
 */
void HnswSetElementTuple(char *base, HnswElementTuple etup, HnswElement element, Size rbqSize)
{
    errno_t rc = EOK;

    etup->type = HNSW_ELEMENT_TUPLE_TYPE;
    etup->level = element->level;
    etup->deleted = 0;
    etup->version = element->version;
    for (int i = 0; i < HNSW_HEAPTIDS; i++) {
        if (i < element->heaptidsLength)
            etup->heaptids[i] = element->heaptids[i];
        else
            ItemPointerSetInvalid(&etup->heaptids[i]);
    }
    if ((Pointer)HnswPtrAccess(base, element->rbqcodes) != NULL) {
        Pointer rbqPtr = (Pointer)HnswPtrAccess(base, element->rbqcodes);
        rc = memcpy_s(&etup->data, rbqSize, rbqPtr, rbqSize);
    } else {
        Pointer valuePtr = (Pointer)HnswPtrAccess(base, element->value);
        rc = memcpy_s(&etup->data, VARSIZE_ANY(valuePtr), valuePtr, VARSIZE_ANY(valuePtr));
    }
    securec_check(rc, "\0", "\0");
}

void HnswCheckNeighborTupleType(HnswNeighborTuple ntup, bool vectorStorage)
{
    if (ntup == NULL) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("HNSW neighbor tuple is null")));
    }
    if (vectorStorage) {
        if (!HnswIsNeighborTupleV2(ntup)) {
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("HNSW vector storage neighbor tuple is missing payload tids; rebuild the index")));
        }
    } else if (!HnswIsNeighborTupleLegacy(ntup)) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("HNSW neighbor tuple has an unexpected type %u", (unsigned int)ntup->type)));
    }
}

/*
 * Set neighbor tuple
 */
void HnswSetNeighborTuple(char *base, HnswNeighborTuple ntup, HnswElement e, int m, bool withPayloadTids)
{
    int idx = 0;

    ntup->type = withPayloadTids ? HNSW_NEIGHBOR_TUPLE_TYPE_V2 : HNSW_NEIGHBOR_TUPLE_TYPE;

    for (int lc = e->level; lc >= 0; lc--) {
        HnswNeighborArray *neighbors = HnswGetNeighbors(base, e, lc);
        int lm = HnswGetLayerM(m, lc);

        for (int i = 0; i < lm; i++) {
            ItemPointer indextid = &ntup->indextids[idx];

            if (i < neighbors->length) {
                HnswCandidate *hc = &neighbors->items[i];
                HnswElement hce = (HnswElement)HnswPtrAccess(base, hc->element);

                ItemPointerSet(indextid, hce->blkno, hce->offno);
            } else {
                ItemPointerSetInvalid(indextid);
            }
            idx++;
        }
    }

    ntup->count = idx;
    ntup->version = e->version;

    if (!withPayloadTids) {
        return;
    }

    idx = 0;
    for (int lc = e->level; lc >= 0; lc--) {
        HnswNeighborArray *neighbors = HnswGetNeighbors(base, e, lc);
        int lm = HnswGetLayerM(m, lc);

        for (int i = 0; i < lm; i++) {
            ItemPointer payloadTid = &ntup->indextids[ntup->count + idx];

            if (i < neighbors->length) {
                HnswCandidate *hc = &neighbors->items[i];
                HnswElement hce = (HnswElement)HnswPtrAccess(base, hc->element);
                if (!ItemPointerIsValid(&hce->payloadTid)) {
                    ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                        errmsg("HNSW vector storage neighbor is missing payload tid")));
                }
                ItemPointerCopy(&hce->payloadTid, payloadTid);
            } else {
                ItemPointerSetInvalid(payloadTid);
            }
            idx++;
        }
    }
}

/*
 * Load neighbors from page
 */
static void LoadNeighborsFromPage(HnswElement element, Relation index, Page page, int m)
{
    char *base = NULL;

    HnswNeighborTuple ntup = (HnswNeighborTuple)PageGetItem(page, PageGetItemId(page, element->neighborOffno));
    int neighborCount = (element->level + 2) * m;

    Assert(HnswIsNeighborTuple(ntup));

    HnswInitNeighbors(base, element, m, NULL);

    /*
    * Ensure the neighbor tuple has not been deleted or replaced between
    * index scan iterations
    */
    if (ntup->version != element->version || ntup->count != neighborCount) {
        return;
    }

    for (int i = 0; i < neighborCount; i++) {
        HnswElement e;
        int level;
        HnswCandidate *hc;
        ItemPointer indextid;
        HnswNeighborArray *neighbors;

        indextid = &ntup->indextids[i];

        if (!ItemPointerIsValid(indextid)) {
            continue;
        }

        e = HnswInitElementFromBlock(ItemPointerGetBlockNumber(indextid), ItemPointerGetOffsetNumber(indextid));
        if (HnswIsNeighborTupleV2(ntup)) {
            ItemPointerCopy(&ntup->indextids[ntup->count + i], &e->payloadTid);
        }

        /* Calculate level based on offset */
        level = element->level - i / m;
        if (level < 0) {
            level = 0;
        }

        neighbors = HnswGetNeighbors(base, element, level);
        hc = &neighbors->items[neighbors->length++];
        HnswPtrStore(base, hc->element, e);
    }
}

/*
 * Load neighbors
 */
void HnswLoadNeighbors(HnswElement element, Relation index, int m)
{
    Buffer buf;
    Page page;

    buf = ReadBuffer(index, element->neighborPage);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    HnswCheckVectorStorageGraphPage(index, page, element->neighborPage);

    LoadNeighborsFromPage(element, index, page, m);

    UnlockReleaseBuffer(buf);
}

/*
 * Load an element from a tuple
 */
void HnswLoadElementFromTuple(HnswElement element, HnswElementTuple etup, bool loadHeaptids, bool loadVec, Datum eRbqDiskVec)
{
    element->level = etup->level;
    element->deleted = etup->deleted;
    element->version = etup->version;
    element->neighborPage = ItemPointerGetBlockNumber(&etup->neighbortid);
    element->neighborOffno = ItemPointerGetOffsetNumber(&etup->neighbortid);
    element->heaptidsLength = 0;

    if (loadHeaptids) {
        for (int i = 0; i < HNSW_HEAPTIDS; i++) {
            /* Can stop at first invalid */
            if (!ItemPointerIsValid(&etup->heaptids[i]))
                break;

            HnswAddHeapTid(element, &etup->heaptids[i]);
        }
    }

    if (loadVec) {
        char *base = NULL;
        Datum value;

        if (DatumGetPointer(eRbqDiskVec) != NULL) {
            value = datumCopy(eRbqDiskVec, false, -1);
        } else if (HnswElementTupleIsVectorStorage(etup)) {
            /*
             * V2 overlay is a payload tid, not a vector varlena. Copying it
             * as Datum(-1) TRAPs in VARSIZE_ANY (refMask 0x0001 -> 1B_E).
             */
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("cannot load an inline vector from an HNSW vector-storage element tuple")));
        } else {
            value = datumCopy(PointerGetDatum(&etup->data), false, -1);
        }

        HnswPtrStore(base, element->value, DatumGetPointer(value));
    }
}

typedef struct HnswV2PayloadLoadContext {
    uint8 level;
    uint8 deleted;
    uint8 version;
    const ItemPointerData *heaptids;
    const ItemPointerData *neighbortid;
    bool loadHeaptids;
    bool loadVec;
    VecPayloadPin *payloadPin;
} HnswV2PayloadLoadContext;

static void HnswLoadElementFromV2Payload(HnswElement element, const HnswV2PayloadLoadContext *context)
{
    element->level = context->level;
    element->deleted = context->deleted;
    element->version = context->version;
    element->neighborPage = ItemPointerGetBlockNumber(context->neighbortid);
    element->neighborOffno = ItemPointerGetOffsetNumber(context->neighbortid);
    element->heaptidsLength = 0;

    if (context->loadHeaptids) {
        for (int i = 0; i < HNSW_HEAPTIDS; i++) {
            if (!ItemPointerIsValid(&context->heaptids[i])) {
                break;
            }

            HnswAddHeapTid(element, &context->heaptids[i]);
        }
    }

    if (context->loadVec) {
        char *base = NULL;
        Datum value = datumCopy(context->payloadPin->datum, false, -1);

        HnswPtrStore(base, element->value, DatumGetPointer(value));
    }
}

static bool HnswElementTupleIsDead(HnswElementTuple etup)
{
    return etup->deleted || !ItemPointerIsValid(&etup->heaptids[0]);
}

bool HnswTryGetTupleFromHeap(Relation relation, ItemPointer heaptids, HeapTuple tuple, Buffer* userbuf)
{
    bool find = false;
    for (int i = 0; i < HNSW_HEAPTIDS; i++) {
        if (!ItemPointerIsValid(&heaptids[i])) {
            continue;
        }
        errno_t rc = memset_s(tuple, BLCKSZ, 0, BLCKSZ);
        securec_check(rc, "\0", "\0");
        tuple->t_data = (HeapTupleHeader)((char *)tuple + HEAPTUPLESIZE);
        Assert(&heaptids[i] != NULL);
        tuple->t_self = heaptids[i];

        if (heap_fetch(relation, SnapshotAny, tuple, userbuf, false, NULL)) {
            tuple->tupTableType = HEAP_TUPLE;
            find = true;
            break;
        }
    }
    return find;
}

void HnswGetTupleFromHeap(Relation relation, ItemPointer heaptids, HeapTuple tuple, Buffer* userbuf)
{
    if (!HnswTryGetTupleFromHeap(relation, heaptids, tuple, userbuf)) {
        ereport(ERROR, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("The tuple is not found"),
            errdetail("Another user is getting tuple or the datum is NULL")));
    }
}

bool HnswTryGetVectorFromHeap(Relation heap, ItemPointer heaptids, IndexInfo *indexInfo, HeapTuple tuple,
                             FmgrInfo *procinfo, FmgrInfo *normprocinfo, Oid collation, Buffer* userbuf,
                             Datum *origin)
{
    Datum result = 0;
    bool foundOrigin = false;

    if (indexInfo->ii_NumIndexAttrs != 1) {
        ereport(ERROR, (errmsg("Supports vector indexing exclusively for a single column.")));
    }
    if (!HnswTryGetTupleFromHeap(heap, heaptids, tuple, userbuf)) {
        return false;
    }

    TupleDesc relTupleDesc = heap->rd_att;
    Datum *val = (Datum *)palloc(sizeof(Datum) * (relTupleDesc->natts + 1));
    bool *null = (bool *)palloc(sizeof(bool) * (relTupleDesc->natts + 1));

    tableam_tops_deform_tuple(tuple, relTupleDesc, val, null);

    for (int i = 0; i < indexInfo->ii_NumIndexAttrs; i++) {
        int keycol = indexInfo->ii_KeyAttrNumbers[i];
        if (keycol != 0 && !null[keycol - 1]) {
            result = (Datum)(PG_DETOAST_DATUM(val[keycol - 1]));
            foundOrigin = true;
        } else {
            pfree(null);
            pfree(val);
            ereport(ERROR, (errmsg("Failed to get origin vector from heap.")));
        }
    }

    if (IS_HALFVEC(procinfo->fn_oid)) {
        if (normprocinfo != NULL) {
            result = DirectFunctionCall1Coll(halfvec_l2_normalize, collation, result);
        }
    } else {
        if (normprocinfo != NULL) {
            result = DirectFunctionCall1Coll(l2_normalize, collation, result);
        }
    }

    pfree(null);
    pfree(val);
    *origin = result;
    return foundOrigin;
}

Datum HnswGetVectorFromHeap(Relation heap, ItemPointer heaptids, IndexInfo *indexInfo, HeapTuple tuple,
                             FmgrInfo *procinfo, FmgrInfo *normprocinfo, Oid collation, Buffer* userbuf)
{
    Datum origin = 0;
    if (!HnswTryGetVectorFromHeap(heap, heaptids, indexInfo, tuple, procinfo, normprocinfo, collation, userbuf,
                                  &origin)) {
        ereport(ERROR, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("The tuple is not found"),
            errdetail("Another user is getting tuple or the datum is NULL")));
    }
    return origin;
}

static inline void HnswScaleDistanceByIso(float *distance, Datum lhs, Datum rhs, FmgrInfo *procinfo, bool enableLsg);

/*
 * Search V2 prune path: pin by payload_tid already copied from the neighbor
 * tuple. VBP key stays the payload address. Distance too far → skip E's graph page.
 */
static bool HnswShouldPinPayloadTidFirst(HnswElement element, IndexScanDesc scan, bool enablePQ, bool enableRabitQ)
{
    HnswScanOpaque scanOpaque;

    if (element == NULL || scan == NULL || enablePQ || enableRabitQ) {
        return false;
    }
    if (!ItemPointerIsValid(&element->payloadTid)) {
        return false;
    }
    scanOpaque = (HnswScanOpaque)scan->opaque;
    if (scanOpaque == NULL || scanOpaque->vectorBufferAccess == NULL ||
        scanOpaque->vectorPayloadLen < VARHDRSZ) {
        return false;
    }
    if (scan->heapRelation != NULL && RelationIsUstoreFormat(scan->heapRelation)) {
        return false;
    }
    return true;
}

typedef struct HnswPayloadTupleSnapshot {
    ItemPointerData heaptids[HNSW_HEAPTIDS];
    ItemPointerData neighbortid;
    uint8 level;
    uint8 deleted;
    uint8 version;
} HnswPayloadTupleSnapshot;

static void HnswReadPayloadTupleSnapshot(Relation index, HnswElement element,
    HnswPayloadTupleSnapshot *snapshot)
{
    Buffer buf = ReadBuffer(index, element->blkno);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    Page page = BufferGetPage(buf);
    HnswCheckVectorStorageGraphPage(index, page, element->blkno);
    HnswElementTuple etup;
    const HnswGraphTupleLocation location = {index, element->blkno, element->offno, true};
    (void)HnswLocateGraphElementTupleInternal(page, &location, &etup);
    if (!HnswElementTupleMatchesMetaLayout(etup, HNSW_VECTOR_STORAGE_META_V2)) {
        UnlockReleaseBuffer(buf);
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("HNSW element tuple format does not match scan metapage layout")));
    }
    errno_t rc = memcpy_s(snapshot->heaptids, sizeof(snapshot->heaptids),
        etup->heaptids, sizeof(snapshot->heaptids));
    if (rc != EOK) {
        UnlockReleaseBuffer(buf);
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
            errmsg("failed to copy HNSW element heap TIDs")));
    }
    snapshot->neighbortid = etup->neighbortid;
    snapshot->level = etup->level;
    snapshot->deleted = etup->deleted;
    snapshot->version = etup->version;
    UnlockReleaseBuffer(buf);
}

static bool HnswLoadElementByPayloadTid(HnswElement element, float *distance, Datum *q, Relation index,
    FmgrInfo *procinfo, Oid collation, bool loadVec, float *maxDistance, IndexScanDesc scan, bool enableLsg)
{
    HnswScanOpaque scanOpaque = (HnswScanOpaque)scan->opaque;
    VecPayloadDiskRef diskRef;
    errno_t rc;

    rc = memset_s(&diskRef, sizeof(diskRef), 0, sizeof(diskRef));
    securec_check(rc, "\0", "\0");
    ItemPointerCopy(&element->payloadTid, &diskRef.tid);
    diskRef.payloadLen = scanOpaque->vectorPayloadLen;
    diskRef.kind = VEC_PAYLOAD_RAW_VECTOR;

    VecPayloadPin payloadPin;
    const VecPayloadPinRequest request = {
        VEC_PAYLOAD_RAW_VECTOR, scanOpaque->vectorPayloadLen, scanOpaque->vectorBufferAccess};
    if (!VecPayloadPinGet(index, &diskRef, &request, &payloadPin)) {
        ereport(ERROR, (errmsg("failed to pin HNSW vector storage payload")));
    }
    if (distance != NULL) {
        if (DatumGetPointer(*q) == NULL) {
            *distance = 0;
        } else {
            *distance = (float)DatumGetFloat8(FunctionCall2Coll(procinfo, collation, *q, payloadPin.datum));
            HnswScaleDistanceByIso(distance, *q, payloadPin.datum, procinfo, enableLsg);
        }
    }
    if (distance != NULL && maxDistance != NULL && *distance >= *maxDistance && !loadVec) {
        VecPayloadUnpin(&payloadPin);
        return true;
    }

    HnswPayloadTupleSnapshot snapshot;
    PG_TRY();
    {
        HnswReadPayloadTupleSnapshot(index, element, &snapshot);
    }
    PG_CATCH();
    {
        VecPayloadUnpin(&payloadPin);
        PG_RE_THROW();
    }
    PG_END_TRY();
    const HnswV2PayloadLoadContext context = {snapshot.level, snapshot.deleted, snapshot.version,
        snapshot.heaptids, &snapshot.neighbortid, true, loadVec, &payloadPin};
    HnswLoadElementFromV2Payload(element, &context);
    VecPayloadUnpin(&payloadPin);
    return true;
}

/*
 * Load an element and optionally get its distance from q
 */
bool HnswLoadElement(HnswElement element, float *distance, Datum *q, Relation index, FmgrInfo *procinfo, Oid collation,
                     bool loadVec, float *maxDistance, bool enableRabitQ, RabitqQueryParams *rbqQueryParams,
                     RabitqInsertOnDiskParams *rbqDiskParams, IndexScanDesc scan, bool enablePQ, PQSearchInfo *pqinfo,
                     bool enableLsg)
{
    Buffer buf;
    Page page;
    HnswElementTuple etup;
    bool needRecheck = false;
    bool isVisible = true;
    uint8 *ePQCode;
    PQParams *params;
    Datum eRbqDiskData = NULL;
    Buffer heapbuf = InvalidBuffer;
    HnswScanOpaque scanOpaque = NULL;
    bool tupleHasVectorStorage;

    if (HnswShouldPinPayloadTidFirst(element, scan, enablePQ, enableRabitQ)) {
        return HnswLoadElementByPayloadTid(element, distance, q, index, procinfo, collation, loadVec, maxDistance,
            scan, enableLsg);
    }

    /* Read vector */
    buf = ReadBuffer(index, element->blkno);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    HnswCheckVectorStorageGraphPage(index, page, element->blkno);
    const HnswGraphTupleLocation tupleLocation = {index, element->blkno, element->offno, true};
    (void)HnswLocateGraphElementTupleInternal(page, &tupleLocation, &etup);
    tupleHasVectorStorage = HnswElementTupleIsVectorStorage(etup);
    if (scan != NULL) {
        HnswVectorStorageMetaLayout scanLayout;
        bool scanLayoutValid;

        scanOpaque = (HnswScanOpaque)scan->opaque;
        scanLayoutValid = scanOpaque != NULL &&
            ((scanOpaque->vectorBufferAccess == NULL && scanOpaque->vectorPayloadLen == 0) ||
             (scanOpaque->vectorBufferAccess != NULL && scanOpaque->vectorPayloadLen >= VARHDRSZ));
        if (!scanLayoutValid) {
            UnlockReleaseBuffer(buf);
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("HNSW scan has an invalid cached vector storage layout")));
        }

        scanLayout = scanOpaque->vectorBufferAccess == NULL ?
            HNSW_VECTOR_STORAGE_META_LEGACY : HNSW_VECTOR_STORAGE_META_V2;
        if (!HnswElementTupleMatchesMetaLayout(etup, scanLayout)) {
            UnlockReleaseBuffer(buf);
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("HNSW element tuple format does not match scan metapage layout")));
        }
    } else if (!tupleHasVectorStorage && HnswRelationHasVectorPayloadStorage(index, NULL, NULL)) {
        UnlockReleaseBuffer(buf);
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("HNSW legacy tuple does not match the vector payload storage metapage layout")));
    }

    if (scan != NULL && HnswPageGetOpaque(page)->pageType == HNSW_USTORE_PAGE_TYPE) {
        scanOpaque->vs.buf = buf;
        isVisible = VecVisibilityCheck(scan, page, element->offno, &needRecheck);
    }

    if (tupleHasVectorStorage) {
        HnswElementTupleV2PayloadData *payloads = HnswElementTupleGetPayloads(etup);
        VecPayloadDiskRef rawRef = payloads->raw;
        uint16 refMask = payloads->refMask;
        bool payloadTidValid = ItemPointerIsValid(&rawRef.tid);
        bool payloadRefValid = HnswElementTupleHasVectorStorage(etup);
        VecPayloadPin payloadPin;
        ItemPointerData heaptids[HNSW_HEAPTIDS];
        ItemPointerData neighbortid = etup->neighbortid;
        uint8 level = etup->level;
        uint8 deleted = etup->deleted;
        uint8 version = etup->version;
        errno_t rc = EOK;
        bool needPayload = (distance != NULL || loadVec);
        uint32 scanPayloadLen = 0;
        VectorBufferAccess *borrowedAccess = NULL;

        rc = memcpy_s(heaptids, sizeof(heaptids), etup->heaptids, sizeof(heaptids));
        securec_check(rc, "\0", "\0");
        if (payloadTidValid) {
            ItemPointerCopy(&rawRef.tid, &element->payloadTid);
        }

        UnlockReleaseBuffer(buf);

        if (scan != NULL) {
            Assert(scanOpaque != NULL);
            Assert(scanOpaque->vectorBufferAccess != NULL);
            Assert(scanOpaque->vectorPayloadLen >= VARHDRSZ);
            scanPayloadLen = scanOpaque->vectorPayloadLen;
            borrowedAccess = scanOpaque->vectorBufferAccess;
        } else if (!HnswRelationHasVectorPayloadStorage(index, NULL, &scanPayloadLen)) {
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("HNSW V2 tuple does not match relation metapage layout")));
        }

        if (rawRef.payloadLen != scanPayloadLen || rawRef.kind != VEC_PAYLOAD_RAW_VECTOR) {
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("HNSW payload reference does not match metapage layout")));
        }

        if (enablePQ || enableRabitQ) {
            ereport(ERROR, (errmsg("HNSW vector storage does not support PQ or RabitQ scan paths")));
        }

        if (!payloadRefValid) {
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("corrupted HNSW vector storage payload reference"),
                errdetail("relation \"%s\", block %u, offset %u, refMask %u, kind %u, payloadLen %u, "
                          "payloadTidValid %s",
                          RelationGetRelationName(index), (unsigned int)element->blkno,
                          (unsigned int)element->offno, (unsigned int)refMask, (unsigned int)rawRef.kind,
                          (unsigned int)rawRef.payloadLen, payloadTidValid ? "true" : "false")));
        }

        if (!needPayload) {
            const HnswV2PayloadLoadContext context = {
                level, deleted, version, heaptids, &neighbortid, true, false, NULL};
            HnswLoadElementFromV2Payload(element, &context);
            return isVisible;
        }

        const VecPayloadPinRequest request = {VEC_PAYLOAD_RAW_VECTOR, scanPayloadLen, borrowedAccess};
        if (!VecPayloadPinGet(index, &rawRef, &request, &payloadPin)) {
            ereport(ERROR, (errmsg("failed to pin HNSW vector storage payload")));
        }

        if (distance != NULL) {
            if (DatumGetPointer(*q) == NULL) {
                *distance = 0;
            } else {
                *distance = (float)DatumGetFloat8(FunctionCall2Coll(procinfo, collation, *q, payloadPin.datum));
                HnswScaleDistanceByIso(distance, *q, payloadPin.datum, procinfo, enableLsg);
            }
        }

        if (distance == NULL || maxDistance == NULL || *distance < *maxDistance) {
            const HnswV2PayloadLoadContext context = {
                level, deleted, version, heaptids, &neighbortid, true, loadVec, &payloadPin};
            HnswLoadElementFromV2Payload(element, &context);
        }

        VecPayloadUnpin(&payloadPin);
        return isVisible;
    }

    if (enableRabitQ && HnswElementTupleIsDead(etup)) {
        if (distance != NULL) {
            *distance = FLT_MAX;
        }
        UnlockReleaseBuffer(buf);
        return false;
    }

    /* Calculate distance */
    if (distance != NULL) {
        if (enablePQ && pqinfo->lc == 0) {
            ePQCode = LoadPQcode(etup);
            params = &pqinfo->params;
            if (pqinfo->pqMode == HNSW_PQMODE_SDC && pqinfo->qPQCode == NULL) {
                *distance = 0;
            } else if (pqinfo->pqMode == HNSW_PQMODE_ADC && pqinfo->pqDistanceTable == NULL) {
                *distance = 0;
            } else {
                size_t pqCodeSize = params->pqM * sizeof(uint8);
                if (GetPQDistance(ePQCode, pqinfo->qPQCode, params, pqinfo->pqDistanceTable, distance,
                                  pqCodeSize, pqCodeSize, pqinfo->pqDistanceTableSize, sizeof(float)) != 0) {
                    ereport(ERROR, (errmsg("failed to compute PQ distance")));
                }
            }
        } else if (enableRabitQ) {
            /* 
             * Rbq insert in memeory, rbqDiskParams == NULL && rbqQueryParams == NULL
             * Rbq insert on disk, rbqDiskParams != NULL
             * Rbq search, rbqQueryParams != NULL
             */
            if (DatumGetPointer(*q) == NULL) {
                *distance = 0;
            } else if (rbqDiskParams != NULL) {
                if (!HnswTryGetVectorFromHeap(rbqDiskParams->heap, etup->heaptids, rbqDiskParams->indexInfo,
                                              rbqDiskParams->heapTuple, procinfo, rbqDiskParams->normprocinfo,
                                              rbqDiskParams->collation, &heapbuf, &eRbqDiskData)) {
                    *distance = FLT_MAX;
                    UnlockReleaseBuffer(buf);
                    return false;
                }
                *distance = (float)DatumGetFloat8(FunctionCall2Coll(procinfo, collation, *q, eRbqDiskData));
            } else if (rbqQueryParams != NULL) {
                RabitqVector *rbqVec = (RabitqVector *)PointerGetDatum(&etup->data);
                RabitQConfig *rbqConfig = rbqQueryParams->rbqConfig;
                QueryRabitqVector* qrbqVec = rbqQueryParams->qrbqVec;
                *distance = ComputeRbqDistance(rbqQueryParams->dim, rbqConfig->rbqQueryBits, rbqVec, qrbqVec, rbqQueryParams->funcType);
            } else {
                *distance = (float)DatumGetFloat8(FunctionCall2Coll(
                            procinfo, collation, *q, PointerGetDatum(&etup->data)));
            }
        } else {
            if (DatumGetPointer(*q) == NULL) {
                *distance = 0;
            } else {
                *distance = (float)DatumGetFloat8(FunctionCall2Coll(
                            procinfo, collation, *q, PointerGetDatum(&etup->data)));
                HnswScaleDistanceByIso(distance, *q, PointerGetDatum(&etup->data), procinfo, enableLsg);
            }
        }
    }

    /* vacuum entrypoint of HNSW RabitQ */
    if (distance == NULL && enableRabitQ && rbqDiskParams != NULL) {
        VectorTransform *vtrans = rbqDiskParams->vtrans;
        Relation heap = rbqDiskParams->heap;
        if (!HnswTryGetVectorFromHeap(heap, etup->heaptids, rbqDiskParams->indexInfo, rbqDiskParams->heapTuple,
                                      procinfo, rbqDiskParams->normprocinfo, rbqDiskParams->collation, &heapbuf,
                                      &eRbqDiskData)) {
            UnlockReleaseBuffer(buf);
            return false;
        }
    }

    /* Load element */
    if (distance == NULL || maxDistance == NULL || *distance < *maxDistance) {
        HnswLoadElementFromTuple(element, etup, true, loadVec, eRbqDiskData);
        if (enablePQ) {
            params = &pqinfo->params;
            Vector *vd1 = &etup->data;
            Vector *vd2 = (Vector *)DatumGetPointer(*q);
            float exactDis;
            if (pqinfo->params.funcType == DIS_IP) {
                exactDis = -VectorInnerProduct(params->dim, vd1->x, vd2->x);
            } else {
                exactDis = VectorL2SquaredDistance(params->dim, vd1->x, vd2->x);
            }
            *distance = exactDis;
        }
    }
    if (BufferIsValid(heapbuf)) {
        ReleaseBuffer(heapbuf);
    }

    UnlockReleaseBuffer(buf);
    return isVisible;
}

/*
 * Only apply LSG isoValue when the index explicitly enables it.
 */
static inline bool HnswUseIsoDistance(FmgrInfo *procinfo, bool enableLsg)
{
    return enableLsg && !IS_SPARSEVEC(procinfo->fn_oid) && !IS_BITVEC(procinfo->fn_oid);
}

static inline float HnswGetIsoWeight(Datum lhs, Datum rhs)
{
    float iso1 = Float16ToFloat32(((Vector *)DatumGetPointer(lhs))->isoValue);
    float iso2 = Float16ToFloat32(((Vector *)DatumGetPointer(rhs))->isoValue);

    return iso1 * iso2;
}

static inline void HnswScaleDistanceByIso(float *distance, Datum lhs, Datum rhs, FmgrInfo *procinfo, bool enableLsg)
{
    if (!HnswUseIsoDistance(procinfo, enableLsg)) {
        return;
    }

    *distance *= HnswGetIsoWeight(lhs, rhs);
}

/*
 * Get the distance for a candidate
 */
static float GetCandidateDistance(char *base, HnswElement element, Datum q, FmgrInfo *procinfo, Oid collation,
                                  bool enableRabitQ, bool enableLsg)
{
    Datum value = HnswGetValue(base, element);
    float realDis = DatumGetFloat8(FunctionCall2Coll(procinfo, collation, q, value));

    if (!HnswUseIsoDistance(procinfo, enableLsg)) {
        return realDis;
    }

    return HnswGetIsoWeight(q, value) * realDis;
}

/*
 * Create a candidate for the entry point
 */
HnswCandidate *HnswEntryCandidate(char *base, HnswElement entryPoint, Datum q, Relation index, FmgrInfo *procinfo,
                                  Oid collation, bool loadVec, bool enableRabitQ, RabitqQueryParams *rbqQueryParams,
                                  RabitqInsertOnDiskParams *rbqDiskParams, IndexScanDesc scan, bool enablePQ,
                                  PQSearchInfo *pqinfo, bool enableLsg)
{
    HnswCandidate *hc = (HnswCandidate *)palloc(sizeof(HnswCandidate));

    HnswPtrStore(base, hc->element, entryPoint);
    if (index == NULL) {
        hc->distance = GetCandidateDistance(base, entryPoint, q, procinfo, collation, enableRabitQ, enableLsg);
    } else {
        bool isVisible = HnswLoadElement(entryPoint, &hc->distance, &q, index, procinfo,
                                         collation, loadVec, NULL, enableRabitQ, rbqQueryParams,
                                         rbqDiskParams, scan, enablePQ, pqinfo, enableLsg);
        if (!isVisible) {
            elog(ERROR, "hnsw entryPoint is invisible\n");
        }
    }
    return hc;
}

/*
 * Compare candidate distances
 */
static int CompareNearestCandidates(const pairingheap_node *a, const pairingheap_node *b, void *arg)
{
    if (HnswGetPairingHeapCandidateConst(c_node, a)->distance < HnswGetPairingHeapCandidateConst(c_node, b)->distance) {
        return 1;
    }
    if (HnswGetPairingHeapCandidateConst(c_node, a)->distance > HnswGetPairingHeapCandidateConst(c_node, b)->distance) {
        return -1;
    }

    return 0;
}

/*
 * Compare discarded candidate distances
 */
static int CompareNearestDiscardedCandidates(const pairingheap_node *a, const pairingheap_node *b, void *arg)
{
    if (HnswGetPairingHeapCandidateConst(w_node, a)->distance < HnswGetPairingHeapCandidateConst(w_node, b)->distance) {
        return 1;
    }

    if (HnswGetPairingHeapCandidateConst(w_node, a)->distance > HnswGetPairingHeapCandidateConst(w_node, b)->distance) {
        return -1;
    }

    return 0;
}

/*
 * Compare candidate distances
 */
static int CompareFurthestCandidates(const pairingheap_node *a, const pairingheap_node *b, void *arg)
{
    if (HnswGetPairingHeapCandidateConst(w_node, a)->distance < HnswGetPairingHeapCandidateConst(w_node, b)->distance) {
        return -1;
    }
    if (HnswGetPairingHeapCandidateConst(w_node, a)->distance > HnswGetPairingHeapCandidateConst(w_node, b)->distance) {
        return 1;
    }

    return 0;
}

/*
 * Create a pairing heap node for a candidate
 */
static HnswPairingHeapNode *CreatePairingHeapNode(HnswCandidate *c)
{
    HnswPairingHeapNode *node = (HnswPairingHeapNode *)palloc(sizeof(HnswPairingHeapNode));

    node->inner = c;
    return node;
}

/*
 * Init visited
 */
static inline void InitVisited(char *base, VisitedHash *v, Relation index, int ef, int m)
{
    if (index != NULL) {
        v->tids = tidhash_create(CurrentMemoryContext, ef * m * 2, NULL);
    } else if (base != NULL) {
        v->offsets = offsethash_create(CurrentMemoryContext, ef * m * 2, NULL);
    } else {
        v->pointers = pointerhash_create(CurrentMemoryContext, ef * m * 2, NULL);
    }
}

/*
 * Add to visited
 */
static inline void AddToVisited(char *base, VisitedHash *v, HnswCandidate *hc, Relation index, bool *found)
{
    if (index != NULL) {
        HnswElement element = (HnswElement)HnswPtrAccess(base, hc->element);
        ItemPointerData indextid;

        ItemPointerSet(&indextid, element->blkno, element->offno);
        tidhash_insert(v->tids, indextid, found);
    } else if (base != NULL) {
        offsethash_insert(v->offsets, HnswPtrOffset(hc->element), found);
    } else {
        pointerhash_insert(v->pointers, (uintptr_t)HnswPtrPointer(hc->element), found);
    }
}

/*
 * Count element towards ef
 */
static inline bool CountElement(char *base, HnswElement skipElement, HnswElement e)
{
    if (skipElement == NULL) {
        return true;
    }

    /* Ensure does not access heaptidsLength during in-memory build */
    pg_memory_barrier();

    return e->heaptidsLength != 0;
}

/*
 * Load unvisited neighbors from memory
 */
static void
HnswLoadUnvisitedFromMemory(char *base, HnswElement element, HnswElement *unvisited, int *unvisitedLength,
                            VisitedHash *v, int lc, HnswNeighborArray *neighborhoodData, Size neighborhoodSize)
{
    /* Get the neighborhood at layer lc */
    HnswNeighborArray *neighborhood = HnswGetNeighbors(base, element, lc);

    /* Copy neighborhood to local memory */
    LWLockAcquire(&element->lock, LW_SHARED);
    memcpy(neighborhoodData, neighborhood, neighborhoodSize);
    LWLockRelease(&element->lock);
    neighborhood = neighborhoodData;

    *unvisitedLength = 0;

    for (int i = 0; i < neighborhood->length; i++) {
        HnswCandidate *hc = &neighborhood->items[i];
        bool found;

        AddToVisited(base, v, hc, NULL, &found);

        if (!found) {
            unvisited[(*unvisitedLength)++] = (HnswElement)HnswPtrAccess(base, hc->element);
        }
    }
}

/*
 * Load unvisited neighbors from disk
 */
void HnswLoadUnvisitedFromDisk(HnswElement element, HnswElement *unvisited, int *unvisitedLength,
                          VisitedHash *v, Relation index, int m, int lm, int lc)
{
    Buffer buf;
    Page page;
    HnswNeighborTuple ntup;
    int start;
    ItemPointerData indextids[HNSW_MAX_M * 2];
    ItemPointerData payloadtids[HNSW_MAX_M * 2];
    bool v2Neighbors;

    buf = ReadBuffer(index, element->neighborPage);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    HnswCheckVectorStorageGraphPage(index, page, element->neighborPage);

    ntup = (HnswNeighborTuple)PageGetItem(page, PageGetItemId(page, element->neighborOffno));
    start = (element->level - lc) * m;
    v2Neighbors = HnswIsNeighborTupleV2(ntup);
    {
        bool vectorStorage = HnswRelationHasVectorPayloadStorage(index, NULL, NULL);
        bool typeOk = vectorStorage ? v2Neighbors : HnswIsNeighborTupleLegacy(ntup);
        if (!typeOk) {
            UnlockReleaseBuffer(buf);
            HnswCheckNeighborTupleType(ntup, vectorStorage);
        }
    }

    /* Copy to minimize lock time */
    memcpy(&indextids, ntup->indextids + start, lm * sizeof(ItemPointerData));
    if (v2Neighbors) {
        memcpy(&payloadtids, ntup->indextids + ntup->count + start, lm * sizeof(ItemPointerData));
    }

    UnlockReleaseBuffer(buf);

    *unvisitedLength = 0;

    for (int i = 0; i < lm; i++) {
        ItemPointer indextid = &indextids[i];
        bool found;

        if (!ItemPointerIsValid(indextid)) {
            break;
        }

        tidhash_insert(v->tids, *indextid, &found);

        if (!found) {
            HnswElement e = HnswInitElementFromBlock(ItemPointerGetBlockNumber(indextid),
                ItemPointerGetOffsetNumber(indextid));
            if (v2Neighbors) {
                ItemPointerCopy(&payloadtids[i], &e->payloadTid);
            }
            unvisited[(*unvisitedLength)++] = e;
        }
    }
}

bool HnswRbqNeedReorder(bool enableRabitQ, RabitqQueryParams *rbqParams, int lc)
{
    if (lc != 0) {
        return false;
    }
    if (!enableRabitQ || rbqParams == NULL) {
        return false;
    }
    if (rbqParams->rbqConfig->reType == NotRefine) {
        return false;
    }
    if (rbqParams->rbqConfig->kreorder == 0) {
        return false;
    }
    return true;
}

float HNSWRbqComputeDis(RabitqQueryParams *rbqParams, float *candidate)
{
    float refineDis = 0;
    Vector *qVec = (Vector *)DatumGetPointer(rbqParams->originQueryVec);
    if (rbqParams->funcType == DIS_L2) {
        refineDis = VectorL2SquaredDistance(qVec->dim, qVec->x, candidate);
    } else {
        refineDis = -VectorInnerProduct(qVec->dim, qVec->x, candidate);
    }
    if (rbqParams->normprocinfo != NULL) {
        float square = (float)vector_square(candidate, qVec->dim);
        refineDis = square == 0 ? 0 : -refineDis * refineDis / square;
    }
    return refineDis;
}

/*
 * Algorithm 2 from paper
 */
List *HnswSearchLayer(char *base, Datum q, List *ep, int ef, int lc, Relation index, FmgrInfo *procinfo,
                      Oid collation, int m, bool inserting, HnswElement skipElement, VisitedHash *v,
                      pairingheap **discarded, bool initVisited, int64 *tuples, bool enableRabitQ, RabitqQueryParams *rbqParams,
                      RabitqInsertOnDiskParams *rbqDiskParams, bool tryMmap, IndexScanDesc scan, bool enablePQ,
                      PQSearchInfo *pqinfo, bool enableLsg)
{
    List *w = NIL;
    pairingheap *C = pairingheap_allocate(CompareNearestCandidates, NULL);
    pairingheap *W = pairingheap_allocate(CompareFurthestCandidates, NULL);
    int wlen = 0;
    VisitedHash vh;
    ListCell *lc2;
    HnswNeighborArray *neighborhoodData = NULL;
    Size neighborhoodSize;
    bool isVisible = true;
    int lm = HnswGetLayerM(m, lc);
    HnswElement *unvisited = (HnswElementData**)palloc(lm * sizeof(HnswElement));
    int unvisitedLength;
    errno_t rc = EOK;
    int vNum = 0;
    int threshold = u_sess->datavec_ctx.hnsw_earlystop_threshold;
    bool enableEarlyStop = threshold == INT32_MAX ? false : true;
    int candidateNum = 0;

    if (v == NULL) {
        v = &vh;
        initVisited = true;
    }

    if (initVisited) {
        InitVisited(base, v, index, ef, m);

        if (discarded != NULL) {
            *discarded = pairingheap_allocate(CompareNearestDiscardedCandidates, NULL);
        }
    }

    /* Create local memory for neighborhood if needed */
    if (index == NULL) {
        neighborhoodSize = HNSW_NEIGHBOR_ARRAY_SIZE(lm);
        neighborhoodData = (HnswNeighborArray *)palloc(neighborhoodSize);
    }

    /* Add entry points to v, C, and W */
    foreach (lc2, ep) {
        HnswCandidate *hc = (HnswCandidate *)lfirst(lc2);
        bool found;
        HnswPairingHeapNode *node;

        if (initVisited) {
            AddToVisited(base, v, hc, index, &found);

            /* OK to count elements instead of tuples */
            if (tuples != NULL) {
                (*tuples)++;
            }
        }

        node = CreatePairingHeapNode(hc);
        pairingheap_add(C, &node->c_node);
        pairingheap_add(W, &node->w_node);
        candidateNum++;

        /*
         * Do not count elements being deleted towards ef when vacuuming. It
         * would be ideal to do this for inserts as well, but this could
         * affect insert performance.
         */
        if (CountElement(base, skipElement, (HnswElement)HnswPtrAccess(base, hc->element))) {
            wlen++;
        }
    }

    while (!pairingheap_is_empty(C)) {
        HnswCandidate *c = HnswGetPairingHeapCandidate(c_node, pairingheap_remove_first(C));
        HnswCandidate *f = HnswGetPairingHeapCandidate(w_node, pairingheap_first(W));
        HnswElement cElement;

        if (c->distance > f->distance || (enableEarlyStop && vNum == threshold))
            break;

        cElement = (HnswElement)HnswPtrAccess(base, c->element);

        /*
         * Iterative scan may resume a discarded candidate that was
         * distance-pruned (payload pin only; no etup / heaptids yet).
         * Load graph metadata before reading its neighbor tuple.
         */
        if (index != NULL && cElement->heaptidsLength == 0) {
            bool vacuumVisibility;
            if (tryMmap) {
                vacuumVisibility = MmapLoadElement(cElement, NULL, &q, index, procinfo, collation, inserting,
                    NULL, enableRabitQ, rbqParams, rbqDiskParams, scan, enablePQ, pqinfo, tryMmap);
            } else {
                vacuumVisibility = HnswLoadElement(cElement, NULL, &q, index, procinfo, collation, inserting,
                    NULL, enableRabitQ, rbqParams, rbqDiskParams, scan, enablePQ, pqinfo, enableLsg);
            }
            if (enableRabitQ && !vacuumVisibility) {
                continue;
            }
        }

        if (index == NULL) {
            HnswLoadUnvisitedFromMemory(base, cElement, unvisited, &unvisitedLength, v,
                                        lc, neighborhoodData, neighborhoodSize);
        } else {
            if (tryMmap) {
                HnswLoadUnvisitedFromMmap(cElement, unvisited, &unvisitedLength, v, index, m, lm, lc, tryMmap);
            } else {
                HnswLoadUnvisitedFromDisk(cElement, unvisited, &unvisitedLength, v, index, m, lm, lc);
            }
        }

        /* OK to count elements instead of tuples */
        if (tuples != NULL) {
            (*tuples) += unvisitedLength;
        }

        for (int i = 0; i < unvisitedLength; i++) {
            HnswElement eElement = unvisited[i];
            float eDistance;
            bool alwaysAdd = wlen < ef;

            f = HnswGetPairingHeapCandidate(w_node, pairingheap_first(W));

            if (index == NULL) {
                if (enablePQ) {
                    uint8 *ePQCode = (uint8*)HnswPtrAccess(base, eElement->pqcodes);
                    size_t pqCodeSz = pqinfo->params.pqM * sizeof(uint8);
                    if (GetPQDistance(ePQCode, pqinfo->qPQCode, &pqinfo->params, pqinfo->pqDistanceTable, &eDistance,
                                      pqCodeSz, pqCodeSz, pqinfo->pqDistanceTableSize, sizeof(float)) != 0) {
                        ereport(ERROR, (errmsg("failed to compute PQ distance")));
                    }
                } else {
                    eDistance = GetCandidateDistance(base, eElement, q, procinfo, collation, enableRabitQ, enableLsg);
                }
            } else {
                bool vacuumVisibility;
                /*
                 * Once W is full, pass the worst-in-W distance so far hops can
                 * skip the graph page. discarded != NULL must not disable that:
                 * iterative scan keeps far candidates as stubs and lazy-loads
                 * etup only if ResumeScanItems actually expands them.
                 */
                float *pruneBound = alwaysAdd ? NULL : &f->distance;
                if (tryMmap) {
                    vacuumVisibility = MmapLoadElement(eElement, &eDistance, &q, index, procinfo, collation, inserting,
                                    pruneBound, enableRabitQ, rbqParams,
                                    rbqDiskParams, scan, enablePQ, pqinfo, tryMmap);
                } else {
                    vacuumVisibility = HnswLoadElement(eElement, &eDistance, &q, index, procinfo, collation, inserting,
                                    pruneBound, enableRabitQ,
                                    rbqParams, rbqDiskParams, scan, enablePQ, pqinfo, enableLsg);
                }
                if (enableRabitQ && !vacuumVisibility) {
                    continue;
                }
            }

            HnswCandidate *e;
            HnswPairingHeapNode *node;
            if (eElement != NULL && (eDistance < f->distance || alwaysAdd)) {
                vNum = 0;

                Assert(!eElement->deleted);

                /* Make robust to issues */
                if (eElement->level < lc) {
                    continue;
                }
                /* Create a new candidate */
                e = (HnswCandidate *)palloc(sizeof(HnswCandidate));
                HnswPtrStore(base, e->element, eElement);
                e->distance = eDistance;

                node = CreatePairingHeapNode(e);
                pairingheap_add(C, &node->c_node);
                pairingheap_add(W, &node->w_node);
                candidateNum++;

                /*
                 * Do not count elements being deleted towards ef when
                 * vacuuming. It would be ideal to do this for inserts as
                 * well, but this could affect insert performance.
                 */
                if (CountElement(base, skipElement, eElement)) {
                    wlen++;

                   /* No need to decrement wlen */
                    if (wlen > ef) {
                        HnswCandidate *d = HnswGetPairingHeapCandidate(w_node, pairingheap_remove_first(W));
                        candidateNum--;

                        if (discarded != NULL) {
                            node = CreatePairingHeapNode(d);
                            pairingheap_add(*discarded, &node->w_node);
                        }
                    }
                }
            } else {
                vNum++;

                if (discarded != NULL) {
                    /* Create a new candidate */
                    e = (HnswCandidate *)palloc(sizeof(HnswCandidate));
                    HnswPtrStore(base, e->element, eElement);
                    e->distance = eDistance;

                    node = CreatePairingHeapNode(e);
                    pairingheap_add(*discarded, &node->w_node);
                }
            }

            if (enableEarlyStop && vNum == threshold) {
                break;
            }
        }
    }

    if (HnswRbqNeedReorder(enableRabitQ, rbqParams, lc)) {
        pairingheap *R = pairingheap_allocate(CompareFurthestCandidates, NULL);
        RabitQConfig *rbqConfig = rbqParams->rbqConfig;
        int64 kreorderStart = candidateNum - rbqConfig->kreorder + 1;
        int num = 0;
        pairingheap_node *node;
        HnswCandidate *c;
        HnswElement cElement;
        BlockNumber blkno = InvalidBlockNumber;
        Buffer buf;
        Page page;
        float refineDis;
        IndexInfo* indexInfo;
        HeapTuple heapTuple;
        float square;
        Buffer heapbuf = InvalidBuffer;
        if (rbqParams->rbqConfig->reType == FP32) {
            indexInfo = BuildIndexInfo(index);
            heapTuple = (HeapTupleData *)heaptup_alloc(BLCKSZ);
        }

        while (!pairingheap_is_empty(W)) {
            num++;
            node = pairingheap_remove_first(W);
            if (num < kreorderStart) {
                continue;
            }
            heapbuf = InvalidBuffer;
            c = HnswGetPairingHeapCandidate(w_node, node);
            cElement = (HnswElement)HnswPtrAccess(base, c->element);

            buf = ReadBuffer(index, cElement->blkno);
            LockBuffer(buf, BUFFER_LOCK_SHARE);
            page = BufferGetPage(buf);

            HnswElementTuple etup = (HnswElementTuple)PageGetItem(page, PageGetItemId(page, cElement->offno));
            Assert(HnswIsElementTuple(etup));

            if (rbqParams->rbqConfig->reType == SQ8) {
                uint8 *refineCode = getRefineCode(PointerGetDatum(&etup->data), rbqConfig->reOffset);
                ScalarQuantizer *sq = rbqConfig->sq;
                int dim = sq->dim;
                VectorDecodeSQ(dim, sq->trained, sq->trained + dim, sq->decodeVec->x, refineCode);
                refineDis = HNSWRbqComputeDis(rbqParams, sq->decodeVec->x);
                UnlockReleaseBuffer(buf);
            } else if (rbqParams->rbqConfig->reType == FP32) {
                if (!ItemPointerIsValid(&etup->heaptids[0])) {
                    continue;
                }
                Datum eRbqDiskData = HnswGetVectorFromHeap(rbqParams->heap, etup->heaptids, indexInfo,
                                                            heapTuple, procinfo, NULL, collation, &heapbuf);
                UnlockReleaseBuffer(buf);
                float *eRbqDiskVec = NULL;
                if (IS_HALFVEC(procinfo->fn_oid)) {
                    eRbqDiskVec = Halfvec2Vector(eRbqDiskData)->x;
                } else {
                    eRbqDiskVec = ((Vector *)eRbqDiskData)->x;
                }
                refineDis = HNSWRbqComputeDis(rbqParams, eRbqDiskVec);
                if (BufferIsValid(heapbuf)) {
                    ReleaseBuffer(heapbuf);
                }

            } else {
                UnlockReleaseBuffer(buf);
                ereport(ERROR, (errmsg("HNSW RabitQ rerank type error!")));
            }

            c->distance = refineDis;
            pairingheap_add(R, node);
        }
        W = R;
        if (rbqParams->rbqConfig->reType == FP32) {
            pfree(indexInfo);
            pfree(heapTuple);
        }
    }

    /* Add each element of W to w */
    while (!pairingheap_is_empty(W)) {
        HnswCandidate *hc = HnswGetPairingHeapCandidate(w_node, pairingheap_remove_first(W));

        w = lcons(hc, w);
    }

    return w;
}

/*
 * Compare candidate distances with pointer tie-breaker
 */
static int
    CompareCandidateDistances(const void *a, const void *b)
{
    HnswCandidate *hca = (HnswCandidate *)lfirst(*(ListCell **)a);
    HnswCandidate *hcb = (HnswCandidate *)lfirst(*(ListCell **)b);

    if (hca->distance < hcb->distance) {
        return 1;
    }

    if (hca->distance > hcb->distance) {
        return -1;
    }

    if (HnswPtrPointer(hca->element) < HnswPtrPointer(hcb->element)) {
        return 1;
    }

    if (HnswPtrPointer(hca->element) > HnswPtrPointer(hcb->element)) {
        return -1;
    }

    return 0;
}

/*
 * Compare candidate distances with offset tie-breaker
 */
static int
    CompareCandidateDistancesOffset(const void *a, const void *b)
{
    HnswCandidate *hca = (HnswCandidate *)lfirst(*(ListCell **)a);
    HnswCandidate *hcb = (HnswCandidate *)lfirst(*(ListCell **)b);

    if (hca->distance < hcb->distance) {
        return 1;
    }

    if (hca->distance > hcb->distance) {
        return -1;
    }

    if (HnswPtrOffset(hca->element) < HnswPtrOffset(hcb->element)) {
        return 1;
    }

    if (HnswPtrOffset(hca->element) > HnswPtrOffset(hcb->element)) {
        return -1;
    }

    return 0;
}

/*
 * Calculate the distance between elements
 */
static float HnswGetDistance(char *base, HnswElement a, HnswElement b, FmgrInfo *procinfo, Oid collation,
                             bool enableRabitQ, bool enableLsg)
{
    Datum aValue = HnswGetValue(base, a);
    Datum bValue = HnswGetValue(base, b);
    float realDis = DatumGetFloat8(FunctionCall2Coll(procinfo, collation, aValue, bValue));

    if (!HnswUseIsoDistance(procinfo, enableLsg)) {
        return realDis;
    }

    return HnswGetIsoWeight(aValue, bValue) * realDis;
}

/*
 * Check if an element is closer to q than any element from R
 */
static bool CheckElementCloser(char *base, HnswCandidate *e, List *r, FmgrInfo *procinfo, Oid collation,
                               bool enableRabitQ, bool enableLsg)
{
    HnswElement eElement = (HnswElement)HnswPtrAccess(base, e->element);
    ListCell *lc2;

    foreach (lc2, r) {
        HnswCandidate *ri = (HnswCandidate *)lfirst(lc2);
        HnswElement riElement = (HnswElement)HnswPtrAccess(base, ri->element);
        float distance = HnswGetDistance(base, eElement, riElement, procinfo, collation, enableRabitQ, enableLsg);

        if (distance <= e->distance) {
            return false;
        }
    }

    return true;
}

/*
 * Algorithm 4 from paper
 */
static List *SelectNeighbors(char *base, List *c, int lm, int lc, FmgrInfo *procinfo, Oid collation, HnswElement e2,
                             HnswCandidate *newCandidate, HnswCandidate **pruned, bool sortCandidates,
                             bool enableRabitQ, bool enableLsg)
{
    List *r = NIL;
    List *w = list_copy(c);
    HnswCandidate **wd;
    int wdlen = 0;
    int wdoff = 0;
    HnswNeighborArray *neighbors = HnswGetNeighbors(base, e2, lc);
    bool mustCalculate = !neighbors->closerSet;
    List *added = NIL;
    bool removedAny = false;

    if (list_length(w) <= lm) {
        return w;
    }

    wd = (HnswCandidate **)palloc(sizeof(HnswCandidate *) * list_length(w));

    /* Ensure order of candidates is deterministic for closer caching */
    if (sortCandidates) {
        if (base == NULL) {
            list_sort(w, CompareCandidateDistances);
        } else {
            list_sort(w, CompareCandidateDistancesOffset);
        }
    }

    while (list_length(w) > 0 && list_length(r) < lm) {
        /* Assumes w is already ordered desc */
        HnswCandidate *e = (HnswCandidate *)linitial(w);

        w = list_delete_first(w);

        /* Use previous state of r and wd to skip work when possible */
        if (mustCalculate) {
            e->closer = CheckElementCloser(base, e, r, procinfo, collation, enableRabitQ, enableLsg);
        } else if (list_length(added) > 0) {
            /* Keep Valgrind happy for in-memory, parallel builds */
            if (base != NULL) {
                VALGRIND_MAKE_MEM_DEFINED(&e->closer, 1);
            }

            /*
             * If the current candidate was closer, we only need to compare it
             * with the other candidates that we have added.
             */
            if (e->closer) {
                e->closer = CheckElementCloser(base, e, added, procinfo, collation, enableRabitQ, enableLsg);

                if (!e->closer) {
                    removedAny = true;
                }
            } else {
                /*
                 * If we have removed any candidates from closer, a candidate
                 * that was not closer earlier might now be.
                 */
                if (removedAny) {
                    e->closer = CheckElementCloser(base, e, r, procinfo, collation, enableRabitQ, enableLsg);
                    if (e->closer) {
                        added = lappend(added, e);
                    }
                }
            }
        } else if (e == newCandidate) {
            e->closer = CheckElementCloser(base, e, r, procinfo, collation, enableRabitQ, enableLsg);
            if (e->closer) {
                added = lappend(added, e);
            }
        }

        /* Keep Valgrind happy for in-memory, parallel builds */
        if (base != NULL) {
            VALGRIND_MAKE_MEM_DEFINED(&e->closer, 1);
        }

        if (e->closer) {
            r = lappend(r, e);
        } else {
            wd[wdlen++] = e;
        }
    }

    /* Cached value can only be used in future if sorted deterministically */
    neighbors->closerSet = sortCandidates;

    /* Keep pruned connections */
    while (wdoff < wdlen && list_length(r) < lm) {
        r = lappend(r, wd[wdoff++]);
    }

    /* Return pruned for update connections */
    if (pruned != NULL) {
        if (wdoff < wdlen) {
            *pruned = wd[wdoff];
        } else {
            *pruned = (HnswCandidate *)linitial(w);
        }
    }

    return r;
}

/*
 * Add connections
 */
static void AddConnections(char *base, HnswElement element, List *neighbors, int lc)
{
    ListCell *lc2;
    HnswNeighborArray *a = HnswGetNeighbors(base, element, lc);

    foreach (lc2, neighbors)
        a->items[a->length++] = *((HnswCandidate *)lfirst(lc2));
}

/*
 * Update connections
 */
void HnswUpdateConnection(char *base, HnswElement element, HnswCandidate *hc, int lm, int lc, int *updateIdx,
                          Relation index, FmgrInfo *procinfo, Oid collation, bool enableRabitQ,
                          RabitqInsertOnDiskParams *rbqDiskParams, bool enableLsg)
{
    HnswElement hce = (HnswElement)HnswPtrAccess(base, hc->element);
    HnswNeighborArray *currentNeighbors = HnswGetNeighbors(base, hce, lc);
    HnswCandidate hc2;

    HnswPtrStore(base, hc2.element, element);
    hc2.distance = hc->distance;

    if (currentNeighbors->length < lm) {
        currentNeighbors->items[currentNeighbors->length++] = hc2;

        /* Track update */
        if (updateIdx != NULL) {
            *updateIdx = -2;
        }
    } else {
        /* Shrink connections */
        HnswCandidate *pruned = NULL;

        /* Load elements on insert */
        if (index != NULL) {
            Datum q = HnswGetValue(base, hce);

            for (int i = 0; i < currentNeighbors->length; i++) {
                HnswCandidate *hc3 = &currentNeighbors->items[i];
                HnswElement hc3Element = (HnswElement)HnswPtrAccess(base, hc3->element);
                bool vacuumVisibility = true;

                if (HnswPtrIsNull(base, hc3Element->value))
                    vacuumVisibility = HnswLoadElement(hc3Element, &hc3->distance, &q, index, procinfo, collation, true,
                                       NULL, enableRabitQ, NULL, rbqDiskParams, NULL, false, NULL, enableLsg);
                else
                    hc3->distance = GetCandidateDistance(base, hc3Element, q, procinfo, collation, enableRabitQ,
                                                         enableLsg);

                /* Prune element if being deleted */
                if (hc3Element->heaptidsLength == 0 || (enableRabitQ && !vacuumVisibility)) {
                    pruned = &currentNeighbors->items[i];
                    break;
                }
            }
        }

        if (pruned == NULL) {
            List *c = NIL;

            /* Add candidates */
            for (int i = 0; i < currentNeighbors->length; i++) {
                c = lappend(c, &currentNeighbors->items[i]);
            }
            c = lappend(c, &hc2);

            SelectNeighbors(base, c, lm, lc, procinfo, collation, hce, &hc2, &pruned, true, enableRabitQ, enableLsg);

            /* Should not happen */
            if (pruned == NULL)
                return;
        }

        /* Find and replace the pruned element */
        for (int i = 0; i < currentNeighbors->length; i++) {
            if (HnswPtrEqual(base, currentNeighbors->items[i].element, pruned->element)) {
                currentNeighbors->items[i] = hc2;

                /* Track update */
                if (updateIdx != NULL) {
                    *updateIdx = i;
                }

                break;
            }
        }
    }
}

/*
 * Remove elements being deleted or skipped
 */
static List *RemoveElements(char *base, List *w, HnswElement skipElement)
{
    ListCell *lc2;
    List *w2 = NIL;

    /* Ensure does not access heaptidsLength during in-memory build */
    pg_memory_barrier();

    foreach (lc2, w) {
        HnswCandidate *hc = (HnswCandidate *)lfirst(lc2);
        HnswElement hce = (HnswElement)HnswPtrAccess(base, hc->element);

        /* Skip self for vacuuming update */
        if (skipElement != NULL && hce->blkno == skipElement->blkno && hce->offno == skipElement->offno) {
            continue;
        }

        if (hce->heaptidsLength != 0) {
            w2 = lappend(w2, hc);
        }
    }

    return w2;
}

/*
 * Algorithm 1 from paper
 */
void HnswFindElementNeighbors(char *base, HnswElement element, HnswElement entryPoint, Relation index,
                              FmgrInfo *procinfo, Oid collation, int m, int efConstruction, bool existing,
                              bool enablePQ, PQParams *params, bool enableRabitQ,
                              RabitqInsertOnDiskParams *rbqDiskParams, bool enableLsg)
{
    List *ep;
    List *w;
    int level = element->level;
    int entryLevel;
    Datum q = HnswGetValue(base, element);
    HnswElement skipElement = existing ? element : NULL;

    if (enablePQ) {
        /* compute pq code */
        Size codesize = params->pqM * sizeof(uint8);
        uint8 *pqcode = (uint8*)palloc(codesize);
        if (ComputeVectorPQCode(DatumGetVector(q)->x, params, pqcode, codesize) != 0) {
            ereport(ERROR, (errmsg("failed to compute PQ vector code")));
        }
        Pointer codePtr = (Pointer)HnswPtrAccess(base, element->pqcodes);
        errno_t err = memcpy_s(codePtr, codesize, pqcode, codesize);
        securec_check(err, "\0", "\0");
    }

    /* No neighbors if no entry point */
    if (entryPoint == NULL)
        return;

    /* Get entry point and level */
    ep = list_make1(HnswEntryCandidate(base, entryPoint, q, index, procinfo, collation, true, enableRabitQ, NULL,
                                       rbqDiskParams, NULL, false, NULL, enableLsg));
    entryLevel = entryPoint->level;

    /* 1st phase: greedy search to insert level */
    for (int lc = entryLevel; lc >= level + 1; lc--) {
        w = HnswSearchLayer(base, q, ep, 1, lc, index, procinfo, collation, m, true, skipElement,
                            NULL, NULL, true, NULL, enableRabitQ, NULL, rbqDiskParams, false, NULL, false, NULL,
                            enableLsg);
        ep = w;
    }

    if (level > entryLevel) {
        level = entryLevel;
    }

    /* Add one for existing element */
    if (existing) {
        efConstruction++;
    }
    /* 2nd phase */
    for (int lc = level; lc >= 0; lc--) {
        int lm = HnswGetLayerM(m, lc);
        List *neighbors;
        List *lw;

        w = HnswSearchLayer(base, q, ep, efConstruction, lc, index, procinfo, collation, m, true, skipElement,
                            NULL, NULL, true, NULL, enableRabitQ, NULL, rbqDiskParams, false, NULL, false, NULL,
                            enableLsg);

        /* Elements being deleted or skipped can help with search */
        /* but should be removed before selecting neighbors */
        if (index != NULL)
            lw = RemoveElements(base, w, skipElement);
        else
            lw = w;

        /*
         * Candidates are sorted, but not deterministically. Could set
         * sortCandidates to true for in-memory builds to enable closer
         * caching, but there does not seem to be a difference in performance.
         */
        neighbors = SelectNeighbors(base, lw, lm, lc, procinfo, collation, element, NULL, NULL, false, enableRabitQ,
                                    enableLsg);

        AddConnections(base, element, neighbors, lc);

        ep = w;
    }
}

/*
* Get the info related to pqTable in metapage
*/
void HnswGetPQInfoFromMetaPage(Relation index, uint16 *pqTableNblk, uint32 *pqTableSize,
                               uint16 *pqDisTableNblk, uint32 *pqDisTableSize)
{
    Buffer buf;
    Page page;

    if (HnswIndexRelationSkipsPhysicalMetapage(index)) {
        if (pqTableNblk != NULL) {
            *pqTableNblk = 0;
        }
        if (pqTableSize != NULL) {
            *pqTableSize = 0;
        }
        if (pqDisTableNblk != NULL) {
            *pqDisTableNblk = 0;
        }
        if (pqDisTableSize != NULL) {
            *pqDisTableSize = 0;
        }
        return;
    }

    buf = ReadBuffer(index, HNSW_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);

    HnswMetaPage metap = HnswPageGetMeta(page);
    PG_TRY();
    {
        if (unlikely(metap->magicNumber != HNSW_MAGIC_NUMBER)) {
            elog(ERROR, "hnsw index is not valid");
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
    if (pqDisTableNblk != NULL) {
        *pqDisTableNblk = metap->pqDisTableNblk;
    }
    if (pqDisTableSize != NULL) {
        *pqDisTableSize = metap->pqDisTableSize;
    }

    UnlockReleaseBuffer(buf);
}
void HnswGetLsgInfoFromMetaPage(Relation index, uint32* lsgCodeBookSize, uint16* nBlks, uint32* lsgDim,
                                uint32* lsgSampleSize, bool* enableLsg)
{
    Buffer buf;
    Page page;

    if (HnswIndexRelationSkipsPhysicalMetapage(index)) {
        if (lsgCodeBookSize != NULL) {
            *lsgCodeBookSize = 0;
        }
        if (nBlks != NULL) {
            *nBlks = 0;
        }
        if (lsgDim != NULL) {
            *lsgDim = 0;
        }
        if (lsgSampleSize != NULL) {
            *lsgSampleSize = 0;
        }
        if (enableLsg != NULL) {
            *enableLsg = false;
        }
        return;
    }
    buf = ReadBuffer(index, HNSW_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);

    HnswMetaPage metap = HnswPageGetMeta(page);

    if (lsgCodeBookSize != NULL) {
        *lsgCodeBookSize = metap->lsgCodeBookSize;
    }
    if (nBlks != NULL) {
        *nBlks = metap->lsgSampleNblk;
    }
    if (lsgDim != NULL) {
        *lsgDim = metap->dimensions;
    }
    if (lsgSampleSize != NULL) {
        *lsgSampleSize = metap->lsgSampleSize;
    }
    if (enableLsg != NULL) {
        *enableLsg = metap->enableLsg;
    }
    UnlockReleaseBuffer(buf);
}

void InitPQParamsOnDisk(PQParams *params, Relation index, FmgrInfo *procinfo, int dim, bool *enablePQ, bool trymmap)
{
    const HnswTypeInfo *typeInfo = HnswGetTypeInfo(index);

    InitParamsMetaPage(index, params, enablePQ, trymmap);
    int pqMode = HNSW_PQMODE_DEFAULT;

    if (*enablePQ && !g_instance.pq_inited) {
        ereport(ERROR, (errmsg("the SQL involves operations related to HNSWPQ, "
                               "but this instance has not currently loaded the PQ dynamic library.")));
    }

    if (*enablePQ) {
        params->funcType = GetFunctionType(procinfo, HnswOptionalProcInfo(index, HNSW_NORM_PROC));
        params->dim = dim;
        Size subItemsize = typeInfo->itemSize(dim / params->pqM);
        params->subItemSize = MAXALIGN(subItemsize);
        /* Now save pqTable and pqDistanceTable in the relcache entry. */
        if (index->pqTable == NULL) {
            MemoryContext oldcxt = MemoryContextSwitchTo(index->rd_indexcxt);
            index->pqTable = LoadPQtable(index);
            (void)MemoryContextSwitchTo(oldcxt);
        }
        if (index->pqDistanceTable == NULL && pqMode == HNSW_PQMODE_SDC) {
            MemoryContext oldcxt = MemoryContextSwitchTo(index->rd_indexcxt);
            index->pqDistanceTable = LoadPQDisTable(index);
            (void)MemoryContextSwitchTo(oldcxt);
        }
        params->pqTable = index->pqTable;
    } else {
        params->pqTable = NULL;
    }

}

void HnswInitDefaultRbqMetaPageInfo(HnswRbqMetaPageInfo *info)
{
    info->enableRabitQ = false;
    info->useFHT = false;
    info->reOffset = 0;
    info->reType = SQ8;
    info->matrixNblk = 0;
    info->matrixSize = 0;
    info->otherNblk = 0;
    info->otherSize = 0;
    info->rbqDelayState = RBQ_BUILD_NORMAL;
    info->rbqInsertRows = 0;
}

static void HnswLoadRbqMetaPageInfoFromPage(HnswMetaPage metap, HnswRbqMetaPageInfo *info)
{
    info->enableRabitQ = metap->enableRabitQ;
    info->useFHT = metap->useFHT;
    info->reOffset = metap->reOffset;
    info->reType = metap->reType;
    info->matrixNblk = metap->matrixNblk;
    info->matrixSize = metap->matrixSize;
    info->otherNblk = metap->otherNblk;
    info->otherSize = metap->otherSize;
    info->rbqDelayState = metap->rbqDelayState;
    info->rbqInsertRows = metap->rbqInsertRows;
}

static void HnswReadRbqMetaPageInfoFromRelation(Relation index, HnswRbqMetaPageInfo *info)
{
    Buffer buf;
    Page page;
    HnswMetaPage metap;

    HnswInitDefaultRbqMetaPageInfo(info);

    buf = ReadBuffer(index, HNSW_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    metap = HnswPageGetMeta(page);
    if (unlikely(metap->magicNumber != HNSW_MAGIC_NUMBER)) {
        UnlockReleaseBuffer(buf);
        elog(ERROR, "hnsw index is not valid");
    }
    HnswLoadRbqMetaPageInfoFromPage(metap, info);
    UnlockReleaseBuffer(buf);
}

static bool HnswPartitionIndexRbqDelayStateIsDelay(Relation indexRel)
{
    HnswRbqMetaPageInfo info;

    HnswReadRbqMetaPageInfoFromRelation(indexRel, &info);
    return info.rbqDelayState == RBQ_BUILD_DELAY;
}

static int HnswAggregateLocalIndexRbqDelayState(Relation parentIndex)
{
    List *partOids = indexGetPartitionOidList(parentIndex);
    ListCell *cell = NULL;

    foreach (cell, partOids) {
        Oid partOid = lfirst_oid(cell);
        Partition part = partitionOpen(parentIndex, partOid, AccessShareLock);

        if (PartitionHasSubpartition(part)) {
            Relation level1IndexRel = partitionGetRelation(parentIndex, part);
            List *subPartTuples = searchPgPartitionByParentId(PART_OBJ_TYPE_INDEX_PARTITION, partOid);
            ListCell *subCell = NULL;

            foreach (subCell, subPartTuples) {
                Oid subPartOid = HeapTupleGetOid((HeapTuple)lfirst(subCell));
                Partition subPart = partitionOpen(level1IndexRel, subPartOid, AccessShareLock);
                Relation subIndexRel = partitionGetRelation(level1IndexRel, subPart);

                if (HnswPartitionIndexRbqDelayStateIsDelay(subIndexRel)) {
                    releaseDummyRelation(&subIndexRel);
                    partitionClose(level1IndexRel, subPart, AccessShareLock);
                    freePartList(subPartTuples);
                    releaseDummyRelation(&level1IndexRel);
                    partitionClose(parentIndex, part, AccessShareLock);
                    list_free(partOids);
                    return RBQ_BUILD_DELAY;
                }
                releaseDummyRelation(&subIndexRel);
                partitionClose(level1IndexRel, subPart, AccessShareLock);
            }
            freePartList(subPartTuples);
            releaseDummyRelation(&level1IndexRel);
        } else {
            Relation partIndexRel = partitionGetRelation(parentIndex, part);

            if (HnswPartitionIndexRbqDelayStateIsDelay(partIndexRel)) {
                releaseDummyRelation(&partIndexRel);
                partitionClose(parentIndex, part, AccessShareLock);
                list_free(partOids);
                return RBQ_BUILD_DELAY;
            }
            releaseDummyRelation(&partIndexRel);
        }
        partitionClose(parentIndex, part, AccessShareLock);
    }

    list_free(partOids);
    return RBQ_BUILD_NORMAL;
}

void HnswGetRbqMetaPageInfo(Relation index, HnswRbqMetaPageInfo *info)
{
    HnswInitDefaultRbqMetaPageInfo(info);

    if (HnswIndexRelationSkipsPhysicalMetapage(index)) {
        info->rbqDelayState = HnswAggregateLocalIndexRbqDelayState(index);
        return;
    }

    HnswReadRbqMetaPageInfoFromRelation(index, info);
}

void* LoadRbq(Relation index, uint16 startBlkNo, uint16 nblk, uint32 size)
{
    Buffer buf;
    Page page;
    uint32 curFlushSize;
    void *rbq = (void *)palloc0(size);

    for (uint16 i = 0; i < nblk; i++) {
        curFlushSize = (i == nblk - 1) ? (size - i * CHUNK_STORAGE_SIZE) : CHUNK_STORAGE_SIZE;
        buf = ReadBuffer(index, startBlkNo + i);
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buf);
        errno_t err = memcpy_s((char *)rbq + i * CHUNK_STORAGE_SIZE, curFlushSize,
                               PageGetContents(page), curFlushSize);
        securec_check(err, "\0", "\0");
        UnlockReleaseBuffer(buf);
    }
    return rbq;
}

RabitQConfig *InitRbqConfigOnDisk(Relation index, bool *enableRabitQ, float **centroid, int dim)
{
    HnswRbqMetaPageInfo rbqInfo;

    HnswGetRbqMetaPageInfo(index, &rbqInfo);
    if (enableRabitQ != NULL) {
        *enableRabitQ = rbqInfo.enableRabitQ;
    }

    if (!rbqInfo.enableRabitQ) {
        return NULL;
    }
    if (index->rbqMatrix == NULL) {
        MemoryContext oldcxt = MemoryContextSwitchTo(index->rd_indexcxt);
        void *rbq = LoadRbq(index, HNSW_CHUNK_START_BLKNO, rbqInfo.matrixNblk, rbqInfo.matrixSize);
        if (rbqInfo.useFHT) {
            FastRotation *fr = FhtDeserialize(rbq);
            index->rbqMatrix = (void *)fr;
            pfree(rbq);
        } else {
            index->rbqMatrix = rbq;
        }
        (void)MemoryContextSwitchTo(oldcxt);
    }
    if (index->rbqOther == NULL) {
        MemoryContext oldcxt = MemoryContextSwitchTo(index->rd_indexcxt);
        index->rbqOther = (float *)LoadRbq(index, HNSW_CHUNK_START_BLKNO + rbqInfo.matrixNblk,
                          rbqInfo.otherNblk, rbqInfo.otherSize);
        (void)MemoryContextSwitchTo(oldcxt);
    }
 
    RabitQConfig *rbqConfig = (RabitQConfig *)palloc(sizeof(RabitQConfig));
    rbqConfig->FHT = rbqInfo.useFHT;
    rbqConfig->reOffset = rbqInfo.reOffset;
    rbqConfig->reType = rbqInfo.reType;
    if (rbqInfo.reType == SQ8) {
        rbqConfig->sq = (ScalarQuantizer *)palloc(sizeof(ScalarQuantizer));
        rbqConfig->sq->dim = dim;
        rbqConfig->sq->trained = index->rbqOther + dim;
        rbqConfig->sq->decodeVec = InitVector(dim);
    } else {
        rbqConfig->sq = NULL;
    }
    VectorTransform *vtrans = (VectorTransform *)palloc(sizeof(VectorTransform));
    rbqConfig->vtrans = vtrans;
    vtrans->dim = dim;
    if (rbqInfo.useFHT) {
        vtrans->type = FAST_HTRANSFORM;
        vtrans->fastRotation = (FastRotation *)index->rbqMatrix;
    } else {
        vtrans->type = RANDOM_ORTHOGONAL;
        vtrans->matrix = (float *)index->rbqMatrix;
    }
    *centroid = index->rbqOther;
    return rbqConfig;
}

void HnswComputeVectorRBQCode(HnswElement element, Vector *transformedVec, float *centroid, int funcType, char *base)
{
    RabitqVector *rbqVec = (RabitqVector *)HnswPtrAccess(base, element->rbqcodes);
    ComputeVectorRBQCode(transformedVec->dim, transformedVec->x, rbqVec, centroid, funcType);
}

void InitLsgSamplesOnDisk(Relation index, FmgrInfo *procinfo, LsgCalculator** LocScalingParam, bool *enableLsg)
{
    uint32 lsgStartNblk;
    uint32 codeBookSize;
    uint16 nBlks;
    uint32 dim;
    uint32 sampleSize;
    uint32 curFlushSize;
    Buffer buf;
    Page page;

    HnswGetLsgInfoFromMetaPage(index, &codeBookSize, &nBlks, &dim, &sampleSize, enableLsg);

    if (*enableLsg) {
        if (index->sampleVec == NULL) {
            MemoryContext oldcxt = MemoryContextSwitchTo(index->rd_indexcxt);
            index->sampleVec = (float*)palloc0(codeBookSize);
            for (int16 i = 0; i < nBlks; i++) {
                curFlushSize = (i == nBlks - 1) ? (codeBookSize - i * LSGSAMPLE_STORAGE_SIZE) : LSGSAMPLE_STORAGE_SIZE;
                buf = ReadBuffer(index, HNSW_LSG_SAMPLE_START_BLKNO + i);
                LockBuffer(buf, BUFFER_LOCK_SHARE);
                page = BufferGetPage(buf);
                errno_t err = memcpy_s((char*)index->sampleVec, curFlushSize, PageGetContents(page), curFlushSize);
                securec_check(err, "\0", "\0");
                UnlockReleaseBuffer(buf);
            }
            (void)MemoryContextSwitchTo(oldcxt);
        }
        *LocScalingParam = (LsgCalculator*)palloc0(sizeof(LsgCalculator));
        InitScalingParam(*LocScalingParam, sampleSize, dim, index->sampleVec,
                         GetLsgfunctionType(procinfo, HnswOptionalProcInfo(index, HNSW_NORM_PROC)),
                         HnswGetLsgDegree(index), HnswGetLsgAlpha(index));
    }
    return;
}

static void SparsevecCheckValue(Pointer v)
{
    SparseVector *vec = (SparseVector *)v;

    if (vec->nnz > HNSW_MAX_NNZ) {
        elog(ERROR, "sparsevec cannot have more than %d non-zero elements for hnsw index", HNSW_MAX_NNZ);
    }
}

/*
 * Get type info
 */
const HnswTypeInfo *HnswGetTypeInfo(Relation index)
{
    FmgrInfo *procinfo = HnswOptionalProcInfo(index, HNSW_TYPE_INFO_PROC);

    if (procinfo == NULL) {
        static const HnswTypeInfo typeInfo = {
            .maxDimensions = HNSW_MAX_DIM, .supportPQ = true, .supportRabitQ = true,
            .itemSize = VectorItemSize, .normalize = l2_normalize, .checkValue = NULL};
        return (&typeInfo);
    } else {
        return (const HnswTypeInfo *)DatumGetPointer(OidFunctionCall0Coll(procinfo->fn_oid, InvalidOid));
    }
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(hnsw_halfvec_support);
Datum hnsw_halfvec_support(PG_FUNCTION_ARGS)
{
    static const HnswTypeInfo typeInfo = {
        .maxDimensions = HNSW_MAX_DIM * 2, .supportPQ = false, .supportRabitQ = true,
        .itemSize = HalfvecItemSize, .normalize = halfvec_l2_normalize, .checkValue = NULL};

    PG_RETURN_POINTER(&typeInfo);
};

PGDLLEXPORT PG_FUNCTION_INFO_V1(hnsw_bit_support);
Datum hnsw_bit_support(PG_FUNCTION_ARGS)
{
    static const HnswTypeInfo typeInfo = {.maxDimensions = HNSW_MAX_DIM * 32, .supportPQ = false,
                                          .supportRabitQ = false, .itemSize = BitItemSize,
                                          .normalize = NULL, .checkValue = NULL};

    PG_RETURN_POINTER(&typeInfo);
};

PGDLLEXPORT PG_FUNCTION_INFO_V1(hnsw_sparsevec_support);
Datum hnsw_sparsevec_support(PG_FUNCTION_ARGS)
{
    static const HnswTypeInfo typeInfo = {
        .maxDimensions = SPARSEVEC_MAX_DIM, .supportPQ = false, .supportRabitQ = false,
        .itemSize = NULL, .normalize = sparsevec_l2_normalize, .checkValue = SparsevecCheckValue};

    PG_RETURN_POINTER(&typeInfo);
};
