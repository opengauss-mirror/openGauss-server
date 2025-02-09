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

#include "access/generic_xlog.h"
#include "catalog/pg_type.h"
#include "fmgr.h"
#include "access/datavec/hnsw.h"
#include "lib/pairingheap.h"
#include "access/datavec/halfvec.h"
#include "access/datavec/sparsevec.h"
#include "access/datavec/utils.h"
#include "storage/buf/bufmgr.h"
#include "utils/datum.h"
#include "utils/rel.h"

#include "utils/hashutils.h"

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

typedef union {
    pointerhash_hash *pointers;
    offsethash_hash *offsets;
    tidhash_hash *tids;
} VisitedHash;

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
Buffer HnswNewBuffer(Relation index, ForkNumber forkNum)
{
    Buffer buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);

    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
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
HnswElement HnswInitElement(char *base, ItemPointer heaptid, int m, double ml, int maxLevel, HnswAllocator *allocator)
{
    HnswElement element = (HnswElement)HnswAlloc(allocator, sizeof(HnswElementData));

    int level = static_cast<int>(-log(RandomDouble()) * ml);
    /* Cap level */
    if (level > maxLevel) {
        level = maxLevel;
    }

    element->heaptidsLength = 0;
    HnswAddHeapTid(element, heaptid);

    element->level = level;
    element->deleted = 0;

    HnswInitNeighbors(base, element, m, allocator);

    HnswPtrStore(base, element->value, (Pointer)NULL);

    return element;
}

/*
 * Add a heap TID to an element
 */
void HnswAddHeapTid(HnswElement element, ItemPointer heaptid)
{
    element->heaptids[element->heaptidsLength++] = *heaptid;
}

/*
 * Allocate an element from block and offset numbers
 */
HnswElement HnswInitElementFromBlock(BlockNumber blkno, OffsetNumber offno)
{
    HnswElement element = (HnswElement)palloc(sizeof(HnswElementData));
    char *base = NULL;

    element->blkno = blkno;
    element->offno = offno;
    HnswPtrStore(base, element->neighbors, (HnswNeighborArrayPtr *)NULL);
    HnswPtrStore(base, element->value, (Pointer)NULL);
    return element;
}

/*
 * Get the metapage info
 */
void HnswGetMetaPageInfo(Relation index, int *m, HnswElement *entryPoint)
{
    Buffer buf;
    Page page;
    HnswMetaPage metap;

    buf = ReadBuffer(index, HNSW_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    metap = HnswPageGetMeta(page);
    if (unlikely(metap->magicNumber != HNSW_MAGIC_NUMBER))
        elog(ERROR, "hnsw index is not valid");

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

    UnlockReleaseBuffer(buf);
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

void FlushPQInfoInternal(Relation index, char* table, BlockNumber startBlkno, uint16 nblks, uint32 totalSize)
{
    Buffer buf;
    Page page;
    uint32 curFlushSize;
    for (uint16 i = 0; i < nblks; i++) {
        curFlushSize = (i == nblks - 1) ?
                        (totalSize - i * HNSW_PQTABLE_STORAGE_SIZE) : HNSW_PQTABLE_STORAGE_SIZE;
        buf = ReadBufferExtended(index, MAIN_FORKNUM, startBlkno + i, RBM_NORMAL, NULL);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        page = BufferGetPage(buf);
        errno_t err = memcpy_s(PageGetContents(page), curFlushSize,
                        table + i * HNSW_PQTABLE_STORAGE_SIZE, curFlushSize);
        securec_check(err, "\0", "\0");
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
    FlushPQInfoInternal(index, pqTable, HNSW_PQTABLE_START_BLKNO, pqTableNblk, pqTableSize);
    if (buildstate->pqMode == HNSW_PQMODE_SDC) {
        /* Flush pq distance table */
        FlushPQInfoInternal(index, (char*)pqDistanceTable,
                            HNSW_PQTABLE_START_BLKNO + pqTableNblk, pqDisTableNblk, pqDisTableSize);
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
        curFlushSize = (i == nblks - 1) ? (pqTableSize - i * HNSW_PQTABLE_STORAGE_SIZE) : HNSW_PQTABLE_STORAGE_SIZE;
        buf = ReadBuffer(index, HNSW_PQTABLE_START_BLKNO + i);
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buf);
        errno_t err = memcpy_s(pqTable + i * HNSW_PQTABLE_STORAGE_SIZE, curFlushSize,
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

    BlockNumber startBlkno = HNSW_PQTABLE_START_BLKNO + pqTableNblk;
    for (uint16 i = 0; i < nblks; i++) {
        curFlushSize = (i == nblks - 1) ? (pqDisTableSize - i * HNSW_PQTABLE_STORAGE_SIZE) : HNSW_PQTABLE_STORAGE_SIZE;
        buf = ReadBuffer(index, startBlkno + i);
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buf);
        errno_t err = memcpy_s((char*)disTable + i * HNSW_PQTABLE_STORAGE_SIZE, curFlushSize,
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

/*
 * Set element tuple, except for neighbor info
 */
void HnswSetElementTuple(char *base, HnswElementTuple etup, HnswElement element)
{
    Pointer valuePtr = (Pointer)HnswPtrAccess(base, element->value);
    errno_t rc = EOK;

    etup->type = HNSW_ELEMENT_TUPLE_TYPE;
    etup->level = element->level;
    etup->deleted = 0;
    for (int i = 0; i < HNSW_HEAPTIDS; i++) {
        if (i < element->heaptidsLength)
            etup->heaptids[i] = element->heaptids[i];
        else
            ItemPointerSetInvalid(&etup->heaptids[i]);
    }
    rc = memcpy_s(&etup->data, VARSIZE_ANY(valuePtr), valuePtr, VARSIZE_ANY(valuePtr));
    securec_check(rc, "\0", "\0");
}

/*
 * Set neighbor tuple
 */
void HnswSetNeighborTuple(char *base, HnswNeighborTuple ntup, HnswElement e, int m)
{
    int idx = 0;

    ntup->type = HNSW_NEIGHBOR_TUPLE_TYPE;

    for (int lc = e->level; lc >= 0; lc--) {
        HnswNeighborArray *neighbors = HnswGetNeighbors(base, e, lc);
        int lm = HnswGetLayerM(m, lc);

        for (int i = 0; i < lm; i++) {
            ItemPointer indextid = &ntup->indextids[idx++];

            if (i < neighbors->length) {
                HnswCandidate *hc = &neighbors->items[i];
                HnswElement hce = (HnswElement)HnswPtrAccess(base, hc->element);

                ItemPointerSet(indextid, hce->blkno, hce->offno);
            } else {
                ItemPointerSetInvalid(indextid);
            }
        }
    }

    ntup->count = idx;
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

    /* Ensure expected neighbors */
    if (ntup->count != neighborCount) {
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

    LoadNeighborsFromPage(element, index, page, m);

    UnlockReleaseBuffer(buf);
}

/*
 * Load an element from a tuple
 */
void HnswLoadElementFromTuple(HnswElement element, HnswElementTuple etup, bool loadHeaptids, bool loadVec)
{
    element->level = etup->level;
    element->deleted = etup->deleted;
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
        Datum value = datumCopy(PointerGetDatum(&etup->data), false, -1);

        HnswPtrStore(base, element->value, DatumGetPointer(value));
    }
}

/*
 * Load an element and optionally get its distance from q
 */
bool HnswLoadElement(HnswElement element, float *distance, Datum *q, Relation index, FmgrInfo *procinfo, Oid collation,
                     bool loadVec, float *maxDistance, IndexScanDesc scan, bool enablePQ, PQSearchInfo *pqinfo)
{
    Buffer buf;
    Page page;
    HnswElementTuple etup;
    bool needRecheck = false;
    bool isVisible = true;
    uint8 *ePQCode;
    PQParams *params;

    /* Read vector */
    buf = ReadBuffer(index, element->blkno);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    if (scan != NULL && HnswPageGetOpaque(page)->pageType == HNSW_USTORE_PAGE_TYPE) {
        HnswScanOpaque so = (HnswScanOpaque)scan->opaque;
        so->vs.buf = buf;
        isVisible = VecVisibilityCheck(scan, page, element->offno, &needRecheck);
    }

    etup = (HnswElementTuple)PageGetItem(page, PageGetItemId(page, element->offno));

    Assert(HnswIsElementTuple(etup));

    /* Calculate distance */
    if (distance != NULL) {
        if (enablePQ && pqinfo->lc == 0) {
            ePQCode = LoadPQcode(etup);
            params = &pqinfo->params;
            if (pqinfo->pqMode == HNSW_PQMODE_SDC && *pqinfo->qPQCode == NULL) {
                *distance = 0;
            } else if (pqinfo->pqMode == HNSW_PQMODE_ADC && pqinfo->pqDistanceTable == NULL) {
                *distance = 0;
            } else {
                GetPQDistance(ePQCode, pqinfo->qPQCode, params, pqinfo->pqDistanceTable, distance);
            }
        } else {
            if (DatumGetPointer(*q) == NULL) {
                *distance = 0;
            } else {
                *distance = (float)DatumGetFloat8(FunctionCall2Coll(
                            procinfo, collation, *q, PointerGetDatum(&etup->data)));
            }
        }
    }

    /* Load element */
    if (distance == NULL || maxDistance == NULL || *distance < *maxDistance) {
        HnswLoadElementFromTuple(element, etup, true, loadVec);
        if (enablePQ) {
            params = &pqinfo->params;
            Vector *vd1 = &etup->data;
            Vector *vd2 = (Vector *)DatumGetPointer(*q);
            float exactDis = VectorL2SquaredDistance(params->dim, vd1->x, vd2->x);
            *distance = exactDis;
        }
    }

    UnlockReleaseBuffer(buf);
    return isVisible;
}

/*
 * Get the distance for a candidate
 */
static float GetCandidateDistance(char *base, HnswElement element, Datum q, FmgrInfo *procinfo, Oid collation)
{
    Datum value = HnswGetValue(base, element);

    return DatumGetFloat8(FunctionCall2Coll(procinfo, collation, q, value));
}

/*
 * Create a candidate for the entry point
 */
HnswCandidate *HnswEntryCandidate(char *base, HnswElement entryPoint, Datum q, Relation index, FmgrInfo *procinfo,
                                  Oid collation, bool loadVec, IndexScanDesc scan, bool enablePQ, PQSearchInfo *pqinfo)
{
    HnswCandidate *hc = (HnswCandidate *)palloc(sizeof(HnswCandidate));

    HnswPtrStore(base, hc->element, entryPoint);
    if (index == NULL) {
        hc->distance = GetCandidateDistance(base, entryPoint, q, procinfo, collation);
    } else {
        bool isVisible = HnswLoadElement(entryPoint, &hc->distance, &q, index, procinfo,
                                         collation, loadVec, NULL, scan, enablePQ, pqinfo);
        if (!isVisible) {
            elog(ERROR, "hnsw entryPoint is invisible\n");
        }
    }
    return hc;
}

#define HnswGetPairingHeapCandidate(membername, ptr) \
(pairingheap_container(HnswPairingHeapNode, membername, ptr)->inner)
#define HnswGetPairingHeapCandidateConst(membername, ptr) \
(pairingheap_const_container(HnswPairingHeapNode, membername, ptr)->inner)

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
static void
HnswLoadUnvisitedFromDisk(HnswElement element, HnswElement *unvisited, int *unvisitedLength,
                          VisitedHash *v, Relation index, int m, int lm, int lc)
{
    Buffer buf;
    Page page;
    HnswNeighborTuple ntup;
    int start;
    ItemPointerData indextids[HNSW_MAX_M * 2];

    buf = ReadBuffer(index, element->neighborPage);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);

    ntup = (HnswNeighborTuple)PageGetItem(page, PageGetItemId(page, element->neighborOffno));
    start = (element->level - lc) * m;

    /* Copy to minimize lock time */
    memcpy(&indextids, ntup->indextids + start, lm * sizeof(ItemPointerData));

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
            unvisited[(*unvisitedLength)++] = HnswInitElementFromBlock(ItemPointerGetBlockNumber(indextid),
                                                                       ItemPointerGetOffsetNumber(indextid));
        }
    }
}

/*
 * Algorithm 2 from paper
 */
List *HnswSearchLayer(char *base, Datum q, List *ep, int ef, int lc, Relation index, FmgrInfo *procinfo,
                      Oid collation, int m, bool inserting, HnswElement skipElement,
                      IndexScanDesc scan, bool enablePQ, PQSearchInfo *pqinfo)
{
    List *w = NIL;
    pairingheap *C = pairingheap_allocate(CompareNearestCandidates, NULL);
    pairingheap *W = pairingheap_allocate(CompareFurthestCandidates, NULL);
    int wlen = 0;
    VisitedHash v;
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

    InitVisited(base, &v, index, ef, m);

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

        AddToVisited(base, &v, hc, index, &found);

        node = CreatePairingHeapNode(hc);
        pairingheap_add(C, &node->c_node);
        pairingheap_add(W, &node->w_node);

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

        if (index == NULL) {
            HnswLoadUnvisitedFromMemory(base, cElement, unvisited, &unvisitedLength, &v,
                                        lc, neighborhoodData, neighborhoodSize);
        } else {
            HnswLoadUnvisitedFromDisk(cElement, unvisited, &unvisitedLength, &v, index, m, lm, lc);
        }

        for (int i = 0; i < unvisitedLength; i++) {
            HnswElement eElement = unvisited[i];
            float eDistance;
            bool alwaysAdd = wlen < ef;

            f = HnswGetPairingHeapCandidate(w_node, pairingheap_first(W));

            if (index == NULL) {
                if (enablePQ) {
                    uint8 *ePQCode = (uint8*)HnswPtrAccess(base, eElement->pqcodes);
                    GetPQDistance(ePQCode, pqinfo->qPQCode, &pqinfo->params, pqinfo->pqDistanceTable, &eDistance);
                } else {
                    eDistance = GetCandidateDistance(base, eElement, q, procinfo, collation);
                }
            } else {
                HnswLoadElement(eElement, &eDistance, &q, index, procinfo, collation, inserting,
                                alwaysAdd ? NULL : &f->distance, NULL, enablePQ, pqinfo);
            }

            if (eDistance < f->distance || alwaysAdd) {
                HnswCandidate *e;
                HnswPairingHeapNode *node;
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

                /*
                 * Do not count elements being deleted towards ef when
                 * vacuuming. It would be ideal to do this for inserts as
                 * well, but this could affect insert performance.
                 */
                if (CountElement(base, skipElement, eElement)) {
                    wlen++;

                   /* No need to decrement wlen */
                    if (wlen > ef) {
                        pairingheap_remove_first(W);
                    }
                }
            } else {
                vNum++;
            }

            if (enableEarlyStop && vNum == threshold) {
                break;
            }
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
static float HnswGetDistance(char *base, HnswElement a, HnswElement b, FmgrInfo *procinfo, Oid collation)
{
    Datum aValue = HnswGetValue(base, a);
    Datum bValue = HnswGetValue(base, b);

    return DatumGetFloat8(FunctionCall2Coll(procinfo, collation, aValue, bValue));
}

/*
 * Check if an element is closer to q than any element from R
 */
static bool CheckElementCloser(char *base, HnswCandidate *e, List *r, FmgrInfo *procinfo, Oid collation)
{
    HnswElement eElement = (HnswElement)HnswPtrAccess(base, e->element);
    ListCell *lc2;

    foreach (lc2, r) {
        HnswCandidate *ri = (HnswCandidate *)lfirst(lc2);
        HnswElement riElement = (HnswElement)HnswPtrAccess(base, ri->element);
        float distance = HnswGetDistance(base, eElement, riElement, procinfo, collation);

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
                             HnswCandidate *newCandidate, HnswCandidate **pruned, bool sortCandidates)
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
            e->closer = CheckElementCloser(base, e, r, procinfo, collation);
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
                e->closer = CheckElementCloser(base, e, added, procinfo, collation);

                if (!e->closer) {
                    removedAny = true;
                }
            } else {
                /*
                 * If we have removed any candidates from closer, a candidate
                 * that was not closer earlier might now be.
                 */
                if (removedAny) {
                    e->closer = CheckElementCloser(base, e, r, procinfo, collation);
                    if (e->closer) {
                        added = lappend(added, e);
                    }
                }
            }
        } else if (e == newCandidate) {
            e->closer = CheckElementCloser(base, e, r, procinfo, collation);
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
                          Relation index, FmgrInfo *procinfo, Oid collation)
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

                if (HnswPtrIsNull(base, hc3Element->value))
                    HnswLoadElement(hc3Element, &hc3->distance, &q, index, procinfo, collation, true, NULL);
                else
                    hc3->distance = GetCandidateDistance(base, hc3Element, q, procinfo, collation);

                /* Prune element if being deleted */
                if (hc3Element->heaptidsLength == 0) {
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

            SelectNeighbors(base, c, lm, lc, procinfo, collation, hce, &hc2, &pruned, true);

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
                              bool enablePQ, PQParams *params)
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
        ComputeVectorPQCode(DatumGetVector(q)->x, params, pqcode);
        Pointer codePtr = (Pointer)HnswPtrAccess(base, element->pqcodes);
        errno_t err = memcpy_s(codePtr, codesize, pqcode, codesize);
        securec_check(err, "\0", "\0");
    }

    /* No neighbors if no entry point */
    if (entryPoint == NULL)
        return;

    /* Get entry point and level */
    ep = list_make1(HnswEntryCandidate(base, entryPoint, q, index, procinfo, collation, true));
    entryLevel = entryPoint->level;

    /* 1st phase: greedy search to insert level */
    for (int lc = entryLevel; lc >= level + 1; lc--) {
        w = HnswSearchLayer(base, q, ep, 1, lc, index, procinfo, collation, m, true, skipElement);
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

        w = HnswSearchLayer(base, q, ep, efConstruction, lc, index, procinfo, collation, m, true, skipElement);

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
        neighbors = SelectNeighbors(base, lw, lm, lc, procinfo, collation, element, NULL, NULL, false);

        AddConnections(base, element, neighbors, lc);

        ep = w;
    }
}

/*
* Get the info related to pqTable in append metapage
*/
void HnswGetPQInfoFromMetaPage(Relation index, uint16 *pqTableNblk, uint32 *pqTableSize,
                               uint16 *pqDisTableNblk, uint32 *pqDisTableSize)
{
    Buffer buf;
    Page page;

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

int getPQfunctionType(FmgrInfo *procinfo, FmgrInfo *normprocinfo)
{
    if (procinfo->fn_oid == 8431) {
        return HNSW_PQ_DIS_L2;
    } else if (procinfo->fn_oid == 8434) {
        if (normprocinfo == NULL) {
            return HNSW_PQ_DIS_IP;
        } else {
            return HNSW_PQ_DIS_COSINE;
        }
    } else {
        ereport(ERROR, (errmsg("current data type or distance type can't support hnswpq.")));
        return -1;
    }
}

void InitPQParamsOnDisk(PQParams *params, Relation index, FmgrInfo *procinfo, int dim, bool *enablePQ)
{
    const HnswTypeInfo *typeInfo = HnswGetTypeInfo(index);
    Buffer buf = ReadBuffer(index, HNSW_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    Page page = BufferGetPage(buf);
    HnswMetaPage metap = HnswPageGetMeta(page);
    *enablePQ = metap->enablePQ;
    params->pqM = metap->pqM;
    params->pqKsub = metap->pqKsub;
    UnlockReleaseBuffer(buf);
    int pqMode = HNSW_PQMODE_DEFAULT;

    if (*enablePQ && !g_instance.pq_inited) {
        ereport(ERROR, (errmsg("the SQL involves operations related to HNSWPQ, "
                               "but this instance has not currently loaded the PQ dynamic library.")));
    }

    if (*enablePQ) {
        params->funcType = getPQfunctionType(procinfo, HnswOptionalProcInfo(index, HNSW_NORM_PROC));
        params->dim = dim;
        params->subItemSize = typeInfo->itemSize(dim / params->pqM);
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
            .maxDimensions = HNSW_MAX_DIM, .supportPQ = true,
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
        .maxDimensions = HNSW_MAX_DIM * 2, .supportPQ = false,
        .itemSize = HalfvecItemSize, .normalize = halfvec_l2_normalize, .checkValue = NULL};

    PG_RETURN_POINTER(&typeInfo);
};

PGDLLEXPORT PG_FUNCTION_INFO_V1(hnsw_bit_support);
Datum hnsw_bit_support(PG_FUNCTION_ARGS)
{
    static const HnswTypeInfo typeInfo = {.maxDimensions = HNSW_MAX_DIM * 32, .supportPQ = false,
                                          .itemSize = BitItemSize, .normalize = NULL, .checkValue = NULL};

    PG_RETURN_POINTER(&typeInfo);
};

PGDLLEXPORT PG_FUNCTION_INFO_V1(hnsw_sparsevec_support);
Datum hnsw_sparsevec_support(PG_FUNCTION_ARGS)
{
    static const HnswTypeInfo typeInfo = {
        .maxDimensions = SPARSEVEC_MAX_DIM, .supportPQ = false,
        .itemSize = NULL, .normalize = sparsevec_l2_normalize, .checkValue = SparsevecCheckValue};

    PG_RETURN_POINTER(&typeInfo);
};
