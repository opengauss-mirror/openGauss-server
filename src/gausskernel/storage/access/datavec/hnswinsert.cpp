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
 * hnswinsert.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/hnswinsert.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include <cmath>

#include "access/generic_xlog.h"
#include "access/xact.h"
#include "access/datavec/hnsw.h"
#include "storage/buf/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/datum.h"
#include "utils/memutils.h"

/*
 * Get the insert page
 */
static BlockNumber GetInsertPage(Relation index)
{
    Buffer buf;
    Page page;
    HnswMetaPage metap;
    BlockNumber insertPage;

    buf = ReadBuffer(index, HNSW_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    metap = HnswPageGetMeta(page);

    insertPage = metap->insertPage;

    UnlockReleaseBuffer(buf);

    return insertPage;
}

/*
 * Check for a free offset
 */
static bool HnswFreeOffset(Relation index, Buffer buf, Page page, HnswElement element, Size ntupSize, Buffer *nbuf,
                           Page *npage, OffsetNumber *freeOffno, OffsetNumber *freeNeighborOffno,
                           BlockNumber *newInsertPage)
{
    OffsetNumber offno;
    OffsetNumber maxoffno = PageGetMaxOffsetNumber(page);

    for (offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno)) {
        HnswElementTuple etup = (HnswElementTuple)PageGetItem(page, PageGetItemId(page, offno));
        /* Skip neighbor tuples */
        if (!HnswIsElementTuple(etup))
            continue;

        if (etup->deleted) {
            BlockNumber elementPage = BufferGetBlockNumber(buf);
            BlockNumber neighborPage = ItemPointerGetBlockNumber(&etup->neighbortid);
            OffsetNumber neighborOffno = ItemPointerGetOffsetNumber(&etup->neighbortid);
            ItemId itemid;

            if (!BlockNumberIsValid(*newInsertPage))
                *newInsertPage = elementPage;

            if (neighborPage == elementPage) {
                *nbuf = buf;
                *npage = page;
            } else {
                *nbuf = ReadBuffer(index, neighborPage);
                LockBuffer(*nbuf, BUFFER_LOCK_EXCLUSIVE);

                /* Skip WAL for now */
                *npage = BufferGetPage(*nbuf);
            }

            itemid = PageGetItemId(*npage, neighborOffno);
            /* Check for space on neighbor tuple page */
            if (PageGetFreeSpace(*npage) + ItemIdGetLength(itemid) - sizeof(ItemIdData) >= ntupSize) {
                *freeOffno = offno;
                *freeNeighborOffno = neighborOffno;
                return true;
            } else if (*nbuf != buf)
                UnlockReleaseBuffer(*nbuf);
        }
    }

    return false;
}

/*
 * Add a new page
 */
static void HnswInsertAppendPage(Relation index, Buffer *nbuf, Page *npage, GenericXLogState *state, Page page,
                                 bool building)
{
    /* Add a new page */
    LockRelationForExtension(index, ExclusiveLock);
    *nbuf = HnswNewBuffer(index, MAIN_FORKNUM);
    UnlockRelationForExtension(index, ExclusiveLock);

    /* Init new page */
    if (building)
        *npage = BufferGetPage(*nbuf);
    else
        *npage = GenericXLogRegisterBuffer(state, *nbuf, GENERIC_XLOG_FULL_IMAGE);

    HnswInitPage(*nbuf, *npage);

    /* Update previous buffer */
    HnswPageGetOpaque(page)->nextblkno = BufferGetBlockNumber(*nbuf);
}

/*
 * Add to element and neighbor pages
 */
static void AddElementOnDisk(Relation index, HnswElement e, int m, BlockNumber insertPage,
                             BlockNumber *updatedInsertPage, bool building)
{
    Buffer buf;
    Page page;
    GenericXLogState *state;
    Size etupSize;
    Size ntupSize;
    Size combinedSize;
    Size maxSize;
    Size minCombinedSize;
    HnswElementTuple etup;
    BlockNumber currentPage = insertPage;
    HnswNeighborTuple ntup;
    Buffer nbuf;
    Page npage;
    OffsetNumber freeOffno = InvalidOffsetNumber;
    OffsetNumber freeNeighborOffno = InvalidOffsetNumber;
    BlockNumber newInsertPage = InvalidBlockNumber;
    char *base = NULL;
    bool isUStore;
    IndexTransInfo *idxXid;
    bool enablePQ;
    Size pqcodesSize;

    /* Get enablePQ and pqcodeSize info from metapage */
    Buffer metaBuf = ReadBuffer(index, HNSW_METAPAGE_BLKNO);
    LockBuffer(metaBuf, BUFFER_LOCK_SHARE);
    HnswMetaPage metap = HnswPageGetMeta(BufferGetPage(metaBuf));
    enablePQ = metap->enablePQ;
    pqcodesSize = metap->pqcodeSize;
    UnlockReleaseBuffer(metaBuf);

    /* Calculate sizes */
    etupSize = HNSW_ELEMENT_TUPLE_SIZE(VARSIZE_ANY(HnswPtrAccess(base, e->value)));
    ntupSize = HNSW_NEIGHBOR_TUPLE_SIZE(e->level, m);
    combinedSize = etupSize + MAXALIGN(pqcodesSize) + ntupSize + sizeof(ItemIdData);
    maxSize = HNSW_MAX_SIZE;
    minCombinedSize = etupSize + MAXALIGN(pqcodesSize) +
                      HNSW_NEIGHBOR_TUPLE_SIZE(0, m) + sizeof(ItemIdData);

    /* Prepare element tuple */
    etup = (HnswElementTuple)palloc0(etupSize);
    HnswSetElementTuple(base, etup, e);

    /* Prepare neighbor tuple */
    ntup = (HnswNeighborTuple)palloc0(ntupSize);
    HnswSetNeighborTuple(base, ntup, e, m);

    /* Find a page (or two if needed) to insert the tuples */
    for (;;) {
        buf = ReadBuffer(index, currentPage);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

        if (building) {
            state = NULL;
            page = BufferGetPage(buf);
        } else {
            state = GenericXLogStart(index);
            page = GenericXLogRegisterBuffer(state, buf, 0);
        }

        isUStore = HnswPageGetOpaque(page)->pageType == HNSW_USTORE_PAGE_TYPE;
        /* Keep track of first page where element at level 0 can fit */
        if (!BlockNumberIsValid(newInsertPage) && PageGetFreeSpace(page) >= minCombinedSize) {
            newInsertPage = currentPage;
        }

        /* First, try the fastest path */
        /* Space for both tuples on the current page */
        /* This can split existing tuples in rare cases */
        if (PageGetFreeSpace(page) >= combinedSize) {
            nbuf = buf;
            npage = page;
            break;
        }

        /* Next, try space from a deleted element */
        if (HnswFreeOffset(index, buf, page, e, ntupSize, &nbuf, &npage, &freeOffno, &freeNeighborOffno,
                           &newInsertPage)) {
            if (nbuf != buf) {
                if (building) {
                    npage = BufferGetPage(nbuf);
                } else {
                    npage = GenericXLogRegisterBuffer(state, nbuf, 0);
                }
            }

            break;
        }

        /* Finally, try space for element only if last page */
        /* Skip if both tuples can fit on the same page */
        if (combinedSize > maxSize && PageGetFreeSpace(page) >= etupSize + MAXALIGN(pqcodesSize) &&
            !BlockNumberIsValid(HnswPageGetOpaque(page)->nextblkno)) {
            HnswInsertAppendPage(index, &nbuf, &npage, state, page, building);
            if (isUStore) {
                HnswPageGetOpaque(npage)->pageType = HNSW_USTORE_PAGE_TYPE;
            }
            break;
        }

        currentPage = HnswPageGetOpaque(page)->nextblkno;
        if (BlockNumberIsValid(currentPage)) {
            /* Move to next page */
            if (!building)
                GenericXLogAbort(state);
            UnlockReleaseBuffer(buf);
        } else {
            Buffer newbuf;
            Page newpage;

            HnswInsertAppendPage(index, &newbuf, &newpage, state, page, building);
            if (isUStore) {
                HnswPageGetOpaque(npage)->pageType = HNSW_USTORE_PAGE_TYPE;
            }
            /* Commit */
            if (building) {
                MarkBufferDirty(buf);
            } else {
                GenericXLogFinish(state);
            }

            /* Unlock previous buffer */
            UnlockReleaseBuffer(buf);

            /* Prepare new buffer */
            buf = newbuf;
            if (building) {
                state = NULL;
                page = BufferGetPage(buf);
            } else {
                state = GenericXLogStart(index);
                page = GenericXLogRegisterBuffer(state, buf, 0);
            }

            /* Create new page for neighbors if needed */
            if (PageGetFreeSpace(page) < combinedSize) {
                HnswInsertAppendPage(index, &nbuf, &npage, state, page, building);
                if (isUStore) {
                    HnswPageGetOpaque(npage)->pageType = HNSW_USTORE_PAGE_TYPE;
                }
            } else {
                nbuf = buf;
                npage = page;
            }

            break;
        }
    }

    e->blkno = BufferGetBlockNumber(buf);
    e->neighborPage = BufferGetBlockNumber(nbuf);

    /* Added tuple to new page if newInsertPage is not set */
    /* So can set to neighbor page instead of element page */
    if (!BlockNumberIsValid(newInsertPage)) {
        newInsertPage = e->neighborPage;
    }

    if (OffsetNumberIsValid(freeOffno)) {
        e->offno = freeOffno;
        e->neighborOffno = freeNeighborOffno;
    } else {
        e->offno = OffsetNumberNext(PageGetMaxOffsetNumber(page));
        if (nbuf == buf) {
            e->neighborOffno = OffsetNumberNext(e->offno);
        } else {
            e->neighborOffno = FirstOffsetNumber;
        }
    }

    ItemPointerSet(&etup->neighbortid, e->neighborPage, e->neighborOffno);

    /* Add element and neighbors */
    if (OffsetNumberIsValid(freeOffno)) {
        if (enablePQ || isUStore) {
            ItemId item_id = PageGetItemId(page, e->offno);
            Size aligned_size = MAXALIGN(ItemIdGetLength(item_id));
            unsigned offset = ItemIdGetOffset(item_id);
            char *itemtail = (char *)page + offset + aligned_size;
            if (enablePQ) {
                Pointer codePtr = (Pointer)HnswPtrAccess(base, e->pqcodes);
                errno_t rc = memcpy_s(itemtail, pqcodesSize, codePtr, pqcodesSize);
                securec_check_c(rc, "\0", "\0");
            }
            if (isUStore) {
                idxXid = (IndexTransInfo *)(itemtail + MAXALIGN(pqcodesSize));
                idxXid->xmin = GetCurrentTransactionId();
                idxXid->xmax = InvalidTransactionId;
            }
        }
        if (!page_index_tuple_overwrite(page, e->offno, (Item)etup, etupSize)) {
            elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));
        }

        if (!page_index_tuple_overwrite(npage, e->neighborOffno, (Item)ntup, ntupSize)) {
            elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));
        }
    } else {
        if (enablePQ) {
            ((PageHeader)page)->pd_upper -= MAXALIGN(pqcodesSize);
            Pointer codePtr = (Pointer)HnswPtrAccess(base, e->pqcodes);
            errno_t rc = memcpy_s(((char *)page) + ((PageHeader)page)->pd_upper,
                                  pqcodesSize, codePtr, pqcodesSize);
            securec_check_c(rc, "\0", "\0");
        }
        if (isUStore) {
            ((PageHeader)page)->pd_upper -= sizeof(IndexTransInfo);
            idxXid = (IndexTransInfo *)(((char *)page) + ((PageHeader)page)->pd_upper);
            idxXid->xmin = GetCurrentTransactionId();
            idxXid->xmax = InvalidTransactionId;
        }
        if (PageAddItem(page, (Item)etup, etupSize, InvalidOffsetNumber, false, false) != e->offno) {
            elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));
        }

        if (PageAddItem(npage, (Item)ntup, ntupSize, InvalidOffsetNumber, false, false) != e->neighborOffno) {
            elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));
        }
    }

    /* Commit */
    if (building) {
        MarkBufferDirty(buf);
        if (nbuf != buf)
            MarkBufferDirty(nbuf);
    } else {
        GenericXLogFinish(state);
    }
    UnlockReleaseBuffer(buf);
    if (nbuf != buf)
        UnlockReleaseBuffer(nbuf);

    /* Update the insert page */
    if (BlockNumberIsValid(newInsertPage) && newInsertPage != insertPage)
        *updatedInsertPage = newInsertPage;
}

/*
 * Check if connection already exists
 */
static bool ConnectionExists(HnswElement e, HnswNeighborTuple ntup, int startIdx, int lm)
{
    for (int i = 0; i < lm; i++) {
        ItemPointer indextid = &ntup->indextids[startIdx + i];

        if (!ItemPointerIsValid(indextid)) {
            break;
        }

        if (ItemPointerGetBlockNumber(indextid) == e->blkno && ItemPointerGetOffsetNumber(indextid) == e->offno) {
            return true;
        }
    }

    return false;
}

/*
 * Update neighbors
 */
void HnswUpdateNeighborsOnDisk(Relation index, FmgrInfo *procinfo, Oid collation, HnswElement e, int m,
                               bool checkExisting, bool building)
{
    char *base = NULL;

    for (int lc = e->level; lc >= 0; lc--) {
        int lm = HnswGetLayerM(m, lc);
        HnswNeighborArray *neighbors = HnswGetNeighbors(base, e, lc);

        for (int i = 0; i < neighbors->length; i++) {
            HnswCandidate *hc = &neighbors->items[i];
            Buffer buf;
            Page page;
            GenericXLogState *state;
            HnswNeighborTuple ntup;
            int idx = -1;
            int startIdx;
            HnswElement neighborElement = (HnswElement)HnswPtrAccess(base, hc->element);
            OffsetNumber offno = neighborElement->neighborOffno;

            /* Get latest neighbors since they may have changed */
            /* Do not lock yet since selecting neighbors can take time */
            HnswLoadNeighbors(neighborElement, index, m);

            /*
             * Could improve performance for vacuuming by checking neighbors
             * against list of elements being deleted to find index. It's
             * important to exclude already deleted elements for this since
             * they can be replaced at any time.
             */

            /* Select neighbors */
            HnswUpdateConnection(NULL, e, hc, lm, lc, &idx, index, procinfo, collation);

            /* New element was not selected as a neighbor */
            if (idx == -1)
                continue;

            /* Register page */
            buf = ReadBuffer(index, neighborElement->neighborPage);
            LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
            if (building) {
                state = NULL;
                page = BufferGetPage(buf);
            } else {
                state = GenericXLogStart(index);
                page = GenericXLogRegisterBuffer(state, buf, 0);
            }

            /* Get tuple */
            ntup = (HnswNeighborTuple)PageGetItem(page, PageGetItemId(page, offno));

            /* Calculate index for update */
            startIdx = (neighborElement->level - lc) * m;

            /* Check for existing connection */
            if (checkExisting && ConnectionExists(e, ntup, startIdx, lm))
                idx = -1;
            else if (idx == -2) {
                /* Find free offset if still exists */
                /* TODO Retry updating connections if not */
                for (int j = 0; j < lm; j++) {
                    if (!ItemPointerIsValid(&ntup->indextids[startIdx + j])) {
                        idx = startIdx + j;
                        break;
                    }
                }
            } else
                idx += startIdx;

            /* Make robust to issues */
            if (idx >= 0 && idx < ntup->count) {
                ItemPointer indextid = &ntup->indextids[idx];

                /* Update neighbor on the buffer */
                ItemPointerSet(indextid, e->blkno, e->offno);

                /* Commit */
                if (building)
                    MarkBufferDirty(buf);
                else
                    GenericXLogFinish(state);
            } else if (!building)
                GenericXLogAbort(state);

            UnlockReleaseBuffer(buf);
        }
    }
}

/*
 * Add a heap TID to an existing element
 */
static bool AddDuplicateOnDisk(Relation index, HnswElement element, HnswElement dup, bool building)
{
    Buffer buf;
    Page page;
    GenericXLogState *state;
    HnswElementTuple etup;
    int i;

    /* Read page */
    buf = ReadBuffer(index, dup->blkno);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    if (building) {
        state = NULL;
        page = BufferGetPage(buf);
    } else {
        state = GenericXLogStart(index);
        page = GenericXLogRegisterBuffer(state, buf, 0);
    }

    /* Find space */
    etup = (HnswElementTuple)PageGetItem(page, PageGetItemId(page, dup->offno));
    for (i = 0; i < HNSW_HEAPTIDS; i++) {
        if (!ItemPointerIsValid(&etup->heaptids[i]))
            break;
    }

    /* Either being deleted or we lost our chance to another backend */
    if (i == 0 || i == HNSW_HEAPTIDS) {
        if (!building)
            GenericXLogAbort(state);
        UnlockReleaseBuffer(buf);
        return false;
    }

    /* Add heap TID, modifying the tuple on the page directly */
    etup->heaptids[i] = element->heaptids[0];

    /* Commit */
    if (building)
        MarkBufferDirty(buf);
    else
        GenericXLogFinish(state);
    UnlockReleaseBuffer(buf);

    return true;
}

/*
 * Find duplicate element
 */
static bool FindDuplicateOnDisk(Relation index, HnswElement element, bool building)
{
    char *base = NULL;
    HnswNeighborArray *neighbors = HnswGetNeighbors(base, element, 0);
    Datum value = HnswGetValue(base, element);

    for (int i = 0; i < neighbors->length; i++) {
        HnswCandidate *neighbor = &neighbors->items[i];
        HnswElement neighborElement = (HnswElement)HnswPtrAccess(base, neighbor->element);
        Datum neighborValue = HnswGetValue(base, neighborElement);
        /* Exit early since ordered by distance */
        if (!datumIsEqual(value, neighborValue, false, -1))
            return false;

        if (AddDuplicateOnDisk(index, element, neighborElement, building))
            return true;
    }

    return false;
}

/*
 * Update graph on disk
 */
static void UpdateGraphOnDisk(Relation index, FmgrInfo *procinfo, Oid collation, HnswElement element, int m,
                              int efConstruction, HnswElement entryPoint, bool building)
{
    BlockNumber newInsertPage = InvalidBlockNumber;

    /* Look for duplicate */
    if (FindDuplicateOnDisk(index, element, building)) {
        return;
    }

    /* Add element */
    AddElementOnDisk(index, element, m, GetInsertPage(index), &newInsertPage, building);

    /* Update insert page if needed */
    if (BlockNumberIsValid(newInsertPage)) {
        HnswUpdateMetaPage(index, 0, NULL, newInsertPage, MAIN_FORKNUM, building);
    }

    /* Update neighbors */
    HnswUpdateNeighborsOnDisk(index, procinfo, collation, element, m, false, building);

    /* Update entry point if needed */
    if (entryPoint == NULL || element->level > entryPoint->level) {
        HnswUpdateMetaPage(index, HNSW_UPDATE_ENTRY_GREATER, element, InvalidBlockNumber, MAIN_FORKNUM, building);
    }
}

/*
 * Insert a tuple into the index
 */
bool HnswInsertTupleOnDisk(Relation index, Datum value, Datum *values, const bool *isnull, ItemPointer heap_tid,
                           bool building)
{
    HnswElement entryPoint;
    HnswElement element;
    int m;
    int efConstruction = HnswGetEfConstruction(index);
    FmgrInfo *procinfo = index_getprocinfo(index, 1, HNSW_DISTANCE_PROC);
    Oid collation = index->rd_indcollation[0];
    LOCKMODE lockmode = ShareLock;
    char *base = NULL;
    PQParams params;
    bool enablePQ;
    int dim = TupleDescAttr(index->rd_att, 0)->atttypmod;

    /*
     * Get a shared lock. This allows vacuum to ensure no in-flight inserts
     * before repairing graph. Use a page lock so it does not interfere with
     * buffer lock (or reads when vacuuming).
     */
    LockPage(index, HNSW_UPDATE_LOCK, lockmode);

    /* Get m and entry point */
    HnswGetMetaPageInfo(index, &m, &entryPoint);

    /* Create an element */
    element = HnswInitElement(base, heap_tid, m, HnswGetMl(m), HnswGetMaxLevel(m), NULL);
    HnswPtrStore(base, element->value, DatumGetPointer(value));

    /* Prevent concurrent inserts when likely updating entry point */
    if (entryPoint == NULL || element->level > entryPoint->level) {
        /* Release shared lock */
        UnlockPage(index, HNSW_UPDATE_LOCK, lockmode);

        /* Get exclusive lock */
        lockmode = ExclusiveLock;
        LockPage(index, HNSW_UPDATE_LOCK, lockmode);

        /* Get latest entry point after lock is acquired */
        entryPoint = HnswGetEntryPoint(index);
    }

    InitPQParamsOnDisk(&params, index, procinfo, dim, &enablePQ, false);

    Pointer codePtr = NULL;
    if (enablePQ) {
        Size codesize = params.pqM * sizeof(uint8);
        codePtr = (Pointer)HnswAlloc(NULL, codesize);
    }
    HnswPtrStore(base, element->pqcodes, codePtr);

    /* Find neighbors for element */
    HnswFindElementNeighbors(base, element, entryPoint, index, procinfo, collation, m,
                             efConstruction, false, enablePQ, &params);

    /* Update graph on disk */
    UpdateGraphOnDisk(index, procinfo, collation, element, m, efConstruction, entryPoint, building);

    /* Release lock */
    UnlockPage(index, HNSW_UPDATE_LOCK, lockmode);

    return true;
}

/*
 * Insert a tuple into the index
 */
static void HnswInsertTuple(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid)
{
    Datum value;
    const HnswTypeInfo *typeInfo = HnswGetTypeInfo(index);
    FmgrInfo *normprocinfo;
    Oid collation = index->rd_indcollation[0];

    /* Detoast once for all calls */
    value = PointerGetDatum(PG_DETOAST_DATUM(values[0]));

    /* Check value */
    if (typeInfo->checkValue != NULL) {
        typeInfo->checkValue(DatumGetPointer(value));
    }

    /* Normalize if needed */
    normprocinfo = HnswOptionalProcInfo(index, HNSW_NORM_PROC);
    if (normprocinfo != NULL) {
        if (!HnswCheckNorm(normprocinfo, collation, value)) {
            return;
        }

        value = HnswNormValue(typeInfo, collation, value);
    }

    HnswInsertTupleOnDisk(index, value, values, isnull, heap_tid, false);
}

/*
 * Insert a tuple into the index
 */
bool hnswinsert_internal(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid, Relation heap,
                         IndexUniqueCheck checkUnique)
{
    MemoryContext oldCtx;
    MemoryContext insertCtx;

    /* Skip nulls */
    if (isnull[0]) {
        return false;
    }
    if (IsRelnodeMmapLoad(index->rd_node.relNode)) {
        ereport(ERROR, (errmsg("cannot do DML after mmap load")));
        return false;
    }

    /* Create memory context */
    insertCtx = AllocSetContextCreate(CurrentMemoryContext, "Hnsw insert temporary context", ALLOCSET_DEFAULT_SIZES);
    oldCtx = MemoryContextSwitchTo(insertCtx);

    /* Insert tuple */
    HnswInsertTuple(index, values, isnull, heap_tid);

    /* Delete memory context */
    MemoryContextSwitchTo(oldCtx);
    MemoryContextDelete(insertCtx);

    return false;
}
