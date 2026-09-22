/*
 * This file contains code from different sources, governed by different open source licenses.
 *
 * 1. Code originating from the PostgreSQL project:
 *    - Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 *    - This code is licensed under the PostgreSQL License.
 *    - Permission is granted to use, copy, modify, and distribute this software in source and binary forms,
 *      provided that the above copyright notice, this condition list, and the following disclaimer
 *      are retained in source distributions, and reproduced in documentation/material provided with
 *      binary distributions.
 *
 * 2. Modifications and new code by Huawei Technologies Co., Ltd.:
 *    - Portions Copyright (c) 2024 Huawei Technologies Co.,Ltd.
 *    - This code is licensed under the Mulan Permissive Software License, Version 2 (Mulan PSL v2).
 *    - Full license text available at: http://license.coscl.org.cn/MulanPSL2
 *
 * 3. General Disclaimer (as required by the PostgreSQL License):
 *    - THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS
 *      OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 *      AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 *      CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 *      DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *      DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
 *      IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 *      OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * By using this file, you acknowledge that you must comply with all applicable terms of both the
 * PostgreSQL License and the Mulan PSL v2 for the respective code portions you utilize.
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
#include "access/datavec/hnsw_vector_storage.h"
#include "access/datavec/vector_storage.h"
#include "catalog/index.h"
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
                           BlockNumber *newInsertPage, uint8 *tupleVersion)
{
    OffsetNumber offno;
    OffsetNumber maxoffno = PageGetMaxOffsetNumber(page);

    for (offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno)) {
        HnswElementTuple etup = (HnswElementTuple)PageGetItem(page, PageGetItemId(page, offno));
        /* Skip neighbor tuples */
        if (!HnswIsElementTuple(etup))
            continue;

        if (etup->deleted) {
            /* A cancelled VACUUM may still need this tuple to recycle its payload. */
            if (HnswElementTupleIsVectorStorage(etup) &&
                ItemPointerIsValid(&HnswElementTupleGetPayloads(etup)->raw.tid)) {
                continue;
            }
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
                *tupleVersion = etup->version;
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
static void HnswInsertAppendPage(Relation index, Buffer buf, Page *page, Buffer *nbuf, Page *npage,
                                 GenericXLogState **state, bool building, bool vectorStorage, bool isUStore,
                                 LWLock *buildExtensionLock)
{
    GenericXLogState *volatile publicationState = NULL;
    bool volatile extensionLocked = false;

    *nbuf = InvalidBuffer;
    PG_TRY();
    {
        LockRelationForExtension(index, ExclusiveLock);
        extensionLocked = true;
        *nbuf = HnswNewBuffer(index, MAIN_FORKNUM, buildExtensionLock);

        if (building) {
            *npage = BufferGetPage(*nbuf);
        } else {
            GenericXLogAbort(*state);
            *state = NULL;
            publicationState = GenericXLogStart(index);
            *page = GenericXLogRegisterBuffer(publicationState, buf, 0);
            *npage = GenericXLogRegisterBuffer(publicationState, *nbuf, GENERIC_XLOG_FULL_IMAGE);
        }

        HnswInitPage(*nbuf, *npage);
        if (vectorStorage) {
            HnswPageSetRole(*npage, HNSW_PAGE_ROLE_GRAPH);
        }
        if (isUStore) {
            HnswPageGetOpaque(*npage)->pageType = HNSW_USTORE_PAGE_TYPE;
        }
        HnswPageGetOpaque(*page)->nextblkno = BufferGetBlockNumber(*nbuf);

        if (building) {
            MarkBufferDirty(buf);
            MarkBufferDirty(*nbuf);
        } else {
            GenericXLogFinish(publicationState);
            publicationState = NULL;
        }
        UnlockRelationForExtension(index, ExclusiveLock);
        extensionLocked = false;
    }
    PG_CATCH();
    {
        if (publicationState != NULL) {
            GenericXLogAbort(publicationState);
        }
        if (BufferIsValid(*nbuf)) {
            UnlockReleaseBuffer(*nbuf);
            *nbuf = InvalidBuffer;
        }
        if (extensionLocked) {
            UnlockRelationForExtension(index, ExclusiveLock);
        }
        PG_RE_THROW();
    }
    PG_END_TRY();

    if (!building) {
        *state = GenericXLogStart(index);
        *page = GenericXLogRegisterBuffer(*state, buf, 0);
        *npage = GenericXLogRegisterBuffer(*state, *nbuf, 0);
    }
}

/*
 * Add to element and neighbor pages
 */
static void AddElementOnDisk(Relation index, HnswElement e, int m, BlockNumber insertPage,
                             BlockNumber *updatedInsertPage, bool building, RabitQConfig *rbqConfig,
                             LWLock *buildExtensionLock)
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
    uint8 tupleVersion;
    char *base = NULL;
    bool isUStore;
    IndexTransInfo *idxXid;
    bool enablePQ;
    Size pqcodesSize;
    bool enableRabitQ;
    int dim;
    Size rbqcodesSize = 0;
    bool vectorStorage;

    /* Get info from metapage */
    Buffer metaBuf = ReadBuffer(index, HNSW_METAPAGE_BLKNO);
    LockBuffer(metaBuf, BUFFER_LOCK_SHARE);
    HnswMetaPage metap = HnswPageGetMeta(BufferGetPage(metaBuf));
    HnswVectorStorageMetaLayout storageLayout = HnswClassifyVectorStorageMeta(metap, NULL);
    if (storageLayout == HNSW_VECTOR_STORAGE_META_INVALID) {
        UnlockReleaseBuffer(metaBuf);
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("hnsw vector storage metapage for relation \"%s\" is malformed",
                RelationGetRelationName(index))));
    }
    vectorStorage = storageLayout == HNSW_VECTOR_STORAGE_META_V2;
    enablePQ = metap->enablePQ;
    pqcodesSize = metap->pqcodeSize;
    enableRabitQ = metap->enableRabitQ;
    dim = metap->dimensions;
    UnlockReleaseBuffer(metaBuf);

    /* Calculate sizes */
    if (vectorStorage) {
        etupSize = HNSW_ELEMENT_TUPLE_V2_SIZE;
        pqcodesSize = 0;
    } else if (enableRabitQ) {
        bool refineSQ8 = rbqConfig->reType == SQ8;
        rbqcodesSize = rbqCodeSize(dim, refineSQ8);
        etupSize = MAXALIGN(offsetof(HnswElementTupleData, data) + rbqcodesSize);
    } else {
        etupSize = HNSW_ELEMENT_TUPLE_SIZE(VARSIZE_ANY(HnswPtrAccess(base, e->value)));
    }
    ntupSize = vectorStorage ?
        HnswNeighborTupleSizeV2(e->level, m) : HNSW_NEIGHBOR_TUPLE_SIZE(e->level, m);
    combinedSize = etupSize + MAXALIGN(pqcodesSize) + ntupSize + sizeof(ItemIdData);
    maxSize = HNSW_MAX_SIZE;
    minCombinedSize = etupSize + MAXALIGN(pqcodesSize) +
                      (vectorStorage ?
                          HnswNeighborTupleSizeV2(0, m) : HNSW_NEIGHBOR_TUPLE_SIZE(0, m)) +
                      sizeof(ItemIdData);

    /* Prepare element tuple */
    etup = (HnswElementTuple)palloc0(etupSize);
    if (vectorStorage) {
        VecPayloadDiskRef rawRef;
        Pointer valuePtr = (Pointer)HnswPtrAccess(base, e->value);
        errno_t rc = memset_s(&rawRef, sizeof(rawRef), 0, sizeof(rawRef));

        securec_check(rc, "\0", "\0");
        if (!ItemPointerIsValid(&e->payloadTid)) {
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("HNSW vector storage insert is missing payload tid")));
        }
        ItemPointerCopy(&e->payloadTid, &rawRef.tid);
        rawRef.payloadLen = (uint32)VARSIZE_ANY(valuePtr);
        rawRef.kind = (uint8)VEC_PAYLOAD_RAW_VECTOR;
        HnswSetElementTupleV2(etup, e, &rawRef);
    } else {
        HnswSetElementTuple(base, etup, e, rbqcodesSize);
    }

    /* Prepare neighbor tuple */
    ntup = (HnswNeighborTuple)palloc0(ntupSize);
    HnswSetNeighborTuple(base, ntup, e, m, vectorStorage);

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
                           &newInsertPage, &tupleVersion)) {
            if (nbuf != buf) {
                if (building) {
                    npage = BufferGetPage(nbuf);
                } else {
                    npage = GenericXLogRegisterBuffer(state, nbuf, 0);
                }
            }

            /* Set tuple version */
            etup->version = tupleVersion;
            ntup->version = tupleVersion;

            break;
        }

        /* Finally, try space for element only if last page */
        /* Skip if both tuples can fit on the same page */
        if (combinedSize > maxSize && PageGetFreeSpace(page) >= etupSize + MAXALIGN(pqcodesSize) &&
            !BlockNumberIsValid(HnswPageGetOpaque(page)->nextblkno)) {
            HnswInsertAppendPage(index, buf, &page, &nbuf, &npage, &state, building,
                vectorStorage, isUStore, buildExtensionLock);
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

            HnswInsertAppendPage(index, buf, &page, &newbuf, &newpage, &state, building,
                vectorStorage, isUStore, buildExtensionLock);
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
                HnswInsertAppendPage(index, buf, &page, &nbuf, &npage, &state, building,
                    vectorStorage, isUStore, buildExtensionLock);
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
                               bool checkExisting, bool building, bool enableRabitQ,
                               RabitqInsertOnDiskParams *rbqDiskParams, bool enableLsg)
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
            HnswUpdateConnection(NULL, e, hc, lm, lc, &idx, index, procinfo, collation, enableRabitQ,
                                 rbqDiskParams, enableLsg);

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
                if (HnswIsNeighborTupleV2(ntup)) {
                    if (!ItemPointerIsValid(&e->payloadTid)) {
                        if (!building) {
                            GenericXLogAbort(state);
                        }
                        UnlockReleaseBuffer(buf);
                        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                            errmsg("HNSW vector storage neighbor update is missing payload tid")));
                    }
                    ItemPointerCopy(&e->payloadTid, &ntup->indextids[ntup->count + idx]);
                }

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
                              int efConstruction, HnswElement entryPoint, bool building, bool enableRabitQ,
                              RabitqInsertOnDiskParams *rbqDiskParams, RabitQConfig *rbqConfig, bool enableLsg,
                              LWLock *buildExtensionLock)
{
    BlockNumber newInsertPage = InvalidBlockNumber;
    char *base = NULL;

    /* Look for duplicate */
    if (FindDuplicateOnDisk(index, element, building)) {
        return;
    }

    BlockNumber startBlkno = InvalidBlockNumber;
    uint32 payloadLen = 0;

    if (HnswRelationHasVectorPayloadStorage(index, &startBlkno, &payloadLen)) {
        BlockNumber landedBlkno = InvalidBlockNumber;
        VecPayloadDiskRef rawRef;
        Pointer valuePtr = (Pointer)HnswPtrAccess(base, element->value);
        uint32 valueLen;

        valueLen = (uint32)VARSIZE_ANY(valuePtr);
        if (valueLen != payloadLen) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("HNSW vector storage requires a fixed payload length"),
                errdetail("Expected %u bytes, but found %u bytes.", payloadLen, valueLen)));
        }
        VecPayloadInput payloadInput = {VEC_PAYLOAD_RAW_VECTOR, valuePtr, payloadLen};
        VecPayloadInsertRequest request = {
            MAIN_FORKNUM, startBlkno, &rawRef, &landedBlkno, buildExtensionLock};

        VecPayloadInsert(index, &payloadInput, &request);
        ItemPointerCopy(&rawRef.tid, &element->payloadTid);
        AddElementOnDisk(index, element, m, GetInsertPage(index), &newInsertPage, building, rbqConfig,
            buildExtensionLock);
    } else {
        AddElementOnDisk(index, element, m, GetInsertPage(index), &newInsertPage, building, rbqConfig,
            buildExtensionLock);
    }

    /* Update insert page if needed */
    if (BlockNumberIsValid(newInsertPage)) {
        HnswUpdateMetaPage(index, 0, NULL, newInsertPage, MAIN_FORKNUM, building);
    }

    /* Update neighbors */
    HnswUpdateNeighborsOnDisk(index, procinfo, collation, element, m, false, building, enableRabitQ,
                              rbqDiskParams, enableLsg);

    /* Update entry point if needed */
    if (entryPoint == NULL || element->level > entryPoint->level) {
        HnswUpdateMetaPage(index, HNSW_UPDATE_ENTRY_GREATER, element, InvalidBlockNumber, MAIN_FORKNUM, building);
    }
}

static bool HnswRabitQEntryPointIsVisible(Relation index, HnswElement entryPoint, FmgrInfo *procinfo, Oid collation,
                                          RabitqInsertOnDiskParams *rbqDiskParams)
{
    if (entryPoint == NULL) {
        return false;
    }

    return HnswLoadElement(entryPoint, NULL, NULL, index, procinfo, collation, false, NULL, true, NULL, rbqDiskParams);
}

/*
 * Insert a tuple into the index
 */
bool HnswInsertTupleOnDisk(Relation index, Datum value, const bool *isnull, ItemPointer heap_tid,
                           bool building, Relation heap, LWLock *buildExtensionLock)
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
    bool enableRabitQ;
    bool useFHT;
    RabitqInsertOnDiskParams rbqDiskParams;
    RabitQConfig *rbqConfig = NULL;
    int dim = TupleDescAttr(index->rd_att, 0)->atttypmod;
    float *centroid = NULL;
    LsgCalculator* LocScalingParam = NULL;
    bool enableLsg = false;

    /*
     * Get a shared lock. This allows vacuum to ensure no in-flight inserts
     * before repairing graph. Use a page lock so it does not interfere with
     * buffer lock (or reads when vacuuming).
     */
    LockPage(index, HNSW_UPDATE_LOCK, lockmode);

    /* Get m and entry point */
    HnswGetMetaPageInfo(index, &m, &entryPoint);

    /* Create an element */
    element = HnswInitElement(base, heap_tid, m, HnswGetMl(m),
        HnswRelationHasVectorPayloadStorage(index, NULL, NULL) ? HnswGetMaxLevelV2(m) : HnswGetMaxLevel(m), NULL);

    rbqConfig = InitRbqConfigOnDisk(index, &enableRabitQ, &centroid, dim);
    if (enableRabitQ) {
        Datum vecVal = value;
        if (IS_HALFVEC(procinfo->fn_oid)) {
            vecVal = (Datum)Halfvec2Vector(value);
        }
        Pointer rbqPtr;
        if (rbqConfig->reType == SQ8) {
            /* Calculate origin vector's SQ8 */
            rbqPtr = (Pointer)HnswAlloc(NULL, rbqCodeSize(dim, true));
            ScalarQuantizer *sq = rbqConfig->sq;
            int dim = sq->dim;
            VectorEncodeSQ(dim, sq->trained, sq->trained + dim, ((Vector *)DatumGetPointer(vecVal))->x,
                                getRefineCode(rbqPtr, rbqConfig->reOffset));
        } else {
            rbqPtr = (Pointer)HnswAlloc(NULL, rbqCodeSize(dim, false));
        }
        HnswPtrStore(base, element->rbqcodes, rbqPtr);

        /* hnsw insert on disk need to transform vector */
        VectorTransform *vtrans = rbqConfig->vtrans;
        Vector *transValue = InitVector(dim);
        if (vtrans->type == RANDOM_ORTHOGONAL) {
            RomTransform(vtrans, ((Vector *)DatumGetPointer(vecVal))->x, transValue->x);
        } else {
            FhtTransform(vtrans, ((Vector *)DatumGetPointer(vecVal))->x, transValue->x);
        }
        FmgrInfo *normprocinfo = HnswOptionalProcInfo(index, HNSW_NORM_PROC);
        int funcType = GetFunctionType(procinfo, normprocinfo);

        HnswComputeVectorRBQCode(element, transValue, centroid, funcType, base);

        (&rbqDiskParams)->heap = heap;
        (&rbqDiskParams)->normprocinfo = normprocinfo;
        (&rbqDiskParams)->collation = collation;
        (&rbqDiskParams)->vtrans = vtrans;
        (&rbqDiskParams)->funcType = funcType;
        (&rbqDiskParams)->heapTuple = (HeapTupleData *)heaptup_alloc(BLCKSZ);
        (&rbqDiskParams)->indexInfo = BuildIndexInfo(index);
    }

    HnswPtrStore(base, element->value, DatumGetPointer(value));

    if (enableRabitQ && !HnswRabitQEntryPointIsVisible(index, entryPoint, procinfo, collation, &rbqDiskParams)) {
        entryPoint = NULL;
    }

    /* Prevent concurrent inserts when likely updating entry point */
    if (entryPoint == NULL || element->level > entryPoint->level) {
        /* Release shared lock */
        UnlockPage(index, HNSW_UPDATE_LOCK, lockmode);

        /* Get exclusive lock */
        lockmode = ExclusiveLock;
        LockPage(index, HNSW_UPDATE_LOCK, lockmode);

        /* Get latest entry point after lock is acquired */
        entryPoint = HnswGetEntryPoint(index);
        if (enableRabitQ && !HnswRabitQEntryPointIsVisible(index, entryPoint, procinfo, collation, &rbqDiskParams)) {
            entryPoint = NULL;
        }
    }

    InitPQParamsOnDisk(&params, index, procinfo, dim, &enablePQ, false);
    InitLsgSamplesOnDisk(index, procinfo, &LocScalingParam, &enableLsg);
    if (LocScalingParam != NULL && !IS_SPARSEVEC(procinfo->fn_oid) && !IS_BITVEC(procinfo->fn_oid)) {
        Vector* currentVec = (Vector*)HnswGetValue(base, element);
        if (enableLsg) {
            currentVec->isoValue = CalcIsoVal((float*)currentVec->x, LocScalingParam);
        } else {
            currentVec->isoValue = Float32ToFloat16(1.0f);
        }
    }

    Pointer codePtr = NULL;
    if (enablePQ) {
        Size codesize = params.pqM * sizeof(uint8);
        codePtr = (Pointer)HnswAlloc(NULL, codesize);
    }
    HnswPtrStore(base, element->pqcodes, codePtr);

    /* Find neighbors for element */
    HnswFindElementNeighbors(base, element, entryPoint, index, procinfo, collation, m, efConstruction,
                             false, enablePQ, &params, enableRabitQ, &rbqDiskParams, enableLsg);

    /* Update graph on disk */
    UpdateGraphOnDisk(index, procinfo, collation, element, m, efConstruction, entryPoint, building,
                      enableRabitQ, &rbqDiskParams, rbqConfig, enableLsg, buildExtensionLock);

    /* Release lock */
    UnlockPage(index, HNSW_UPDATE_LOCK, lockmode);

    if (enableRabitQ) {
        pfree((&rbqDiskParams)->heapTuple);
        pfree((&rbqDiskParams)->indexInfo);
    }
    if (LocScalingParam != NULL) {
        if (LocScalingParam->sampleVecs != NULL) {
            pfree(LocScalingParam->sampleVecs);
            LocScalingParam->sampleVecs = NULL;
        }
        pfree(LocScalingParam);
        LocScalingParam = NULL;
    }

    return true;
}

/*
 * Insert a tuple into the index
 */
static void HnswInsertTuple(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid, Relation heap)
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

    HnswInsertTupleOnDisk(index, value, isnull, heap_tid, false, heap);
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

    /* When the amount of inserted data is less than the threshold,
     * update insertedRows in the metapage; when the threshold is reached,
     * update insertedRows in the metapage, change the state of rbqDelay,
     * and then build the index with HNSW RabitQ; when the threshold is exceeded,
     * insert data into the built index.
     */
    HnswRbqMetaPageInfo rbqInfo;
    HnswGetRbqMetaPageInfo(index, &rbqInfo);
    if (rbqInfo.rbqDelayState == RBQ_BUILD_DELAY) {
        LockPage(index, HNSW_UPDATE_LOCK, ExclusiveLock);
        HnswRbqMetaPageInfo rbqInfoCheck;
        HnswGetRbqMetaPageInfo(index, &rbqInfoCheck);
        if (rbqInfoCheck.rbqDelayState == RBQ_BUILD_DELAY) {
            int64 sampleRows = u_sess->datavec_ctx.rbq_sample_rows;
            if (rbqInfoCheck.rbqInsertRows + 1 < sampleRows) {
                HnswUpdateMetaPageRbq(index, MAIN_FORKNUM, false);
            } else if (rbqInfoCheck.rbqInsertRows + 1 == sampleRows) {
                HnswUpdateMetaPageRbq(index, MAIN_FORKNUM, true);
                HnswBuildState buildstate;
                IndexInfo *indexInfo = BuildIndexInfo(index);
                BuildIndex(heap, index, indexInfo, &buildstate, MAIN_FORKNUM, true);
                ereport(LOG, (errmsg("The amount of data in the heap table is equal to rbq_sample_rows,"
                    "build HNSW RabitQ index.")));
            } else {
                UnlockPage(index, HNSW_UPDATE_LOCK, ExclusiveLock);
                ereport(ERROR, (errmsg("The amount of data in the heap table is greater than rbq_sample_rows,"
                    "but the state of rbqDelay has not changed.")));
            }
            UnlockPage(index, HNSW_UPDATE_LOCK, ExclusiveLock);
            return false;
        }
        UnlockPage(index, HNSW_UPDATE_LOCK, ExclusiveLock);
    }

    /* Create memory context */
    insertCtx = AllocSetContextCreate(CurrentMemoryContext, "Hnsw insert temporary context", ALLOCSET_DEFAULT_SIZES);
    oldCtx = MemoryContextSwitchTo(insertCtx);

    /* Insert tuple */
    HnswInsertTuple(index, values, isnull, heap_tid, heap);

    /* Delete memory context */
    MemoryContextSwitchTo(oldCtx);
    MemoryContextDelete(insertCtx);

    return false;
}
