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
 * hnswdelete.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/hnswdelete.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "access/ubtree.h"
#include "access/datavec/hnsw.h"
#include "access/datavec/vecindex.h"

bool HnswIsTIDEquals(ItemPointer p1, ItemPointer p2)
{
    int id;
    bool equal = true;
    for (id = 0; id < HNSW_HEAPTIDS; id++) {
        if (ItemPointerIsValid(&p1[id]) && ItemPointerIsValid(&p2[id])) {
            equal = ItemPointerEquals(&p1[id], &p2[id]);
        } else if (!ItemPointerIsValid(&p1[id]) && !ItemPointerIsValid(&p2[id])) {
            continue;
        } else {
            equal = false;
        }

        if (!equal) {
            break;
        }
    }

    return equal;
}

bool HnswIsETUPEqual(HnswElementTuple etup1, HnswElementTuple etup2)
{
    if (etup1 == NULL || etup2 == NULL) {
        return false;
    }
    Size len1 = MAXALIGN(VARSIZE_ANY(&etup1->data));
    Size len2 = MAXALIGN(VARSIZE_ANY(&etup2->data));
    if (len1 == 0 || len2 == 0 || len1 != len2) {
        return false;
    }
    return memcmp(&etup1->data, &etup2->data, len1) == 0;
}

OffsetNumber HnswFindDeleteLocation(Relation index, Buffer buf, HnswElementTuple etup)
{
    OffsetNumber off;
    OffsetNumber maxOff;
    Page page;
    TransactionId xmin;
    TransactionId xmax;

    page = BufferGetPage(buf);
    maxOff = PageGetMaxOffsetNumber(page);

    if (RelationIsGlobalIndex(index)) {
        elog(ERROR, "the GLOBAL partitioned index is not supported.\n");
    }

    for (off = FirstOffsetNumber; off < maxOff; off++) {
        ItemId iid;
        HnswElementTuple tup;

        iid = PageGetItemId(page, off);
        if (!ItemIdIsDead(iid)) {
            tup = (HnswElementTuple)PageGetItem(page, iid);
            if (!HnswIsTIDEquals(etup->heaptids, tup->heaptids)) {
                continue;
            }

            if (!HnswIsETUPEqual(etup, tup)) {
                continue;
            }

            bool xminCommitted = false;
            bool xmaxCommitted = false;
            bool isDead = VecItupGetXminXmax(page, off, InvalidTransactionId, &xmin, &xmax, &xminCommitted,
                                             &xmaxCommitted, RelationGetNamespace(index) == PG_TOAST_NAMESPACE);
            if (!isDead && !TransactionIdIsValid(xmax)) {
                return off;
            }
        }
    }
    return InvalidOffsetNumber;
}

void HnswDeleteOnPage(Relation index, Buffer buf, OffsetNumber offset)
{
    ItemId iid;
    IndexTransInfo *idxXid;
    Page page;
    HnswElementTuple etup;

    page = BufferGetPage(buf);
    iid = PageGetItemId(page, offset);
    etup = (HnswElementTuple)PageGetItem(page, iid);
    idxXid = (IndexTransInfo *)VecIndexTupleGetXid(etup);

    idxXid->xmax = GetCurrentTransactionId();

    MarkBufferDirty(buf);
}

bool IsHnswEntryPoint(Relation index, BlockNumber blkno, OffsetNumber offno)
{
    Buffer buf;
    Page page;
    HnswMetaPage metap;
    bool res = false;

    buf = ReadBuffer(index, HNSW_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    metap = HnswPageGetMeta(page);
    if (blkno == metap->entryBlkno && offno == metap->entryOffno) {
        res = true;
    }
    UnlockReleaseBuffer(buf);
    return res;
}

bool HnswDeleteIndex(Relation index, HnswElementTuple etup)
{
    bool found = false;
    BlockNumber blkno;
    Buffer buf;
    char *base = NULL;
    Datum q;
    List *ep;
    List *w;
    ListCell *cell;
    int m;
    HnswElement entryPoint;
    FmgrInfo *procinfo;
    Oid collation;
    OffsetNumber offset;
    Page page;

    blkno = InvalidBlockNumber;
    procinfo = index_getprocinfo(index, 1, 1);
    collation = index->rd_indcollation[0];
    q = (Datum)(&etup->data);
    HnswGetMetaPageInfo(index, &m, &entryPoint);
    ep = list_make1(HnswEntryCandidate(base, entryPoint, q, index, procinfo, collation, false, NULL));

    for (int lc = entryPoint->level; lc >= 0; lc--) {
        w = HnswSearchLayer(base, q, ep, 1, lc, index, procinfo, collation, m, false, NULL);
        ep = w;
    }

    foreach (cell, ep) {
        HnswCandidate *hc = (HnswCandidate *)lfirst(cell);
        HnswElement element = (HnswElement)HnswPtrAccess(base, hc->element);
        blkno = element->blkno;
    }

    while (BlockNumberIsValid(blkno)) {
        buf = ReadBuffer(index, blkno);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        offset = HnswFindDeleteLocation(index, buf, etup);
        if (offset != InvalidOffsetNumber && !IsHnswEntryPoint(index, blkno, offset)) {
            HnswDeleteOnPage(index, buf, offset);
            UnlockReleaseBuffer(buf);
            found = true;
            break;
        }

        page = BufferGetPage(buf);
        blkno = HnswPageGetOpaque(page)->nextblkno;
        UnlockReleaseBuffer(buf);
    }
    return found;
}

HnswElementTuple IndexFormHnswElementTuple(TupleDesc tupleDesc, Datum *values, const bool *isnull,
                                           ItemPointer heapTCtid)
{
    Datum value;
    HnswElementTuple etup;
    Size etupSize;
    errno_t rc = EOK;

    value = PointerGetDatum(PG_DETOAST_DATUM(values[0]));

    etup = (HnswElementTuple)palloc0(HNSW_TUPLE_ALLOC_SIZE);
    etupSize = HNSW_ELEMENT_TUPLE_SIZE(VARSIZE_ANY(DatumGetPointer(value)));

    etup->heaptids[0] = *heapTCtid;
    for (int i = 1; i < HNSW_HEAPTIDS; i++) {
        ItemPointerSetInvalid(&etup->heaptids[i]);
    }

    rc = memcpy_s(&etup->data, VARSIZE_ANY(DatumGetPointer(value)), DatumGetPointer(value), VARSIZE_ANY(DatumGetPointer(value)));
    securec_check(rc, "\0", "\0");
    return etup;
}

bool hnswdelete_internal(Relation index, Datum *values, const bool *isnull, ItemPointer heapTCtid, bool isRollbackIndex)
{
    if (IsRelnodeMmapLoad(index->rd_node.relNode)) {
        ereport(ERROR, (errmsg("cannot do DML after mmap load")));
        return false;
    }
    bool found;
    HnswElementTuple etup;

    etup = IndexFormHnswElementTuple(RelationGetDescr(index), values, isnull, heapTCtid);
    found = HnswDeleteIndex(index, etup);

    return found;
}
