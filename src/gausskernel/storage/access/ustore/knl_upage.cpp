/* -------------------------------------------------------------------------
 *
 * knl_upage.cpp
 *     the page format of inplace update engine.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * src/gausskernel/storage/access/ustore/knl_upage.cpp
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "storage/buf/bufmgr.h"
#include "utils/rel.h"
#include "access/transam.h"
#include "access/ustore/knl_upage.h"
#include "access/ustore/knl_uvisibility.h"
#include "storage/freespace.h"

#ifdef DEBUG_UHEAP
#include "pgstat.h"
#endif

#define ISNULL_BITMAP_NUMBER 2

template<UPageType upagetype> void UPageInit(Page page, Size pageSize, Size specialSize, uint8 tdSlots)
{
    Assert(pageSize == BLCKSZ);

    char *ptr = (char *)page;
    memset_s(ptr, pageSize, 0, pageSize);

    /* Fixme: Restructure the code such that there are individual instatiations for all possible page types. This will
     * allow us to have compile time bindings for all the page types. e.g. See the instatiation for UPAGE_HEAP below. */
    switch (upagetype) {
        case UPAGE_INDEX: {
            break;
        }
        default: {
            Assert(0);
            break;
        }
    }
}

template<> void UPageInit<UPAGE_HEAP>(Page page, Size page_size, Size special_size, uint8 tdSlots)
{
    Assert(page_size == BLCKSZ);

    char *ptr = (char *)page;
    memset_s(ptr, page_size, 0, page_size);

    Assert(page_size > (SizeOfUHeapPageHeaderData + special_size + (tdSlots * sizeof(TD))));
    PageInit(page, page_size, special_size, false);
    ptr += SizeOfUHeapPageHeaderData;
    UHeapPageHeaderData *uheap_page = (UHeapPageHeaderData *)page;
    uheap_page->td_count = tdSlots;
    uheap_page->pd_lower = SizeOfUHeapPageHeaderData + tdSlots * sizeof(TD);
    uheap_page->pd_upper = page_size - special_size;
    uheap_page->potential_freespace = uheap_page->pd_upper - uheap_page->pd_lower;
    PageSetPageSizeAndVersion(page, page_size, PG_UHEAP_PAGE_LAYOUT_VERSION);
}

template<> void UPageInit<UPAGE_TOAST>(Page page, Size page_size, Size special_size, uint8 tdSlots)
{
    Assert(page_size == BLCKSZ);

    char *ptr = (char *)page;
    memset_s(ptr, page_size, 0, page_size);

    Assert(page_size > (SizeOfUHeapPageHeaderData + special_size + (UHEAP_DEFAULT_TOAST_TD_COUNT * sizeof(TD))));
    PageInit(page, page_size, special_size, false);
    ptr += SizeOfUHeapPageHeaderData;
    UHeapPageHeaderData *uheap_page = (UHeapPageHeaderData *)page;
    uheap_page->td_count = UHEAP_DEFAULT_TOAST_TD_COUNT;
    uheap_page->pd_lower = SizeOfUHeapPageHeaderData + UHEAP_DEFAULT_TOAST_TD_COUNT * sizeof(TD);
    uheap_page->pd_upper = page_size - special_size;
    uheap_page->potential_freespace = uheap_page->pd_upper - uheap_page->pd_lower;
    PageSetPageSizeAndVersion(page, page_size, PG_UHEAP_PAGE_LAYOUT_VERSION);
}


static bool CheckOffsetValidity(Page page, OffsetNumber offsetNumber, bool *needshuffle, bool overwrite)
{
    RowPtr              *itemId = NULL;
    OffsetNumber         limit;
    UHeapPageHeaderData *uphdr = (UHeapPageHeaderData *)page;

    limit = OffsetNumberNext(UHeapPageGetMaxOffsetNumber(page));
    if (overwrite) {
        if (offsetNumber < limit) {
            itemId = UPageGetRowPtr(uphdr, offsetNumber);

            if (t_thrd.xlog_cxt.InRecovery) {
                if (RowPtrHasStorage(itemId)) {
                    elog(WARNING, "will not overwrite a used ItemId");
                    return false;
                }
            } else if (RowPtrIsUsed(itemId) || RowPtrHasStorage(itemId)) {
                elog(WARNING, "will not overwrite a used ItemId");
                return false;
            }
        }
    } else {
        if (offsetNumber < limit) {
            *needshuffle = true; /* need to move existing linp's */
        }
    }

    return true;
}

static void FindNextFreeSlot(const UHeapBufferPage *bufpage, Page input_page, OffsetNumber *offsetNumber)
{
    RowPtr *itemId = NULL;
    Page page = bufpage->page;
    UHeapPageHeaderData *uphdr = (UHeapPageHeaderData *)page;
    OffsetNumber limit;

    limit = OffsetNumberNext(UHeapPageGetMaxOffsetNumber(page));
    if (UPageHasFreeLinePointers(uphdr)) {
        bool hasPendingXact = false;

        /*
         * Look for "recyclable" (unused) ItemId.  We check for no storage
         * as well, just to be paranoid --- unused items should never have
         * storage.
         */
        for (*offsetNumber = FirstOffsetNumber; *offsetNumber < limit; (*offsetNumber)++) {
            itemId = UPageGetRowPtr(uphdr, *offsetNumber);
            if (!RowPtrIsUsed(itemId) && !RowPtrHasStorage(itemId)) {
                /*
                 * We allow Unused entries to be reused only if there is
                 * no transaction information for the entry or the
                 * transaction is committed.
                 */
                if (RowPtrHasPendingXact(itemId)) {
                    UHeapTupleTransInfo xactinfo;

                    xactinfo.td_slot = RowPtrGetTDSlot(itemId);

                    /*
                     * We can't reach here for a valid input page as the
                     * callers passed it for the pages that wouldn't have
                     * been pruned.
                     */
                    Assert(!PageIsValid(input_page));

                    /*
                     * Here, we are relying on the transaction information
                     * in slot as if the corresponding slot has been
                     * reused, then transaction information from the entry
                     * would have been cleared.  See PageFreezeTransSlots.
                     */
                    if (xactinfo.td_slot == UHEAPTUP_SLOT_FROZEN) {
                        break;
                    }

                    GetTDSlotInfo(bufpage->buffer, xactinfo.td_slot, &xactinfo);

                    if (xactinfo.td_slot == UHEAPTUP_SLOT_FROZEN) {
                        break;
                    }

                    if (TransactionIdIsValid(xactinfo.xid) &&
                        !UHeapTransactionIdDidCommit(xactinfo.xid)) {
                        hasPendingXact = true;
                        continue;
                    }
                }
                break;
            }
        }

        if (*offsetNumber >= limit && !hasPendingXact) {
            /* the hint is wrong, so reset it */
            UPageClearHasFreeLinePointers(uphdr);
        }
    } else {
        /* don't bother searching if hint says there's no free slot */
        *offsetNumber = limit;
    }
}

static bool CalculateLowerUpperPointers(Page page, OffsetNumber offsetNumber, Item item, Size size, bool needshuffle)
{
    int             lower;
    int             upper;
    Size            alignedSize;
    RowPtr          *itemId = NULL;
    OffsetNumber    limit;
    UHeapPageHeaderData *uphdr = (UHeapPageHeaderData *)page;

    limit = OffsetNumberNext(UHeapPageGetMaxOffsetNumber(page));
    /* Reject placing items beyond the first unused line pointer */
    if (offsetNumber > limit) {
        elog(WARNING, "specified item offset is too large");
        return false;
    }

    /*
     * Compute new lower and upper pointers for page, see if it'll fit.
     *
     * Note: do arithmetic as signed ints, to avoid mistakes if, say, size >
     * pd_upper.
     */
    if (offsetNumber == limit || needshuffle) {
        lower = uphdr->pd_lower + sizeof(ItemIdData);
    } else {
        lower = uphdr->pd_lower;
    }
    alignedSize = SHORTALIGN(size);

    upper = (int) uphdr->pd_upper - (int) alignedSize;

    if (lower > upper) {
        return false;
    }

    /*
     * OK to insert the item.  First, shuffle the existing pointers if needed.
     */
    itemId = UPageGetRowPtr(uphdr, offsetNumber);

    if (needshuffle) {
        errno_t rc = memmove_s(itemId + 1, (limit - offsetNumber) * sizeof(ItemIdData),
                               itemId, (limit - offsetNumber) * sizeof(ItemIdData));
        securec_check(rc, "\0", "\0");
    }

    /* set the item pointer */
    SetNormalRowPointer(itemId, upper, size);

    /* copy the item's data onto the page */
    errno_t rc = memcpy_s((char *) page + upper, size, item, size);
    securec_check(rc, "\0", "\0");

    /* adjust page header */
    uphdr->pd_lower = (uint16) lower;
    uphdr->pd_upper   = (uint16) upper;

    return true;
}

/*
 * UPageAddItem: this function is similar to PageAddItem, but we
 * removed the flag is_heap since UPageAddItem is only called on
 * a UHeap data page
 */
OffsetNumber UPageAddItem(Relation rel, UHeapBufferPage *bufpage, Item item, Size size,
    OffsetNumber offsetNumber, bool overwrite)
{
    Page page, input_page = NULL;
    bool needshuffle = false;
    bool offsetvalid = false;

    /* Either one of buffer or page could be valid. */
    if (BufferIsValid(bufpage->buffer)) {
        Assert(!PageIsValid(bufpage->page));
        page = BufferGetPage(bufpage->buffer);
        bufpage->page = page;
    } else {
        Assert(PageIsValid(bufpage->page));
        page = bufpage->page;
        input_page = bufpage->page;
    }

    Assert(PageIsValid(page));

    UHeapPageHeaderData *uphdr = (UHeapPageHeaderData *)page;

    /*
     * Be wary about corrupted page pointers
     * Fixme: Add more sanity checks for page
     */
    if (uphdr->pd_lower > uphdr->pd_upper)
        ereport(PANIC, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("corrupted page pointers: pd_lower = %u, pd_upper = %u", uphdr->pd_lower,
                uphdr->pd_upper)));

    /* was offsetNumber passed in? */
    if (OffsetNumberIsValid(offsetNumber)) {
        offsetvalid = CheckOffsetValidity(page, offsetNumber, &needshuffle, overwrite);
        if (!offsetvalid) {
            return InvalidOffsetNumber;
        }
    } else {
        FindNextFreeSlot(bufpage, input_page, &offsetNumber);
    }

    /* Reject placing items beyond heap boundary, if heap */
    if (!t_thrd.xlog_cxt.InRecovery && offsetNumber > CalculatedMaxUHeapTuplesPerPage(RelationGetInitTd(rel))) {
        elog(WARNING, "can't put (%d)th item in a Ustore page with max %d items",
            offsetNumber, CalculatedMaxUHeapTuplesPerPage(RelationGetInitTd(rel)));
        return InvalidOffsetNumber;
    }

    /* Reject placing items beyond heap boundary, if heap */
    if (!t_thrd.xlog_cxt.InRecovery && offsetNumber > CalculatedMaxUHeapTuplesPerPage(RelationGetInitTd(rel))) {
        elog(WARNING, "can't put (%d)th item in a Ustore page with max %d items",
            offsetNumber, CalculatedMaxUHeapTuplesPerPage(RelationGetInitTd(rel)));
        return InvalidOffsetNumber;
    }

    if (!CalculateLowerUpperPointers(page, offsetNumber, item, size, needshuffle)) {
        return InvalidOffsetNumber;
    }

    return offsetNumber;
}

/*
 * UHeapGetTuple
 *
 * Copy a raw tuple from a uheap page, forming a UHeapTuple.
 */
UHeapTuple UHeapGetTuple(Relation relation, Buffer buffer, OffsetNumber offnum, UHeapTuple freebuf)
{
    Page dp;
    RowPtr *rp;
    Size tupleLen;
    UHeapDiskTuple item;
    UHeapTuple tuple;

#ifdef DEBUG_UHEAP
    UHEAPSTAT_COUNT_TUPLE_VISITS();
#endif

    dp = BufferGetPage(buffer);
    rp = UPageGetRowPtr(dp, offnum);

    Assert(offnum >= FirstOffsetNumber && offnum <= UHeapPageGetMaxOffsetNumber(dp));
    Assert(RowPtrIsNormal(rp));

    tupleLen = RowPtrIsDeleted(rp) ? 0 : RowPtrGetLen(rp);

    if (freebuf) {
        Assert(freebuf->disk_tuple != NULL);
        tuple = freebuf;

        /*
         * The caller is using a pre allocated buffer, eg: TidScan.
         * Make sure to initialize the correct tuple type.
         */
        tuple->tupTableType = UHEAP_TUPLE;
    } else {
        tuple = (UHeapTuple)uheaptup_alloc(UHeapTupleDataSize + tupleLen);
        tuple->disk_tuple = (UHeapDiskTuple)((char *)tuple + UHeapTupleDataSize);
    }

    tuple->table_oid = RelationGetRelid(relation);
    tuple->xc_node_id = u_sess->pgxc_cxt.PGXCNodeIdentifier;
    tuple->disk_tuple_size = tupleLen;
    ItemPointerSet(&tuple->ctid, BufferGetBlockNumber(buffer), offnum);
    item = (UHeapDiskTuple)UPageGetRowData(dp, rp);
    errno_t rc = memcpy_s(tuple->disk_tuple, tupleLen, item, tupleLen);
    securec_check(rc, "", "");

    return tuple;
}

/*
 * UHeapGetTuplePartial
 *
 * Copy part of a raw tuple from a uheap page, forming a UHeapTuple that excludes some columns
 * Columns that aren't copied are marked as NULL
 */
UHeapTuple UHeapGetTuplePartial(Relation relation, Buffer buffer, OffsetNumber offnum, AttrNumber lastVar,
    bool *boolArr)
{
    Page dp = BufferGetPage(buffer);
    RowPtr *rp = UPageGetRowPtr(dp, offnum);
    bool enableReverseBitmap = false;

#ifdef DEBUG_UHEAP
    UHEAPSTAT_COUNT_TUPLE_VISITS();
#endif

    Assert(offnum >= FirstOffsetNumber && offnum <= UHeapPageGetMaxOffsetNumber(dp));
    Assert(RowPtrIsNormal(rp));

    UHeapDiskTuple item = (UHeapDiskTuple)UPageGetRowData(dp, rp);

    /* length of source tuple */
    Size tupleLen = RowPtrIsDeleted(rp) ? 0 : RowPtrGetLen(rp);

    /* tuple_desc */
    TupleDesc rowDesc = RelationGetDescr(relation);

#ifdef USE_ASSERT_CHECKING
    int natts = rowDesc->natts;
#endif

    int diskTupleNAttrs = UHeapTupleHeaderGetNatts(item);
    enableReverseBitmap = NAttrsReserveSpace(diskTupleNAttrs);
    uint8 nullsLen = BITMAPLEN(diskTupleNAttrs);
    if (enableReverseBitmap) {
        nullsLen += BITMAPLEN(diskTupleNAttrs);
    }

    /* make room for null bitmap and reserve bitmap in dest tuple */
    if (lastVar > 0) {
        tupleLen += (nullsLen + UHEAP_MAX_ATTR_PAD);
        if (tupleLen > MaxUHeapTupleSize(relation)) {
            /* if bitmaps cannot fit within tuple length limit, fallback */
            lastVar = -1;
        }
    }

    UHeapTuple tuple = (UHeapTuple)uheaptup_alloc(UHeapTupleDataSize + tupleLen);
    tuple->disk_tuple = (UHeapDiskTuple)((char *)tuple + UHeapTupleDataSize);

    tuple->table_oid = RelationGetRelid(relation);
    tuple->xc_node_id = u_sess->pgxc_cxt.PGXCNodeIdentifier;
    tuple->disk_tuple_size = tupleLen;

    ItemPointerSet(&tuple->ctid, BufferGetBlockNumber(buffer), offnum);

    if (lastVar <= 0) {
        errno_t rc = memcpy_s(tuple->disk_tuple, tupleLen, item, tupleLen);
        securec_check(rc, "", "");
    } else {
        Assert(lastVar <= natts);
        errno_t rc = memcpy_s(tuple->disk_tuple, SizeOfUHeapDiskTupleTillThoff, item, SizeOfUHeapDiskTupleTillThoff);
        securec_check(rc, "", "");
        UHeapDiskTupSetHasNulls(tuple->disk_tuple); // dest tuple will always have nulls
        tuple->disk_tuple->t_hoff = SizeOfUHeapDiskTupleData + nullsLen;

        rc = memset_s(tuple->disk_tuple->data, nullsLen, 0, nullsLen);
        securec_check(rc, "", "");

        if (UHeapDiskTupNoNulls(item)) {
            UHeapCopyDiskTupleNoNull(rowDesc, boolArr, tuple, lastVar, item);
        } else {
            UHeapCopyDiskTupleWithNulls(rowDesc, boolArr, tuple, lastVar, item);
        }
    }
    Assert(UHeapTupleHeaderGetNatts(tuple->disk_tuple) == diskTupleNAttrs);
    return tuple;
}

static Size UPageGetFreeSpace(Page page)
{
    int space = (int)((UHeapPageHeaderData *)page)->pd_upper -
        (int)((UHeapPageHeaderData *)page)->pd_lower;
    if (space < (int)sizeof(RowPtr)) {
        return 0;
    }
    space -= sizeof(RowPtr);

    return (Size)space;
}


/*
 * PageGetUHeapFreeSpace
 * Returns the size of the free (allocatable) space on a uheap page,
 * reduced by the space needed for a new line pointer.
 *
 * This is same as PageGetHeapFreeSpace except for max tuples that can
 * be accommodated on a page or the way unused items are dealt.
 */
Size PageGetUHeapFreeSpace(Page page)
{
    Size space;

    space = UPageGetFreeSpace(page);
    if (space > 0) {
        OffsetNumber offnum, nline;

        nline = UHeapPageGetMaxOffsetNumber(page);
        if (nline >= CalculatedMaxUHeapTuplesPerPage(UPageGetTDSlotCount(page))) {
            if (PageHasFreeLinePointers((PageHeader)page)) {
                /*
                 * Since this is just a hint, we must confirm that there is
                 * indeed a free line pointer
                 */
                for (offnum = FirstOffsetNumber; offnum <= nline; offnum = OffsetNumberNext(offnum)) {
                    RowPtr *lp = UPageGetRowPtr(page, offnum);

                    /*
                     * The unused items that have pending xact information
                     * can't be reused.
                     */
                    if (!RowPtrIsUsed(lp) && !RowPtrHasPendingXact(lp))
                        break;
                }

                if (offnum > nline) {
                    /*
                     * The hint is wrong, but we can't clear it here since we
                     * don't have the ability to mark the page dirty.
                     */
                    space = 0;
                }
            } else {
                /*
                 * Although the hint might be wrong, PageAddItem will believe
                 * it anyway, so we must believe it too.
                 */
                space = 0;
            }
        }
    }

    return space;
}

/*
 * PageGetExactUHeapFreeSpace
 * Returns the size of the free (allocatable) space on a page,
 * without any consideration for adding/removing line pointers.
 */
Size PageGetExactUHeapFreeSpace(Page page)
{
    int space;

    /*
     * Use signed arithmetic here so that we behave sensibly if pd_lower >
     * pd_upper.
     */
    space = (int)((UHeapPageHeaderData *)page)->pd_upper - (int)((UHeapPageHeaderData *)page)->pd_lower;

    if (space < 0) {
        return 0;
    }

    return (Size)space;
}

static UHeapFreeOffsetRanges *
LocateUsableItemIds(Page page, UHeapTuple *tuples, int ntuples, Size saveFreeSpace, int *nthispage, Size *usedSpace)
{
    bool         inRange = false;
    OffsetNumber offsetNumber;
    OffsetNumber limit;
    Size         availSpace;
    UHeapFreeOffsetRanges *ufreeOffsetRanges;

    ufreeOffsetRanges = (UHeapFreeOffsetRanges *) palloc0(sizeof(UHeapFreeOffsetRanges));
    ufreeOffsetRanges->nranges = 0;

    availSpace = PageGetExactUHeapFreeSpace(page);
    limit = OffsetNumberNext(UHeapPageGetMaxOffsetNumber(page));

    /*
     * Look for "recyclable" (unused) ItemId.  We check for no storage as
     * well, just to be paranoid --- unused items should never have
     * storage.
     */
    for (offsetNumber = 1; offsetNumber < limit; offsetNumber++) {
        RowPtr *rp = UPageGetRowPtr(page, offsetNumber);

        if (*nthispage >= ntuples) {
            /* No more tuples to insert */
            break;
        }

        if (!RowPtrIsUsed(rp) && !RowPtrHasStorage(rp)) {
            UHeapTuple      uheaptup = tuples[*nthispage];
            Size            neededSpace = *usedSpace + uheaptup->disk_tuple_size + saveFreeSpace;

            /* Check if we can fit this tuple in the page */
            if (availSpace < neededSpace) {
                /* No more space to insert tuples in this page */
                break;
            }

            (*usedSpace) += uheaptup->disk_tuple_size;
            (*nthispage)++;

            if (!inRange) {
                /* Start of a new range */
                ufreeOffsetRanges->nranges++;
                ufreeOffsetRanges->startOffset[ufreeOffsetRanges->nranges - 1] = offsetNumber;
                inRange = true;
            }
            ufreeOffsetRanges->endOffset[ufreeOffsetRanges->nranges - 1] = offsetNumber;
        } else {
            inRange = false;
        }
    }

    return ufreeOffsetRanges;
}

/*
 * UHeapGetUsableOffsetRanges
 *
 * Given a page and a set of tuples, it calculates how many tuples can fit in
 * the page and the contiguous ranges of free offsets that can be used/reused
 * in the same page to store those tuples.
 */
UHeapFreeOffsetRanges *UHeapGetUsableOffsetRanges(Buffer buffer, UHeapTuple *tuples, int ntuples, Size saveFreeSpace)
{
    Page page;
    int nthispage = 0;
    Size usedSpace = 0;
    Size availSpace;
    OffsetNumber limit;
    UHeapFreeOffsetRanges *ufreeOffsetRanges = NULL;

    page = BufferGetPage(buffer);

    limit = OffsetNumberNext(UHeapPageGetMaxOffsetNumber(page));
    availSpace = PageGetExactUHeapFreeSpace(page);

    UHeapPageHeaderData *uphdr = (UHeapPageHeaderData *)page;
    if (UPageHasFreeLinePointers(uphdr)) {
        ufreeOffsetRanges = LocateUsableItemIds(page, tuples, ntuples, saveFreeSpace, &nthispage, &usedSpace);
    } else {
        ufreeOffsetRanges = (UHeapFreeOffsetRanges *) palloc0(sizeof(UHeapFreeOffsetRanges));
        ufreeOffsetRanges->nranges = 0;
    }

    /*
     * Now, there are no free line pointers. Check whether we can insert
     * another tuple in the page, then we'll insert another range starting
     * from limit to max required offset number. We can decide the actual end
     * offset for this range while inserting tuples in the buffer.
     */
    if ((limit <= CalculatedMaxUHeapTuplesPerPage(UPageGetTDSlotCount(page))) && (nthispage < ntuples)) {
        UHeapTuple uheaptup = tuples[nthispage];
        Size neededSpace = usedSpace + sizeof(ItemIdData) + uheaptup->disk_tuple_size + saveFreeSpace;

        /* Check if we can fit this tuple + a new offset in the page */
        if (availSpace >= neededSpace) {
            OffsetNumber maxRequiredOffset;
            int requiredTuples = ntuples - nthispage;

            /*
             * Choose minimum among MaxOffsetNumber and the maximum offsets
             * required for tuples.
             */
            maxRequiredOffset = Min(MaxOffsetNumber, (limit + requiredTuples));

            ufreeOffsetRanges->nranges++;
            ufreeOffsetRanges->startOffset[ufreeOffsetRanges->nranges - 1] = limit;
            ufreeOffsetRanges->endOffset[ufreeOffsetRanges->nranges - 1] = maxRequiredOffset;
        } else if (ufreeOffsetRanges->nranges == 0) {
            ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED), errmsg("tuple is too big, please check the table fillfactor: "
                                                         "needed_size = %lu, free_space = %lu, save_free_space = %lu.",
                                                         (unsigned long)neededSpace,
                                                         (unsigned long)availSpace,
                                                         (unsigned long)saveFreeSpace)));
        }
    }

    if (ufreeOffsetRanges->nranges == 0) {
        ereport(PANIC, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("please check free space: limit = %u, num_this_page = %d, free_space = %lu.", limit, nthispage,
                (unsigned long)availSpace)));
    }

    return ufreeOffsetRanges;
}

void UHeapRecordPotentialFreeSpace(Buffer buffer, int delta)
{
    UHeapPageHeaderData *page = (UHeapPageHeaderData *)BufferGetPage(buffer);
    Assert(page->potential_freespace >= 0);

    Size maxWritableSpc =
        page->pd_special - (SizeOfUHeapPageHeaderData + page->td_count * sizeof(TD));
    Size oldPotentialFreespace = page->potential_freespace;
    Size newPotentialFreespace = Min(oldPotentialFreespace + delta, maxWritableSpc);

    page->potential_freespace = newPotentialFreespace;
}
