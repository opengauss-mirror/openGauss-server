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
#define HIGH_BITS_LENGTH_OF_LSN 32

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
    itemId = UPageGenerateRowPtr(uphdr, offsetNumber);

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
    FastVerifyUPageRowPtr(itemId, uphdr, offsetNumber);

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
    FastVerifyUTuple(tuple->disk_tuple, buffer);
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
                    if (!RowPtrIsUsed(lp))
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

bool VerifyPageHeader(Page page)
{
    if (page == NULL) {
        return false;
    }
    
    PageHeader phdr = (PageHeader)page;
    uint16 pdLower = phdr->pd_lower;
    uint16 pdUpper = phdr->pd_upper;
    uint16 pdSpecial = phdr->pd_special;

    if (pdLower > pdUpper || pdUpper > pdSpecial || pdSpecial != BLCKSZ) {
        ereport(PANIC, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("page header invalid: lower = %u, upper = %u, special = %u.",
            pdLower, pdUpper, pdSpecial)));
    }

    return true;
}

static bool VerifyUPageHeader(Page page, VerifyLevel level, int logLevel)
{
    if (page == NULL) {
        return false;
    }

    UHeapPageHeader phdr = (UHeapPageHeader)page;
    uint16 pdLower = phdr->pd_lower;
    uint16 pdUpper = phdr->pd_upper;
    uint16 pdSpecial = phdr->pd_special;
    uint16 potentialSpace = phdr->potential_freespace;
    uint16 tdCount = phdr->td_count;
    TransactionId pruneXid = phdr->pd_prune_xid;
    TransactionId xidBase = phdr->pd_xid_base;

    if (pdLower < (SizeOfUHeapPageHeaderData + SizeOfUHeapTDData(phdr)) || pdLower > pdUpper ||
        pdUpper > pdSpecial || pdSpecial != BLCKSZ || potentialSpace > BLCKSZ) {
        ereport(logLevel, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("upage header invalid: lower = %u, upper = %u, special = %u, potential = %u.",
            pdLower, pdUpper, pdSpecial, potentialSpace)));
    }

    if (tdCount <= 0 || tdCount > UHEAP_MAX_TD) {
        ereport(logLevel, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("upage tdcount invalid: tdcount = %u.", tdCount)));
    }

    if ((level > USTORE_VERIFY_FAST) &&
        (TransactionIdFollows(pruneXid, t_thrd.xact_cxt.ShmemVariableCache->nextXid) ||
        TransactionIdFollows(xidBase, t_thrd.xact_cxt.ShmemVariableCache->nextXid))) {
        ereport(logLevel, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("upage xidbase invalid: xidbase = %lu, nextxid = %lu.",
            xidBase, t_thrd.xact_cxt.ShmemVariableCache->nextXid)));
    }

    return true;
}

static bool VerifyTDInfo(Page page, int tdId, UHeapTupleTransInfo *tdinfo, VerifyLevel level,
    bool tdReuse, TransactionId tupXid, int logLevel)
{
    if (page == NULL || tdinfo == NULL) {
        return false;
    }

    UHeapPageTDData *tdPtr = (UHeapPageTDData *)PageGetTDPointer(page);
    UHeapPageHeaderData *phdr = (UHeapPageHeaderData *)page;

    if (tdId == UHEAPTUP_SLOT_FROZEN) {
        tdinfo->td_slot = tdId;
        tdinfo->cid = InvalidCommandId;
        tdinfo->xid = InvalidTransactionId;
        tdinfo->urec_add = INVALID_UNDO_REC_PTR;
        return true;
    }

    if (tdId < 1 || tdId > phdr->td_count) {
        ereport(logLevel, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("An out of bounds of the array td_info, tdid = %d.", tdId)));
    }

    TD *thistrans = &tdPtr->td_info[tdId - 1];

    if ((level > USTORE_VERIFY_FAST) &&
        TransactionIdFollows(thistrans->xactid, t_thrd.xact_cxt.ShmemVariableCache->nextXid)) {
        ereport(logLevel, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("td xid invalid: tdid %d, tdxid = %lu, nextxid = %lu.",
            tdId, thistrans->xactid, t_thrd.xact_cxt.ShmemVariableCache->nextXid)));
    }

    if (!tdReuse && (!TransactionIdIsValid(thistrans->xactid) ||
        (TransactionIdIsValid(tupXid) && !TransactionIdEquals(thistrans->xactid, tupXid)))) {
        ereport(logLevel, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("tup xid inconsistency with td: tupxid = %lu, tdxid = %lu, urp %lu.",
            tupXid, thistrans->xactid, thistrans->undo_record_ptr)));
    }

    tdinfo->td_slot = tdId;
    tdinfo->cid = InvalidCommandId;
    tdinfo->urec_add = thistrans->undo_record_ptr;
    tdinfo->xid = thistrans->xactid;

    return true;
}

static bool VerifyUTuple(Relation rel, Page page, BlockNumber blkno, OffsetNumber offnum,
    TupleDesc tupDesc, VerifyLevel level, int logLevel)
{
    RowPtr *rp = UPageGetRowPtr(page, offnum);
    UHeapDiskTuple diskTup = (UHeapDiskTuple)UPageGetRowData(page, rp);
    int tdId = UHeapTupleHeaderGetTDSlot(diskTup);
    bool isInvalidSlot = UHeapTupleHasInvalidXact(diskTup->flag);
    TransactionId tupXid = UDiskTupleGetModifiedXid(diskTup, page);
    UHeapTupleTransInfo tdinfo;
    int tupSize = (rel == NULL) ? 0 : CalTupSize(rel, diskTup, tupDesc);
    if (tupSize > (int)RowPtrGetLen(rp) || (diskTup->reserved != 0 &&
        diskTup->reserved != 0xFF)) {
        ereport(logLevel, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("corrupted tuple: tupsize = %d, rpsize = %u.",
            tupSize, RowPtrGetLen(rp))));
    }

    VerifyTDInfo(page, tdId, &tdinfo, level, isInvalidSlot, tupXid, logLevel);
    if (!isInvalidSlot && IS_VALID_UNDO_REC_PTR(tdinfo.urec_add) &&
        (!TransactionIdIsValid(tdinfo.xid) || (TransactionIdIsValid(tupXid) &&
        !TransactionIdEquals(tdinfo.xid, tupXid)))) {
        ereport(logLevel, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("tup xid inconsistency with td: tupxid = %lu, tdxid = %lu.",
            tupXid, tdinfo.xid)));
    }

    if (level <= USTORE_VERIFY_FAST || !TransactionIdIsValid(tupXid)) {
        return true;
    }

    if (isInvalidSlot) {
        if (!TransactionIdDidCommit(tupXid)) {
            ereport(logLevel, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("tup xid not commit, tupxid = %lu.", tupXid)));
        }
        if (TransactionIdEquals(tdinfo.xid, tupXid)) {
            ereport(logLevel, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("td reused but xid equal td: tupxid = %lu, tdxid = %lu.",
                tupXid, tdinfo.xid)));
        }
    }

    return true;
}

static int RpCompare(const void *rp1, const void *rp2)
{
    return ((RpSort)rp1)->start - ((RpSort)rp2)->start;
}

static bool VerifyUPageRowPtr(Relation rel, Page page, BlockNumber blkno, TupleDesc tupDesc,
    VerifyLevel level, int logLevel)
{
    UHeapPageHeader phdr = (UHeapPageHeader)page;
    uint16 pdLower = phdr->pd_lower;
    uint16 pdUpper = phdr->pd_upper;
    uint16 pdSpecial = phdr->pd_special;
    int nline = UHeapPageGetMaxOffsetNumber(page);
    int tdSlot = 0;
    int nstorage = 0;
    int i;
    RpSortData rpBase[MaxPossibleUHeapTuplesPerPage];
    RpSort rpSortPtr = rpBase;
    RowPtr *rp = NULL;
    UHeapTupleTransInfo tdinfo;

    for (i = FirstOffsetNumber; i <= nline; i++) {
        rp = UPageGetRowPtr(page, i);
        if (RowPtrIsNormal(rp)) {
            rpSortPtr->start = RowPtrGetOffset(rp);
            rpSortPtr->end = rpSortPtr->start + SHORTALIGN(RowPtrGetLen(rp));
            rpSortPtr->offset = i;
            if (rpSortPtr->start < pdUpper || rpSortPtr->end > pdSpecial) {
                ereport(logLevel, (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("corrupted line pointer: offset = %u, rpstart = %u, "
                    "rplen = %u, pdlower = %u, pdupper = %u.",
                    i, RowPtrGetOffset(rp), RowPtrGetLen(rp), pdLower, pdUpper)));
            }
            rpSortPtr++;
            VerifyUTuple(rel, page, blkno, i, tupDesc, level, logLevel);
        } else if (RowPtrIsDeleted(rp)) {
            bool tdReuse = (RowPtrGetVisibilityInfo(rp) & ROWPTR_XACT_INVALID);
            tdSlot = RowPtrGetTDSlot(rp);
            if (tdSlot == UHEAPTUP_SLOT_FROZEN) {
                ereport(WARNING, (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("rowptr(offsetnumber = %d) tdslot frozen, tdid = %d.", i, tdSlot)));
            }
            VerifyTDInfo(page, tdSlot, &tdinfo, level, tdReuse, InvalidTransactionId, logLevel);
        }
    }

    nstorage = rpSortPtr - rpBase;

    if (nstorage <= 1 || level <= USTORE_VERIFY_FAST) {
        return true;
    }

    qsort((char *)rpBase, nstorage, sizeof(RpSortData), RpCompare);

    for (i = 0; i < nstorage - 1; i++) {
        RpSort tempPtr1 = &rpBase[i];
        RpSort tempPtr2 = &rpBase[i + 1];
        if (tempPtr1->end > tempPtr2->start) {
            ereport(logLevel, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("corrupted line pointer: rp1offset %u, rp1start = %u, rp1end = %u, "
                "rp2offset = %u, rp2start = %u, rp2end = %u.",
                tempPtr1->offset, tempPtr1->start, tempPtr1->end,
                tempPtr2->offset, tempPtr2->start, tempPtr2->end)));
        }
    }

    return true;
}

/*
 * Checks whether the LSN in the header of the uheap page is smaller than the value
 * of the last checkpoint. This check item is mainly used for verification after the page
 * is modified in parallel redo mode..
 */
static void ValidateUPageLsn(Page page, XLogRecPtr lastCheckpoint, int logLevel)
{
    if (page == NULL || lastCheckpoint == InvalidXLogRecPtr) {
        return;
    }

    if (PageGetLSN(page) < lastCheckpoint) {
        ereport(logLevel, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("Current lsn(%X/%X) in page is smaller than last checkpoint(%X/%X)).",
            (uint32)(PageGetLSN(page) >> HIGH_BITS_LENGTH_OF_LSN), (uint32)PageGetLSN(page),
            (uint32)(lastCheckpoint >> HIGH_BITS_LENGTH_OF_LSN), (uint32)lastCheckpoint)));
    }
}

bool VerifyUPageValid(UPageVerifyParams *verifyParams)
{
    if (verifyParams == NULL) {
        return false;
    }

    VerifyLevel vLevel = verifyParams->bvInfo.vLevel;
    Relation rel = verifyParams->bvInfo.rel;
    Page page = verifyParams->page;
    BlockNumber blkno = verifyParams->blk;
    TupleDesc tupDesc = verifyParams->tupDesc;
    XLogRecPtr latestRedo = InvalidXLogRecPtr;
    int logLevel = (verifyParams->bvInfo.analyzeVerify) ? WARNING : PANIC;

    VerifyUPageHeader(page, vLevel, logLevel);
    VerifyUPageRowPtr(rel, page, blkno, tupDesc, vLevel, logLevel);
    ValidateUPageLsn(page, latestRedo, logLevel);
    return true;
}

bool VerifyRedoUPageValid(URedoVerifyParams*verifyParams)
{
    if (verifyParams == NULL) {
        return false;
    }

    VerifyLevel vLevel = verifyParams->pageVerifyParams.bvInfo.vLevel;
    Relation rel = verifyParams->pageVerifyParams.bvInfo.rel;
    Page page = verifyParams->pageVerifyParams.page;
    BlockNumber blkno = verifyParams->pageVerifyParams.blk;
    TupleDesc tupDesc = verifyParams->pageVerifyParams.tupDesc;
    XLogRecPtr latestRedo = verifyParams->latestRedo;

    VerifyUPageHeader(page, vLevel, PANIC);
    VerifyUPageRowPtr(rel, page, blkno, tupDesc, vLevel, PANIC);
    ValidateUPageLsn(page, latestRedo, PANIC);
    return true;
}


void FastVerifyUTuple(UHeapDiskTuple diskTup, Buffer buffer)
{
    if (u_sess->attr.attr_storage.ustore_verify_level < (int) USTORE_VERIFY_DEFAULT) {
        return;
    }

    int tdId = UHeapTupleHeaderGetTDSlot(diskTup);
    int tdCount = UHEAP_MAX_TD;
    BlockNumber blockno = InvalidBlockNumber;
    Oid relId = InvalidOid;
    uint16 reserved = diskTup->reserved;
    if (!BufferIsInvalid(buffer)) {
        BufferDesc *bufdesc = GetBufferDescriptor(buffer - 1);
        Page page = BufferGetPage(buffer);
        UHeapPageHeaderData *phdr = (UHeapPageHeaderData *)page;
        tdCount = phdr->td_count;
        relId = bufdesc->tag.rnode.relNode;
        blockno = BufferGetBlockNumber(buffer);
    }
    if (tdId < 0 || tdId > tdCount) {
        ereport(PANIC, (errmodule(MOD_USTORE), errmsg(
            "verify utuple invalid! "
            "LogInfo: tdid %d, tdcount %u, reserved %u. "
            "TransInfo: oid %u, blockno %u.",
            tdId, tdCount, reserved, relId, blockno)));
    }
}

RowPtr *FastVerifyUPageRowPtr(RowPtr *rp, UHeapPageHeader uphdr, OffsetNumber offsetNumber)
{
    OffsetNumber maxOffsetNum = UHeapPageGetMaxOffsetNumber((char *)uphdr);
    if (offsetNumber > maxOffsetNum) {
        return rp;
    }
    if (u_sess->attr.attr_storage.ustore_verify_level >= (int) USTORE_VERIFY_DEFAULT) {
        if (RowPtrIsNormal(rp)) {
            if (RowPtrGetOffset(rp) < uphdr->pd_upper || RowPtrGetOffset(rp) >= uphdr->pd_special ||
                RowPtrGetOffset(rp) + RowPtrGetLen(rp) > BLCKSZ) {
                ereport(PANIC,
                        (errmodule(MOD_USTORE),
                         errmsg("row pointer error, offset:%u, flags:%u, len:%u, upper:%u, special:%u.",
                                RowPtrGetOffset(rp), rp->flags, RowPtrGetLen(rp), (uphdr)->pd_upper,
                                (uphdr)->pd_special)));
            }
        } else if (RowPtrGetLen(rp) != 0) {
            ereport(PANIC,
                    (errmodule(MOD_USTORE), errmsg("row pointer's length is too long, offset:%u, flags:%u, len:%u.",
                                                   RowPtrGetOffset(rp), (rp)->flags, RowPtrGetLen(rp))));
        }
    }
    return rp;
}