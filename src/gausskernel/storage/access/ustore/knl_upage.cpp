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

static void UpageVerifyTuple(UHeapPageHeader header, OffsetNumber off, TupleDesc tupDesc, Relation rel,
    RelFileNode* rNode, BlockNumber blkno, bool isRedo = false);
static void UpageVerifyAllRowptr(UHeapPageHeader header, RelFileNode* rNode, BlockNumber blkno, bool isRedo = false);
static void UpageVerifyRowptr(RowPtr *rowPtr, Page page, OffsetNumber offnum, RelFileNode* rNode, BlockNumber blkno);

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
    bool isFirstInsert = true;
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
            UHeapTuple uheaptup = tuples[*nthispage];
            Size neededSpace;
            if (isFirstInsert) {
                neededSpace = *usedSpace + uheaptup->disk_tuple_size;
                isFirstInsert = false;
            } else {
                neededSpace = *usedSpace + uheaptup->disk_tuple_size + saveFreeSpace;
            }
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
    bool isFirstInsert;
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
    isFirstInsert = (ufreeOffsetRanges->nranges == 0);

    /*
     * Now, there are no free line pointers. Check whether we can insert
     * another tuple in the page, then we'll insert another range starting
     * from limit to max required offset number. We can decide the actual end
     * offset for this range while inserting tuples in the buffer.
     */
    if ((limit <= CalculatedMaxUHeapTuplesPerPage(UPageGetTDSlotCount(page))) && (nthispage < ntuples)) {
        UHeapTuple uheaptup = tuples[nthispage];
        Size neededSpace;
        if (isFirstInsert) {
            neededSpace = usedSpace + sizeof(ItemIdData) + uheaptup->disk_tuple_size;
            isFirstInsert = false;
        } else {
            neededSpace = usedSpace + sizeof(ItemIdData) + uheaptup->disk_tuple_size + saveFreeSpace;
        }
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

static int RpCompare(const void *rp1, const void *rp2)
{
    return ((RpSort)rp1)->start - ((RpSort)rp2)->start;
}

void FastVerifyUTuple(UHeapDiskTuple diskTup, Buffer buffer)
{
    if (u_sess->attr.attr_storage.ustore_verify_level < (int) USTORE_VERIFY_DEFAULT) {
        return;
    }

    int tdSlot = UHeapTupleHeaderGetTDSlot(diskTup);
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
    if (tdSlot < 0 || tdSlot > tdCount) {
        ereport(PANIC, (errmodule(MOD_USTORE), errmsg(
            "verify utuple invalid! "
            "LogInfo: tdSlot %d, tdcount %u, reserved %u. "
            "TransInfo: oid %u, blockno %u.",
            tdSlot, tdCount, reserved, relId, blockno)));
    }
}

static int getModule(bool isRedo)
{
    return isRedo ? USTORE_VERIFY_MOD_REDO : USTORE_VERIFY_MOD_UPAGE;
}

void UpageVerify(UHeapPageHeader header, XLogRecPtr lastRedo, TupleDesc tupDesc, Relation rel,
    RelFileNode* rNode, BlockNumber blkno, bool isRedo, uint8 mask, OffsetNumber num)
{
    BYPASS_VERIFY(getModule(isRedo), rel);

    if (!isRedo) {
        rNode = rel ? &rel->rd_node : NULL;
    }
    if (rNode == NULL) {
        RelFileNode invalidRelFileNode = {InvalidOid, InvalidOid, InvalidOid};
        rNode = &invalidRelFileNode;
    }
    
    CHECK_VERIFY_LEVEL(USTORE_VERIFY_FAST);
    uint8 curMask = mask & USTORE_VERIFY_UPAGE_MASK;
    if ((curMask & USTORE_VERIFY_UPAGE_HEADER) > 0) {
        UpageVerifyHeader(header, lastRedo, rNode, blkno, isRedo);
    }
    
    if (num != InvalidOffsetNumber) {
        if ((curMask & USTORE_VERIFY_UPAGE_ROW) > 0) {
            RowPtr *rowptr = UPageGetRowPtr(header, num);
            UpageVerifyRowptr(rowptr, (Page)header, num, rNode, blkno);
        }
        if ((curMask & USTORE_VERIFY_UPAGE_TUPLE) > 0) {
            UpageVerifyTuple(header, num, tupDesc, rel, rNode, blkno, isRedo);
        }
    }

    CHECK_VERIFY_LEVEL(USTORE_VERIFY_COMPLETE);
    if ((curMask & USTORE_VERIFY_UPAGE_ROWS) > 0) {
        UpageVerifyAllRowptr(header, rNode, blkno, isRedo);
    }

    if ((curMask & USTORE_VERIFY_UPAGE_TUPLE) > 0) {
        if (num == InvalidOffsetNumber) {
            for (OffsetNumber offNum= FirstOffsetNumber; offNum <= UHeapPageGetMaxOffsetNumber((char *)header); offNum++) {
                UpageVerifyTuple(header, offNum, tupDesc, rel, rNode, blkno, isRedo);
            }
        }
    }
}

void UpageVerifyHeader(UHeapPageHeader header, XLogRecPtr lastRedo, RelFileNode* rNode, BlockNumber blkno, bool isRedo)
{
    if (lastRedo != InvalidXLogRecPtr && PageGetLSN(header) < lastRedo) {
        ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("[UPAGE_VERIFY|HEADER] Current lsn(%X/%X) in page is smaller than last checkpoint(%X/%X)), "
            "rnode[%u,%u,%u], block %u.", (uint32)(PageGetLSN(header) >> HIGH_BITS_LENGTH_OF_LSN),
            (uint32)PageGetLSN(header), (uint32)(lastRedo >> HIGH_BITS_LENGTH_OF_LSN), (uint32)lastRedo,
            rNode->spcNode, rNode->dbNode, rNode->relNode, blkno)));
    }

    if (unlikely(header->pd_lower < (SizeOfUHeapPageHeaderData + SizeOfUHeapTDData(header)) || 
        header->pd_lower > header->pd_upper || header->pd_upper > header->pd_special || 
        header->potential_freespace > BLCKSZ ||  header->pd_special != BLCKSZ)) {
        ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("[UPAGE_VERIFY|HEADER] lower = %u, upper = %u, special = %u, potential = %u,"
            " rnode[%u,%u,%u], block %u.", header->pd_lower, header->pd_upper, header->pd_special,
            header->potential_freespace, rNode->spcNode, rNode->dbNode, rNode->relNode, blkno)));
    }

    if (header->td_count <= 0 || header->td_count > UHEAP_MAX_TD) {
        ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("[UPAGE_VERIFY|HEADER] tdcount invalid: tdcount = %u, rnode[%u,%u,%u], block %u.",
            header->td_count, rNode->spcNode, rNode->dbNode, rNode->relNode, blkno)));
    }
 
    if (TransactionIdFollows(header->pd_prune_xid, t_thrd.xact_cxt.ShmemVariableCache->nextXid)) {
        ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("[UPAGE_VERIFY|HEADER] prune_xid invalid: prune_xid = %lu, nextxid = %lu."
            " rnode[%u,%u,%u], block %u.", header->pd_prune_xid, t_thrd.xact_cxt.ShmemVariableCache->nextXid,
            rNode->spcNode, rNode->dbNode, rNode->relNode, blkno)));
    }
}

static void UpageVerifyTuple(UHeapPageHeader header, OffsetNumber offnum, TupleDesc tupDesc, Relation rel,
    RelFileNode* rNode, BlockNumber blkno, bool isRedo)
{
    RowPtr *rp = NULL;
    UHeapDiskTuple diskTuple = NULL;
    int tdSlot = InvalidTDSlotId;
    bool hasInvalidXact = false;
    TransactionId tupXid = InvalidTransactionId;
    UHeapTupleTransInfo td_info = {InvalidTDSlotId, InvalidTransactionId, InvalidCommandId, INVALID_UNDO_REC_PTR};

    rp = UPageGetRowPtr(header, offnum);
    if (RowPtrIsNormal(rp)) {
        diskTuple = (UHeapDiskTuple)UPageGetRowData(header, rp);
        tdSlot = UHeapTupleHeaderGetTDSlot(diskTuple);
        hasInvalidXact = UHeapTupleHasInvalidXact(diskTuple->flag);
        tupXid = UDiskTupleGetModifiedXid(diskTuple, (Page)header);
 
        td_info.td_slot = tdSlot;
        if ((tdSlot != UHEAPTUP_SLOT_FROZEN)) {
            if (tdSlot < 1 || tdSlot > header->td_count) {
                ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("[UPAGE_VERIFY|TUPLE]tdSlot out of bounds, tdSlot = %d, td_count = %d, "
                    "rnode[%u,%u,%u], block %u, offnum %u.", tdSlot, header->td_count,
                    rNode->spcNode, rNode->dbNode, rNode->relNode, blkno, offnum)));
                return;
            }

            UHeapPageTDData *tdPtr = (UHeapPageTDData *)PageGetTDPointer(header);
            TD *this_trans = &tdPtr->td_info[tdSlot - 1];
            td_info.td_slot = tdSlot;
            td_info.cid = InvalidCommandId;
            td_info.urec_add = this_trans->undo_record_ptr;
            td_info.xid = this_trans->xactid;
        
            TransactionId xid = this_trans->xactid;
            if (TransactionIdFollows(xid, t_thrd.xact_cxt.ShmemVariableCache->nextXid)) {
                ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("[UPAGE_VERIFY|TUPLE]tdxid invalid: tdSlot = %d, tdxid = %lu, nextxid = %lu, "
                    "rnode[%u,%u,%u], block %u, offnum %u.", tdSlot, xid, t_thrd.xact_cxt.ShmemVariableCache->nextXid,
                    rNode->spcNode, rNode->dbNode, rNode->relNode, blkno, offnum)));
            }

            if (TransactionIdIsValid(xid) && !UHeapTransactionIdDidCommit(xid) &&
                TransactionIdPrecedes(xid, g_instance.undo_cxt.globalFrozenXid)) {
                ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("[UPAGE_VERIFY|TUPLE]tdxid %lu in tdslot(%d) is smaller than global frozen xid %lu, "
                    "rnode[%u,%u,%u], block %u, offnum %u.", xid, tdSlot, g_instance.undo_cxt.globalFrozenXid,
                    rNode->spcNode, rNode->dbNode, rNode->relNode, blkno, offnum)));
            }
        }

        if (!hasInvalidXact && IS_VALID_UNDO_REC_PTR(td_info.urec_add) &&
            (!TransactionIdIsValid(td_info.xid) || (TransactionIdIsValid(tupXid) &&
                !TransactionIdEquals(td_info.xid, tupXid)))) {
            ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("[UPAGE_VERIFY|TUPLE] tup xid inconsistency with td: tupxid = %lu, tdxid = %lu, urp %lu, "
                "rnode[%u,%u,%u], block %u, offnum %u.", tupXid, td_info.xid, td_info.urec_add,
                rNode->spcNode, rNode->dbNode, rNode->relNode, blkno, offnum)));
            return;
        }
 
        CHECK_VERIFY_LEVEL(USTORE_VERIFY_COMPLETE)
        int tupSize = (rel == NULL) ? 0 : CalTupSize(rel, diskTuple, tupDesc);
        if (tupSize > (int)RowPtrGetLen(rp) || (diskTuple->reserved != 0 &&
            diskTuple->reserved != 0xFF)) {
            ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("[UPAGE_VERIFY|TUPLE]corrupted tuple: tupsize = %d, rpsize = %u, "
                "rnode[%u,%u,%u], block %u, offnum %u.",
                tupSize, RowPtrGetLen(rp), rNode->spcNode, rNode->dbNode, rNode->relNode, blkno, offnum)));
            return;
        }

        if (!TransactionIdIsValid(tupXid)) {
            return;
        }

        if (hasInvalidXact) {
            if (!UHeapTransactionIdDidCommit(tupXid) && !t_thrd.xlog_cxt.InRecovery) {
                ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("[UPAGE_VERIFY|TUPLE] tup xid not commit, tupxid = %lu, "
                    "rnode[%u,%u,%u], block %u, offnum %u.", tupXid,
                    rNode->spcNode, rNode->dbNode, rNode->relNode, blkno, offnum)));
                return;
            }
            if (TransactionIdEquals(td_info.xid, tupXid)) {
                ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("[UPAGE_VERIFY|TUPLE] td reused but xid equal td: tupxid = %lu, tdxid = %lu, "
                    "rnode[%u,%u,%u], block %u, offnum %u.", tupXid, td_info.xid,
                    rNode->spcNode, rNode->dbNode, rNode->relNode, blkno, offnum)));
                return;
            }
        }
    }
}

static void UpageVerifyRowptr(RowPtr *rowPtr, Page page, OffsetNumber offnum, RelFileNode* rNode, BlockNumber blkno)
{
    UHeapPageHeader phdr = (UHeapPageHeader)page;
    int nline = UHeapPageGetMaxOffsetNumber(page);
    UHeapDiskTuple diskTuple = (UHeapDiskTuple)UPageGetRowData(page, rowPtr);
    uint32 offset = RowPtrGetOffset(rowPtr);
    uint32 len = SHORTALIGN(RowPtrGetLen(rowPtr));
    int tdSlot = UHeapTupleHeaderGetTDSlot(diskTuple);
    bool hasInvalidXact = UHeapTupleHasInvalidXact(diskTuple->flag);
    TransactionId tupXid = UDiskTupleGetModifiedXid(diskTuple, page);
    TransactionId locker = UHeapDiskTupleGetRawXid(diskTuple, page);
    TransactionId topXid =  GetTopTransactionId();
    UHeapPageTDData *tdPtr = (UHeapPageTDData *)PageGetTDPointer(page);

    if (!RowPtrIsNormal(rowPtr)) {
        ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED), 
            errmsg("[UPAGE_VERIFY|ROWPTR] Rowptr is abnormal (flags:%d, offset %d, len %d), "
            "rnode[%u,%u,%u], block %u, offnum %u.", rowPtr->flags, offset, len, rNode->spcNode,
            rNode->dbNode, rNode->relNode, blkno, offnum)));
        return;
    }

    if (tdSlot < 1 || tdSlot > phdr->td_count) {
        ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED), 
            errmsg("[UPAGE_VERIFY|ROWPTR] Invalid tdSlot %d, td count of page is %d, "
            "rnode[%u,%u,%u], block %u, offnum %u.",
            tdSlot, phdr->td_count, rNode->spcNode, rNode->dbNode, rNode->relNode, blkno, offnum)));
        return;
    }

    TD *thistrans = &tdPtr->td_info[tdSlot - 1];
    UndoRecPtr tdUrp = thistrans->undo_record_ptr;
    TransactionId tdXid = thistrans->xactid;
    if (UHEAP_XID_IS_LOCK(diskTuple->flag)) {
        if (!TransactionIdEquals(locker, topXid)) {
            ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED), 
                errmsg("[UPAGE_VERIFY|ROWPTR] locker invalid: locker %lu, topxid %lu, "
                "rnode[%u,%u,%u], block %u, offnum %u.",
                locker, topXid, rNode->spcNode, rNode->dbNode, rNode->relNode, blkno, offnum)));
            return;
        }
    } else if (!IS_VALID_UNDO_REC_PTR(tdUrp) || hasInvalidXact || !TransactionIdEquals(tdXid, locker) ||
        !TransactionIdEquals(tdXid, topXid) || !TransactionIdEquals(tdXid, tupXid)) {
        ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("[UPAGE_VERIFY|ROWPTR] Td xid invalid: tdSlot %d, tdxid %lu, topxid %lu, "
            "tupxid %lu, isInvalidSlot %d, rnode[%u,%u,%u], block %u, offnum %u.",
            tdSlot, tdXid, topXid, tupXid, hasInvalidXact,
            rNode->spcNode, rNode->dbNode, rNode->relNode, blkno, offnum)));
        return;
    }

    CHECK_VERIFY_LEVEL(USTORE_VERIFY_COMPLETE)
    for (OffsetNumber i = FirstOffsetNumber; i <= nline; i++) {
        if (i == offnum) {
            continue;
        }
        RowPtr *rp = UPageGetRowPtr(page, i);
        if (RowPtrIsNormal(rp)) {
            uint32 tupOffset = RowPtrGetOffset(rp);
            uint32 tupLen = SHORTALIGN(RowPtrGetLen(rp));
            if (tupOffset < offset) {
                if (tupOffset + tupLen > offset) {
                    ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),
                        errmsg("[UPAGE_VERIFY|ROWPTR] Rowptr data is abnormal, flags %d, offset %u,"
                        " len %d, alignTupLen %u, targetRpOffset %u, "
                        "rnode[%u,%u,%u], block %u, offnum %u, offnum2 %u.",
                        rp->flags, tupOffset, RowPtrGetLen(rp), tupLen, offset,
                        rNode->spcNode, rNode->dbNode, rNode->relNode, blkno, offnum, i)));
                }
            } else if (offset + len > tupOffset) {
                ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("[UPAGE_VERIFY|ROWPTR] Rowptr data is abnormal, flags %d, offset %u,"
                    " len %d, alignTupLen %u, targetRpOffset %u, targetRpLen %u, "
                    "rnode[%u,%u,%u], block %u, offnum %u, offnum2 %u.",
                    rp->flags, tupOffset, RowPtrGetLen(rp), tupLen, offset, len,
                    rNode->spcNode, rNode->dbNode, rNode->relNode, blkno, offnum, i)));
            }
        }
    }
}

static void UpageVerifyAllRowptr(UHeapPageHeader header, RelFileNode* rNode, BlockNumber blkno, bool isRedo)
{
    int nline = UHeapPageGetMaxOffsetNumber((char *)header);
    int tdSlot = 0;
    int nstorage = 0;
    OffsetNumber i;
    RpSortData rowptrs[MaxPossibleUHeapTuplesPerPage];
    RpSort sortPtr = rowptrs;
    RowPtr *rp = NULL;

    for (i = FirstOffsetNumber; i <= nline; i++) {
        rp = UPageGetRowPtr(header, i);
        if (RowPtrIsNormal(rp)) {
            sortPtr->start = (int)RowPtrGetOffset(rp);
            sortPtr->end = sortPtr->start + (int)SHORTALIGN(RowPtrGetLen(rp));
            sortPtr->offset = i;
            if (sortPtr->start < header->pd_upper || sortPtr->end > header->pd_special) {
                ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("[UPAGE_VERIFY|ALLROWPTR] rpstart = %u, rplen = %u, pdlower = %u, pdupper = %u, "
                    "rnode[%u,%u,%u], block %u, offnum %u.",
                    RowPtrGetOffset(rp), RowPtrGetLen(rp), header->pd_lower, header->pd_upper,
                    rNode->spcNode, rNode->dbNode, rNode->relNode, blkno, i)));
                    return;
            }
            sortPtr++;
        } else if (RowPtrIsDeleted(rp)) {
            tdSlot = RowPtrGetTDSlot(rp);
            if (tdSlot == UHEAPTUP_SLOT_FROZEN) {
                ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("[UPAGE_VERIFY|ALLROWPTR] tdslot frozen, tdSlot = %d, "
                    "rnode[%u,%u,%u], block %u, offnum %u.", tdSlot,
                    rNode->spcNode, rNode->dbNode, rNode->relNode, blkno, i)));
                return;
            }

            if (tdSlot < 1 || tdSlot > header->td_count) {
                ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("[UPAGE_VERIFY|ALLROWPTR] tdSlot out of bounds, tdSlot = %d, "
                    "td_count = %d, rnode[%u,%u,%u], block %u, offnum %u.", tdSlot, header->td_count,
                    rNode->spcNode, rNode->dbNode, rNode->relNode, blkno, i)));
                return;
            }

            UHeapPageTDData *tdPtr = (UHeapPageTDData *)PageGetTDPointer(header);
            TD * this_trans = &tdPtr->td_info[tdSlot - 1];
            if (TransactionIdFollows(this_trans->xactid, t_thrd.xact_cxt.ShmemVariableCache->nextXid)) {
                ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("[UPAGE_VERIFY|ALLROWPTR] tdxid invalid: tdSlot %d, tdxid = %lu, "
                    "nextxid = %lu, rnode[%u,%u,%u], block %u, offnum %u.", tdSlot, this_trans->xactid,
                    t_thrd.xact_cxt.ShmemVariableCache->nextXid,
                    rNode->spcNode, rNode->dbNode, rNode->relNode, blkno, i)));
                return;
            }
        }
    }
 
    nstorage = sortPtr - rowptrs;
 
    CHECK_VERIFY_LEVEL(USTORE_VERIFY_COMPLETE)
    if (nstorage <= 1) {
        return;
    }
 
    qsort((char *)rowptrs, nstorage, sizeof(RpSortData), RpCompare);
 
    for (i = 0; i < nstorage - 1; i++) {
        RpSort temp_ptr1 = &rowptrs[i];
        RpSort temp_ptr2 = &rowptrs[i + 1];
        if (temp_ptr1->end > temp_ptr2->start) {
            ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("[UPAGE_VERIFY|ALLROWPTR]corrupted line pointer: rp1offnum %u, rp1start = %u, rp1end = %u, "
                "rp2offnum = %u, rp2start = %u, rp2end = %u, rnode[%u,%u,%u], block %u.",
                temp_ptr1->offset, temp_ptr1->start, temp_ptr1->end,
                temp_ptr2->offset, temp_ptr2->start, temp_ptr2->end,
                rNode->spcNode, rNode->dbNode, rNode->relNode, blkno)));
            return;
        }
    }
}