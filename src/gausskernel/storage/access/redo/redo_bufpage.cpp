/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * redo_bufpage.cpp
 *    common function about page
 *
 * IDENTIFICATION
 *  src/gausskernel/storage/access/redo/redo_bufpage.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "storage/buf/bufpage.h"
#include "access/itup.h"
#include "access/htup.h"

static bool PageCheckAndFreeDict(Page page);
static int ItemOffCompare(const void *itemIdp1, const void *itemIdp2);

/*
 * PageInit
 *		Initializes the contents of a page.
 */
void PageInit(Page page, Size pageSize, Size specialSize, bool isHeap)
{
    PageHeader p = (PageHeader)page;

    specialSize = MAXALIGN(specialSize);

    Assert(pageSize == BLCKSZ);
    Assert(pageSize > specialSize + SizeOfPageHeaderData);

    /* Make sure all fields of page are zero, as well as unused space */
    errno_t ret = memset_s(p, pageSize, 0, pageSize);
    securec_check(ret, "", "");

    /* p->pd_flags = 0;								done by above MemSet */
    p->pd_upper = pageSize - specialSize;
    p->pd_special = pageSize - specialSize;

    if (isHeap) {
        p->pd_lower = SizeOfHeapPageHeaderData;
        PageSetPageSizeAndVersion(page, pageSize, PG_HEAP_PAGE_LAYOUT_VERSION);
    } else {
        p->pd_lower = SizeOfPageHeaderData;
        PageSetPageSizeAndVersion(page, pageSize, PG_COMM_PAGE_LAYOUT_VERSION);
    }
}

void SegPageInit(Page page, Size pageSize)
{
    PageHeader p = (PageHeader)page;

    Assert(pageSize == BLCKSZ);

    /* Make sure all fields of page are zero, as well as unused space */
    errno_t ret = memset_s(p, pageSize, 0, pageSize);
    securec_check(ret, "", "");

    p->pd_upper = pageSize;
    p->pd_special = pageSize;

    p->pd_lower = SizeOfPageHeaderData;
    PageSetPageSizeAndVersion(page, pageSize, PG_SEGMENT_PAGE_LAYOUT_VERSION);
}

void PageReinitWithDict(Page page, Size dictSize)
{
    HeapPageHeader pd;

    /* re-init this page with dictionary area */
    Assert(PageIsEmpty(page));

    PageSetCompressed(page);
    pd = (HeapPageHeader)page;
    pd->pd_upper = PageGetPageSize(page) - MAXALIGN(dictSize);
    pd->pd_special = pd->pd_upper;
}

static int ItemOffCompare(const void *itemIdp1, const void *itemIdp2)
{
    /* Sort in decreasing itemoff order */
    return ((itemIdSort)itemIdp2)->itemoff - ((itemIdSort)itemIdp1)->itemoff;
}

/*
 * PageIndexMultiDelete
 *
 * This routine handles the case of deleting multiple tuples from an
 * index page at once.	It is considerably faster than a loop around
 * PageIndexTupleDelete ... however, the caller *must* supply the array
 * of item numbers to be deleted in item number order!
 */
void PageIndexMultiDelete(Page page, OffsetNumber *itemnos, int nitems)
{
    PageHeader phdr = (PageHeader)page;
    Offset pdLower = phdr->pd_lower;
    Offset pdUpper = phdr->pd_upper;
    Offset pdSpecial = phdr->pd_special;
    itemIdSortData itemidbase[MaxIndexTuplesPerPage];
    itemIdSort itemidptr = NULL;
    ItemId lp;
    int nline, nused;
    int i;
    Size totallen;
    Offset upper;
    Size size;
    unsigned offset;
    int nextitm;
    OffsetNumber offnum;
    errno_t rc = EOK;

    Assert(nitems < MaxIndexTuplesPerPage);

    /*
     * If there aren't very many items to delete, then retail
     * PageIndexTupleDelete is the best way.  Delete the items in reverse
     * order so we don't have to think about adjusting item numbers for
     * previous deletions.
     *
     * Description: tune the magic number here
     */
    if (nitems <= PAGE_INDEX_CAN_TUPLE_DELETE) {
        while (--nitems >= 0)
            PageIndexTupleDelete(page, itemnos[nitems]);
        return;
    }

    /*
     * As with PageRepairFragmentation, paranoia seems justified.
     */
    Assert(!PageIsCompressed(page));
    if ((unsigned int)(pdLower) < GetPageHeaderSize(page) || pdLower > pdUpper || pdUpper > pdSpecial ||
        pdSpecial > BLCKSZ || (unsigned int)(pdSpecial) != MAXALIGN(pdSpecial))
        ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
                 errmsg("corrupted page pointers: lower = %d, upper = %d, special = %d", pdLower, pdUpper, pdSpecial)));

    /*
     * Scan the item pointer array and build a list of just the ones we are
     * going to keep.  Notice we do not modify the page yet, since we are
     * still validity-checking.
     */
    nline = PageGetMaxOffsetNumber(page);
    itemidptr = itemidbase;
    totallen = 0;
    nused = 0;
    nextitm = 0;
    for (offnum = FirstOffsetNumber; offnum <= nline; offnum = OffsetNumberNext(offnum)) {
        lp = PageGetItemId(page, offnum);
        Assert(ItemIdHasStorage(lp));
        size = ItemIdGetLength(lp);
        offset = ItemIdGetOffset(lp);
        if (offset < (unsigned int)(pdUpper) || (offset + size) > (unsigned int)(pdSpecial) ||
            offset != MAXALIGN(offset))
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                            errmsg("corrupted item pointer: offset = %u, size = %u", offset, (unsigned int)size)));

        if (nextitm < nitems && offnum == itemnos[nextitm]) {
            /* skip item to be deleted */
            nextitm++;
        } else {
            itemidptr->offsetindex = nused; /* where it will go */
            itemidptr->itemoff = offset;
            itemidptr->olditemid = *lp;
            itemidptr->alignedlen = MAXALIGN(size);
            totallen += itemidptr->alignedlen;
            itemidptr++;
            nused++;
        }
    }

    /* this will catch invalid or out-of-order itemnos[] */
    if (nextitm != nitems)
        ereport(ERROR, (errcode(ERRCODE_INVALID_ROW_COUNT_IN_RESULT_OFFSET_CLAUSE),
                        errmsg("incorrect index offsets supplie")));

    if (totallen > (Size)(pdSpecial - pdLower))
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("corrupted item lengths: total %u, available space %d",
                                                                (unsigned int)totallen, pdSpecial - pdLower)));

    /* sort itemIdSortData array into decreasing itemoff order */
    qsort((char *)itemidbase, nused, sizeof(itemIdSortData), ItemOffCompare);

    /* compactify page and install new itemids */
    upper = pdSpecial;

    for (i = 0, itemidptr = itemidbase; i < nused; i++, itemidptr++) {
        lp = PageGetItemId(page, itemidptr->offsetindex + 1);
        upper -= itemidptr->alignedlen;
        rc = memmove_s((char *)page + upper, itemidptr->alignedlen, (char *)page + itemidptr->itemoff,
                       itemidptr->alignedlen);
        securec_check(rc, "", "");
        *lp = itemidptr->olditemid;
        lp->lp_off = upper;
    }

    phdr->pd_lower = SizeOfPageHeaderData + nused * sizeof(ItemIdData);
    phdr->pd_upper = upper;
}

/*
 * PageIndexTupleDelete
 *
 * This routine does the work of removing a tuple from an index page.
 *
 * Unlike heap pages, we compact out the line pointer for the removed tuple.
 */
void PageIndexTupleDelete(Page page, OffsetNumber offnum)
{
    PageHeader phdr = (PageHeader)page;
    char *addr = NULL;
    ItemId tup;
    Size size;
    unsigned offset;
    int nbytes;
    int offidx;
    int nline;
    errno_t rc = EOK;

    /*
     * As with PageRepairFragmentation, paranoia seems justified.
     */
    Assert(!PageIsCompressed(page));
    if (phdr->pd_lower < SizeOfPageHeaderData || phdr->pd_lower > phdr->pd_upper || phdr->pd_upper > phdr->pd_special ||
        phdr->pd_special > BLCKSZ)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                        errmsg("corrupted page pointers: lower = %u, upper = %u, special = %u", phdr->pd_lower,
                               phdr->pd_upper, phdr->pd_special)));

    nline = PageGetMaxOffsetNumber(page);
    if ((int)offnum <= 0 || (int)offnum > nline)
        ereport(ERROR, (errcode(ERRCODE_INVALID_ROW_COUNT_IN_RESULT_OFFSET_CLAUSE),
                        errmsg("invalid index offnum: %u", offnum)));

    /* change offset number to offset index */
    offidx = offnum - 1;

    tup = PageGetItemId(page, offnum);
    Assert(ItemIdHasStorage(tup));
    size = ItemIdGetLength(tup);
    offset = ItemIdGetOffset(tup);
    if (offset < phdr->pd_upper || (offset + size) > phdr->pd_special || offset != (unsigned int)(MAXALIGN(offset)) ||
        size != (unsigned int)(MAXALIGN(size)))
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                        errmsg("corrupted item pointer: offset = %u, size = %u", offset, (unsigned int)size)));

    /*
     * First, we want to get rid of the pd_linp entry for the index tuple. We
     * copy all subsequent linp's back one slot in the array. We don't use
     * PageGetItemId, because we are manipulating the _array_, not individual
     * linp's.
     */
    nbytes = phdr->pd_lower - ((char *)&phdr->pd_linp[offidx + 1] - (char *)phdr);

    if (nbytes > 0) {
        rc = memmove_s((char *)&(phdr->pd_linp[offidx]), nbytes, (char *)&(phdr->pd_linp[offidx + 1]), nbytes);
        securec_check(rc, "", "");
    }

    /*
     * Now move everything between the old upper bound (beginning of tuple
     * space) and the beginning of the deleted tuple forward, so that space in
     * the middle of the page is left free.  If we've just deleted the tuple
     * at the beginning of tuple space, then there's no need to do the copy
     * (and bcopy on some architectures SEGV's if asked to move zero bytes).
     */
    /* beginning of tuple space */
    addr = (char *)page + phdr->pd_upper;

    if (offset > phdr->pd_upper) {
        rc = memmove_s(addr + size, (int)(offset - phdr->pd_upper), addr, (int)(offset - phdr->pd_upper));
        securec_check(rc, "", "");
    }

    /* adjust free space boundary pointers */
    phdr->pd_upper += size;
    phdr->pd_lower -= sizeof(ItemIdData);

    /*
     * Finally, we need to adjust the linp entries that remain.
     *
     * Anything that used to be before the deleted tuple's data was moved
     * forward by the size of the deleted tuple.
     */
    if (!PageIsEmpty(page)) {
        int i;

        nline--; /* there's one less than when we started */
        for (i = 1; i <= nline; i++) {
            ItemId ii = PageGetItemId(phdr, i);

            Assert(ItemIdHasStorage(ii));
            if (ItemIdGetOffset(ii) <= offset)
                ii->lp_off += size;
        }
    }
}

/*
 * PageRestoreTempPage
 *		Copy temporary page back to permanent page after special processing
 *		and release the temporary page.
 */
void PageRestoreTempPage(Page tempPage, Page oldPage)
{
    Size pageSize;
    errno_t rc = EOK;

    pageSize = PageGetPageSize(tempPage);
    rc = memcpy_s((char *)oldPage, pageSize, (char *)tempPage, pageSize);
    securec_check(rc, "", "");

    pfree(tempPage);
}

void DumpPageInfo(Page page, XLogRecPtr newLsn)
{
    PageHeader phdr = (PageHeader)page;
    OffsetNumber nline = PageGetMaxOffsetNumber(page);

    ereport(DEBUG4, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                     errmsg("DumpPageInfo: lsn:%X/%X, pd_checksum:%u, flags:%u, lower:%u, upper:%u, special:%u"
                            "pagesize_version:%u, prune_xid:%u, nline:%u, newLsn:%X/%X",
                            phdr->pd_lsn.xlogid, phdr->pd_lsn.xrecoff, phdr->pd_checksum, phdr->pd_flags,
                            phdr->pd_lower, phdr->pd_upper, phdr->pd_special, phdr->pd_pagesize_version,
                            phdr->pd_prune_xid, nline, (uint32)(newLsn >> 32), (uint32)newLsn)));
}

/*
 * PageGetHeapFreeSpace
 *		Returns the size of the free (allocatable) space on a page,
 *		reduced by the space needed for a new line pointer.
 *
 * The difference between this and PageGetFreeSpace is that this will return
 * zero if there are already MaxHeapTuplesPerPage line pointers in the page
 * and none are free.  We use this to enforce that no more than
 * MaxHeapTuplesPerPage line pointers are created on a heap page.  (Although
 * no more tuples than that could fit anyway, in the presence of redirected
 * or dead line pointers it'd be possible to have too many line pointers.
 * To avoid breaking code that assumes MaxHeapTuplesPerPage is a hard limit
 * on the number of line pointers, we make this extra check.)
 */
Size PageGetHeapFreeSpace(Page page)
{
    Size space;

    space = PageGetFreeSpace(page);
    if (space > 0) {
        OffsetNumber offnum, nline;

        /*
         * Are there already MaxHeapTuplesPerPage line pointers in the page?
         */
        nline = PageGetMaxOffsetNumber(page);
        if (nline >= MaxHeapTuplesPerPage) {
            if (PageHasFreeLinePointers((PageHeader)page)) {
                /*
                 * Since this is just a hint, we must confirm that there is
                 * indeed a free line pointer
                 */
                for (offnum = FirstOffsetNumber; offnum <= nline; offnum = OffsetNumberNext(offnum)) {
                    ItemId lp = PageGetItemId(page, offnum);
                    if (!ItemIdIsUsed(lp))
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
 * PageGetFreeSpace
 *		Returns the size of the free (allocatable) space on a page,
 *		reduced by the space needed for a new line pointer.
 *
 * Note: this should usually only be used on index pages.  Use
 * PageGetHeapFreeSpace on heap pages.
 */
Size PageGetFreeSpace(Page page)
{
    int space;

    /*
     * Use signed arithmetic here so that we behave sensibly if pd_lower >
     * pd_upper.
     */
    space = (int)((PageHeader)page)->pd_upper - (int)((PageHeader)page)->pd_lower;

    if (space < (int)sizeof(ItemIdData))
        return 0;
    space -= sizeof(ItemIdData);

    return (Size)space;
}

/*
 * PageRepairFragmentation
 *
 * Frees fragmented space on a page.
 * It doesn't remove unused line pointers! Please don't change this.
 *
 * This routine is usable for heap pages only, but see PageIndexMultiDelete.
 *
 * As a side effect, the page's PD_HAS_FREE_LINES hint bit is updated.
 */
void PageRepairFragmentation(Page page)
{
    Offset pdLower = ((PageHeader)page)->pd_lower;
    Offset pdUpper = ((PageHeader)page)->pd_upper;
    Offset pdSpecial = ((PageHeader)page)->pd_special;
    ItemId lp;
    int nline, nstorage, nunused;
    int i;
    Size totallen;
    Offset upper;
    errno_t rc = EOK;

    /*
     * It's worth the trouble to be more paranoid here than in most places,
     * because we are about to reshuffle data in (what is usually) a shared
     * disk buffer.  If we aren't careful then corrupted pointers, lengths,
     * etc could cause us to clobber adjacent disk buffers, spreading the data
     * loss further.  So, check everything.
     */
    if ((unsigned int)(pdLower) < SizeOfPageHeaderData || pdLower > pdUpper || pdUpper > pdSpecial ||
        pdSpecial > BLCKSZ || (unsigned int)(pdSpecial) != MAXALIGN(pdSpecial))
        ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
                 errmsg("corrupted page pointers: lower = %d, upper = %d, special = %d", pdLower, pdUpper, pdSpecial)));

    nline = PageGetMaxOffsetNumber(page);
    nunused = nstorage = 0;
    for (i = FirstOffsetNumber; i <= nline; i++) {
        lp = PageGetItemId(page, i);
        if (ItemIdIsUsed(lp)) {
            if (ItemIdHasStorage(lp))
                nstorage++;
        } else {
            /* Unused entries should have lp_len = 0, but make sure */
            ItemIdSetUnused(lp);
            nunused++;
        }
    }

    if (nstorage == 0) {
        /* Page is completely empty, so just reset it quickly */
        ((PageHeader)page)->pd_upper = pdSpecial;
    } else {
        /* Need to compact the page the hard way */
        itemIdSortData itemidbase[MaxHeapTuplesPerPage];
        itemIdSort itemidptr = itemidbase;

        totallen = 0;
        for (i = 0; i < nline; i++) {
            lp = PageGetItemId(page, i + 1);
            if (ItemIdHasStorage(lp)) {
                itemidptr->offsetindex = i;
                itemidptr->itemoff = ItemIdGetOffset(lp);
                if (itemidptr->itemoff < (int)pdUpper || itemidptr->itemoff >= (int)pdSpecial)
                    ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                                    errmsg("corrupted item pointer: %d", itemidptr->itemoff)));
                itemidptr->alignedlen = MAXALIGN(ItemIdGetLength(lp));
                totallen += itemidptr->alignedlen;
                itemidptr++;
            }
        }

        if (totallen > (Size)(pdSpecial - pdLower))
            ereport(ERROR,
                    (errcode(ERRCODE_DATA_CORRUPTED), errmsg("corrupted item lengths: total %u, available space %d",
                                                             (unsigned int)totallen, pdSpecial - pdLower)));

        /* sort itemIdSortData array into decreasing itemoff order */
        qsort((char *)itemidbase, nstorage, sizeof(itemIdSortData), ItemOffCompare);

        /* compactify page */
        upper = pdSpecial;

        for (i = 0, itemidptr = itemidbase; i < nstorage; i++, itemidptr++) {
            lp = PageGetItemId(page, itemidptr->offsetindex + 1);
            upper -= itemidptr->alignedlen;
            rc = memmove_s((char *)page + upper, itemidptr->alignedlen, (char *)page + itemidptr->itemoff,
                           itemidptr->alignedlen);
            securec_check(rc, "", "");
            lp->lp_off = upper;
        }

        ((PageHeader)page)->pd_upper = upper;
    }

    /* Set hint bit for PageAddItem */
    if (nunused > 0)
        PageSetHasFreeLinePointers(page);
    else
        PageClearHasFreeLinePointers(page);
}

static bool PageCheckAndFreeDict(Page page)
{
    PageHeader phdr = (PageHeader)page;
    HeapTupleHeader htup;
    ItemId lp;
    int nline, i;
    Offset pdUpper;
    Size pageSize;
    Size tupsSize;
    Size dictsize;
    uint16 headersize;
    errno_t rc = EOK;

    headersize = GetPageHeaderSize(page);
    if (phdr->pd_lower < headersize || phdr->pd_lower > phdr->pd_upper || phdr->pd_upper > phdr->pd_special ||
        phdr->pd_special > BLCKSZ) {
        ereport(PANIC, (errcode(ERRCODE_DATA_CORRUPTED),
                        errmsg("corrupted page pointers: lower = %u, upper = %u, special = %u", phdr->pd_lower,
                               phdr->pd_upper, phdr->pd_special)));
    }

    Assert(PageIsCompressed(page) && PageIsAllVisible(page));
    nline = PageGetMaxOffsetNumber(page);
    for (i = FirstOffsetNumber; i <= nline; i++) {
        lp = PageGetItemId(page, i);
        if (ItemIdIsUsed(lp) && ItemIdHasStorage(lp)) {
            htup = (HeapTupleHeader)PageGetItem(page, lp);
            if (HEAP_TUPLE_IS_COMPRESSED(htup)) {
                return false;
            }
        }
    }

    pageSize = PageGetPageSize(page);
    Assert(pageSize == BLCKSZ);
    dictsize = (Size)PageGetSpecialSize(page);
    pdUpper = phdr->pd_upper + dictsize;
    tupsSize = pageSize - dictsize - phdr->pd_upper;
    if (tupsSize > 0) {
        rc = memmove_s((char *)page + pdUpper, tupsSize, (char *)page + phdr->pd_upper, tupsSize);
        securec_check(rc, "", "");
    }

    for (i = FirstOffsetNumber; i <= nline; i++) {
        lp = PageGetItemId(phdr, i);
        if (!ItemIdIsUsed(lp) || !ItemIdHasStorage(lp))
            continue;

        Assert(ItemIdIsNormal(lp));
        ItemIdSetNormal(lp, (ItemIdGetOffset(lp) + dictsize), ItemIdGetLength(lp));
    }

    phdr->pd_upper = pdUpper;
    phdr->pd_special = pageSize;
    PageClearCompressed(page);
    return true;
}

bool PageFreeDict(Page page)
{
    Assert(PageIsCompressed(page));

    /* case 1: free page dictionary directly if page is empty. */
    if (PageIsEmpty(page)) {
        PageHeader pd;
        pd = (PageHeader)page;
        pd->pd_upper = pd->pd_special;
        PageClearCompressed(page);
        return true;
    }

    /* case 2: check all tuples in page, and free page dictionary if there
     * are no compressed tuples. */
    if (PageIsAllVisible(page)) {
        return PageCheckAndFreeDict(page);
    }

    return false;
}

/*
 * PageGetTempPageCopySpecial
 *		Get a temporary page in local memory for special processing.
 *		The page is PageInit'd with the same special-space size as the
 *		given page, and the special space is copied from the given page.
 */
Page PageGetTempPageCopySpecial(Page page)
{
    Size pageSize;
    Page temp;
    errno_t rc = EOK;

    pageSize = PageGetPageSize(page);
    temp = (Page)palloc(pageSize);

    Assert(!PageIsCompressed(page));

    PageInit(temp, pageSize, PageGetSpecialSize(page));
    rc = memcpy_s(PageGetSpecialPointer(temp), PageGetSpecialSize(page), PageGetSpecialPointer(page),
                  PageGetSpecialSize(page));
    securec_check(rc, "", "");

    return temp;
}

/*
 *	PageAddItem
 *
 *	Add an item to a page.	Return value is offset at which it was
 *	inserted, or InvalidOffsetNumber if there's not room to insert.
 *
 *	If overwrite is true, we just store the item at the specified
 *	offsetNumber (which must be either a currently-unused item pointer,
 *	or one past the last existing item).  Otherwise,
 *	if offsetNumber is valid and <= current max offset in the page,
 *	insert item into the array at that position by shuffling ItemId's
 *	down to make room.
 *	If offsetNumber is not valid, then assign one by finding the first
 *	one that is both unused and deallocated.
 *
 *	If is_heap is true, we enforce that there can't be more than
 *	MaxHeapTuplesPerPage line pointers on the page.
 *
 *	!!! EREPORT(ERROR) IS DISALLOWED HERE !!!
 */
OffsetNumber PageAddItem(Page page, Item item, Size size, OffsetNumber offsetNumber, bool overwrite, bool is_heap)
{
    PageHeader phdr = (PageHeader)page;
    Size alignedSize;
    int lower;
    int upper;
    ItemId itemId;
    OffsetNumber limit;
    bool needshuffle = false;
    uint16 headersize;
    errno_t rc = EOK;

    /*
     * Be wary about corrupted page pointers
     */
    headersize = GetPageHeaderSize(page);
    if (phdr->pd_lower < headersize || phdr->pd_lower > phdr->pd_upper || phdr->pd_upper > phdr->pd_special ||
        phdr->pd_special > BLCKSZ)
        ereport(PANIC, (errcode(ERRCODE_DATA_CORRUPTED),
                        errmsg("corrupted page pointers: lower = %u, upper = %u, special = %u", phdr->pd_lower,
                               phdr->pd_upper, phdr->pd_special)));

    /*
     * Select offsetNumber to place the new item at
     */
    limit = OffsetNumberNext(PageGetMaxOffsetNumber(page));

    /* was offsetNumber passed in? */
    if (OffsetNumberIsValid(offsetNumber)) {
        /* yes, check it */
        if (overwrite) {
            if (offsetNumber < limit) {
                itemId = PageGetItemId(phdr, offsetNumber);
                if (ItemIdIsUsed(itemId) || ItemIdHasStorage(itemId)) {
                    ereport(WARNING, (errmsg("will not overwrite a used ItemId")));
                    return InvalidOffsetNumber;
                }
            }
        } else {
            if (offsetNumber < limit)
                needshuffle = true; /* need to move existing linp's */
        }
    } else {
        /* offsetNumber was not passed in, so find a free slot */
        /* if no free slot, we'll put it at limit (1st open slot) */
        if (PageHasFreeLinePointers(phdr)) {
            /*
             * Look for "recyclable" (unused) ItemId.  We check for no storage
             * as well, just to be paranoid --- unused items should never have
             * storage.
             */
            for (offsetNumber = 1; offsetNumber < limit; offsetNumber++) {
                itemId = PageGetItemId(phdr, offsetNumber);
                if (!ItemIdIsUsed(itemId) && !ItemIdHasStorage(itemId))
                    break;
            }
            if (offsetNumber >= limit) {
                /* the hint is wrong, so reset it */
                PageClearHasFreeLinePointers(phdr);
            }
        } else {
            /* don't bother searching if hint says there's no free slot */
            offsetNumber = limit;
        }
    }

    if (offsetNumber > limit) {
        ereport(WARNING, (errmsg("specified item offset is too large")));
        return InvalidOffsetNumber;
        return InvalidOffsetNumber;
    }

    if (is_heap && offsetNumber > MaxHeapTuplesPerPage) {
        ereport(WARNING, (errmsg("can't put more than MaxHeapTuplesPerPage items in a heap page")));
        return InvalidOffsetNumber;
    }

    /*
     * Compute new lower and upper pointers for page, see if it'll fit.
     *
     * Note: do arithmetic as signed ints, to avoid mistakes if, say,
     * alignedSize > pd_upper.
     */
    if (offsetNumber == limit || needshuffle)
        lower = phdr->pd_lower + sizeof(ItemIdData);
    else
        lower = phdr->pd_lower;

    alignedSize = MAXALIGN(size);

    upper = (int)phdr->pd_upper - (int)alignedSize;

    if (lower > upper)
        return InvalidOffsetNumber;

    /*
     * OK to insert the item.  First, shuffle the existing pointers if needed.
     */
    itemId = PageGetItemId(phdr, offsetNumber);

    if (needshuffle) {
        rc = memmove_s(itemId + 1, (limit - offsetNumber) * sizeof(ItemIdData), itemId,
                       (limit - offsetNumber) * sizeof(ItemIdData));
        securec_check(rc, "", "");
    }

    /* set the item pointer */
    ItemIdSetNormal(itemId, upper, size);

    /* copy the item's data onto the page */
    rc = memcpy_s((char *)page + upper, alignedSize, item, size);
    securec_check(rc, "", "");

    /* adjust page header */
    phdr->pd_lower = (LocationIndex)lower;
    phdr->pd_upper = (LocationIndex)upper;

    return offsetNumber;
}
