/* -------------------------------------------------------------------------
 *
 * bufpage.h
 *	  Standard openGauss buffer page definitions.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/buf/bufpage.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef BUFPAGE_H
#define BUFPAGE_H

#include "access/xlogdefs.h"
#include "storage/buf/block.h"
#include "storage/item/item.h"
#include "storage/off.h"
#include "storage/buf/buf.h"
#include "tde_key_management/data_common.h"

/*
 * A openGauss disk page is an abstraction layered on top of a openGauss
 * disk block (which is simply a unit of i/o, see block.h).
 *
 * specifically, while a disk block can be unformatted, a openGauss
 * disk page is always a slotted page of the form:
 *
 * +----------------+---------------------------------+
 * | PageHeaderData | linp1 linp2 linp3 ...			  |
 * +-----------+----+---------------------------------+
 * | ... linpN |									  |
 * +-----------+--------------------------------------+
 * |		   ^ pd_lower							  |
 * |												  |
 * |			 v pd_upper							  |
 * +-------------+------------------------------------+
 * |			 | tupleN ...						  |
 * +-------------+------------------+-----------------+
 * |	   ... tuple3 tuple2 tuple1 | "special space" |
 * +--------------------------------+-----------------+
 *									^ pd_special
 *
 * a page is full when nothing can be added between pd_lower and
 * pd_upper.
 *
 * all blocks written out by an access method must be disk pages.
 *
 * EXCEPTIONS:
 *
 * obviously, a page is not formatted before it is initialized by
 * a call to PageInit.
 *
 * NOTES:
 *
 * linp1..N form an ItemId array.  ItemPointers point into this array
 * rather than pointing directly to a tuple.  Note that OffsetNumbers
 * conventionally start at 1, not 0.
 *
 * tuple1..N are added "backwards" on the page.  because a tuple's
 * ItemPointer points to its ItemId entry rather than its actual
 * byte-offset position, tuples can be physically shuffled on a page
 * whenever the need arises.
 *
 * AM-generic per-page information is kept in PageHeaderData.
 *
 * AM-specific per-page data (if any) is kept in the area marked "special
 * space"; each AM has an "opaque" structure defined somewhere that is
 * stored as the page trailer.	an access method should always
 * initialize its pages with PageInit and then set its own opaque
 * fields.
 */

typedef Pointer Page;

/*
 * location (byte offset) within a page.
 *
 * note that this is actually limited to 2^15 because we have limited
 * ItemIdData.lp_off and ItemIdData.lp_len to 15 bits (see itemid.h).
 */
typedef uint16 LocationIndex;

/*
 * For historical reasons, the 64-bit LSN value is stored as two 32-bit
 * values.
 */
typedef struct {
    uint32 xlogid;  /* high bits */
    uint32 xrecoff; /* low bits */
} PageXLogRecPtr;

/*
 * disk page organization
 *
 * space management information generic to any page
 *
 *		pd_lsn		- identifies xlog record for last change to this page.
 *		pd_checksum	- page checksum, if set.
 *		pd_flags	- flag bits.
 *		pd_lower	- offset to start of free space.
 *		pd_upper	- offset to end of free space.
 *		pd_special	- offset to start of special space.
 *		pd_pagesize_version - size in bytes and page layout version number.
 *		pd_prune_xid - oldest XID among potentially prunable tuples on page.
 *
 * The LSN is used by the buffer manager to enforce the basic rule of WAL:
 * "thou shalt write xlog before data".  A dirty buffer cannot be dumped
 * to disk until xlog has been flushed at least as far as the page's LSN.
 *
 * pd_checksum stores the page checksum, if it has been set for this page;
 * zero is a valid value for a checksum. If a checksum is not in use then
 * we leave the field unset. This will typically mean the field is zero
 * though non-zero values may also be present if databases have been
 * pg_upgraded from releases prior to 9.3, when the same byte offset was
 * used to store the current timelineid when the page was last updated.
 * Note that there is no indication on a page as to whether the checksum
 * is valid or not, a deliberate design choice which avoids the problem
 * of relying on the page contents to decide whether to verify it. Hence
 * there are no flag bits relating to checksums.
 *
 * pd_prune_xid is a hint field that helps determine whether pruning will be
 * useful.  It is currently unused in index pages.
 *
 * The page version number and page size are packed together into a single
 * uint16 field.  This is for historical reasons: before PostgreSQL 7.3,
 * there was no concept of a page version number, and doing it this way
 * lets us pretend that pre-7.3 databases have page version number zero.
 * We constrain page sizes to be multiples of 256, leaving the low eight
 * bits available for a version number.
 *
 * Minimum possible page size is perhaps 64B to fit page header, opaque space
 * and a minimal tuple; of course, in reality you want it much bigger, so
 * the constraint on pagesize mod 256 is not an important restriction.
 * On the high end, we can only support pages up to 32KB because lp_off/lp_len
 * are 15 bits.
 */
typedef struct {
    /* XXX LSN is member of *any* block, not only page-organized ones */
    PageXLogRecPtr pd_lsn;    /* LSN: next byte after last byte of xlog
                               * record for last change to this page */
    uint16 pd_checksum;       /* checksum */
    uint16 pd_flags;          /* flag bits, see below */
    LocationIndex pd_lower;   /* offset to start of free space */
    LocationIndex pd_upper;   /* offset to end of free space */
    LocationIndex pd_special; /* offset to start of special space */
    uint16 pd_pagesize_version;
    ShortTransactionId pd_prune_xid;           /* oldest prunable XID, or zero if none */
    ItemIdData pd_linp[FLEXIBLE_ARRAY_MEMBER]; /* beginning of line pointer array */
} PageHeaderData;

typedef PageHeaderData* PageHeader;

/*
 * HeapPageHeaderData -- data that stored at the begin of each new version heap page.
 *		pd_xid_base - base value for transaction IDs on page
 *		pd_multi_base - base value for multixact IDs on page
 *
 */
typedef struct {
    /* XXX LSN is member of *any* block, not only page-organized ones */
    PageXLogRecPtr pd_lsn;    /* LSN: next byte after last byte of xlog
                               * record for last change to this page */
    uint16 pd_checksum;       /* checksum */
    uint16 pd_flags;          /* flag bits, see below */
    LocationIndex pd_lower;   /* offset to start of free space */
    LocationIndex pd_upper;   /* offset to end of free space */
    LocationIndex pd_special; /* offset to start of special space */
    uint16 pd_pagesize_version;
    ShortTransactionId pd_prune_xid;           /* oldest prunable XID, or zero if none */
    TransactionId pd_xid_base;                 /* base value for transaction IDs on page */
    TransactionId pd_multi_base;               /* base value for multixact IDs on page */
    ItemIdData pd_linp[FLEXIBLE_ARRAY_MEMBER]; /* beginning of line pointer array */
} HeapPageHeaderData;

typedef HeapPageHeaderData* HeapPageHeader;

#define GetPageHeaderSize(page) (PageIs8BXidHeapVersion(page) ? SizeOfHeapPageHeaderData : SizeOfPageHeaderData)

#define SizeOfHeapPageUpgradeData MAXALIGN(offsetof(HeapPageHeaderData, pd_linp) - offsetof(PageHeaderData, pd_linp))
    
#define GET_ITEMID_BY_IDX(buf, i) ((ItemIdData *)(buf + GetPageHeaderSize(buf) + (i) * sizeof(ItemIdData)))

#define PageXLogRecPtrGet(val) \
	((uint64) (val).xlogid << 32 | (val).xrecoff)
#define PageXLogRecPtrSet(ptr, lsn) \
	((ptr).xlogid = (uint32) ((lsn) >> 32), (ptr).xrecoff = (uint32) (lsn))

/*
 * pd_flags contains the following flag bits.  Undefined bits are initialized
 * to zero and may be used in the future.
 *
 * PD_HAS_FREE_LINES is set if there are any LP_UNUSED line pointers before
 * pd_lower.  This should be considered a hint rather than the truth, since
 * changes to it are not WAL-logged.
 *
 * PD_PAGE_FULL is set if an UPDATE doesn't find enough free space in the
 * page for its new tuple version; this suggests that a prune is needed.
 * Again, this is just a hint.
 */
#define PD_HAS_FREE_LINES 0x0001  /* are there any unused line pointers? */
#define PD_PAGE_FULL 0x0002       /* not enough free space for new tuple? */
#define PD_ALL_VISIBLE 0x0004     /* all tuples on page are visible to everyone */
#define PD_COMPRESSED_PAGE 0x0008 /* compressed page flag */
#define PD_LOGICAL_PAGE 0x0010    /* logical page flag used by bulkload or copy */
#define PD_ENCRYPT_PAGE 0x0020    /* is a encryt cluster */
#define PD_CHECKSUM_FNV1A 0x0040  /* page checksum using FNV-1a hash */
#define PD_JUST_AFTER_FPW 0x0080  /* page just after redo full page write */
#define PD_TDE_PAGE 0x0100        /* there is TdePageInfo at the end of a page */

#define PD_VALID_FLAG_BITS 0x01FF /* OR of all valid pd_flags bits */

/*
 * Page layout version number 0 is for pre-7.3 Postgres releases.
 * Releases 7.3 and 7.4 use 1, denoting a new HeapTupleHeader layout.
 * Release 8.0 uses 2; it changed the HeapTupleHeader layout again.
 * Release 8.1 uses 3; it redefined HeapTupleHeader infomask bits.
 * Release 8.3 uses 4; it changed the HeapTupleHeader layout again, and
 *		added the pd_flags field (by stealing some bits from pd_tli),
 *		as well as adding the pd_prune_xid field (which enlarges the header).
 *
 * As of Release 9.3, the checksum version must also be considered when
 * handling pages.
 */
#define PG_SEGMENT_PAGE_LAYOUT_VERSION 8
#define PG_UHEAP_PAGE_LAYOUT_VERSION 7
#define PG_HEAP_PAGE_LAYOUT_VERSION 6
#define PG_COMM_PAGE_LAYOUT_VERSION 5

/* ----------------------------------------------------------------
 *						page support macros
 * ----------------------------------------------------------------
 */

/*
 * PageIsValid
 *		True iff page is valid.
 */
#define PageIsValid(page) PointerIsValid(page)

/*
 * line pointer(s) do not count as part of header
 */
#define SizeOfPageHeaderData (offsetof(PageHeaderData, pd_linp))
#define SizeOfHeapPageHeaderData (offsetof(HeapPageHeaderData, pd_linp))

/*
 * PageIsEmpty
 *		returns true iff no itemid has been allocated on the page
 */
#define PageIsEmpty(page) (((PageHeader)(page))->pd_lower <= GetPageHeaderSize(page))

/*
 * PageIsNew
 *		returns true iff page has not been initialized (by PageInit)
 */
#define PageIsNew(page) (((PageHeader)(page))->pd_upper == 0)

#define PageUpperIsInitNew(page) (((PageHeader)(page))->pd_upper == ((PageHeader)(page))->pd_special)

/*
 * PageGetItemId
 *		Returns an item identifier of a page.
 */
#define PageGetItemId(page, offsetNumber)                                                            \
    (PageIs8BXidHeapVersion(page) ? ((ItemId)(&((HeapPageHeader)(page))->pd_linp[(offsetNumber)-1])) \
                                  : ((ItemId)(&((PageHeader)(page))->pd_linp[(offsetNumber)-1])))

#define HeapPageGetItemId(page, offsetNumber) ((ItemId)(&((HeapPageHeader)(page))->pd_linp[(offsetNumber)-1])) \

/*
 * PageGetContents
 *		To be used in case the page does not contain item pointers.
 *
 * Note: prior to 8.3 this was not guaranteed to yield a MAXALIGN'd result.
 * Now it is.  Beware of old code that might think the offset to the contents
 * is just SizeOfPageHeaderData rather than MAXALIGN(SizeOfPageHeaderData).
 */
#define PageGetContents(page) ((char*)(page) + MAXALIGN(GetPageHeaderSize(page)))

/* ----------------
 *		macros to access page size info
 * ----------------
 */

/*
 * PageSizeIsValid
 *		True iff the page size is valid.
 */
#define PageSizeIsValid(pageSize) ((pageSize) == BLCKSZ)

/*
 * PageGetPageSize
 *		Returns the page size of a page.
 *
 * this can only be called on a formatted page (unlike
 * BufferGetPageSize, which can be called on an unformatted page).
 * however, it can be called on a page that is not stored in a buffer.
 */
#define PageGetPageSize(page) ((Size)(((PageHeader)(page))->pd_pagesize_version & (uint16)0xFF00))

/*
 * PageGetPageLayoutVersion
 *		Returns the page layout version of a page.
 */
#define PageGetPageLayoutVersion(page) (((PageHeader)(page))->pd_pagesize_version & 0x00FF)

#define PageIs8BXidHeapVersion(page) (PageGetPageLayoutVersion(page) == PG_HEAP_PAGE_LAYOUT_VERSION)

#define PageIsSegmentVersion(page) (PageGetPageLayoutVersion(page) == PG_SEGMENT_PAGE_LAYOUT_VERSION)

/*
 * PageSetPageSizeAndVersion
 *		Sets the page size and page layout version number of a page.
 *
 * We could support setting these two values separately, but there's
 * no real need for it at the moment.
 */
#define PageSetPageSizeAndVersion(page, size, version) \
    (AssertMacro(((size)&0xFF00) == (size)),           \
        AssertMacro(((version)&0x00FF) == (version)),  \
        ((PageHeader)(page))->pd_pagesize_version = (size) | (version))

/* ----------------
 *		page special data macros
 * ----------------
 */
/*
 * PageGetSpecialSize
 *		Returns size of special space on a page.
 */
#define PageGetSpecialSize(page) ((uint16)(PageGetPageSize(page) - ((PageHeader)(page))->pd_special))

/*
 * PageGetSpecialPointer
 *		Returns pointer to special space on a page.
 */
#define PageGetSpecialPointer(page) \
    (AssertMacro(PageIsValid(page)), (char*)((char*)(page) + ((PageHeader)(page))->pd_special))

#define BTPageGetSpecial(page)                                                                             \
(\
    AssertMacro(((PageHeader)page)->pd_special == BLCKSZ - MAXALIGN(sizeof(BTPageOpaqueData))), \
    (BTPageOpaque)((Pointer)page + BLCKSZ - MAXALIGN(sizeof(BTPageOpaqueData)))  \
)

#define HeapPageSetPruneXid(page, xid)                                \
    (((PageHeader)(page))->pd_prune_xid = NormalTransactionIdToShort( \
         ((HeapPageHeader)(page))->pd_xid_base, (xid)))

#define PageSetPruneXid(page, xid)                                \
    (((PageHeader)(page))->pd_prune_xid = NormalTransactionIdToShort( \
         ((UBTPageOpaqueInternal)PageGetSpecialPointer(page))->xid_base, (xid)))

#define HeapPageGetPruneXid(page) \
    (ShortTransactionIdToNormal(  \
        ((HeapPageHeader)(page))->pd_xid_base, ((PageHeader)(page))->pd_prune_xid))

#define PageGetPruneXid(page) \
    (ShortTransactionIdToNormal(  \
       ((UBTPageOpaqueInternal)PageGetSpecialPointer(page))->xid_base, ((PageHeader)(page))->pd_prune_xid))

/*
 * PageGetItem
 *		Retrieves an item on the given page.
 *
 * Note:
 *		This does not change the status of any of the resources passed.
 *		The semantics may change in the future.
 */
#define PageGetItem(page, itemId)              \
    (AssertMacro(PageIsValid(page)),           \
        AssertMacro(ItemIdHasStorage(itemId)), \
        (Item)(((char*)(page)) + ItemIdGetOffset(itemId)))

/*
 * PageGetMaxOffsetNumber
 *		Returns the maximum offset number used by the given page.
 *		Since offset numbers are 1-based, this is also the number
 *		of items on the page.
 *
 *		NOTE: if the page is not initialized (pd_lower == 0), we must
 *		return zero to ensure sane behavior.  Accept double evaluation
 *		of the argument so that we can ensure this.
 */
inline OffsetNumber PageGetMaxOffsetNumber(char* pghr)
{
    OffsetNumber maxoff = InvalidOffsetNumber;
    Size pageheadersize = GetPageHeaderSize(pghr);

    if (((PageHeader)pghr)->pd_lower <= pageheadersize)
        maxoff = 0;
    else
        maxoff = (((PageHeader)pghr)->pd_lower - pageheadersize) / sizeof(ItemIdData);

    return maxoff;
}

/*
 * Additional macros for access to page headers
 */
#define PageGetLSN(page) (((uint64)((PageHeader)(page))->pd_lsn.xlogid << 32) | ((PageHeader)(page))->pd_lsn.xrecoff)
#define PageSetLSNInternal(page, lsn) \
    (((PageHeader)(page))->pd_lsn.xlogid = (uint32)((lsn) >> 32), ((PageHeader)(page))->pd_lsn.xrecoff = (uint32)(lsn))

#ifndef FRONTEND
inline void PageSetLSN(Page page, XLogRecPtr LSN, bool check = true)
{
    if (check && XLByteLT(LSN, PageGetLSN(page))) {
        elog(PANIC, "The Page's LSN[%lu] bigger than want set LSN [%lu]", PageGetLSN(page), LSN);
    }
    PageSetLSNInternal(page, LSN);
}
#endif

#define PageHasFreeLinePointers(page) (((PageHeader)(page))->pd_flags & PD_HAS_FREE_LINES)
#define PageSetHasFreeLinePointers(page) (((PageHeader)(page))->pd_flags |= PD_HAS_FREE_LINES)
#define PageClearHasFreeLinePointers(page) (((PageHeader)(page))->pd_flags &= ~PD_HAS_FREE_LINES)

#define PageIsFull(page) (((PageHeader)(page))->pd_flags & PD_PAGE_FULL)
#define PageSetFull(page) (((PageHeader)(page))->pd_flags |= PD_PAGE_FULL)
#define PageClearFull(page) (((PageHeader)(page))->pd_flags &= ~PD_PAGE_FULL)

#define PageIsAllVisible(page) (((PageHeader)(page))->pd_flags & PD_ALL_VISIBLE)
#define PageSetAllVisible(page) (((PageHeader)(page))->pd_flags |= PD_ALL_VISIBLE)
#define PageClearAllVisible(page) (((PageHeader)(page))->pd_flags &= ~PD_ALL_VISIBLE)

#define PageIsCompressed(page) ((bool)(((PageHeader)(page))->pd_flags & PD_COMPRESSED_PAGE))
#define PageSetCompressed(page) (((PageHeader)(page))->pd_flags |= PD_COMPRESSED_PAGE)
#define PageClearCompressed(page) (((PageHeader)(page))->pd_flags &= ~PD_COMPRESSED_PAGE)

#define PageIsLogical(page) (((PageHeader)(page))->pd_flags & PD_LOGICAL_PAGE)
#define PageSetLogical(page) (((PageHeader)(page))->pd_flags |= PD_LOGICAL_PAGE)
#define PageClearLogical(page) (((PageHeader)(page))->pd_flags &= ~PD_LOGICAL_PAGE)

#define PageIsEncrypt(page) (((PageHeader)(page))->pd_flags & PD_ENCRYPT_PAGE)
#define PageSetEncrypt(page) (((PageHeader)(page))->pd_flags |= PD_ENCRYPT_PAGE)
#define PageClearEncrypt(page) (((PageHeader)(page))->pd_flags &= ~PD_ENCRYPT_PAGE)

#define PageIsTDE(page) (((PageHeader)(page))->pd_flags & PD_TDE_PAGE)
#define PageSetTDE(page) (((PageHeader)(page))->pd_flags |= PD_TDE_PAGE)
#define PageClearTDE(page) (((PageHeader)(page))->pd_flags &= ~PD_TDE_PAGE)

#define PageSetChecksumByFNV1A(page) (((PageHeader)(page))->pd_flags |= PD_CHECKSUM_FNV1A)
#define PageClearChecksumByFNV1A(page) (((PageHeader)(page))->pd_flags &= ~PD_CHECKSUM_FNV1A)

#define PageIsJustAfterFullPageWrite(page) (((PageHeader)(page))->pd_flags & PD_JUST_AFTER_FPW)
#define PageSetJustAfterFullPageWrite(page) (((PageHeader)(page))->pd_flags |= PD_JUST_AFTER_FPW)
#define PageClearJustAfterFullPageWrite(page) (((PageHeader)(page))->pd_flags &= ~PD_JUST_AFTER_FPW)

#define PageIsPrunable(page, oldestxmin)                   \
    (AssertMacro(TransactionIdIsNormal(oldestxmin)),       \
        TransactionIdIsValid(HeapPageGetPruneXid(page)) && \
            TransactionIdPrecedes(HeapPageGetPruneXid(page), oldestxmin))

#define IndexPageIsPrunable(page, oldestxmin)                   \
    (AssertMacro(TransactionIdIsNormal(oldestxmin)),       \
        TransactionIdIsValid(PageGetPruneXid(page)) && \
            TransactionIdPrecedes(PageGetPruneXid(page), oldestxmin))

#define PageSetPrunable(page, xid)                                                                                     \
    do {                                                                                                               \
        Assert(TransactionIdIsNormal(xid));                                                                            \
        if (!TransactionIdIsValid(HeapPageGetPruneXid(page)) || TransactionIdPrecedes(xid, HeapPageGetPruneXid(page))) \
            HeapPageSetPruneXid(page, xid);                                                                            \
    } while (0)
#define PageClearPrunable(page) (HeapPageSetPruneXid(page, InvalidTransactionId))

#define GlobalTempRelationPageIsNotInitialized(rel, page) \
    ((rel)->rd_rel->relpersistence == RELPERSISTENCE_GLOBAL_TEMP && PageIsNew(page))

const int PAGE_INDEX_CAN_TUPLE_DELETE = 2;
inline bool CheckPageZeroCases(const PageHeader page)
{
    return page->pd_lsn.xlogid != 0 || page->pd_lsn.xrecoff != 0 || (page->pd_flags & ~PD_LOGICAL_PAGE) != 0 ||
        page->pd_lower != 0 || page->pd_upper != 0 || page->pd_special != 0 || page->pd_pagesize_version != 0;
}

/* ----------------------------------------------------------------
 *		extern declarations
 * --------------------------TerminateBufferIO--------------------------------------
 */
typedef struct {
    int offsetindex;      /* linp array index */
    int itemoff;          /* page offset of item data */
    Size alignedlen;      /* MAXALIGN(item data len) */
    ItemIdData olditemid; /* used only in PageIndexMultiDelete */
} itemIdSortData;
typedef itemIdSortData* itemIdSort;

extern void PageInit(Page page, Size pageSize, Size specialSize, bool isheap = false);
extern bool PageIsVerified(Page page, BlockNumber blkno);
extern bool PageHeaderIsValid(PageHeader page);
struct UHeapPageHeaderData;
extern bool UPageHeaderIsValid(const UHeapPageHeaderData* page);
extern OffsetNumber PageAddItem(
    Page page, Item item, Size size, OffsetNumber offsetNumber, bool overwrite, bool is_heap);
extern Page PageGetTempPage(Page page);
extern Page PageGetTempPageCopy(Page page);
extern Page PageGetTempPageCopySpecial(Page page);
extern void PageRestoreTempPage(Page tempPage, Page oldPage);
extern void PageRepairFragmentation(Page page);
extern Size PageGetFreeSpace(Page page);
extern Size PageGetFreeSpaceForMultipleTuples(Page page, int ntups);
extern Size PageGetExactFreeSpace(Page page);
extern Size PageGetHeapFreeSpace(Page page);
extern void PageIndexTupleDelete(Page page, OffsetNumber offset);
extern void PageIndexMultiDelete(Page page, OffsetNumber* itemnos, int nitems);

extern void PageReinitWithDict(Page page, Size dictSize);
extern bool PageFreeDict(Page page);
extern char* PageDataEncryptIfNeed(Page page, TdeInfo* tdeinfo = NULL, bool need_copy = true, bool is_segbuf = false);
extern void PageDataDecryptIfNeed(Page page);
extern char* PageSetChecksumCopy(Page page, BlockNumber blkno, bool is_segbuf = false);
extern void PageSetChecksumInplace(Page page, BlockNumber blkno);

extern void DumpPageInfo(Page page, XLogRecPtr newLsn);
extern void SegPageInit(Page page, Size pageSize);
#endif /* BUFPAGE_H */
