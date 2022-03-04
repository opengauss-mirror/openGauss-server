/* -------------------------------------------------------------------------
 *
 * knl_upage.h
 * the page format of inplace update engine.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * src/include/access/ustore/knl_upage.h
 * -------------------------------------------------------------------------
 */

#ifndef KNL_UPAGE_H
#define KNL_UPAGE_H

#include "storage/buf/bufpage.h"
#include "access/ustore/knl_utype.h"
#include "access/ustore/knl_utuple.h"
#include "utils/rel.h"

#define TD_SLOT_INCREMENT_SIZE 2
#define TD_THRESHOLD_FOR_PAGE_SWITCH 32
#define FORCE_EXTEND_THRESHOLD 3
#define DML_MAX_RETRY_TIMES 100000

#define UHEAP_HAS_FREE_LINES 0x0001  /* are there any unused line pointers? */
#define UHEAP_PAGE_FULL 0x0002       /* not enough free space for new \
                                    * tuple? */
#define UHP_ALL_VISIBLE 0x0004     /* all tuples on page are visible to \
                                    * everyone */
#define UHEAP_VALID_FLAG_BITS 0xFFFF /* OR of all valid flag bits */

#define UPageHasFreeLinePointers(_page) (((UHeapPageHeaderData *)(_page))->pd_flags & UHEAP_HAS_FREE_LINES)
#define UPageSetHasFreeLinePointers(_page) (((UHeapPageHeaderData *)(_page))->pd_flags |= UHEAP_HAS_FREE_LINES)
#define UPageClearHasFreeLinePointers(_page) (((UHeapPageHeaderData *)(_page))->pd_flags &= ~UHEAP_HAS_FREE_LINES)

#define UPageIsFull(_page) (((UHeapPageHeaderData *)(_page))->pd_flags & UHEAP_PAGE_FULL)
#define UPageSetFull(_page) (((UHeapPageHeaderData *)(_page))->pd_flags |= UHEAP_PAGE_FULL)
#define UPageClearFull(_page) (((UHeapPageHeaderData *)(_page))->pd_flags &= ~UHEAP_PAGE_FULL)

#define SizeOfUHeapPageHeaderData (sizeof(UHeapPageHeaderData))

#define SizeOfUHeapTDData(_uphdr) ((_uphdr)->td_count * sizeof(TD))

#define UPageGetRowPtrOffset(_page) (SizeOfUHeapPageHeaderData + SizeOfUHeapTDData((UHeapPageHeaderData *)_page))

#define UPageGetRowPtr(_upage, _offsetNumber)                                                                      \
    ((RowPtr *)(((char *)_upage) + SizeOfUHeapPageHeaderData + SizeOfUHeapTDData((UHeapPageHeaderData *)_upage) + \
        ((_offsetNumber - 1) * sizeof(RowPtr))))

#define SetNormalRowPointer(_rowptr, _off, _size) \
    ((_rowptr)->flags = RP_NORMAL, (_rowptr)->offset = (_off), (_rowptr)->len = (_size))

#define UPageGetRowData(_upage, _rowptr)                                       \
    (AssertMacro(PageIsValid(_upage)), AssertMacro(RowPtrHasStorage(_rowptr)), \
        (Item)(((char *)(_upage)) + RowPtrGetOffset(_rowptr)))

#define PageGetTDPointer(_page) ((char *)((char *)(_page) + SizeOfUHeapPageHeaderData))

#define GetTDCount(_uphdr) ((_uphdr)->td_count)

#define UPageGetTDSlotCount(_page) ((UHeapPageHeaderData *)(_page))->td_count

#define UPageIsPrunable(_page)                                            \
    (TransactionIdIsValid(((UHeapPageHeaderData *)(_page))->pd_prune_xid) && \
        !TransactionIdIsInProgress(((UHeapPageHeaderData *)(_page))->pd_prune_xid))

#define UPageIsPrunableWithXminHorizon(_page, _oldestxmin)                   \
    (AssertMacro(TransactionIdIsNormal(_oldestxmin)),                        \
        TransactionIdIsValid(((UHeapPageHeaderData *)(_page))->pd_prune_xid) && \
        TransactionIdPrecedes(((UHeapPageHeaderData *)(_page))->pd_prune_xid, _oldestxmin))

#define UPageSetPrunable(_page, _xid)                                                 \
    do {                                                                              \
        Assert(TransactionIdIsNormal(_xid));                                          \
        if (!TransactionIdIsValid(((UHeapPageHeaderData *)(_page))->pd_prune_xid) ||     \
            TransactionIdPrecedes(_xid, ((UHeapPageHeaderData *)(_page))->pd_prune_xid)) \
            ((UHeapPageHeaderData *)(_page))->pd_prune_xid = (_xid);                     \
    } while (0)

#define UPageClearPrunable(_page) (((UHeapPageHeaderData *)(_page))->pd_prune_xid = InvalidTransactionId)

#define LimitRetryTimes(retryTimes)                                         \
    do {                                                                    \
        if ((retryTimes) > DML_MAX_RETRY_TIMES) {                             \
            elog(ERROR, "Transaction aborted due to too many retries.");    \
        }                                                                   \
    } while (0)

/*
 * RowPtr "flags" has these possible states.  An UNUSED row pointer is available
 * for immediate re-use, the other states are not.
 */
#define RP_UNUSED 0   /* unused (should always have len=0) */
#define RP_NORMAL 1   /* used (should always have len>0) */
#define RP_REDIRECT 2 /* HOT redirect (should have len=0) */
#define RP_DEAD 3     /* dead, may or may not have storage */

/*
 * Flags used in UHeap.  These flags are used in a row pointer of a deleted
 * row that has no actual storage.  These help in fetching the tuple from
 * undo when required.
 */
#define ROWPTR_DELETED 0x0001      /* Row is deleted */
#define ROWPTR_XACT_INVALID 0x0002 /* TD slot on tuple got reused */
#define ROWPTR_XACT_PENDING 0x0003 /* transaction that has marked item as \
                                    * unused is pending */
#define VISIBILTY_MASK 0x007F      /* 7 bits (1..7) for visibility mask */
#define XACT_SLOT 0x7F80           /* 8 bits (8..15) of offset for transaction \
                                    * slot */
#define XACT_SLOT_MASK 0x0007      /* 7 - mask to retrieve transaction slot */


/*
 * RowPtrIsUsed
 * True iff row pointer is in use.
 */
#define RowPtrIsUsed(_rowptr) ((_rowptr)->flags != RP_UNUSED)

#define RowPtrHasStorage(_rowptr) ((_rowptr)->len > 0)

#define RowPtrIsNormal(_rowptr) ((_rowptr)->flags == RP_NORMAL)

#define RowPtrGetLen(_rowptr) ((_rowptr)->len)

#define RowPtrGetOffset(_rowptr) ((_rowptr)->offset)
/*
 * RowPtrSetUnused
 * Set the row pointer to be UNUSED, with no storage.
 * Beware of multiple evaluations of itemId!
 */
#define RowPtrSetUnused(_rowptr) ((_rowptr)->flags = RP_UNUSED, (_rowptr)->offset = 0, (_rowptr)->len = 0)

#define RowPtrSetDead(rowptr) ((rowptr)->flags = RP_DEAD, (rowptr)->offset = 0, (rowptr)->len = 0)

#define RowPtrSetDeleted(_itemId, _td_slot, _vis_info)                                                       \
    ((_itemId)->flags = RP_REDIRECT, (_itemId)->offset = ((_itemId)->offset & ~VISIBILTY_MASK) | (_vis_info), \
        (_itemId)->offset = ((_itemId)->offset & ~XACT_SLOT) | (_td_slot) << XACT_SLOT_MASK, (_itemId)->len = 0)

/*
 * ItemIdChangeLen
 * Change the length of itemid.
 */
#define RowPtrChangeLen(_rowptr, _length) (_rowptr)->len = (_length)

/*
 * RowPtrIsDead
 * True iff row pointer is in state DEAD.
 */
#define RowPtrIsDead(_rowptr) ((_rowptr)->flags == RP_DEAD)

/*
 * RowPtrIsDeleted
 * True iff row pointer is in state REDIRECT.
 */
#define RowPtrIsDeleted(_rowptr) ((_rowptr)->flags == RP_REDIRECT)

/*
 * RowPtrGetVisibilityInfo
 * In a REDIRECT pointer, offset field contains the visibility information in
 * least significant 7 bits.
 */
#define RowPtrGetVisibilityInfo(_rowptr) ((_rowptr)->offset & VISIBILTY_MASK)

#define RowPtrHasPendingXact(_rowptr) (((_rowptr)->offset & VISIBILTY_MASK) & ROWPTR_XACT_PENDING)

#define RowPtrSetInvalidXact(_rowptr) ((_rowptr)->offset = ((_rowptr)->offset & ~VISIBILTY_MASK) | ROWPTR_XACT_INVALID)

#define RowPtrResetInvalidXact(_rowptr) \
    ((_rowptr)->offset = ((_rowptr)->offset & ~VISIBILTY_MASK) & ~(ROWPTR_XACT_INVALID))

/*
 * RowPtrGetTDSlot
 * In a REDIRECT pointer, offset contains the TD slot information in
 * most significant 8 bits.
 */
#define RowPtrGetTDSlot(_rowptr) (((_rowptr)->offset & XACT_SLOT) >> XACT_SLOT_MASK)

/*
 * MaxUHeapTupFixedSize - Fixed size for tuple, this is computed based
 * on data alignment.
 */
#define MaxUHeapTupFixedSize (SizeOfUHeapDiskTupleData + sizeof(ItemIdData))

/* MaxUHeapPageFixedSpace - Maximum fixed size for page */
#define MaxUHeapPageFixedSpace(relation) \
    (BLCKSZ - SizeOfUHeapPageHeaderData - (RelationGetInitTd(relation) * sizeof(TD)) - UHEAP_SPECIAL_SIZE)

#define MaxPossibleUHeapPageFixedSpace (BLCKSZ - SizeOfUHeapPageHeaderData - UHEAP_SPECIAL_SIZE)

#define MaxUHeapToastPageFixedSpace \
    (BLCKSZ - SizeOfUHeapPageHeaderData - (UHEAP_DEFAULT_TOAST_TD_COUNT * sizeof(TD)) - UHEAP_SPECIAL_SIZE)

/*
 * MaxUHeapTuplesPerPage is an upper bound on the number of tuples that can
 * fit on one uheap page.
 */
#define MaxUHeapTuplesPerPage(relation) ((int)((MaxUHeapPageFixedSpace(relation)) / (MaxUHeapTupFixedSize)))

#define MaxPossibleUHeapTuplesPerPage ((int)((MaxPossibleUHeapPageFixedSpace) / (MaxUHeapTupFixedSize)))

#define CalculatedMaxUHeapTuplesPerPage(tdSlots)                                               \
    ((int)((BLCKSZ - SizeOfUHeapPageHeaderData - (tdSlots * sizeof(TD)) - UHEAP_SPECIAL_SIZE) \
    / (MaxUHeapTupFixedSize)))

#define UPageGetPruneXID(_page) ((UHeapPageHeaderData)(_page))->pd_prune_xid

const uint8 UHEAP_DEFAULT_TOAST_TD_COUNT = 4;
const uint8 UHEAP_MAX_ATTR_PAD = 3;
/*
 * the disk page format of inplace table
 *
 * +---------------------+----------------------------+
 * | UHeapPageHeaderData | TD1 TD2 TD3 ...TDN         |
 * +-----------+----+---------------------------------+
 * | RowPtr1 RowPtr2 RowPtr3... RowPtrN               |
 * +-----------+--------------------------------------+
 * |           ^ pd_lower                             |
 * |                                                  |
 * |             v pd_upper                           |
 * +-------------+------------------------------------+
 * |             | rowN ...                           |
 * +-------------+------------------+-----------------+
 * |       ... row3 row2 row1 | "special space"       |
 * +--------------------------------+-----------------+
 * ^ special
 */
typedef struct RowPtr {
    unsigned offset : 15, /* offset to row (from start of page) */
        flags : 2,        /* state of row pointer */
        len : 15;         /* byte length of row */
} RowPtr;

typedef enum UPageType {
    UPAGE_HEAP = 0,
    UPAGE_INDEX,
    UPAGE_TOAST
} UPageType;

typedef struct UHeapPageHeaderData {
    PageXLogRecPtr pd_lsn;
    uint16 pd_checksum;
    uint16 pd_flags;            /* Various page attribute flags e.g. free rowptrs */
    uint16 pd_lower; /* Start of the free space between row pointers and row data */
    uint16 pd_upper;
    uint16 pd_special; /* Pointer to AM-specific per-page data */
    uint16 pd_pagesize_version;        /* Page version identifier */
    uint16 potential_freespace; /* Potential space from deleted and updated tuples */
    uint16 td_count;           /* Number of TD entries on the page */
    TransactionId pd_prune_xid;    /* oldest prunable XID, or zero if none */
    TransactionId pd_xid_base;
    TransactionId pd_multi_base;
    uint32 reserved;
} UHeapPageHeaderData;

typedef struct UHeapPageTDData {
    TD td_info[1];
} UHeapPageTDData;

typedef UHeapPageHeaderData* UHeapPageHeader;

inline bool UPageIsEmpty(UHeapPageHeaderData *phdr)
{
    uint16 td_count = phdr->td_count;
    uint16 start = (uint16)SizeOfUHeapPageHeaderData + (td_count * sizeof(TD));
    return phdr->pd_lower <= start;
}
/*
 * Given a page, it stores contiguous ranges of free offsets that can be
 * used/reused in the same page. This is used in UHeapMultiInsert to decide
 * the number of undo records needs to be prepared before entering into critical
 * section.
 */

typedef struct UHeapFreeOffsetRanges {
    OffsetNumber startOffset[MaxOffsetNumber];
    OffsetNumber endOffset[MaxOffsetNumber];
    int nranges;
} UHeapFreeOffsetRanges;

typedef struct UHeapBufferPage {
    Buffer buffer;
    Page page;
} UHeapBufferPage;

template<UPageType pagetype> 
void UPageInit(Page page, Size pageSize, Size specialSize, uint8 tdSlots = UHEAP_DEFAULT_TD);

template<> void UPageInit<UPageType::UPAGE_HEAP>(Page page, Size page_size, Size special_size, uint8 tdSlots);

template<> void UPageInit<UPageType::UPAGE_TOAST>(Page page, Size page_size, Size special_size, uint8 tdSlots);

OffsetNumber UPageAddItem(Relation relation, UHeapBufferPage *bufpage, Item item, Size size,
    OffsetNumber offsetNumber, bool overwrite);
UHeapTuple UHeapGetTuple(Relation relation, Buffer buffer, OffsetNumber offnum, UHeapTuple freebuf = NULL);
UHeapTuple UHeapGetTuplePartial(Relation relation, Buffer buffer, OffsetNumber offnum, AttrNumber lastVar = -1,
    bool *boolArr = NULL);
Size PageGetUHeapFreeSpace(Page page);
Size PageGetExactUHeapFreeSpace(Page page);
extern UHeapFreeOffsetRanges *UHeapGetUsableOffsetRanges(Buffer buffer, UHeapTuple *tuples, int ntuples,
    Size saveFreeSpace);

void UHeapRecordPotentialFreeSpace(Buffer buffer, int delta);

/*
 * UPageGetMaxOffsetNumber
 * Returns the maximum offset number used by the given page.
 * Since offset numbers are 1-based, this is also the number
 * of items on the page.
 *
 * NOTE: if the page is not initialized (pd_lower == 0), we must
 * return zero to ensure sane behavior.
 */
inline OffsetNumber UHeapPageGetMaxOffsetNumber(char *upage)
{
    OffsetNumber maxoff = InvalidOffsetNumber;
    UHeapPageHeaderData *upghdr = (UHeapPageHeaderData *)upage;

    if (upghdr->pd_lower <= SizeOfUHeapPageHeaderData)
        maxoff = 0;
    else
        maxoff = (upghdr->pd_lower - (SizeOfUHeapPageHeaderData + SizeOfUHeapTDData(upghdr))) / sizeof(RowPtr);

    return maxoff;
}
#endif
