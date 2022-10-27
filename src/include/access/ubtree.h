/* -------------------------------------------------------------------------
 *
 * ubtree.h
 *  header file for openGauss multi-version btree access method implementation.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * src/include/access/nbtree.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef UBTREE_H
#define UBTREE_H

#include "access/nbtree.h"
#include "access/genam.h"
#include "access/itup.h"
#include "access/sdir.h"
#include "access/transam.h"
#include "access/xlogreader.h"
#include "access/visibilitymap.h"
#include "access/ustore/knl_whitebox_test.h"
#include "catalog/pg_index.h"
#include "lib/stringinfo.h"
#include "storage/buf/bufmgr.h"

/*
 * prototypes for functions in ubtree.cpp (external entry points for ubtree)
 */
extern Datum ubtbuild(PG_FUNCTION_ARGS);
extern Datum ubtbuildempty(PG_FUNCTION_ARGS);
extern Datum ubtinsert(PG_FUNCTION_ARGS);
extern Datum ubtbeginscan(PG_FUNCTION_ARGS);
extern Datum ubtgettuple(PG_FUNCTION_ARGS);
extern Datum ubtgetbitmap(PG_FUNCTION_ARGS);
extern Datum ubtrescan(PG_FUNCTION_ARGS);
extern Datum ubtendscan(PG_FUNCTION_ARGS);
extern Datum ubtmarkpos(PG_FUNCTION_ARGS);
extern Datum ubtrestrpos(PG_FUNCTION_ARGS);
extern Datum ubtbulkdelete(PG_FUNCTION_ARGS);
extern Datum ubtvacuumcleanup(PG_FUNCTION_ARGS);
extern Datum ubtcanreturn(PG_FUNCTION_ARGS);
extern Datum ubtoptions(PG_FUNCTION_ARGS);

extern bool UBTreeDelete(Relation index_relation, Datum* values, const bool* isnull, ItemPointer heapTCtid,
                         bool isRollbackIndex);
extern bool IndexPagePrepareForXid(Relation rel, Page page, TransactionId xid, bool needWal, Buffer buf);
extern void FreezeSingleIndexPage(Relation rel, Buffer buf, bool *hasPruned,
    TransactionId oldestXmin, OidRBTree *invisibleParts = NULL);

/* structures for ubtrecycle.cpp */
typedef enum {
    RECYCLE_FREED_FORK = 0,
    RECYCLE_EMPTY_FORK,
    RECYCLE_NONE_FORK,   //last unused item
} UBTRecycleForkNumber;

typedef struct UBTRecycleQueueAddress {
    Buffer queueBuf;
    BlockNumber indexBlkno;
    uint16 offset;
} UBTRecycleQueueAddress;

typedef struct UBTRecycleMetaData {
    uint32 flags; /* reserved */
    volatile BlockNumber headBlkno;
    volatile BlockNumber tailBlkno;
    BlockNumber nblocksUpper; /* optimistic estimate of used blocks */
    /* not used yet, reserved for possible upgrades */
    BlockNumber nblocksLower; /* number of blocks that have been used correctly */
} UBTRecycleMetaData;

typedef UBTRecycleMetaData* UBTRecycleMeta;

typedef struct UBTRecycleQueueItemData {
    TransactionId xid;
    BlockNumber blkno;
    uint16 prev;
    uint16 next;
} UBTRecycleQueueItemData;

typedef UBTRecycleQueueItemData* UBTRecycleQueueItem;

typedef struct UBTRecycleQueueHeaderData {
    uint32 flags;
    uint16 head;            /* head of the ordered list in this page */
    uint16 tail;            /* tail of the ordered list in this page */
    uint16 freeItems;
    uint16 freeListHead;
    BlockNumber prevBlkno;
    BlockNumber nextBlkno;
    UBTRecycleQueueItemData items[FLEXIBLE_ARRAY_MEMBER];
} UBTRecycleQueueHeaderData;

typedef UBTRecycleQueueHeaderData* UBTRecycleQueueHeader;

#define URQ_HEAD_PAGE (1 << 0)
#define URQ_TAIL_PAGE (1 << 1)

#define UBTRecycleMaxItems \
    ((BLCKSZ - sizeof(PageHeaderData) - offsetof(UBTRecycleQueueHeaderData, items)) / sizeof(UBTRecycleQueueItemData))

/*
 * There are rare cases where _bt_truncate() will need to enlarge
 * a heap index tuple to make space for a tiebreaker heap TID
 * attribute, which we account for here.
 */
#define UBTMaxItemSize(page) \
    MAXALIGN_DOWN((PageGetPageSize(page) - \
                   MAXALIGN(SizeOfPageHeaderData + \
                            3*sizeof(ItemIdData) + \
                            3*sizeof(ItemPointerData)) - \
                   MAXALIGN(sizeof(UBTPageOpaqueData))) / 3)

#define UBTDefaultMaxItemSize \
    MAXALIGN_DOWN((BLCKSZ - \
                   MAXALIGN(SizeOfPageHeaderData + \
                            3*sizeof(ItemIdData) + \
                            3*sizeof(ItemPointerData)) - \
                   MAXALIGN(sizeof(UBTPageOpaqueData))) / 3)

/*
 * XLOG records for ubtree operations
 *
 * XLOG allows to store some information in high 4 bits of log
 * record xl_info field
 */
#define XLOG_UBTREE_INSERT_LEAF 0x00      /* add index tuple without split */
#define XLOG_UBTREE_INSERT_UPPER 0x10     /* same, on a non-leaf page */
#define XLOG_UBTREE_INSERT_META 0x20      /* same, plus update metapage */
#define XLOG_UBTREE_SPLIT_L 0x30          /* add index tuple with split */
#define XLOG_UBTREE_SPLIT_R 0x40          /* as above, new item on right */
#define XLOG_UBTREE_SPLIT_L_ROOT 0x50     /* add tuple with split of root */
#define XLOG_UBTREE_SPLIT_R_ROOT 0x60     /* as above, new item on right */
#define XLOG_UBTREE_DELETE 0x70           /* delete leaf index tuples for a page */
#define XLOG_UBTREE_UNLINK_PAGE 0x80      /* delete an entire page */
#define XLOG_UBTREE_UNLINK_PAGE_META 0x90 /* same, and update metapage */
#define XLOG_UBTREE_NEWROOT 0xA0          /* new root page */
#define XLOG_UBTREE_MARK_PAGE_HALFDEAD  \
    0xB0 /* page deletion that makes \
          * parent half-dead */
#define XLOG_UBTREE_VACUUM                   \
    0xC0 /* delete entries on a page during \
          * vacuum */
#define XLOG_UBTREE_REUSE_PAGE                   \
    0xD0 /* old page is about to be reused from \
          * FSM */
#define XLOG_UBTREE_MARK_DELETE 0xE0

#define XLOG_UBTREE_PRUNE_PAGE 0xF0

#define XLOG_UBTREE2_SHIFT_BASE 0x00

#define XLOG_UBTREE2_RECYCLE_QUEUE_INIT_PAGE 0x10

#define XLOG_UBTREE2_RECYCLE_QUEUE_ENDPOINT 0x20

#define XLOG_UBTREE2_RECYCLE_QUEUE_MODIFY 0x30

#define XLOG_UBTREE2_FREEZE 0x40

#define UBTREE_VERIFY_OUTPUT_PARAM_CNT 3
#define UBTREE_RECYCLE_OUTPUT_PARAM_CNT 6
#define UBTREE_RECYCLE_OUTPUT_XID_STR_LEN 32

enum {
    UBTREE_MARK_DELETE_BLOCK_NUM,
};

enum {
    UBTREE_PAGE_PRUNE_BLOCK_NUM,
};

enum {
    UBTREE2_BASE_SHIFT_BLOCK_NUM,
};

enum {
    UBTREE2_RECYCLE_QUEUE_INIT_PAGE_CURR_BLOCK_NUM,
    UBTREE2_RECYCLE_QUEUE_INIT_PAGE_LEFT_BLOCK_NUM,
    UBTREE2_RECYCLE_QUEUE_INIT_PAGE_RIGHT_BLOCK_NUM,
};

enum {
    UBTREE2_RECYCLE_QUEUE_ENDPOINT_CURR_BLOCK_NUM,
    UBTREE2_RECYCLE_QUEUE_ENDPOINT_NEXT_BLOCK_NUM,
};

enum {
    UBTREE2_RECYCLE_QUEUE_MODIFY_BLOCK_NUM,
};

enum {
    UBTREE2_FREEZE_BLOCK_NUM,
};

typedef struct xl_ubtree_mark_delete {
    TransactionId xid;
    OffsetNumber offset;
} xl_ubtree_mark_delete;

#define SizeOfUBTreeMarkDelete (offsetof(xl_ubtree_mark_delete, offset) + sizeof(OffsetNumber))

typedef struct xl_ubtree_prune_page {
    TransactionId new_prune_xid;
    TransactionId latestRemovedXid;
    int32 count;
} xl_ubtree_prune_page;

#define SizeOfUBTreePrunePage (offsetof(xl_ubtree_prune_page, count) + sizeof(int32))

/* shift the base of xids on heap page */
typedef struct xl_ubtree2_shift_base {
    int64 delta; /* delta value to shift the base */
} xl_ubtree2_shift_base;

#define SizeOfUBTree2ShiftBase (sizeof(xl_ubtree2_shift_base))

typedef struct xl_ubtree2_recycle_queue_init_page {
    bool insertingNewPage;
    BlockNumber prevBlkno;
    BlockNumber currBlkno;
    BlockNumber nextBlkno;
} xl_ubtree2_recycle_queue_init_page;

#define SizeOfUBTree2RecycleQueueInitPage (sizeof(xl_ubtree2_recycle_queue_init_page))

typedef struct xl_ubtree2_recycle_queue_endpoint {
    bool isHead;
    BlockNumber leftBlkno;
    BlockNumber rightBlkno;
} xl_ubtree2_recycle_queue_endpoint;

#define SizeOfUBTree2RecycleQueueEndpoint (sizeof(xl_ubtree2_recycle_queue_endpoint))

typedef struct xl_ubtree2_recycle_queue_modify {
    bool isInsert;
    uint16 offset;
    BlockNumber blkno;
    UBTRecycleQueueItemData item;
    UBTRecycleQueueHeaderData header;
} xl_ubtree2_recycle_queue_modify;

#define SizeOfUBTree2RecycleQueueModify (sizeof(xl_ubtree2_recycle_queue_modify))

typedef struct xl_ubtree2_freeze {
    TransactionId oldestXmin;
    BlockNumber blkno;
    int32 nfrozen;
} xl_ubtree2_freeze;

#define SizeOfUBTree2Freeze (sizeof(xl_ubtree2_freeze))

typedef struct xl_ubtree_split {
    uint32 level;            /* tree level of page being split */
    OffsetNumber firstright; /* first item moved to right page */
    OffsetNumber newitemoff; /* new item's offset (if placed on left page) */
    uint32 opaqueVersion;    /* the opaque version */
    UBTPageOpaqueDataInternal lopaque;
    UBTPageOpaqueDataInternal ropaque;
} xl_ubtree_split;

#define SizeOfUBtreeSplit (sizeof(xl_ubtree_split))

#define UBTREE_OPAQUE_VERSION_RCR 1

/*
 * Notes on B-Tree tuple format, and key and non-key attributes:
 *
 *      !!! Special Note !!!
 *      There is no Version 4 index in both openGauss and GaussDB now.
 *      heapkeyspace feature only used inUStore's multi-version index, which
 *      is distinguished by RelationIsUstoreIndex(index).
 *
 * INCLUDE B-Tree indexes have non-key attributes.  These are extra
 * attributes that may be returned by index-only scans, but do not influence
 * the order of items in the index (formally, non-key attributes are not
 * considered to be part of the key space).  Non-key attributes are only
 * present in leaf index tuples whose item pointers actually point to heap
 * tuples (non-pivot tuples).  BtCheckNatts() enforces the rules
 * described here.
 *
 * Non-pivot tuple format (plain/non-posting variant):
 *
 *  t_tid | t_info | key values | INCLUDE columns, if any
 *
 * t_tid points to the heap TID, which is a tiebreaker key column as of
 * BTREE_VERSION 4.
 *
 * Non-pivot tuples complement pivot tuples, which only have key columns.
 * The sole purpose of pivot tuples is to represent how the key space is
 * separated.  In general, any B-Tree index that has more than one level
 * (i.e. any index that does not just consist of a metapage and a single
 * leaf root page) must have some number of pivot tuples, since pivot
 * tuples are used for traversing the tree.  Suffix truncation can omit
 * trailing key columns when a new pivot is formed, which makes minus
 * infinity their logical value.  Since BTREE_VERSION 4 indexes treat heap
 * TID as a trailing key column that ensures that all index tuples are
 * physically unique, it is necessary to represent heap TID as a trailing
 * key column in pivot tuples, though very often this can be truncated
 * away, just like any other key column. (Actually, the heap TID is
 * omitted rather than truncated, since its representation is different to
 * the non-pivot representation.)
 *
 * Pivot tuple format:
 *
 *  t_tid | t_info | key values | [heap TID]
 *
 * We store the number of columns present inside pivot tuples by abusing
 * their t_tid offset field, since pivot tuples never need to store a real
 * offset (pivot tuples generally store a downlink in t_tid, though).  The
 * offset field only stores the number of columns/attributes when the
 * INDEX_ALT_TID_MASK bit is set, which doesn't count the trailing heap
 * TID column sometimes stored in pivot tuples -- that's represented by
 * the presence of UBT_PIVOT_HEAP_TID_ATTR.  The INDEX_ALT_TID_MASK bit in
 * t_info is always set on BTREE_VERSION 4 pivot tuples, since
 * UBTreeTupleIsPivot() must work reliably on heapkeyspace versions.
 *
 * In version 2 or version 3 (!heapkeyspace) indexes, INDEX_ALT_TID_MASK
 * might not be set in pivot tuples.  UBTreeTupleIsPivot() won't work
 * reliably as a result.  The number of columns stored is implicitly the
 * same as the number of columns in the index, just like any non-pivot
 * tuple. (The number of columns stored should not vary, since suffix
 * truncation of key columns is unsafe within any !heapkeyspace index.)
 *
 * The 12 least significant offset bits from t_tid are used to represent
 * the number of columns in INDEX_ALT_TID_MASK tuples, leaving 4 status
 * bits (BT_RESERVED_OFFSET_MASK bits), 3 of which that are reserved for
 * future use.  BT_N_KEYS_OFFSET_MASK should be large enough to store any
 * number of columns/attributes <= INDEX_MAX_KEYS.
 */

/* Item pointer offset bit masks */
#define UBT_OFFSET_MASK				0x0FFF
#define UBT_STATUS_OFFSET_MASK		0xF000
/* UBT_STATUS_OFFSET_MASK status bits */
#define UBT_PIVOT_HEAP_TID_ATTR		0x1000

const uint16 InvalidOffset = ((uint16)0) - 1;

/*
 * Note: UBTreeTupleIsPivot() can have false negatives (but not false
 * positives) when used with !heapkeyspace indexes
 */
static inline bool
UBTreeTupleIsPivot(IndexTuple itup)
{
    return  ((itup->t_info & INDEX_ALT_TID_MASK) != 0);
}

/*
 * Get/set downlink block number in pivot tuple.
 *
 * Note: Cannot assert that tuple is a pivot tuple.  If we did so then
 * !heapkeyspace indexes would exhibit false positive assertion failures.
 */
static inline BlockNumber
UBTreeTupleGetDownLink(IndexTuple pivot)
{
    return ItemPointerGetBlockNumberNoCheck(&pivot->t_tid);
}

static inline void
UBTreeTupleSetDownLink(IndexTuple pivot, BlockNumber blkno)
{
    ItemPointerSetBlockNumber(&pivot->t_tid, blkno);
}

/*
 * Get number of attributes within tuple.
 *
 * Note that this does not include an implicit tiebreaker heap TID
 * attribute, if any.  Note also that the number of key attributes must be
 * explicitly represented in all heapkeyspace pivot tuples.
 *
 * Note: This is defined as a macro rather than an inline function to
 * avoid including rel.h.
 */
#define UBTreeTupleGetNAtts(itup, rel)                                                                   \
    ((UBTreeTupleIsPivot(itup)) ? (ItemPointerGetOffsetNumberNoCheck(&(itup)->t_tid) & UBT_OFFSET_MASK) : \
                                 IndexRelationGetNumberOfAttributes(rel))

/*
 * Set number of key attributes in tuple.
 *
 * The heap TID tiebreaker attribute bit may also be set here, indicating that
 * a heap TID value will be stored at the end of the tuple (i.e. using the
 * special pivot tuple representation).
 */
static inline void
UBTreeTupleSetNAtts(IndexTuple itup, uint16 nkeyatts, bool heaptid)
{
    Assert(nkeyatts <= INDEX_MAX_KEYS);
    Assert((nkeyatts & UBT_STATUS_OFFSET_MASK) == 0);
    Assert(!heaptid || nkeyatts > 0);
    Assert(!UBTreeTupleIsPivot(itup) || nkeyatts == 0);

    itup->t_info |= INDEX_ALT_TID_MASK;

    if (heaptid)
        nkeyatts |= UBT_PIVOT_HEAP_TID_ATTR;

    /* BT_IS_POSTING bit is deliberately unset here */
    ItemPointerSetOffsetNumber(&itup->t_tid, nkeyatts);
    Assert(UBTreeTupleIsPivot(itup));
}

/*
 * Get/set leaf page's "top parent" link from its high key.  Used during page
 * deletion.
 *
 * Note: Cannot assert that tuple is a pivot tuple.  If we did so then
 * !heapkeyspace indexes would exhibit false positive assertion failures.
 */
static inline BlockNumber
UBTreeTupleGetTopParent(IndexTuple leafhikey)
{
    return ItemPointerGetBlockNumberNoCheck(&leafhikey->t_tid);
}

static inline void
UBTreeTupleSetTopParent(IndexTuple leafhikey, BlockNumber blkno)
{
    ItemPointerSetBlockNumber(&leafhikey->t_tid, blkno);
    UBTreeTupleSetNAtts(leafhikey, 0, false);
}

/*
 * Get tiebreaker heap TID attribute, if any.
 *
 * This returns the first/lowest heap TID in the case of a posting list tuple.
 */
static inline ItemPointer
UBTreeTupleGetHeapTID(IndexTuple itup)
{
    if (UBTreeTupleIsPivot(itup)) {
        /* Pivot tuple heap TID representation? */
        if ((ItemPointerGetOffsetNumberNoCheck(&itup->t_tid) & UBT_PIVOT_HEAP_TID_ATTR) != 0) {
            return (ItemPointer) ((char *) itup + IndexTupleSize(itup) - sizeof(ItemPointerData));
        }
        /* Heap TID attribute was truncated */
        return NULL;
    }

    return &itup->t_tid;
}

/*
 * Get maximum heap TID attribute, which could be the only TID in the case of
 * a non-pivot tuple that does not have a posting list tuple.
 *
 * Works with non-pivot tuples only.
 */
static inline ItemPointer
UBTreeTupleGetMaxHeapTID(IndexTuple itup)
{
    Assert(!UBTreeTupleIsPivot(itup));

    return &itup->t_tid;
}

/* Working data for heap_page_prune and subroutines */
typedef struct {
    TransactionId new_prune_xid;    /* new prune hint value for page */
    TransactionId latestRemovedXid; /* latest xid to be removed by this prune */
    int ndead;
    /* arrays that accumulate indexes of items to be changed */
    OffsetNumber nowdead[MaxIndexTuplesPerPage];
    OffsetNumber previousdead[MaxIndexTuplesPerPage];
} IndexPruneState;

#define TXNINFOSIZE (sizeof(ShortTransactionId) * 2)

/*
 * prototypes for functions in ubtinsert.cpp
 */
extern bool UBTreeDoInsert(Relation rel, IndexTuple itup, IndexUniqueCheck checkUnique, Relation heapRel);
extern bool UBTreeDoDelete(Relation rel, IndexTuple itup, bool isRollbackIndex);

extern bool UBTreePagePruneOpt(Relation rel, Buffer buf, bool tryDelete);
extern bool UBTreePagePrune(Relation rel, Buffer buf, TransactionId oldestXmin, OidRBTree *invisibleParts = NULL);
extern bool UBTreePruneItem(Page page, OffsetNumber offnum, TransactionId oldestXmin, IndexPruneState* prstate);
extern void UBTreePagePruneExecute(Page page, OffsetNumber* nowdead, int ndead, IndexPruneState* prstate,
    TransactionId oldest_xmin);
extern void UBTreePageRepairFragmentation(Relation rel, BlockNumber blkno, Page page);

extern void UBTreeInsertParent(Relation rel, Buffer buf, Buffer rbuf, BTStack stack, bool is_root, bool is_only);
extern void UBTreeFinishSplit(Relation rel, Buffer lbuf, BTStack stack);
extern Buffer UBTreeGetStackBuf(Relation rel, BTStack stack);

/*
 * prototypes for functions in ubtsort.cpp
 */
extern void UBTreeLeafBuild(BTSpool *btspool, BTSpool *btspool2);
extern void UBTreeBuildAdd(BTWriteState* wstate, BTPageState* state, IndexTuple itup, bool hasxid);
extern void UBTreeUpperShutDown(BTWriteState* wstate, BTPageState* state);
extern BTPageState* UBTreePageState(BTWriteState* wstate, uint32 level);

/*
 * prototypes for functions in ubtsearch.cpp
 */
extern BTStack UBTreeSearch(Relation rel, BTScanInsert key, Buffer *bufP, int access, bool needStack = true);
extern Buffer UBTreeMoveRight(Relation rel, BTScanInsert itup_key, Buffer buf, bool forupdate, BTStack stack,
    int access);
extern OffsetNumber UBTreeBinarySearch(Relation rel, BTScanInsert key, Buffer buf, bool fixActiveCount);
extern int32 UBTreeCompare(Relation rel, BTScanInsert key, Page page, OffsetNumber offnum, Buffer buf);
extern bool UBTreeFirst(IndexScanDesc scan, ScanDirection dir);
extern bool UBTreeNext(IndexScanDesc scan, ScanDirection dir);
extern void UBTreeTraceTuple(IndexScanDesc scan, OffsetNumber offnum, bool isVisible, bool isHikey = false);
extern Buffer UBTreeGetEndPoint(Relation rel, uint32 level, bool rightmost);
extern bool UBTreeGetTupleInternal(IndexScanDesc scan, ScanDirection dir);

/*
 * prototypes for functions in ubtutils.cpp
 */
extern BTScanInsert UBTreeMakeScanKey(Relation rel, IndexTuple itup);
extern IndexTuple UBTreeCheckKeys(IndexScanDesc scan, Page page, OffsetNumber offnum,
    ScanDirection dir, bool* continuescan, bool *needRecheck);
extern void UBTreeCheckThirdPage(Relation rel, Relation heap, bool needheaptidspace, Page page, IndexTuple newtup);
extern IndexTuple UBTreeTruncate(Relation rel, IndexTuple lastleft,
    IndexTuple firstright, BTScanInsert itup_key, bool itup_extended);
extern int	UBTreeKeepNattsFast(Relation rel, IndexTuple lastleft, IndexTuple firstright);
extern bool UBTreeCheckNatts(const Relation index, bool heapkeyspace, Page page, OffsetNumber offnum);
extern void BtCheckThirdPage(Relation rel, Relation heap, bool needheaptidspace, Page page, IndexTuple newtup);
extern bool UBTreeItupGetXminXmax(Page page, OffsetNumber offnum, TransactionId oldest_xmin, TransactionId *xmin,
    TransactionId *xmax, bool *xminCommitted, bool *xmaxCommitted);
extern TransactionIdStatus UBTreeCheckXid(TransactionId xid);

/*
 * prototypes for functions in ubtpage.cpp
 */
extern void UBTreePageInit(Page page, Size size);
extern void UBTreeInitMetaPage(Page page, BlockNumber rootbknum, uint32 level);
extern Buffer UBTreeGetRoot(Relation rel, int access);
extern bool UBTreePageRecyclable(Page page);
extern int UBTreePageDel(Relation rel, Buffer buf);

extern OffsetNumber UBTreeFindsplitloc(Relation rel, Buffer buf, OffsetNumber newitemoff,
    Size newitemsz, bool* newitemonleft);
extern OffsetNumber UBTreeFindsplitlocInsertpt(Relation rel, Buffer buf, OffsetNumber newitemoff, Size newitemsz,
    bool *newitemonleft, IndexTuple newitem);

extern Buffer UBTreeGetNewPage(Relation rel, UBTRecycleQueueAddress* addr);

/*
 * prototypes for functions in ubtxlog.cpp
 */
extern void UBTreeRedo(XLogReaderState* record);
extern void UBTree2Redo(XLogReaderState* record);
extern void UBTreeDesc(StringInfo buf, XLogReaderState* record);
extern const char* ubtree_type_name(uint8 subtype);
extern void UBTree2Desc(StringInfo buf, XLogReaderState* record);
extern const char* ubtree2_type_name(uint8 subtype);
extern void UBTreeXlogStartup(void);
extern void UBTreeXlogCleanup(void);
extern bool UBTreeSafeRestartPoint(void);
extern void* UBTreeGetIncompleteActions();
extern void UBTreeClearIncompleteAction();
extern bool IsUBTreeVacuum(const XLogReaderState* record);
extern void UBTreeRestorePage(Page page, char* from, int len);
extern void DumpUBTreeDeleteInfo(XLogRecPtr lsn, OffsetNumber offsetList[], uint64 offsetNum);


/*
 * prototypes for functions in ubtdump.cpp
 */
typedef enum {
    VERIFY_XID_BASE_TOO_LARGE,
    VERIFY_XID_TOO_LARGE,
    VERIFY_HIKEY_ERROR,
    VERIFY_PREV_HIKEY_ERROR,
    VERIFY_ORDER_ERROR,
    VERIFY_XID_ORDER_ERROR,
    VERIFY_CSN_ORDER_ERROR,
    VERIFY_INCONSISTENT_XID_STATUS,
    VERIFY_XID_STATUS_ERROR,
    VERIFY_RECYCLE_QUEUE_HEAD_ERROR,
    VERIFY_RECYCLE_QUEUE_TAIL_ERROR,
    VERIFY_INCONSISTENT_USED_PAGE,
    VERIFY_RECYCLE_QUEUE_ENDLESS,
    VERIFY_RECYCLE_QUEUE_TAIL_MISSED,
    VERIFY_RECYCLE_QUEUE_PAGE_TOO_LESS,
    VERIFY_RECYCLE_QUEUE_OFFSET_ERROR,
    VERIFY_RECYCLE_QUEUE_XID_TOO_LARGE,
    VERIFY_RECYCLE_QUEUE_UNEXPECTED_TAIL,
    VERIFY_RECYCLE_QUEUE_FREE_LIST_ERROR,
    VERIFY_RECYCLE_QUEUE_FREE_LIST_INVALID_OFFSET,
    VERIFY_NORMAL
} UBTVerifyErrorCode;
typedef enum {
    VERIFY_MAIN_PAGE,
    VERIFY_RECYCLE_QUEUE_PAGE
} UBTVerifyPageType;

extern char * UBTGetVerifiedPageTypeStr(uint32 type);
extern char * UBTGetVerifiedResultStr(uint32 type);
extern void UBTreeVerifyRecordOutput(uint blkType, BlockNumber blkno, int errorCode,
    TupleDesc *tupDesc, Tuplestorestate *tupstore, uint32 cols);
extern void UBTreeVerifyIndex(Relation rel, TupleDesc *tupDesc, Tuplestorestate *tupstore, uint32 cols);
extern int UBTreeVerifyOnePage(Relation rel, Page page, BTScanInsert cmpKeys, IndexTuple prevHikey);
extern uint32 UBTreeVerifyRecycleQueue(Relation rel, TupleDesc *tupleDesc, Tuplestorestate *tupstore, uint32 cols);
extern Buffer RecycleQueueGetEndpointPage(Relation rel, UBTRecycleForkNumber forkNumber, bool needHead, int access);
extern bool UBTreePageVerify(UBtreePageVerifyParams *verifyParams);

typedef enum IndexTraceLevel {
    TRACE_NO = 0,
    TRACE_NORMAL,
    TRACE_VISIBILITY,
    TRACE_SHOWHIKEY,
    TRACE_ALL
} IndexTraceLevel;

/*
 * prototypes for functions in ubtrecycle.cpp
 */
const BlockNumber minRecycleQueueBlockNumber = 6;
extern UBTRecycleQueueHeader GetRecycleQueueHeader(Page page, BlockNumber blkno);
extern Buffer ReadRecycleQueueBuffer(Relation rel, BlockNumber blkno);
extern void UBTreeInitializeRecycleQueue(Relation rel);
extern void UBTreeTryRecycleEmptyPage(Relation rel);
extern void UBTreeRecordFreePage(Relation rel, BlockNumber blkno, TransactionId xid);
extern void UBTreeRecordEmptyPage(Relation rel, BlockNumber blkno, TransactionId xid);
extern void UBTreeRecordUsedPage(Relation rel, UBTRecycleQueueAddress addr);
extern Buffer UBTreeGetAvailablePage(Relation rel, UBTRecycleForkNumber forkNumber, UBTRecycleQueueAddress* addr);
extern void UBTreeRecycleQueueInitPage(Relation rel, Page page, BlockNumber blkno, BlockNumber prevBlkno,
    BlockNumber nextBlkno);
extern void UBtreeRecycleQueueChangeChain(Buffer buf, BlockNumber newBlkno, bool setNext);
extern void UBTreeRecycleQueuePageChangeEndpointLeftPage(Buffer buf, bool isHead);
extern void UBTreeRecycleQueuePageChangeEndpointRightPage(Buffer buf, bool isHead);
extern void UBTreeXlogRecycleQueueModifyPage(Buffer buf, xl_ubtree2_recycle_queue_modify *xlrec);
extern uint32 UBTreeRecycleQueuePageDump(Relation rel, Buffer buf, bool recordEachItem,
    TupleDesc *tupleDesc, Tuplestorestate *tupstore, uint32 cols);
extern void UBTreeDumpRecycleQueueFork(Relation rel, UBTRecycleForkNumber forkNum, TupleDesc *tupDesc,
    Tuplestorestate *tupstore, uint32 cols);
extern void UBTreeBuildCallback(Relation index, HeapTuple htup, Datum *values, const bool *isnull, bool tupleIsAlive,
    void *state);
#endif /* UBTREE_H */
