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
 * --------------------------------------------------------------------------------------
 *
 * ubteepcr.h
 *      header file for openGauss multi-version btree pcr access method implementation.
 *
 * IDENTIFICATION
 *      src/include/access/ubtreepcr.h
 *
 * --------------------------------------------------------------------------------------
 */

#ifndef UBTREE_PCR_H
#define UBTREE_PCR_H

#include "access/nbtree.h"
#include "access/ubtree.h"
#include "access/ustore/knl_uundorecord.h"
#include "storage/procarray.h"

/*
 * UBtreeIndexType
 */
typedef enum {
    UBTREE_DEFAULT,  /* ubtree normal type */
    UBTREE_PCR,      /* ubtree pcr type */
} UBtreeIndexType;


/*
* Definitions of pcr TD
*/
#define UBTreeFrozenTDSlotId  0
#define UBTreeInvalidTDSlotId 255

#define UBTreeTDSlotIsNormal(slot) \
    (UBTreeFrozenTDSlotId < slot && slot < UBTreeInvalidTDSlotId)

#define UBTREE_TD_SLOT_INCREMENT_SIZE        2
#define UBTREE_DEFAULT_TD_COUNT              4
#define UBTREE_TD_THRESHOLD_FOR_PAGE_SWITCH  32
#define UBTREE_MAX_TD_COUNT                  128


/*
* TD status for pcr page
*/
#define TD_FROZEN   (1 << 0)
#define TD_ACTIVE   (1 << 1)
#define TD_COMMITED (1 << 2)
#define TD_DELETE   (1 << 3)
#define TD_CSN      (1 << 4)

const int CR_ROLLBACL_COUNT_THRESHOLD = 10;

/*
 * An item pointer (also called line pointer) on a ubtree pcr index buffer page
 */
typedef struct UBTreeItemIdData {
    unsigned lp_off : 15, /* offset to tuple (from start of page) */
        lp_flags : 2,     /* state of item pointer, see below */
        lp_td_id : 8,
        lp_td_invalid : 1,
        lp_deleted : 1,
        lp_xmin_frozen : 1,
        lp_aligned : 4;
} UBTreeItemIdData;

typedef UBTreeItemIdData* UBTreeItemId;

#define UBTreeItemIdSetNormal(itemId, off) \
    ((itemId)->lp_flags = LP_NORMAL, (itemId)->lp_off = (off))

#define UBTreeItemIdHasStorage(itemId) ((itemId)->lp_off != 0)

typedef struct {
    int offsetindex;
    int itemoff;
    Size alignedlen;
    UBTreeItemIdData olditemid;
} UBTreeItemIdSortData;

typedef UBTreeItemIdSortData* UBTreeItemIdSort;

/*
 * UBtreeIndexType
 *    Transaction directory for ubtree pcr page
 */
typedef struct {
    TransactionId xactid;
    union {
        CommitSeqNo csn;
        struct {
            CommandId cid;
            uint32 aligned;
        } ctid;
    } combine;
    UndoRecPtr undoRecPtr;
    uint8 tdStatus;

    void setInfo(TransactionId xact, UndoRecPtr urecPtr)
    {
        xactid = xact;
        undoRecPtr = urecPtr;
        combine.csn = InvalidCommitSeqNo;
        tdStatus &= ~(TD_FROZEN | TD_COMMITED | TD_CSN);
        tdStatus |= TD_ACTIVE;
    }

    void setFrozen()
    {
        xactid = InvalidTransactionId;
        undoRecPtr = INVALID_UNDO_REC_PTR;
        tdStatus |= TD_FROZEN;
        tdStatus &= ~(TD_ACTIVE | TD_COMMITED | TD_CSN);
    }
} UBTreeTDData;
typedef UBTreeTDData* UBTreeTD;

#define UBTreePCRTDIsFrozen(td) ((td)->tdStatus & TD_FROZEN)
#define UBTreePCRTDIsCommited(td) ((td)->tdStatus & TD_COMMITED)
#define UBTreePCRTDIsActive(td) ((td)->tdStatus & TD_ACTIVE)
#define UBTreePCRTDIsDelete(td) ((td)->tdStatus & TD_DELETE)
#define UBTreePCRTDHasCsn(td) ((td)->tdStatus & TD_CSN)
#define UBTreePCRTDSetStatus(td, status) ((td)->tdStatus |= status)
#define UBTreePCRTDClearStatus(td, status) ((td)->tdStatus &= ~status)
#define UBTreePCRTDIdIsNormal(td_id) \
    ((unsigned)(td_id) != UBTreeFrozenTDSlotId && (unsigned)(td_id) != UBTreeInvalidTDSlotId)

#define UBTreePCRSetIndexTupleTDInvalid(iid) \
    (((UBTreeItemId)(iid))->lp_td_invalid = 1)

#define IsUBTreePCRTDReused(iid) \
    (((UBTreeItemId)(iid))->lp_td_invalid == 1)

#define UBTreePCRClearIndexTupleTDInvalid(iid) \
    (((UBTreeItemId)(iid))->lp_td_invalid = 0)
    
#define UBTreePCRSetIndexTupleDeleted(iid) \
    (((UBTreeItemId)(iid))->lp_deleted = 1)

#define UBTreePCRClearIndexTupleDeleted(iid) \
    (((UBTreeItemId)(iid))->lp_deleted = 0)

#define IsUBTreePCRItemDeleted(iid) \
    (((UBTreeItemId)(iid))->lp_deleted == 1)

#define UBTreePCRSetIndexTupleTDSlot(iid, slot) \
    (((UBTreeItemId)(iid))->lp_td_id = slot)


// ubtree undo
typedef struct UBTreeUndoInfoData {
    uint8 prev_td_id;
} UBTreeUndoInfoData;
typedef UBTreeUndoInfoData* UBTreeUndoInfo;

#define SizeOfUBTreeUndoInfoData (sizeof(uint8))

typedef struct UBTree3WalInfo {
    Oid relOid;
    Oid partitionOid;
    Oid relfilenode;
    TransactionId xid;     /* if rel is logically logged, and operation is under a subxact, xid is the xid of subxact,
                              otherwise, it's InvalidTransactionId */
    TransactionId oldXid;  /* xid in old td, i.e. xid of last operation on this tuple */
    CommandId cid;
    Buffer buffer;
    UndoRecPtr urecptr;    /* urecptr where the current operation's undo is located */
    UndoRecPtr blkprev;    /* byte offset of previous undo for block */
    UndoRecPtr prevurp;    /* urecptr where the previous undo of the current transaction */
    uint8 tdId;         /* old TD and locker TD */
    UBTreeUndoInfo undoInfo;
    IndexTuple itup;
    undo::XlogUndoMeta *xlum;        /* infomation about undometa, needed in every operation */
    uint8 flag;                /* flags for partitionOid, hashSubXact, blkprev, prevurp */
    OffsetNumber offnum;
} UBTree3WalInfo;

/*
 * UBTPCRPruneState
 */
typedef struct {
    int nowDeadLen;
    int previousDeadlen;
    TransactionId latestRemovedXid; /* latest xid to be removed by this prune */
        /* arrays that accumulate indexes of items to be changed */
    OffsetNumber nowDead[MaxIndexTuplesPerPage];
    OffsetNumber previousDead[MaxIndexTuplesPerPage];
} UBTPCRPruneState;

/*
 * UBTreeLatestChangeInfo
 *  Record latest change in undo
 */
typedef struct {
    uint8 undoType;
    TransactionId xid;
    CommandId cid;
    TransactionId prevXid;
} UBTreeLatestChangeInfo;

/*
* UBtreeIndexIsPCR
*   True means ubtree is pcr
*/
#define UBTreePageGetTDSlotCount(page) \
    (((UBTPCRPageOpaque)(PageGetSpecialPointer(page)))->td_count)

#define UBTreePageGetTDPointer(page) \
    (((char *)page) + SizeOfPageHeaderData)

#define IsValidUBTreeTransationSlotId(slot) \
    (slot != UBTreeInvalidTDSlotId && slot != UBTreeFrozenTDSlotId && slot < UBTREE_MAX_TD_COUNT)

#define SizeOfUBTreeTDData(page) \
    (UBTreePageGetTDSlotCount(page) * sizeof(UBTreeTDData))

#define UBTreePCRGetTD(page, tdid) \
    ((UBTreeTD)((char *)(page) + SizeOfPageHeaderData + (sizeof(UBTreeTDData) * (tdid - 1))))

#define UBTreePCRGetRowPtrOffset(page) \
    (SizeOfPageHeaderData + SizeOfUBTreeTDData(page))

#define UBTreePCRGetRowPtr(page, offset) \
    ((UBTreeItemId)((char*)(page) + UBTreePCRGetRowPtrOffset(page) + sizeof(UBTreeItemIdData) * (offset - 1)))

#define UBTreePCRGetIndexTuple(page, offset) \
    ((IndexTuple)((char*)(page) + (UBTreePCRGetRowPtr(page, offset))->lp_off))

#define UBTreePCRGetIndexTupleByItemId(page, iid) \
    ((IndexTuple)((char*)(page) + ((UBTreeItemId)(iid))->lp_off))

#define UBTreePCRPageGetMaxOffsetNumber(page) \
    ((((PageHeader)(page))->pd_lower - UBTreePCRGetRowPtrOffset(page)) / sizeof(UBTreeItemIdData))

#define UBTreePCRTdSlotSize(tdSlots) \
    (tdSlots * sizeof(UBTreeTDData))

#define UBTreePCRMaxItemSize(page) \
    MAXALIGN_DOWN((PageGetPageSize(page) - \
                   MAXALIGN(SizeOfPageHeaderData + \
                            3 * sizeof(UBTreeItemIdData) + \
                            3 * sizeof(ItemPointerData) + \
                            3 * sizeof(UBTreeTDData)) - \
                   MAXALIGN(sizeof(UBTPCRPageOpaqueData))) / 3)

#define UBTreeHasIncluding(rel) \
    (RelationIsGlobalIndex(rel) ? \
     (IndexRelationGetNumberOfAttributes(rel) > IndexRelationGetNumberOfKeyAttributes(rel) + 1) : \
     (IndexRelationGetNumberOfAttributes(rel) > IndexRelationGetNumberOfKeyAttributes(rel)))

/** ubtree rollback redo */
const int DOUBLE_SIZE = 2;

typedef struct UBTreeRedoRollbackItemData {
    OffsetNumber offnum;
    UBTreeItemIdData iid;
} UBTreeRedoRollbackItemData;

typedef UBTreeRedoRollbackItemData* UBTreeRedoRollbackItem;

struct UBTreeRedoRollbackItems {
    static constexpr int DEFAULT_ITEM_SIZE = 4;
    int size = 0;
    int next_item = 0;
    UBTreeRedoRollbackItem items = NULL;

    int append(Page page, OffsetNumber offnum) {
        if (size == 0) {
            size = DEFAULT_ITEM_SIZE;
            items = (UBTreeRedoRollbackItem)palloc0(sizeof(UBTreeRedoRollbackItemData) * size);
        } else if (next_item >= size) {
            items = (UBTreeRedoRollbackItem)repalloc(items, sizeof(UBTreeRedoRollbackItemData) * size * DOUBLE_SIZE);
            size *= DOUBLE_SIZE;
        }
        UBTreeItemId iid = UBTreePCRGetRowPtr(page, offnum);
        items[next_item].offnum = offnum;
        items[next_item].iid = *iid;
        return next_item++;
    }

    void release() {
        if (items != NULL) {
            pfree(items);
            size = next_item = 0;
        }
    }
};

struct UBTreeRedoRollbackAction {
    TransactionId xid = InvalidTransactionId;
    UBTreeTDData td;
    uint8 td_id = UBTreeInvalidTDSlotId;
    UBTreeRedoRollbackItems rollback_items;
};

/*
 * UBTPCRPageGetMaxOffsetNumber
 * Returns the maximum offset number used by the given page.
 * Since offset numbers are 1-based, this is also the number
 * of items on the page.
 *
 * NOTE: if the page is not initialized (pd_lower == 0), we must
 * return zero to ensure sane behavior.
 */
inline OffsetNumber UBTPCRPageGetMaxOffsetNumber(char *upage)
{
    OffsetNumber maxoff = InvalidOffsetNumber;
    PageHeader upghdr = (PageHeader)upage;

    if (upghdr->pd_lower <= SizeOfPageHeaderData)
        maxoff = 0;
    else
        maxoff = (upghdr->pd_lower - (SizeOfPageHeaderData + SizeOfUBTreeTDData(upghdr))) / sizeof(UBTreeItemIdData);

    return maxoff;
}

/*
 * prototypes for functions in ubtpcrsort.cpp
 */
extern void UBTreePCRLeafBuild(BTSpool *btspool, BTSpool *btspool2);
extern void UBTreePCRBuildAdd(BTWriteState* wstate, BTPageState* state, IndexTuple itup, bool hasxid);
extern void UBTreePCRUpperShutDown(BTWriteState* wstate, BTPageState* state);
extern BTPageState* UBTreePCRPageState(BTWriteState* wstate, uint32 level);
extern OffsetNumber UBTPCRPageAddItem(Page page, Item item, Size size, OffsetNumber offsetNumber, bool overwrite);

/*
 * prototypes for functions in ubtpcrpage.cpp
 */
extern void UBTreePCRInitTD(Page page);
extern void UBTreePCRPageInit(Page page, Size size);
extern void UBTreePCRInitMetaPage(Page page, BlockNumber rootbknum, uint32 level);
extern Buffer UBTreePCRGetRoot(Relation rel, int access);
extern bool UBTreePCRPageRecyclable(Page page);
extern void UBTreePCRDeleteOnPage(Relation rel, Buffer buf, OffsetNumber offset, bool isRollbackIndex, int tdslot,
    UndoRecPtr urecPtr, undo::XlogUndoMeta *xlumPtr);
extern bool UBTreePCRCheckNatts(const Relation index, bool heapkeyspace, Page page, OffsetNumber offnum);

/*
 * prototypes for functions in ubtpcrundo.cpp
 */
extern void ExecuteUndoActionsForUBTreePage(Relation relation, Buffer buf, uint8 slotID);
extern IndexTuple FetchTupleFromUndoRecord(UndoRecord *urec);
extern UBTreeUndoInfo FetchUndoInfoFromUndoRecord(UndoRecord *urec);
extern UndoRecPtr UBTreePCRPrepareUndoDelete(Oid relOid, Oid partitionOid, Oid relfilenode, Oid tablespace,
    UndoPersistence persistence, Buffer buffer, TransactionId xid, CommandId cid, UndoRecPtr prevurpInOneXact,
    IndexTuple oldtuple, BlockNumber blk, XlUndoHeader *xlundohdr, undo::XlogUndoMeta *xlundometa,
    UBTreeUndoInfo undoInfo);
/*
 * prototypes for functions in ubtpcrrollback.cpp
 */
extern void RollbackCRPage(IndexScanDesc scan, Page crPage, uint8 tdid,
    CommandId *page_cid = NULL, CommandId cid = InvalidCommandId, IndexTuple itup = NULL);
extern UndoRecPtr UBTreePCRPrepareUndoInsert(Oid relOid, Oid partitionOid, Oid relfilenode, Oid tablespace,
    UndoPersistence persistence, TransactionId xid, CommandId cid, UndoRecPtr prevurpInOneBlk,
    UndoRecPtr prevurpInOneXact, BlockNumber blk, XlUndoHeader *xlundohdr, undo::XlogUndoMeta *xlundometa,
    OffsetNumber offset, Buffer buf, TransactionId oldXid, UBTreeUndoInfo undoinfo, IndexTuple itup);
extern int UBTreePCRRollback(URecVector *urecvec, int startIdx, int endIdx, TransactionId xid, Oid reloid,
    Oid partitionoid, BlockNumber blkno, bool isFullChain, int preRetCode, Oid *preReloid, Oid *prePartitionoid);
/*
 * prototypes for functions in ubtpcrsearch.cpp
 */
extern BTStack UBTreePCRSearch(Relation rel, BTScanInsert key, Buffer *bufP, int access, bool needStack);
extern OffsetNumber UBTreePCRBinarySearch(Relation rel, BTScanInsert key, Page page);
extern bool UBTreePCRFirst(IndexScanDesc scan, ScanDirection dir);
extern Buffer UBTreePCRMoveRight(Relation rel, BTScanInsert itup_key, Buffer buf, bool forupdate,
    BTStack stack, int access);
extern int32 UBTreePCRCompare(Relation rel, BTScanInsert key, Page page, OffsetNumber offnum);
extern bool UBTreeCPRFirst(IndexScanDesc scan, ScanDirection dir);
extern bool UBTreePCRNext(IndexScanDesc scan, ScanDirection dir);
extern Buffer UBTreePCRGetEndPoint(Relation rel, uint32 level, bool rightmost);
extern bool UBTreePCRGetTupleInternal(IndexScanDesc scan, ScanDirection dir);
extern void ReportSnapshotTooOld(IndexScanDesc scan, Page page, OffsetNumber offnum,
    UndoRecPtr urecptr, const char* when);
void LogInsertOrDelete(UBTree3WalInfo *walInfo, uint8 opt);


/*
 * prototypes for functions in ubtpcrinsert.cpp
 */
extern bool UBTreePCRDoInsert(Relation rel, IndexTuple itup, IndexUniqueCheck checkUnique, Relation heapRel);
extern bool UBTreePCRDoDelete(Relation rel, IndexTuple itup, bool isRollbackIndex);
extern void UBTreePCRInsertParent(Relation rel, Buffer buf, Buffer rbuf, BTStack stack, bool is_root, bool is_only);
extern void UBTreePCRFinishSplit(Relation rel, Buffer lbuf, BTStack stack);
extern Buffer UBTreePCRGetStackBuf(Relation rel, BTStack stack);
extern void UBTreeResetWaitTimeForTDSlot();
extern OffsetNumber UBTreePCRFindDeleteLoc(Relation rel, Buffer* bufP, OffsetNumber offset, BTScanInsert itup_key,
    IndexTuple itup);
extern bool UBTreePCRIndexTupleMatches(Relation rel, Page page, OffsetNumber offnum, IndexTuple target_itup,
    BTScanInsert itup_key, Oid target_part_oid);
extern uint8 PreparePCRDelete(Relation rel, Buffer buf, OffsetNumber offnum, UBTreeUndoInfo undoInfo,
    TransactionId* minXid, bool* needRetry);
extern bool UBTreePCRIsEqual(Relation idxrel, Page page, OffsetNumber offnum, int keysz, ScanKey scankey);
extern void VerifyPCRIndexHikeyAndOpaque(Relation rel, Page page, BlockNumber blkno);
extern void UBTreePCRVerify(Relation rel, Page page, BlockNumber blkno, OffsetNumber offnum, bool fromInsert);
extern void UBTreeFreezeOrInvalidIndexTuples(Buffer buf, int nSlots, const uint8 *slots, bool isFrozen);
extern void UBTreePCRCopyTDSlot(Page origin, Page target);
extern bool UBTreePCRPageAddTuple(Page page, Size itemsize, UBTreeItemId iid, IndexTuple itup, OffsetNumber itup_off,
    bool copyflags, uint8 tdslot);

/*
 * prototypes for functions in ubtpcrsplitloc.cpp
 */
extern OffsetNumber UBTreePCRFindsplitloc(Relation rel, Buffer buf, OffsetNumber newitemoff,
    Size newitemsz, bool* newitemonleft);
extern OffsetNumber UBTreePCRFindsplitlocInsertpt(Relation rel, Buffer buf, OffsetNumber newitemoff, Size newitemsz,
    bool *newitemonleft, IndexTuple newitem);

/*
 * prototypes for functions in ubtpcrrecycle.cpp
 */
extern bool UBTreePCRIsPageHalfDead(Relation rel, BlockNumber blk);
extern bool UBTreePCRLockBranchParent(Relation rel, BlockNumber child, BTStack stack, Buffer *topparent,
    OffsetNumber *topoff, BlockNumber *target, BlockNumber *rightsib);
extern bool UBTreePCRMarkPageHalfDead(Relation rel, Buffer leafbuf, BTStack stack);
extern void UBTreePCRDoReserveDeletion(Page page);
extern bool UBTreePCRReserveForDeletion(Relation rel, Buffer buf);
extern bool UBTreePCRUnlinkHalfDeadPage(Relation rel, Buffer leafbuf, bool *rightsib_empty, BTStack del_blknos);
extern bool UBTreePCRPagePrune(Relation rel, Buffer buf, TransactionId globalFrozenXid, OidRBTree *invisibleParts,
    bool PruneDelete);
extern bool UBTreeIsToastIndex(Relation rel);
extern int ComputeCompactTDCount(int tdCount, bool* frozenTDMap);
extern bool UBTreePCRPruneItem(Page page, OffsetNumber offNum, UBTreeItemId itemid, TransactionId globalFrozenXid,
    bool* frozenTDMap, bool pruneDelete, UBTPCRPruneState prstate, UndoPersistence upersistence);
extern void RecordDeadTuple(UBTPCRPruneState* prstate, OffsetNumber offNum, TransactionId xid);
extern TransactionId GetXidFromTD(Page page, UBTreeItemId itemid, bool* frozenTDMap);
extern void PruneFirstDataKey(Page page, UBTreeItemId itemid, OffsetNumber offNum,
    bool*frozenTDMap, UBTPCRPruneState* prstate, UndoPersistence upersistence);
extern void UBTreePCRPrunePageExecute(Page page, OffsetNumber* deadOff, int deadLen);
extern void CompactTd(Page page, int tdCount, int canCompactCount, bool* frozenTDMap);
extern void FreezeTd(Page page, bool* frozenTDMap, int tdCount);
extern int UBTreePCRItemOffCompare(const void* itemIdp1, const void* itemIdp2);
extern void UBTreePCRPageRepairFragmentation(Relation rel, BlockNumber blkno, Page page);
extern bool UBTreePCRPrunePageOpt(Relation rel, Buffer buf, bool tryDelete, BTStack del_blknos, bool pruneDelete);
extern int UBTreePCRPageDel(Relation rel, Buffer buf, BTStack del_blknos);
extern bool UBTreePCRCanPrune(Relation rel, Buffer buf, TransactionId globalFrozenXid, bool pruneDelete);
extern bool UBTreeFetchLatestChangeFromUndo(IndexTuple itup, UndoRecord* urec,
    UBTreeLatestChangeInfo* uInfo, UndoPersistence upersistence);
extern void UBTreePCRFreezeOrReuseItemId(Page page, uint16 ncompletedXactSlots,
    const uint16 *completedXactSlots, bool isFrozen);
extern bool UBTreePCRTryRecycleEmptyPageInternal(Relation rel);
extern void UBTreePCRTryRecycleEmptyPage(Relation rel);
extern Buffer UBTreePCRGetAvailablePage(Relation rel, UBTRecycleForkNumber forkNumber, UBTRecycleQueueAddress* addr,
    UBTreeGetNewPageStats* stats = NULL);

/*
* Inline functions of ubtree pcr page
*/

extern void UBTreePCRPageIndexTupleDelete(Page page, OffsetNumber offnum);
extern Buffer UBTreePCRGetNewPage(Relation rel, UBTRecycleQueueAddress* addr);


/*
* prototypes for functions in ubtreetd.cpp
*/
extern uint8 UBTreePageReserveTransactionSlot(Relation relation, Buffer buf, TransactionId fxid, 
    UBTreeTD oldTd, TransactionId *minXid);
extern void UBTreePCRHandlePreviousTD(Relation rel, Buffer buf, uint8 *slotNo, UBTreeItemId iid, bool *needRetry);


/*
* Inline functions of ubtree page
*/
bool UBTreeIndexIsPCRType(Relation rel);

extern int UBTreePCRRollback(URecVector *urecvec, int startIdx, int endIdx, TransactionId xid, Oid reloid,
    Oid partitionoid, BlockNumber blkno, bool isFullChain, int preRetCode, Oid *preReloid, Oid *prePartitionoid);
extern bool UBTreePCRIsKeyEqual(Relation idxrel, IndexTuple itup, BTScanInsert itupKey);


#endif /* UBTREE_PCR_H */
