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

#include "access/ubtree.h"

/*
 * UBtreeIndexType
 * 
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

#define UBTREE_TD_SLOT_INCREMENT_SIZE        2
#define UBTREE_TD_THRESHOLD_FOR_PAGE_SWITCH  32
#define UBTREE_DEFAULT_TD_COUNT              4
#define UBTREE_MAX_TD_COUNT                  128


/*
* TD status for pcr page
*/
#define TD_FROZEN   0x00
#define TD_ACTIVE   0x01
#define TD_COMMITED 0x02
#define TD_DELETE   0x04
#define TD_CSN      0x08

/*
 * UBtreeIndexType
 *    Transaction directory for ubtree pcr page
 */
typedef struct {
    TransactionId xactid;
    union  
    {
        CommitSeqNo csn;
        struct {
            CommandId cid;
            uint32 aligned;
        };
    };
    UndoRecPtr undoRecPtr;
    uint8 tdStatus;
} UBTreeTDData;
typedef UBTreeTDData* UBTreeTD;

/*
* IndexTupleTrxData
*     
*/
typedef struct {
    uint8 tdSlot;           /* slot id */
    uint8 slotIsInvalid: 1; /* slot is reused */
    uint8 isDeleted: 1;     /* index tuple is deleted */
    uint8 aligned: 6;       /* aligned bit*/ 
} IndexTupleTrxData;
typedef IndexTupleTrxData* IndexTupleTrx;

#define UBTreePCRSetIndexTupleTrxInvalid(trx) \
    (((IndexTupleTrx)(trx))->slotIsInvalid = 1)
    
#define UBTreePCRSetIndexTupleDeleted(trx) \
    (((IndexTupleTrx)(trx))->isDeleted = 1)

#define UBTreePCRSetIndexTupleTrxSlot(trx, slot) \
    (((IndexTupleTrx)(trx))->tdSlot = slot)

#define IsUBTreePCRItemDeleted(trx) \
    (((IndexTupleTrx)(trx))->isDeleted == 1)

/*
 * UBTPCRPageOpaqueData 
 *  
 */
typedef struct {
    BlockNumber btpoPrev; /* left sibling, or P_NONE if leftmost */
    BlockNumber btpoNext; /* right sibling, or P_NONE if rightmost */
    union {
        uint32 level;                /* tree level --- zero for leaf pages */
        ShortTransactionId xactOld; /* next transaction ID, if deleted */
    } btpo;
    uint16 btpoFlags;      /* flag bits, see below */
    BTCycleId btpoCycleid; /* vacuum cycle ID of latest split */

    TransactionId lastDeleteXid;
    TransactionId lastCommitXid;
    uint8 tdCount;
    uint16 activeTupleCount;
    uint32 flags; 
} UBTPCRPageOpaqueData;
typedef UBTPCRPageOpaqueData* UBTPCRPageOpaque;

/*
* UBtreeIndexIsPCR
*   True means ubtree is pcr
*/
#define UBTreeIndexIsPCR(relation) \
    ((relation)->rd_ubtreeindextype == UBTREE_PCR)

#define UBTreePageGetTDSlotCount(page) \
    (((UBTPCRPageOpaque)(PageGetSpecialPointer(page)))->tdCount)

#define UBTreePageGetTDPointer(page) \
    ((char *)(page) + SizeOfPageHeaderData)

#define IsValidUBTreeTransationSlotId(slot) \
    (slot != UBTreeInvalidTDSlotId && slot != UBTreeFrozenTDSlotId && slot < UBTREE_MAX_TD_COUNT)

#define SizeOfUBTreeTDData(page) \
    (UBTreePageGetTDSlotCount(page) * sizeof(UBTreeTDData))

#define UBTreePCRGetRowPtrOffset(page) \
    (SizeOfPageHeaderData + SizeOfUBTreeTDData(page))

#define UBTreePCRGetRowPtr(page, offset) \      
    (ItemId)((char*)(page) + UBTreePCRGetRowPtrOffset(page) + sizeof(ItemIdData) * (offset - 1))

#define UBTreePCRGetIndexTuple(page, offset) \  
    ((char*)(page) + ((ItemId)UBTreePCRGetRowPtr(page, offset))->lp_off)

#define UBTreePCRGetIndexTupleTrx(itup) \
    ((char*)(itup) + IndexTupleSize(itup))

#define UBTreePcrPageGetMaxOffsetNumber(page) \
    (((PageHeader)page)->pd_lower - UBTreePCRGetRowPtrOffset(page)) / sizeof(ItemIdData);

#define UBTreePCRMaxItemSize(page) \
    MAXALIGN_DOWN((PageGetPageSize(page) - \
                   MAXALIGN(SizeOfPageHeaderData + \
                            3 * sizeof(ItemIdData) + \
                            3 * sizeof(ItemPointerData) + \
                            3 * sizeof(UBTreeTDData)) - \
                   MAXALIGN(sizeof(UBTPCRPageOpaqueData))) / 3)


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
        maxoff = (upghdr->pd_lower - (SizeOfPageHeaderData + SizeOfUBTreeTDData(upghdr))) / sizeof(ItemIdData);

    return maxoff;
}




/*
 * prototypes for functions in ubtpcrsort.cpp
 */
extern void UBTreePCRLeafBuild(BTSpool *btspool, BTSpool *btspool2);
extern void UBTreePCRBuildAdd(BTWriteState* wstate, BTPageState* state, IndexTuple itup, bool hasxid);
extern void UBTreePCRUpperShutDown(BTWriteState* wstate, BTPageState* state);
extern BTPageState* UBTreePCRPageState(BTWriteState* wstate, uint32 level);


/*
 * prototypes for functions in ubtpcrpage.cpp
 */
extern void UBTreePCRPageInit(Page page, Size size);
extern void UBTreePCRInitMetaPage(Page page, BlockNumber rootbknum, uint32 level);
extern Buffer UBTreePCRGetRoot(Relation rel, int access);
extern bool UBTreePCRPageRecyclable(Page page);
extern int UBTreePCRPageDel(Relation rel, Buffer buf, BTStack del_blknos = NULL);


/*
 * prototypes for functions in ubtpcrundo.cpp
 */
extern void ExecuteUndoActionsForUBTreePage(Relation relation, Buffer buf, uint8 slotID);



/*
 * prototypes for functions in ubtpcrsearch.cpp
 */
extern BTStack UBTreePCRSearch(Relation rel, BTScanInsert key, Buffer *bufP, int access, bool needStack);
extern OffsetNumber UBTreePCRBinarySearch(Relation rel, BTScanInsert key, Buffer buf, bool fixActiveCount);
extern bool UBTreePCRFirst(IndexScanDesc scan, ScanDirection dir);
extern Buffer UBTreePCRMoveRight(Relation rel, BTScanInsert itup_key, Buffer buf, bool forupdate,
    BTStack stack, int access);
extern int32 UBTreePCRCompare(Relation rel, BTScanInsert key, Page page, OffsetNumber offnum, Buffer buf);
extern bool UBTreeCPRFirst(IndexScanDesc scan, ScanDirection dir);
extern bool UBTreePCRNext(IndexScanDesc scan, ScanDirection dir);
extern void UBTreePCRTraceTuple(IndexScanDesc scan, OffsetNumber offnum, bool isVisible, bool isHikey = false);
extern Buffer UBTreePCRGetEndPoint(Relation rel, uint32 level, bool rightmost);
extern bool UBTreePCRGetTupleInternal(IndexScanDesc scan, ScanDirection dir);


/*
 * prototypes for functions in ubtpcrinsert.cpp
 */
extern bool UBTreePCRDoInsert(Relation rel, IndexTuple itup, IndexUniqueCheck checkUnique, Relation heapRel);
extern bool UBTreePCRDoDelete(Relation rel, IndexTuple itup, bool isRollbackIndex);
extern bool UBTreePCRPagePruneOpt(Relation rel, Buffer buf, bool tryDelete, BTStack del_blknos = NULL);
extern bool UBTreePCRPagePrune(Relation rel, Buffer buf, TransactionId oldestXmin, OidRBTree *invisibleParts = NULL);
extern bool UBTreePCRPruneItem(Page page, OffsetNumber offnum, TransactionId oldestXmin, IndexPruneState* prstate,
    bool isToast);
extern void UBTreePCRPagePruneExecute(Page page, OffsetNumber* nowdead, int ndead, IndexPruneState* prstate,
    TransactionId oldest_xmin);
extern void UBTreePCRPageRepairFragmentation(Relation rel, BlockNumber blkno, Page page);

extern void UBTreePCRInsertParent(Relation rel, Buffer buf, Buffer rbuf, BTStack stack, bool is_root, bool is_only);
extern void UBTreePCRFinishSplit(Relation rel, Buffer lbuf, BTStack stack);
extern Buffer UBTreePCRGetStackBuf(Relation rel, BTStack stack);


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
extern UBTRecycleQueueHeader UBTreePCRGetRecycleQueueHeader(Page page, BlockNumber blkno);
extern Buffer UBTreePCRReadRecycleQueueBuffer(Relation rel, BlockNumber blkno);
extern void UBTreePCRInitializeRecycleQueue(Relation rel);
extern void UBTreePCRTryRecycleEmptyPage(Relation rel);
extern void UBTreePCRRecordFreePage(Relation rel, BlockNumber blkno, TransactionId xid);
extern void UBTreePCRRecordEmptyPage(Relation rel, BlockNumber blkno, TransactionId xid);
extern void UBTreePCRRecordUsedPage(Relation rel, UBTRecycleQueueAddress addr);
extern Buffer UBTreePCRGetAvailablePage(Relation rel, UBTRecycleForkNumber forkNumber, UBTRecycleQueueAddress* addr,
    UBTreeGetNewPageStats* stats = NULL);
extern void UBTreePCRRecycleQueueInitPage(Relation rel, Page page, BlockNumber blkno, BlockNumber prevBlkno,
    BlockNumber nextBlkno);
extern void UBtreePCRRecycleQueueChangeChain(Buffer buf, BlockNumber newBlkno, bool setNext);
extern void UBTreePCRRecycleQueuePageChangeEndpointLeftPage(Relation rel, Buffer buf, bool isHead);
extern void UBTreePCRRecycleQueuePageChangeEndpointRightPage(Relation rel, Buffer buf, bool isHead);
extern void UBTreePCRXlogRecycleQueueModifyPage(Buffer buf, xl_ubtree2_recycle_queue_modify *xlrec);
extern uint32 UBTreePCRRecycleQueuePageDump(Relation rel, Buffer buf, bool recordEachItem,
    TupleDesc *tupleDesc, Tuplestorestate *tupstore, uint32 cols);
extern void UBTreePCRDumpRecycleQueueFork(Relation rel, UBTRecycleForkNumber forkNum, TupleDesc *tupDesc,
    Tuplestorestate *tupstore, uint32 cols);
extern void UBTreePCRBuildCallback(Relation index, HeapTuple htup, Datum *values, const bool *isnull, bool tupleIsAlive,
    void *state);

/*
* Inline functions of ubtree pcr page
*/
extern void UBTreePCRPageIndexTupleDelete(Page page, OffsetNumber offnum);
/*
* Inline functions of ubtree page
*/
bool UBTreeIndexIsPCRType(Relation rel);

#endif /* UBTREE_PCR_H */
