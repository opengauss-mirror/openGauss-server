/* -------------------------------------------------------------------------
 *
 * knl_uheap.h
 * the access interfaces of inplace update engine.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * src/include/access/ustore/knl_uheap.h
 * -------------------------------------------------------------------------
 */

#ifndef KNL_UHEAP_H
#define KNL_UHEAP_H

#include "utils/snapshot.h"
#include "nodes/parsenodes.h"

#include "access/ustore/knl_utuple.h"
#include "access/ustore/knl_upage.h"
#include "access/ustore/knl_uredo.h"
#include "access/ustore/knl_uundovec.h"
#include "access/ustore/undo/knl_uundoxlog.h"
#include "access/ustore/undo/knl_uundozone.h"
#include "access/heapam.h"

/*
 * Threshold for the number of blocks till which non-inplace updates due to
 * reuse of transaction slot. The performance testing on various sizes of tables 
 * indicate that threshold of 200 is good enough to keep the contention on 
 * transaction slots under control.
 */
#define MIN_SAVING_LEN 3

typedef struct UHeapWALInfo {
    Oid relOid;
    Oid partitionOid;
    Oid relfilenode;
    Oid tablespace;
    TransactionId oldXid;  /* xid in old td, i.e. xid of last operation on this tuple */
    TransactionId xid;     /* if rel is logically logged, and operation is under a subxact, xid is the xid of subxact,
                              otherwise, it's InvalidTransactionId */
    UndoRecPtr oldurecadd; /* urecptr in old td, i.e. urecptr of last operation on this tuple */
    Buffer buffer;
    StringInfoData oldUTuple; /* Whole tuple before the logging operation */
    UHeapTuple utuple;         /* whole tuple after the logging operation */
    UHeapTuple toastTuple;     /* whole tuple where TOAST is expanded */
    UndoRecPtr urecptr;        /* urecptr where the current operation's undo is located */
    UndoRecPtr blkprev;        /* byte offset of previous undo for block */
    UndoRecPtr prevurp;        /* urecptr where the previous undo of the current transaction */
    uint16 td_id : 8, locker_td_id : 8; /* old TD and locker TD */
    uint8 flag;                /* flags for partitionOid, hashSubXact, blkprev, prevurp */
    bool hasSubXact;           /* Contains subtransaction */
    undo::XlogUndoMeta *xlum;        /* infomation about undometa, needed in every operation */
    undo::UndoZone *hZone;     /* undo zone pointer, used for update undo zone lsn */
} UHeapWALInfo;

typedef struct UHeapMultiInsertWALInfo {
    UHeapWALInfo *genWalInfo;
    Relation relation;
    UHeapTuple *utuples;
    UHeapFreeOffsetRanges *ufree_offsets;
    int curpage_ntuples;
    int ntuples;
    int ndone;
    UndoRecPtr lastURecptr;
} UHeapMultiInsertWALInfo;

/* Working data for UHeapPagePrune and subroutines */
typedef struct {
    TransactionId new_prune_xid;    /* new prune hint value for page */
    TransactionId latestRemovedXid; /* latest xid to be removed by this prune */
    int ndeleted;                   /* numbers of entries in arrays below */
    int ndead;
    int nunused;
    uint16 nfixed;
    /* arrays that accumulate indexes of items to be changed */

    OffsetNumber nowdeleted[MaxPossibleUHeapTuplesPerPage];
    OffsetNumber nowdead[MaxPossibleUHeapTuplesPerPage];
    OffsetNumber nowunused[MaxPossibleUHeapTuplesPerPage];
    OffsetNumber nowfixed[MaxPossibleUHeapTuplesPerPage];
    uint16 fixedlen[MaxPossibleUHeapTuplesPerPage];
    /* marked[i] is TRUE if item i is entered in one of the above arrays */
    bool marked[MaxPossibleUHeapTuplesPerPage + 1];
} UPruneState;

/* Controls the overall probability of conducting FSM update during page pruning */
#define FSM_UPDATE_HEURISTI_PROBABILITY 0.3

typedef struct {
    Relation relation;
    Buffer buffer;
} RelationBuffer;

Datum UHeapFastGetAttr(UHeapTuple tup, int attnum, TupleDesc tupleDesc, bool *isnull);
Oid UHeapInsert(Relation rel, UHeapTuple tuple, CommandId cid, BulkInsertState bistate = NULL, bool isToast = false);
extern TransactionId UHeapFetchInsertXid(UHeapTuple uhtup, Buffer buffer);
UHeapTuple UHeapPrepareInsert(Relation rel, UHeapTuple tuple, int options);
void RelationPutUTuple(Relation relation, Buffer buffer, UHeapTupleData *tuple);

extern bool TableFetchAndStore(Relation scanRelation, Snapshot snapshot, Tuple tuple, Buffer* userbuf,
                               bool keepBuf, bool keepTup, TupleTableSlot* slot, Relation statsRelation);
TM_Result UHeapUpdate(Relation relation, Relation parentRelation, ItemPointer otid, UHeapTuple newtup, CommandId cid,
    Snapshot crosscheck, Snapshot snapshot, bool wait, TupleTableSlot **oldslot, TM_FailureData *tmfd,
    bool *indexkey_update_flag, Bitmapset **modifiedIdxAttrs, bool allow_inplace_update = true);
extern void PutLinkUpdateTuple(Page page, Item item, RowPtr *lp, Size size);

TM_Result UHeapLockTuple(Relation relation, UHeapTuple tuple, Buffer* buffer,
                           CommandId cid, LockTupleMode mode, LockWaitPolicy waitPolicy, TM_FailureData *tmfd,
                           bool follow_updates, bool eval, Snapshot snapshot,
                           bool isSelectForUpdate = false, bool allowLockSelf = false, bool isUpsert = false, 
                           TransactionId conflictXid = InvalidTransactionId, int waitSec = 0);

UHeapTuple UHeapLockUpdated(CommandId cid, Relation relation,
                                      LockTupleMode lock_mode, ItemPointer tid,
                                      TransactionId priorXmax, Snapshot snapshot, bool isSelectForUpdate = false);
bool UHeapFetchRow(Relation relation, ItemPointer tid, Snapshot snapshot, TupleTableSlot *slot, UHeapTuple utuple);
bool UHeapFetch(Relation relation, Snapshot snapshot, ItemPointer tid, UHeapTuple tuple, Buffer *buf, bool keepBuf,
    bool keepTup, bool* has_cur_xact_write = NULL);
TM_Result UHeapDelete(Relation relation, ItemPointer tid, CommandId cid, Snapshot crosscheck, Snapshot snapshot,
    bool wait, TupleTableSlot** oldslot, TM_FailureData *tmfd, bool changingPart, bool allowDeleteSelf);
void UHeapReserveDualPageTDSlot(Relation relation, Buffer oldbuf, Buffer newbuf, TransactionId fxid,
    UndoRecPtr *oldbufPrevUrecptr, UndoRecPtr *newbufPrevUrecptr, int *oldbufTransSlotId, int *newbufTransSlotId,
    bool *lockReacquired, bool *oldbufLockReacquired, TransactionId *minXidInTDSlots, bool *oldBufLockReleased);
void UHeapMultiInsert(Relation relation, UHeapTuple *tuples, int ntuples, CommandId cid, int options,
    BulkInsertState bistate);
int UHeapPageReserveTransactionSlot(Relation relation, Buffer buf, TransactionId fxid, UndoRecPtr *urecPtr,
    bool *lockReacquired, Buffer otherBuf, TransactionId *minxid, bool aggressiveSearch = true);
bool UHeapPageFreezeTransSlots(Relation relation, Buffer buf, bool *lockReacquired, TD *transinfo,
    Buffer otherBuf);
void UHeapFreezeOrInvalidateTuples(Buffer buf, int nSlots, const int *slots, bool isFrozen);
void UHeapPageSetUndo(Buffer buffer, int transSlotId, TransactionId fxid, UndoRecPtr urecptr);
int UPageGetTDSlotId(Buffer buf, TransactionId fxid, UndoRecPtr *urecAdd);
UHeapTuple ExecGetUHeapTupleFromSlot(TupleTableSlot *slot);
/* in knl_uvacuumlazy.cpp */
extern void LazyVacuumUHeapRel(Relation onerel, VacuumStmt *vacstmt, BufferAccessStrategy bstrategy);

void UHeapResetPreparedUndo();
UndoRecPtr UHeapPrepareUndoInsert(Oid relOid, Oid partitionOid, Oid relfilenode, Oid tablespace,
    UndoPersistence persistence, TransactionId xid, CommandId cid, UndoRecPtr prevurpInOneBlk,
    UndoRecPtr prevurpInOneXact, BlockNumber blk = InvalidBlockNumber, XlUndoHeader *xlundohdr = NULL,
    undo::XlogUndoMeta *xlundometa = NULL);
UndoRecPtr UHeapPrepareUndoMultiInsert(Oid relOid, Oid partitionOid, Oid relfilenode, Oid tablespace,
    UndoPersistence persistence, Buffer buffer, int nranges, TransactionId xid, CommandId cid,
    UndoRecPtr prevurpInOneBlk, UndoRecPtr prevurpInOneXact, URecVector **urecvec_ptr, UndoRecPtr *first_urecptr,
    UndoRecPtr *urpvec, BlockNumber blk = InvalidBlockNumber, XlUndoHeader *xlundohdr = NULL,
    undo::XlogUndoMeta *xlundometa = NULL);
UndoRecPtr UHeapPrepareUndoDelete(Oid relOid, Oid partitionOid, Oid relfilenode, Oid tablespace,
    UndoPersistence persistence, Buffer buffer, OffsetNumber offnum, TransactionId xid, SubTransactionId subxid,
    CommandId cid, UndoRecPtr prevurpInOneBlk, UndoRecPtr prevurpInOneXact, _in_ TD *oldtd, UHeapTuple oldtuple,
    BlockNumber blk = InvalidBlockNumber, XlUndoHeader *xlundohdr = NULL, undo::XlogUndoMeta *xlundometa = NULL);
UndoRecPtr UHeapPrepareUndoUpdate(Oid relOid, Oid partitionOid, Oid relfilenode, Oid tablespace, 
    UndoPersistence persistence, Buffer buffer, Buffer newbuffer, OffsetNumber offnum, TransactionId xid,
    SubTransactionId subxid, CommandId cid, UndoRecPtr prevurpInOneBlk, UndoRecPtr newprevurpInOneBlk,
    UndoRecPtr prevurpInOneXact, _in_ TD *oldtd, UHeapTuple oldtuple, bool isInplaceUpdate, UndoRecPtr *new_urec,
    int undo_xor_delta_size, BlockNumber oldblk = InvalidBlockNumber, BlockNumber newblk = InvalidBlockNumber,
    XlUndoHeader *xlundohdr = NULL, undo::XlogUndoMeta *xlundometa = NULL);
extern bool UHeapPagePruneOptPage(Relation relation, Buffer buffer, TransactionId xid,
    bool acquireConditionalLock = false);
extern int CalTupSize(Relation relation, UHeapDiskTuple tup, TupleDesc tupDesc = NULL);
extern int UHeapPagePrune(Relation rel, const RelationBuffer *relbuf, TransactionId oldest_xmin, bool report_stats,
    TransactionId *latest_removed_xid, bool *pruned);
extern bool UHeapPagePruneOpt(Relation relation, Buffer buffer, OffsetNumber offnum, Size space_required);
extern int UHeapPagePruneGuts(Relation relation, const RelationBuffer *relbuf, TransactionId OldestXmin, OffsetNumber target_offnum,
    Size space_required, bool report_stats, bool forcePrune, TransactionId *latestRemovedXid, bool *pruned);
extern void UHeapPagePruneExecute(Buffer buffer, OffsetNumber target_offnum, const UPruneState *prstate);
extern void UPageRepairFragmentation(Relation rel, Buffer buffer, OffsetNumber target_offnum, Size space_required,
    bool *pruned, bool unused_set);
extern void UHeapPagePruneFSM(Relation relation, Buffer buffer, TransactionId fxid, Page page, BlockNumber blkno);
int UHeapPageGetTDSlotId(Buffer buffer, TransactionId xid, UndoRecPtr *urp);
int UpdateTupleHeaderFromUndoRecord(UndoRecord *urec, UHeapDiskTuple tuple, Page page);
bool UHeapExecPendingUndoActions(Relation rel, Buffer buffer, TransactionId xwait);

extern XLogRecPtr LogUHeapClean(Relation reln, Buffer buffer, OffsetNumber target_offnum, Size space_required,
    OffsetNumber *nowdeleted, int ndeleted, OffsetNumber *nowdead, int ndead, OffsetNumber *nowunused, int nunused,
    OffsetNumber *nowfixed, uint16 *fixedlen, uint16 nfixed, TransactionId latestRemovedXid, bool pruned);

extern void SimpleUHeapDelete(Relation relation, ItemPointer tid, Snapshot snapshot, TupleTableSlot** oldslot = NULL,
    TransactionId* tmfdXmin = NULL);

void UHeapPageShiftBase(Buffer buffer, Page page, bool multi, int64 delta);

void UHeapAbortSpeculative(Relation relation, UHeapTuple tuple);

void UHeapSleepOrWaitForTDSlot(TransactionId xWait, TransactionId myXid, bool isInsert = false);
void UHeapResetWaitTimeForTDSlot();

uint8 UPageExtendTDSlots(Relation relation, Buffer buf);
#endif
