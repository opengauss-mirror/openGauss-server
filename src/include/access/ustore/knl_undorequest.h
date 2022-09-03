/* -------------------------------------------------------------------------
 *
 * knl_undorequest.h
 * access interfaces for the async rollback hash
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * opengauss_server/src/include/access/ustore/knl_undorequest.h
 * -------------------------------------------------------------------------
 */


#ifndef KNL_UNDOREQUEST_H
#define KNL_UNDOREQUEST_H

#include "access/ustore/undo/knl_uundotype.h"
#include "access/ustore/knl_uundovec.h"
#include "utils/rel.h"

/* Maximum size of the cache of undo records in a single rollback, 64M. */
const uint64 MAX_UNDO_APPLY_SIZE = 64 * 1024 * 1024;

typedef struct RollbackRequestsHashKey {
    TransactionId xid;
    UndoRecPtr startUndoPtr;
} RollbackRequestsHashKey;

typedef struct RollbackRequestsHashEntry {
    TransactionId xid;
    UndoRecPtr startUndoPtr;
    UndoRecPtr endUndoPtr;
    Oid dbid;
    UndoSlotPtr slotPtr;
    bool launched;
} RollbackRequestsHashEntry;

typedef struct UndoRecInfo {
    int index;       /* Index of the element to make qsort stable. */
    UndoRecPtr urp;  /* undo recptr (undo record location). */
    UndoRecord *uur; /* actual undo record. */
} UndoRecInfo;

typedef struct UndoRelationData {
    Relation rel;
    Partition partition;
    Relation relation; /* dummy relation if rel is partitioned, otherwise same as rel */
    bool isPartitioned;
} UndoRelationData;

typedef enum RollbackReturnType {
    ROLLBACK_OK         = 0,
    ROLLBACK_ERR        = -1,
    ROLLBACK_OK_NOEXIST = -2
} RollbackReturnType;

Size AsyncRollbackRequestsHashSize(void);
Size AsyncRollbackHashShmemSize(void);
void AsyncRollbackHashShmemInit(void);

RollbackRequestsHashEntry *GetNextRollbackRequest();
bool IsRollbackRequestHashFull();
bool AddRollbackRequest(TransactionId xid, UndoRecPtr fromAddr, UndoRecPtr toAddr, Oid dbid, UndoSlotPtr slotPtr);
bool RemoveRollbackRequest(TransactionId xid, UndoRecPtr startAddr, ThreadId pid);
void ReportFailedRollbackRequest(TransactionId xid, UndoRecPtr fromAddr, UndoRecPtr toAddr, Oid dbid);
void ExecuteUndoActions(TransactionId fullXid, UndoRecPtr fromUrecptr, UndoRecPtr toUrecptr, UndoSlotPtr slotPtr,
    bool nopartial);
void ExecuteUndoActionsPage(UndoRecPtr urp, Relation relation, Buffer buf, TransactionId xid);
int UHeapUndoActions(URecVector *urecvector, int startIdx, int endIdx, TransactionId xid, Oid reloid, Oid partitionoid,
    BlockNumber blkno, bool isFullChain, int preRetCode, Oid *preReloid, Oid *prePartitionoid);
void ExecuteUndoForInsert(Relation rel, Buffer buffer, OffsetNumber off, TransactionId xid);
void ExecuteUndoForInsertRecovery(Buffer buffer, OffsetNumber off, TransactionId xid, bool relhasindex, int *tdid);

bool UHeapUndoActionsOpenRelation(Oid reloid, Oid partitionoid, UndoRelationData *relationData);
void UHeapUndoActionsCloseRelation(UndoRelationData *relationData);
bool UHeapUndoActionsFindRelidByRelfilenode(RelFileNode *relfilenode, Oid *reloid, Oid *partitionoid);
#endif
