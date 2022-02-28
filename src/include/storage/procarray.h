/* -------------------------------------------------------------------------
 *
 * procarray.h
 *	  openGauss process array definitions.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/storage/procarray.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef PROCARRAY_H
#define PROCARRAY_H

#include "storage/standby.h"
#include "storage/lock/lwlock.h"
#include "utils/snapshot.h"
#include "pgstat.h"

#define INIT_ROLEID_HASHTBL       512
#define MAX_ROLEID_HASHTBL        1024

typedef struct RoleIdHashEntry {
    Oid roleoid;
    int64 roleNum;
} RoleIdHashEntry;

extern void InitRoleIdHashTable();
extern int GetRoleIdCount(Oid roleoid);
extern int IncreaseUserCount(Oid roleoid);
extern int DecreaseUserCount(Oid roleoid);

extern void SyncLocalXidWait(TransactionId xid);

extern Size ProcArrayShmemSize(void);
extern void CreateSharedProcArray(void);
extern void ProcArrayAdd(PGPROC* proc);
extern void ProcArrayRemove(PGPROC* proc, TransactionId latestXid);

extern Size RingBufferShmemSize(void);
extern void CreateSharedRingBuffer(void);

extern void ProcArrayEndTransaction(PGPROC* proc, TransactionId latestXid, bool isCommit = true);
extern void ProcArrayClearTransaction(PGPROC* proc);
extern bool TransactionIdIsInProgress(TransactionId xid, uint32 *needSync = NULL, bool shortcutByRecentXmin = false,
    bool bCareNextxid = false, bool isTopXact = false, bool checkLatestCompletedXid = true);

#ifdef PGXC /* PGXC_DATANODE */
extern void SetGlobalSnapshotData(
    TransactionId xmin, TransactionId xmax, uint64 scn, GTM_Timeline timeline, bool ss_need_sync_wait_all);
extern void UnsetGlobalSnapshotData(void);
extern void ReloadConnInfoOnBackends(void);
#endif /* PGXC */
extern char dump_memory_context_name[MEMORY_CONTEXT_NAME_LEN];
extern void DumpMemoryCtxOnBackend(ThreadId tid, const char* mem_ctx);
extern void ProcArrayInitRecovery(TransactionId initializedUptoXID);
extern void ProcArrayApplyRecoveryInfo(RunningTransactions running);

extern int GetMaxSnapshotXidCount(void);
extern int GetMaxSnapshotSubxidCount(void);

#ifndef ENABLE_MULTIPLE_NODES
Snapshot GetSnapshotData(Snapshot snapshot, bool force_local_snapshot, bool forHSFeedBack = false);
#else
extern Snapshot GetSnapshotData(Snapshot snapshot, bool force_local_snapshot);
#endif

extern Snapshot GetLocalSnapshotData(Snapshot snapshot);

extern bool ProcArrayInstallImportedXmin(TransactionId xmin, TransactionId sourcexid);
extern RunningTransactions GetRunningTransactionData(void);

extern bool TransactionIdIsActive(TransactionId xid);
extern TransactionId GetOldestCatalogXmin();
extern TransactionId GetRecentGlobalXmin(void);
extern TransactionId GetOldestXmin(Relation rel, bool bFixRecentGlobalXmin = false,
    bool bRecentGlobalXminNoCheck = false);
extern TransactionId GetGlobalOldestXmin(void);
extern TransactionId GetOldestXminForUndo(TransactionId * recycleXmin);
extern void CheckCurrentTimeline(GTM_Timeline timeline);
extern TransactionId GetOldestActiveTransactionId(TransactionId *globalXmin);
extern TransactionId GetOldestSafeDecodingTransactionId(bool catalogOnly);
extern void CheckSnapshotTooOldException(Snapshot snapshot, const char* location);

extern VirtualTransactionId* GetVirtualXIDsDelayingChkpt(int* nvxids);
extern bool HaveVirtualXIDsDelayingChkpt(VirtualTransactionId* vxids, int nvxids);

extern PGPROC* BackendPidGetProc(ThreadId pid);
extern int BackendXidGetPid(TransactionId xid);
extern bool IsBackendPid(ThreadId pid);

extern VirtualTransactionId* GetCurrentVirtualXIDs(
    TransactionId limitXmin, bool excludeXmin0, bool allDbs, int excludeVacuum, int* nvxids);
extern VirtualTransactionId* GetConflictingVirtualXIDs(TransactionId limitXmin, Oid dbOid, XLogRecPtr lsn = 0);
extern ThreadId CancelVirtualTransaction(const VirtualTransactionId& vxid, ProcSignalReason sigmode);

extern bool MinimumActiveBackends(int min);
extern int CountDBBackends(Oid database_oid);
extern int CountDBActiveBackends(Oid database_oid);
extern int CountSingleNodeActiveBackends(Oid databaseOid, Oid userOid);
extern void CancelDBBackends(Oid databaseid, ProcSignalReason sigmode, bool conflictPending);
extern void CancelSingleNodeBackends(Oid databaseOid, Oid userOid, ProcSignalReason sigmode, bool conflictPending);
extern int CountUserBackends(Oid roleid);
extern bool CountOtherDBBackends(Oid databaseId, int* nbackends, int* nprepared);

extern void XidCacheRemoveRunningXids(TransactionId xid, int nxids, const TransactionId* xids, TransactionId latestXid);
extern void SetPgXactXidInvalid(void);

extern void ProcArraySetReplicationSlotXmin(TransactionId xmin, TransactionId catalog_xmin, bool already_locked);

extern void ProcArrayGetReplicationSlotXmin(TransactionId* xmin, TransactionId* catalog_xmin);
extern TransactionId GetGlobal2pcXmin();

extern void CSNLogRecordAssignedTransactionId(TransactionId newXid);

/*
 * Fast search of ProcArray mapping (xid => proc array index),
 * reuse the concurrency control logic of ProcArray
 */
#define InvalidProcessId  -1

typedef struct {
        TransactionId xid;
        int proc_id; /* index of the process in ProcArray */
} ProcXactLookupEntry;

extern Size ProcXactHashTableShmemSize(void);
extern void CreateProcXactHashTable(void);
extern void ProcXactHashTableAdd(TransactionId xid, int procId);
extern void ProcXactHashTableRemove(TransactionId xid);
extern int ProcXactHashTableLookup(TransactionId xid);
/* definition of  ProcXactHashTable end */

#ifdef PGXC
typedef enum {
    SNAPSHOT_UNDEFINED,   /* Coordinator has not sent snapshot or not yet connected */
    SNAPSHOT_LOCAL,       /* Coordinator has instructed Datanode to build up snapshot from the local procarray */
    SNAPSHOT_COORDINATOR, /* Coordinator has sent snapshot data */
    SNAPSHOT_DIRECT,      /* Datanode obtained directly from GTM */
    SNAPSHOT_DATANODE     /* obtained directly from other thread in the same datanode*/
} SnapshotSource;

extern CommitSeqNo set_proc_csn_and_check(
    const char* func, CommitSeqNo csn_min, GTM_SnapshotType gtm_snapshot_type, SnapshotSource from);
#endif

extern void PrintCurrentSnapshotInfo(int logelevel, TransactionId xid, Snapshot snapshot, const char* action);
extern void ProcSubXidCacheClean();
extern void InitProcSubXidCacheContext();
extern void ProcArrayResetXmin(PGPROC* proc);
extern uint64 GetCommitCsn();
extern void setCommitCsn(uint64 commit_csn);
extern void SyncWaitXidEnd(TransactionId xid, Buffer buffer);
extern CommitSeqNo calculate_local_csn_min();
extern void proc_cancel_invalid_gtm_lite_conn();
extern void forward_recent_global_xmin(void);
extern void UpdateXLogMaxCSN(CommitSeqNo xlogCSN);

extern void UpdateCSNLogAtTransactionEND(
    TransactionId xid, int nsubxids, TransactionId* subXids, CommitSeqNo csn, bool isCommit);

#endif /* PROCARRAY_H */

#ifdef ENABLE_UT
extern void ResetProcXidCache(PGPROC* proc, bool needlock);
#endif /* USE_UT */

// For GTT
extern TransactionId ListAllThreadGttFrozenxids(int maxSize, ThreadId *pids, TransactionId *xids, int *n);
extern TransactionId GetReplicationSlotCatalogXmin();
