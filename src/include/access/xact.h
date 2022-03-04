/* -------------------------------------------------------------------------
 *
 * xact.h
 *	  postgres transaction system definitions
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * src/include/access/xact.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef XACT_H
#define XACT_H

#include "access/cstore_am.h"
#include "access/xlogreader.h"
#ifdef PGXC
#include "gtm/gtm_c.h"
#endif
#include "nodes/pg_list.h"
#include "storage/smgr/relfilenode.h"
#include "utils/datetime.h"
#include "utils/hsearch.h"
#include "utils/snapshot.h"
#include "utils/plancache.h"
#include "threadpool/threadpool_worker.h"
#include "access/ustore/undo/knl_uundotype.h"

/*
 * Xact isolation levels
 */
#define XACT_READ_UNCOMMITTED 0
#define XACT_READ_COMMITTED 1
#define XACT_REPEATABLE_READ 2
#define XACT_SERIALIZABLE 3

#define SNAPSHOT_UPDATE_NEED_SYNC (1 << 0)
#define SNAPSHOT_NOW_NEED_SYNC (1 << 1)

/*
 * start- and end-of-transaction callbacks for dynamically loaded modules
 */
typedef enum {
    XACT_EVENT_START,               // For MOT, callback will notify us about new transaction.
    XACT_EVENT_COMMIT,
    XACT_EVENT_END_TRANSACTION,
    XACT_EVENT_RECORD_COMMIT,       // For MOT, to write redo and apply changes (after setCommitCsn).
    XACT_EVENT_ABORT,
    XACT_EVENT_PREPARE,
    XACT_EVENT_COMMIT_PREPARED,
    XACT_EVENT_ROLLBACK_PREPARED,
    XACT_EVENT_PREROLLBACK_CLEANUP  // For MOT, to cleanup some internal resources.
} XactEvent;

typedef void (*XactCallback)(XactEvent event, void* arg);

typedef enum {
    SUBXACT_EVENT_START_SUB,
    SUBXACT_EVENT_COMMIT_SUB,
    SUBXACT_EVENT_ABORT_SUB
} SubXactEvent;

typedef void (*SubXactCallback)(SubXactEvent event, SubTransactionId mySubid, SubTransactionId parentSubid, void* arg);

#ifdef PGXC
/*
 * GTM callback events
 */
typedef enum {
    GTM_EVENT_COMMIT,
    GTM_EVENT_ABORT,
    GTM_EVENT_PREPARE
} GTMEvent;

typedef void (*GTMCallback)(GTMEvent event, void* arg);
#endif

/*
 * We implement three isolation levels internally.
 * The two stronger ones use one snapshot per database transaction;
 * the others use one snapshot per statement.
 * Serializable uses predicate locks in addition to snapshots.
 * These macros should be used to check which isolation level is selected.
 */
#define IsolationUsesXactSnapshot() (u_sess->utils_cxt.XactIsoLevel >= XACT_REPEATABLE_READ)
#define IsolationIsSerializable() (u_sess->utils_cxt.XactIsoLevel == XACT_SERIALIZABLE)

extern THR_LOCAL bool TwoPhaseCommit;

typedef enum {
    SYNCHRONOUS_COMMIT_OFF,            /* asynchronous commit */
    SYNCHRONOUS_COMMIT_LOCAL_FLUSH,    /* wait for local flush only */
    SYNCHRONOUS_COMMIT_REMOTE_RECEIVE, /* wait for local flush and remote receive */
    SYNCHRONOUS_COMMIT_REMOTE_WRITE,   /* wait for local flush and remote write */
    SYNCHRONOUS_COMMIT_REMOTE_FLUSH,   /* wait for local and remote flush */
    SYNCHRONOUS_COMMIT_REMOTE_APPLY,  /* wait for local and remote replay */
    SYNCHRONOUS_BAD
} SyncCommitLevel;

/* Define the default setting for synchonous_commit */
#define SYNCHRONOUS_COMMIT_ON SYNCHRONOUS_COMMIT_REMOTE_FLUSH


/* ----------------
 *		transaction-related XLOG entries
 * ----------------
 */

/*
 * XLOG allows to store some information in high 4 bits of log
 * record xl_info field
 */
#define XLOG_XACT_COMMIT 0x00
#define XLOG_XACT_PREPARE 0x10
#define XLOG_XACT_ABORT 0x20
#define XLOG_XACT_COMMIT_PREPARED 0x30
#define XLOG_XACT_ABORT_PREPARED 0x40
#define XLOG_XACT_ASSIGNMENT 0x50
#define XLOG_XACT_COMMIT_COMPACT 0x60
#define XLOG_XACT_ABORT_WITH_XID 0x70

typedef struct xl_xact_assignment {
    TransactionId xtop;    /* assigned XID's top-level XID */
    int nsubxacts;         /* number of subtransaction XIDs */
    TransactionId xsub[1]; /* assigned subxids */
} xl_xact_assignment;

typedef struct xl_xact_origin {
    XLogRecPtr  origin_lsn;
    TimestampTz origin_timestamp;
} xl_xact_origin;

#define MinSizeOfXactAssignment offsetof(xl_xact_assignment, xsub)

typedef struct xl_xact_commit_compact {
    TimestampTz xact_time; /* time of commit */
    uint64 csn;            /* commit sequence number */
    int nsubxacts;         /* number of subtransaction XIDs */
    /* ARRAY OF COMMITTED SUBTRANSACTION XIDs FOLLOWS */
    TransactionId subxacts[FLEXIBLE_ARRAY_MEMBER]; /* VARIABLE LENGTH ARRAY */
} xl_xact_commit_compact;

#define MinSizeOfXactCommitCompact offsetof(xl_xact_commit_compact, subxacts)

typedef struct xl_xact_commit {
    TimestampTz xact_time; /* time of commit */
    uint64 csn;            /* commit sequence number */
    uint64 xinfo;          /* info flags */
    int nrels;             /* number of RelFileNodes */
    int nsubxacts;         /* number of subtransaction XIDs */
    int nmsgs;             /* number of shared inval msgs */
    Oid dbId;              /* u_sess->proc_cxt.MyDatabaseId */
    Oid tsId;              /* u_sess->proc_cxt.MyDatabaseTableSpace */
    int nlibrary;          /* number of library */
    /* Array of ColFileNode(s) to drop at commit */
    ColFileNodeRel xnodes[1]; /* VARIABLE LENGTH ARRAY */
                           /* ARRAY OF COMMITTED SUBTRANSACTION XIDs FOLLOWS */
                           /* ARRAY OF SHARED INVALIDATION MESSAGES FOLLOWS */
                           /* xl_xact_origin if XACT_HAS_ORIGIN present */
} xl_xact_commit;

#define MinSizeOfXactCommit offsetof(xl_xact_commit, xnodes)

/*
 * These flags are set in the xinfo fields of WAL commit records,
 * indicating a variety of additional actions that need to occur
 * when emulating transaction effects during recovery.
 * They are named XactCompletion... to differentiate them from
 * EOXact... routines which run at the end of the original
 * transaction completion.
 */
#define XACT_COMPLETION_UPDATE_RELCACHE_FILE 0x01
#define XACT_COMPLETION_FORCE_SYNC_COMMIT 0x02
#define XACT_MOT_ENGINE_USED 0x04
#define XACT_HAS_ORIGIN 0x08

/* Access macros for above flags */
#define XactCompletionRelcacheInitFileInval(xinfo) (xinfo & XACT_COMPLETION_UPDATE_RELCACHE_FILE)
#define XactCompletionForceSyncCommit(xinfo) (xinfo & XACT_COMPLETION_FORCE_SYNC_COMMIT)
#define XactMOTEngineUsed(xinfo) (xinfo & XACT_MOT_ENGINE_USED)

typedef struct xl_xact_abort {
    TimestampTz xact_time; /* time of abort */
    int nrels;             /* number of RelFileNodes */
    int nsubxacts;         /* number of subtransaction XIDs */
    int nlibrary;          /* number of library */
    /* Array of ColFileNode(s) to drop at abort */
    ColFileNodeRel xnodes[1]; /* VARIABLE LENGTH ARRAY */
                           /* ARRAY OF ABORTED SUBTRANSACTION XIDs FOLLOWS */
} xl_xact_abort;

/* Note the intentional lack of an invalidation message array c.f. commit */

#define MinSizeOfXactAbort offsetof(xl_xact_abort, xnodes)

/*
 * COMMIT_PREPARED and ABORT_PREPARED are identical to COMMIT/ABORT records
 * except that we have to store the XID of the prepared transaction explicitly
 * --- the XID in the record header will be for the transaction doing the
 * COMMIT PREPARED or ABORT PREPARED command.
 */

typedef struct xl_xact_commit_prepared {
    TransactionId xid;   /* XID of prepared xact */
    xl_xact_commit crec; /* COMMIT record */
                         /* MORE DATA FOLLOWS AT END OF STRUCT */
} xl_xact_commit_prepared;

#define MinSizeOfXactCommitPrepared offsetof(xl_xact_commit_prepared, crec.xnodes)

typedef struct xl_xact_abort_prepared {
    TransactionId xid;  /* XID of prepared xact */
    xl_xact_abort arec; /* ABORT record */
                        /* MORE DATA FOLLOWS AT END OF STRUCT */
} xl_xact_abort_prepared;

#define MinSizeOfXactAbortPrepared offsetof(xl_xact_abort_prepared, arec.xnodes)

typedef struct TransactionStateData TransactionStateData;
typedef TransactionStateData* TransactionState;

extern TransactionId NextXidAfterReovery;
extern TransactionId OldestXidAfterRecovery;
extern volatile bool IsPendingXactsRecoveryDone;

typedef struct {
    TransactionId txnId;
    Snapshot snapshot;

    /* Combocid.c */
    HTAB* comboHash;
    void* comboCids; /* ComboCidKey */
    int usedComboCids;
    int sizeComboCids;

    /* xact.c */
    void* CurrentTransactionState; /* TransactionState */
    SubTransactionId subTransactionId;
    SubTransactionId currentSubTransactionId;
    CommandId currentCommandId;
    TimestampTz xactStartTimestamp;
    TimestampTz stmtStartTimestamp;
    TimestampTz xactStopTimestamp;
    TimestampTz GTMxactStartTimestamp;
    TimestampTz stmtSystemTimestamp;

    /* snapmgr.c */
    TransactionId RecentGlobalXmin;
    TransactionId TransactionXmin;
    TransactionId RecentXmin;

    /* procarray.c */
    TransactionId* allDiffXids; /*different xids between GTM and the local */
    uint32 DiffXidsCount;       /*number of different xids between GTM and the local*/
    LocalSysDBCache *lsc_dbcache;
} StreamTxnContext;

#define STCSaveElem(dest, src) ((dest) = (src))
#define STCRestoreElem(dest, src) ((src) = (dest))

#ifdef ENABLE_MOT
typedef void (*RedoCommitCallback)(TransactionId xid, void* arg);
void RegisterRedoCommitCallback(RedoCommitCallback callback, void* arg);
void CallRedoCommitCallback(TransactionId xid);
#endif

typedef enum SavepointStmtType
{
    SUB_STMT_SAVEPOINT,
    SUB_STMT_RELEASE,
    SUB_STMT_ROLLBACK_TO
} SavepointStmtType;

/*
 * savepoint sent state structure
 * It record whether the savepoint cmd has been sent to non-execution cn.
 */
typedef struct SavepointData
{
    char*                cmd;
    char*                name;
    bool                 hasSent;
    SavepointStmtType    stmtType;
    GlobalTransactionId  transactionId;
} SavepointData;

/* ----------------
 *		extern definitions
 * ----------------
 */
extern void InitTopTransactionState(void);
extern void InitCurrentTransactionState(void);
extern bool IsTransactionState(void);
extern bool IsAbortedTransactionBlockState(void);
extern void RemoveFromDnHashTable(void);
extern bool WorkerThreadCanSeekAnotherMission(ThreadStayReason* reason);
extern TransactionId GetTopTransactionId(void);
extern TransactionId GetTopTransactionIdIfAny(void);
extern TransactionId GetCurrentTransactionId(void);
extern TransactionId GetCurrentTransactionIdIfAny(void);
extern GTM_TransactionHandle GetTransactionHandleIfAny(TransactionState s);
extern GTM_TransactionHandle GetCurrentTransactionHandleIfAny(void);
extern TransactionState GetCurrentTransactionState(void);
extern TransactionId GetParentTransactionIdIfAny(TransactionState s);
extern void ResetTransactionInfo(void);
extern void EndParallelWorkerTransaction(void);

#ifdef PGXC /* PGXC_COORD */
extern bool GetCurrentLocalParamStatus(void);
extern void SetCurrentLocalParamStatus(bool status);
extern GlobalTransactionId GetTopGlobalTransactionId(void);
extern void SetTopGlobalTransactionId(GlobalTransactionId gxid);
#endif
extern TransactionId GetStableLatestTransactionId(void);
extern void SetCurrentSubTransactionLocked(void);
extern bool HasCurrentSubTransactionLock(void);
extern ResourceOwner GetCurrentTransactionResOwner(void);
extern SubTransactionId GetCurrentSubTransactionId(void);
extern bool SubTransactionIsActive(SubTransactionId subxid);
extern CommandId GetCurrentCommandId(bool used);
extern bool GetCurrentCommandIdUsed(void);
extern TimestampTz GetCurrentTransactionStartTimestamp(void);
extern TimestampTz GetCurrentStatementStartTimestamp(void);
extern TimestampTz GetCurrentStatementLocalStartTimestamp(void);
extern TimestampTz GetCurrentTransactionStopTimestamp(void);
extern void SetCurrentStatementStartTimestamp();
#ifdef PGXC
extern TimestampTz GetCurrentGTMStartTimestamp(void);
extern TimestampTz GetCurrentStmtsysTimestamp(void);
extern void SetCurrentGTMTimestamp(TimestampTz timestamp);
extern void SetCurrentStmtTimestamp(TimestampTz timestamp);
void SetCurrentStmtTimestamp();
extern void SetCurrentGTMDeltaTimestamp(void);
extern void SetStmtSysGTMDeltaTimestamp(void);
extern void CleanGTMDeltaTimeStamp();
extern void CleanstmtSysGTMDeltaTimeStamp();
#endif
extern int GetCurrentTransactionNestLevel(void);
extern void MarkCurrentTransactionIdLoggedIfAny(void);
extern void CopyTransactionIdLoggedIfAny(TransactionState state);
extern bool TransactionIdIsCurrentTransactionId(TransactionId xid);
extern void CommandCounterIncrement(void);
extern void ForceSyncCommit(void);
extern void StartTransactionCommand(bool STP_rollback = false);
extern void CommitTransactionCommand(bool STP_commit = false);
extern bool CommitSubTransactionExpectionCheck(void);
extern void SaveCurrentSTPTopTransactionState();
extern void RestoreCurrentSTPTopTransactionState();
extern bool IsStpInOuterSubTransaction();
#ifdef PGXC
extern void AbortCurrentTransactionOnce(void);
#endif
extern void AbortCurrentTransaction(bool STP_rollback = false);
extern void AbortSubTransaction(bool STP_rollback = false);
extern void CleanupSubTransaction(bool inSTP = false);
extern void BeginTransactionBlock(void);
extern bool EndTransactionBlock(void);
extern bool PrepareTransactionBlock(const char* gid);
extern void UserAbortTransactionBlock(void);
extern void ReleaseSavepoint(const char* name, bool inSTP);
extern void DefineSavepoint(const char* name);
extern void RollbackToSavepoint(const char* name, bool inSTP);
extern void BeginInternalSubTransaction(const char* name);
extern void ReleaseCurrentSubTransaction(bool inSTP = false);
extern void RollbackAndReleaseCurrentSubTransaction(bool inSTP = false);
extern bool IsSubTransaction(void);
extern bool IsTransactionBlock(void);
extern bool IsTransactionOrTransactionBlock(void);
extern char TransactionBlockStatusCode(void);
extern void AbortOutOfAnyTransaction(bool reserve_topxact_abort = false);
extern void PreventTransactionChain(bool isTopLevel, const char* stmtType);
extern void RequireTransactionChain(bool isTopLevel, const char* stmtType);
extern bool IsInTransactionChain(bool isTopLevel);
extern void RegisterXactCallback(XactCallback callback, void* arg);
extern void UnregisterXactCallback(XactCallback callback, const void* arg);
extern void RegisterSubXactCallback(SubXactCallback callback, void* arg);
extern void UnregisterSubXactCallback(SubXactCallback callback, const void* arg);
extern void CallXactCallbacks(XactEvent event);
extern bool AtEOXact_GlobalTxn(bool commit, bool is_write = false);

#ifdef PGXC
extern void RegisterSequenceCallback(GTMCallback callback, void* arg);
extern void RegisterTransactionNodes(int count, void** connections, bool write);
extern void PrintRegisteredTransactionNodes(void);
extern void ForgetTransactionNodes(void);
extern void RegisterTransactionLocalNode(bool write);
extern void ForgetTransactionLocalNode(void);
extern bool IsXidImplicit(const char* xid);
extern void SaveReceivedCommandId(CommandId cid);
extern void SetReceivedCommandId(CommandId cid);
extern CommandId GetReceivedCommandId(void);
extern void ReportCommandIdChange(CommandId cid);
extern void ReportTopXid(TransactionId local_top_xid);
extern bool IsSendCommandId(void);
extern void SetSendCommandId(bool status);
extern bool IsPGXCNodeXactReadOnly(void);
extern bool IsPGXCNodeXactDatanodeDirect(void);
#endif

extern int xactGetCommittedChildren(TransactionId** ptr);

extern void xact_redo(XLogReaderState* record);
extern void xact_desc(StringInfo buf, XLogReaderState* record);
extern const char *xact_type_name(uint8 subtype);

extern void xactApplyXLogDropRelation(XLogReaderState* record);

extern void StreamTxnContextSaveXact(StreamTxnContext* stc);
extern void StreamTxnContextRestoreXact(StreamTxnContext* stc);
extern void StreamTxnContextSetTransactionState(StreamTxnContext* stc);
extern void StreamTxnContextSetSnapShot(void* snapshotPtr);
extern void StreamTxnContextSetMyPgXactXmin(TransactionId xmin);

extern void WLMTxnContextSetTransactionState();
extern void parseAndRemoveLibrary(char* library, int nlibrary);
extern bool IsInLiveSubtransaction();
extern void ExtendCsnlogForSubtrans(TransactionId parent_xid, int nsub_xid, TransactionId* sub_xids);
extern CommitSeqNo SetXact2CommitInProgress(TransactionId xid, CommitSeqNo csn);
extern void XactGetRelFiles(XLogReaderState* record, ColFileNodeRel** xnodesPtr, int* nrelsPtr);
extern bool XactWillRemoveRelFiles(XLogReaderState *record);
extern HTAB* relfilenode_hashtbl_create();
extern CommitSeqNo getLocalNextCSN();

extern void UpdateNextMaxKnownCSN(CommitSeqNo csn);
extern void XLogInsertStandbyCSNCommitting(TransactionId xid, CommitSeqNo csn,
    TransactionId *children, uint64 nchildren);
#ifdef ENABLE_MOT
extern bool IsMOTEngineUsed();
extern bool IsMOTEngineUsedInParentTransaction();
extern bool IsPGEngineUsed();
extern bool IsMixedEngineUsed();
extern void SetCurrentTransactionStorageEngine(StorageEngineType storageEngineType);
#endif

extern bool XidIsConcurrent(TransactionId xid);
extern void unlink_onefile(RelFileNode node, ForkNumber forknum, Oid ownerid);


extern char* GetSavepointName(List* options);
extern void RecordSavepoint(const char* cmd, const char* name, bool hasSent, SavepointStmtType stmtType);
extern void SendSavepointToRemoteCoordinator();
extern void HandleReleaseOrRollbackSavepoint(const char* cmd, const char* name, SavepointStmtType stmtType);
extern void FreeSavepointList();
extern TransactionState CopyTxnStateByCurrentMcxt(TransactionState state);

extern void SetCurrentTransactionUndoRecPtr(UndoRecPtr urecPtr, UndoPersistence upersistence);
extern UndoRecPtr GetCurrentTransactionUndoRecPtr(UndoPersistence upersistence);
extern void ApplyUndoActions(void);
extern void SetUndoActionsInfo(void);
extern void ResetUndoActionsInfo(void);
extern bool CanPerformUndoActions(void);
extern void push_unlink_rel_to_hashtbl(ColFileNodeRel *xnodes, int nrels);

extern void XactCleanExceptionSubTransaction(SubTransactionId head);
extern char* GetCurrentTransactionName();
extern List* GetTransactionList(List *head);
#endif /* XACT_H */
