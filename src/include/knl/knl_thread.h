/*
 * Portions Copyright (c) 2021, openGauss Contributors
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * ---------------------------------------------------------------------------------------
 *
 * knl_thread.h
 *        Data stucture for thread level global variables.
 *
 *   When anyone try to added variable in this file, which means add a thread level
 *   variable, there are several rules needed to obey:
 *
 *   1. Only used in one thread.
 *        If we try to share the variable with all threads, then this variable should be
 *        added to instance level context rather than thread level context.
 *
 *   2. Session independent.
 *        When we use thread pool to server for all sessions, then the thread in
 *        thread pool should be stateless. So, session related info can not be stored
 *        in thread level context, for instance, client user, password, cached plans,
 *        compiled plpgsql, etc.
 *
 *   3. Transaction level lifecycle.
 *        The thread will only change to another session when one transaction has
 *        already finished in current session. So, we can put the variable in thread
 *        level context if it will be reset at thread transaction finish or start
 *        time.
 *
 *   4. Name pattern
 *         All context define below should follow naming rules:
 *         knl_t_xxxx
 *
 *
 * IDENTIFICATION
 *        src/include/knl/knl_thread.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_KNL_KNL_THRD_H_
#define SRC_INCLUDE_KNL_KNL_THRD_H_

#include <setjmp.h>

#include "c.h"
#include "access/sdir.h"
#include "datatype/timestamp.h"
#include "gs_thread.h"
#include "gssignal/gs_signal.h"
#include "knl/knl_guc.h"
#include "knl/knl_session.h"
#include "nodes/pg_list.h"
#include "storage/lock/s_lock.h"
#include "utils/knl_localsysdbcache.h"
#include "utils/palloc.h"
#include "storage/latch.h"
#include "portability/instr_time.h"
#include "cipher.h"
#include "openssl/ossl_typ.h"
#include "workload/qnode.h"
#include "streaming/init.h"
#include "tcop/dest.h"
#include "streaming/init.h"
#include "utils/memgroup.h"
#include "lib/circularqueue.h"
#include "pgxc/barrier.h"
#include "communication/commproxy_basic.h"
#include "replication/replicainternal.h"
#include "replication/libpqwalreceiver.h"
#include "replication/worker_internal.h"
#include "replication/origin.h"
#include "catalog/pg_subscription.h"
#include "port/pg_crc32c.h"
#define MAX_PATH_LEN 1024

#define RESERVE_SIZE 52
#define PARTKEY_VALUE_MAXNUM 64

typedef struct ResourceOwnerData* ResourceOwner;
typedef struct logicalLog logicalLog;

typedef struct knl_t_codegen_context {
    void* thr_codegen_obj;

    bool g_runningInFmgr;

    long codegen_IRload_thr_count;

} knl_t_codegen_context;

typedef struct knl_t_relopt_context {
    struct relopt_gen** relOpts;

    bits32 last_assigned_kind;

    int num_custom_options;

    struct relopt_gen** custom_options;

    bool need_initialization;

    int max_custom_options;

} knl_t_relopt_context;

typedef struct knl_t_mem_context {
    MemoryContext postmaster_mem_cxt;

    MemoryContext msg_mem_cxt;

    MemoryContext cur_transaction_mem_cxt;

    MemoryContext gs_signal_mem_cxt;

    MemoryContext mask_password_mem_cxt;

    MemoryContext row_desc_mem_cxt;

    /* This is a transient link to the active portal's memory context: */
    MemoryContext portal_mem_cxt;

    /* used to track the memory usage */
    MemoryContext mem_track_mem_cxt;

    /* private memory context for profile logging, it's under t_thrd.top_mem_cxt */
    MemoryContext profile_log_mem_cxt;

    /* gBatchEncodeNumericMemCnxt -- special memory context for encoding batch numeric.
     * It's created under TOP memory context, and destroyed until this thread exits.
     * It's used during encoding of each batch of numeric values, and reset after done.

     * Bacause varlena with 1B maybe exist, we have to free them at the
     * end. the number of LOOP is always 60000 and releasing space is so
     * ineffective. So that use gBatchEncodeNumericMemCnxt.
     * Alloc some times, and reset one time.
     */
    MemoryContext batch_encode_numeric_mem_cxt;

    /* system auditor memory context. */
    MemoryContext pgAuditLocalContext;
} knl_t_mem_context;

#ifdef HAVE_INT64_TIMESTAMP
typedef int64 TimestampTz;
#else

typedef double TimestampTz;
#endif
/*
 * During prepare, the state file is assembled in memory before writing it
 * to WAL and the actual state file.  We use a chain of StateFileChunk blocks
 * for that.
 */
typedef struct StateFileChunk {
    char* data;
    uint32 len;
    struct StateFileChunk* next;
} StateFileChunk;

typedef struct xllist {
    StateFileChunk* head; /* first data block in the chain */
    StateFileChunk* tail; /* last block in chain */
    uint32 num_chunks;
    uint32 bytes_free; /* free bytes left in tail block */
    uint32 total_len;  /* total data bytes in chain */
} xllist;

typedef struct {
    volatile uint64 totalDuration;
    volatile uint64 counter;
    volatile uint64 startTime;
}RedoTimeCost;

typedef enum {
    TIME_COST_STEP_1 = 0,
    TIME_COST_STEP_2,
    TIME_COST_STEP_3,
    TIME_COST_STEP_4,
    TIME_COST_STEP_5,
    TIME_COST_STEP_6,
    TIME_COST_STEP_7,
    TIME_COST_STEP_8,
    TIME_COST_STEP_9,
    TIME_COST_NUM,
} TimeCostPosition;

/*
for extreme rto
thread                step1                     step2                 step3                 step4
               step5                 step6                step7        step8
redo batch          get a record           redo record(total)   update stanbystate        parse xlog
         dispatch to redo manager    null                 null          null
redo manager        get a record           redo record(total)   proc page xlog            redo ddl
         dispatch to redo worker     null                 null          null
redo worker         get a record           redo record(total)   redo page xlog(total)     read xlog page
          redo page xlog          redi other xlog       fsm update   full sync wait
trxn mamanger       get a record           redo record(total)   update flush lsn           wait sync work
         dispatch to trxn worker  global lsn update     null           null
trxn worker         get a record           redo record(total)   redo xlog                 update thread lsn
         full sync wait             null                null          null
read worker         get xlog page(total)   read xlog page       change segment              null
         null                      null                 null          null
read page worker    get a record           make lsn forwarder   get new item              put to dispatch thread
        update thread lsn        crc check             null          null
startup             get a record           check stop           delay redo                 dispatch(total)
         decode                  null                  null          null

for parallel redo
thread     step1            step2               step3                 step4                step5
          step6              step7                  step8                    step9
page redo  get a record    redo record(total)  update stanbystate   redo undo log     redo share trxn log
   redo sync trxn log     redo single log    redo all workers log     redo multi workers log
startup    read a record   check stop          delay redo          dispatch(total)   trxn apply
        force apply wait    null                   null                  null
*/

typedef struct RedoWorkerTimeCountsInfo {
    char *worker_name;
    RedoTimeCost *time_cost;
}RedoWorkerTimeCountsInfo;

typedef struct knl_t_xact_context {
    /* var in transam.cpp */
    typedef uint64 CommitSeqNo;
    typedef int CLogXidStatus;
    typedef uint64 XLogRecPtr;
    /*
     * Single-item cache for results of TransactionIdGetCommitSeqNo.  It's worth
     * having
     * such a cache because we frequently find ourselves repeatedly checking the
     * same XID, for example when scanning a table just after a bulk insert,
     * update, or delete.
     */
    TransactionId cachedFetchCSNXid;
    CommitSeqNo cachedFetchCSN;
    TransactionId latestFetchCSNXid;
    CommitSeqNo latestFetchCSN;

    /* Just use for check set hitbit right */
    /* Just use for check set hitbit right */
    TransactionId latestFetchXid;
    CLogXidStatus latestFetchXidStatus;

    /*
     * Single-item cache for results of TransactionLogFetch.  It's worth having
     * such a cache because we frequently find ourselves repeatedly checking the
     * same XID, for example when scanning a table just after a bulk insert,
     * update, or delete.
     */
    TransactionId cachedFetchXid;
    CLogXidStatus cachedFetchXidStatus;
    XLogRecPtr cachedCommitLSN;

    /* var in multixact.cpp*/
    struct mXactCacheEnt* MXactCache;
    MemoryContext MXactContext;

    /* var in twophase.cpp*/
    /*
     * Global transaction entry currently locked by us, if any.  Note that any
     * access to the entry pointed to by this variable must be protected by
     * TwoPhaseStateLock, though obviously the pointer itself doesn't need to be
     * (since it's just local memory).
     */
    struct GlobalTransactionData* MyLockedGxact;
    bool twophaseExitRegistered;
    TransactionId cached_xid;
    struct GlobalTransactionData* cached_gxact;
    struct TwoPhaseStateData** TwoPhaseState;
    xllist records;

    /* var in varsup.cpp*/
    TransactionId cn_xid;
    TransactionId next_xid;
    bool force_get_xid_from_gtm;
    Oid InplaceUpgradeNextOid;
    /* pointer to "variable cache" in shared memory (set up by shmem.c) */
    struct VariableCacheData* ShmemVariableCache;

    /* var in xact.cpp */
    bool CancelStmtForReadOnly;
    /*
     * MyXactAccessedTempRel is set when a temporary relation is accessed.
     * We don't allow PREPARE TRANSACTION in that case.  (This is global
     * so that it can be set from heapam.c.)
     */
    /* Kluge for 2PC support */
    bool MyXactAccessedTempRel;
    bool MyXactAccessedRepRel;
    bool needRemoveTwophaseState;
    /* Whether in abort transaction procedure */
    bool bInAbortTransaction;
    bool handlesDestroyedInCancelQuery;
    /* if true, we do not unlink dropped col files at xact commit or abort */
    bool xactDelayDDL;
    /*
     * unreportedXids holds XIDs of all subtransactions that have not yet been
     * reported in a XLOG_XACT_ASSIGNMENT record.
     */
#define PGPROC_MAX_CACHED_SUBXIDS 64
    int nUnreportedXids;
    TransactionId unreportedXids[PGPROC_MAX_CACHED_SUBXIDS];
    /*
     * The subtransaction ID and command ID assignment counters are global
     * to a whole transaction, so we do not keep them in the state stack.
     */
    TransactionId currentSubTransactionId;
    CommandId currentCommandId;
    bool currentCommandIdUsed;
    /*
     * Parameters for communication control of Command ID between openGauss nodes.
     * isCommandIdReceived is used to determine of a command ID has been received by a remote
     * node from a Coordinator.
     * sendCommandId is used to determine if a openGauss node needs to communicate its command ID.
     * This is possible for both remote nodes and Coordinators connected to applications.
     * receivedCommandId is the command ID received on Coordinator from remote node or on remote node
     * from Coordinator.
     */
    bool isCommandIdReceived;
    bool sendCommandId;
    CommandId receivedCommandId;

    /*
     * xactStartTimestamp is the value of transaction_timestamp().
     * stmtStartTimestamp is the value of statement_timestamp().
     * xactStopTimestamp is the time at which we log a commit or abort WAL record.
     * These do not change as we enter and exit subtransactions, so we don't
     * keep them inside the TransactionState stack.
     */
    TimestampTz xactStartTimestamp;
    TimestampTz stmtStartTimestamp;
    TimestampTz xactStopTimestamp;

    /*
     * PGXC receives from GTM a timestamp value at the same time as a GXID
     * This one is set as GTMxactStartTimestamp and is a return value of now(), current_transaction().
     * GTMxactStartTimestamp is also sent to each node with gxid and snapshot and delta is calculated locally.
     * GTMdeltaTimestamp is used to calculate current_statement as its value can change
     * during a transaction. Delta can have a different value through the nodes of the cluster
     * but its uniqueness in the cluster is maintained thanks to the global value GTMxactStartTimestamp.
     */
    TimestampTz GTMxactStartTimestamp;
    TimestampTz GTMdeltaTimestamp;
    bool timestamp_from_cn;

    bool XactLocalNodePrepared;
    bool XactReadLocalNode;
    bool XactWriteLocalNode;
    bool XactLocalNodeCanAbort;
    bool XactPrepareSent;
    bool AlterCoordinatorStmt;

    /*
     * Some commands want to force synchronous commit.
     */
    bool forceSyncCommit;
    /*
     * Private context for transaction-abort work --- we reserve space for this
     * at startup to ensure that AbortTransaction and AbortSubTransaction can work
     * when we've run out of memory.
     */
    MemoryContext TransactionAbortContext;
    struct GTMCallbackItem* Seq_callbacks;

    LocalTransactionId lxid;
    TransactionId stablexid;

    /* var in gtm.cpp */
    TransactionId currentGxid;
    struct gtm_conn* conn;

    /* var in slru.cpp */
    typedef int SlruErrorCause;
    SlruErrorCause slru_errcause;
    int slru_errno;

    /* var in predicate.cpp */
    uint32 ScratchTargetTagHash;
    struct LWLock *ScratchPartitionLock;
    /*
     * The local hash table used to determine when to combine multiple fine-
     * grained locks into a single courser-grained lock.
     */
    struct HTAB* LocalPredicateLockHash;
    /*
     * Keep a pointer to the currently-running serializable transaction (if any)
     * for quick reference. Also, remember if we have written anything that could
     * cause a rw-conflict.
     */
    struct SERIALIZABLEXACT* MySerializableXact;
    bool MyXactDidWrite;
    bool applying_subxact_undo;

#ifdef PGXC
    bool useLocalSnapshot;
    /*
     * Hash bucket map of the group.
     * Used only in the DN for DDL operations.
     * Allocated from t_thrd.mem_cxt.top_transaction_mem_cxt.
     */
    uint2 *PGXCBucketMap;
    int    PGXCBucketCnt;
    int    PGXCGroupOid;
    int    PGXCNodeId;
    bool   inheritFileNode;
    bool enable_lock_cancel;
#endif
    TransactionId XactXidStoreForCheck;
    Oid ActiveLobRelid;
    bool isSelectInto;
} knl_t_xact_context;

typedef struct RepairBlockKey RepairBlockKey;
typedef void (*RedoInterruptCallBackFunc)(void);
typedef void (*RedoPageRepairCallBackFunc)(RepairBlockKey key, XLogPhyBlock pblk);

typedef struct knl_t_xlog_context {
#define MAXFNAMELEN 64
    typedef uint32 TimeLineID;
    typedef int HotStandbyState;
    typedef uint32 pg_crc32;
    typedef int RecoveryTargetType;
    typedef int ServerMode;
    typedef uint64 XLogRecPtr;
    typedef uint64 XLogSegNo;
    /*
     * ThisTimeLineID will be same in all backends --- it identifies current
     * WAL timeline for the database system.
     */
    TimeLineID ThisTimeLineID;

    /*
     * Are we doing recovery from XLOG?
     *
     * This is only ever true in the startup process; it should be read as meaning
     * "this process is replaying WAL records", rather than "the system is in
     * recovery mode".  It should be examined primarily by functions that need
     * to act differently when called from a WAL redo function (e.g., to skip WAL
     * logging).  To check whether the system is in recovery regardless of which
     * process you're running in, use RecoveryInProgress() but only after shared
     * memory startup and lock initialization.
     */
    bool InRecovery;
    /* Are we in Hot Standby mode? Only valid in startup process, see xlog.h */

    HotStandbyState standbyState;
    XLogRecPtr LastRec;
    pg_crc32 latestRecordCrc;
    /*
     * During recovery, lastFullPageWrites keeps track of full_page_writes that
     * the replayed WAL records indicate. It's initialized with full_page_writes
     * that the recovery starting checkpoint record indicates, and then updated
     * each time XLOG_FPW_CHANGE record is replayed.
     */
    bool lastFullPageWrites;
    /*
     * Local copy of SharedRecoveryInProgress variable. True actually means "not
     * known, need to check the shared state".
     */
    bool LocalRecoveryInProgress;
    /*
     * Local copy of SharedHotStandbyActive variable. False actually means "not
     * known, need to check the shared state".
     */
    bool LocalHotStandbyActive;

    /*
     * Local state for XLogInsertAllowed():
     *      1: unconditionally allowed to insert XLOG
     *      0: unconditionally not allowed to insert XLOG
     *      -1: must check RecoveryInProgress(); disallow until it is false
     * Most processes start with -1 and transition to 1 after seeing that recovery
     * is not in progress.  But we can also force the value for special cases.
     * The coding in XLogInsertAllowed() depends on the first two of these states
     * being numerically the same as bool true and false.
     */
    int LocalXLogInsertAllowed;

    /*
     * When ArchiveRecoveryRequested is set, archive recovery was requested,
     * ie. recovery.conf file was present. When InArchiveRecovery is set, we are
     * currently recovering using offline XLOG archives. These variables are only
     * valid in the startup process.
     *
     * When ArchiveRecoveryRequested is true, but InArchiveRecovery is false, we're
     * currently performing crash recovery using only XLOG files in pg_xlog, but
     * will switch to using offline XLOG archives as soon as we reach the end of
     * WAL in pg_xlog.
     *
     * When recovery.conf is configed, it means that we will recover from offline XLOG
     * archives, set ArchiveRstoreRequested to true to distinguish archive recovery with
     * other recovery scenarios.
     */
    bool ArchiveRecoveryRequested;
    bool InArchiveRecovery;
    bool ArchiveRestoreRequested;

    /* Was the last xlog file restored from archive, or local? */
    bool restoredFromArchive;

    /* options taken from recovery.conf for archive recovery */
    char* recoveryRestoreCommand;
    char* recoveryEndCommand;
    char* archiveCleanupCommand;
    RecoveryTargetType recoveryTarget;
    bool recoveryTargetInclusive;
    bool recoveryPauseAtTarget;
    TransactionId recoveryTargetXid;
    TimestampTz recoveryTargetTime;
    char* obsRecoveryTargetTime;
    char* recoveryTargetBarrierId;
    char* recoveryTargetName;
    XLogRecPtr recoveryTargetLSN;
    bool isRoachSingleReplayDone;
    /* options taken from recovery.conf for XLOG streaming */
    bool StandbyModeRequested;
    char* PrimaryConnInfo;
    char* TriggerFile;

    /* are we currently in standby mode? */
    bool StandbyMode;

    /* if trigger has been set in reader for any reason, check it in read record */
    bool recoveryTriggered;

    /* if recoveryStopsHere returns true, it saves actual stop xid/time/name here */
    TransactionId recoveryStopXid;
    TimestampTz recoveryStopTime;
    XLogRecPtr recoveryStopLSN;
    char recoveryStopName[MAXFNAMELEN];
    bool recoveryStopAfter;

    /*
     * During normal operation, the only timeline we care about is ThisTimeLineID.
     * During recovery, however, things are more complicated.  To simplify life
     * for rmgr code, we keep ThisTimeLineID set to the "current" timeline as we
     * scan through the WAL history (that is, it is the line that was active when
     * the currently-scanned WAL record was generated).  We also need these
     * timeline values:
     *
     * recoveryTargetTLI: the desired timeline that we want to end in.
     *
     * recoveryTargetIsLatest: was the requested target timeline 'latest'?
     *
     * expectedTLIs: an integer list of recoveryTargetTLI and the TLIs of
     * its known parents, newest first (so recoveryTargetTLI is always the
     * first list member).  Only these TLIs are expected to be seen in the WAL
     * segments we read, and indeed only these TLIs will be considered as
     * candidate WAL files to open at all.
     *
     * curFileTLI: the TLI appearing in the name of the current input WAL file.
     * (This is not necessarily the same as ThisTimeLineID, because we could
     * be scanning data that was copied from an ancestor timeline when the current
     * file was created.)  During a sequential scan we do not allow this value
     * to decrease.
     */
    TimeLineID recoveryTargetTLI;
    bool recoveryTargetIsLatest;
    List* expectedTLIs;
    TimeLineID curFileTLI;

    /*
     * ProcLastRecPtr points to the start of the last XLOG record inserted by the
     * current backend.  It is updated for all inserts.  XactLastRecEnd points to
     * end+1 of the last record, and is reset when we end a top-level transaction,
     * or start a new one; so it can be used to tell if the current transaction has
     * created any XLOG records.
     */
    XLogRecPtr ProcLastRecPtr;

    XLogRecPtr XactLastRecEnd;

    /* record end of last commit record, used for subscription */
    XLogRecPtr XactLastCommitEnd;

    /*
     * RedoRecPtr is this backend's local copy of the REDO record pointer
     * (which is almost but not quite the same as a pointer to the most recent
     * CHECKPOINT record).  We update this from the shared-memory copy,
     * XLogCtl->Insert.RedoRecPtr, whenever we can safely do so (ie, when we
     * hold the Insert lock).  See XLogInsertRecord for details.    We are also
     * allowed to update from XLogCtl->Insert.RedoRecPtr if we hold the info_lck;
     * see GetRedoRecPtr.  A freshly spawned backend obtains the value during
     * InitXLOGAccess.
     */
    XLogRecPtr RedoRecPtr;

    /*
     * doPageWrites is this backend's local copy of (forcePageWrites ||
     * fullPageWrites).  It is used together with RedoRecPtr to decide whether
     * a full-page image of a page need to be taken.
     */
    bool doPageWrites;

    /*
     * RedoStartLSN points to the checkpoint's REDO location which is specified
     * in a backup label file, backup history file or control file. In standby
     * mode, XLOG streaming usually starts from the position where an invalid
     * record was found. But if we fail to read even the initial checkpoint
     * record, we use the REDO location instead of the checkpoint location as
     * the start position of XLOG streaming. Otherwise we would have to jump
     * backwards to the REDO location after reading the checkpoint record,
     * because the REDO record can precede the checkpoint record.
     */
    XLogRecPtr RedoStartLSN;
    ServerMode server_mode;
    bool is_cascade_standby;
    bool is_hadr_main_standby;

    /* Flags to tell if we are in an startup process */
    bool startup_processing;

    /*
     * openLogFile is -1 or a kernel FD for an open log file segment.
     * When it's open, openLogOff is the current seek offset in the file.
     * openLogSegNo identifies the segment.  These variables are only
     * used to write the XLOG, and so will normally refer to the active segment.
     */
    int openLogFile;
    XLogSegNo openLogSegNo;
    uint32 openLogOff;

    /*
     * These variables are used similarly to the ones above, but for reading
     * the XLOG.  Note, however, that readOff generally represents the offset
     * of the page just read, not the seek position of the FD itself, which
     * will be just past that page. readLen indicates how much of the current
     * page has been read into readBuf, and readSource indicates where we got
     * the currently open file from.
     */
    int readFile;
    XLogSegNo readSegNo;
    uint32 readOff;
    uint32 readLen;
    unsigned int readSource; /* XLOG_FROM_* code */

    /*
     * Keeps track of which sources we've tried to read the current WAL
     * record from and failed.
     */
    unsigned int failedSources; /* OR of XLOG_FROM_* codes */
    /*
     * These variables track when we last obtained some WAL data to process,
     * and where we got it from.  (XLogReceiptSource is initially the same as
     * readSource, but readSource gets reset to zero when we don't have data
     * to process right now.)
     */
    TimestampTz XLogReceiptTime;
    int XLogReceiptSource; /* XLOG_FROM_* code */

    /* State information for XLOG reading */
    XLogRecPtr ReadRecPtr; /* start of last record read */
    XLogRecPtr EndRecPtr;  /* end+1 of last record read */

    XLogRecPtr minRecoveryPoint; /* local copy of
                                  * ControlFile->minRecoveryPoint */
    bool updateMinRecoveryPoint;

    /*
     * Have we reached a consistent database state? In crash recovery, we have
     * to replay all the WAL, so reachedConsistency is never set. During archive
     * recovery, the database is consistent once minRecoveryPoint is reached.
     */
    bool reachedConsistency;

    bool InRedo;
    bool RedoDone;

    /* Have we launched bgwriter during recovery? */
    bool bgwriterLaunched;
    bool pagewriter_launched;

    /* Added for XLOG scaling*/
    /* For WALInsertLockAcquire/Release functions */
    int MyLockNo;
    bool holdingAllLocks;

    int lockToTry;
    uint64 cachedPage;
    char* cachedPos;
#ifdef WIN32
    unsigned int deletedcounter;
#endif

#define STR_TIME_BUF_LEN 128
    char buf[STR_TIME_BUF_LEN];
    XLogRecPtr receivedUpto;
    XLogRecPtr lastComplaint;
    bool failover_triggered;
    bool switchover_triggered;

    struct registered_buffer* registered_buffers;
    int max_registered_buffers;  /* allocated size */
    int max_registered_block_id; /* highest block_id + 1
                                  * currently registered */

    /*
     * A chain of XLogRecDatas to hold the "main data" of a WAL record, registered
     * with XLogRegisterData(...).
     */
    struct XLogRecData* mainrdata_head;
    struct XLogRecData* mainrdata_last;
    uint32 mainrdata_len; /* total # of bytes in chain */

    /*
     * These are used to hold the record header while constructing a record.
     * 'hdr_scratch' is not a plain variable, but is palloc'd at initialization,
     * because we want it to be MAXALIGNed and padding bytes zeroed.
     *
     * For simplicity, it's allocated large enough to hold the headers for any
     * WAL record.
     */
    struct XLogRecData* ptr_hdr_rdt;
    char* hdr_scratch;

    /*
     * An array of XLogRecData structs, to hold registered data.
     */
    struct XLogRecData* rdatas;
    int num_rdatas; /* entries currently used */
    int max_rdatas; /* allocated size */

    bool begininsert_called;

    /* Should te in-progress insertion log the origin */
    bool include_origin;

    /* Memory context to hold the registered buffer and data references. */
    MemoryContext xloginsert_cxt;

    struct HTAB* invalid_page_tab;

    struct HTAB* remain_segs;

    /* state maintained across calls */
    uint32 sendId;
    int sendFile;
    XLogSegNo sendSegNo;
    uint32 sendOff;
    TimeLineID sendTLI;

    List* incomplete_actions;

    MemoryContext spg_opCtx;
    MemoryContext gist_opCtx;
    MemoryContext gin_opCtx;

    /*
     * Statistics for current checkpoint are collected in this global struct.
     * Because only the checkpointer or a stand-alone backend can perform
     * checkpoints, this will be unused in normal backends.
     */
    struct CheckpointStatsData* CheckpointStats;
    struct XLogwrtResult* LogwrtResult;
    struct XLogwrtPaxos* LogwrtPaxos;
    bool needImmediateCkp;
    int redoItemIdx;
    /* ignore cleanup when startup end. when isIgoreCleanup is true, a standby DN always keep isIgoreCleanup true */
    bool        forceFinishHappened;
    uint32      invaildPageCnt;
    uint32      imcompleteActionCnt;
    XLogRecPtr  max_page_flush_lsn;
    bool        permit_finish_redo;

    /* redo RM_STANDBY_ID record committing csn's transaction id */
    List* committing_csn_list;

    RedoInterruptCallBackFunc redoInterruptCallBackFunc;
    RedoPageRepairCallBackFunc redoPageRepairCallBackFunc;

    void *xlog_atomic_op;
    /* Record current xlog lsn to avoid pass parameter to underlying functions level-to-level */
    XLogRecPtr current_redo_xlog_lsn;
    /* for switchover failed when load xlog record invalid retry count */
    int currentRetryTimes;
    RedoTimeCost timeCost[TIME_COST_NUM];
} knl_t_xlog_context;

typedef struct knl_t_dfs_context {
    /*
     * Provide a pending-delete like mechanism to allow external routines to release the
     * memory stuffs that were NOT allocated in mem-context e.g. those allocated in
     * 3rd-party libraries, we have to do so as a none-dfs error-out may miss dropping
     * this kind of resources which defintely causing memory leak.
     */
    List* pending_free_reader_list;

    List* pending_free_writer_list;

} knl_t_dfs_context;

typedef struct knl_t_obs_context {
    MemoryContext ObsMemoryContext;

    char* pCAInfo;

    int retrySleepInterval;

    int statusG;

#define ERROR_DETAIL_LEN 4096
    char errorDetailsG[ERROR_DETAIL_LEN];

    int uriStyleG;
} knl_t_obs_context;

typedef struct knl_t_cbm_context {
    /* The xlog parsing and bitmap output struct instance */
    struct XlogBitmapStruct* XlogCbmSys;

    /* cbmwriter.cpp */
    /* Flags set by interrupt handlers for later service in the main loop. */
    volatile sig_atomic_t got_SIGHUP;
    volatile sig_atomic_t shutdown_requested;
    MemoryContext cbmwriter_context;
    MemoryContext cbmwriter_page_context;
} knl_t_cbm_context;

/* thread local pointer to the shared memory */
typedef struct knl_t_shemem_ptr_context {
    struct ss_scan_locations_t* scan_locations;
    struct SlruCtlData* MultiXactOffsetCtl;
    struct SlruCtlData* MultiXactMemberCtl;
    struct MultiXactStateData* MultiXactState;
    MultiXactId* OldestMemberMXactId;
    MultiXactId* OldestVisibleMXactId;

    struct SlruCtlData* ClogCtl;

    struct SlruCtlData* CsnlogCtlPtr;

    struct XLogCtlData* XLogCtl;

    union WALInsertLockPadded **GlobalWALInsertLocks;
    union WALInsertLockPadded *LocalGroupWALInsertLocks;

    /*
     * We maintain an image of pg_control in shared memory.
     */
    struct ControlFileData* ControlFile;

    struct LsnXlogFlushData* g_LsnXlogFlushChkFile;

    struct SlruCtlData* OldSerXidSlruCtl;

    struct OldSerXidControlData* oldSerXidControl;

    /*
     * When the oldest committed transaction on the "finished" list is moved to
     * SLRU, its predicate locks will be moved to this "dummy" transaction,
     * collapsing duplicate targets.  When a duplicate is found, the later
     * commitSeqNo is used.
     */
    struct SERIALIZABLEXACT* OldCommittedSxact;

    /*
     * This provides a list of objects in order to track transactions
     * participating in predicate locking.    Entries in the list are fixed size,
     * and reside in shared memory.  The memory address of an entry must remain
     * fixed during its lifetime.  The list will be protected from concurrent
     * update externally; no provision is made in this code to manage that.  The
     * number of entries in the list, and the size allowed for each entry is
     * fixed upon creation.
     */
    struct PredXactListData* PredXact;

    /*
     * This provides a pool of RWConflict data elements to use in conflict lists
     * between transactions.
     */
    struct RWConflictPoolHeaderData* RWConflictPool;
    /*
     * The predicate locking hash tables are in shared memory.
     * Each backend keeps pointers to them.
     */
    struct HTAB* SerializableXidHash;
    struct HTAB* PredicateLockTargetHash;
    struct HTAB* PredicateLockHash;
    struct SHM_QUEUE* FinishedSerializableTransactions;

    /* ------------------------------------------------------------
     * Functions for management of the shared-memory PgBackendStatus array
     * ------------------------------------------------------------
     */
    PgBackendStatus* BackendStatusArray;
    char* BackendClientHostnameBuffer;
    char* BackendAppnameBuffer;
    char* BackendConninfoBuffer;
    char* BackendActivityBuffer;
    struct PgStat_WaitCountStatus* WaitCountBuffer;
    Size BackendActivityBufferSize;

    PgBackendStatus* MyBEEntry;

    volatile struct SessionLevelStatistic* mySessionStatEntry;
    struct SessionLevelStatistic* sessionStatArray;
    struct SessionLevelMemory* mySessionMemoryEntry;
    struct SessionLevelMemory* sessionMemoryArray;
    volatile struct SessionTimeEntry* mySessionTimeEntry;
    struct SessionTimeEntry* sessionTimeArray;

    struct ProcSignalSlot* ProcSignalSlots;

    volatile struct ProcSignalSlot* MyProcSignalSlot;  // volatile

    struct PGShmemHeader* ShmemSegHdr; /* shared mem segment header */

    void* ShmemBase; /* start address of shared memory */

    void* ShmemEnd; /* end+1 address of shared memory */

    slock_t* ShmemLock; /* spinlock for shared memory and LWLock allocation */

    struct HTAB* ShmemIndex; /* primary index hashtable for shmem */

    struct SISeg* shmInvalBuffer; /* pointer to the shared inval buffer */

    struct PMSignalData* PMSignalState;  // volatile

    /*
     * This points to the array of LWLocks in shared memory.  Backends inherit
     * the pointer by fork from the postmaster (except in the EXEC_BACKEND case,
     * where we have special measures to pass it down).
     */
    union LWLockPadded *mainLWLockArray;

    // for GTT table to track sessions and their usage of GTTs
    struct gtt_ctl_data* gtt_shared_ctl;
    struct HTAB* active_gtt_shared_hash;

    struct HTAB* undoGroupLinkMap;
    /* Maintain an image of DCF paxos index file */
    struct DCFData *dcfData;
} knl_t_shemem_ptr_context;

typedef struct knl_t_cstore_context {
    /*
     * remember all the relation ALTER TABLE SET DATATYPE is applied to.
     * note that this object is live in t_thrd.top_mem_cxt.
     */
    class CStoreAlterRegister* gCStoreAlterReg;

    /* bulkload_memsize_used
     * Remember how many memory has been allocated and used.
     * It will be <= partition_max_cache_size.
     */
    Size bulkload_memsize_used;

    int cstore_prefetch_count;

    /* local state for aio clean up resource  */
    struct AioDispatchCUDesc** InProgressAioCUDispatch;
    int InProgressAioCUDispatchCount;
} knl_t_cstore_context;

typedef struct knl_t_undo_context {
    int zids[UNDO_PERSISTENCE_LEVELS];
    TransactionId prevXid[UNDO_PERSISTENCE_LEVELS];
    void *slots[UNDO_PERSISTENCE_LEVELS];
    uint64 slotPtr[UNDO_PERSISTENCE_LEVELS];
    uint64 transUndoSize;
    bool fetchRecord;
} knl_t_undo_context;

typedef struct knl_t_index_context {
    typedef uint64 XLogRecPtr;
    struct ginxlogInsertDataInternal* ptr_data;
    struct ginxlogInsertEntry* ptr_entry;
    /*
     * ginInsertCtx -- static thread local memory context for gininsert().
     * ginInsertCtx is to avoid create and destory context frequently.
     * It's created under TOP memory context, and destroyed until this thread exits.
     * It's used during insert one tuple into gin index, and reset after done.
     */
    MemoryContext ginInsertCtx;
    struct BTVacInfo* btvacinfo;
} knl_t_index_context;

typedef struct knl_t_wlmthrd_context {
    /* thread level node group */
    struct WLMNodeGroupInfo* thread_node_group;

    /* dynamic workload client structure */
    struct ClientDynamicManager* thread_climgr;

    /* dynamic workload server structure */
    struct ServerDynamicManager* thread_srvmgr;

    /* collect info */
    struct WLMCollectInfo* collect_info;

    /* exception control manager */
    struct ExceptionManager* except_ctl;

    /* dn cpu info detail */
    struct WLMDNRealTimeInfoDetail* dn_cpu_detail;

    /* query node for workload manager */
    WLMQNodeInfo qnode;

    /* the states of query while doing parallel control */
    ParctlState parctl_state;

    /* indicate if it is alrm pending */
    bool wlmalarm_pending;

    /* indicate if alarm timeout is active */
    bool wlmalarm_timeout_active;

    /* indicate if alarm dump is active */
    bool wlmalarm_dump_active;

    /* indicate if transcation is started */
    bool wlm_xact_start;

    /* thread initialization has been finished */
    bool wlm_init_done;

    /*check if current stmt has recorded cursor*/
    bool has_cursor_record;

    /* alarm finish time */
    TimestampTz wlmalarm_fin_time;

    /* latch for wlm */
    Latch wlm_mainloop_latch;

    /* got sigterm signal */
    int wlm_got_sigterm;

    /* wlm thread is got sighup signal */
    int wlm_got_sighup;

    MemoryContext MaskPasswordMemoryContext;
    MemoryContext query_resource_track_mcxt;
} knl_t_wlmthrd_context;

#define RANDOM_LEN 16
#define NUMBER_OF_SAVED_DERIVEKEYS 48

typedef struct knl_t_aes_context {
    /* Save several used derive_keys, random_salt and user_key in one thread. */
    bool encryption_function_call;
    GS_UCHAR derive_vector_saved[RANDOM_LEN];
    GS_UCHAR mac_vector_saved[RANDOM_LEN];
    GS_UCHAR input_saved[RANDOM_LEN];

    /*
     * Thread-local variables including random_salt, derive_key and key
     * will be saved during the thread.
     */
    GS_UCHAR random_salt_saved[RANDOM_LEN];

    /* Save several used derive_keys, random_salt and user_key in one thread. */
    bool decryption_function_call;
    GS_UCHAR derive_vector_used[NUMBER_OF_SAVED_DERIVEKEYS][RANDOM_LEN];
    GS_UCHAR mac_vector_used[NUMBER_OF_SAVED_DERIVEKEYS][RANDOM_LEN];
    GS_UCHAR random_salt_used[NUMBER_OF_SAVED_DERIVEKEYS][RANDOM_LEN];
    GS_UCHAR user_input_used[NUMBER_OF_SAVED_DERIVEKEYS][RANDOM_LEN];
    GS_UINT64 decryption_count;

    /*
     * The usage_frequency is used to decided which random_salt to start comparing with
     * The usage_frequency is based on the recent using times of derive_key
     */
    GS_UINT32 usage_frequency[NUMBER_OF_SAVED_DERIVEKEYS];
    /*
     * The insert_position is used to seperate two different region for usage_frequency
     * From 0 to NUMBER_OF_SAVED_DERIVEKEYS/2 -1 , there are derive_keys used many times.
     * From NUMBER_OF_SAVED_DERIVEKEYS/2 to NUMBER_OF_SAVED_DERIVEKEYS -1,
     * these derive_keys were used only one time.
     * Therefore, the newborn derive_key will be saved in insert_position.
     */
    GS_UINT32 insert_position;

    /* use saved random salt unless unavailable*/
    GS_UCHAR gs_random_salt_saved[RANDOM_LEN];
    bool random_salt_tag;
    GS_UINT64 random_salt_count;
} knl_t_aes_context;

typedef struct knl_t_time_context {
    /* Set at postmaster start */
    TimestampTz pg_start_time;

    /* Set at configuration reload */
    TimestampTz pg_reload_time;

    TimestampTz stmt_system_timestamp;

    bool is_abstimeout_in;

} knl_t_time_context;

/* We provide a small stack of ErrorData records for re-entrant cases */
#define ERRORDATA_STACK_SIZE 5

/* buffers for formatted timestamps that might be used by both
 * log_line_prefix and csv logs.
 */
#define FORMATTED_TS_LEN 128

typedef struct knl_t_log_context {
    /* switch a new plog message every 1s */
    struct timeval plog_msg_switch_tm;

    /* for magnetic disk */
    char* plog_md_read_entry;

    char* plog_md_write_entry;

    /* for OBS */
    char* plog_obs_list_entry;

    char* plog_obs_read_entry;

    char* plog_obs_write_entry;

    /* for Hadoop */
    char* plog_hdp_read_entry;

    char* plog_hdp_write_entry;

    char* plog_hdp_open_entry;

    /* for remote datanode */
    char* plog_remote_read_entry;

    char*** g_plog_msgmem_array;

    /* for elog.cpp */
    struct ErrorContextCallback* error_context_stack;

    struct FormatCallStack* call_stack;

    sigjmp_buf* PG_exception_stack;

    char** thd_bt_symbol;

    bool flush_message_immediately;

    int Log_destination;

    bool disable_log_output;

    bool on_mask_password;

    bool openlog_done;

    bool error_with_nodename;

    struct ErrorData* errordata;

    struct LogControlData* pLogCtl;

    int errordata_stack_depth; /* index of topmost active frame */

    int recursion_depth; /* to detect actual recursion */

    char formatted_start_time[FORMATTED_TS_LEN];

    char formatted_log_time[FORMATTED_TS_LEN];

    int save_format_errnumber;

    const char* save_format_domain;

    unsigned long syslog_seq;

    /* static counter for line numbers */
    long log_line_number;

    /* has counter been reset in current process? */
    ThreadId log_my_pid;

    /* static counter for line numbers */
    long csv_log_line_number;

    /* has counter been reset in current process? */
    ThreadId csv_log_my_pid;

    /*
     * msgbuf is declared as static to save the data to put
     * which can be flushed in next put_line()
     */
    struct StringInfoData* msgbuf;

    unsigned char* module_logging_configure;

} knl_t_log_context;

typedef struct knl_t_format_context {
    bool all_digits;

    struct DCHCacheEntry* DCH_cache;

    /* number of entries */
    int n_DCH_cache;

    int DCH_counter;

    struct NUMCacheEntry* NUM_cache;

    /* number of entries */
    int n_NUM_cache;

    int NUM_counter;

    /* see `EarlyBindingTLSVariables` */
    struct NUMCacheEntry* last_NUM_cache_entry;
} knl_t_format_context;

typedef struct knl_t_audit_context {
    bool Audit_delete;
    /* for only sessionid needed by SDBSS */
    TimestampTz user_login_time;
    volatile sig_atomic_t need_exit;
    volatile sig_atomic_t got_SIGHUP;
    volatile sig_atomic_t rotation_requested;
    uint64 space_beyond_size; /* The static variable for print log when exceeding the space limit */
    uint64 pgaudit_totalspace;
    int64 next_rotation_time;
    bool pipe_eof_seen;
    bool rotation_disabled;
    FILE *sysauditFile;
    FILE *policyauditFile;
    Latch sysAuditorLatch;
    time_t last_pgaudit_start_time;
    struct AuditIndexTableNew* audit_indextbl;
    char pgaudit_filepath[MAXPGPATH];

    int cur_thread_idx;
#define NBUFFER_LISTS 256
    List* buffer_lists[NBUFFER_LISTS];
} knl_stat_context;

typedef struct knl_t_async_context {
    struct AsyncQueueControl* asyncQueueControl;

    /*
     * listenChannels identifies the channels we are actually listening to
     * (ie, have committed a LISTEN on).  It is a simple list of channel names,
     * allocated in t_thrd.top_mem_cxt.
     */
    List* listenChannels;       /* list of C strings */
    List* pendingActions;       /* list of ListenAction */
    List* upperPendingActions;  /* list of upper-xact lists */
    List* pendingNotifies;      /* list of Notifications */
    List* upperPendingNotifies; /* list of upper-xact lists */

    /* True if we've registered an on_shmem_exit cleanup */
    bool unlistenExitRegistered;
    /* True if we're currently registered as a listener in t_thrd.asy_cxt.asyncQueueControl */
    bool amRegisteredListener;
    /* has this backend sent notifications in the current transaction? */
    bool backendHasSentNotifications;
} knl_t_async_context;

typedef struct knl_t_explain_context {
    int explain_perf_mode;
    bool explain_light_proxy;
} knl_t_explain_context;

typedef struct knl_t_arch_context {
    time_t last_pgarch_start_time;
    time_t last_sigterm_time;
    /*
     * Flags set by interrupt handlers for later service in the main loop.
     */
    volatile sig_atomic_t got_SIGHUP;
    volatile sig_atomic_t got_SIGTERM;
    volatile sig_atomic_t wakened;
    volatile sig_atomic_t ready_to_stop;
    /*
     * Latch used by signal handlers to wake up the sleep in the main loop.
     */
    Latch mainloop_latch;

    XLogRecPtr pitr_task_last_lsn;
    TimestampTz arch_start_timestamp;
    XLogRecPtr arch_start_lsn;
    /* millsecond */
    int task_wait_interval;
    int sync_walsender_idx;
#ifndef ENABLE_MULTIPLE_NODES
    int sync_follower_id;
#endif
    /* for standby millsecond*/
    long last_arch_time;
    char *slot_name;
    volatile int slot_tline;
    int slot_idx;
    int archive_task_idx;
    struct ArchiveSlotConfig* archive_config;
} knl_t_arch_context;

typedef struct knl_t_barrier_arch_context {
    volatile sig_atomic_t ready_to_stop;
    volatile sig_atomic_t got_SIGHUP;
    volatile sig_atomic_t got_SIGTERM;
    volatile sig_atomic_t wakened;
    char* slot_name;
    char barrierName[MAX_BARRIER_ID_LENGTH];
    XLogRecPtr lastArchiveLoc;
}knl_t_barrier_arch_context;


/* Maximum length of a timezone name (not including trailing null) */
#define TZ_STRLEN_MAX 255
#define LOG_MAX_NODENAME_LEN 64

typedef struct knl_t_logger_context {
    int64 next_rotation_time;
    bool pipe_eof_seen;
    bool rotation_disabled;
    FILE* syslogFile;
    FILE* csvlogFile;
    FILE* querylogFile;
    char* last_query_log_file_name;
    FILE* asplogFile;
    int64 first_syslogger_file_time;
    char* last_file_name;
    char* last_csv_file_name;
    char* last_asp_file_name;
    Latch sysLoggerLatch;

#define NBUFFER_LISTS 256
    List* buffer_lists[NBUFFER_LISTS];
    volatile sig_atomic_t got_SIGHUP;
    volatile sig_atomic_t rotation_requested;
} knl_t_logger_context;

/*****************************************************************************
 *      System interrupt and critical section handling
 *
 * There are two types of interrupts that a running backend needs to accept
 * without messing up its state: QueryCancel (SIGINT) and ProcDie (SIGTERM).
 * In both cases, we need to be able to clean up the current transaction
 * gracefully, so we can't respond to the interrupt instantaneously ---
 * there's no guarantee that internal data structures would be self-consistent
 * if the code is interrupted at an arbitrary instant.    Instead, the signal
 * handlers set flags that are checked periodically during execution.
 *
 * The CHECK_FOR_INTERRUPTS() macro is called at strategically located spots
 * where it is normally safe to accept a cancel or die interrupt.  In some
 * cases, we invoke CHECK_FOR_INTERRUPTS() inside low-level subroutines that
 * might sometimes be called in contexts that do *not* want to allow a cancel
 * or die interrupt.  The HOLD_INTERRUPTS() and RESUME_INTERRUPTS() macros
 * allow code to ensure that no cancel or die interrupt will be accepted,
 * even if CHECK_FOR_INTERRUPTS() gets called in a subroutine.    The interrupt
 * will be held off until CHECK_FOR_INTERRUPTS() is done outside any
 * HOLD_INTERRUPTS() ... RESUME_INTERRUPTS() section.
 *
 * Special mechanisms are used to let an interrupt be accepted when we are
 * waiting for a lock or when we are waiting for command input (but, of
 * course, only if the interrupt holdoff counter is zero).    See the
 * related code for details.
 *
 * A lost connection is handled similarly, although the loss of connection
 * does not raise a signal, but is detected when we fail to write to the
 * socket. If there was a signal for a broken connection, we could make use of
 * it by setting t_thrd.int_cxt.ClientConnectionLost in the signal handler.
 *
 * A related, but conceptually distinct, mechanism is the "critical section"
 * mechanism.  A critical section not only holds off cancel/die interrupts,
 * but causes any ereport(ERROR) or ereport(FATAL) to become ereport(PANIC)
 * --- that is, a system-wide reset is forced.    Needless to say, only really
 * *critical* code should be marked as a critical section!    Currently, this
 * mechanism is only used for XLOG-related code.
 *
 *****************************************************************************/
typedef struct knl_t_interrupt_context {
    /* these are marked volatile because they are set by signal handlers: */
    volatile bool QueryCancelPending;

    volatile bool PoolValidateCancelPending;

    volatile bool ProcDiePending;

    volatile bool ClientConnectionLost;

    volatile bool StreamConnectionLost;

    volatile bool ImmediateInterruptOK;

    /* these are marked volatile because they are examined by signal handlers: */
    volatile uint32 InterruptHoldoffCount;

    volatile uint32 CritSectionCount;

    volatile uint32 QueryCancelHoldoffCount;

    volatile bool InterruptByCN;

    volatile bool InterruptCountResetFlag;

    volatile bool ignoreBackendSignal; /* ignore signal for threadpool worker */

} knl_t_interrupt_context;

typedef int64 pg_time_t;
#define INVALID_CANCEL_KEY (0)

typedef struct knl_t_proc_context {
    ThreadId MyProcPid;

    pg_time_t MyStartTime;

    int MyPMChildSlot;

    long MyCancelKey;

    char* MyProgName;

    BackendId MyBackendId;

    /*
     * t_thrd.proc_cxt.DataDir is the absolute path to the top level of the PGDATA directory tree.
     * Except during early startup, this is also the server's working directory;
     * most code therefore can simply use relative paths and not reference t_thrd.proc_cxt.DataDir
     * explicitly.
     */
    char* DataDir;

    char OutputFileName[MAXPGPATH]; /* debugging output file */

    char pkglib_path[MAXPGPATH]; /* full path to lib directory */

    char postgres_exec_path[MAXPGPATH]; /* full path to backend */

    /* Flag: PostgresMain enter queries loop */
    bool postgres_initialized;

    class PostgresInitializer* PostInit;

    /*
     * This flag is set during proc_exit() to change ereport()'s behavior,
     * so that an ereport() from an on_proc_exit routine cannot get us out
     * of the exit procedure.  We do NOT want to go back to the idle loop...
     */
    bool proc_exit_inprogress;
    bool sess_exit_inprogress;
    bool pooler_connection_inprogress;
} knl_t_proc_context;

typedef struct knl_t_vacuum_context {
    int VacuumPageHit;

    int VacuumPageMiss;

    int VacuumPageDirty;

    int VacuumCostBalance; /* working state for vacuum */

    bool VacuumCostActive;

    /* just for dfs table on "vacuum full" */
    bool vacuum_full_compact;

    /* A few variables that don't seem worth passing around as parameters */
    MemoryContext vac_context;

    bool in_vacuum;

} knl_t_vacuum_context;

typedef struct knl_t_autovacuum_context {
    /* Flags set by signal handlers */
    volatile sig_atomic_t got_SIGHUP;
    volatile sig_atomic_t got_SIGUSR2;
    volatile sig_atomic_t got_SIGTERM;

    /* Comparison points for determining whether freeze_max_age is exceeded */
    TransactionId recentXid;
    MultiXactId recentMulti;

    /* Default freeze ages to use for autovacuum (varies by database) */
    int64 default_freeze_min_age;
    int64 default_freeze_table_age;

    /* Memory context for long-lived data */
    MemoryContext AutovacMemCxt;

    /* hash table to keep all tuples stat info that fetchs from DataNode */
    HTAB* pgStatAutoVacInfo;

    char* autovacuum_coordinators_string;
    int autovac_iops_limits;
    struct AutoVacuumShmemStruct* AutoVacuumShmem;

    /* the database list in the launcher, and the context that contains it */
    struct Dllist* DatabaseList;
    MemoryContext DatabaseListCxt;

    /* Pointer to my own WorkerInfo, valid on each worker */
    struct WorkerInfoData* MyWorkerInfo;

    /* PID of launcher, valid only in worker while shutting down */
    ThreadId AutovacuumLauncherPid;

    TimestampTz last_read;
} knl_t_autovacuum_context;

typedef struct knl_t_undolauncher_context {
    /* Flags set by signal handlers */
    volatile sig_atomic_t got_SIGHUP;
    volatile sig_atomic_t got_SIGUSR2;
    volatile sig_atomic_t got_SIGTERM;

    /* PID of launcher, valid only in worker while shutting down */
    ThreadId UndoLauncherPid;

    struct UndoWorkerShmemStruct *UndoWorkerShmem;
} knl_t_undolauncher_context;

typedef struct knl_t_undoworker_context {
    /* Flags set by signal handlers */
    volatile sig_atomic_t got_SIGHUP;
    volatile sig_atomic_t got_SIGUSR2;
    volatile sig_atomic_t got_SIGTERM;
} knl_t_undoworker_context;

typedef struct knl_t_undorecycler_context {
    /* Flags set by signal handlers */
    volatile sig_atomic_t got_SIGHUP;
    volatile sig_atomic_t shutdown_requested;
} knl_t_undorecycler_context;

typedef struct knl_t_rollback_requests_context {
    /* This hash holds all pending rollback requests */
    struct HTAB *rollback_requests_hash;

    /* Pointer to the next hash bucket for scan.
     * We do not want to always start at bucket zero
     * when scanning the hash for the next request to process.
     */
    uint32 next_bucket_for_scan;
} knl_t_rollback_requests_context;

typedef struct knl_t_gstat_context {
    /* Flags set by signal handlers */
    volatile sig_atomic_t got_SIGHUP;
    volatile sig_atomic_t got_SIGUSR2;
    volatile sig_atomic_t got_SIGTERM;
} knl_t_gstat_context;

typedef struct knl_t_aiocompleter_context {
    /* Flags set by interrupt handlers for later service in the main loop. */
    volatile sig_atomic_t shutdown_requested;
    volatile sig_atomic_t config_requested;
} knl_t_aiocompleter_context;

typedef struct knl_t_twophasecleaner_context {
    char pgxc_clean_log_path[MAX_PATH_LEN];
    volatile sig_atomic_t got_SIGHUP;
    volatile sig_atomic_t shutdown_requested;
} knl_t_twophasecleaner_context;

typedef struct knl_t_bgwriter_context {
    volatile sig_atomic_t got_SIGHUP;
    volatile sig_atomic_t shutdown_requested;
} knl_t_bgwriter_context;

typedef struct knl_t_pagewriter_context {
    volatile sig_atomic_t got_SIGHUP;
    volatile sig_atomic_t shutdown_requested;
    volatile sig_atomic_t sync_requested;
    volatile sig_atomic_t sync_retry;
    int page_writer_after;
    int pagewriter_id;
    uint64 next_flush_time;
    uint64 next_scan_time;
} knl_t_pagewriter_context;

typedef struct knl_t_pagerepair_context {
    volatile sig_atomic_t got_SIGHUP;
    volatile sig_atomic_t shutdown_requested;
    volatile sig_atomic_t page_repair_requested;
    volatile sig_atomic_t file_repair_requested;
} knl_t_pagerepair_context;


typedef struct knl_t_sharestoragexlogcopyer_context_ {
    volatile sig_atomic_t got_SIGHUP;
    volatile sig_atomic_t shutdown_requested;
    bool wakeUp;
    int readFile;
    XLogSegNo readSegNo;
    uint32 readOff;
    char *originBuf;
    char *buf;
} knl_t_sharestoragexlogcopyer_context;


#define MAX_SEQ_SCANS 100

typedef struct knl_t_dynahash_context {
    MemoryContext CurrentDynaHashCxt;

    /*
     * We track active hash_seq_search scans here.    The need for this mechanism
     * comes from the fact that a scan will get confused if a bucket split occurs
     * while it's in progress: it might visit entries twice, or even miss some
     * entirely (if it's partway through the same bucket that splits).    Hence
     * we want to inhibit bucket splits if there are any active scans on the
     * table being inserted into.  This is a fairly rare case in current usage,
     * so just postponing the split until the next insertion seems sufficient.
     *
     * Given present usages of the function, only a few scans are likely to be
     * open concurrently; so a finite-size stack of open scans seems sufficient,
     * and we don't worry that linear search is too slow.  Note that we do
     * allow multiple scans of the same hashtable to be open concurrently.
     *
     * This mechanism can support concurrent scan and insertion in a shared
     * hashtable if it's the same backend doing both.  It would fail otherwise,
     * but locking reasons seem to preclude any such scenario anyway, so we don't
     * worry.
     *
     * This arrangement is reasonably robust if a transient hashtable is deleted
     * without notifying us.  The absolute worst case is we might inhibit splits
     * in another table created later at exactly the same address.    We will give
     * a warning at transaction end for reference leaks, so any bugs leading to
     * lack of notification should be easy to catch.
     */
    HTAB *seq_scan_tables[MAX_SEQ_SCANS]; /* tables being scanned */

    int seq_scan_level[MAX_SEQ_SCANS]; /* subtransaction nest level */

    int num_seq_scans;
} knl_t_dynahash_context;

typedef struct knl_t_bulkload_context {
    char distExportDataDir[MAX_PATH_LEN];
#define TIME_STAMP_STR_LEN 15
    char distExportTimestampStr[15];
    TransactionId distExportCurrXid;
    uint32 distExportNextSegNo;
    int illegal_character_err_cnt;
    bool illegal_character_err_threshold_reported;
} knl_t_bulkload_context;

typedef struct knl_t_job_context {
    /* Share memory for job scheudler. */
    struct JobScheduleShmemStruct* JobScheduleShmem;
    /* Flags set by signal handlers */
    volatile sig_atomic_t got_SIGHUP;
    volatile sig_atomic_t got_SIGUSR2;
    volatile sig_atomic_t got_SIGTERM;
    /* Memory context for long-lived data */
    MemoryContext JobScheduleMemCxt;
    /* expired job list at present, and the context that contains it */
    struct Dllist* ExpiredJobList;
    MemoryContext ExpiredJobListCtx;

    /* job info */
    struct JobWorkerInfoData* MyWorkerInfo;
} knl_t_job_context;

typedef uintptr_t Datum;

typedef struct knl_t_postgres_context {
    /* clear key message that may appear in core file for security */
    bool clear_key_memory;

    /* true if create table in create table as select' has been done */
    bool table_created_in_CTAS;
    const char* debug_query_string; /* client-supplied query string */
    bool isInResetUserName;

    /* Note: whereToSendOutput is initialized for the bootstrap/standalone case */
    int whereToSendOutput;  // enum CommandDest
    struct ResourcePool* local_foreign_respool;

    /* max_stack_depth converted to bytes for speed of checking */
    long max_stack_depth_bytes;

    /* mark if the initial password has been changed or not */
    bool password_changed;

    /*
     * Stack base pointer -- initialized by PostmasterMain and inherited by
     * subprocesses. This is not static because old versions of PL/Java modify
     * it directly. Newer versions use set_stack_base(), but we want to stay
     * binary-compatible for the time being.
     */
    char* stack_base_ptr;

    /*
     * Flag to keep track of whether we have started a transaction.
     * For extended query protocol this has to be remembered across messages.
     */
    bool xact_started;

    /*
     * Flag to indicate that we are doing the outer loop's read-from-client,
     * as opposed to any random read from client that might happen within
     * commands like COPY FROM STDIN.
     */
    bool DoingCommandRead;

    /* assorted command-line switches */
    const char* userDoption; /* -D switch */
    bool EchoQuery;

    /*
     * people who want to use EOF should #define DONTUSENEWLINE in
     * tcop/tcopdebug.h
     */
#ifndef TCOP_DONTUSENEWLINE
    int UseNewLine; /* Use newlines query delimiters (the default) */
#else
    int UseNewLine; /* Use EOF as query delimiters */
#endif /* TCOP_DONTUSENEWLINE */

    /* whether or not, and why, we were canceled by conflict with recovery */
    bool RecoveryConflictPending;
    bool RecoveryConflictRetryable;
    int RecoveryConflictReason;  // enum ProcSignalReason

    /* reused buffer to pass to SendRowDescriptionMessage() */
    struct StringInfoData* row_description_buf;
    const char* clobber_qstr;

    /*
     * for delta merge
     */
    Datum query_result;

    /*
     * It is a list of some relations or columns which have no statistic info.
     * we should output them to warning or log if it is not null.
     */
    List* g_NoAnalyzeRelNameList;

    /* show real run datanodes of pbe query for explain analyze/performance */
    bool mark_explain_analyze;

    /*
     * false if the query is non-explain or explain analyze(performance), true if
     * it is simple explain.
     */
    bool mark_explain_only;

    /* GUC variable to enable plan cache if stmt_name is not given. */
    bool enable_explicit_stmt_name;
    long val;
    // struct rusage Save_r;        // not define
    struct timeval Save_t;

    bool gpc_fisrt_send_clean;   // cn send clean to dn for global plan cache
} knl_t_postgres_context;

typedef struct knl_t_utils_context {
    /* to record the sequent count when creating memory context */
    int mctx_sequent_count;

    /* to track the memory context when query is executing */
    struct MemoryTrackData* ExecutorMemoryTrack;

#ifdef MEMORY_CONTEXT_CHECKING
    /* to record the sequent count of GUC setting */
    int memory_track_sequent_count;

    /* to record the plan node id of GUC setting */
    int memory_track_plan_nodeid;

    /* to save the detail allocation information */
    struct StringInfoData* detailTrackingBuf;
#endif

    /* used to track the element count in the output file */
    int track_cnt;

    /* gValueCompareContext -- special memory context for partition value routing.
     * It's created under TOP memory context, and destroyed until this thread exits.
     * It's used during each value's partition routing, and reset after done.
     */
    MemoryContext gValueCompareContext;
    /*ContextUsedCount-- is used to count the number of gValueCompareContext to
     * avoid the allocated memory is released early.
     */
    int ContextUsedCount;
    struct PartitionIdentifier* partId;
    struct Const* valueItemArr[PARTKEY_VALUE_MAXNUM + 1];
    struct ResourceOwnerData* TopResourceOwner;
    struct ResourceOwnerData* CurrentResourceOwner;
    struct ResourceOwnerData* STPSavedResourceOwner;
    struct ResourceOwnerData* CurTransactionResourceOwner;
    struct ResourceOwnerData* TopTransactionResourceOwner;
    bool SortColumnOptimize;
    struct RelationData* pRelatedRel;

#ifndef WIN32
    timer_t sigTimerId;
#endif
    unsigned int ConfigFileLineno;
    const char* GUC_flex_fatal_errmsg;
    sigjmp_buf* GUC_flex_fatal_jmp;

    /* Static state for pg_strtok */
    char* pg_strtok_ptr;
    /*
     * Support for newNode() macro
     *
     * In a GCC build there is no need for the global variable newNodeMacroHolder.
     * However, we create it anyway, to support the case of a non-GCC-built
     * loadable module being loaded into a GCC-built backend.
     */
    Node* newNodeMacroHolder;

    /* Memory Protecting feature initialization flag */
    bool gs_mp_inited;

    /* Memory Protecting need flag */
    bool memNeedProtect;

    /* Track memory usage in chunks at individual thread level */
    int32 trackedMemChunks;

    /* Track memory usage in bytes at individual thread level */
    int64 trackedBytes;

    int64 peakedBytesInQueryLifeCycle;
    int64 basedBytesInQueryLifeCycle;

    /* Per thread/query quota in chunks */
    int32 maxChunksPerThread; /* Will be updated by CostSize */

    /* Memory Protecting feature initialization flag */
    int32 beyondChunk;

    bool backend_reserved;
} knl_t_utils_context;

typedef struct knl_t_pgxc_context {
    /*
     * Local cache for current installation/redistribution node group, allocated in
     * t_thrd.top_mem_cxt at session start-up time
     */
    char* current_installation_nodegroup;
    char* current_redistribution_nodegroup;
	int globalBucketLen;

    /* Global number of nodes. Point to a shared memory block */
    int* shmemNumCoords;
    int* shmemNumCoordsInCluster;
    int* shmemNumDataNodes;
    int* shmemNumDataStandbyNodes;
    int* shmemNumSkipNodes;

    /* Shared memory tables of node definitions */
    struct NodeDefinition* coDefs;
    struct NodeDefinition* coDefsInCluster;
    struct NodeDefinition* dnDefs;
    struct NodeDefinition* dnStandbyDefs;
    struct SkipNodeDefinition* skipNodes;

    /* pgxcnode.cpp */
    struct PGXCNodeNetCtlLayer* pgxc_net_ctl;

    struct Instrumentation* GlobalNetInstr;

    /* save the connection to the compute pool */
    struct pgxc_node_handle* compute_pool_handle;
    struct pg_conn* compute_pool_conn;

    /* execRemote.cpp */

    /*
     * Flag to track if a temporary object is accessed by the current transaction
     */
    bool temp_object_included;

#ifdef PGXC
    struct abort_callback_type* dbcleanup_info;
#endif
#define SOCKET_BUFFER_LEN 256
    char socket_buffer[SOCKET_BUFFER_LEN];
    bool is_gc_fdw;
    bool is_gc_fdw_analyze;
    int gc_fdw_current_idx;
    int gc_fdw_max_idx;
    int gc_fdw_run_version;
    struct SnapshotData* gc_fdw_snapshot;
#define BEGIN_CMD_BUFF_SIZE 1024
    char begin_cmd[BEGIN_CMD_BUFF_SIZE];
} knl_t_pgxc_context;

typedef struct knl_t_conn_context {
    /* connector.cpp */
    char* value_drivertype;
    char* value_dsn;
    char* value_username;
    char* value_password;
    char* value_sql;
    char* value_encoding;
    /*
     * Extension Connector Controler
     */
    class ECControlBasic* ecCtrl;

    /* odbc_connector.cpp */

    void* dl_handle;
    void* _conn;
    void* _result;
    struct pg_enc2name* _DatabaseEncoding;
    const char* _float_inf;
} knl_t_conn_context;

typedef struct {
    volatile sig_atomic_t shutdown_requested;
    volatile sig_atomic_t got_SIGHUP;
    volatile sig_atomic_t sleep_long;
    volatile sig_atomic_t check_repair;
} knl_t_page_redo_context;

typedef struct knl_t_startup_context {
    /*
     * Flags set by interrupt handlers for later service in the redo loop.
     */
    volatile sig_atomic_t got_SIGHUP;
    volatile sig_atomic_t shutdown_requested;
    volatile sig_atomic_t failover_triggered;
    volatile sig_atomic_t switchover_triggered;
    volatile sig_atomic_t primary_triggered;
    volatile sig_atomic_t standby_triggered;

    /*
     * Flag set when executing a restore command, to tell SIGTERM signal handler
     * that it's safe to just proc_exit.
     */
    volatile sig_atomic_t in_restore_command;
    volatile sig_atomic_t check_repair;

    struct notifysignaldata* NotifySigState;
} knl_t_startup_context;

typedef struct knl_t_alarmchecker_context {
    /* private variables for alarm checker thread */
    Latch AlarmCheckerLatch; /* the tls latch for alarm checker thread */

    /* signal handle flags */
    volatile sig_atomic_t gotSighup; /* the signal flag of SIGHUP */
    volatile sig_atomic_t gotSigdie; /* the signal flag of SIGTERM and SIGINT */
} knl_t_alarmchecker_context;

typedef struct knl_t_snapcapturer_context {
    /* signal handle flags */
    volatile sig_atomic_t got_SIGHUP;
    volatile sig_atomic_t got_SIGTERM;

    struct TxnSnapCapShmemStruct *snapCapShmem;
} knl_t_snapcapturer_context;

typedef struct knl_t_rbcleaner_context {
    /* private variables for rbcleaner thread */
    volatile sig_atomic_t got_SIGHUP;
    volatile sig_atomic_t got_SIGTERM;

    struct RbCleanerShmemStruct *RbCleanerShmem;
} knl_t_rbcleaner_context;

typedef struct knl_t_lwlockmoniter_context {
    /* Flags set by interrupt handlers for later service in the main loop. */
    volatile sig_atomic_t got_SIGHUP;
    volatile sig_atomic_t shutdown_requested;
} knl_t_lwlockmoniter_context;

typedef struct knl_t_walwriter_context {
    volatile sig_atomic_t got_SIGHUP;
    volatile sig_atomic_t shutdown_requested;
} knl_t_walwriter_context;

typedef struct knl_t_walwriterauxiliary_context {
    volatile sig_atomic_t got_SIGHUP;
    volatile sig_atomic_t shutdown_requested;
} knl_t_walwriterauxiliary_context;

typedef struct knl_t_poolcleaner_context {
    volatile sig_atomic_t shutdown_requested;
    volatile sig_atomic_t got_SIGHUP;
} knl_t_poolcleaner_context;

typedef struct knl_t_catchup_context {
    volatile sig_atomic_t catchup_shutdown_requested;
} knl_t_catchup_context;

/*
 * Pointer to a location in the XLOG.  These pointers are 64 bits wide,
 * because we don't want them ever to overflow.
 */
typedef uint64 XLogRecPtr;

typedef struct knl_t_checkpoint_context {
    struct CheckpointerShmemStruct* CheckpointerShmem;

    /*
     * Flags set by interrupt handlers for later service in the main loop.
     */
    volatile sig_atomic_t got_SIGHUP;
    volatile sig_atomic_t checkpoint_requested;
    volatile sig_atomic_t shutdown_requested;

    /* Private state */
    bool ckpt_active;

    /* these values are valid when ckpt_active is true: */
    pg_time_t ckpt_start_time;
    XLogRecPtr ckpt_start_recptr;
    double ckpt_cached_elapsed;

    pg_time_t last_checkpoint_time;
    pg_time_t last_xlog_switch_time;
    pg_time_t last_truncate_log_time;
    int absorbCounter;
    int ckpt_done;
} knl_t_checkpoint_context;

typedef struct knl_t_snapshot_context {
    struct SnapshotData* SnapshotNowData;
    struct SnapshotData* SnapshotSelfData;
    struct SnapshotData* SnapshotAnyData;
    struct SnapshotData* SnapshotToastData;
} knl_t_snapshot_context;

typedef struct knl_t_comm_context {
    /*
     *last epoll wait up time of receiver loop thread
     */
    uint64 g_receiver_loop_poll_up;
    int LibcommThreadType;
    /*
     * libcomm thread use libcomm_semaphore wake up work thread
     * libcomm_semaphore save as mailbox->semaphore
     * libcomm_semaphore create by gs_poll_create when work thread call gs_connect/gs_send/gs_wait_poll first time
     * libcomm_semaphore destory by  gs_poll_close when thread exit
     */
    struct binary_semaphore* libcomm_semaphore;
    struct mc_poller* g_libcomm_poller_list;
    struct mc_poller_hndl_list* g_libcomm_recv_poller_hndl_list;
    /*
     *last time when consumer thread exit gs_wait_poll
     */
    uint64 g_consumer_process_duration;
    /*
     *last time when producer thread exit gs_send
     */
    uint64 g_producer_process_duration;
    pid_t MyPid;
    uint64 debug_query_id;
} knl_t_comm_context;

typedef struct knl_t_libpq_context {
    /*
     * listen socket for unix domain connection
     * between receiver flow control thread and postmaster
     */
    int listen_fd_for_recv_flow_ctrl;
    /* Where the Unix socket file is */
    char sock_path[MAXPGPATH];
    /* Where the Unix socket file for ha port is */
    char ha_sock_path[MAXPGPATH];
    char* PqSendBuffer;
    /* Size send buffer */
    int PqSendBufferSize;
    /* Next index to store a byte in PqSendBuffer */
    int PqSendPointer;
    /* Next index to send a byte in PqSendBuffer */
    int PqSendStart;
    char* PqRecvBuffer;
    /* Size recv buffer */
    int PqRecvBufferSize;
    /* Next index to read a byte from PqRecvBuffer */
    int PqRecvPointer;
    /* End of data available in PqRecvBuffer */
    int PqRecvLength;
    /* Message status */
    bool PqCommBusy;
    bool DoingCopyOut;
#ifdef HAVE_SIGPROCMASK
    sigset_t UnBlockSig, BlockSig, StartupBlockSig;
#else
    int UnBlockSig, BlockSig, StartupBlockSig;
#endif

    /* variables for save query results to temp file*/
    bool save_query_result_to_disk;
    struct TempFileContextInfo* PqTempFileContextInfo;

    /*
     * pre-parsed content of HBA config file: list of HbaLine structs.
     * parsed_hba_context is the memory context where it lives.
     */
    List* parsed_hba_lines;
    MemoryContext parsed_hba_context;
} knl_t_libpq_context;

typedef struct knl_t_contrib_context {
    int g_searchletId;
    struct S_VectorInfo* vec_info;
    bool assert_enabled;
    Datum g_log_hostname;
    Datum g_log_nodename;

    /* Hash table for caching the results of shippability lookups */
    HTAB* ShippableCacheHash;

    /*
     * Valid options for gc_fdw.
     * Allocated and filled in InitGcFdwOptions.
     */
    struct PgFdwOption* gc_fdw_options;
} knl_t_contrib_context;

typedef struct knl_t_basebackup_context {
    char g_xlog_location[MAXPGPATH];

    char* buf_block;
} knl_t_basebackup_context;

typedef struct knl_t_datarcvwriter_context {
    HTAB* data_writer_rel_tab;

    /* Data receiver writer flush page error times */
    uint32 dataRcvWriterFlushPageErrorCount;

    /*
     * Flags set by interrupt handlers for later service in the main loop.
     */
    volatile sig_atomic_t gotSIGHUP;
    volatile sig_atomic_t shutdownRequested;

    bool AmDataReceiverForDummyStandby;

    /* max dummy data write file (default: 1GB) */
    uint32 dummy_data_writer_file_num;
} knl_t_datarcvwriter_context;

typedef struct knl_t_libwalreceiver_context {
    /* Current connection to the primary, if any */
    struct pg_conn* streamConn;

    /* Buffer for currently read records */
    char* recvBuf;
    char* shared_storage_buf;
    char* shared_storage_read_buf;
    char* decompressBuf;
    XLogReaderState* xlogreader;
    LibpqrcvConnectParam connect_param;
} knl_t_libwalreceiver_context;

typedef struct knl_t_sig_context {
    unsigned long signal_handle_cnt;
    GsSignalCheckType gs_sigale_check_type;
    uint64 session_id;
    int cur_ctrl_index;
} knl_t_sig_context;

typedef struct knl_t_slot_context {
    /* Control array for replication slot management */
    struct ReplicationSlotCtlData* ReplicationSlotCtl;

    /* My backend's replication slot in the shared memory array */
    struct ReplicationSlot* MyReplicationSlot;
} knl_t_slot_context;

typedef struct knl_t_datareceiver_context {
    int DataReplFlag;

    /*
     * Flags set by interrupt handlers of datareceiver for later service in the
     * main loop.
     */
    volatile sig_atomic_t got_SIGHUP;
    volatile sig_atomic_t got_SIGTERM;

    struct StandbyDataReplyMessage* reply_message;

    /* Current connection to the primary, if any */
    struct pg_conn* dataStreamingConn;

    /* DummyStandby */
    bool AmDataReceiverForDummyStandby;

    /* Buffer for currently read data */
    char* recvBuf;

    struct DataRcvData* DataRcv;

    /*
     * About SIGTERM handling:
     *
     * We can't just exit(1) within SIGTERM signal handler, because the signal
     * might arrive in the middle of some critical operation, like while we're
     * holding a spinlock. We also can't just set a flag in signal handler and
     * check it in the main loop, because we perform some blocking operations
     * like libpqrcv_PQexec(), which can take a long time to finish.
     *
     * We use a combined approach: When DataRcvImmediateInterruptOK is true, it's
     * safe for the signal handler to elog(FATAL) immediately. Otherwise it just
     * sets got_SIGTERM flag, which is checked in the main loop when convenient.
     *
     * This is very much like what regular backends do with t_thrd.int_cxt.ImmediateInterruptOK,
     * ProcessInterrupts() etc.
     */
    volatile bool DataRcvImmediateInterruptOK;
} knl_t_datareceiver_context;

typedef struct knl_t_datasender_context {
    /* Array of DataSnds in shared memory */
    struct DataSndCtlData* DataSndCtl;

    /* My slot in the shared memory array */
    struct DataSnd* MyDataSnd;

    /* Global state */
    bool am_datasender; /* Am I a datasender process ? */

    /*
     * Buffer for processing reply messages.
     */
    struct StringInfoData* reply_message;

    /*
     * Buffer for constructing outgoing messages
     * (1 + sizeof(DataPageMessageHeader) + MAX_SEND_SIZE bytes)
     */
    char* output_message;

    /*
     * dummy standby read data file num and offset.
     */
    uint32 dummy_data_read_file_num;
    FILE* dummy_data_read_file_fd;

    /*
     * Timestamp of the last receipt of the reply from the standby.
     */
    TimestampTz last_reply_timestamp;

    /* Have we sent a heartbeat message asking for reply, since last reply? */
    bool ping_sent;

    /* Flags set by signal handlers for later service in main loop */
    volatile sig_atomic_t got_SIGHUP;
    volatile sig_atomic_t datasender_shutdown_requested;
    volatile sig_atomic_t datasender_ready_to_stop;
} knl_t_datasender_context;

typedef struct knl_t_walreceiver_context {
    volatile sig_atomic_t got_SIGHUP;
    volatile sig_atomic_t got_SIGTERM;
    volatile sig_atomic_t start_switchover;
    char gucconf_file[MAXPGPATH];
    char temp_guc_conf_file[MAXPGPATH];
    char gucconf_lock_file[MAXPGPATH];
    /*
     * Define guc parameters which would not be synchronized to standby.
     * NB: RESERVE_SIZE must be changed at the same time.
     */
    char** reserve_item;
    time_t standby_config_modify_time;
    time_t Primary_config_modify_time;
    TimestampTz last_sendfilereply_timestamp;
    int check_file_timeout;
    struct WalRcvCtlBlock* walRcvCtlBlock;
    struct StandbyReplyMessage* reply_message;
    struct StandbyHSFeedbackMessage* feedback_message;
    struct StandbySwitchRequestMessage* request_message;
    struct ConfigModifyTimeMessage* reply_modify_message;
    volatile bool WalRcvImmediateInterruptOK;
    bool AmWalReceiverForFailover;
    bool AmWalReceiverForStandby;
    int control_file_writed;
    bool checkConsistencyOK;
    bool hasReceiveNewData;
    bool termChanged;
} knl_t_walreceiver_context;

typedef struct knl_t_walsender_context {
    char* load_cu_buffer;
    int load_cu_buffer_size;
    /* Array of WalSnds in shared memory */
    struct WalSndCtlData* WalSndCtl;
    /* My slot in the shared memory array */
    struct WalSnd* MyWalSnd;
    int logical_xlog_advanced_timeout; /* maximum time to write xlog * of logical slot advance */
    typedef int ServerMode;
    typedef int DemoteMode;
    DemoteMode Demotion;
    /* State for WalSndWakeupRequest */
    bool wake_wal_senders;
    bool wal_send_completed;
    /*
     * These variables are used similarly to openLogFile/Id/Seg/Off,
     * but for walsender to read the XLOG.
     */
    int sendFile;
    typedef uint64 XLogSegNo;
    XLogSegNo sendSegNo;
    uint32 sendOff;
    struct WSXLogJustSendRegion* wsXLogJustSendRegion;
    /*
     * How far have we sent WAL already? This is also advertised in
     * MyWalSnd->sentPtr.  (Actually, this is the next WAL location to send.)
     */
    typedef uint64 XLogRecPtr;
    XLogRecPtr sentPtr;
    XLogRecPtr catchup_threshold;
    /*
     * Buffer for processing reply messages.
     */
    struct StringInfoData* reply_message;
    /*
     * Buffer for processing timestamp.
     */
    struct StringInfoData* tmpbuf;
    /*
     * Buffer for constructing outgoing messages
     * (1 + sizeof(WalDataMessageHeader) + MAX_SEND_SIZE bytes)
     * if with enable_mix_replication being true the new message as the following:
     * (1 + sizeof(WalDataMessageHeader) + 1 + sizeof(XLogRecPtr) + MAX_SEND_SIZE bytes)
     * 1 --> 'w'
     * sizeof(WalDataMessageHeader) --> WalDataMessageHeader
     * 1 --> 'w'
     * sizeof(XLogRecPtr) -->XLogRecPtr dataStart
     * MAX_SEND_SIZE bytes --> wal data bytes
     */
    char* output_xlog_message;
    Size output_xlog_msg_prefix_len;
    /*
     * Buffer for constructing outgoing messages
     * (sizeof(DataElementHeaderData) + MAX_SEND_SIZE bytes)
     * if with enable_mix_replication being true the new message as the following:
     * (1 + sizeof(WalDataMessageHeader) + 1 + MAX_SEND_SIZE bytes)
     * 1 --> 'd'
     * sizeof(WalDataMessageHeader) --> WalDataMessageHeader
     * 1 --> 'd'
     * MAX_SEND_SIZE bytes --> wal data bytes
     */
    char* output_data_message;
    /* used to flag the latest length in output_data_message*/
    uint32 output_data_msg_cur_len;
    XLogRecPtr output_data_msg_start_xlog;
    XLogRecPtr output_data_msg_end_xlog;
    /*
     * The xlog reader and private info employed for the xlog parsing in wal sender.
     */
    struct XLogReaderState* ws_xlog_reader;
    /*
     * Timestamp of the last receipt of the reply from the standby.
     */
    TimestampTz last_reply_timestamp;
    /*
     * Timestamp of the last logical xlog advanced is written.
     */
    TimestampTz last_logical_xlog_advanced_timestamp;
    /* Have we sent a heartbeat message asking for reply, since last reply? */
    bool waiting_for_ping_response;
    /* Flags set by signal handlers for later service in main loop */
    volatile sig_atomic_t got_SIGHUP;
    volatile sig_atomic_t walsender_shutdown_requested;
    volatile sig_atomic_t walsender_ready_to_stop;
    volatile sig_atomic_t response_switchover_requested;
    ServerMode server_run_mode;
    /* for config_file */
    char gucconf_file[MAXPGPATH];
    char gucconf_lock_file[MAXPGPATH];
    char slotname[NAMEDATALEN];
    /* the dummy data reader fd for the wal streaming */
    FILE* ws_dummy_data_read_file_fd;
    uint32 ws_dummy_data_read_file_num;
    /* Missing CU checking stuff */
    struct cbmarray* CheckCUArray;
    struct LogicalDecodingContext* logical_decoding_ctx;
    struct ParallelLogicalDecodingContext* parallel_logical_decoding_ctx;
    XLogRecPtr logical_startptr;
    int remotePort;
    /* Have we caught up with primary? */
    bool walSndCaughtUp;
    int LogicalSlot;

    /* Notify primary to advance logical replication slot. */
    struct pg_conn* advancePrimaryConn;
    /* Timestamp of the last check-timeout time in WalSndCheckTimeOut. */
    TimestampTz last_check_timeout_timestamp;

    /* Read data from WAL into xlogReadBuf, then compress it to compressBuf */
    char *xlogReadBuf;
    char *compressBuf;

    /* flag set in WalSndCheckTimeout */
    bool isWalSndSendTimeoutMessage;

    int datafd;
    int ep_fd;
    logicalLog* restoreLogicalLogHead;
    /* is obs backup, in this mode, skip backup replication slot */
    bool is_obsmode;
    bool standbyConnection;
    bool cancelLogCtl;
} knl_t_walsender_context;

typedef struct knl_t_walreceiverfuncs_context {
    struct WalRcvData* WalRcv;
    int WalReplIndex;
} knl_t_walreceiverfuncs_context;

typedef struct knl_t_replgram_context {
    /* Result of the parsing is returned here */
    struct Node* replication_parse_result;
} knl_t_replgram_context;

typedef struct knl_t_replscanner_context {
    struct yy_buffer_state* scanbufhandle;
    struct StringInfoData* litbuf;
} knl_t_replscanner_context;

typedef struct knl_t_syncrepgram_context {
    /* Result of parsing is returned in one of these two variables */
    List* syncrep_parse_result;
} knl_t_syncrepgram_context;

typedef struct knl_t_syncrepscanner_context {
    struct yy_buffer_state* scanbufhandle;
    struct StringInfoData* xdbuf;
    int result;
} knl_t_syncrepscanner_context;

typedef struct knl_t_syncrep_context {
    struct SyncRepConfigData** SyncRepConfig;   // array of SyncRepConfig
    int SyncRepConfigGroups;                    // group of SyncRepConfig
    int SyncRepMaxPossib;                       // max possible sync standby number

    bool announce_next_takeover;
} knl_t_syncrep_context;

typedef struct knl_t_logical_context {
    int sendFd;
    uint64 sendSegNo;
    uint32 sendOff;
    bool ExportInProgress;
    bool IsAreaDecode;
    ResourceOwner SavedResourceOwnerDuringExport;
} knl_t_logical_context;

extern struct ParallelDecodeWorker** parallelDecodeWorker;

typedef struct knl_t_parallel_decode_worker_context {
    volatile sig_atomic_t shutdown_requested;
    volatile sig_atomic_t got_SIGHUP;
    volatile sig_atomic_t sleep_long;
    int slotId;
    int parallelDecodeId;
} knl_t_parallel_decode_worker_context;

typedef struct knl_t_logical_read_worker_context {
    volatile sig_atomic_t shutdown_requested;
    volatile sig_atomic_t got_SIGHUP;
    volatile sig_atomic_t sleep_long;
    volatile sig_atomic_t got_SIGTERM;
    MemoryContext ReadWorkerCxt;
    ParallelDecodeWorker** parallelDecodeWorkers;
    int slotId;
    int totalWorkerCount;
} knl_t_logical_read_worker_context;

typedef struct knl_t_dataqueue_context {
    struct DataQueueData* DataSenderQueue;
    struct DataQueueData* DataWriterQueue;
    struct BCMElementData* BCMElementArray;
    uint32 BCMElementArrayIndex1;
    uint32 BCMElementArrayIndex2;
    struct DataQueuePtr* BCMElementArrayOffset1;
    struct DataQueuePtr* BCMElementArrayOffset2;
    uint32 save_send_dummy_count;
    struct HTAB* heap_sync_rel_tab;
} knl_t_dataqueue_context;

typedef struct knl_t_walrcvwriter_context {
    uint64 walStreamWrite; /* last byte + 1 written out in the standby */

    /*
     * Flags set by interrupt handlers for later service in the main loop.
     */
    volatile sig_atomic_t gotSIGHUP;
    volatile sig_atomic_t shutdownRequested;

    int WAL_WRITE_UNIT_BYTES;
    uint32 ws_dummy_data_writer_file_num;
} knl_t_walrcvwriter_context;

typedef int CacheSlotId_t;
typedef void (*pg_on_exit_callback)(int code, Datum arg);
typedef void (*shmem_startup_hook_type)(void);
typedef struct ONEXIT {
    pg_on_exit_callback function;
    Datum arg;
} ONEXIT;
#define MAX_ON_EXITS 21
typedef struct knl_t_storage_context {
    /*
     * Bookkeeping for tracking emulated transactions in recovery
     */
    TransactionId latestObservedXid;
    struct RunningTransactionsData* CurrentRunningXacts;
    struct VirtualTransactionId* proc_vxids;
    union BufferDescPadded* BufferDescriptors;
    char* BufferBlocks;
    struct WritebackContext* BackendWritebackContext;
    struct HTAB* SharedBufHash;
    struct HTAB* BufFreeListHash;
    struct BufferDesc* InProgressBuf;
    /* local state for StartBufferIO and related functions */
    volatile bool IsForInput;
    /* local state for LockBufferForCleanup */
    struct BufferDesc* PinCountWaitBuf;
    /* local state for aio clean up resource  */
    struct AioDispatchDesc** InProgressAioDispatch;
    int InProgressAioDispatchCount;
    struct BufferDesc* InProgressAioBuf;
    int InProgressAioType;
    /*
     * When btree split, it will record two xlog:
     * 1. page split
     * 2. insert new split page to parent
     * Since it is an atomic action, don't interrupt above two steps.
     * Now use a special flag to remark and dont't ereport error when read buffers.
     */
    bool is_btree_split;
    /*
     * Backend-Private refcount management:
     *
     * Each buffer also has a private refcount that keeps track of the number of
     * times the buffer is pinned in the current process.  This is so that the
     * shared refcount needs to be modified only once if a buffer is pinned more
     * than once by a individual backend.  It's also used to check that no buffers
     * are still pinned at the end of transactions and when exiting.
     *
     *
     * To avoid - as we used to - requiring an array with g_instance.attr.attr_storage.NBuffers entries to keep
     * track of local buffers we use a small sequentially searched array
     * (PrivateRefCountArray) and a overflow hash table (PrivateRefCountHash) to
     * keep track of backend local pins.
     *
     * Until no more than REFCOUNT_ARRAY_ENTRIES buffers are pinned at once, all
     * refcounts are kept track of in the array; after that, new array entries
     * displace old ones into the hash table. That way a frequently used entry
     * can't get "stuck" in the hashtable while infrequent ones clog the array.
     *
     * Note that in most scenarios the number of pinned buffers will not exceed
     * REFCOUNT_ARRAY_ENTRIES.
     */
    struct PrivateRefCountEntry* PrivateRefCountArray;
    struct HTAB* PrivateRefCountHash;
    int32 PrivateRefCountOverflowed;
    uint32 PrivateRefCountClock;
    /*
     * Information saved between calls so we can determine the strategy
     * point's advance rate and avoid scanning already-cleaned buffers.
     */
    bool saved_info_valid;
    int prev_strategy_buf_id;
    uint32 prev_strategy_passes;
    int next_to_clean;
    uint32 next_passes;
    /* Moving averages of allocation rate and clean-buffer density */
    float smoothed_alloc;
    float smoothed_density;

    /* Pointers to shared state */
    struct BufferStrategyControl* StrategyControl;
    int NLocBuffer; /* until buffers are initialized */
    struct BufferDesc* LocalBufferDescriptors;
    Block* LocalBufferBlockPointers;
    int32* LocalRefCount;
    int nextFreeLocalBuf;
    struct HTAB* LocalBufHash;
    char* cur_block;
    int next_buf_in_block;
    int num_bufs_in_block;
    int total_bufs_allocated;
    MemoryContext LocalBufferContext;
    /* remember global block slot in progress */
    CacheSlotId_t CacheBlockInProgressIO;
    CacheSlotId_t CacheBlockInProgressUncompress;
    CacheSlotId_t MetaBlockInProgressIO;

    List* RecoveryLockList;
    int standbyWait_us;
    /*
     * All accesses to pg_largeobject and its index make use of a single Relation
     * reference, so that we only need to open pg_relation once per transaction.
     * To avoid problems when the first such reference occurs inside a
     * subtransaction, we execute a slightly klugy maneuver to assign ownership of
     * the Relation reference to TopTransactionResourceOwner.
     */
    struct RelationData* lo_heap_r;
    struct RelationData* lo_index_r;
    slock_t dummy_spinlock;
    int spins_per_delay;

    /* Workspace for FindLockCycle */
    struct PGPROC** visitedProcs; /* Array of visited procs */
    int nVisitedProcs;
    /* Workspace for TopoSort */
    struct PGPROC** topoProcs; /* Array of not-yet-output procs */
    int* beforeConstraints;    /* Counts of remaining before-constraints */
    int* afterConstraints;     /* List head for after-constraints */
    /* Output area for ExpandConstraints */
    struct WAIT_ORDER* waitOrders; /* Array of proposed queue rearrangements */
    int nWaitOrders;
    struct PGPROC** waitOrderProcs; /* Space for waitOrders queue contents */
    /* Current list of constraints being considered */
    struct EDGE* curConstraints;
    int nCurConstraints;
    int maxCurConstraints;
    /* Storage space for results from FindLockCycle */
    struct EDGE* possibleConstraints;
    int nPossibleConstraints;
    int maxPossibleConstraints;
    struct DEADLOCK_INFO* deadlockDetails;
    int nDeadlockDetails;
    /* PGPROC pointer of all blocking autovacuum worker found and its max size.
     * blocking_autovacuum_proc_num is in [0, max_blocking_autovacuum_proc_num].
     *
     * Partitioned table has been supported, which maybe have hundreds of partitions.
     * When a few autovacuum workers are active and running, partitions of the same partitioned
     * table, whose shared lock is hold by these workers, may be handled at the same time.
     * So that remember all the blocked autovacuum workers and notify to cancle them.
     */
    struct PGPROC** blocking_autovacuum_proc;
    int blocking_autovacuum_proc_num;
    int max_blocking_autovacuum_proc_num;
    TimestampTz deadlock_checker_start_time;
    const char* conflicting_lock_mode_name;
    ThreadId conflicting_lock_thread_id;
    bool conflicting_lock_by_holdlock;
    /*
     * Count of the number of fast path lock slots we believe to be used.  This
     * might be higher than the real number if another backend has transferred
     * our locks to the primary lock table, but it can never be lower than the
     * real value, since only we can acquire locks on our own behalf.
     */
    int FastPathLocalUseCount;
    volatile struct FastPathStrongRelationLockData* FastPathStrongRelationLocks;
    /*
     * Pointers to hash tables containing lock state
     *
     * The LockMethodLockHash and LockMethodProcLockHash hash tables are in
     * shared memory; LockMethodLocalHash is local to each backend.
     */
    struct HTAB* LockMethodLockHash;
    struct HTAB* LockMethodProcLockHash;
    struct HTAB* LockMethodLocalHash;
    /* private state for error cleanup */
    struct LOCALLOCK* StrongLockInProgress;
    struct LOCALLOCK* awaitedLock;
    struct ResourceOwnerData* awaitedOwner;
    /* PGPROC pointer of blocking data redistribution proc. */
    struct PGPROC* blocking_redistribution_proc;
    struct VirtualTransactionId* lock_vxids;

    /* if false, this transaction has the same timeout to check deadlock
     * with the others. if true, the larger timeout, the lower possibility
     * of aborting. the default timeout is defined by *DeadlockTimeout*.
     * one case is that, when VACUUM FULL PARTITION is running while another
     * transaction is also running, and deadlock maybe happens, so that one
     * of them shall abort and rollback. because VACUUM FULL is a heavy work
     * and we always want it to work done. so that we can enlarge the timeout
     * to check deadlock, then another transaction has the bigger possibility
     * to check and rollback.
     */
    bool EnlargeDeadlockTimeout;
    /* If we are waiting for a lock, this points to the associated LOCALLOCK */
    struct LOCALLOCK* lockAwaited;
    /* Mark these volatile because they can be changed by signal handler */
    volatile bool standby_timeout_active;
    volatile bool statement_timeout_active;
    volatile bool deadlock_timeout_active;
    volatile bool lockwait_timeout_active;
    volatile int deadlock_state;
    volatile bool cancel_from_timeout;
    /* timeout_start_time is set when log_lock_waits is true */
    TimestampTz timeout_start_time;
    /* statement_fin_time is valid only if statement_timeout_active is true */
    TimestampTz statement_fin_time;
    TimestampTz statement_fin_time2; /* valid only in recovery */
    /* restimems is used for timer pause */
    int restimems;
    bool timeIsPausing; /* To ensure invoke pause_sig_alarm and resume_sig_alarm properly */

    /* global variable */
    char* pageCopy;
    char* segPageCopy;

    bool isSwitchoverLockHolder;
    int num_held_lwlocks;
    struct LWLockHandle* held_lwlocks;
    int lock_addin_request;
    bool lock_addin_request_allowed;
    int counts_for_pid;
    int* block_counts;
    /* description, memory context opt */
    MemoryContext remote_function_context;
    bool work_env_init;

    shmem_startup_hook_type shmem_startup_hook;
    Size total_addin_request;
    bool addin_request_allowed;

    /*
     * This flag tracks whether we've called atexit() in the current process
     * (or in the parent postmaster).
     */
    bool atexit_callback_setup;
    ONEXIT on_proc_exit_list[MAX_ON_EXITS];
    ONEXIT on_shmem_exit_list[MAX_ON_EXITS];
    int on_proc_exit_index;
    int on_shmem_exit_index;

    union CmprMetaUnion* cmprMetaInfo;

    /* Thread share file id cache */
    struct HTAB* DataFileIdCache;
    /* Thread shared Seg Spc cache */
    struct HTAB* SegSpcCache;

    struct HTAB* uidHashCache;
    struct HTAB* DisasterCache;
    /*
     * Maximum number of file descriptors to open for either VFD entries or
     * AllocateFile/AllocateDir/OpenTransientFile operations.  This is initialized
     * to a conservative value, and remains that way indefinitely in bootstrap or
     * standalone-backend cases.  In normal postmaster operation, the postmaster
     * calls set_max_safe_fds() late in initialization to update the value, and
     * that value is then inherited by forked subprocesses. *
     * Note: the value of max_files_per_process is taken into account while
     * setting this variable, and so need not be tested separately.
     */
    int max_safe_fds; /* default if not changed */
    /* reserve `1000' for thread-private file id */
    int max_userdatafiles;

    int timeoutRemoteOpera;

} knl_t_storage_context;

typedef struct knl_t_port_context {
    char cryptresult[21]; /* encrypted result (1 + 4 + 4 + 11 + 1)*/
    char buf[24];

    bool thread_is_exiting;
    struct ThreadArg* m_pThreadArg;

    locale_t save_locale_r;
    NameData cur_datcollate; /* LC_COLLATE setting */
    NameData cur_datctype;   /* LC_CTYPE setting */
    NameData cur_monetary;   /* LC_MONETARY setting */
    NameData cur_numeric;    /* LC_NUMERIC setting */
} knl_t_port_context;

typedef struct knl_t_tsearch_context {
    int nres;
    int ntres;
} knl_t_tsearch_context;

typedef enum {
    NO_CHANGE = 0,
    OLD_REPL_CHANGE_IP_OR_PORT,
    ADD_REPL_CONN_INFO_WITH_OLD_LOCAL_IP_PORT,
    ADD_REPL_CONN_INFO_WITH_NEW_LOCAL_IP_PORT,
    ADD_DISASTER_RECOVERY_INFO
} ReplConnInfoChangeType;


typedef struct knl_t_postmaster_context {
/* Notice: the value is same sa GUC_MAX_REPLNODE_NUM */
#define MAX_REPLNODE_NUM 9
#define MAXLISTEN 64
#define IP_LEN 64

    /* flag when process startup packet for logic conn */
    bool ProcessStartupPacketForLogicConn;

    /* socket and port for recv gs_sock from receiver flow control*/
    int sock_for_libcomm;
    struct Port* port_for_libcomm;
    bool KeepSocketOpenForStream;

    /*
     * Stream replication connection info between primary, standby and secondary.
     *
     * ReplConn*1 is used to connect primary on standby, or standby on primary, or
     * connect primary or standby on secondary.
     * ReplConn*2 is used to connect secondary on primary or standby, or connect primary
     * or standby on secondary.
     */
    struct replconninfo* ReplConnArray[DOUBLE_MAX_REPLNODE_NUM + 1];
    int ReplConnChangeType[DOUBLE_MAX_REPLNODE_NUM + 1];
    struct replconninfo* CrossClusterReplConnArray[MAX_REPLNODE_NUM];
    bool CrossClusterReplConnChanged[MAX_REPLNODE_NUM];
    struct hashmemdata* HaShmData;

    /* The socket(s) we're listening to. */
    pgsocket ListenSocket[MAXLISTEN];
    char LocalAddrList[MAXLISTEN][IP_LEN];
    int LocalIpNum;
    int listen_sock_type[MAXLISTEN]; /* ori type: enum ListenSocketType */
    gs_thread_t CurExitThread;

    bool IsRPCWorkerThread;

    /* private variables for reaper backend thread */
    Latch ReaperBackendLatch;

    /* Database Security: Support database audit */
    bool audit_primary_start;
    bool audit_primary_failover;
    bool audit_standby_switchover;
    bool senderToDummyStandby;
    bool senderToBuildStandby;
    bool ReachedNormalRunning; /* T if we've reached PM_RUN */
    bool redirection_done;     /* stderr redirected for syslogger? */

    /* received START_AUTOVAC_LAUNCHER signal */
    volatile sig_atomic_t start_autovac_launcher;
    /* the launcher needs to be signalled to communicate some condition */
    volatile bool avlauncher_needs_signal;
    /* received PMSIGNAL_START_JOB_SCHEDULER signal */
    volatile sig_atomic_t start_job_scheduler;
    /* the jobscheduler needs to be signalled to communicate some condition */
    volatile bool jobscheduler_needs_signal;

    /*
     * State for assigning random salts and cancel keys.
     * Also, the global t_thrd.proc_cxt.MyCancelKey passes the cancel key assigned to a given
     * backend from the postmaster to that backend (via fork).
     */
    unsigned int random_seed;
    struct timeval random_start_time;

    /* key pair to be used as object id while using advisory lock for backup */
    Datum xc_lockForBackupKey1;
    Datum xc_lockForBackupKey2;

    bool forceNoSeparate;

#ifndef WIN32
    /*
     * File descriptors for pipe used to monitor if postmaster is alive.
     * First is POSTMASTER_FD_WATCH, second is POSTMASTER_FD_OWN.
     */
    int postmaster_alive_fds[2];
#endif

#ifndef WIN32
    int syslogPipe[2];
#endif

} knl_t_postmaster_context;

#define CACHE_BUFF_LEN 128

typedef struct knl_t_buf_context {
    char cash_buf[CACHE_BUFF_LEN];
    char config_opt_buf[256];
    char config_opt_reset_buf[256];
    /* this buffer is only used if errno has a bogus value */
    char errorstr_buf[48];
    char pg_rusage_show_buf[100];
    char unpack_sql_state_buf[12];
    char show_enable_dynamic_workload_buf[8];
    char show_enable_memory_limit_buf[8];
    char show_log_file_mode_buf[8];
    char show_memory_detail_tracking_buf[256];
    char show_tcp_keepalives_idle_buf[16];
    char show_tcp_keepalives_interval_buf[16];
    char show_tcp_keepalives_count_buf[16];
    char show_unix_socket_permissions_buf[8];
} knl_t_buf_context;
#define SQL_STATE_BUF_LEN 12

typedef struct knl_t_bootstrap_context {
#define MAXATTR 40
#define NAMEDATALEN 64
    int num_columns_read;
    int yyline; /* line number for error reporting */

    int MyAuxProcType;                                /* declared in miscadmin.h */
    struct RelationData* boot_reldesc;                /* current relation descriptor */
    struct FormData_pg_attribute* attrtypes[MAXATTR]; /* points to attribute info */
    int numattr;                                      /* number of attributes for cur. rel */

    struct typmap** Typ;
    struct typmap* Ap;

    MemoryContext nogc; /* special no-gc mem context */
    struct _IndexList* ILHead;
    char newStr[NAMEDATALEN + 1]; /* array type names < NAMEDATALEN long */
} knl_t_bootstrap_context;

typedef struct knl_t_locale_context {
    /* Environment variable storage area */
    char lc_collate_envbuf[LC_ENV_BUFSIZE];

    char lc_ctype_envbuf[LC_ENV_BUFSIZE];

    char lc_monetary_envbuf[LC_ENV_BUFSIZE];

    char lc_numeric_envbuf[LC_ENV_BUFSIZE];

    char lc_time_envbuf[LC_ENV_BUFSIZE];

} knl_t_locale_context;

typedef struct knl_t_stat_context {
    /*
     * thread statistics of bad block
     * used for cache current thread statistics of bad block
     * update global_bad_block_stat by pgstat_send_badblock_stat()
     */
    MemoryContext local_bad_block_mcxt;
    HTAB* local_bad_block_stat;
    volatile bool need_exit;
} knl_t_stat_context;

typedef struct knl_t_thread_pool_context {
    class ThreadPoolGroup* group;
    class ThreadPoolListener* listener;
    class ThreadPoolWorker* worker;
    class ThreadPoolScheduler* scheduler;
    class ThreadPoolStream* stream;
    bool reaper_dead_session;
} knl_t_thread_pool_context;

/* percentile sql responese time */
typedef struct knl_t_percentile_context {
    bool need_exit;
    volatile bool need_reset_timer;
    struct PGXCNodeAllHandles* pgxc_all_handles;
    volatile sig_atomic_t got_SIGHUP;
} knl_t_percentile_context;

typedef struct knl_t_perf_snap_context {
    volatile sig_atomic_t need_exit;
    volatile bool got_SIGHUP;
    time_t last_snapshot_start_time;

    struct pg_conn* connect; /* Cross database query */
    struct pg_result* res;
    int cancel_request;      /* connection cancel request */
    uint64 curr_table_size;  // Current table size
    uint64 curr_snapid;      // snapid is Increasing
    MemoryContext PerfSnapMemCxt;
    volatile bool request_snapshot;
    volatile bool is_mem_protect;
} knl_t_perf_snap_context;

#ifdef DEBUG_UHEAP
typedef struct knl_t_uheap_stats_context {
    /* monitoring infrastructure */
    struct UHeapStat_Collect *uheap_stat_shared;
    struct UHeapStat_Collect *uheap_stat_local;
} knl_t_uheap_stats_context;
#endif

typedef struct knl_t_ash_context {
    time_t last_ash_start_time;
    volatile sig_atomic_t need_exit;
    volatile bool got_SIGHUP;
    uint32 slot; // the slot of ActiveSessionHistArray
    struct wait_event_info* waitEventStr;
} knl_t_ash_context;

typedef struct knl_t_statement_context {
    volatile sig_atomic_t need_exit;
    volatile bool got_SIGHUP;
    int slow_sql_retention_time;
    int full_sql_retention_time;
    void *instr_prev_post_parse_analyze_hook;
} knl_t_statement_context;

/* Default send interval is 1s */
const int DEFAULT_SEND_INTERVAL = 1000;
typedef struct knl_t_heartbeat_context {
    volatile sig_atomic_t got_SIGHUP;
    volatile sig_atomic_t shutdown_requested;
    struct heartbeat_state* state;
    int total_failed_times;
    TimestampTz last_failed_timestamp;
} knl_t_heartbeat_context;

/* compaction and compaction worker use */
typedef struct knl_t_ts_compaction_context {
    volatile sig_atomic_t got_SIGHUP;
    volatile sig_atomic_t shutdown_requested;
    volatile sig_atomic_t sleep_long;
    MemoryContext compaction_mem_cxt;
    MemoryContext compaction_data_cxt;
} knl_t_ts_compaction_context;

typedef struct knl_t_security_policy_context {
    // memory context
    MemoryContext StringMemoryContext;
    MemoryContext VectorMemoryContext;
    MemoryContext MapMemoryContext;
    MemoryContext SetMemoryContext;
    MemoryContext OidRBTreeMemoryContext;

    // masking
    const char* prepare_stmt_name;
    int node_location;
} knl_t_security_policy_context;

typedef struct knl_t_security_ledger_context {
    void *prev_ExecutorEnd;
} knl_t_security_ledger_context;

typedef struct knl_t_csnmin_sync_context {
    volatile sig_atomic_t got_SIGHUP;
    volatile sig_atomic_t shutdown_requested;
} knl_t_csnmin_sync_context;

typedef struct knl_t_bgworker_context {
    slist_head  bgwlist;
    void   *bgwcontext;
    void   *bgworker;
    uint64  bgworkerId;
} knl_t_bgworker_context;

typedef struct knl_t_index_advisor_context {
    List* stmt_table_list;
    List* stmt_target_list;
}
knl_t_index_advisor_context;

#ifdef ENABLE_MOT
/* MOT thread attributes */
#define MOT_MAX_ERROR_MESSAGE 256
#define MOT_MAX_ERROR_FRAMES  32

#define MOT_LOG_BUF_SIZE_KB     1
#define MOT_MAX_LOG_LINE_LENGTH (MOT_LOG_BUF_SIZE_KB * 1024 - 1)

namespace MOT {
  struct StringBuffer;
}

struct mot_error_frame {
    int m_errorCode;
    int m_severity;
    const char* m_file;
    int m_line;
    const char* m_function;
    const char* m_entity;
    const char* m_context;
    char m_errorMessage[MOT_MAX_ERROR_MESSAGE];
};

typedef struct knl_t_mot_context {
    // last error
    int last_error_code;
    int last_error_severity;
    mot_error_frame error_stack[MOT_MAX_ERROR_FRAMES];
    int error_frame_count;

    // log line
    char log_line[MOT_MAX_LOG_LINE_LENGTH + 1];
    int log_line_pos;
    bool log_line_overflow;
    MOT::StringBuffer* log_line_buf;

    // misc
    uint8_t log_level;

    uint16_t currentThreadId;
    int currentNumaNodeId;

    int bindPolicy;
    unsigned int mbindFlags;
} knl_t_mot_context;
#endif

typedef struct knl_t_barrier_creator_context {
    volatile sig_atomic_t got_SIGHUP;
    volatile sig_atomic_t shutdown_requested;
    bool is_first_barrier;
    struct BarrierUpdateLastTimeInfo* barrier_update_last_time_info;
    List* archive_slot_names;
    uint64 first_cn_timeline;
} knl_t_barrier_creator_context;

typedef struct knl_t_barrier_preparse_context {
    volatile sig_atomic_t got_SIGHUP;
    volatile sig_atomic_t shutdown_requested;
} knl_t_barrier_preparse_context;


// the length of t_thrd.proxy_cxt.identifier
#define IDENTIFIER_LENGTH    64
typedef struct knl_t_proxy_context {
    char identifier[IDENTIFIER_LENGTH];
} knl_t_proxy_context;

#define DCF_MAX_NODES 10
/* For log ctrl. Willing let standby flush and apply log under RTO seconds */
typedef struct DCFLogCtrlData {
    int64 prev_sleep_time;
    int64 sleep_time;
    int64 balance_sleep_time;
    int64 prev_RTO;
    int64 current_RTO;
    uint64 sleep_count;
    uint64 sleep_count_limit;
    XLogRecPtr prev_flush;
    XLogRecPtr prev_apply;
    TimestampTz prev_reply_time;
    uint64 pre_rate1;
    uint64 pre_rate2;
    int64 prev_RPO;
    int64 current_RPO;
    bool just_keep_alive;
} DCFLogCtrlData;

typedef struct DCFStandbyInfo {
    /*
     * The xlog locations that have been received, written, flushed, and applied by
     * standby-side. These may be invalid if the standby-side is unable to or
     * chooses not to report these.
     */
    uint32 nodeID;
    bool isMember;
    bool isActive;
    XLogRecPtr receive;
    XLogRecPtr write;
    XLogRecPtr flush;
    XLogRecPtr apply;
    XLogRecPtr applyRead;

    /* local role on walreceiver, they will be sent to walsender */
    ServerMode peer_role;
    DbState peer_state;

    /* Sender's system clock at the time of transmission */
    TimestampTz sendTime;
} DCFStandbyInfo;

typedef struct DcfContextInfo {
    /* 1.Control dcf launch */
    bool isDcfStarted;
    slock_t dcfStartedMutex;

    /* 2.Control index save and truncate */
    bool isWalRcvReady;
    bool isRecordIdxBlocked;
    slock_t recordDcfIdxMutex;
    uint64 recordLsn;
    uint64 dcfRecordIndex;
    uint64 appliedLsn;          /* the lsn replayed */
    uint64 truncateDcfIndex;    /* consider set it properly after start */

    /* 3.Control promote */
    bool dcf_to_be_leader;      /* used by promoting leader */

    /* 4.Control full build */
    /* check version systemid and timeline between standby and primary */
    bool dcf_build_done;
    bool dcf_need_build_set;

    /* 5.Control config file sync */
    /* leader need sync config to follower after guc reload */
    bool dcfNeedSyncConfig;
    char gucconf_file[MAXPGPATH];
    char temp_guc_conf_file[MAXPGPATH];
    char bak_guc_conf_file[MAXPGPATH];
    char gucconf_lock_file[MAXPGPATH];
    TimestampTz last_sendfilereply_timestamp;
    int check_file_timeout;
    time_t standby_config_modify_time;
    time_t Primary_config_modify_time;
    struct ConfigModifyTimeMessage* reply_modify_message;

    /* 6.Control flow speed */
    struct DCFStandbyReplyMessage* dcf_reply_message;
    DCFStandbyInfo nodes_info[DCF_MAX_NODES];
    DCFLogCtrlData log_ctrl[DCF_MAX_NODES];
    int targetRTO;
    int targetRPO;
} DcfContextInfo;

/* dcf (distribute consensus frame work) */
typedef struct knl_t_dcf_context {
    bool isDcfShmemInited;
    bool is_dcf_thread;
    DcfContextInfo* dcfCtxInfo;
} knl_t_dcf_context;

typedef struct knl_t_lsc_context {
    LocalSysDBCache *lsc;
    bool enable_lsc;
    FetTupleFrom FetchTupleFromCatCList;
}knl_t_lsc_context;

/* replication apply launcher, for subscription */
typedef struct knl_t_apply_launcher_context {
    /* Flags set by signal handlers */
    volatile sig_atomic_t got_SIGHUP;
    volatile sig_atomic_t newWorkerRequest;
    volatile sig_atomic_t got_SIGTERM;
    bool onCommitLauncherWakeup;
    ApplyLauncherShmStruct *applyLauncherShm;
} knl_t_apply_launcher_context;

/* replication apply worker, for subscription */
typedef struct knl_t_apply_worker_context {
    /* Flags set by signal handlers */
    volatile sig_atomic_t got_SIGHUP;
    volatile sig_atomic_t got_SIGTERM;
    dlist_head lsnMapping;
    HTAB *logicalRepRelMap;
    TimestampTz sendTime;
    XLogRecPtr lastRecvpos;
    XLogRecPtr lastWritepos;
    XLogRecPtr lastFlushpos;

    Subscription *mySubscription;
    bool mySubscriptionValid;
    bool inRemoteTransaction;
    LogicalRepWorker *curWorker;
    MemoryContext messageContext;
    MemoryContext logicalRepRelMapContext;
    MemoryContext applyContext;
} knl_t_apply_worker_context;

typedef struct knl_t_publication_context {
    bool publications_valid;
    /* Map used to remember which relation schemas we sent. */
    HTAB* RelationSyncCache;
} knl_t_publication_context;

/* thread context. */
typedef struct knl_thrd_context {
    knl_thread_role role;
    knl_thread_role subrole;  /* we need some sub role status. */

    struct GsSignalSlot* signal_slot;
    /* Pointer to this process's PGPROC and PGXACT structs, if any */
    struct PGPROC* proc;
    struct PGXACT* pgxact;
    struct Backend* bn;
    int child_slot;
    bool is_inited; /* is new thread get new backend? */
    // we need to have a fake session to do some initialize
    knl_session_context* fake_session;

    int myLogicTid;

    MemoryContext top_mem_cxt;
    MemoryContextGroup* mcxt_group;
    knl_t_lsc_context lsc_cxt;

	/* variables to support comm proxy */
    CommSocketOption comm_sock_option;
    CommEpollOption comm_epoll_option;
    CommPollOption comm_poll_option;

    knl_t_aes_context aes_cxt;
    knl_t_aiocompleter_context aio_cxt;
    knl_t_alarmchecker_context alarm_cxt;
    knl_t_arch_context arch;
    knl_t_barrier_arch_context barrier_arch;
    knl_t_async_context asy_cxt;
    knl_t_audit_context audit;
    knl_t_autovacuum_context autovacuum_cxt;
    knl_t_basebackup_context basebackup_cxt;
    knl_t_bgwriter_context bgwriter_cxt;
    knl_t_bootstrap_context bootstrap_cxt;
    knl_t_pagewriter_context pagewriter_cxt;
    knl_t_pagerepair_context pagerepair_cxt;
    knl_t_sharestoragexlogcopyer_context sharestoragexlogcopyer_cxt;
    knl_t_barrier_preparse_context barrier_preparse_cxt;
    knl_t_buf_context buf_cxt;
    knl_t_bulkload_context bulk_cxt;
    knl_t_cbm_context cbm_cxt;
    knl_t_checkpoint_context checkpoint_cxt;
    knl_t_codegen_context codegen_cxt;
    knl_t_comm_context comm_cxt;
    knl_t_conn_context conn_cxt;
    knl_t_contrib_context contrib_cxt;
    knl_t_cstore_context cstore_cxt;
    knl_t_dataqueue_context dataqueue_cxt;
    knl_t_datarcvwriter_context datarcvwriter_cxt;
    knl_t_datareceiver_context datareceiver_cxt;
    knl_t_datasender_context datasender_cxt;
    knl_t_dfs_context dfs_cxt;
    knl_t_dynahash_context dyhash_cxt;
    knl_t_explain_context explain_cxt;
    knl_t_format_context format_cxt;
    knl_t_index_context index_cxt;
    knl_t_interrupt_context int_cxt;
    knl_t_job_context job_cxt;
    knl_t_libwalreceiver_context libwalreceiver_cxt;
    knl_t_locale_context lc_cxt;
    knl_t_log_context log_cxt;
    knl_t_logger_context logger;
    knl_t_logical_context logical_cxt;
    knl_t_libpq_context libpq_cxt;
    knl_t_lwlockmoniter_context lwm_cxt;
    knl_t_mem_context mem_cxt;
    knl_t_obs_context obs_cxt;
    knl_t_pgxc_context pgxc_cxt;
    knl_t_port_context port_cxt;
    knl_t_postgres_context postgres_cxt;
    knl_t_postmaster_context postmaster_cxt;
    knl_t_proc_context proc_cxt;
    knl_t_relopt_context relopt_cxt;
    knl_t_replgram_context replgram_cxt;
    knl_t_replscanner_context replscanner_cxt;
    knl_t_shemem_ptr_context shemem_ptr_cxt;
    knl_t_sig_context sig_cxt;
    knl_t_slot_context slot_cxt;
    knl_t_snapshot_context snapshot_cxt;
    knl_t_startup_context startup_cxt;
    knl_t_stat_context stat_cxt;
    knl_t_storage_context storage_cxt;
    knl_t_syncrep_context syncrep_cxt;
    knl_t_syncrepgram_context syncrepgram_cxt;
    knl_t_syncrepscanner_context syncrepscanner_cxt;
    knl_t_thread_pool_context threadpool_cxt;
    knl_t_time_context time_cxt;
    knl_t_tsearch_context tsearch_cxt;
    knl_t_twophasecleaner_context tpcleaner_cxt;
    knl_t_utils_context utils_cxt;
    knl_t_vacuum_context vacuum_cxt;
    knl_t_walsender_context walsender_cxt;
    knl_t_walrcvwriter_context walrcvwriter_cxt;
    knl_t_walreceiver_context walreceiver_cxt;
    knl_t_walreceiverfuncs_context walreceiverfuncs_cxt;
    knl_t_walwriter_context walwriter_cxt;
    knl_t_walwriterauxiliary_context walwriterauxiliary_cxt;
    knl_t_catchup_context catchup_cxt;
    knl_t_wlmthrd_context wlm_cxt;
    knl_t_xact_context xact_cxt;
    knl_t_xlog_context xlog_cxt;
    knl_t_percentile_context percentile_cxt;
    knl_t_perf_snap_context perf_snap_cxt;
    knl_t_page_redo_context page_redo_cxt;
    knl_t_parallel_decode_worker_context parallel_decode_cxt;
    knl_t_logical_read_worker_context logicalreadworker_cxt;
    knl_t_heartbeat_context heartbeat_cxt;
    knl_t_security_policy_context security_policy_cxt;
    knl_t_security_ledger_context security_ledger_cxt;
    knl_t_poolcleaner_context poolcleaner_cxt;
    knl_t_snapcapturer_context snapcapturer_cxt;
    knl_t_rbcleaner_context rbcleaner_cxt;
    knl_t_undo_context undo_cxt;
    knl_t_undolauncher_context undolauncher_cxt;
    knl_t_undoworker_context undoworker_cxt;
    knl_t_undorecycler_context undorecycler_cxt;
    knl_t_rollback_requests_context rollback_requests_cxt;
    knl_t_ts_compaction_context ts_compaction_cxt;
    knl_t_ash_context ash_cxt;
    knl_t_statement_context statement_cxt;
    knl_t_streaming_context streaming_cxt;
    knl_t_csnmin_sync_context csnminsync_cxt;
#ifdef ENABLE_MOT
    knl_t_mot_context mot_cxt;
#endif

    knl_t_gstat_context gstat_cxt;

#ifdef DEBUG_UHEAP
    knl_t_uheap_stats_context uheap_stats_cxt;
#endif

    knl_t_barrier_creator_context barrier_creator_cxt;
    knl_t_proxy_context proxy_cxt;
    knl_t_dcf_context dcf_cxt;
    knl_t_bgworker_context bgworker_cxt;
    knl_t_index_advisor_context index_advisor_cxt;
    knl_t_apply_launcher_context applylauncher_cxt;
    knl_t_apply_worker_context applyworker_cxt;
    knl_t_publication_context publication_cxt;
} knl_thrd_context;

#ifdef ENABLE_MOT
extern void knl_thread_mot_init();
#endif

extern void knl_t_syscache_init();
extern void knl_thread_init(knl_thread_role role);
extern THR_LOCAL knl_thrd_context t_thrd;

inline bool StreamThreadAmI()
{
    return (t_thrd.role == STREAM_WORKER || t_thrd.role == THREADPOOL_STREAM);
}

inline void StreamTopConsumerIam()
{
    t_thrd.subrole = TOP_CONSUMER;
}

inline bool StreamTopConsumerAmI()
{
    return (t_thrd.subrole == TOP_CONSUMER);
}

RedoInterruptCallBackFunc RegisterRedoInterruptCallBack(RedoInterruptCallBackFunc func);
void RedoInterruptCallBack();
RedoPageRepairCallBackFunc RegisterRedoPageRepairCallBack(RedoPageRepairCallBackFunc func);
void RedoPageRepairCallBack(RepairBlockKey key, XLogPhyBlock pblk);

#endif /* SRC_INCLUDE_KNL_KNL_THRD_H_ */
