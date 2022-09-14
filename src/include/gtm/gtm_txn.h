/* -------------------------------------------------------------------------
 *
 * gtm_txn.h
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * $PostgreSQL$
 *
 * -------------------------------------------------------------------------
 */
#ifndef _GTM_TXN_H
#define _GTM_TXN_H

#include "gtm/utils/libpq-be.h"
#include "gtm/gtm_c.h"
#include "gtm/gtm_lock.h"
#include "gtm/gtm_list.h"
#include "gtm/utils/stringinfo.h"
#include "gtm/gtm_msg.h"
#include "gtm/gtm_atomic.h"

typedef struct MemoryContextData* MemoryContext;

/* ----------------
 *		Special transaction ID values
 *
 * BootstrapGlobalTransactionId is the XID for "bootstrap" operations, and
 * FrozenGlobalTransactionId is used for very old tuples.  Both should
 * always be considered valid.
 *
 * FirstNormalGlobalTransactionId is the first "normal" transaction id.
 * Note: if you need to change it, you must change pg_class.h as well.
 * ----------------
 */
#define BootstrapGlobalTransactionId ((GlobalTransactionId)1)
#define FrozenGlobalTransactionId ((GlobalTransactionId)2)
#define FirstNormalGlobalTransactionId ((GlobalTransactionId)3)

/* ----------------
 *		transaction ID manipulation macros
 * ----------------
 */
#define GlobalTransactionIdIsNormal(xid) ((xid) >= FirstNormalGlobalTransactionId)
#define GlobalTransactionIdEquals(id1, id2) ((id1) == (id2))
#define GlobalTransactionIdStore(xid, dest) (*(dest) = (xid))
#define StoreInvalidGlobalTransactionId(dest) (*(dest) = InvalidGlobalTransactionId)

/* advance a transaction ID variable */
#define GlobalTransactionIdAdvance(dest) do { \
    (dest)++;                                    \
    if ((dest) < FirstNormalGlobalTransactionId) \
        (dest) = FirstNormalGlobalTransactionId; \
} while (0)

typedef int XidStatus;

#define TRANSACTION_STATUS_IN_PROGRESS 0x00
#define TRANSACTION_STATUS_COMMITTED 0x01
#define TRANSACTION_STATUS_ABORTED 0x02

#define XACT_RESFILE "pgxc_running_xact.ready"
#define XACT_DONEFILE "pgxc_running_xact.done"

#define ECTD_RETRY_TIMES 10
#define ETCD_RETRY_TIMEOUT 12 /* assume etcd RTO is 10s */

/*
 * check whether etcd retry limit exceeded
 */
extern bool etcd_retry_exceeded(time_t start_time, int count);

/*
 * prototypes for functions in transam/transam.c
 */
extern bool GlobalTransactionIdDidCommit(GlobalTransactionId transactionId);
extern bool GlobalTransactionIdDidAbort(GlobalTransactionId transactionId);
extern void GlobalTransactionIdAbort(GlobalTransactionId transactionId);

/*
 * GlobalTransactionIdPrecedes --- is id1 < id2?
 */
#define GlobalTransactionIdPrecedes(id1, id2) ((id1) < (id2))

/*
 * GlobalTransactionIdPrecedesOrEquals --- is id1 <= id2?
 */
#define GlobalTransactionIdPrecedesOrEquals(id1, id2) ((id1) <= (id2))

/*
 * GlobalTransactionIdFollows --- is id1 > id2?
 */
#define GlobalTransactionIdFollows(id1, id2) ((id1) > (id2))

/*
 * GlobalTransactionIdFollowsOrEquals --- is id1 >= id2?
 */
#define GlobalTransactionIdFollowsOrEquals(id1, id2) ((id1) >= (id2))

extern void GTM_CalculateLatestSnapshot(bool calc_xmin);

extern bool GTM_SetDisasterClusterToEtcd(const char* mainCluster);
extern char* GTM_GetDisasterClusterFromEtcd();
extern bool GTM_DelDisasterClusterFromEtcd();

/* in transam/varsup.c */
extern bool GTM_SetDoVacuum(GTM_TransactionHandle handle);
extern GlobalTransactionId GTM_GetGlobalTransactionId(GTM_TransactionHandle handle, bool is_sub_xact);
extern GlobalTransactionId GTM_GetGlobalTransactionIdMulti(GTM_TransactionHandle handle[], int txn_count,
                                                           bool is_sub_xact);
extern GlobalTransactionId ReadNewGlobalTransactionId(void);
extern void SetGlobalTransactionIdLimit(GlobalTransactionId oldest_datfrozenxid);
extern void SetNextGlobalTransactionId(GlobalTransactionId gxid);
extern void GTM_SetShuttingDown(void);

/* For restoration point backup */
extern bool GTM_NeedXidRestoreUpdate(void);
extern bool GTM_NeedCsnRestoreUpdate(void);
extern void GTM_WriteRestorePointXid(FILE* f, bool xid_restore);
extern void GTM_WriteRestorePointCSN(FILE* f);
extern void GTM_WriteTimeline(FILE* f);
extern void GTM_WriteTransactionLength(FILE* f);

typedef enum GTM_States {
    GTM_STARTING,
    GTM_RUNNING,
    GTM_SHUTTING_DOWN
} GTM_States;

/* Global transaction states at the GTM */
typedef enum GTM_TransactionStates {
    GTM_TXN_STARTING,
    GTM_TXN_IN_PROGRESS,
    GTM_TXN_PREPARE_IN_PROGRESS,
    GTM_TXN_PREPARED,
    GTM_TXN_COMMIT_IN_PROGRESS,
    GTM_TXN_COMMITTED,
    GTM_TXN_ABORT_IN_PROGRESS,
    GTM_TXN_ABORTED
} GTM_TransactionStates;

typedef struct GTM_TransactionInfo {
    GTM_TransactionHandle gti_handle;
    GTM_ThreadID gti_thread_id;
    uint32 gti_client_id;

    GTM_Atomic<uint64_t> gti_in_use;
    GlobalTransactionId gti_gxid;
    GTM_TransactionStates gti_state;
    char* gti_coordname;
    GlobalTransactionId gti_xmin;
    GlobalTransactionId gti_xmax; /* The max gxid of the current txn_slot(including sub xacts) */
    GTM_IsolationLevel gti_isolevel;
    bool gti_readonly;
    GTMProxy_ConnID gti_backend_id;
    char* nodestring; /* List of nodes prepared */
    char* gti_gid;

    GTM_SnapshotData gti_current_snapshot;
    bool gti_snapshot_set;

    volatile bool gti_vacuum;
    uint32 gti_timeline;
    GTM_RWLock gti_lock;
} GTM_TransactionInfo;

struct MemReservation {
    pthread_cond_t mem_Reserve_cond;
    int4 mem_to_reserve;
    char* rp_name;

    bool is_invalid;
};

/* By default a GID length is limited to 256 bits in openGauss */
#define GTM_MAX_GID_LEN 256
#define GTM_CheckTransactionHandle(x) ((x) >= 0 && (x) < gtm_max_trans)
#define GTM_IsTransSerializable(x) ((x)->gti_isolevel == GTM_ISOLATION_SERIALIZABLE)

typedef struct GTM_Transactions {
    uint32 gt_txn_count;
    GTM_States gt_gtm_state;

    GTM_RWLock gt_XidGenLock;

    /*
     * These fields are protected by XidGenLock
     */
    GlobalTransactionId gt_nextXid;     /* next XID to assign */
    volatile GlobalTransactionId gt_backedUpXid; /* backed up, restoration point */

    volatile uint64 gt_csn;                    /* current commit sequence number */
    volatile uint64 gt_consistencyPointCsn;
    volatile uint64 gt_baseConsistencyPoint;
    GlobalTransactionId gt_oldestXid; /* cluster-wide minimum datfrozenxid */

    /*
     * These fields are protected by TransArrayLock.
     */
    GlobalTransactionId gt_latestCompletedXid; /* newest XID that has committed or
                                                * aborted */

    GlobalTransactionId gt_xmin;
    GlobalTransactionId gt_recent_global_xmin;

    int32 gt_lastslot;
    GTM_TransactionInfo* gt_transactions_array;

    GTM_MutexLock gt_TransArrayLock;

    /*
     *  These fields are for lock-free snapshot management
     */
    MemoryContext gt_SnapCalcCtx;
    GTM_Snapshot gt_snapArray;
    volatile uint32_t gt_currentSnap;
    volatile uint32_t gt_nextSnap;
    uint32_t gt_numVersions;
    GTM_Atomic<uint32_t>* gt_rcArray;
    GTM_Atomic<uint32_t> gt_concurrentTransactions;
    uint32_t gt_snapshotPendingCnt;
} GTM_Transactions;

extern GTM_Transactions GTMTransactions;

/*
 * When gtm restart as primary or restart as pending->primary, its xid will increase 2000.
 * Then we should sync the new xid to standby, so we make the GTM_SyncGXIDFlag to true.
 * When a gtm client get a new xid from gtm primary, if the GTM_SyncGXIDFlag is true, it
 * will sync the new xid to stanby.
 */
extern volatile bool GTM_SyncGXIDFlag;

/*
 * This macro should be used with READ lock held on gt_TransArrayLock as the
 * number of open transactions might change when counting open transactions
 * if a lock is not hold.
 */
#define GTM_CountOpenTransactions() (GTMTransactions.gt_concurrentTransactions)

/*
 * Two hash tables will be maintained to quickly find the
 * GTM_TransactionInfo block given either the GXID or the GTM_TransactionHandle.
 */
GlobalTransactionId GTM_HandleToTransactionId(GTM_TransactionHandle handle);
GTM_TransactionInfo* GTM_HandleToTransactionInfo(GTM_TransactionHandle handle);
GTM_TransactionHandle GTM_GXIDToHandle(GlobalTransactionId gxid);
GTM_TransactionHandle GTM_GIDToHandle(const char* gid);

/* Transaction Control */
void GTM_InitTxnManager(void);
GTM_TransactionHandle GTM_BeginTransaction(const char* coord_name, GTM_IsolationLevel isolevel, bool readonly);
int GTM_BeginTransactionMulti(const char* coord_name, GTM_IsolationLevel isolevel[], const bool readonly[],
    GTMProxy_ConnID connid[], int txn_count, GTM_TransactionHandle txns[]);
bool GTM_FillTransactionInfo(const char* coord_name, GTM_TransactionHandle handle, GlobalTransactionId gxid,
    GTM_TransactionStates state, GlobalTransactionId xmin, bool vacuum, uint32 timeline);

int GTM_RollbackTransaction(GTM_TransactionHandle txn, bool isTimelineOK);
int GTM_RollbackTransactionMulti(GTM_TransactionHandle txn[], int txn_count, int status[], bool isTimelineOK);
int GTM_RollbackTransactionGXID(GlobalTransactionId gxid);
int GTM_CommitTransaction(GTM_TransactionHandle txn, bool isTimelineOK, uint64 *csn);
int GTM_CommitTransactionMulti(GTM_TransactionHandle txn[], int txn_count, int status[], bool isTimelineOK,
                               uint64 *csn);

int GTM_PrepareTransaction(GTM_TransactionHandle txn);
int GTM_StartPreparedTransaction(GTM_TransactionHandle txn, const char* gid, const char* nodestring);
int GTM_StartPreparedTransactionGXID(GlobalTransactionId gxid, const char* gid, const char* nodestring);
int GTM_GetGIDData(GTM_TransactionHandle prepared_txn, GlobalTransactionId* prepared_gxid, char** nodestring);
uint32 GTM_GetAllPrepared(GlobalTransactionId gxids[], uint32 gxidcnt);
GTM_TransactionStates GTM_GetStatus(GTM_TransactionHandle txn);
GTM_TransactionStates GTM_GetStatusGXID(GlobalTransactionId gxid);
int GTM_GetAllTransactions(GTM_TransactionInfo txninfo[], uint32 txncnt);
void GTM_RemoveAllTransInfos(int backend_id);
void GTM_RemoveAllTransInfosForClient(uint32 client_id, int backend_id);
void GTM_RemoveAllTransInfosOutofTimeline(char* nodename, uint32 timeline);

GTM_Snapshot GTM_GetSnapshotData(GTM_TransactionInfo* my_txninfo, GTM_Snapshot snapshot);
GTM_Snapshot GTM_GetTransactionSnapshot(GTM_TransactionHandle handle[], int txn_count, int* status);
void GTM_FreeCachedTransInfo(void);

void ProcessResetTrasactionXminCommand(Port* myport, StringInfo message);
void ProcessSetVacuumCommand(Port* myport, StringInfo message);
void ProcessBeginTransactionCommand(Port* myport, StringInfo message);

void ProcessBeginTransactionCommandMulti(Port* myport, StringInfo message);
void ProcessBeginTransactionGetGXIDCommand(Port* myport, StringInfo message);
void ProcessCommitTransactionCommand(Port* myport, StringInfo message, bool is_backup);
void ProcessCommitPreparedTransactionCommand(Port* myport, StringInfo message, bool is_backup);
void ProcessRollbackTransactionCommand(Port* myport, StringInfo message, bool is_backup);
void ProcessPrepareTransactionCommand(Port* myport, StringInfo message, bool is_backup);
void ProcessGetGIDDataTransactionCommand(Port* myport, StringInfo message);

void ProcessGetGXIDTransactionCommand(Port* myport, StringInfo message, bool is_sub_xact);

void ProcessGXIDListCommand(Port* myport, StringInfo message);
void ProcessGetNextGXIDTransactionCommand(Port* myport, StringInfo message);
void ProcessGetNextCSNTransactionCommand(Port* myport, StringInfo message, bool send_timestamp);
void ProcessGetNextCSNTransactionLiteCommand(Port* myport, StringInfo message, bool send_timestamp);
void ProcessSetConsistencyPointTransactionCommand(Port* myport, StringInfo message);
void ProcessGetGlobalXminTransactionCommand(Port* myport, StringInfo message);
void ProcessGetTimelineTransactionCommand(Port* myport, StringInfo message);

void ProcessBeginTransactionGetGXIDAutovacuumCommand(Port* myport, StringInfo message);

/*
 *  * workload manager
 *   */
struct GTM_WorkloadManager {
    int free_server_mem;
    pthread_mutex_t workldmgr_mem_mutex;
};

void ProcessWorkloadManagerInitCommand(Port* myport, StringInfo message);
void ProcessWorkloadManagerReserveMemCommand(Port* myport, StringInfo message);
void ProcessWorkloadManagerReleaseMemCommand(Port* myport, StringInfo message);

void ProcessBeginTransactionGetGXIDCommandMulti(Port* myport, StringInfo message);
void ProcessCommitTransactionCommandMulti(Port* myport, StringInfo message, bool is_backup);
void ProcessRollbackTransactionCommandMulti(Port* myport, StringInfo message, bool is_backup);

void GTM_SaveTxnInfo(FILE* ctlf);
void GTM_SaveTimelineInfo(FILE* ctlf);
void GTM_SaveTransactionLength(FILE* ctlf);

void GTM_RestoreTxnInfo(FILE* ctlf, GlobalTransactionId next_gxid);
void GTM_RestoreTimelineInfo(FILE* ctlf, GTM_Timeline* timeline);
void GTM_RestoreTransactionsLen(FILE* ctlf, uint32* len);

void ProcessBkupGtmControlFileGXIDCommand(Port* myport, StringInfo message);
void ProcessBkupGtmControlFileTIMELINECommand(Port* myport, StringInfo message);

void GTM_SyncGXID(GlobalTransactionId xid, int txn_count = 0);
bool GTM_SyncXidToStandby(GlobalTransactionId xid);
bool GTM_SetXidToEtcd(GlobalTransactionId xid, bool save, GlobalTransactionId etcd_xid = InvalidGlobalTransactionId);
bool GTMSyncXidToEtcdInternal(GlobalTransactionId xid, bool force,
                              GlobalTransactionId etcd_xid = InvalidGlobalTransactionId);
bool GTM_SetGlobalMaxXidToEtcd(GlobalTransactionId xid, bool save, int txn_count = 0);
bool GTM_GetXidFromEtcd(GlobalTransactionId& xid, bool force, bool need_retry = false);
void GTM_GetSyncXidFromEtcd(const char* errString);

void GTM_SyncTimeline(GTM_Timeline timeline);
bool GTM_SyncTimelineToStandby(GTM_Timeline timeline);
bool GTM_SetTimelineToEtcd(GTM_Timeline timeline);
bool GTM_GetTimelineFromEtcd(GTM_Timeline& timeline);

bool GTM_SetConsistencyPointToEtcd(uint64 cp_csn);
bool GTM_GetConsistencyPointFromEtcd();

void ProcessWorkloadManagerInitCommand(Port* myport, StringInfo message, bool is_backup);
void ProcessWorkloadManagerReserveMemCommand(Port* myport, StringInfo message, bool is_backup);
void ProcessWorkloadManagerReleaseMemCommand(Port* myport, StringInfo message, bool is_backup);

extern void initialize_resource_pool(void);
int CheckInitialization(Port* myport, StringInfo message, GTM_MessageType mtype);
void ProcessWorkloadManagerInitializeResourcePoolCommand(Port* myport, StringInfo message, bool is_backup);
void ProcessWorkloadManagerCreateResourcePoolCommand(Port* myport, StringInfo message, bool is_backup);
void ProcessWorkloadManagerUpdateResourcePoolCommand(Port* myport, StringInfo message, bool is_backup);
void ProcessWorkloadManagerDeleteResourcePoolCommand(Port* myport, StringInfo message, bool is_backup);

/*
 * In gtm_snap.c
 */
void ProcessGetSnapshotCommand(Port* myport, StringInfo message, bool get_gxid);
void ProcessGetSnapshotLiteCommand(Port* myport, StringInfo message);
void ProcessGetSnapshotDRCommand(Port* myport, StringInfo message);
void ProcessGetSnapshotStatusCommand(Port* myport, StringInfo message);
void ProcessGetGTMLiteStatusCommand(Port* myport, StringInfo message);
void GTM_FreeSnapshotData(GTM_Snapshot snapshot);
void ProcessSetDisasterClusterCommand(Port *myport, GTM_MessageType mtype, StringInfo message);
void ProcessGetDisasterClusterCommand(Port *myport, GTM_MessageType mtype, StringInfo message);
void ProcessDelDisasterClusterCommand(Port *myport, GTM_MessageType mtype, StringInfo message);
#endif
