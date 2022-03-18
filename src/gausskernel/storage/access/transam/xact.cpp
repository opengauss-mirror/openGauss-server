/* -------------------------------------------------------------------------
 *
 * xact.cpp
 *	  top level transaction system support routines
 *
 * See src/backend/access/transam/README for more information.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/access/transam/xact.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#ifdef PGXC
#include "pgxc/pgxc.h"
#include "access/gtm.h"
/* PGXC_COORD */
#include "gtm/gtm_c.h"
#include "gtm/gtm_txn.h"
#include "pgxc/execRemote.h"
#include "pgxc/pgxcXact.h"
/* PGXC_DATANODE */
#include "postmaster/autovacuum.h"
#include "libpq/pqformat.h"
#include "libpq/libpq.h"
#endif
#include "access/clog.h"
#include "access/csnlog.h"
#include "access/cstore_am.h"
#include "access/cstore_rewrite.h"
#include "access/multixact.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "access/twophase.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "access/multi_redo_api.h"
#include "catalog/catalog.h"
#include "catalog/namespace.h"
#include "catalog/pg_authid.h"
#include "catalog/storage.h"
#include "commands/async.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "commands/trigger.h"
#include "commands/sequence.h"
#include "commands/verify.h"
#include "catalog/pg_hashbucket_fn.h"
#include "distributelayer/streamCore.h"
#include "catalog/storage_xlog.h"
#include "distributelayer/streamMain.h"
#include "executor/lightProxy.h"
#include "executor/spi.h"
#include "libpq/be-fsstubs.h"
#include "miscadmin.h"
#include "opfusion/opfusion.h"
#include "pgstat.h"
#include "pgxc/groupmgr.h"
#include "replication/datasyncrep.h"
#include "replication/datasender.h"
#include "replication/dataqueue.h"
#include "replication/logical.h"
#include "replication/logicallauncher.h"
#include "replication/walsender.h"
#include "replication/syncrep.h"
#include "replication/origin.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/procarray.h"
#include "storage/sinvaladt.h"
#include "storage/smgr/smgr.h"
#include "utils/combocid.h"
#include "utils/guc.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/plog.h"
#include "utils/relmapper.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "pg_trace.h"
#include "utils/distribute_test.h"
#include "storage/cstore/cstore_mem_alloc.h"
#include "workload/cpwlm.h"
#include "instruments/ash.h"
#include "instruments/instr_workload.h"
#include "gstrace/gstrace_infra.h"
#include "gstrace/access_gstrace.h"
#include "instruments/instr_statement.h"
#include "access/ustore/knl_undorequest.h"
#include "access/ustore/undo/knl_uundoapi.h"
#include "access/ustore/undo/knl_uundozone.h"
#include "commands/sequence.h"
#include "postmaster/bgworker.h"
#include "replication/walreceiver.h"
#ifdef ENABLE_MULTIPLE_NODES
#include "tsdb/cache/queryid_cachemgr.h"
#include "tsdb/cache/part_cachemgr.h"
#include "tsdb/storage/part.h"
#endif   /* ENABLE_MULTIPLE_NODES */

extern void CodeGenThreadTearDown();
extern void CleanupDfsHandlers(bool isTop);
extern void deleteGlobalOBSInstrumentation();
extern void CancelAutoAnalyze();
extern void rollback_searchlet();
extern void reset_searchlet_id();
extern void CodeGenThreadReset();
extern void uuid_struct_destroy_function();

THR_LOCAL bool CancelStmtForReadOnly = false; /* just need cancel stmt once when DefaultXactReadOnly=true */
THR_LOCAL bool TwoPhaseCommit = false;


extern bool is_user_name_changed();
extern void HDFSAbortCacheBlock();
extern THR_LOCAL Oid lastUDFOid;
#define MAX_GID_LENGTH 256

#define SPI_COMMIT 0
#define SPI_ROLLBACK 1

/*
 *	transaction states - transaction state from server perspective
 */
typedef enum TransState {
    TRANS_DEFAULT,    /* idle */
    TRANS_START,      /* transaction starting */
    TRANS_INPROGRESS, /* inside a valid transaction */
    TRANS_COMMIT,     /* commit in progress */
    TRANS_ABORT,      /* abort in progress */
    TRANS_PREPARE,     /* prepare in progress */
    TRANS_UNDO        /* applying undo */
} TransState;

/*
 *	transaction block states - transaction state of client queries
 *
 * Note: the subtransaction states are used only for non-topmost
 * transactions; the others appear only in the topmost transaction.
 */
typedef enum TBlockState {
    /* not-in-transaction-block states */
    TBLOCK_DEFAULT, /* idle */
    TBLOCK_STARTED, /* running single-query transaction */

    /* transaction block states */
    TBLOCK_BEGIN,         /* starting transaction block */
    TBLOCK_INPROGRESS,    /* live transaction */
    TBLOCK_END,           /* COMMIT received */
    TBLOCK_ABORT,         /* failed xact, awaiting ROLLBACK */
    TBLOCK_ABORT_END,     /* failed xact, ROLLBACK received */
    TBLOCK_ABORT_PENDING, /* live xact, ROLLBACK received */
    TBLOCK_PREPARE,       /* live xact, PREPARE received */
    TBLOCK_UNDO,          /* Need rollback to be executed for this topxact */

    /* subtransaction states */
    TBLOCK_SUBBEGIN,         /* starting a subtransaction */
    TBLOCK_SUBINPROGRESS,    /* live subtransaction */
    TBLOCK_SUBRELEASE,       /* RELEASE received */
    TBLOCK_SUBCOMMIT,        /* COMMIT received while TBLOCK_SUBINPROGRESS */
    TBLOCK_SUBABORT,         /* failed subxact, awaiting ROLLBACK */
    TBLOCK_SUBABORT_END,     /* failed subxact, ROLLBACK received */
    TBLOCK_SUBABORT_PENDING, /* live subxact, ROLLBACK received */
    TBLOCK_SUBRESTART,       /* live subxact, ROLLBACK TO received */
    TBLOCK_SUBABORT_RESTART, /* failed subxact, ROLLBACK TO received */
    TBLOCK_SUBUNDO           /* Need rollback to be executed for this subxact */
} TBlockState;

/*
 *	transaction state structure
 */
struct TransactionStateData {
#ifdef PGXC /* PGXC_COORD */
    /* my GXID, or Invalid if none */
    GlobalTransactionId transactionId;
    GTM_TransactionKey txnKey;
    bool isLocalParameterUsed; /* Check if a local parameter is active
                                * in transaction block (SET LOCAL, DEFERRED) */
    DList *savepointList;      /* SavepointData list */
#else
    TransactionId transactionId; /* my XID, or Invalid if none */
#endif
    SubTransactionId subTransactionId;   /* my subxact ID */
    char *name;                          /* savepoint name, if any */
    int savepointLevel;                  /* savepoint level */
    TransState state;                    /* low-level state */
    TBlockState blockState;              /* high-level state */
    int nestingLevel;                    /* transaction nesting depth */
    int gucNestLevel;                    /* GUC context nesting depth */
    MemoryContext curTransactionContext; /* my xact-lifetime context */
    ResourceOwner curTransactionOwner;   /* my query resources */
    TransactionId *childXids;            /* subcommitted child XIDs, in XID order */
    int nChildXids;                      /* # of subcommitted child XIDs */
    int maxChildXids;                    /* allocated size of childXids[] */
    Oid prevUser;                        /* previous CurrentUserId setting */
    int prevSecContext;                  /* previous SecurityRestrictionContext */
    bool prevXactReadOnly;               /* entry-time xact r/o state */
    bool startedInRecovery;              /* did we start in recovery? */
    bool didLogXid;                      /* has xid been included in WAL record? */
    struct TransactionStateData* parent; /* back link to parent */

#ifdef ENABLE_MOT
    /* which storage engine tables are used in current transaction for D/I/U/S statements */
    StorageEngineType storageEngineType;
#endif

    UndoRecPtr first_urp[UNDO_PERSISTENCE_LEVELS]; /* First UndoRecPtr create by this transaction */
    UndoRecPtr latest_urp[UNDO_PERSISTENCE_LEVELS]; /* Last UndoRecPtr created by this transaction */
    UndoRecPtr latest_urp_xact[UNDO_PERSISTENCE_LEVELS]; /* Last UndoRecPtr created by this transaction including its
                                                          * parent if any */
    bool perform_undo;
    bool  subXactLock;
};

/*
 * CurrentTransactionState always points to the current transaction state
 * block.  It will point to TopTransactionStateData when not in a
 * transaction at all, or when in a top-level transaction.
 */
static THR_LOCAL TransactionStateData TopTransactionStateData = {
#ifdef PGXC
    0,                                                        /* global transaction id */
    { InvalidTransactionHandle, InvalidTransactionTimeline }, /* transaction key in GTM */
    false,                                                    /* isLocalParameterUsed */
    NULL,
#else
    0,                           /* transaction id */
#endif
    0,                  /* subtransaction id */
    NULL,               /* savepoint name */
    0,                  /* savepoint level */
    TRANS_DEFAULT,      /* transaction state */
    TBLOCK_DEFAULT,     /* transaction block state from the client perspective */
    0,                  /* transaction nesting depth */
    0,                  /* GUC context nesting depth */
    NULL,               /* cur transaction context */
    NULL,               /* cur transaction resource owner */
    NULL,               /* subcommitted child Xids */
    0,                  /* # of subcommitted child Xids */
    0,                  /* allocated size of childXids[] */
    InvalidOid,         /* previous CurrentUserId setting */
    0,                  /* previous SecurityRestrictionContext */
    false,              /* entry-time xact r/o state */
    false,              /* startedInRecovery */
    false,              /* didLogXid */
#ifdef ENABLE_MOT
    NULL,               /* link to parent state block */
    SE_TYPE_UNSPECIFIED /* storage engine used in transaction */
#else
    NULL                /* link to parent state block */
#endif
};

static THR_LOCAL TransactionState CurrentTransactionState = NULL;
static THR_LOCAL TBlockState SavedSTPTransactionBlockState = TBLOCK_DEFAULT;
static THR_LOCAL TransState SavedSTPTransactionState = TRANS_DEFAULT;

/*
 * PGXC receives from GTM a timestamp value at the same time as a GXID
 * This one is set as GTMxactStartTimestamp and is a return value of now(), current_transaction().
 * GTMxactStartTimestamp is also sent to each node with gxid and snapshot and delta is calculated locally.
 * GTMdeltaTimestamp is used to calculate current_statement as its value can change
 * during a transaction. Delta can have a different value through the nodes of the cluster
 * but its uniqueness in the cluster is maintained thanks to the global value GTMxactStartTimestamp.
 */
#ifdef PGXC
static THR_LOCAL TimestampTz stmtSysGTMdeltaTimestamp = 0;
#endif
extern THR_LOCAL int UDFRPCSocket;

/*
 * List of add-on start- and end-of-xact callbacks
 */
typedef struct XactCallbackItem {
    struct XactCallbackItem *next;
    XactCallback callback;
    void *arg;
} XactCallbackItem;

/*
 * List of add-on start- and end-of-subxact callbacks
 */
typedef struct SubXactCallbackItem {
    struct SubXactCallbackItem *next;
    SubXactCallback callback;
    void *arg;
} SubXactCallbackItem;

#ifdef PGXC
/*
 * List of callback items for GTM.
 * Those are called at transaction commit/abort to perform actions
 * on GTM in order to maintain data consistency on GTM with other cluster nodes.
 */
typedef struct GTMCallbackItem {
    struct GTMCallbackItem *next;
    GTMCallback callback;
    void *arg;
} GTMCallbackItem;
#endif

#ifdef ENABLE_MOT
typedef struct RedoCommitCallbackItem {
    struct RedoCommitCallbackItem* next;
    RedoCommitCallback callback;
    void* arg;
} RedoCommitCallbackItem;
#endif

/* local function prototypes */
static void AssignTransactionId(TransactionState s);
static void AbortTransaction(bool PerfectRollback = false, bool STP_rollback = false);
static void AtAbort_Memory(void);
static void AtCleanup_Memory(void);
static void AtAbort_ResourceOwner(void);
static void AtCCI_LocalCache(void);
static void AtCommit_Memory(void);
static void AtStart_Cache(void);
static void AtStart_Memory(void);
static void AtStart_ResourceOwner(void);
static void CallSubXactCallbacks(SubXactEvent event, SubTransactionId mySubid, SubTransactionId parentSubid);
#ifdef PGXC
static void CleanSequenceCallbacks(void);
static void CallSequenceCallbacks(GTMEvent event);
static void DeleteSavepoint(DList **dlist, DListCell *cell);
static void SendOneSavepointToRemoteCoordinators(const char *cmd, const char *name, SavepointStmtType stmtType,
                                                 GlobalTransactionId transactionId = InvalidTransactionId);

#endif
static void CleanupTransaction(void);
static void CommitTransaction(bool STP_commit = false);
static TransactionId RecordTransactionAbort(bool isSubXact);
static void StartTransaction(bool begin_on_gtm);

static void StartSubTransaction(void);
static void CommitSubTransaction(bool STP_commit = false);
static void PushTransaction(void);
static void PopTransaction(void);

static void AtSubAbort_Memory(void);
static void AtSubCleanup_Memory(void);
static void AtSubAbort_ResourceOwner(void);
static void AtSubCommit_Memory(void);
static void AtSubStart_Memory(void);
static void AtSubStart_ResourceOwner(void);

static void ShowTransactionState(const char *str);
static void ShowTransactionStateRec(TransactionState state);
static const char *BlockStateAsString(TBlockState blockState);
static const char *TransStateAsString(TransState state);
static void PrepareTransaction(bool STP_commit = false);

extern void print_leak_warning_at_commit();
#ifndef ENABLE_LLT
extern void clean_ec_conn();
extern void delete_ec_ctrl();
#endif

void InitTopTransactionState(void)
{
    TopTransactionStateData = {
#ifdef PGXC
        0,
        { InvalidTransactionHandle, InvalidTransactionTimeline },
        false,
        NULL,
#else
        0,
#endif
        0,
        NULL,
        0,
        TRANS_DEFAULT,
        TBLOCK_DEFAULT,
        0,
        0,
        NULL,
        NULL,
        NULL,
        0,
        0,
        InvalidOid,
        0,
        false,
        false,
        false,
        NULL
    };
}


/* ----------------------------------------------------------------
 *      Transaction routines for store procedure
 *     
 * For supporting transaction state in transaction block(transaction is started 
 * by begin/start statement) transforms with transaction state in STP's. When the 
 * STP run in transaction started by begin/start statement and STP contains transaction 
 * statement, it should call SaveCurrentSTPTopTransactionState() to save Top 
 * Transaction's state and restore transaction state with RestoreCurrentSTPTopTransactionState.
 * to keep the Top transaction state. 
 *
 * ----------------------------------------------------------------
 */

void SaveCurrentSTPTopTransactionState()
{
    TransactionState s = &TopTransactionStateData;

    /* Only support call this in STP.*/
    if (u_sess->SPI_cxt._stack == NULL || !u_sess->SPI_cxt.is_stp) {
        ereport(FATAL, (errcode(ERRCODE_INVALID_TRANSACTION_STATE), 
            errmsg("SaveCurrentSTPTopTransactionState can only be called in STP.")));
    }

    /* TBLOCK_STARTED: the transaction block state when call or select STP immediately. */
    /* TBLOCK_INPROGRESS: the transaction block state when call or select STP in existed transactions. */
    if (s->blockState != TBLOCK_STARTED 
        && s->blockState != TBLOCK_INPROGRESS) {
        ereport(FATAL, (errcode(ERRCODE_INVALID_TRANSACTION_STATE), 
            errmsg("SaveCurrentSTPTopTransactionState: unexpected state %s", 
            BlockStateAsString(s->blockState))));
    }

    SavedSTPTransactionBlockState = s->blockState;
    SavedSTPTransactionState = s->state;
}

void RestoreCurrentSTPTopTransactionState()
{
    TransactionState s = &TopTransactionStateData;

    /* Only support call this in STP.*/
    if (u_sess->SPI_cxt._stack == NULL || !u_sess->SPI_cxt.is_stp) {
        ereport(FATAL, (errcode(ERRCODE_INVALID_TRANSACTION_STATE), 
            errmsg("RestoreCurrentSTPTopTransactionState can only be called in STP.")));
    }

    /* STP's transaction state can't be TBLOCK_DEFAULT. */	
    if (SavedSTPTransactionBlockState == TBLOCK_DEFAULT) {
        ereport(FATAL, (errcode(ERRCODE_INVALID_TRANSACTION_STATE), 
            errmsg("RestoreCurrentSTPTopTransactionState:NULL, call RestoreCurrentSTPTopTransactionState firstly.")));
    }

    /* TBLOCK_STARTED: The transaction block state whatever call or select STP immediately or in */ 
    /* outer transaction block. */
    /* TBLOCK_INPROGRESS: the transaction block state when call or select STP in existed sub transaction. */
    if (s->blockState != TBLOCK_STARTED
        && s->blockState != TBLOCK_INPROGRESS) {
        ereport(FATAL, (errcode(ERRCODE_INVALID_TRANSACTION_STATE), 
            errmsg("RestoreCurrentSTPTopTransactionState: unexpected state %s", 
            BlockStateAsString(s->blockState))));
    }

    s->blockState = SavedSTPTransactionBlockState;
    SavedSTPTransactionBlockState = TBLOCK_DEFAULT;
    s->state = SavedSTPTransactionState;
    SavedSTPTransactionState = TRANS_DEFAULT;
}

bool IsStpInOuterSubTransaction() 
{
    TransactionState s = CurrentTransactionState;
    if (s->blockState >= TBLOCK_SUBBEGIN
        && u_sess->SPI_cxt.is_stp 
        && s->nestingLevel > (u_sess->SPI_cxt.portal_stp_exception_counter
            + u_sess->plsql_cxt.stp_savepoint_cnt + 1)) {
        return true;
    }
    return false;
}

/* ----------------------------------------------------------------
 *	transaction initialization routines
 * ----------------------------------------------------------------
 */
void InitCurrentTransactionState(void)
{
    CurrentTransactionState = &TopTransactionStateData;
}

/* ----------------------------------------------------------------
 *	Get transaction list
 * ----------------------------------------------------------------
 */
List* GetTransactionList(List *head)
{
    TransactionState s = GetCurrentTransactionState();
    MemoryContext savedCxt = MemoryContextSwitchTo(t_thrd.mem_cxt.portal_mem_cxt);
    int level = 0;
    for (; s != NULL; s = s->parent) {
        if (OidIsValid(s->prevUser)) {
            transactionNode* node = (transactionNode *)palloc0(sizeof(transactionNode));
            node->level = level;
            node->userId = s->prevUser;
            node->secContext = s->prevSecContext;
            head = lappend(head, node);
            level++;
        }
    }
    MemoryContextSwitchTo(savedCxt);
    return head;
}

/* ----------------------------------------------------------------
 *	transaction state accessors
 * ----------------------------------------------------------------
 */
/*
 *	IsTransactionState
 *
 *	This returns true if we are inside a valid transaction; that is,
 *	it is safe to initiate database access, take heavyweight locks, etc.
 */
bool IsTransactionState(void)
{
    TransactionState s = CurrentTransactionState;

    /*
     * TRANS_DEFAULT and TRANS_ABORT are obviously unsafe states.  However, we
     * also reject the startup/shutdown states TRANS_START, TRANS_COMMIT,
     * TRANS_PREPARE since it might be too soon or too late within those
     * transition states to do anything interesting.  Hence, the only "valid"
     * state is TRANS_INPROGRESS.
     */
    return (s->state == TRANS_INPROGRESS);
}

#ifdef ENABLE_MULTIPLE_NODES 
static void CheckDeleteLock(bool is_commit)
{
    ereport(DEBUG2, (errcode(ERRCODE_INVALID_TABLE_DEFINITION), errmsg("check delete query enter.")));
    if (g_instance.attr.attr_common.enable_tsdb  && 
        Tsdb::TableStatus::GetInstance().is_in_deletion()) {
        Tsdb::TableStatus::GetInstance().remove_query();
    }

    return;    
}
#endif   /* ENABLE_MULTIPLE_NODES */ 

bool WorkerThreadCanSeekAnotherMission(ThreadStayReason* reason)
{
    TransactionState s = CurrentTransactionState;

    if (t_thrd.threadpool_cxt.reaper_dead_session) {
        *reason = TWORKER_PREDEADSESSION;
        return true;
    }

    if (u_sess->status == KNL_SESS_UNINIT) {
        *reason = TWORKER_PREDEADSESSION;
        t_thrd.threadpool_cxt.reaper_dead_session = true;
        return true;
    }

    /* can not release worker if session is holding session level lock */
    for (int i = 0; i < MAX_LOCKMETHOD; i++) {
        if (u_sess->storage_cxt.holdSessionLock[i]) {
            *reason = TWORKER_HOLDSESSIONLOCK;
            return false;
        }
    }

    /* can not release worker if dn is in the process of 2pc, otherwise commit-prepare might get blocked */
    if (u_sess->storage_cxt.twoPhaseCommitInProgress) {
        *reason = TWORKER_TWOPHASECOMMIT;
        return false;
    }

    /* can not release worker if session is holding lwlocks, e.g. barrier lock */
    if (t_thrd.storage_cxt.num_held_lwlocks > 0) {
        *reason = TWORKER_HOLDLWLOCK;
        return false;
    }

    /* can not release worker if we have been assigned next global xid */
    if (t_thrd.xact_cxt.next_xid != InvalidTransactionId) {
        *reason = TWORKER_GETNEXTXID;
        return false;
    }
    /* can not release worker if session is inside a transaction */
    if (s->blockState != TBLOCK_DEFAULT) {
        *reason = TWORKER_STILLINTRANS;
        return false;
    } else {
        if (t_thrd.libpq_cxt.PqRecvPointer < t_thrd.libpq_cxt.PqRecvLength) {
            *reason = TWORKER_UNCONSUMEMESSAGE;
            return false;
        } else {
            *reason = TWORKER_CANSEEKNEXTSESSION;
            return true;
        }
    }
}

/*
 *	IsAbortedTransactionBlockState
 *
 *	This returns true if we are within an aborted transaction block.
 */
bool IsAbortedTransactionBlockState(void)
{
    TransactionState s = CurrentTransactionState;

    if (s->blockState == TBLOCK_ABORT || s->blockState == TBLOCK_SUBABORT)
        return true;

    return false;
}

void CleanUpDnHashTable(void)
{
    if ((StreamThreadAmI() && StreamTopConsumerAmI()) || !StreamThreadAmI()) {
        if (IsUnderPostmaster && (GetUserId() != BOOTSTRAP_SUPERUSERID)) {
            (void)LWLockAcquire(DnUsedSpaceHashLock, LW_EXCLUSIVE);
            hash_search(g_instance.comm_cxt.usedDnSpace, &u_sess->debug_query_id, HASH_REMOVE, NULL);
            LWLockRelease(DnUsedSpaceHashLock);
        }
    }
}

void RemoveFromDnHashTable(void)
{
    if (IS_PGXC_DATANODE && (u_sess->debug_query_id != 0) && (u_sess->attr.attr_resource.sqlUseSpaceLimit > 0)) {
        CleanUpDnHashTable();
    }
}

/*
 *	GetTopTransactionId
 *
 * This will return the XID of the main transaction, assigning one if
 * it's not yet set.  Be careful to call this only inside a valid xact.
 */
TransactionId GetTopTransactionId(void)
{
    if (!TransactionIdIsValid(TopTransactionStateData.transactionId))
        AssignTransactionId(&TopTransactionStateData);
    return TopTransactionStateData.transactionId;
}


/*
 *	GetTopTransactionIdIfAny
 *
 * This will return the XID of the main transaction, if one is assigned.
 * It will return InvalidTransactionId if we are not currently inside a
 * transaction, or inside a transaction that hasn't yet been assigned an XID.
 */
TransactionId GetTopTransactionIdIfAny(void)
{
    return TopTransactionStateData.transactionId;
}

/*
 *	GetCurrentTransactionId
 *
 * This will return the XID of the current transaction (main or sub
 * transaction), assigning one if it's not yet set.  Be careful to call this
 * only inside a valid xact.
 */
TransactionId GetCurrentTransactionId(void)
{
    TransactionState s = CurrentTransactionState;

    if (!TransactionIdIsValid(s->transactionId))
        AssignTransactionId(s);

    /*
     * We guarantee it always return a valid xid
     * don't need retry here:
     * just check here, we retry when we do AssignTransactionId
     */
    if (!TransactionIdIsValid(s->transactionId))
        ereport(ERROR, (errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION), errmsg("Xid is invalid.")));

    return s->transactionId;
}

/*
 * Stream threads need not commit transaction at all.
 * Only parent thread allow to commit transacton, So only stream thread will
 * call this function to clean transaction info
 */
void ResetTransactionInfo(void)
{
    if (CurrentTransactionState) {
        CurrentTransactionState->transactionId = InvalidTransactionId;
        ProcArrayClearTransaction(t_thrd.proc);
    }
}

/*
 * @Description: return the current transaction state of this thread.
 * @return: the current transaction state.
 */
TransactionState GetCurrentTransactionState(void)
{
    return CurrentTransactionState;
}

/*
 *	GetCurrentTransactionIdIfAny
 *
 * This will return the XID of the current sub xact, if one is assigned.
 * It will return InvalidTransactionId if we are not currently inside a
 * transaction, or inside a transaction that hasn't been assigned an XID yet.
 */
TransactionId GetCurrentTransactionIdIfAny(void)
{
    return CurrentTransactionState->transactionId;
}

GTM_TransactionHandle GetTransactionHandleIfAny(TransactionState s)
{
    return s->txnKey.txnHandle;
}

GTM_TransactionHandle GetCurrentTransactionHandleIfAny(void)
{
    return CurrentTransactionState->txnKey.txnHandle;
}

/*
 *	GetStableLatestTransactionId
 *
 * Get the transaction's XID if it has one, else read the next-to-be-assigned
 * XID.  Once we have a value, return that same value for the remainder of the
 * current transaction.  This is meant to provide the reference point for the
 * age(xid) function, but might be useful for other maintenance tasks as well.
 */
TransactionId GetStableLatestTransactionId(void)
{
    if (t_thrd.xact_cxt.lxid != t_thrd.proc->lxid) {
        t_thrd.xact_cxt.lxid = t_thrd.proc->lxid;
        t_thrd.xact_cxt.stablexid = GetTopTransactionIdIfAny();
        if (!TransactionIdIsValid(t_thrd.xact_cxt.stablexid))
            t_thrd.xact_cxt.stablexid = ReadNewTransactionId();
    }

    Assert(TransactionIdIsValid(t_thrd.xact_cxt.stablexid));

    return t_thrd.xact_cxt.stablexid;
}

/* Get parent xid if any, return InvalidTransactionId if it is top transaction */
TransactionId GetParentTransactionIdIfAny(const TransactionState s)
{
    if (s->parent != NULL) {
        return s->parent->transactionId;
    } else {
        return InvalidTransactionId;
    }
}

#ifdef PGXC
/*
 *	GetCurrentLocalParamStatus
 *
 * This will return if current sub xact is using local parameters
 * that may involve pooler session related parameters (SET LOCAL).
 */
bool GetCurrentLocalParamStatus(void)
{
    return CurrentTransactionState->isLocalParameterUsed;
}

/*
 *	SetCurrentLocalParamStatus
 *
 * This sets local parameter usage for current sub xact.
 */
void SetCurrentLocalParamStatus(bool status)
{
    CurrentTransactionState->isLocalParameterUsed = status;
}
#endif

/*
 * MarkCurrentTransactionIdLoggedIfAny
 *
 * Remember that the current xid - if it is assigned - now has been wal logged.
 */
void MarkCurrentTransactionIdLoggedIfAny(void)
{
    if (TransactionIdIsValid(CurrentTransactionState->transactionId))
        CurrentTransactionState->didLogXid = true;
}

/*
 * @Description: set the didLogXid of the current transaction state to true.
 * @out state: the current transaction state.
 */
void CopyTransactionIdLoggedIfAny(TransactionState state)
{
    if (TransactionIdIsValid(state->transactionId))
        state->didLogXid = true;
}

/*
 * AssignTransactionId
 *
 * Assigns a new permanent XID to the given TransactionState.
 * We do not assign XIDs to transactions until/unless this is called.
 * Also, any parent TransactionStates that don't yet have XIDs are assigned
 * one; this maintains the invariant that a child transaction has an XID
 * following its parent's.
 */
static void AssignTransactionId(TransactionState s)
{
    bool isSubXact = (s->parent != NULL);
    ResourceOwner currentOwner;
    bool log_unknown_top = false;

    /* Assert that caller didn't screw up */
    Assert(!TransactionIdIsValid(s->transactionId));
    Assert(s->state == TRANS_INPROGRESS);

    /*
     * Ensure parent(s) have XIDs, so that a child always has an XID later
     * than its parent.  Musn't recurse here, or we might get a stack overflow
     * if we're at the bottom of a huge stack of subtransactions none of which
     * have XIDs yet.
     */
    if (isSubXact && !TransactionIdIsValid(s->parent->transactionId)) {
        TransactionState p = s->parent;
        TransactionState *parents = NULL;
        size_t parentOffset = 0;

        parents = (TransactionState *)palloc(sizeof(TransactionState) * s->nestingLevel);
        while (p != NULL && !TransactionIdIsValid(p->transactionId)) {
            parents[parentOffset++] = p;
            p = p->parent;
        }

        /*
         * This is technically a recursive call, but the recursion will never
         * be more than one layer deep.
         */
        while (parentOffset != 0)
            AssignTransactionId(parents[--parentOffset]);

        pfree(parents);
    }
    /*
     * When wal_level=logical, guarantee that a subtransaction's xid can only
     * be seen in the WAL stream if its toplevel xid has been logged
     * before. If necessary we log a xact_assignment record with fewer than
     * PGPROC_MAX_CACHED_SUBXIDS. Note that it is fine if didLogXid isn't set
     * for a transaction even though it appears in a WAL record, we just might
     * superfluously log something. That can happen when an xid is included
     * somewhere inside a wal record, but not in XLogRecord->xl_xid, like in
     * xl_standby_locks.
     */
    if (isSubXact && XLogLogicalInfoActive() && !TopTransactionStateData.didLogXid)
        log_unknown_top = true;

    /* allocate undo zone before generate a new xid. */
    if (!isSubXact && IsUnderPostmaster) {
        undo::AllocateUndoZone();
        pg_memory_barrier();
    }

        /*
         * Generate a new Xid and record it in PG_PROC and pg_subtrans.
         *
         * NB: we must make the subtrans entry BEFORE the Xid appears anywhere in
         * shared storage other than PG_PROC; because if there's no room for it in
         * PG_PROC, the subtrans entry is needed to ensure that other backends see
         * the Xid as "running".  See GetNewTransactionId.
         */
#ifdef PGXC /* PGXC_COORD */
    s->transactionId = GetNewTransactionId(isSubXact, s);
#else
    s->transactionId = GetNewTransactionId(isSubXact);
#endif
    
    if (!isSubXact) {
        ProcXactHashTableAdd(s->transactionId, t_thrd.proc->pgprocno);
    }

    /* send my top transaction id to exec CN */
    if (!isSubXact && IsConnFromCoord() && u_sess->need_report_top_xid)
        ReportTopXid(s->transactionId);
    if (!isSubXact)
        instr_stmt_report_txid(s->transactionId);

    if (isSubXact)
        SubTransSetParent(s->transactionId, s->parent->transactionId);

    /*
     * If it's a top-level transaction, the predicate locking system needs to
     * be told about it too.
     */
    if (!isSubXact)
        RegisterPredicateLockingXid(s->transactionId);

    /*
     * Acquire lock on the transaction XID.  (We assume this cannot block.) We
     * have to ensure that the lock is assigned to the transaction's own
     * ResourceOwner.
     */
    currentOwner = t_thrd.utils_cxt.CurrentResourceOwner;
    PG_TRY();
    {
        t_thrd.utils_cxt.CurrentResourceOwner = s->curTransactionOwner;
        XactLockTableInsert(s->transactionId);
    }
    PG_CATCH();
    {
        /* Ensure CurrentResourceOwner is restored on error */
        t_thrd.utils_cxt.CurrentResourceOwner = currentOwner;
        PG_RE_THROW();
    }
    PG_END_TRY();
    t_thrd.utils_cxt.CurrentResourceOwner = currentOwner;

    /*
     * Every PGPROC_MAX_CACHED_SUBXIDS assigned transaction ids within each
     * top-level transaction we issue a WAL record for the assignment. We
     * include the top-level xid and all the subxids that have not yet been
     * reported using XLOG_XACT_ASSIGNMENT records.
     *
     * This is required to limit the amount of shared memory required in a hot
     * standby server to keep track of in-progress XIDs.
     * See notes for RecordKnownAssignedTransactionIds().
     *
     * We don't keep track of the immediate parent of each subxid, only the
     * top-level transaction that each subxact belongs to. This is correct in
     * recovery only because aborted subtransactions are separately WAL
     * logged.
     *
     * This is correct even for the case where several levels above us didn't
     * have an xid assigned as we recursed up to them beforehand.
     */
    if (isSubXact && XLogStandbyInfoActive()) {
        t_thrd.xact_cxt.unreportedXids[t_thrd.xact_cxt.nUnreportedXids] = s->transactionId;
        t_thrd.xact_cxt.nUnreportedXids++;

        /*
         * ensure this test matches similar one in RecoverPreparedTransactions()
         */
        if (t_thrd.xact_cxt.nUnreportedXids >= PGPROC_MAX_CACHED_SUBXIDS || log_unknown_top) {
            xl_xact_assignment xlrec;

            /*
             * xtop is always set by now because we recurse up transaction
             * stack to the highest unassigned xid and then come back down
             */
            xlrec.xtop = GetTopTransactionId();
            Assert(TransactionIdIsValid(xlrec.xtop));
            xlrec.nsubxacts = t_thrd.xact_cxt.nUnreportedXids;

            XLogBeginInsert();
            XLogRegisterData((char *)&xlrec, MinSizeOfXactAssignment);
            XLogRegisterData((char *)t_thrd.xact_cxt.unreportedXids,
                             t_thrd.xact_cxt.nUnreportedXids * sizeof(TransactionId));

            (void)XLogInsert(RM_XACT_ID, XLOG_XACT_ASSIGNMENT);

            t_thrd.xact_cxt.nUnreportedXids = 0;
            /* mark top, not current xact as having been logged */
            TopTransactionStateData.didLogXid = true;
        }
    }
}

/*
 * SetCurrentSubTransactionLocked
 */
void SetCurrentSubTransactionLocked()
{
    TransactionState s = CurrentTransactionState;
    s->subXactLock = true;
}

/*
 * HasCurrentSubTransactionLock
 */
bool HasCurrentSubTransactionLock()
{
    TransactionState s = CurrentTransactionState;
    return s->subXactLock;
}

/*
 * GetCurrentTransactionResOwner
 */
ResourceOwner GetCurrentTransactionResOwner(void)
{
    TransactionState s = CurrentTransactionState;
    return s->curTransactionOwner;
}

/*
 * GetCurrentSubTransactionId
 */
SubTransactionId GetCurrentSubTransactionId(void)
{
    TransactionState s = CurrentTransactionState;
    return s->subTransactionId;
}

/*
 *	SubTransactionIsActive
 *
 * Test if the specified subxact ID is still active.  Note caller is
 * responsible for checking whether this ID is relevant to the current xact.
 */
bool SubTransactionIsActive(SubTransactionId subxid)
{
    TransactionState s;

    for (s = CurrentTransactionState; s != NULL; s = s->parent) {
        if (s->state == TRANS_ABORT)
            continue;
        if (s->subTransactionId == subxid)
            return true;
    }
    return false;
}

/*
 *	GetCurrentCommandId
 *
 * "used" must be TRUE if the caller intends to use the command ID to mark
 * inserted/updated/deleted tuples.  FALSE means the ID is being fetched
 * for read-only purposes (ie, as a snapshot validity cutoff).	See
 * CommandCounterIncrement() for discussion.
 */
CommandId GetCurrentCommandId(bool used)
{
#ifdef PGXC
    /* If coordinator has sent a command id, remote node should use it */
    if (IsConnFromCoord() && t_thrd.xact_cxt.isCommandIdReceived) {
        /*
         * Indicate to successive calls of this function that the sent command id has
         * already been used.
         */
        t_thrd.xact_cxt.isCommandIdReceived = false;
        t_thrd.xact_cxt.currentCommandId = GetReceivedCommandId();
    } else if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        /*
         * If command id reported by remote node is greater that the current
         * command id, the coordinator needs to use it. This is required because
         * a remote node can increase the command id sent by the coordinator
         * e.g. in case a trigger fires at the remote node and inserts some rows
         * The coordinator should now send the next command id knowing
         * the largest command id either current or received from remote node.
         */
        if (GetReceivedCommandId() > t_thrd.xact_cxt.currentCommandId)
            t_thrd.xact_cxt.currentCommandId = GetReceivedCommandId();
    }
#endif

    /* this is global to a transaction, not subtransaction-local */
    if (used)
        t_thrd.xact_cxt.currentCommandIdUsed = true;
    return t_thrd.xact_cxt.currentCommandId;
}

/*
 *  GetCurrentCommandIdUsed
 */
bool GetCurrentCommandIdUsed(void)
{
    return t_thrd.xact_cxt.currentCommandIdUsed;
}

/*
 *	GetCurrentTransactionStartTimestamp
 */
TimestampTz GetCurrentTransactionStartTimestamp(void)
{
    /*
     * In Postgres-XC, Transaction start timestamp is the value received
     * from GTM along with GXID.
     */
#ifdef PGXC
    return t_thrd.xact_cxt.GTMxactStartTimestamp;
#else
    return t_thrd.xact_cxt.xactStartTimestamp;
#endif
}

/*
 *	GetCurrentStatementStartTimestamp
 */
TimestampTz GetCurrentStatementStartTimestamp(void)
{
    /*
     * For Postgres-XC, Statement start timestamp is adjusted at each node
     * (Coordinator and Datanode) with a difference value that is calculated
     * based on the global timestamp value received from GTM and the local
     * clock. This permits to follow the GTM timeline in the cluster.
     */
#ifdef PGXC
    return t_thrd.xact_cxt.stmtStartTimestamp + t_thrd.xact_cxt.GTMdeltaTimestamp;
#else
    return t_thrd.xact_cxt.stmtStartTimestamp;
#endif
}

TimestampTz GetCurrentStatementLocalStartTimestamp(void)
{
    return t_thrd.xact_cxt.stmtStartTimestamp;
}

/*
 *	GetCurrentTransactionStopTimestamp
 *
 * We return current time if the transaction stop time hasn't been set
 * (which can happen if we decide we don't need to log an XLOG record).
 */
TimestampTz GetCurrentTransactionStopTimestamp(void)
{
    /*
     * As for Statement start timestamp, stop timestamp has to
     * be adjusted with the delta value calculated with the
     * timestamp received from GTM and the local node clock.
     */
#ifdef PGXC
    TimestampTz timestamp;

    if (t_thrd.xact_cxt.xactStopTimestamp != 0)
        return t_thrd.xact_cxt.xactStopTimestamp + t_thrd.xact_cxt.GTMdeltaTimestamp;

    timestamp = GetCurrentTimestamp() + t_thrd.xact_cxt.GTMdeltaTimestamp;

    return timestamp;
#else
    if (t_thrd.xact_cxt.xactStopTimestamp != 0)
        return t_thrd.xact_cxt.xactStopTimestamp;
    return GetCurrentTimestamp();
#endif
}

#ifdef PGXC
TimestampTz GetCurrentGTMStartTimestamp(void)
{
    return t_thrd.xact_cxt.GTMxactStartTimestamp;
}

TimestampTz GetCurrentStmtsysTimestamp(void)
{
    return t_thrd.time_cxt.stmt_system_timestamp + stmtSysGTMdeltaTimestamp;
}

#endif

/*
 *	SetCurrentStatementStartTimestamp
 *
 *	The time on the DN is obtained from the CN. If the CN does not deliver the time,
 *	the time of the current DN is used.
 */
void SetCurrentStatementStartTimestamp(void)
{
    t_thrd.xact_cxt.stmtStartTimestamp = GetCurrentTimestamp();
}

/*
 *	SetCurrentTransactionStopTimestamp
 */
static inline void SetCurrentTransactionStopTimestamp(void)
{
    t_thrd.xact_cxt.xactStopTimestamp = GetCurrentTimestamp();
}

#ifdef PGXC
void SetCurrentGTMTimestamp(TimestampTz timestamp)
{
    t_thrd.xact_cxt.GTMxactStartTimestamp = timestamp;
}

void SetCurrentStmtTimestamp(TimestampTz timestamp)
{
    t_thrd.time_cxt.stmt_system_timestamp = timestamp;
}

void SetCurrentStmtTimestamp()
{
    t_thrd.time_cxt.stmt_system_timestamp = GetCurrentTimestamp();
}

/*
 *  SetCurrentGTMDeltaTimestamp
 *
 *  Note: Sets local timestamp delta with the value received from GTM
 */
void SetCurrentGTMDeltaTimestamp(void)
{
    t_thrd.xact_cxt.GTMdeltaTimestamp = t_thrd.xact_cxt.GTMxactStartTimestamp - t_thrd.xact_cxt.stmtStartTimestamp;
}

/*
 *  SetStmtSysGTMDeltaTimestamp
 *
 * Note: Sets the delta time between query start time(local CN timestamp)
 * with the value received from GTM
 */
void SetStmtSysGTMDeltaTimestamp(void)
{
    stmtSysGTMdeltaTimestamp = t_thrd.xact_cxt.GTMxactStartTimestamp - t_thrd.time_cxt.stmt_system_timestamp;
}

/*
 *  clean the GTMdeltaTimestamp to 0 before committing or aborting the transaction.
 */
void CleanGTMDeltaTimeStamp()
{
    t_thrd.xact_cxt.GTMdeltaTimestamp = 0;
}

/*
 *  clean the CleanstmtSysGTMDeltaTimeStamp to 0 before committing or aborting the transaction.
 */
void CleanstmtSysGTMDeltaTimeStamp()
{
    stmtSysGTMdeltaTimestamp = 0;
}

#endif

/*
 *	GetCurrentTransactionNestLevel
 *
 * Note: this will return zero when not inside any transaction, one when
 * inside a top-level transaction, etc.
 */
int GetCurrentTransactionNestLevel(void)
{
    TransactionState s = CurrentTransactionState;

    return s->nestingLevel;
}

/*
 *	TransactionIdIsCurrentTransactionId
 */
bool TransactionIdIsCurrentTransactionId(TransactionId xid)
{
    TransactionState s;

    /*
     * We always say that BootstrapTransactionId is "not my transaction ID"
     * even when it is (ie, during bootstrap).	Along with the fact that
     * transam.c always treats BootstrapTransactionId as already committed,
     * this causes the heapam_visibility.c routines to see all tuples as 
     * committed, which is what we need during bootstrap.  (Bootstrap mode 
     * only inserts tuples, it never updates or deletes them, so all tuples 
     * can be presumed good immediately.)
     *
     * Likewise, InvalidTransactionId and FrozenTransactionId are certainly
     * not my transaction ID, so we can just return "false" immediately for
     * any non-normal XID.
     */
    if (!TransactionIdIsNormal(xid))
        return false;

    /*
     * We will return true for the Xid of the current subtransaction, any of
     * its subcommitted children, any of its parents, or any of their
     * previously subcommitted children.  However, a transaction being aborted
     * is no longer "current", even though it may still have an entry on the
     * state stack.
     */
    for (s = CurrentTransactionState; s != NULL; s = s->parent) {
        int low, high;

        if (s->state == TRANS_ABORT || s->state == TRANS_UNDO)
            continue;
        if (!TransactionIdIsValid(s->transactionId))
            continue; /* it can't have any child XIDs either */
        if (TransactionIdEquals(xid, s->transactionId))
            return true;
        /* As the childXids array is ordered, we can use binary search */
        low = 0;
        high = s->nChildXids - 1;
        while (low <= high) {
            int middle;
            TransactionId probe;

            middle = low + (high - low) / 2;
            probe = s->childXids[middle];
            if (TransactionIdEquals(probe, xid))
                return true;
            else if (TransactionIdPrecedes(probe, xid))
                low = middle + 1;
            else
                high = middle - 1;
        }
    }

    return false;
}

/*
 *	TransactionStartedDuringRecovery
 *
 * Returns true if the current transaction started while recovery was still
 * in progress. Recovery might have ended since so RecoveryInProgress() might
 * return false already.
 */
bool TransactionStartedDuringRecovery(void)
{
    return CurrentTransactionState->startedInRecovery;
}

/*
 *	CommandCounterIncrement
 */
void CommandCounterIncrement(void)
{
    /*
     * If the current value of the command counter hasn't been "used" to mark
     * tuples, we need not increment it, since there's no need to distinguish
     * a read-only command from others.  This helps postpone command counter
     * overflow, and keeps no-op CommandCounterIncrement operations cheap.
     */
    if (t_thrd.xact_cxt.currentCommandIdUsed) {
        t_thrd.xact_cxt.currentCommandId += 1;
        if (t_thrd.xact_cxt.currentCommandId == InvalidCommandId) { /* check for overflow */
            t_thrd.xact_cxt.currentCommandId -= 1;
            ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                            errmsg("cannot have more than 2^32-2 commands in a transaction")));
        }
        t_thrd.xact_cxt.currentCommandIdUsed = false;

        /* Propagate new command ID into static snapshots */
        SnapshotSetCommandId(t_thrd.xact_cxt.currentCommandId);

#ifdef PGXC
        /*
         * Remote node should report local command id changes only if
         * required by the Coordinator. The requirement of the
         * Coordinator is inferred from the fact that Coordinator
         * has itself sent the command id to the remote nodes.
         */
        if (IsConnFromCoord() && IsSendCommandId())
            ReportCommandIdChange(t_thrd.xact_cxt.currentCommandId);
#endif

        /*
         * Make any catalog changes done by the just-completed command visible
         * in the local syscache.  We obviously don't need to do this after a
         * read-only command.  (But see hacks in inval.c to make real sure we
         * don't think a command that queued inval messages was read-only.)
         */
        AtCCI_LocalCache();
    }
}

/*
 * ForceSyncCommit
 *
 * Interface routine to allow commands to force a synchronous commit of the
 * current top-level transaction
 */
void ForceSyncCommit(void)
{
    t_thrd.xact_cxt.forceSyncCommit = true;
}

/* ----------------------------------------------------------------
 *						StartTransaction stuff
 * ----------------------------------------------------------------
 */
static void AtStart_Cache(void)
{
    CleanSystemCaches(false);
    AcceptInvalidationMessages();
}

static void AtStart_Memory(void)
{
    TransactionState s = CurrentTransactionState;

    /*
     * If this is the first time through, create a private context for
     * AbortTransaction to work in.  By reserving some space now, we can
     * insulate AbortTransaction from out-of-memory scenarios.	Like
     * ErrorContext, we set it up with slow growth rate and a nonzero minimum
     * size, so that space will be reserved immediately.
     */
    if (t_thrd.xact_cxt.TransactionAbortContext == NULL)
        t_thrd.xact_cxt.TransactionAbortContext = AllocSetContextCreate(t_thrd.top_mem_cxt, "TransactionAbortContext",
                                                                        32 * 1024, 32 * 1024, 32 * 1024);


    /* We shouldn't have a transaction context already. */
    Assert(u_sess->top_transaction_mem_cxt == NULL);

    /* Create a toplevel context for the transaction. */
    Assert(t_thrd.xact_cxt.PGXCBucketMap == NULL);

    u_sess->top_transaction_mem_cxt = AllocSetContextCreate(u_sess->top_mem_cxt, "TopTransactionContext",
                                                            ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE,
                                                            ALLOCSET_DEFAULT_MAXSIZE);

    /*
     * In a top-level transaction, t_thrd.mem_cxt.cur_transaction_mem_cxt is the same as
     * u_sess->top_transaction_mem_cxt.
     */
    t_thrd.mem_cxt.cur_transaction_mem_cxt = u_sess->top_transaction_mem_cxt;
    s->curTransactionContext = t_thrd.mem_cxt.cur_transaction_mem_cxt;

    /* Make the t_thrd.mem_cxt.cur_transaction_mem_cxt active. */
    (void)MemoryContextSwitchTo(t_thrd.mem_cxt.cur_transaction_mem_cxt);
}

static void AtStart_ResourceOwner(void)
{
    TransactionState s = CurrentTransactionState;

    /* We shouldn't have a transaction resource owner already. */
    Assert(t_thrd.utils_cxt.TopTransactionResourceOwner == NULL);
    Assert(CurrentResourceOwnerIsEmpty(t_thrd.utils_cxt.CurrentResourceOwner));
    Assert(!EnableLocalSysCache() || CurrentResourceOwnerIsEmpty(t_thrd.lsc_cxt.lsc->local_sysdb_resowner));

    /* Create a toplevel resource owner for the transaction. */
    s->curTransactionOwner = ResourceOwnerCreate(NULL, "TopTransaction",
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));

    t_thrd.utils_cxt.TopTransactionResourceOwner = s->curTransactionOwner;
    t_thrd.utils_cxt.CurTransactionResourceOwner = s->curTransactionOwner;
    t_thrd.utils_cxt.CurrentResourceOwner = s->curTransactionOwner;
}

/* ----------------------------------------------------------------
 *						StartSubTransaction stuff
 * ----------------------------------------------------------------
 */
static void AtSubStart_Memory(void)
{
    TransactionState s = CurrentTransactionState;

    Assert(t_thrd.mem_cxt.cur_transaction_mem_cxt != NULL);

    /*
     * Create a t_thrd.mem_cxt.cur_transaction_mem_cxt, which will be used to hold data that
     * survives subtransaction commit but disappears on subtransaction abort.
     * We make it a child of the immediate parent's t_thrd.mem_cxt.cur_transaction_mem_cxt.
     */
    t_thrd.mem_cxt.cur_transaction_mem_cxt = AllocSetContextCreate(t_thrd.mem_cxt.cur_transaction_mem_cxt,
                                                                   "CurTransactionContext", ALLOCSET_DEFAULT_MINSIZE,
                                                                   ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    s->curTransactionContext = t_thrd.mem_cxt.cur_transaction_mem_cxt;

    /* Make the t_thrd.mem_cxt.cur_transaction_mem_cxt active. */
    (void)MemoryContextSwitchTo(t_thrd.mem_cxt.cur_transaction_mem_cxt);
}

static void AtSubStart_ResourceOwner(void)
{
    TransactionState s = CurrentTransactionState;

    Assert(s->parent != NULL);

    /*
     * Create a resource owner for the subtransaction.	We make it a child of
     * the immediate parent's resource owner.
     */
    s->curTransactionOwner = ResourceOwnerCreate(s->parent->curTransactionOwner, "SubTransaction",
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));

    t_thrd.utils_cxt.CurTransactionResourceOwner = s->curTransactionOwner;
    t_thrd.utils_cxt.CurrentResourceOwner = s->curTransactionOwner;
}

CommitSeqNo getLocalNextCSN()
{
    return pg_atomic_fetch_add_u64(&t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo, 1);
}

void UpdateNextMaxKnownCSN(CommitSeqNo csn)
{
    /* 
     * GTM-Free mode use getLocalNextCSN to update nextCommitSeqNo,
     * GTM mode update nextCommitSeqNo in UpdateCSNAtTransactionCommit.
     * GTM-Lite mode update nextCommitSeqNo in this function.
     */
    if (!GTM_LITE_MODE) {
        return;
    }
    CommitSeqNo currentNextCommitSeqNo;
    CommitSeqNo nextMaxKnownCommitSeqNo = csn + 1;
 loop:
    currentNextCommitSeqNo = pg_atomic_read_u64(&t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo);
    if (nextMaxKnownCommitSeqNo <= currentNextCommitSeqNo) {
        return;
    }
    if (!pg_atomic_compare_exchange_u64(&t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo,
        &currentNextCommitSeqNo, nextMaxKnownCommitSeqNo)) {
        goto loop;
    }
}
void XLogInsertStandbyCSNCommitting(TransactionId xid, CommitSeqNo csn, TransactionId *children, uint64 nchildren)
{
#ifdef ENABLE_MULTIPLE_NODES
    if (t_thrd.proc->workingVersionNum < DISASTER_READ_VERSION_NUM || IS_PGXC_COORDINATOR) {
        return;
    }
#endif

    if (!XLogStandbyInfoActive()) {
        return;
    }
    XLogBeginInsert();
    XLogRegisterData((char *) (&xid), sizeof(TransactionId));
    XLogRegisterData((char *) (&csn), sizeof(CommitSeqNo));
    uint64 childrenxidnum = nchildren;
    XLogRegisterData((char *) (&childrenxidnum), sizeof(uint64));
    if (childrenxidnum > 0) {
        XLogRegisterData((char *)children, nchildren * sizeof(TransactionId));
    }
    XLogInsert(RM_STANDBY_ID, XLOG_STANDBY_CSN_COMMITTING);
}

/* ----------------------------------------------------------------
 *						CommitTransaction stuff
 * ----------------------------------------------------------------
 */
/*
 *	RecordTransactionCommit
 *
 * Returns latest XID among xact and its children, or InvalidTransactionId
 * if the xact has no XID.	(We compute that here just because it's easier.)
 */
static TransactionId RecordTransactionCommit(void)
{
    TransactionId xid = GetTopTransactionIdIfAny();
    bool markXidCommitted = TransactionIdIsValid(xid);
    TransactionId latestXid = InvalidTransactionId;
    int nrels;
    int temp_nrels = 0;
    ColFileNodeRel *rels = NULL;
    int nchildren;
    TransactionId *children = NULL;
    int nmsgs = 0;
    SharedInvalidationMessage *invalMessages = NULL;
    int nlibrary = 0;
    char *library_name = NULL;
    int library_length = 0;
    bool RelcacheInitFileInval = false;
    bool wrote_xlog = false;
    bool isExecCN = (IS_PGXC_COORDINATOR && !IsConnFromCoord());
    XLogRecPtr globalDelayDDLLSN = InvalidXLogRecPtr;
    XLogRecPtr commitRecLSN = InvalidXLogRecPtr;

    /* Get data needed for commit record */
    nrels = smgrGetPendingDeletes(true, &rels, false, &temp_nrels);
    nchildren = xactGetCommittedChildren(&children);
    if (XLogStandbyInfoActive())
        nmsgs = xactGetCommittedInvalidationMessages(&invalMessages, &RelcacheInitFileInval);
    nlibrary = libraryGetPendingDeletes(true, &library_name, &library_length);
    /* cn must flush xlog, gs_clean status depend on it. */
    wrote_xlog = (t_thrd.xlog_cxt.XactLastRecEnd != 0) || (isExecCN && u_sess->xact_cxt.savePrepareGID);

    /*
     * If we haven't been assigned an XID yet, we neither can, nor do we want
     * to write a COMMIT record.
     */
    if (!markXidCommitted) {
        /*
         * We expect that every smgrscheduleunlink is followed by a catalog
         * update, and hence XID assignment, so we shouldn't get here with any
         * pending deletes.  Use a real test not just an Assert to check this,
         * since it's a bit fragile.
         */
        if (nrels != 0)
            ereport(ERROR, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                            errmsg("cannot commit a transaction that deleted files but has no xid")));

        /* Can't have child XIDs either; AssignTransactionId enforces this */
        Assert(nchildren == 0);

#ifdef ENABLE_MOT
        /*
         * For MOT, XACT_EVENT_COMMIT will just do the OCC validation.
         * Actual commit (write redo and apply changes) will be done during XACT_EVENT_RECORD_COMMIT event.
         * This should be done after setCommitCsn for the transaction.
         * Note: Currently, MOT only transactions don't use CSN, so we are not calling setCommitCsn here.
         */
        CallXactCallbacks(XACT_EVENT_RECORD_COMMIT);

        /*
         * For MOT, XLOG entries will be written in the above callback for XACT_EVENT_RECORD_COMMIT.
         * So, we should re-check and update wrote_xlog accordingly.
         */
        if (t_thrd.xlog_cxt.XactLastRecEnd != 0) {
            wrote_xlog = true;
        }
#endif

        /*
         * If we didn't create XLOG entries, we're done here; otherwise we
         * should flush those entries the same as a commit record.	(An
         * example of a possible record that wouldn't cause an XID to be
         * assigned is a sequence advance record due to nextval() --- we want
         * to flush that to disk before reporting commit.)
         */
        if (!wrote_xlog) {
            goto cleanup;
        }
    } else {
        /*
         * Check that we haven't commited halfway through RecordTransactionAbort.
         */
        if (TransactionIdDidAbort(xid))
            ereport(PANIC, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                            errmsg("cannot commit transaction %lu, it was already aborted", xid)));

        /*
         * Begin commit critical section and insert the commit XLOG record.
         *
         * Tell bufmgr and smgr to prepare for commit
         */
        BufmgrCommit();

        /*
         * Mark ourselves as within our "commit critical section".  This
         * forces any concurrent checkpoint to wait until we've updated
         * pg_clog.  Without this, it is possible for the checkpoint to set
         * REDO after the XLOG record but fail to flush the pg_clog update to
         * disk, leading to loss of the transaction commit if the system
         * crashes a little later.
         *
         * Note: we could, but don't bother to, set this flag in
         * RecordTransactionAbort.  That's because loss of a transaction abort
         * is noncritical; the presumption would be that it aborted, anyway.
         *
         * It's safe to change the delayChkpt flag of our own backend without
         * holding the ProcArrayLock, since we're the only one modifying it.
         * This makes checkpoint's determination of which xacts are delayChkpt a
         * bit fuzzy, but it doesn't matter.
         */
        START_CRIT_SECTION();
        t_thrd.pgxact->delayChkpt = true;

        if (useLocalXid || !IsPostmasterEnvironment || GTM_FREE_MODE) {
#ifndef ENABLE_MULTIPLE_NODES
            /* For hot standby, set csn to commit in progress */
            CommitSeqNo csn = SetXact2CommitInProgress(xid, 0);
            XLogInsertStandbyCSNCommitting(xid, csn, children, nchildren);
#else
            /* set commit CSN and update global CSN in gtm free mode. */
            SetXact2CommitInProgress(xid, 0);
#endif
            setCommitCsn(getLocalNextCSN());
        } else {
            /* for dn auto commit condition, get a new next csn from gtm. */
            if (TransactionIdIsNormal(xid) &&
                (!(useLocalXid || !IsPostmasterEnvironment || GTM_FREE_MODE || GetForceXidFromGTM())) &&
                (GetCommitCsn() == 0)) {
                /* First set csn to commit in progress */
                CommitSeqNo csn = SetXact2CommitInProgress(xid, 0);
                XLogInsertStandbyCSNCommitting(xid, csn, children, nchildren);
                END_CRIT_SECTION();
                PG_TRY();
                {
                    /* Then get a new csn from gtm */
                    ereport(LOG, (errmsg("Set a new csn from gtm for auto commit transactions.")));
                    setCommitCsn(CommitCSNGTM(true));
                }
                PG_CATCH();
                {
                    /* be careful to deal with delay chkpt flag. */
                    t_thrd.pgxact->delayChkpt = false;
                    PG_RE_THROW();
                }
                PG_END_TRY();
                START_CRIT_SECTION();
            }
        }

#ifdef ENABLE_MOT
        /*
         * For MOT, XACT_EVENT_COMMIT will just do the OCC validation.
         * Actual commit (write redo and apply changes) will be done during XACT_EVENT_RECORD_COMMIT event.
         * This should be done after setCommitCsn for the transaction.
         */
        CallXactCallbacks(XACT_EVENT_RECORD_COMMIT);
#endif


        UpdateNextMaxKnownCSN(GetCommitCsn());

        SetCurrentTransactionStopTimestamp();

        /*
         * Do we need the long commit record? If not, use the compact format.
         *
         * For now always use the non-compact version if wal_level=logical, so
         * we can hide commits from other databases. In the future we
         * should merge compact and non-compact commits and use a flags
         * variable to determine if it contains subxacts, relations or
         * invalidation messages, that's more extensible and degrades more
         * gracefully. Till then, it's just 20 bytes of overhead.
         */
#ifdef ENABLE_MULTIPLE_NODES
        bool hasOrigin = false;
#else
        bool hasOrigin = u_sess->reporigin_cxt.originId != InvalidRepOriginId &&
            u_sess->reporigin_cxt.originId != DoNotReplicateId;
#endif
        if (nrels > 0 || nmsgs > 0 || RelcacheInitFileInval || t_thrd.xact_cxt.forceSyncCommit ||
            XLogLogicalInfoActive() || hasOrigin) {
            xl_xact_commit xlrec;
            xl_xact_origin origin;

            /* Set flags required for recovery processing of commits. */
            xlrec.xinfo = 0;
            if (RelcacheInitFileInval)
                xlrec.xinfo |= XACT_COMPLETION_UPDATE_RELCACHE_FILE;
            if (t_thrd.xact_cxt.forceSyncCommit)
                xlrec.xinfo |= XACT_COMPLETION_FORCE_SYNC_COMMIT;
#ifdef ENABLE_MOT
            if (IsMOTEngineUsed() || IsMixedEngineUsed()) {
                xlrec.xinfo |= XACT_MOT_ENGINE_USED;
            }
#endif
            if (hasOrigin) {
                xlrec.xinfo |= XACT_HAS_ORIGIN;
            }

            xlrec.dbId = u_sess->proc_cxt.MyDatabaseId;
            xlrec.tsId = u_sess->proc_cxt.MyDatabaseTableSpace;
            xlrec.csn = GetCommitCsn();

#ifdef PGXC
            /* In Postgres-XC, stop timestamp has to follow the timeline of GTM */
            xlrec.xact_time = t_thrd.xact_cxt.xactStopTimestamp + t_thrd.xact_cxt.GTMdeltaTimestamp;
#else
            xlrec.xact_time = t_thrd.xact_cxt.xactStopTimestamp;
#endif
            xlrec.nrels = nrels;
            xlrec.nsubxacts = nchildren;
            xlrec.nmsgs = nmsgs;
            xlrec.nlibrary = nlibrary;

            XLogBeginInsert();
            XLogRegisterData((char *)(&xlrec), MinSizeOfXactCommit);

            /* dump rels to delete */
            if (nrels > 0) {
                XLogRegisterData((char *)rels, nrels * sizeof(ColFileNodeRel));
                (void)LWLockAcquire(DelayDDLLock, LW_SHARED);
            }

            /* dump committed child Xids */
            if (nchildren > 0) {
                XLogRegisterData((char *)children, nchildren * sizeof(TransactionId));
            }

            /* dump shared cache invalidation messages */
            if (nmsgs > 0) {
                XLogRegisterData((char *)invalMessages, nmsgs * sizeof(SharedInvalidationMessage));
            }

#ifndef ENABLE_MULTIPLE_NODES
            XLogRegisterData((char *) &u_sess->utils_cxt.RecentXmin, sizeof(TransactionId));
#endif

            if (nlibrary > 0) {
                XLogRegisterData((char *)library_name, library_length);
            }

            if (hasOrigin) {
                origin.origin_lsn = u_sess->reporigin_cxt.originLsn;
                origin.origin_timestamp = u_sess->reporigin_cxt.originTs;
                XLogRegisterData((char*)&origin, sizeof(xl_xact_origin));
            }

            /* we allow filtering by xacts */
            XLogIncludeOrigin();

            commitRecLSN = XLogInsert(RM_XACT_ID, XLOG_XACT_COMMIT);

            if (nrels > 0) {
                globalDelayDDLLSN = GetDDLDelayStartPtr();
                if (!XLogRecPtrIsInvalid(globalDelayDDLLSN) && XLByteLT(globalDelayDDLLSN, commitRecLSN))
                    t_thrd.xact_cxt.xactDelayDDL = true;
                else
                    t_thrd.xact_cxt.xactDelayDDL = false;

                LWLockRelease(DelayDDLLock);
            }

            if (hasOrigin) {
                replorigin_session_advance(u_sess->reporigin_cxt.originLsn, t_thrd.xlog_cxt.XactLastRecEnd);
            }
        } else {
            xl_xact_commit_compact xlrec;

            xlrec.xact_time = t_thrd.xact_cxt.xactStopTimestamp;
            xlrec.csn = GetCommitCsn();
            xlrec.nsubxacts = nchildren;

            XLogBeginInsert();
            XLogRegisterData((char *)(&xlrec), MinSizeOfXactCommitCompact);

            /* dump committed child Xids */
            if (nchildren > 0) {
                XLogRegisterData((char *)children, nchildren * sizeof(TransactionId));
            }

#ifndef ENABLE_MULTIPLE_NODES
            XLogRegisterData((char *) &u_sess->utils_cxt.RecentXmin, sizeof(TransactionId));
#endif

            /* we allow filtering by xacts */
            XLogIncludeOrigin();

            (void)XLogInsert(RM_XACT_ID, XLOG_XACT_COMMIT_COMPACT);
        }
    }

    /*
     * Check if we want to commit asynchronously.  We can allow the XLOG flush
     * to happen asynchronously if synchronous_commit=off, or if the current
     * transaction has not performed any WAL-logged operation.	The latter
     * case can arise if the current transaction wrote only to temporary
     * and/or unlogged tables.	In case of a crash, the loss of such a
     * transaction will be irrelevant since temp tables will be lost anyway,
     * and unlogged tables will be truncated.  (Given the foregoing, you might
     * think that it would be unnecessary to emit the XLOG record at all in
     * this case, but we don't currently try to do that.  It would certainly
     * cause problems at least in Hot Standby mode, where the
     * KnownAssignedXids machinery requires tracking every XID assignment.	It
     * might be OK to skip it only when wal_level < hot_standby, but for now
     * we don't.)
     *
     * However, if we're doing cleanup of any non-temp rels or committing any
     * command that wanted to force sync commit, then we must flush XLOG
     * immediately.  (We must not allow asynchronous commit if there are any
     * non-temp tables to be deleted, because we might delete the files before
     * the COMMIT record is flushed to disk.  We do allow asynchronous commit
     * if all to-be-deleted tables are temporary though, since they are lost
     * anyway if we crash.)
     */
    if ((wrote_xlog && u_sess->attr.attr_storage.guc_synchronous_commit > SYNCHRONOUS_COMMIT_OFF) ||
        t_thrd.xact_cxt.forceSyncCommit || nrels > 0) {
        /*
         * Synchronous commit case:
         *
         * Sleep before flush! So we can flush more than one commit records
         * per single fsync.  (The idea is some other backend may do the
         * XLogFlush while we're sleeping.  This needs work still, because on
         * most Unixen, the minimum select() delay is 10msec or more, which is
         * way too long.)
         *
         * We do not sleep if u_sess->attr.attr_storage.enableFsync is not turned on, nor if there are
         * fewer than u_sess->attr.attr_storage.CommitSiblings other backends with active transactions.
         */
        /* Wait for local flush only when we don't wait for the remote server */
        if (!IsInitdb && g_instance.attr.attr_storage.dcf_attr.enable_dcf) {
            SyncPaxosWaitForLSN(t_thrd.xlog_cxt.XactLastRecEnd);
        } else {
            XLogWaitFlush(t_thrd.xlog_cxt.XactLastRecEnd);
            /*
             * Wait for quorum synchronous replication, if required.
             *
             * Note for normal cast that at this stage we sync wait before marking
             * clog and still show as running in the procarray and continue to hold locks.
             */
            if (wrote_xlog && u_sess->attr.attr_storage.guc_synchronous_commit > SYNCHRONOUS_COMMIT_LOCAL_FLUSH) {
                SyncRepWaitForLSN(t_thrd.xlog_cxt.XactLastRecEnd, !markXidCommitted);
                g_instance.comm_cxt.localinfo_cxt.set_term = true;
            }
        }
        /* Now we may update the CLOG, if we wrote a COMMIT record above */
        if (markXidCommitted) {
            t_thrd.pgxact->needToSyncXid |= SNAPSHOT_UPDATE_NEED_SYNC;
            TransactionIdCommitTree(xid, nchildren, children, GetCommitCsn());
        }
    } else {
        /*
         * Asynchronous commit case:
         *
         * This enables possible committed transaction loss in the case of a
         * postmaster crash because WAL buffers are left unwritten. Ideally we
         * could issue the WAL write without the fsync, but some
         * wal_sync_methods do not allow separate write/fsync.
         *
         * Report the latest async commit LSN, so that the WAL writer knows to
         * flush this commit.
         */

        /*
         * We must not immediately update the CLOG, since we didn't flush the
         * XLOG. Instead, we store the LSN up to which the XLOG must be
         * flushed before the CLOG may be updated.
         */
        if (markXidCommitted) {
            t_thrd.pgxact->needToSyncXid |= SNAPSHOT_UPDATE_NEED_SYNC;
            TransactionIdAsyncCommitTree(xid, nchildren, children, t_thrd.xlog_cxt.XactLastRecEnd, GetCommitCsn());
        }
    }

    /*
     * If we entered a commit critical section, leave it now, and let
     * checkpoints proceed.
     */
    if (markXidCommitted) {
        t_thrd.pgxact->delayChkpt = false;
        END_CRIT_SECTION();
    }

    /* Compute latestXid while we have the child XIDs handy */
    latestXid = TransactionIdLatest(xid, nchildren, children);

    /* remember end of last commit record */
    t_thrd.xlog_cxt.XactLastCommitEnd = t_thrd.xlog_cxt.XactLastRecEnd;

    /* Reset XactLastRecEnd until the next transaction writes something */
    t_thrd.xlog_cxt.XactLastRecEnd = 0;

cleanup:
    /* Clean up local data */
    if (rels != NULL)
        pfree(rels);

    return latestXid;
}

static void AtCCI_LocalCache(void)
{
    /*
     * Make any pending relation map changes visible.  We must do this before
     * processing local sinval messages, so that the map changes will get
     * reflected into the relcache when relcache invals are processed.
     */
    AtCCI_RelationMap();

    /* Make catalog changes visible to me for the next command. */
    CommandEndInvalidationMessages();
}

static void AtCommit_Memory(void)
{
    /*
     * Now that we're "out" of a transaction, have the system allocate things
     * in the top memory context instead of per-transaction contexts.
     */
    (void)MemoryContextSwitchTo(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DEFAULT));

    /* Release all transaction-local memory. */
    Assert(u_sess->top_transaction_mem_cxt != NULL);
    MemoryContextDelete(u_sess->top_transaction_mem_cxt);
    u_sess->top_transaction_mem_cxt = NULL;
    t_thrd.mem_cxt.cur_transaction_mem_cxt = NULL;
    CurrentTransactionState->curTransactionContext = NULL;

    t_thrd.xact_cxt.PGXCBucketMap = NULL;
    t_thrd.xact_cxt.PGXCBucketCnt = 0;
    t_thrd.xact_cxt.PGXCGroupOid = InvalidOid;
    t_thrd.xact_cxt.PGXCNodeId = -1;
    t_thrd.xact_cxt.ActiveLobRelid = InvalidOid;

    CStoreMemAlloc::Reset();
}

/* ----------------------------------------------------------------
 *						CommitSubTransaction stuff
 * ----------------------------------------------------------------
 */
static void AtSubCommit_Memory(void)
{
    TransactionState s = CurrentTransactionState;

    Assert(s->parent != NULL);

    /* Return to parent transaction level's memory context. */
    t_thrd.mem_cxt.cur_transaction_mem_cxt = s->parent->curTransactionContext;
    (void)MemoryContextSwitchTo(t_thrd.mem_cxt.cur_transaction_mem_cxt);

    /*
     * Ordinarily we cannot throw away the child's t_thrd.mem_cxt.cur_transaction_mem_cxt,
     * since the data it contains will be needed at upper commit.  However, if
     * there isn't actually anything in it, we can throw it away.  This avoids
     * a small memory leak in the common case of "trivial" subxacts.
     */
    if (MemoryContextIsValid(s->curTransactionContext) && MemoryContextIsEmpty(s->curTransactionContext)) {
        MemoryContextDelete(s->curTransactionContext);
        s->curTransactionContext = NULL;
    }
}

/*
 * AtSubCommit_childXids
 *
 * Pass my own XID and my child XIDs up to my parent as committed children.
 */
static void AtSubCommit_childXids(void)
{
    TransactionState s = CurrentTransactionState;
    int new_nChildXids;
    errno_t errorno = EOK;

    Assert(s->parent != NULL);

    /*
     * The parent childXids array will need to hold my XID and all my
     * childXids, in addition to the XIDs already there.
     */
    new_nChildXids = s->parent->nChildXids + s->nChildXids + 1;

    /* Allocate or enlarge the parent array if necessary */
    if (s->parent->maxChildXids < new_nChildXids) {
        int new_maxChildXids;
        TransactionId *new_childXids = NULL;

        /*
         * Make it 2x what's needed right now, to avoid having to enlarge it
         * repeatedly. But we can't go above MaxAllocSize.  (The latter limit
         * is what ensures that we don't need to worry about integer overflow
         * here or in the calculation of new_nChildXids.)
         */
        new_maxChildXids = Min(new_nChildXids * 2, (int)(MaxAllocSize / sizeof(TransactionId)));

        if (new_maxChildXids < new_nChildXids)
            ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                            errmsg("maximum number of committed subtransactions (%d) exceeded",
                                   (int)(MaxAllocSize / sizeof(TransactionId)))));

        /*
         * We keep the child-XID arrays in u_sess->top_transaction_mem_cxt; this avoids
         * setting up child-transaction contexts for what might be just a few
         * bytes of grandchild XIDs.
         */
        if (s->parent->childXids == NULL)
            new_childXids = (TransactionId *)MemoryContextAlloc(u_sess->top_transaction_mem_cxt,
                                                                (unsigned)new_maxChildXids * sizeof(TransactionId));
        else
            new_childXids = (TransactionId *)repalloc(s->parent->childXids,
                                                      (unsigned)new_maxChildXids * sizeof(TransactionId));

        s->parent->childXids = new_childXids;
        s->parent->maxChildXids = new_maxChildXids;
    }

    /*
     * Copy all my XIDs to parent's array.
     *
     * Note: We rely on the fact that the XID of a child always follows that
     * of its parent.  By copying the XID of this subtransaction before the
     * XIDs of its children, we ensure that the array stays ordered. Likewise,
     * all XIDs already in the array belong to subtransactions started and
     * subcommitted before us, so their XIDs must precede ours.
     */
    s->parent->childXids[s->parent->nChildXids] = s->transactionId;

    if (s->nChildXids > 0) {
        errorno = memcpy_s(&s->parent->childXids[s->parent->nChildXids + 1],
                           (unsigned)s->parent->maxChildXids * sizeof(TransactionId), s->childXids,
                           (unsigned)s->nChildXids * sizeof(TransactionId));
        securec_check(errorno, "", "");
    }

    s->parent->nChildXids = new_nChildXids;

    /* Release child's array to avoid leakage */
    if (s->childXids != NULL)
        pfree(s->childXids);
    /* We must reset these to avoid double-free if fail later in commit */
    s->childXids = NULL;
    s->nChildXids = 0;
    s->maxChildXids = 0;
}

/* ----------------------------------------------------------------
 *						AbortTransaction stuff
 * ----------------------------------------------------------------
 */
/*
 *	RecordTransactionAbort
 *
 * Returns latest XID among xact and its children, or InvalidTransactionId
 * if the xact has no XID.	(We compute that here just because it's easier.)
 */
static TransactionId RecordTransactionAbort(bool isSubXact)
{
    TransactionId latestXid;
    xl_xact_abort xlrec;
    int nrels = 0;
    int temp_nrels = 0;
    ColFileNodeRel *rels = NULL;
    int nchildren = 0;
    TransactionId *children = NULL;
    int nlibrary = 0;
    char *library_name = NULL;
    int library_length = 0;
    bool bCanAbort = true;
    TransactionId xid = GetCurrentTransactionIdIfAny();
    XLogRecPtr globalDelayDDLLSN = InvalidXLogRecPtr;
    XLogRecPtr abortRecLSN = InvalidXLogRecPtr;

    nlibrary = libraryGetPendingDeletes(false, &library_name, &library_length);
    /*
     * If we haven't been assigned an XID, nobody will care whether we aborted
     * or not.	Hence, we're done in that case.  It does not matter if we have
     * rels to delete (note that this routine is not responsible for actually
     * deleting 'em).  We cannot have any child XIDs, either.
     */
    if (!TransactionIdIsValid(xid)) {
        /* Reset XactLastRecEnd until the next transaction writes something */
        if (!isSubXact)
            t_thrd.xlog_cxt.XactLastRecEnd = 0;
        return InvalidTransactionId;
    }

    /*
     * If we send commit prepared command to DN, we can not write an ABORT record
     * in xlog and clog.
     */
    if (IsNormalProcessingMode()) {
        if (!t_thrd.xact_cxt.XactLocalNodeCanAbort) {
            /* Reset XactLastRecEnd until the next transaction writes something */
            if (!isSubXact)
                t_thrd.xlog_cxt.XactLastRecEnd = 0;

            bCanAbort = false;
        }
    }

    /*
     * We have a valid XID, so we should write an ABORT record for it.
     *
     * We do not flush XLOG to disk here, since the default assumption after a
     * crash would be that we aborted, anyway.	For the same reason, we don't
     * need to worry about interlocking against checkpoint start.
     */
    /*
     * Check that we haven't aborted halfway through RecordTransactionCommit.
     */
    if (bCanAbort && TransactionIdDidCommit(xid))
        ereport(PANIC, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                        errmsg("cannot abort transaction %lu, it was already committed", xid)));

    /* Fetch the data we need for the abort record */
    nrels = smgrGetPendingDeletes(false, &rels, false, &temp_nrels);
    nchildren = xactGetCommittedChildren(&children);

    if (bCanAbort) {
        /* XXX do we really need a critical section here? */
        START_CRIT_SECTION();

        /* Write the ABORT record */
        if (isSubXact) {
            xlrec.xact_time = GetCurrentTimestamp();
        } else {
            SetCurrentTransactionStopTimestamp();
#ifdef PGXC
            /* In Postgres-XC, stop timestamp has to follow the timeline of GTM */
            xlrec.xact_time = t_thrd.xact_cxt.xactStopTimestamp + t_thrd.xact_cxt.GTMdeltaTimestamp;
#else
            xlrec.xact_time = t_thrd.xact_cxt.xactStopTimestamp;
#endif
        }
        xlrec.nrels = nrels;
        xlrec.nsubxacts = nchildren;
        xlrec.nlibrary = nlibrary;

        XLogBeginInsert();
        XLogRegisterData((char *)(&xlrec), MinSizeOfXactAbort);

        /* dump rels to delete */
        if (nrels > 0) {
            XLogRegisterData((char *)rels, nrels * sizeof(ColFileNodeRel));
            (void)LWLockAcquire(DelayDDLLock, LW_SHARED);
        }

        /* dump committed child Xids */
        if (nchildren > 0)
            XLogRegisterData((char *)children, nchildren * sizeof(TransactionId));

#ifndef ENABLE_MULTIPLE_NODES
        TransactionId curId = InvalidTransactionId;
        if (t_thrd.proc->workingVersionNum >= DECODE_ABORT_VERSION_NUM && XLogLogicalInfoActive()) {
            curId = CurrentTransactionState->transactionId;
            XLogRegisterData((char*)(&curId), sizeof(TransactionId));
        }
#endif

        /* dump library */
        if (nlibrary > 0) {
            XLogRegisterData((char *)library_name, library_length);
        }

#ifndef ENABLE_MULTIPLE_NODES
        if (t_thrd.proc->workingVersionNum >= DECODE_ABORT_VERSION_NUM && XLogLogicalInfoActive()) {
            abortRecLSN = XLogInsert(RM_XACT_ID, XLOG_XACT_ABORT_WITH_XID);
        } else {
            abortRecLSN = XLogInsert(RM_XACT_ID, XLOG_XACT_ABORT);
        }
#else
        abortRecLSN = XLogInsert(RM_XACT_ID, XLOG_XACT_ABORT);
#endif

        if (nrels > 0) {
            globalDelayDDLLSN = GetDDLDelayStartPtr();

            if (!XLogRecPtrIsInvalid(globalDelayDDLLSN) && XLByteLT(globalDelayDDLLSN, abortRecLSN))
                t_thrd.xact_cxt.xactDelayDDL = true;
            else
                t_thrd.xact_cxt.xactDelayDDL = false;

            LWLockRelease(DelayDDLLock);
        }

        /*
         * Report the latest async abort LSN, so that the WAL writer knows to
         * flush this abort. There's nothing to be gained by delaying this, since
         * WALWriter may as well do this when it can. This is important with
         * streaming replication because if we don't flush WAL regularly we will
         * find that large aborts leave us with a long backlog for when commits
         * occur after the abort, increasing our window of data loss should
         * problems occur at that point.
         */
        if (!isSubXact)
            XLogSetAsyncXactLSN(t_thrd.xlog_cxt.XactLastRecEnd);
        if (nrels > 0) {
            XLogWaitFlush(abortRecLSN);
        }
        /*
         * Mark the transaction aborted in clog.  This is not absolutely necessary
         * but we may as well do it while we are here; also, in the subxact case
         * it is helpful because XactLockTableWait makes use of it to avoid
         * waiting for already-aborted subtransactions.  It is OK to do it without
         * having flushed the ABORT record to disk, because in event of a crash
         * we'd be assumed to have aborted anyway.
         */
        TransactionIdAbortTree(xid, nchildren, children);

        END_CRIT_SECTION();
    }

    /* Compute latestXid while we have the child XIDs handy */
    latestXid = TransactionIdLatest(xid, nchildren, children);

    /*
     * If we're aborting a subtransaction, we can immediately remove failed
     * XIDs from PGPROC's cache of running child XIDs.  We do that here for
     * subxacts, because we already have the child XID array at hand.  For
     * main xacts, the equivalent happens just after this function returns.
     */
    if (isSubXact)
        XidCacheRemoveRunningXids(xid, nchildren, children, latestXid);

    /* Reset XactLastRecEnd until the next transaction writes something */
    if (!isSubXact)
        t_thrd.xlog_cxt.XactLastRecEnd = 0;

    /* And clean up local data */
    if (rels != NULL)
        pfree(rels);

    return latestXid;
}

static void AtAbort_Memory(void)
{
    /*
     * Switch into TransactionAbortContext, which should have some free space
     * even if nothing else does.  We'll work in this context until we've
     * finished cleaning up.
     *
     * It is barely possible to get here when we've not been able to create
     * TransactionAbortContext yet; if so use t_thrd.top_mem_cxt.
     */
    if (t_thrd.xact_cxt.TransactionAbortContext != NULL)
        (void)MemoryContextSwitchTo(t_thrd.xact_cxt.TransactionAbortContext);
    else
        (void)MemoryContextSwitchTo(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));
}

static void AtSubAbort_Memory(void)
{
    Assert(t_thrd.xact_cxt.TransactionAbortContext != NULL);

    (void)MemoryContextSwitchTo(t_thrd.xact_cxt.TransactionAbortContext);
}

static void AtAbort_ResourceOwner(void)
{
    /*
     * Make sure we have a valid ResourceOwner, if possible (else it will be
     * NULL, which is OK)
     */
    t_thrd.utils_cxt.CurrentResourceOwner = t_thrd.utils_cxt.TopTransactionResourceOwner;
}

static void AtSubAbort_ResourceOwner(void)
{
    TransactionState s = CurrentTransactionState;

    /* Make sure we have a valid ResourceOwner */
    t_thrd.utils_cxt.CurrentResourceOwner = s->curTransactionOwner;
}

static void AtSubAbort_childXids(void)
{
    TransactionState s = CurrentTransactionState;

    /*
     * We keep the child-XID arrays in u_sess->top_transaction_mem_cxt (see
     * AtSubCommit_childXids).	This means we'd better free the array
     * explicitly at abort to avoid leakage.
     */
    if (s->childXids != NULL)
        pfree(s->childXids);
    s->childXids = NULL;
    s->nChildXids = 0;
    s->maxChildXids = 0;

    /*
     * We could prune the unreportedXids array here. But we don't bother. That
     * would potentially reduce number of XLOG_XACT_ASSIGNMENT records but it
     * would likely introduce more CPU time into the more common paths, so we
     * choose not to do that.
     */
}

static void AtCleanup_Memory(void)
{
    Assert(StreamThreadAmI() || IsBgWorkerProcess() || CurrentTransactionState->parent == NULL);

    /*
     * Now that we're "out" of a transaction, have the system allocate things
     * in the top memory context instead of per-transaction contexts.
     */
    (void)MemoryContextSwitchTo(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));

    /*
     * Clear the special abort context for next time.
     */
    if (t_thrd.xact_cxt.TransactionAbortContext != NULL)
        MemoryContextResetAndDeleteChildren(t_thrd.xact_cxt.TransactionAbortContext);

    /*
     * Release all transaction-local memory.
     */
    if (u_sess->top_transaction_mem_cxt != NULL)
        MemoryContextDelete(u_sess->top_transaction_mem_cxt);
    u_sess->top_transaction_mem_cxt = NULL;
    t_thrd.mem_cxt.cur_transaction_mem_cxt = NULL;

    /* the memory is allocated from top_transaction_mem_cxt */
    t_thrd.asy_cxt.upperPendingActions = NULL;
    t_thrd.asy_cxt.upperPendingNotifies = NULL;

    CurrentTransactionState->curTransactionContext = NULL;
    t_thrd.xact_cxt.PGXCBucketMap = NULL;
    t_thrd.xact_cxt.PGXCBucketCnt = 0;
    t_thrd.xact_cxt.PGXCGroupOid = InvalidOid;
    t_thrd.xact_cxt.PGXCNodeId = -1;
}

/* CleanupSubTransaction stuff */
static void AtSubCleanup_Memory(void)
{
    TransactionState s = CurrentTransactionState;

    Assert(s->parent != NULL);

    /* Make sure we're not in an about-to-be-deleted context */
    (void)MemoryContextSwitchTo(s->parent->curTransactionContext);
    t_thrd.mem_cxt.cur_transaction_mem_cxt = s->parent->curTransactionContext;

    /* Clear the special abort context for next time. */
    if (t_thrd.xact_cxt.TransactionAbortContext != NULL)
        MemoryContextResetAndDeleteChildren(t_thrd.xact_cxt.TransactionAbortContext);

    /*
     * Delete the subxact local memory contexts. Its t_thrd.mem_cxt.cur_transaction_mem_cxt can
     * go too (note this also kills t_thrd.mem_cxt.cur_transaction_mem_cxts from any children
     * of the subxact).
     */
    if (s->curTransactionContext)
        MemoryContextDelete(s->curTransactionContext);
    s->curTransactionContext = NULL;
}

static void StartTransaction(bool begin_on_gtm)
{
    TransactionState s;
    VirtualTransactionId vxid;

    gstrace_entry(GS_TRC_ID_StartTransaction);
    /* clean stream snapshot register info */
    ForgetRegisterStreamSnapshots();

    /* Let's just make sure the state stack is empty */
    s = &TopTransactionStateData;
    CurrentTransactionState = s;
    t_thrd.xact_cxt.bInAbortTransaction = false;
    t_thrd.xact_cxt.handlesDestroyedInCancelQuery = false;

    /* enable CN retry by transaction flag */
    StmtRetrySetTransactionCommitFlag(false);

    /* check the current transaction state */
    if (s->state != TRANS_DEFAULT) {
        ereport(WARNING, (errmsg("StartTransaction while in %s state", TransStateAsString(s->state))));
    }
    /* it's safe to do cleanup at the begin of transaction. */
    DestroyCstoreAlterReg();

    /* when this transaction starts, set the default value. */
    t_thrd.storage_cxt.EnlargeDeadlockTimeout = false;
    /* we must free extral memory which are palloced for aborting transaction */
    gs_memprot_reset_beyondchunk();

    /* set the current transaction state information appropriately during start processing */
    s->state = TRANS_START;
#ifdef PGXC
    s->isLocalParameterUsed = false;
#endif
    s->transactionId = InvalidTransactionId; /* until assigned */

    ResetUndoActionsInfo();

    /*
     * Make sure we've reset xact state variables
     *
     * If recovery is still in progress, mark this transaction as read-only.
     * We have lower level defences in XLogInsert and elsewhere to stop us
     * from modifying data during recovery, but this gives the normal
     * indication to the user that the transaction is read-only.
     */
    if (RecoveryInProgress()) {
        s->startedInRecovery = true;
        u_sess->attr.attr_common.XactReadOnly = true;
    } else if (t_thrd.xlog_cxt.LocalXLogInsertAllowed == 0 && g_instance.streaming_dr_cxt.isInSwitchover == true) {
        /* we are about to start streaming switch over, writting is forbidden. */
        s->startedInRecovery = false;
        u_sess->attr.attr_common.XactReadOnly = true;
    } else {
        s->startedInRecovery = false;
        u_sess->attr.attr_common.XactReadOnly = u_sess->attr.attr_storage.DefaultXactReadOnly;
#ifdef PGXC
        /* Save Postgres-XC session as read-only if necessary */
        if (!u_sess->attr.attr_common.xc_maintenance_mode)
            u_sess->attr.attr_common.XactReadOnly = u_sess->attr.attr_common.XactReadOnly || IsPGXCNodeXactReadOnly();
#endif
        /* if the transaction comes from cm_agent, we always treat it as writable */
        if (u_sess->libpq_cxt.IsConnFromCmAgent) {
            u_sess->attr.attr_common.XactReadOnly = false;
        }
    }
    u_sess->attr.attr_storage.XactDeferrable = u_sess->attr.attr_storage.DefaultXactDeferrable;
#ifdef PGXC
    /* PGXC - PGXC doesn't support 9.1 serializable transactions. They are
     * silently turned into repeatable-reads which is same as pre 9.1
     * serializable isolation level
     */
    if (u_sess->attr.attr_common.DefaultXactIsoLevel == XACT_SERIALIZABLE)
        u_sess->attr.attr_common.DefaultXactIsoLevel = XACT_REPEATABLE_READ;
#endif
    u_sess->utils_cxt.XactIsoLevel = u_sess->attr.attr_common.DefaultXactIsoLevel;
    t_thrd.xact_cxt.forceSyncCommit = false;
    t_thrd.xact_cxt.MyXactAccessedTempRel = false;
    t_thrd.xact_cxt.MyXactAccessedRepRel = false;
    t_thrd.xact_cxt.XactLocalNodePrepared = false;
    t_thrd.xact_cxt.XactLocalNodeCanAbort = true;
    t_thrd.xact_cxt.XactPrepareSent = false;
    t_thrd.xact_cxt.AlterCoordinatorStmt = false;
    t_thrd.utils_cxt.pRelatedRel = NULL;

    /* reinitialize within-transaction counters */
    s->subTransactionId = TopSubTransactionId;
    t_thrd.xact_cxt.currentSubTransactionId = TopSubTransactionId;
    t_thrd.xact_cxt.currentCommandId = FirstCommandId;
    t_thrd.xact_cxt.currentCommandIdUsed = false;
#ifdef PGXC
    /*
     * Parameters related to global command ID control for transaction.
     * Send the 1st command ID.
     */
    t_thrd.xact_cxt.isCommandIdReceived = false;
    if (IsConnFromCoord()) {
        SetReceivedCommandId(FirstCommandId);
        SetSendCommandId(false);
    }
#endif

    /* initialize reported xid accounting */
    t_thrd.xact_cxt.nUnreportedXids = 0;
    s->didLogXid = false;

    /* must initialize resource-management stuff first */
    AtStart_Memory();
    AtStart_ResourceOwner();

    /*
     * Assign a new LocalTransactionId, and combine it with the backendId to
     * form a virtual transaction id.
     */
    vxid.backendId = t_thrd.proc_cxt.MyBackendId;
    vxid.localTransactionId = GetNextLocalTransactionId();

    /* Lock the virtual transaction id before we announce it in the proc array */
    VirtualXactLockTableInsert(vxid);

    /*
     * Advertise it in the proc array.	We assume assignment of
     * LocalTransactionID is atomic, and the backendId should be set already.
     */
    Assert(t_thrd.proc->backendId == vxid.backendId);
    t_thrd.proc->lxid = vxid.localTransactionId;

    TRACE_POSTGRESQL_TRANSACTION_START(vxid.localTransactionId);

    /*
     * initialize current transaction state fields
     *
     * note: prevXactReadOnly is not used at the outermost level
     */
    s->subXactLock = false;
    s->nestingLevel = 1;
    s->gucNestLevel = 1;
    s->childXids = NULL;
    s->nChildXids = 0;
    s->maxChildXids = 0;
    GetUserIdAndSecContext(&s->prevUser, &s->prevSecContext);
    /* SecurityRestrictionContext should never be set outside a transaction */

    /*
     * set transaction_timestamp() (a/k/a now()).  We want this to be the same
     * as the first command's statement_timestamp(), so don't do a fresh
     * GetCurrentTimestamp() call (which'd be expensive anyway).  Also, mark
     * xactStopTimestamp as unset.
     */
    t_thrd.xact_cxt.xactStartTimestamp = t_thrd.xact_cxt.stmtStartTimestamp;
    t_thrd.xact_cxt.xactStopTimestamp = 0;

    s->txnKey.txnHandle = InvalidTransactionHandle;
    s->txnKey.txnTimeline = InvalidTransactionTimeline;
    /*
     * begin transaction and get timestamp but not gxid from GTM
     * NB: autovacuum begin transaction and get gxid together in BeginTranAutovacuumGTM
     */
    bool normal_working = begin_on_gtm && IsNormalProcessingMode() && /* not in CommitTransaction and InitPostgres */
                          !(IsAutoVacuumWorkerProcess() && (t_thrd.pgxact->vacuumFlags & PROC_IN_VACUUM));
    bool update_xact_time = !GTM_FREE_MODE && !u_sess->attr.attr_common.xc_maintenance_mode && normal_working &&
                            IS_PGXC_COORDINATOR && !IsConnFromCoord();
    if (update_xact_time) {
        t_thrd.xact_cxt.GTMxactStartTimestamp = t_thrd.xact_cxt.xactStartTimestamp;
        SetCurrentGTMDeltaTimestamp();

        SetCurrentStmtTimestamp();
        SetStmtSysGTMDeltaTimestamp();
    }

    /*
     * CN uses local time to initialize timestamp when gtm_free is on, and,
     * DN uses local time to initialize timestamp if not get timestamp from cn
     */
    update_xact_time = (GTM_FREE_MODE && IS_PGXC_COORDINATOR && !IsConnFromCoord()) ||
                       (!t_thrd.xact_cxt.timestamp_from_cn && IS_PGXC_DATANODE && normal_working);
    if (update_xact_time) {
        t_thrd.xact_cxt.GTMxactStartTimestamp = t_thrd.xact_cxt.xactStartTimestamp;
        t_thrd.xact_cxt.GTMdeltaTimestamp = 0;
        SetCurrentStmtTimestamp();
    }
    /* reset timestamp flag */
    t_thrd.xact_cxt.timestamp_from_cn = false;
#ifdef PGXC
    /* For Postgres-XC, transaction start timestamp has to follow the GTM timeline */
    pgstat_report_xact_timestamp(t_thrd.xact_cxt.GTMxactStartTimestamp);
#else
    pgstat_report_xact_timestamp(t_thrd.xact_cxt.xactStartTimestamp);
#endif

    /* initialize other subsystems for new transaction */
    AtStart_GUC();
    AtStart_Inval();
    AtStart_Cache();
    AfterTriggerBeginXact();
#ifdef ENABLE_MULTIPLE_NODES
    reset_searchlet_id();
#endif
    ResetBCMArray();

    /* 
     * Get node group status and save in cache,
     * if we are doing two phase commit, skip init cache.
     */
    if (begin_on_gtm && !u_sess->storage_cxt.twoPhaseCommitInProgress) {
        InitNodeGroupStatus();
    }

    /* done with start processing, set current transaction state to "in progress" */
    s->state = TRANS_INPROGRESS;

#ifdef ENABLE_MOT
    CallXactCallbacks(XACT_EVENT_START);
#endif

    if (module_logging_is_on(MOD_TRANS_XACT)) {
        ereport(LOG, (errmodule(MOD_TRANS_XACT),
                      errmsg("start transaction succ. In Node %s, trans state: %s -> %s.",
                             g_instance.attr.attr_common.PGXCNodeName, TransStateAsString(TRANS_START),
                             TransStateAsString(TRANS_INPROGRESS))));
    }

    ShowTransactionState("StartTransaction");

    gstrace_exit(GS_TRC_ID_StartTransaction);
}

void ThreadLocalFlagCleanUp()
{
    if (ENABLE_DN_GPC) {
        CleanSessGPCPtr(u_sess);
    }

    /* clean hash table in opfusion */
    if (IS_PGXC_DATANODE) {
        OpFusion::ClearInUnexpectSituation();
    }

    if (UDFRPCSocket > -1) {
        close(UDFRPCSocket);
        UDFRPCSocket = -1;
    }
    lastUDFOid = InvalidOid;
    /*
     * when this transaction will be aborted, restore and set
     * the default value. this var may be changed druing this transaction.
     */
    t_thrd.storage_cxt.EnlargeDeadlockTimeout = false;
    ResetDeepthInAcceptInvalidationMessage(0);
    t_thrd.xact_cxt.handlesDestroyedInCancelQuery = false;
    u_sess->mb_cxt.insertValuesBind_compatible_illegal_chars = false;
}

/*
 *	CommitTransaction
 *
 * NB: if you change this routine, better look at PrepareTransaction too!
 */
static void CommitTransaction(bool STP_commit)
{
    u_sess->exec_cxt.isLockRows = false;
    u_sess->need_report_top_xid = false;
    TransactionState s = CurrentTransactionState;
    TransactionId latestXid;
    bool barrierLockHeld = false;
#ifdef ENABLE_MULTIPLE_NODES
        checkAndDoUpdateSequence();
#endif

    ShowTransactionState("CommitTransaction");

    /* Check relcache init flag */
    if (needNewLocalCacheFile) {
        ereport(WARNING, (errcode(ERRCODE_WARNING), errmsg("Wrong flag of relcache init flag at commit transaction.")));

        needNewLocalCacheFile = false;
        pg_atomic_exchange_u32(&t_thrd.xact_cxt.ShmemVariableCache->CriticalCacheBuildLock, 0);
    }

    /* Clean node group status cache */
    CleanNodeGroupStatus();

    /* check the current transaction state */
    if (s->state != TRANS_INPROGRESS)
        ereport(WARNING, (errcode(ERRCODE_WARNING),
                          errmsg("CommitTransaction while in %s state", TransStateAsString(s->state))));
    Assert(StreamThreadAmI() || IsBgWorkerProcess() || s->parent == NULL);
    /*
     * Note that parent thread will do commit transaction.
     * Stream thread should read only, no change to xlog files.
     */
    if (StreamThreadAmI() || IsBgWorkerProcess()) {
        ResetTransactionInfo();
    }

    /* destory the global register.  note that this function supports re-enter. */
    DestroyCstoreAlterReg();
    /*
     * when this transaction will be committed, restore and set
     * the default value. this var may be changed during this transaction.
     */
    t_thrd.storage_cxt.EnlargeDeadlockTimeout = false;
    ResetDeepthInAcceptInvalidationMessage(0);
    t_thrd.xact_cxt.handlesDestroyedInCancelQuery = false;
    ThreadLocalFlagCleanUp();

    if (!STP_commit) {
        /* release ref for spi's cachedplan */
        ReleaseSpiPlanRef();

        /* reset store procedure transaction context */
        stp_reset_xact();
    }

    /* When commit within nested store procedure, it will create a plan cache.
     * During commit time, need to clean up those plan cahce.
     */
    if (STP_commit) {
        ResourceOwnerDecrementNPlanRefs(t_thrd.utils_cxt.CurrentResourceOwner, true);
    }
#ifdef PGXC
    /*
     * If we are a Coordinator and currently serving the client,
     * we must run a 2PC if more than one nodes are involved in this
     * transaction. We first prepare on the remote nodes and if everything goes
     * right, we commit locally and then commit on the remote nodes. We must
     * also be careful to prepare locally on this Coordinator only if the
     * local Coordinator has done some write activity.
     *
     * If there are any errors, they will be reported via ereport and the
     * transaction will be aborted.
     *
     * First save the current top transaction ID before it may get overwritten
     * by PrepareTransaction below. We must not reset topGlobalTransansactionId
     * until we are done with finishing the transaction
     */
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        t_thrd.xact_cxt.XactLocalNodePrepared = false;
        if (u_sess->xact_cxt.savePrepareGID) {
            pfree(u_sess->xact_cxt.savePrepareGID);
            u_sess->xact_cxt.savePrepareGID = NULL;
        }

        /*
         * Check if there are any ON COMMIT actions or if temporary objects are in use.
         * Now temp table is just like unlogged table, except auto drop on session close.
         * So no need to enforce_twophase_commit to off
         */
        if (IsOnCommitActions() || ExecIsTempObjectIncluded())
            ExecSetTempObjectIncluded();

        /*
         * If the local node has done some write activity, prepare the local node
         * first. If that fails, the transaction is aborted on all the remote
         * nodes
         */
        if (IsTwoPhaseCommitRequired(t_thrd.xact_cxt.XactWriteLocalNode)) {
            errno_t errorno = EOK;
            u_sess->xact_cxt.prepareGID = (char *)MemoryContextAlloc(u_sess->top_transaction_mem_cxt, MAX_GID_LENGTH);
            errorno = snprintf_s(u_sess->xact_cxt.prepareGID, MAX_GID_LENGTH, MAX_GID_LENGTH - 1, "N%lu_%s",
                GetTopTransactionId(), g_instance.attr.attr_common.PGXCNodeName);
            securec_check_ss(errorno, "", "");

            u_sess->xact_cxt.savePrepareGID = MemoryContextStrdup(
                SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), u_sess->xact_cxt.prepareGID);

            if (t_thrd.xact_cxt.XactWriteLocalNode) {
                /*
                 * OK, local node is involved in the transaction. Prepare the
                 * local transaction now. Errors will be reported via ereport
                 * and that will lead to transaction abortion.
                 */
                Assert(GlobalTransactionIdIsValid(s->transactionId));
                
                /* let gs_clean know local prepared xact is running */
                t_thrd.pgxact->prepare_xid = GetCurrentTransactionIdIfAny();

                PrepareTransaction(STP_commit);
                s->blockState = TBLOCK_DEFAULT;

                /*
                 * PrepareTransaction would have ended the current transaction.
                 * Start a new transaction. We can also use the GXID of this
                 * new transaction to run the COMMIT/ROLLBACK PREPARED
                 * commands. Note that information as part of the
                 * auxilliaryTransactionId
                 */
                StartTransaction(false);
                t_thrd.xact_cxt.XactLocalNodeCanAbort = false;
                t_thrd.xact_cxt.XactLocalNodePrepared = true;
            }
        }
    }
#endif

    /*
     * Do pre-commit processing that involves calling user-defined code, such
     * as triggers.  Since closing cursors could queue trigger actions,
     * triggers could open cursors, etc, we have to keep looping until there's
     * nothing left to do.
     */
    for (;;) {
        /* Fire all currently pending deferred triggers. */
        AfterTriggerFireDeferred();

        /*
         * Close open portals (converting holdable ones into static portals).
         * If there weren't any, we are done ... otherwise loop back to check
         * if they queued deferred triggers.  Lather, rinse, repeat.
         */
        if (!PreCommit_Portals(false, STP_commit))
            break;
    }

    /*
     * The remaining actions cannot call any user-defined code, so it's safe
     * to start shutting down within-transaction services.	But note that most
     * of this stuff could still throw an error, which would switch us into
     * the transaction-abort path.
     */
    /* Shut down the deferred-trigger manager */
    AfterTriggerEndXact(true);

    /*
     * Let ON COMMIT management do its thing (must happen after closing
     * cursors, to avoid dangling-reference problems)
     */
    PreCommit_on_commit_actions();

    /* close large objects before lower-level cleanup */
    AtEOXact_LargeObject(true);

    /*
     * Mark serializable transaction as complete for predicate locking
     * purposes.  This should be done as late as we can put it and still allow
     * errors to be raised for failure patterns found at commit.
     */
    PreCommit_CheckForSerializationFailure();

    /*
     * Insert notifications sent by NOTIFY commands into the queue.  This
     * should be late in the pre-commit sequence to minimize time spent
     * holding the notify-insertion lock.
     */
    PreCommit_Notify();

#ifdef PGXC
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        /*
         * Commit the local transaction as well. Remember, any errors
         * before this point would have been reported via ereport. The fact
         * that we are here shows that the transaction has been committed
         * successfully on the remote nodes
         */
        if (t_thrd.xact_cxt.XactLocalNodePrepared) {
            t_thrd.xact_cxt.XactLocalNodePrepared = false;
            PreventTransactionChain(true, "COMMIT IMPLICIT PREPARED");

#ifdef ENABLE_DISTRIBUTE_TEST
            if (TEST_STUB(CN_COMMIT_PREPARED_FAILED, twophase_default_error_emit)) {
                ereport(g_instance.distribute_test_param_instance->elevel,
                        (errmsg("GTM_TEST  %s: commit prepare %s failed", g_instance.attr.attr_common.PGXCNodeName,
                                u_sess->xact_cxt.savePrepareGID)));
            }
            /* white box test start */
            if (execute_whitebox(WHITEBOX_LOC, u_sess->xact_cxt.savePrepareGID, WHITEBOX_DEFAULT, 0.0001)) {
                ereport(g_instance.distribute_test_param_instance->elevel,
                        (errmsg("WHITE_BOX TEST  %s: failed before phase1 commit prepared.",
                                g_instance.attr.attr_common.PGXCNodeName)));
            }
            /* white box test end */
#endif
            /*
             * Hold barrier lock before cn local commit.
             * Otherwise, cn backup may contain commit record preceding barrier record
             * while dn backup contain barrier record followed by prepare record and
             * commit record.
             */
            (void)LWLockAcquire(BarrierLock, LW_SHARED);
            barrierLockHeld = true;
            FinishPreparedTransaction(u_sess->xact_cxt.savePrepareGID, true);
            StmtRetrySetTransactionCommitFlag(true);
        } else {
            /*
             * Run Remote prepare on the remote nodes.
             * Any errors will be reported via ereport and we will run error recovery as part of AbortTransaction
             */
            PrePrepare_Remote(u_sess->xact_cxt.savePrepareGID, false, false);
        }

#ifdef ENABLE_DISTRIBUTE_TEST
        if (u_sess->xact_cxt.prepareGID != NULL && TEST_STUB(CN_PREPARED_SLEEP, twophase_default_error_emit)) {
            ereport(g_instance.distribute_test_param_instance->elevel,
                    (errmsg("GTM_TEST  %s: prepare sleep", g_instance.attr.attr_common.PGXCNodeName)));
            pg_usleep(g_instance.distribute_test_param_instance->sleep_time * 1000000);
        }

        /* white box test start */
        if (u_sess->xact_cxt.prepareGID) {
            execute_whitebox(WHITEBOX_LOC, u_sess->xact_cxt.savePrepareGID, WHITEBOX_WAIT, 0.0001);
        }
        /* white box test end */
#endif

        /*
         * The current transaction may have been ended and we might have
         * started a new transaction. Re-initialize with
         * CurrentTransactionState
         */
        s = CurrentTransactionState;
    } else {
        if (!GTM_FREE_MODE) {
            /*
             * Data nodes or other coordinators would get transaction id from GTM directly when vacuum or analyze,
             * They should end transaction from GTM firstly before local commit.
             */
            if (!AtEOXact_GlobalTxn(true, true)) {
                ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
                                errmsg("Failed to receive GTM commit transaction response for DN or other CN.")));
            }
        }
    }
#endif

    /* Prevent cancel/die interrupt while cleaning up */
    HOLD_INTERRUPTS();

    /* Commit updates to the relation map --- do this as late as possible */
    AtEOXact_RelationMap(true);

    /*
     * set the current transaction state information appropriately during
     * commit processing
     */
    TransState oldstate = s->state;
    s->state = TRANS_COMMIT;

    /* Wait data replicate */
    if (!IsInitdb && !g_instance.attr.attr_storage.enable_mix_replication) {
        if (g_instance.attr.attr_storage.max_wal_senders > 0)
            DataSndWakeup();

        /* wait for the data synchronization */
        WaitForDataSync();
        Assert(BCMArrayIsEmpty());
    }

    /*
     * For MOT, XACT_EVENT_COMMIT will just do the validation.
     * Actual commit (write redo and apply changes) will be done during XACT_EVENT_RECORD_COMMIT event.
     */
    CallXactCallbacks(XACT_EVENT_COMMIT);

    /*
     * Here is where we really truly local commit.
     */
    latestXid = RecordTransactionCommit();

    if (TwoPhaseCommit)
        StmtRetrySetTransactionCommitFlag(true);

    /*
     * Delete sequence record from GTM
     * Because the transaction can't abort now, so it's safe to delete the record from GTM,
     * and because the sequence record is visible now, so create new sequence with same
     * name will be blocked.
     * We do this to ensure that the sequence record is safely deleted from GTM and would
     * not conflict with creating new sequence with the same name.
     */
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        CallSequenceCallbacks(GTM_EVENT_COMMIT);
    }

    /*
     * If local commit, it can't abort anyway.
     * Even if remote can't commit succeed, local won't write abort. It just end.
     * Wait util gs_clean to process remained prepared xacts
     */
    t_thrd.xact_cxt.XactLocalNodeCanAbort = false;

    /* log for local commit status */
    if (module_logging_is_on(MOD_TRANS_XACT)) {
        ereport(LOG, (errmodule(MOD_TRANS_XACT),
                      errmsg("Local Node %s: local commit has written clog and xlog, trans state : %s -> %s",
                             g_instance.attr.attr_common.PGXCNodeName, TransStateAsString(oldstate),
                             TransStateAsString(s->state))));
    }

#ifdef ENABLE_DISTRIBUTE_TEST

    if (GlobalTransactionIdIsValid(t_thrd.pgxact->prepare_xid) &&
        TEST_STUB(CN_COMMIT_PREPARED_SLEEP, twophase_default_error_emit)) {
        ereport(g_instance.distribute_test_param_instance->elevel,
                (errmsg("GTM_TEST  %s: commit prepare sleep", g_instance.attr.attr_common.PGXCNodeName)));
        /* sleep 30s or more */
        pg_usleep(g_instance.distribute_test_param_instance->sleep_time * 1000000);
    }

    /* white box test start */
    if (GlobalTransactionIdIsValid(t_thrd.pgxact->prepare_xid)) {
        execute_whitebox(WHITEBOX_LOC, u_sess->xact_cxt.savePrepareGID, WHITEBOX_WAIT, 0.0001);
    }
    /* white box test end */
#endif

    /*
     * only execute CN do Remote commit after local commit.
     * alse resume_interrupt here, because we may error and
     * wait remote response here.
     */
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
        RESUME_INTERRUPTS();

    PreCommit_Remote(u_sess->xact_cxt.savePrepareGID, barrierLockHeld);

    if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
        HOLD_INTERRUPTS();

#ifdef ENABLE_DISTRIBUTE_TEST
    if (GlobalTransactionIdIsValid(t_thrd.pgxact->prepare_xid) &&
        TEST_STUB(CN_ABORT_AFTER_ALL_COMMITTED, twophase_default_error_emit)) {
        ereport(g_instance.distribute_test_param_instance->elevel,
                (errmsg("GTM_TEST  %s: cn abort after all committed ", g_instance.attr.attr_common.PGXCNodeName)));
    }

    /* white box test start */
    if (GlobalTransactionIdIsValid(t_thrd.pgxact->prepare_xid) &&
        execute_whitebox(WHITEBOX_LOC, u_sess->xact_cxt.savePrepareGID, WHITEBOX_DEFAULT, 0.0001)) {
        ereport(g_instance.distribute_test_param_instance->elevel,
                (errmsg("WHITE_BOX TEST  %s: cn abort after all committed failed",
                        g_instance.attr.attr_common.PGXCNodeName)));
    }
    /* white box test end */
#endif

    /*
     * Finish 2PC main process succeed
     */
    t_thrd.pgxact->prepare_xid = InvalidTransactionId;

    TRACE_POSTGRESQL_TRANSACTION_COMMIT(t_thrd.proc->lxid);

    /*
     * Let others know about no transaction in progress by me. Note that this
     * must be done _before_ releasing locks we hold and _after_
     * RecordTransactionCommit.
     */
    ProcArrayEndTransaction(t_thrd.proc, latestXid);

#ifdef ENABLE_MOT
    /* Release MOT locks */
    CallXactCallbacks(XACT_EVENT_END_TRANSACTION);
#endif

    /*
     * This is all post-commit cleanup.  Note that if an error is raised here,
     * it's too late to abort the transaction.  This should be just
     * noncritical resource releasing.
     *
     * The ordering of operations is not entirely random.  The idea is:
     * release resources visible to other backends (eg, files, buffer pins);
     * then release locks; then release backend-local resources. We want to
     * release locks at the point where any backend waiting for us will see
     * our transaction as being fully cleaned up.
     *
     * Resources that can be associated with individual queries are handled by
     * the ResourceOwner mechanism.  The other calls here are for backend-wide
     * state.
     */
    instr_report_workload_xact_info(true);
#ifdef PGXC
    /*
     * Call any callback functions initialized for post-commit cleaning up
     * of database/tablespace operations. Mostly this should involve resetting
     * the abort callback functions registered during the db/tbspc operations.
     */
    AtEOXact_DBCleanup(true);
#endif

    AtEOXact_SysDBCache(true);
    ResourceOwnerRelease(t_thrd.utils_cxt.TopTransactionResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, true, true);

    /* Check we've released all buffer pins */
    AtEOXact_Buffers(true);

    /* Clean up the relation cache */
    AtEOXact_RelationCache(true);
    AtEOXact_FreeTupleDesc();

    AtEOXact_PartitionCache(true);

    AtEOXact_BucketCache(true);

    /*
     * Make catalog changes visible to all backends.  This has to happen after
     * relcache references are dropped (see comments for
     * AtEOXact_RelationCache), but before locks are released (if anyone is
     * waiting for lock on a relation we've modified, we want them to know
     * about the catalog change before they start using the relation).
     */
    AtEOXact_Inval(true);

    if ((ENABLE_CN_GPC && !STP_commit) || ENABLE_DN_GPC) {
        g_instance.plan_cache->Commit();
    }

    /*
     * Likewise, dropping of files deleted during the transaction is best done
     * after releasing relcache and buffer pins.  (This is not strictly
     * necessary during commit, since such pins should have been released
     * already, but this ordering is definitely critical during abort.)
     */
    smgrDoPendingDeletes(true);

    release_conn_to_compute_pool();
    release_pgfdw_conn();
    deleteGlobalOBSInstrumentation();
    decrease_rp_number();

    /* Delete C-function remain library file. */
    libraryDoPendingDeletes(true);
    AtEOXact_MultiXact();

    ResourceOwnerRelease(t_thrd.utils_cxt.TopTransactionResourceOwner, RESOURCE_RELEASE_LOCKS, true, true);
    ResourceOwnerRelease(t_thrd.utils_cxt.TopTransactionResourceOwner, RESOURCE_RELEASE_AFTER_LOCKS, true, true);

    /* Check we've released all catcache entries */
    AtEOXact_CatCache(true);

    AtCommit_Notify();
    AtEOXact_GUC(true, 1);
    AtEOXact_SPI(true, false, STP_commit);
    AtEOXact_on_commit_actions(true);
    if (!STP_commit) {
        AtEOXact_Namespace(true);
    }
    AtEOXact_SMgr();
    AtEOXact_Files();
    AtEOXact_ComboCid();
    AtEOXact_HashTables(true);
    AtEOXact_PgStat(true);

#ifdef DEBUG_UHEAP
    AtEOXact_UHeapStats();
#endif

    AtEOXact_Snapshot(true);
    AtEOXact_ApplyLauncher(true);

    pgstat_report_xact_timestamp(0);

    t_thrd.utils_cxt.CurrentResourceOwner = NULL;
    ResourceOwnerDelete(t_thrd.utils_cxt.TopTransactionResourceOwner);
    s->curTransactionOwner = NULL;
    t_thrd.utils_cxt.CurTransactionResourceOwner = NULL;
    t_thrd.utils_cxt.TopTransactionResourceOwner = NULL;
    IsolatedResourceOwner = NULL;
    AtCommit_RelationSync();

    AtCommit_Memory();
#ifdef PGXC
    /* Clean up GTM callbacks at the end of transaction */
    CleanSequenceCallbacks();
#endif

    s->transactionId = InvalidTransactionId;
    s->subTransactionId = InvalidSubTransactionId;
    s->nestingLevel = 0;
    s->gucNestLevel = 0;
    s->childXids = NULL;
    s->nChildXids = 0;
    s->maxChildXids = 0;

#ifdef ENABLE_MOT
    s->storageEngineType = SE_TYPE_UNSPECIFIED;
#endif

    ResetUndoActionsInfo();

#ifdef PGXC
    s->isLocalParameterUsed = false;
    ForgetTransactionLocalNode();

    /*
     * In order the GTMDeltaTimeStamp/stmtSysGTMDeltaTimeStamp of this
     * transaction not to affect the next transaction, reset the GTMdeltaTimestamp
     * and stmtSysGTMDeltaTimeStamp before committing the transaction.
     */
    CleanGTMDeltaTimeStamp();
    CleanstmtSysGTMDeltaTimeStamp();

    /*
     * Set the command ID of Coordinator to be sent to the remote nodes
     * as the 1st one.
     * For remote nodes, enforce the command ID sending flag to false to avoid
     * sending any command ID by default as now transaction is done.
     */
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
        SetReceivedCommandId(FirstCommandId);
    else
        SetSendCommandId(false);

    /*AtCommit_Memory has freed up the memory for savepointList*/
    s->savepointList = NULL;
#endif

    TwoPhaseCommit = false;
    /* done with commit processing, set current transaction state back to default */
    oldstate = s->state;
    s->state = TRANS_DEFAULT;

    if (module_logging_is_on(MOD_TRANS_XACT)) {
        ereport(LOG, (errmodule(MOD_TRANS_XACT),
                      errmsg("Local Node %s: local commit process completed, trans state : %s -> %s",
                             g_instance.attr.attr_common.PGXCNodeName, TransStateAsString(oldstate),
                             TransStateAsString(s->state))));
    }

    RESUME_INTERRUPTS();

    AtEOXact_Remote();
    /* flush all profile log about this worker thread */
    flush_plog();
#ifdef ENABLE_MULTIPLE_NODES 
    CheckDeleteLock(true);
    Tsdb::PartCacheMgr::GetInstance().commit_item();
#endif   /* ENABLE_MULTIPLE_NODES */        
    print_leak_warning_at_commit();
#ifdef ENABLE_MULTIPLE_NODES
    closeAllVfds();
#endif
}

#ifdef ENABLE_MULTIPLE_NODES
static int finish_txn_gtm_lite(bool commit, bool is_write)
{
    TransactionState s = CurrentTransactionState;
    int ret = 0;
    uint64 csn = 0;

    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        if (commit) {
            if (is_write) {
                /* We will always call CommitCSNGTM() in CN. */
                csn = CommitCSNGTM(false);
                setCommitCsn(csn);
                ret = (csn == InvalidCommitSeqNo) ? -1 : 0;
            }
        }
    } else if (IS_PGXC_DATANODE || IsConnFromCoord()) {
        /*
         * If we are autovacuum, commit on GTM
         * GTMLite: we don't need to check if connected to GTM as GetCSNGTM will
         * establish the connection if one doesn't exist
         */
        if (IsAutoVacuumWorkerProcess() || GetForceXidFromGTM() ||
            IsStatementFlushProcess() || IsJobAspProcess()) {
            if (commit) {
                if (GlobalTransactionIdIsValid(s->transactionId)) {
                    csn = CommitCSNGTM(false);
                    setCommitCsn(csn);
                    ret = (csn == InvalidCommitSeqNo) ? -1 : 0;
                }
            }
        }
    }

    /* reset state info */
    s->txnKey.txnHandle = InvalidTransactionHandle;
    s->txnKey.txnTimeline = InvalidTransactionTimeline;

    return ret;
}
#endif
/*
 * Mark the end of global transaction. This is called at the end of the commit
 * or abort processing when the local and remote transactions have been either
 * committed or aborted and we just need to close the transaction on the GTM.
 * Obviously, we don't call this at the PREPARE time because the GXIDs must not
 * be closed at the GTM until the transaction finishes
 */
bool AtEOXact_GlobalTxn(bool commit, bool is_write)
{
#ifndef ENABLE_MULTIPLE_NODES
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return false;
#else
    int ret = 0;
    ret = finish_txn_gtm_lite(commit, is_write);
    SetNextTransactionId(InvalidTransactionId, true);

    return (ret < 0) ? false : true;
#endif
}

/*
 *  EndParallelWorkerTransaction
 *  End a parallel worker transaction.
 */
void EndParallelWorkerTransaction(void)
{
    CommitTransactionCommand();
    CurrentTransactionState->blockState = TBLOCK_DEFAULT;
}

/*
 * PrepareTransaction
 * NB: if you change this routine, better look at CommitTransaction too!
 *
 * Only a Postgres-XC Coordinator that received a PREPARE Command from
 * an application can use this special prepare.
 * If PrepareTransaction is called during an implicit 2PC, do not release ressources,
 * this is made by CommitTransaction when transaction has been committed on Nodes.
 */
static void PrepareTransaction(bool STP_commit)
{
    u_sess->need_report_top_xid = false;
    TransactionState s = CurrentTransactionState;
    TransactionId xid = GetCurrentTransactionId();
    GTM_TransactionHandle handle = GetTransactionHandleIfAny(s);
    GlobalTransaction gxact;
    TimestampTz prepared_at;
#ifdef PGXC
    bool isImplicit = !(s->blockState == TBLOCK_PREPARE);
    char *nodestring = NULL;
#endif

    ShowTransactionState("PrepareTransaction");

    /*
     * check the current transaction state
     */
    if (s->state != TRANS_INPROGRESS)
        ereport(WARNING, (errcode(ERRCODE_WARNING),
                          errmsg("PrepareTransaction while in %s state", TransStateAsString(s->state))));
    Assert((!StreamThreadAmI() && s->parent == NULL) || StreamThreadAmI());

#ifdef PGXC
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        if (u_sess->xact_cxt.savePrepareGID) {
            pfree(u_sess->xact_cxt.savePrepareGID);
            u_sess->xact_cxt.savePrepareGID = NULL;
        }
        u_sess->xact_cxt.savePrepareGID = MemoryContextStrdup(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), u_sess->xact_cxt.prepareGID);

#ifdef ENABLE_DISTRIBUTE_TEST
        if (TEST_STUB(CN_LOCAL_PREPARED_FAILED_A, twophase_default_error_emit)) {
            ereport(g_instance.distribute_test_param_instance->elevel,
                    (errmsg("GTM_TEST  %s: prepare transaction %s failed", g_instance.attr.attr_common.PGXCNodeName,
                            u_sess->xact_cxt.savePrepareGID)));
        }

        /* white box test start */
        if (execute_whitebox(WHITEBOX_LOC, u_sess->xact_cxt.savePrepareGID, WHITEBOX_DEFAULT, 0.0001)) {
            ereport(g_instance.distribute_test_param_instance->elevel,
                    (errmsg("WHITE_BOX TEST  %s: prepare transaction failed before remote prepare",
                            g_instance.attr.attr_common.PGXCNodeName)));
        }
        /* white box test end */
#endif

        nodestring = PrePrepare_Remote(u_sess->xact_cxt.savePrepareGID, isImplicit, true);

#ifdef ENABLE_DISTRIBUTE_TEST
        if (TEST_STUB(CN_LOCAL_PREPARED_FAILED_B, twophase_default_error_emit)) {
            ereport(g_instance.distribute_test_param_instance->elevel,
                    (errmsg("GTM_TEST  %s: prepare transaction %s failed", g_instance.attr.attr_common.PGXCNodeName,
                            u_sess->xact_cxt.savePrepareGID)));
        }

        /* white box test start */
        if (execute_whitebox(WHITEBOX_LOC, u_sess->xact_cxt.savePrepareGID, WHITEBOX_DEFAULT, 0.002)) {
            ereport(g_instance.distribute_test_param_instance->elevel,
                    (errmsg("WHITE_BOX TEST  %s: prepare transaction failed after remote prepare",
                            g_instance.attr.attr_common.PGXCNodeName)));
        }
        /* white box test end */
#endif
    }
#endif

    /*
     * Do pre-commit processing that involves calling user-defined code, such
     * as triggers.  Since closing cursors could queue trigger actions,
     * triggers could open cursors, etc, we have to keep looping until there's
     * nothing left to do.
     */
    if (IS_PGXC_DATANODE) {
        OpFusion::ClearInUnexpectSituation();
    }
    for (;;) {
        /*
         * Fire all currently pending deferred triggers.
         */
        AfterTriggerFireDeferred();

        /*
         * Close open portals (converting holdable ones into static portals).
         * If there weren't any, we are done ... otherwise loop back to check
         * if they queued deferred triggers.  Lather, rinse, repeat.
         */
        if (!PreCommit_Portals(true, STP_commit))
            break;
    }

    /*
     * The remaining actions cannot call any user-defined code, so it's safe
     * to start shutting down within-transaction services.	But note that most
     * of this stuff could still throw an error, which would switch us into
     * the transaction-abort path.
     */
    /* Shut down the deferred-trigger manager */
    AfterTriggerEndXact(true);

    /*
     * Let ON COMMIT management do its thing (must happen after closing
     * cursors, to avoid dangling-reference problems)
     */
    PreCommit_on_commit_actions();

    /* close large objects before lower-level cleanup */
    AtEOXact_LargeObject(true);

    /*
     * Mark serializable transaction as complete for predicate locking
     * purposes.  This should be done as late as we can put it and still allow
     * errors to be raised for failure patterns found at commit.
     */
    PreCommit_CheckForSerializationFailure();

    /*
     * Prepare MOT Engine - Check for Serialization failures in FDW
     */
    CallXactCallbacks(XACT_EVENT_PREPARE);

    /* NOTIFY will be handled below
     *
     * Likewise, don't allow PREPARE after pg_export_snapshot.  This could be
     * supported if we added cleanup logic to twophase.c, but for now it
     * doesn't seem worth the trouble.
     */
    if (XactHasExportedSnapshots())
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("cannot PREPARE a transaction that has exported snapshots")));

    /* Prevent cancel/die interrupt while cleaning up */
    HOLD_INTERRUPTS();

    /*
     * set the current transaction state information appropriately during
     * prepare processing
     */
    s->state = TRANS_PREPARE;

    prepared_at = GetCurrentTimestamp();

    /* Tell bufmgr and smgr to prepare for commit */
    BufmgrCommit();

    /*
     * Reserve the GID for this transaction. This could fail if the requested
     * GID is invalid or already in use.
     */
    gxact = MarkAsPreparing(handle, xid, u_sess->xact_cxt.prepareGID, prepared_at, GetUserId(),
                            u_sess->proc_cxt.MyDatabaseId, t_thrd.proc->sessionid);
    u_sess->xact_cxt.prepareGID = NULL;

    /*
     * Collect data for the 2PC state file.  Note that in general, no actual
     * state change should happen in the called modules during this step,
     * since it's still possible to fail before commit, and in that case we
     * want transaction abort to be able to clean up.  (In particular, the
     * AtPrepare routines may error out if they find cases they cannot
     * handle.)  State cleanup should happen in the PostPrepare routines
     * below.  However, some modules can go ahead and clear state here because
     * they wouldn't do anything with it during abort anyway.
     *
     * Note: because the 2PC state file records will be replayed in the same
     * order they are made, the order of these calls has to match the order in
     * which we want things to happen during COMMIT PREPARED or ROLLBACK
     * PREPARED; in particular, pay attention to whether things should happen
     * before or after releasing the transaction's locks.
     */
    StartPrepare(gxact);

    AtPrepare_Notify();
    AtPrepare_Locks();
    AtPrepare_PredicateLocks();
    AtPrepare_PgStat();
    AtPrepare_MultiXact();
    AtPrepare_RelationMap();

    /*
     * Here is where we really truly prepare.
     *
     * We have to record transaction prepares even if we didn't make any
     * updates, because the transaction manager might get confused if we lose
     * a global transaction.
     */
    EndPrepare(gxact);

    /*
     * Now we clean up backend-internal state and release internal resources.
     *
     * Reset XactLastRecEnd until the next transaction writes something
     */
    t_thrd.xlog_cxt.XactLastRecEnd = 0;

    /*
     * Let others know about no transaction in progress by me.	This has to be
     * done *after* the prepared transaction has been marked valid, else
     * someone may think it is unlocked and recyclable.
     */
    ProcArrayClearTransaction(t_thrd.proc);

    /*
     * In normal commit-processing, this is all non-critical post-transaction
     * cleanup.  When the transaction is prepared, however, it's important that
     * the locks and other per-backend resources are transfered to the
     * prepared transaction's PGPROC entry.  Note that if an error is raised
     * here, it's too late to abort the transaction. XXX: This probably should
     * be in a critical section, to force a PANIC if any of this fails, but
     * that cure could be worse than the disease.
     */

    AtEOXact_SysDBCache(true);
    ResourceOwnerRelease(t_thrd.utils_cxt.TopTransactionResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, true, true);

    /* Check we've released all buffer pins */
    AtEOXact_Buffers(true);

    /* Clean up the relation cache */
    AtEOXact_RelationCache(true);
    AtEOXact_FreeTupleDesc();

    AtEOXact_PartitionCache(true);

    AtEOXact_BucketCache(true);
    /* notify doesn't need a postprepare call */
    PostPrepare_PgStat();

    PostPrepare_Inval();

    PostPrepare_smgr();

    ResetPendingLibraryDelete();

    PostPrepare_MultiXact(xid);

    PostPrepare_Locks(xid);
    PostPrepare_PredicateLocks(xid);
    if (IS_PGXC_DATANODE || IsConnFromCoord()) {
        u_sess->storage_cxt.twoPhaseCommitInProgress = true;
    }
    t_thrd.xact_cxt.needRemoveTwophaseState = false;

    ResourceOwnerRelease(t_thrd.utils_cxt.TopTransactionResourceOwner, RESOURCE_RELEASE_LOCKS, true, true);
    ResourceOwnerRelease(t_thrd.utils_cxt.TopTransactionResourceOwner, RESOURCE_RELEASE_AFTER_LOCKS, true, true);

    /*
     * Allow another backend to finish the transaction.  After
     * PostPrepare_Twophase(), the transaction is completely detached from
     * our backend.  The rest is just non-critical cleanup of backend-local
     * state.
     */
    PostPrepare_Twophase();

    /* Check we've released all catcache entries */
    AtEOXact_CatCache(true);

    /*
     * set csn to commit_in_progress, this must be done before
     * CurrentTransactionState is clean.
     */
    if (!useLocalXid) {
        SetXact2CommitInProgress(xid, 0);
    }

#ifdef PGXC
    /*
     * Notify GTM when all involved node finish prepare
     * must notify here before we release CurrentTransactionState
     * we can retry here because transaction would finally abort in such situation.
     */
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        t_thrd.xact_cxt.XactLocalNodeCanAbort = false;
        if (!GTM_FREE_MODE && !AtEOXact_GlobalTxn(true, true)) {
            ereport(
                ERROR,
                (errcode(ERRCODE_CONNECTION_EXCEPTION),
                 errmsg("Failed to receive GTM commit transaction response after CN local PREPARE TRANSACTION '%s'.",
                        u_sess->xact_cxt.savePrepareGID)));
        }
    }
#endif

    /* PREPARE acts the same as COMMIT as far as GUC is concerned */
    AtEOXact_GUC(true, 1);
    AtEOXact_SPI(true, false, STP_commit);
    AtEOXact_on_commit_actions(true);
    /*
     * For commit within stored procedure dont clean up namespace.
     * Otherwise it will throw warning leaked override search path,
     * since we push the search path hasn't pop yet.
     */
    if (!STP_commit) {
        AtEOXact_Namespace(true);
    }
    AtEOXact_SMgr();
    AtEOXact_Files();
    AtEOXact_ComboCid();
    AtEOXact_HashTables(true);
    /* don't call AtEOXact_PgStat here; we fixed pgstat state above */
    AtEOXact_Snapshot(true);
    pgstat_report_xact_timestamp(0);

    t_thrd.utils_cxt.CurrentResourceOwner = NULL;
    ResourceOwnerDelete(t_thrd.utils_cxt.TopTransactionResourceOwner);
    s->curTransactionOwner = NULL;
    t_thrd.utils_cxt.CurTransactionResourceOwner = NULL;
    t_thrd.utils_cxt.TopTransactionResourceOwner = NULL;

    AtCommit_RelationSync();

    AtCommit_Memory();

    s->transactionId = InvalidTransactionId;
    s->subTransactionId = InvalidSubTransactionId;
    s->nestingLevel = 0;
    s->gucNestLevel = 0;
    s->childXids = NULL;
    s->nChildXids = 0;
    s->maxChildXids = 0;

#ifdef ENABLE_MOT
    s->storageEngineType = SE_TYPE_UNSPECIFIED;
#endif

    /*
     * done with 1st phase commit processing, set current transaction state
     * back to default
     */
    s->state = TRANS_DEFAULT;

    RESUME_INTERRUPTS();

#ifdef PGXC /* PGXC_DATANODE */
    /*
     * Now also prepare the remote nodes involved in this transaction. We do
     * this irrespective of whether we are doing an implicit or an explicit
     * prepare.
     *
     * XXX Like CommitTransaction and AbortTransaction, we do this after
     * resuming interrupts because we are going to access the communication
     * channels. So we want to keep receiving signals to avoid infinite
     * blocking. But this must be checked for correctness
     */
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        PostPrepare_Remote(u_sess->xact_cxt.savePrepareGID, nodestring, isImplicit);
        if (!isImplicit) {
            s->txnKey.txnHandle = InvalidTransactionHandle;
        }
        ForgetTransactionLocalNode();
    }
    SetNextTransactionId(InvalidTransactionId, true);

    /*
     * Set the command ID of Coordinator to be sent to the remote nodes
     * as the 1st one.
     * For remote nodes, enforce the command ID sending flag to false to avoid
     * sending any command ID by default as now transaction is done.
     */
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
        SetReceivedCommandId(FirstCommandId);
    else
        SetSendCommandId(false);
#endif
}

static void AbortTransaction(bool PerfectRollback, bool STP_rollback)
{
    u_sess->exec_cxt.isLockRows = false;
    u_sess->need_report_top_xid = false;
    TransactionState s = CurrentTransactionState;
    TransactionId latestXid;
    t_thrd.xact_cxt.bInAbortTransaction = true;
    t_thrd.utils_cxt.pRelatedRel = NULL;

    /* clean stream snapshot register info */
    ForgetRegisterStreamSnapshots();

    /* release the memory of uuid_t struct */
    uuid_struct_destroy_function();

    /* release relcache init file lock */
    if (needNewLocalCacheFile) {
        needNewLocalCacheFile = false;
        pg_atomic_exchange_u32(&t_thrd.xact_cxt.ShmemVariableCache->CriticalCacheBuildLock, 0);
    }

    /* Prevent cancel/die interrupt while cleaning up */
    HOLD_INTERRUPTS();

    /* Make sure we have a valid memory context and resource owner */
    AtAbort_Memory();

    /*
     * No need to set the CurrentResourceOwner to
     * TopTransactionResourceOwner for rollback within stored procedure.
     * Rollback will clean up local resources attached to CurrentResourceOwner.
     * Otherwise will throw warning for snapshot reference leak or plancache reference leak.
     */
    AtAbort_ResourceOwner();

#ifdef ENABLE_MULTIPLE_NODES 
    /* check and release delete LW lock for timeseries store */
    CheckDeleteLock(false);
    Tsdb::PartCacheMgr::GetInstance().abort_item();
#endif   /* ENABLE_MULTIPLE_NODES */   

    /* CStoreMemAlloc Reset is not allowed interrupt */
    CStoreMemAlloc::Reset();

    /* abort CU cache inserting before release all LW locks */
    CStoreAbortCU();
    /* abort async io, must before LWlock release */
    AbortAsyncListIO();
    /* abort orc metadata block */
    HDFSAbortCacheBlock();

    /*
     * clean urecvec when transaction is aborted
     * no need to release locks all released
     * need free memory before releasing portal context
     */
    if (u_sess->ustore_cxt.urecvec) {
        u_sess->ustore_cxt.urecvec->Reset(false);
    }

    /*
     * Release any LW locks we might be holding as quickly as possible.
     * (Regular locks, however, must be held till we finish aborting.)
     * Releasing LW locks is critical since we might try to grab them again
     * while cleaning up!
     */
    LWLockReleaseAll();

    RESUME_INTERRUPTS();

    /* Clean node group status cache */
    CleanNodeGroupStatus();

#ifdef ENABLE_LLVM_COMPILE
    /*
     * @llvm
     * when the query is abnormal exited, the (GsCodeGen *)t_thrd.codegen_cxt.thr_codegen_obj->codeGenState
     * must be reseted. the parent of code gen context the code is executor context.
     * we can release any memory in subsidiary executor contexts in AtAbort_Portals.
     * function.
     */
    CodeGenThreadTearDown();
#endif

    CancelAutoAnalyze();
    lightProxy::setCurrentProxy(NULL);

    ResetWlmCatalogFlag();
    /*
     * @dfs
     * Clean up DfsReaders that miss invoking deconstructors of dfs::reader::Reader
     * to release the memory not allocated in memory context
     */
    CleanupDfsHandlers(true);
    /*
     * Note that parent thread will do abort transaction.
     * Stream thread should read only, no change to xlog files.
     */
    if (StreamThreadAmI() || IsBgWorkerProcess()) {
        ResetTransactionInfo();
    }
    /*
     * destory the global register.
     * note that this function supports re-enter.
     */
    DestroyCstoreAlterReg();

    ThreadLocalFlagCleanUp();

    if (!STP_rollback) {
        /* release ref for spi's cachedplan */
        ReleaseSpiPlanRef();

        /* reset store procedure transaction context */
        stp_reset_xact();
    }
#ifdef PGXC
    /*
     * Cleanup the files created during database/tablespace operations.
     * This must happen before we release locks, because we want to hold the
     * locks acquired initially while we cleanup the files.
     * If XactLocalNodeCanAbort is false, needn't do DBCleanup, Createdb,movedb,createtablespace e.g.
     */
    AtEOXact_DBCleanup(false);

    /*
     * Notice GTM firstly when xact end. If failed, report warning but not error,
     * if error recurse error might happen, If warning, local exit but GTM might
     * hasn't ended. No problem. snapshot of transaction rollback approximate that
     * in progress.
     */
    if (!GTM_FREE_MODE) {
        if (t_thrd.xact_cxt.XactLocalNodeCanAbort && !AtEOXact_GlobalTxn(false, false) &&
            !t_thrd.proc_cxt.proc_exit_inprogress) {
            ereport(WARNING, (errmsg("Failed to receive GTM rollback transaction response  for aborting prepared %s.",
                                     u_sess->xact_cxt.savePrepareGID)));
        }
    }

#ifdef ENABLE_MOT
    /* If it is MOT transaction reserve the connection. */
    if (IsMOTEngineUsed()) {
        PerfectRollback = true;
    }
#endif

    /* Handle remote abort first. */
    bool reserved_conn = (PerfectRollback && !is_user_name_changed());
    PreAbort_Remote(reserved_conn);
#ifdef ENABLE_MULTIPLE_NODES
    reset_handles_at_abort();
#endif
    if (t_thrd.xact_cxt.XactLocalNodePrepared && t_thrd.xact_cxt.XactLocalNodeCanAbort) {
#ifdef ENABLE_DISTRIBUTE_TEST
        if (TEST_STUB(CN_ABORT_PREPARED_FAILED, twophase_default_error_emit)) {
            ereport(g_instance.distribute_test_param_instance->elevel,
                    (errmsg("GTM_TEST  %s: abort prepared %s failed", g_instance.attr.attr_common.PGXCNodeName,
                            u_sess->xact_cxt.savePrepareGID)));
        }

        /* white box test start */
        if (execute_whitebox(WHITEBOX_LOC, u_sess->xact_cxt.savePrepareGID, WHITEBOX_DEFAULT, 0.0001)) {
            ereport(LOG,
                    (errmsg("WHITE_BOX TEST  %s: abort prepared failed", g_instance.attr.attr_common.PGXCNodeName)));
        }
        /* white box test end */
#endif

        PreventTransactionChain(true, "ROLLBACK IMPLICIT PREPARED");
        FinishPreparedTransaction(u_sess->xact_cxt.savePrepareGID, false);
        t_thrd.xact_cxt.XactLocalNodePrepared = false;
    }

    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        if (t_thrd.xact_cxt.XactLocalNodeCanAbort) {
            CallSequenceCallbacks(GTM_EVENT_ABORT);
        } else {
            ereport(DEBUG1, (errmsg("AbortTransaction in commit state, not to call sequence call back")));
        }
    }
#endif

    /* Prevent cancel/die interrupt while cleaning up */
    HOLD_INTERRUPTS();

#ifdef PGXC
    /* Clean up GTM callbacks */
    CleanSequenceCallbacks();
#endif

    /* clean up ec conection */
#ifndef ENABLE_LLT
    clean_ec_conn();
    delete_ec_ctrl();
#endif

    /* Clear wait information */
    pgstat_report_waitevent(WAIT_EVENT_END);

    /* Clean up buffer I/O and buffer context locks, too */
    AbortBufferIO();
    UnlockBuffers();

    /* Reset WAL record construction state */
    XLogResetInsertion();

    /*
     * Also clean up any open wait for lock, since the lock manager will choke
     * if we try to wait for another lock before doing this.
     */
    LockErrorCleanup();

    RESUME_INTERRUPTS();

    /*
     * When copy failed, we should heap sync the relation, avoid to get
     * the error of invaild page when redo. When heap_sync, maybe we will
     * handle singal, so we can not HOLD_INTERRUPTS().
     */
    AtAbort_RelationSync();

    HOLD_INTERRUPTS();

    if (t_thrd.xact_cxt.needRemoveTwophaseState)
        RemoveStaleTwophaseState(GetCurrentTransactionIdIfAny());

    t_thrd.xact_cxt.needRemoveTwophaseState = false;

    /* check the current transaction state */
    if (s->state != TRANS_INPROGRESS && s->state != TRANS_PREPARE)
        ereport(WARNING,
                (errcode(ERRCODE_WARNING), errmsg("AbortTransaction while in %s state", TransStateAsString(s->state))));

    Assert(StreamThreadAmI() || IsBgWorkerProcess() || s->parent == NULL);

    /* set the current transaction state information appropriately during the abort processing */
    s->state = TRANS_ABORT;

    /* Wait data replicate */
    if (!IsInitdb && !g_instance.attr.attr_storage.enable_mix_replication) {
        if (g_instance.attr.attr_storage.max_wal_senders > 0)
            DataSndWakeup();

        /* wait for the data synchronization */
        WaitForDataSync();
        Assert(BCMArrayIsEmpty());
    }

    /*
     * Reset user ID which might have been changed transiently.  We need this
     * to clean up in case control escaped out of a SECURITY DEFINER function
     * or other local change of CurrentUserId; therefore, the prior value of
     * SecurityRestrictionContext also needs to be restored.
     *
     * (Note: it is not necessary to restore session authorization or role
     * settings here because those can only be changed via GUC, and GUC will
     * take care of rolling them back if need be.)
     */
    SetUserIdAndSecContext(s->prevUser, s->prevSecContext);
    u_sess->exec_cxt.is_exec_trigger_func = false;

    /* reset flag is_delete_function */
    u_sess->plsql_cxt.is_delete_function = false;

    /*
     * do abort processing
     */
    AfterTriggerEndXact(false); /* 'false' means it's abort */

#ifdef ENABLE_MOT
    CallXactCallbacks(XACT_EVENT_PREROLLBACK_CLEANUP);
#endif

    AtAbort_Portals(STP_rollback);
    AtEOXact_LargeObject(false);
    AtAbort_Notify();
    AtEOXact_RelationMap(false);
    AtAbort_Twophase();
#ifdef ENABLE_MULTIPLE_NODES
    rollback_searchlet();
#endif

    setCommitCsn(COMMITSEQNO_ABORTED);

    /*
     * Advertise the fact that we aborted in pg_clog (assuming that we got as
     * far as assigning an XID to advertise).
     */
    latestXid = RecordTransactionAbort(false);

    t_thrd.pgxact->prepare_xid = InvalidTransactionId;

    TRACE_POSTGRESQL_TRANSACTION_ABORT(t_thrd.proc->lxid);

    /*
     * Let others know about no transaction in progress by me. Note that this
     * must be done _before_ releasing locks we hold and _after_
     * RecordTransactionAbort.
     */
    ProcArrayEndTransaction(t_thrd.proc, latestXid, false);

    /*
     * Post-abort cleanup.	See notes in CommitTransaction() concerning
     * ordering.  We can skip all of it if the transaction failed before
     * creating a resource owner.
     */
    if (t_thrd.utils_cxt.TopTransactionResourceOwner != NULL) {
        bool change_user_name = false;
        instr_report_workload_xact_info(false);
        CallXactCallbacks(XACT_EVENT_ABORT);

        AtEOXact_SysDBCache(false);
        ResourceOwnerRelease(t_thrd.utils_cxt.TopTransactionResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, false, true);
        AtEOXact_Buffers(false);
        AtEOXact_RelationCache(false);
        AtEOXact_FreeTupleDesc();
        AtEOXact_PartitionCache(false);
        AtEOXact_BucketCache(false);
        AtEOXact_Inval(false);
        smgrDoPendingDeletes(false);
        release_conn_to_compute_pool();
        release_pgfdw_conn();
        deleteGlobalOBSInstrumentation();
        decrease_rp_number();
        libraryDoPendingDeletes(false);
        AtEOXact_MultiXact();
        ResourceOwnerRelease(t_thrd.utils_cxt.TopTransactionResourceOwner, RESOURCE_RELEASE_LOCKS, false, true);
        ResourceOwnerRelease(t_thrd.utils_cxt.TopTransactionResourceOwner, RESOURCE_RELEASE_AFTER_LOCKS, false, true);
        AtEOXact_CatCache(false);

        /* If we are in a transaction block, we'll check whether username have been changed. */
        if (IsTransactionBlock())
            change_user_name = is_user_name_changed();

        AtEOXact_GUC(false, 1);

        if (change_user_name)
            u_sess->misc_cxt.CurrentUserName = NULL;

        AtEOXact_SPI(false, STP_rollback, false);
        AtEOXact_on_commit_actions(false);
        /*
         * For rollback within stored procedure don't need clean up namespace.
         * Otherwise it will throw warning leaked override search path,
         * since we push the search path hasn't pop yet.
         */
        if(!STP_rollback) {
            AtEOXact_Namespace(false);
        }
        AtEOXact_SMgr();
        AtEOXact_Files();
        AtEOXact_ComboCid();
        AtEOXact_HashTables(false);
        AtEOXact_PgStat(false);
        AtEOXact_ApplyLauncher(false);

#ifdef DEBUG_UHEAP
        AtEOXact_UHeapStats();
#endif

        pgstat_report_xact_timestamp(0);
    } else {
        AtEOXact_SysDBCache(false);
    }

#ifdef PGXC
    ForgetTransactionLocalNode();

    /*
     * In order the GTMDeltaTimeStamp/stmtSysGTMDeltaTimeStamp of this
     * transaction not to affect the next transaction, reset the GTMdeltaTimestamp
     * and stmtSysGTMDeltaTimeStamp before abort the transaction.
     */
    CleanGTMDeltaTimeStamp();
    CleanstmtSysGTMDeltaTimeStamp();
#endif
    /*
     * State remains TRANS_ABORT until CleanupTransaction().
     */
    RESUME_INTERRUPTS();

#ifdef PGXC
    AtEOXact_Remote();
#endif

    /* flush all profile log about this worker thread */
    flush_plog();
#ifdef ENABLE_MULTIPLE_NODES
    closeAllVfds();
#endif

#ifndef ENABLE_MULTIPLE_NODES
    /* mark that stream query quits in error, to avoid stream threads not quit while PortalDrop */
    if (u_sess->stream_cxt.global_obj != NULL) {
        u_sess->stream_cxt.global_obj->MarkStreamQuitStatus(STREAM_ERROR);
    }
#endif

    s->savepointList = NULL;

    TwoPhaseCommit = false;
    t_thrd.xact_cxt.bInAbortTransaction = false;
    t_thrd.xact_cxt.enable_lock_cancel = false;
    t_thrd.xact_cxt.ActiveLobRelid = InvalidOid;
    t_thrd.xact_cxt.isSelectInto = false;
}

static void CleanupTransaction(void)
{
    TransactionState s = CurrentTransactionState;

    /* State should still be TRANS_ABORT from AbortTransaction(). */
    if (s->state != TRANS_ABORT)
        ereport(FATAL, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                        errmsg("CleanupTransaction: unexpected state %s", TransStateAsString(s->state))));

    /* do abort cleanup processing */
    AtCleanup_Portals();      /* now safe to release portal memory */
    AtEOXact_Snapshot(false); /* and release the transaction's snapshots */
    list_free_deep(u_sess->xact_cxt.sendSeqDbName);
    list_free_deep(u_sess->xact_cxt.sendSeqSchmaName);
    list_free_deep(u_sess->xact_cxt.sendSeqName);
    list_free_deep(u_sess->xact_cxt.send_result);
    u_sess->xact_cxt.sendSeqDbName = NULL;
    u_sess->xact_cxt.sendSeqSchmaName = NULL;
    u_sess->xact_cxt.sendSeqName = NULL;
    u_sess->xact_cxt.send_result = NULL;
    t_thrd.utils_cxt.CurrentResourceOwner = NULL; /* and resource owner */
    if (t_thrd.utils_cxt.TopTransactionResourceOwner)
        ResourceOwnerDelete(t_thrd.utils_cxt.TopTransactionResourceOwner);
    s->curTransactionOwner = NULL;
    t_thrd.utils_cxt.CurTransactionResourceOwner = NULL;
    t_thrd.utils_cxt.TopTransactionResourceOwner = NULL;
    IsolatedResourceOwner = NULL;

    AtCleanup_Memory(); /* and transaction memory */

    s->transactionId = InvalidTransactionId;
    s->subTransactionId = InvalidSubTransactionId;
    s->nestingLevel = 0;
    s->gucNestLevel = 0;
    s->childXids = NULL;
    s->nChildXids = 0;
    s->maxChildXids = 0;

#ifdef ENABLE_MOT
    s->storageEngineType = SE_TYPE_UNSPECIFIED;
#endif

    ResetUndoActionsInfo();

    /* done with abort processing, set current transaction state back to default */
    s->state = TRANS_DEFAULT;
#ifdef PGXC
    /*
     * Set the command ID of Coordinator to be sent to the remote nodes
     * as the 1st one.
     * For remote nodes, enforce the command ID sending flag to false to avoid
     * sending any command ID by default as now transaction is done.
     */
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
        SetReceivedCommandId(FirstCommandId);
    else
        SetSendCommandId(false);
#endif
}

void StartTransactionCommand(bool STP_rollback)
{
    TransactionState s = CurrentTransactionState;

    switch (s->blockState) {
            /* if we aren't in a transaction block, we just do our usual start transaction. */
        case TBLOCK_DEFAULT:
            StartTransaction(true);
            s->blockState = TBLOCK_STARTED;

            if (module_logging_is_on(MOD_TRANS_XACT)) {
                ereport(LOG, (errmodule(MOD_TRANS_XACT),
                              errmsg("StartTransactionCommand: In Node %s, TransBlock state : %s -> %s",
                                     g_instance.attr.attr_common.PGXCNodeName, BlockStateAsString(TBLOCK_DEFAULT),
                                     BlockStateAsString(s->blockState))));
            }

            break;

            /*
             * We are somewhere in a transaction block or subtransaction and
             * about to start a new command.  For now we do nothing, but
             * someday we may do command-local resource initialization. (Note
             * that any needed CommandCounterIncrement was done by the
             * previous CommitTransactionCommand.)
             */
        case TBLOCK_INPROGRESS:
        case TBLOCK_SUBINPROGRESS:
            break;

            /*
             * Here we are in a failed transaction block (one of the commands
             * caused an abort) so we do nothing but remain in the abort
             * state.  Eventually we will get a ROLLBACK command which will
             * get us out of this state.  (It is up to other code to ensure
             * that no commands other than ROLLBACK will be processed in these
             * states.)
             */
        case TBLOCK_ABORT:
        case TBLOCK_SUBABORT:
            if (STP_rollback) {
                s->blockState = TBLOCK_DEFAULT;
            }
            break;

            /* These cases are invalid. */
        case TBLOCK_STARTED:
        case TBLOCK_BEGIN:
        case TBLOCK_SUBBEGIN:
        case TBLOCK_END:
        case TBLOCK_SUBRELEASE:
        case TBLOCK_SUBCOMMIT:
        case TBLOCK_ABORT_END:
        case TBLOCK_SUBABORT_END:
        case TBLOCK_ABORT_PENDING:
        case TBLOCK_SUBABORT_PENDING:
        case TBLOCK_SUBRESTART:
        case TBLOCK_SUBABORT_RESTART:
        case TBLOCK_PREPARE:
        default:
            ereport(ERROR, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                            errmsg("StartTransactionCommand: unexpected state %s", BlockStateAsString(s->blockState))));
            break;
    }

    /*
     * We must switch to t_thrd.mem_cxt.cur_transaction_mem_cxt before returning. This is
     * already done if we called StartTransaction, otherwise not.
     */
    Assert(t_thrd.mem_cxt.cur_transaction_mem_cxt != NULL);
    (void)MemoryContextSwitchTo(t_thrd.mem_cxt.cur_transaction_mem_cxt);
}

void CommitTransactionCommand(bool STP_commit)
{
    TransactionState s = CurrentTransactionState;
    TBlockState oldstate = s->blockState;
    switch (s->blockState) {
            /*
             * This shouldn't happen, because it means the previous
             * StartTransactionCommand didn't set the STARTED state
             * appropriately.
             */
        case TBLOCK_DEFAULT:
            ereport(FATAL,
                    (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                     errmsg("CommitTransactionCommand: unexpected state %s", BlockStateAsString(s->blockState))));
            break;

            /*
             * If we aren't in a transaction block, just do our usual
             * transaction commit, and return to the idle state.
             */
        case TBLOCK_STARTED:
            CommitTransaction(STP_commit);
            s->blockState = TBLOCK_DEFAULT;
            break;

            /*
             * We are completing a "BEGIN TRANSACTION" command, so we change
             * to the "transaction block in progress" state and return.  (We
             * assume the BEGIN did nothing to the database, so we need no
             * CommandCounterIncrement.)
             */
        case TBLOCK_BEGIN:
            s->blockState = TBLOCK_INPROGRESS;
            break;

            /*
             * This is the case when we have finished executing a command
             * someplace within a transaction block.  We increment the command
             * counter and return.
             */
        case TBLOCK_INPROGRESS:
            CommandCounterIncrement();

            if (STP_commit) {
                CommitTransaction(STP_commit);
                s->blockState = TBLOCK_DEFAULT;
            }
            break;

        case TBLOCK_SUBINPROGRESS:
            CommandCounterIncrement();

            if (STP_commit || TopTransactionStateData.blockState == TBLOCK_STARTED) {
                Assert(!StreamThreadAmI());
                do {
                    MemoryContextSwitchTo(t_thrd.mem_cxt.cur_transaction_mem_cxt);
                    CommitSubTransaction(STP_commit);
                    s = CurrentTransactionState;    /* changed by pop */
                } while (s->blockState == TBLOCK_SUBINPROGRESS);
                if (STP_commit) {
                    /* If we had a COMMIT command, finish off the main xact too */
                    Assert(t_thrd.utils_cxt.STPSavedResourceOwner != NULL);
                    t_thrd.utils_cxt.CurrentResourceOwner = t_thrd.utils_cxt.STPSavedResourceOwner;
                }
                CommitTransaction(STP_commit);
                s->blockState = TBLOCK_DEFAULT;
            }
            break;

            /*
             * We are completing a "COMMIT" command.  Do it and return to the
             * idle state.
             */
        case TBLOCK_END:
            CommitTransaction(STP_commit);
            s->blockState = TBLOCK_DEFAULT;
            break;

            /*
             * Here we are in the middle of a transaction block but one of the
             * commands caused an abort so we do nothing but remain in the
             * abort state.  Eventually we will get a ROLLBACK comand.
             */
        case TBLOCK_ABORT:
        case TBLOCK_SUBABORT:
            break;

            /*
             * Here we were in an aborted transaction block and we just got
             * the ROLLBACK command from the user, so clean up the
             * already-aborted transaction and return to the idle state.
             */
        case TBLOCK_ABORT_END:
            CleanupTransaction();
            s->blockState = TBLOCK_DEFAULT;
            break;

            /*
             * Here we were in a perfectly good transaction block but the user
             * told us to ROLLBACK anyway.	We have to abort the transaction
             * and then clean up.
             */
        case TBLOCK_ABORT_PENDING:
            SetUndoActionsInfo();
            AbortTransaction(true, STP_commit);
            ApplyUndoActions();
            CleanupTransaction();
            s->blockState = TBLOCK_DEFAULT;
            break;

            /*
             * We are completing a "PREPARE TRANSACTION" command.  Do it and
             * return to the idle state.
             */
        case TBLOCK_PREPARE:
            PrepareTransaction();
            s->blockState = TBLOCK_DEFAULT;
            break;

            /*
             * We were just issued a SAVEPOINT inside a transaction block.
             * Start a subtransaction.	(DefineSavepoint already did
             * PushTransaction, so as to have someplace to put the SUBBEGIN
             * state.)
             */
        case TBLOCK_SUBBEGIN:
            StartSubTransaction();
            s->blockState = TBLOCK_SUBINPROGRESS;
            break;

            /*
             * We were issued a RELEASE command, so we end the current
             * subtransaction and return to the parent transaction. The parent
             * might be ended too, so repeat till we find an INPROGRESS
             * transaction or subtransaction.
             */
        case TBLOCK_SUBRELEASE:
            do {
                CommitSubTransaction(STP_commit);
                s = CurrentTransactionState; /* changed by pop */
            } while (s->blockState == TBLOCK_SUBRELEASE);

            Assert(s->blockState == TBLOCK_INPROGRESS || s->blockState == TBLOCK_SUBINPROGRESS ||
                (s->blockState == TBLOCK_STARTED && STP_commit));
            break;

            /*
             * We were issued a COMMIT, so we end the current subtransaction
             * hierarchy and perform final commit. We do this by rolling up
             * any subtransactions into their parent, which leads to O(N^2)
             * operations with respect to resource owners - this isn't that
             * bad until we approach a thousands of savepoints but is
             * necessary for correctness should after triggers create new
             * resource owners.
             */
        case TBLOCK_SUBCOMMIT:
            /* Stream thread just run in top transaction state even in sub xact */
            Assert(!StreamThreadAmI());
            do {
                for (int i = 0; i < UNDO_PERSISTENCE_LEVELS; i++) {
                    if (IS_VALID_UNDO_REC_PTR(s->latest_urp[i])) {
                        s->parent->latest_urp[i] = s->latest_urp[i];
                        s->parent->latest_urp_xact[i] = s->latest_urp[i];
                    }
                    if (!IS_VALID_UNDO_REC_PTR(s->parent->first_urp[i]))
                        s->parent->first_urp[i] = s->first_urp[i];
                }
                CommitSubTransaction(STP_commit);
                s = CurrentTransactionState; /* changed by pop */
            } while (s->blockState == TBLOCK_SUBCOMMIT);
            /* If we had a COMMIT command, finish off the main xact too */
            if (s->blockState == TBLOCK_END) {
                Assert(s->parent == NULL);
                CommitTransaction(STP_commit);
                s->blockState = TBLOCK_DEFAULT;
            } else if (s->blockState == TBLOCK_PREPARE) {
                Assert(s->parent == NULL);
                PrepareTransaction(STP_commit);
                s->blockState = TBLOCK_DEFAULT;
            } else
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                         errmsg("CommitTransactionCommand: unexpected state %s", BlockStateAsString(s->blockState))));
            break;

            /*
             * The current already-failed subtransaction is ending due to a
             * ROLLBACK or ROLLBACK TO command, so pop it and recursively
             * examine the parent (which could be in any of several states).
             */
        case TBLOCK_SUBABORT_END:
            CleanupSubTransaction();
            CommitTransactionCommand(STP_commit);
            break;

            /* As above, but it's not dead yet, so abort first. */
        case TBLOCK_SUBABORT_PENDING:
            SetUndoActionsInfo();
            AbortSubTransaction(STP_commit);
            ApplyUndoActions();
            CleanupSubTransaction(STP_commit);
            if (t_thrd.xact_cxt.handlesDestroyedInCancelQuery) {
                ereport(
                    WARNING,
                    (errmsg(
                        "Transaction aborted as connection handles were destroyed due to clean up stream failed.")));
                AbortOutOfAnyTransaction(true);
            } else
                CommitTransactionCommand(STP_commit);
            break;

            /*
             * The current subtransaction is the target of a ROLLBACK TO
             * command.  Abort and pop it, then start a new subtransaction
             * with the same name.
             */
        case TBLOCK_SUBRESTART: {
            char *name = NULL;
            int savepointLevel;

            /* save name and keep Cleanup from freeing it */
            name = s->name;
            s->name = NULL;
            savepointLevel = s->savepointLevel;

            SetUndoActionsInfo();
            AbortSubTransaction(STP_commit);
            ApplyUndoActions();
            CleanupSubTransaction(STP_commit);
            if (t_thrd.xact_cxt.handlesDestroyedInCancelQuery) {
                ereport(
                    WARNING,
                    (errmsg(
                        "Transaction aborted as connection handles were destroyed due to clean up stream failed.")));
                AbortOutOfAnyTransaction(true);
            } else if (STP_commit) {
                BeginInternalSubTransaction(NULL);

                s = CurrentTransactionState; /* changed by push */
                s->name = name;
                s->savepointLevel = savepointLevel;
            } else {
                DefineSavepoint(NULL);
                s = CurrentTransactionState; /* changed by push */
                s->name = name;
                s->savepointLevel = savepointLevel;

                /* This is the same as TBLOCK_SUBBEGIN case */
                AssertState(s->blockState == TBLOCK_SUBBEGIN);
                StartSubTransaction();
                s->blockState = TBLOCK_SUBINPROGRESS;
            }
        } break;

            /*
             * Same as above, but the subtransaction had already failed, so we
             * don't need AbortSubTransaction.
             */
        case TBLOCK_SUBABORT_RESTART: {
            char *name = NULL;
            int savepointLevel;

            /* save name and keep Cleanup from freeing it */
            name = s->name;
            s->name = NULL;
            savepointLevel = s->savepointLevel;

            CleanupSubTransaction();

            if (STP_commit) {
                BeginInternalSubTransaction(NULL);
                s = CurrentTransactionState; /* changed by push */
                s->name = name;
                s->savepointLevel = savepointLevel;
            } else {
                DefineSavepoint(NULL);
                s = CurrentTransactionState; /* changed by push */
                s->name = name;
                s->savepointLevel = savepointLevel;

                /* This is the same as TBLOCK_SUBBEGIN case */
                AssertState(s->blockState == TBLOCK_SUBBEGIN);
                StartSubTransaction();
                s->blockState = TBLOCK_SUBINPROGRESS;
            }
        } break;
        default:
            ereport(FATAL,
                    (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                     errmsg("CommitTransactionCommand: unexpected state %s", BlockStateAsString(s->blockState))));
            break;
    }

    if (module_logging_is_on(MOD_TRANS_XACT)) {
        ereport(LOG,
                (errmodule(MOD_TRANS_XACT), errmsg("CommitTransactionCommand: TransBlock state %s -> %s",
                                                   BlockStateAsString(oldstate), BlockStateAsString(s->blockState))));
    }
}

void AbortCurrentTransaction(bool STP_rollback)
{
    TransactionState s = CurrentTransactionState;
    bool PerfectRollback = false;
    /*
     * Here, we just detect whether there are any pending undo actions so that
     * we can skip releasing the locks during abort transaction.  We don't
     * release the locks till we execute undo actions otherwise, there is a
     * risk of deadlock.
     */
    SetUndoActionsInfo();

    switch (s->blockState) {
        case TBLOCK_DEFAULT:
            if (s->state == TRANS_DEFAULT) {
                /* we are idle, so nothing to do */
            } else {
                /*
                 * We can get here after an error during transaction start
                 * (state will be TRANS_START).  Need to clean up the
                 * incompletely started transaction.  First, adjust the
                 * low-level state to suppress warning message from
                 * AbortTransaction.
                 */
                if (s->state == TRANS_START)
                    s->state = TRANS_INPROGRESS;
                AbortTransaction(PerfectRollback, STP_rollback);
                CleanupTransaction();
            }
            break;

            /*
             * if we aren't in a transaction block, we just do the basic abort
             * & cleanup transaction.
             */
        case TBLOCK_STARTED:
            AbortTransaction(PerfectRollback, STP_rollback);
            ApplyUndoActions();
            CleanupTransaction();
            s->blockState = TBLOCK_DEFAULT;
            break;

            /*
             * If we are in TBLOCK_BEGIN it means something screwed up right
             * after reading "BEGIN TRANSACTION".  We assume that the user
             * will interpret the error as meaning the BEGIN failed to get him
             * into a transaction block, so we should abort and return to idle
             * state.
             */
        case TBLOCK_BEGIN:
            AbortTransaction(PerfectRollback, STP_rollback);
            CleanupTransaction();
            s->blockState = TBLOCK_DEFAULT;
            break;

            /*
             * We are somewhere in a transaction block and we've gotten a
             * failure, so we abort the transaction and set up the persistent
             * ABORT state.  We will stay in ABORT until we get a ROLLBACK.
             */
        case TBLOCK_INPROGRESS:
            AbortTransaction(PerfectRollback, STP_rollback);
            ApplyUndoActions();
            if (STP_rollback) {
                s->blockState = TBLOCK_DEFAULT;
                CleanupTransaction();
            } else {
                s->blockState = TBLOCK_ABORT;
            }
            /* CleanupTransaction happens when we exit TBLOCK_ABORT_END */
            break;

            /*
             * Here, we failed while trying to COMMIT.	Clean up the
             * transaction and return to idle state (we do not want to stay in
             * the transaction).
             */
        case TBLOCK_END:
            AbortTransaction(PerfectRollback, STP_rollback);
            ApplyUndoActions();
            CleanupTransaction();
            s->blockState = TBLOCK_DEFAULT;
            break;

            /*
             * Here, we are already in an aborted transaction state and are
             * waiting for a ROLLBACK, but for some reason we failed again! So
             * we just remain in the abort state.
             */
        case TBLOCK_ABORT:
        case TBLOCK_SUBABORT:
            break;

            /*
             * We are in a failed transaction and we got the ROLLBACK command.
             * We have already aborted, we just need to cleanup and go to idle
             * state.
             */
        case TBLOCK_ABORT_END:
            CleanupTransaction();
            s->blockState = TBLOCK_DEFAULT;
            break;

            /*
             * We are in a live transaction and we got a ROLLBACK command.
             * Abort, cleanup, go to idle state.
             */
        case TBLOCK_ABORT_PENDING:
            AbortTransaction(PerfectRollback, STP_rollback);
            ApplyUndoActions();
            CleanupTransaction();
            s->blockState = TBLOCK_DEFAULT;
            break;

            /*
             * Here, we failed while trying to PREPARE.  Clean up the
             * transaction and return to idle state (we do not want to stay in
             * the transaction).
             */
        case TBLOCK_PREPARE:
            AbortTransaction(PerfectRollback, STP_rollback);
            ApplyUndoActions();
            CleanupTransaction();
            s->blockState = TBLOCK_DEFAULT;
            break;

            /*
             * We got an error inside a subtransaction.  Abort just the
             * subtransaction, and go to the persistent SUBABORT state until
             * we get ROLLBACK.
             */
        case TBLOCK_SUBINPROGRESS:
            if (STP_rollback || TopTransactionStateData.blockState == TBLOCK_STARTED) {
                do {
                    AbortSubTransaction(STP_rollback);
                    ApplyUndoActions();
                    s->blockState = TBLOCK_SUBABORT;
                    CleanupSubTransaction(STP_rollback);
                    s = CurrentTransactionState;
                } while(s->blockState == TBLOCK_SUBINPROGRESS);

                if (s->state == TRANS_START) {
                    s->state = TRANS_INPROGRESS;
                }
                AbortTransaction(PerfectRollback, STP_rollback);
                ApplyUndoActions();
                CleanupTransaction();
                s->blockState = TBLOCK_DEFAULT;
            } else {
                AbortSubTransaction(STP_rollback);
                ApplyUndoActions();
                s->blockState = TBLOCK_SUBABORT;
            }

            if (t_thrd.xact_cxt.handlesDestroyedInCancelQuery) {
                ereport(
                    WARNING,
                    (errmsg(
                        "Transaction aborted as connection handles were destroyed due to clean up stream failed.")));
                AbortOutOfAnyTransaction(true);
            }
            break;

            /*
             * If we failed while trying to create a subtransaction, clean up
             * the broken subtransaction and abort the parent.	The same
             * applies if we get a failure while ending a subtransaction.
             */
        case TBLOCK_SUBBEGIN:
        case TBLOCK_SUBRELEASE:
        case TBLOCK_SUBCOMMIT:
        case TBLOCK_SUBABORT_PENDING:
        case TBLOCK_SUBRESTART:
            AbortSubTransaction(STP_rollback);
            ApplyUndoActions();
            CleanupSubTransaction(STP_rollback);
            if (t_thrd.xact_cxt.handlesDestroyedInCancelQuery) {
                ereport(
                    WARNING,
                    (errmsg(
                        "Transaction aborted as connection handles were destroyed due to clean up stream failed.")));
                AbortOutOfAnyTransaction(true);
            } else
                AbortCurrentTransaction(STP_rollback);
            break;

            /* Same as above, except the Abort() was already done. */
        case TBLOCK_SUBABORT_END:
        case TBLOCK_SUBABORT_RESTART:
            CleanupSubTransaction();
            AbortCurrentTransaction(STP_rollback);
            break;
        default:
            ereport(FATAL, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                            errmsg("AbortCurrentTransaction: unexpected state %s", BlockStateAsString(s->blockState))));
            break;
    }
}

/*
 *	PreventTransactionChain
 *
 *	This routine is to be called by statements that must not run inside
 *	a transaction block, typically because they have non-rollback-able
 *	side effects or do internal commits.
 *
 *	If we have already started a transaction block, issue an error; also issue
 *	an error if we appear to be running inside a user-defined function (which
 *	could issue more commands and possibly cause a failure after the statement
 *	completes).  Subtransactions are verboten too.
 *
 *	isTopLevel: passed down from ProcessUtility to determine whether we are
 *	inside a function or multi-query querystring.  (We will always fail if
 *	this is false, but it's convenient to centralize the check here instead of
 *	making callers do it.)
 *	stmtType: statement type name, for error messages.
 */
void PreventTransactionChain(bool isTopLevel, const char *stmtType)
{
    /*
     * xact block already started?
     */
    if (IsTransactionBlock())
        ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
                        /* translator: %s represents an SQL statement name */
                        errmsg("%s cannot run inside a transaction block", stmtType)));

    if (IsSubTransaction())
        ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
                        /* translator: %s represents an SQL statement name */
                        errmsg("%s cannot run inside a subtransaction", stmtType)));

    /* inside a function call? */
    if (!isTopLevel)
        ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
                        /* translator: %s represents an SQL statement name */
                        errmsg("%s cannot be executed from a function or multi-command string", stmtType)));

    /* If we got past IsTransactionBlock test, should be in default state */
    if (CurrentTransactionState->blockState != TBLOCK_DEFAULT && CurrentTransactionState->blockState != TBLOCK_STARTED)
        ereport(FATAL, (errcode(ERRCODE_INVALID_TRANSACTION_STATE), errmsg("cannot prevent transaction chain")));
    /* all okay */
}

/*
 *	RequireTransactionChain
 *
 *	This routine is to be called by statements that must run inside
 *	a transaction block, because they have no effects that persist past
 *	transaction end (and so calling them outside a transaction block
 *	is presumably an error).  DECLARE CURSOR is an example.
 *
 *	If we appear to be running inside a user-defined function, we do not
 *	issue an error, since the function could issue more commands that make
 *	use of the current statement's results.  Likewise subtransactions.
 *	Thus this is an inverse for PreventTransactionChain.
 *
 *	isTopLevel: passed down from ProcessUtility to determine whether we are
 *	inside a function.
 *	stmtType: statement type name, for error messages.
 */
void RequireTransactionChain(bool isTopLevel, const char *stmtType)
{
    /* xact block already started? */
    if (IsTransactionBlock()) {
        return;
    }

    /* subtransaction? */
    if (IsSubTransaction()) {
        return;
    }

    /* inside a function call? */
    if (!isTopLevel) {
        return;
    }

    ereport(ERROR, (errcode(ERRCODE_NO_ACTIVE_SQL_TRANSACTION),
                    /* translator: %s represents an SQL statement name */
                    errmsg("%s can only be used in transaction blocks", stmtType)));
}

/*
 *	IsInTransactionChain
 *
 *	This routine is for statements that need to behave differently inside
 *	a transaction block than when running as single commands.  ANALYZE is
 *	currently the only example.
 *
 *	isTopLevel: passed down from ProcessUtility to determine whether we are
 *	inside a function.
 */
bool IsInTransactionChain(bool isTopLevel)
{
    /* Return true on same conditions that would make PreventTransactionChain error out */
    if (IsTransactionBlock()) {
        return true;
    }

    if (IsSubTransaction()) {
        return true;
    }

    if (!isTopLevel) {
        return true;
    }

    if (CurrentTransactionState->blockState != TBLOCK_DEFAULT &&
        CurrentTransactionState->blockState != TBLOCK_STARTED) {
        return true;
    }

    return false;
}

/*
 * Register or deregister callback functions for start- and end-of-xact
 * operations.
 *
 * These functions are intended for use by dynamically loaded modules.
 * For built-in modules we generally just hardwire the appropriate calls
 * (mainly because it's easier to control the order that way, where needed).
 *
 * At transaction end, the callback occurs post-commit or post-abort, so the
 * callback functions can only do noncritical cleanup.
 */
void RegisterXactCallback(XactCallback callback, void *arg)
{
    XactCallbackItem *item = NULL;

    item = (XactCallbackItem *)MemoryContextAlloc(
        SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), sizeof(XactCallbackItem));
    item->callback = callback;
    item->arg = arg;
    item->next = u_sess->xact_cxt.Xact_callbacks;
    u_sess->xact_cxt.Xact_callbacks = item;
}

void UnregisterXactCallback(XactCallback callback, const void *arg)
{
    XactCallbackItem *item = NULL;
    XactCallbackItem *prev = NULL;

    prev = NULL;
    for (item = u_sess->xact_cxt.Xact_callbacks; item; prev = item, item = item->next) {
        if (item->callback == callback && item->arg == arg) {
            if (prev != NULL) {
                prev->next = item->next;
            } else {
                u_sess->xact_cxt.Xact_callbacks = item->next;
            }
            pfree(item);
            break;
        }
    }
}

void CallXactCallbacks(XactEvent event)
{
    XactCallbackItem *item = NULL;

    for (item = u_sess->xact_cxt.Xact_callbacks; item; item = item->next) {
        (*item->callback)(event, item->arg);
    }
}

/*
 * Register or deregister callback functions for start- and end-of-subxact
 * operations.
 *
 * Pretty much same as above, but for subtransaction events.
 *
 * At subtransaction end, the callback occurs post-subcommit or post-subabort,
 * so the callback functions can only do noncritical cleanup.  At
 * subtransaction start, the callback is called when the subtransaction has
 * finished initializing.
 */
void RegisterSubXactCallback(SubXactCallback callback, void *arg)
{
    SubXactCallbackItem *item = NULL;

    item = (SubXactCallbackItem *)MemoryContextAlloc(
        SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), sizeof(SubXactCallbackItem));
    item->callback = callback;
    item->arg = arg;
    item->next = u_sess->xact_cxt.SubXact_callbacks;
    u_sess->xact_cxt.SubXact_callbacks = item;
}

void UnregisterSubXactCallback(SubXactCallback callback, const void *arg)
{
    SubXactCallbackItem *item = NULL;
    SubXactCallbackItem *prev = NULL;

    prev = NULL;
    for (item = u_sess->xact_cxt.SubXact_callbacks; item; prev = item, item = item->next) {
        if (item->callback == callback && item->arg == arg) {
            if (prev != NULL) {
                prev->next = item->next;
            } else {
                u_sess->xact_cxt.SubXact_callbacks = item->next;
            }
            pfree(item);
            break;
        }
    }
}

static void CallSubXactCallbacks(SubXactEvent event, SubTransactionId mySubid, SubTransactionId parentSubid)
{
    SubXactCallbackItem *item = NULL;

    for (item = u_sess->xact_cxt.SubXact_callbacks; item; item = item->next) {
        (*item->callback)(event, mySubid, parentSubid, item->arg);
    }
}

#ifdef PGXC
/*
 * Similar as RegisterGTMCallback, but use t_thrd.top_mem_cxt instead
 * of u_sess->top_transaction_mem_cxt, because we want to delete the seqence
 * on the GTM after CN/DN commit.
 */
void RegisterSequenceCallback(GTMCallback callback, void *arg)
{
    GTMCallbackItem *item = NULL;

    item = (GTMCallbackItem *)MemoryContextAlloc(
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), sizeof(GTMCallbackItem));
    item->callback = callback;
    item->arg = arg;
    item->next = t_thrd.xact_cxt.Seq_callbacks;
    t_thrd.xact_cxt.Seq_callbacks = item;
}

static void CallSequenceCallbacks(GTMEvent event)
{
    GTMCallbackItem *item = NULL;

    uint32 saveInterruptHoldoffCount = t_thrd.int_cxt.InterruptHoldoffCount;
    PG_TRY();
    {
        for (item = t_thrd.xact_cxt.Seq_callbacks; item; item = item->next) {
            (*item->callback)(event, item->arg);
        }
    }
    PG_CATCH();
    {
        /*
         * Once GTM is faulty, CN will try to connect GTM recursively
         * if the error level is ERROR, which may cause coredump.
         */
        if (event == GTM_EVENT_ABORT) {
            t_thrd.int_cxt.InterruptHoldoffCount = saveInterruptHoldoffCount;
            ereport(WARNING, (errmsg("Fail to call sequence call backs when aborting transaction.")));
        } else {
            PG_RE_THROW();
        }
    }
    PG_END_TRY();
}

/*
 * CleanSequenceCallbacks, different from CleanGTMCallBack, need to clean the
 * the THR_LOCAL variable seq_callbacks
 */
static void CleanSequenceCallbacks(void)
{
    GTMCallbackItem *item = NULL;
    GTMCallbackItem *next = NULL;
    for (item = t_thrd.xact_cxt.Seq_callbacks; item; item = next) {
        next = item->next;

        if (item->callback == drop_sequence_cb) {
            drop_sequence_callback_arg *cbargs = (drop_sequence_callback_arg *)(item->arg);
            if (cbargs != NULL) {
                pfree_ext(cbargs);
            }
        } else if (item->callback == rename_sequence_cb) {
            rename_sequence_callback_arg *rcbargs = (rename_sequence_callback_arg *)(item->arg);
            if (rcbargs != NULL) {
                if (rcbargs->newseqname) {
                    pfree_ext(rcbargs->newseqname);
                }

                if (rcbargs->oldseqname) {
                    pfree_ext(rcbargs->oldseqname);
                }
                pfree_ext(rcbargs);
            }
        }
        pfree_ext(item);
    }
    t_thrd.xact_cxt.Seq_callbacks = NULL;
}

#endif

/* ----------------------------------------------------------------
 *					    transaction block support
 * ----------------------------------------------------------------
 */
/*
 * This executes a BEGIN command.
 */
void BeginTransactionBlock(void)
{
    TransactionState s = CurrentTransactionState;

    switch (s->blockState) {
            /*
             * We are not inside a transaction block, so allow one to begin.
             */
        case TBLOCK_STARTED:
            s->blockState = TBLOCK_BEGIN;
            break;

            /* Already a transaction block in progress. */
        case TBLOCK_INPROGRESS:
        case TBLOCK_SUBINPROGRESS:
        case TBLOCK_ABORT:
        case TBLOCK_SUBABORT:
            if ((IS_PGXC_COORDINATOR && !IsConnFromCoord()) ||
                (IS_PGXC_DATANODE == true && IS_SINGLE_NODE == true && useLocalXid == true)) {
                ereport(WARNING, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
                                  errmsg("there is already a transaction in progress")));
            } else {
                ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
                                errmsg("non-execute cn or dn: there is already a transaction in progress")));
            }
            break;

            /* These cases are invalid. */
        case TBLOCK_DEFAULT:
        case TBLOCK_BEGIN:
        case TBLOCK_SUBBEGIN:
        case TBLOCK_END:
        case TBLOCK_SUBRELEASE:
        case TBLOCK_SUBCOMMIT:
        case TBLOCK_ABORT_END:
        case TBLOCK_SUBABORT_END:
        case TBLOCK_ABORT_PENDING:
        case TBLOCK_SUBABORT_PENDING:
        case TBLOCK_SUBRESTART:
        case TBLOCK_SUBABORT_RESTART:
        case TBLOCK_PREPARE:
        default:
            ereport(FATAL, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                            errmsg("BeginTransactionBlock: unexpected state %s", BlockStateAsString(s->blockState))));
            break;
    }

#ifdef PGXC
    /*
     * Set command Id sending flag only for a local Coordinator when transaction begins,
     * For a remote node this flag is set to true only if a command ID has been received
     * from a Coordinator. This may not be always the case depending on the queries being
     * run and how command Ids are generated on remote nodes.
     */
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
        SetSendCommandId(true);
#endif
}

/*
 *	PrepareTransactionBlock
 *		This executes a PREPARE command.
 *
 * Since PREPARE may actually do a ROLLBACK, the result indicates what
 * happened: TRUE for PREPARE, FALSE for ROLLBACK.
 *
 * Note that we don't actually do anything here except change blockState.
 * The real work will be done in the upcoming PrepareTransaction().
 * We do it this way because it's not convenient to change memory context,
 * resource owner, etc while executing inside a Portal.
 */
bool PrepareTransactionBlock(const char *gid)
{
    TransactionState s;
    bool result = false;

#ifdef ENABLE_DISTRIBUTE_TEST
    if (TEST_STUB(DN_PREPARED_FAILED, twophase_default_error_emit)) {
        ereport(g_instance.distribute_test_param_instance->elevel,
                (errmsg("GTM_TEST  %s: prepared transaction %s failed", g_instance.attr.attr_common.PGXCNodeName,
                        gid)));
    }
    /* white box test start */
    if (execute_whitebox(WHITEBOX_LOC, gid, WHITEBOX_DEFAULT, 0.0001)) {
        ereport(g_instance.distribute_test_param_instance->elevel,
                (errmsg("WHITE_BOX TEST  %s: dn prepare transaction failed in prepareTransactionBlock",
                        g_instance.attr.attr_common.PGXCNodeName)));
    }
    /* white box test end */
#endif

    /* Set up to commit the current transaction */
    result = EndTransactionBlock();

    /* If successful, change outer tblock state to PREPARE */
    if (result) {
        s = CurrentTransactionState;

        while (s->parent != NULL)
            s = s->parent;

        if (s->blockState == TBLOCK_END) {
            /* Save GID where PrepareTransaction can find it again */
            u_sess->xact_cxt.prepareGID = MemoryContextStrdup(u_sess->top_transaction_mem_cxt, gid);

            s->blockState = TBLOCK_PREPARE;

            if (module_logging_is_on(MOD_TRANS_XACT)) {
                ereport(LOG, (errmodule(MOD_TRANS_XACT),
                              errmsg("Node %s: state in prepareTransactionBlock is %s",
                                     g_instance.attr.attr_common.PGXCNodeName, BlockStateAsString(s->blockState))));
            }
        } else {
            /*
             * ignore case where we are not in a transaction;
             * EndTransactionBlock already issued a warning.
             */
            Assert(s->blockState == TBLOCK_STARTED);
            /* Don't send back a PREPARE result tag... */
            result = false;
        }
    }

#ifdef PGXC
    /* Reset command ID sending flag */
    SetSendCommandId(false);
#endif

    return result;
}

static void SubTransactionBlockAbort()
{
    TransactionState s = CurrentTransactionState;
    while (s->parent != NULL) {
        if (s->blockState == TBLOCK_SUBINPROGRESS) {
            s->blockState = TBLOCK_SUBABORT_PENDING;
        } else if (s->blockState == TBLOCK_SUBABORT) {
            s->blockState = TBLOCK_SUBABORT_END;
        } else {
            ereport(FATAL,
                    (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                     errmsg("EndTransactionBlock: unexpected state %s", BlockStateAsString(s->blockState))));
        }
        s = s->parent;
    }
    if (s->blockState == TBLOCK_INPROGRESS) {
        s->blockState = TBLOCK_ABORT_PENDING;
    } else if (s->blockState == TBLOCK_ABORT) {
        s->blockState = TBLOCK_ABORT_END;
    } else {
        ereport(FATAL, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                        errmsg("EndTransactionBlock: unexpected state %s", BlockStateAsString(s->blockState))));
    }
}

/*
 *	EndTransactionBlock
 *		This executes a COMMIT command.
 *
 * Since COMMIT may actually do a ROLLBACK, the result indicates what
 * happened: TRUE for COMMIT, FALSE for ROLLBACK.
 *
 * Note that we don't actually do anything here except change blockState.
 * The real work will be done in the upcoming CommitTransactionCommand().
 * We do it this way because it's not convenient to change memory context,
 * resource owner, etc while executing inside a Portal.
 */
bool EndTransactionBlock(void)
{
    TransactionState s = CurrentTransactionState;
    bool result = false;

    switch (s->blockState) {
            /*
             * We are in a transaction block, so tell CommitTransactionCommand
             * to COMMIT.
             */
        case TBLOCK_INPROGRESS:
            s->blockState = TBLOCK_END;

            if (module_logging_is_on(MOD_TRANS_XACT)) {
                ereport(LOG, (errmodule(MOD_TRANS_XACT),
                              errmsg("EndTransactionBlock: state %s", BlockStateAsString(s->blockState))));
            }

            result = true;
            break;

            /*
             * We are in a failed transaction block.  Tell
             * CommitTransactionCommand it's time to exit the block.
             */
        case TBLOCK_ABORT:
            s->blockState = TBLOCK_ABORT_END;
            break;

            /*
             * We are in a live subtransaction block.  Set up to subcommit all
             * open subtransactions and then commit the main transaction.
             */
        case TBLOCK_SUBINPROGRESS:
            while (s->parent != NULL) {
                if (s->blockState == TBLOCK_SUBINPROGRESS) {
                    s->blockState = TBLOCK_SUBCOMMIT;
                } else {
                    ereport(FATAL,
                            (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                             errmsg("EndTransactionBlock: unexpected state %s", BlockStateAsString(s->blockState))));
                }
                s = s->parent;
            }
            if (s->blockState == TBLOCK_INPROGRESS) {
                s->blockState = TBLOCK_END;
            } else {
                ereport(FATAL, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                                errmsg("EndTransactionBlock: unexpected state %s", BlockStateAsString(s->blockState))));
            }
            result = true;
            break;

            /*
             * Here we are inside an aborted subtransaction.  Treat the COMMIT
             * as ROLLBACK: set up to abort everything and exit the main
             * transaction.
             */
        case TBLOCK_SUBABORT:
            SubTransactionBlockAbort();
            break;

            /*
             * The user issued COMMIT when not inside a transaction.  Issue a
             * WARNING, staying in TBLOCK_STARTED state.  The upcoming call to
             * CommitTransactionCommand() will then close the transaction and
             * put us back into the default state.
             */
        case TBLOCK_STARTED:
            ereport(WARNING,
                    (errcode(ERRCODE_NO_ACTIVE_SQL_TRANSACTION), errmsg("there is no transaction in progress")));
            result = true;
            break;

            /* These cases are invalid. */
        case TBLOCK_DEFAULT:
        case TBLOCK_BEGIN:
        case TBLOCK_SUBBEGIN:
        case TBLOCK_END:
        case TBLOCK_SUBRELEASE:
        case TBLOCK_SUBCOMMIT:
        case TBLOCK_ABORT_END:
        case TBLOCK_SUBABORT_END:
        case TBLOCK_ABORT_PENDING:
        case TBLOCK_SUBABORT_PENDING:
        case TBLOCK_SUBRESTART:
        case TBLOCK_SUBABORT_RESTART:
        case TBLOCK_PREPARE:
        default:
            ereport(FATAL, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                            errmsg("EndTransactionBlock: unexpected state %s", BlockStateAsString(s->blockState))));
            break;
    }

#ifdef PGXC
    /* Reset command Id sending flag */
    SetSendCommandId(false);
#endif

    return result;
}

/*
 *	UserAbortTransactionBlock
 *        This executes a ROLLBACK command.
 *
 * As above, we don't actually do anything here except change blockState.
 */
void UserAbortTransactionBlock(void)
{
    TransactionState s = CurrentTransactionState;

    switch (s->blockState) {
            /*
             * We are inside a transaction block and we got a ROLLBACK command
             * from the user, so tell CommitTransactionCommand to abort and
             * exit the transaction block.
             */
        case TBLOCK_INPROGRESS:
            s->blockState = TBLOCK_ABORT_PENDING;
            break;

            /*
             * We are inside a failed transaction block and we got a ROLLBACK
             * command from the user.  Abort processing is already done, so
             * CommitTransactionCommand just has to cleanup and go back to
             * idle state.
             */
        case TBLOCK_ABORT:
            s->blockState = TBLOCK_ABORT_END;
            break;

            /*
             * We are inside a subtransaction.	Mark everything up to top
             * level as exitable.
             */
        case TBLOCK_SUBINPROGRESS:
        case TBLOCK_SUBABORT:
            while (s->parent != NULL) {
                if (s->blockState == TBLOCK_SUBINPROGRESS) {
                    s->blockState = TBLOCK_SUBABORT_PENDING;
                } else if (s->blockState == TBLOCK_SUBABORT) {
                    s->blockState = TBLOCK_SUBABORT_END;
                } else {
                    ereport(FATAL, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                                    errmsg("UserAbortTransactionBlock: unexpected state %s",
                                           BlockStateAsString(s->blockState))));
                }
                s = s->parent;
            }
            if (s->blockState == TBLOCK_INPROGRESS) {
                s->blockState = TBLOCK_ABORT_PENDING;
            } else if (s->blockState == TBLOCK_ABORT) {
                s->blockState = TBLOCK_ABORT_END;
            } else {
                ereport(FATAL,
                        (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                         errmsg("UserAbortTransactionBlock: unexpected state %s", BlockStateAsString(s->blockState))));
            }

            break;

            /*
             * The user issued ABORT when not inside a transaction. Issue a
             * WARNING and go to abort state.  The upcoming call to
             * CommitTransactionCommand() will then put us back into the
             * default state.
             */
        case TBLOCK_STARTED:
            ereport(NOTICE,
                    (errcode(ERRCODE_NO_ACTIVE_SQL_TRANSACTION), errmsg("there is no transaction in progress")));
            s->blockState = TBLOCK_ABORT_PENDING;
            break;

            /* These cases are invalid. */
        case TBLOCK_DEFAULT:
        case TBLOCK_BEGIN:
        case TBLOCK_SUBBEGIN:
        case TBLOCK_END:
        case TBLOCK_SUBRELEASE:
        case TBLOCK_SUBCOMMIT:
        case TBLOCK_ABORT_END:
        case TBLOCK_SUBABORT_END:
        case TBLOCK_ABORT_PENDING:
        case TBLOCK_SUBABORT_PENDING:
        case TBLOCK_SUBRESTART:
        case TBLOCK_SUBABORT_RESTART:
        case TBLOCK_PREPARE:
        default:
            ereport(FATAL,
                    (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                     errmsg("UserAbortTransactionBlock: unexpected state %s", BlockStateAsString(s->blockState))));
            break;
    }

#ifdef PGXC
    /* Reset Command Id sending flag */
    SetSendCommandId(false);
#endif
}

/*
 * DefineSavepoint
 *		This executes a SAVEPOINT command.
 */
void DefineSavepoint(const char *name)
{
    TransactionState s = CurrentTransactionState;
    switch (s->blockState) {
        case TBLOCK_INPROGRESS:
        case TBLOCK_SUBINPROGRESS:
            /* Normal subtransaction start */
            PushTransaction();
            s = CurrentTransactionState; /* changed by push */

            /*
             * Savepoint names, like the TransactionState block itself, live
             * in u_sess->top_transaction_mem_cxt.
             */
            if (name != NULL) {
                s->name = MemoryContextStrdup(u_sess->top_transaction_mem_cxt, name);
            }
            break;

            /* These cases are invalid. */
        case TBLOCK_DEFAULT:
        case TBLOCK_STARTED:
        case TBLOCK_BEGIN:
        case TBLOCK_SUBBEGIN:
        case TBLOCK_END:
        case TBLOCK_SUBRELEASE:
        case TBLOCK_SUBCOMMIT:
        case TBLOCK_ABORT:
        case TBLOCK_SUBABORT:
        case TBLOCK_ABORT_END:
        case TBLOCK_SUBABORT_END:
        case TBLOCK_ABORT_PENDING:
        case TBLOCK_SUBABORT_PENDING:
        case TBLOCK_SUBRESTART:
        case TBLOCK_SUBABORT_RESTART:
        case TBLOCK_PREPARE:
        default:
            ereport(FATAL, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                            errmsg("DefineSavepoint: unexpected state %s", BlockStateAsString(s->blockState))));
            break;
    }
}

static TransactionState FindTargetSavepoint(const char* name, bool inSTP)
{
    TransactionState s = CurrentTransactionState;
    TransactionState target;

    Assert(PointerIsValid(name) || inSTP);
    for (target = s; PointerIsValid(target); target = target->parent) {
        if (!PointerIsValid(target->name) && !PointerIsValid(name)) {
            /* internal savepoint for exception */
            Assert(s != &TopTransactionStateData && inSTP);
            break;
        } else if (PointerIsValid(target->name) && PointerIsValid(name)) {
            /* user defined savepoint */
            if (strcmp(target->name, name) == 0) {
                break;
            }
        }
    }

    if (!PointerIsValid(target)) {
        ereport(ERROR, (errcode(ERRCODE_S_E_INVALID_SPECIFICATION), errmsg("no such savepoint")));
    }

    /* disallow crossing savepoint level boundaries */
    if (target->savepointLevel != s->savepointLevel) {
        ereport(ERROR, (errcode(ERRCODE_S_E_INVALID_SPECIFICATION), errmsg("no such savepoint")));
    }

    return target;
}

static void CheckReleaseSavepointBlockState(bool inSTP)
{
    TransactionState s = CurrentTransactionState;

    switch (s->blockState) {
            /*
             * We can't rollback to a savepoint if there is no savepoint
             * defined.
             */
        case TBLOCK_INPROGRESS:
            ereport(ERROR, (errcode(ERRCODE_S_E_INVALID_SPECIFICATION), errmsg("no such savepoint")));
            break;

            /*
             * We are in a non-aborted subtransaction.	This is the only valid
             * case.
             */
        case TBLOCK_SUBINPROGRESS:
            break;

        case TBLOCK_STARTED:
            if (inSTP)
                break;
            /* These cases are invalid. */
        case TBLOCK_DEFAULT:
        case TBLOCK_BEGIN:
        case TBLOCK_SUBBEGIN:
        case TBLOCK_END:
        case TBLOCK_SUBRELEASE:
        case TBLOCK_SUBCOMMIT:
        case TBLOCK_ABORT:
        case TBLOCK_SUBABORT:
        case TBLOCK_ABORT_END:
        case TBLOCK_SUBABORT_END:
        case TBLOCK_ABORT_PENDING:
        case TBLOCK_SUBABORT_PENDING:
        case TBLOCK_SUBRESTART:
        case TBLOCK_SUBABORT_RESTART:
        case TBLOCK_PREPARE:
        default:
            ereport(FATAL, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                            errmsg("ReleaseSavepoint: unexpected state %s", BlockStateAsString(s->blockState))));
            break;
    }
}

/*
 * ReleaseSavepoint
 *		This executes a RELEASE command.
 *
 * As above, we don't actually do anything here except change blockState.
 */
void ReleaseSavepoint(const char* name, bool inSTP)
{
#ifdef ENABLE_DISTRIBUTE_TEST
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        if (TEST_STUB(CN_RELEASESAVEPOINT_BEFORE_LOCAL_DEAL_FAILED, twophase_default_error_emit)) {
            ereport(g_instance.distribute_test_param_instance->elevel,
                    (errmsg("SUBXACT_TEST %s: cn release savepoint before local deal failed.",
                            g_instance.attr.attr_common.PGXCNodeName)));
        }
    } else {
        if (TEST_STUB(DN_RELEASESAVEPOINT_BEFORE_LOCAL_DEAL_FAILED, twophase_default_error_emit)) {
            ereport(g_instance.distribute_test_param_instance->elevel,
                    (errmsg("SUBXACT_TEST %s:dn release savepoint before local deal failed.",
                            g_instance.attr.attr_common.PGXCNodeName)));
        }
    }
#endif

    /* check expected state. */
    CheckReleaseSavepointBlockState(inSTP);

    TransactionState target = FindTargetSavepoint(name, inSTP);

    /*
     * Mark "commit pending" all subtransactions up to the target
     * subtransaction.	The actual commits will happen when control gets to
     * CommitTransactionCommand.
     */
    TransactionState xact = CurrentTransactionState;
    errno_t rc;
    UndoRecPtr latestUrecPtr[UNDO_PERSISTENCE_LEVELS];
    UndoRecPtr startUrecPtr[UNDO_PERSISTENCE_LEVELS];

    rc = memcpy_s(latestUrecPtr, sizeof(latestUrecPtr), xact->latest_urp, sizeof(latestUrecPtr));
    securec_check(rc, "\0", "\0");
    rc = memcpy_s(startUrecPtr, sizeof(startUrecPtr), xact->first_urp, sizeof(startUrecPtr));

    securec_check(rc, "\0", "\0");
    for (;;) {
        Assert(xact->blockState == TBLOCK_SUBINPROGRESS);
        xact->blockState = TBLOCK_SUBRELEASE;

        /*
         * User savepoint is dropped automatically, dont take PL exception's one
         * into consideration.
         */
        if (inSTP) {
            if (PointerIsValid(xact->name) && u_sess->plsql_cxt.stp_savepoint_cnt > 0) {
                u_sess->plsql_cxt.stp_savepoint_cnt--;
            } else if (!PointerIsValid(xact->name) && u_sess->SPI_cxt.portal_stp_exception_counter > 0) {
                u_sess->SPI_cxt.portal_stp_exception_counter--;
            }
        }

        if (xact == target) {
            break;
        }
        xact = xact->parent;

        /* Propogate start and end undo address to parent transaction */
        for (int i = 0; i < UNDO_PERSISTENCE_LEVELS; i++) {
            if (!IS_VALID_UNDO_REC_PTR(latestUrecPtr[i])) {
                latestUrecPtr[i] = xact->latest_urp[i];
            }

            if (IS_VALID_UNDO_REC_PTR(xact->first_urp[i])) {
                startUrecPtr[i] = xact->first_urp[i];
            }
        }

        Assert(PointerIsValid(xact));
    }

    for (int i = 0; i < UNDO_PERSISTENCE_LEVELS; i++) {
        if (IS_VALID_UNDO_REC_PTR(latestUrecPtr[i])) {
            xact->parent->latest_urp[i] = latestUrecPtr[i];
            xact->parent->latest_urp_xact[i] = latestUrecPtr[i];
        }

        if (!IS_VALID_UNDO_REC_PTR(xact->parent->first_urp[i])) {
            xact->parent->first_urp[i] = startUrecPtr[i];
        }
    }
}

static void CheckRollbackSavepointBlockState(bool inSTP)
{
    TransactionState s = CurrentTransactionState;

    switch (s->blockState) {
        /*
         * We can't rollback to a savepoint if there is no savepoint
         * defined.
         */
        case TBLOCK_INPROGRESS:
        case TBLOCK_ABORT:
            ereport(ERROR, (errcode(ERRCODE_S_E_INVALID_SPECIFICATION), errmsg("no such savepoint")));
            break;

        /* There is at least one savepoint, so proceed. */
        case TBLOCK_SUBINPROGRESS:
        case TBLOCK_SUBABORT:
            break;
        case TBLOCK_STARTED:
            if (inSTP)
                break;

        /* These cases are invalid. */
        case TBLOCK_DEFAULT:
        case TBLOCK_BEGIN:
        case TBLOCK_SUBBEGIN:
        case TBLOCK_END:
        case TBLOCK_SUBRELEASE:
        case TBLOCK_SUBCOMMIT:
        case TBLOCK_ABORT_END:
        case TBLOCK_SUBABORT_END:
        case TBLOCK_ABORT_PENDING:
        case TBLOCK_SUBABORT_PENDING:
        case TBLOCK_SUBRESTART:
        case TBLOCK_SUBABORT_RESTART:
        case TBLOCK_PREPARE:
        default:
            ereport(FATAL, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                            errmsg("RollbackToSavepoint: unexpected state %s", BlockStateAsString(s->blockState))));
            break;
    }
}

/*
 * RollbackToSavepoint
 *		This executes a ROLLBACK TO <savepoint> command.
 *
 * As above, we don't actually do anything here except change blockState.
 */
void RollbackToSavepoint(const char* name, bool inSTP)
{
    /* check expected state. */
    CheckRollbackSavepointBlockState(inSTP);

    TransactionState target = FindTargetSavepoint(name, inSTP);

    /*
     * Mark "abort pending" all subtransactions up to the target
     * subtransaction.	The actual aborts will happen when control gets to
     * CommitTransactionCommand.
     */
    TransactionState xact = CurrentTransactionState;
    for (;;) {
        if (xact == target) {
            break;
        }
        if (xact->blockState == TBLOCK_SUBINPROGRESS) {
            xact->blockState = TBLOCK_SUBABORT_PENDING;
        } else if (xact->blockState == TBLOCK_SUBABORT) {
            xact->blockState = TBLOCK_SUBABORT_END;
        } else {
            ereport(FATAL, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                            errmsg("RollbackToSavepoint: unexpected state %s", BlockStateAsString(xact->blockState))));
        }

        /*
         * User savepoint is dropped automatically, dont take PL exception's one
         * into consideration.
         */
        if (inSTP) {
            if (PointerIsValid(xact->name) && u_sess->plsql_cxt.stp_savepoint_cnt > 0) {
                u_sess->plsql_cxt.stp_savepoint_cnt--;
            } else if (!PointerIsValid(xact->name) && u_sess->SPI_cxt.portal_stp_exception_counter > 0) {
                u_sess->SPI_cxt.portal_stp_exception_counter--;
            }
        }

        xact = xact->parent;
        Assert(PointerIsValid(xact));
    }

    /* And mark the target as "restart pending" */
    if (xact->blockState == TBLOCK_SUBINPROGRESS) {
        xact->blockState = TBLOCK_SUBRESTART;
    } else if (xact->blockState == TBLOCK_SUBABORT) {
        xact->blockState = TBLOCK_SUBABORT_RESTART;
    } else {
        ereport(FATAL, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                        errmsg("RollbackToSavepoint: unexpected state %s", BlockStateAsString(xact->blockState))));
    }
}

/*
 * BeginInternalSubTransaction
 *		This is the same as DefineSavepoint except it allows TBLOCK_STARTED,
 *		TBLOCK_END, and TBLOCK_PREPARE states, and therefore it can safely be
 *		used in functions that might be called when not inside a BEGIN block
 *		or when running deferred triggers at COMMIT/PREPARE time.  Also, it
 *		automatically does CommitTransactionCommand/StartTransactionCommand
 *		instead of expecting the caller to do it.
 */
void BeginInternalSubTransaction(const char *name)
{
    TransactionState s = CurrentTransactionState;

    switch (s->blockState) {
        case TBLOCK_STARTED:
        case TBLOCK_INPROGRESS:
        case TBLOCK_END:
        case TBLOCK_PREPARE:
        case TBLOCK_SUBINPROGRESS:
            /* Normal subtransaction start */
            PushTransaction();
            s = CurrentTransactionState; /* changed by push */

            /*
             * Savepoint names, like the TransactionState block itself, live
             * in u_sess->top_transaction_mem_cxt.
             */
            if (name != NULL)
                s->name = MemoryContextStrdup(u_sess->top_transaction_mem_cxt, name);
            break;

            /* These cases are invalid. */
        case TBLOCK_DEFAULT:
        case TBLOCK_BEGIN:
        case TBLOCK_SUBBEGIN:
        case TBLOCK_SUBRELEASE:
        case TBLOCK_SUBCOMMIT:
        case TBLOCK_ABORT:
        case TBLOCK_SUBABORT:
        case TBLOCK_ABORT_END:
        case TBLOCK_SUBABORT_END:
        case TBLOCK_ABORT_PENDING:
        case TBLOCK_SUBABORT_PENDING:
        case TBLOCK_SUBRESTART:
        case TBLOCK_SUBABORT_RESTART:
        default:
            ereport(FATAL,
                    (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                     errmsg("BeginInternalSubTransaction: unexpected state %s", BlockStateAsString(s->blockState))));
            break;
    }

    CommitTransactionCommand(true);
    StartTransactionCommand(true);
}

/*
 * RELEASE (ie, commit) the innermost subtransaction, regardless of its
 * savepoint name (if any).
 * NB: do NOT use CommitTransactionCommand/StartTransactionCommand with this.
 */
void ReleaseCurrentSubTransaction(bool inSTP)
{
    TransactionState s = CurrentTransactionState;
    int              i;

    if (s->blockState != TBLOCK_SUBINPROGRESS) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                 errmsg("ReleaseCurrentSubTransaction: unexpected state %s", BlockStateAsString(s->blockState))));
    }
    Assert(s->state == TRANS_INPROGRESS);
    (void)MemoryContextSwitchTo(t_thrd.mem_cxt.cur_transaction_mem_cxt);

    for (i = 0; i < UNDO_PERSISTENCE_LEVELS; i++) {
        if (IS_VALID_UNDO_REC_PTR(s->latest_urp[i])) {
            s->parent->latest_urp[i] = s->latest_urp[i];
            s->parent->latest_urp_xact[i] = s->latest_urp[i];
        }

        if (!IS_VALID_UNDO_REC_PTR(s->parent->first_urp[i]))
            s->parent->first_urp[i] = s->first_urp[i];
    }

    CommitSubTransaction(inSTP);
    s = CurrentTransactionState; /* changed by pop */
    Assert(s->state == TRANS_INPROGRESS);
}

/*
 * RollbackAndReleaseCurrentSubTransaction
 *
 * ROLLBACK and RELEASE (ie, abort) the innermost subtransaction, regardless
 * of its savepoint name (if any).
 * NB: do NOT use CommitTransactionCommand/StartTransactionCommand with this.
 */
void RollbackAndReleaseCurrentSubTransaction(bool inSTP)
{
    TransactionState s = CurrentTransactionState;

    switch (s->blockState) {
            /* Must be in a subtransaction */
        case TBLOCK_SUBINPROGRESS:
        case TBLOCK_SUBABORT:
            break;
        case TBLOCK_SUBBEGIN:
            if (inSTP)
                break;

            /* These cases are invalid. */
        case TBLOCK_DEFAULT:
        case TBLOCK_STARTED:
        case TBLOCK_BEGIN:
        case TBLOCK_INPROGRESS:
        case TBLOCK_END:
        case TBLOCK_SUBRELEASE:
        case TBLOCK_SUBCOMMIT:
        case TBLOCK_ABORT:
        case TBLOCK_ABORT_END:
        case TBLOCK_SUBABORT_END:
        case TBLOCK_ABORT_PENDING:
        case TBLOCK_SUBABORT_PENDING:
        case TBLOCK_SUBRESTART:
        case TBLOCK_SUBABORT_RESTART:
        case TBLOCK_PREPARE:
        default:
            ereport(FATAL, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                            errmsg("RollbackAndReleaseCurrentSubTransaction: unexpected state %s",
                                   BlockStateAsString(s->blockState))));
            break;
    }

    /*
     * Set the information required to perform undo actions.  Note that, it
     * must be done before AbortSubTransaction as we need to skip releasing
     * locks if that is the case.  See ApplyUndoActions.
     */
    SetUndoActionsInfo();

    /* Abort the current subtransaction, if needed. */
    if (s->blockState == TBLOCK_SUBINPROGRESS) {
        AbortSubTransaction(inSTP);
    }

    ApplyUndoActions();

    /* And clean it up, too */
    CleanupSubTransaction(inSTP);

    s = CurrentTransactionState; /* changed by pop */
    AssertState(s->blockState == TBLOCK_SUBINPROGRESS || s->blockState == TBLOCK_INPROGRESS ||
                s->blockState == TBLOCK_STARTED);
}

char *GetSavepointName(List *options)
{
    ListCell *cell = NULL;
    char *name = NULL;

    foreach (cell, options) {
        DefElem *elem = (DefElem *)lfirst(cell);

        if (strcmp(elem->defname, "savepoint_name") == 0) {
            name = strVal(elem->arg);
        }
    }
    AssertEreport(PointerIsValid(name), MOD_TRANS_XACT, "name pointer is Invalid");
    return name;
}

/*
 * RecordSavepoint
 * record savepoint's cmd/name/send state/type info for sending to other cns when ddl comes.
 */
void RecordSavepoint(const char *cmd, const char *name, bool hasSent, SavepointStmtType stmtType)
{
    TransactionState s = &TopTransactionStateData;
    GlobalTransactionId transactionId = GetCurrentTransactionId();
    SavepointData *savepointInfo = NULL;
    MemoryContext curContext;

    curContext = MemoryContextSwitchTo(u_sess->top_transaction_mem_cxt);

    savepointInfo = (SavepointData *)MemoryContextAlloc(u_sess->top_transaction_mem_cxt, sizeof(SavepointData));
    savepointInfo->cmd = MemoryContextStrdup(u_sess->top_transaction_mem_cxt, cmd);
    savepointInfo->name = MemoryContextStrdup(u_sess->top_transaction_mem_cxt, name);
    savepointInfo->hasSent = false;
    savepointInfo->stmtType = stmtType;
    savepointInfo->transactionId = transactionId;
    s->savepointList = dlappend(s->savepointList, savepointInfo);

    MemoryContextSwitchTo(curContext);
}

void SendOneSavepointToRemoteCoordinators(const char *cmd, const char *name, SavepointStmtType stmtType,
                                          GlobalTransactionId transactionId)
{
    switch (stmtType) {
        case SUB_STMT_SAVEPOINT:
            pgxc_node_remote_savepoint(cmd, EXEC_ON_COORDS, true, true, transactionId);
            break;
        case SUB_STMT_RELEASE:
            pgxc_node_remote_savepoint(cmd, EXEC_ON_COORDS, false, false, InvalidTransactionId);
            break;
        case SUB_STMT_ROLLBACK_TO:
            pgxc_node_remote_savepoint(cmd, EXEC_ON_COORDS, false, false, InvalidTransactionId);
            break;
        default:
            ereport(ERROR, (errmsg("Wrong type: %d in execSendSavepoint.", stmtType)));
            break;
    }
}

/*
 * Savepoints were sent to all nodes in old version, which leads to error when non-exec CN is down.
 * To solve this problem, we send savepoints to other CNs when executing non-exec-CN-participated
 * utilities in subtransaction. This function is called when executing utilities rather than defining savepoints.
 */
void SendSavepointToRemoteCoordinator()
{
    DList *dlist = TopTransactionStateData.savepointList;
    DListCell *cell = NULL;

    dlist_foreach_cell(cell, dlist)
    {
        SavepointData *elem = (SavepointData *)lfirst(cell);

        if (elem->hasSent == false) {
            elem->hasSent = true;
            SendOneSavepointToRemoteCoordinators(elem->cmd, elem->name, elem->stmtType, elem->transactionId);
        }
    }
}

/*
 * When the excute coordinate recieves "rollback to/release targetSavePoint",
 *  (1) delete related savepoints in savepointList.
 *  (2) send the "rollback to/release targetSavePoint" command to other coordinators if there's DDL/DCL before it.
 */
void HandleReleaseOrRollbackSavepoint(const char *cmd, const char *name, SavepointStmtType stmtType)
{
    DList **dlist = &TopTransactionStateData.savepointList;
    DListCell *cell = dlist_tail_cell(*dlist);
    bool targetSendState = false;

    /*
     * If the recieved command is "rollback to targetSavePoint", delete savepints after targetSavePoint.
     * If the recieved command is "release targetSavePoint", delete the last targetSavePoint and savepoints after it.
     */
    while (cell != NULL) {
        SavepointData *elem = (SavepointData *)lfirst(cell);
        AssertEreport(PointerIsValid(elem), MOD_TRANS_XACT, "Savepoint pointer is invalid");

        if (strcmp(elem->name, name) != 0) {
            DeleteSavepoint(dlist, cell);
        } else {
            targetSendState = elem->hasSent;
            if (stmtType == SUB_STMT_ROLLBACK_TO) {
                break;
            } else if (stmtType == SUB_STMT_RELEASE) {
                DeleteSavepoint(dlist, cell);
                break;
            } else {
                ereport(ERROR, (errmsg("Wrong type: %d when handling savepoints.", stmtType)));
            }
        }
        /* The tail has been delete in DeleteSavepoint, we get the new tail here. */
        cell = dlist_tail_cell(*dlist);
    }

    /*
     * If there's DDL/DCL before "rollback to/release targetSavePoint", the define savepoint command has been sent
     * when handling DDL/DCL.  We need to send "rollback to/release targetSavePoint" to other coordinators.
     */
    if (targetSendState) {
        SendOneSavepointToRemoteCoordinators(cmd, name, stmtType);
    }
}

void DeleteSavepoint(DList **dlist, DListCell *cell)
{
    SavepointData *savepointInfo = (SavepointData *)lfirst(cell);
    if (savepointInfo != NULL) {
        if (savepointInfo->cmd != NULL) {
            pfree(savepointInfo->cmd);
            savepointInfo->cmd = NULL;
        }
        if (savepointInfo->name != NULL) {
            pfree(savepointInfo->name);
            savepointInfo->name = NULL;
        }
    }

    /* dlist can be freed in dlist_delete_cell if there's no element in it. */
    *dlist = dlist_delete_cell(*dlist, cell, true);
}

void FreeSavepointList()
{
    DList **dlist = &TopTransactionStateData.savepointList;
    DListCell *cell = dlist_head_cell(*dlist);

    while (cell != NULL) {
        DeleteSavepoint(dlist, cell);
        /* The head has been delete in DeleteSavepoint, we get the new head here. */
        cell = dlist_head_cell(*dlist);
    }
}

/*
 *	AbortOutOfAnyTransaction
 *
 *	This routine is provided for error recovery purposes.  It aborts any
 *	active transaction or transaction block, leaving the system in a known
 *	idle state.
 *
 *	When handles are destroyed during subxact abort, we use this function
 *	to clean up the doomed xact, with reserve_topxact_abort being set true.
 *	If the top xact is in TBLOCK_INPROGRESS state, we only abort it and then
 *	reserve it in TBLOCK_ABORT state, waiting for an eventual ROLLBACK command.
 */
void AbortOutOfAnyTransaction(bool reserve_topxact_abort)
{
    TransactionState s = CurrentTransactionState;

    /*
     * Get out of any transaction or nested transaction
     */
    do {
        /*
         * Set the flag to perform undo actions if transaction terminates without
         * explicit rollback or commit.
         * Here, we just detect whether there are any pending undo actions so that
         * we can skip releasing the locks during abort transaction.  We don't
         * release the locks till we execute undo actions otherwise, there is a
         * risk of deadlock.
         */
        SetUndoActionsInfo();

        switch (s->blockState) {
            case TBLOCK_DEFAULT:
                if (reserve_topxact_abort) {
                    ereport(FATAL, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                                    errmsg("AbortOutOfAnyTransaction reserving top xact abort: unexpected state %s",
                                           BlockStateAsString(s->blockState))));
                }

                if (s->state == TRANS_DEFAULT) {
                    /* Not in a transaction, do nothing */
                } else {
                    /*
                     * We can get here after an error during transaction start
                     * (state will be TRANS_START).  Need to clean up the
                     * incompletely started transaction.  First, adjust the
                     * low-level state to suppress warning message from
                     * AbortTransaction.
                     */
                    if (s->state == TRANS_START) {
                        s->state = TRANS_INPROGRESS;
                    }
                    AbortTransaction();
                    CleanupTransaction();
                }
                break;

            /* In a transaction, so clean up */
            case TBLOCK_STARTED:
            case TBLOCK_BEGIN:
                if (reserve_topxact_abort) {
                    ereport(FATAL, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                                    errmsg("AbortOutOfAnyTransaction reserving top xact abort: unexpected state %s",
                                           BlockStateAsString(s->blockState))));
                }
                /* fall through */
            case TBLOCK_END:
            case TBLOCK_ABORT_PENDING:
            case TBLOCK_PREPARE:
                AbortTransaction();
                ApplyUndoActions();
                CleanupTransaction();
                s->blockState = TBLOCK_DEFAULT;
                break;

            case TBLOCK_INPROGRESS:
                AbortTransaction();
                if (reserve_topxact_abort) {
                    s->blockState = TBLOCK_ABORT;
                } else {
                    ApplyUndoActions();
                    CleanupTransaction();
                    s->blockState = TBLOCK_DEFAULT;
                }
                break;

            case TBLOCK_UNDO:
                ResetUndoActionsInfo();
                AbortTransaction();
                CleanupTransaction();
                s->blockState = TBLOCK_DEFAULT;
                break;

            /* AbortTransaction already done, still need Cleanup */
            case TBLOCK_ABORT:
            case TBLOCK_ABORT_END:
                if (reserve_topxact_abort) {
                    ereport(FATAL, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                                    errmsg("AbortOutOfAnyTransaction reserving top xact abort: unexpected state %s",
                                           BlockStateAsString(s->blockState))));
                }
                CleanupTransaction();
                s->blockState = TBLOCK_DEFAULT;
                break;

            /* In a subtransaction, so clean it up and abort parent too */
            case TBLOCK_SUBBEGIN:
                if (reserve_topxact_abort) {
                    ereport(FATAL, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                                    errmsg("AbortOutOfAnyTransaction reserving top xact abort: unexpected state %s",
                                           BlockStateAsString(s->blockState))));
                }
                /* fall through */
            case TBLOCK_SUBINPROGRESS:
            case TBLOCK_SUBRELEASE:
            case TBLOCK_SUBCOMMIT:
            case TBLOCK_SUBABORT_PENDING:
            case TBLOCK_SUBRESTART:
                AbortSubTransaction();
                ApplyUndoActions();
                CleanupSubTransaction();
                s = CurrentTransactionState; /* changed by pop */
                break;

            case TBLOCK_SUBUNDO:
                ResetUndoActionsInfo();
                AbortSubTransaction();
                CleanupSubTransaction();
                s = CurrentTransactionState; /* changed by pop */
                break;

            /* As above, but AbortSubTransaction already done */
            case TBLOCK_SUBABORT_END:
            case TBLOCK_SUBABORT_RESTART:
                if (reserve_topxact_abort) {
                    ereport(FATAL, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                                    errmsg("AbortOutOfAnyTransaction reserving top xact abort: unexpected state %s",
                                           BlockStateAsString(s->blockState))));
                }
                /* fall through */
            case TBLOCK_SUBABORT:
                CleanupSubTransaction();
                s = CurrentTransactionState; /* changed by pop */
                break;
            default:
                ereport(FATAL, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                                errmsg("AbortOutOfAnyTransaction reserving top xact abort: unexpected state %s",
                                       BlockStateAsString(s->blockState))));
                break;
        }

        if (reserve_topxact_abort && (s->parent == NULL) && (s->blockState == TBLOCK_ABORT)) {
            break;
        }
    } while (s->blockState != TBLOCK_DEFAULT);

    /* Should be out of all subxacts now */
    Assert(StreamThreadAmI() || IsBgWorkerProcess() || s->parent == NULL);
}

/* IsTransactionBlock --- are we within a transaction block? */
bool IsTransactionBlock(void)
{
    TransactionState s = CurrentTransactionState;

    if (u_sess->SPI_cxt.portal_stp_exception_counter > 0 && s->blockState == TBLOCK_SUBINPROGRESS) {
        return false;
    }

    if (s->blockState == TBLOCK_DEFAULT || s->blockState == TBLOCK_STARTED) {
        return false;
    }

    return true;
}

/*
 * IsTransactionOrTransactionBlock --- are we within either a transaction
 * or a transaction block?	(The backend is only really "idle" when this
 * returns false.)
 *
 * This should match up with IsTransactionBlock and IsTransactionState.
 */
bool IsTransactionOrTransactionBlock(void)
{
    TransactionState s = CurrentTransactionState;

    if (s->blockState == TBLOCK_DEFAULT) {
        return false;
    }

    return true;
}

/*
 * TransactionBlockStatusCode - return status code to send in ReadyForQuery
 */
char TransactionBlockStatusCode(void)
{
    TransactionState s = CurrentTransactionState;

    switch (s->blockState) {
        case TBLOCK_DEFAULT:
        case TBLOCK_STARTED:
            return 'I'; /* idle --- not in transaction */
        case TBLOCK_BEGIN:
        case TBLOCK_SUBBEGIN:
        case TBLOCK_INPROGRESS:
        case TBLOCK_SUBINPROGRESS:
        case TBLOCK_END:
        case TBLOCK_SUBRELEASE:
        case TBLOCK_SUBCOMMIT:
        case TBLOCK_PREPARE:
            return 'T'; /* in transaction */
        case TBLOCK_ABORT:
        case TBLOCK_SUBABORT:
        case TBLOCK_ABORT_END:
        case TBLOCK_SUBABORT_END:
        case TBLOCK_ABORT_PENDING:
        case TBLOCK_SUBABORT_PENDING:
        case TBLOCK_SUBRESTART:
        case TBLOCK_SUBABORT_RESTART:
            return 'E'; /* in failed transaction */
        default:
            break;
    }

    /* should never get here */
    ereport(FATAL, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                    errmsg("invalid transaction block state: %s", BlockStateAsString(s->blockState))));
    return 0; /* keep compiler quiet */
}

bool IsSubTransaction(void)
{
    TransactionState s = CurrentTransactionState;

    if (s->nestingLevel >= 2) {
        return true;
    }

    return false;
}

/*
 * StartSubTransaction
 *
 * If you're wondering why this is separate from PushTransaction: it's because
 * we can't conveniently do this stuff right inside DefineSavepoint.  The
 * SAVEPOINT utility command will be executed inside a Portal, and if we
 * muck with CurrentMemoryContext or CurrentResourceOwner then exit from
 * the Portal will undo those settings.  So we make DefineSavepoint just
 * push a dummy transaction block, and when control returns to the main
 * idle loop, CommitTransactionCommand will be called, and we'll come here
 * to finish starting the subtransaction.
 */
static void StartSubTransaction(void)
{
    TransactionState s = CurrentTransactionState;
    int              i;

    if (s->state != TRANS_DEFAULT) {
        ereport(WARNING, (errmsg("StartSubTransaction while in %s state", TransStateAsString(s->state))));
    }

    s->state = TRANS_START;

    /*
     * Initialize subsystems for new subtransaction
     *
     * must initialize resource-management stuff first
     */
    AtSubStart_Memory();
    AtSubStart_ResourceOwner();
    AtSubStart_Inval();
    AtSubStart_Notify();
    AfterTriggerBeginSubXact();

    ResetUndoActionsInfo();

    s->txnKey.txnHandle = InvalidTransactionHandle;
    s->txnKey.txnTimeline = InvalidTransactionTimeline;

    /* Not InitPostgres */
    Assert(IsNormalProcessingMode());
    Assert(!(IsAutoVacuumWorkerProcess() && (t_thrd.pgxact->vacuumFlags & PROC_IN_VACUUM)));

    /* Begin transaction and get timestamp but not gxid from GTM */
    if (!u_sess->attr.attr_common.xc_maintenance_mode && IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        s->txnKey = s->parent->txnKey;
    }

    for (i = 0; i < UNDO_PERSISTENCE_LEVELS; i++) {
        s->first_urp[i] = INVALID_UNDO_REC_PTR;
        s->latest_urp[i] = INVALID_UNDO_REC_PTR;
        s->latest_urp_xact[i] = s->parent->latest_urp_xact[i];
    }
    s->subXactLock = false;

    s->state = TRANS_INPROGRESS;

    /* for sub transaction, assign gxid when needed like normal */
    s->transactionId = InvalidTransactionId;

    /* Call start-of-subxact callbacks */
    CallSubXactCallbacks(SUBXACT_EVENT_START_SUB, s->subTransactionId, s->parent->subTransactionId);

    ShowTransactionState("StartSubTransaction");
}

/*
 * CommitSubTransaction
 *
 *	The caller has to make sure to always reassign CurrentTransactionState
 *	if it has a local pointer to it after calling this function.
 */
static void CommitSubTransaction(bool STP_commit)
{
    TransactionState s = CurrentTransactionState;

    ShowTransactionState("CommitSubTransaction");

    /* clean hash table for sub transaction in opfusion */
    if (IS_PGXC_DATANODE) {
        OpFusion::ClearInSubUnexpectSituation(s->curTransactionOwner);
    }

    if (s->state != TRANS_INPROGRESS) {
        ereport(WARNING, (errmsg("CommitSubTransaction while in %s state", TransStateAsString(s->state))));
    }

    /* Pre-commit processing goes here -- nothing to do at the moment */
    s->state = TRANS_COMMIT;

    /* Wait data replicate when sub xact commit */
    if (!IsInitdb && !g_instance.attr.attr_storage.enable_mix_replication) {
        if (g_instance.attr.attr_storage.max_wal_senders > 0) {
            DataSndWakeup();
        }

        /* wait for the data synchronization */
        WaitForDataSync();
        Assert(BCMArrayIsEmpty());
    }

    /* Must CCI to ensure commands of subtransaction are seen as done */
    CommandCounterIncrement();

    /*
     * clean dfs handler for sub transaction in such condition:
     * start transaction;
     * savepoint s1;
     * execute direct on (datanode1) 'select cursor_demo()';
     * rollback;
     * ps: the procedure includes exceptions and don't close the cursor
     * */
    CleanupDfsHandlers(false);

    /*
     * Prior to 8.4 we marked subcommit in clog at this point.	We now only
     * perform that step, if required, as part of the atomic update of the
     * whole transaction tree at top level commit or abort.
     *
     * Post-commit cleanup
     */
    if (TransactionIdIsValid(s->transactionId)) {
        AtSubCommit_childXids();
    }
    AfterTriggerEndSubXact(true);
    AtSubCommit_Portals(s->subTransactionId, s->parent->subTransactionId, s->parent->curTransactionOwner);
    AtEOSubXact_LargeObject(true, s->subTransactionId, s->parent->subTransactionId);
    AtSubCommit_Notify();

    CallSubXactCallbacks(SUBXACT_EVENT_COMMIT_SUB, s->subTransactionId, s->parent->subTransactionId);

    /* Notice GTM commit sub xact */
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
#ifdef ENABLE_DISTRIBUTE_TEST
        if (TEST_STUB(CN_COMMIT_SUBXACT_BEFORE_SEND_GTM_FAILED, twophase_default_error_emit)) {
            ereport(g_instance.distribute_test_param_instance->elevel,
                    (errmsg("SUBXACT_TEST %s: Commit subtransaction %s before notice GTM failed.",
                            g_instance.attr.attr_common.PGXCNodeName, s->name)));
        } else if (TEST_STUB(CN_COMMIT_BEFORE_GTM_FAILED_AND_CANCEL_FLUSH_FAILED, twophase_default_error_emit)) {
            ereport(ERROR, (errmsg("SUBXACT_TEST %s: Commit subtransaction %s before notice GTM failed.",
                                   g_instance.attr.attr_common.PGXCNodeName, s->name)));
        }
#endif
#ifdef ENABLE_DISTRIBUTE_TEST
        if (TEST_STUB(CN_COMMIT_SUBXACT_AFTER_SEND_GTM_FAILED, twophase_default_error_emit)) {
            ereport(g_instance.distribute_test_param_instance->elevel,
                    (errmsg("SUBXACT_TEST  %s: Commit subtransaction %s after notice GTM failed.",
                            g_instance.attr.attr_common.PGXCNodeName, s->name)));
        }
#endif
    }

    ResourceOwnerRelease(s->curTransactionOwner, RESOURCE_RELEASE_BEFORE_LOCKS, true, false);
    AtEOSubXact_RelationCache(true, s->subTransactionId, s->parent->subTransactionId);
    AtEOSubXact_PartitionCache(true, s->subTransactionId, s->parent->subTransactionId);
    AtEOSubXact_BucketCache(true, s->subTransactionId, s->parent->subTransactionId);
    AtEOSubXact_Inval(true);
    AtSubCommit_smgr();

    /* The only lock we actually release here is the subtransaction XID lock. */
    t_thrd.utils_cxt.CurrentResourceOwner = s->curTransactionOwner;
    if (TransactionIdIsValid(s->transactionId)) {
        XactLockTableDelete(s->transactionId);
    }

    /* When commit within nested store procedure, it will create a plan cache.
     * During commit time, need to clean up those plan cahce.
     */
    if (STP_commit) {
        ResourceOwnerDecrementNPlanRefs(t_thrd.utils_cxt.CurrentResourceOwner, true);
        ResourceOwnerDecrementNsnapshots(t_thrd.utils_cxt.CurrentResourceOwner, NULL);
    }

    /* Other locks should get transferred to their parent resource owner. */
    ResourceOwnerRelease(s->curTransactionOwner, RESOURCE_RELEASE_LOCKS, true, false);
    ResourceOwnerRelease(s->curTransactionOwner, RESOURCE_RELEASE_AFTER_LOCKS, true, false);

    /* reserve some related resource once any SPI have referenced it. */
    if (STP_commit) {
        stp_reserve_subxact_resowner(s->curTransactionOwner);
        s->curTransactionOwner = NULL;
    }

    AtEOXact_GUC(true, s->gucNestLevel);
    if(!STP_commit) {
        AtEOSubXact_SPI(true, s->subTransactionId, false, STP_commit);
    }
    AtEOSubXact_on_commit_actions(true, s->subTransactionId, s->parent->subTransactionId);
    if(!STP_commit) {
        AtEOSubXact_Namespace(true, s->subTransactionId, s->parent->subTransactionId);
    }
    AtEOSubXact_Files(true, s->subTransactionId, s->parent->subTransactionId);
    AtEOSubXact_HashTables(true, s->nestingLevel);
    AtEOSubXact_PgStat(true, s->nestingLevel);
    AtSubCommit_Snapshot(s->nestingLevel);

    /*
     * We need to restore the upper transaction's read-only state, in case the
     * upper is read-write while the child is read-only; GUC will incorrectly
     * think it should leave the child state in place.
     */
    u_sess->attr.attr_common.XactReadOnly = s->prevXactReadOnly;

    t_thrd.utils_cxt.CurrentResourceOwner = s->parent->curTransactionOwner;
    t_thrd.utils_cxt.CurTransactionResourceOwner = s->parent->curTransactionOwner;
    if (s->curTransactionOwner != NULL) {
        ResourceOwnerDelete(s->curTransactionOwner);
        s->curTransactionOwner = NULL;
    }

    AtCommit_RelationSync();
    AtSubCommit_Memory();

    s->state = TRANS_DEFAULT;

    PopTransaction();
}

/*
 * Clean up subtransaction's runtime context excluding transaction, xlog module.
 */
void AbortSubTxnRuntimeContext(TransactionState s, bool inPL)
{
    u_sess->exec_cxt.isLockRows = false;

    /*
     * @dfs
     * Clean up DfsReaders that miss invoking deconstructors of dfs::reader::Reader
     * to release the memory not allocated in memory context
     */
    CleanupDfsHandlers(false);

    /* clean up ec connection */
#ifndef ENABLE_LLT
    clean_ec_conn();
    delete_ec_ctrl();
#endif

#ifdef ENABLE_LLVM_COMPILE
    /* reset machine code */
    CodeGenThreadReset();
#endif

    /* Reset the compatible illegal chars import flag */
    u_sess->mb_cxt.insertValuesBind_compatible_illegal_chars = false;

    /* Prevent cancel/die interrupt while cleaning up */
    HOLD_INTERRUPTS();

    /* Make sure we have a valid memory context and resource owner */
    if (!inPL) {
        AtSubAbort_Memory();
        AtSubAbort_ResourceOwner();
    }

    /* abort CU cache inserting before release all LW locks */
    CStoreAbortCU();
    /* abort async io, must before LWlock release */
    AbortAsyncListIO();

    HDFSAbortCacheBlock();

    /*
     * Release any LW locks we might be holding as quickly as possible.
     * (Regular locks, however, must be held till we finish aborting.)
     * Releasing LW locks is critical since we might try to grab them again
     * while cleaning up!
     */
    LWLockReleaseAll();

    /* Clear wait information */
    pgstat_report_waitevent(WAIT_EVENT_END);

    AbortBufferIO();
    UnlockBuffers();

    /* Reset WAL record construction state */
    XLogResetInsertion();

    LockErrorCleanup();

    if (inPL) {
        /* Notice GTM rollback sub xact */
        if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
            /* Cancel query remote nodes and clear connection remaining data for connection reuse */
            SubXactCancel_Remote();
        }

#ifdef ENABLE_MULTIPLE_NODES
        reset_handles_at_abort();
#endif

        /*
         * Reset user ID which might have been changed transiently.  (See notes in
         * AbortTransaction.)
         */
        SetUserIdAndSecContext(s->prevUser, s->prevSecContext);
        u_sess->exec_cxt.is_exec_trigger_func = false;
    }

    RESUME_INTERRUPTS();
}

void AbortSubTransaction(bool STP_rollback)
{
    TransactionState s = CurrentTransactionState;

    t_thrd.xact_cxt.bInAbortTransaction = true;

    /* clean hash table for sub transaction in opfusion */
    if (IS_PGXC_DATANODE) {
        OpFusion::ClearInSubUnexpectSituation(s->curTransactionOwner);
    }

    AbortSubTxnRuntimeContext(s, false);

    /*
     * When copy failed in subTransaction, we should heap sync the relation when
     * AbortSubTransaction, avoid to get the error of invaild page when redo.
     * When heap_sync, maybe we will handle singal, so we can not HOLD_INTERRUPTS().
     * We should not AtAbort_RelationSync() until AbortTransaction because these
     * relations would not considered as valid using snapshotNow snapshot then.
     * Maybe other tables in heap_sync_rel_tab before this subtransaction might be synced
     * in advance, but it doesn't matter as they may sync anyway.
     */
    AtAbort_RelationSync();

    HOLD_INTERRUPTS();

    /* check the current transaction state */
    ShowTransactionState("AbortSubTransaction");

    if (s->state != TRANS_INPROGRESS) {
        ereport(WARNING, (errmsg("AbortSubTransaction while in %s state", TransStateAsString(s->state))));
    }

    s->state = TRANS_ABORT;

    /*
     * Wait data replicate in AbortSubTransaction. As related relation info has been cleared in
     * AbortSubTransaction. AtSubAbort_smgr e.g. In AbortTransaction, related info is missing.
     * So we should Data sync here for sub xacts abort.
     */
    if (!IsInitdb && !g_instance.attr.attr_storage.enable_mix_replication) {
        if (g_instance.attr.attr_storage.max_wal_senders > 0) {
            DataSndWakeup();
        }

        /* wait for the data synchronization */
        WaitForDataSync();
        Assert(BCMArrayIsEmpty());
    }

    /*
     * Reset user ID which might have been changed transiently.  (See notes in
     * AbortTransaction.)
     */
    SetUserIdAndSecContext(s->prevUser, s->prevSecContext);
    u_sess->exec_cxt.is_exec_trigger_func = false;

    /*
     * We can skip all this stuff if the subxact failed before creating a
     * ResourceOwner...
     */
    if (s->curTransactionOwner) {
        AfterTriggerEndSubXact(false);
        AtSubAbort_Portals(s->subTransactionId, s->parent->subTransactionId, s->curTransactionOwner,
                           s->parent->curTransactionOwner, STP_rollback);
        AtEOSubXact_LargeObject(false, s->subTransactionId, s->parent->subTransactionId);
        AtSubAbort_Notify();

        /* Advertise the fact that we aborted in pg_clog. */
        (void)RecordTransactionAbort(true);

        /* Post-abort cleanup */
        if (TransactionIdIsValid(s->transactionId)) {
            AtSubAbort_childXids();
        }

        CallSubXactCallbacks(SUBXACT_EVENT_ABORT_SUB, s->subTransactionId, s->parent->subTransactionId);

        /* Notice GTM rollback sub xact */
        if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
            /* Cancel query remote nodes and clear connection remaining data for connection reuse */
            SubXactCancel_Remote();
        }
#ifdef ENABLE_MULTIPLE_NODES
        reset_handles_at_abort();
#endif

        ResourceOwnerRelease(s->curTransactionOwner, RESOURCE_RELEASE_BEFORE_LOCKS, false, false);
        AtEOSubXact_RelationCache(false, s->subTransactionId, s->parent->subTransactionId);
        AtEOSubXact_PartitionCache(false, s->subTransactionId, s->parent->subTransactionId);
        AtEOSubXact_BucketCache(false, s->subTransactionId, s->parent->subTransactionId);
        AtEOSubXact_Inval(false);
        AtSubAbort_smgr();
        ResourceOwnerRelease(s->curTransactionOwner, RESOURCE_RELEASE_LOCKS, false, false);
        ResourceOwnerRelease(s->curTransactionOwner, RESOURCE_RELEASE_AFTER_LOCKS, false, false);

        AtEOXact_GUC(false, s->gucNestLevel);
        AtEOSubXact_SPI(false, s->subTransactionId, STP_rollback, false);
        AtEOSubXact_on_commit_actions(false, s->subTransactionId, s->parent->subTransactionId);
        if (!STP_rollback) {
            AtEOSubXact_Namespace(false, s->subTransactionId, s->parent->subTransactionId);
        }
        AtEOSubXact_Files(false, s->subTransactionId, s->parent->subTransactionId);
        AtEOSubXact_HashTables(false, s->nestingLevel);
        AtEOSubXact_PgStat(false, s->nestingLevel);
        AtSubAbort_Snapshot(s->nestingLevel);
    }

    /*
     * Restore the upper transaction's read-only state, too.  This should be
     * redundant with GUC's cleanup but we may as well do it for consistency
     * with the commit case.
     */
    u_sess->attr.attr_common.XactReadOnly = s->prevXactReadOnly;

    t_thrd.xact_cxt.bInAbortTransaction = false;
    t_thrd.xact_cxt.ActiveLobRelid = InvalidOid;

    RESUME_INTERRUPTS();
}

/*
 * CleanupSubTransaction
 *
 *	The caller has to make sure to always reassign CurrentTransactionState
 *	if it has a local pointer to it after calling this function.
 */
void CleanupSubTransaction(bool inSTP)
{
    TransactionState s = CurrentTransactionState;

    ShowTransactionState("CleanupSubTransaction");

    if (s->state != TRANS_ABORT) {
        ereport(WARNING, (errmsg("CleanupSubTransaction while in %s state", TransStateAsString(s->state))));
    }

    AtSubCleanup_Portals(s->subTransactionId);

    t_thrd.utils_cxt.CurrentResourceOwner = s->parent->curTransactionOwner;
    t_thrd.utils_cxt.CurTransactionResourceOwner = s->parent->curTransactionOwner;

    if (s->curTransactionOwner != NULL) {
        if (inSTP) {
            /* reserve ResourceOwner in STP running. */
            stp_reserve_subxact_resowner(s->curTransactionOwner);
        } else {
            ResourceOwnerDelete(s->curTransactionOwner);
        }
        s->curTransactionOwner = NULL;
    }

    AtSubCleanup_Memory();

    s->state = TRANS_DEFAULT;

#ifdef ENABLE_MOT
    s->storageEngineType = SE_TYPE_UNSPECIFIED;
#endif

    PopTransaction();
}

/*
 * PushTransaction
 *		Create transaction state stack entry for a subtransaction
 *
 *	The caller has to make sure to always reassign CurrentTransactionState
 *	if it has a local pointer to it after calling this function.
 */
static void PushTransaction(void)
{
#ifdef ENABLE_DISTRIBUTE_TEST
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        if (TEST_STUB(CN_SAVEPOINT_BEFORE_PUSHXACT_FAILED, twophase_default_error_emit)) {
            ereport(g_instance.distribute_test_param_instance->elevel,
                    (errmsg("SUBXACT_TEST  %s: before push transaction failed",
                            g_instance.attr.attr_common.PGXCNodeName)));
        }
    } else {
        if (TEST_STUB(DN_SAVEPOINT_BEFORE_PUSHXACT_FAILED, twophase_default_error_emit)) {
            ereport(g_instance.distribute_test_param_instance->elevel,
                    (errmsg("SUBXACT_TEST  %s: before push transaction failed",
                            g_instance.attr.attr_common.PGXCNodeName)));
        }
    }

    /* white box test start */
    if (execute_whitebox(WHITEBOX_LOC, NULL, WHITEBOX_DEFAULT, 0.001)) {
        ereport(g_instance.distribute_test_param_instance->elevel,
                (errmsg("WHITE_BOX TEST  %s: savepoint before push transaction failed",
                        g_instance.attr.attr_common.PGXCNodeName)));
    }
    /* white box test end */
#endif

    TransactionState p = CurrentTransactionState;
    TransactionState s;

    /* We keep subtransaction state nodes in u_sess->top_transaction_mem_cxt. */
    s = (TransactionState)MemoryContextAllocZero(u_sess->top_transaction_mem_cxt, sizeof(TransactionStateData));

    /* Assign a subtransaction ID */
    t_thrd.xact_cxt.currentSubTransactionId += 1;
    if (t_thrd.xact_cxt.currentSubTransactionId == InvalidSubTransactionId) {
        t_thrd.xact_cxt.currentSubTransactionId -= 1;
        pfree(s);
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                        errmsg("cannot have more than 2^32-1 subtransactions in a transaction")));
    }

    /* We can now stack a minimally valid subtransaction without fear of failure. */
    s->transactionId = InvalidTransactionId; /* until assigned */
    s->txnKey.txnHandle = InvalidTransactionHandle;
    s->txnKey.txnTimeline = InvalidTransactionTimeline;
    s->subTransactionId = t_thrd.xact_cxt.currentSubTransactionId;
    s->parent = p;
    s->nestingLevel = p->nestingLevel + 1;
    s->gucNestLevel = NewGUCNestLevel();
    s->savepointLevel = p->savepointLevel;
    s->state = TRANS_DEFAULT;
    s->blockState = TBLOCK_SUBBEGIN;
    GetUserIdAndSecContext(&s->prevUser, &s->prevSecContext);
    s->prevXactReadOnly = u_sess->attr.attr_common.XactReadOnly;

    CurrentTransactionState = s;

    /*
     * AbortSubTransaction and CleanupSubTransaction have to be able to cope
     * with the subtransaction from here on out; in particular they should not
     * assume that it necessarily has a transaction context, resource owner,
     * or XID.
     */
#ifdef ENABLE_DISTRIBUTE_TEST
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        if (TEST_STUB(CN_SAVEPOINT_AFTER_PUSHXACT_FAILED, twophase_default_error_emit)) {
            ereport(g_instance.distribute_test_param_instance->elevel,
                    (errmsg("SUBXACT_TEST  %s: after push transaction failed",
                            g_instance.attr.attr_common.PGXCNodeName)));
        }
    } else {
        if (TEST_STUB(DN_SAVEPOINT_AFTER_PUSHXACT_FAILED, twophase_default_error_emit)) {
            ereport(g_instance.distribute_test_param_instance->elevel,
                    (errmsg("SUBXACT_TEST  %s: after push transaction failed",
                            g_instance.attr.attr_common.PGXCNodeName)));
        }
    }
    /* white box test start */
    if (execute_whitebox(WHITEBOX_LOC, NULL, WHITEBOX_DEFAULT, 0.001)) {
        ereport(g_instance.distribute_test_param_instance->elevel,
                (errmsg("WHITE_BOX TEST  %s: savepoint after push transaction failed",
                        g_instance.attr.attr_common.PGXCNodeName)));
    }
    /* white box test end */
#endif
}

/*
 * PopTransaction
 *		Pop back to parent transaction state
 *
 *	The caller has to make sure to always reassign CurrentTransactionState
 *	if it has a local pointer to it after calling this function.
 */
static void PopTransaction(void)
{
    TransactionState s = CurrentTransactionState;

    if (s->state != TRANS_DEFAULT) {
        ereport(WARNING, (errmsg("PopTransaction while in %s state", TransStateAsString(s->state))));
    }
    if (s->parent == NULL) {
        ereport(FATAL, (errmsg("PopTransaction with no parent")));
    }

    CurrentTransactionState = s->parent;

    /* Let's just make sure t_thrd.mem_cxt.cur_transaction_mem_cxt is good */
    t_thrd.mem_cxt.cur_transaction_mem_cxt = s->parent->curTransactionContext;
    (void)MemoryContextSwitchTo(t_thrd.mem_cxt.cur_transaction_mem_cxt);

    /* Ditto for ResourceOwner links */
    t_thrd.utils_cxt.CurTransactionResourceOwner = s->parent->curTransactionOwner;
    t_thrd.utils_cxt.CurrentResourceOwner = s->parent->curTransactionOwner;
    t_thrd.xact_cxt.currentSubTransactionId = s->parent->subTransactionId;

    /* Free the old child structure */
    if (s->name) {
        pfree(s->name);
        s->name = NULL;
    }
    pfree(s);
}

/*
 * ShowTransactionState
 *		Debug support
 */
static void ShowTransactionState(const char *str)
{
    /* skip work if message will definitely not be printed */
    if (log_min_messages <= DEBUG3 || client_min_messages <= DEBUG3) {
        ereport(DEBUG3, (errmsg("%s", str)));
        ShowTransactionStateRec(CurrentTransactionState);
    }
}

/*
 * ShowTransactionStateRec
 *		Recursive subroutine for ShowTransactionState
 */
static void ShowTransactionStateRec(TransactionState s)
{
    StringInfoData buf;

    initStringInfo(&buf);

    if (s->nChildXids > 0) {
        int i;

        appendStringInfo(&buf, XID_FMT, s->childXids[0]);
        for (i = 1; i < s->nChildXids; i++)
            appendStringInfo(&buf, " " XID_FMT, s->childXids[i]);
    }

    if (s->parent) {
        ShowTransactionStateRec(s->parent);
    }

    /* use ereport to suppress computation if msg will not be printed */
    ereport(DEBUG3,
            (errmsg_internal("name: %s; blockState: %13s; state: %7s, xid/subid/cid: " XID_FMT "/" XID_FMT
                             "/%u%s, nestlvl: %d, children: %s",
                             PointerIsValid(s->name) ? s->name : "unnamed", BlockStateAsString(s->blockState),
                             TransStateAsString(s->state), s->transactionId, s->subTransactionId,
                             (unsigned int)t_thrd.xact_cxt.currentCommandId,
                             t_thrd.xact_cxt.currentCommandIdUsed ? " (used)" : "", s->nestingLevel, buf.data)));

    pfree(buf.data);
    buf.data = NULL;
}

/*
 * BlockStateAsString
 *		Debug support
 */
static const char *BlockStateAsString(TBlockState blockState)
{
    switch (blockState) {
        case TBLOCK_DEFAULT:
            return "DEFAULT";
        case TBLOCK_STARTED:
            return "STARTED";
        case TBLOCK_BEGIN:
            return "BEGIN";
        case TBLOCK_INPROGRESS:
            return "INPROGRESS";
        case TBLOCK_END:
            return "END";
        case TBLOCK_ABORT:
            return "ABORT";
        case TBLOCK_ABORT_END:
            return "ABORT END";
        case TBLOCK_ABORT_PENDING:
            return "ABORT PEND";
        case TBLOCK_PREPARE:
            return "PREPARE";
        case TBLOCK_SUBBEGIN:
            return "SUB BEGIN";
        case TBLOCK_SUBINPROGRESS:
            return "SUB INPROGRS";
        case TBLOCK_SUBRELEASE:
            return "SUB RELEASE";
        case TBLOCK_SUBCOMMIT:
            return "SUB COMMIT";
        case TBLOCK_SUBABORT:
            return "SUB ABORT";
        case TBLOCK_SUBABORT_END:
            return "SUB ABORT END";
        case TBLOCK_SUBABORT_PENDING:
            return "SUB ABRT PEND";
        case TBLOCK_SUBRESTART:
            return "SUB RESTART";
        case TBLOCK_SUBABORT_RESTART:
            return "SUB AB RESTRT";
        default:
            break;
    }
    return "UNRECOGNIZED";
}

/*
 * TransStateAsString
 *		Debug support
 */
static const char *TransStateAsString(TransState state)
{
    switch (state) {
        case TRANS_DEFAULT:
            return "DEFAULT";
        case TRANS_START:
            return "START";
        case TRANS_INPROGRESS:
            return "INPROGR";
        case TRANS_COMMIT:
            return "COMMIT";
        case TRANS_ABORT:
            return "ABORT";
        case TRANS_PREPARE:
            return "PREPARE";
        default:
            break;
    }
    return "UNRECOGNIZED";
}

/*
 * xactGetCommittedChildren
 *
 * Gets the list of committed children of the current transaction.	The return
 * value is the number of child transactions.  *ptr is set to point to an
 * array of TransactionIds.  The array is allocated in u_sess->top_transaction_mem_cxt;
 * the caller should *not* pfree() it (this is a change from pre-8.4 code!).
 * If there are no subxacts, *ptr is set to NULL.
 */
int xactGetCommittedChildren(TransactionId **ptr)
{
    TransactionState s = CurrentTransactionState;

    if (s->nChildXids == 0) {
        *ptr = NULL;
    } else {
        *ptr = s->childXids;
    }

    return s->nChildXids;
}

HTAB *relfilenode_hashtbl_create()
{
    HASHCTL hashCtrl;
    HTAB *hashtbl = NULL;
    errno_t rc;

    rc = memset_s(&hashCtrl, sizeof(hashCtrl), 0, sizeof(hashCtrl));
    securec_check(rc, "", "");
    hashCtrl.hcxt = (MemoryContext)CurrentMemoryContext;
    hashCtrl.hash = tag_hash;
    hashCtrl.keysize = sizeof(RelFileNode);
    /* keep  entrysize >= keysize, stupid limits */
    hashCtrl.entrysize = sizeof(RelFileNode);

    hashtbl = hash_create("relfilenode_hashtbl_create", 64, &hashCtrl, (HASH_CONTEXT | HASH_FUNCTION | HASH_ELEM));
    return hashtbl;
}

#define REL_NODE_FORMAT(rnode) rnode.spcNode, rnode.dbNode, rnode.relNode, rnode.bucketNode

static bool xact_redo_match_xids(const TransactionId *subXids, uint32 subXidCnt, TransactionId searchedXid)
{
    for (uint32 idx = 0; idx < subXidCnt; idx++) {
        if (subXids[idx] == searchedXid) {
            return true;
        }
    }

    return false;
}

static void xact_redo_forget_alloc_segs(TransactionId xid, TransactionId *subXids, uint32 subXidCnt, XLogRecPtr lsn)
{
    bool isNeedLogRemainSegs = IsNeedLogRemainSegs(lsn);
    if (!isNeedLogRemainSegs) {
        return;
    }

    AutoMutexLock remainSegsLock(&g_instance.xlog_cxt.remain_segs_lock);
    remainSegsLock.lock();
    if (t_thrd.xlog_cxt.remain_segs == NULL) {
        t_thrd.xlog_cxt.remain_segs = redo_create_remain_segs_htbl();
    }

    if (hash_get_num_entries(t_thrd.xlog_cxt.remain_segs) == 0) {
        remainSegsLock.unLock();
        return;
    }

    HASH_SEQ_STATUS status;
    hash_seq_init(&status, t_thrd.xlog_cxt.remain_segs);
    ExtentTag* extentTag = NULL;
    while ((extentTag = (ExtentTag *)hash_seq_search(&status)) != NULL) {
        /* Only alloced segment shold be checked by commit transaction */
        if (extentTag->remainExtentType != ALLOC_SEGMENT) {
            ereport(DEBUG5, (errmodule(MOD_SEGMENT_PAGE),
                    errmsg("Extent [%u, %u, %u, %d] remain type %u isn't alloc_seg.",
                        extentTag->remainExtentHashTag.rnode.spcNode, extentTag->remainExtentHashTag.rnode.dbNode,
                        extentTag->remainExtentHashTag.rnode.relNode, extentTag->remainExtentHashTag.rnode.bucketNode,
                        extentTag->remainExtentType)));
            continue;
        }

        Assert(TransactionIdIsValid(extentTag->xid));

        /* Xid smaller than oldestXid means it must be a invalid transaction */
        if (TransactionIdPrecedes(extentTag->xid, t_thrd.xact_cxt.ShmemVariableCache->oldestXid)) {
            ereport(DEBUG5, (errmodule(MOD_SEGMENT_PAGE),
                    errmsg("Extent [%u, %u, %u, %d] xid %lu, remain type %u is smaller than %lu.",
                        extentTag->remainExtentHashTag.rnode.spcNode, extentTag->remainExtentHashTag.rnode.dbNode,
                        extentTag->remainExtentHashTag.rnode.relNode, extentTag->remainExtentHashTag.rnode.bucketNode,
                        extentTag->xid, extentTag->remainExtentType,
                        t_thrd.xact_cxt.ShmemVariableCache->oldestXid)));
            continue;
        }
        
        if (extentTag->xid != xid && !xact_redo_match_xids(subXids, subXidCnt, xid)) {
            ereport(DEBUG5, (errmodule(MOD_SEGMENT_PAGE),
                    errmsg("Extent [%u, %u, %u, %d] xid %lu, remain type %u is not equal xid %lu.",
                        extentTag->remainExtentHashTag.rnode.spcNode, extentTag->remainExtentHashTag.rnode.dbNode,
                        extentTag->remainExtentHashTag.rnode.relNode, extentTag->remainExtentHashTag.rnode.bucketNode,
                        extentTag->xid, extentTag->remainExtentType, xid)));
            continue;
        }

        if (hash_search(t_thrd.xlog_cxt.remain_segs, &extentTag->remainExtentHashTag, HASH_REMOVE, NULL) == NULL) {
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("hash table corrupted, cannot remove remain segment [%u, %u, %u, %d].",
                       extentTag->remainExtentHashTag.rnode.spcNode, extentTag->remainExtentHashTag.rnode.dbNode,
                       extentTag->remainExtentHashTag.rnode.relNode,
                       extentTag->remainExtentHashTag.rnode.bucketNode)));
        } else {
            ereport(DEBUG5, (errmodule(MOD_SEGMENT_PAGE),
                    errmsg("Extent [%u, %u, %u, %d] xid %lu, remain type %u is removed.",
                        extentTag->remainExtentHashTag.rnode.spcNode, extentTag->remainExtentHashTag.rnode.dbNode,
                        extentTag->remainExtentHashTag.rnode.relNode, extentTag->remainExtentHashTag.rnode.bucketNode,
                        extentTag->xid, extentTag->remainExtentType)));
        }
    }
    
    remainSegsLock.unLock();
}

static void xact_redo_log_drop_segs(_in_ ColFileNodeRel *xnodes, _in_ int nrels, XLogRecPtr lsn)
{
    bool isNeedLogRemainSegs = IsNeedLogRemainSegs(lsn);
    if (!isNeedLogRemainSegs) {
        return;
    }

    AutoMutexLock remainSegsLock(&g_instance.xlog_cxt.remain_segs_lock);
    remainSegsLock.lock();
    if (t_thrd.xlog_cxt.remain_segs == NULL && nrels > 0) {
        t_thrd.xlog_cxt.remain_segs = redo_create_remain_segs_htbl();
    }

    for (int i = 0; i < nrels; ++i) {
        ColFileNode colFileNode;
        ColFileNodeRel *colFileNodeRel = xnodes + i;

        ColFileNodeCopy(&colFileNode, colFileNodeRel);

        if (!IsValidColForkNum(colFileNode.forknum) && IsSegmentFileNode(colFileNode.filenode)) {
            RemainExtentHashTag remainExtentHashTag;
            remainExtentHashTag.rnode = colFileNode.filenode;
            remainExtentHashTag.extentType = EXTENT_1;

            bool found = false;
            ExtentTag* extentTag = (ExtentTag *)hash_search(t_thrd.xlog_cxt.remain_segs, (void *)&remainExtentHashTag,
                                                            HASH_ENTER, &found);
            if (found) {
                ereport(WARNING, (errmsg("Segment [%u, %u, %u, %d] should not be repeatedly dropped "
                        "before really freed, its xid %lu, remainExtentType %u.",
                        REL_NODE_FORMAT(remainExtentHashTag.rnode), extentTag->xid, extentTag->remainExtentType)));
            } else {
                extentTag->remainExtentType = DROP_SEGMENT;
                extentTag->lsn = lsn;

                extentTag->forkNum = InvalidForkNumber;
                extentTag->xid = InvalidTransactionId;
                ereport(DEBUG5, (errmodule(MOD_SEGMENT_PAGE), errmsg("Segment [%u, %u, %u, %d] is marked dropped.",
                        REL_NODE_FORMAT(remainExtentHashTag.rnode))));
            }
        }
    }
    remainSegsLock.unLock();
}

/* This function cannot be used to process segment tables. */
void push_unlink_rel_to_hashtbl(ColFileNodeRel *xnodes, int nrels)
{
    HTAB *relfilenode_hashtbl = g_instance.bgwriter_cxt.unlink_rel_hashtbl;
    uint del_rel_num = 0;

    if (nrels == 0) {
        return;
    }

    LWLockAcquire(g_instance.bgwriter_cxt.rel_hashtbl_lock, LW_EXCLUSIVE);
    for (int i = 0; i < nrels; i++) {
        ColFileNode colFileNode;
        ColFileNodeRel *colFileNodeRel = xnodes + i;
        bool found = false;
        DelFileTag *entry = NULL;

        ColFileNodeCopy(&colFileNode, colFileNodeRel);
        if (IS_COMPRESS_DELETE_FORK(colFileNode.forknum)) {
            SET_OPT_BY_NEGATIVE_FORK(colFileNode.filenode, colFileNode.forknum);
            colFileNode.forknum = MAIN_FORKNUM;
        }
        if (!IsValidColForkNum(colFileNode.forknum) && !IsSegmentFileNode(colFileNode.filenode)) {
            entry = (DelFileTag*)hash_search(relfilenode_hashtbl, &(colFileNode.filenode), HASH_ENTER, &found);
            if (!found) {
                entry->rnode.spcNode = colFileNode.filenode.spcNode;
                entry->rnode.dbNode = colFileNode.filenode.dbNode;
                entry->rnode.relNode = colFileNode.filenode.relNode;
                entry->rnode.bucketNode = colFileNode.filenode.bucketNode;
                entry->rnode.opt = colFileNode.filenode.opt;
                entry->maxSegNo = -1;
                del_rel_num++;
            }
            BatchClearBadBlock(colFileNode.filenode, colFileNode.forknum, 0);
        }
    }
    LWLockRelease(g_instance.bgwriter_cxt.rel_hashtbl_lock);

    if (del_rel_num > 0 && g_instance.bgwriter_cxt.invalid_buf_proc_latch != NULL) {
        SetLatch(g_instance.bgwriter_cxt.invalid_buf_proc_latch);
    }
    return;
}



/*
 *	XLOG support routines
 */
static void unlink_relfiles(_in_ ColFileNodeRel *xnodes, _in_ int nrels)
{
    ColMainFileNodesCreate();

    if (relsContainsSegmentTable(xnodes, nrels)) {
        if (IS_DEL_RELS_OVER_HASH_THRESHOLD(nrels)) {
            DropBufferForDelRelsinXlogUsingHash(xnodes, nrels);
        } else {
            DropBufferForDelRelinXlogUsingScan(xnodes, nrels);
        }
    }
    push_unlink_rel_to_hashtbl(xnodes, nrels);

    for (int i = 0; i < nrels; ++i) {
        ColFileNode colFileNode;
        ColFileNodeRel *colFileNodeRel = xnodes + i;

        ColFileNodeCopy(&colFileNode, colFileNodeRel);
        if (IS_COMPRESS_DELETE_FORK(colFileNode.forknum)) {
            SET_OPT_BY_NEGATIVE_FORK(colFileNode.filenode, colFileNode.forknum);
            colFileNode.forknum = MAIN_FORKNUM;
        }
        if (!IsValidColForkNum(colFileNode.forknum)) {
            RelFileNode relFileNode = colFileNode.filenode;
            ForkNumber fork;
            int ifork;

            for (ifork = 0; (ForkNumber)ifork <= MAX_FORKNUM; ifork++) {
                fork = (ForkNumber)ifork;
                XLogDropRelation(relFileNode, fork);
            }

            LockRelFileNode(relFileNode, AccessExclusiveLock);

            /* drop hdfs directories just on CN during redo. */
            if (IS_PGXC_COORDINATOR && IsValidPaxDfsForkNum(colFileNode.forknum)) {
                ClearDfsStorage(&colFileNode, 1, true, true);
            }

            /*
             * For truncate dfs table:
             * -During redo commit, if we found mapper file and dfs file list,
             * we will delete the corresponding files in dfs file list, then drop
             * the mapper and dfs file list.
             * If we can not find the mapper or dfs file list, we will not issue error.
             * -During redo abort, nrels is 0, we will not enter here
             */
            if (!IS_PGXC_COORDINATOR && IsTruncateDfsForkNum(colFileNode.forknum)) {
                ClearDfsDirectory(&colFileNode, true);
                DropMapperFile(colFileNode.filenode);
                DropDfsFilelist(colFileNode.filenode);
            }

            SMgrRelation srel = smgropen(relFileNode, InvalidBackendId);
            smgrdounlink(srel, true);
            smgrclose(srel);

            UnlockRelFileNode(relFileNode, AccessExclusiveLock);

            /*
             * After files are deleted, append this filenode into Column Heap Main file list,
             * so that we know all shared buffers of column relation (including BCM) has been
             * invalided.
             */
            ColMainFileNodesAppend(&relFileNode, InvalidBackendId);

            /*
             * do nothing for row table, or invalid space cache for column table.
             */
            CStore::InvalidRelSpaceCache(&relFileNode);
        } else {
            Assert(IsValidColForkNum(colFileNode.forknum));
            RelFileNode relFileNode = colFileNode.filenode;

            LockRelFileNode(relFileNode, AccessExclusiveLock);
            /* Column relation not support hash bucket now */
            ColumnRelationDoDeleteFiles(&colFileNode.filenode, colFileNode.forknum, InvalidBackendId,
                                        colFileNode.ownerid);
            UnlockRelFileNode(relFileNode, AccessExclusiveLock);
        }
    }
    ColMainFileNodesDestroy();
}

/*
 * Before 9.0 this was a fairly short function, but now it performs many
 * actions for which the order of execution is critical.
 */
static void xact_redo_commit_internal(TransactionId xid, XLogRecPtr lsn, TransactionId* sub_xids, int nsubxacts,
                                      SharedInvalidationMessage* inval_msgs, int nmsgs, ColFileNodeRel* xnodes,
                                      int nrels, int nlibrary, Oid dbId, Oid tsId, uint32 xinfo,
                                      uint64 csn, TransactionId newStandbyXmin, RepOriginId originId)
{
    TransactionId max_xid;
    XLogRecPtr globalDelayDDLLSN;

    max_xid = TransactionIdLatest(xid, nsubxacts, sub_xids);

    ExtendCsnlogForSubtrans(xid, nsubxacts, sub_xids);

    /*
     * Make sure nextXid is beyond any XID mentioned in the record.
     *
     * We don't expect anyone else to modify nextXid, hence we don't need to
     * hold a lock while checking this. We still acquire the lock to modify
     * it, though.
     */
    if (TransactionIdFollowsOrEquals(max_xid, t_thrd.xact_cxt.ShmemVariableCache->nextXid)) {
        (void)LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
        if (TransactionIdFollowsOrEquals(max_xid, t_thrd.xact_cxt.ShmemVariableCache->nextXid)) {
            t_thrd.xact_cxt.ShmemVariableCache->nextXid = max_xid;
            TransactionIdAdvance(t_thrd.xact_cxt.ShmemVariableCache->nextXid);
        }
        LWLockRelease(XidGenLock);
    }

    if (t_thrd.xlog_cxt.standbyState == STANDBY_DISABLED) {
#ifdef ENABLE_MOT
        /*
         * Report committed transaction to MOT Engine.
         * If (XactMOTEngineUsed(xinfo)) - This is an optimization to avoid calling
         * MOT redo commit callbacks in case of commit does not have MOT records.
         * It is disabled for the time being since data used to identify storage
         * engine type is cleared in 2phase commit prepare phase.
         */
        CallRedoCommitCallback(xid);
#endif

        /*
         * Mark the transaction committed in pg_xact. We don't bother updating
         * pg_csnlog during replay.
         */
        CLogSetTreeStatus(xid, nsubxacts, sub_xids, CLOG_XID_STATUS_COMMITTED, InvalidXLogRecPtr);

        /* Update csn log. */
        if (csn >= COMMITSEQNO_FROZEN) {
            Assert(COMMITSEQNO_IS_COMMITTED(csn));
            CSNLogSetCommitSeqNo(xid, nsubxacts, sub_xids, csn);

            if (t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo < csn + 1) {
                t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo = csn + 1;
            }
        }
#ifdef ENABLE_MULTIPLE_NODES
        else if (csn == COMMITSEQNO_INPROGRESS) {
            CSNLogSetCommitSeqNo(xid, nsubxacts, sub_xids, COMMITSEQNO_FROZEN);
        }
#else
        else {
            ereport(
                PANIC, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                    errmsg("xact_redo_commit_internal: unknown csn state %lu", (uint64)csn)));
        }
        if (EnableGlobalSysCache()) {
            ProcessCommittedInvalidationMessages(inval_msgs, nmsgs, XactCompletionRelcacheInitFileInval(xinfo),
                dbId, tsId);
        }
#endif
    } else {
        CSNLogRecordAssignedTransactionId(max_xid);

#ifdef ENABLE_MOT
        /*
         * Report committed transaction to MOT Engine.
         * If (XactMOTEngineUsed(xinfo)) - This is an optimization to avoid calling
         * MOT redo commit callbacks in case of commit does not have MOT records.
         * It is disabled for the time being since data used to identify storage
         * engine type is cleared in 2phase commit prepare phase.
         */
        CallRedoCommitCallback(xid);
#endif

        /*
         * Mark the transaction committed in pg_clog. We use async commit
         * protocol during recovery to provide information on database
         * consistency for when users try to set hint bits. It is important
         * that we do not set hint bits until the minRecoveryPoint is past
         * this commit record. This ensures that if we crash we don't see hint
         * bits set on changes made by transactions that haven't yet
         * recovered. It's unlikely but it's good to be safe.
         */
        /* update the Clog */
        CLogSetTreeStatus(xid, nsubxacts, sub_xids, CLOG_XID_STATUS_COMMITTED, lsn);

        /* Update csn log. */
        if (csn >= COMMITSEQNO_FROZEN) {
            Assert(COMMITSEQNO_IS_COMMITTED(csn));
            CSNLogSetCommitSeqNo(xid, nsubxacts, sub_xids, csn);

            if (t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo < csn + 1)
                t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo = csn + 1;
        }
#ifdef ENABLE_MULTIPLE_NODES
        else if (csn == COMMITSEQNO_INPROGRESS) {
            CSNLogSetCommitSeqNo(xid, nsubxacts, sub_xids, COMMITSEQNO_FROZEN);
        }
#else
        else {
            ereport(
                PANIC, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                    errmsg("xact_redo_commit_internal: unknown csn state %lu", (uint64)csn)));
        }

        if (TransactionIdPrecedes(t_thrd.xact_cxt.ShmemVariableCache->standbyXmin, newStandbyXmin)) {
            t_thrd.xact_cxt.ShmemVariableCache->standbyXmin = newStandbyXmin;
        }

#endif

        /* As in ProcArrayEndTransaction, advance latestCompletedXid */
        if (TransactionIdPrecedes(t_thrd.xact_cxt.ShmemVariableCache->latestCompletedXid, max_xid)) {
            t_thrd.xact_cxt.ShmemVariableCache->latestCompletedXid = max_xid;
        }

        /*
         * Send any cache invalidations attached to the commit. We must
         * maintain the same order of invalidation then release locks as
         * occurs in CommitTransaction().
         */
        ProcessCommittedInvalidationMessages(inval_msgs, nmsgs, XactCompletionRelcacheInitFileInval(xinfo), dbId, tsId);

        /*
         * Release locks, if any. We do this for both two phase and normal one
         * phase transactions. In effect we are ignoring the prepare phase and
         * just going straight to lock release. At commit we release all locks
         * via their top-level xid only, so no need to provide subxact list,
         * which will save time when replaying commits.
         */
        StandbyReleaseLockTree(xid, 0, NULL);
    }

    xact_redo_forget_alloc_segs(xid, sub_xids, nsubxacts, lsn);

    /* Make sure files supposed to be dropped are dropped */
    if (nrels > 0) {
        /*
         * First update minimum recovery point to cover this WAL record. Once
         * a relation is deleted, there's no going back. The buffer manager
         * enforces the WAL-first rule for normal updates to relation files,
         * so that the minimum recovery point is always updated before the
         * corresponding change in the data file is flushed to disk, but we
         * have to do the same here since we're bypassing the buffer manager.
         *
         * Doing this before deleting the files means that if a deletion fails
         * for some reason, you cannot start up the system even after restart,
         * until you fix the underlying situation so that the deletion will
         * succeed. Alternatively, we could update the minimum recovery point
         * after deletion, but that would leave a small window where the
         * WAL-first rule would be violated.
         */
        UpdateMinRecoveryPoint(lsn, false);

        globalDelayDDLLSN = GetDDLDelayStartPtr();

        t_thrd.xact_cxt.xactDelayDDL =
            ((!XLogRecPtrIsInvalid(globalDelayDDLLSN) && XLByteLT(globalDelayDDLLSN, lsn)) ? true : false);

        unlink_relfiles(xnodes, nrels);
        xact_redo_log_drop_segs(xnodes, nrels, lsn);
    }

    /* remove library file */
    if (nlibrary > 0) {
#ifdef ENABLE_MULTIPLE_NODES
        char* filename = (char*)xnodes + (nrels * sizeof(ColFileNodeRel)) + (nsubxacts * sizeof(TransactionId)) +
                         (nmsgs * sizeof(SharedInvalidationMessage));
#else
        char* filename = (char*)xnodes + (nrels * sizeof(ColFileNodeRel)) + ((nsubxacts + 1) * sizeof(TransactionId)) +
                         (nmsgs * sizeof(SharedInvalidationMessage));
#endif
        parseAndRemoveLibrary(filename, nlibrary);
    }

    if (xinfo & XACT_HAS_ORIGIN) {
        xl_xact_origin *origin = (xl_xact_origin *)GetRepOriginPtr((char*)xnodes, xinfo, nsubxacts,
            nmsgs, nrels, nlibrary);
        /* recover apply progress */
        replorigin_advance(originId, origin->origin_lsn, lsn, false, false);
    }

    /*
     * We issue an XLogFlush() for the same reason we emit ForceSyncCommit()
     * in normal operation. For example, in CREATE DATABASE, we copy all files
     * from the template database, and then commit the transaction. If we
     * crash after all the files have been copied but before the commit, you
     * have files in the data directory without an entry in pg_database. To
     * minimize the window
     * for that, we use ForceSyncCommit() to rush the commit record to disk as
     * quick as possible. We have the same window during recovery, and forcing
     * an XLogFlush() (which updates minRecoveryPoint during recovery) helps
     * to reduce that problem window, for any user that requested ForceSyncCommit().
     */
    if (XactCompletionForceSyncCommit(xinfo)) {
        UpdateMinRecoveryPoint(lsn, false);
    }

    if (RemoveCommittedCsnInfo(xid)) {
        XactLockTableDelete(xid);
    }
}

/*
 * Utility function to call xact_redo_commit_internal after breaking down xlrec
 */
static void xact_redo_commit(xl_xact_commit *xlrec, TransactionId xid, XLogRecPtr lsn, RepOriginId originId)
{
    TransactionId* subxacts = NULL;
    SharedInvalidationMessage* inval_msgs = NULL;
    TransactionId newStandbyXmin = InvalidTransactionId;

    Assert(TransactionIdIsValid(xid));

    /* subxid array follows relfilenodes */
    subxacts = (TransactionId *)&(xlrec->xnodes[xlrec->nrels]);

    /* invalidation messages array follows subxids */
    inval_msgs = (SharedInvalidationMessage*)&(subxacts[xlrec->nsubxacts]);

#ifndef ENABLE_MULTIPLE_NODES
    /* recent_xmin follows inval_msgs */
    newStandbyXmin = *((TransactionId *)&(inval_msgs[xlrec->nmsgs]));
#endif
    xact_redo_commit_internal(xid,
        lsn,
        subxacts,
        xlrec->nsubxacts,
        inval_msgs,
        xlrec->nmsgs,
        xlrec->xnodes,
        xlrec->nrels,
        xlrec->nlibrary,
        xlrec->dbId,
        xlrec->tsId,
        xlrec->xinfo,
        xlrec->csn,
        newStandbyXmin,
        originId);
}

/*
 * Utility function to call xact_redo_commit_internal  for compact form of message.
 */
static void xact_redo_commit_compact(xl_xact_commit_compact *xlrec, TransactionId xid, XLogRecPtr lsn)
{
    /* recent_xmin follows subxacts for hot standby */
    TransactionId globalXmin = InvalidTransactionId;

#ifndef ENABLE_MULTIPLE_NODES
    Assert(TransactionIdIsValid(xid));

    globalXmin = *((TransactionId *)&(xlrec->subxacts[xlrec->nsubxacts]));
#endif

    xact_redo_commit_internal(xid,
        lsn,
        xlrec->subxacts,
        xlrec->nsubxacts,
        NULL,
        0, /* inval msgs */
        NULL,
        0, /* relfilenodes */
        0,
        InvalidOid,  /* dbId */
        InvalidOid,  /* tsId */
        0,           /* xinfo */
        xlrec->csn, /* csn */
        globalXmin, /* recent_xmin */
        InvalidRepOriginId);
}

/*
 * Be careful with the order of execution, as with xact_redo_commit().
 * The two functions are similar but differ in key places.
 *
 * Note also that an abort can be for a subtransaction and its children,
 * not just for a top level abort. That means we have to consider
 * topxid != xid, whereas in commit we would find topxid == xid always
 * because subtransaction commit is never WAL logged.
 */
static void xact_redo_abort(xl_xact_abort *xlrec, TransactionId xid, XLogRecPtr lsn, bool abortXlogNewVersion)
{
    TransactionId *sub_xids = NULL;
    TransactionId max_xid;
    XLogRecPtr globalDelayDDLLSN;

    Assert(TransactionIdIsValid(xid));

    sub_xids = (TransactionId *)&(xlrec->xnodes[xlrec->nrels]);

    max_xid = TransactionIdLatest(xid, xlrec->nsubxacts, sub_xids);

    ExtendCsnlogForSubtrans(xid, xlrec->nsubxacts, sub_xids);

    /*
     * Make sure nextXid is beyond any XID mentioned in the record.
     *
     * We don't expect anyone else to modify nextXid, hence we don't need to
     * hold a lock while checking this. We still acquire the lock to modify
     * it, though.
     */
    if (TransactionIdFollowsOrEquals(max_xid, t_thrd.xact_cxt.ShmemVariableCache->nextXid)) {
        (void)LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
        if (TransactionIdFollowsOrEquals(max_xid, t_thrd.xact_cxt.ShmemVariableCache->nextXid)) {
            t_thrd.xact_cxt.ShmemVariableCache->nextXid = max_xid;
            TransactionIdAdvance(t_thrd.xact_cxt.ShmemVariableCache->nextXid);
        }
        LWLockRelease(XidGenLock);
    }

    if (t_thrd.xlog_cxt.standbyState == STANDBY_DISABLED) {
        /* Mark the transaction committed in pg_xact. */
        TransactionIdAbortTree(xid, xlrec->nsubxacts, sub_xids);
    } else {
        CSNLogRecordAssignedTransactionId(max_xid);

        /* Mark the transaction aborted in pg_clog, no need for async stuff */
        TransactionIdAbortTree(xid, xlrec->nsubxacts, sub_xids);

        /* As in ProcArrayEndTransaction, advance latestCompletedXid */
        if (TransactionIdPrecedes(t_thrd.xact_cxt.ShmemVariableCache->latestCompletedXid, max_xid))
            t_thrd.xact_cxt.ShmemVariableCache->latestCompletedXid = max_xid;

        /* Release locks, if any. There are no invalidations to send. */
        StandbyReleaseLockTree(xid, xlrec->nsubxacts, sub_xids);
    }

    xact_redo_forget_alloc_segs(xid, sub_xids, xlrec->nsubxacts, lsn);

    if (xlrec->nrels > 0) {
        globalDelayDDLLSN = GetDDLDelayStartPtr();
        if (!XLogRecPtrIsInvalid(globalDelayDDLLSN) && XLByteLT(globalDelayDDLLSN, lsn))
            t_thrd.xact_cxt.xactDelayDDL = true;
        else
            t_thrd.xact_cxt.xactDelayDDL = false;
        UpdateMinRecoveryPoint(lsn, false);
        /* Make sure files supposed to be dropped are dropped */
        unlink_relfiles(xlrec->xnodes, xlrec->nrels);
        xact_redo_log_drop_segs(xlrec->xnodes, xlrec->nrels, lsn);
    }

    if (xlrec->nlibrary) {
        /* ship rel and TransactionId */
        char *filename = NULL;
        filename = (char *)xlrec->xnodes + ((unsigned)xlrec->nrels * sizeof(ColFileNodeRel)) +
                   ((unsigned)xlrec->nsubxacts * sizeof(TransactionId));
        if (abortXlogNewVersion) {
            filename += sizeof(TransactionId);
        }
        parseAndRemoveLibrary(filename, xlrec->nlibrary);
    }

    if (RemoveCommittedCsnInfo(xid)) {
        XactLockTableDelete(xid);
    }
}

static void xact_redo_prepare(TransactionId xid)
{
    if (TransactionIdFollowsOrEquals(xid, t_thrd.xact_cxt.ShmemVariableCache->nextXid)) {
        (void)LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
        if (TransactionIdFollowsOrEquals(xid, t_thrd.xact_cxt.ShmemVariableCache->nextXid)) {
            t_thrd.xact_cxt.ShmemVariableCache->nextXid = xid;
            TransactionIdAdvance(t_thrd.xact_cxt.ShmemVariableCache->nextXid);
        }
        LWLockRelease(XidGenLock);
    }
}

void xact_redo(XLogReaderState *record)
{
    XLogRecPtr lsn = record->EndRecPtr;
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    bool abortXlogNewVersion = (info == XLOG_XACT_ABORT_WITH_XID);
    /* Backup blocks are not used in xact records */
    Assert(!XLogRecHasAnyBlockRefs(record));

    if (info == XLOG_XACT_COMMIT_COMPACT) {
        xl_xact_commit_compact *xlrec = (xl_xact_commit_compact *)XLogRecGetData(record);

        xact_redo_commit_compact(xlrec, XLogRecGetXid(record), lsn);
    } else if (info == XLOG_XACT_COMMIT) {
        xl_xact_commit *xlrec = (xl_xact_commit *)XLogRecGetData(record);
        xact_redo_commit(xlrec, XLogRecGetXid(record), lsn, XLogRecGetOrigin(record));
    } else if (info == XLOG_XACT_ABORT || info == XLOG_XACT_ABORT_WITH_XID) {
        xl_xact_abort *xlrec = (xl_xact_abort *)XLogRecGetData(record);

        xact_redo_abort(xlrec, XLogRecGetXid(record), lsn, abortXlogNewVersion);
    } else if (info == XLOG_XACT_PREPARE) {
        TransactionId xid = XLogRecGetXid(record);
        Assert(TransactionIdIsValid(xid));
        xact_redo_prepare(xid);

        /*
         * Store xid and start/end pointers of the WAL record in
         * TwoPhaseState gxact entry.
         */
        (void)TWOPAHSE_LWLOCK_ACQUIRE(xid, LW_EXCLUSIVE);
        PrepareRedoAdd(XLogRecGetData(record), record->ReadRecPtr, record->EndRecPtr);

        if (IS_DISASTER_RECOVER_MODE) {
            TwoPhaseFileHeader *hdr = (TwoPhaseFileHeader *) XLogRecGetData(record);
            XactLockTableInsert(hdr->xid);
        }

        TWOPAHSE_LWLOCK_RELEASE(xid);

        /* Update prepare trx's csn to commit-in-progress. */
        RecoverPrepareTransactionCSNLog(XLogRecGetData(record));
    } else if (info == XLOG_XACT_COMMIT_PREPARED) {
        xl_xact_commit_prepared *xlrec = (xl_xact_commit_prepared *)XLogRecGetData(record);

        xact_redo_commit(&xlrec->crec, xlrec->xid, lsn, XLogRecGetOrigin(record));

        /* Delete TwoPhaseState gxact entry and/or 2PC file. */
        (void)TWOPAHSE_LWLOCK_ACQUIRE(xlrec->xid, LW_EXCLUSIVE);
        PrepareRedoRemove(xlrec->xid, false);
        if (IS_DISASTER_RECOVER_MODE) {
            XactLockTableDelete(xlrec->xid);
        }
        TWOPAHSE_LWLOCK_RELEASE(xlrec->xid);
    } else if (info == XLOG_XACT_ABORT_PREPARED) {
        xl_xact_abort_prepared *xlrec = (xl_xact_abort_prepared *)XLogRecGetData(record);

        xact_redo_abort(&xlrec->arec, xlrec->xid, lsn, abortXlogNewVersion);

        /* Delete TwoPhaseState gxact entry and/or 2PC file. */
        (void)TWOPAHSE_LWLOCK_ACQUIRE(xlrec->xid, LW_EXCLUSIVE);
        PrepareRedoRemove(xlrec->xid, false);
        if (IS_DISASTER_RECOVER_MODE) {
            XactLockTableDelete(xlrec->xid);
        }
        TWOPAHSE_LWLOCK_RELEASE(xlrec->xid);
    } else if (info == XLOG_XACT_ASSIGNMENT) {
    } else {
        ereport(PANIC,
                (errcode(ERRCODE_INVALID_TRANSACTION_STATE), errmsg("xact_redo: unknown op code %u", (uint32)info)));
    }
}

void XactGetRelFiles(XLogReaderState *record, ColFileNodeRel **xnodesPtr, int *nrelsPtr)
{
    Assert(XLogRecGetRmid(record) == RM_XACT_ID);

    uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));
    xl_xact_commit *commit = NULL;
    xl_xact_abort *abort = NULL;

    switch (info) {
        case XLOG_XACT_COMMIT_COMPACT:
        case XLOG_XACT_PREPARE:
        case XLOG_XACT_ASSIGNMENT:
            break;
        case XLOG_XACT_COMMIT:
            commit = (xl_xact_commit *)XLogRecGetData(record);
            break;
        case XLOG_XACT_ABORT_WITH_XID:
        case XLOG_XACT_ABORT:
            abort = (xl_xact_abort *)XLogRecGetData(record);
            break;
        case XLOG_XACT_COMMIT_PREPARED:
            commit = &(((xl_xact_commit_prepared *)XLogRecGetData(record))->crec);
            break;
        case XLOG_XACT_ABORT_PREPARED:
            abort = &(((xl_xact_abort_prepared *)XLogRecGetData(record))->arec);
            break;
        default:
            ereport(PANIC, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                            errmsg("xactWillRemoveRelFiles: unknown op code %u", (uint32)info)));
    }

    if (commit != NULL) {
        *xnodesPtr = commit->xnodes;
        *nrelsPtr = commit->nrels;
    } else if (abort != NULL) {
        *xnodesPtr = abort->xnodes;
        *nrelsPtr = abort->nrels;
    } else {
        *xnodesPtr = NULL;
        *nrelsPtr = 0;
    }

    return;
}

bool XactWillRemoveRelFiles(XLogReaderState *record)
{
    /*
     * Relation files under tablespace folders are removed only from
     * applying transaction log record.
     */
    int nrels = 0;
    ColFileNodeRel *xnodes = NULL;

    if (XLogRecGetRmid(record) != RM_XACT_ID) {
        return false;
    }

    XactGetRelFiles(record, &xnodes, &nrels);

    return (nrels > 0);
}

bool xactWillRemoveRelFiles(XLogReaderState *record)
{
    int nrels = 0;
    ColFileNodeRel *xnodes = NULL;

    Assert(XLogRecGetRmid(record) == RM_XACT_ID);
    XactGetRelFiles(record, &xnodes, &nrels);
    return nrels > 0;
}

void xactApplyXLogDropRelation(XLogReaderState *record)
{
    int nrels = 0;
    ColFileNodeRel *xnodes = NULL;

    XactGetRelFiles(record, &xnodes, &nrels);
    for (int i = 0; i < nrels; i++) {
        RelFileNodeBackend rbnode;
        ColFileNode node;
        ColFileNodeRel *nodeRel = xnodes + i;

        ColFileNodeCopy(&node, nodeRel);
        if (IS_COMPRESS_DELETE_FORK(node.forknum)) {
            SET_OPT_BY_NEGATIVE_FORK(node.filenode, node.forknum);
            node.forknum = MAIN_FORKNUM;
        }
        if (!IsValidColForkNum(node.forknum)) {
            for (int fork = 0; fork <= MAX_FORKNUM; fork++)
                XLogDropRelation(node.filenode, fork);

            /* close the relnode */
            rbnode.node = node.filenode;
            rbnode.backend = InvalidBackendId;
            smgrclosenode(rbnode);
        }
    }
}

#ifdef PGXC
/* Remember that the local node has done some write activity */
void RegisterTransactionLocalNode(bool write)
{
    if (write) {
        t_thrd.xact_cxt.XactWriteLocalNode = true;
        t_thrd.xact_cxt.XactReadLocalNode = false;
    } else
        t_thrd.xact_cxt.XactReadLocalNode = true;
}

/* Forget about the local node's involvement in the transaction */
void ForgetTransactionLocalNode(void)
{
    t_thrd.xact_cxt.XactReadLocalNode = t_thrd.xact_cxt.XactWriteLocalNode = false;
}

/* Check if the given xid is form implicit 2PC */
bool IsXidImplicit(const char *xid)
{
#define implicit2PC_head "_$XC$"
    const size_t implicit2PC_head_len = strlen(implicit2PC_head);

    if (strncmp(xid, implicit2PC_head, implicit2PC_head_len))
        return false;
    return true;
}

/* Save a received command ID from another node for future use. */
void SaveReceivedCommandId(CommandId cid)
{
    /* Set the new command ID */
    SetReceivedCommandId(cid);

    /*
     * Change command ID information status to report any changes in remote ID
     * for a remote node. A new command ID has also been received.
     */
    if (IsConnFromCoord()) {
        SetSendCommandId(true);
        t_thrd.xact_cxt.isCommandIdReceived = true;
    }
}

/* Set the command Id received from other nodes */
void SetReceivedCommandId(CommandId cid)
{
    t_thrd.xact_cxt.receivedCommandId = cid;
}

/* Get the command id received from other nodes */
CommandId GetReceivedCommandId(void)
{
    return t_thrd.xact_cxt.receivedCommandId;
}

/*
 * ReportCommandIdChange
 * ReportCommandIdChange reports a change in current command id at remote node
 * to the Coordinator. This is required because a remote node can increment command
 * Id in case of triggers or constraints.
 */
void ReportCommandIdChange(CommandId cid)
{
    StringInfoData buf;

    /* Send command Id change to Coordinator */
    pq_beginmessage(&buf, 'M');
    pq_sendint32(&buf, cid);
    pq_endmessage(&buf);
}

void ReportTopXid(TransactionId local_top_xid)
{
    if ((t_thrd.proc->workingVersionNum <= GTM_OLD_VERSION_NUM)) {
        return;
    }
    StringInfoData buf;

    /* Send dn top xid change to Coordinator */
    pq_beginmessage(&buf, 'g');
    pq_sendint64(&buf, local_top_xid);
    pq_endmessage(&buf);
}

/*
 * Get status of command ID sending. If set at true, command ID needs to be communicated
 * to other nodes.
 */
bool IsSendCommandId(void)
{
    return t_thrd.xact_cxt.sendCommandId;
}

/* Change status of command ID sending. */
void SetSendCommandId(bool status)
{
    t_thrd.xact_cxt.sendCommandId = status;
}

/*
 * Determine if a Postgres-XC node session
 * is read-only or not.
 */
bool IsPGXCNodeXactReadOnly(void)
{
    /*
     * For the time being a Postgres-XC session is read-only
     * under very specific conditions.
     * This is the case of an application accessing directly
     * a Datanode provided the server was not started in restore mode.
     */
    return IsPGXCNodeXactDatanodeDirect() && !isRestoreMode && !StreamThreadAmI();
}

/*
 * Determine if a Postgres-XC node session
 * is being accessed directly by an application.
 */
bool IsPGXCNodeXactDatanodeDirect(void)
{
    /*
     * For the time being a Postgres-XC session is considered
     * as being connected directly under very specific conditions.
     *
     * IsPostmasterEnvironment || !useLocalXid
     *     All standalone backends except initdb are considered to be
     *     "directly connected" by application, which implies that for xid
     *     consistency, the backend should use global xids. initdb is the only
     *     one where local xids are used. So any standalone backend except
     *     initdb is supposed to use global xids.
     * IsNormalProcessingMode() - checks for new connections
     * IsAutoVacuumLauncherProcess - checks for autovacuum launcher process
     * IsConnFromDatanode() -  checks for if the connection is from another datanode
     */
    return IS_PGXC_DATANODE && (IsPostmasterEnvironment || !useLocalXid) && IsNormalProcessingMode() &&
           !IsAutoVacuumLauncherProcess() && !IsConnFromCoord() && !IsConnFromDatanode() && !IS_SINGLE_NODE;
}
#endif

/*
 * Currently we shallow copy CurrentTransactionState as the member 'parent' is a complex structure,
 * it would inject complicated logic if copy it thoroughly.
 * As we mainly use transactionState for data visibility check in stream thread.
 * The transactionState reset sequence of consumer stream threads in abort scenario might impact transactionState
 * value of producer stream thread, but it doesn't impact data visiblity in abort scenario. So it doesn't matter.
 * But the value of CurrentTransactionState->subTransactionId might impact Portal cleanup logic check
 * in AtAbort_Portal(), we copy it separately.
 * In future we might copy CurrentTransactionState thoroughly to avoid
 * other potential transaction state dependence in abort scenarios.
 */
void StreamTxnContextSaveXact(StreamTxnContext *stc)
{
    STCSaveElem(stc->CurrentTransactionState, CurrentTransactionState);
    STCSaveElem(stc->subTransactionId, CurrentTransactionState->subTransactionId);
    STCSaveElem(stc->currentSubTransactionId, t_thrd.xact_cxt.currentSubTransactionId);
    STCSaveElem(stc->currentCommandId, t_thrd.xact_cxt.currentCommandId);
    STCSaveElem(stc->xactStartTimestamp, t_thrd.xact_cxt.xactStartTimestamp);
    STCSaveElem(stc->stmtStartTimestamp, t_thrd.xact_cxt.stmtStartTimestamp);
    STCSaveElem(stc->xactStopTimestamp, t_thrd.xact_cxt.xactStopTimestamp);
    STCSaveElem(stc->GTMxactStartTimestamp, t_thrd.xact_cxt.GTMxactStartTimestamp);
    STCSaveElem(stc->stmtSystemTimestamp, t_thrd.time_cxt.stmt_system_timestamp);
}

void StreamTxnContextRestoreXact(StreamTxnContext *stc)
{
    STCRestoreElem(stc->subTransactionId, CurrentTransactionState->subTransactionId);
    STCRestoreElem(stc->currentSubTransactionId, t_thrd.xact_cxt.currentSubTransactionId);
    STCRestoreElem(stc->currentCommandId, t_thrd.xact_cxt.currentCommandId);
    STCRestoreElem(stc->xactStartTimestamp, t_thrd.xact_cxt.xactStartTimestamp);
    STCRestoreElem(stc->stmtStartTimestamp, t_thrd.xact_cxt.stmtStartTimestamp);
    STCRestoreElem(stc->xactStopTimestamp, t_thrd.xact_cxt.xactStopTimestamp);
    STCRestoreElem(stc->GTMxactStartTimestamp, t_thrd.xact_cxt.GTMxactStartTimestamp);
    STCRestoreElem(stc->stmtSystemTimestamp, t_thrd.time_cxt.stmt_system_timestamp);
}

void StreamTxnContextSetTransactionState(StreamTxnContext *stc)
{
    TransactionState srcTranState = (TransactionState)stc->CurrentTransactionState;
    TransactionState s = CurrentTransactionState;
    /*
     * Stream thread always is TRANS_INPROGRESS.
     * Parent thread do commit
     */
    s->state = TRANS_INPROGRESS;

    /*
     * reinitialize within-transaction counters
     */
    s->subTransactionId = stc->subTransactionId;
    s->transactionId = stc->txnId;

    /*
     * initialize current transaction state fields
     */
    s->nestingLevel = srcTranState->nestingLevel;
    s->childXids = srcTranState->childXids;
    s->nChildXids = srcTranState->nChildXids;
    s->maxChildXids = srcTranState->maxChildXids;
    s->parent = srcTranState->parent;
    /*
     * Stream thread always is TRANS_INPROGRESS.
     * Parent thread do commit
     */
    s->blockState = TBLOCK_INPROGRESS;
    s->name = srcTranState->name;
}

/*
 * To check if we are currently a running subtransaction in a transaction block.
 */
bool IsInLiveSubtransaction()
{
    return (CurrentTransactionState->blockState == TBLOCK_SUBINPROGRESS);
}

/*
 * Extend the csnlog if the sub transactions' ID need a new page.
 *
 * @in parent_xid - the parent transaction id
 * @in nsub_xid - the number of sub transactions
 * @in sub_xids - the array of sub transactions
 * @return - no return
 */
void ExtendCsnlogForSubtrans(TransactionId parent_xid, int nsub_xid, TransactionId *sub_xids)
{
    ExtendCSNLOG(parent_xid);
    for (int i = 0; i < nsub_xid; i++) {
        ExtendCSNLOG(sub_xids[i]);
    }
}

/*
 * Set transactin's csn log to commit-in-progress status,
 * and store the lastest csn by CN or DN in csn log.
 *
 * xid: Transaction's xid.
 * csn: the latest csn pass down by CN or 0, use to optimize visibility check.
 *
 */
CommitSeqNo SetXact2CommitInProgress(TransactionId xid, CommitSeqNo csn)
{
    int nchildren;
    TransactionId *children = NULL;
    CommitSeqNo latestCSN = t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo;
    latestCSN = latestCSN > csn ? latestCSN : csn;
    if (!TransactionIdIsValid(xid))
        xid = GetTopTransactionIdIfAny();
    /*
     * Top transaction xid may be 0 when CN notify commit in progress.
     * Just return because we will do nothing at RecordTransactionCommit.
     */
    if (!TransactionIdIsValid(xid))
        return InvalidCommitSeqNo;

    nchildren = xactGetCommittedChildren(&children);
    CSNLogSetCommitSeqNo(xid, nchildren, children, COMMITSEQNO_COMMIT_INPROGRESS | latestCSN);

    ereport(
        DEBUG1, (errmsg("Set %lu to commit in progress, latest csn is %lu", xid, latestCSN)));
    return latestCSN;
}

TransactionState CopyTxnStateByCurrentMcxt(TransactionState state)
{
    if (state == NULL) {
        return NULL;
    }
    TransactionState ret = (TransactionState)palloc(sizeof(TransactionStateData));
    errno_t code = memcpy_s(ret, sizeof(TransactionStateData), state, sizeof(TransactionStateData));
    securec_check(code, "", "");

    if (state->name) {
        ret->name = MemoryContextStrdup(CurrentMemoryContext, state->name);
    }

    if (state->childXids) {
        ret->childXids = (TransactionId *)palloc(sizeof(TransactionId) * (unsigned)ret->maxChildXids);
        code = memcpy_s(ret->childXids, sizeof(TransactionId) * (unsigned)ret->maxChildXids, state->childXids,
                        sizeof(TransactionId) * (unsigned)ret->nChildXids);
        securec_check(code, "", "");
    }

    if (state->parent) {
        ret->parent = CopyTxnStateByCurrentMcxt(state->parent);
    }
    return ret;
}

#ifdef ENABLE_MOT
/*
 * Check if we are using MOT storage engine in current transaction.
 */
bool IsMOTEngineUsed()
{
    return (CurrentTransactionState->storageEngineType == SE_TYPE_MOT);
}

/*
 * Check if we are using PG storage engine in current transaction.
 */
bool IsPGEngineUsed()
{
    return (CurrentTransactionState->storageEngineType == SE_TYPE_PAGE_BASED);
}

/*
 * Check if we are using MOT storage engine in parent transaction.
 */
bool IsMOTEngineUsedInParentTransaction()
{
    TransactionState s = CurrentTransactionState;
    while (s->parent != NULL) {
        s = s->parent;
        if (s->storageEngineType == SE_TYPE_MOT) {
            return true;
        }
    }
    return false;
}

/*
 * Check if we are using both PG and MOT storage engines in current transaction.
 */
bool IsMixedEngineUsed()
{
    return (CurrentTransactionState->storageEngineType == SE_TYPE_MIXED);
}

/*
 * Sets the storage engine used in current transaction.
 * cleaned at Commit(), CleanupTransaction() and StartTransaction()
 */
void SetCurrentTransactionStorageEngine(StorageEngineType storageEngineType)
{
    if (storageEngineType == SE_TYPE_UNSPECIFIED) {
        return;
    } else if (storageEngineType == SE_TYPE_MOT &&
        (CurrentTransactionState->storageEngineType == SE_TYPE_UNSPECIFIED || IsMOTEngineUsed())) {
        CurrentTransactionState->storageEngineType = SE_TYPE_MOT;
    } else if (storageEngineType == SE_TYPE_PAGE_BASED &&
        (CurrentTransactionState->storageEngineType == SE_TYPE_UNSPECIFIED || IsPGEngineUsed())) {
        CurrentTransactionState->storageEngineType = SE_TYPE_PAGE_BASED;
    } else if (storageEngineType == SE_TYPE_MIXED || (storageEngineType == SE_TYPE_PAGE_BASED && IsMOTEngineUsed()) ||
        (storageEngineType == SE_TYPE_MOT && IsPGEngineUsed())) {
        CurrentTransactionState->storageEngineType = SE_TYPE_MIXED;
    }
}

void RegisterRedoCommitCallback(RedoCommitCallback callback, void* arg)
{
    RedoCommitCallbackItem* item;

    item = (RedoCommitCallbackItem*)MemoryContextAlloc(
        INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), sizeof(RedoCommitCallbackItem));
    item->callback = callback;
    item->arg = arg;
    item->next = g_instance.xlog_cxt.redoCommitCallback;
    g_instance.xlog_cxt.redoCommitCallback = item;
}

void CallRedoCommitCallback(TransactionId xid)
{
    RedoCommitCallbackItem* item;

    for (item = g_instance.xlog_cxt.redoCommitCallback; item; item = item->next) {
        (*item->callback) (xid, item->arg);
    }
}
#endif

void SetCurrentTransactionUndoRecPtr(UndoRecPtr urecPtr, UndoPersistence upersistence)
{
    Assert(IS_VALID_UNDO_REC_PTR(urecPtr));
    if (CurrentTransactionState->first_urp[upersistence] == INVALID_UNDO_REC_PTR) {
        CurrentTransactionState->first_urp[upersistence] = urecPtr;
    }

    CurrentTransactionState->latest_urp[upersistence] = urecPtr;
    CurrentTransactionState->latest_urp_xact[upersistence] = urecPtr;
}

UndoRecPtr GetCurrentTransactionUndoRecPtr(UndoPersistence upersistence)
{
    return CurrentTransactionState->latest_urp_xact[upersistence];
}

void TryExecuteUndoActions(TransactionState s, UndoPersistence pLevel)
{
    uint32 saveHoldoff = t_thrd.int_cxt.InterruptHoldoffCount;
    bool error = false;
    undo::TransactionSlot *slot = (undo::TransactionSlot *)t_thrd.undo_cxt.slots[pLevel];
    Assert(slot->XactId() != InvalidTransactionId);
    Assert(slot->DbId() == u_sess->proc_cxt.MyDatabaseId);
    PG_TRY();
    {
        UndoSlotPtr slotPtr = t_thrd.undo_cxt.slotPtr[pLevel];
        ExecuteUndoActions(slot->XactId(), s->latest_urp[pLevel], s->first_urp[pLevel],
            slotPtr, !IsSubTransaction());
    }
    PG_CATCH();
    {
        if (pLevel == UNDO_TEMP || pLevel == UNDO_UNLOGGED) {
            PgRethrowAsFatal();
        }
        elog(LOG,
            "[ApplyUndoActions:] Error occured while executing undo actions "
            "TransactionId: %ld, latest_urp: %ld, dbid: %d",
            slot->XactId(), s->latest_urp[pLevel], u_sess->proc_cxt.MyDatabaseId);

        /*
         * Errors can reset holdoff count, so restore back.  This is
         * required because this function can be called after holding
         * interrupts.
         */
        t_thrd.int_cxt.InterruptHoldoffCount = saveHoldoff;

        /* Send the error only to server log. */
        ErrOutToClient(false);
        EmitErrorReport();

        error = true;

        /*
         * We promote the error level to FATAL if we get an error
         * while applying undo for the subtransaction.  See errstart.
         * So, we should never reach here for such a case.
         */
        Assert(!t_thrd.xact_cxt.applying_subxact_undo);
    }
    PG_END_TRY();

    if (error) {
        /*
         * This should take care of releasing the locks held under
         * TopTransactionResourceOwner.
         */
        AbortTransaction();
    }
}

void ApplyUndoActions()
{
    TransactionState s = CurrentTransactionState;

    if (!s->perform_undo)
        return;

    /*
     * State should still be TRANS_ABORT from AbortTransaction().
     */
    if (s->state != TRANS_ABORT)
        elog(FATAL, "ApplyUndoActions: unexpected state %s", TransStateAsString(s->state));

    /*
     * We promote the error level to FATAL if we get an error while applying
     * undo for the subtransaction.  See errstart.  So, we should never reach
     * here for such a case.
     */
    Assert(!t_thrd.xact_cxt.applying_subxact_undo);

    /*
     * Do abort cleanup processing before applying the undo actions.  We must
     * do this before applying the undo actions to remove the effects of
     * failed transaction.
     */
    if (IsSubTransaction()) {
        AtSubCleanup_Portals(s->subTransactionId);
        s->blockState = TBLOCK_SUBUNDO;
        t_thrd.xact_cxt.applying_subxact_undo = true;

        /* We can't afford to allow cancel of subtransaction's rollback. */
        HOLD_CANCEL_INTERRUPTS();
    } else {
        AtCleanup_Portals();      /* now safe to release portal memory */
        AtEOXact_Snapshot(false); /* and release the transaction's snapshots */
        s->transactionId = InvalidTransactionId;
        s->subTransactionId = TopSubTransactionId;
        s->blockState = TBLOCK_UNDO;
    }

    s->state = TRANS_UNDO;

    for (int i = 0; i < UNDO_PERSISTENCE_LEVELS; i++) {
        if (s->latest_urp[i]) {
            WaitState oldStatus = pgstat_report_waitstatus(STATE_WAIT_TRANSACTION_ROLLBACK);
            TryExecuteUndoActions(s, (UndoPersistence)i);
            pgstat_report_waitstatus(oldStatus);
        }
    }

    /* Reset undo information */
    ResetUndoActionsInfo();

    t_thrd.xact_cxt.applying_subxact_undo = false;

    /* Release the locks after applying undo actions. */
    if (IsSubTransaction()) {
        ResourceOwnerRelease(s->curTransactionOwner, RESOURCE_RELEASE_LOCKS, false, false);
        RESUME_CANCEL_INTERRUPTS();
    } else {
        ResourceOwnerRelease(s->curTransactionOwner, RESOURCE_RELEASE_LOCKS, false, true);
    }

    /*
     * Here we again put back the transaction in abort state so that callers
     * can proceed with the cleanup work.
     */
    s->state = TRANS_ABORT;
}

extern void SetUndoActionsInfo(void)
{
    for (int i = 0; i < UNDO_PERSISTENCE_LEVELS; i++) {
        if (CurrentTransactionState->latest_urp[i]) {
            CurrentTransactionState->perform_undo = true;
            break;
        }
    }
}

extern void ResetUndoActionsInfo(void)
{
    CurrentTransactionState->perform_undo = false;

    for (int i = 0; i < UNDO_PERSISTENCE_LEVELS; i++) {
        CurrentTransactionState->first_urp[i] = INVALID_UNDO_REC_PTR;
        CurrentTransactionState->latest_urp[i] = INVALID_UNDO_REC_PTR;
        CurrentTransactionState->latest_urp_xact[i] = INVALID_UNDO_REC_PTR;
    }
}

/*
 *  * CanPerformUndoActions - Returns true, if the current transaction can
 *   * perform undo actions, false otherwise.
 *    */
bool
CanPerformUndoActions(void)
{
    TransactionState s = CurrentTransactionState;

    return s->perform_undo;
}

/*
 * With longjump after ERROR, some resource, such as Snapshot, are not released as done by normal.
 * Some cleanup are required for subtransactions inside PL exception block. This acts much like
 * AbortSubTransaction except for dealing with more than one as read only and keeping them going.
 *
 * head: the first subtransaction in this Exception block, cleanup is required for all after this.
 */
void XactCleanExceptionSubTransaction(SubTransactionId head)
{
    TransactionState s = CurrentTransactionState;

    t_thrd.xact_cxt.bInAbortTransaction = true;

    AbortSubTxnRuntimeContext(s, true);

    /* Prevent cancel/die interrupt while cleaning up */
    HOLD_INTERRUPTS();

    while (s->subTransactionId >= head && s->parent != NULL) {
        if (IS_PGXC_DATANODE) {
            OpFusion::ClearInSubUnexpectSituation(s->curTransactionOwner);
        }

        /* reset cursor's attribute var */
        ResetPortalCursor(s->subTransactionId, InvalidOid, 0);

        AtSubAbort_Portals(s->subTransactionId, s->parent->subTransactionId,
                           s->curTransactionOwner, s->parent->curTransactionOwner, true);

        ResourceOwnerRelease(s->curTransactionOwner, RESOURCE_RELEASE_BEFORE_LOCKS, false, false);
        ResourceOwnerRelease(s->curTransactionOwner, RESOURCE_RELEASE_AFTER_LOCKS, false, false);

        AtEOSubXact_HashTables(false, s->nestingLevel);
        AtEOSubXact_PgStat(false, s->nestingLevel);
        AtSubAbort_Snapshot(s->nestingLevel);

        s = s->parent;
    }

    t_thrd.xact_cxt.bInAbortTransaction = false;

    RESUME_INTERRUPTS();
}

/*
 * Get Current Transaction's name.
 */
char* GetCurrentTransactionName()
{
    return CurrentTransactionState->name;
}

