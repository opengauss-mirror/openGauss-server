/* -------------------------------------------------------------------------
 *
 * spi.cpp
 * 				Server Programming Interface
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 * 	  src/gausskernel/runtime/executor/spi.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/hash.h"
#include "access/printtup.h"
#include "access/sysattr.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/heap.h"
#include "catalog/pg_type.h"
#include "commands/prepare.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "executor/spi_priv.h"
#include "miscadmin.h"
#include "parser/parser.h"
#include "pgxc/pgxc.h"
#include "tcop/autonomoustransaction.h"
#include "tcop/pquery.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/dynahash.h"
#include "utils/globalplancore.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "utils/elog.h"
#include "commands/sqladvisor.h"
#include "distributelayer/streamMain.h"
#include "replication/libpqsw.h"

#ifdef ENABLE_MOT
#include "storage/mot/jit_exec.h"
#endif

THR_LOCAL uint32 SPI_processed = 0;
THR_LOCAL SPITupleTable *SPI_tuptable = NULL;
THR_LOCAL int SPI_result;

static Portal SPI_cursor_open_internal(const char *name, SPIPlanPtr plan, ParamListInfo paramLI, bool read_only,
                                       bool isCollectParam = false);

void _SPI_prepare_plan(const char *src, SPIPlanPtr plan);

#ifdef ENABLE_MOT
static bool _SPI_prepare_plan_guarded(const char *src, SPIPlanPtr plan, parse_query_func parser);
#endif

#ifdef PGXC
static void _SPI_pgxc_prepare_plan(const char *src, List *src_parsetree, SPIPlanPtr plan, parse_query_func parser);
#endif

void _SPI_prepare_oneshot_plan(const char *src, SPIPlanPtr plan);

int _SPI_execute_plan(SPIPlanPtr plan, ParamListInfo paramLI, Snapshot snapshot, Snapshot crosscheck_snapshot,
    bool read_only, bool fire_triggers, long tcount, bool from_lock);

ParamListInfo _SPI_convert_params(int nargs, Oid *argtypes, Datum *Values, const char *Nulls,
    Cursor_Data *cursor_data);

static int _SPI_pquery(QueryDesc *queryDesc, bool fire_triggers, long tcount, bool from_lock = false);

void _SPI_error_callback(void *arg);

static void _SPI_cursor_operation(Portal portal, FetchDirection direction, long count, DestReceiver *dest);

static SPIPlanPtr _SPI_make_plan_non_temp(SPIPlanPtr plan);
static SPIPlanPtr _SPI_save_plan(SPIPlanPtr plan);

static MemoryContext _SPI_execmem(void);
static MemoryContext _SPI_procmem(void);
static bool _SPI_checktuples(void);
extern void ClearVacuumStmt(VacuumStmt *stmt);
static void CopySPI_Plan(SPIPlanPtr newplan, SPIPlanPtr plan, MemoryContext plancxt);

/* =================== interface functions =================== */
int SPI_connect(CommandDest dest, void (*spiCallbackfn)(void *), void *clientData)
{
     return SPI_connect_ext(dest, spiCallbackfn, clientData, 0);
}

int SPI_connect_ext(CommandDest dest, void (*spiCallbackfn)(void *), void *clientData, int options, Oid func_oid)
{
    int new_depth;
    /*
     * When procedure called by Executor u_sess->SPI_cxt._curid expected to be equal to
     * u_sess->SPI_cxt._connected
     */
    if (u_sess->SPI_cxt._curid != u_sess->SPI_cxt._connected) {
        return SPI_ERROR_CONNECT;
    }

    bool atomic = ((options & SPI_OPT_NONATOMIC) ? false : true);
    if (u_sess->SPI_cxt._stack == NULL) {
        if (u_sess->SPI_cxt._connected != -1 || u_sess->SPI_cxt._stack_depth != 0) {
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("SPI stack corrupted when connect SPI, %s",
                (u_sess->SPI_cxt._connected != -1) ? "init level is not -1." : "stack depth is not zero.")));
        }
        new_depth = 16;
        u_sess->SPI_cxt._stack = (_SPI_connection*)MemoryContextAlloc(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER), new_depth * sizeof(_SPI_connection));
        u_sess->SPI_cxt._stack_depth = new_depth;
    } else {
        if (u_sess->SPI_cxt._stack_depth <= 0 || u_sess->SPI_cxt._stack_depth <= u_sess->SPI_cxt._connected) {
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("SPI stack corrupted when connect SPI, stack depth %d", u_sess->SPI_cxt._stack_depth)));
        }
        if (u_sess->SPI_cxt._stack_depth == u_sess->SPI_cxt._connected + 1) {
            new_depth = u_sess->SPI_cxt._stack_depth * 2;
            u_sess->SPI_cxt._stack =
                (_SPI_connection *)repalloc(u_sess->SPI_cxt._stack, new_depth * sizeof(_SPI_connection));
            u_sess->SPI_cxt._stack_depth = new_depth;
        }
    }

    /*
     * We're entering procedure where u_sess->SPI_cxt._curid == u_sess->SPI_cxt._connected - 1
     */
    u_sess->SPI_cxt._connected++;
    Assert(u_sess->SPI_cxt._connected >= 0 && u_sess->SPI_cxt._connected < u_sess->SPI_cxt._stack_depth);

    u_sess->SPI_cxt._current = &(u_sess->SPI_cxt._stack[u_sess->SPI_cxt._connected]);
    u_sess->SPI_cxt._current->processed = 0;
    u_sess->SPI_cxt._current->lastoid = InvalidOid;
    u_sess->SPI_cxt._current->tuptable = NULL;
    u_sess->SPI_cxt._current->procCxt = NULL; /* in case we fail to create 'em */
    u_sess->SPI_cxt._current->execCxt = NULL;
    u_sess->SPI_cxt._current->connectSubid = GetCurrentSubTransactionId();
    u_sess->SPI_cxt._current->dest = dest;
    u_sess->SPI_cxt._current->spiCallback = (void (*)(void *))spiCallbackfn;
    u_sess->SPI_cxt._current->clientData = clientData;
    u_sess->SPI_cxt._current->func_oid = func_oid;
    u_sess->SPI_cxt._current->spi_hash_key = INVALID_SPI_KEY;
    u_sess->SPI_cxt._current->visit_id = (uint32)-1;
    u_sess->SPI_cxt._current->plan_id = -1;
    if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT) {
        u_sess->SPI_cxt._current->stmtTimestamp = t_thrd.time_cxt.stmt_system_timestamp;
    } else {
        u_sess->SPI_cxt._current->stmtTimestamp = -1;
    }

    u_sess->SPI_cxt._current->atomic = atomic;
    u_sess->SPI_cxt._current->internal_xact = false;

    if (u_sess->SPI_cxt._connected == 0) {
        /* may leak by last time, better clean it */
        DestoryAutonomousSession(true);
    }

    /*
     * Create memory contexts for this procedure
     *
     * In atomic contexts (the normal case), we use TopTransactionContext,
     * otherwise PortalContext, so that it lives across transaction
     * boundaries.
     *
     * XXX It could be better to use PortalContext as the parent context in
     * all cases, but we may not be inside a portal (consider deferred-trigger
     * execution).  Perhaps CurTransactionContext could be an option?  For now
     * it doesn't matter because we clean up explicitly in AtEOSubXact_SPI().
     */
    u_sess->SPI_cxt._current->procCxt = AllocSetContextCreate(
        u_sess->SPI_cxt._current->atomic ? u_sess->top_transaction_mem_cxt : t_thrd.mem_cxt.portal_mem_cxt,
        "SPI Proc", ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    u_sess->SPI_cxt._current->execCxt = AllocSetContextCreate(
        u_sess->SPI_cxt._current->atomic ? u_sess->top_transaction_mem_cxt : u_sess->SPI_cxt._current->procCxt,
        "SPI Exec", ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    /* ... and switch to procedure's context */
    u_sess->SPI_cxt._current->savedcxt = MemoryContextSwitchTo(u_sess->SPI_cxt._current->procCxt);

    return SPI_OK_CONNECT;
}

int SPI_finish(void)
{
    SPI_STACK_LOG("begin", NULL, NULL);
    int res = _SPI_begin_call(false); /* live in procedure memory */
    if (res < 0) {
        return res;
    }
    /* Restore memory context as it was before procedure call */
    (void)MemoryContextSwitchTo(u_sess->SPI_cxt._current->savedcxt);

    /* Release memory used in procedure call */
    MemoryContextDelete(u_sess->SPI_cxt._current->execCxt);
    u_sess->SPI_cxt._current->execCxt = NULL;
    MemoryContextDelete(u_sess->SPI_cxt._current->procCxt);
    u_sess->SPI_cxt._current->procCxt = NULL;

    /*
     * Reset result variables, especially SPI_tuptable which is probably
     * pointing at a just-deleted tuptable
     */
    SPI_processed = 0;
    u_sess->SPI_cxt.lastoid = InvalidOid;
    SPI_tuptable = NULL;

    /* Recover timestamp */
    if (u_sess->SPI_cxt._current->stmtTimestamp > 0) {
        SetCurrentStmtTimestamp(u_sess->SPI_cxt._current->stmtTimestamp);
    }

    /*
     * After _SPI_begin_call u_sess->SPI_cxt._connected == u_sess->SPI_cxt._curid. Now we are closing
     * connection to SPI and returning to upper Executor and so u_sess->SPI_cxt._connected
     * must be equal to u_sess->SPI_cxt._curid.
     */
    u_sess->SPI_cxt._connected--;
    u_sess->SPI_cxt._curid--;
    if (u_sess->SPI_cxt._connected == -1) {
        u_sess->SPI_cxt._current = NULL;
    } else {
        u_sess->SPI_cxt._current = &(u_sess->SPI_cxt._stack[u_sess->SPI_cxt._connected]);
    }

    return SPI_OK_FINISH;
}

void SPI_save_current_stp_transaction_state()
{
    SaveCurrentSTPTopTransactionState();
}

void SPI_restore_current_stp_transaction_state()
{
    RestoreCurrentSTPTopTransactionState();
}

/* This function will be called by commit/rollback inside STP to start a new transaction */
void SPI_start_transaction(List* transactionHead)
{
    Oid savedCurrentUser = InvalidOid;
    int saveSecContext = 0;
    MemoryContext savedContext = MemoryContextSwitchTo(t_thrd.mem_cxt.portal_mem_cxt);
    GetUserIdAndSecContext(&savedCurrentUser, &saveSecContext);
    if (transactionHead != NULL) {
        ListCell* cell;
        foreach(cell, transactionHead) {
            transactionNode* node = (transactionNode*)lfirst(cell);
            SetUserIdAndSecContext(node->userId, node->secContext);
            break;
        }
    }
    MemoryContextSwitchTo(savedContext);
    MemoryContext oldcontext = CurrentMemoryContext;
    PG_TRY();
    {
        StartTransactionCommand(true);
    }
    PG_CATCH();
    {
        SetUserIdAndSecContext(savedCurrentUser, saveSecContext);
        PG_RE_THROW();
    }
    PG_END_TRY();
    MemoryContextSwitchTo(oldcontext);
    SetUserIdAndSecContext(savedCurrentUser, saveSecContext);
}

/* 
 * Firstly Call this to check whether commit/rollback statement is supported, then call 
 * SPI_commit/SPI_rollback to change transaction state.
 */
void SPI_stp_transaction_check(bool read_only, bool savepoint)
{
    /* Can not commit/rollback if it's atomic is true */
    if (u_sess->SPI_cxt._current->atomic) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_TRANSACTION_TERMINATION), 
            errmsg("%s", u_sess->SPI_cxt.forbidden_commit_rollback_err_msg)));
    }

    /* If commit/rollback is not within store procedure report error */
    if (!u_sess->SPI_cxt.is_stp) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_TRANSACTION_TERMINATION), 
            errmsg("cannot commit/rollback/savepoint within function or procedure started by trigger")));
    }

    if (u_sess->SPI_cxt.is_complex_sql) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_TRANSACTION_TERMINATION),
            errmsg("cannot commit/rollback within function or procedure started by complicated SQL statements")));
    }

#ifdef ENABLE_MULTIPLE_NODES
    /* Can not commit/rollback at non-CN nodes */
    if (!IS_PGXC_COORDINATOR || IsConnFromCoord()) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_TRANSACTION_TERMINATION), 
            errmsg("cannot commit/rollback/savepoint at non-CN node")));
    }
#endif

    if (read_only) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), 
            errmsg("commit/rollback/savepoint is not allowed in a non-volatile function")));
            /* translator: %s is a SQL statement name */
    }

    if (!savepoint && IsStpInOuterSubTransaction()) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), 
            errmsg("commit/rollback is not allowed in outer sub transaction block.")));

    }
}

/*
 * See SPI_stp_transaction_check.
 */
void SPI_commit()
{
    MemoryContext oldcontext = CurrentMemoryContext;

    /*
     * This restriction is required by PLs implemented on top of SPI.  They
     * use subtransactions to establish exception blocks that are supposed to
     * be rolled back together if there is an error.  Terminating the
     * top-level transaction in such a block violates that idea.  A future PL
     * implementation might have different ideas about this, in which case
     * this restriction would have to be refined or the check possibly be
     * moved out of SPI into the PLs.
     */
     u_sess->SPI_cxt._current->internal_xact = true;

     while (ActiveSnapshotSet()) {
         PopActiveSnapshot();
     }

    CommitTransactionCommand(true);
    MemoryContextSwitchTo(oldcontext);

    u_sess->SPI_cxt._current->internal_xact = false;
  
    return;
}

/*
 * See SPI_stp_transaction_check.
 */
void SPI_rollback()
{
    MemoryContext oldcontext = CurrentMemoryContext;

    /* see under SPI_commit() */
    u_sess->SPI_cxt._current->internal_xact = true;

    AbortCurrentTransaction(true);
    MemoryContextSwitchTo(oldcontext);

    u_sess->SPI_cxt._current->internal_xact = false;

    return;
}

/*
 * rollback and release current transaction.
 */
void SPI_savepoint_rollbackAndRelease(const char *spName, SubTransactionId subXid)
{
    if (subXid == InvalidTransactionId) {
        RollbackAndReleaseCurrentSubTransaction(true);

        if (spName != NULL) {
            u_sess->plsql_cxt.stp_savepoint_cnt--;
        } else {
            u_sess->SPI_cxt.portal_stp_exception_counter--;
        }
    } else if (subXid != GetCurrentSubTransactionId()) {
        /* errors during starting a subtransaction */
        AbortSubTransaction();
        CleanupSubTransaction();
    }

#ifdef ENABLE_MULTIPLE_NODES
    /* DN had aborted the complete transaction already while cancel_query failed. */
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord() &&
        !t_thrd.xact_cxt.handlesDestroyedInCancelQuery) {
        /* internal savepoint while spName is NULL */
        const char *name = (spName != NULL) ? spName : "s1";

        StringInfoData str;
        initStringInfo(&str);
        appendStringInfo(&str, "ROLLBACK TO %s;", name);

        HandleReleaseOrRollbackSavepoint(str.data, name, SUB_STMT_ROLLBACK_TO);
        /* CN send rollback savepoint to remote nodes to abort sub transaction remotely */
        pgxc_node_remote_savepoint(str.data, EXEC_ON_DATANODES, false, false);

        resetStringInfo(&str);
        appendStringInfo(&str, "RELEASE %s;", name);
        HandleReleaseOrRollbackSavepoint(str.data, name, SUB_STMT_RELEASE);
        /* CN should send release savepoint command to remote nodes for savepoint name reuse */
        pgxc_node_remote_savepoint(str.data, EXEC_ON_DATANODES, false, false);

        FreeStringInfo(&str);
    }
#endif
}

/*
 * define a savepint in STP
 */
void SPI_savepoint_create(const char* spName)
{
#ifdef ENABLE_MULTIPLE_NODES
    /* CN should send savepoint command to remote nodes to begin sub transaction remotely. */
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        /* internal savepoint while spName is NULL */
        const char *name = (spName != NULL) ? spName : "s1";

        StringInfoData str;
        initStringInfo(&str);
        appendStringInfo(&str, "SAVEPOINT %s;", name);

        /* if do savepoint, always treat myself as local write node */
        RegisterTransactionLocalNode(true);
        RecordSavepoint(str.data, name, false, SUB_STMT_SAVEPOINT);
        pgxc_node_remote_savepoint(str.data, EXEC_ON_DATANODES, true, true);

        FreeStringInfo(&str);
    }
#endif

    SubTransactionId subXid = GetCurrentSubTransactionId();
    PG_TRY();
    {
        BeginInternalSubTransaction(spName);
    }
    PG_CATCH();
    {
        SPI_savepoint_rollbackAndRelease(spName, subXid);

        PG_RE_THROW();
    }
    PG_END_TRY();

    if (spName != NULL) {
        u_sess->plsql_cxt.stp_savepoint_cnt++;
    } else {
        u_sess->SPI_cxt.portal_stp_exception_counter++;
    }
}

/*
 * rollback to a savepoint in STP
 */
void SPI_savepoint_rollback(const char* spName)
{
    /* mark subtransaction to be rollback as abort pending */
    RollbackToSavepoint(spName, true);

#ifdef ENABLE_MULTIPLE_NODES
    /* savepoint for exception while spName is NULL */
    const char *name = (spName != NULL) ? spName : "s1";

    StringInfoData str;
    initStringInfo(&str);
    appendStringInfo(&str, "ROLLBACK TO %s;", name);

    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        HandleReleaseOrRollbackSavepoint(str.data, name, SUB_STMT_ROLLBACK_TO);
        /* CN send rollback savepoint to remote nodes to abort sub transaction remotely */
        pgxc_node_remote_savepoint(str.data, EXEC_ON_DATANODES, false, false);
    }

    FreeStringInfo(&str);
#endif

    /*
     * Do this after remote savepoint, so no cancel in AbortSubTransaction will be sent to
     * interrupt previous cursor fetch.
     */
    CommitTransactionCommand(true);
}

/*
 * release savepoint in STP
 */
void SPI_savepoint_release(const char* spName)
{
    /* Commit the inner transaction, return to outer xact context */
    ReleaseSavepoint(spName, true);

#ifdef ENABLE_MULTIPLE_NODES
    /* CN should send release savepoint command to remote nodes for savepoint name reuse */
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        /* internal savepoint while spName is NULL */
        const char *name = (spName != NULL) ? spName : "s1";

        StringInfoData str;
        initStringInfo(&str);
        appendStringInfo(&str, "RELEASE %s;", name);

        HandleReleaseOrRollbackSavepoint(str.data, name, SUB_STMT_RELEASE);
        pgxc_node_remote_savepoint(str.data, EXEC_ON_DATANODES, false, false);

        FreeStringInfo(&str);
    }
#endif

    /*
     * Do this after remote savepoint, so no cancel in AbortSubTransaction will be sent to
     * interrupt previous cursor fetch.
     */
    CommitTransactionCommand(true);
}

/*
 * return SPI current connect's stack level.
 */
int SPI_connectid()
{
    return u_sess->SPI_cxt._connected;
}

/*
 * pop SPI connect till specified stack level.
 */
void SPI_disconnect(int connect)
{
    while (u_sess->SPI_cxt._connected >= connect) {
        /* Restore memory context as it was before procedure call */
        (void)MemoryContextSwitchTo(u_sess->SPI_cxt._current->savedcxt);

        /*
         * Memory can't be destoryed now since cleanup as those as in AbortSubTransaction
         * are not done still. We ignore them here, and they will be freed along with top
         * transaction's termination or portal drop. Any idea to free it in advance?
         */
        u_sess->SPI_cxt._current->execCxt = NULL;
        u_sess->SPI_cxt._current->procCxt = NULL;

        /* Recover timestamp */
        if (u_sess->SPI_cxt._current->stmtTimestamp > 0) {
            SetCurrentStmtTimestamp(u_sess->SPI_cxt._current->stmtTimestamp);
        }

        u_sess->SPI_cxt._connected--;
        u_sess->SPI_cxt._curid = u_sess->SPI_cxt._connected;
        if (u_sess->SPI_cxt._connected == -1) {
            u_sess->SPI_cxt._current = NULL;
        } else {
            u_sess->SPI_cxt._current = &(u_sess->SPI_cxt._stack[u_sess->SPI_cxt._connected]);
        }
    }

    /*
     * Reset result variables, especially SPI_tuptable which is probably
     * pointing at a just-deleted tuptable
     */
    SPI_processed = 0;
    u_sess->SPI_cxt.lastoid = InvalidOid;
    SPI_tuptable = NULL;
}

/*
 * Clean up SPI state.  Called on transaction end (of non-SPI-internal
 * transactions) and when returning to the main loop on error.
 */
void SPICleanup(void)
{
    u_sess->SPI_cxt._current = NULL;
    u_sess->SPI_cxt._connected = u_sess->SPI_cxt._curid = -1;
    SPI_processed = 0; 
    u_sess->SPI_cxt.lastoid = InvalidOid;
    SPI_tuptable = NULL;
}

/*
 * Clean up SPI state at transaction commit or abort.
 */
void AtEOXact_SPI(bool isCommit, bool STP_rollback, bool STP_commit)
{
    /* Do nothing if the transaction end was initiated by SPI */
    if (STP_rollback || STP_commit) {
        return;
    }

    /*
     * Note that memory contexts belonging to SPI stack entries will be freed
     * automatically, so we can ignore them here.  We just need to restore our
     * static variables to initial state.
     */
    if (isCommit && u_sess->SPI_cxt._connected != -1) {
        ereport(WARNING, (errcode(ERRCODE_WARNING), errmsg("transaction left non-empty SPI stack"),
            errhint("Check for missing \"SPI_finish\" calls.")));
    }

    SPICleanup();
}

/*
 * Clean up SPI state at subtransaction commit or abort.
 *
 * During commit, there shouldn't be any unclosed entries remaining from
 * the current subtransaction; we emit a warning if any are found.
 */
void AtEOSubXact_SPI(bool isCommit, SubTransactionId mySubid, bool STP_rollback, bool STP_commit)
{
    /* Do nothing if the transaction end was initiated by SPI */
    if (STP_rollback || STP_commit) {
        return;
    }

    bool found = false;
    while (u_sess->SPI_cxt._connected >= 0) {
        _SPI_connection *connection = &(u_sess->SPI_cxt._stack[u_sess->SPI_cxt._connected]);

        if (connection->connectSubid != mySubid) {
            break;    /* couldn't be any underneath it either */
        }

        /* During multi commit within stored procedure should not clean SPI_connected,
         * it should be clean up when all the statements within STP is done
         */
        if (connection->internal_xact) {
            break;
        }

        found = true;

        /*
         * Release procedure memory explicitly (see note in SPI_connect)
         */
        bool need_free_context = isCommit ? true : connection->atomic;
        if (connection->execCxt && need_free_context) {
            MemoryContextDelete(connection->execCxt);
            connection->execCxt = NULL;
        }    
        if (connection->procCxt && need_free_context) {
            MemoryContextDelete(connection->procCxt);
            connection->procCxt = NULL;
        }

        /* Recover timestamp */
        if (connection->stmtTimestamp > 0) {
            SetCurrentStmtTimestamp(connection->stmtTimestamp);
        }

        /*
         * Pop the stack entry and reset global variables. Unlike
         * SPI_finish(), we don't risk switching to memory contexts that might
         * be already gone.
         */
        u_sess->SPI_cxt._connected--;
        u_sess->SPI_cxt._curid = u_sess->SPI_cxt._connected;
        if (u_sess->SPI_cxt._connected == -1) {
            u_sess->SPI_cxt._current = NULL;
        } else {
            u_sess->SPI_cxt._current = &(u_sess->SPI_cxt._stack[u_sess->SPI_cxt._connected]);
        }
        SPI_processed = 0;
        u_sess->SPI_cxt.lastoid = InvalidOid;
        SPI_tuptable = NULL;
    }

    if (found && isCommit) {
        ereport(WARNING, (errcode(ERRCODE_WARNING), errmsg("subtransaction left non-empty SPI stack"),
            errhint("Check for missing \"SPI_finish\" calls.")));
    }

    /*
     * If we are aborting a subtransaction and there is an open SPI context
     * surrounding the subxact, clean up to prevent memory leakage.
     */
    if (u_sess->SPI_cxt._current && !isCommit) {
        /* free Executor memory the same as _SPI_end_call would do */
        MemoryContextResetAndDeleteChildren(u_sess->SPI_cxt._current->execCxt);
        /* throw away any partially created tuple-table */
        SPI_freetuptable(u_sess->SPI_cxt._current->tuptable);
        u_sess->SPI_cxt._current->tuptable = NULL;
    }
}

/* Pushes SPI stack to allow recursive SPI calls */
void SPI_push(void)
{
    u_sess->SPI_cxt._curid++;
}

/* Pops SPI stack to allow recursive SPI calls */
void SPI_pop(void)
{
    u_sess->SPI_cxt._curid--;
}

/* Conditional push: push only if we're inside a SPI procedure */
bool SPI_push_conditional(void)
{
    bool pushed = (u_sess->SPI_cxt._curid != u_sess->SPI_cxt._connected);

    if (pushed) {
        u_sess->SPI_cxt._curid++;
        /* We should now be in a state where SPI_connect would succeed */
        Assert(u_sess->SPI_cxt._curid == u_sess->SPI_cxt._connected);
    }
    return pushed;
}

/* Conditional pop: pop only if SPI_push_conditional pushed */
void SPI_pop_conditional(bool pushed)
{
    /* We should be in a state where SPI_connect would succeed */
    Assert(u_sess->SPI_cxt._curid == u_sess->SPI_cxt._connected);
    if (pushed) {
        u_sess->SPI_cxt._curid--;
    }
}

/* Restore state of SPI stack after aborting a subtransaction */
void SPI_restore_connection(void)
{
    Assert(u_sess->SPI_cxt._connected >= 0);
    u_sess->SPI_cxt._curid = u_sess->SPI_cxt._connected - 1;
}

void SPI_restore_connection_on_exception(void)
{
    Assert(u_sess->SPI_cxt._connected >= 0);
    if (u_sess->SPI_cxt._current && u_sess->SPI_cxt._curid > u_sess->SPI_cxt._connected - 1) {
        MemoryContextResetAndDeleteChildren(u_sess->SPI_cxt._current->execCxt);
    }
    u_sess->SPI_cxt._curid = u_sess->SPI_cxt._connected - 1;
}

#ifdef PGXC
/* SPI_execute_direct:
 * Runs the 'remote_sql' query string on the node 'nodename'
 * Create the ExecDirectStmt parse tree node using remote_sql, and then prepare
 * and execute it using SPI interface.
 * This function is essentially used for making internal exec-direct operations;
 * and this should not require super-user privileges. We cannot run EXEC-DIRECT
 * query because it is meant only for superusers. So this function needs to
 * bypass the parse stage. This is achieved here by calling
 * _SPI_pgxc_prepare_plan which accepts a parse tree.
 */
int SPI_execute_direct(const char *remote_sql, char *nodename, parse_query_func parser)
{
    _SPI_plan plan;
    ExecDirectStmt *stmt = makeNode(ExecDirectStmt);
    StringInfoData execdirect;

    initStringInfo(&execdirect);

    /* This string is never used. It is just passed to fill up spi_err_context.arg */
    appendStringInfo(&execdirect, "EXECUTE DIRECT ON (%s) '%s'", nodename, remote_sql);

    stmt->node_names = list_make1(makeString(nodename));
    stmt->query = pstrdup(remote_sql);

    SPI_STACK_LOG("begin", remote_sql, NULL);
    int res = _SPI_begin_call(true);
    if (res < 0) {
        return res;
    }

    errno_t errorno = memset_s(&plan, sizeof(_SPI_plan), '\0', sizeof(_SPI_plan));
    securec_check(errorno, "\0", "\0");
    plan.magic = _SPI_PLAN_MAGIC;
    plan.cursor_options = 0;
    plan.spi_key = INVALID_SPI_KEY;

    /* Now pass the ExecDirectStmt parsetree node */
    _SPI_pgxc_prepare_plan(execdirect.data, list_make1(stmt), &plan, parser);

    res = _SPI_execute_plan(&plan, NULL, InvalidSnapshot, InvalidSnapshot, false, true, 0, true);

    SPI_STACK_LOG("end", remote_sql, NULL);
    _SPI_end_call(true);
    return res;
}
#endif

/* 
 * Parse, plan, and execute a query string 
 * @isCollectParam: default false, is used to collect sql info in sqladvisor online mode.
 */
int SPI_execute(const char *src, bool read_only, long tcount, bool isCollectParam, parse_query_func parser)
{
    _SPI_plan plan;

    if (src == NULL || tcount < 0) {
        return SPI_ERROR_ARGUMENT;
    }
    SPI_STACK_LOG("begin", src, NULL);

    int res = _SPI_begin_call(true);
    if (res < 0) {
        return res;
    }
    errno_t errorno = memset_s(&plan, sizeof(_SPI_plan), '\0', sizeof(_SPI_plan));
    securec_check(errorno, "\0", "\0");
    plan.magic = _SPI_PLAN_MAGIC;
    plan.cursor_options = 0;
    plan.spi_key = INVALID_SPI_KEY;

    _SPI_prepare_oneshot_plan(src, &plan, parser);

    res = _SPI_execute_plan(&plan, NULL, InvalidSnapshot, InvalidSnapshot, read_only, true, tcount);

#ifdef ENABLE_MULTIPLE_NODES
    if (isCollectParam && checkSPIPlan(&plan)) {
        collectDynWithArgs(src, NULL, 0);
    }
#endif
    SPI_STACK_LOG("end", src, NULL);

    _SPI_end_call(true);
    return res;
}

/* Obsolete version of SPI_execute */
int SPI_exec(const char *src, long tcount, parse_query_func parser)
{
    return SPI_execute(src, false, tcount, false, parser);
}

/* Execute a previously prepared plan */
int SPI_execute_plan(SPIPlanPtr plan, Datum *Values, const char *Nulls, bool read_only, long tcount)
{
    if (plan == NULL || plan->magic != _SPI_PLAN_MAGIC || tcount < 0) {
        return SPI_ERROR_ARGUMENT;
    }

    if (plan->nargs > 0 && Values == NULL)
        return SPI_ERROR_PARAM;

    SPI_STACK_LOG("begin", NULL, plan);
    int res = _SPI_begin_call(true);
    if (res < 0) {
        return res;
    }
    res = _SPI_execute_plan(plan, _SPI_convert_params(plan->nargs, plan->argtypes, Values, Nulls), InvalidSnapshot,
        InvalidSnapshot, read_only, true, tcount);

    SPI_STACK_LOG("end", NULL, plan);
    _SPI_end_call(true);
    return res;
}

/* Obsolete version of SPI_execute_plan */
int SPI_execp(SPIPlanPtr plan, Datum *Values, const char *Nulls, long tcount)
{
    return SPI_execute_plan(plan, Values, Nulls, false, tcount);
}

/* Execute a previously prepared plan */
int SPI_execute_plan_with_paramlist(SPIPlanPtr plan, ParamListInfo params, bool read_only, long tcount)
{
    if (plan == NULL || plan->magic != _SPI_PLAN_MAGIC || tcount < 0) {
        return SPI_ERROR_ARGUMENT;
    }
    SPI_STACK_LOG("begin", NULL, plan);

    int res = _SPI_begin_call(true);
    if (res < 0) {
        return res;
    }
    res = _SPI_execute_plan(plan, params, InvalidSnapshot, InvalidSnapshot, read_only, true, tcount);

    SPI_STACK_LOG("end", NULL, plan);
    _SPI_end_call(true);
    return res;
}

/*
 * SPI_execute_snapshot -- identical to SPI_execute_plan, except that we allow
 * the caller to specify exactly which snapshots to use, which will be
 * registered here.  Also, the caller may specify that AFTER triggers should be
 * queued as part of the outer query rather than being fired immediately at the
 * end of the command.
 *
 * This is currently not documented in spi.sgml because it is only intended
 * for use by RI triggers.
 *
 * Passing snapshot == InvalidSnapshot will select the normal behavior of
 * fetching a new snapshot for each query.
 */
int SPI_execute_snapshot(SPIPlanPtr plan, Datum *Values, const char *Nulls, Snapshot snapshot,
    Snapshot crosscheck_snapshot, bool read_only, bool fire_triggers, long tcount)
{
    if (plan == NULL || plan->magic != _SPI_PLAN_MAGIC || tcount < 0) {
        return SPI_ERROR_ARGUMENT;
    }

    if (plan->nargs > 0 && Values == NULL) {
        return SPI_ERROR_PARAM;
    }
    SPI_STACK_LOG("begin", NULL, plan);

    int res = _SPI_begin_call(true);
    if (res < 0) {
        return res;
    }
    res = _SPI_execute_plan(plan, _SPI_convert_params(plan->nargs, plan->argtypes, Values, Nulls), snapshot,
        crosscheck_snapshot, read_only, fire_triggers, tcount);

    SPI_STACK_LOG("end", NULL, plan);
    _SPI_end_call(true);
    return res;
}

/*
 * SPI_execute_with_args -- plan and execute a query with supplied arguments
 *
 * This is functionally equivalent to SPI_prepare followed by
 * SPI_execute_plan.
 */
int SPI_execute_with_args(const char *src, int nargs, Oid *argtypes, Datum *Values, const char *Nulls, bool read_only,
    long tcount, Cursor_Data *cursor_data, parse_query_func parser)
{
    _SPI_plan plan;

    if (src == NULL || nargs < 0 || tcount < 0) {
        return SPI_ERROR_ARGUMENT;
    }

    if (nargs > 0 && (argtypes == NULL || Values == NULL)) {
        return SPI_ERROR_PARAM;
    }

    SPI_STACK_LOG("begin", src, NULL);
    int res = _SPI_begin_call(true);
    if (res < 0) {
        return res;
    }
    errno_t errorno = memset_s(&plan, sizeof(_SPI_plan), '\0', sizeof(_SPI_plan));
    securec_check(errorno, "\0", "\0");
    plan.magic = _SPI_PLAN_MAGIC;
    plan.cursor_options = 0;
    plan.nargs = nargs;
    plan.argtypes = argtypes;
    plan.parserSetup = NULL;
    plan.parserSetupArg = NULL;

    ParamListInfo param_list_info = _SPI_convert_params(nargs, argtypes, Values, Nulls, cursor_data);

    _SPI_prepare_oneshot_plan(src, &plan, parser);

    res = _SPI_execute_plan(&plan, param_list_info, InvalidSnapshot, InvalidSnapshot, read_only, true, tcount);
#ifdef ENABLE_MULTIPLE_NODES
    if (checkAdivsorState() && checkSPIPlan(&plan)) {
        collectDynWithArgs(src, param_list_info, plan.cursor_options);
    }
#endif
    SPI_STACK_LOG("end", src, NULL);
    _SPI_end_call(true);
    return res;
}

SPIPlanPtr SPI_prepare(const char *src, int nargs, Oid *argtypes, parse_query_func parser)
{
    return SPI_prepare_cursor(src, nargs, argtypes, 0, parser);
}

#ifdef USE_SPQ
SPIPlanPtr SPI_prepare_spq(const char *src, int nargs, Oid *argtypes, parse_query_func parser)
{
    return SPI_prepare_cursor(src, nargs, argtypes, CURSOR_OPT_SPQ_OK | CURSOR_OPT_SPQ_FORCE, parser);
}
#endif

SPIPlanPtr SPI_prepare_cursor(const char *src, int nargs, Oid *argtypes, int cursorOptions, parse_query_func parser)
{
    _SPI_plan plan;

    if (src == NULL || nargs < 0 || (nargs > 0 && argtypes == NULL)) {
        SPI_result = SPI_ERROR_ARGUMENT;
        return NULL;
    }

    SPI_STACK_LOG("begin", src, NULL);
    SPI_result = _SPI_begin_call(true);
    if (SPI_result < 0) {
        return NULL;
    }
    errno_t errorno = memset_s(&plan, sizeof(_SPI_plan), '\0', sizeof(_SPI_plan));
    securec_check(errorno, "\0", "\0");
    plan.magic = _SPI_PLAN_MAGIC;
    plan.cursor_options = cursorOptions;
    plan.nargs = nargs;
    plan.argtypes = argtypes;
    plan.parserSetup = NULL;
    plan.parserSetupArg = NULL;
    plan.spi_key = INVALID_SPI_KEY;
    plan.id = (uint32)-1;

    /* don't call SPI_keepplan, so won't put plancache into first_save_plan.
       so this plancache can't share into gpc, set spi_hash_key to invalid when call _SPI_prepare_plan. */
    uint32 old_key = u_sess->SPI_cxt._current->spi_hash_key;
    u_sess->SPI_cxt._current->spi_hash_key = INVALID_SPI_KEY;
    PG_TRY();
    {
        _SPI_prepare_plan(src, &plan, parser);
    }
    PG_CATCH();
    {
        u_sess->SPI_cxt._current->spi_hash_key = old_key;
        PG_RE_THROW();
    }
    PG_END_TRY();
    u_sess->SPI_cxt._current->spi_hash_key = old_key;

    /* copy plan to procedure context */
    SPIPlanPtr result = _SPI_make_plan_non_temp(&plan);

    SPI_STACK_LOG("end", src, NULL);
    _SPI_end_call(true);

    return result;
}

SPIPlanPtr SPI_prepare_params(const char *src, ParserSetupHook parserSetup, void *parserSetupArg, int cursorOptions,
                              parse_query_func parser)
{
    _SPI_plan plan;

    if (src == NULL) {
        SPI_result = SPI_ERROR_ARGUMENT;
        return NULL;
    }

    SPI_STACK_LOG("begin", src, NULL);
    SPI_result = _SPI_begin_call(true);
    if (SPI_result < 0) {
        return NULL;
    }

    errno_t errorno = memset_s(&plan, sizeof(_SPI_plan), '\0', sizeof(_SPI_plan));
    securec_check(errorno, "\0", "\0");
    plan.magic = _SPI_PLAN_MAGIC;
    plan.cursor_options = cursorOptions;
    plan.nargs = 0;
    plan.argtypes = NULL;
    plan.parserSetup = parserSetup;
    plan.parserSetupArg = parserSetupArg;
    plan.stmt_list = NIL;
    plan.spi_key = INVALID_SPI_KEY;
    plan.id = (uint32)-1;

#ifdef ENABLE_MOT
    if (u_sess->mot_cxt.jit_compile_depth > 0) {
        if (!_SPI_prepare_plan_guarded(src, &plan, parser)) {
            _SPI_end_call(true);
            SPI_result = SPI_ERROR_ARGUMENT;
            return NULL;
        }
    } else {
        _SPI_prepare_plan(src, &plan, parser);
    }
#else
    _SPI_prepare_plan(src, &plan, parser);
#endif

    /* copy plan to procedure context */
    SPIPlanPtr result = _SPI_make_plan_non_temp(&plan);

    SPI_STACK_LOG("end", src, NULL);
    _SPI_end_call(true);

    return result;
}

int SPI_keepplan(SPIPlanPtr plan)
{
    ListCell *lc = NULL;

    if (plan == NULL || plan->magic != _SPI_PLAN_MAGIC || plan->saved || plan->oneshot) {
        return SPI_ERROR_ARGUMENT;
    }

    /*
     * Mark it saved, reparent it under u_sess->cache_mem_cxt, and mark all the
     * component CachedPlanSources as saved.  This sequence cannot fail
     * partway through, so there's no risk of long-term memory leakage.
     */
    plan->saved = true;
    MemoryContextSetParent(plan->plancxt, u_sess->cache_mem_cxt);
    if (ENABLE_CN_GPC && plan->spi_key != INVALID_SPI_KEY) {
        foreach (lc, SPI_plan_get_plan_sources(plan)) {
            CachedPlanSource *plansource = (CachedPlanSource *)lfirst(lc);
            /* first created plan or shared plan from gpc */
            if (!plansource->gpc.status.InShareTable()) {
                Assert (!plansource->gpc.status.InUngpcPlanList());
                SaveCachedPlan(plansource);
                plansource->gpc.status.SetLoc(GPC_SHARE_IN_LOCAL_SAVE_PLAN_LIST);
            }
        }
    } else {
        foreach (lc, plan->plancache_list) {
            CachedPlanSource *plansource = (CachedPlanSource *)lfirst(lc);
            SaveCachedPlan(plansource);
        }
    }
    // spiplan should on cache_mem_cxt
    Assert(plan->magic == _SPI_PLAN_MAGIC);
    if (ENABLE_CN_GPC && plan->spi_key != INVALID_SPI_KEY && list_length(plan->plancache_list) > 0) {
        Assert (plan->spi_key == u_sess->SPI_cxt._current->spi_hash_key);
        SPICacheTableInsertPlan(plan->spi_key, plan);
    }

    return 0;
}

SPIPlanPtr SPI_saveplan(SPIPlanPtr plan)
{
    if (plan == NULL || plan->magic != _SPI_PLAN_MAGIC) {
        SPI_result = SPI_ERROR_ARGUMENT;
        return NULL;
    }
    SPI_STACK_LOG("begin", NULL, plan);
    SPI_result = _SPI_begin_call(false); /* don't change context */
    if (SPI_result < 0) {
        return NULL;
    }

    SPIPlanPtr new_plan = _SPI_save_plan(plan);

    SPI_result = _SPI_end_call(false);

    SPI_STACK_LOG("end", NULL, plan);
    return new_plan;
}

int SPI_freeplan(SPIPlanPtr plan)
{
    ListCell *lc = NULL;

    if (plan == NULL || plan->magic != _SPI_PLAN_MAGIC) {
        return SPI_ERROR_ARGUMENT;
    }

    if (ENABLE_CN_GPC) {
        CN_GPC_LOG("drop spi plan SPI_freeplan", 0, 0);
        g_instance.plan_cache->RemovePlanCacheInSPIPlan(plan);
        MemoryContextDelete(plan->plancxt);
        return 0;
    }
    /* Release the plancache entries */
    foreach (lc, plan->plancache_list) {
        CachedPlanSource *plansource = (CachedPlanSource *)lfirst(lc);
        DropCachedPlan(plansource);
    }

    /* Now get rid of the _SPI_plan and subsidiary data in its plancxt */
    MemoryContextDelete(plan->plancxt);

    return 0;
}

HeapTuple SPI_copytuple(HeapTuple tuple)
{
    MemoryContext old_ctx = NULL;

    if (tuple == NULL) {
        SPI_result = SPI_ERROR_ARGUMENT;
        return NULL;
    }

    if (u_sess->SPI_cxt._curid + 1 == u_sess->SPI_cxt._connected) { /* connected */
        if (u_sess->SPI_cxt._current != &(u_sess->SPI_cxt._stack[u_sess->SPI_cxt._curid + 1])) {
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("SPI stack corrupted when copy tuple, connected level: %d", u_sess->SPI_cxt._connected)));
        }

        old_ctx = MemoryContextSwitchTo(u_sess->SPI_cxt._current->savedcxt);
    }

    HeapTuple c_tuple = (HeapTuple)tableam_tops_copy_tuple(tuple);

    if (old_ctx) {
        (void)MemoryContextSwitchTo(old_ctx);
    }

    return c_tuple;
}

HeapTupleHeader SPI_returntuple(HeapTuple tuple, TupleDesc tupdesc)
{
    MemoryContext old_ctx = NULL;

    if (tuple == NULL || tupdesc == NULL) {
        SPI_result = SPI_ERROR_ARGUMENT;
        return NULL;
    }

    /* For RECORD results, make sure a typmod has been assigned */
    if (tupdesc->tdtypeid == RECORDOID && tupdesc->tdtypmod < 0) {
        assign_record_type_typmod(tupdesc);
    }

    if (u_sess->SPI_cxt._curid + 1 == u_sess->SPI_cxt._connected) { /* connected */
        if (u_sess->SPI_cxt._current != &(u_sess->SPI_cxt._stack[u_sess->SPI_cxt._curid + 1])) {
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("SPI stack corrupted when return tuple, connected level: %d", u_sess->SPI_cxt._connected)));
        }

        old_ctx = MemoryContextSwitchTo(u_sess->SPI_cxt._current->savedcxt);
    }

    HeapTupleHeader d_tup = (HeapTupleHeader)palloc(tuple->t_len);
    errno_t rc = memcpy_s((char *)d_tup, tuple->t_len, (char *)tuple->t_data, tuple->t_len);
    securec_check(rc, "\0", "\0");

    HeapTupleHeaderSetDatumLength(d_tup, tuple->t_len);
    HeapTupleHeaderSetTypeId(d_tup, tupdesc->tdtypeid);
    HeapTupleHeaderSetTypMod(d_tup, tupdesc->tdtypmod);

    if (old_ctx) {
        (void)MemoryContextSwitchTo(old_ctx);
    }

    return d_tup;
}

HeapTuple SPI_modifytuple(Relation rel, HeapTuple tuple, int natts, int *attnum, Datum *Values, const char *Nulls)
{
    MemoryContext old_ctx = NULL;
    HeapTuple m_tuple = NULL;
    int num_of_attr;
    Datum *v = NULL;
    bool *n = NULL;
    int i;

    if (rel == NULL || tuple == NULL || natts < 0 || attnum == NULL || Values == NULL) {
        SPI_result = SPI_ERROR_ARGUMENT;
        return NULL;
    }

    if (u_sess->SPI_cxt._curid + 1 == u_sess->SPI_cxt._connected) { /* connected */
        if (u_sess->SPI_cxt._current != &(u_sess->SPI_cxt._stack[u_sess->SPI_cxt._curid + 1])) {
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("SPI stack corrupted when modify tuple, connected level: %d", u_sess->SPI_cxt._connected)));
        }

        old_ctx = MemoryContextSwitchTo(u_sess->SPI_cxt._current->savedcxt);
    }
    SPI_result = 0;
    num_of_attr = rel->rd_att->natts;
    v = (Datum *)palloc(num_of_attr * sizeof(Datum));
    n = (bool *)palloc(num_of_attr * sizeof(bool));

    /* fetch old values and nulls */
    heap_deform_tuple(tuple, rel->rd_att, v, n);
    /* replace values and nulls */
    for (i = 0; i < natts; i++) {
        if (attnum[i] <= 0 || attnum[i] > num_of_attr) {
            break;
        }
        v[attnum[i] - 1] = Values[i];
        n[attnum[i] - 1] = (Nulls && Nulls[i] == 'n') ? true : false;
    }

    if (i == natts) {
        /* no errors in *attnum */
        m_tuple = heap_form_tuple(rel->rd_att, v, n);
        /*
         * copy the identification info of the old tuple: t_ctid, t_self, and
         * OID (if any)
         */
        m_tuple->t_data->t_ctid = tuple->t_data->t_ctid;
        m_tuple->t_self = tuple->t_self;
        m_tuple->t_tableOid = tuple->t_tableOid;
        m_tuple->t_bucketId = tuple->t_bucketId;
        HeapTupleCopyBase(m_tuple, tuple);
#ifdef PGXC
        m_tuple->t_xc_node_id = tuple->t_xc_node_id;
#endif

        if (rel->rd_att->tdhasoid) {
            HeapTupleSetOid(m_tuple, HeapTupleGetOid(tuple));
        }
    } else {
        m_tuple = NULL;
        SPI_result = SPI_ERROR_NOATTRIBUTE;
    }

    pfree_ext(v);
    pfree_ext(n);

    if (old_ctx) {
        (void)MemoryContextSwitchTo(old_ctx);
    }
    return m_tuple;
}

int SPI_fnumber(TupleDesc tupdesc, const char *fname)
{
    int res;
    Form_pg_attribute sys_att;

    for (res = 0; res < tupdesc->natts; res++) {
        if (u_sess->attr.attr_sql.dolphin) {
            if (namestrcasecmp(&tupdesc->attrs[res].attname, fname) == 0) {
                return res + 1;
            }
        } else if (namestrcmp(&tupdesc->attrs[res].attname, fname) == 0) {
            return res + 1;
        }
    }

    sys_att = SystemAttributeByName(fname, true /* "oid" will be accepted */);
    if (sys_att != NULL) {
        return sys_att->attnum;
    }

    /* SPI_ERROR_NOATTRIBUTE is different from all sys column numbers */
    return SPI_ERROR_NOATTRIBUTE;
}

char *SPI_fname(TupleDesc tupdesc, int fnumber)
{
    Form_pg_attribute attr;
    SPI_result = 0;

    if (fnumber > tupdesc->natts || fnumber == 0 || fnumber <= FirstLowInvalidHeapAttributeNumber) {
        SPI_result = SPI_ERROR_NOATTRIBUTE;
        return NULL;
    }

    if (fnumber > 0) {
        attr = &tupdesc->attrs[fnumber - 1];
    } else {
        attr = SystemAttributeDefinition(fnumber, true, false, false);
    }

    return pstrdup(NameStr(attr->attname));
}

char *SPI_getvalue(HeapTuple tuple, TupleDesc tupdesc, int fnumber)
{
    Datum val;
    bool is_null = false;
    Oid typoid, foutoid;
    bool typo_is_varlen = false;

    SPI_result = 0;

    if (fnumber > tupdesc->natts || fnumber == 0 || fnumber <= FirstLowInvalidHeapAttributeNumber) {
        SPI_result = SPI_ERROR_NOATTRIBUTE;
        return NULL;
    }

    val = tableam_tops_tuple_getattr(tuple, (unsigned int)fnumber, tupdesc, &is_null);

    if (is_null) {
        return NULL;
    }

    if (fnumber > 0) {
        typoid = tupdesc->attrs[fnumber - 1].atttypid;
    } else {
        typoid = (SystemAttributeDefinition(fnumber, true, false, false))->atttypid;
    }

    getTypeOutputInfo(typoid, &foutoid, &typo_is_varlen);

    return OidOutputFunctionCall(foutoid, val);
}

Datum SPI_getbinval(HeapTuple tuple, TupleDesc tupdesc, int fnumber, bool *isnull)
{
    SPI_result = 0;

    if (fnumber > tupdesc->natts || fnumber == 0 || fnumber <= FirstLowInvalidHeapAttributeNumber) {
        SPI_result = SPI_ERROR_NOATTRIBUTE;
        *isnull = true;
        return (Datum)NULL;
    }

    return tableam_tops_tuple_getattr(tuple, (unsigned int)fnumber, tupdesc, isnull);
}

char *SPI_gettype(TupleDesc tupdesc, int fnumber)
{
    Oid typoid;
    HeapTuple type_tuple = NULL;
    char *result = NULL;
    SPI_result = 0;

    if (fnumber > tupdesc->natts || fnumber == 0 || fnumber <= FirstLowInvalidHeapAttributeNumber) {
        SPI_result = SPI_ERROR_NOATTRIBUTE;
        return NULL;
    }

    if (fnumber > 0) {
        typoid = tupdesc->attrs[fnumber - 1].atttypid;
    } else {
        typoid = (SystemAttributeDefinition(fnumber, true, false, false))->atttypid;
    }

    type_tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typoid));
    if (!HeapTupleIsValid(type_tuple)) {
        SPI_result = SPI_ERROR_TYPUNKNOWN;
        return NULL;
    }

    result = pstrdup(NameStr(((Form_pg_type)GETSTRUCT(type_tuple))->typname));
    ReleaseSysCache(type_tuple);
    return result;
}

Oid SPI_gettypeid(TupleDesc tupdesc, int fnumber)
{
    SPI_result = 0;

    if (fnumber > tupdesc->natts || fnumber == 0 || fnumber <= FirstLowInvalidHeapAttributeNumber) {
        SPI_result = SPI_ERROR_NOATTRIBUTE;
        return InvalidOid;
    }

    if (fnumber > 0) {
        return tupdesc->attrs[fnumber - 1].atttypid;
    } else {
        return (SystemAttributeDefinition(fnumber, true, false, false))->atttypid;
    }
}

Oid SPI_getcollation(TupleDesc tupdesc, int fnumber)
{
    SPI_result = 0;

    if (fnumber > tupdesc->natts || fnumber == 0 || fnumber <= FirstLowInvalidHeapAttributeNumber) {
        SPI_result = SPI_ERROR_NOATTRIBUTE;
        return InvalidOid;
    }

    if (fnumber > 0) {
        return tupdesc->attrs[fnumber - 1].attcollation;
    } else {
        return (SystemAttributeDefinition(fnumber, true, false, false))->attcollation;
    }
}

char *SPI_getrelname(Relation rel)
{
    return pstrdup(RelationGetRelationName(rel));
}

char *SPI_getnspname(Relation rel)
{
    return get_namespace_name(RelationGetNamespace(rel));
}

void *SPI_palloc(Size size)
{
    MemoryContext old_ctx = NULL;
    void *pointer = NULL;

    if (u_sess->SPI_cxt._curid + 1 == u_sess->SPI_cxt._connected) { /* connected */
        if (u_sess->SPI_cxt._current != &(u_sess->SPI_cxt._stack[u_sess->SPI_cxt._curid + 1])) {
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("SPI stack corrupted when allocate, connected level: %d", u_sess->SPI_cxt._connected)));
        }

        old_ctx = MemoryContextSwitchTo(u_sess->SPI_cxt._current->savedcxt);
    }

    pointer = palloc(size);

    if (old_ctx) {
        (void)MemoryContextSwitchTo(old_ctx);
    }

    return pointer;
}

void *SPI_repalloc(void *pointer, Size size)
{
    /* No longer need to worry which context chunk was in... */
    return repalloc(pointer, size);
}

void SPI_pfree_ext(void *pointer)
{
    /* No longer need to worry which context chunk was in... */
    pfree_ext(pointer);
}

void SPI_freetuple(HeapTuple tuple)
{
    /* No longer need to worry which context tuple was in... */
    tableam_tops_free_tuple(tuple);
}

void SPI_freetuptable(SPITupleTable *tuptable)
{
    if (tuptable != NULL) {
        MemoryContextDelete(tuptable->tuptabcxt);
    }
}

/*
 * SPI_cursor_open
 *
 * 	Open a prepared SPI plan as a portal
 */
Portal SPI_cursor_open(const char *name, SPIPlanPtr plan, Datum *Values, const char *Nulls, bool read_only)
{
    Portal portal;
    ParamListInfo param_list_info;

    /* build transient ParamListInfo in caller's context */
    param_list_info = _SPI_convert_params(plan->nargs, plan->argtypes, Values, Nulls);

    portal = SPI_cursor_open_internal(name, plan, param_list_info, read_only);

    /* done with the transient ParamListInfo */
    if (param_list_info) {
        pfree_ext(param_list_info);
    }

    return portal;
}

/*
 * SPI_cursor_open_with_args
 *
 * Parse and plan a query and open it as a portal.
 */
Portal SPI_cursor_open_with_args(const char *name, const char *src, int nargs, Oid *argtypes, Datum *Values,
    const char *Nulls, bool read_only, int cursorOptions, parse_query_func parser)
{
    _SPI_plan plan;
    errno_t errorno = EOK;

    if (src == NULL || nargs < 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), 
            errmsg("open cursor with args has invalid arguments,%s",
                (src == NULL) ? "query string is NULL" : "argument number is less than zero.")));
    }

    if (nargs > 0 && (argtypes == NULL || Values == NULL)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), 
            errmsg("open cursor with args has invalid arguments,%s",
                (argtypes == NULL) ? "argument type is NULL" : "value is NULL")));
    }

    SPI_STACK_LOG("begin", src, NULL);
    SPI_result = _SPI_begin_call(true);
    if (SPI_result < 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_CURSOR_STATE),
            errmsg("SPI stack is corrupted when open cursor with args, current level: %d, connected level: %d",
            u_sess->SPI_cxt._curid, u_sess->SPI_cxt._connected)));
    }

    errorno = memset_s(&plan, sizeof(_SPI_plan), '\0', sizeof(_SPI_plan));
    securec_check(errorno, "\0", "\0");

    plan.magic = _SPI_PLAN_MAGIC;
    plan.cursor_options = cursorOptions;
    plan.nargs = nargs;
    plan.argtypes = argtypes;
    plan.parserSetup = NULL;
    plan.parserSetupArg = NULL;
    plan.spi_key = INVALID_SPI_KEY;
    plan.id = (uint32)-1;

    /* build transient ParamListInfo in executor context */
    ParamListInfo param_list_info = _SPI_convert_params(nargs, argtypes, Values, Nulls);
    /* don't call SPI_keepplan, so won't put plancache into first_save_plan.
       so this plancache can't share into gpc, set spi_hash_key to invalid when call _SPI_prepare_plan. */
    uint32 old_key = u_sess->SPI_cxt._current->spi_hash_key;
    u_sess->SPI_cxt._current->spi_hash_key = INVALID_SPI_KEY;
    PG_TRY();
    {
        _SPI_prepare_plan(src, &plan, parser);
    }
    PG_CATCH();
    {
        u_sess->SPI_cxt._current->spi_hash_key = old_key;
        PG_RE_THROW();
    }
    PG_END_TRY();
    u_sess->SPI_cxt._current->spi_hash_key = old_key;

    /* We needn't copy the plan; SPI_cursor_open_internal will do so */
    /* Adjust stack so that SPI_cursor_open_internal doesn't complain */
    u_sess->SPI_cxt._curid--;
    bool isCollectParam = false;
#ifdef ENABLE_MULTIPLE_NODES
    if (checkAdivsorState()) {
        isCollectParam = true;
    }
#endif
    Portal result = SPI_cursor_open_internal(name, &plan, param_list_info, read_only, isCollectParam);

    /* And clean up */
    SPI_STACK_LOG("end", src, NULL);
    u_sess->SPI_cxt._curid++;
    _SPI_end_call(true);

    return result;
}

/*
 * SPI_cursor_open_with_paramlist
 *
 * 	Same as SPI_cursor_open except that parameters (if any) are passed
 * 	as a ParamListInfo, which supports dynamic parameter set determination
 */
Portal SPI_cursor_open_with_paramlist(const char *name, SPIPlanPtr plan, ParamListInfo params,
                                      bool read_only, bool isCollectParam)
{
    return SPI_cursor_open_internal(name, plan, params, read_only, isCollectParam);
}

#ifdef ENABLE_MULTIPLE_NODES
/* check plan's stream node and set flag */
static void check_portal_stream(Portal portal)
{
    if (IS_PGXC_COORDINATOR && check_stream_for_loop_fetch(portal)) {
        if (!ENABLE_SQL_BETA_FEATURE(PLPGSQL_STREAM_FETCHALL)) {
            /* save flag for warning */
            u_sess->SPI_cxt.has_stream_in_cursor_or_forloop_sql = portal->hasStreamForPlpgsql;
        }
    }
}
#endif

/*
 * SPI_cursor_open_internal
 *
 * 	Common code for SPI_cursor_open variants
 */
static Portal SPI_cursor_open_internal(const char *name, SPIPlanPtr plan, ParamListInfo paramLI, bool read_only,
                                       bool isCollectParam)
{
    CachedPlanSource *plansource = NULL;
    List *stmt_list = NIL;
    char *query_string = NULL;
    Snapshot snapshot;
    MemoryContext old_ctx;
    Portal portal;
    ErrorContextCallback spi_err_context;
#ifndef ENABLE_MULTIPLE_NODES
    AutoDopControl dopControl;
    dopControl.CloseSmp();
#endif
    NodeTag old_node_tag = t_thrd.postgres_cxt.cur_command_tag;

    /*
     * Check that the plan is something the Portal code will special-case as
     * returning one tupleset.
     */
    if (!SPI_is_cursor_plan(plan, paramLI)) {
        /* try to give a good error message */
        if (list_length(plan->plancache_list) != 1) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_CURSOR_DEFINITION), errmsg("cannot open multi-query plan as cursor")));
        }

        plansource = (CachedPlanSource *)linitial(plan->plancache_list);
        ereport(ERROR, (errcode(ERRCODE_INVALID_CURSOR_DEFINITION),
            /* translator: %s is name of a SQL command, eg INSERT */
            errmsg("cannot open %s query as cursor", plansource->commandTag)));
    }

    Assert(list_length(plan->plancache_list) == 1);
    plansource = (CachedPlanSource *)linitial(plan->plancache_list);
    t_thrd.postgres_cxt.cur_command_tag = transform_node_tag(plansource->raw_parse_tree);

    SPI_STACK_LOG("begin", NULL, plan);
    /* Push the SPI stack */
    if (_SPI_begin_call(true) < 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_CURSOR_STATE),
            errmsg("SPI stack is corrupted when open cursor, current level: %d, connected level: %d",
            u_sess->SPI_cxt._curid, u_sess->SPI_cxt._connected)));
    }

    /* Reset SPI result (note we deliberately don't touch lastoid) */
    SPI_processed = 0;
    SPI_tuptable = NULL;
    u_sess->SPI_cxt._current->processed = 0;
    u_sess->SPI_cxt._current->tuptable = NULL;

    /* Create the portal */
    if (name == NULL || name[0] == '\0') {
        /* Use a random nonconflicting name */
        portal = CreateNewPortal(true);
    } else {
        /* In this path, error if portal of same name already exists */
        portal = CreatePortal(name, false, false, true);
    }

    /* Copy the plan's query string into the portal */
    query_string = MemoryContextStrdup(PortalGetHeapMemory(portal), plansource->query_string);

    /*
     * Setup error traceback support for ereport(), in case GetCachedPlan
     * throws an error.
     */
    spi_err_context.callback = _SPI_error_callback;
    spi_err_context.arg = (void *)plansource->query_string;
    spi_err_context.previous = t_thrd.log_cxt.error_context_stack;
    t_thrd.log_cxt.error_context_stack = &spi_err_context;

    /*
     * Note: for a saved plan, we mustn't have any failure occur between
     * GetCachedPlan and PortalDefineQuery; that would result in leaking our
     * plancache refcount.
     */
    /* Replan if needed, and increment plan refcount for portal */
    CachedPlan* cplan = GetCachedPlan(plansource, paramLI, false);

    if (ENABLE_GPC && plan->saved && plansource->gplan) {
        stmt_list = CopyLocalStmt(cplan->stmt_list, u_sess->top_portal_cxt, &portal->copyCxt);
    } else {
        stmt_list = cplan->stmt_list;
    }

    /* Pop the error context stack */
    t_thrd.log_cxt.error_context_stack = spi_err_context.previous;

    if (!plan->saved) {
        /*
         * We don't want the portal to depend on an unsaved CachedPlanSource,
         * so must copy the plan into the portal's context.  An error here
         * will result in leaking our refcount on the plan, but it doesn't
         * matter because the plan is unsaved and hence transient anyway.
         */
        old_ctx = MemoryContextSwitchTo(PortalGetHeapMemory(portal));
        stmt_list = (List *)copyObject(stmt_list);
        (void)MemoryContextSwitchTo(old_ctx);
        ReleaseCachedPlan(cplan, false);
        cplan = NULL; /* portal shouldn't depend on cplan */
    }

    /*
     * Set up the portal.
     */
    PortalDefineQuery(portal, NULL, /* no statement name */
        query_string, plansource->commandTag, stmt_list, cplan);
    portal->nextval_default_expr_type = plansource->nextval_default_expr_type;

    /*
     * Set up options for portal.  Default SCROLL type is chosen the same way
     * as PerformCursorOpen does it.
     */
    portal->cursorOptions = plan->cursor_options;
    if (!(portal->cursorOptions & (CURSOR_OPT_SCROLL | CURSOR_OPT_NO_SCROLL))) {
        if (list_length(stmt_list) == 1 && IsA((Node *)linitial(stmt_list), PlannedStmt) &&
            ((PlannedStmt *)linitial(stmt_list))->rowMarks == NIL &&
            ExecSupportsBackwardScan(((PlannedStmt *)linitial(stmt_list))->planTree)) {
            portal->cursorOptions |= CURSOR_OPT_SCROLL;
        } else {
            portal->cursorOptions |= CURSOR_OPT_NO_SCROLL;
        }
    }

    /*
     * Disallow SCROLL with SELECT FOR UPDATE.	This is not redundant with the
     * check in transformDeclareCursorStmt because the cursor options might
     * not have come through there.
     */
    if (portal->cursorOptions & CURSOR_OPT_SCROLL) {
        if (list_length(stmt_list) == 1 && IsA((Node *)linitial(stmt_list), PlannedStmt) &&
            ((PlannedStmt *)linitial(stmt_list))->rowMarks != NIL) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("DECLARE SCROLL CURSOR ... FOR UPDATE/SHARE is not supported"),
                errdetail("Scrollable cursors must be READ ONLY.")));
        }
    }

    /*
     * If told to be read-only, we'd better check for read-only queries. This
     * can't be done earlier because we need to look at the finished, planned
     * queries.  (In particular, we don't want to do it between GetCachedPlan
     * and PortalDefineQuery, because throwing an error between those steps
     * would result in leaking our plancache refcount.)
     */
    if (read_only) {
        ListCell *lc = NULL;

        foreach (lc, stmt_list) {
            Node *pstmt = (Node *)lfirst(lc);

            if (!CommandIsReadOnly(pstmt)) {
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    /* translator: %s is a SQL statement name */
                    errmsg("%s is not allowed in a non-volatile function", CreateCommandTag(pstmt)),
                    errhint("You can change function definition.")));
            }
        }
    }

    /* Set up the snapshot to use. */
    if (read_only) {
        snapshot = GetActiveSnapshot();
    } else {
        CommandCounterIncrement();
        snapshot = GetTransactionSnapshot();
    }

#ifdef ENABLE_MULTIPLE_NODES
    if (isCollectParam && checkCommandTag(portal->commandTag) && checkPlan(portal->stmts)) {
        collectDynWithArgs(query_string, paramLI, portal->cursorOptions);
    }
    /* check plan if has stream */
    check_portal_stream(portal);
#endif

    /*
     * If the plan has parameters, copy them into the portal.  Note that this
     * must be done after revalidating the plan, because in dynamic parameter
     * cases the set of parameters could have changed during re-parsing.
     */
    if (paramLI) {
        old_ctx = MemoryContextSwitchTo(PortalGetHeapMemory(portal));
        paramLI = copyParamList(paramLI);
        (void)MemoryContextSwitchTo(old_ctx);
    }

    /*
     * Start portal execution.
     */
    PortalStart(portal, paramLI, 0, snapshot);

    Assert(portal->strategy != PORTAL_MULTI_QUERY);

    /* Pop the SPI stack */
    SPI_STACK_LOG("end", NULL, plan);

    /* reset flag */
    u_sess->SPI_cxt.has_stream_in_cursor_or_forloop_sql = false;
    t_thrd.postgres_cxt.cur_command_tag = old_node_tag;

    _SPI_end_call(true);

    /* Return the created portal */
    return portal;
}

/*
 * SPI_cursor_find
 *
 * 	Find the portal of an existing open cursor
 */
Portal SPI_cursor_find(const char *name)
{
    return GetPortalByName(name);
}

/*
 * SPI_cursor_fetch
 *
 * 	Fetch rows in a cursor
 */
void SPI_cursor_fetch(Portal portal, bool forward, long count)
{
    _SPI_cursor_operation(portal, forward ? FETCH_FORWARD : FETCH_BACKWARD, count, CreateDestReceiver(DestSPI));
    /* we know that the DestSPI receiver doesn't need a destroy call */
}

/*
 * SPI_cursor_move
 *
 * 	Move in a cursor
 */
void SPI_cursor_move(Portal portal, bool forward, long count)
{
    _SPI_cursor_operation(portal, forward ? FETCH_FORWARD : FETCH_BACKWARD, count, None_Receiver);
}

/*
 * SPI_scroll_cursor_fetch
 *
 * 	Fetch rows in a scrollable cursor
 */
void SPI_scroll_cursor_fetch(Portal portal, FetchDirection direction, long count)
{
    _SPI_cursor_operation(portal, direction, count, CreateDestReceiver(DestSPI));
    /* we know that the DestSPI receiver doesn't need a destroy call */
}

/*
 * SPI_scroll_cursor_move
 *
 * 	Move in a scrollable cursor
 */
void SPI_scroll_cursor_move(Portal portal, FetchDirection direction, long count)
{
    _SPI_cursor_operation(portal, direction, count, None_Receiver);
}

/*
 * SPI_cursor_close
 *
 * 	Close a cursor
 */
void SPI_cursor_close(Portal portal)
{
    if (!PortalIsValid(portal)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_CURSOR_STATE), errmsg("invalid portal in SPI cursor close operation")));
    }

    PortalDrop(portal, false);
}

/*
 * Returns the Oid representing the type id for argument at argIndex. First
 * parameter is at index zero.
 */
Oid SPI_getargtypeid(SPIPlanPtr plan, int argIndex)
{
    if (plan == NULL || plan->magic != _SPI_PLAN_MAGIC || argIndex < 0 || argIndex >= plan->nargs) {
        SPI_result = SPI_ERROR_ARGUMENT;
        return InvalidOid;
    }
    return plan->argtypes[argIndex];
}

/*
 * Returns the number of arguments for the prepared plan.
 */
int SPI_getargcount(SPIPlanPtr plan)
{
    if (plan == NULL || plan->magic != _SPI_PLAN_MAGIC) {
        SPI_result = SPI_ERROR_ARGUMENT;
        return -1;
    }
    return plan->nargs;
}

/*
 * Returns true if the plan contains exactly one command
 * and that command returns tuples to the caller (eg, SELECT or
 * INSERT ... RETURNING, but not SELECT ... INTO). In essence,
 * the result indicates if the command can be used with SPI_cursor_open
 *
 * Parameters
 * 	  plan: A plan previously prepared using SPI_prepare
 * 	 paramLI: hook function and possibly data values for plan
 */
bool SPI_is_cursor_plan(SPIPlanPtr plan, ParamListInfo paramLI)
{
    CachedPlanSource *plan_source = NULL;

    if (plan == NULL || plan->magic != _SPI_PLAN_MAGIC) {
        SPI_result = SPI_ERROR_ARGUMENT;
        return false;
    }

    if (list_length(plan->plancache_list) != 1) {
        SPI_result = 0;
        return false; /* not exactly 1 pre-rewrite command */
    }
    plan_source = (CachedPlanSource *)linitial(plan->plancache_list);

    /*
     * We used to force revalidation of the cached plan here, but that seems
     * unnecessary: invalidation could mean a change in the rowtype of the
     * tuples returned by a plan, but not whether it returns tuples at all.
     */
    SPI_result = 0;

    /* Does it return tuples? */
    if (plan_source->resultDesc) {
        CachedPlan *cplan = NULL;
        cplan = GetCachedPlan(plan_source, paramLI, false);
        if (cplan->isShared())
            (void)pg_atomic_fetch_add_u32((volatile uint32*)&cplan->global_refcount, 1);
        PortalStrategy strategy = ChoosePortalStrategy(cplan->stmt_list);
        ReleaseCachedPlan(cplan, false);
        if (strategy == PORTAL_MULTI_QUERY) {
            return false;
        } else {
            return true;
        }
    }

    return false;
}

/*
 * SPI_plan_is_valid --- test whether a SPI plan is currently valid
 * (that is, not marked as being in need of revalidation).
 *
 * See notes for CachedPlanIsValid before using this.
 */
bool SPI_plan_is_valid(SPIPlanPtr plan)
{
    ListCell *lc = NULL;

    Assert(plan->magic == _SPI_PLAN_MAGIC);

    foreach (lc, plan->plancache_list) {
        CachedPlanSource *plansource = (CachedPlanSource *)lfirst(lc);

        if (!CachedPlanIsValid(plansource)) {
            return false;
        }
    }
    return true;
}

/*
 * SPI_result_code_string --- convert any SPI return code to a string
 *
 * This is often useful in error messages.	Most callers will probably
 * only pass negative (error-case) codes, but for generality we recognize
 * the success codes too.
 */
const char* SPI_result_code_string(int code)
{
    char *buf = u_sess->SPI_cxt.buf;

    switch (code) {
        case SPI_ERROR_CONNECT:
            return "SPI_ERROR_CONNECT";
        case SPI_ERROR_COPY:
            return "SPI_ERROR_COPY";
        case SPI_ERROR_OPUNKNOWN:
            return "SPI_ERROR_OPUNKNOWN";
        case SPI_ERROR_UNCONNECTED:
            return "SPI_ERROR_UNCONNECTED";
        case SPI_ERROR_ARGUMENT:
            return "SPI_ERROR_ARGUMENT";
        case SPI_ERROR_PARAM:
            return "SPI_ERROR_PARAM";
        case SPI_ERROR_TRANSACTION:
            return "SPI_ERROR_TRANSACTION";
        case SPI_ERROR_NOATTRIBUTE:
            return "SPI_ERROR_NOATTRIBUTE";
        case SPI_ERROR_NOOUTFUNC:
            return "SPI_ERROR_NOOUTFUNC";
        case SPI_ERROR_TYPUNKNOWN:
            return "SPI_ERROR_TYPUNKNOWN";
        case SPI_OK_CONNECT:
            return "SPI_OK_CONNECT";
        case SPI_OK_FINISH:
            return "SPI_OK_FINISH";
        case SPI_OK_FETCH:
            return "SPI_OK_FETCH";
        case SPI_OK_UTILITY:
            return "SPI_OK_UTILITY";
        case SPI_OK_SELECT:
            return "SPI_OK_SELECT";
        case SPI_OK_SELINTO:
            return "SPI_OK_SELINTO";
        case SPI_OK_INSERT:
            return "SPI_OK_INSERT";
        case SPI_OK_DELETE:
            return "SPI_OK_DELETE";
        case SPI_OK_UPDATE:
            return "SPI_OK_UPDATE";
        case SPI_OK_CURSOR:
            return "SPI_OK_CURSOR";
        case SPI_OK_INSERT_RETURNING:
            return "SPI_OK_INSERT_RETURNING";
        case SPI_OK_DELETE_RETURNING:
            return "SPI_OK_DELETE_RETURNING";
        case SPI_OK_UPDATE_RETURNING:
            return "SPI_OK_UPDATE_RETURNING";
        case SPI_OK_REWRITTEN:
            return "SPI_OK_REWRITTEN";
        default:
            break;
    }
    /* Unrecognized code ... return something useful ... */
    errno_t sret = sprintf_s(buf, BUFLEN, "Unrecognized SPI code %d", code);
    securec_check_ss(sret, "\0", "\0");

    return buf;
}

/*
 * SPI_plan_get_plan_sources --- get a SPI plan's underlying list of
 * CachedPlanSources.
 *
 * This is exported so that pl/pgsql can use it (this beats letting pl/pgsql
 * look directly into the SPIPlan for itself).  It's not documented in
 * spi.sgml because we'd just as soon not have too many places using this.
 */
List *SPI_plan_get_plan_sources(SPIPlanPtr plan)
{
    Assert(plan->magic == _SPI_PLAN_MAGIC);
    return plan->plancache_list;
}

/*
 * SPI_plan_get_cached_plan --- get a SPI plan's generic CachedPlan,
 * if the SPI plan contains exactly one CachedPlanSource.  If not,
 * return NULL.  Caller is responsible for doing ReleaseCachedPlan().
 *
 * This is exported so that pl/pgsql can use it (this beats letting pl/pgsql
 * look directly into the SPIPlan for itself).  It's not documented in
 * spi.sgml because we'd just as soon not have too many places using this.
 */
CachedPlan* SPI_plan_get_cached_plan(SPIPlanPtr plan)
{
    CachedPlanSource *plan_source = NULL;
    CachedPlan *cplan = NULL;
    ErrorContextCallback spi_err_context;

    Assert(plan->magic == _SPI_PLAN_MAGIC);

    /* Can't support one-shot plans here */
    if (plan->oneshot) {
        return NULL;
    }

    /* Must have exactly one CachedPlanSource */
    if (list_length(plan->plancache_list) != 1) {
        return NULL;
    }
    plan_source = (CachedPlanSource *)linitial(plan->plancache_list);

    /* Setup error traceback support for ereport() */
    spi_err_context.callback = _SPI_error_callback;
    spi_err_context.arg = (void *)plan_source->query_string;
    spi_err_context.previous = t_thrd.log_cxt.error_context_stack;
    t_thrd.log_cxt.error_context_stack = &spi_err_context;

#ifndef ENABLE_MULTIPLE_NODES
    AutoDopControl dopControl;
    dopControl.CloseSmp();
#endif

    /* Get the generic plan for the query */
    cplan = GetCachedPlan(plan_source, NULL, plan->saved);
    if (!ENABLE_CACHEDPLAN_MGR) {
        Assert(cplan == plan_source->gplan);
    }
    if (cplan->isShared())
        (void)pg_atomic_fetch_add_u32((volatile uint32*)&cplan->global_refcount, 1);

    /* Pop the error context stack */
    t_thrd.log_cxt.error_context_stack = spi_err_context.previous;

    return cplan;
}

/* =================== private functions =================== */
static void spi_check_connid()
{
    /*
     * When called by Executor u_sess->SPI_cxt._curid expected to be equal to
     * u_sess->SPI_cxt._connected
     */
    if (u_sess->SPI_cxt._curid != u_sess->SPI_cxt._connected || u_sess->SPI_cxt._connected < 0) {
        ereport(ERROR, (errcode(ERRORCODE_SPI_IMPROPER_CALL),
            errmsg("SPI stack level is corrupted when checking SPI id, current level: %d, connected level: %d",
            u_sess->SPI_cxt._curid, u_sess->SPI_cxt._connected)));
    }

    if (u_sess->SPI_cxt._current != &(u_sess->SPI_cxt._stack[u_sess->SPI_cxt._curid])) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("SPI stack is corrupted when checking SPI id.")));
    }
}

/*
 * spi_dest_startup
 * 		Initialize to receive tuples from Executor into SPITupleTable
 * 		of current SPI procedure
 */
void spi_dest_startup(DestReceiver *self, int operation, TupleDesc typeinfo)
{
    SPITupleTable *tuptable = NULL;
    MemoryContext old_ctx;
    MemoryContext tup_tab_cxt;

    spi_check_connid();

    if (u_sess->SPI_cxt._current->tuptable != NULL) {
        ereport(ERROR,
            (errcode(ERRORCODE_SPI_IMPROPER_CALL), errmsg("SPI tupletable is not cleaned when initializing SPI.")));
    }

    old_ctx = _SPI_procmem(); /* switch to procedure memory context */

    tup_tab_cxt = AllocSetContextCreate(CurrentMemoryContext, "SPI TupTable", ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    (void)MemoryContextSwitchTo(tup_tab_cxt);

    u_sess->SPI_cxt._current->tuptable = tuptable = (SPITupleTable *)palloc(sizeof(SPITupleTable));
    tuptable->tuptabcxt = tup_tab_cxt;

    if (DestSPI == self->mydest) {
        tuptable->alloced = tuptable->free = 128;
    } else {
        tuptable->alloced = tuptable->free = DEFAULT_SAMPLE_ROWCNT;
    }

    tuptable->vals = (HeapTuple *)palloc(tuptable->alloced * sizeof(HeapTuple));
    tuptable->tupdesc = CreateTupleDescCopy(typeinfo);

    (void)MemoryContextSwitchTo(old_ctx);
}

/*
 * spi_printtup
 * 		store tuple retrieved by Executor into SPITupleTable
 * 		of current SPI procedure
 */
void spi_printtup(TupleTableSlot *slot, DestReceiver *self)
{
    SPITupleTable *tuptable = NULL;
    MemoryContext old_ctx;
    HeapTuple tuple;

    spi_check_connid();
    tuptable = u_sess->SPI_cxt._current->tuptable;
    if (tuptable == NULL) {
        ereport(ERROR, (errcode(ERRORCODE_SPI_IMPROPER_CALL), errmsg("tuple is NULL when store to SPI tupletable.")));
    }

    old_ctx = MemoryContextSwitchTo(tuptable->tuptabcxt);

    if (tuptable->free == 0) {
        /* Double the size of the pointer array */
        tuptable->free = tuptable->alloced;
        tuptable->alloced += tuptable->free;
        tuptable->vals = (HeapTuple *)repalloc(tuptable->vals, tuptable->alloced * sizeof(HeapTuple));
    }

    tuple = ExecCopySlotTuple(slot);
    /* check ExecCopySlotTuple result */
    if (tuple == NULL) {
        ereport(WARNING, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
            (errmsg("slot tuple copy failed, unexpexted return value. Maybe the slot tuple is invalid or tuple "
            "data is null "))));
    }

    tuptable->vals[tuptable->alloced - tuptable->free] = tuple;

    (tuptable->free)--;

    (void)MemoryContextSwitchTo(old_ctx);
}

/*
 * Static functions
 */
#ifdef ENABLE_MOT
static bool _SPI_prepare_plan_guarded(const char *src, SPIPlanPtr plan, parse_query_func parser)
{
    bool result = true;
    PG_TRY();
    {
        _SPI_prepare_plan(src, plan, parser);
    }
    PG_CATCH();
    {
        // report error and reset error state, but first switch back to executor memory context
        _SPI_execmem();
        ErrorData* edata = CopyErrorData();
        JitExec::JitReportParseError(edata, src);
        FlushErrorState();
        FreeErrorData(edata);
        result = false;
    }
    PG_END_TRY();
    return result;
}
#endif

/*
 * Parse and analyze a querystring.
 *
 * At entry, plan->argtypes and plan->nargs (or alternatively plan->parserSetup
 * and plan->parserSetupArg) must be valid, as must plan->cursor_options.
 *
 * Results are stored into *plan (specifically, plan->plancache_list).
 * Note that the result data is all in CurrentMemoryContext or child contexts
 * thereof; in practice this means it is in the SPI executor context, and
 * what we are creating is a "temporary" SPIPlan.  Cruft generated during
 * parsing is also left in CurrentMemoryContext.
 */
void _SPI_prepare_plan(const char *src, SPIPlanPtr plan, parse_query_func parser)
{
#ifdef PGXC
    _SPI_pgxc_prepare_plan(src, NULL, plan, parser);
}

/*
 * _SPI_pgxc_prepare_plan: Optionally accepts a parsetree which allows it to
 * bypass the parse phase, and directly analyse, rewrite and plan. Meant to be
 * called for internally executed execute-direct statements that are
 * transparent to the user.
 */
static void _SPI_pgxc_prepare_plan(const char *src, List *src_parsetree, SPIPlanPtr plan, parse_query_func parser)
{
#endif
    List *raw_parsetree_list = NIL;
    List *plancache_list = NIL;
    ListCell *list_item = NULL;
    ErrorContextCallback spi_err_context;

    /*
     * Setup error traceback support for ereport()
     */
    spi_err_context.callback = _SPI_error_callback;
    spi_err_context.arg = (void *)src;
    spi_err_context.previous = t_thrd.log_cxt.error_context_stack;
    t_thrd.log_cxt.error_context_stack = &spi_err_context;
    NodeTag old_node_tag = t_thrd.postgres_cxt.cur_command_tag;

    /*
     * Parse the request string into a list of raw parse trees.
     */
#ifdef PGXC
    /* Parse it only if there isn't an already parsed tree passed */
    if (src_parsetree != NIL)
        raw_parsetree_list = src_parsetree;
    else
#endif
        raw_parsetree_list = pg_parse_query(src, NULL, parser);
    /*
     * Do parse analysis and rule rewrite for each raw parsetree, storing the
     * results into unsaved plancache entries.
     */
    plancache_list = NIL;
    int i = 0;
    bool enable_spi_gpc = false;
    foreach (list_item, raw_parsetree_list) {
        Node *parsetree = (Node *)lfirst(list_item);
        List *stmt_list = NIL;
        CachedPlanSource *plansource = NULL;
        t_thrd.postgres_cxt.cur_command_tag = transform_node_tag(parsetree);
        // get cachedplan if has any
        enable_spi_gpc = false;
        if (ENABLE_CN_GPC && u_sess->SPI_cxt._current->spi_hash_key != INVALID_SPI_KEY
            && u_sess->SPI_cxt._current->visit_id != (uint32)-1) {
            enable_spi_gpc = SPIParseEnableGPC(parsetree);
        }
        enable_spi_gpc = false;
        if (enable_spi_gpc) {
            Assert(src_parsetree == NIL);
            Assert(plan->oneshot == false);
            u_sess->SPI_cxt._current->plan_id = i;
            SPISign spi_signature;
            spi_signature.spi_key = u_sess->SPI_cxt._current->spi_hash_key;
            spi_signature.func_oid = u_sess->SPI_cxt._current->func_oid;
            spi_signature.spi_id = u_sess->SPI_cxt._current->visit_id;
            spi_signature.plansource_id = u_sess->SPI_cxt._current->plan_id;
            plansource = g_instance.plan_cache->Fetch(src, (uint32)strlen(src), plan->nargs,
                                                      plan->argtypes, &spi_signature);
            if (plansource) {
                Assert(plansource->is_oneshot == false);
                Assert(plansource->gpc.status.IsSharePlan());
                plancache_list = lappend(plancache_list, plansource);
                i++;
                // get param into expr
                ParseState* pstate = NULL;
                pstate = make_parsestate(NULL);
                pstate->p_sourcetext = src;
                (*plan->parserSetup)(pstate, plan->parserSetupArg);
                (void*)transformTopLevelStmt(pstate, parsetree);
                free_parsestate(pstate);
                /* set spiplan id and spi_key if plancache is shared */
                plan->id = u_sess->SPI_cxt._current->visit_id;
                plan->spi_key = u_sess->SPI_cxt._current->spi_hash_key;
                continue;
            }
        }

        /*
         * Create the CachedPlanSource before we do parse analysis, since it
         * needs to see the unmodified raw parse tree.
         */
        plansource = CreateCachedPlan(parsetree, src,
#ifdef PGXC
            NULL,
#endif
            CreateCommandTag(parsetree), enable_spi_gpc);

        /*
         * Parameter datatypes are driven by parserSetup hook if provided,
         * otherwise we use the fixed parameter list.
         */
        if (plan->parserSetup != NULL) {
            Assert(plan->nargs == 0);
            stmt_list = pg_analyze_and_rewrite_params(parsetree, src, plan->parserSetup, plan->parserSetupArg);
        } else {
            stmt_list = pg_analyze_and_rewrite(parsetree, src, plan->argtypes, plan->nargs);
        }
        plan->stmt_list = list_concat(plan->stmt_list, list_copy(stmt_list));
        /* Finish filling in the CachedPlanSource */
        CompleteCachedPlan(plansource, stmt_list, NULL, plan->argtypes, NULL, plan->nargs, plan->parserSetup,
            plan->parserSetupArg, plan->cursor_options, false, /* not fixed result */
            "");

        if (enable_spi_gpc && plansource->gpc.status.IsSharePlan()) {
            /* for needRecompilePlan, plansource need recreate each time, no need to global it.
             * for temp table, only one session can use it, no need to global it */
            bool need_recreate_everytime = checkRecompileCondition(plansource);
            bool has_tmp_table = false;
            ListCell* cell = NULL;
            foreach(cell, stmt_list) {
                Query* query = (Query*)lfirst(cell);
                if (contains_temp_tables(query->rtable)) {
                    has_tmp_table = true;
                    break;
                }
            }
            if (need_recreate_everytime || has_tmp_table) {
                plansource->gpc.status.SetKind(GPC_UNSHARED);
            } else {
                /* set spiplan id and spi_key if plancache is shared */
                plan->id = u_sess->SPI_cxt._current->visit_id;
                plan->spi_key = u_sess->SPI_cxt._current->spi_hash_key;
            }
        }
        plancache_list = lappend(plancache_list, plansource);
        i++;
    }

    plan->plancache_list = plancache_list;
    plan->oneshot = false;
    u_sess->SPI_cxt._current->plan_id = -1;
    t_thrd.postgres_cxt.cur_command_tag = old_node_tag;

    /*
     * Pop the error context stack
     */
    t_thrd.log_cxt.error_context_stack = spi_err_context.previous;
}

static void SPIParseOneShotPlan(CachedPlanSource* plansource, SPIPlanPtr plan)
{
    Node *parsetree = plansource->raw_parse_tree;
    const char *src = plansource->query_string;
    List *statement_list = NIL;

    if (!plansource->is_complete) {
        /*
         * Parameter datatypes are driven by parserSetup hook if provided,
         * otherwise we use the fixed parameter list.
         */
        if (plan->parserSetup != NULL) {
            Assert(plan->nargs == 0);
            statement_list = pg_analyze_and_rewrite_params(parsetree, src, plan->parserSetup, plan->parserSetupArg);
        } else {
            statement_list = pg_analyze_and_rewrite(parsetree, src, plan->argtypes, plan->nargs);
        }

        /* Finish filling in the CachedPlanSource */
        CompleteCachedPlan(plansource, statement_list, NULL, plan->argtypes, NULL, plan->nargs, plan->parserSetup,
                           plan->parserSetupArg, plan->cursor_options, false, ""); /* not fixed result */
    }
}

/*
 * Parse, but don't analyze, a querystring.
 *
 * This is a stripped-down version of _SPI_prepare_plan that only does the
 * initial raw parsing.  It creates "one shot" CachedPlanSources
 * that still require parse analysis before execution is possible.
 *
 * The advantage of using the "one shot" form of CachedPlanSource is that
 * we eliminate data copying and invalidation overhead.  Postponing parse
 * analysis also prevents issues if some of the raw parsetrees are DDL
 * commands that affect validity of later parsetrees.  Both of these
 * attributes are good things for SPI_execute() and similar cases.
 *
 * Results are stored into *plan (specifically, plan->plancache_list).
 * Note that the result data is all in CurrentMemoryContext or child contexts
 * thereof; in practice this means it is in the SPI executor context, and
 * what we are creating is a "temporary" SPIPlan.  Cruft generated during
 * parsing is also left in CurrentMemoryContext.
 */
void _SPI_prepare_oneshot_plan(const char *src, SPIPlanPtr plan, parse_query_func parser)
{
    List *raw_parsetree_list = NIL;
    List *plancache_list = NIL;
    ListCell *list_item = NULL;
    ErrorContextCallback spi_err_context;
    List *query_string_locationlist = NIL;
    int stmt_num = 0;
    NodeTag old_node_tag = t_thrd.postgres_cxt.cur_command_tag;
    /*
     * Setup error traceback support for ereport()
     */
    spi_err_context.callback = _SPI_error_callback;
    spi_err_context.arg = (void *)src;
    spi_err_context.previous = t_thrd.log_cxt.error_context_stack;
    t_thrd.log_cxt.error_context_stack = &spi_err_context;

    /*
     * Parse the request string into a list of raw parse trees.
     */
    raw_parsetree_list = pg_parse_query(src, &query_string_locationlist, parser);

    /*
     * Construct plancache entries, but don't do parse analysis yet.
     */
    plancache_list = NIL;
    char **query_string_single = NULL;

    foreach (list_item, raw_parsetree_list) {
        Node *parsetree = (Node *)lfirst(list_item);
        CachedPlanSource *plansource = NULL;
        t_thrd.postgres_cxt.cur_command_tag = transform_node_tag(parsetree);

#ifndef ENABLE_MULTIPLE_NODES
        if (g_instance.attr.attr_sql.enableRemoteExcute) {
            libpqsw_check_ddl_on_primary(CreateCommandTag(parsetree));
        }
#endif

#ifdef ENABLE_MULTIPLE_NODES
        if (IS_PGXC_COORDINATOR && PointerIsValid(query_string_locationlist) &&
            list_length(query_string_locationlist) > 1) {
#else
        if (PointerIsValid(query_string_locationlist) && list_length(query_string_locationlist) > 1) {
#endif
            query_string_single = get_next_snippet(query_string_single, src, query_string_locationlist, &stmt_num);
            plansource =
                CreateOneShotCachedPlan(parsetree, query_string_single[stmt_num - 1], CreateCommandTag(parsetree));
        } else {
            plansource = CreateOneShotCachedPlan(parsetree, src, CreateCommandTag(parsetree));
        }

        plancache_list = lappend(plancache_list, plansource);
    }

    plan->plancache_list = plancache_list;
    plan->oneshot = true;
    t_thrd.postgres_cxt.cur_command_tag = old_node_tag;

    /*
     * Pop the error context stack
     */
    t_thrd.log_cxt.error_context_stack = spi_err_context.previous;
}

bool RememberSpiPlanRef(CachedPlan* cplan, CachedPlanSource* plansource)
{
    bool ans = false;
    /* incase commit/rollback release cachedplan from resource owner during execute spi plan,
     * make sure current spi has cplan. So without commit/rollback, spi should has 2 refcount on cplan */
    if (cplan->isShared()) {
        (void)pg_atomic_fetch_add_u32((volatile uint32*)&cplan->global_refcount, 1);
        ans = true;
    } else if (!plansource->is_oneshot) {
        cplan->refcount++;
        ans = true;
    }
    if (ans) {
        SPICachedPlanStack* cur_spi_cplan = (SPICachedPlanStack*)MemoryContextAlloc(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR), sizeof(SPICachedPlanStack));
        cur_spi_cplan->previous = u_sess->SPI_cxt.spi_exec_cplan_stack;
        cur_spi_cplan->cplan = cplan;
        cur_spi_cplan->subtranid = GetCurrentSubTransactionId();
        u_sess->SPI_cxt.spi_exec_cplan_stack = cur_spi_cplan;
    }
    return ans;
}

void ForgetSpiPlanRef()
{
    if (u_sess->SPI_cxt.spi_exec_cplan_stack == NULL)
        return;
    SPICachedPlanStack* cur_spi_cplan = u_sess->SPI_cxt.spi_exec_cplan_stack;
    CachedPlan* cplan = cur_spi_cplan->cplan;
    u_sess->SPI_cxt.spi_exec_cplan_stack = cur_spi_cplan->previous;
    pfree_ext(cur_spi_cplan);
    ReleaseCachedPlan(cplan, false);
}

ResourceOwner AddCplanRefAgainIfNecessary(SPIPlanPtr plan,
    CachedPlanSource* plansource, CachedPlan* cplan, TransactionId oldTransactionId, ResourceOwner oldOwner)
{
    /*
     * When commit/rollback occurs, or its subtransaction is finished by savepoint, except
     * for the oneshot plan, plan cache's refcount has already been released in resourceowner.
     *  To match subsequent processing, an extra increment about reference is required here.
     */
    if ((oldTransactionId != SPI_get_top_transaction_id() ||
        !ResourceOwnerIsValid(oldOwner)) && !plansource->is_oneshot) {
        /* need addrefcount and save into resource owner again */
        if (plan->saved)
            ResourceOwnerEnlargePlanCacheRefs(t_thrd.utils_cxt.CurrentResourceOwner);
        if (!cplan->isShared()) {
            cplan->refcount++;
        } else {
            (void)pg_atomic_fetch_add_u32((volatile uint32*)&cplan->global_refcount, 1);
        }
        if (plan->saved)
            ResourceOwnerRememberPlanCacheRef(t_thrd.utils_cxt.CurrentResourceOwner, cplan);

        return plan->saved ? t_thrd.utils_cxt.CurrentResourceOwner : oldOwner;
    }

    return oldOwner;
}

void FreeMultiQueryString(SPIPlanPtr plan)
{
#ifdef ENABLE_MULTIPLE_NODES
    if (IS_PGXC_COORDINATOR && PointerIsValid(plan->plancache_list) && list_length(plan->plancache_list) > 1) {
#else
    if (plan->oneshot && PointerIsValid(plan->plancache_list) && list_length(plan->plancache_list) > 1) {
#endif
        ListCell *list_item = NULL;

        foreach (list_item, plan->plancache_list) {
            CachedPlanSource *PlanSource = (CachedPlanSource *)lfirst(list_item);
            if (PlanSource->query_string != NULL) {
                pfree_ext((PlanSource->query_string));
                PlanSource->query_string = NULL;
            }
        }
    }
}

/*
 * Execute the given plan with the given parameter values
 *
 * snapshot: query snapshot to use, or InvalidSnapshot for the normal
 * 		behavior of taking a new snapshot for each query.
 * crosscheck_snapshot: for RI use, all others pass InvalidSnapshot
 * read_only: TRUE for read-only execution (no CommandCounterIncrement)
 * fire_triggers: TRUE to fire AFTER triggers at end of query (normal case);
 * 		FALSE means any AFTER triggers are postponed to end of outer query
 * tcount: execution tuple-count limit, or 0 for none
 */
static int _SPI_execute_plan0(SPIPlanPtr plan, ParamListInfo paramLI, Snapshot snapshot, Snapshot crosscheck_snapshot,
    bool read_only, bool fire_triggers, long tcount, bool from_lock)
{
    int my_res = 0;
    uint32 my_processed = 0;
    Oid my_lastoid = InvalidOid;
    MemoryContext tmp_cxt = NULL;
    SPITupleTable *my_tuptable = NULL;
    int res = 0;
    bool pushed_active_snap = false;
    ErrorContextCallback spi_err_context;
    CachedPlan *cplan = NULL;
    ListCell *lc1 = NULL;
    bool tmp_enable_light_proxy = u_sess->attr.attr_sql.enable_light_proxy;
    NodeTag old_command_tag = t_thrd.postgres_cxt.cur_command_tag;
    TransactionId oldTransactionId = SPI_get_top_transaction_id();
    bool need_remember_cplan = false;

    /*
     * With savepoint in STP feature, CurrentResourceOwner is different before and after _SPI_pquery.
     * We need to hold the original one in order to forget the snapshot, plan reference.and etc.
     */
    ResourceOwner oldOwner = t_thrd.utils_cxt.CurrentResourceOwner;

    /* not allow Light CN */
    u_sess->attr.attr_sql.enable_light_proxy = false;

    /*
     * Setup error traceback support for ereport()
     */
    spi_err_context.callback = _SPI_error_callback;
    spi_err_context.arg = NULL; /* we'll fill this below */
    spi_err_context.previous = t_thrd.log_cxt.error_context_stack;
    t_thrd.log_cxt.error_context_stack = &spi_err_context;

    /*
     * We support four distinct snapshot management behaviors:
     *
     * snapshot != InvalidSnapshot, read_only = true: use exactly the given
     * snapshot.
     *
     * snapshot != InvalidSnapshot, read_only = false: use the given snapshot,
     * modified by advancing its command ID before each querytree.
     *
     * snapshot == InvalidSnapshot, read_only = true: use the entry-time
     * ActiveSnapshot, if any (if there isn't one, we run with no snapshot).
     *
     * snapshot == InvalidSnapshot, read_only = false: take a full new
     * snapshot for each user command, and advance its command ID before each
     * querytree within the command.
     *
     * In the first two cases, we can just push the snap onto the stack once
     * for the whole plan list.
     */
    if (snapshot != InvalidSnapshot) {
        if (read_only) {
            PushActiveSnapshot(snapshot);
            pushed_active_snap = true;
        } else {
            /* Make sure we have a private copy of the snapshot to modify */
            PushCopiedSnapshot(snapshot);
            pushed_active_snap = true;
        }
    }

#ifndef ENABLE_MULTIPLE_NODES
    AutoDopControl dopControl;
    dopControl.CloseSmp();
#endif

    foreach (lc1, plan->plancache_list) {
        CachedPlanSource *plansource = (CachedPlanSource *)lfirst(lc1);
        List *stmt_list = NIL;

        spi_err_context.arg = (void *)plansource->query_string;
        t_thrd.postgres_cxt.cur_command_tag = transform_node_tag(plansource->raw_parse_tree);

        /*
         * If this is a one-shot plan, we still need to do parse analysis.
         */
        if (plan->oneshot) {
            SPIParseOneShotPlan(plansource, plan);
        }

        /*
         * Replan if needed, and increment plan refcount.  If it's a saved
         * plan, the refcount must be backed by the CurrentResourceOwner.
         */
        cplan = GetCachedPlan(plansource, paramLI, plan->saved);

        /* use shared plan here, add refcount */
        if (cplan->isShared())
            (void)pg_atomic_fetch_add_u32((volatile uint32*)&cplan->global_refcount, 1);

        if (ENABLE_GPC && plansource->gplan)
            stmt_list = CopyLocalStmt(cplan->stmt_list, u_sess->SPI_cxt._current->execCxt, &tmp_cxt);
        else
            stmt_list = cplan->stmt_list;

        /*
         * In the default non-read-only case, get a new snapshot, replacing
         * any that we pushed in a previous cycle.
         */
        if (snapshot == InvalidSnapshot && !read_only) {
            if (pushed_active_snap) {
                PopActiveSnapshot();
            }
            PushActiveSnapshot(GetTransactionSnapshot());
            pushed_active_snap = true;
        }

        /*
         * Handle No Plans Here.
         * 1 release cplan.
         * 2 increase counter if needed
         */
        if (stmt_list == NIL) {
            ReleaseCachedPlan(cplan, plan->saved);
            cplan = NULL;

            if (!read_only) {
                CommandCounterIncrement();
            }

            continue;
        }

        need_remember_cplan = RememberSpiPlanRef(cplan, plansource);

        for (int i = 0; i < stmt_list->length; i++) {
            Node *stmt = (Node *)list_nth(stmt_list, i);
            bool canSetTag = false;

            u_sess->SPI_cxt._current->processed = 0;
            u_sess->SPI_cxt._current->lastoid = InvalidOid;
            u_sess->SPI_cxt._current->tuptable = NULL;

            if (IsA(stmt, PlannedStmt)) {
                canSetTag = ((PlannedStmt *)stmt)->canSetTag;

                if (((PlannedStmt*)stmt)->commandType != CMD_SELECT) {
                    SPI_forbid_exec_push_down_with_exception();
                }
            } else {
                /* utilities are canSetTag if only thing in list */
                canSetTag = (list_length(stmt_list) == 1);

                if (IsA(stmt, CopyStmt)) {
                    CopyStmt *cstmt = (CopyStmt *)stmt;

                    if (cstmt->filename == NULL) {
                        my_res = SPI_ERROR_COPY;
                        goto fail;
                    }
                } else if (IsA(stmt, TransactionStmt)
#ifndef ENABLE_MULTIPLE_NODES
                            && !u_sess->attr.attr_sql.dolphin
#endif
                ) {
                    my_res = SPI_ERROR_TRANSACTION;
                    goto fail;
                } 
            }

            if (read_only && !CommandIsReadOnly(stmt)) {
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    /* translator: %s is a SQL statement name */
                    errmsg("%s is not allowed in a non-volatile function", CreateCommandTag(stmt))));
            }

            /*
             * If not read-only mode, advance the command counter before each
             * command and update the snapshot.
             */
            if (!read_only) {
                CommandCounterIncrement();
                UpdateActiveSnapshotCommandId();
            }

            DestReceiver *dest = CreateDestReceiver(canSetTag ? u_sess->SPI_cxt._current->dest : DestNone);
            if (u_sess->SPI_cxt._current->dest == DestSqlProcSPI && u_sess->hook_cxt.pluginSpiReciverParamHook)
                ((SpiReciverParamHook)u_sess->hook_cxt.pluginSpiReciverParamHook)(dest,plan);

            if (IsA(stmt, PlannedStmt) && ((PlannedStmt *)stmt)->utilityStmt == NULL) {
                QueryDesc *qdesc = NULL;
                Snapshot snap = ActiveSnapshotSet() ? GetActiveSnapshot() : InvalidSnapshot;

#ifndef ENABLE_MULTIPLE_NODES
                Port *MyPort = u_sess->proc_cxt.MyProcPort; 
                if (MyPort && MyPort->protocol_config && MyPort->protocol_config->fn_set_DR_params) {
                    MyPort->protocol_config->fn_set_DR_params(dest, ((PlannedStmt *)stmt)->planTree->targetlist);
                }
#endif

#ifdef ENABLE_MOT
                qdesc = CreateQueryDesc((PlannedStmt *)stmt, plansource->query_string, snap, crosscheck_snapshot, dest,
                    paramLI, 0, plansource->mot_jit_context);
#else
                qdesc = CreateQueryDesc((PlannedStmt *)stmt, plansource->query_string, snap, crosscheck_snapshot, dest,
                    paramLI, 0);
#endif

                res = _SPI_pquery(qdesc, fire_triggers, canSetTag ? tcount : 0, from_lock);
                ResourceOwner tmp = t_thrd.utils_cxt.CurrentResourceOwner;
                t_thrd.utils_cxt.CurrentResourceOwner = oldOwner;
                FreeQueryDesc(qdesc);
                t_thrd.utils_cxt.CurrentResourceOwner = tmp;
            } else {
                char completionTag[COMPLETION_TAG_BUFSIZE];

                /*
                 * Reset schema name for analyze in stored procedure.
                 * When analyze has error, there is no time for schema name to be reseted.
                 * It will be kept in the plan for stored procedure and the result
                 * is uncertain.
                 */
                if (IsA(stmt, VacuumStmt)) {
                    ClearVacuumStmt((VacuumStmt *)stmt);
                }
                if (IsA(stmt, CreateSeqStmt)) {
                    ClearCreateSeqStmtUUID((CreateSeqStmt *)stmt);
                }
                if (IsA(stmt, CreateStmt)) {
                    ClearCreateStmtUUIDS((CreateStmt *)stmt);
                }

                if (IsA(stmt, CreateRoleStmt) || IsA(stmt, AlterRoleStmt) ||
                    (IsA(stmt, VariableSetStmt) && ((VariableSetStmt *)stmt)->kind == VAR_SET_ROLEPWD)) {
                    stmt = (Node *)copyObject(stmt);
                }

                processutility_context proutility_cxt;
                proutility_cxt.parse_tree = stmt;
                proutility_cxt.query_string = plansource->query_string;
                proutility_cxt.readOnlyTree = true;  /* protect plancache's node tree */
                proutility_cxt.params = paramLI;
                proutility_cxt.is_top_level = false;  /* not top level */
                ProcessUtility(&proutility_cxt,
                    dest,
#ifdef PGXC
                    false,
#endif /* PGXC */
                    completionTag, PROCESS_UTILITY_QUERY);

                /* Update "processed" if stmt returned tuples */
                if (u_sess->SPI_cxt._current->tuptable) {
                    u_sess->SPI_cxt._current->processed =
                        u_sess->SPI_cxt._current->tuptable->alloced - u_sess->SPI_cxt._current->tuptable->free;
                }

                /*
                 * CREATE TABLE AS is a messy special case for historical
                 * reasons.  We must set u_sess->SPI_cxt._current->processed even though
                 * the tuples weren't returned to the caller, and we must
                 * return a special result code if the statement was spelled
                 * SELECT INTO.
                 */
                if (IsA(stmt, CreateTableAsStmt) && ((CreateTableAsStmt *)stmt)->relkind != OBJECT_MATVIEW) {
                    Assert(strncmp(completionTag, "SELECT ", 7) == 0);
                    u_sess->SPI_cxt._current->processed = strtoul(completionTag + 7, NULL, 10);
                    if (((CreateTableAsStmt *)stmt)->is_select_into) {
                        res = SPI_OK_SELINTO;
                    } else {
                        res = SPI_OK_UTILITY;
                    }
                } else {
                    res = SPI_OK_UTILITY;
                }

                if (IsA(stmt, CreateRoleStmt) || IsA(stmt, AlterRoleStmt) ||
                    (IsA(stmt, VariableSetStmt) && ((VariableSetStmt *)stmt)->kind == VAR_SET_ROLEPWD)) {
                    pfree_ext(stmt);
                }
            }

            /*
             * The last canSetTag query sets the status values returned to the
             * caller.	Be careful to free any tuptables not returned, to
             * avoid intratransaction memory leak.
             */
            if (canSetTag) {
                my_processed = u_sess->SPI_cxt._current->processed;
                my_lastoid = u_sess->SPI_cxt._current->lastoid;
                SPI_freetuptable(my_tuptable);
                my_tuptable = u_sess->SPI_cxt._current->tuptable;
                my_res = res;
            } else {
                SPI_freetuptable(u_sess->SPI_cxt._current->tuptable);
                u_sess->SPI_cxt._current->tuptable = NULL;
            }
            /* we know that the receiver doesn't need a destroy call */
            if (res < 0) {
                my_res = res;
                goto fail;
            }
            /* When commit/rollback occurs
             * the plan cache will be release refcount by resourceowner(except for oneshot plan) */
            oldOwner = AddCplanRefAgainIfNecessary(plan, plansource, cplan, oldTransactionId, oldOwner);
        }

        /* Done with this plan, so release refcount */
        if (need_remember_cplan)
            ForgetSpiPlanRef();
        ResourceOwner tmp = t_thrd.utils_cxt.CurrentResourceOwner;
        t_thrd.utils_cxt.CurrentResourceOwner = oldOwner;
        ReleaseCachedPlan(cplan, plan->saved);
        t_thrd.utils_cxt.CurrentResourceOwner  = tmp;
        cplan = NULL;
        if (ENABLE_GPC && tmp_cxt)
            MemoryContextDelete(tmp_cxt);

        /*
         * If not read-only mode, advance the command counter after the last
         * command.  This ensures that its effects are visible, in case it was
         * DDL that would affect the next CachedPlanSource.
         */
        if (!read_only) {
            CommandCounterIncrement();
        }
    }

fail:

    /* Pop the snapshot off the stack if we pushed one */
    if (pushed_active_snap) {
        PopActiveSnapshot();
    }

    /* We no longer need the cached plan refcount, if any */
    if (cplan != NULL) {
        if (need_remember_cplan)
            ForgetSpiPlanRef();
        ResourceOwner tmp = t_thrd.utils_cxt.CurrentResourceOwner;
        t_thrd.utils_cxt.CurrentResourceOwner = oldOwner;
        ReleaseCachedPlan(cplan, plan->saved);
        t_thrd.utils_cxt.CurrentResourceOwner  = tmp;
    }
    /*
     * When plan->plancache_list > 1 means it's a multi query and  have been malloc memory
     * through get_next_snippet, so we need free them here.
     */
    FreeMultiQueryString(plan);

    /*
     * Pop the error context stack
     */
    t_thrd.log_cxt.error_context_stack = spi_err_context.previous;

    /* Save results for caller */
    SPI_processed = my_processed;
    u_sess->SPI_cxt.lastoid = my_lastoid;
    SPI_tuptable = my_tuptable;

    /* tuptable now is caller's responsibility, not SPI's */
    u_sess->SPI_cxt._current->tuptable = NULL;

    /*
     * If none of the queries had canSetTag, return SPI_OK_REWRITTEN. Prior to
     * 8.4, we used return the last query's result code, but not its auxiliary
     * results, but that's confusing.
     */
    if (my_res == 0) {
        my_res = SPI_OK_REWRITTEN;
    }

    u_sess->attr.attr_sql.enable_light_proxy = tmp_enable_light_proxy;
    t_thrd.postgres_cxt.cur_command_tag = old_command_tag;

    return my_res;
}

static bool IsNeedSubTxnForSPIPlan(SPIPlanPtr plan)
{
    /* not required to act as the same as O */
    if (!PLSTMT_IMPLICIT_SAVEPOINT) {
        return false;
    }

    /* not inside exception block */
    if (u_sess->SPI_cxt.portal_stp_exception_counter == 0) {
        return false;
    }

    /* wrap statement doing modifications with subtransaction, so changes can be rollback. */
    ListCell *lc1 = NULL;
    foreach (lc1, plan->plancache_list) {
        CachedPlanSource *plansource = (CachedPlanSource *)lfirst(lc1);

        if (plan->oneshot) {
            SPIParseOneShotPlan(plansource, plan);
        }

        ListCell* l = NULL;
        foreach (l, plansource->query_list) {
            Query* qry = (Query*)lfirst(l);

            /*
             * Here act as the opsite as CommandIsReadOnly does. (1) utility cmd;
             * (2) SELECT FOR UPDATE/SHARE; (3) data-modifying CTE.
             */
            if (qry->commandType != CMD_SELECT || qry->rowMarks != NULL || qry->hasModifyingCTE) {
                return true;
            }

#ifdef ENABLE_MULTIPLE_NODES
            /*
             * DN aborts subtransaction automatically once error occurs. Current start an new implicit
             * savepoint to isolate from runtime error even that it is a read only statement.
             *
             * If statement contains only system table, it will run at CN without savepint. It's
             * a worth thing to check it?
             */
            return list_length(plansource->relationOids) != 0;
#endif
        }
    }

    return false;
}

/*
 * Execute plan with the given parameter values
 *
 * Here we would wrap it with a savepoint for rollback its updates.
 */
extern int _SPI_execute_plan(SPIPlanPtr plan, ParamListInfo paramLI, Snapshot snapshot,
    Snapshot crosscheck_snapshot, bool read_only, bool fire_triggers, long tcount, bool from_lock)
{
    int my_res = 0;

    if (IsNeedSubTxnForSPIPlan(plan)) {
        volatile int curExceptionCounter;
        MemoryContext oldcontext = CurrentMemoryContext;
        int64 stackId = u_sess->plsql_cxt.nextStackEntryId;

        /* start an implicit savepoint for this stmt. */
        SPI_savepoint_create(NULL);

        /* switch into old memory context, don't run it under subtransaction. */
        MemoryContextSwitchTo(oldcontext);

        curExceptionCounter = u_sess->SPI_cxt.portal_stp_exception_counter;
        PG_TRY();
        {
            my_res = _SPI_execute_plan0(plan, paramLI, snapshot,
                crosscheck_snapshot, read_only, fire_triggers, tcount, from_lock);
        }
        PG_CATCH();
        {
            /* rollback this stmt's updates. */
            if (curExceptionCounter == u_sess->SPI_cxt.portal_stp_exception_counter &&
                GetCurrentTransactionName() == NULL) {
                SPI_savepoint_rollbackAndRelease(NULL, InvalidTransactionId);
                stp_cleanup_subxact_resource(stackId);
            }
            PG_RE_THROW();
        }
        PG_END_TRY();

        /*
         * if there is any new inner subtransaction, don't release savepoint.
         * any idea for this remained subtransaction?
         */
        if (curExceptionCounter == u_sess->SPI_cxt.portal_stp_exception_counter &&
            GetCurrentTransactionName() == NULL) {
            SPI_savepoint_release(NULL);
            stp_cleanup_subxact_resource(stackId);
        }
    } else {
        my_res = _SPI_execute_plan0(plan, paramLI, snapshot,
            crosscheck_snapshot, read_only, fire_triggers, tcount, from_lock);
    }

    return my_res;
}

/* For transaction, mysubid is TopSubTransactionId */
void ReleaseSpiPlanRef(TransactionId mysubid)
{
    SPICachedPlanStack* cur_spi_cplan = u_sess->SPI_cxt.spi_exec_cplan_stack;

    while (cur_spi_cplan != NULL && cur_spi_cplan->subtranid >= mysubid) {
        CachedPlan* cplan = cur_spi_cplan->cplan;
        u_sess->SPI_cxt.spi_exec_cplan_stack = cur_spi_cplan->previous;
        pfree_ext(cur_spi_cplan);
        cur_spi_cplan = u_sess->SPI_cxt.spi_exec_cplan_stack;
        ReleaseCachedPlan(cplan, false);
    }
}

/*
 * Convert arrays of query parameters to form wanted by planner and executor
 */
ParamListInfo _SPI_convert_params(int nargs, Oid *argtypes, Datum *Values, const char *Nulls,
    Cursor_Data *cursor_data)
{
    ParamListInfo param_list_info;

    if (nargs > 0) {
        int i;

        param_list_info = (ParamListInfo)palloc(offsetof(ParamListInfoData, params) + nargs * sizeof(ParamExternData));
        /* we have static list of params, so no hooks needed */
        param_list_info->paramFetch = NULL;
        param_list_info->paramFetchArg = NULL;
        param_list_info->parserSetup = NULL;
        param_list_info->parserSetupArg = NULL;
        param_list_info->params_need_process = false;
        param_list_info->uParamInfo = DEFUALT_INFO;
        param_list_info->params_lazy_bind = false;
        param_list_info->numParams = nargs;

        for (i = 0; i < nargs; i++) {
            ParamExternData *prm = &param_list_info->params[i];

            prm->value = Values[i];
            prm->isnull = (Nulls && Nulls[i] == 'n');
            prm->pflags = PARAM_FLAG_CONST;
            prm->ptype = argtypes[i];
            if (cursor_data != NULL) {
                CopyCursorInfoData(&prm->cursor_data, &cursor_data[i]);
            }
            prm->tabInfo = NULL;
        }
    } else {
        param_list_info = NULL;
    }
    return param_list_info;
}

static int _SPI_pquery(QueryDesc *queryDesc, bool fire_triggers, long tcount, bool from_lock)
{
    int operation = queryDesc->operation;
    int eflags;
    int res;

    switch (operation) {
        case CMD_SELECT:
            Assert(queryDesc->plannedstmt->utilityStmt == NULL);
            if (queryDesc->dest->mydest != DestSPI 
#ifndef ENABLE_MULTIPLE_NODES
                && queryDesc->dest->mydest != DestRemote
#endif
            ) {
                /* Don't return SPI_OK_SELECT if we're discarding result */
                res = SPI_OK_UTILITY;
            } else
                res = SPI_OK_SELECT;
            break;
        case CMD_INSERT:
            if (queryDesc->plannedstmt->hasReturning)
                res = SPI_OK_INSERT_RETURNING;
            else
                res = SPI_OK_INSERT;
            break;
        case CMD_DELETE:
            if (queryDesc->plannedstmt->hasReturning)
                res = SPI_OK_DELETE_RETURNING;
            else
                res = SPI_OK_DELETE;
            break;
        case CMD_UPDATE:
            if (queryDesc->plannedstmt->hasReturning)
                res = SPI_OK_UPDATE_RETURNING;
            else
                res = SPI_OK_UPDATE;
            break;
        case CMD_MERGE:
            res = SPI_OK_MERGE;
            break;
        default:
            return SPI_ERROR_OPUNKNOWN;
    }

#ifdef SPI_EXECUTOR_STATS
    if (ShowExecutorStats)
        ResetUsage();
#endif

    /* Select execution options */
    if (fire_triggers) {
        eflags = 0; /* default run-to-completion flags */
    } else {
        eflags = EXEC_FLAG_SKIP_TRIGGERS;
    }

    TransactionId oldTransactionId = SPI_get_top_transaction_id();
    /*
     * With savepoint in STP feature, CurrentResourceOwner is different before and after executor.
     * We need to hold the original one in order to forget the snapshot, plan reference.and etc.
     */
    ResourceOwner oldOwner = t_thrd.utils_cxt.CurrentResourceOwner;

    ExecutorStart(queryDesc, eflags);

    bool forced_control = !from_lock && IS_PGXC_COORDINATOR &&
        (t_thrd.wlm_cxt.parctl_state.simple == 1 || u_sess->wlm_cxt->is_active_statements_reset) &&
        ENABLE_WORKLOAD_CONTROL;
    Qid stroedproc_qid = { 0, 0, 0 };
    unsigned char stroedproc_parctl_state_except = 0;
    WLMStatusTag stroedproc_g_collectInfo_status = WLM_STATUS_RESERVE;
    bool stroedproc_is_active_statements_reset = false;
    if (forced_control) {
        if (!u_sess->wlm_cxt->is_active_statements_reset && !u_sess->attr.attr_resource.enable_transaction_parctl) {
            u_sess->wlm_cxt->stroedproc_rp_reserve = t_thrd.wlm_cxt.parctl_state.rp_reserve;
            u_sess->wlm_cxt->stroedproc_rp_release = t_thrd.wlm_cxt.parctl_state.rp_release;
            u_sess->wlm_cxt->stroedproc_release = t_thrd.wlm_cxt.parctl_state.release;
        }

        /* Retain the parameters of the main statement */
        if (!IsQidInvalid(&u_sess->wlm_cxt->wlm_params.qid)) {
            error_t rc = memcpy_s(&stroedproc_qid, sizeof(Qid), &u_sess->wlm_cxt->wlm_params.qid, sizeof(Qid));
            securec_check(rc, "\0", "\0");
        }
        stroedproc_parctl_state_except = t_thrd.wlm_cxt.parctl_state.except;
        stroedproc_g_collectInfo_status = t_thrd.wlm_cxt.collect_info->status;
        stroedproc_is_active_statements_reset = u_sess->wlm_cxt->is_active_statements_reset;

        t_thrd.wlm_cxt.parctl_state.subquery = 1;
        WLMInitQueryPlan(queryDesc);
        dywlm_client_manager(queryDesc);
    }

    ExecutorRun(queryDesc, ForwardScanDirection, tcount);

    if (forced_control) {
        t_thrd.wlm_cxt.parctl_state.except = 0;
        if (g_instance.wlm_cxt->dynamic_workload_inited && (t_thrd.wlm_cxt.parctl_state.simple == 0)) {
            dywlm_client_release(&t_thrd.wlm_cxt.parctl_state);
        } else {
            // only release resource pool count
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord() &&
                (u_sess->wlm_cxt->parctl_state_exit || IsQueuedSubquery())) {
                WLMReleaseGroupActiveStatement();
            }
        }

        WLMSetCollectInfoStatus(WLM_STATUS_FINISHED);
        t_thrd.wlm_cxt.parctl_state.subquery = 0;
        t_thrd.wlm_cxt.parctl_state.except = stroedproc_parctl_state_except;
        t_thrd.wlm_cxt.collect_info->status = stroedproc_g_collectInfo_status;
        u_sess->wlm_cxt->is_active_statements_reset = stroedproc_is_active_statements_reset;
        if (!IsQidInvalid(&stroedproc_qid)) {
            error_t rc = memcpy_s(&u_sess->wlm_cxt->wlm_params.qid, sizeof(Qid), &stroedproc_qid, sizeof(Qid));
            securec_check(rc, "\0", "\0");
        }

        /* restore state condition if guc para is off since it contains unreleased count */
        if (!u_sess->attr.attr_resource.enable_transaction_parctl && (u_sess->wlm_cxt->reserved_in_active_statements ||
            u_sess->wlm_cxt->reserved_in_group_statements || u_sess->wlm_cxt->reserved_in_group_statements_simple)) {
            t_thrd.wlm_cxt.parctl_state.rp_reserve = u_sess->wlm_cxt->stroedproc_rp_reserve;
            t_thrd.wlm_cxt.parctl_state.rp_release = u_sess->wlm_cxt->stroedproc_rp_release;
            t_thrd.wlm_cxt.parctl_state.release = u_sess->wlm_cxt->stroedproc_release;
        }
    }

    u_sess->SPI_cxt._current->processed = queryDesc->estate->es_processed;
    u_sess->SPI_cxt._current->lastoid = queryDesc->estate->es_lastoid;

    if ((res == SPI_OK_SELECT || queryDesc->plannedstmt->hasReturning) && queryDesc->dest->mydest == DestSPI) {
        if (_SPI_checktuples()) {
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("consistency check on SPI tuple count failed when execute plan, %s",
                (u_sess->SPI_cxt._current->tuptable == NULL) ? "tupletable is NULL." : "processed tuples is not matched.")));
        }
    }

    ExecutorFinish(queryDesc);
    /*
     * If there are commit/rollback within stored procedure. Snapshot has already free during commit/rollback process
     * Therefore, need to set queryDesc snapshots to NULL. Otherwise the reference will be stale pointers.
     */
    if (oldTransactionId != SPI_get_top_transaction_id()) {
        queryDesc->snapshot = NULL;
        queryDesc->crosscheck_snapshot = NULL;
        queryDesc->estate->es_snapshot = NULL;
        queryDesc->estate->es_crosscheck_snapshot = NULL;
    }

    ResourceOwner tmp  = t_thrd.utils_cxt.CurrentResourceOwner;
    t_thrd.utils_cxt.CurrentResourceOwner = oldOwner;
    ExecutorEnd(queryDesc);
    t_thrd.utils_cxt.CurrentResourceOwner = tmp;

    /* FreeQueryDesc is done by the caller */
#ifdef SPI_EXECUTOR_STATS
    if (ShowExecutorStats)
        ShowUsage("SPI EXECUTOR STATS");
#endif

    return res;
}

/*
 * _SPI_error_callback
 *
 * Add context information when a query invoked via SPI fails
 */
void _SPI_error_callback(void *arg)
{
    /* We can't expose query when under analyzing with tablesample. */
    if (u_sess->analyze_cxt.is_under_analyze) {
        return;
    }

    const char *query = (const char *)arg;
    int syntax_err_pos;

    if (query == NULL) { /* in case arg wasn't set yet */
        return;
    }

    char *mask_string = maskPassword(query);
    if (mask_string == NULL) {
        mask_string = (char *)query;
    }

    /*
     * If there is a syntax error position, convert to internal syntax error;
     * otherwise treat the query as an item of context stack
     */
    syntax_err_pos = geterrposition();
    if (syntax_err_pos > 0) {
        errposition(0);
        internalerrposition(syntax_err_pos);
        internalerrquery(mask_string);
    } else {
        errcontext("SQL statement \"%s\"", mask_string);
    }

    if (mask_string != query) {
        pfree(mask_string);
    }
}

/*
 * _SPI_cursor_operation
 *
 * 	Do a FETCH or MOVE in a cursor
 */
static void _SPI_cursor_operation(Portal portal, FetchDirection direction, long count, DestReceiver *dest)
{
    long n_fetched;

    /* Check that the portal is valid */
    if (!PortalIsValid(portal)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_CURSOR_STATE), errmsg("invalid portal in SPI cursor operation")));
    }

    /* Push the SPI stack */
    SPI_STACK_LOG("begin", portal->sourceText, NULL);
    if (_SPI_begin_call(true) < 0) {
        ereport(ERROR, (errcode(ERRCODE_SPI_CONNECTION_FAILURE),
            errmsg("SPI stack is corrupted when perform cursor operation, current level: %d, connected level: %d",
            u_sess->SPI_cxt._curid, u_sess->SPI_cxt._connected)));
    }

    /* Reset the SPI result (note we deliberately don't touch lastoid) */
    SPI_processed = 0;
    SPI_tuptable = NULL;
    u_sess->SPI_cxt._current->processed = 0;
    u_sess->SPI_cxt._current->tuptable = NULL;

    /* Run the cursor */
    n_fetched = PortalRunFetch(portal, direction, count, dest);

    /*
     * Think not to combine this store with the preceding function call. If
     * the portal contains calls to functions that use SPI, then SPI_stack is
     * likely to move around while the portal runs.  When control returns,
     * u_sess->SPI_cxt._current will point to the correct stack entry... but the pointer
     * may be different than it was beforehand. So we must be sure to re-fetch
     * the pointer after the function call completes.
     */
    u_sess->SPI_cxt._current->processed = n_fetched;

    if (dest->mydest == DestSPI && _SPI_checktuples()) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("consistency check on SPI tuple count failed, %s",
            (u_sess->SPI_cxt._current->tuptable == NULL) ? "tupletable is NULL." : "processed tuples is not matched.")));
    }

    /* Put the result into place for access by caller */
    SPI_processed = u_sess->SPI_cxt._current->processed;
    SPI_tuptable = u_sess->SPI_cxt._current->tuptable;

    /* tuptable now is caller's responsibility, not SPI's */
    u_sess->SPI_cxt._current->tuptable = NULL;

    /* Pop the SPI stack */
    SPI_STACK_LOG("end", portal->sourceText, NULL);
    _SPI_end_call(true);
}

/*
 * _SPI_hold_cursor
 *
 * 	hold a pinned cursor
 */
void _SPI_hold_cursor(bool is_rollback)
{
    /* Push the SPI stack */
    SPI_STACK_LOG("begin", NULL, NULL);
    if (_SPI_begin_call(true) < 0) {
        ereport(ERROR, (errcode(ERRCODE_SPI_CONNECTION_FAILURE),
            errmsg("SPI stack is corrupted when perform cursor operation, current level: %d, connected level: %d",
            u_sess->SPI_cxt._curid, u_sess->SPI_cxt._connected)));
    }

    HoldPinnedPortals(is_rollback);

    /* Pop the SPI stack */
    SPI_STACK_LOG("end", NULL, NULL);
    _SPI_end_call(true);
}

static MemoryContext _SPI_execmem(void)
{
    return MemoryContextSwitchTo(u_sess->SPI_cxt._current->execCxt);
}

static MemoryContext _SPI_procmem(void)
{
    return MemoryContextSwitchTo(u_sess->SPI_cxt._current->procCxt);
}

/*
 * _SPI_begin_call: begin a SPI operation within a connected procedure
 */
int _SPI_begin_call(bool execmem)
{
    if (u_sess->SPI_cxt._curid + 1 != u_sess->SPI_cxt._connected)
        return SPI_ERROR_UNCONNECTED;
    u_sess->SPI_cxt._curid++;
    if (u_sess->SPI_cxt._current != &(u_sess->SPI_cxt._stack[u_sess->SPI_cxt._curid])) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("SPI stack corrupted when begin SPI operation.")));
    }

    if (execmem) { /* switch to the Executor memory context */
        _SPI_execmem();
    }

    return 0;
}

/*
 * _SPI_end_call: end a SPI operation within a connected procedure
 *
 * Note: this currently has no failure return cases, so callers don't check
 */
int _SPI_end_call(bool procmem)
{
    /*
     * We're returning to procedure where u_sess->SPI_cxt._curid == u_sess->SPI_cxt._connected - 1
     */
    u_sess->SPI_cxt._curid--;

    /* must put last after smp thread has reach the sync point, then we can release the memory. */
    if (procmem) {
        /* switch to the procedure memory context */
        _SPI_procmem();
        /* and free Executor memory */
        MemoryContextResetAndDeleteChildren(u_sess->SPI_cxt._current->execCxt);
    }

    return 0;
}

static bool _SPI_checktuples(void)
{
    uint64 processed = u_sess->SPI_cxt._current->processed;
    SPITupleTable *tuptable = u_sess->SPI_cxt._current->tuptable;
    bool failed = false;

    if (tuptable == NULL) { /* spi_dest_startup was not called */
        failed = true;
    }

    else if (processed != (tuptable->alloced - tuptable->free)) {
        failed = true;
    }

    return failed;
}

/*
 * Convert a "temporary" SPIPlan into an "unsaved" plan.
 *
 * The passed _SPI_plan struct is on the stack, and all its subsidiary data
 * is in or under the current SPI executor context.  Copy the plan into the
 * SPI procedure context so it will survive _SPI_end_call().  To minimize
 * data copying, this destructively modifies the input plan, by taking the
 * plancache entries away from it and reparenting them to the new SPIPlan.
 */
static SPIPlanPtr _SPI_make_plan_non_temp(SPIPlanPtr plan)
{
    SPIPlanPtr newplan = NULL;
    MemoryContext parentcxt = u_sess->SPI_cxt._current->procCxt;
    MemoryContext plancxt = NULL;
    MemoryContext oldcxt = NULL;
    ListCell *lc = NULL;

    /* Assert the input is a temporary SPIPlan */
    Assert(plan->magic == _SPI_PLAN_MAGIC);
    Assert(plan->plancxt == NULL);
    /* One-shot plans can't be saved */
    Assert(!plan->oneshot);

    /*
     * Create a memory context for the plan, underneath the procedure context.
     * We don't expect the plan to be very large, so use smaller-than-default
     * alloc parameters.
     */
    plancxt = AllocSetContextCreate(parentcxt, "SPI Plan", ALLOCSET_SMALL_MINSIZE, ALLOCSET_SMALL_INITSIZE,
        ALLOCSET_SMALL_MAXSIZE);
    oldcxt = MemoryContextSwitchTo(plancxt);
    /* Copy the SPI_plan struct and subsidiary data into the new context */
    newplan = (SPIPlanPtr)palloc(sizeof(_SPI_plan));
    CopySPI_Plan(newplan, plan, plancxt);

    /*
     * Reparent all the CachedPlanSources into the procedure context.  In
     * theory this could fail partway through due to the pallocs, but we don't
     * care too much since both the procedure context and the executor context
     * would go away on error.
     */
    foreach (lc, plan->plancache_list) {
        CachedPlanSource *plansource = (CachedPlanSource *)lfirst(lc);
        /* shared spiplan's plancache is global */
        if (plansource->gpc.status.IsPrivatePlan())
            CachedPlanSetParentContext(plansource, parentcxt);

        /* Build new list, with list cells in plancxt */
        newplan->plancache_list = lappend(newplan->plancache_list, plansource);
    }

    (void)MemoryContextSwitchTo(oldcxt);

    /* For safety, unlink the CachedPlanSources from the temporary plan */
    plan->plancache_list = NIL;

    return newplan;
}

void CopySPI_Plan(SPIPlanPtr newplan, SPIPlanPtr plan, MemoryContext plancxt)
{
    newplan->magic = _SPI_PLAN_MAGIC;
    newplan->saved = false;
    newplan->oneshot = false;
    newplan->plancache_list = NIL;
    newplan->plancxt = plancxt;
    newplan->cursor_options = plan->cursor_options;
    newplan->nargs = plan->nargs;
    newplan->stmt_list = NIL;
    newplan->id = plan->id;
    newplan->spi_key = plan->spi_key;
    if (plan->nargs > 0) {
        newplan->argtypes = (Oid *)palloc(plan->nargs * sizeof(Oid));
        errno_t rc = memcpy_s(newplan->argtypes, plan->nargs * sizeof(Oid), plan->argtypes, plan->nargs * sizeof(Oid));
        securec_check(rc, "\0", "\0");
    } else {
        newplan->argtypes = NULL;
    }
    newplan->parserSetup = plan->parserSetup;
    newplan->parserSetupArg = plan->parserSetupArg;
    ListCell* lc = NULL;
    foreach (lc, plan->stmt_list) {
        Query* q = (Query*)lfirst(lc);
        Query* new_q = (Query*)copyObject(q);
        newplan->stmt_list = lappend(newplan->stmt_list, new_q);
    }
}

/*
 * Make a "saved" copy of the given plan.
 */
static SPIPlanPtr _SPI_save_plan(SPIPlanPtr plan)
{
    SPIPlanPtr newplan = NULL;
    MemoryContext plancxt = NULL;
    MemoryContext oldcxt = NULL;
    ListCell *lc = NULL;

    /* One-shot plans can't be saved */
    Assert(!plan->oneshot);

    /*
     * Create a memory context for the plan.  We don't expect the plan to be
     * very large, so use smaller-than-default alloc parameters.  It's a
     * transient context until we finish copying everything.
     */
    plancxt = AllocSetContextCreate(CurrentMemoryContext, "SPI Plan", ALLOCSET_SMALL_MINSIZE, ALLOCSET_SMALL_INITSIZE,
        ALLOCSET_SMALL_MAXSIZE);
    oldcxt = MemoryContextSwitchTo(plancxt);

    /* Copy the SPI plan into its own context */
    newplan = (SPIPlanPtr)palloc(sizeof(_SPI_plan));
    CopySPI_Plan(newplan, plan, plancxt);

    /* Copy all the plancache entries */
    foreach (lc, plan->plancache_list) {
        CachedPlanSource *plansource = (CachedPlanSource *)lfirst(lc);
        CachedPlanSource *newsource = NULL;

        newsource = CopyCachedPlan(plansource, false);
        newplan->plancache_list = lappend(newplan->plancache_list, newsource);
    }

    (void)MemoryContextSwitchTo(oldcxt);

    /*
     * Mark it saved, reparent it under u_sess->cache_mem_cxt, and mark all the
     * component CachedPlanSources as saved.  This sequence cannot fail
     * partway through, so there's no risk of long-term memory leakage.
     */
    newplan->saved = true;
    MemoryContextSetParent(newplan->plancxt, u_sess->cache_mem_cxt);

    foreach (lc, newplan->plancache_list) {
        CachedPlanSource *plansource = (CachedPlanSource *)lfirst(lc);

        SaveCachedPlan(plansource);
    }

    return newplan;
}

/*
 * spi_dest_shutdownAnalyze: We receive 30000 samples each time and callback to process when analyze for table sample,
 * 					if the num of last batch less than 30000, we should callback to process in this.
 *
 * Parameters:
 * 	@in self: a base type for destination-specific local state.
 *
 * Returns: void
 */
static void spi_dest_shutdownAnalyze(DestReceiver *self)
{
    SPITupleTable *tuptable = NULL;

    spi_check_connid();
    tuptable = u_sess->SPI_cxt._current->tuptable;
    if (tuptable == NULL) {
        ereport(ERROR, (errcode(ERRCODE_SPI_ERROR), errmsg("SPI tupletable is NULL when shutdown SPI for analyze.")));
    }

    if ((tuptable->free < tuptable->alloced) && (u_sess->SPI_cxt._current->spiCallback)) {
        SPI_tuptable = tuptable;
        SPI_processed = tuptable->alloced - tuptable->free;
        u_sess->SPI_cxt._current->spiCallback(u_sess->SPI_cxt._current->clientData);
    }
}

/*
 * spi_dest_destroyAnalyze: pfree the state for receiver.
 *
 * Parameters:
 * 	@in self: a base type for destination-specific local state.
 *
 * Returns: void
 */
static void spi_dest_destroyAnalyze(DestReceiver *self)
{
    pfree_ext(self);
}

/*
 * spi_dest_printTupleAnalyze: Receive sample tuples each time and callback to process
 * 							when analyze for table sample.
 *
 * Parameters:
 * 	@in slot: the struct which executor stores tuples.
 * 	@in self: a base type for destination-specific local state.
 *
 * Returns: void
 */
static void spi_dest_printTupleAnalyze(TupleTableSlot *slot, DestReceiver *self)
{
    SPITupleTable *tuptable = NULL;
    MemoryContext oldcxt = NULL;

    spi_check_connid();
    tuptable = u_sess->SPI_cxt._current->tuptable;
    if (tuptable == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_SPI_ERROR), errmsg("SPI tupletable is NULL when store tuple to it for analyze.")));
    }

    oldcxt = MemoryContextSwitchTo(tuptable->tuptabcxt);

    if (tuptable->free == 0) {
        SPI_processed = tuptable->alloced - tuptable->free;
        SPI_tuptable = tuptable;

        /* Callback process tuples we have received.  */
        if (u_sess->SPI_cxt._current->spiCallback) {
            u_sess->SPI_cxt._current->spiCallback(u_sess->SPI_cxt._current->clientData);
        }

        for (uint32 i = 0; i < SPI_processed; i++) {
            pfree_ext(tuptable->vals[i]);
        }

        pfree_ext(tuptable->vals);
        tuptable->free = tuptable->alloced;
        tuptable->vals = (HeapTuple *)palloc0(tuptable->alloced * sizeof(HeapTuple));
    }

    tuptable->vals[tuptable->alloced - tuptable->free] = ExecCopySlotTuple(slot);
    (tuptable->free)--;

    (void)MemoryContextSwitchTo(oldcxt);
}

/*
 * createAnalyzeSPIDestReceiver: create a DestReceiver for printtup of SPI when analyze for table sample..
 *
 * Parameters:
 * 	@in dest: identify the desired destination, results sent to SPI manager.
 *
 * Returns: DestReceiver*
 */
DestReceiver *createAnalyzeSPIDestReceiver(CommandDest dest)
{
    DestReceiver *spi_dst_receiver = (DestReceiver *)palloc0(sizeof(DestReceiver));

    spi_dst_receiver->rStartup = spi_dest_startup;
    spi_dst_receiver->receiveSlot = spi_dest_printTupleAnalyze;
    spi_dst_receiver->rShutdown = spi_dest_shutdownAnalyze;
    spi_dst_receiver->rDestroy = spi_dest_destroyAnalyze;
    spi_dst_receiver->mydest = dest;

    return spi_dst_receiver;
}

/*
 * spi_exec_with_callback: this is a helper method that executes a SQL statement using
 * 					the SPI interface. It optionally calls a callback function with result pointer.
 *
 * Parameters:
 * 	@in dest: indentify execute the plan using oreitation-row or column
 * 	@in src: SQL string
 * 	@in read_only: is it a read-only call?
 * 	@in tcount: execution tuple-count limit, or 0 for none
 * 	@in spec: the sample info of special attribute for compute statistic
 * 	@in callbackFn: callback function to be executed once SPI is done.
 * 	@in clientData: argument to call back function (usually pointer to data-structure
 * 				that the callback function populates).
 *
 * Returns: void
 */
void spi_exec_with_callback(CommandDest dest, const char *src, bool read_only, long tcount, bool direct_call,
    void (*callbackFn)(void *), void *clientData, parse_query_func parser)
{
    bool connected = false;
    int ret = 0;

    PG_TRY();
    {
        if (SPI_OK_CONNECT != SPI_connect(dest, callbackFn, clientData)) {
            ereport(ERROR, (errcode(ERRCODE_SPI_CONNECTION_FAILURE),
                errmsg("Unable to connect to execute internal query, current level: %d, connected level: %d",
                u_sess->SPI_cxt._curid, u_sess->SPI_cxt._connected)));
        }
        connected = true;

        elog(DEBUG1, "Executing SQL: %s", src);

        /* Do the query. */
        ret = SPI_execute(src, read_only, tcount, false, parser);
        Assert(ret > 0);

        if (direct_call && callbackFn != NULL) {
            callbackFn(clientData);
        }

        connected = false;
        SPI_STACK_LOG("finish", NULL, NULL);
        (void)SPI_finish();
    }
    /* Clean up in case of error. */
    PG_CATCH();
    {
        if (connected) {
            SPI_STACK_LOG("finish", NULL, NULL);
            SPI_finish();
        }

        /* Carry on with error handling. */
        PG_RE_THROW();
    }
    PG_END_TRY();
}
/* 
 * SPI_forbid_exec_push_down_with_exception 
 * Function with exception can't be pushed down,because the
 * exception start a subtransaction in DN Node,it will cause the result
 * incorrect
 * Returns: Void
 */
void SPI_forbid_exec_push_down_with_exception() 
{
    if (u_sess->SPI_cxt.current_stp_with_exception && IS_PGXC_DATANODE && IsConnFromCoord()) {
        ereport(FATAL, (errmsg("the function or procedure with exception can't be pushed down for execution")));
    }
}

/*
 * SPI_get_top_transaction_id
 * Returns the virtual transaction ID created at the beginning of the transaction
 * The distribution only allows commit and rollback on CN, which returns InvalidTransactionId when the DN is called.
 * When cn or singlenode: returns the virtual transaction id
 *
 * Returns: TransactionId
 */
TransactionId SPI_get_top_transaction_id()
{
#ifdef ENABLE_MULTIPLE_NODES
    if (IS_PGXC_COORDINATOR) {
        return t_thrd.proc->lxid;
    } else {
        return InvalidTransactionId;
    }    
#else
    return t_thrd.proc->lxid;
#endif
}

List* _SPI_get_querylist(SPIPlanPtr plan)
{
    return plan ? plan->stmt_list : NULL;
}

void _SPI_prepare_oneshot_plan_for_validator(const char *src, SPIPlanPtr plan, parse_query_func parser)
{
    _SPI_prepare_oneshot_plan(src, plan, parser);
}

void InitSPIPlanCxt()
{
    /* initialize memory context */
    if (g_instance.spi_plan_cxt.global_spi_plan_context == NULL) {
        g_instance.spi_plan_cxt.global_spi_plan_context = AllocSetContextCreate(
            g_instance.instance_context, "SPIPlanContext", 
            ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE, SHARED_CONTEXT);
    }
}
