/* -------------------------------------------------------------------------
 * logical.cpp
 *	 openGauss logical decoding coordination
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Copyright (c) 2012-2014, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	src/gausskernel/storage/replication/logical/logical.cpp
 *
 * NOTES
 *	This file coordinates interaction between the various modules that
 *	together provide logical decoding, primarily by providing so
 *	called LogicalDecodingContexts. The goal is to encapsulate most of the
 *	internal complexity for consumers of logical decoding, so they can
 *	create and consume a changestream with a low amount of code. Builtin
 *	consumers are the walsender and SQL SRF interface, but it's possible to
 *	add further ones without changing core code, e.g. to consume changes in
 *	a bgworker
 *
 *	The idea is that a consumer provides three callbacks, one to read WAL,
 *	one to prepare a data write, and a final one for actually writing since
 *	their implementation depends on the type of consumer.  Check
 *	logicalfuncs.c for an example implementation of a fairly simple consumer
 *	and a implementation of a WAL reading callback that's suitable for
 *	simple consumers.
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <unistd.h>
#include <sys/stat.h>

#include "miscadmin.h"

#include "access/xact.h"
#include "access/xlogdefs.h"
#include "access/transam.h"
#include "access/xlog_internal.h"
#include "libpq/libpq-int.h"

#include "replication/decode.h"
#include "replication/logical.h"
#include "replication/reorderbuffer.h"
#include "replication/snapbuild.h"
#include "replication/walreceiver.h"

#include "storage/proc.h"
#include "storage/procarray.h"

#include "utils/builtins.h"
#include "utils/memutils.h"
/* data for errcontext callback */
typedef struct LogicalErrorCallbackState {
    LogicalDecodingContext *ctx;
    const char *callback_name;
    XLogRecPtr report_location;
} LogicalErrorCallbackState;

typedef struct ParallelLogicalErrorCallbackState {
    ParallelLogicalDecodingContext *ctx;
    const char *callback_name;
    XLogRecPtr report_location;
} ParallelLogicalErrorCallbackState;

LogicalDispatcher g_Logicaldispatcher[20];

/* wrappers around output plugin callbacks */
static void output_plugin_error_callback(void *arg);
static void startup_cb_wrapper(LogicalDecodingContext *ctx, OutputPluginOptions *opt, bool is_init);
void shutdown_cb_wrapper(LogicalDecodingContext *ctx);
static void begin_cb_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn);
static void commit_cb_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn, XLogRecPtr commit_lsn);
static void abort_cb_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn);
static void prepare_cb_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn);

static void change_cb_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn, Relation relation,
                              ReorderBufferChange *change);
static void parallel_change_cb_wrapper(ParallelReorderBuffer *cache, ReorderBufferTXN *txn, Relation relation,
    ParallelReorderBufferChange *change);

static void LoadOutputPlugin(OutputPluginCallbacks *callbacks, const char *plugin);
static void LoadOutputPlugin(ParallelOutputPluginCallbacks *callbacks, const char *plugin);

/*
 * Make sure the current settings & environment are capable of doing logical
 * decoding.
 */
void CheckLogicalDecodingRequirements(Oid databaseId)
{
    CheckSlotRequirements();
    if (g_instance.attr.attr_storage.wal_level < WAL_LEVEL_LOGICAL)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("logical decoding requires wal_level >= logical")));

    if (databaseId == InvalidOid)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("logical decoding requires a database connection")));

    /* ----
     * description: We got to change that someday soon...
     *
     * There's basically three things missing to allow this:
     * 1) We need to be able to correctly and quickly identify the timeline a
     *	LSN belongs to
     * 2) We need to force hot_standby_feedback to be enabled at all times so
     *	the primary cannot remove rows we need.
     * 3) support dropping replication slots referring to a database, in
     *	dbase_redo. There can't be any active ones due to HS recovery
     *	conflicts, so that should be relatively easy.
     * ----
     */
}

/*
 * Helper function for CreateInitialDecodingContext() and
 * CreateDecodingContext() performing common tasks.
 */
static LogicalDecodingContext *StartupDecodingContext(List *output_plugin_options, XLogRecPtr start_lsn,
                                                      TransactionId xmin_horizon, bool need_full_snapshot,
                                                      bool fast_forward, XLogPageReadCB read_page,
                                                      LogicalOutputPluginWriterPrepareWrite prepare_write,
                                                      LogicalOutputPluginWriterWrite do_write)
{
    ReplicationSlot *slot = NULL;
    MemoryContext context, old_context;
    LogicalDecodingContext *ctx = NULL;

    /* shorter lines... */
    slot = t_thrd.slot_cxt.MyReplicationSlot;

    context = AllocSetContextCreate(CurrentMemoryContext, "Changeset Extraction Context", ALLOCSET_DEFAULT_MINSIZE,
                                    ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    old_context = MemoryContextSwitchTo(context);
    ctx = (LogicalDecodingContext *)palloc0(sizeof(LogicalDecodingContext));

    ctx->context = context;

    /* (re-)load output plugins, so we detect a bad (removed) output plugin now. */
    if (!fast_forward)
        LoadOutputPlugin(&ctx->callbacks, NameStr(slot->data.plugin));

    /*
     * Now that the slot's xmin has been set, we can announce ourselves as a
     * logical decoding backend which doesn't need to be checked individually
     * when computing the xmin horizon because the xmin is enforced via
     * replication slots.
     */
    (void)LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
    t_thrd.pgxact->vacuumFlags |= PROC_IN_LOGICAL_DECODING;
    LWLockRelease(ProcArrayLock);

    ctx->slot = slot;

    ctx->reader = XLogReaderAllocate(read_page, ctx);
    if (unlikely(ctx->reader == NULL))
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                        errmsg("memory is temporarily unavailable while allocate xlog reader")));

    ctx->reader->private_data = ctx;

    ctx->reorder = ReorderBufferAllocate();
    ctx->snapshot_builder = AllocateSnapshotBuilder(ctx->reorder, xmin_horizon, start_lsn, need_full_snapshot);

    ctx->reorder->private_data = ctx;

    /* wrap output plugin callbacks, so we can add error context information */
    ctx->reorder->begin = begin_cb_wrapper;
    ctx->reorder->apply_change = change_cb_wrapper;
    ctx->reorder->commit = commit_cb_wrapper;

    ctx->out = makeStringInfo();
    ctx->prepare_write = prepare_write;
    ctx->write = do_write;

    ctx->output_plugin_options = output_plugin_options;
    ctx->fast_forward = fast_forward;

    (void)MemoryContextSwitchTo(old_context);

    return ctx;
}

static LogicalDecodingContext *StartupDecodingContextForArea(List *output_plugin_options, XLogRecPtr start_lsn,
                                                      TransactionId xmin_horizon, bool need_full_snapshot,
                                                      bool fast_forward, XLogPageReadCB read_page,
                                                      LogicalOutputPluginWriterPrepareWrite prepare_write,
                                                      LogicalOutputPluginWriterWrite do_write)
{
    MemoryContext context, old_context;
    LogicalDecodingContext *ctx = NULL;

    context = AllocSetContextCreate(CurrentMemoryContext, "Changeset Extraction Context", ALLOCSET_DEFAULT_MINSIZE,
                                    ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    old_context = MemoryContextSwitchTo(context);
    ctx = (LogicalDecodingContext *)palloc0(sizeof(LogicalDecodingContext));

    ctx->context = context;

    ctx->reader = XLogReaderAllocate(read_page, ctx);
    if (unlikely(ctx->reader == NULL))
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                        errmsg("memory is temporarily unavailable while allocate xlog reader")));

    ctx->reader->private_data = ctx;

    ctx->reorder = ReorderBufferAllocate();

    ctx->reorder->private_data = ctx;

    /* wrap output plugin callbacks, so we can add error context information */
    ctx->reorder->begin = begin_cb_wrapper;
    ctx->reorder->apply_change = change_cb_wrapper;
    ctx->reorder->commit = commit_cb_wrapper;
    ctx->reorder->abort = abort_cb_wrapper;
    ctx->reorder->prepare = prepare_cb_wrapper;


    ctx->out = makeStringInfo();
    ctx->prepare_write = prepare_write;
    ctx->write = do_write;

    ctx->output_plugin_options = output_plugin_options;
    ctx->fast_forward = fast_forward;

    (void)MemoryContextSwitchTo(old_context);

    return ctx;
}


static ParallelLogicalDecodingContext *ParallelStartupDecodingContext(List *output_plugin_options, XLogRecPtr start_lsn,
    TransactionId xmin_horizon, bool need_full_snapshot, bool fast_forward, XLogPageReadCB read_page, int slotId)
{
    ReplicationSlot *slot = NULL;
    MemoryContext old_context;
    ParallelLogicalDecodingContext *ctx = NULL;

    /* shorter lines... */
    slot = t_thrd.slot_cxt.MyReplicationSlot;

    old_context = MemoryContextSwitchTo(g_instance.comm_cxt.pdecode_cxt[slotId].parallelDecodeCtx);
    ctx = (ParallelLogicalDecodingContext *)palloc0(sizeof(ParallelLogicalDecodingContext));

    ctx->context = g_instance.comm_cxt.pdecode_cxt[slotId].parallelDecodeCtx;

    /* (re-)load output plugins, so we detect a bad (removed) output plugin now. */
    if (!fast_forward && slot != NULL)
        LoadOutputPlugin(&ctx->callbacks, NameStr(slot->data.plugin));

    /*
     * Now that the slot's xmin has been set, we can announce ourselves as a
     * logical decoding backend which doesn't need to be checked individually
     * when computing the xmin horizon because the xmin is enforced via
     * replication slots.
     */
    (void)LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
    t_thrd.pgxact->vacuumFlags |= PROC_IN_LOGICAL_DECODING;
    LWLockRelease(ProcArrayLock);

    ctx->reader = XLogReaderAllocate(read_page, ctx);
    if (unlikely(ctx->reader == NULL))
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                        errmsg("memory is temporarily unavailable while allocate xlog reader")));

    ctx->reader->private_data = ctx;
    ctx->slot = slot;

    ctx->reorder = ParallelReorderBufferAllocate(slotId);
    ctx->reorder->private_data = ctx;

    /* wrap output plugin callbacks, so we can add error context information */
    ctx->reorder->begin = begin_cb_wrapper;
    ctx->reorder->apply_change = parallel_change_cb_wrapper;
    ctx->reorder->commit = commit_cb_wrapper;

    ctx->out = makeStringInfo();

    ctx->output_plugin_options = output_plugin_options;
    ctx->fast_forward = fast_forward;

    (void)MemoryContextSwitchTo(old_context);

    return ctx;
}

/*
 * Create a new decoding context, for a new logical slot.
 *
 * plugin contains the name of the output plugin
 * output_plugin_options contains options passed to the output plugin
 * read_page, prepare_write, do_write are callbacks that have to be filled to
 *	  perform the use-case dependent, actual, work.
 *
 * Needs to be called while in a memory context that's at least as long lived
 * as the decoding context because further memory contexts will be created
 * inside it.
 *
 * Returns an initialized decoding context after calling the output plugin's
 * startup function.
 */
LogicalDecodingContext *CreateInitDecodingContext(const char *plugin, List *output_plugin_options,
                                                  bool need_full_snapshot, XLogPageReadCB read_page,
                                                  LogicalOutputPluginWriterPrepareWrite prepare_write,
                                                  LogicalOutputPluginWriterWrite do_write)
{
    TransactionId xmin_horizon = InvalidTransactionId;
    ReplicationSlot *slot = NULL;
    LogicalDecodingContext *ctx = NULL;
    MemoryContext old_context = NULL;
    int rc = 0;

    /* shorter lines... */
    slot = t_thrd.slot_cxt.MyReplicationSlot;

    /* first some sanity checks that are unlikely to be violated */
    if (slot == NULL)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("cannot perform logical decoding without a acquired slot")));

    if (plugin == NULL)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("cannot initialize logical decoding without a specified plugin")));

    /* Make sure the passed slot is suitable. These are user facing errors. */
    if (slot->data.database == InvalidOid)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("cannot use physical replication slot created for logical decoding")));

    if (slot->data.database != u_sess->proc_cxt.MyDatabaseId)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("replication slot \"%s\" was not created in this database", NameStr(slot->data.name))));

    if (IsTransactionState() && GetTopTransactionIdIfAny() != InvalidTransactionId)
        ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
                        errmsg("cannot create logical replication slot in transaction that has performed writes")));

    /* register output plugin name with slot */
    SpinLockAcquire(&slot->mutex);
    rc = strncpy_s(NameStr(slot->data.plugin), NAMEDATALEN, plugin, NAMEDATALEN - 1);
    securec_check(rc, "\0", "\0");
    NameStr(slot->data.plugin)[NAMEDATALEN - 1] = '\0';
    SpinLockRelease(&slot->mutex);

    /*
     * The replication slot mechanism is used to prevent removal of required
     * WAL. As there is no interlock between this and checkpoints required WAL
     * could be removed before ReplicationSlotsComputeRequiredLSN() has been
     * called to prevent that. In the very unlikely case that this happens
     * we'll just retry.
     */
    while (true) {
        XLogRecPtr segno;

        /*
         * Let's start with enough information if we can, so log a standby
         * snapshot and start decoding at exactly that position.
         */
        if (!RecoveryInProgress()) {
            XLogRecPtr flushptr;

            /* start at current insert position */
            slot->data.restart_lsn = GetXLogInsertRecPtr();

            /* make sure we have enough information to start */
            flushptr = LogStandbySnapshot();

            /* and make sure it's fsynced to disk */
            XLogWaitFlush(flushptr);
        } else
            slot->data.restart_lsn = GetRedoRecPtr();

        /* prevent WAL removal as fast as possible */
        ReplicationSlotsComputeRequiredLSN(NULL);

        /*
         * If all required WAL is still there, great, otherwise retry. The
         * slot should prevent further removal of WAL, unless there's a
         * concurrent ReplicationSlotsComputeRequiredLSN() after we've written
         * the new restart_lsn above, so normally we should never need to loop
         * more than twice.
         */
        XLByteToSeg(slot->data.restart_lsn, segno);
        XLogRecPtr LastRemovedSegno = XLogGetLastRemovedSegno();
        if (XLByteLT(LastRemovedSegno, segno))
            break;
    }

    /* ----
     * This is a bit tricky: We need to determine a safe xmin horizon to start
     * decoding from, to avoid starting from a running xacts record referring
     * to xids whose rows have been vacuumed or pruned
     * already. GetOldestSafeDecodingTransactionId() returns such a value, but
     * without further interlock it's return value might immediately be out of
     * date.
     *
     * So we have to acquire the ProcArrayLock to prevent computation of new
     * xmin horizons by other backends, get the safe decoding xid, and inform
     * the slot machinery about the new limit. Once that's done the
     * ProcArrayLock can be be released as the slot machinery now is
     * protecting against vacuum.
     *
     * Note that, temporarily, the data, not just the catalog, xmin has to be
     * reserved if a data snapshot is to be exported.  Otherwise the initial
     * data snapshot created here is not guaranteed to be valid. After that
     * the data xmin doesn't need to be managed anymore and the global xmin
     * should be recomputed. As we are fine with losing the pegged data xmin
     * after crash - no chance a snapshot would get exported anymore - we can
     * get away with just setting the slot's
     * effective_xmin. ReplicationSlotRelease will reset it again.
     *
     * ----
     */
    (void)LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);

    xmin_horizon = GetOldestSafeDecodingTransactionId(need_full_snapshot);

    slot->effective_catalog_xmin = xmin_horizon;
    slot->data.catalog_xmin = xmin_horizon;
    if (need_full_snapshot)
        slot->effective_xmin = xmin_horizon;

    ReplicationSlotsComputeRequiredXmin(true);

    LWLockRelease(ProcArrayLock);

    ReplicationSlotMarkDirty();
    ReplicationSlotSave();

    ctx = StartupDecodingContext(NIL, InvalidXLogRecPtr, xmin_horizon, need_full_snapshot, true, read_page,
                                 prepare_write, do_write);

    /* call output plugin initialization callback */
    old_context = MemoryContextSwitchTo(ctx->context);
    if (ctx->callbacks.startup_cb != NULL)
        startup_cb_wrapper(ctx, &ctx->options, true);
    (void)MemoryContextSwitchTo(old_context);

    return ctx;
}

/*
 * Create a new decoding context, for a logical slot that has previously been
 * used already.
 *
 * start_lsn contains the LSN of the last received data or InvalidXLogRecPtr
 * output_plugin_options contains options passed to the output plugin
 * read_page, prepare_write, do_write are callbacks that have to be filled to
 *	  perform the use-case dependent, actual, work.
 *
 * Needs to be called while in a memory context that's at least as long lived
 * as the decoding context because further memory contexts will be created
 * inside it.
 *
 * Returns an initialized decoding context after calling the output plugin's
 * startup function.
 */
LogicalDecodingContext *CreateDecodingContext(XLogRecPtr start_lsn, List *output_plugin_options, bool fast_forward,
                                              XLogPageReadCB read_page,
                                              LogicalOutputPluginWriterPrepareWrite prepare_write,
                                              LogicalOutputPluginWriterWrite do_write)
{
    LogicalDecodingContext *ctx = NULL;
    ReplicationSlot *slot = NULL;
    MemoryContext old_context = NULL;

    /* shorter lines... */
    slot = t_thrd.slot_cxt.MyReplicationSlot;

    /* first some sanity checks that are unlikely to be violated */
    if (slot == NULL)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("cannot perform logical decoding without a acquired slot")));

    /* make sure the passed slot is suitable, these are user facing errors */
    if (slot->data.database == InvalidOid)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        (errmsg("cannot use physical replication slot for logical decoding"))));

    if (slot->data.database != u_sess->proc_cxt.MyDatabaseId)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 (errmsg("replication slot \"%s\" was not created in this database", NameStr(slot->data.name)))));

    if (XLByteEQ(start_lsn, InvalidXLogRecPtr)) {
        /* continue from last position */
        start_lsn = slot->data.confirmed_flush;
    } else if (XLByteLT(start_lsn, slot->data.confirmed_flush)) {
        /*
         * It might seem like we should error out in this case, but it's
         * pretty common for a client to acknowledge a LSN it doesn't have to
         * do anything for, and thus didn't store persistently, because the
         * xlog records didn't result in anything relevant for logical
         * decoding. Clients have to be able to do that to support
         * synchronous replication.
         */
        if (!RecoveryInProgress())
            ereport(DEBUG1, (errmsg("cannot stream from %X/%X, minimum is %X/%X, forwarding", (uint32)(start_lsn >> 32),
                                    uint32(start_lsn), (uint32)(slot->data.confirmed_flush >> 32),
                                    (uint32)slot->data.confirmed_flush)));

        start_lsn = slot->data.confirmed_flush;
    }

    ctx = StartupDecodingContext(output_plugin_options, start_lsn, InvalidTransactionId, false, fast_forward, read_page,
                                 prepare_write, do_write);

    /* call output plugin initialization callback */
    old_context = MemoryContextSwitchTo(ctx->context);
    if (ctx->callbacks.startup_cb != NULL)
        startup_cb_wrapper(ctx, &ctx->options, false);
    (void)MemoryContextSwitchTo(old_context);

    if (!RecoveryInProgress())
        ereport(LOG, (errmsg("starting logical decoding for slot %s", NameStr(slot->data.name)),
                      errdetail("streaming transactions committing after %X/%X, reading WAL from %X/%X",
                                (uint32)(slot->data.confirmed_flush >> 32), (uint32)slot->data.confirmed_flush,
                                (uint32)(slot->data.restart_lsn >> 32), (uint32)slot->data.restart_lsn)));

    return ctx;
}


LogicalDecodingContext *CreateDecodingContextForArea(XLogRecPtr start_lsn, const char* plugin, List *output_plugin_options, bool fast_forward,
                                              XLogPageReadCB read_page,
                                              LogicalOutputPluginWriterPrepareWrite prepare_write,
                                              LogicalOutputPluginWriterWrite do_write)
{
    LogicalDecodingContext *ctx = NULL;
    MemoryContext old_context = NULL;

    ctx = StartupDecodingContextForArea(output_plugin_options, start_lsn, InvalidTransactionId, false, fast_forward, read_page,
                                 prepare_write, do_write);
    LoadOutputPlugin(&ctx->callbacks, plugin);

    /* call output plugin initialization callback */
    old_context = MemoryContextSwitchTo(ctx->context);
    if (ctx->callbacks.startup_cb != NULL)
        startup_cb_wrapper(ctx, &ctx->options, false);
    (void)MemoryContextSwitchTo(old_context);


    return ctx;
}

ParallelLogicalDecodingContext *ParallelCreateDecodingContext(XLogRecPtr start_lsn, List *output_plugin_options,
    bool fast_forward, XLogPageReadCB read_page, int slotId)
{
    ParallelLogicalDecodingContext *ctx = NULL;

    ctx = ParallelStartupDecodingContext(output_plugin_options, start_lsn, InvalidTransactionId, false, fast_forward,
        read_page, slotId);

    if (!RecoveryInProgress()) {
        ereport(LOG, (errmsg("starting logical decoding for slot "),
                      errdetail("streaming transactions committing after")));
    }

    return ctx;
}

/*
 * Returns true if an consistent initial decoding snapshot has been built.
 */
bool DecodingContextReady(LogicalDecodingContext *ctx)
{
    return SnapBuildCurrentState(ctx->snapshot_builder) == SNAPBUILD_CONSISTENT;
}

/*
 * Read from the decoding slot, until it is ready to start extracting changes.
 */
void DecodingContextFindStartpoint(LogicalDecodingContext *ctx)
{
    XLogRecPtr startptr;

    /* Initialize from where to start reading WAL. */
    startptr = ctx->slot->data.restart_lsn;
    if (!RecoveryInProgress()) {
        ereport(DEBUG1, (errmsg("searching for logical decoding starting point, starting at %X/%X",
                                (uint32)(ctx->slot->data.restart_lsn >> 32), (uint32)ctx->slot->data.restart_lsn)));
    }

    /* Wait for a consistent starting point */
    for (;;) {
        XLogRecord *record = 0;
        char *err = NULL;

        /* the read_page callback waits for new WAL */
        record = XLogReadRecord(ctx->reader, startptr, &err);
        if (err != NULL)
            ereport(ERROR, (errcode(ERRCODE_LOGICAL_DECODE_ERROR),
                            errmsg("Stopped to parse any valid XLog Record at %X/%X: %s.",
                                   (uint32)(ctx->reader->EndRecPtr >> 32), (uint32)ctx->reader->EndRecPtr, err)));

        Assert(record);

        startptr = InvalidXLogRecPtr;
        if (record != NULL) {
            LogicalDecodingProcessRecord(ctx, ctx->reader);
        }
        /* only continue till we found a consistent spot */
        if (DecodingContextReady(ctx))
            break;
        CHECK_FOR_INTERRUPTS();
    }

    ctx->slot->data.confirmed_flush = ctx->reader->EndRecPtr;
}

/*
 * Free a previously allocated decoding context, invoking the shutdown
 * callback if necessary.
 */
void FreeDecodingContext(LogicalDecodingContext *ctx)
{
    if (ctx->callbacks.shutdown_cb != NULL)
        shutdown_cb_wrapper(ctx);

    ReorderBufferFree(ctx->reorder);
    FreeSnapshotBuilder(ctx->snapshot_builder);
    XLogReaderFree(ctx->reader);
    MemoryContextDelete(ctx->context);
}

/*
 * Prepare a write using the context's output routine.
 */
void OutputPluginPrepareWrite(struct LogicalDecodingContext *ctx, bool last_write)
{
    if (!ctx->accept_writes)
        ereport(ERROR, (errcode(ERRCODE_LOGICAL_DECODE_ERROR),
                        errmsg("writes are only accepted in commit, begin and change callbacks")));

    ctx->prepare_write(ctx, ctx->write_location, ctx->write_xid, last_write);
    ctx->prepared_write = true;
}

/*
 * Perform a write using the context's output routine.
 */
void OutputPluginWrite(struct LogicalDecodingContext *ctx, bool last_write)
{
    if (!ctx->prepared_write)
        ereport(ERROR, (errcode(ERRCODE_LOGICAL_DECODE_ERROR),
                        errmsg("OutputPluginPrepareWrite needs to be called before OutputPluginWrite")));

    ctx->write(ctx, ctx->write_location, ctx->write_xid, last_write);
    ctx->prepared_write = false;
}

static void CheckLogicalAllowedPlugin(const char *plugin)
{
    bool have_slash = (first_dir_separator(plugin) != NULL);
    if (have_slash) {
        ereport(ERROR, (errcode(ERRCODE_LOGICAL_DECODE_ERROR),
            errmsg("The path cannot be specified for the decoding plugin.")));
    }
}

/*
 * Load the output plugin, lookup its output plugin init function, and check
 * that it provides the required callbacks.
 */
static void LoadOutputPlugin(OutputPluginCallbacks *callbacks, const char *plugin)
{
    CheckLogicalAllowedPlugin(plugin);
    LogicalOutputPluginInit plugin_init;
    CFunInfo tmpCF = load_external_function(plugin, "_PG_output_plugin_init", false, false);
    plugin_init = (LogicalOutputPluginInit)tmpCF.user_fn;

    if (plugin_init == NULL)
        ereport(ERROR, (errcode(ERRCODE_LOGICAL_DECODE_ERROR),
                        errmsg("output plugins have to declare the _PG_output_plugin_init symbol")));

    /* ask the output plugin to fill the callback struct */
    plugin_init(callbacks);

    if (callbacks->begin_cb == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_LOGICAL_DECODE_ERROR), errmsg("output plugins have to register a begin callback")));
    if (callbacks->change_cb == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_LOGICAL_DECODE_ERROR), errmsg("output plugins have to register a change callback")));
    if (callbacks->commit_cb == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_LOGICAL_DECODE_ERROR), errmsg("output plugins have to register a commit callback")));
    if (callbacks->abort_cb == NULL)
        ereport(WARNING,
                (errcode(ERRCODE_LOGICAL_DECODE_ERROR), errmsg("output plugins have to register a abort callback")));
}

static void LoadOutputPlugin(ParallelOutputPluginCallbacks *callbacks, const char *plugin)
{
    CheckLogicalAllowedPlugin(plugin);
    ParallelLogicalOutputPluginInit plugin_init;
    CFunInfo tmpCF = load_external_function(plugin, "_PG_output_plugin_init", false, false);
    plugin_init = (ParallelLogicalOutputPluginInit)tmpCF.user_fn;

    if (plugin_init == NULL) {
        ereport(ERROR, (errcode(ERRCODE_LOGICAL_DECODE_ERROR),
                        errmsg("output plugins have to declare the _PG_output_plugin_init symbol")));
    }

    /* ask the output plugin to fill the callback struct */
    plugin_init(callbacks);

    if (callbacks->begin_cb == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_LOGICAL_DECODE_ERROR), errmsg("output plugins have to register a begin callback")));
    }
    if (callbacks->change_cb == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_LOGICAL_DECODE_ERROR), errmsg("output plugins have to register a change callback")));
    }
    if (callbacks->commit_cb == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_LOGICAL_DECODE_ERROR), errmsg("output plugins have to register a commit callback")));
    }
}

static void output_plugin_error_callback(void *arg)
{
    LogicalErrorCallbackState *state = (LogicalErrorCallbackState *)arg;
    /* not all callbacks have an associated LSN  */
    if (!t_thrd.logical_cxt.IsAreaDecode) {
        if (!XLByteEQ(state->report_location, InvalidXLogRecPtr)) {
            (void)errcontext("slot \"%s\", output plugin \"%s\", in the %s callback, associated LSN %X/%X",
                             NameStr(state->ctx->slot->data.name), NameStr(state->ctx->slot->data.plugin),
                             state->callback_name, (uint32)(state->report_location >> 32), (uint32)state->report_location);
        } else {
            (void)errcontext("slot \"%s\", output plugin \"%s\", in the %s callback", NameStr(state->ctx->slot->data.name),
                             NameStr(state->ctx->slot->data.plugin), state->callback_name);
        }
    } else {
        if (!XLByteEQ(state->report_location, InvalidXLogRecPtr)) {
            (void)errcontext("Area decode:in the %s callback, associated LSN %X/%X",
                             state->callback_name, (uint32)(state->report_location >> 32), (uint32)state->report_location);
        } else {
            (void)errcontext("Area decode:in the %s callback",
                             state->callback_name);
        }
    }
}

static void parallel_change_cb_wrapper(ParallelReorderBuffer *cache, ReorderBufferTXN *txn, Relation relation,
    ParallelReorderBufferChange *change)
{
    ParallelLogicalDecodingContext *ctx = (ParallelLogicalDecodingContext *)cache->private_data;
    ParallelLogicalErrorCallbackState state;
    ErrorContextCallback errcallback;
    Assert(!ctx->fast_forward);
    /* Push callback + info on the error context stack */
    state.ctx = ctx;
    state.callback_name = "change";
    state.report_location = change->lsn;
    errcallback.previous = t_thrd.log_cxt.error_context_stack;
    errcallback.callback = output_plugin_error_callback;
    errcallback.arg = (void *)&state;
    t_thrd.log_cxt.error_context_stack = &errcallback;
    /* set output state */
    ctx->accept_writes = true;
    /*
     * report this change's lsn so replies from clients can give an up2date
     * answer. This won't ever be enough (and shouldn't be!) to confirm
     * receipt of this transaction, but it might allow another transaction's
     * commit to be confirmed with one message.
     */
    ctx->write_location = change->lsn;
    ctx->callbacks.change_cb(ctx, txn, relation, change);
    /* Pop the error context stack */
    t_thrd.log_cxt.error_context_stack = errcallback.previous;
}

static void startup_cb_wrapper(LogicalDecodingContext *ctx, OutputPluginOptions *opt, bool is_init)
{
    LogicalErrorCallbackState state;
    ErrorContextCallback errcallback;
    Assert(!ctx->fast_forward);

    /* Push callback + info on the error context stack */
    state.ctx = ctx;
    state.callback_name = "startup";
    state.report_location = InvalidXLogRecPtr;
    errcallback.callback = output_plugin_error_callback;
    errcallback.arg = (void *)&state;
    errcallback.previous = t_thrd.log_cxt.error_context_stack;
    t_thrd.log_cxt.error_context_stack = &errcallback;

    /* set output state */
    ctx->accept_writes = false;

    /* do the actual work: call callback */
    ctx->callbacks.startup_cb(ctx, opt, is_init);

    /* Pop the error context stack */
    t_thrd.log_cxt.error_context_stack = errcallback.previous;
}

void shutdown_cb_wrapper(LogicalDecodingContext *ctx)
{
    LogicalErrorCallbackState state;
    ErrorContextCallback errcallback;
    Assert(!ctx->fast_forward);

    /* Push callback + info on the error context stack */
    state.ctx = ctx;
    state.callback_name = "shutdown";
    state.report_location = InvalidXLogRecPtr;
    errcallback.callback = output_plugin_error_callback;
    errcallback.arg = (void *)&state;
    errcallback.previous = t_thrd.log_cxt.error_context_stack;
    t_thrd.log_cxt.error_context_stack = &errcallback;

    /* set output state */
    ctx->accept_writes = false;

    /* do the actual work: call callback */
    ctx->callbacks.shutdown_cb(ctx);

    /* Pop the error context stack */
    t_thrd.log_cxt.error_context_stack = errcallback.previous;
}

static void abort_cb_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn)
{
    LogicalDecodingContext *ctx = (LogicalDecodingContext *)cache->private_data;
    LogicalErrorCallbackState state;
    ErrorContextCallback errcallback;

    Assert(!ctx->fast_forward);

    /* Push callback + info on the error context stack */
    state.ctx = ctx;
    state.callback_name = "abort";
    state.report_location = txn->final_lsn; /* beginning of abort record */
    errcallback.callback = output_plugin_error_callback;
    errcallback.arg = (void *)&state;
    errcallback.previous = t_thrd.log_cxt.error_context_stack;
    t_thrd.log_cxt.error_context_stack = &errcallback;

    /* set output state */
    ctx->accept_writes = true;
    ctx->write_xid = txn->xid;
    ctx->write_location = txn->end_lsn; /* points to the end of the record */

    /* do the actual work: call callback */
    ctx->callbacks.abort_cb(ctx, txn);

    /* Pop the error context stack */
    t_thrd.log_cxt.error_context_stack = errcallback.previous;
}

static void prepare_cb_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn)
{
    LogicalDecodingContext *ctx = (LogicalDecodingContext *)cache->private_data;
    LogicalErrorCallbackState state;
    ErrorContextCallback errcallback;

    Assert(!ctx->fast_forward);

    /* Push callback + info on the error context stack */
    state.ctx = ctx;
    state.callback_name = "prepare";
    state.report_location = txn->final_lsn; /* beginning of abort record */
    errcallback.callback = output_plugin_error_callback;
    errcallback.arg = (void *)&state;
    errcallback.previous = t_thrd.log_cxt.error_context_stack;
    t_thrd.log_cxt.error_context_stack = &errcallback;

    /* set output state */
    ctx->accept_writes = true;
    ctx->write_xid = txn->xid;
    ctx->write_location = txn->end_lsn; /* points to the end of the record */

    /* do the actual work: call callback */
    if (ctx->callbacks.prepare_cb) {
        ctx->callbacks.prepare_cb(ctx, txn);
    }

    /* Pop the error context stack */
    t_thrd.log_cxt.error_context_stack = errcallback.previous;
}


/*
 * Callbacks for ReorderBuffer which add in some more information and then call
 * output_plugin.h plugins.
 */
static void begin_cb_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn)
{
    LogicalDecodingContext *ctx = (LogicalDecodingContext *)cache->private_data;
    LogicalErrorCallbackState state;
    ErrorContextCallback errcallback;

    Assert(!ctx->fast_forward);

    /* Push callback + info on the error context stack */
    state.ctx = ctx;
    state.callback_name = "begin";
    state.report_location = txn->first_lsn;
    errcallback.callback = output_plugin_error_callback;
    errcallback.arg = (void *)&state;
    errcallback.previous = t_thrd.log_cxt.error_context_stack;
    t_thrd.log_cxt.error_context_stack = &errcallback;

    /* set output state */
    ctx->accept_writes = true;
    ctx->write_xid = txn->xid;
    ctx->write_location = txn->first_lsn;

    /* do the actual work: call callback */
    ctx->callbacks.begin_cb(ctx, txn);

    /* Pop the error context stack */
    t_thrd.log_cxt.error_context_stack = errcallback.previous;
}

static void commit_cb_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn, XLogRecPtr commit_lsn)
{
    LogicalDecodingContext *ctx = (LogicalDecodingContext *)cache->private_data;
    LogicalErrorCallbackState state;
    ErrorContextCallback errcallback;

    Assert(!ctx->fast_forward);

    /* Push callback + info on the error context stack */
    state.ctx = ctx;
    state.callback_name = "commit";
    state.report_location = txn->final_lsn; /* beginning of commit record */
    errcallback.callback = output_plugin_error_callback;
    errcallback.arg = (void *)&state;
    errcallback.previous = t_thrd.log_cxt.error_context_stack;
    t_thrd.log_cxt.error_context_stack = &errcallback;

    /* set output state */
    ctx->accept_writes = true;
    ctx->write_xid = txn->xid;
    ctx->write_location = txn->end_lsn; /* points to the end of the record */

    /* do the actual work: call callback */
    ctx->callbacks.commit_cb(ctx, txn, commit_lsn);

    /* Pop the error context stack */
    t_thrd.log_cxt.error_context_stack = errcallback.previous;
}

static void change_cb_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn, Relation relation,
                              ReorderBufferChange *change)
{
    LogicalDecodingContext *ctx = (LogicalDecodingContext *)cache->private_data;
    LogicalErrorCallbackState state;
    ErrorContextCallback errcallback;
    Assert(!ctx->fast_forward);

    /* Push callback + info on the error context stack */
    state.ctx = ctx;
    state.callback_name = "change";
    state.report_location = change->lsn;
    errcallback.callback = output_plugin_error_callback;
    errcallback.arg = (void *)&state;
    errcallback.previous = t_thrd.log_cxt.error_context_stack;
    t_thrd.log_cxt.error_context_stack = &errcallback;

    /* set output state */
    ctx->accept_writes = true;
    if (txn != NULL) {
        ctx->write_xid = txn->xid;
    }
    /*
     * report this change's lsn so replies from clients can give an up2date
     * answer. This won't ever be enough (and shouldn't be!) to confirm
     * receipt of this transaction, but it might allow another transaction's
     * commit to be confirmed with one message.
     */
    ctx->write_location = change->lsn;

    ctx->callbacks.change_cb(ctx, txn, relation, change);

    /* Pop the error context stack */
    t_thrd.log_cxt.error_context_stack = errcallback.previous;
}

bool filter_by_origin_cb_wrapper(LogicalDecodingContext *ctx, RepOriginId origin_id)
{
    LogicalErrorCallbackState state;
    ErrorContextCallback errcallback;
    bool ret = false;

    /* Push callback + info on the error context stack */
    state.ctx = ctx;
    state.callback_name = "shutdown";
    state.report_location = InvalidXLogRecPtr;
    errcallback.callback = output_plugin_error_callback;
    errcallback.arg = (void *)&state;
    errcallback.previous = t_thrd.log_cxt.error_context_stack;
    t_thrd.log_cxt.error_context_stack = &errcallback;

    /* set output state */
    ctx->accept_writes = false;

    /* do the actual work: call callback */
    ret = ctx->callbacks.filter_by_origin_cb(ctx, origin_id);

    /* Pop the error context stack */
    t_thrd.log_cxt.error_context_stack = errcallback.previous;

    return ret;
}

/*
 * Set the required catalog xmin horizon for historic snapshots in the current
 * replication slot.
 *
 * Note that in the most cases, we won't be able to immediately use the xmin
 * to increase the xmin horizon, we need to wait till the client has confirmed
 * receiving current_lsn with LogicalConfirmReceivedLocation().
 */
void LogicalIncreaseXminForSlot(XLogRecPtr current_lsn, TransactionId xmin)
{
    bool updated_xmin = false;
    ReplicationSlot *slot = NULL;

    slot = t_thrd.slot_cxt.MyReplicationSlot;

    Assert(slot != NULL);

    SpinLockAcquire(&slot->mutex);

    /*
     * don't overwrite if we already have a newer xmin. This can
     * happen if we restart decoding in a slot.
     */
    if (TransactionIdPrecedesOrEquals(xmin, slot->data.catalog_xmin)) {
    } else if (XLByteLE(current_lsn, slot->data.confirmed_flush)) {
        /*
         * If the client has already confirmed up to this lsn, we directly
         * can mark this as accepted. This can happen if we restart
         * decoding in a slot.
         */
        slot->candidate_catalog_xmin = xmin;
        slot->candidate_xmin_lsn = current_lsn;

        /* our candidate can directly be used */
        updated_xmin = true;
    } else if (XLByteEQ(slot->candidate_xmin_lsn, InvalidXLogRecPtr)) {
        /*
         * Only increase if the previous values have been applied, otherwise we
         * might never end up updating if the receiver acks too slowly.
         */
        slot->candidate_catalog_xmin = xmin;
        slot->candidate_xmin_lsn = current_lsn;
    }
    SpinLockRelease(&slot->mutex);

    /* candidate already valid with the current flush position, apply */
    if (updated_xmin)
        LogicalConfirmReceivedLocation(slot->data.confirmed_flush);
}

/*
 * Mark the minimal LSN (restart_lsn) we need to read to replay all
 * transactions that have not yet committed at current_lsn.
 *
 * Just like IncreaseRestartDecodingForSlot this nly takes effect when the
 * client has confirmed to have received current_lsn.
 */
void LogicalIncreaseRestartDecodingForSlot(XLogRecPtr current_lsn, XLogRecPtr restart_lsn)
{
    bool updated_lsn = false;
    ReplicationSlot *slot = NULL;

    slot = t_thrd.slot_cxt.MyReplicationSlot;

    Assert(slot != NULL);
    Assert(!XLByteEQ(restart_lsn, InvalidXLogRecPtr));
    Assert(!XLByteEQ(current_lsn, InvalidXLogRecPtr));

    SpinLockAcquire(&slot->mutex);

    /* don't overwrite if have a newer restart lsn */
    if (XLByteLE(restart_lsn, slot->data.restart_lsn)) {
    } else if (XLByteLE(current_lsn, slot->data.confirmed_flush)) {
        /*
         * We might have already flushed far enough to directly accept this lsn, in
         * this case there is no need to check for existing candidate LSNs
         */
        slot->candidate_restart_valid = current_lsn;
        slot->candidate_restart_lsn = restart_lsn;

        /* our candidate can directly be used */
        updated_lsn = true;
    }
    /*
     * Only increase if the previous values have been applied, otherwise we
     * might never end up updating if the receiver acks too slowly. A missed
     * value here will just cause some extra effort after reconnecting.
     */
    if (XLByteEQ(slot->candidate_restart_valid, InvalidXLogRecPtr)) {
        slot->candidate_restart_valid = current_lsn;
        slot->candidate_restart_lsn = restart_lsn;
        if (!RecoveryInProgress())
            ereport(DEBUG1, (errmsg("got new restart lsn %X/%X at %X/%X", (uint32)(restart_lsn >> 32),
                                    (uint32)restart_lsn, (uint32)(current_lsn >> 32), (uint32)current_lsn)));
    } else {
        if (!RecoveryInProgress())
            ereport(
                DEBUG1,
                (errmsg("failed to increase restart lsn: proposed %X/%X, after %X/%X, current candidate %X/%X, current "
                        "after %X/%X, flushed up to %X/%X",
                        (uint32)(restart_lsn >> 32), (uint32)restart_lsn, (uint32)(current_lsn >> 32),
                        (uint32)current_lsn, (uint32)(slot->candidate_restart_lsn >> 32),
                        (uint32)slot->candidate_restart_lsn, (uint32)(slot->candidate_restart_valid >> 32),
                        (uint32)slot->candidate_restart_valid, (uint32)(slot->data.confirmed_flush >> 32),
                        (uint32)slot->data.confirmed_flush)));
    }
    SpinLockRelease(&slot->mutex);

    /* candidates are already valid with the current flush position, apply */
    if (updated_lsn)
        LogicalConfirmReceivedLocation(slot->data.confirmed_flush);
}

/*
 * Handle a consumer's conformation having received all changes up to lsn.
 */
void LogicalConfirmReceivedLocation(XLogRecPtr lsn)
{
    /*
     * Check if the slot is not moving backwards. Logical slots have confirmed
     * consumption up to confirmed_lsn, meaning that data older than that is
     * not available anymore.
     */
    if (XLByteLE(lsn, t_thrd.slot_cxt.MyReplicationSlot->data.confirmed_flush))
        return;

    Assert(!XLByteEQ(lsn, InvalidXLogRecPtr));
    (void)LWLockAcquire(LogicalReplicationSlotPersistentDataLock, LW_EXCLUSIVE);
    /* Do an unlocked check for candidate_lsn first. */
    if (!XLByteEQ(t_thrd.slot_cxt.MyReplicationSlot->candidate_xmin_lsn, InvalidXLogRecPtr) ||
        !XLByteEQ(t_thrd.slot_cxt.MyReplicationSlot->candidate_restart_valid, InvalidXLogRecPtr)) {
        bool updated_xmin = false;
        bool updated_restart = false;

        /* use volatile pointer to prevent code rearrangement */
        ReplicationSlot *slot = t_thrd.slot_cxt.MyReplicationSlot;

        SpinLockAcquire(&slot->mutex);

        slot->data.confirmed_flush = lsn;

        /* if were past the location required for bumping xmin, do so */
        if (!XLByteEQ(slot->candidate_xmin_lsn, InvalidXLogRecPtr) && XLByteLE(slot->candidate_xmin_lsn, lsn)) {
            /*
             * We have to write the changed xmin to disk *before* we change
             * the in-memory value, otherwise after a crash we wouldn't know
             * that some catalog tuples might have been removed already.
             *
             * Ensure that by first writing to ->xmin and only update
             * ->effective_xmin once the new state is synced to disk. After a
             * crash ->effective_xmin is set to ->xmin.
             */
            if (TransactionIdIsValid(slot->candidate_catalog_xmin) &&
                slot->data.catalog_xmin != slot->candidate_catalog_xmin) {
                slot->data.catalog_xmin = slot->candidate_catalog_xmin;
                slot->candidate_catalog_xmin = InvalidTransactionId;
                slot->candidate_xmin_lsn = InvalidXLogRecPtr;
                updated_xmin = true;
            }
        }

        if (!XLByteEQ(slot->candidate_restart_valid, InvalidXLogRecPtr) &&
            XLByteLE(slot->candidate_restart_valid, lsn)) {
            Assert(!XLByteEQ(slot->candidate_restart_lsn, InvalidXLogRecPtr));
            Assert(XLByteLE(slot->data.restart_lsn, slot->candidate_restart_lsn));

            slot->data.restart_lsn = slot->candidate_restart_lsn;
            slot->candidate_restart_lsn = InvalidXLogRecPtr;
            slot->candidate_restart_valid = InvalidXLogRecPtr;
            updated_restart = true;
        }

        SpinLockRelease(&slot->mutex);

        /* first write new xmin to disk, so we know whats up after a crash */
        if (updated_xmin || updated_restart) {
            ReplicationSlotMarkDirty();
            ReplicationSlotSave();
            if (!RecoveryInProgress())
                ereport(DEBUG1, (errmsg("updated xmin: %d restart: %d", updated_xmin, updated_restart)));
        }
        /*
         * Now the new xmin is safely on disk, we can let the global value
         * advance. We do not take ProcArrayLock or similar since we only
         * advance xmin here and there's not much harm done by a concurrent
         * computation missing that.
         */
        if (updated_xmin) {
            SpinLockAcquire(&slot->mutex);
            slot->effective_catalog_xmin = slot->data.catalog_xmin;
            SpinLockRelease(&slot->mutex);

            ReplicationSlotsComputeRequiredXmin(false);
            ReplicationSlotsComputeRequiredLSN(NULL);
        }
    } else {
        volatile ReplicationSlot *slot = t_thrd.slot_cxt.MyReplicationSlot;

        SpinLockAcquire(&slot->mutex);
        slot->data.confirmed_flush = lsn;
        SpinLockRelease(&slot->mutex);
    }
    LWLockRelease(LogicalReplicationSlotPersistentDataLock);
}

/* Connect primary to advance logical replication slot. */
void LogicalAdvanceConnect()
{
    char conninfoRepl[MAXCONNINFO + 75];
    char conninfo[MAXCONNINFO];
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    PGresult* res = NULL;
    int count = 0;
    int retryNum = 10;
    uint32 remoteSversion;
    uint32 localSversion;
    char *remotePversion = NULL;
    char *localPversion = NULL;
    uint32 remoteTerm;
    uint32 localTerm;
    errno_t rc = 0;
    int nRet = 0;

    rc = memset_s(conninfo, MAXCONNINFO, 0, MAXCONNINFO);
    securec_check(rc, "\0", "\0");

    /* Fetch information required to start streaming */
    rc = strncpy_s(conninfo, MAXCONNINFO, (char *)walrcv->conninfo, MAXCONNINFO - 1);
    securec_check(rc, "\0", "\0");

    nRet = snprintf_s(conninfoRepl, sizeof(conninfoRepl), sizeof(conninfoRepl) - 1,
                      "%s dbname=replication replication=true "
                      "fallback_application_name=%s "
                      "connect_timeout=%d",
                      conninfo, "DRS_sender",
                      u_sess->attr.attr_storage.wal_receiver_connect_timeout);
    securec_check_ss(nRet, "", "");

retry:
    /* 1. try to connect to primary */
    t_thrd.walsender_cxt.advancePrimaryConn = PQconnectdb(conninfoRepl);
    if (PQstatus(t_thrd.walsender_cxt.advancePrimaryConn) != CONNECTION_OK) {
        if (++count < retryNum) {
            ereport(LOG,
                (errmsg("DRS_sender could not connect to the remote server, "
                        "the connection info :%s : %s",
                        conninfo, PQerrorMessage(t_thrd.walsender_cxt.advancePrimaryConn))));

            PQfinish(t_thrd.walsender_cxt.advancePrimaryConn);
            t_thrd.walsender_cxt.advancePrimaryConn = NULL;

            /* sleep 0.1 s */
            pg_usleep(100000L);
            goto retry;
        }
        ereport(FATAL,
            (errmsg("DRS_sender could not connect to the remote server, "
                     "we have tried %d times, the connection info :%s : %s",
                     count, conninfo, PQerrorMessage(t_thrd.walsender_cxt.advancePrimaryConn))));
    }

    /* 2. identify version */
    res = PQexec(t_thrd.walsender_cxt.advancePrimaryConn, "IDENTIFY_VERSION");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        PQclear(res);
        ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                        errmsg("could not receive database system version and protocol "
                               "version from the remote server: %s",
                               PQerrorMessage(t_thrd.walsender_cxt.advancePrimaryConn))));

        return;
    }
    if (PQnfields(res) != 3 || PQntuples(res) != 1) {
        int numTuples = PQntuples(res);
        int numFields = PQnfields(res);

        PQclear(res);
        ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                        errmsg("invalid response from remote server"),
                        errdetail("Expected 1 tuple with 3 fields, got %d tuples with %d fields.",
                                  numTuples, numFields)));

        return;
    }
    remoteSversion = pg_strtoint32(PQgetvalue(res, 0, 0));
    localSversion = PG_VERSION_NUM;
    remotePversion = PQgetvalue(res, 0, 1);
    localPversion = pstrdup(PG_PROTOCOL_VERSION);
    remoteTerm = pg_strtoint32(PQgetvalue(res, 0, 2));
    localTerm = Max(g_instance.comm_cxt.localinfo_cxt.term_from_file,
                    g_instance.comm_cxt.localinfo_cxt.term_from_xlog);
    ereport(LOG, (errmsg("remote term[%u], local term[%u]", remoteTerm, localTerm)));

    if (remoteSversion != localSversion ||
        strncmp(remotePversion, localPversion, strlen(PG_PROTOCOL_VERSION)) != 0) {
        PQclear(res);

        if (remoteSversion != localSversion) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                            errmsg("database system version is different between the remote and local"),
                            errdetail("The remote's system version is %u, the local's system version is %u.",
                                      remoteSversion, localSversion)));
        } else {
            ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                            errmsg("the remote protocal version %s is not the same as "
                                   "the local protocal version %s.",
                                   remotePversion, localPversion)));
        }

        if (localPversion != NULL) {
            pfree(localPversion);
            localPversion = NULL;
        }
        return;
    }

    PQclear(res);

    /* 3. connect to primary, check remote role */
    res = PQexec(t_thrd.walsender_cxt.advancePrimaryConn, "IDENTIFY_MODE");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        PQclear(res);
        ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                        errmsg("could not receive the ongoing mode infomation from "
                               "the remote server: %s",
                               PQerrorMessage(t_thrd.walsender_cxt.advancePrimaryConn))));

        return;
    }
    if (PQnfields(res) != 1 || PQntuples(res) != 1) {
        int numTuples = PQntuples(res);
        int numFields = PQnfields(res);

        PQclear(res);
        ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                        errmsg("invalid response from remote server"),
                        errdetail("Expected 1 tuple with 1 fields, got %d tuples with %d fields.",
                                  numTuples, numFields)));

        return;
    }
    ServerMode remoteMode = (ServerMode)pg_strtoint32(PQgetvalue(res, 0, 0));
    if (!t_thrd.walreceiver_cxt.AmWalReceiverForFailover && remoteMode != PRIMARY_MODE) {
        PQclear(res);
        ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                        errmsg("the mode of the remote server must be primary, current is %s",
                               wal_get_role_string(remoteMode))));

        return;
    }

    PQclear(res);
}

/* Clean the connection for advance logical replication slot. */
void CloseLogicalAdvanceConnect()
{
    if (t_thrd.walsender_cxt.advancePrimaryConn != NULL) {
        PQfinish(t_thrd.walsender_cxt.advancePrimaryConn);
        t_thrd.walsender_cxt.advancePrimaryConn = NULL;
    }
}

/* Notify the primary to advance logical replication slot. */
void NotifyPrimaryAdvance(XLogRecPtr restart, XLogRecPtr flush)
{
    char query[256];
    PGresult* res = NULL;
    int nRet = 0;

    nRet = snprintf_s(query, sizeof(query), sizeof(query) - 1,
                      "ADVANCE_REPLICATION SLOT \"%s\" LOGICAL %X/%X %X/%X",
                      NameStr(t_thrd.slot_cxt.MyReplicationSlot->data.name),
                      (uint32)(restart >> 32),
                      (uint32)restart,
                      (uint32)(flush >> 32),
                      (uint32)flush);

    securec_check_ss_c(nRet, "\0", "\0");

    if (t_thrd.walsender_cxt.advancePrimaryConn == NULL) {
        LogicalAdvanceConnect();
    }
    res = PQexec(t_thrd.walsender_cxt.advancePrimaryConn, query);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        PQclear(res);
        ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                        errmsg("could not send replication command \"%s\": %s\n",
                               query, PQerrorMessage(t_thrd.walsender_cxt.advancePrimaryConn))));

        return;
    }

    if (PQnfields(res) != 2 || PQntuples(res) != 1) {
            int numTuples = PQntuples(res);
            int numFields = PQnfields(res);

            PQclear(res);
            ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                            errmsg("invalid response from remote server"),
                            errdetail("Expected 1 tuple with 2 fields, got %d tuples with %d fields.",
                                      numTuples, numFields)));

            return;
    }

    PQclear(res);
}

/* Check a boolean option with a default value */
void CheckBooleanOption(DefElem *elem, bool *booleanOption, bool defaultValue)
{
    if (elem->arg == NULL) {
        *booleanOption = defaultValue;
    } else if (!parse_bool(strVal(elem->arg), booleanOption)) {
        ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("could not parse value \"%s\" for parameter \"%s\"", strVal(elem->arg), elem->defname),
            errdetail("N/A"),  errcause("Wrong input value"), erraction("Input \"on\" or \"off\"")));
    }
}

/* parse each decoding option */
void ParseDecodingOptionPlugin(ListCell* option, PluginTestDecodingData* data, OutputPluginOptions* opt)
{
    DefElem* elem = (DefElem*)lfirst(option);

    Assert(elem->arg == NULL || IsA(elem->arg, String));

    if (strncmp(elem->defname, "force-binary", sizeof("force-binary")) == 0) {
        bool force_binary = false;
        CheckBooleanOption(elem, &force_binary, false);
        if (force_binary) {
            opt->output_type = OUTPUT_PLUGIN_BINARY_OUTPUT;
        }
    } else if (strncmp(elem->defname, "include-xids", sizeof("include-xids")) == 0) {
        /* if option does not provide a value, it means its value is true */
        CheckBooleanOption(elem, &data->include_xids, true);
    } else if (strncmp(elem->defname, "include-timestamp", sizeof("include-timestamp")) == 0) {
        CheckBooleanOption(elem, &data->include_timestamp, false);
    } else if (strncmp(elem->defname, "skip-empty-xacts", sizeof("skip-empty-xacts")) == 0) {
        CheckBooleanOption(elem, &data->skip_empty_xacts, false);
    } else if (strncmp(elem->defname, "only-local", sizeof("only-local"))== 0) {
        CheckBooleanOption(elem, &data->only_local, true);
    } else if (strncmp(elem->defname, "white-table-list", sizeof("white-table-list")) == 0) {
        ParseWhiteList(&data->tableWhiteList, elem);
    } else if (strncmp(elem->defname, "standby-connection", sizeof("standby-connection")) == 0 &&
        t_thrd.role != WORKER) {
        CheckBooleanOption(elem, &t_thrd.walsender_cxt.standbyConnection, false);
    } else if (strncmp(elem->defname, "parallel-decode-num", sizeof("parallel-decode-num")) != 0) {
        ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("option \"%s\" = \"%s\" is unknown", elem->defname, elem->arg ? strVal(elem->arg) : "(null)"),
            errdetail("N/A"),  errcause("Wrong input option"), erraction("Please check documents for help")));
    }
}

