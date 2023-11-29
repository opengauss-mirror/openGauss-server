/*-------------------------------------------------------------------------
 * tablesync.c
 *	  PostgreSQL logical replication: initial table data synchronization
 *
 * Copyright (c) 2012-2016, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/logical/tablesync.c
 *
 * NOTES
 *	  This file contains code for initial table data synchronization for
 *	  logical replication.
 *
 *	  The initial data synchronization is done separately for each table,
 *	  in separate apply worker that only fetches the initial snapshot data
 *	  from the publisher and then synchronizes the position in stream with
 *	  the main apply worker.
 *
 *	  The are several reasons for doing the synchronization this way:
 *	   - It allows us to parallelize the initial data synchronization
 *		 which lowers the time needed for it to happen.
 *	   - The initial synchronization does not have to hold the xid and LSN
 *		 for the time it takes to copy data of all tables, causing less
 *		 bloat and lower disk consumption compared to doing the
 *		 synchronization in single process for whole database.
 *	   - It allows us to synchronize the tables added after the initial
 *		 synchronization has finished.
 *
 *	  The stream position synchronization works in multiple steps:
 *	   - Apply worker requests a tablesync worker to start, setting the new
 *		 table state to INIT.
 *	   - Tablesync worker starts; changes table state from INIT to DATASYNC while
 *		 copying.
 *	   - Tablesync worker does initial table copy; there is a FINISHEDCOPY (sync
 *		 worker specific) state to indicate when the copy phase has completed, so
 *		 if the worker crashes with this (non-memory) state then the copy will not
 *		 be re-attempted.
 *	   - Tablesync worker then sets table state to SYNCWAIT; waits for state change.
 *	   - Apply worker periodically checks for tables in SYNCWAIT state.  When
 *		 any appear, it sets the table state to CATCHUP and starts loop-waiting
 *		 until either the table state is set to SYNCDONE or the sync worker
 *		 exits.
 *	   - After the sync worker has seen the state change to CATCHUP, it will
 *		 read the stream and apply changes (acting like an apply worker) until
 *		 it catches up to the specified stream position.  Then it sets the
 *		 state to SYNCDONE.  There might be zero changes applied between
 *		 CATCHUP and SYNCDONE, because the sync worker might be ahead of the
 *		 apply worker.
 *	   - Once the state is set to SYNCDONE, the apply will continue tracking
 *		 the table until it reaches the SYNCDONE stream position, at which
 *		 point it sets state to READY and stops tracking.  Again, there might
 *		 be zero changes in between.
 *
 *    So the state progression is always: INIT -> DATASYNC -> FINISHEDCOPY
 *    -> SYNCWAIT -> CATCHUP -> SYNCDONE -> READY.
 *
  *	  The catalog pg_subscription_rel is used to keep information about
 *	  subscribed tables and their state. Some transient state during data
 *	  synchronization is kept in shared memory.  The states SYNCWAIT and
 *	  CATCHUP only appear in memory.
 *
 *	  Example flows look like this:
 *	   - Apply is in front:
 *		  sync:8
 *			-> set in catalog FINISHEDCOPY
 *			-> set in memory SYNCWAIT
 *		  apply:10
 *			-> set in memory CATCHUP
 *			-> enter wait-loop
 *		  sync:10
 *			-> set in catalog SYNCDONE 
 *			-> exit
 *		  apply:10
 *			-> exit wait-loop
 *			-> continue rep
  *		  apply:11
 *			-> set in catalog READY
 *
 *	   - Sync is in front:
 *		  sync:10
 *			-> set in catalog FINISHEDCOPY
 *			-> set in memory SYNCWAIT
 *		  apply:8
 *			-> set in memory CATCHUP
 *			-> continue per-table filtering
 *		  sync:10
 *			-> set in catalog SYNCDONE
 *			-> exit
 *		  apply:10
 *			-> set in catalog READY
 *			-> stop per-table filtering
 *			-> continue rep
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"
#include "pgstat.h"

#include "access/xact.h"

#include "catalog/pg_subscription_rel.h"
#include "catalog/pg_type.h"

#include "commands/copy.h"
#include "commands/subscriptioncmds.h"

#include "replication/logicallauncher.h"
#include "replication/logicalrelation.h"
#include "replication/walreceiver.h"
#include "replication/worker_internal.h"

#include "storage/ipc.h"
#include "storage/lmgr.h"

#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "access/tableam.h"
#include "libpq/libpq-fe.h"

static void finish_sync_worker(char *slotName = NULL);

/*
 * Exit routine for synchronization worker.
 */
static void finish_sync_worker(char *slotName)
{
    /*
     * Commit any outstanding transaction. This is the usual case, unless
     * there was nothing to do for the table.
     */
    if (IsTransactionState()) {
        CommitTransactionCommand();
        pgstat_report_stat(false);
    }

    /* And flush all writes. */
    XLogWaitFlush(GetXLogWriteRecPtr());

    ereport(LOG, (errmsg("logical replication table synchronization worker for subscription \"%s\","
                         " table \"%s\" has finished", t_thrd.applyworker_cxt.mySubscription->name,
                         get_rel_name(t_thrd.applyworker_cxt.curWorker->relid))));

    /* Stop gracefully */
    (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_disconnect();

    /* Cleanup the tablesync slot. */
    if (slotName != NULL) {
        if (!AttemptConnectPublisher(t_thrd.applyworker_cxt.mySubscription->conninfo, slotName, true)) {
            ereport(ERROR, (errmsg("could not connect to the publisher: %s",
                                PQerrorMessage(t_thrd.libwalreceiver_cxt.streamConn))));
        }
        /*
         * It is important to give an error if we are unable to drop the slot,
         * otherwise, it won't be dropped till the corresponding subscription
         * is dropped. So passing missing_ok = false.
         */
        ReplicationSlotDropAtPubNode(slotName, false);
        /* Stop gracefully */
        (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_disconnect();
    }

    /* Find the main apply worker and signal it. */
    logicalrep_worker_wakeup(t_thrd.applyworker_cxt.curWorker->subid, InvalidOid);

    proc_exit(0);
}

/*
 * Wait until the relation sync state is set in catalog to the expected
 * one; return true when it happens.
 *
 * Returns false if the table sync worker or the table itself have
 * disappeared, or the table state has been reset.
 *
 * Currently, this is used in the apply worker when transitioning from
 * CATCHUP state to SYNCDONE.
 */
static bool wait_for_relation_state_change(Oid relid, char expected_state)
{
    int rc;
    char state;

    for (;;) {
        LogicalRepWorker *worker;
        XLogRecPtr statelsn;

        CHECK_FOR_INTERRUPTS();

        state = GetSubscriptionRelState(t_thrd.applyworker_cxt.curWorker->subid,
                                        relid, &statelsn);

        if (state == SUBREL_STATE_UNKNOWN)
            break;

        if (state == expected_state)
            return true;

        /* Check if the sync worker is still running and bail if not. */
        LWLockAcquire(LogicalRepWorkerLock, LW_SHARED);
        worker = logicalrep_worker_find(t_thrd.applyworker_cxt.curWorker->subid, relid, false);
        LWLockRelease(LogicalRepWorkerLock);
        if (!worker) {
            break;
        }

        pgstat_report_waitevent(WAIT_EVENT_LOGICAL_SYNC_STATE_CHANGE);
        rc = WaitLatch(&t_thrd.proc->procLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, 10000L);
        pgstat_report_waitevent(WAIT_EVENT_END);

        /* emergency bailout if postmaster has died */
        if (rc & WL_POSTMASTER_DEATH)
            proc_exit(1);

        ResetLatch(&t_thrd.proc->procLatch);
    }

    return false;
}

/*
 * Wait until the the apply worker changes the state of our synchronization
 * worker to the expected one.
 *
 * Used when transitioning from SYNCWAIT state to CATCHUP.
 *
 * Returns false if the apply worker has disappeared.
 */
static bool wait_for_worker_state_change(char expected_state)
{
    int rc;

    for (;;) {
        LogicalRepWorker *worker;

        CHECK_FOR_INTERRUPTS();

        /*
         * Done if already in correct state.  (We assume this fetch is atomic
         * enough to not give a misleading answer if we do it with no lock.)
         */
        if (t_thrd.applyworker_cxt.curWorker->relstate == expected_state)
            return true;

        /*
         * Bail out if the apply worker has died, else signal it we're
         * waiting.
         */
        LWLockAcquire(LogicalRepWorkerLock, LW_SHARED);
        worker = logicalrep_worker_find(t_thrd.applyworker_cxt.curWorker->subid, InvalidOid, false);
        if (worker && worker->proc)
            logicalrep_worker_wakeup_ptr(worker);
        LWLockRelease(LogicalRepWorkerLock);
        if (!worker)
            break;

        /*
         * Wait.  We expect to get a latch signal back from the apply worker,
         * but use a timeout in case it dies without sending one.
         */
        pgstat_report_waitevent(WAIT_EVENT_LOGICAL_SYNC_STATE_CHANGE);
        rc = WaitLatch(&t_thrd.proc->procLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, 1000L);
        pgstat_report_waitevent(WAIT_EVENT_END);

        /* emergency bailout if postmaster has died */
        if (rc & WL_POSTMASTER_DEATH)
            proc_exit(1);

        if (rc & WL_LATCH_SET)
            ResetLatch(&t_thrd.proc->procLatch);
    }

    return false;
}

/*
 * Callback from syscache invalidation.
 */
void invalidate_syncing_table_states(Datum arg, int cacheid, uint32 hashvalue)
{
    t_thrd.applyworker_cxt.tableStatesValid = false;
}

/*
 * Determine the tablesync slot name.
 *
 * The name must not exceed NAMEDATALEN - 1 because of remote node constraints
 * on slot name length. We append system_identifier to avoid slot_name
 * collision with subscriptions in other clusters. With the current scheme
 * pg_%u_sync_%u_UINT64_FORMAT (3 + 10 + 6 + 10 + 20 + '\0'), the maximum
 * length of slot_name will be 50.
 *
 * The returned slot name is stored in the supplied buffer (syncslotname) with
 * the given size.
 *
 * Note: We don't use the subscription slot name as part of tablesync slot name
 * because we are responsible for cleaning up these slots and it could become
 * impossible to recalculate what name to cleanup if the subscription slot name
 * had changed.
 */
void ReplicationSlotNameForTablesync(Oid suboid, Oid relid, char *syncslotname, int szslot)
{
    int rc = snprintf_s(syncslotname, szslot, szslot - 1, "pg_%u_sync_%u_" UINT64_FORMAT, suboid,
                        relid, GetSystemIdentifier());
    securec_check_ss(rc, "\0", "\0");
}

/*
 * Form the origin name for tablesync.
 *
 * Return the name in the supplied buffer.
 */
void ReplicationOriginNameForTablesync(Oid suboid, Oid relid, char *originname, int szorgname)
{
    int rc = snprintf_s(originname, szorgname, szorgname - 1, "pg_%u_%u", suboid, relid);
    securec_check_ss(rc, "\0", "\0");
}

/*
 * Handle table synchronization cooperation from the synchronization
 * worker.
 *
 * If the sync worker is in CATCHUP state and reached (or passed) the
 * predetermined synchronization point in the WAL stream, mark the table as
 * SYNCDONE and finish.
 */
static void process_syncing_tables_for_sync(XLogRecPtr current_lsn)
{
    LogicalRepWorker* myWorker = t_thrd.applyworker_cxt.curWorker;

    SpinLockAcquire(&myWorker->relmutex);

    if (myWorker->relstate == SUBREL_STATE_CATCHUP && current_lsn >= myWorker->relstate_lsn) {
        char syncslotname[NAMEDATALEN] = {0};

        myWorker->relstate = SUBREL_STATE_SYNCDONE;
        myWorker->relstate_lsn = current_lsn;

        SpinLockRelease(&myWorker->relmutex);

        /*
         * UpdateSubscriptionRelState must be called within a transaction.
         * That transaction will be ended within the finish_sync_worker().
         */
        if (!IsTransactionState())
            StartTransactionCommand();

        UpdateSubscriptionRelState(myWorker->subid, myWorker->relid, myWorker->relstate, myWorker->relstate_lsn);

        ReplicationSlotNameForTablesync(myWorker->subid, myWorker->relid, syncslotname, sizeof(syncslotname));
        finish_sync_worker(syncslotname);
    } else
        SpinLockRelease(&myWorker->relmutex);
}

/*
 * Handle table synchronization cooperation from the apply worker.
 *
 * Walk over all subscription tables that are individually tracked by the
 * apply process (currently, all that have state other than
 * SUBREL_STATE_READY) and manage synchronization for them.
 *
 * If there are tables that need synchronizing and are not being synchronized
 * yet, start sync workers for them (if there are free slots for sync
 * workers).
 *
 * For tables that are being synchronized already, check if sync workers
 * either need action from the apply worker or have finished.  This is the
 * SYNCWAIT to CATCHUP transition.
 *
 * If the synchronization position is reached (SYNCDONE), then the table can
 * be marked as READY and is no longer tracked.
 */
static void process_syncing_tables_for_apply(XLogRecPtr current_lsn)
{
    ListCell *lc;
    int rc;
    bool started_tx = false;

    Assert(!IsTransactionState());

    /* We need up to date sync state info for subscription tables here. */
    if (!t_thrd.applyworker_cxt.tableStatesValid) {
        MemoryContext oldctx;
        List *rstates;
        ListCell *lc;
        SubscriptionRelState *rstate;

        /* Clean the old list. */
        list_free_deep(t_thrd.applyworker_cxt.tableStates);
        t_thrd.applyworker_cxt.tableStates = NIL;

        StartTransactionCommand();
        started_tx = true;

        /* Fetch all non-ready tables. */
        rstates = GetSubscriptionRelations(t_thrd.applyworker_cxt.mySubscription->oid, true);

        /* Allocate the tracking info in a permanent memory context. */
        oldctx = MemoryContextSwitchTo(t_thrd.applyworker_cxt.applyContext);
        foreach (lc, rstates) {
            rstate = (SubscriptionRelState *)palloc(sizeof(SubscriptionRelState));
            rc = memcpy_s(rstate, sizeof(SubscriptionRelState), lfirst(lc), sizeof(SubscriptionRelState));
            securec_check(rc, "\0", "\0");
            t_thrd.applyworker_cxt.tableStates = lappend(t_thrd.applyworker_cxt.tableStates, rstate);
        }
        MemoryContextSwitchTo(oldctx);

        t_thrd.applyworker_cxt.tableStatesValid = true;
    }

    /* Process all tables that are being synchronized. */
    foreach (lc, t_thrd.applyworker_cxt.tableStates) {
        SubscriptionRelState *rstate = (SubscriptionRelState *)lfirst(lc);

        if (rstate->state == SUBREL_STATE_SYNCDONE) {
            /*
             * Apply has caught up to the position where the table sync
             * has finished.  Time to mark the table as ready so that
             * apply will just continue to replicate it normally.
             */
            if (current_lsn >= rstate->lsn) {
                char originname[NAMEDATALEN];

                rstate->state = SUBREL_STATE_READY;
                rstate->lsn = current_lsn;
                if (!started_tx) {
                    StartTransactionCommand();
                    started_tx = true;
                }

                /*
                 * Remove the tablesync origin tracking if exists.
                 *
                 * The normal case origin drop is done here instead of in the
                 * process_syncing_tables_for_sync function because we don't
                 * allow to drop the origin till the process owning the origin
                 * is alive.
                 *
                 * There is a chance that the user is concurrently performing
                 * refresh for the subscription where we remove the table
                 * state and its origin and by this time the origin might be
                 * already removed. So passing missing_ok = true.
                 */
                ReplicationOriginNameForTablesync(t_thrd.applyworker_cxt.curWorker->subid, rstate->relid, originname,
                                                  sizeof(originname));
                replorigin_drop_by_name(originname, true, false);

                /*
                 * Update the state to READY only after the origin cleanup.
                 */
                UpdateSubscriptionRelState(t_thrd.applyworker_cxt.curWorker->subid, rstate->relid,
                                           rstate->state, rstate->lsn);
            }
        } else {
            LogicalRepWorker *syncworker;

            /*
             * Look for a sync worker for this relation.
             */
            LWLockAcquire(LogicalRepWorkerLock, LW_SHARED);
            syncworker = logicalrep_worker_find(t_thrd.applyworker_cxt.curWorker->subid, rstate->relid, false);
            if (syncworker) {
                /* Found one, update our copy of its state */
                SpinLockAcquire(&syncworker->relmutex);
                rstate->state = syncworker->relstate;
                rstate->lsn = syncworker->relstate_lsn;
                if (rstate->state == SUBREL_STATE_SYNCWAIT) {
                    /*
                     * Sync worker is waiting for apply.  Tell sync worker it
                     * can catchup now.
                     */
                    syncworker->relstate = SUBREL_STATE_CATCHUP;
                    syncworker->relstate_lsn = Max(syncworker->relstate_lsn, current_lsn);
                }
                SpinLockRelease(&syncworker->relmutex);
                /* If we told worker to catch up, wait for it. */
                if (rstate->state == SUBREL_STATE_SYNCWAIT) {
                    /* Signal the sync worker, as it may be waiting for us. */
                    if (syncworker->proc)
                        logicalrep_worker_wakeup_ptr(syncworker);

                    /* Now safe to release the LWLock */
                    LWLockRelease(LogicalRepWorkerLock);

                    /*
                     * Enter busy loop and wait for synchronization worker to
                     * reach expected state (or die trying).
                     */
                    if (!started_tx) {
                        StartTransactionCommand();
                        started_tx = true;
                    }

                    wait_for_relation_state_change(rstate->relid, SUBREL_STATE_SYNCDONE);
                }
                else
                    LWLockRelease(LogicalRepWorkerLock);
            } else {
                /*
                 * If no sync worker for this table yet, could running sync
                 * workers for this subscription, while we have the lock, for
                 * later.
                 */
                int nsyncworkers = logicalrep_sync_worker_count(t_thrd.applyworker_cxt.curWorker->subid);

                /* Now safe to release the LWLock */
                LWLockRelease(LogicalRepWorkerLock);

                /*
                 * If there are free sync worker slot(s), start a new sync
                 * worker for the table.
                 */
                if (nsyncworkers < u_sess->attr.attr_storage.max_sync_workers_per_subscription) {
                    logicalrep_worker_launch(t_thrd.applyworker_cxt.curWorker->dbid,
                                             t_thrd.applyworker_cxt.mySubscription->oid,
                                             t_thrd.applyworker_cxt.mySubscription->name,
                                             t_thrd.applyworker_cxt.curWorker->userid,
                                             rstate->relid);
                }
            }
        }
    }

    if (started_tx) {
        CommitTransactionCommand();
        pgstat_report_stat(false);
    }
}

/*
 * Process possible change(s) of tables that are being synchronized.
 */
void process_syncing_tables(XLogRecPtr current_lsn)
{
    if (AM_TABLESYNC_WORKER)
        process_syncing_tables_for_sync(current_lsn);
    else
        process_syncing_tables_for_apply(current_lsn);
}

/*
 * Create list of columns for COPY based on logical relation mapping.
 */
static List *make_copy_attnamelist(LogicalRepRelMapEntry *rel)
{
    List *attnamelist = NIL;
    int i;

    for (i = 0; i < rel->remoterel.natts; i++) {
        attnamelist = lappend(attnamelist, makeString(rel->remoterel.attnames[i]));
    }

    return attnamelist;
}

/*
 * Data source callback for the COPY FROM, which reads from the remote
 * connection and passes the data back to our local COPY.
 */
static int copy_read_data(CopyState cstate, void *outbuf, int minread, int maxread)
{
    int bytesread = 0;
    int avail;
    int rc;
    StringInfo copybuf = t_thrd.applyworker_cxt.copybuf;

    /* If there are some leftover data from previous read, use them. */
    avail = copybuf->len - copybuf->cursor;
    if (avail) {
        if (avail > maxread)
            avail = maxread;
        rc = memcpy_s(outbuf, maxread, &copybuf->data[copybuf->cursor], avail);
        securec_check(rc, "\0", "\0");
        copybuf->cursor += avail;
        maxread -= avail;
        bytesread += avail;
    }

    while (maxread > 0 && bytesread < minread) {
        int rc;
        int len;
        char *buf = NULL;


        for (;;) {
            /* Try read the data. */
            if ((WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_receive(0, NULL, &buf, &len)) {
                CHECK_FOR_INTERRUPTS();

                if (len == 0)
                    break;
                else if (len < 0)
                    return bytesread;
                else {
                    /* Process the data */
                    copybuf->data = buf;
                    copybuf->len = len;
                    copybuf->cursor = 0;

                    avail = copybuf->len - copybuf->cursor;
                    if (avail > maxread)
                        avail = maxread;
                    rc = memcpy_s(outbuf, maxread, &copybuf->data[copybuf->cursor], avail);
                    securec_check(rc, "\0", "\0");
                    outbuf = (void *)((char *)outbuf + avail);
                    copybuf->cursor += avail;
                    maxread -= avail;
                    bytesread += avail;
                }

                if (maxread <= 0 || bytesread >= minread)
                    return bytesread;
            } else {
                if (len == 0)
                    break;
                else if (len < 0)
                    return bytesread;
            }
        }

        /*
         * Wait for more data or latch.
         */
        pgstat_report_waitevent(WAIT_EVENT_LOGICAL_SYNC_DATA);
        rc = WaitLatchOrSocket(&t_thrd.proc->procLatch, WL_SOCKET_READABLE | WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
            u_sess->proc_cxt.MyProcPort->sock, 1000L);
        pgstat_report_waitevent(WAIT_EVENT_END);

        /* Emergency bailout if postmaster has died */
        if (rc & WL_POSTMASTER_DEATH)
            proc_exit(1);

        ResetLatch(&t_thrd.proc->procLatch);
    }

    return bytesread;
}

/*
 * Get information about remote relation in similar fashion the RELATION
 * message provides during replication.
 */
static void fetch_remote_table_info(char *nspname, char *relname, LogicalRepRelation *lrel)
{
    WalRcvExecResult *res;
    StringInfoData cmd;
    TupleTableSlot *slot;
    Oid tableRow[2] = {OIDOID, CHAROID};
    Oid attrRow[4] = {TEXTOID, OIDOID, INT4OID, BOOLOID};
    bool isnull;
    int natt;

    lrel->nspname = nspname;
    lrel->relname = relname;

    /* First fetch Oid and replica identity. */
    initStringInfo(&cmd);
    appendStringInfo(&cmd,
        "SELECT c.oid, c.relreplident"
        "  FROM pg_catalog.pg_class c,"
        "       pg_catalog.pg_namespace n"
        " WHERE n.nspname = %s"
        "   AND c.relname = %s"
        "   AND c.relkind = 'r'",
        quote_literal_cstr(nspname), quote_literal_cstr(relname));
    res = (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_exec(cmd.data, 2, tableRow);

    if (res->status != WALRCV_OK_TUPLES)
        ereport(ERROR,
            (errmsg("could not fetch table info for table \"%s.%s\" from publisher: %s", nspname, relname, res->err)));

    slot = MakeSingleTupleTableSlot(res->tupledesc);
    if (!tuplestore_gettupleslot(res->tuplestore, true, false, slot))
        ereport(ERROR, (errmsg("table \"%s.%s\" not found on publisher", nspname, relname)));

    lrel->remoteid = DatumGetObjectId(tableam_tslot_getattr(slot, 1, &isnull));
    Assert(!isnull);
    lrel->replident = DatumGetChar(tableam_tslot_getattr(slot, 2, &isnull));
    Assert(!isnull);

    ExecDropSingleTupleTableSlot(slot);
    walrcv_clear_result(res);

    /* Now fetch columns. */
    resetStringInfo(&cmd);
    appendStringInfo(&cmd,
        "SELECT a.attname,"
        "       a.atttypid,"
        "       a.atttypmod,"
        "       a.attnum = ANY(i.indkey)"
        "  FROM pg_catalog.pg_attribute a"
        "  LEFT JOIN pg_catalog.pg_index i"
        "       ON (i.indexrelid = pg_get_replica_identity_index(%u))"
        " WHERE a.attnum > 0::pg_catalog.int2"
        "   AND NOT a.attisdropped"
        "   AND NOT EXISTS (SELECT * FROM pg_attrdef b WHERE b.adrelid = a.attrelid AND b.adnum = a.attnum"
        " AND b.adgencol = 's')"
        "   AND a.attrelid = %u"
        " ORDER BY a.attnum",
        lrel->remoteid, lrel->remoteid);
    res = (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_exec(cmd.data, 4, attrRow);

    if (res->status != WALRCV_OK_TUPLES)
        ereport(ERROR, (errmsg("could not fetch table info for table \"%s.%s\": %s", nspname, relname, res->err)));

    /* We don't know number of rows coming, so allocate enough space. */
    lrel->attnames = (char**)palloc0(MaxTupleAttributeNumber * sizeof(char *));
    lrel->atttyps = (Oid*)palloc0(MaxTupleAttributeNumber * sizeof(Oid));
    lrel->attkeys = NULL;

    natt = 0;
    slot = MakeSingleTupleTableSlot(res->tupledesc);
    while (tuplestore_gettupleslot(res->tuplestore, true, false, slot)) {
        lrel->attnames[natt] = pstrdup(TextDatumGetCString(tableam_tslot_getattr(slot, 1, &isnull)));
        Assert(!isnull);
        lrel->atttyps[natt] = DatumGetObjectId(tableam_tslot_getattr(slot, 2, &isnull));
        Assert(!isnull);
        if (DatumGetBool(tableam_tslot_getattr(slot, 4, &isnull)))
            lrel->attkeys = bms_add_member(lrel->attkeys, natt);

        /* Should never happen. */
        if (++natt >= MaxTupleAttributeNumber)
            elog(ERROR, "too many columns in remote table \"%s.%s\"", nspname, relname);

        ExecClearTuple(slot);
    }
    ExecDropSingleTupleTableSlot(slot);

    lrel->natts = natt;

    walrcv_clear_result(res);
    pfree(cmd.data);
}

/*
 * Copy existing data of a table from publisher.
 *
 * Caller is responsible for locking the local relation.
 */
static void copy_table(Relation rel)
{
    LogicalRepRelMapEntry *relmapentry;
    LogicalRepRelation lrel;
    WalRcvExecResult *res;
    StringInfoData cmd;
    CopyState cstate;
    List *attnamelist;
    AdaptMem mem_info;
    mem_info.max_mem = 0;
    mem_info.work_mem = 0;

    /* Get the publisher relation info. */
    fetch_remote_table_info(get_namespace_name(RelationGetNamespace(rel)), RelationGetRelationName(rel), &lrel);

    /* Put the relation into relmap. */
    logicalrep_relmap_update(&lrel);

    /* Map the publisher relation to local one. */
    relmapentry = logicalrep_rel_open(lrel.remoteid, NoLock);
    Assert(rel == relmapentry->localrel);

    /* Start copy on the publisher. */
    initStringInfo(&cmd);
    appendStringInfo(&cmd, "COPY %s TO STDOUT", quote_qualified_identifier(lrel.nspname, lrel.relname));
    res = (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_exec(cmd.data, 0, NULL);
    pfree(cmd.data);
    if (res->status != WALRCV_OK_COPY_OUT)
        ereport(ERROR, (errmsg("could not start initial contents copy for table \"%s.%s\": %s", lrel.nspname,
            lrel.relname, res->err)));
    walrcv_clear_result(res);

    t_thrd.applyworker_cxt.copybuf = makeStringInfo();

    /* Create CopyState for ingestion of the data from publisher. */
    attnamelist = make_copy_attnamelist(relmapentry);
    cstate = BeginCopyFrom(rel, NULL, attnamelist, NIL, &mem_info, NULL, copy_read_data);

    RangeTblEntry *rte = makeNode(RangeTblEntry);
    rte->rtekind = RTE_RELATION;
    rte->relid = RelationGetRelid(rel);
    rte->relkind = rel->rd_rel->relkind;
    rte->requiredPerms = ACL_SELECT;

    cstate->range_table = list_make1(rte);

    /* Do the copy */
    (void)CopyFrom(cstate);

    logicalrep_rel_close(relmapentry, NoLock);
}

/*
 * Start syncing the table in the sync worker.
 *
 * If nothing needs to be done to sync the table, we exit the worker without
 * any further action.
 */
char *LogicalRepSyncTableStart(XLogRecPtr *origin_startpos)
{
    char *slotname;
    char relstate;
    XLogRecPtr relstate_lsn;
    LibpqrcvConnectParam options;
    CommitSeqNo csn = InvalidCommitSeqNo;
    int rc;
    Relation rel;
    WalRcvExecResult *res;
    char originname[NAMEDATALEN];
    RepOriginId originid;

    /* Check the state of the table synchronization. */
    StartTransactionCommand();
    relstate = GetSubscriptionRelState(t_thrd.applyworker_cxt.curWorker->subid,
                                       t_thrd.applyworker_cxt.curWorker->relid,
                                       &relstate_lsn, &csn);
    CommitTransactionCommand();

    SpinLockAcquire(&t_thrd.applyworker_cxt.curWorker->relmutex);
    t_thrd.applyworker_cxt.curWorker->relstate = relstate;
    t_thrd.applyworker_cxt.curWorker->relstate_lsn = relstate_lsn;
    t_thrd.applyworker_cxt.curWorker->relcsn = csn;
    SpinLockRelease(&t_thrd.applyworker_cxt.curWorker->relmutex);

    /*
     * If synchronization is already done or no longer necessary, exit now
     * that we've updated shared memory state.
     */
    switch (relstate) {
        case SUBREL_STATE_SYNCDONE:
        case SUBREL_STATE_READY:
        case SUBREL_STATE_UNKNOWN:
            finish_sync_worker(); /* doesn't return */
    }

    /* Calculate the name of the tablesync slot. */
    slotname = (char *)palloc(NAMEDATALEN);
    ReplicationSlotNameForTablesync(t_thrd.applyworker_cxt.mySubscription->oid,
                                    t_thrd.applyworker_cxt.curWorker->relid, slotname, NAMEDATALEN);

    if (!AttemptConnectPublisher(t_thrd.applyworker_cxt.mySubscription->conninfo, slotname, true)) {
        ereport(ERROR, (errmsg("could not connect to the publisher: %s",
                               PQerrorMessage(t_thrd.libwalreceiver_cxt.streamConn))));
    }

    Assert(t_thrd.applyworker_cxt.curWorker->relstate == SUBREL_STATE_INIT ||
           t_thrd.applyworker_cxt.curWorker->relstate == SUBREL_STATE_DATASYNC ||
           t_thrd.applyworker_cxt.curWorker->relstate == SUBREL_STATE_FINISHEDCOPY);

    /* Assign the origin tracking record name. */
    ReplicationOriginNameForTablesync(t_thrd.applyworker_cxt.mySubscription->oid,
                                      t_thrd.applyworker_cxt.curWorker->relid, originname, sizeof(originname));

    if (t_thrd.applyworker_cxt.curWorker->relstate == SUBREL_STATE_DATASYNC) {
        /*
         * We have previously errored out before finishing the copy so the
         * replication slot might exist. We want to remove the slot if it
         * already exists and proceed.
         *
         * XXX We could also instead try to drop the slot, last time we failed
         * but for that, we might need to clean up the copy state as it might
         * be in the middle of fetching the rows. Also, if there is a network
         * breakdown then it wouldn't have succeeded so trying it next time
         * seems like a better bet.
         */
        StartTransactionCommand();
        ReplicationSlotDropAtPubNode(slotname, true);
        CommitTransactionCommand();
    } else if (t_thrd.applyworker_cxt.curWorker->relstate == SUBREL_STATE_FINISHEDCOPY) {
        /*
         * The COPY phase was previously done, but tablesync then crashed
         * before it was able to finish normally.
         */
        StartTransactionCommand();

        /*
         * The origin tracking name must already exist. It was created first
         * time this tablesync was launched.
         */
        originid = replorigin_by_name(originname, false);
        replorigin_session_setup(originid);
        u_sess->reporigin_cxt.originId = originid;
        *origin_startpos = replorigin_session_get_progress(false);

        CommitTransactionCommand();

        goto copy_table_done;
    }

    SpinLockAcquire(&t_thrd.applyworker_cxt.curWorker->relmutex);
    t_thrd.applyworker_cxt.curWorker->relstate = SUBREL_STATE_DATASYNC;
    t_thrd.applyworker_cxt.curWorker->relstate_lsn = InvalidXLogRecPtr;
    SpinLockRelease(&t_thrd.applyworker_cxt.curWorker->relmutex);

    /* Update the state and make it visible to others. */
    StartTransactionCommand();
    UpdateSubscriptionRelState(t_thrd.applyworker_cxt.curWorker->subid,
                               t_thrd.applyworker_cxt.curWorker->relid,
                               t_thrd.applyworker_cxt.curWorker->relstate,
                               t_thrd.applyworker_cxt.curWorker->relstate_lsn);
    CommitTransactionCommand();
    pgstat_report_stat(false);

    StartTransactionCommand();

    /*
     * Use standard write lock here. It might be better to
     * disallow access to table while it's being synchronized.
     * But we don't want to block the main apply process from
     * working and it has to open relation in RowExclusiveLock
     * when remapping remote relation id to local one.
     */
    rel = heap_open(t_thrd.applyworker_cxt.curWorker->relid, RowExclusiveLock);

    /*
     * Start a transaction in the remote node in REPEATABLE READ mode.  This
     * ensures that both the replication slot we create (see below) and the
     * COPY are consistent with each other.
     */
    res = (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_exec("BEGIN READ ONLY ISOLATION LEVEL "
                                                           "REPEATABLE READ",
                                                           0, NULL);
    if (res->status != WALRCV_OK_COMMAND)
        ereport(ERROR, (errmsg("table copy could not start transaction on publisher"),
            errdetail("The error was: %s", res->err)));
    walrcv_clear_result(res);

    /*
     * Create a new permanent logical decoding slot. This slot will be used
     * for the catchup phase after COPY is done, so tell it to use the
     * snapshot to make the final data consistent.
     */
    rc = memset_s(&options, sizeof(LibpqrcvConnectParam), 0, sizeof(LibpqrcvConnectParam));
    securec_check(rc, "", "");
    options.logical = true;
    options.slotname = slotname;
    options.useSnapshot = true;
    (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_create_slot(&options, origin_startpos, &csn);

    /*
     * Setup replication origin tracking. The purpose of doing this before the
     * copy is to avoid doing the copy again due to any error in setting up
     * origin tracking.
     */
    originid = replorigin_by_name(originname, true);
    if (!OidIsValid(originid)) {
        /*
         * Origin tracking does not exist, so create it now.
         *
         * Then advance to the LSN got from walrcv_create_slot. This is WAL
         * logged for the purpose of recovery. Locks are to prevent the
         * replication origin from vanishing while advancing.
         */
        originid = replorigin_create(originname);

        LockRelationOid(ReplicationOriginRelationId, RowExclusiveLock);
        replorigin_advance(originid, *origin_startpos, InvalidXLogRecPtr, true /* go backward */, true /* WAL log */);
        UnlockRelationOid(ReplicationOriginRelationId, RowExclusiveLock);

        replorigin_session_setup(originid);
        u_sess->reporigin_cxt.originId = originid;
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_DUPLICATE_OBJECT), errmsg("replication origin \"%s\" already exists", originname)));
    }

    /* Now do the initial data copy */
    PushActiveSnapshot(GetTransactionSnapshot());
    copy_table(rel);
    PopActiveSnapshot();

    res = (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_exec("COMMIT", 0, NULL);
    if (res->status != WALRCV_OK_COMMAND)
        ereport(ERROR, (errmsg("table copy could not finish transaction on publisher"),
            errdetail("The error was: %s", res->err)));
    walrcv_clear_result(res);

    heap_close(rel, NoLock);

    /* Make the copy visible. */
    CommandCounterIncrement();

    /*
     * Update the persisted state to indicate the COPY phase is done; make it
     * visible to others.
     */
    UpdateSubscriptionRelState(t_thrd.applyworker_cxt.curWorker->subid,
                               t_thrd.applyworker_cxt.curWorker->relid,
                               SUBREL_STATE_FINISHEDCOPY,
                               t_thrd.applyworker_cxt.curWorker->relstate_lsn,
                               csn);

    CommitTransactionCommand();

copy_table_done:

    ereport(DEBUG1, (errmsg("LogicalRepSyncTableStart: '%s' origin_startpos lsn %X/%X", originname,
        (uint32)(*origin_startpos >> 32), (uint32)*origin_startpos)));

    /*
     * We are done with the initial data synchronization,
     * update the state.
     */
    SpinLockAcquire(&t_thrd.applyworker_cxt.curWorker->relmutex);
    t_thrd.applyworker_cxt.curWorker->relstate = SUBREL_STATE_SYNCWAIT;
    t_thrd.applyworker_cxt.curWorker->relstate_lsn = *origin_startpos;
    t_thrd.applyworker_cxt.curWorker->relcsn = csn;
    SpinLockRelease(&t_thrd.applyworker_cxt.curWorker->relmutex);

    /*
     * Finally, wait until the main apply worker tells us to catch up and then
     * return to let LogicalRepApplyLoop do it.
     */
    wait_for_worker_state_change(SUBREL_STATE_CATCHUP);

    return slotname;
}
