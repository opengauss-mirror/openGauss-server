/* -------------------------------------------------------------------------
 *
 * worker_internal.h
 * 	  Internal headers shared by logical replication workers.
 *
 * Portions Copyright (c) 2010-2016, PostgreSQL Global Development Group
 *
 * src/include/replication/worker_internal.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef WORKER_INTERNAL_H
#define WORKER_INTERNAL_H

#include "catalog/pg_subscription.h"
#include "storage/lock/lock.h"

typedef struct LogicalRepWorker
{
    /* Increased everytime the slot is tabken by new worker */
    uint16 generation;

    /* Pointer to proc array. NULL if not running. */
    PGPROC *proc;

    /* Database id to connect to. */
    Oid dbid;

    Oid userid;

    /* Subscription id for the worker. */
    Oid subid;

    /* Used for initial table synchronization. */
    Oid relid;
    char relstate;
    XLogRecPtr relstate_lsn;
    CommitSeqNo relcsn;
    slock_t relmutex;

    TimestampTz workerLaunchTime;

    /* Stats. */
    XLogRecPtr last_lsn;
    TimestampTz last_send_time;
    TimestampTz last_recv_time;
    XLogRecPtr reply_lsn;
    TimestampTz reply_time;

    bool needCheckConflict;
} LogicalRepWorker;

typedef struct ApplyLauncherShmStruct {
    LogicalRepWorker *startingWorker;
    ThreadId applyLauncherPid;

    /* last parameter */
    LogicalRepWorker workers[FLEXIBLE_ARRAY_MEMBER];
} ApplyLauncherShmStruct;

extern void logicalrep_worker_attach();
extern LogicalRepWorker *logicalrep_worker_find(Oid subid, Oid relid, bool only_running);
extern List *logicalrep_workers_find(Oid subid, bool only_running);
extern void logicalrep_worker_launch(Oid dbid, Oid subid, const char *subname, Oid userid, Oid relid);
extern void logicalrep_worker_stop(Oid subid, Oid relid);
extern void logicalrep_worker_wakeup(Oid subid, Oid relid);
extern void logicalrep_worker_wakeup_ptr(LogicalRepWorker *worker);

extern int logicalrep_sync_worker_count(Oid subid);

extern void ReplicationOriginNameForTablesync(Oid suboid, Oid relid, char *originname, int szorgname);
extern char* DefListToString(const List *defList);
extern List* ConninfoToDefList(const char *conn);
extern char *LogicalRepSyncTableStart(XLogRecPtr *origin_startpos);
void process_syncing_tables(XLogRecPtr current_lsn);
void invalidate_syncing_table_states(Datum arg, int cacheid, uint32 hashvalue);

#define AM_TABLESYNC_WORKER (OidIsValid(t_thrd.applyworker_cxt.curWorker->relid))

#endif /* WORKER_INTERNAL_H */
