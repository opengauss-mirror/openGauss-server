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
    /* Pointer to proc array. NULL if not running. */
    PGPROC *proc;

    /* Database id to connect to. */
    Oid dbid;

    /* User to use for connection (will be same as owner of subscription). */
    NameData username;

    Oid userid;

    /* Subscription id for the worker. */
    Oid subid;

    /* Used for initial table synchronization. */
    Oid relid;

    TimestampTz workerLaunchTime;

    /* Stats. */
    XLogRecPtr last_lsn;
    TimestampTz last_send_time;
    TimestampTz last_recv_time;
    XLogRecPtr reply_lsn;
    TimestampTz reply_time;
} LogicalRepWorker;

typedef struct ApplyLauncherShmStruct {
    LogicalRepWorker *startingWorker;
    ThreadId applyLauncherPid;

    /* last parameter */
    LogicalRepWorker workers[FLEXIBLE_ARRAY_MEMBER];
} ApplyLauncherShmStruct;

extern void logicalrep_worker_attach();
extern List *logicalrep_workers_find(Oid subid, bool only_running);
extern void logicalrep_worker_stop(Oid subid);

extern char* DefListToString(const List *defList);
extern List* ConninfoToDefList(const char *conn);

#endif /* WORKER_INTERNAL_H */
