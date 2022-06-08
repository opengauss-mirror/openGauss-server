/* -------------------------------------------------------------------------
 * origin.h
 * 	   Exports from replication/logical/origin.c
 *
 * Copyright (c) 2013-2017, PostgreSQL Global Development Group
 *
 * src/include/replication/origin.h
 * -------------------------------------------------------------------------
 */
#ifndef PG_ORIGIN_H
#define PG_ORIGIN_H

#include "fmgr.h"
#include "access/xlog.h"
#include "access/xlogdefs.h"
#include "access/xlogreader.h"
#include "catalog/pg_replication_origin.h"

typedef struct xl_replorigin_set {
    XLogRecPtr remote_lsn;
    RepOriginId node_id;
    bool force;
} xl_replorigin_set;

typedef struct xl_replorigin_drop {
    RepOriginId node_id;
} xl_replorigin_drop;

/*
 * Replay progress of a single remote node.
 */
typedef struct ReplicationState {
    /*
     * Local identifier for the remote node.
     */
    RepOriginId roident;

    /*
     * Location of the latest commit from the remote side.
     */
    XLogRecPtr remote_lsn;

    /*
     * Remember the local lsn of the commit record so we can XLogFlush() to it
     * during a checkpoint so we know the commit record actually is safe on
     * disk.
     */
    XLogRecPtr local_lsn;

    /*
     * PID of backend that's acquired slot, or 0 if none.
     */
    ThreadId acquired_by;

    pthread_mutex_t originMutex;

    pthread_cond_t orginCV;

    pthread_condattr_t originAttr;

    /*
     * Lock protecting remote_lsn and local_lsn.
     */
    LWLock lock;
} ReplicationState;

typedef struct ReplicationStateShmStruct {
    int tranche_id;
    ReplicationState states[FLEXIBLE_ARRAY_MEMBER];
} ReplicationStateShmStruct;

#define XLOG_REPLORIGIN_SET 0x00
#define XLOG_REPLORIGIN_DROP 0x10

#define InvalidRepOriginId 0
#define DoNotReplicateId PG_UINT16_MAX

/* API for querying & manipulating replication origins */
extern RepOriginId replorigin_by_name(const char *name, bool missing_ok);
extern RepOriginId replorigin_create(const char *name);
extern void replorigin_drop_by_name(const char *name, bool missing_ok, bool nowait);
extern bool replorigin_by_oid(RepOriginId roident, bool missing_ok, char **roname);

/* API for querying & manipulating replication progress tracking */
extern void replorigin_advance(RepOriginId node, XLogRecPtr remote_commit, XLogRecPtr local_commit, bool go_backward,
    bool wal_log);

extern void replorigin_session_advance(XLogRecPtr remote_commit, XLogRecPtr local_commit);
extern void replorigin_session_setup(RepOriginId node);
extern XLogRecPtr replorigin_session_get_progress(bool flush);

/* Checkpoint/Startup integration */
extern void CheckPointReplicationOrigin(void);
extern void StartupReplicationOrigin(void);

/* WAL logging */
void replorigin_redo(XLogReaderState *record);
void replorigin_desc(StringInfo buf, XLogReaderState *record);
const char* replorigin_type_name(uint8 subtype);

/* shared memory allocation */
extern Size ReplicationOriginShmemSize(void);
extern void ReplicationOriginShmemInit(void);

#endif /* PG_ORIGIN_H */
