/* -------------------------------------------------------------------------
 *
 * logicalproto.h
 * 		logical replication protocol
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 * 		src/include/replication/logicalproto.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef LOGICAL_PROTO_H
#define LOGICAL_PROTO_H

#include "access/ustore/knl_utuple.h"
#include "replication/reorderbuffer.h"
#include "utils/rel.h"

/*
 * Protocol capabilities
 *
 * LOGICALREP_PROTO_VERSION_NUM is our native protocol.
 * LOGICALREP_CONNINFO_PROTO_VERSION_NUM is the version that need to handle changed conninfo.
 * LOGICALREP_PROTO_MAX_VERSION_NUM is the greatest version we can support + 1.
 * LOGICALREP_PROTO_MIN_VERSION_NUM is the oldest version we
 * have backwards compatibility for - 1. The client requests protocol version at
 * connect time.
 */
typedef enum {
    LOGICALREP_PROTO_MIN_VERSION_NUM = 0,
    LOGICALREP_PROTO_VERSION_NUM,
    LOGICALREP_CONNINFO_PROTO_VERSION_NUM,
    LOGICALREP_PROTO_MAX_VERSION_NUM
} LOGICALREP_VERSION_NUM;

/*
 * This struct stores a tuple received via logical replication.
 * Keep in mind that the columns correspond to the *remote* table.
 */
typedef struct LogicalRepTupleData {
    /* Array of StringInfos, one per column; some may be unused */
    StringInfoData *colvalues;
    /* Array of markers for null/unchanged/text/binary, one per column */
    char       *colstatus;
    /* Length of above arrays */
    int ncols;
} LogicalRepTupleData;

/* Possible values for LogicalRepTupleData.colstatus[colnum] */
/* These values are also used in the on-the-wire protocol */
#define LOGICALREP_COLUMN_NULL        'n'
#define LOGICALREP_COLUMN_UNCHANGED    'u'
#define LOGICALREP_COLUMN_TEXT        't'
#define LOGICALREP_COLUMN_BINARY    'b' /* added in PG14 */
#define LOGICAL_REP_MSG_DDL   'L'

typedef uint32 LogicalRepRelId;

/* Relation information */
typedef struct LogicalRepRelation {
    /* Info coming from the remote side. */
    LogicalRepRelId remoteid; /* unique id of the relation */
    char *nspname;            /* schema name */
    char *relname;            /* relation name */
    int natts;                /* number of columns */
    char **attnames;          /* column names */
    Oid *atttyps;             /* column types */
    char replident;           /* replica identity */
    Bitmapset *attkeys;       /* Bitmap of key columns */
} LogicalRepRelation;

/* Type mapping info */
typedef struct LogicalRepTyp {
    Oid remoteid;  /* unique id of the remote type */
    char *nspname; /* schema name of remote type */
    char *typname; /* name of the remote type */
} LogicalRepTyp;

/* Transaction info */
typedef struct LogicalRepBeginData {
    XLogRecPtr final_lsn;
    TimestampTz committime;
    TransactionId xid;
    CommitSeqNo csn;
} LogicalRepBeginData;

typedef struct LogicalRepCommitData {
    XLogRecPtr commit_lsn;
    XLogRecPtr end_lsn;
    TimestampTz committime;
} LogicalRepCommitData;

extern void logicalrep_write_begin(StringInfo out, ReorderBufferTXN *txn);
extern void logicalrep_read_begin(StringInfo in, LogicalRepBeginData *begin_data);
extern void logicalrep_write_commit(StringInfo out, ReorderBufferTXN *txn, XLogRecPtr commit_lsn);
extern void logicalrep_read_commit(StringInfo in, LogicalRepCommitData *commit_data);
extern void logicalrep_write_origin(StringInfo out, const char *origin, XLogRecPtr origin_lsn);
extern void logicalrep_write_insert(StringInfo out, Relation rel, HeapTuple newtuple, bool binary);
extern LogicalRepRelId logicalrep_read_insert(StringInfo in, LogicalRepTupleData *newtup);
extern void logicalrep_write_update(StringInfo out, Relation rel, HeapTuple oldtuple, HeapTuple newtuple, bool binary);
extern LogicalRepRelId logicalrep_read_update(StringInfo in, bool *has_oldtuple, LogicalRepTupleData *oldtup,
    LogicalRepTupleData *newtup);
extern void logicalrep_write_delete(StringInfo out, Relation rel, HeapTuple oldtuple, bool binary);
extern LogicalRepRelId logicalrep_read_delete(StringInfo in, LogicalRepTupleData *oldtup);
extern void logicalrep_write_rel(StringInfo out, Relation rel);
extern LogicalRepRelation *logicalrep_read_rel(StringInfo in);
extern void logicalrep_write_typ(StringInfo out, Oid typoid);
extern void logicalrep_read_typ(StringInfo out, LogicalRepTyp *ltyp);
extern void logicalrep_write_conninfo(StringInfo out, char* conninfo);
extern void logicalrep_read_conninfo(StringInfo in, char** conninfo);
extern void logicalrep_write_ddl(StringInfo out, XLogRecPtr lsn,
                                const char *prefix, Size sz, const char *message);
extern char *logicalrep_read_ddl(StringInfo in, XLogRecPtr *lsn, const char **prefix, Size *sz);

#endif /* LOGICALREP_PROTO_H */
