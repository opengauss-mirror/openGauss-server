/* -------------------------------------------------------------------------
 *
 * sequence.h
 *	  prototypes for sequence.c.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/sequence.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef SEQUENCE_H
#define SEQUENCE_H

#include "access/xlogreader.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "storage/relfilenode.h"

#ifdef PGXC
#include "utils/relcache.h"
#include "gtm/gtm_c.h"
#include "access/xact.h"
#include "optimizer/streamplan.h"
#include "pgxc/pgxcnode.h"
#include "tcop/utility.h"
#endif

#define INVALIDSEQUUID 0

// extern THR_LOCAL char *PGXCNodeName;

typedef int64 GTM_UUID;

typedef struct FormData_pg_sequence {
    NameData sequence_name;
    int64 last_value;
    int64 start_value;
    int64 increment_by;
    int64 max_value;
    int64 min_value;
    int64 cache_value;
    int64 log_cnt;
    bool is_cycled;
    bool is_called;
    GTM_UUID uuid;
} FormData_pg_sequence;

typedef FormData_pg_sequence* Form_pg_sequence;

/*
 * Columns of a sequence relation
 */

#define SEQ_COL_NAME 1
#define SEQ_COL_LASTVAL 2
#define SEQ_COL_STARTVAL 3
#define SEQ_COL_INCBY 4
#define SEQ_COL_MAXVALUE 5
#define SEQ_COL_MINVALUE 6
#define SEQ_COL_CACHE 7
#define SEQ_COL_LOG 8
#define SEQ_COL_CYCLE 9
#define SEQ_COL_CALLED 10
#define SEQ_COL_UUID 11

#define SEQ_COL_FIRSTCOL SEQ_COL_NAME
#define SEQ_COL_LASTCOL SEQ_COL_UUID

/* XLOG stuff */
#define XLOG_SEQ_LOG 0x00

/*
 * The "special area" of a sequence's buffer page looks like this.
 */
#define SEQ_MAGIC 0x1717

typedef struct xl_seq_rec {
    RelFileNodeOld node;
    /* SEQUENCE TUPLE DATA FOLLOWS AT THE END */
} xl_seq_rec;

/*
 * We don't want to log each fetching of a value from a sequence,
 * so we pre-log a few fetches in advance. In the event of
 * crash we can lose (skip over) as many values as we pre-logged.
 */
#define SEQ_LOG_VALS 32


/*
 * The "special area" of a old version sequence's buffer page looks like this.
 */

typedef struct sequence_magic {
    uint32 magic;
} sequence_magic;


extern Datum nextval(PG_FUNCTION_ARGS);
extern Datum nextval_oid(PG_FUNCTION_ARGS);
extern Datum currval_oid(PG_FUNCTION_ARGS);
extern Datum setval_oid(PG_FUNCTION_ARGS);
extern Datum setval3_oid(PG_FUNCTION_ARGS);
extern Datum lastval(PG_FUNCTION_ARGS);

extern Datum pg_sequence_parameters(PG_FUNCTION_ARGS);

extern void DefineSequence(CreateSeqStmt* stmt);
extern void AlterSequence(AlterSeqStmt* stmt);
extern void PreventAlterSeqInTransaction(bool isTopLevel, AlterSeqStmt* stmt);
extern void ResetSequence(Oid seq_relid);

extern void seq_redo(XLogReaderState* rptr);
extern void seq_desc(StringInfo buf, XLogReaderState* record);
extern GTM_UUID get_uuid_from_rel(Relation rel);
extern void lockNextvalOnCn(Oid relid);

extern void get_sequence_params(Relation rel, int64* uuid, int64* start, int64* increment, int64* maxvalue,
    int64* minvalue, int64* cache, bool* cycle);

#ifdef PGXC
/*
 * List of actions that registered the callback.
 * This is listed here and not in sequence.c because callback can also
 * be registered in dependency.c and tablecmds.c as sequences can be dropped
 * or renamed in cascade.
 */
typedef enum { GTM_CREATE_SEQ, GTM_DROP_SEQ } GTM_SequenceDropType;
/*
 * Arguments for callback of sequence drop on GTM
 */
typedef struct drop_sequence_callback_arg {
    GTM_UUID seq_uuid;
    GTM_SequenceDropType type;
    GTM_SequenceKeyType key;
} drop_sequence_callback_arg;

/*
 * Arguments for callback of sequence rename on GTM
 */
typedef struct rename_sequence_callback_arg {
    char* newseqname;
    char* oldseqname;
} rename_sequence_callback_arg;

extern void delete_global_seq(Oid relid, Relation seqrel);
/* Sequence callbacks on GTM */
extern void register_sequence_rename_cb(const char* oldseqname, const char* newseqname);
extern void rename_sequence_cb(GTMEvent event, void* args);
extern void register_sequence_cb(GTM_UUID seq_uuid, GTM_SequenceDropType type);
extern void drop_sequence_cb(GTMEvent event, void* args);

extern bool IsTempSequence(Oid relid);
extern char* GetGlobalSeqName(Relation rel, const char* new_seqname, const char* new_schemaname);
extern char* gen_hybirdmsg_for_CreateSeqStmt(CreateSeqStmt* stmt, const char* queryString);
extern int64 gen_uuid(List* uuids);
extern char* gen_hybirdmsg_for_CreateSchemaStmt(CreateSchemaStmt* stmt, const char* queryString);
extern void gen_uuid_for_CreateStmt(CreateStmt* stmt, List* uuids);
extern void gen_uuid_for_CreateSchemaStmt(List* stmts, List* uuids);
extern void  processUpdateSequenceMsg(const char* seqname, int64 lastvalue);
extern void checkAndDoUpdateSequence();

#endif

#endif /* SEQUENCE_H */
