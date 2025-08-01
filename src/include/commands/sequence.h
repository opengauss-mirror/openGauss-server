/* -------------------------------------------------------------------------
 *
 * sequence.h
 *	  prototypes for sequence.c.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * src/include/commands/sequence.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef SEQUENCE_H
#define SEQUENCE_H

#include "access/xlogreader.h"
#include "catalog/objectaddress.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "storage/smgr/relfilenode.h"

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

typedef struct FormData_pg_large_sequence {
    NameData sequence_name;
    int128 last_value;
    int128 start_value;
    int128 increment_by;
    int128 max_value;
    int128 min_value;
    int128 cache_value;
    int64 log_cnt;
    bool is_cycled;
    bool is_called;
    GTM_UUID uuid;
} FormData_pg_large_sequence;

typedef FormData_pg_large_sequence* Form_pg_large_sequence;

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

#define GS_NUM_OF_BUCKETS 1024

#define DMODE_MAX_PRECISION 38
#define DMODE_DEFAULT_PRECISION 18

/*
 * The "special area" of a old version sequence's buffer page looks like this.
 */

/*
 * We store a SeqTable item for every sequence we have touched in the current
 * session.  This is needed to hold onto nextval/currval state.  (We can't
 * rely on the relcache, since it's only, well, a cache, and may decide to
 * discard entries.)
 *
 * XXX We use linear search to find pre-existing SeqTable entries.	This is
 * good when only a small number of sequences are touched in a session, but
 * would suck with many different sequences.  Perhaps use a hashtable someday.
 */
typedef struct SeqTableData {
    struct SeqTableData* next; /* link to next SeqTable object */
    Oid relid;                 /* pg_class OID of this sequence */
    Oid filenode;              /* last seen relfilenode of this sequence */
    LocalTransactionId lxid;   /* xact in which we last did a seq op */
    bool last_valid;           /* do we have a valid "last" value? */
    bool is_cycled;
    int128 last;   /* value last returned by nextval */
    int128 cached; /* last value already cached for nextval */
    /* if last != cached, we have not used up all the cached values */
    int128 increment; /* copy of sequence's increment field */
    /* note that increment is zero until we first do read_seq_tuple() */
    int128 minval;
    int128 maxval;
    int128 startval;
    int64 uuid;
} SeqTableData;

typedef SeqTableData* SeqTable;

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

/* Information needed to define a sequence. */
typedef struct sequence_values
{
    bool large;
    char *sequence_name;
    char *last_value;
    char *start_value;
    char *increment_by;
    char *max_value;
    char *min_value;
    char *cache_value;
    bool is_cycled;
} sequence_values;

/*
 * We don't want to log each fetching of a value from a sequence,
 * so we pre-log a few fetches in advance. In the event of
 * crash we can lose (skip over) as many values as we pre-logged.
 */
const int SEQ_LOG_VALS = 32;


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
extern Datum last_insert_id(PG_FUNCTION_ARGS);
extern Datum last_insert_id_no_args(PG_FUNCTION_ARGS);
extern Datum pg_sequence_parameters(PG_FUNCTION_ARGS);
extern Datum pg_sequence_last_value(PG_FUNCTION_ARGS);

extern int128 nextval_internal(Oid relid);
extern void autoinc_setval(Oid relid, int128 next, bool iscalled);
extern int128 autoinc_get_nextval(Oid relid);
extern bool CheckSeqOwnedByAutoInc(Oid seqoid);
extern ObjectAddress DefineSequenceWrapper(CreateSeqStmt* stmt);
extern ObjectAddress AlterSequenceWrapper(AlterSeqStmt* stmt);
extern void PreventAlterSeqInTransaction(bool isTopLevel, AlterSeqStmt* stmt);
extern void ResetSequence(Oid seq_relid, bool restart);

extern void seq_redo(XLogReaderState* rptr);
extern void seq_desc(StringInfo buf, XLogReaderState* record);
extern const char* seq_type_name(uint8 subtype);
extern GTM_UUID get_uuid_from_rel(Relation rel);
extern void lockNextvalOnCn(Oid relid);
extern sequence_values *get_sequence_values(Oid sequenceId);

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

typedef enum {
    NDE_UNKNOWN = 0,
    NDE_NUMERIC, /* expected numeric nextval */
    NDE_BIGINT /* expected bigint nextval */
} nextval_default_expr_type_enum;


#define FULL_TABLE_NAME_MAX_LENGTH 256
extern void get_last_value_and_max_value(text* txt, int128* last_value, int128* current_max_value);
extern int128 get_and_reset_last_value(text* txt, int128 new_value, bool need_reseed);


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
extern void processUpdateSequenceMsg(List* nameList, int64 lastvalue);
extern void flushSequenceMsg();
extern void checkAndDoUpdateSequence();
#endif

/* Sequence util */
extern void fill_seq_with_data(Relation rel, HeapTuple tuple);
extern void ResetvalGlobal(Oid relid);
extern Relation lock_and_open_seq(SeqTable seq);
extern void init_sequence(Oid relid, SeqTable* p_elm, Relation* p_rel);
extern SeqTable GetSessSeqElm(Oid relid);
extern char* GetGlobalSeqNameForUpdate(Relation seqrel, char** dbname, char** schemaname);
extern uint32 RelidGetHash(Oid seq_relid);
extern SeqTable GetGlobalSeqElm(Oid relid, GlobalSeqInfoHashBucket* bucket);
extern Oid pg_get_serial_sequence_oid(text* tablename, text* columnname, bool find_identity = false);
bool StrEndWith(const char *str, const char *suffix);
typedef void (*InvokeNextvalHookType) (Oid relid, int128 val);

#endif /* SEQUENCE_H */
