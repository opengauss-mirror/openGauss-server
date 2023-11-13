/* -------------------------------------------------------------------------
 *
 * spi.h
 *				Server Programming Interface public declarations
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * Portions Copyright (c) 2021, openGauss Contributors
 * src/include/executor/spi.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef SPI_H
#define SPI_H

#include "nodes/parsenodes.h"
#include "tcop/dest.h"
#include "utils/resowner.h"
#include "utils/portal.h"

typedef struct SPITupleTable {
    MemoryContext tuptabcxt; /* memory context of result table */
    uint32 alloced;          /* # of alloced vals */
    uint32 free;             /* # of free vals */
    TupleDesc tupdesc;       /* tuple descriptor */
    HeapTuple* vals;         /* tuples */
} SPITupleTable;

/* Plans are opaque structs for standard users of SPI */
typedef struct _SPI_plan* SPIPlanPtr;

typedef struct SPICachedPlanStack {
    CachedPlan* cplan;
    SPICachedPlanStack* previous;
    SubTransactionId subtranid;
} SPIPlanStack;

#define SPI_ERROR_CONNECT (-1)
#define SPI_ERROR_COPY (-2)
#define SPI_ERROR_OPUNKNOWN (-3)
#define SPI_ERROR_UNCONNECTED (-4)
#define SPI_ERROR_CURSOR (-5) /* not used anymore */
#define SPI_ERROR_ARGUMENT (-6)
#define SPI_ERROR_PARAM (-7)
#define SPI_ERROR_TRANSACTION (-8)
#define SPI_ERROR_NOATTRIBUTE (-9)
#define SPI_ERROR_NOOUTFUNC (-10)
#define SPI_ERROR_TYPUNKNOWN (-11)

#define SPI_OK_CONNECT 1
#define SPI_OK_FINISH 2
#define SPI_OK_FETCH 3
#define SPI_OK_UTILITY 4
#define SPI_OK_SELECT 5
#define SPI_OK_SELINTO 6
#define SPI_OK_INSERT 7
#define SPI_OK_DELETE 8
#define SPI_OK_UPDATE 9
#define SPI_OK_CURSOR 10
#define SPI_OK_INSERT_RETURNING 11
#define SPI_OK_DELETE_RETURNING 12
#define SPI_OK_UPDATE_RETURNING 13
#define SPI_OK_REWRITTEN 14
#define SPI_OK_MERGE 15
#define SPI_OPT_NONATOMIC (1 << 0)

typedef List* (*parse_query_func)(const char *query_string, List **query_string_locationlist);
typedef void (*SpiReciverParamHook)(DestReceiver *self, SPIPlanPtr plan);
/* in postgres.cpp, avoid include tcopprot.h */
extern List* raw_parser(const char* query_string, List** query_string_locationlist);

static inline parse_query_func GetRawParser()
{
#ifndef ENABLE_MULTIPLE_NODES
    if (u_sess->attr.attr_sql.dolphin) {
        int id = GetCustomParserId();
        if (id >= 0 && g_instance.raw_parser_hook[id] != NULL) {
            return (parse_query_func)g_instance.raw_parser_hook[id];
        }
    }
#endif
    return raw_parser;
}

extern THR_LOCAL PGDLLIMPORT uint32 SPI_processed;
extern THR_LOCAL PGDLLIMPORT SPITupleTable* SPI_tuptable;
extern THR_LOCAL PGDLLIMPORT int SPI_result;

extern int SPI_connect(CommandDest dest = DestSPI, void (*spiCallbackfn)(void*) = NULL, void* clientData = NULL);
extern int SPI_connect_ext(CommandDest dest = DestSPI, void (*spiCallbackfn)(void *) = NULL, void *clientData = NULL,
                           int options = 0, Oid func_oid = InvalidOid);
extern int SPI_finish(void);
extern void SPI_push(void);
extern void SPI_pop(void);
extern bool SPI_push_conditional(void);
extern void SPI_pop_conditional(bool pushed);
extern void SPI_restore_connection(void);
extern void SPI_restore_connection_on_exception(void);
extern int SPI_execute(const char* src, bool read_only, long tcount, bool isCollectParam = false, parse_query_func parser = GetRawParser());
extern int SPI_execute_plan(SPIPlanPtr plan, Datum* Values, const char* Nulls, bool read_only, long tcount);
extern int SPI_execute_plan_with_paramlist(SPIPlanPtr plan, ParamListInfo params, bool read_only, long tcount);
extern int SPI_exec(const char* src, long tcount, parse_query_func parser = GetRawParser());
extern int SPI_execp(SPIPlanPtr plan, Datum* Values, const char* Nulls, long tcount);
extern int SPI_execute_snapshot(SPIPlanPtr plan, Datum* Values, const char* Nulls, Snapshot snapshot,
    Snapshot crosscheck_snapshot, bool read_only, bool fire_triggers, long tcount);
extern int SPI_execute_with_args(const char* src, int nargs, Oid* argtypes, Datum* Values, const char* Nulls,
    bool read_only, long tcount, Cursor_Data* cursor_data, parse_query_func parser = GetRawParser());
extern SPIPlanPtr SPI_prepare(const char* src, int nargs, Oid* argtypes, parse_query_func parser = GetRawParser());
#ifdef USE_SPQ
extern SPIPlanPtr SPI_prepare_spq(const char* src, int nargs, Oid* argtypes, parse_query_func parser = GetRawParser());
#endif
extern SPIPlanPtr SPI_prepare_cursor(const char* src, int nargs, Oid* argtypes, int cursorOptions, parse_query_func parser = GetRawParser());
extern SPIPlanPtr SPI_prepare_params(const char* src, ParserSetupHook parserSetup, void* parserSetupArg, int cursorOptions, parse_query_func parser = GetRawParser());
extern int SPI_keepplan(SPIPlanPtr plan);
extern SPIPlanPtr SPI_saveplan(SPIPlanPtr plan);
extern int SPI_freeplan(SPIPlanPtr plan);

extern Oid SPI_getargtypeid(SPIPlanPtr plan, int argIndex);
extern int SPI_getargcount(SPIPlanPtr plan);
extern bool SPI_is_cursor_plan(SPIPlanPtr plan, ParamListInfo paramLI);
extern bool SPI_plan_is_valid(SPIPlanPtr plan);
extern const char* SPI_result_code_string(int code);

extern List* SPI_plan_get_plan_sources(SPIPlanPtr plan);
extern CachedPlan* SPI_plan_get_cached_plan(SPIPlanPtr plan);

extern HeapTuple SPI_copytuple(HeapTuple tuple);
extern HeapTupleHeader SPI_returntuple(HeapTuple tuple, TupleDesc tupdesc);
extern HeapTuple SPI_modifytuple(
    Relation rel, HeapTuple tuple, int natts, int* attnum, Datum* Values, const char* Nulls);
extern int SPI_fnumber(TupleDesc tupdesc, const char* fname);
extern char* SPI_fname(TupleDesc tupdesc, int fnumber);
extern char* SPI_getvalue(HeapTuple tuple, TupleDesc tupdesc, int fnumber);
extern Datum SPI_getbinval(HeapTuple tuple, TupleDesc tupdesc, int fnumber, bool* isnull);
extern char* SPI_gettype(TupleDesc tupdesc, int fnumber);
extern Oid SPI_gettypeid(TupleDesc tupdesc, int fnumber);
extern Oid SPI_getcollation(TupleDesc tupdesc, int fnumber);
extern char* SPI_getrelname(Relation rel);
extern char* SPI_getnspname(Relation rel);
extern void* SPI_palloc(Size size);
extern void* SPI_repalloc(void* pointer, Size size);
extern void SPI_pfree(void* pointer);
extern void SPI_freetuple(HeapTuple pointer);
extern void SPI_freetuptable(SPITupleTable* tuptable);

extern Portal SPI_cursor_open(const char* name, SPIPlanPtr plan, Datum* Values, const char* Nulls, bool read_only);
extern Portal SPI_cursor_open_with_args(const char* name, const char* src, int nargs, Oid* argtypes, Datum* Values,
    const char* Nulls, bool read_only, int cursorOptions, parse_query_func parser = GetRawParser());
extern Portal SPI_cursor_open_with_paramlist(const char* name, SPIPlanPtr plan, ParamListInfo params,
                                             bool read_only, bool isCollectParam = false);
extern Portal SPI_cursor_find(const char* name);
extern void SPI_cursor_fetch(Portal portal, bool forward, long count);
extern void SPI_cursor_move(Portal portal, bool forward, long count);
extern void SPI_scroll_cursor_fetch(Portal, FetchDirection direction, long count);
extern void SPI_scroll_cursor_move(Portal, FetchDirection direction, long count);
extern void SPI_cursor_close(Portal portal);
extern void SPI_start_transaction(List* transactionHead);
extern void SPI_stp_transaction_check(bool read_only, bool savepoint = false);
extern void SPI_commit();
extern void SPI_rollback();
extern void SPI_save_current_stp_transaction_state();
extern void SPI_restore_current_stp_transaction_state();
extern TransactionId SPI_get_top_transaction_id();

extern void SPI_forbid_exec_push_down_with_exception();
extern void SPICleanup(void);
extern void AtEOXact_SPI(bool isCommit, bool STP_rollback, bool STP_commit);
extern void AtEOSubXact_SPI(bool isCommit, SubTransactionId mySubid, bool STP_rollback, bool STP_commit);
extern void AtEOSubXact_SPI(bool isCommit, SubTransactionId mySubid);
extern DestReceiver* createAnalyzeSPIDestReceiver(CommandDest dest);

extern void ReleaseSpiPlanRef(TransactionId mysubid);

/* SPI execution helpers */
extern void spi_exec_with_callback(CommandDest dest, const char* src, bool read_only, long tcount, bool direct_call,
    void (*callbackFn)(void*), void* clientData, parse_query_func parser = GetRawParser());

extern void _SPI_error_callback(void *arg);

extern List* _SPI_get_querylist(SPIPlanPtr plan);
#ifdef PGXC
extern int SPI_execute_direct(const char* src, char* nodename, parse_query_func parser = GetRawParser());
#endif
extern int _SPI_begin_call(bool execmem);
extern int _SPI_end_call(bool procmem);
extern void _SPI_hold_cursor(bool is_rollback = false);
extern void _SPI_prepare_oneshot_plan_for_validator(const char* src, SPIPlanPtr plan, parse_query_func parser = GetRawParser());
extern void InitSPIPlanCxt();
extern void _SPI_prepare_plan(const char *src, SPIPlanPtr plan, parse_query_func parser = GetRawParser());
extern ParamListInfo _SPI_convert_params(int nargs, Oid *argtypes, Datum *Values, const char *Nulls,
    Cursor_Data *cursor_data = NULL);
extern void _SPI_prepare_oneshot_plan(const char *src, SPIPlanPtr plan, parse_query_func parser = GetRawParser());
extern int _SPI_execute_plan(SPIPlanPtr plan, ParamListInfo paramLI, Snapshot snapshot, Snapshot crosscheck_snapshot,
    bool read_only, bool fire_triggers, long tcount, bool from_lock = false);

extern int SPI_connectid();
extern void SPI_disconnect(int connect);
extern void SPI_savepoint_create(const char* spName);
extern void SPI_savepoint_rollback(const char* spName);
extern void SPI_savepoint_release(const char* spName);
extern void SPI_savepoint_rollbackAndRelease(const char *spName, SubTransactionId subXid);

extern ResourceOwner AddCplanRefAgainIfNecessary(SPIPlanPtr plan,
    CachedPlanSource* plansource, CachedPlan* cplan, TransactionId oldTransactionId, ResourceOwner oldOwner);
#endif /* SPI_H */
