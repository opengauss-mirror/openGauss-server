/* -------------------------------------------------------------------------
 *
 * pgstatfuncs.c
 *	  Functions for accessing the statistics collector data
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/pgstatfuncs.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include <time.h>

#include "access/transam.h"
#include "access/tableam.h"
#include "access/ustore/undo/knl_uundoapi.h"
#include "access/ustore/undo/knl_uundotxn.h"
#include "access/ustore/undo/knl_uundozone.h"
#include "access/ubtree.h"
#include "access/redo_statistic.h"
#include "access/xlog_internal.h"
#include "access/multi_redo_api.h"
#include "connector.h"
#include "catalog/namespace.h"
#include "catalog/pg_database.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_type.h"
#include "catalog/pg_partition_fn.h"
#include "catalog/pg_namespace.h"
#include "commands/dbcommands.h"
#include "commands/user.h"
#include "commands/vacuum.h"
#include "commands/verify.h"
#include "funcapi.h"
#include "gaussdb_version.h"
#include "libpq/ip.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/globalplancache.h"
#include "utils/inet.h"
#include "utils/timestamp.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/memprot.h"
#include "utils/typcache.h"
#include "utils/syscache.h"
#include "pgxc/pgxc.h"
#include "pgxc/nodemgr.h"
#include "postmaster/autovacuum.h"
#include "postmaster/postmaster.h"
#include "storage/lock/lwlock.h"
#include "postgres.h"
#include "knl/knl_variable.h"
#include "storage/smgr/segment.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/buf/buf_internals.h"
#include "workload/cpwlm.h"
#include "workload/workload.h"
#include "pgxc/pgxcnode.h"
#include "access/hash.h"
#include "libcomm/libcomm.h"
#include "pgxc/poolmgr.h"
#include "pgxc/execRemote.h"
#include "utils/elog.h"
#include "utils/memtrace.h"
#include "commands/user.h"
#include "instruments/gs_stat.h"
#include "instruments/list.h"
#include "replication/rto_statistic.h"
#include "storage/lock/lock.h"
#include "nodes/makefuncs.h"

#define UINT32_ACCESS_ONCE(var) ((uint32)(*((volatile uint32*)&(var))))
#define NUM_PG_LOCKTAG_ID 12
#define NUM_PG_LOCKMODE_ID 13
#define NUM_PG_BLOCKSESSION_ID 14
#define MAX_LOCKTAG_STRING 6
#define UPPERCASE_LETTERS_ID 55
#define LOWERCASE_LETTERS_ID 87
#define DISPLACEMENTS_VALUE 32

const uint32 INDEX_STATUS_VIEW_COL_NUM = 3;

/* bogus ... these externs should be in a header file */
extern Datum pg_stat_get_numscans(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_tuples_returned(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_tuples_fetched(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_tuples_inserted(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_tuples_updated(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_tuples_changed(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_tuples_deleted(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_tuples_hot_updated(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_live_tuples(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_dead_tuples(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_blocks_fetched(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_blocks_hit(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_last_vacuum_time(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_last_data_changed_time(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_last_autovacuum_time(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_last_analyze_time(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_last_autoanalyze_time(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_vacuum_count(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_autovacuum_count(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_analyze_count(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_autoanalyze_count(PG_FUNCTION_ARGS);

extern Datum pg_stat_get_partition_tuples_changed(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_partition_dead_tuples(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_partition_tuples_inserted(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_partition_tuples_updated(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_partition_tuples_deleted(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_partition_tuples_hot_updated(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_partition_live_tuples(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_xact_partition_tuples_deleted(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_xact_partition_tuples_inserted(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_xact_partition_tuples_updated(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_xact_partition_tuples_hot_updated(PG_FUNCTION_ARGS);

extern Datum autovac_report_coname(PG_FUNCTION_ARGS);
extern Datum pg_autovac_stat(PG_FUNCTION_ARGS);
extern Datum pg_autoanalyze_stat(PG_FUNCTION_ARGS);

extern Datum pg_stat_get_function_calls(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_function_total_time(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_function_self_time(PG_FUNCTION_ARGS);

extern Datum pg_stat_get_backend_idset(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_activity(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_activity_with_conninfo(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_activity_helper(PG_FUNCTION_ARGS, bool has_conninfo);
extern Datum pg_stat_get_activity_ng(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_activity_for_temptable(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_status(PG_FUNCTION_ARGS);
extern Datum pgxc_stat_get_status(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_sql_count(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_thread(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_env(PG_FUNCTION_ARGS);
extern Datum pg_backend_pid(PG_FUNCTION_ARGS);
extern Datum pg_current_userid(PG_FUNCTION_ARGS);
extern Datum pg_current_sessionid(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_backend_pid(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_backend_dbid(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_backend_userid(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_backend_activity(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_backend_waiting(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_backend_activity_start(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_backend_xact_start(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_backend_start(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_backend_client_addr(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_backend_client_port(PG_FUNCTION_ARGS);

extern Datum pg_stat_get_db_numbackends(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_db_xact_commit(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_db_xact_rollback(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_db_blocks_fetched(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_db_blocks_hit(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_db_tuples_returned(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_db_tuples_fetched(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_db_tuples_inserted(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_db_tuples_updated(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_db_tuples_deleted(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_db_conflict_tablespace(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_db_conflict_lock(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_db_conflict_snapshot(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_db_conflict_bufferpin(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_db_conflict_startup_deadlock(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_db_conflict_all(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_db_deadlocks(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_db_stat_reset_time(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_db_temp_files(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_db_temp_bytes(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_db_blk_read_time(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_db_blk_write_time(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_mem_mbytes_reserved(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_workload_struct_info(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_dnode_cpu_time(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_wlm_statistics(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_session_wlmstat(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_wlm_session_info(PG_FUNCTION_ARGS);

extern Datum pg_stat_get_bgwriter_timed_checkpoints(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_bgwriter_requested_checkpoints(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_checkpoint_write_time(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_checkpoint_sync_time(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_bgwriter_buf_written_checkpoints(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_bgwriter_buf_written_clean(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_bgwriter_maxwritten_clean(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_bgwriter_stat_reset_time(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_buf_written_backend(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_buf_fsync_backend(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_buf_alloc(PG_FUNCTION_ARGS);

char g_dir[100] = {0};
typedef enum XactAction {
    XACTION_INSERT = 0,
    XACTION_UPDATE,
    XACTION_DELETE,
    XACTION_HOTUPDATE,
    XACTION_OTHER
} XactAction;
static void pg_stat_update_xact_tuples(Oid relid, PgStat_Counter *result, XactAction action);
extern Datum pg_stat_get_xact_numscans(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_xact_tuples_returned(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_xact_tuples_fetched(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_xact_tuples_inserted(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_xact_tuples_updated(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_xact_tuples_deleted(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_xact_tuples_hot_updated(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_xact_blocks_fetched(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_xact_blocks_hit(PG_FUNCTION_ARGS);

extern Datum pg_stat_get_xact_function_calls(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_xact_function_total_time(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_xact_function_self_time(PG_FUNCTION_ARGS);

extern Datum pg_stat_clear_snapshot(PG_FUNCTION_ARGS);
extern Datum pg_stat_reset(PG_FUNCTION_ARGS);
extern Datum pg_stat_reset_shared(PG_FUNCTION_ARGS);
extern Datum pg_stat_reset_single_table_counters(PG_FUNCTION_ARGS);
extern Datum pg_stat_reset_single_function_counters(PG_FUNCTION_ARGS);

extern Datum pv_os_run_info(PG_FUNCTION_ARGS);
extern Datum pv_session_memory_detail(PG_FUNCTION_ARGS);
extern Datum mot_session_memory_detail(PG_FUNCTION_ARGS);
extern Datum pg_shared_memory_detail(PG_FUNCTION_ARGS);
extern Datum pg_buffercache_pages(PG_FUNCTION_ARGS);
extern Datum pv_session_time(PG_FUNCTION_ARGS);
extern Datum pv_instance_time(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_file_stat(PG_FUNCTION_ARGS);
extern Datum get_local_rel_iostat(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_redo_stat(PG_FUNCTION_ARGS);
extern Datum pv_session_stat(PG_FUNCTION_ARGS);
extern Datum pv_session_memory(PG_FUNCTION_ARGS);

extern Datum pg_stat_bad_block(PG_FUNCTION_ARGS);
extern Datum pg_stat_bad_block_clear(PG_FUNCTION_ARGS);

extern Datum pg_stat_get_wlm_instance_info(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_wlm_instance_info_with_cleanup(PG_FUNCTION_ARGS);
extern Datum global_comm_get_recv_stream(PG_FUNCTION_ARGS);
extern Datum global_comm_get_send_stream(PG_FUNCTION_ARGS);
extern Datum global_comm_get_status(PG_FUNCTION_ARGS);
extern Datum comm_client_info(PG_FUNCTION_ARGS);
extern Datum global_comm_get_client_info(PG_FUNCTION_ARGS);

/*internal interface for pmk functions.*/
extern Datum total_cpu(PG_FUNCTION_ARGS);
extern Datum get_hostname(PG_FUNCTION_ARGS);
extern Datum sessionid2pid(PG_FUNCTION_ARGS);
extern Datum total_memory(PG_FUNCTION_ARGS);
extern Datum table_data_skewness(PG_FUNCTION_ARGS);
extern Datum pv_session_memctx_detail(PG_FUNCTION_ARGS);
extern Datum pv_total_memory_detail(PG_FUNCTION_ARGS);
extern Datum mot_global_memory_detail(PG_FUNCTION_ARGS);
extern Datum mot_local_memory_detail(PG_FUNCTION_ARGS);
extern Datum gs_total_nodegroup_memory_detail(PG_FUNCTION_ARGS);


extern Datum track_memory_context_detail(PG_FUNCTION_ARGS);

/* Global bgwriter statistics, from bgwriter.c */
extern PgStat_MsgBgWriter bgwriterStats;

extern GlobalNodeDefinition* global_node_definition;

extern int64 MetaCacheGetCurrentUsedSize();
extern char* getNodenameByIndex(int index);

extern char* GetRoleName(Oid rolid, char* rolname, size_t size);
static int find_next_user_idx(unsigned int ordinary_userid, int call_cntr, int max_calls, int current_index);
static PgStat_WaitCountStatus* wcslist_nthcell_array(int current_index);

extern uint64 g_searchserver_memory;
extern bool is_searchserver_api_load();
extern void* get_searchlet_resource_info(int* used_mem, int* peak_mem);
extern int do_autovac_coor(List* coors, Name relname);
extern List* parse_autovacuum_coordinators();
extern Datum pg_autovac_timeout(PG_FUNCTION_ARGS);
static int64 pgxc_exec_autoanalyze_timeout(Oid relOid, int32 coordnum, char* funcname);
extern bool allow_autoanalyze(HeapTuple tuple);

int g_stat_file_id = -1;

/* the size of GaussDB_expr.ir */
#define IR_FILE_SIZE 29800
#define WAITSTATELEN 256

static int64 pgxc_exec_partition_tuples_stat(HeapTuple classTuple, HeapTuple partTuple, char* funcname);

static inline void InitPlanOperatorInfoTuple(int operator_plan_info_attrnum, FuncCallContext *funcctx);

void pg_stat_get_stat_list(List** stat_list, uint32* statFlag_ref, Oid relid);

void insert_pg_stat_get_activity_with_conninfo(Tuplestorestate *tupStore, TupleDesc tupDesc,
                                               const PgBackendStatus *beentry);
void insert_pg_stat_get_activity(Tuplestorestate *tupStore, TupleDesc tupDesc, const PgBackendStatus *beentry);
void insert_pg_stat_get_activity_for_temptable(Tuplestorestate *tupStore, TupleDesc tupDesc,
                                               const PgBackendStatus *beentry);
void insert_pg_stat_get_activity_ng(Tuplestorestate *tupStore, TupleDesc tupDesc, const PgBackendStatus *beentry);
void insert_pg_stat_get_status(Tuplestorestate *tupStore, TupleDesc tupDesc, const PgBackendStatus *beentry);
void insert_pg_stat_get_thread(Tuplestorestate *tupStore, TupleDesc tupDesc, const PgBackendStatus *beentry);
void insert_pg_stat_get_session_wlmstat(Tuplestorestate *tupStore, TupleDesc tupDesc, const PgBackendStatus *beentry);
void insert_pg_stat_get_session_respool(Tuplestorestate *tupStore, TupleDesc tupDesc, const PgBackendStatus *beentry);
void insert_comm_client_info(Tuplestorestate *tupStore, TupleDesc tupDesc, const PgBackendStatus *beentry);
void insert_gs_stat_activity_timeout(Tuplestorestate* tupStore, TupleDesc tupDesc, const PgBackendStatus *beentry);

void insert_pv_session_time(Tuplestorestate* tupStore, TupleDesc tupDesc, const SessionTimeEntry* entry);
void insert_pv_session_stat(Tuplestorestate* tupStore, TupleDesc tupDesc, const SessionLevelStatistic* entry);
void insert_pv_session_memory(Tuplestorestate* tupStore, TupleDesc tupDesc, const SessionLevelMemory* entry);


/*
 * description for WaitState enums.
 * Note. If you want to add a new desc item, use the new description as short as possible.
 */
static const char* WaitStateDesc[] = {
    "none",                          // STATE_WAIT_UNDEFINED
    "acquire lwlock",                // STATE_WAIT_LWLOCK
    "acquire lock",                  // STATE_WAIT_LOCK
    "wait io",                       // STATE_WAIT_IO
    "wait cmd",                      // STATE_WAIT_COMM
    "wait pooler get conn",          // STATE_WAIT_POOLER_GETCONN
    "wait pooler abort conn",        // STATE_WAIT_POOLER_ABORTCONN
    "wait pooler clean conn",        // STATE_WAIT_POOLER_CLEANCONN
    "pooler create conn",            // STATE_POOLER_CREATE_CONN
    "get conn",                      // STATE_POOLER_WAIT_GETCONN
    "set cmd",                       // STATE_POOLER_WAIT_SETCMD
    "reset cmd",                     // STATE_POOLER_WAIT_RESETCMD
    "cancel query",                  // STATE_POOLER_WAIT_CANCEL
    "stop query",                    // STATE_POOLER_WAIT_STOP
    "wait node",                     // STATE_WAIT_NODE
    "wait transaction sync",         // STATE_WAIT_XACTSYNC
    "wait wal sync",                 // STATE_WAIT_WALSYNC
    "wait data sync",                // STATE_WAIT_DATASYNC
    "wait data sync queue",          // STATE_WAIT_DATASYNC_QUEUE
    "flush data",                    // STATE_WAIT_FLUSH_DATA
    "wait reserve td",               // STATE_WAIT_RESERVE_TD
    "wait td rollback",              // STATE_WAIT_TD_ROLLBACK
    "wait transaction rollback",     // STATE_WAIT_TRANSACTION_ROLLBACK
    "prune table",                   // STATE_PRUNE_TABLE
    "prune index",                   // STATE_PRUNE_INDEX
    "stream get conn",               // STATE_STREAM_WAIT_CONNECT_NODES
    "wait producer ready",           // STATE_STREAM_WAIT_PRODUCER_READY
    "synchronize quit",              // STATE_STREAM_WAIT_THREAD_SYNC_QUIT
    "wait stream group destroy",     // STATE_STREAM_WAIT_NODEGROUP_DESTROY
    "wait active statement",         // STATE_WAIT_ACTIVE_STATEMENT
    "wait memory",                   // STATE_WAIT_MEMORY
    "Sort",                          // STATE_EXEC_SORT
    "Sort - fetch tuple",            // STATE_EXEC_SORT_FETCH_TUPLE
    "Sort - write file",             // STATE_EXEC_SORT_WRITE_FILE
    "Material",                      // STATE_EXEC_MATERIAL
    "Material - write file",         // STATE_EXEC_MATERIAL_WRITE_FILE
    "HashJoin - build hash",         // STATE_EXEC_HASHJOIN_BUILD_HASH
    "HashJoin - write file",         // STATE_EXEC_HASHJOIN_WRITE_FILE
    "HashAgg - build hash",          // STATE_EXEC_HASHAGG_BUILD_HASH
    "HashAgg - write file",          // STATE_EXEC_HASHAGG_WRITE_FILE
    "HashSetop - build hash",        // STATE_EXEC_HASHSETOP_BUILD_HASH
    "HashSetop - write file",        // STATE_EXEC_HASHSETOP_WRITE_FILE
    "NestLoop",                      // STATE_EXEC_NESTLOOP
    "create index",                  // STATE_CREATE_INDEX
    "analyze",                       // STATE_ANALYZE
    "vacuum",                        // STATE_VACUUM
    "vacuum full",                   // STATE_VACUUM_FULL
    "gtm connect",                   // STATE_GTM_CONNECT
    "gtm reset xmin",                // STATE_GTM_RESET_XMIN
    "gtm get xmin",                  // STATE_GTM_GET_XMIN
    "gtm get gxid",                  // STATE_GTM_GET_GXID
    "gtm get csn",                   // STATE_GTM_GET_CSN
    "gtm get snapshot",              // STATE_GTM_GET_SNAPSHOT
    "gtm begin trans",               // STATE_GTM_BEGIN_TRANS
    "gtm commit trans",              // STATE_GTM_COMMIT_TRANS
    "gtm rollback trans",            // STATE_GTM_ROLLBACK_TRANS
    "gtm start preprare trans",      // STATE_GTM_START_PREPARE_TRANS
    "gtm prepare trans",             // STATE_GTM_PREPARE_TRANS
    "gtm open sequence",             // STATE_GTM_OPEN_SEQUENCE
    "gtm close sequence",            // STATE_GTM_CLOSE_SEQUENCE
    "gtm create sequence",           // STATE_GTM_CREATE_SEQUENCE
    "gtm alter sequence",            // STATE_GTM_ALTER_SEQUENCE
    "gtm get sequence val",          // STATE_GTM_SEQUENCE_GET_NEXT_VAL
    "gtm set sequence val",          // STATE_GTM_SEQUENCE_SET_VAL
    "gtm drop sequence",             // STATE_GTM_DROP_SEQUENCE
    "gtm rename sequence",           // STATE_GTM_RENAME_SEQUENCE
    "gtm set disaster cluster",      // STATE_GTM_SET_DISASTER_CLUSTER
    "gtm get disaster cluster",      // STATE_GTM_GET_DISASTER_CLUSTER
    "gtm del disaster cluster",      // STATE_GTM_DEL_DISASTER_CLUSTER
    "wait sync consumer next step",  // STATE_WAIT_SYNC_CONSUMER_NEXT_STEP
    "wait sync producer next step",  // STATE_WAIT_SYNC_PRODUCER_NEXT_STEP
    "gtm set consistency point",     // STATE_GTM_SET_CONSISTENCY_POINT
    "wait sync bgworkers"            // STATE_WAIT_SYNC_BGWORKERS
};

// description for WaitStatePhase enums.
static const char* WaitStatePhaseDesc[] = {
    "none",        // PHASE_NONE
    "begin",       // PHASE_BEGIN
    "commit",      // PHASE_COMMIT
    "rollback",    // PHASE_ROLLBACK
    "wait quota",  // PHASE_WAIT_QUOTA
    "autovacuum",  // PHASE_AUTOVACUUM
};


/* ----------
 * pgstat_get_waitstatusdesc() -
 *
 * Return the corresponding wait status description in WaitStateDesc.
 * ----------
 */
const char* pgstat_get_waitstatusdesc(uint32 wait_event_info)
{
    uint32 classId;
    classId = wait_event_info & 0xFF000000;

    switch (classId) {
        case PG_WAIT_LWLOCK:
            return WaitStateDesc[STATE_WAIT_LWLOCK];
        case PG_WAIT_LOCK:
            return WaitStateDesc[STATE_WAIT_LOCK];
        case PG_WAIT_IO:
            return WaitStateDesc[STATE_WAIT_IO];
        default:
            return WaitStateDesc[STATE_WAIT_UNDEFINED];
    }
}

const char* pgstat_get_waitstatusname(uint32 wait_event_info)
{
    return WaitStateDesc[wait_event_info];
}

const char* PgstatGetWaitstatephasename(uint32 waitPhaseInfo)
{
    return WaitStatePhaseDesc[waitPhaseInfo];
}

/*
 * compute tuples stat for table insert/update/delete
 * 1. if it is a replication table, return max value on all datanode
 * 2. if it is a hashtable, return sum value on all datanode
 */
static void compute_tuples_stat(ParallelFunctionState* state, bool replicated)
{

    TupleTableSlot* slot = NULL;
    Datum datum;
    int64 result = 0;

    Assert(state && state->tupstore);
    if (!state->tupdesc) {
        /*
         * we fetch autovac count from remote coordinator, if there is only one
         * coordinator, there is no remote coordinator, tupdesc is uninitialized
         * , so we just do return 0 here
         */
        state->result = 0;
        return;
    }

    slot = MakeSingleTupleTableSlot(state->tupdesc);
    for (;;) {
        bool isnull = false;

        if (!tuplestore_gettupleslot(state->tupstore, true, false, slot))
            break;

        datum = tableam_tslot_getattr(slot, 1, &isnull);
        if (!isnull) {
            result = DatumGetInt64(datum);

            if (!replicated)
                state->result += result;
            else if (state->result < result)
                state->result = result;
        }

        ExecClearTuple(slot);
    }
}

static int64 pgxc_exec_tuples_stat(Oid relOid, char* funcname, RemoteQueryExecType exec_type)
{
    Relation rel = NULL;
    char* relname = NULL;
    char* nspname = NULL;
    ExecNodes* exec_nodes = NULL;
    StringInfoData buf;
    ParallelFunctionState* state = NULL;
    int64 result = 0;
    bool replicated = false;

    rel = try_relation_open(relOid, AccessShareLock);
    if (!rel) {
        /* it has been dropped */
        return 0;
    }

    exec_nodes = (ExecNodes*)makeNode(ExecNodes);
    exec_nodes->baselocatortype = LOCATOR_TYPE_HASH;
    exec_nodes->accesstype = RELATION_ACCESS_READ;
    exec_nodes->en_relid = relOid;
    exec_nodes->primarynodelist = NIL;
    exec_nodes->en_expr = NULL;

    if (EXEC_ON_DATANODES == exec_type) {
        /* for tuples stat(insert/update/delete/changed..) count */
        exec_nodes->en_relid = relOid;
        exec_nodes->nodeList = NIL;
        replicated = IsRelationReplicated(RelationGetLocInfo(rel));
    } else if (EXEC_ON_COORDS == exec_type) {
        /* for autovac count */
        exec_nodes->en_relid = InvalidOid;
        exec_nodes->nodeList = GetAllCoordNodes();
    } else {
        Assert(false);
    }

    /*
     * we awalys send some query string to datanode, Maybe it contains some object name such
     * as table name, schema name. If the relname has single quotes ('), we have to replace
     * it with two single quotes ('') to avoid syntax error when the sql string excutes in datanode
     */
    relname = repairObjectName(RelationGetRelationName(rel));
    nspname = repairObjectName(get_namespace_name(rel->rd_rel->relnamespace));
    relation_close(rel, AccessShareLock);

    initStringInfo(&buf);
    appendStringInfo(&buf,
        "SELECT pg_catalog.%s(c.oid) FROM pg_class c "
        "INNER JOIN pg_namespace n on c.relnamespace = n.oid "
        "WHERE n.nspname='%s' AND relname = '%s'",
        funcname,
        nspname,
        relname);

    state = RemoteFunctionResultHandler(buf.data, exec_nodes, NULL, true, exec_type, true);
    compute_tuples_stat(state, replicated);
    result = state->result;

    pfree_ext(nspname);
    pfree_ext(relname);
    pfree_ext(buf.data);
    FreeParallelFunctionState(state);

    return result;
}

/* get lastest autovac time */
static void StrategyFuncMaxTimestamptz(ParallelFunctionState* state)
{
    TupleTableSlot* slot = NULL;
    TimestampTz* result = NULL;

    state->resultset = (TimestampTz*)palloc(sizeof(TimestampTz));
    result = (TimestampTz*)state->resultset;
    *result = 0;

    Assert(state && state->tupstore);
    if (NULL == state->tupdesc) {
        /*
         * if there is only one coordinator, there is no remote coordinator, tupdesc
         * will be uninitialized, we just do return here
         */
        return;
    }

    slot = MakeSingleTupleTableSlot(state->tupdesc);
    for (;;) {
        bool isnull = false;
        TimestampTz tmp = 0;

        if (!tuplestore_gettupleslot(state->tupstore, true, false, slot))
            break;

        tmp = DatumGetTimestampTz(tableam_tslot_getattr(slot, 1, &isnull));
        if (!isnull && timestamp_cmp_internal(tmp, *result) > 0) 
            *result = tmp;

        ExecClearTuple(slot);
    }
}

/* get lastest autoanalyze/autovacuum timestamp from remote coordinator */
static TimestampTz pgxc_last_autovac_time(Oid relOid, char* funcname)
{
    Oid nspid = get_rel_namespace(relOid);
    char* relname = NULL;
    char* nspname = NULL;
    ExecNodes* exec_nodes = NULL;
    TimestampTz result = 0;
    StringInfoData buf;
    ParallelFunctionState* state = NULL;

    relname = get_rel_name(relOid);
    nspname = get_namespace_name(nspid);
    /* it has been dropped */
    if ((relname == NULL) || (nspname == NULL))
        return 0;

    exec_nodes = (ExecNodes*)makeNode(ExecNodes);
    exec_nodes->accesstype = RELATION_ACCESS_READ;
    exec_nodes->baselocatortype = LOCATOR_TYPE_HASH;
    exec_nodes->nodeList = GetAllCoordNodes();
    exec_nodes->primarynodelist = NIL;
    exec_nodes->en_expr = NULL;

    relname = repairObjectName(relname);
    nspname = repairObjectName(nspname);

    initStringInfo(&buf);
    appendStringInfo(&buf,
        "SELECT pg_catalog.%s(c.oid) FROM pg_class c "
        "INNER JOIN pg_namespace n on c.relnamespace = n.oid "
        "WHERE n.nspname='%s' AND relname = '%s' ",
        funcname,
        nspname,
        relname);
    state = RemoteFunctionResultHandler(buf.data, exec_nodes, StrategyFuncMaxTimestamptz, true, EXEC_ON_COORDS, true);
    result = *((TimestampTz*)state->resultset);
    FreeParallelFunctionState(state);

    return result;
}

/*
 * Build tuple desc and store for the caller result
 * return the tuple store, the tupdesc would be return by pointer.
 */
Tuplestorestate *BuildTupleResult(FunctionCallInfo fcinfo, TupleDesc *tupdesc)
{
    ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
    Tuplestorestate *tupstore = NULL;

    MemoryContext per_query_ctx;
    MemoryContext oldcontext;

    /* check to see if caller supports returning a tuplestore */
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("set-valued function called in context that cannot accept a set")));
    }

    if (!(rsinfo->allowedModes & SFRM_Materialize))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("materialize mode required, but it is not "
                                                                       "allowed in this context")));

    /* Build a tuple descriptor for our result type */
    if (get_call_result_type(fcinfo, NULL, tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("return type must be a row type")));

    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);

    tupstore = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = *tupdesc;

    (void)MemoryContextSwitchTo(oldcontext);

    return tupstore;
}

Datum pg_stat_get_numscans(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    int64 result = 0;
    List* stat_list = NIL;
    ListCell* stat_cell = NULL;
    PgStat_StatTabKey tabkey;
    PgStat_StatTabEntry* tabentry = NULL;
    pg_stat_get_stat_list(&stat_list, &tabkey.statFlag, relid);

    foreach (stat_cell, stat_list) {
        tabkey.tableid = lfirst_oid(stat_cell);
        tabentry = pgstat_fetch_stat_tabentry(&tabkey);

        if (!PointerIsValid(tabentry)) {
            result += 0;
        } else {
            result += (int64)(tabentry->numscans);
        }
    }

    if (PointerIsValid(stat_list)) {
        list_free(stat_list);
    }

    PG_RETURN_INT64(result);
}

void pg_stat_get_stat_list(List** stat_list, uint32* statFlag_ref, Oid relid)
{
    if (isPartitionedObject(relid, RELKIND_RELATION, true)) {
        *statFlag_ref = STATFLG_PARTITION;
        *stat_list = getPartitionObjectIdList(relid, PART_OBJ_TYPE_TABLE_PARTITION);
    } else if (isPartitionedObject(relid, RELKIND_INDEX, true)) {
        *statFlag_ref = STATFLG_PARTITION;
        *stat_list = getPartitionObjectIdList(relid, PART_OBJ_TYPE_INDEX_PARTITION);
    } else {
        *statFlag_ref = STATFLG_RELATION;
        *stat_list = lappend_oid(*stat_list, relid);
    }
}

Datum pg_stat_get_tuples_returned(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    int64 result = 0;
    List* stat_list = NIL;
    ListCell* stat_cell = NULL;
    PgStat_StatTabKey tabkey;
    PgStat_StatTabEntry* tabentry = NULL;
    pg_stat_get_stat_list(&stat_list, &tabkey.statFlag, relid);

    foreach (stat_cell, stat_list) {
        tabkey.tableid = lfirst_oid(stat_cell);
        tabentry = pgstat_fetch_stat_tabentry(&tabkey);

        if (!PointerIsValid(tabentry)) {
            result += 0;
        } else {
            result += (int64)(tabentry->tuples_returned);
        }
    }

    if (PointerIsValid(stat_list)) {
        list_free(stat_list);
    }

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_tuples_fetched(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    int64 result = 0;
    List* stat_list = NIL;
    ListCell* stat_cell = NULL;
    PgStat_StatTabKey tabkey;
    PgStat_StatTabEntry* tabentry = NULL;
    pg_stat_get_stat_list(&stat_list, &tabkey.statFlag, relid);

    foreach (stat_cell, stat_list) {
        tabkey.tableid = lfirst_oid(stat_cell);
        tabentry = pgstat_fetch_stat_tabentry(&tabkey);

        if (!PointerIsValid(tabentry)) {
            result += 0;
        } else {
            result += (int64)(tabentry->tuples_fetched);
        }
    }

    if (PointerIsValid(stat_list)) {
        list_free(stat_list);
    }

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_tuples_inserted(PG_FUNCTION_ARGS)
{
    PgStat_StatTabKey tabkey;
    int64 result = 0;
    Oid tableid = PG_GETARG_OID(0);
    PgStat_StatTabEntry* tabentry = NULL;

    /* for user-define table, fetch inserted tuples from datanode*/
    if (IS_PGXC_COORDINATOR && GetRelationLocInfo(tableid))
        PG_RETURN_INT64(pgxc_exec_tuples_stat(tableid, "pg_stat_get_tuples_inserted", EXEC_ON_DATANODES));

    tabkey.statFlag = InvalidOid;
    tabkey.tableid = tableid;
    tabentry = pgstat_fetch_stat_tabentry(&tabkey);

    if (tabentry != NULL)
        result = (int64)(tabentry->tuples_inserted);

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_tuples_updated(PG_FUNCTION_ARGS)
{
    PgStat_StatTabKey tabkey;
    int64 result = 0;
    Oid tableid = PG_GETARG_OID(0);
    PgStat_StatTabEntry* tabentry = NULL;

    /* for user-define table, fetch updated tuples from datanode*/
    if (IS_PGXC_COORDINATOR && GetRelationLocInfo(tableid))
        PG_RETURN_INT64(pgxc_exec_tuples_stat(tableid, "pg_stat_get_tuples_updated", EXEC_ON_DATANODES));

    tabkey.statFlag = InvalidOid;
    tabkey.tableid = tableid;
    tabentry = pgstat_fetch_stat_tabentry(&tabkey);

    if (tabentry != NULL)
        result = (int64)(tabentry->tuples_updated);

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_tuples_deleted(PG_FUNCTION_ARGS)
{
    Oid tableid = PG_GETARG_OID(0);
    PgStat_StatTabKey tabkey;
    int64 result = 0;
    PgStat_StatTabEntry* tabentry = NULL;

    /* for user-define table, fetch deleted tuples from datanode*/
    if (IS_PGXC_COORDINATOR && GetRelationLocInfo(tableid))
        PG_RETURN_INT64(pgxc_exec_tuples_stat(tableid, "pg_stat_get_tuples_deleted", EXEC_ON_DATANODES));

    tabkey.statFlag = InvalidOid;
    tabkey.tableid = tableid;
    tabentry = pgstat_fetch_stat_tabentry(&tabkey);
    if (tabentry != NULL)
        result = (int64)(tabentry->tuples_deleted);

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_tuples_hot_updated(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    PgStat_StatTabKey tabkey;
    PgStat_StatTabEntry* tabentry = NULL;
    int64 result = 0;

    /* for user-define table, fetch hot-updated tuples from datanode*/
    if (IS_PGXC_COORDINATOR && GetRelationLocInfo(relid))
        PG_RETURN_INT64(pgxc_exec_tuples_stat(relid, "pg_stat_get_tuples_hot_updated", EXEC_ON_DATANODES));

    tabkey.statFlag = InvalidOid;
    tabkey.tableid = relid;
    tabentry = pgstat_fetch_stat_tabentry(&tabkey);

    if (tabentry != NULL)
        result = (int64)(tabentry->tuples_hot_updated);

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_tuples_changed(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    int64 result = 0;
    PgStat_StatTabKey tabkey;
    PgStat_StatTabEntry* tabentry = NULL;

    /* for user-define table, fetch changed tuples from datanode*/
    if (IS_PGXC_COORDINATOR && GetRelationLocInfo(relid))
        PG_RETURN_INT64(pgxc_exec_tuples_stat(relid, "pg_stat_get_tuples_changed", EXEC_ON_DATANODES));

    tabkey.statFlag = InvalidOid;
    tabkey.tableid = relid;
    tabentry = pgstat_fetch_stat_tabentry(&tabkey);

    if (tabentry != NULL)
        result = (int64)(tabentry->changes_since_analyze);

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_live_tuples(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    PgStat_StatTabKey tabkey;
    PgStat_StatTabEntry* tabentry = NULL;
    int64 result = 0;
    List* stat_list = NIL;
    ListCell* stat_cell = NULL;
    List* sublist = NIL;
    ListCell* subcell = NULL;

    /* for user-define table, fetch live tuples from datanode*/
    if (IS_PGXC_COORDINATOR && GetRelationLocInfo((relid)))
        PG_RETURN_INT64(pgxc_exec_tuples_stat(relid, "pg_stat_get_live_tuples", EXEC_ON_DATANODES));

    HeapTuple reltuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(reltuple)) {
        PG_RETURN_INT64(result);
    }
    Form_pg_class relation = (Form_pg_class)GETSTRUCT(reltuple);
    if (PARTTYPE_SUBPARTITIONED_RELATION == relation->parttype) {
        stat_list = getPartitionObjectIdList(relid, PART_OBJ_TYPE_TABLE_PARTITION);
        foreach (stat_cell, stat_list) {
            Oid subparentid = lfirst_oid(stat_cell);
            sublist = getPartitionObjectIdList(subparentid, PART_OBJ_TYPE_TABLE_SUB_PARTITION);
            tabkey.statFlag = subparentid;
            foreach (subcell, sublist) {
                tabkey.tableid = lfirst_oid(subcell);
                tabentry = pgstat_fetch_stat_tabentry(&tabkey);
                if (tabentry != NULL)
                    result += (int64)(tabentry->n_live_tuples);
            }
            list_free_ext(sublist);
        }
        list_free_ext(stat_list);
    } else if (PARTTYPE_PARTITIONED_RELATION == relation->parttype) {
        char objtype = PART_OBJ_TYPE_TABLE_PARTITION;
        if (relation->relkind == RELKIND_INDEX) {
            objtype = PART_OBJ_TYPE_INDEX_PARTITION;
        }
        stat_list = getPartitionObjectIdList(relid, objtype);
        tabkey.statFlag = relid;
        foreach (stat_cell, stat_list) {
            tabkey.tableid = lfirst_oid(stat_cell);
            tabentry = pgstat_fetch_stat_tabentry(&tabkey);
            if (tabentry != NULL)
                result += (int64)(tabentry->n_live_tuples);
        }
        list_free_ext(stat_list);
    } else {
        tabkey.statFlag = InvalidOid;
        tabkey.tableid = relid;
        tabentry = pgstat_fetch_stat_tabentry(&tabkey);
        if (tabentry != NULL)
            result += (int64)(tabentry->n_live_tuples);
    }
    ReleaseSysCache(reltuple);

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_dead_tuples(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    PgStat_StatTabKey tabkey;
    PgStat_StatTabEntry* tabentry = NULL;
    int64 result = 0;
    List* stat_list = NIL;
    ListCell* stat_cell = NULL;
    List* sublist = NIL;
    ListCell* subcell = NULL;

    if (IS_PGXC_COORDINATOR && GetRelationLocInfo((relid)))
        PG_RETURN_INT64(pgxc_exec_tuples_stat(relid, "pg_stat_get_dead_tuples", EXEC_ON_DATANODES));

    HeapTuple reltuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(reltuple)) {
        PG_RETURN_INT64(result);
    }
    Form_pg_class relation = (Form_pg_class)GETSTRUCT(reltuple);
    if (PARTTYPE_SUBPARTITIONED_RELATION == relation->parttype) {
        stat_list = getPartitionObjectIdList(relid, PART_OBJ_TYPE_TABLE_PARTITION);
        foreach (stat_cell, stat_list) {
            Oid subparentid = lfirst_oid(stat_cell);
            sublist = getPartitionObjectIdList(subparentid, PART_OBJ_TYPE_TABLE_SUB_PARTITION);
            tabkey.statFlag = subparentid;
            foreach (subcell, sublist) {
                tabkey.tableid = lfirst_oid(subcell);
                tabentry = pgstat_fetch_stat_tabentry(&tabkey);
                if (tabentry != NULL)
                    result += (int64)(tabentry->n_dead_tuples);
            }
            list_free_ext(sublist);
        }
        list_free_ext(stat_list);
    } else if (PARTTYPE_PARTITIONED_RELATION == relation->parttype) {
        char objtype = PART_OBJ_TYPE_TABLE_PARTITION;
        if (relation->relkind == RELKIND_INDEX) {
            objtype = PART_OBJ_TYPE_INDEX_PARTITION;
        }
        stat_list = getPartitionObjectIdList(relid, objtype);
        tabkey.statFlag = relid;
        foreach (stat_cell, stat_list) {
            tabkey.tableid = lfirst_oid(stat_cell);
            tabentry = pgstat_fetch_stat_tabentry(&tabkey);
            if (tabentry != NULL)
                result += (int64)(tabentry->n_dead_tuples);
        }
        list_free_ext(stat_list);
    } else {
        tabkey.statFlag = InvalidOid;
        tabkey.tableid = relid;
        tabentry = pgstat_fetch_stat_tabentry(&tabkey);
        if (tabentry != NULL)
            result += (int64)(tabentry->n_dead_tuples);
    }
    ReleaseSysCache(reltuple);

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_blocks_fetched(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    int64 result = 0;
    List* stat_list = NIL;
    ListCell* stat_cell = NULL;
    PgStat_StatTabKey tabkey;
    PgStat_StatTabEntry* tabentry = NULL;
    pg_stat_get_stat_list(&stat_list, &tabkey.statFlag, relid);

    foreach (stat_cell, stat_list) {
        tabkey.tableid = lfirst_oid(stat_cell);
        tabentry = pgstat_fetch_stat_tabentry(&tabkey);

        if (!PointerIsValid(tabentry)) {
            result += 0;
        } else {
            result += (int64)(tabentry->blocks_fetched);
        }
    }

    if (PointerIsValid(stat_list)) {
        list_free(stat_list);
    }

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_blocks_hit(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    int64 result = 0;
    List* stat_list = NIL;
    ListCell* stat_cell = NULL;
    PgStat_StatTabKey tabkey;
    PgStat_StatTabEntry* tabentry = NULL;
    pg_stat_get_stat_list(&stat_list, &tabkey.statFlag, relid);
    foreach (stat_cell, stat_list) {
        tabkey.tableid = lfirst_oid(stat_cell);
        tabentry = pgstat_fetch_stat_tabentry(&tabkey);

        if (!PointerIsValid(tabentry)) {
            result += 0;
        } else {
            result += (int64)(tabentry->blocks_hit);
        }
    }

    if (PointerIsValid(stat_list)) {
        list_free(stat_list);
    }

    PG_RETURN_INT64(result);
}

/**
 * @Description:  get cu stat mem hit
 * @return  datum
 */
Datum pg_stat_get_cu_mem_hit(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    int64 result = 0;
    List* stat_list = NIL;
    ListCell* stat_cell = NULL;
    PgStat_StatTabKey tabkey;
    PgStat_StatTabEntry* tabentry = NULL;
    pg_stat_get_stat_list(&stat_list, &tabkey.statFlag, relid);

    foreach (stat_cell, stat_list) {
        tabkey.tableid = lfirst_oid(stat_cell);
        tabentry = pgstat_fetch_stat_tabentry(&tabkey);

        if (PointerIsValid(tabentry)) {
            result += (int64)(tabentry->cu_mem_hit);
        }
    }

    if (PointerIsValid(stat_list)) {
        list_free(stat_list);
    }

    PG_RETURN_INT64(result);
}

/**
 * @Description:  get cu stat hdd sync read
 * @return  datum
 */
Datum pg_stat_get_cu_hdd_sync(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    int64 result = 0;
    List* stat_list = NIL;
    ListCell* stat_cell = NULL;
    PgStat_StatTabKey tabkey;
    PgStat_StatTabEntry* tabentry = NULL;
    pg_stat_get_stat_list(&stat_list, &tabkey.statFlag, relid);

    foreach (stat_cell, stat_list) {
        tabkey.tableid = lfirst_oid(stat_cell);
        tabentry = pgstat_fetch_stat_tabentry(&tabkey);

        if (PointerIsValid(tabentry)) {
            result += (int64)(tabentry->cu_hdd_sync);
        }
    }

    if (PointerIsValid(stat_list)) {
        list_free(stat_list);
    }

    PG_RETURN_INT64(result);
}

/**
 * @Description:  get cu stat hdd async read
 * @return  datum
 */
Datum pg_stat_get_cu_hdd_asyn(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    int64 result = 0;
    List* stat_list = NIL;
    ListCell* stat_cell = NULL;
    PgStat_StatTabKey tabkey;
    PgStat_StatTabEntry* tabentry = NULL;
    pg_stat_get_stat_list(&stat_list, &tabkey.statFlag, relid);

    foreach (stat_cell, stat_list) {
        tabkey.tableid = lfirst_oid(stat_cell);
        tabentry = pgstat_fetch_stat_tabentry(&tabkey);

        if (PointerIsValid(tabentry)) {
            result += (int64)(tabentry->cu_hdd_asyn);
        }
    }

    if (PointerIsValid(stat_list)) {
        list_free(stat_list);
    }

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_last_data_changed_time(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    TimestampTz result = 0;
    PgStat_StatTabKey tabkey;
    PgStat_StatTabEntry* tabentry = NULL;

    tabkey.statFlag = STATFLG_RELATION;
    tabkey.tableid = relid;
    tabentry = pgstat_fetch_stat_tabentry(&tabkey);

    if (!PointerIsValid(tabentry))
        result = 0;
    else
        result = tabentry->data_changed_timestamp;

    if (result == 0)
        PG_RETURN_NULL();
    else
        PG_RETURN_TIMESTAMPTZ(result);
}

Datum pg_stat_set_last_data_changed_time(PG_FUNCTION_ARGS)
{
    if (!IS_PGXC_COORDINATOR && !IS_SINGLE_NODE)
        PG_RETURN_VOID();

    Oid relid = PG_GETARG_OID(0);

    Relation rel = heap_open(relid, AccessShareLock);

    pgstat_report_data_changed(relid, STATFLG_RELATION, rel->rd_rel->relisshared);

    heap_close(rel, AccessShareLock);

    PG_RETURN_VOID();
}

Datum pg_stat_get_last_vacuum_time(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    PgStat_StatTabKey tabkey;
    PgStat_StatTabEntry* tabentry = NULL;
    TimestampTz result = 0;

    tabkey.statFlag = InvalidOid;
    tabkey.tableid = relid;
    tabentry = pgstat_fetch_stat_tabentry(&tabkey);

    if (tabentry != NULL)
        result = tabentry->vacuum_timestamp;

    if (result == 0)
        PG_RETURN_NULL();
    else
        PG_RETURN_TIMESTAMPTZ(result);
}

Datum pg_stat_get_last_autovacuum_time(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    PgStat_StatTabKey tabkey;
    PgStat_StatTabEntry* tabentry = NULL;
    TimestampTz result = 0;

    if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
        result = pgxc_last_autovac_time(relid, "pg_stat_get_last_autovacuum_time");

    tabkey.statFlag = InvalidOid;
    tabkey.tableid = relid;
    tabentry = pgstat_fetch_stat_tabentry(&tabkey);

    /* get lastest autovacuum timestamp from local timestamp and remote timestamp */
    if (tabentry && timestamptz_cmp_internal(tabentry->autovac_vacuum_timestamp, result) > 0)
        result = tabentry->autovac_vacuum_timestamp;

    if (result == 0)
        PG_RETURN_NULL();
    else
        PG_RETURN_TIMESTAMPTZ(result);
}

Datum pg_stat_get_last_analyze_time(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    PgStat_StatTabKey tabkey;
    PgStat_StatTabEntry* tabentry = NULL;
    TimestampTz result = 0;

    tabkey.statFlag = InvalidOid;
    tabkey.tableid = relid;
    tabentry = pgstat_fetch_stat_tabentry(&tabkey);

    if (tabentry != NULL)
        result = tabentry->analyze_timestamp;

    if (result == 0)
        PG_RETURN_NULL();
    else
        PG_RETURN_TIMESTAMPTZ(result);
}

Datum pg_stat_get_last_autoanalyze_time(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    PgStat_StatTabKey tabkey;
    PgStat_StatTabEntry* tabentry = NULL;
    TimestampTz result = 0;

    if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
        result = pgxc_last_autovac_time(relid, "pg_stat_get_last_autoanalyze_time");

    tabkey.statFlag = InvalidOid;
    tabkey.tableid = relid;
    tabentry = pgstat_fetch_stat_tabentry(&tabkey);

    if (tabentry && timestamptz_cmp_internal(tabentry->autovac_analyze_timestamp, result) > 0)
        result = tabentry->autovac_analyze_timestamp;

    if (result == 0)
        PG_RETURN_NULL();
    else
        PG_RETURN_TIMESTAMPTZ(result);
}

Datum pg_stat_get_vacuum_count(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    PgStat_StatTabKey tabkey;
    PgStat_StatTabEntry* tabentry = NULL;
    int64 result = 0;

    tabkey.statFlag = InvalidOid;
    tabkey.tableid = relid;
    tabentry = pgstat_fetch_stat_tabentry(&tabkey);

    if (tabentry != NULL)
        result += (int64)(tabentry->vacuum_count);

    PG_RETURN_INT64(result);
}

const dw_view_col_t segment_space_info_view[SEGMENT_SPACE_INFO_VIEW_COL_NUM] = {
    {"node_name", TEXTOID, NULL},
    {"extent_size", OIDOID, NULL},
    {"forknum", INT4OID, NULL},
    {"total_blocks", OIDOID, NULL},
    {"meta_data_blocks", OIDOID, NULL},
    {"used_data_blocks", OIDOID, NULL},
    {"utilization", FLOAT4OID, NULL},
    {"high_water_mark", OIDOID, NULL},
};

static TupleDesc create_segment_space_info_tupdesc()
{
    int colnum = sizeof(segment_space_info_view) / sizeof(dw_view_col_t);
    SegmentCheck(colnum == SEGMENT_SPACE_INFO_VIEW_COL_NUM);

    TupleDesc tupdesc = CreateTemplateTupleDesc(colnum, false);
    for (int i = 0; i < colnum; i++) {
        TupleDescInitEntry(tupdesc, (AttrNumber)i + 1, segment_space_info_view[i].name,
                           segment_space_info_view[i].data_type, -1, 0);
    }
    BlessTupleDesc(tupdesc);

    return tupdesc;
}

static Datum pg_stat_segment_space_info_internal(Oid spaceid, Oid dbid, PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx = NULL;

    if (SRF_IS_FIRSTCALL()) {
        funcctx = SRF_FIRSTCALL_INIT();
        SegSpace *spc = spc_open(spaceid, dbid, false);

        // segment space does not exist
        if (spc == NULL || spc_status(spc) == SpaceDataFileStatus::EMPTY) {
            ereport(INFO, (errmsg("Segment is not initialized in current database")));
            SRF_RETURN_DONE(funcctx);
        }
        MemoryContext oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        funcctx->tuple_desc = create_segment_space_info_tupdesc();
        funcctx->user_fctx = (void *)spc;
        funcctx->max_calls = EXTENT_TYPES * (SEGMENT_MAX_FORKNUM + 1);

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->call_cntr < funcctx->max_calls) {
        SegSpace *spc = (SegSpace *)funcctx->user_fctx;

        while (funcctx->call_cntr < funcctx->max_calls) {
            // each line report one extent group
            int groupId = funcctx->call_cntr / (SEGMENT_MAX_FORKNUM + 1);
            ForkNumber forknum = funcctx->call_cntr % (SEGMENT_MAX_FORKNUM + 1);
            SegmentSpaceStat stat = spc_storage_stat(spc, groupId, forknum);

            if (stat.total_blocks == 0) {
                // empty segment
                funcctx->call_cntr++;
                continue;
            }
            /* Values available to all callers */
            Datum values[SEGMENT_SPACE_INFO_VIEW_COL_NUM + 1];
            int i = 0;
            values[i++] = dw_get_node_name();
            values[i++] = ObjectIdGetDatum(stat.extent_size);
            values[i++] = Int32GetDatum(stat.forknum);
            values[i++] = ObjectIdGetDatum(stat.total_blocks);
            values[i++] = ObjectIdGetDatum(stat.meta_data_blocks);
            values[i++] = ObjectIdGetDatum(stat.used_data_blocks);
            values[i++] = Float4GetDatum(stat.utilization);
            values[i++] = ObjectIdGetDatum(stat.high_water_mark);

            bool nulls[SEGMENT_SPACE_INFO_VIEW_COL_NUM];
            for (int i = 0; i < SEGMENT_SPACE_INFO_VIEW_COL_NUM; i++) {
                nulls[i] = false;
            }

            HeapTuple tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
            if (tuple != NULL) {
                SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
            }
        }
    }

    SRF_RETURN_DONE(funcctx);
}

Oid get_tablespace_oid_by_name(const char *tablespacename)
{
    Relation rel = heap_open(TableSpaceRelationId, AccessShareLock);
    ScanKeyData scanKey;
    ScanKeyInit(
        &scanKey, Anum_pg_tablespace_spcname, BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum(tablespacename));
    TableScanDesc scandesc = tableam_scan_begin(rel, SnapshotNow, 1, &scanKey);
    HeapTuple tuple = (HeapTuple) tableam_scan_getnexttuple(scandesc, ForwardScanDirection);
    if (!HeapTupleIsValid(tuple)) {
        tableam_scan_end(scandesc);
        heap_close(rel, AccessShareLock);
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("Tablespace \"%s\" does not exist.", tablespacename)));
        return InvalidOid;
    }

    Oid spaceid = HeapTupleGetOid(tuple);
    tableam_scan_end(scandesc);
    heap_close(rel, AccessShareLock);

    return spaceid;
}

Oid get_database_oid_by_name(const char *dbname)
{
    Relation rel = heap_open(DatabaseRelationId, AccessShareLock);
    ScanKeyData scanKey;
    SysScanDesc scan;
    HeapTuple tuple;
    Oid dbOid;

    ScanKeyInit(&scanKey, Anum_pg_database_datname, BTEqualStrategyNumber, F_NAMEEQ, NameGetDatum(dbname));
    scan = systable_beginscan(rel, DatabaseNameIndexId, true, NULL, 1, &scanKey);
    tuple = systable_getnext(scan);
    if (!HeapTupleIsValid(tuple)) {
        /* definitely no database of that name */
        systable_endscan(scan);
        heap_close(rel, AccessShareLock);
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("Database \"%s\" does not exist.", dbname)));
        return InvalidOid;
    }
    dbOid = HeapTupleGetOid(tuple);
    systable_endscan(scan);
    heap_close(rel, AccessShareLock);

    return dbOid;
}

Datum local_segment_space_info(PG_FUNCTION_ARGS)
{
    char *tablespacename = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char *dbname = text_to_cstring(PG_GETARG_TEXT_PP(1));
    Oid spaceid = get_tablespace_oid_by_name(tablespacename);
    Oid dbid = get_database_oid_by_name(dbname);
    return pg_stat_segment_space_info_internal(spaceid, dbid, fcinfo);
}

TableDistributionInfo* get_remote_segment_space_info(TupleDesc tuple_desc, char *tablespacename, char *dbname)
{
    StringInfoData buf;
    TableDistributionInfo* distribuion_info = NULL;

    /* the memory palloced here should be free outside where it was called. */
    distribuion_info = (TableDistributionInfo*)palloc0(sizeof(TableDistributionInfo));

    initStringInfo(&buf);

    appendStringInfo(&buf, "select * from local_segment_space_info(\'%s\', \'%s\')", tablespacename, dbname);

    /* send sql and parallel fetch distribution info from all data nodes */
    distribuion_info->state = RemoteFunctionResultHandler(buf.data, NULL, NULL, true, EXEC_ON_ALL_NODES, true);
    distribuion_info->slot = MakeSingleTupleTableSlot(tuple_desc);

    return distribuion_info;
}

Datum remote_segment_space_info(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_MULTIPLE_NODES
    FuncCallContext* funcctx = NULL;
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        errmsg("unsupported view in single node mode.")));

    SRF_RETURN_DONE(funcctx);
#else
    FuncCallContext* funcctx = NULL;
    HeapTuple tuple = NULL;
    Datum values[SEGMENT_SPACE_INFO_VIEW_COL_NUM];
    bool nulls[SEGMENT_SPACE_INFO_VIEW_COL_NUM] = {false};

    if (SRF_IS_FIRSTCALL()) {
        char *tablespacename = text_to_cstring(PG_GETARG_TEXT_PP(0));
        char *dbname = text_to_cstring(PG_GETARG_TEXT_PP(1));
        /* check tablespace exiting and ACL before distributing to DNs */
        get_tablespace_oid_by_name(tablespacename);
        get_database_oid_by_name(dbname);

        funcctx = SRF_FIRSTCALL_INIT();
        /* switch the memory context to allocate user_fctx */
        MemoryContext oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        funcctx->max_calls = u_sess->pgxc_cxt.NumDataNodes + u_sess->pgxc_cxt.NumCoords;
        funcctx->tuple_desc = create_segment_space_info_tupdesc();
        funcctx->user_fctx = get_remote_segment_space_info(funcctx->tuple_desc, tablespacename, dbname);

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->user_fctx) {
        Tuplestorestate* tupstore = ((TableDistributionInfo*)funcctx->user_fctx)->state->tupstore;
        TupleTableSlot* slot = ((TableDistributionInfo*)funcctx->user_fctx)->slot;

        if (!tuplestore_gettupleslot(tupstore, true, false, slot)) {
            FreeParallelFunctionState(((TableDistributionInfo*)funcctx->user_fctx)->state);
            ExecDropSingleTupleTableSlot(slot);
            pfree_ext(funcctx->user_fctx);
            funcctx->user_fctx = NULL;
            SRF_RETURN_DONE(funcctx);
        }

        for (int i = 0; i < PAGEWRITER_VIEW_COL_NUM; i++) {
            values[i] = tableam_tslot_getattr(slot, (i + 1), &nulls[i]);
        }
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        ExecClearTuple(slot);

        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }

    SRF_RETURN_DONE(funcctx);
#endif
}

typedef struct SegmentExtentUsageCtx {
    uint32 cnt;
    ExtentInversePointer *iptrs;
    BlockNumber *extents;
} SegmentExtentUsageCtx;

Datum pg_stat_segment_extent_usage(PG_FUNCTION_ARGS)
{
    Oid spaceid = PG_GETARG_OID(0);
    Oid dbid = PG_GETARG_OID(1);
    uint32 extent_type = PG_GETARG_UINT32(2);
    ForkNumber forknum = PG_GETARG_INT32(3);
    if (!ExtentTypeIsValid(extent_type)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmodule(MOD_SEGMENT_PAGE),
                        errmsg("The parameter extent_type is not valid"), errhint("extent_type should be in [1, 4]")));
    }
    if (forknum < 0 || forknum > MAX_FORKNUM) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmodule(MOD_SEGMENT_PAGE),
                        errmsg("The forknumber is invalid"), errdetail("forknum should be in [0, %d]", MAX_FORKNUM)));
    }
    FuncCallContext *funcctx = NULL;

    if (forknum > SEGMENT_MAX_FORKNUM) {
        SRF_RETURN_DONE(funcctx);
    }

    if (SRF_IS_FIRSTCALL()) {
        funcctx = SRF_FIRSTCALL_INIT();
        SegSpace *spc = spc_open(spaceid, dbid, false);

        // segment space does not exist
        if (spc == NULL || spc_status(spc) == SpaceDataFileStatus::EMPTY) {
            ereport(INFO, (errmsg("Segment is not initialized in current database")));
            SRF_RETURN_DONE(funcctx);
        }

        SegExtentGroup *seg = &spc->extent_group[EXTENT_TYPE_TO_GROUPID(extent_type)][forknum];

        MemoryContext oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        TupleDesc tupdesc = CreateTemplateTupleDesc(SEGMENT_SPACE_EXTENT_USAGE_COL_NUM, false);
        TupleDescInitEntry(tupdesc, (AttrNumber)1, "start_block", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "extent_size", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "usage_type", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)4, "ower_location", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)5, "special_data", OIDOID, -1, 0);

        BlessTupleDesc(tupdesc);

        SegmentExtentUsageCtx *usage_ctx = (SegmentExtentUsageCtx *) palloc(sizeof(SegmentExtentUsageCtx));
        GetAllInversePointer(seg, &funcctx->max_calls, &usage_ctx->iptrs, &usage_ctx->extents);

        funcctx->tuple_desc = tupdesc;
        funcctx->user_fctx = (void *)usage_ctx;

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->call_cntr < funcctx->max_calls) {
        SegmentExtentUsageCtx *usage_ctx = (SegmentExtentUsageCtx *)funcctx->user_fctx;
        int k = funcctx->call_cntr;

        Datum values[SEGMENT_SPACE_EXTENT_USAGE_COL_NUM + 1];
        int i = 0;
        values[i++] = ObjectIdGetDatum(usage_ctx->extents[k]);
        values[i++] = ObjectIdGetDatum(EXTENT_TYPE_TO_SIZE(extent_type));
        values[i++] = CStringGetTextDatum(GetExtentUsageName(usage_ctx->iptrs[k]));
        values[i++] = ObjectIdGetDatum(usage_ctx->iptrs[k].owner);
        values[i++] = ObjectIdGetDatum(SPC_INVRSPTR_GET_SPECIAL_DATA(usage_ctx->iptrs[k]));

        bool nulls[SEGMENT_SPACE_EXTENT_USAGE_COL_NUM];
        for (int i = 0; i < SEGMENT_SPACE_EXTENT_USAGE_COL_NUM; i++) {
            nulls[i] = false;
        }

        HeapTuple tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        if (tuple != NULL) {
            SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
        }
    }

    SegmentExtentUsageCtx *usage_ctx = (SegmentExtentUsageCtx *)funcctx->user_fctx;
    if (usage_ctx->extents) {
        pfree(usage_ctx->extents);
    }
    if (usage_ctx->iptrs) {
        pfree(usage_ctx->iptrs);
    }

    SRF_RETURN_DONE(funcctx);
}

char* g_remainExtentNames[INVALID_REMAIN_TYPE + 1] = {
    "ALLOC_SEGMENT",
    "DROP_SEGMENT",
    "SHRINK_EXTENT",
    "INVALID_REMAIN_TYPE"
};

const char* XlogGetRemainExtentTypeName(StatRemainExtentType remainExtentType)
{
    return g_remainExtentNames[remainExtentType];
}

typedef struct RemainSegsCtx
{
    ExtentTag* remainSegsBuf;
    uint32 remainSegsNum;
    XLogRecPtr remainStartPoint;
} RemainSegsCtx;

static void ReadRemainSegsFileForFunc(RemainSegsCtx *remainSegsCtx)
{
    bool isFileExist = IsRemainSegsFileExist();
    if (!isFileExist) {
        return;
    }

    int fd = BasicOpenFile(XLOG_REMAIN_SEGS_FILE_PATH, O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not open remain segs file \"%s\": %m",
                XLOG_REMAIN_SEGS_FILE_PATH)));
    }

    uint32 magicNumber = 0;
    uint32_t readLen = read(fd, &magicNumber, sizeof(uint32));
    if (readLen < sizeof(uint32) || (magicNumber != REMAIN_SEG_MAGIC_NUM)) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("read len %u from remain segs file \"%s\""
                "is less than %lu: %m.", readLen, XLOG_REMAIN_SEGS_FILE_PATH, sizeof(uint32))));
    }

    XLogRecPtr oldStartPoint = InvalidXLogRecPtr;
    readLen = read(fd, &oldStartPoint, sizeof(XLogRecPtr));
    if (readLen < sizeof(XLogRecPtr)) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("read len %u from remain segs file \"%s\""
                " is less than %lu: %m.", readLen, XLOG_REMAIN_SEGS_FILE_PATH, sizeof(XLogRecPtr))));
    }

    uint32 remainEntryNum = 0;
    readLen = read(fd, &remainEntryNum, sizeof(uint32));
    if (readLen < sizeof(uint32)) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("read len %u from remain segs file \"%s\""
                " is less than %lu: %m.", readLen, XLOG_REMAIN_SEGS_FILE_PATH, sizeof(uint32))));
    }
    remainSegsCtx->remainSegsNum = remainEntryNum;
    remainSegsCtx->remainStartPoint = oldStartPoint;

    if (remainEntryNum != 0) {
        uint32 extentTagBufLen = sizeof(ExtentTag) * remainEntryNum;
        remainSegsCtx->remainSegsBuf = (ExtentTag *)palloc0(extentTagBufLen);

        readLen = read(fd, remainSegsCtx->remainSegsBuf, extentTagBufLen);
        if (readLen < extentTagBufLen) {
            pfree(remainSegsCtx->remainSegsBuf);
            remainSegsCtx->remainSegsBuf = NULL;
            ereport(ERROR, (errcode_for_file_access(), errmsg("read len %u from remain segs file \"%s\""
                    " is less than %u: %m.", readLen, XLOG_REMAIN_SEGS_FILE_PATH, extentTagBufLen)));
        }
    }

    readLen = read(fd, &magicNumber, sizeof(uint32));
    if (readLen < sizeof(uint32) || (magicNumber != REMAIN_SEG_MAGIC_NUM)) {
        pfree(remainSegsCtx->remainSegsBuf);
        remainSegsCtx->remainSegsBuf = NULL;
        ereport(ERROR, (errcode_for_file_access(), errmsg("read len %u from remain segs file \"%s\""
                "is less than %lu: %m.", readLen, XLOG_REMAIN_SEGS_FILE_PATH, sizeof(uint32))));
    }

    if (close(fd)) {
        pfree(remainSegsCtx->remainSegsBuf);
        remainSegsCtx->remainSegsBuf = NULL;
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not close remain segs file \"%s\": %m", 
                XLOG_REMAIN_SEGS_FILE_PATH)));
    }
}

Datum pg_stat_remain_segment_info(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx = NULL;
    if (SRF_IS_FIRSTCALL()) {
        funcctx = SRF_FIRSTCALL_INIT();

        MemoryContext oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        TupleDesc tupdesc = CreateTemplateTupleDesc(REMAIN_SEGS_INFO_COL_NUM, true);
        TupleDescInitEntry(tupdesc, (AttrNumber)1, "space_id", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "db_id", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "block_id", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)4, "type", TEXTOID, -1, 0);

        BlessTupleDesc(tupdesc);

        funcctx->tuple_desc = tupdesc;
        RemainSegsCtx* remainSegsCtx = (RemainSegsCtx *)palloc(sizeof(RemainSegsCtx));
        remainSegsCtx->remainSegsBuf = NULL;
        remainSegsCtx->remainSegsNum = 0;
        
        ReadRemainSegsFileForFunc(remainSegsCtx);
        funcctx->max_calls += remainSegsCtx->remainSegsNum;
        
        funcctx->user_fctx = (void *)remainSegsCtx;

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->call_cntr < funcctx->max_calls) {
        uint32 idx = funcctx->call_cntr;
        RemainSegsCtx* remainSegsCtx  = (RemainSegsCtx *)funcctx->user_fctx;

        Datum values[REMAIN_SEGS_INFO_COL_NUM];
        int i = 0;
            
        values[i++] = ObjectIdGetDatum(remainSegsCtx->remainSegsBuf[idx].remainExtentHashTag.rnode.spcNode);
        values[i++] = ObjectIdGetDatum(remainSegsCtx->remainSegsBuf[idx].remainExtentHashTag.rnode.dbNode);
        values[i++] = ObjectIdGetDatum(remainSegsCtx->remainSegsBuf[idx].remainExtentHashTag.rnode.relNode);
        values[i++] = CStringGetTextDatum(g_remainExtentNames[remainSegsCtx->remainSegsBuf[idx].remainExtentType]);

        bool nulls[REMAIN_SEGS_INFO_COL_NUM];
        for (int i = 0; i < REMAIN_SEGS_INFO_COL_NUM; i++) {
            nulls[i] = false;
        }

        HeapTuple tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        if (tuple != NULL) {
            SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
        }
    }

    RemainSegsCtx *remainCtx = (RemainSegsCtx *)funcctx->user_fctx;
    if (remainCtx->remainSegsBuf != NULL) {
        pfree(remainCtx->remainSegsBuf);
        remainCtx->remainSegsBuf = NULL;
    }
    pfree(remainCtx);
    funcctx->user_fctx = NULL;
    
    SRF_RETURN_DONE(funcctx);
}

static int pgCheckRemainSegment(const RemainSegsCtx* remainSegsCtx, Oid spaceId, Oid dbId, Oid segmentId)
{
    for (uint32 i = 0; i < remainSegsCtx->remainSegsNum; i++) {
        RelFileNode* relfilenode = &(remainSegsCtx->remainSegsBuf[i].remainExtentHashTag.rnode);
        if (relfilenode->spcNode == spaceId && relfilenode->dbNode == dbId && relfilenode->relNode == segmentId) {
            return i;
        }
    }

    return -1;
}

static void pgDoFreeRemainSegment(const RemainSegsCtx* remainSegsCtx, uint32 segIdx)
{
    ExtentTag* extentTag = &remainSegsCtx->remainSegsBuf[segIdx];
    StatRemainExtentType remainExtentType = remainSegsCtx->remainSegsBuf[segIdx].remainExtentType;

    SegSpace *spc = spc_open(extentTag->remainExtentHashTag.rnode.spcNode,
                             extentTag->remainExtentHashTag.rnode.dbNode, false);
    if (spc == NULL) {
        ereport(ERROR, (errmodule(MOD_SEGMENT_PAGE), errmsg("Failed to open segspace[%u, %u]",
            extentTag->remainExtentHashTag.rnode.spcNode, extentTag->remainExtentHashTag.rnode.dbNode)));
        return;
    }

    if (remainExtentType == ALLOC_SEGMENT || remainExtentType == DROP_SEGMENT) {
        SMgrRelation srel = smgropen(extentTag->remainExtentHashTag.rnode, InvalidBackendId);
        if (smgrexists(srel, extentTag->forkNum)) {
            smgrdounlink(srel, false);
        }
        smgrclose(srel);
    } else if (remainExtentType == SHRINK_EXTENT) {
        if (extentTag->remainExtentHashTag.extentType == EXTENT_INVALID || extentTag->forkNum == InvalidForkNumber) {
            ereport(ERROR, (errmodule(MOD_SEGMENT_PAGE), errmsg("Remain Seg's extent type %u or forkNum %u is wrong.",
                extentTag->remainExtentHashTag.extentType, extentTag->forkNum)));
            return;
        }
        XLogAtomicOpStart();
        int egid = EXTENT_TYPE_TO_GROUPID(extentTag->remainExtentHashTag.extentType);
        SegExtentGroup *seg = &spc->extent_group[egid][extentTag->forkNum];
        eg_free_extent(seg, extentTag->remainExtentHashTag.rnode.relNode);
        XLogAtomicOpCommit();
    } else {
        ereport(ERROR, (errmodule(MOD_SEGMENT_PAGE), errmsg("Remain Segment type %u is wrong.", remainExtentType)));
    }
}

static void pgRemoveRemainSegInfo(RemainSegsCtx* remainSegsCtx, uint32 segIdx)
{
    remainSegsCtx->remainSegsBuf[segIdx] = remainSegsCtx->remainSegsBuf[remainSegsCtx->remainSegsNum - 1];
    (remainSegsCtx->remainSegsNum)--;
}

static uint32 pgGetRemainContentLen(RemainSegsCtx* remainSegsCtx)
{
    uint32 contentLen = 0;
    contentLen += sizeof(const uint32);
    contentLen += sizeof(XLogRecPtr);
    contentLen += sizeof(uint32);
    contentLen += remainSegsCtx->remainSegsNum * sizeof(ExtentTag);
    contentLen += sizeof(const uint32);
    return contentLen;
}

static void pgMakeUpRemainSegsContent(const RemainSegsCtx* remainSegsCtx, char* contentBuffer)
{
    uint32 usedLen = 0;
    const uint32 magicNum = REMAIN_SEG_MAGIC_NUM;
    errno_t errc = memcpy_s(contentBuffer, sizeof(const uint32), &magicNum, sizeof(const uint32));
    securec_check(errc, "\0", "\0");
    usedLen += sizeof(uint32);

    errc = memcpy_s(contentBuffer + usedLen, sizeof(XLogRecPtr), &remainSegsCtx->remainStartPoint, sizeof(XLogRecPtr));
    securec_check(errc, "\0", "\0");
    usedLen += sizeof(XLogRecPtr);

    errc = memcpy_s(contentBuffer + usedLen, sizeof(uint32), &remainSegsCtx->remainSegsNum, sizeof(uint32));
    securec_check(errc, "\0", "\0");
    usedLen += sizeof(uint32);

    if (remainSegsCtx->remainSegsNum) {
        errc = memcpy_s(contentBuffer + usedLen, remainSegsCtx->remainSegsNum * sizeof(ExtentTag),
                        remainSegsCtx->remainSegsBuf, remainSegsCtx->remainSegsNum * sizeof(ExtentTag));
        securec_check(errc, "\0", "\0");
        usedLen += remainSegsCtx->remainSegsNum * sizeof(ExtentTag);
    }
    
    errc = memcpy_s(contentBuffer + usedLen, sizeof(const uint32), &magicNum, sizeof(const uint32));
    securec_check(errc, "\0", "\0");
}

static void pgOutputRemainInfoToFile(RemainSegsCtx* remainSegsCtx)
{
    uint32 contentLen = pgGetRemainContentLen(remainSegsCtx);
    pg_crc32c crc;
    char* contentBuffer = (char *)palloc_huge(CurrentMemoryContext, (contentLen + sizeof(pg_crc32c)));

    pgMakeUpRemainSegsContent(remainSegsCtx, contentBuffer);

    /* Contents are protected with a CRC */
    INIT_CRC32C(crc);
    COMP_CRC32C(crc, contentBuffer, contentLen);
    FIN_CRC32C(crc);
    *(pg_crc32c *)(contentBuffer + contentLen) = crc;
    
    int fd = BasicOpenFile(XLOG_REMAIN_SEGS_FILE_PATH, O_RDWR | O_CREAT | O_TRUNC | PG_BINARY, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        pfree_ext(contentBuffer);
        ereport(ERROR, (errcode_for_file_access(), 
                errmsg("could not create remain segs file \"%s\": %m", XLOG_REMAIN_SEGS_FILE_PATH)));
        return;
    }

    WriteRemainSegsFile(fd, contentBuffer, contentLen + sizeof(pg_crc32c));

    if (close(fd)) {
        pfree_ext(contentBuffer);
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not close remain segs file \"%s\": %m",
                XLOG_REMAIN_SEGS_FILE_PATH)));
        return;
    }

    pfree_ext(contentBuffer);
}

Datum pg_free_remain_segment(PG_FUNCTION_ARGS)
{
    Oid spaceId = PG_GETARG_OID(0);
    Oid dbId = PG_GETARG_OID(1);
    Oid segmentId = PG_GETARG_OID(2);

    RemainSegsCtx* remainSegsCtx = (RemainSegsCtx *)palloc(sizeof(RemainSegsCtx));
    remainSegsCtx->remainSegsBuf = NULL;
    remainSegsCtx->remainSegsNum = 0;
    
    ReadRemainSegsFileForFunc(remainSegsCtx);

    int segIdx = pgCheckRemainSegment(remainSegsCtx, spaceId, dbId, segmentId);
    if (segIdx >= 0 && (uint32)segIdx < remainSegsCtx->remainSegsNum) {
        pgDoFreeRemainSegment(remainSegsCtx, segIdx);

        pgRemoveRemainSegInfo(remainSegsCtx, segIdx);

        pgOutputRemainInfoToFile(remainSegsCtx);
    } else {
        ereport(ERROR, (errmsg("Input segment [%u, %u, %u] is not remained segment!", spaceId, dbId, segmentId)));
     }

    if (remainSegsCtx->remainSegsBuf != NULL) {
        pfree(remainSegsCtx->remainSegsBuf);
        remainSegsCtx->remainSegsBuf = NULL;
    }
    pfree(remainSegsCtx);

    return 0;
}

/* get max autovac count from multi-node */
static void StrategyFuncMaxCount(ParallelFunctionState* state)
{
    TupleTableSlot* slot = NULL;
    int64* result = NULL;

    state->resultset = (int64*)palloc(sizeof(int64));
    result = (int64*)state->resultset;
    *result = 0;

    Assert(state && state->tupstore);
    if (NULL == state->tupdesc) {
        /*
         * if there is only one coordinator, there is no remote coordinator, tupdesc
         * will be uninitialized, we just do return here
         */
        return;
    }

    slot = MakeSingleTupleTableSlot(state->tupdesc);
    for (;;) {
        bool isnull = false;
        int64 tmp = 0;

        if (!tuplestore_gettupleslot(state->tupstore, true, false, slot))
            break;

        tmp = DatumGetInt64(tableam_tslot_getattr(slot, 1, &isnull));
        if (!isnull && tmp > *result)
            *result = tmp;

        ExecClearTuple(slot);
    }
}

static int64 pgxc_get_autovac_count(Oid relOid, char* funcname)
{
    Oid nspid = get_rel_namespace(relOid);
    char* relname = NULL;
    char* nspname = NULL;
    ExecNodes* exec_nodes = NULL;
    int64 result = 0;
    StringInfoData buf;
    ParallelFunctionState* state = NULL;

    relname = get_rel_name(relOid);
    nspname = get_namespace_name(nspid);
    /* it has been dropped */
    if ((relname == NULL) || (nspname == NULL))
        return 0;

    exec_nodes = (ExecNodes*)makeNode(ExecNodes);
    exec_nodes->accesstype = RELATION_ACCESS_READ;
    exec_nodes->baselocatortype = LOCATOR_TYPE_HASH;
    exec_nodes->nodeList = GetAllCoordNodes();
    exec_nodes->primarynodelist = NIL;
    exec_nodes->en_expr = NULL;

    relname = repairObjectName(relname);
    nspname = repairObjectName(nspname);

    initStringInfo(&buf);
    appendStringInfo(&buf,
        "SELECT pg_catalog.%s(c.oid) FROM pg_class c "
        "INNER JOIN pg_namespace n on c.relnamespace = n.oid "
        "WHERE n.nspname='%s' AND relname = '%s' ",
        funcname,
        nspname,
        relname);
    pfree(relname);
    pfree(nspname);

    state = RemoteFunctionResultHandler(buf.data, exec_nodes, StrategyFuncMaxCount, true, EXEC_ON_COORDS, true);
    result = *((int64*)state->resultset);

    FreeParallelFunctionState(state);
    pfree(buf.data);
    list_free(exec_nodes->nodeList);
    pfree(exec_nodes);

    return result;
}

Datum pg_stat_get_autovacuum_count(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    PgStat_StatTabKey tabkey;
    PgStat_StatTabEntry* tabentry = NULL;
    int64 result = 0;

    if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
        result = pgxc_get_autovac_count(relid, "pg_stat_get_autovacuum_count");

    tabkey.statFlag = InvalidOid;
    tabkey.tableid = relid;
    tabentry = pgstat_fetch_stat_tabentry(&tabkey);

    if (tabentry != NULL)
        result = Max(result, (int64)(tabentry->autovac_vacuum_count));

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_analyze_count(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    PgStat_StatTabKey tabkey;
    PgStat_StatTabEntry* tabentry = NULL;
    int64 result = 0;

    tabkey.statFlag = InvalidOid;
    tabkey.tableid = relid;
    tabentry = pgstat_fetch_stat_tabentry(&tabkey);

    if ((tabentry = pgstat_fetch_stat_tabentry(&tabkey)))
        result += (int64)(tabentry->analyze_count);

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_autoanalyze_count(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    PgStat_StatTabKey tabkey;
    PgStat_StatTabEntry* tabentry = NULL;
    int64 result = 0;

    if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
        result = pgxc_get_autovac_count(relid, "pg_stat_get_autoanalyze_count");

    tabkey.statFlag = InvalidOid;
    tabkey.tableid = relid;
    tabentry = pgstat_fetch_stat_tabentry(&tabkey);

    if (tabentry != NULL)
        result = Max(result, (int64)(tabentry->autovac_analyze_count));

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_function_calls(PG_FUNCTION_ARGS)
{
    Oid funcid = PG_GETARG_OID(0);
    PgStat_StatFuncEntry* funcentry = NULL;

    if ((funcentry = pgstat_fetch_stat_funcentry(funcid)) == NULL)
        PG_RETURN_NULL();
    PG_RETURN_INT64(funcentry->f_numcalls);
}

Datum pg_stat_get_function_total_time(PG_FUNCTION_ARGS)
{
    Oid funcid = PG_GETARG_OID(0);
    PgStat_StatFuncEntry* funcentry = NULL;

    if ((funcentry = pgstat_fetch_stat_funcentry(funcid)) == NULL)
        PG_RETURN_NULL();
    /* convert counter from microsec to millisec for display */
    PG_RETURN_FLOAT8(((double)funcentry->f_total_time) / 1000.0);
}

Datum pg_stat_get_function_self_time(PG_FUNCTION_ARGS)
{
    Oid funcid = PG_GETARG_OID(0);
    PgStat_StatFuncEntry* funcentry = NULL;

    if ((funcentry = pgstat_fetch_stat_funcentry(funcid)) == NULL)
        PG_RETURN_NULL();
    /* convert counter from microsec to millisec for display */
    PG_RETURN_FLOAT8(((double)funcentry->f_self_time) / 1000.0);
}

Datum pg_stat_get_backend_idset(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;
    int* fctx = NULL;
    int32 result;
    uint32 numbackends = 0;

    /* stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL()) {
        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        fctx = (int*)MemoryContextAlloc(funcctx->multi_call_memory_ctx, 2 * sizeof(int));
        funcctx->user_fctx = fctx;

        fctx[0] = 0;
        /* Only numbackends is required. */
        numbackends = gs_stat_read_current_status(NULL, NULL, NULL);
        fctx[1] = (int)numbackends;
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();
    fctx = (int*)funcctx->user_fctx;

    fctx[0] += 1;
    result = fctx[0];

    if (result <= fctx[1]) {
        /* do when there is more left to send */
        SRF_RETURN_NEXT(funcctx, Int32GetDatum(result));
    } else {
        /* do when there is no more left */
        SRF_RETURN_DONE(funcctx);
    }
}

Datum pg_stat_get_activity_with_conninfo(PG_FUNCTION_ARGS)
{
    return pg_stat_get_activity_helper(fcinfo, true);
}

Datum pg_stat_get_activity(PG_FUNCTION_ARGS)
{
    return pg_stat_get_activity_helper(fcinfo, false);
}

Datum pg_stat_get_activity_helper(PG_FUNCTION_ARGS, bool has_conninfo)
{
    const int ATT_COUNT = has_conninfo ? 22 : 21;
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    MemoryContext oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);


    TupleDesc tupdesc = CreateTemplateTupleDesc(ATT_COUNT, false);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_1, "datid", OIDOID, -1, 0);
    /* This should have been called 'pid';  can't change it. 2011-06-11 */
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_2, "pid", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_3, "sessionid", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_4, "usesysid", OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_5, "application_name", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_6, "state", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_7, "query", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_8, "waiting", BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_9, "xact_start", TIMESTAMPTZOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_10, "query_start", TIMESTAMPTZOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_11, "backend_start", TIMESTAMPTZOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_12, "state_change", TIMESTAMPTZOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_13, "client_addr", INETOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_14, "client_hostname", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_15, "client_port", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_16, "enqueue", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_17, "query_id", INT8OID, -1, 0);
    if (has_conninfo) {
        TupleDescInitEntry(tupdesc, (AttrNumber)ARG_18, "connection_info", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ARG_19, "srespool", NAMEOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ARG_20, "global_sessionid", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ARG_21, "unique_sql_id", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ARG_22, "trace_id", TEXTOID, -1, 0);
    } else {
        TupleDescInitEntry(tupdesc, (AttrNumber)ARG_18, "srespool", NAMEOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ARG_19, "global_sessionid", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ARG_20, "unique_sql_id", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ARG_21, "trace_id", TEXTOID, -1, 0);
    }

    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->setDesc = BlessTupleDesc(tupdesc);

    MemoryContextSwitchTo(oldcontext);

    bool hasTID = false;
    ThreadId tid = 0;
    if (!PG_ARGISNULL(0)) {
        hasTID = true;
        tid = PG_GETARG_INT64(0);
    }

    if (u_sess->attr.attr_common.pgstat_track_activities) {
        if (has_conninfo) {
            (void)gs_stat_read_current_status(rsinfo->setResult, rsinfo->setDesc,
                                              insert_pg_stat_get_activity_with_conninfo, hasTID, tid);
        } else {
            (void)gs_stat_read_current_status(rsinfo->setResult, rsinfo->setDesc,
                                              insert_pg_stat_get_activity, hasTID, tid);
        }
    } else {
        ereport(INFO, (errmsg("The collection of information is disabled because track_activities is off.")));
    }

    /* clean up and return the tuplestore */
    tuplestore_donestoring(rsinfo->setResult);

    return (Datum) 0;
}

char* GetGlobalSessionStr(GlobalSessionId globalSessionId)
{
    StringInfoData gId;
    initStringInfo(&gId);
    appendStringInfo(&gId, "%d:%lu#%lu",
        (int)globalSessionId.nodeId, globalSessionId.sessionId, globalSessionId.seq);
    return gId.data;
}

void insert_pg_stat_get_activity_with_conninfo(Tuplestorestate *tupStore, TupleDesc tupDesc,
                                               const PgBackendStatus *beentry)
{
    const int ATT_NUM = 22;

    Datum values[ATT_NUM];
    bool nulls[ATT_NUM];
    errno_t rc = 0;

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    /* Values only available to same user or superuser */
    if (superuser() || isMonitoradmin(GetUserId()) || beentry->st_userid == GetUserId()) {
        values[ARR_0] = ObjectIdGetDatum(beentry->st_databaseid);
        values[ARR_1] = Int64GetDatum(beentry->st_procpid);
        values[ARR_2] = Int64GetDatum(beentry->st_sessionid);
        values[ARR_3] = ObjectIdGetDatum(beentry->st_userid);
        if (beentry->st_appname)
            values[ARR_4] = CStringGetTextDatum(beentry->st_appname);
        else
            nulls[ARR_4] = true;

        SockAddr zero_clientaddr;

        switch (beentry->st_state) {
            case STATE_IDLE:
                values[ARR_5] = CStringGetTextDatum("idle");
                break;
            case STATE_RUNNING:
                values[ARR_5] = CStringGetTextDatum("active");
                break;
            case STATE_IDLEINTRANSACTION:
                values[ARR_5] = CStringGetTextDatum("idle in transaction");
                break;
            case STATE_FASTPATH:
                values[ARR_5] = CStringGetTextDatum("fastpath function call");
                break;
            case STATE_IDLEINTRANSACTION_ABORTED:
                values[ARR_5] = CStringGetTextDatum("idle in transaction (aborted)");
                break;
            case STATE_DISABLED:
                values[ARR_5] = CStringGetTextDatum("disabled");
                break;
            case STATE_RETRYING:
                values[ARR_5] = CStringGetTextDatum("retrying");
                break;
            case STATE_COUPLED:
                values[ARR_5] = CStringGetTextDatum("coupled to session");
                break;
            case STATE_DECOUPLED:
                values[ARR_5] = CStringGetTextDatum("decoupled from session");
                break;
            case STATE_UNDEFINED:
                nulls[ARR_5] = true;
                break;
            default:
                break;
        }
        if (beentry->st_state == STATE_UNDEFINED || beentry->st_state == STATE_DISABLED) {
            nulls[ARR_6] = true;
        } else {
            char* mask_string = NULL;

            /* Mask the statement in the activity view. */
            mask_string = maskPassword(beentry->st_activity);
            if (mask_string == NULL)
                mask_string = beentry->st_activity;

            values[ARR_6] = CStringGetTextDatum(mask_string);

            if (mask_string != beentry->st_activity)
                pfree(mask_string);
        }
        values[ARR_7] = BoolGetDatum(pgstat_get_waitlock(beentry->st_waitevent));

        if (beentry->st_xact_start_timestamp != 0)
            values[ARR_8] = TimestampTzGetDatum(beentry->st_xact_start_timestamp);
        else
            nulls[ARR_8] = true;

        if (beentry->st_activity_start_timestamp != 0)
            values[ARR_9] = TimestampTzGetDatum(beentry->st_activity_start_timestamp);
        else
            nulls[ARR_9] = true;

        if (beentry->st_proc_start_timestamp != 0)
            values[ARR_10] = TimestampTzGetDatum(beentry->st_proc_start_timestamp);
        else
            nulls[ARR_10] = true;

        if (beentry->st_state_start_timestamp != 0)
            values[ARR_11] = TimestampTzGetDatum(beentry->st_state_start_timestamp);
        else
            nulls[ARR_11] = true;

        /* A zeroed client addr means we don't know */
        rc = memset_s(&zero_clientaddr, sizeof(zero_clientaddr), 0, sizeof(zero_clientaddr));
        securec_check(rc, "\0", "\0");
        if (memcmp(&(beentry->st_clientaddr), &zero_clientaddr, sizeof(zero_clientaddr)) == 0) {
            nulls[ARR_12] = true;
            nulls[ARR_13] = true;
            nulls[ARR_14] = true;
        } else {
            if (beentry->st_clientaddr.addr.ss_family == AF_INET
#ifdef HAVE_IPV6
                || beentry->st_clientaddr.addr.ss_family == AF_INET6
#endif
                    ) {
                char remote_host[NI_MAXHOST];
                char remote_port[NI_MAXSERV];
                int ret;

                remote_host[0] = '\0';
                remote_port[0] = '\0';
                ret = pg_getnameinfo_all(&beentry->st_clientaddr.addr,
                                         beentry->st_clientaddr.salen,
                                         remote_host,
                                         sizeof(remote_host),
                                         remote_port,
                                         sizeof(remote_port),
                                         NI_NUMERICHOST | NI_NUMERICSERV);
                if (ret == 0) {
                    clean_ipv6_addr(beentry->st_clientaddr.addr.ss_family, remote_host);
                    values[ARR_12] = DirectFunctionCall1(inet_in, CStringGetDatum(remote_host));
                    if (beentry->st_clienthostname && beentry->st_clienthostname[0])
                        values[ARR_13] = CStringGetTextDatum(beentry->st_clienthostname);
                    else
                        nulls[ARR_13] = true;
                    values[14] = Int32GetDatum(atoi(remote_port));
                } else {
                    nulls[ARR_12] = true;
                    nulls[ARR_13] = true;
                    nulls[ARR_14] = true;
                }
            } else if (beentry->st_clientaddr.addr.ss_family == AF_UNIX) {
                /*
                 * Unix sockets always reports NULL for host and -1 for
                 * port, so it's possible to tell the difference to
                 * connections we have no permissions to view, or with
                 * errors.
                 */
                nulls[ARR_12] = true;
                nulls[ARR_13] = true;
                values[ARR_14] = DatumGetInt32(-1);
            } else {
                /* Unknown address type, should never happen */
                nulls[ARR_12] = true;
                nulls[ARR_13] = true;
                nulls[ARR_14] = true;
            }
        }

        switch (beentry->st_waiting_on_resource) {
            case STATE_MEMORY:
                values[ARR_15] = CStringGetTextDatum("memory");
                break;
            case STATE_ACTIVE_STATEMENTS:
                values[ARR_15] = CStringGetTextDatum("waiting in queue");
                break;
            case STATE_NO_ENQUEUE:
                nulls[ARR_15] = true;
                break;
            default:
                break;
        }

        values[ARR_16] = Int64GetDatum(beentry->st_queryid);

        if (beentry->st_conninfo != NULL) {
            values[ARR_17] = CStringGetTextDatum(beentry->st_conninfo);
        }
        else {
            nulls[ARR_17] = true;
        }
        /*use backend state to get all workload info*/
        WLMStatistics* backstat = (WLMStatistics*)&beentry->st_backstat;
        if (ENABLE_WORKLOAD_CONTROL && *backstat->srespool) {
            values[ARR_18] = NameGetDatum((Name)backstat->srespool);
        } else {
            values[ARR_18] = DirectFunctionCall1(namein, CStringGetDatum("unknown"));
        }
        char* gId = GetGlobalSessionStr(beentry->globalSessionId);
        values[ARR_19] = CStringGetTextDatum(gId);
        pfree(gId);
        values[ARR_20] = Int64GetDatum(beentry->st_unique_sql_key.unique_sql_id);
        values[ARR_21] = CStringGetTextDatum(beentry->trace_cxt.trace_id);
    } else {
        /* No permissions to view data about this session */
        values[ARR_6] = CStringGetTextDatum("<insufficient privilege>");

        nulls[ARR_0] = true;
        nulls[ARR_1] = true;
        nulls[ARR_2] = true;
        nulls[ARR_3] = true;
        nulls[ARR_4] = true;
        nulls[ARR_5] = true;
        nulls[ARR_7] = true;
        nulls[ARR_8] = true;
        nulls[ARR_9] = true;
        nulls[ARR_10] = true;
        nulls[ARR_11] = true;
        nulls[ARR_12] = true;
        nulls[ARR_13] = true;
        nulls[ARR_14] = true;
        nulls[ARR_15] = true;
        nulls[ARR_16] = true;
        nulls[ARR_17] = true;
        nulls[ARR_18] = true;
        nulls[ARR_19] = true;
        nulls[ARR_20] = true;
        nulls[ARR_21] = true;
    }

    tuplestore_putvalues(tupStore, tupDesc, values, nulls);
}

void insert_pg_stat_get_activity(Tuplestorestate *tupStore, TupleDesc tupDesc, const PgBackendStatus *beentry)
{
    const int ATT_NUM = 21;

    Datum values[ATT_NUM];
    bool nulls[ATT_NUM];
    errno_t rc = 0;

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    /* Values only available to same user or superuser */
    if (superuser() || isMonitoradmin(GetUserId()) || beentry->st_userid == GetUserId()) {
        values[ARR_0] = ObjectIdGetDatum(beentry->st_databaseid);
        values[ARR_1] = Int64GetDatum(beentry->st_procpid);
        values[ARR_2] = Int64GetDatum(beentry->st_sessionid);
        values[ARR_3] = ObjectIdGetDatum(beentry->st_userid);
        if (beentry->st_appname)
            values[ARR_4] = CStringGetTextDatum(beentry->st_appname);
        else
            nulls[ARR_4] = true;

        SockAddr zero_clientaddr;

        switch (beentry->st_state) {
            case STATE_IDLE:
                values[ARR_5] = CStringGetTextDatum("idle");
                break;
            case STATE_RUNNING:
                values[ARR_5] = CStringGetTextDatum("active");
                break;
            case STATE_IDLEINTRANSACTION:
                values[ARR_5] = CStringGetTextDatum("idle in transaction");
                break;
            case STATE_FASTPATH:
                values[ARR_5] = CStringGetTextDatum("fastpath function call");
                break;
            case STATE_IDLEINTRANSACTION_ABORTED:
                values[ARR_5] = CStringGetTextDatum("idle in transaction (aborted)");
                break;
            case STATE_DISABLED:
                values[ARR_5] = CStringGetTextDatum("disabled");
                break;
            case STATE_RETRYING:
                values[ARR_5] = CStringGetTextDatum("retrying");
                break;
            case STATE_COUPLED:
                values[ARR_5] = CStringGetTextDatum("coupled to session");
                break;
            case STATE_DECOUPLED:
                values[ARR_5] = CStringGetTextDatum("decoupled from session");
                break;
            case STATE_UNDEFINED:
                nulls[ARR_5] = true;
                break;
            default:
                break;
        }
        if (beentry->st_state == STATE_UNDEFINED || beentry->st_state == STATE_DISABLED) {
            nulls[ARR_6] = true;
        } else {
            char* mask_string = NULL;

            /* Mask the statement in the activity view. */
            mask_string = maskPassword(beentry->st_activity);
            if (mask_string == NULL)
                mask_string = beentry->st_activity;

            values[ARR_6] = CStringGetTextDatum(mask_string);

            if (mask_string != beentry->st_activity)
                pfree(mask_string);
        }
        values[ARR_7] = BoolGetDatum(pgstat_get_waitlock(beentry->st_waitevent));

        if (beentry->st_xact_start_timestamp != 0)
            values[ARR_8] = TimestampTzGetDatum(beentry->st_xact_start_timestamp);
        else
            nulls[ARR_8] = true;

        if (beentry->st_activity_start_timestamp != 0)
            values[ARR_9] = TimestampTzGetDatum(beentry->st_activity_start_timestamp);
        else
            nulls[ARR_9] = true;

        if (beentry->st_proc_start_timestamp != 0)
            values[ARR_10] = TimestampTzGetDatum(beentry->st_proc_start_timestamp);
        else
            nulls[ARR_10] = true;

        if (beentry->st_state_start_timestamp != 0)
            values[ARR_11] = TimestampTzGetDatum(beentry->st_state_start_timestamp);
        else
            nulls[ARR_11] = true;

        /* A zeroed client addr means we don't know */
        rc = memset_s(&zero_clientaddr, sizeof(zero_clientaddr), 0, sizeof(zero_clientaddr));
        securec_check(rc, "\0", "\0");
        if (memcmp(&(beentry->st_clientaddr), &zero_clientaddr, sizeof(zero_clientaddr)) == 0) {
            nulls[ARR_12] = true;
            nulls[ARR_13] = true;
            nulls[ARR_14] = true;
        } else {
            if (beentry->st_clientaddr.addr.ss_family == AF_INET
#ifdef HAVE_IPV6
                || beentry->st_clientaddr.addr.ss_family == AF_INET6
#endif
                    ) {
                char remote_host[NI_MAXHOST];
                char remote_port[NI_MAXSERV];
                int ret;

                remote_host[0] = '\0';
                remote_port[0] = '\0';
                ret = pg_getnameinfo_all(&beentry->st_clientaddr.addr,
                                         beentry->st_clientaddr.salen,
                                         remote_host,
                                         sizeof(remote_host),
                                         remote_port,
                                         sizeof(remote_port),
                                         NI_NUMERICHOST | NI_NUMERICSERV);
                if (ret == 0) {
                    clean_ipv6_addr(beentry->st_clientaddr.addr.ss_family, remote_host);
                    values[ARR_12] = DirectFunctionCall1(inet_in, CStringGetDatum(remote_host));
                    if (beentry->st_clienthostname && beentry->st_clienthostname[0])
                        values[ARR_13] = CStringGetTextDatum(beentry->st_clienthostname);
                    else
                        nulls[ARR_13] = true;
                    values[14] = Int32GetDatum(atoi(remote_port));
                } else {
                    nulls[ARR_12] = true;
                    nulls[ARR_13] = true;
                    nulls[ARR_14] = true;
                }
            } else if (beentry->st_clientaddr.addr.ss_family == AF_UNIX) {
                /*
                 * Unix sockets always reports NULL for host and -1 for
                 * port, so it's possible to tell the difference to
                 * connections we have no permissions to view, or with
                 * errors.
                 */
                nulls[ARR_12] = true;
                nulls[ARR_13] = true;
                values[ARR_14] = DatumGetInt32(-1);
            } else {
                /* Unknown address type, should never happen */
                nulls[ARR_12] = true;
                nulls[ARR_13] = true;
                nulls[ARR_14] = true;
            }
        }

        switch (beentry->st_waiting_on_resource) {
            case STATE_MEMORY:
                values[ARR_15] = CStringGetTextDatum("memory");
                break;
            case STATE_ACTIVE_STATEMENTS:
                values[ARR_15] = CStringGetTextDatum("waiting in queue");
                break;
            case STATE_NO_ENQUEUE:
                nulls[ARR_15] = true;
                break;
            default:
                break;
        }

        values[ARR_16] = Int64GetDatum(beentry->st_queryid);

        /*use backend state to get all workload info*/
        WLMStatistics* backstat = (WLMStatistics*)&beentry->st_backstat;
        if (ENABLE_WORKLOAD_CONTROL && *backstat->srespool) {
            values[ARR_17] = NameGetDatum((Name)backstat->srespool);
        } else {
            values[ARR_17] = DirectFunctionCall1(namein, CStringGetDatum("unknown"));
        }
        StringInfoData globalSessionId;
        initStringInfo(&globalSessionId);
        appendStringInfo(&globalSessionId, "%d#%lu#%lu",
            (int)beentry->globalSessionId.nodeId, beentry->globalSessionId.sessionId, beentry->globalSessionId.seq);
        values[ARR_18] = CStringGetTextDatum(globalSessionId.data);
        pfree(globalSessionId.data);
        values[ARR_19] = Int64GetDatum(beentry->st_unique_sql_key.unique_sql_id);
        values[ARR_20] = CStringGetTextDatum(beentry->trace_cxt.trace_id);
    } else {
        /* No permissions to view data about this session */
        values[ARR_6] = CStringGetTextDatum("<insufficient privilege>");

        nulls[ARR_0] = true;
        nulls[ARR_1] = true;
        nulls[ARR_2] = true;
        nulls[ARR_3] = true;
        nulls[ARR_4] = true;
        nulls[ARR_5] = true;
        nulls[ARR_7] = true;
        nulls[ARR_8] = true;
        nulls[ARR_9] = true;
        nulls[ARR_10] = true;
        nulls[ARR_11] = true;
        nulls[ARR_12] = true;
        nulls[ARR_13] = true;
        nulls[ARR_14] = true;
        nulls[ARR_15] = true;
        nulls[ARR_16] = true;
        nulls[ARR_17] = true;
        nulls[ARR_18] = true;
        nulls[ARR_19] = true;
        nulls[ARR_20] = true;
    }

    tuplestore_putvalues(tupStore, tupDesc, values, nulls);
}

Datum pg_stat_get_activity_for_temptable(PG_FUNCTION_ARGS)
{

    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    MemoryContext oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);

    const int ATT_COUNT = 4;
    TupleDesc tupdesc = CreateTemplateTupleDesc(ATT_COUNT, false);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_1, "datid", OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_2, "templineid", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_3, "tempid", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_4, "sessionid", INT8OID, -1, 0);

    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->setDesc = BlessTupleDesc(tupdesc);

    (void)gs_stat_read_current_status(rsinfo->setResult, rsinfo->setDesc, insert_pg_stat_get_activity_for_temptable);

    MemoryContextSwitchTo(oldcontext);

    /* clean up and return the tuplestore */
    tuplestore_donestoring(rsinfo->setResult);

    return (Datum) 0;
}

void insert_pg_stat_get_activity_for_temptable(Tuplestorestate *tupStore, TupleDesc tupDesc,
                                               const PgBackendStatus *beentry)
{
    const int ATT_COUNT = 4;
    errno_t rc = 0;

    /* for each row */
    Datum values[ATT_COUNT];
    bool nulls[ATT_COUNT];

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    if (beentry == NULL || beentry->st_sessionid == 0) {
        for (int i = 0; i < ATT_COUNT; i++)
            nulls[i] = true;
        tuplestore_putvalues(tupStore, tupDesc, values, nulls);
        return;
    }

    /* Values available to all callers */
    values[ARR_0] = ObjectIdGetDatum(beentry->st_databaseid);
    values[ARR_1] = Int32GetDatum(beentry->st_timelineid);
    values[ARR_2] = Int32GetDatum(beentry->st_tempid);
    values[ARR_3] = Int64GetDatum(beentry->st_sessionid);

    tuplestore_putvalues(tupStore, tupDesc, values, nulls);
}

Datum gs_stat_activity_timeout(PG_FUNCTION_ARGS)
{
    const int ACTIVITY_TIMEOUT_ATTRS = 9;
    int timeout_threshold = PG_GETARG_INT32(0);
    if (timeout_threshold < 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("the timeout_threshold(%d) could not less than 0",
            timeout_threshold)));
    }
    if (timeout_threshold > (INT_MAX / MSECS_PER_SEC)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("the timeout_threshold(%d) could not larger than 2147483",
            timeout_threshold)));
    }
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    MemoryContext oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);

    TupleDesc tupdesc = CreateTemplateTupleDesc(ACTIVITY_TIMEOUT_ATTRS, false);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_1, "database", NAMEOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_2, "pid", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_3, "sessionid", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_4, "usesysid", OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_5, "application_name", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_6, "query", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_7, "xact_start", TIMESTAMPTZOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_8, "query_start", TIMESTAMPTZOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_9, "query_id", INT8OID, -1, 0);

    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->setDesc = BlessTupleDesc(tupdesc);

    MemoryContextSwitchTo(oldcontext);

    if (u_sess->attr.attr_common.pgstat_track_activities) {
        gs_stat_get_timeout_beentry(timeout_threshold, rsinfo->setResult, rsinfo->setDesc,
                                    insert_gs_stat_activity_timeout);
    }

    /* clean up and return the tuplestore */
    tuplestore_donestoring(rsinfo->setResult);

    return (Datum) 0;
}

void insert_gs_stat_activity_timeout(Tuplestorestate* tupStore, TupleDesc tupDesc, const PgBackendStatus *beentry)
{
    const int ACTIVITY_TIMEOUT_ATTRS = 9;
    int i = 0;

    char* dbname = get_database_name(beentry->st_databaseid);
    if (dbname == NULL || dbname[0] == '\0') {
        return;
    }

    NameData database_name;
    namestrcpy(&database_name, dbname);
    pfree(dbname);

    /* for each row */
    Datum values[ACTIVITY_TIMEOUT_ATTRS];
    bool nulls[ACTIVITY_TIMEOUT_ATTRS];
    errno_t rc = 0;

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    /* Values only available to same user or superuser */
    if (superuser() || isMonitoradmin(GetUserId()) ||
        beentry->st_userid == GetUserId()) {
        /* Values available to all callers */
        values[i++] = NameGetDatum(&database_name);
        values[i++] = Int64GetDatum(beentry->st_procpid);
        values[i++] = Int64GetDatum(beentry->st_sessionid);
        values[i++] = ObjectIdGetDatum(beentry->st_userid);
        if (beentry->st_appname)
            values[i++] = CStringGetTextDatum(beentry->st_appname);
        else
            nulls[i++] = true;

        char* mask_string = NULL;

        /* Mask the statement in the activity view. */
        mask_string = maskPassword(beentry->st_activity);
        if (mask_string == NULL)
            mask_string = beentry->st_activity;

        values[i++] = CStringGetTextDatum(mask_string);

        if (mask_string != beentry->st_activity) {
            pfree(mask_string);
        }

        if (beentry->st_xact_start_timestamp != 0)
            values[i++] = TimestampTzGetDatum(beentry->st_xact_start_timestamp);
        else
            nulls[i++] = true;

        if (beentry->st_activity_start_timestamp != 0)
            values[i++] = TimestampTzGetDatum(beentry->st_activity_start_timestamp);
        else
            nulls[i++] = true;

        values[i++] = Int64GetDatum(beentry->st_queryid);
    } else {
        nulls[i++] = true;
        nulls[i++] = true;
        nulls[i++] = true;
        nulls[i++] = true;
        nulls[i++] = true;
        /* No permissions to view data about this session */
        values[i++] = CStringGetTextDatum("<insufficient privilege>");
        nulls[i++] = true;
        nulls[i++] = true;
        nulls[i++] = true;
    }

    tuplestore_putvalues(tupStore, tupDesc, values, nulls);
    return;
}

Datum pg_stat_get_activity_ng(PG_FUNCTION_ARGS)
{
    const int ATT_NUM = 4;
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    MemoryContext oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);

    TupleDesc tupdesc = CreateTemplateTupleDesc(ATT_NUM, false);

    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_1, "datid", OIDOID, -1, 0);
    /* This should have been called 'pid';  can't change it. 2011-06-11 */
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_2, "pid", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_3, "sessionid", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_4, "node_group", TEXTOID, -1, 0);

    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->setDesc = BlessTupleDesc(tupdesc);

    bool hasTID = false;
    ThreadId tid = 0;
    if (!PG_ARGISNULL(0)) {
        hasTID = true;
        tid = PG_GETARG_INT64(0);
    }

    (void)gs_stat_read_current_status(rsinfo->setResult, rsinfo->setDesc, insert_pg_stat_get_activity_ng, hasTID, tid);

    MemoryContextSwitchTo(oldcontext);
    /* clean up and return the tuplestore */
    tuplestore_donestoring(rsinfo->setResult);

    return (Datum) 0;
}

void insert_pg_stat_get_activity_ng(Tuplestorestate *tupStore, TupleDesc tupDesc, const PgBackendStatus *beentry)
{
    const int ATT_NUM = 4;
    Datum values[ATT_NUM];
    bool nulls[ATT_NUM];
    errno_t rc = 0;

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    /* Values available to all callers */
    values[ARR_0] = ObjectIdGetDatum(beentry->st_databaseid);
    values[ARR_1] = Int64GetDatum(beentry->st_procpid);
    values[ARR_2] = Int64GetDatum(beentry->st_sessionid);

    /* Values only available to same user or superuser */
    if (superuser() || isMonitoradmin(GetUserId()) || beentry->st_userid == GetUserId()) {
        WLMStatistics* backstat = (WLMStatistics*)&beentry->st_backstat;
        if (StringIsValid(backstat->groupname))
            values[ARR_3] = CStringGetTextDatum(backstat->groupname);
        else
            values[ARR_3] = CStringGetTextDatum("installation");
    } else {
        nulls[ARR_3] = true;
    }

    tuplestore_putvalues(tupStore, tupDesc, values, nulls);
}

char* getThreadWaitStatusDesc(PgBackendStatus* beentry)
{
    char* waitStatus = NULL;
    errno_t rc = 0;

    Assert(beentry);

    waitStatus = (char*)palloc0(WAITSTATELEN);

    if (beentry->st_waitevent != 0) {
        rc = snprintf_s(waitStatus,
            WAITSTATELEN,
            WAITSTATELEN - 1,
            "%s: %s",
            pgstat_get_waitstatusdesc(beentry->st_waitevent),
            pgstat_get_wait_event(beentry->st_waitevent));
        securec_check_ss(rc, "\0", "\0");
    } else if (beentry->st_xid != 0) {
        rc = snprintf_s(waitStatus,
            WAITSTATELEN,
            WAITSTATELEN - 1,
            "%s: %lu",
            WaitStateDesc[beentry->st_waitstatus],
            beentry->st_xid);
        securec_check_ss(rc, "\0", "\0");
    } else if (beentry->st_relname && beentry->st_relname[0] != '\0' &&
               (STATE_VACUUM == beentry->st_waitstatus || STATE_VACUUM_FULL == beentry->st_waitstatus ||
                   STATE_ANALYZE == beentry->st_waitstatus)) {
        if (PHASE_NONE != beentry->st_waitstatus_phase) {
            rc = snprintf_s(waitStatus,
                WAITSTATELEN,
                WAITSTATELEN - 1,
                "%s: %s, %s",
                WaitStateDesc[beentry->st_waitstatus],
                beentry->st_relname,
                WaitStatePhaseDesc[beentry->st_waitstatus_phase]);
            securec_check_ss(rc, "\0", "\0");
        } else {
            rc = snprintf_s(waitStatus,
                WAITSTATELEN,
                WAITSTATELEN - 1,
                "%s: %s",
                WaitStateDesc[beentry->st_waitstatus],
                beentry->st_relname);
            securec_check_ss(rc, "\0", "\0");
        }
    } else if (beentry->st_libpq_wait_nodeid != InvalidOid && beentry->st_libpq_wait_nodecount != 0) {
        rc = snprintf_s(waitStatus,
            WAITSTATELEN,
            WAITSTATELEN - 1,
            "%s: %d, total %d",
            WaitStateDesc[beentry->st_waitstatus],
            PgxcGetNodeOid(beentry->st_libpq_wait_nodeid),
            beentry->st_libpq_wait_nodecount);
        securec_check_ss(rc, "\0", "\0");
    } else if (beentry->st_nodeid != -1) {
        if (beentry->st_waitnode_count > 0) {
            rc = snprintf_s(waitStatus,
                WAITSTATELEN,
                WAITSTATELEN - 1,
                "%s: %d, total %d",
                WaitStateDesc[beentry->st_waitstatus],
                PgxcGetNodeOid(beentry->st_nodeid),
                beentry->st_waitnode_count);
        } else {
            if (PHASE_NONE != beentry->st_waitstatus_phase)
                rc = snprintf_s(waitStatus,
                    WAITSTATELEN,
                    WAITSTATELEN - 1,
                    "%s: %d, %s",
                    WaitStateDesc[beentry->st_waitstatus],
                    PgxcGetNodeOid(beentry->st_nodeid),
                    WaitStatePhaseDesc[beentry->st_waitstatus_phase]);
            else
                rc = snprintf_s(waitStatus,
                    WAITSTATELEN,
                    WAITSTATELEN - 1,
                    "%s: %d",
                    WaitStateDesc[beentry->st_waitstatus],
                    PgxcGetNodeOid(beentry->st_nodeid));
        }
        securec_check_ss(rc, "\0", "\0");
    } else if (STATE_WAIT_COMM != beentry->st_waitstatus) {
        rc = snprintf_s(waitStatus, WAITSTATELEN, WAITSTATELEN - 1, "%s", WaitStateDesc[beentry->st_waitstatus]);
        securec_check_ss(rc, "\0", "\0");
    }

    return waitStatus;
}

static void GetBlockInfo(const PgBackendStatus* beentry, Datum values[], bool nulls[])
{
    struct LOCKTAG locktag = beentry->locallocktag.lock;

    if (beentry->locallocktag.lock.locktag_lockmethodid == 0) {
        nulls[NUM_PG_LOCKTAG_ID] = true;
        nulls[NUM_PG_LOCKMODE_ID] = true;
        nulls[NUM_PG_BLOCKSESSION_ID] = true;
        return;
    }

    if ((beentry->st_waitevent & 0xFF000000) == PG_WAIT_LOCK) {
        char* blocklocktag = LocktagToString(locktag);
        values[NUM_PG_LOCKTAG_ID] = CStringGetTextDatum(blocklocktag);
        values[NUM_PG_LOCKMODE_ID] = CStringGetTextDatum(
            GetLockmodeName(beentry->locallocktag.lock.locktag_lockmethodid,
                            beentry->locallocktag.mode));
        values[NUM_PG_BLOCKSESSION_ID] = Int64GetDatum(beentry->st_block_sessionid);
        pfree_ext(blocklocktag);
    } else {
        nulls[NUM_PG_LOCKTAG_ID] = true;
        nulls[NUM_PG_LOCKMODE_ID] = true;
        nulls[NUM_PG_BLOCKSESSION_ID] = true;
    }
}

/*
 * ConstructWaitStatus
 *
 * form wait status string from beentry object.
 */
static void ConstructWaitStatus(const PgBackendStatus* beentry, Datum values[16], bool nulls[16])
{
    char waitStatus[WAITSTATELEN];
    errno_t rc = EOK;

    rc = memset_s(waitStatus, sizeof(waitStatus), 0, sizeof(waitStatus));
    securec_check(rc, "\0", "\0");

    /* Values available to all callers */
    if (beentry->st_databaseid != 0 && get_database_name(beentry->st_databaseid) != NULL)
        values[ARR_1] = CStringGetTextDatum(get_database_name(beentry->st_databaseid));
    else
        nulls[ARR_1] = true;

    if (beentry->st_appname)
        values[ARR_2] = CStringGetTextDatum(beentry->st_appname);
    else
        nulls[ARR_2] = true;

    values[ARR_4] = Int64GetDatum(beentry->st_procpid);
    values[ARR_5] = Int64GetDatum(beentry->st_sessionid);

    /* Values only available to same user or superuser */
    if (superuser() || isMonitoradmin(GetUserId()) || beentry->st_userid == GetUserId()) {
        values[ARR_0] = CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);
        values[ARR_3] = Int64GetDatum(beentry->st_queryid);
        values[ARR_6] = Int32GetDatum(beentry->st_tid);
        if (0 != beentry->st_parent_sessionid)
            values[ARR_7] = Int64GetDatum(beentry->st_parent_sessionid);
        else
            nulls[ARR_7] = true;
        values[ARR_8] = Int32GetDatum(beentry->st_thread_level);
        values[ARR_9] = UInt32GetDatum(beentry->st_smpid);

        /* wait_event info */
        if (beentry->st_waitevent != 0) {
            uint32 raw_wait_event;
            /* read only once be atomic */
            raw_wait_event = UINT32_ACCESS_ONCE(beentry->st_waitevent);
            values[ARR_10] = CStringGetTextDatum(pgstat_get_waitstatusdesc(raw_wait_event));
            values[ARR_11] = CStringGetTextDatum(pgstat_get_wait_event(raw_wait_event));
            GetBlockInfo(beentry, values, nulls);
        } else if (beentry->st_xid != 0) {
            if (STATE_WAIT_XACTSYNC == beentry->st_waitstatus) {
                rc = snprintf_s(waitStatus,
                    WAITSTATELEN,
                    WAITSTATELEN - 1,
                    "%s: %lu",
                    WaitStateDesc[beentry->st_waitstatus],
                    beentry->st_xid);
                securec_check_ss(rc, "\0", "\0");
            } else {
                rc =
                    snprintf_s(waitStatus, WAITSTATELEN, WAITSTATELEN - 1, "%s", WaitStateDesc[beentry->st_waitstatus]);
                securec_check_ss(rc, "\0", "\0");
            }
            values[ARR_10] = CStringGetTextDatum(waitStatus);
            values[ARR_11] = CStringGetTextDatum(pgstat_get_waitstatusname(beentry->st_waitstatus));
            nulls[ARR_12] = true;
            nulls[ARR_13] = true;
            nulls[ARR_14] = true;
        } else if (beentry->st_relname && beentry->st_relname[0] != '\0' &&
                   (STATE_VACUUM == beentry->st_waitstatus || STATE_ANALYZE == beentry->st_waitstatus ||
                       STATE_VACUUM_FULL == beentry->st_waitstatus)) {
            if (PHASE_NONE != beentry->st_waitstatus_phase) {
                rc = snprintf_s(waitStatus,
                    WAITSTATELEN,
                    WAITSTATELEN - 1,
                    "%s: %s, %s",
                    WaitStateDesc[beentry->st_waitstatus],
                    beentry->st_relname,
                    WaitStatePhaseDesc[beentry->st_waitstatus_phase]);
                securec_check_ss(rc, "\0", "\0");
            } else {
                rc = snprintf_s(waitStatus,
                    WAITSTATELEN,
                    WAITSTATELEN - 1,
                    "%s: %s",
                    WaitStateDesc[beentry->st_waitstatus],
                    beentry->st_relname);
                securec_check_ss(rc, "\0", "\0");
            }
            values[ARR_10] = CStringGetTextDatum(waitStatus);
            values[ARR_11] = CStringGetTextDatum(pgstat_get_waitstatusname(beentry->st_waitstatus));
            nulls[ARR_12] = true;
            nulls[ARR_13] = true;
            nulls[ARR_14] = true;
        } else if (beentry->st_libpq_wait_nodeid != InvalidOid && beentry->st_libpq_wait_nodecount != 0) {
            if (IS_PGXC_COORDINATOR) {
                NameData nodename = {{0}};
                rc = snprintf_s(waitStatus,
                    WAITSTATELEN,
                    WAITSTATELEN - 1,
                    "%s: %s, total %d",
                    WaitStateDesc[beentry->st_waitstatus],
                    get_pgxc_nodename(beentry->st_libpq_wait_nodeid, &nodename),
                    beentry->st_libpq_wait_nodecount);
                securec_check_ss(rc, "\0", "\0");
            } else if (IS_PGXC_DATANODE) {
                if (global_node_definition != NULL && global_node_definition->nodesDefinition &&
                    global_node_definition->num_nodes == beentry->st_numnodes) {
                    AutoMutexLock copyLock(&nodeDefCopyLock);
                    NodeDefinition* nodeDef = global_node_definition->nodesDefinition;
                    copyLock.lock();
                    rc = snprintf_s(waitStatus,
                        WAITSTATELEN,
                        WAITSTATELEN - 1,
                        "%s: %s, total %d",
                        WaitStateDesc[beentry->st_waitstatus],
                        nodeDef[beentry->st_libpq_wait_nodeid].nodename.data,
                        beentry->st_libpq_wait_nodecount);
                    securec_check_ss(rc, "\0", "\0");
                    copyLock.unLock();
                } else {
                    rc = snprintf_s(
                        waitStatus, WAITSTATELEN, WAITSTATELEN - 1, "%s", WaitStateDesc[beentry->st_waitstatus]);
                    securec_check_ss(rc, "\0", "\0");
                }
            }
            values[ARR_10] = CStringGetTextDatum(waitStatus);
            values[ARR_11] = CStringGetTextDatum(pgstat_get_waitstatusname(beentry->st_waitstatus));
            nulls[ARR_12] = true;
            nulls[ARR_13] = true;
            nulls[ARR_14] = true;
        } else if (beentry->st_nodeid != -1) {
            if (IS_PGXC_COORDINATOR && (Oid)beentry->st_nodeid != InvalidOid) {
                NameData nodename = {{0}};

                if (beentry->st_waitnode_count > 0) {
                    if (PHASE_NONE != beentry->st_waitstatus_phase) {
                        rc = snprintf_s(waitStatus,
                            WAITSTATELEN,
                            WAITSTATELEN - 1,
                            "%s: %s, total %d, %s",
                            WaitStateDesc[beentry->st_waitstatus],
                            get_pgxc_nodename(beentry->st_nodeid, &nodename),
                            beentry->st_waitnode_count,
                            WaitStatePhaseDesc[beentry->st_waitstatus_phase]);
                        securec_check_ss(rc, "\0", "\0");
                    } else {
                        rc = snprintf_s(waitStatus,
                            WAITSTATELEN,
                            WAITSTATELEN - 1,
                            "%s: %s, total %d",
                            WaitStateDesc[beentry->st_waitstatus],
                            get_pgxc_nodename(beentry->st_nodeid, &nodename),
                            beentry->st_waitnode_count);
                        securec_check_ss(rc, "\0", "\0");
                    }
                } else {
                    if (PHASE_NONE != beentry->st_waitstatus_phase) {
                        rc = snprintf_s(waitStatus,
                            WAITSTATELEN,
                            WAITSTATELEN - 1,
                            "%s: %s, %s",
                            WaitStateDesc[beentry->st_waitstatus],
                            get_pgxc_nodename(beentry->st_nodeid, &nodename),
                            WaitStatePhaseDesc[beentry->st_waitstatus_phase]);
                        securec_check_ss(rc, "\0", "\0");
                    } else {
                        rc = snprintf_s(waitStatus,
                            WAITSTATELEN,
                            WAITSTATELEN - 1,
                            "%s: %s",
                            WaitStateDesc[beentry->st_waitstatus],
                            get_pgxc_nodename(beentry->st_nodeid, &nodename));
                        securec_check_ss(rc, "\0", "\0");
                    }
                }
            } else if (IS_PGXC_DATANODE) {
                if (global_node_definition != NULL && global_node_definition->nodesDefinition &&
                    global_node_definition->num_nodes == beentry->st_numnodes) {
                    AutoMutexLock copyLock(&nodeDefCopyLock);
                    NodeDefinition* nodeDef = global_node_definition->nodesDefinition;
                    copyLock.lock();

                    if (beentry->st_waitnode_count > 0) {
                        if (PHASE_NONE != beentry->st_waitstatus_phase) {
                            if (beentry->st_plannodeid != -1) {
                                rc = snprintf_s(waitStatus,
                                    WAITSTATELEN,
                                    WAITSTATELEN - 1,
                                    "%s: %s(%d), total %d, %s",
                                    WaitStateDesc[beentry->st_waitstatus],
                                    nodeDef[beentry->st_nodeid].nodename.data,
                                    beentry->st_plannodeid,
                                    beentry->st_waitnode_count,
                                    WaitStatePhaseDesc[beentry->st_waitstatus_phase]);
                                securec_check_ss(rc, "\0", "\0");
                            } else {
                                rc = snprintf_s(waitStatus,
                                    WAITSTATELEN,
                                    WAITSTATELEN - 1,
                                    "%s: %s, total %d, %s",
                                    WaitStateDesc[beentry->st_waitstatus],
                                    nodeDef[beentry->st_nodeid].nodename.data,
                                    beentry->st_waitnode_count,
                                    WaitStatePhaseDesc[beentry->st_waitstatus_phase]);
                                securec_check_ss(rc, "\0", "\0");
                            }
                        } else {
                            if (beentry->st_plannodeid != -1) {
                                rc = snprintf_s(waitStatus,
                                    WAITSTATELEN,
                                    WAITSTATELEN - 1,
                                    "%s: %s(%d), total %d",
                                    WaitStateDesc[beentry->st_waitstatus],
                                    nodeDef[beentry->st_nodeid].nodename.data,
                                    beentry->st_plannodeid,
                                    beentry->st_waitnode_count);
                                securec_check_ss(rc, "\0", "\0");
                            } else {
                                rc = snprintf_s(waitStatus,
                                    WAITSTATELEN,
                                    WAITSTATELEN - 1,
                                    "%s: %s, total %d",
                                    WaitStateDesc[beentry->st_waitstatus],
                                    nodeDef[beentry->st_nodeid].nodename.data,
                                    beentry->st_waitnode_count);
                                securec_check_ss(rc, "\0", "\0");
                            }
                        }
                    } else {
                        if (PHASE_NONE != beentry->st_waitstatus_phase) {
                            if (beentry->st_plannodeid != -1) {
                                rc = snprintf_s(waitStatus,
                                    WAITSTATELEN,
                                    WAITSTATELEN - 1,
                                    "%s: %s(%d), %s",
                                    WaitStateDesc[beentry->st_waitstatus],
                                    nodeDef[beentry->st_nodeid].nodename.data,
                                    beentry->st_plannodeid,
                                    WaitStatePhaseDesc[beentry->st_waitstatus_phase]);
                                securec_check_ss(rc, "\0", "\0");
                            } else {
                                rc = snprintf_s(waitStatus,
                                    WAITSTATELEN,
                                    WAITSTATELEN - 1,
                                    "%s: %s, %s",
                                    WaitStateDesc[beentry->st_waitstatus],
                                    nodeDef[beentry->st_nodeid].nodename.data,
                                    WaitStatePhaseDesc[beentry->st_waitstatus_phase]);
                                securec_check_ss(rc, "\0", "\0");
                            }
                        } else {
                            if (beentry->st_plannodeid != -1) {
                                rc = snprintf_s(waitStatus,
                                    WAITSTATELEN,
                                    WAITSTATELEN - 1,
                                    "%s: %s(%d)",
                                    WaitStateDesc[beentry->st_waitstatus],
                                    nodeDef[beentry->st_nodeid].nodename.data,
                                    beentry->st_plannodeid);
                                securec_check_ss(rc, "\0", "\0");
                            } else {
                                rc = snprintf_s(waitStatus,
                                    WAITSTATELEN,
                                    WAITSTATELEN - 1,
                                    "%s: %s",
                                    WaitStateDesc[beentry->st_waitstatus],
                                    nodeDef[beentry->st_nodeid].nodename.data);
                                securec_check_ss(rc, "\0", "\0");
                            }
                        }
                    }
                    copyLock.unLock();
                } else {
                    rc = snprintf_s(
                        waitStatus, WAITSTATELEN, WAITSTATELEN - 1, "%s", WaitStateDesc[beentry->st_waitstatus]);
                    securec_check_ss(rc, "\0", "\0");
                }
            }
            values[ARR_10] = CStringGetTextDatum(waitStatus);
            values[ARR_11] = CStringGetTextDatum(pgstat_get_waitstatusname(beentry->st_waitstatus));
            nulls[ARR_12] = true;
            nulls[ARR_13] = true;
            nulls[ARR_14] = true;
        } else {
            rc = snprintf_s(waitStatus, WAITSTATELEN, WAITSTATELEN - 1, "%s", WaitStateDesc[beentry->st_waitstatus]);
            securec_check_ss(rc, "\0", "\0");
            values[ARR_10] = CStringGetTextDatum(waitStatus);
            values[ARR_11] = CStringGetTextDatum(pgstat_get_waitstatusname(beentry->st_waitstatus));
            nulls[ARR_12] = true;
            nulls[ARR_13] = true;
            nulls[ARR_14] = true;
        }
        char* gId = GetGlobalSessionStr(beentry->globalSessionId);
        values[ARR_15] = CStringGetTextDatum(gId);
        pfree(gId);
    } else {
        nulls[ARR_0] = true;
        nulls[ARR_3] = true;
        nulls[ARR_6] = true;
        nulls[ARR_7] = true;
        nulls[ARR_8] = true;
        nulls[ARR_9] = true;
        nulls[ARR_10] = true;
        nulls[ARR_11] = true;
        nulls[ARR_12] = true;
        nulls[ARR_13] = true;
        nulls[ARR_14] = true;
        nulls[ARR_15] = true;
    }
}

static int Translat(char c)
{
    if (c <= '9' && c >= '0') {
        return c - '0';
    } else if (c >= 'a' && c <= 'f') {
        return c - LOWERCASE_LETTERS_ID;
    } else if (c >= 'A' && c <= 'F') {
        return c - UPPERCASE_LETTERS_ID;
    } else {
        return -1;
    }
}

static int Htoi(const char* str)
{
    int n = 0;

    for (unsigned int i = 0; i < strlen(str); i++) {
        int stat = Translat(str[i]);

        if (stat >= 0) {
            n = n * 16 + stat;
        } else {
            ereport(ERROR, (errmsg("An exception occurred when the locktag data is transferred!")));
        }
    }

    return n;
}

static char** GetLocktag(const char* locktag) 
{
    int i = 0;
    int j = 0;
    char** subStr = (char**)palloc(MAX_LOCKTAG_STRING * sizeof(char*));
    char* strs = pstrdup(locktag);
    int len = (int)strlen(strs);
    int substrStart = 0;

    for (; j < len; j++) {
        if (strs[j] == ':') {
            if (substrStart == j || i >= MAX_LOCKTAG_STRING) {
                ereport(ERROR, (errmsg("The input parameter %s is invalid!", locktag)));
            } else {
                strs[j] = '\0';
                subStr[i++] = pstrdup(strs + substrStart);
                substrStart = j + 1;
            }
        }
    }

    if (substrStart < j) {
        if (i >= MAX_LOCKTAG_STRING) {
            ereport(ERROR, (errmsg("The input parameter %s is invalid!", locktag)));
        }
        subStr[i++] = pstrdup(strs + substrStart);

        if (i != MAX_LOCKTAG_STRING) {
            ereport(ERROR, (errmsg("The input parameter %s is invalid!", locktag)));
        }
    } else {
        ereport(ERROR, (errmsg("The input parameter %s is invalid!", locktag)));
    }

    pfree_ext(strs);
    return subStr;
}

static char* GetLocktagDecode(const char* locktag)
{
    if (locktag == NULL) {
        ereport(ERROR, (errmsg("The input locktag is invalid!")));
    }

    char **ltag = GetLocktag(locktag);

    uint32 locktagField1 = Htoi(ltag[0]);
    uint32 locktagField2 = Htoi(ltag[1]);
    uint32 locktagField3 = Htoi(ltag[2]);
    uint32 locktagField4 = Htoi(ltag[3]);
    uint32 locktagField5 = Htoi(ltag[4]);
    uint32 locktagType = Htoi(ltag[5]);

    for (int i = 0; i < MAX_LOCKTAG_STRING; i++) {
         pfree_ext(ltag[i]);
    }
    pfree_ext(ltag);

    StringInfoData tag;
    initStringInfo(&tag);

    if (locktagType <= LOCKTAG_LAST_TYPE) {
        appendStringInfo(&tag, "locktype:%s, ", LockTagTypeNames[locktagType]);
    } else {
        appendStringInfo(&tag, "locktype:unknown %d, ", (int)locktagType);
    }

    switch ((LockTagType)locktagType) {
        case LOCKTAG_RELATION:
        case LOCKTAG_CSTORE_FREESPACE:
            appendStringInfo(&tag, "database:%u, relation:%u", locktagField1, locktagField2);
            break;
        case LOCKTAG_RELATION_EXTEND:
            appendStringInfo(&tag, "database:%u, relation:%u, bucket:%u",
                locktagField1, locktagField2, locktagField5);
            break;
        case LOCKTAG_PAGE:
            appendStringInfo(&tag, "database:%u, relation:%u, page:=%u, bucket:%u", locktagField1,
                locktagField2, locktagField3, locktagField5);
            break;
        case LOCKTAG_TUPLE:
            appendStringInfo(&tag, "database:%u, relation:%u, page:=%u, tuple:%u, bucket:%u", locktagField1,
                locktagField2, locktagField3, locktagField4, locktagField5);
            break;
        case LOCKTAG_UID:
            appendStringInfo(&tag, "database:%u, relation:%u, uid:%lu", locktagField1, locktagField2,
                (((uint64)locktagField3) << DISPLACEMENTS_VALUE) + locktagField4);
            break;
        case LOCKTAG_TRANSACTION:
            appendStringInfo(&tag, "transactionid:%lu", TransactionIdGetDatum(
                (TransactionId)locktagField1 | ((TransactionId)locktagField2 << DISPLACEMENTS_VALUE)));
            break;
        case LOCKTAG_VIRTUALTRANSACTION:
            appendStringInfo(&tag, "virtualxid:%u/%lu", locktagField1,
                (TransactionId)locktagField2 | ((TransactionId)locktagField3 << DISPLACEMENTS_VALUE));
            break;
        case LOCKTAG_OBJECT:
        case LOCKTAG_USERLOCK:
        case LOCKTAG_ADVISORY:
            /* act as the same as the default */
        default: /* treat unknown locktags like OBJECT */
            appendStringInfo(&tag, "database:%u, classid:%u, objid:%u, objsubid:%u", locktagField1, locktagField2,
                locktagField3, locktagField4);
            break;
    }  

    return tag.data;
}

Datum locktag_decode(PG_FUNCTION_ARGS)
{
    char* locktag = TextDatumGetCString(PG_GETARG_DATUM(0));
    char* tag = GetLocktagDecode(locktag);

    text* result = cstring_to_text(tag);
    pfree_ext(tag);
    PG_RETURN_TEXT_P(result);
}

Datum working_version_num(PG_FUNCTION_ARGS)
{
    PG_RETURN_UINT32(t_thrd.proc->workingVersionNum);
}

/*
 * @Description: get wait status info of all threads in local node.
 * @return: return the status of all threads.
 */
Datum pg_stat_get_status(PG_FUNCTION_ARGS)
{
    const int ATT_NUM = 16;
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    MemoryContext oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);

    Assert(STATE_WAIT_NUM == sizeof(WaitStateDesc) / sizeof(WaitStateDesc[0]));
    TupleDesc tupdesc = CreateTemplateTupleDesc(ATT_NUM, false);

    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_1, "node_name", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_2, "db_name", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_3, "thread_name", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_4, "query_id", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_5, "tid", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_6, "sessionid", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_7, "lwtid", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_8, "psessionid", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_9, "tlevel", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_10, "smpid", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_11, "wait_status", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_12, "wait_event", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_13, "locktag", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_14, "lockmode", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_15, "block_sessionid", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_16, "global_sessionid", TEXTOID, -1, 0);

    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->setDesc = BlessTupleDesc(tupdesc);

    bool hasTID = false;
    ThreadId tid = 0;
    if (!PG_ARGISNULL(0)) {
        hasTID = true;
        tid = PG_GETARG_INT64(0);
    }

    (void)gs_stat_read_current_status(rsinfo->setResult, rsinfo->setDesc, insert_pg_stat_get_status, hasTID, tid);

    MemoryContextSwitchTo(oldcontext);
    /* clean up and return the tuplestore */
    tuplestore_donestoring(rsinfo->setResult);
    return (Datum) 0;
}

void insert_pg_stat_get_status(Tuplestorestate *tupStore, TupleDesc tupDesc, const PgBackendStatus *beentry)
{
    const int ATT_NUM = 16;
    errno_t rc = 0;

    /* for each row */
    Datum values[ATT_NUM];
    bool nulls[ATT_NUM];

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    ConstructWaitStatus(beentry, values, nulls);
    tuplestore_putvalues(tupStore, tupDesc, values, nulls);
}

static void CheckVersion()
{
    if (t_thrd.proc->workingVersionNum < GLOBAL_SESSION_ID_VERSION) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            (errmsg("This view cannot be select during upgrade"))));
    }
}
/*
 * @Description: get wait status info of all threads in entire cluster.
 * @return: return the status of global threads.
 */
Datum pgxc_stat_get_status(PG_FUNCTION_ARGS)
{
    CheckVersion();
#ifndef ENABLE_MULTIPLE_NODES
    FuncCallContext* funcctx = NULL;
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        errmsg("unsupported view in single node mode.")));

    SRF_RETURN_DONE(funcctx);
#else
    if (IS_PGXC_DATANODE && !IS_SINGLE_NODE) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), 
            errmsg("pgxc view cannot be executed on datanodes!")));
    }
    Assert(STATE_WAIT_NUM == sizeof(WaitStateDesc) / sizeof(WaitStateDesc[0]));
    const int ATT_NUM = 16;
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    MemoryContext oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);

    /* build tupdesc for result tuples. */
    TupleDesc tupdesc = CreateTemplateTupleDesc(ATT_NUM, false);

    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_1, "node_name", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_2, "db_name", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_3, "thread_name", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_4, "query_id", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_5, "tid", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_6, "sessionid", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_7, "lwtid", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_8, "psessionid", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_9, "tlevel", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_10, "smpid", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_11, "wait_status", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_12, "wait_event", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_13, "locktag", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_14, "lockmode", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_15, "block_sessionid", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_16, "global_sessionid", TEXTOID, -1, 0);

    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->setDesc = BlessTupleDesc(tupdesc);

    (void)gs_stat_read_current_status(rsinfo->setResult, rsinfo->setDesc, insert_pg_stat_get_status);
    /* step2. fetch way for remote nodes. */
    ThreadWaitStatusInfo* threadWaitStatusInfo = getGlobalThreadWaitStatus(rsinfo->setDesc);
    MemoryContextSwitchTo(oldcontext);

    while (threadWaitStatusInfo != NULL) {
        Tuplestorestate* tupstore = threadWaitStatusInfo->state->tupstore;
        TupleTableSlot* slot = threadWaitStatusInfo->slot;

        if (!tuplestore_gettupleslot(tupstore, true, false, slot)) {
            FreeParallelFunctionState(threadWaitStatusInfo->state);
            ExecDropSingleTupleTableSlot(slot);
            pfree_ext(threadWaitStatusInfo);
            break;
        }

        tuplestore_puttupleslot(rsinfo->setResult, slot);
        ExecClearTuple(slot);
    }
    /* clean up and return the tuplestore */
    tuplestore_donestoring(rsinfo->setResult);
    return (Datum) 0;
#endif
}

/*
 * @Description: parallel get total thread wait status in entire cluster
 *               througn ExecRemoteFunctionInParallel in RemoteFunctionResultHandler.
 * @return: one tuple slot of result set.
 */
ThreadWaitStatusInfo* getGlobalThreadWaitStatus(TupleDesc tupleDesc)
{
    StringInfoData buf;
    ThreadWaitStatusInfo* threadsInfo = NULL;

    initStringInfo(&buf);
    threadsInfo = (ThreadWaitStatusInfo*)palloc0(sizeof(ThreadWaitStatusInfo));

    appendStringInfo(&buf, "SELECT * FROM pg_thread_wait_status;");

    threadsInfo->state = RemoteFunctionResultHandler(buf.data, NULL, NULL, true, EXEC_ON_ALL_NODES, true);
    threadsInfo->slot = MakeSingleTupleTableSlot(tupleDesc);

    /* clean up. */
    pfree_ext(buf.data);
    return threadsInfo;
}

/*
 * @Description: find the next exit user index in g_instance.stat_cxt.WaitCountStatusList.
 *			if find return the user`s index.
 *			if not, return 0.
 * @in1 - ordinary_userid : if it is the ordinary_userid who call the function, rutern 0.
 * @in2 - call_cntr : function 'pg_stat_get_sql_count' currently call times.
 * @in3 - max_calls : function 'pg_stat_get_sql_count' max call times.
 * @in4 - current_index : from current_index start to find
 * @out - int : if find return the user`s index, if not, return 0.
 */
static int find_next_user_idx(unsigned int ordinary_userid, int call_cntr, int max_calls, int current_index)
{
    int dataid = 0;
    int listNodeid = 0;
    int next_index = 0;
    ListCell* cell = NULL;
    ListCell* initcell = NULL;
    PgStat_WaitCountStatusCell* WaitCountStatusCellTmp = NULL;
    bool foundid = false;

    dataid = current_index % WAIT_COUNT_ARRAY_SIZE;
    listNodeid = current_index / WAIT_COUNT_ARRAY_SIZE;
    if (dataid == (WAIT_COUNT_ARRAY_SIZE - 1)) {
        listNodeid++;
        initcell = list_nth_cell(g_instance.stat_cxt.WaitCountStatusList, listNodeid);
        dataid = 0;
    } else {
        initcell = list_nth_cell(g_instance.stat_cxt.WaitCountStatusList, listNodeid);
        dataid++;
    }

    /* ordinary user can not select other`s count, and the function called times less than max_calls */
    if (!ordinary_userid && call_cntr + 1 < max_calls) {
        for_each_cell(cell, initcell)
        {
            WaitCountStatusCellTmp = (PgStat_WaitCountStatusCell*)lfirst(cell);
            for (; dataid < WAIT_COUNT_ARRAY_SIZE; dataid++) {
                if (WaitCountStatusCellTmp->WaitCountArray[dataid].userid != 0) {
                    if (CheckUserExist(WaitCountStatusCellTmp->WaitCountArray[dataid].userid, true)) {
                        foundid = true;
                        next_index = listNodeid * WAIT_COUNT_ARRAY_SIZE + dataid;
                        break;
                    }
                }
            }
            dataid = 0;
            listNodeid++;
            if (foundid)
                break;
        }
    }
    return next_index;
}

/*
 * @Description: get the mtharray user in nthcell in g_instance.stat_cxt.WaitCountStatusList
 * @in -current_index : user`s index in g_instance.stat_cxt.WaitCountStatusList
 * @out - PgStat_WaitCountStatus *wcb : the user sql count info
 */
static PgStat_WaitCountStatus* wcslist_nthcell_array(int current_index)
{
    int dataid = current_index % WAIT_COUNT_ARRAY_SIZE;
    int listNodeid = current_index / WAIT_COUNT_ARRAY_SIZE;
    PgStat_WaitCountStatus* wcb = NULL;
    PgStat_WaitCountStatusCell* WaitCountStatusCell = NULL;

    WaitCountStatusCell = (PgStat_WaitCountStatusCell*)list_nth(g_instance.stat_cxt.WaitCountStatusList, listNodeid);
    wcb = &WaitCountStatusCell->WaitCountArray[dataid];

    return wcb;
}
#define SET_AVG_TUPLE(total_time, count)                                            \
    do {                                                                            \
        values[++i] = Int64GetDatum((total_time) / (((count) == 0) ? 1 : (count))); \
    } while (0)
/*
 * @Description: system function for SQL count. When guc parameter 'pgstat_track_sql_count' is set on
 *			and user selcets view of pg_sql_count, the functoin wiil be called to find user`s SQL count
 *			results from g_instance.stat_cxt.WaitCountStatusList. If system admin selcet the view , the function will be
 *call many times to return all uers` SQL count results.
 * @in  - PG_FUNCTION_ARGS
 * @out - Datum
 */
Datum pg_stat_get_sql_count(PG_FUNCTION_ARGS)
{
    const int SQL_COUNT_ATTR = 26;
    FuncCallContext* funcctx = NULL;
    Oid ordinary_userid = 0;
    int ordinary_user_idx = 0;
    int i;
    MemoryContext oldcontext;

    /* the first call the function  */
    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tupdesc;

        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        i = 0;

        /* init the tuple description */
        tupdesc = CreateTemplateTupleDesc(SQL_COUNT_ATTR, false, TAM_HEAP);

        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "user_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "select_count", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "update_count", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "insert_count", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "delete_count", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "mergeinto_count", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "ddl_count", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "dml_count", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "dcl_count", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "total_select_elapse", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "avg_select_elapse", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "max_select_elapse", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "min_select_elapse", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "total_update_elapse", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "avg_update_elapse", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "max_update_elapse", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "min_update_elapse", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "total_insert_elapse", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "avg_insert_elapse", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "max_insert_elapse", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "min_insert_elapse", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "total_delete_elapse", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "avg_delete_elapse", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "max_delete_elapse", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "min_delete_elapse", INT8OID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->user_fctx = palloc0(sizeof(int));
        Oid userId = GetUserId();

        *(int*)(funcctx->user_fctx) = 0;
        if (u_sess->attr.attr_common.pgstat_track_activities == false ||
            u_sess->attr.attr_common.pgstat_track_sql_count == false || !CheckUserExist(userId, false)) {
            funcctx->max_calls = 0;
            if (u_sess->attr.attr_common.pgstat_track_activities == false) {
                /* track_activities is off and report error */
                ereport(LOG, (errcode(ERRCODE_WARNING), (errmsg("GUC parameter 'track_activities' is off"))));
            }
            if (u_sess->attr.attr_common.pgstat_track_sql_count == false) {
                /* track_sql_count is off and report error */
                ereport(LOG, (errcode(ERRCODE_WARNING), (errmsg("GUC parameter 'track_sql_count' is off"))));
            }
            pfree_ext(funcctx->user_fctx);
            MemoryContextSwitchTo(oldcontext);
            SRF_RETURN_DONE(funcctx);
        }

        /* if g_instance.stat_cxt.WaitCountHashTbl is null, re-initSqlCount */
        if (g_instance.stat_cxt.WaitCountHashTbl == NULL) {
            initSqlCount();
        }

        /* find the ordinary user`s  index in g_instance.stat_cxt.WaitCountStatusList */
        if (!superuser() && !isMonitoradmin(GetUserId())) {
            WaitCountHashValue* WaitCountIdx = NULL;
            LWLockAcquire(WaitCountHashLock, LW_SHARED);
            WaitCountIdx =
                (WaitCountHashValue*)hash_search(g_instance.stat_cxt.WaitCountHashTbl, &userId, HASH_FIND, NULL);
            if (WaitCountIdx != NULL) {
                ordinary_userid = userId;
                ordinary_user_idx = WaitCountIdx->idx;
            }
            LWLockRelease(WaitCountHashLock);
        }

        /* function max calls is the entry numbers of g_instance.stat_cxt.WaitCountHashTbl */
        funcctx->max_calls = ordinary_userid ? 1 : hash_get_num_entries(g_instance.stat_cxt.WaitCountHashTbl);

        MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->call_cntr < funcctx->max_calls) {
        /* for each row */
        Datum values[SQL_COUNT_ATTR];
        bool nulls[SQL_COUNT_ATTR] = {false};
        HeapTuple tuple = NULL;
        PgStat_WaitCountStatus* wcb = NULL;
        Oid userid;
        char user_name[NAMEDATALEN];
        errno_t ss_rc = EOK;

        int current_idx = ordinary_userid ? ordinary_user_idx : *(int*)(funcctx->user_fctx);
        i = -1;
        ss_rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(ss_rc, "\0", "\0");
        ss_rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(ss_rc, "\0", "\0");
        ss_rc = memset_s(user_name, NAMEDATALEN, 0, NAMEDATALEN);
        securec_check(ss_rc, "\0", "\0");

        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        LWLockAcquire(WaitCountHashLock, LW_SHARED);

        wcb = wcslist_nthcell_array(current_idx);

        /* if the user has been droped, then find the next exist user  */
        if (!CheckUserExist(wcb->userid, true)) {
            /* find the next exist user in g_instance.stat_cxt.WaitCountStatusList */
            *(int*)(funcctx->user_fctx) = find_next_user_idx(
                ordinary_userid, funcctx->call_cntr, funcctx->max_calls, *(int*)(funcctx->user_fctx));
            /* no user left, all exist users` SQL count results are return done */
            if (*(int*)(funcctx->user_fctx) == 0) {
                LWLockRelease(WaitCountHashLock);
                pfree_ext(funcctx->user_fctx);
                MemoryContextSwitchTo(oldcontext);
                SRF_RETURN_DONE(funcctx);
            }

            /* get the next exist user`s SQL count result */
            wcb = wcslist_nthcell_array(*(int*)(funcctx->user_fctx));
        }

        LWLockRelease(WaitCountHashLock);

        userid = wcb->userid;
        GetRoleName(userid, user_name, NAMEDATALEN);

        /* Values available to all callers */
        values[++i] = CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);
        values[++i] = CStringGetTextDatum(user_name);
        values[++i] = Int64GetDatum(wcb->wc_cnt.wc_sql_select);
        values[++i] = Int64GetDatum(wcb->wc_cnt.wc_sql_update);
        values[++i] = Int64GetDatum(wcb->wc_cnt.wc_sql_insert);
        values[++i] = Int64GetDatum(wcb->wc_cnt.wc_sql_delete);
        values[++i] = Int64GetDatum(wcb->wc_cnt.wc_sql_mergeinto);
        values[++i] = Int64GetDatum(wcb->wc_cnt.wc_sql_ddl);
        values[++i] = Int64GetDatum(wcb->wc_cnt.wc_sql_dml);
        values[++i] = Int64GetDatum(wcb->wc_cnt.wc_sql_dcl);
        values[++i] = Int64GetDatum(wcb->wc_cnt.selectElapse.total_time);
        SET_AVG_TUPLE(wcb->wc_cnt.selectElapse.total_time, wcb->wc_cnt.wc_sql_select);
        values[++i] = Int64GetDatum(wcb->wc_cnt.selectElapse.max_time);
        values[++i] = Int64GetDatum(wcb->wc_cnt.selectElapse.min_time);
        values[++i] = Int64GetDatum(wcb->wc_cnt.updateElapse.total_time);
        SET_AVG_TUPLE(wcb->wc_cnt.updateElapse.total_time, wcb->wc_cnt.wc_sql_update);
        values[++i] = Int64GetDatum(wcb->wc_cnt.updateElapse.max_time);
        values[++i] = Int64GetDatum(wcb->wc_cnt.updateElapse.min_time);
        values[++i] = Int64GetDatum(wcb->wc_cnt.insertElapse.total_time);
        SET_AVG_TUPLE(wcb->wc_cnt.insertElapse.total_time, wcb->wc_cnt.wc_sql_insert);
        values[++i] = Int64GetDatum(wcb->wc_cnt.insertElapse.max_time);
        values[++i] = Int64GetDatum(wcb->wc_cnt.insertElapse.min_time);
        values[++i] = Int64GetDatum(wcb->wc_cnt.deleteElapse.total_time);
        SET_AVG_TUPLE(wcb->wc_cnt.deleteElapse.total_time, wcb->wc_cnt.wc_sql_delete);
        values[++i] = Int64GetDatum(wcb->wc_cnt.deleteElapse.max_time);
        values[++i] = Int64GetDatum(wcb->wc_cnt.deleteElapse.min_time);

        /* find the next user for the next function call  */
        LWLockAcquire(WaitCountHashLock, LW_SHARED);

        (*(int*)(funcctx->user_fctx)) =
            find_next_user_idx(ordinary_userid, funcctx->call_cntr, funcctx->max_calls, *(int*)(funcctx->user_fctx));
        if ((*(int*)(funcctx->user_fctx)) == 0)
            funcctx->call_cntr = funcctx->max_calls;

        LWLockRelease(WaitCountHashLock);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);

        MemoryContextSwitchTo(oldcontext);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));

    } else {
        /* nothing left */
        pfree_ext(funcctx->user_fctx);
        SRF_RETURN_DONE(funcctx);
    }
}

Datum pg_stat_get_thread(PG_FUNCTION_ARGS)
{
    const int ATT_NUM = 5;
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    MemoryContext oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);

    TupleDesc tupdesc = CreateTemplateTupleDesc(ATT_NUM, false);
    /* This should have been called 'pid';  can't change it. 2011-06-11 */
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_1, "node_name", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_2, "pid", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_3, "lwpid", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_4, "thread_name", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_5, "creation_time", TIMESTAMPTZOID, -1, 0);

    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->setDesc = BlessTupleDesc(tupdesc);

    (void)gs_stat_read_current_status(rsinfo->setResult, rsinfo->setDesc, insert_pg_stat_get_thread);

    MemoryContextSwitchTo(oldcontext);
    /* clean up and return the tuplestore */
    tuplestore_donestoring(rsinfo->setResult);

    return (Datum) 0;
}

void insert_pg_stat_get_thread(Tuplestorestate *tupStore, TupleDesc tupDesc, const PgBackendStatus *beentry)
{
    const int ATT_NUM = 5;

    errno_t rc = 0;
    Datum values[ATT_NUM];
    bool nulls[ATT_NUM];

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    if (beentry == NULL || beentry->st_procpid == 0) {
        int i;

        for (i = 0; (unsigned int)(i) < sizeof(nulls) / sizeof(nulls[0]); i++)
            nulls[i] = true;

        nulls[ARR_3] = false;
        if (beentry == NULL)
            values[ARR_3] = CStringGetTextDatum("<backend information not available>");
        else {
            Assert(beentry->st_sessionid != 0);
            values[ARR_3] = CStringGetTextDatum("<decoupled session information not displayed>");
        }
        tuplestore_putvalues(tupStore, tupDesc, values, nulls);
        return;
    }

    /* Values available to all callers */
    values[ARR_0] = CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);

    /* Values only available to same user or superuser */
    if (superuser() || isMonitoradmin(GetUserId()) || beentry->st_userid == GetUserId()) {
        values[ARR_1] = Int64GetDatum(beentry->st_procpid);
        values[ARR_2] = Int32GetDatum(beentry->st_tid);
        if (beentry->st_appname)
            values[ARR_3] = CStringGetTextDatum(beentry->st_appname);
        else
            nulls[ARR_3] = true;
        if (beentry->st_state_start_timestamp != 0)
            values[ARR_4] = TimestampTzGetDatum(beentry->st_proc_start_timestamp);
        else
            nulls[ARR_4] = true;
    } else {
        /* No permissions to view data about this session */
        nulls[ARR_1] = true;
        nulls[ARR_2] = true;
        values[ARR_3] = CStringGetTextDatum("<insufficient privilege>");
        nulls[ARR_4] = true;
    }

    tuplestore_putvalues(tupStore, tupDesc, values, nulls);
}


void get_network_info(char** node_host, int* node_port)
{
#ifdef ENABLE_MULTIPLE_NODES
    Oid node_oid = InvalidOid;

    node_oid = get_pgxc_nodeoid(g_instance.attr.attr_common.PGXCNodeName);
    *node_host = get_pgxc_nodehost(node_oid);
    *node_port = get_pgxc_nodeport(node_oid);
#else
    if (strcmp("*", g_instance.attr.attr_network.ListenAddresses) == 0) {
        *node_host = "localhost";
    } else {
        *node_host = g_instance.attr.attr_network.ListenAddresses;
    }
    *node_port = g_instance.attr.attr_network.PostPortNumber;
#endif
    return;
}

Datum pg_stat_get_env(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;

    if (!superuser() && !isMonitoradmin(GetUserId())) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied to get system environment"),
                errhint("Must be system admin or monitor admin can get system environment.")));
    }

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        TupleDesc tupdesc;

        funcctx = SRF_FIRSTCALL_INIT();

        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(7, false, TAM_HEAP);
        /* This should have been called 'pid';  can't change it. 2011-06-11 */
        TupleDescInitEntry(tupdesc, (AttrNumber)1, "node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "host", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "process", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)4, "port", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)5, "installpath", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)6, "datapath", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)7, "log_directory", TEXTOID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        funcctx->user_fctx = palloc0(sizeof(int));
        funcctx->max_calls = 1;

        MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->call_cntr < funcctx->max_calls) {
        /* for each row */
        Datum values[7];
        bool nulls[7] = {false};
        HeapTuple tuple = NULL;
        char *node_host = NULL, *install_path = NULL, *data_path = NULL;
        int node_process = 0, node_port = 0;
        char self_path[MAXPGPATH] = {0}, real_path[MAXPGPATH] = {0};

        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        get_network_info(&node_host, &node_port);
        node_process = getpid();
        /* Get envirenment of GAUSSHOME firstly, if it is null, it should get from /proc/self/exe */
        install_path = gs_getenv_r("GAUSSHOME");
        if (NULL == install_path) {
            install_path = self_path;
            int r = readlink("/proc/self/exe", self_path, sizeof(self_path) - 1);
            if (r < 0 || r >= MAXPGPATH)
                install_path = "";
            else {
                int ret = 0;
                char* ptr = strrchr(self_path, '/');
                if (ptr != NULL) {
                    *ptr = '\0';
                    ret = strcat_s(self_path, MAXPGPATH - strlen(self_path), "/..");
                    securec_check_c(ret, "\0", "\0");
                    (void)realpath(self_path, real_path);
                    install_path = real_path;
                }
            }
        }
        check_backend_env(install_path);
        char* tmp_install_path = (char*)palloc(strlen(install_path) + 1);
        rc = strcpy_s(tmp_install_path, strlen(install_path) + 1, install_path);
        securec_check(rc, "\0", "\0");

        data_path = t_thrd.proc_cxt.DataDir;
        if (NULL == data_path)
            data_path = "";

        /* Values available to all callers */
        values[0] = CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);
        values[1] = CStringGetTextDatum(node_host);
        values[2] = Int32GetDatum(node_process);
        values[3] = Int32GetDatum(node_port);
        values[4] = CStringGetTextDatum(tmp_install_path);
        values[5] = CStringGetTextDatum(data_path);
        values[6] = CStringGetTextDatum(u_sess->attr.attr_common.Log_directory);

        pfree_ext(tmp_install_path);
        tmp_install_path = NULL;
        install_path = NULL;

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);

        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    } else {
        /* nothing left */
        SRF_RETURN_DONE(funcctx);
    }
}

Datum pg_backend_pid(PG_FUNCTION_ARGS)
{
    PG_RETURN_INT64(t_thrd.proc_cxt.MyProcPid);
}

Datum pg_current_userid(PG_FUNCTION_ARGS)
{
    PG_RETURN_OID(GetCurrentUserId());
}

Datum pg_current_sessionid(PG_FUNCTION_ARGS)
{
    char sessid[SESSION_ID_LEN];
    getSessionID(sessid,
        IS_THREAD_POOL_WORKER ? u_sess->proc_cxt.MyProcPort->SessionStartTime : t_thrd.proc_cxt.MyStartTime,
        IS_THREAD_POOL_WORKER ? u_sess->session_id : t_thrd.proc_cxt.MyProcPid);

    text* result = cstring_to_text(sessid);

    PG_RETURN_TEXT_P(result);
}

/*
 * Due to historical reasons, pg_current_sessionid is a concatenation of session start time
 * and session id. pg_current_sessid is for current backend's u_sess->session_id, which falls
 * back to pid under non thread pool mode.
 */
Datum pg_current_sessid(PG_FUNCTION_ARGS)
{
    PG_RETURN_INT64(IS_THREAD_POOL_WORKER ? u_sess->session_id : t_thrd.proc_cxt.MyProcPid);
}

Datum pg_stat_get_backend_pid(PG_FUNCTION_ARGS)
{
    int32 beid = PG_GETARG_INT32(0);
    PgBackendStatus* beentry = NULL;
    ThreadId st_procpid;

    if ((beentry = gs_stat_fetch_stat_beentry(beid)) == NULL) {
        PG_RETURN_NULL();
    }
    st_procpid = beentry->st_procpid;
    PG_RETURN_INT64(st_procpid);
}

Datum pg_stat_get_backend_dbid(PG_FUNCTION_ARGS)
{
    int32 beid = PG_GETARG_INT32(0);
    PgBackendStatus* beentry = NULL;
    Oid st_databaseid;

    if ((beentry = gs_stat_fetch_stat_beentry(beid)) == NULL) {
        PG_RETURN_NULL();
    }
    st_databaseid = beentry->st_databaseid;
    PG_RETURN_OID(st_databaseid);
}

Datum pg_stat_get_backend_userid(PG_FUNCTION_ARGS)
{
    int32 beid = PG_GETARG_INT32(0);
    PgBackendStatus* beentry = NULL;
    Oid st_userid;
    
    if ((beentry = gs_stat_fetch_stat_beentry(beid)) == NULL) {
        PG_RETURN_NULL();
    }
    if ((!superuser()) && (beentry->st_userid != GetUserId())) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("must be system admin or owner to use this function")));
    }
    st_userid = beentry->st_userid;
    PG_RETURN_OID(st_userid);
}

Datum pg_stat_get_backend_activity(PG_FUNCTION_ARGS)
{
    int32 beid = PG_GETARG_INT32(0);
    PgBackendStatus* beentry = NULL;
    const char* activity = NULL;
    text *result = NULL;
    if ((beentry = gs_stat_fetch_stat_beentry(beid)) == NULL)
        activity = "<backend information not available>";
    else if (!superuser() && beentry->st_userid != GetUserId())
        activity = "<insufficient privilege>";
    else if (*(beentry->st_activity) == '\0')
        activity = "<command string not enabled>";
    else
        activity = beentry->st_activity;

    result = cstring_to_text(activity);

    PG_RETURN_TEXT_P(result);
}

Datum pg_stat_get_backend_waiting(PG_FUNCTION_ARGS)
{
    int32 beid = PG_GETARG_INT32(0);
    bool result = false;
    PgBackendStatus* beentry = NULL;

    if ((beentry = gs_stat_fetch_stat_beentry(beid)) == NULL)
        PG_RETURN_NULL();

    if (!superuser() && beentry->st_userid != GetUserId())
        PG_RETURN_NULL();

    result = pgstat_get_waitlock(beentry->st_waitevent);
    PG_RETURN_BOOL(result);
}

Datum pg_stat_get_backend_activity_start(PG_FUNCTION_ARGS)
{
    int32 beid = PG_GETARG_INT32(0);
    TimestampTz result;
    PgBackendStatus* beentry = NULL;

    if ((beentry = gs_stat_fetch_stat_beentry(beid)) == NULL)
        PG_RETURN_NULL();

    if (!superuser() && beentry->st_userid != GetUserId())
        PG_RETURN_NULL();

    result = beentry->st_activity_start_timestamp;
    /*
     * No time recorded for start of current query -- this is the case if the
     * user hasn't enabled query-level stats collection.
     */
    if (result == 0)
        PG_RETURN_NULL();

    PG_RETURN_TIMESTAMPTZ(result);
}

Datum pg_stat_get_backend_xact_start(PG_FUNCTION_ARGS)
{
    int32 beid = PG_GETARG_INT32(0);
    TimestampTz result;
    PgBackendStatus* beentry = NULL;

    if ((beentry = gs_stat_fetch_stat_beentry(beid)) == NULL)
        PG_RETURN_NULL();

    if (!superuser() && beentry->st_userid != GetUserId()) {
        PG_RETURN_NULL();
    }
    result = beentry->st_xact_start_timestamp;

    if (result == 0) /* not in a transaction */
        PG_RETURN_NULL();

    PG_RETURN_TIMESTAMPTZ(result);
}

Datum pg_stat_get_backend_start(PG_FUNCTION_ARGS)
{
    int32 beid = PG_GETARG_INT32(0);
    TimestampTz result;
    PgBackendStatus* beentry = NULL;

    if ((beentry = gs_stat_fetch_stat_beentry(beid)) == NULL)
        PG_RETURN_NULL();

    if (!superuser() && beentry->st_userid != GetUserId()) {
        PG_RETURN_NULL();
    }

    result = beentry->st_proc_start_timestamp;

    if (result == 0) /* probably can't happen? */
        PG_RETURN_NULL();

    PG_RETURN_TIMESTAMPTZ(result);
}

Datum pg_stat_get_backend_client_addr(PG_FUNCTION_ARGS)
{
    int32 beid = PG_GETARG_INT32(0);
    PgBackendStatus* beentry = NULL;
    SockAddr zero_clientaddr;
    char remote_host[NI_MAXHOST];
    int ret;
    errno_t rc = 0;

    if ((beentry = gs_stat_fetch_stat_beentry(beid)) == NULL)
        PG_RETURN_NULL();

    if (!superuser() && beentry->st_userid != GetUserId()) {
        PG_RETURN_NULL();
    }

    /* A zeroed client addr means we don't know */
    rc = memset_s(&zero_clientaddr, sizeof(zero_clientaddr), 0, sizeof(zero_clientaddr));
    securec_check(rc, "\0", "\0");
    if (memcmp(&(beentry->st_clientaddr), &zero_clientaddr, sizeof(zero_clientaddr)) == 0) {
        PG_RETURN_NULL();
    }
    switch (beentry->st_clientaddr.addr.ss_family) {
        case AF_INET:
#ifdef HAVE_IPV6
        case AF_INET6:
#endif
            break;
        default:            
            PG_RETURN_NULL();
            break;
    }

    remote_host[0] = '\0';
    ret = pg_getnameinfo_all(&beentry->st_clientaddr.addr,
        beentry->st_clientaddr.salen,
        remote_host,
        sizeof(remote_host),
        NULL,
        0,
        NI_NUMERICHOST | NI_NUMERICSERV);
    if (ret != 0) {
        PG_RETURN_NULL();
    }
    clean_ipv6_addr(beentry->st_clientaddr.addr.ss_family, remote_host);
    PG_RETURN_INET_P(DirectFunctionCall1(inet_in, CStringGetDatum(remote_host)));
}

Datum pg_stat_get_backend_client_port(PG_FUNCTION_ARGS)
{
    int32 beid = PG_GETARG_INT32(0);
    PgBackendStatus* beentry = NULL;
    SockAddr zero_clientaddr;
    char remote_port[NI_MAXSERV];
    int ret;

    if ((beentry = gs_stat_fetch_stat_beentry(beid)) == NULL)
        PG_RETURN_NULL();

    if (!superuser() && beentry->st_userid != GetUserId()) {
        PG_RETURN_NULL();
    }
    /* A zeroed client addr means we don't know */
    errno_t rc = memset_s(&zero_clientaddr, sizeof(zero_clientaddr), 0, sizeof(zero_clientaddr));
    securec_check(rc, "\0", "\0");
    if (memcmp(&(beentry->st_clientaddr), &zero_clientaddr, sizeof(zero_clientaddr)) == 0) {
        PG_RETURN_NULL();
    }
    switch (beentry->st_clientaddr.addr.ss_family) {
        case AF_INET:
#ifdef HAVE_IPV6
        case AF_INET6:
#endif
            break;
        case AF_UNIX:            
            PG_RETURN_INT32(-1);
        default:
            PG_RETURN_NULL();
            break;
    }

    remote_port[0] = '\0';
    ret = pg_getnameinfo_all(&beentry->st_clientaddr.addr,
        beentry->st_clientaddr.salen,
        NULL,
        0,
        remote_port,
        sizeof(remote_port),
        NI_NUMERICHOST | NI_NUMERICSERV);
    if (ret != 0) {
        PG_RETURN_NULL();
    }
    PG_RETURN_DATUM(DirectFunctionCall1(int4in, CStringGetDatum(remote_port)));
}

Datum pg_stat_get_db_numbackends(PG_FUNCTION_ARGS)
{
    Oid dbid = PG_GETARG_OID(0);
    int32 result = 0;
    int before_changecount = 0;
    int after_changecount = 0;
    PgBackendStatus *beentry = t_thrd.shemem_ptr_cxt.BackendStatusArray + BackendStatusArray_size - 1;

    for (int i = 1; i <= BackendStatusArray_size; i++) {
        for (;;) {
            bool is_same_db = false;
            pgstat_save_changecount_before(beentry, before_changecount);
            if ((beentry->st_procpid > 0 || beentry->st_sessionid > 0) && beentry->st_databaseid == dbid) {
                is_same_db = true;
            }

            pgstat_save_changecount_after(beentry, after_changecount);
            if (before_changecount == after_changecount && ((uint)before_changecount & 1) == 0) {
                if (is_same_db) {
                    result++;
                }
                beentry--;
                break;
            }
            CHECK_FOR_INTERRUPTS();
        }
    }
    
    PG_RETURN_INT32(result);
}

Datum pg_stat_get_db_xact_commit(PG_FUNCTION_ARGS)
{
    Oid dbid = PG_GETARG_OID(0);
    int64 result;
    PgStat_StatDBEntry* dbentry = NULL;

    if ((dbentry = pgstat_fetch_stat_dbentry(dbid)) == NULL)
        result = 0;
    else
        result = (int64)(dbentry->n_xact_commit);

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_db_xact_rollback(PG_FUNCTION_ARGS)
{
    Oid dbid = PG_GETARG_OID(0);
    int64 result;
    PgStat_StatDBEntry* dbentry = NULL;

    if ((dbentry = pgstat_fetch_stat_dbentry(dbid)) == NULL)
        result = 0;
    else
        result = (int64)(dbentry->n_xact_rollback);

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_db_blocks_fetched(PG_FUNCTION_ARGS)
{
    Oid dbid = PG_GETARG_OID(0);
    int64 result;
    PgStat_StatDBEntry* dbentry = NULL;

    if ((dbentry = pgstat_fetch_stat_dbentry(dbid)) == NULL)
        result = 0;
    else
        result = (int64)(dbentry->n_blocks_fetched);

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_db_blocks_hit(PG_FUNCTION_ARGS)
{
    Oid dbid = PG_GETARG_OID(0);
    int64 result;
    PgStat_StatDBEntry* dbentry = NULL;

    if ((dbentry = pgstat_fetch_stat_dbentry(dbid)) == NULL)
        result = 0;
    else
        result = (int64)(dbentry->n_blocks_hit);

    PG_RETURN_INT64(result);
}

/**
 * @Description:  get database cu stat mem hit
 * @return  datum
 */
Datum pg_stat_get_db_cu_mem_hit(PG_FUNCTION_ARGS)
{
    Oid dbid = PG_GETARG_OID(0);
    int64 result;
    PgStat_StatDBEntry* dbentry = NULL;

    if ((dbentry = pgstat_fetch_stat_dbentry(dbid)) == NULL)
        result = 0;
    else
        result = (int64)(dbentry->n_cu_mem_hit);

    PG_RETURN_INT64(result);
}

/**
 * @Description:  get database cu stat hdd sync read
 * @return  datum
 */
Datum pg_stat_get_db_cu_hdd_sync(PG_FUNCTION_ARGS)
{
    Oid dbid = PG_GETARG_OID(0);
    int64 result;
    PgStat_StatDBEntry* dbentry = NULL;

    if ((dbentry = pgstat_fetch_stat_dbentry(dbid)) == NULL)
        result = 0;
    else
        result = (int64)(dbentry->n_cu_hdd_sync);

    PG_RETURN_INT64(result);
}

/**
 * @Description:  get database cu stat hdd async read
 * @return  datum
 */
Datum pg_stat_get_db_cu_hdd_asyn(PG_FUNCTION_ARGS)
{
    Oid dbid = PG_GETARG_OID(0);
    int64 result;
    PgStat_StatDBEntry* dbentry = NULL;

    if ((dbentry = pgstat_fetch_stat_dbentry(dbid)) == NULL)
        result = 0;
    else
        result = (int64)(dbentry->n_cu_hdd_asyn);

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_db_tuples_returned(PG_FUNCTION_ARGS)
{
    Oid dbid = PG_GETARG_OID(0);
    int64 result;
    PgStat_StatDBEntry* dbentry = NULL;

    if ((dbentry = pgstat_fetch_stat_dbentry(dbid)) == NULL)
        result = 0;
    else
        result = (int64)(dbentry->n_tuples_returned);

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_db_tuples_fetched(PG_FUNCTION_ARGS)
{
    Oid dbid = PG_GETARG_OID(0);
    int64 result;
    PgStat_StatDBEntry* dbentry = NULL;

    if ((dbentry = pgstat_fetch_stat_dbentry(dbid)) == NULL)
        result = 0;
    else
        result = (int64)(dbentry->n_tuples_fetched);

    PG_RETURN_INT64(result);
}

/* fetch tuples stat info for total database */
static int64 pgxc_exec_db_stat(Oid dbid, char* funcname, RemoteQueryExecType exec_type)
{
    ExecNodes* exec_nodes = NULL;
    StringInfoData buf;
    ParallelFunctionState* state = NULL;
    int64 result = 0;
    char* databasename = NULL;

    databasename = get_database_name(dbid);
    if (databasename == NULL) {
        /* have been dropped */
        return 0;
    }

    databasename = repairObjectName(databasename);

    exec_nodes = (ExecNodes*)makeNode(ExecNodes);
    exec_nodes->baselocatortype = LOCATOR_TYPE_HASH;
    exec_nodes->accesstype = RELATION_ACCESS_READ;
    exec_nodes->primarynodelist = NIL;
    exec_nodes->en_expr = NULL;
    exec_nodes->en_relid = InvalidOid;
    exec_nodes->nodeList = NIL;

    initStringInfo(&buf);
    appendStringInfo(&buf, "SELECT pg_catalog.%s(oid) from pg_database where datname='%s';", funcname, databasename);
    state = RemoteFunctionResultHandler(buf.data, exec_nodes, NULL, true, exec_type, true);
    compute_tuples_stat(state, false);
    result = state->result;

    pfree_ext(databasename);
    pfree_ext(buf.data);
    FreeParallelFunctionState(state);

    return result;
}

Datum pg_stat_get_db_tuples_inserted(PG_FUNCTION_ARGS)
{
    Oid dbid = PG_GETARG_OID(0);
    int64 result = 0;
    PgStat_StatDBEntry* dbentry = NULL;

    if (IS_PGXC_COORDINATOR)
        PG_RETURN_INT64(pgxc_exec_db_stat(dbid, "pg_stat_get_db_tuples_inserted", EXEC_ON_DATANODES));

    if (NULL != (dbentry = pgstat_fetch_stat_dbentry(dbid)))
        result = (int64)(dbentry->n_tuples_inserted);

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_db_tuples_updated(PG_FUNCTION_ARGS)
{
    Oid dbid = PG_GETARG_OID(0);
    int64 result = 0;
    PgStat_StatDBEntry* dbentry = NULL;

    if (IS_PGXC_COORDINATOR)
        PG_RETURN_INT64(pgxc_exec_db_stat(dbid, "pg_stat_get_db_tuples_updated", EXEC_ON_DATANODES));

    if (NULL != (dbentry = pgstat_fetch_stat_dbentry(dbid)))
        result = (int64)(dbentry->n_tuples_updated);

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_db_tuples_deleted(PG_FUNCTION_ARGS)
{
    Oid dbid = PG_GETARG_OID(0);
    int64 result = 0;
    PgStat_StatDBEntry* dbentry = NULL;

    if (IS_PGXC_COORDINATOR)
        PG_RETURN_INT64(pgxc_exec_db_stat(dbid, "pg_stat_get_db_tuples_deleted", EXEC_ON_DATANODES));

    if (NULL != (dbentry = pgstat_fetch_stat_dbentry(dbid)))
        result = (int64)(dbentry->n_tuples_deleted);

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_db_stat_reset_time(PG_FUNCTION_ARGS)
{
    Oid dbid = PG_GETARG_OID(0);
    TimestampTz result = 0;
    PgStat_StatDBEntry* dbentry = NULL;

    if (NULL != (dbentry = pgstat_fetch_stat_dbentry(dbid)))
        result = dbentry->stat_reset_timestamp;

    if (result == 0)
        PG_RETURN_NULL();
    else
        PG_RETURN_TIMESTAMPTZ(result);
}

Datum pg_stat_get_db_temp_files(PG_FUNCTION_ARGS)
{
    Oid dbid = PG_GETARG_OID(0);
    int64 result;
    PgStat_StatDBEntry* dbentry = NULL;

    if ((dbentry = pgstat_fetch_stat_dbentry(dbid)) == NULL)
        result = 0;
    else
        result = dbentry->n_temp_files;

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_db_temp_bytes(PG_FUNCTION_ARGS)
{
    Oid dbid = PG_GETARG_OID(0);
    int64 result;
    PgStat_StatDBEntry* dbentry = NULL;

    if ((dbentry = pgstat_fetch_stat_dbentry(dbid)) == NULL)
        result = 0;
    else
        result = dbentry->n_temp_bytes;

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_db_conflict_tablespace(PG_FUNCTION_ARGS)
{
    Oid dbid = PG_GETARG_OID(0);
    int64 result = 0;
    PgStat_StatDBEntry* dbentry = NULL;

    if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
        result = pgxc_exec_db_stat(dbid, "pg_stat_get_db_conflict_tablespace", EXEC_ON_ALL_NODES);

    if (NULL != (dbentry = pgstat_fetch_stat_dbentry(dbid)))
        result += (int64)(dbentry->n_conflict_tablespace);

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_db_conflict_lock(PG_FUNCTION_ARGS)
{
    Oid dbid = PG_GETARG_OID(0);
    int64 result = 0;
    PgStat_StatDBEntry* dbentry = NULL;

    if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
        result = pgxc_exec_db_stat(dbid, "pg_stat_get_db_conflict_lock", EXEC_ON_ALL_NODES);

    if (NULL != (dbentry = pgstat_fetch_stat_dbentry(dbid)))
        result += (int64)(dbentry->n_conflict_lock);

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_db_conflict_snapshot(PG_FUNCTION_ARGS)
{
    Oid dbid = PG_GETARG_OID(0);
    int64 result = 0;
    PgStat_StatDBEntry* dbentry = NULL;

    if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
        result = pgxc_exec_db_stat(dbid, "pg_stat_get_db_conflict_snapshot", EXEC_ON_ALL_NODES);

    if (NULL != (dbentry = pgstat_fetch_stat_dbentry(dbid)))
        result += (int64)(dbentry->n_conflict_snapshot);

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_db_conflict_bufferpin(PG_FUNCTION_ARGS)
{
    Oid dbid = PG_GETARG_OID(0);
    int64 result = 0;
    PgStat_StatDBEntry* dbentry = NULL;

    if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
        result = pgxc_exec_db_stat(dbid, "pg_stat_get_db_conflict_bufferpin", EXEC_ON_ALL_NODES);

    if (NULL != (dbentry = pgstat_fetch_stat_dbentry(dbid)))
        result += (int64)(dbentry->n_conflict_bufferpin);

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_db_conflict_startup_deadlock(PG_FUNCTION_ARGS)
{
    Oid dbid = PG_GETARG_OID(0);
    int64 result = 0;
    PgStat_StatDBEntry* dbentry = NULL;

    if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
        result = pgxc_exec_db_stat(dbid, "pg_stat_get_db_conflict_startup_deadlock", EXEC_ON_ALL_NODES);

    if (NULL != (dbentry = pgstat_fetch_stat_dbentry(dbid)))
        result += (int64)(dbentry->n_conflict_startup_deadlock);

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_db_conflict_all(PG_FUNCTION_ARGS)
{
    Oid dbid = PG_GETARG_OID(0);
    int64 result = 0;
    PgStat_StatDBEntry* dbentry = NULL;

    if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
        result = pgxc_exec_db_stat(dbid, "pg_stat_get_db_conflict_all", EXEC_ON_ALL_NODES);

    if (NULL != (dbentry = pgstat_fetch_stat_dbentry(dbid)))
        result += (int64)(dbentry->n_conflict_tablespace + dbentry->n_conflict_lock + dbentry->n_conflict_snapshot +
                          dbentry->n_conflict_bufferpin + dbentry->n_conflict_startup_deadlock);

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_db_deadlocks(PG_FUNCTION_ARGS)
{
    Oid dbid = PG_GETARG_OID(0);
    int64 result = 0;
    PgStat_StatDBEntry* dbentry = NULL;

    if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
        result = pgxc_exec_db_stat(dbid, "pg_stat_get_db_deadlocks", EXEC_ON_ALL_NODES);

    if (NULL != (dbentry = pgstat_fetch_stat_dbentry(dbid)))
        result += (int64)(dbentry->n_deadlocks);

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_db_blk_read_time(PG_FUNCTION_ARGS)
{
    Oid dbid = PG_GETARG_OID(0);
    double result;
    PgStat_StatDBEntry* dbentry = NULL;

    /* convert counter from microsec to millisec for display */
    if ((dbentry = pgstat_fetch_stat_dbentry(dbid)) == NULL)
        result = 0;
    else
        result = ((double)dbentry->n_block_read_time) / 1000.0;

    PG_RETURN_FLOAT8(result);
}

Datum pg_stat_get_db_blk_write_time(PG_FUNCTION_ARGS)
{
    Oid dbid = PG_GETARG_OID(0);
    double result;
    PgStat_StatDBEntry* dbentry = NULL;

    /* convert counter from microsec to millisec for display */
    if ((dbentry = pgstat_fetch_stat_dbentry(dbid)) == NULL)
        result = 0;
    else
        result = ((double)dbentry->n_block_write_time) / 1000.0;

    PG_RETURN_FLOAT8(result);
}

Datum pg_stat_get_mem_mbytes_reserved(PG_FUNCTION_ARGS)
{
    ThreadId pid = PG_GETARG_INT64(0);
    const char* result = "GetFailed";

    if (pid == 0)
        pid = (IS_THREAD_POOL_WORKER ? u_sess->session_id : t_thrd.proc_cxt.MyProcPid);

    StringInfoData buf;

    if (pid <= 0)
        PG_RETURN_TEXT_P(cstring_to_text(result));

    initStringInfo(&buf);

    /*get backend status with thread id*/
    PgBackendStatus* beentry = NULL;
    PgBackendStatusNode* node = gs_stat_read_current_status(NULL);
    while (node != NULL) {
        PgBackendStatus* tmpBeentry = node->data;
        if ((tmpBeentry != NULL) && (tmpBeentry->st_sessionid == pid)) {
            beentry = tmpBeentry;
            break;
        }
        node = node->next;
    }

    if (beentry != NULL) {
        if (beentry->st_connect_info) {
            PGXCNodeConnectionInfo* info = (PGXCNodeConnectionInfo*)beentry->st_connect_info;

            double consume_dn = (double)(info->dn_connect_time * 1.0) / MSECS_PER_SEC;
            double consume_cn = (double)(info->cn_connect_time * 1.0) / MSECS_PER_SEC;

            appendStringInfo(&buf,
                _("ConnectInfo: datanodes elapsed time: %.3fms, "
                  "coordinators elapsed time: %.3fms, total time: %.3fms\n"),
                consume_dn,
                consume_cn,
                consume_dn + consume_cn);
        }

        if (beentry->st_debug_info)
            WLMGetDebugInfo(&buf, (WLMDebugInfo*)beentry->st_debug_info);

        if (beentry->st_cgname)
            appendStringInfo(&buf, _("ControlGroup: %s\n"), beentry->st_cgname);

        appendStringInfo(&buf, _("IOSTATE: %d\n"), beentry->st_io_state);

        result = buf.data;
    }

    PG_RETURN_TEXT_P(cstring_to_text(result));
}

/* print the data structure info of workload manager */
Datum pg_stat_get_workload_struct_info(PG_FUNCTION_ARGS)
{
    text* result = NULL;

    if (!superuser()) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_PROC,
            "pg_stat_get_workload_struct_info");
    }

    if (!ENABLE_WORKLOAD_CONTROL) {
        const char* tmpresult = "FailedToGetSessionInfo";
        PG_RETURN_TEXT_P(cstring_to_text(tmpresult));
    }

    StringInfoData buf;

    initStringInfo(&buf);

    /* get the structure info */
    WLMGetWorkloadStruct(&buf);

    result = cstring_to_text(buf.data);

    pfree_ext(buf.data);

    PG_RETURN_TEXT_P(result);
}

/*
 * function name: pg_stat_get_realtime_info_internal
 * description  : Called by pg_stat_get_wlm_realtime_session_info
 *                as internal function. Fet collect info from
 *                each data nodes.
 */
Datum pg_stat_get_realtime_info_internal(PG_FUNCTION_ARGS)
{
    char* result = NULL;

    if (!ENABLE_WORKLOAD_CONTROL) {
        result = "FailedToGetSessionInfo";
        PG_RETURN_TEXT_P(cstring_to_text(result));
    }

    if (result == NULL)
        result = "FailedToGetSessionInfo";

    PG_RETURN_TEXT_P(cstring_to_text(result));
}

/*
 * function name: pg_stat_get_wlm_realtime_session_info
 * description  : the view will show real time resource in use.
 */
Datum pg_stat_get_wlm_realtime_session_info(PG_FUNCTION_ARGS)
{
    const int WLM_REALTIME_SESSION_INFO_ATTRNUM = 56;
    FuncCallContext* funcctx = NULL;
    int num = 0, i = 0;

    if (!superuser()) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_PROC,
            "pg_stat_get_wlm_realtime_session_info");
    }

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        TupleDesc tupdesc;
        i = 0;

        funcctx = SRF_FIRSTCALL_INIT();

        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(WLM_REALTIME_SESSION_INFO_ATTRNUM, false, TAM_HEAP);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "nodename", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "threadid", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "block_time", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "duration", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "estimate_total_time", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "estimate_left_time", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "schemaname", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "query_band", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "SpillInfo", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "control_group", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "estimate_memory", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "MinPeakMemory", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "MaxPeakMemory", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "average_peak_memory", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "memory_skew_percent", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "min_spill_size", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "max_spill_size", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "average_spill_size", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "spill_skew_percent", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "min_dn_time", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "max_dn_time", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "average_dn_time", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "dntime_skew_percent", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "MinCpuTime", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "MaxCpuTime", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "TotalCpuTime", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "cpu_skew_percent", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "min_peak_iops", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "max_peak_iops", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "average_peak_iops", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "iops_skew_percent", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "warning", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "query", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "query_plan", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "cpu_top1_node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "cpu_top2_node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "cpu_top3_node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "cpu_top4_node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "cpu_top5_node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "mem_top1_node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "mem_top2_node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "mem_top3_node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "mem_top4_node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "mem_top5_node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "cpu_top1_value", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "cpu_top2_value", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "cpu_top3_value", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "cpu_top4_value", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "cpu_top5_value", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "mem_top1_value", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "mem_top2_value", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "mem_top3_value", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "mem_top4_value", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "mem_top5_value", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "top_mem_dn", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "top_cpu_dn", TEXTOID, -1, 0);
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->user_fctx = WLMGetSessionStatistics(&num);
        funcctx->max_calls = num;

        MemoryContextSwitchTo(oldcontext);

        if (funcctx->user_fctx == NULL)
            SRF_RETURN_DONE(funcctx);
    }

    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->user_fctx && funcctx->call_cntr < funcctx->max_calls) {
        const int TOP5 = 5;
        Datum values[WLM_REALTIME_SESSION_INFO_ATTRNUM];
        bool nulls[WLM_REALTIME_SESSION_INFO_ATTRNUM] = {false};
        HeapTuple tuple = NULL;
        Datum result;
        i = -1;

        char warnstr[WARNING_INFO_LEN] = {0};

        WLMSessionStatistics* statistics = (WLMSessionStatistics*)funcctx->user_fctx + funcctx->call_cntr;

        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        /* get warn info string */
        WLMGetWarnInfo(warnstr,
            sizeof(warnstr),
            statistics->gendata.warning,
            statistics->gendata.spillData.max_value,
            statistics->gendata.broadcastData.max_value,
            statistics);

        int64 estimate_left_time =
            (statistics->estimate_time > statistics->duration) ? statistics->estimate_time - statistics->duration : 0;

        values[++i] = CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);
        values[++i] = Int64GetDatum(statistics->qid.queryId);
        values[++i] = Int64GetDatum(statistics->block_time);
        values[++i] = Int64GetDatum(statistics->duration);
        values[++i] = Int64GetDatum(statistics->estimate_time);
        values[++i] = Int64GetDatum(estimate_left_time);
        values[++i] = CStringGetTextDatum(statistics->schname);
        if (statistics->query_band && statistics->query_band[0])
            values[++i] = CStringGetTextDatum(statistics->query_band);
        else
            nulls[++i] = true;
        values[++i] = CStringGetTextDatum(statistics->spillInfo);
        values[++i] = CStringGetTextDatum(statistics->cgroup);
        values[++i] = Int32GetDatum(statistics->estimate_memory);
        values[++i] = Int32GetDatum(statistics->gendata.usedMemoryData.min_value);
        values[++i] = Int32GetDatum(statistics->gendata.usedMemoryData.max_value);
        values[++i] = Int32GetDatum(statistics->gendata.usedMemoryData.avg_value);
        values[++i] = Int32GetDatum(statistics->gendata.usedMemoryData.skew_percent);
        values[++i] = Int32GetDatum(statistics->gendata.spillData.min_value);
        values[++i] = Int32GetDatum(statistics->gendata.spillData.max_value);
        values[++i] = Int32GetDatum(statistics->gendata.spillData.avg_value);
        values[++i] = Int32GetDatum(statistics->gendata.spillData.skew_percent);
        values[++i] = Int64GetDatum(statistics->gendata.dnTimeData.min_value);
        values[++i] = Int64GetDatum(statistics->gendata.dnTimeData.max_value);
        values[++i] = Int64GetDatum(statistics->gendata.dnTimeData.avg_value);
        values[++i] = Int32GetDatum(statistics->gendata.dnTimeData.skew_percent);
        values[++i] = Int64GetDatum(statistics->gendata.cpuData.min_value);
        values[++i] = Int64GetDatum(statistics->gendata.cpuData.max_value);
        values[++i] = Int64GetDatum(statistics->gendata.cpuData.total_value);
        values[++i] = Int32GetDatum(statistics->gendata.cpuData.skew_percent);
        values[++i] = Int32GetDatum(statistics->gendata.peakIopsData.min_value);
        values[++i] = Int32GetDatum(statistics->gendata.peakIopsData.max_value);
        values[++i] = Int32GetDatum(statistics->gendata.peakIopsData.avg_value);
        values[++i] = Int32GetDatum(statistics->gendata.peakIopsData.skew_percent);
        if (warnstr[0])
            values[++i] = CStringGetTextDatum(warnstr);
        else
            nulls[++i] = true;
        values[++i] = CStringGetTextDatum(statistics->statement);
        values[++i] = CStringGetTextDatum(statistics->query_plan);
        int CPU[TOP5] = {-1};
        int MEM[TOP5] = {-1};
        char CPUName[TOP5][NAMEDATALEN] = {{0}};
        char MEMName[TOP5][NAMEDATALEN] = {{0}};
        for (int j = 0; j < TOP5; j++) {
            rc = memset_s(CPUName[j], NAMEDATALEN, 0, NAMEDATALEN);
            securec_check(rc, "\0", "\0");
            rc = memset_s(MEMName[j], NAMEDATALEN, 0, NAMEDATALEN);
            securec_check(rc, "\0", "\0");
        }
        GetTopNInfo(statistics->gendata.WLMCPUTopDnInfo, CPUName, CPU, TOP5);
        GetTopNInfo(statistics->gendata.WLMMEMTopDnInfo, MEMName, MEM, TOP5);

        values[++i] = CStringGetTextDatum(CPUName[0]);
        values[++i] = CStringGetTextDatum(CPUName[1]);
        values[++i] = CStringGetTextDatum(CPUName[2]);
        values[++i] = CStringGetTextDatum(CPUName[3]);
        values[++i] = CStringGetTextDatum(CPUName[4]);
        values[++i] = CStringGetTextDatum(MEMName[0]);
        values[++i] = CStringGetTextDatum(MEMName[1]);
        values[++i] = CStringGetTextDatum(MEMName[2]);
        values[++i] = CStringGetTextDatum(MEMName[3]);
        values[++i] = CStringGetTextDatum(MEMName[4]);
        values[++i] = Int64GetDatum(CPU[0]);
        values[++i] = Int64GetDatum(CPU[1]);
        values[++i] = Int64GetDatum(CPU[2]);
        values[++i] = Int64GetDatum(CPU[3]);
        values[++i] = Int64GetDatum(CPU[4]);
        values[++i] = Int64GetDatum(MEM[0]);
        values[++i] = Int64GetDatum(MEM[1]);
        values[++i] = Int64GetDatum(MEM[2]);
        values[++i] = Int64GetDatum(MEM[3]);
        values[++i] = Int64GetDatum(MEM[4]);
        StringInfoData CputopNstr;
        StringInfoData MemtopNstr;
        initStringInfo(&CputopNstr);
        initStringInfo(&MemtopNstr);
        GetTopNInfoJsonFormat(CputopNstr, CPUName, CPU, TOP5);
        GetTopNInfoJsonFormat(MemtopNstr, MEMName, MEM, TOP5);
        values[++i] = CStringGetTextDatum(MemtopNstr.data);
        values[++i] = CStringGetTextDatum(CputopNstr.data);
        pfree_ext(MemtopNstr.data);
        pfree_ext(CputopNstr.data);
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, result);
    }

    pfree_ext(funcctx->user_fctx);
    funcctx->user_fctx = NULL;

    SRF_RETURN_DONE(funcctx);
}

/*
 * @Description:  get realtime ec operator information from hashtable.
 * @return - Datum
 */
Datum pg_stat_get_wlm_realtime_ec_operator_info(PG_FUNCTION_ARGS)
{
#define OPERATOR_REALTIME_SESSION_EC_INFO_ATTRNUM 12
    FuncCallContext* funcctx = NULL;
    int num = 0, i = 0;

    if (!superuser()) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_PROC,
            "pg_stat_get_wlm_realtime_ec_operator_info");
    }

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        TupleDesc tupdesc;
        i = 0;

        funcctx = SRF_FIRSTCALL_INIT();

        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(OPERATOR_REALTIME_SESSION_EC_INFO_ATTRNUM, false, TAM_HEAP);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "queryid", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "plan_node_id", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "plan_node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "start_time", TIMESTAMPTZOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "ec_operator", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "ec_status", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "ec_execute_datanode", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "ec_dsn", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "ec_username", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "ec_query", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "ec_libodbc_type", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "ec_fetch_count", INT8OID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->user_fctx = ExplainGetSessionStatistics(&num);
        funcctx->max_calls = num;

        (void)MemoryContextSwitchTo(oldcontext);

        if (funcctx->user_fctx == NULL)
            SRF_RETURN_DONE(funcctx);
    }

    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->user_fctx && funcctx->call_cntr < funcctx->max_calls) {
        Datum values[OPERATOR_REALTIME_SESSION_EC_INFO_ATTRNUM];
        bool nulls[OPERATOR_REALTIME_SESSION_EC_INFO_ATTRNUM] = {false};
        HeapTuple tuple = NULL;
        Datum result;
        i = -1;
        errno_t rc = EOK;

        ExplainGeneralInfo* statistics = (ExplainGeneralInfo*)funcctx->user_fctx + funcctx->call_cntr;

        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        values[++i] = Int64GetDatum(statistics->query_id);
        values[++i] = Int32GetDatum(statistics->plan_node_id);
        values[++i] = CStringGetTextDatum(statistics->plan_node_name);
        if (statistics->start_time == 0)
            nulls[++i] = true;
        else
            values[++i] = TimestampTzGetDatum(statistics->start_time);

        if (statistics->ec_operator) {
            values[++i] = Int32GetDatum(statistics->ec_operator);
        } else {
            nulls[++i] = true;
        }

        if (statistics->ec_status) {
            if (statistics->ec_status == EC_STATUS_CONNECTED) {
                values[++i] = CStringGetTextDatum("CONNECTED");
            } else if (statistics->ec_status == EC_STATUS_EXECUTED) {
                values[++i] = CStringGetTextDatum("EXECUTED");
            } else if (statistics->ec_status == EC_STATUS_FETCHING) {
                values[++i] = CStringGetTextDatum("FETCHING");
            } else if (statistics->ec_status == EC_STATUS_END) {
                values[++i] = CStringGetTextDatum("END");
            }
        } else {
            nulls[++i] = true;
        }

        if (statistics->ec_execute_datanode) {
            values[++i] = CStringGetTextDatum(statistics->ec_execute_datanode);
        } else {
            nulls[++i] = true;
        }

        if (statistics->ec_dsn) {
            values[++i] = CStringGetTextDatum(statistics->ec_dsn);
        } else {
            nulls[++i] = true;
        }

        if (statistics->ec_username) {
            values[++i] = CStringGetTextDatum(statistics->ec_username);
        } else {
            nulls[++i] = true;
        }

        if (statistics->ec_query) {
            char* mask_string = NULL;
            MASK_PASSWORD_START(mask_string, statistics->ec_query);
            values[++i] = CStringGetTextDatum(mask_string);
            MASK_PASSWORD_END(mask_string, statistics->ec_query);
        } else {
            nulls[++i] = true;
        }

        if (statistics->ec_libodbc_type) {
            if (statistics->ec_libodbc_type == EC_LIBODBC_TYPE_ONE) {
                values[++i] = CStringGetTextDatum("libodbc.so.1");
            } else if (statistics->ec_libodbc_type == EC_LIBODBC_TYPE_TWO) {
                values[++i] = CStringGetTextDatum("libodbc.so.2");
            }
        } else {
            nulls[++i] = true;
        }

        if (statistics->ec_fetch_count) {
            values[++i] = Int64GetDatum(statistics->ec_fetch_count);
        } else {
            nulls[++i] = true;
        }

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, result);
    }

    pfree_ext(funcctx->user_fctx);
    funcctx->user_fctx = NULL;

    SRF_RETURN_DONE(funcctx);
}

/*
 * @Description:  get realtime operator information from hashtable.
 * @return - Datum
 */
Datum pg_stat_get_wlm_realtime_operator_info(PG_FUNCTION_ARGS)
{
#define OPERATOR_REALTIME_SESSION_INFO_ATTRNUM 23
    FuncCallContext* funcctx = NULL;
    int num = 0, i = 0;

    if (!superuser()) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_PROC,
            "pg_stat_get_wlm_realtime_operator_info");
    }

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        TupleDesc tupdesc;
        i = 0;

        funcctx = SRF_FIRSTCALL_INIT();

        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(OPERATOR_REALTIME_SESSION_INFO_ATTRNUM, false, TAM_HEAP);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "queryid", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "pid", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "plan_node_id", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "plan_node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "start_time", TIMESTAMPTZOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "duration", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "status", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "query_dop", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "estimated_rows", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "tuple_processed", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "min_peak_memory", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "max_peak_memory", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "average_peak_memory", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "memory_skew_percent", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "min_spill_size", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "max_spill_size", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "average_spill_size", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "spill_skew_percent", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "min_cpu_time", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "max_cpu_time", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "total_cpu_time", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "cpu_skew_percent", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "warn_prof_info", TEXTOID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->user_fctx = ExplainGetSessionStatistics(&num);
        funcctx->max_calls = num;

        (void)MemoryContextSwitchTo(oldcontext);

        if (funcctx->user_fctx == NULL)
            SRF_RETURN_DONE(funcctx);
    }

    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->user_fctx && funcctx->call_cntr < funcctx->max_calls) {
        Datum values[OPERATOR_REALTIME_SESSION_INFO_ATTRNUM];
        bool nulls[OPERATOR_REALTIME_SESSION_INFO_ATTRNUM] = {false};
        HeapTuple tuple = NULL;
        Datum result;
        i = -1;
        char warnstr[WARNING_INFO_LEN] = {0};
        errno_t rc = EOK;

        ExplainGeneralInfo* statistics = (ExplainGeneralInfo*)funcctx->user_fctx + funcctx->call_cntr;

        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        WLMGetWarnInfo(warnstr, sizeof(warnstr), statistics->warn_prof_info, statistics->max_spill_size, 0);

        values[++i] = Int64GetDatum(statistics->query_id);
        values[++i] = Int64GetDatum(statistics->tid);
        values[++i] = Int32GetDatum(statistics->plan_node_id);
        values[++i] = CStringGetTextDatum(statistics->plan_node_name);
        if (statistics->start_time == 0)
            nulls[++i] = true;
        else
            values[++i] = TimestampTzGetDatum(statistics->start_time);
        values[++i] = Int64GetDatum(statistics->duration_time);
        if (statistics->status)
            values[++i] = CStringGetTextDatum("finished");
        else
            values[++i] = CStringGetTextDatum("running");
        values[++i] = Int32GetDatum(statistics->query_dop);
        values[++i] = Int64GetDatum(statistics->estimate_rows);
        values[++i] = Int64GetDatum(statistics->tuple_processed);
        values[++i] = Int32GetDatum(statistics->min_peak_memory);
        values[++i] = Int32GetDatum(statistics->max_peak_memory);
        values[++i] = Int32GetDatum(statistics->avg_peak_memory);
        values[++i] = Int32GetDatum(statistics->memory_skewed);
        values[++i] = Int32GetDatum(statistics->min_spill_size);
        values[++i] = Int32GetDatum(statistics->max_spill_size);
        values[++i] = Int64GetDatum(statistics->avg_spill_size);
        values[++i] = Int32GetDatum(statistics->i_o_skew);
        values[++i] = Int64GetDatum(statistics->min_cpu_time);
        values[++i] = Int64GetDatum(statistics->max_cpu_time);
        values[++i] = Int64GetDatum(statistics->total_cpu_time);
        values[++i] = Int32GetDatum(statistics->cpu_skew);
        if (warnstr[0])
            values[++i] = CStringGetTextDatum(warnstr);
        else
            nulls[++i] = true;

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, result);
    }

    pfree_ext(funcctx->user_fctx);
    funcctx->user_fctx = NULL;

    SRF_RETURN_DONE(funcctx);
}

/*
 * function name: pg_stat_get_wlm_statistics
 * description  : the view will show the info which exception is handled.
 */
Datum pg_stat_get_wlm_statistics(PG_FUNCTION_ARGS)
{
#define WLM_STATISTICS_NUM 9
    FuncCallContext* funcctx = NULL;
    int num = 0, i = 0;

    if (!superuser()) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_PROC,
            "pg_stat_get_wlm_statistics");
    }

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        TupleDesc tupdesc;
        i = 0;

        funcctx = SRF_FIRSTCALL_INIT();

        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(WLM_STATISTICS_NUM, false, TAM_HEAP);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "statement", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "block_time", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "elapsed_time", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "total_cpu_time", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "qualification_time", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "skew_percent", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "control_group", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "status", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "action", TEXTOID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->user_fctx = WLMGetStatistics(&num);
        funcctx->max_calls = num;

        MemoryContextSwitchTo(oldcontext);

        if (funcctx->user_fctx == NULL)
            SRF_RETURN_DONE(funcctx);
    }

    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->user_fctx && funcctx->call_cntr < funcctx->max_calls) {
        Datum values[WLM_STATISTICS_NUM];
        bool nulls[WLM_STATISTICS_NUM] = {false};
        HeapTuple tuple = NULL;
        Datum result;
        i = -1;

        WLMStatistics* statistics = (WLMStatistics*)funcctx->user_fctx + funcctx->call_cntr;

        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        /* Locking is probably not really necessary */
        char* mask_string = NULL;
        MASK_PASSWORD_START(mask_string, statistics->stmt);
        values[++i] = CStringGetTextDatum(mask_string);
        MASK_PASSWORD_END(mask_string, statistics->stmt);
        values[++i] = Int64GetDatum(statistics->blocktime);
        values[++i] = Int64GetDatum(statistics->elapsedtime);
        values[++i] = Int64GetDatum(statistics->totalcputime);
        values[++i] = Int64GetDatum(statistics->qualitime);
        values[++i] = Int32GetDatum(statistics->skewpercent);
        values[++i] = CStringGetTextDatum(statistics->cgroup);
        values[++i] = CStringGetTextDatum(statistics->status);
        values[++i] = CStringGetTextDatum(statistics->action);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, result);
    }

    pfree_ext(funcctx->user_fctx);
    funcctx->user_fctx = NULL;

    SRF_RETURN_DONE(funcctx);
}

/*
 * function name: pg_stat_get_session_wlmstat
 * description  : the view will show current workload state.
 */
Datum pg_stat_get_session_wlmstat(PG_FUNCTION_ARGS)
{
    const int SESSION_WLMSTAT_NUM = 22;

    if (!superuser()) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_PROC,
            "pg_stat_get_session_wlmstat");
    }

    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    MemoryContext oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);

    TupleDesc tupdesc = CreateTemplateTupleDesc(SESSION_WLMSTAT_NUM, false);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_1, "datid", OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_2, "threadid", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_3, "sessionid", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_4, "threadpid", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_5, "usesysid", OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_6, "application_name", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_7, "statement", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_8, "priority", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_9, "block_time", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_10, "elapsed_time", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_11, "total_cpu_time", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_12, "skew_percent", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_13, "statement_mem", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_14, "active_points", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_15, "dop_value", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_16, "cgroup", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_17, "status", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_18, "enqueue", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_19, "attribute", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_20, "is_plana", BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_21, "node_group", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_22, "srespool", NAMEOID, -1, 0);

    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->setDesc = BlessTupleDesc(tupdesc);

    bool hasTID = false;
    ThreadId tid = 0;
    if (!PG_ARGISNULL(0)) {
        hasTID = true;
        tid = PG_GETARG_INT64(0);
    }

    (void)gs_stat_read_current_status(rsinfo->setResult, rsinfo->setDesc,
                                      insert_pg_stat_get_session_wlmstat, hasTID, tid);

    MemoryContextSwitchTo(oldcontext);
    /* clean up and return the tuplestore */
    tuplestore_donestoring(rsinfo->setResult);

    return (Datum) 0;
}

void insert_pg_stat_get_session_wlmstat(Tuplestorestate *tupStore, TupleDesc tupDesc, const PgBackendStatus *beentry)
{
    const int SESSION_WLMSTAT_NUM = 22;
    Datum values[SESSION_WLMSTAT_NUM];
    bool nulls[SESSION_WLMSTAT_NUM];

    errno_t rc = 0;
    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    int i = -1;

    /* Values available to all callers */
    if (superuser() || isMonitoradmin(GetUserId()) || beentry->st_userid == GetUserId()) {
        values[++i] = ObjectIdGetDatum(beentry->st_databaseid);
        values[++i] = Int64GetDatum(beentry->st_procpid);
        values[++i] = Int64GetDatum(beentry->st_sessionid);
        values[++i] = Int32GetDatum(beentry->st_tid);
        values[++i] = ObjectIdGetDatum(beentry->st_userid);
        if (beentry->st_appname)
            values[++i] = CStringGetTextDatum(beentry->st_appname);
        else
            nulls[++i] = true;

        /* Values only available to same user or superuser */
        pgstat_refresh_statement_wlm_time(const_cast<PgBackendStatus *>(beentry));

        if (beentry->st_state == STATE_UNDEFINED || beentry->st_state == STATE_DISABLED) {
            nulls[++i] = true;
        } else {
            char* mask_string = NULL;
            /* Mask password in query string */
            mask_string = maskPassword(beentry->st_activity);
            if (mask_string == NULL) {
                mask_string = beentry->st_activity;
            }
            values[++i] = CStringGetTextDatum(mask_string);

            if (mask_string != beentry->st_activity) {
                pfree(mask_string);
            }
        }

        /*use backend state to get all workload info*/
        WLMStatistics* backstat = NULL;
        WLMStatistics backstat_init;
        errno_t errval = memset_s(&backstat_init, sizeof(backstat_init), 0, sizeof(backstat_init));
        securec_check_errval(errval, , LOG);

        backstat = (u_sess->attr.attr_resource.use_workload_manager) ? (WLMStatistics*)&beentry->st_backstat
            : (WLMStatistics*)&backstat_init;

        values[++i] = Int64GetDatum(backstat->priority);
        values[++i] = Int64GetDatum(backstat->blocktime);
        values[++i] = Int64GetDatum(backstat->elapsedtime);
        values[++i] = Int64GetDatum(backstat->totalcputime);
        values[++i] = Int32GetDatum(backstat->skewpercent);
        values[++i] = Int32GetDatum(backstat->stmt_mem);
        values[++i] = Int32GetDatum(backstat->act_pts);
        values[++i] = Int32GetDatum(backstat->dop_value);
        values[++i] = CStringGetTextDatum(backstat->cgroup);
        if (backstat->status)
            values[++i] = CStringGetTextDatum(backstat->status);
        else
            values[++i] = CStringGetTextDatum("unknown");
        if (backstat->enqueue)
            values[++i] = CStringGetTextDatum(backstat->enqueue);
        else
            nulls[++i] = true;
        if (backstat->qtype)
            values[++i] = CStringGetTextDatum(backstat->qtype);
        else
            values[++i] = CStringGetTextDatum("Unknown");
        values[++i] = BoolGetDatum(backstat->is_planA);
        if (StringIsValid(backstat->groupname))
            values[++i] = CStringGetTextDatum(backstat->groupname);
        else
            values[++i] = CStringGetTextDatum("installation");

        if (ENABLE_WORKLOAD_CONTROL && *backstat->srespool)
            values[++i] = NameGetDatum((Name)backstat->srespool);
        else
            values[++i] = DirectFunctionCall1(namein, CStringGetDatum("unknown"));

    } else {
        for (i = 0; i < SESSION_WLMSTAT_NUM; ++i) {
            nulls[i] = true;
        }
        nulls[ARR_6] = false;
        values[ARR_6] = CStringGetTextDatum("<insufficient privilege>");
    }

    tuplestore_putvalues(tupStore, tupDesc, values, nulls);
}

/*
 * function name: pg_stat_get_session_wlmstat
 * description  : the view will show current workload state.
 */
Datum pg_stat_get_session_respool(PG_FUNCTION_ARGS)
{
    const int SESSION_WLMSTAT_RESPOOL_NUM = 7;

    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    MemoryContext oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);

    TupleDesc tupdesc = CreateTemplateTupleDesc(SESSION_WLMSTAT_RESPOOL_NUM, false);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_1, "datid", OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_2, "threadid", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_3, "sessionid", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_4, "threadpid", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_5, "usesysid", OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_6, "cgroup", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_7, "srespool", NAMEOID, -1, 0);

    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->setDesc = BlessTupleDesc(tupdesc);

    (void)gs_stat_read_current_status(rsinfo->setResult, rsinfo->setDesc,
                                      insert_pg_stat_get_session_respool);

    MemoryContextSwitchTo(oldcontext);
    /* clean up and return the tuplestore */
    tuplestore_donestoring(rsinfo->setResult);

    return (Datum) 0;
}

void insert_pg_stat_get_session_respool(Tuplestorestate *tupStore, TupleDesc tupDesc, const PgBackendStatus *beentry)
{
    const int SESSION_WLMSTAT_RESPOOL_NUM = 7;
    Datum values[SESSION_WLMSTAT_RESPOOL_NUM];
    bool nulls[SESSION_WLMSTAT_RESPOOL_NUM];
    int i = -1;
    errno_t rc = 0;
    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    /* Values available to all callers */
    values[++i] = ObjectIdGetDatum(beentry->st_databaseid);
    values[++i] = Int64GetDatum(beentry->st_procpid);
    values[++i] = Int64GetDatum(beentry->st_sessionid);
    values[++i] = Int32GetDatum(beentry->st_tid);
    values[++i] = ObjectIdGetDatum(beentry->st_userid);

    /* Values only available to same user or superuser */
    if (superuser() || isMonitoradmin(GetUserId()) || beentry->st_userid == GetUserId()) {
        pgstat_refresh_statement_wlm_time(const_cast<PgBackendStatus *>(beentry));

        /*use backend state to get all workload info*/
        WLMStatistics* backstat = (WLMStatistics*)&beentry->st_backstat;

        values[++i] = CStringGetTextDatum(backstat->cgroup);

        if (ENABLE_WORKLOAD_CONTROL && *backstat->srespool)
            values[++i] = NameGetDatum((Name)backstat->srespool);
        else
            values[++i] = DirectFunctionCall1(namein, CStringGetDatum("unknown"));
    } else {
        for (i = 5; i < SESSION_WLMSTAT_RESPOOL_NUM; ++i)
            nulls[i] = true;
    }

    tuplestore_putvalues(tupStore, tupDesc, values, nulls);

}


/*
 * function name: pg_stat_get_wlm_session_info_internal
 * description  : Called by pg_stat_get_wlm_session_info
 *                as internal function. Fetch session info
 *                of the finished query from each data nodes.
 */
Datum pg_stat_get_wlm_session_info_internal(PG_FUNCTION_ARGS)
{
    if (!superuser()) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_PROC,
            "pg_stat_get_wlm_session_info_internal");
    }
#ifndef ENABLE_MULTIPLE_NODES
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported proc in single node mode.")));
    PG_RETURN_NULL();
#else
    Oid procId = PG_GETARG_OID(0);
    uint64 queryId = PG_GETARG_INT64(1);
    TimestampTz stamp = PG_GETARG_INT64(2);
    Oid removed = PG_GETARG_OID(3);

    char* result = "FailedToGetSessionInfo";

    if (IS_PGXC_COORDINATOR)
        result = "GetWLMSessionInfo";

    if (IS_PGXC_DATANODE) {
        Qid qid = {procId, queryId, stamp};

        /*get session info with qid*/
        result = (char*)WLMGetSessionInfo(&qid, removed, NULL);

        /*no result, failed to get session info*/
        if (result == NULL)
            result = "FailedToGetSessionInfo";
    }

    PG_RETURN_TEXT_P(cstring_to_text(result));
#endif
}

/*
 * function name: IOPriorityGetString
 * description  : get the string of io_priority
 * arguments    : io_priority type value
 * return value : the string of io_priority
 */
char* IOPriorityGetString(int iopri)
{
    if (iopri == IOPRIORITY_HIGH)
        return "High";
    else if (iopri == IOPRIORITY_MEDIUM)
        return "Medium";
    else if (iopri == IOPRIORITY_LOW)
        return "Low";
    else if (iopri == IOPRIORITY_NONE)
        return "None";

    return "null";
}

/*
 * @Description: get global io wait status
 * @IN void
 * @Return: records
 * @See also:
 */
Datum pg_stat_get_wlm_io_wait_status(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_MULTIPLE_NODES
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported proc in single node mode.")));
    PG_RETURN_NULL();
#else
    const int io_wait_status_attrnum = 9;
    FuncCallContext* funcctx = NULL;
    const int num = 1;
    int i = 0;

    if (!superuser() && !isMonitoradmin(GetUserId())) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_PROC,
            "pg_stat_get_wlm_session_iostat_info");
    }

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        TupleDesc tupdesc;
        i = 0;

        funcctx = SRF_FIRSTCALL_INIT();

        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(io_wait_status_attrnum, false, TAM_HEAP);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "device_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "read_per_second", FLOAT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "write_per_second", FLOAT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "write_ratio", FLOAT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "io_util", FLOAT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "total_io_util", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "tick_count", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "io_wait_list_len", INT4OID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->max_calls = num;

        USE_AUTO_LWLOCK(WorkloadIOUtilLock, LW_SHARED);
        IoWaitStatGlobalInfo *io_global_info = (IoWaitStatGlobalInfo*)palloc0(sizeof(IoWaitStatGlobalInfo));
        int rc = memcpy_s(io_global_info->device, MAX_DEVICE_DIR, 
            g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.device, MAX_DEVICE_DIR);
        securec_check_c(rc, "\0", "\0");
        io_global_info->rs = ceil(g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.rs);
        io_global_info->ws = ceil(g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.ws);
        io_global_info->w_ratio = ceil(g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.w_ratio);
        io_global_info->util = ceil(g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.util);
        io_global_info->total_tbl_util = g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.total_tbl_util;
        io_global_info->tick_count = g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.tick_count;
        io_global_info->io_wait_list_len = list_length(g_instance.wlm_cxt->io_context.waiting_list);

        funcctx->user_fctx = io_global_info;

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->call_cntr < funcctx->max_calls) {
        Datum values[io_wait_status_attrnum];
        bool nulls[io_wait_status_attrnum] = {false};
        HeapTuple tuple = NULL;
        i = -1;

        errno_t rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");

        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        IoWaitStatGlobalInfo *info = (IoWaitStatGlobalInfo*)(funcctx->user_fctx);
        
        /* fill in all attribute value */
        values[++i] = CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);
        values[++i] = CStringGetTextDatum(info->device);
        values[++i] = Float8GetDatumFast(info->rs);
        values[++i] = Float8GetDatumFast(info->ws);
        values[++i] = Float8GetDatumFast(info->w_ratio);
        values[++i] = Float8GetDatumFast(info->util);
        values[++i] = Int32GetDatum(info->total_tbl_util);
        values[++i] = Int32GetDatum(info->tick_count);
        values[++i] = Int32GetDatum(info->io_wait_list_len);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }

    pfree_ext(funcctx->user_fctx);
    funcctx->user_fctx = NULL;

    SRF_RETURN_DONE(funcctx);
#endif
}



/*
 * @Description: get user resource info
 * @IN void
 * @Return: records
 * @See also:
 */
Datum pg_stat_get_wlm_user_iostat_info(PG_FUNCTION_ARGS)
{
#define WLM_USER_IO_RESOURCE_ATTRNUM 8
    char* name = PG_GETARG_CSTRING(0);

    if (!superuser() && !isMonitoradmin(GetUserId())) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_PROC,
            "pg_stat_get_wlm_session_iostat_info");
    }

    FuncCallContext* funcctx = NULL;
    const int num = 1;
    int i = 0;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        TupleDesc tupdesc;
        i = 0;

        funcctx = SRF_FIRSTCALL_INIT();

        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(WLM_USER_IO_RESOURCE_ATTRNUM, false, TAM_HEAP);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "user_id", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "mincurr_iops", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "maxcurr_iops", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "minpeak_iops", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "maxpeak_iops", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "iops_limits", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "io_priority", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "curr_io_limits", INT4OID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->user_fctx = GetUserResourceData(name); /* get user resource data */
        funcctx->max_calls = num;

        MemoryContextSwitchTo(oldcontext);

        if (funcctx->user_fctx == NULL)
            SRF_RETURN_DONE(funcctx);
    }

    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->call_cntr < funcctx->max_calls) {
        Datum values[WLM_USER_IO_RESOURCE_ATTRNUM];
        bool nulls[WLM_USER_IO_RESOURCE_ATTRNUM] = {false};
        HeapTuple tuple = NULL;
        i = -1;

        UserResourceData* rsdata = (UserResourceData*)funcctx->user_fctx + funcctx->call_cntr;

        errno_t rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");

        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        /* fill in all attribute value */
        values[++i] = ObjectIdGetDatum(rsdata->userid);
        values[++i] = Int32GetDatum(rsdata->mincurr_iops);
        values[++i] = Int32GetDatum(rsdata->maxcurr_iops);
        values[++i] = Int32GetDatum(rsdata->minpeak_iops);
        values[++i] = Int32GetDatum(rsdata->maxpeak_iops);
        values[++i] = Int32GetDatum(rsdata->iops_limits);
        values[++i] = CStringGetTextDatum(IOPriorityGetString(rsdata->io_priority));
        values[++i] = Int32GetDatum(rsdata->curr_iops_limit);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }

    pfree_ext(funcctx->user_fctx);
    funcctx->user_fctx = NULL;

    SRF_RETURN_DONE(funcctx);
}

Datum pg_stat_get_wlm_session_iostat_info(PG_FUNCTION_ARGS)
{
#define WLM_SESSION_IOSTAT_ATTRNUM 8
    FuncCallContext* funcctx = NULL;
    int i = 0;

    if (!superuser() && !isMonitoradmin(GetUserId())) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_PROC,
            "pg_stat_get_wlm_session_iostat_info");
    }

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        TupleDesc tupdesc;
        i = 0;

        funcctx = SRF_FIRSTCALL_INIT();

        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(WLM_SESSION_IOSTAT_ATTRNUM, false, TAM_HEAP);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "threadid", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "maxcurr_iops", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "mincurr_iops", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "maxpeak_iops", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "minpeak_iops", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "iops_limits", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "io_priority", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "curr_io_limits", INT4OID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->user_fctx = WLMGetIOStatisticsGeneral();

        MemoryContextSwitchTo(oldcontext);

        if (funcctx->user_fctx == NULL)
            SRF_RETURN_DONE(funcctx);
    }

    funcctx = SRF_PERCALL_SETUP();
    WLMIoStatisticsList* list_node = (WLMIoStatisticsList*)funcctx->user_fctx;

    while (list_node != NULL) {
        Datum values[WLM_SESSION_IOSTAT_ATTRNUM];
        bool nulls[WLM_SESSION_IOSTAT_ATTRNUM] = {false};
        HeapTuple tuple = NULL;
        Datum result;
        i = -1;

        WLMIoStatisticsGenenral* statistics = list_node->node;

        errno_t rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");

        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        values[++i] = Int64GetDatum(statistics->tid);
        values[++i] = Int32GetDatum(statistics->maxcurr_iops);
        values[++i] = Int32GetDatum(statistics->mincurr_iops);
        values[++i] = Int32GetDatum(statistics->maxpeak_iops);
        values[++i] = Int32GetDatum(statistics->minpeak_iops);
        values[++i] = Int32GetDatum(statistics->iops_limits);
        values[++i] = Int32GetDatum(statistics->io_priority);
        values[++i] = Int32GetDatum(statistics->curr_iops_limit);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);

        funcctx->user_fctx = (void*)list_node->next;
        SRF_RETURN_NEXT(funcctx, result);
    }

    pfree_ext(funcctx->user_fctx);
    funcctx->user_fctx = NULL;

    SRF_RETURN_DONE(funcctx);
}

/*
 * function name: pg_stat_get_wlm_node_resource_info
 * description  : the view will show node resource info.
 */
Datum pg_stat_get_wlm_node_resource_info(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_MULTIPLE_NODES
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("unsupported view in single node mode.")));
#endif

#define WLM_NODE_RESOURCE_ATTRNUM 7
    FuncCallContext* funcctx = NULL;
    const int num = 1;
    int i = 0;

    if (!superuser()) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_PROC,
            "pg_stat_get_wlm_node_resource_info");
    }

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        TupleDesc tupdesc;
        i = 0;

        funcctx = SRF_FIRSTCALL_INIT();

        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(WLM_NODE_RESOURCE_ATTRNUM, false, TAM_HEAP);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "min_mem_util", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "max_mem_util", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "min_cpu_util", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "max_cpu_util", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "min_io_util", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "max_io_util", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "phy_usemem_rate", INT4OID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->user_fctx = dywlm_get_resource_info(&g_instance.wlm_cxt->MyDefaultNodeGroup.srvmgr);
        funcctx->max_calls = num;

        MemoryContextSwitchTo(oldcontext);

        if (funcctx->user_fctx == NULL)
            SRF_RETURN_DONE(funcctx);
    }

    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->user_fctx && funcctx->call_cntr < funcctx->max_calls) {
        Datum values[WLM_SESSION_IOSTAT_ATTRNUM];
        bool nulls[WLM_SESSION_IOSTAT_ATTRNUM] = {false};
        HeapTuple tuple = NULL;
        Datum result;
        i = -1;

        DynamicNodeData* nodedata = (DynamicNodeData*)funcctx->user_fctx;

        errno_t errval = memset_s(&values, sizeof(values), 0, sizeof(values));
        securec_check_errval(errval, , LOG);
        errval = memset_s(&nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check_errval(errval, , LOG);

        values[++i] = Int32GetDatum(nodedata->min_memutil);
        values[++i] = Int32GetDatum(nodedata->max_memutil);
        values[++i] = Int32GetDatum(nodedata->min_cpuutil);
        values[++i] = Int32GetDatum(nodedata->max_cpuutil);
        values[++i] = Int32GetDatum(nodedata->min_ioutil);
        values[++i] = Int32GetDatum(nodedata->max_ioutil);
        values[++i] = Int32GetDatum(nodedata->phy_usemem_rate);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, result);
    }

    pfree_ext(funcctx->user_fctx);
    funcctx->user_fctx = NULL;

    SRF_RETURN_DONE(funcctx);
}

/*
 * function name: pg_stat_get_wlm_session_info
 * description  : the view will show the session info.
 */
Datum pg_stat_get_wlm_session_info(PG_FUNCTION_ARGS)
{
    if (!superuser() && !isMonitoradmin(GetUserId())) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_PROC,
            "pg_stat_get_wlm_session_info");
    }

    int WLM_SESSION_INFO_ATTRNUM = 0;
    if (t_thrd.proc->workingVersionNum >= SLOW_QUERY_VERSION)
        WLM_SESSION_INFO_ATTRNUM = 87;
    else
        WLM_SESSION_INFO_ATTRNUM = 68;

    FuncCallContext* funcctx = NULL;
    int num = 0, i = 0;

    Oid removed = PG_GETARG_OID(0); /* remove session info ? */
    Qid qid = {0, 0, 0};

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        TupleDesc tupdesc;
        i = 0;

        funcctx = SRF_FIRSTCALL_INIT();

        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(WLM_SESSION_INFO_ATTRNUM, false, TAM_HEAP);

        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "datid", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "dbname", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "schemaname", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "nodename", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "username", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "application_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "client_addr", INETOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "client_hostname", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "client_port", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "query_band", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "block_time", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "start_time", TIMESTAMPTZOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "finish_time", TIMESTAMPTZOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "duration", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "estimate_total_time", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "status", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "abort_info", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "resource_pool", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "control_group", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "estimate_memory", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "MinPeakMemory", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "MaxPeakMemory", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "average_peak_memory", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "memory_skew_percent", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "spill_info", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "min_spill_size", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "max_spill_size", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "average_spill_size", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "spill_skew_percent", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "min_dn_time", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "max_dn_time", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "average_dn_time", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "dntime_skew_percent", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "MinCpuTime", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "MaxCpuTime", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "total_cpu_time", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "cpu_skew_percent", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "min_peak_iops", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "max_peak_iops", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "average_peak_iops", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "iops_skew_percent", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "warning", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "queryid", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "query", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "query_plan", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "node_group", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "cpu_top1_node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "cpu_top2_node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "cpu_top3_node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "cpu_top4_node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "cpu_top5_node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "mem_top1_node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "mem_top2_node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "mem_top3_node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "mem_top4_node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "mem_top5_node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "cpu_top1_value", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "cpu_top2_value", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "cpu_top3_value", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "cpu_top4_value", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "cpu_top5_value", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "mem_top1_value", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "mem_top2_value", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "mem_top3_value", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "mem_top4_value", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "mem_top5_value", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "top_mem_dn", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "top_cpu_dn", TEXTOID, -1, 0);
        if (t_thrd.proc->workingVersionNum >= SLOW_QUERY_VERSION) {
            TupleDescInitEntry(tupdesc, (AttrNumber)++i, "n_returned_rows", INT8OID, -1, 0);
            TupleDescInitEntry(tupdesc, (AttrNumber)++i, "n_tuples_fetched", INT8OID, -1, 0);
            TupleDescInitEntry(tupdesc, (AttrNumber)++i, "n_tuples_returned", INT8OID, -1, 0);
            TupleDescInitEntry(tupdesc, (AttrNumber)++i, "n_tuples_inserted", INT8OID, -1, 0);
            TupleDescInitEntry(tupdesc, (AttrNumber)++i, "n_tuples_updated", INT8OID, -1, 0);
            TupleDescInitEntry(tupdesc, (AttrNumber)++i, "n_tuples_deleted", INT8OID, -1, 0);
            TupleDescInitEntry(tupdesc, (AttrNumber)++i, "t_blocks_fetched", INT8OID, -1, 0);
            TupleDescInitEntry(tupdesc, (AttrNumber)++i, "t_blocks_hit", INT8OID, -1, 0);
            for (num = 0; num < TOTAL_TIME_INFO_TYPES; num++) {
                TupleDescInitEntry(tupdesc, (AttrNumber)++i, TimeInfoTypeName[num], INT8OID, -1, 0);
            }
            TupleDescInitEntry(tupdesc, (AttrNumber)++i, "is_slow_query", INT8OID, -1, 0);
        }
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->user_fctx = WLMGetSessionInfo(&qid, removed, &num);
        funcctx->max_calls = num;

        MemoryContextSwitchTo(oldcontext);

        if (funcctx->user_fctx == NULL)
            SRF_RETURN_DONE(funcctx);
    }

    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->user_fctx && funcctx->call_cntr < funcctx->max_calls) {
        const int TOP5 = 5;
        Datum values[WLM_SESSION_INFO_ATTRNUM];
        bool nulls[WLM_SESSION_INFO_ATTRNUM] = {false};
        HeapTuple tuple = NULL;
        Datum result;
        i = -1;
        errno_t rc = 0;
        char spill_info[NAMEDATALEN];
        char warnstr[WARNING_INFO_LEN] = {0};
        SockAddr zero_clientaddr;

        WLMSessionStatistics* detail = (WLMSessionStatistics*)funcctx->user_fctx + funcctx->call_cntr;

        WLMGetSpillInfo(spill_info, sizeof(spill_info), detail->gendata.spillNodeCount);
        WLMGetWarnInfo(warnstr,
            sizeof(warnstr),
            detail->gendata.warning,
            detail->gendata.spillData.max_value,
            detail->gendata.broadcastData.max_value,
            detail);

        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");

        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        char* dbname = get_database_name(detail->databaseid);

        /* Locking is probably not really necessary */
        values[++i] = Int32GetDatum(detail->databaseid);
        if (dbname != NULL)
            values[++i] = CStringGetTextDatum(dbname);
        else
            nulls[++i] = true;
        if (detail->schname && detail->schname[0])
            values[++i] = CStringGetTextDatum(detail->schname);
        else
            nulls[++i] = true;
        values[++i] = CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);
        char* user_name = GetUserNameById(detail->userid);
        if (user_name != NULL)
            values[++i] = CStringGetTextDatum(user_name);
        else
            nulls[++i] = true;
        values[++i] = CStringGetTextDatum(detail->appname);

        errno_t errval = memset_s(&zero_clientaddr, sizeof(zero_clientaddr), 0, sizeof(zero_clientaddr));
        securec_check_errval(errval, , LOG);
        /* A zeroed client addr means we don't know */
        if (detail->clientaddr == NULL || memcmp(detail->clientaddr, &zero_clientaddr, sizeof(zero_clientaddr)) == 0) {
            nulls[++i] = true;
            nulls[++i] = true;
            nulls[++i] = true;
        } else {
            SockAddr* sockaddr = (SockAddr*)detail->clientaddr;

            if (sockaddr->addr.ss_family == AF_INET
#ifdef HAVE_IPV6
                || sockaddr->addr.ss_family == AF_INET6
#endif
            ) {
                char remote_host[NI_MAXHOST];
                char remote_port[NI_MAXSERV];
                int ret;

                remote_host[0] = '\0';
                remote_port[0] = '\0';
                ret = pg_getnameinfo_all(&sockaddr->addr,
                    sockaddr->salen,
                    remote_host,
                    sizeof(remote_host),
                    remote_port,
                    sizeof(remote_port),
                    NI_NUMERICHOST | NI_NUMERICSERV);
                if (ret == 0) {
                    clean_ipv6_addr(sockaddr->addr.ss_family, remote_host);
                    values[++i] = DirectFunctionCall1(inet_in, CStringGetDatum(remote_host));
                    if (detail->clienthostname && detail->clienthostname[0])
                        values[++i] = CStringGetTextDatum(detail->clienthostname);
                    else
                        nulls[++i] = true;
                    values[++i] = Int32GetDatum(atoi(remote_port));
                } else {
                    nulls[++i] = true;
                    nulls[++i] = true;
                    nulls[++i] = true;
                }
            } else if (sockaddr->addr.ss_family == AF_UNIX) {
                /*
                 * Unix sockets always reports NULL for host and -1 for
                 * port, so it's possible to tell the difference to
                 * connections we have no permissions to view, or with
                 * errors.
                 */
                nulls[++i] = true;
                nulls[++i] = true;
                values[++i] = DatumGetInt32(-1);
            } else {
                /* Unknown address type, should never happen */
                nulls[++i] = true;
                nulls[++i] = true;
                nulls[++i] = true;
            }
        }

        if (detail->query_band && detail->query_band[0])
            values[++i] = CStringGetTextDatum(detail->query_band);
        else
            nulls[++i] = true;
        values[++i] = Int64GetDatum(detail->block_time);
        values[++i] = TimestampTzGetDatum(detail->start_time);
        values[++i] = TimestampTzGetDatum(detail->fintime);
        values[++i] = Int64GetDatum(detail->duration);
        values[++i] = Int64GetDatum(detail->estimate_time);
        values[++i] = CStringGetTextDatum(GetStatusName(detail->status));
        if (detail->err_msg && detail->err_msg[0])
            values[++i] = CStringGetTextDatum(detail->err_msg);
        else
            nulls[++i] = true;
        values[++i] = CStringGetTextDatum(detail->respool);
        values[++i] = CStringGetTextDatum(detail->cgroup);
        values[++i] = Int32GetDatum(detail->estimate_memory);
        values[++i] = Int32GetDatum(detail->gendata.peakMemoryData.min_value);
        values[++i] = Int32GetDatum(detail->gendata.peakMemoryData.max_value);
        values[++i] = Int32GetDatum(detail->gendata.peakMemoryData.avg_value);
        values[++i] = Int32GetDatum(detail->gendata.peakMemoryData.skew_percent);
        values[++i] = CStringGetTextDatum(spill_info);
        values[++i] = Int32GetDatum(detail->gendata.spillData.min_value);
        values[++i] = Int32GetDatum(detail->gendata.spillData.max_value);
        values[++i] = Int32GetDatum(detail->gendata.spillData.avg_value);
        values[++i] = Int32GetDatum(detail->gendata.spillData.skew_percent);
        values[++i] = Int64GetDatum(detail->gendata.dnTimeData.min_value);
        values[++i] = Int64GetDatum(detail->gendata.dnTimeData.max_value);
        values[++i] = Int64GetDatum(detail->gendata.dnTimeData.avg_value);
        values[++i] = Int32GetDatum(detail->gendata.dnTimeData.skew_percent);
        values[++i] = Int64GetDatum(detail->gendata.cpuData.min_value);
        values[++i] = Int64GetDatum(detail->gendata.cpuData.max_value);
        values[++i] = Int64GetDatum(detail->gendata.cpuData.total_value);
        values[++i] = Int32GetDatum(detail->gendata.cpuData.skew_percent);
        values[++i] = Int32GetDatum(detail->gendata.peakIopsData.min_value);
        values[++i] = Int32GetDatum(detail->gendata.peakIopsData.max_value);
        values[++i] = Int32GetDatum(detail->gendata.peakIopsData.avg_value);
        values[++i] = Int32GetDatum(detail->gendata.peakIopsData.skew_percent);
        if (warnstr[0])
            values[++i] = CStringGetTextDatum(warnstr);
        else
            nulls[++i] = true;
        values[++i] = Int64GetDatum(detail->debug_query_id);
        if (detail->statement && detail->statement[0]) {
            char* mask_string = NULL;
            MASK_PASSWORD_START(mask_string, detail->statement);
            values[++i] = CStringGetTextDatum(mask_string);
            MASK_PASSWORD_END(mask_string, detail->statement);
        } else {
            nulls[++i] = true;
        }
        if (t_thrd.proc->workingVersionNum >= SLOW_QUERY_VERSION) {
            values[++i] = CStringGetTextDatum(PlanListToString(detail->gendata.query_plan));
        } else {
            values[++i] = CStringGetTextDatum(detail->query_plan);
        }

        if (StringIsValid(detail->nodegroup))
            values[++i] = CStringGetTextDatum(detail->nodegroup);
        else
            values[++i] = CStringGetTextDatum("installation");

        int CPU[TOP5] = {-1};
        int MEM[TOP5] = {-1};
        char CPUName[TOP5][NAMEDATALEN] = {{0}};
        char MEMName[TOP5][NAMEDATALEN] = {{0}};
        for (int j = 0; j < TOP5; j++) {
            rc = memset_s(CPUName[j], NAMEDATALEN, 0, NAMEDATALEN);
            securec_check(rc, "\0", "\0");
            rc = memset_s(MEMName[j], NAMEDATALEN, 0, NAMEDATALEN);
            securec_check(rc, "\0", "\0");
        }
        GetTopNInfo(detail->gendata.WLMCPUTopDnInfo, CPUName, CPU, TOP5);
        GetTopNInfo(detail->gendata.WLMMEMTopDnInfo, MEMName, MEM, TOP5);

        values[++i] = CStringGetTextDatum(CPUName[0]);
        values[++i] = CStringGetTextDatum(CPUName[1]);
        values[++i] = CStringGetTextDatum(CPUName[2]);
        values[++i] = CStringGetTextDatum(CPUName[3]);
        values[++i] = CStringGetTextDatum(CPUName[4]);
        values[++i] = CStringGetTextDatum(MEMName[0]);
        values[++i] = CStringGetTextDatum(MEMName[1]);
        values[++i] = CStringGetTextDatum(MEMName[2]);
        values[++i] = CStringGetTextDatum(MEMName[3]);
        values[++i] = CStringGetTextDatum(MEMName[4]);
        values[++i] = Int64GetDatum(CPU[0]);
        values[++i] = Int64GetDatum(CPU[1]);
        values[++i] = Int64GetDatum(CPU[2]);
        values[++i] = Int64GetDatum(CPU[3]);
        values[++i] = Int64GetDatum(CPU[4]);
        values[++i] = Int64GetDatum(MEM[0]);
        values[++i] = Int64GetDatum(MEM[1]);
        values[++i] = Int64GetDatum(MEM[2]);
        values[++i] = Int64GetDatum(MEM[3]);
        values[++i] = Int64GetDatum(MEM[4]);
        StringInfoData CputopNstr;
        StringInfoData MemtopNstr;
        initStringInfo(&CputopNstr);
        initStringInfo(&MemtopNstr);
        GetTopNInfoJsonFormat(CputopNstr, CPUName, CPU, TOP5);
        GetTopNInfoJsonFormat(MemtopNstr, MEMName, MEM, TOP5);
        values[++i] = CStringGetTextDatum(MemtopNstr.data);
        values[++i] = CStringGetTextDatum(CputopNstr.data);
        pfree_ext(MemtopNstr.data);
        pfree_ext(CputopNstr.data);
        if (t_thrd.proc->workingVersionNum >= SLOW_QUERY_VERSION) {
            values[++i] = Int64GetDatum(detail->n_returned_rows);
            values[++i] = Int64GetDatum(detail->gendata.slowQueryInfo.current_table_counter->t_tuples_fetched);
            values[++i] = Int64GetDatum(detail->gendata.slowQueryInfo.current_table_counter->t_tuples_returned);
            values[++i] = Int64GetDatum(detail->gendata.slowQueryInfo.current_table_counter->t_tuples_inserted);
            values[++i] = Int64GetDatum(detail->gendata.slowQueryInfo.current_table_counter->t_tuples_updated);
            values[++i] = Int64GetDatum(detail->gendata.slowQueryInfo.current_table_counter->t_tuples_deleted);
            values[++i] = Int64GetDatum(detail->gendata.slowQueryInfo.current_table_counter->t_blocks_fetched);
            values[++i] = Int64GetDatum(detail->gendata.slowQueryInfo.current_table_counter->t_blocks_hit);
            /* time Info */
            for (num = 0; num < TOTAL_TIME_INFO_TYPES; num++) {
                values[++i] = Int64GetDatum(detail->gendata.slowQueryInfo.localTimeInfoArray[num]);
            }
            values[++i] = Int64GetDatum(0);
        }
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, result);
    }

    pfree_ext(funcctx->user_fctx);
    funcctx->user_fctx = NULL;

    SRF_RETURN_DONE(funcctx);
}

/*
 * @Description:  get ec operator information from hashtable.
 * @return - Datum
 */
Datum pg_stat_get_wlm_ec_operator_info(PG_FUNCTION_ARGS)
{
    if (!superuser()) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_PROC,
            "pg_stat_get_wlm_ec_operator_info");
    }

#define OPERATOR_SESSION_EC_INFO_ATTRNUM 17
    FuncCallContext* funcctx = NULL;
    int num = 0, i = 0;

    Oid removed = PG_GETARG_OID(0); /* remove session info ? */
    Qpid qid = {0, 0, 0};

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        TupleDesc tupdesc;
        i = 0;

        funcctx = SRF_FIRSTCALL_INIT();

        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(OPERATOR_SESSION_EC_INFO_ATTRNUM, false, TAM_HEAP);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "queryid", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "plan_node_id", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "plan_node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "start_time", TIMESTAMPTZOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "duration", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "tuple_processed", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "min_peak_memory", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "max_peak_memory", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "average_peak_memory", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "ec_operator", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "ec_status", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "ec_execute_datanode", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "ec_dsn", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "ec_username", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "ec_query", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "ec_libodbc_type", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "ec_fetch_count", INT8OID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->user_fctx = ExplainGetSessionInfo(&qid, removed, &num);
        funcctx->max_calls = num;

        MemoryContextSwitchTo(oldcontext);

        if (funcctx->user_fctx == NULL)
            SRF_RETURN_DONE(funcctx);
    }

    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->user_fctx && funcctx->call_cntr < funcctx->max_calls) {
        Datum values[OPERATOR_SESSION_EC_INFO_ATTRNUM];
        bool nulls[OPERATOR_SESSION_EC_INFO_ATTRNUM] = {false};
        HeapTuple tuple = NULL;
        Datum result;
        i = -1;
        errno_t rc = EOK;

        ExplainGeneralInfo* statistics = (ExplainGeneralInfo*)funcctx->user_fctx + funcctx->call_cntr;

        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        values[++i] = Int64GetDatum(statistics->query_id);
        values[++i] = Int32GetDatum(statistics->plan_node_id);
        values[++i] = CStringGetTextDatum(statistics->plan_node_name);
        if (statistics->start_time == 0)
            nulls[++i] = true;
        else
            values[++i] = TimestampGetDatum(statistics->start_time);
        values[++i] = Int64GetDatum(statistics->duration_time);
        values[++i] = Int64GetDatum(statistics->tuple_processed);
        values[++i] = Int32GetDatum(statistics->min_peak_memory);
        values[++i] = Int32GetDatum(statistics->max_peak_memory);
        values[++i] = Int32GetDatum(statistics->avg_peak_memory);

        if (statistics->ec_operator) {
            values[++i] = Int32GetDatum(statistics->ec_operator);
        } else {
            nulls[++i] = true;
        }

        if (statistics->ec_status) {
            if (statistics->ec_status == EC_STATUS_CONNECTED) {
                values[++i] = CStringGetTextDatum("CONNECTED");
            } else if (statistics->ec_status == EC_STATUS_EXECUTED) {
                values[++i] = CStringGetTextDatum("EXECUTED");
            } else if (statistics->ec_status == EC_STATUS_FETCHING) {
                values[++i] = CStringGetTextDatum("FETCHING");
            } else if (statistics->ec_status == EC_STATUS_END) {
                values[++i] = CStringGetTextDatum("END");
            }
        } else {
            nulls[++i] = true;
        }

        if (statistics->ec_execute_datanode) {
            values[++i] = CStringGetTextDatum(statistics->ec_execute_datanode);
        } else {
            nulls[++i] = true;
        }

        if (statistics->ec_dsn) {
            values[++i] = CStringGetTextDatum(statistics->ec_dsn);
        } else {
            nulls[++i] = true;
        }

        if (statistics->ec_username) {
            values[++i] = CStringGetTextDatum(statistics->ec_username);
        } else {
            nulls[++i] = true;
        }

        if (statistics->ec_query) {
            char* mask_string = NULL;
            MASK_PASSWORD_START(mask_string, statistics->ec_query);
            values[++i] = CStringGetTextDatum(mask_string);
            MASK_PASSWORD_END(mask_string, statistics->ec_query);
        } else {
            nulls[++i] = true;
        }

        if (statistics->ec_libodbc_type) {
            if (statistics->ec_libodbc_type == EC_LIBODBC_TYPE_ONE) {
                values[++i] = CStringGetTextDatum("libodbc.so.1");
            } else if (statistics->ec_libodbc_type == EC_LIBODBC_TYPE_TWO) {
                values[++i] = CStringGetTextDatum("libodbc.so.2");
            }
        } else {
            nulls[++i] = true;
        }

        if (statistics->ec_fetch_count) {
            values[++i] = Int64GetDatum(statistics->ec_fetch_count);
        } else {
            nulls[++i] = true;
        }

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, result);
    }

    pfree_ext(funcctx->user_fctx);
    funcctx->user_fctx = NULL;

    SRF_RETURN_DONE(funcctx);
}

static void GetStaticValInfo(Datum *values, ExplainGeneralInfo *statistics)
{
    int i = -1;
    values[++i] = CStringGetTextDatum(statistics->datname);
    values[++i] = Int64GetDatum(statistics->query_id);
    values[++i] = Int32GetDatum(statistics->plan_node_id);
    values[++i] = Int64GetDatum(statistics->startup_time);
    values[++i] = Int64GetDatum(statistics->duration_time);
    values[++i] = Int64GetDatum(statistics->tuple_processed);
    values[++i] = Int32GetDatum(statistics->max_peak_memory);
    values[++i] = Int32GetDatum(statistics->query_dop);
    values[++i] = Int32GetDatum(statistics->parent_node_id);
    values[++i] = Int32GetDatum(statistics->left_child_id);
    values[++i] = Int32GetDatum(statistics->right_child_id);
    values[++i] = CStringGetTextDatum(statistics->operation);
    values[++i] = CStringGetTextDatum(statistics->orientation);
    values[++i] = CStringGetTextDatum(statistics->strategy);
    values[++i] = CStringGetTextDatum(statistics->options);
    values[++i] = CStringGetTextDatum(statistics->condition);
    values[++i] = CStringGetTextDatum(statistics->projection);
}

/**
 * @Description: get table information of gs_wlm_plan_operator_info from in memory hash table
 * @in 1: remove data after look; 0: do not remove data
 * @out setof record in the form of gs_wlm_plan_operator_info
 */
Datum gs_stat_get_wlm_plan_operator_info(PG_FUNCTION_ARGS)
{
#ifdef ENABLE_MULTIPLE_NODES
    ereport(ERROR, (errmodule(MOD_OPT_AI), 
                    errmsg("This function is not available in multipule nodes mode")));
#endif
    const static int operator_plan_info_attrnum = 17;
    FuncCallContext *funcctx = NULL;
    int num = 0;

    Oid removed = PG_GETARG_OID(0); /* remove session info ? */
    Qpid qid = {0, 0, 0};

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        funcctx = SRF_FIRSTCALL_INIT();

        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        InitPlanOperatorInfoTuple(operator_plan_info_attrnum, funcctx);
        funcctx->user_fctx  = ExplainGetSessionInfo(&qid, removed, &num);
        funcctx->max_calls  = num;

        (void) MemoryContextSwitchTo(oldcontext);

        if (funcctx->user_fctx == NULL)
            SRF_RETURN_DONE(funcctx);
    }

    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->user_fctx && funcctx->call_cntr < funcctx->max_calls) {
        Datum       values[operator_plan_info_attrnum];
        bool        nulls[operator_plan_info_attrnum];
        HeapTuple   tuple;
        Datum       result;
        errno_t rc = EOK;

        ExplainGeneralInfo *statistics = (ExplainGeneralInfo*)funcctx->user_fctx + funcctx->call_cntr;

        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        GetStaticValInfo(values, statistics);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, result);
    }

    pfree_ext(funcctx->user_fctx);
    funcctx->user_fctx = NULL;

    SRF_RETURN_DONE(funcctx);
}

static void InitPlanOperatorInfoTuple(int operator_plan_info_attrnum, FuncCallContext *funcctx)
{
    int i = 0;
    TupleDesc   tupdesc;

    tupdesc = CreateTemplateTupleDesc(operator_plan_info_attrnum, false);
    TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "datname", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "queryid", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "plan_node_id", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "startup_time", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "total_time", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "actual_rows", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "max_peak_memory", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "query_dop", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "parent_node_id", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "left_child_id", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "right_child_id", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "operation", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "orientation", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "strategy", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "options", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "condition", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "projection", TEXTOID, -1, 0);
    funcctx->tuple_desc = BlessTupleDesc(tupdesc);
}

/*
 * @Description:  get  operator information from hashtable.
 * @return - Datum
 */
Datum pg_stat_get_wlm_operator_info(PG_FUNCTION_ARGS)
{
    if (!superuser()) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_PROC,
            "pg_stat_get_wlm_operator_info");
    }

#define OPERATOR_SESSION_INFO_ATTRNUM 22
    FuncCallContext* funcctx = NULL;
    int num = 0, i = 0;

    Oid removed = PG_GETARG_OID(0); /* remove session info ? */
    Qpid qid = {0, 0, 0};

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        TupleDesc tupdesc;
        i = 0;

        funcctx = SRF_FIRSTCALL_INIT();

        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(OPERATOR_SESSION_INFO_ATTRNUM, false, TAM_HEAP);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "queryid", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "pid", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "plan_node_id", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "plan_node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "start_time", TIMESTAMPTZOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "duration", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "query_dop", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "estimated_rows", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "tuple_processed", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "min_peak_memory", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "max_peak_memory", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "average_peak_memory", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "memory_skew_percent", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "min_spill_size", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "max_spill_size", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "average_spill_size", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "spill_skew_percent", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "min_cpu_time", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "max_cpu_time", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "total_cpu_time", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "cpu_skew_percent", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "warn_prof_info", TEXTOID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->user_fctx = ExplainGetSessionInfo(&qid, removed, &num);
        funcctx->max_calls = num;

        (void)MemoryContextSwitchTo(oldcontext);

        if (funcctx->user_fctx == NULL)
            SRF_RETURN_DONE(funcctx);
    }

    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->user_fctx && funcctx->call_cntr < funcctx->max_calls) {
        Datum values[OPERATOR_SESSION_INFO_ATTRNUM];
        bool nulls[OPERATOR_SESSION_INFO_ATTRNUM] = {false};
        HeapTuple tuple = NULL;
        Datum result;
        i = -1;
        char warnstr[WARNING_INFO_LEN] = {0};
        errno_t rc = EOK;

        ExplainGeneralInfo* statistics = (ExplainGeneralInfo*)funcctx->user_fctx + funcctx->call_cntr;

        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        WLMGetWarnInfo(warnstr, sizeof(warnstr), statistics->warn_prof_info, statistics->max_spill_size, 0);

        values[++i] = Int64GetDatum(statistics->query_id);
        values[++i] = Int64GetDatum(statistics->tid);
        values[++i] = Int32GetDatum(statistics->plan_node_id);
        values[++i] = CStringGetTextDatum(statistics->plan_node_name);
        if (statistics->start_time == 0)
            nulls[++i] = true;
        else
            values[++i] = TimestampGetDatum(statistics->start_time);
        values[++i] = Int64GetDatum(statistics->duration_time);
        values[++i] = Int32GetDatum(statistics->query_dop);
        values[++i] = Int64GetDatum(statistics->estimate_rows);
        values[++i] = Int64GetDatum(statistics->tuple_processed);
        values[++i] = Int32GetDatum(statistics->min_peak_memory);
        values[++i] = Int32GetDatum(statistics->max_peak_memory);
        values[++i] = Int32GetDatum(statistics->avg_peak_memory);
        values[++i] = Int32GetDatum(statistics->memory_skewed);
        values[++i] = Int32GetDatum(statistics->min_spill_size);
        values[++i] = Int32GetDatum(statistics->max_spill_size);
        values[++i] = Int32GetDatum(statistics->avg_spill_size);
        values[++i] = Int32GetDatum(statistics->i_o_skew);
        values[++i] = Int64GetDatum(statistics->min_cpu_time);
        values[++i] = Int64GetDatum(statistics->max_cpu_time);
        values[++i] = Int64GetDatum(statistics->total_cpu_time);
        values[++i] = Int32GetDatum(statistics->cpu_skew);
        if (warnstr[0])
            values[++i] = CStringGetTextDatum(warnstr);
        else
            nulls[++i] = true;

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, result);
    }

    pfree_ext(funcctx->user_fctx);
    funcctx->user_fctx = NULL;

    SRF_RETURN_DONE(funcctx);
}

/*
 * function name: pg_stat_get_wlm_instance_info
 * description  : the view will show the instance info.
 */
Datum pg_stat_get_wlm_instance_info(PG_FUNCTION_ARGS)
{
    if (!superuser()) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_PROC,
            "pg_stat_get_wlm_instance_info");
    }

#define WLM_INSTANCE_INFO_ATTRNUM 15
    FuncCallContext* funcctx = NULL;
    int num = 0, i = 0;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        TupleDesc tupdesc;
        i = 0;

        funcctx = SRF_FIRSTCALL_INIT();

        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(WLM_INSTANCE_INFO_ATTRNUM, false, TAM_HEAP);

        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "instancename", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "timestamp", TIMESTAMPTZOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "used_cpu", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "free_mem", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "used_mem", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "io_await", FLOAT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "io_util", FLOAT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "disk_read", FLOAT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "disk_write", FLOAT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "process_read", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "process_write", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "logical_read", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "logical_write", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "read_counts", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "write_counts", INT8OID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->user_fctx = WLMGetInstanceInfo(&num, false);
        funcctx->max_calls = num;

        MemoryContextSwitchTo(oldcontext);

        if (funcctx->user_fctx == NULL)
            SRF_RETURN_DONE(funcctx);
    }

    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->user_fctx && funcctx->call_cntr < funcctx->max_calls) {
        Datum values[WLM_INSTANCE_INFO_ATTRNUM];
        bool nulls[WLM_INSTANCE_INFO_ATTRNUM];
        HeapTuple tuple;
        Datum result;
        i = -1;
        errno_t rc = 0;

        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");

        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        WLMInstanceInfo* instanceInfo = (WLMInstanceInfo*)funcctx->user_fctx + funcctx->call_cntr;

        values[++i] = CStringGetTextDatum(g_instance.wlm_cxt->instance_manager.instancename);
        values[++i] = Int64GetDatum(instanceInfo->timestamp);
        values[++i] = Int32GetDatum((int)(instanceInfo->usr_cpu + instanceInfo->sys_cpu));
        values[++i] = Int32GetDatum(instanceInfo->free_mem);
        values[++i] = Int32GetDatum(instanceInfo->used_mem);
        values[++i] = Float8GetDatum(instanceInfo->io_await);
        values[++i] = Float8GetDatum(instanceInfo->io_util);
        values[++i] = Float8GetDatum(instanceInfo->disk_read);
        values[++i] = Float8GetDatum(instanceInfo->disk_write);
        values[++i] = Int64GetDatum(instanceInfo->process_read_speed);
        values[++i] = Int64GetDatum(instanceInfo->process_write_speed);
        values[++i] = Int64GetDatum(instanceInfo->logical_read_speed);
        values[++i] = Int64GetDatum(instanceInfo->logical_write_speed);
        values[++i] = Int64GetDatum(instanceInfo->read_counts);
        values[++i] = Int64GetDatum(instanceInfo->write_counts);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, result);
    }

    pfree_ext(funcctx->user_fctx);
    funcctx->user_fctx = NULL;

    SRF_RETURN_DONE(funcctx);
}

/*
 * function name: pg_stat_get_wlm_instance_info_with_cleanup
 * description  : this function is used for persistent history data.
 */
Datum pg_stat_get_wlm_instance_info_with_cleanup(PG_FUNCTION_ARGS)
{
    if (!superuser()) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_PROC,
            "pg_stat_get_wlm_instance_info_with_cleanup");
    }

#define WLM_INSTANCE_INFO_ATTRNUM 15
    FuncCallContext* funcctx = NULL;
    int num = 0, i = 0;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        TupleDesc tupdesc;
        i = 0;

        funcctx = SRF_FIRSTCALL_INIT();

        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(WLM_INSTANCE_INFO_ATTRNUM, false, TAM_HEAP);

        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "instancename", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "timestamp", TIMESTAMPTZOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "used_cpu", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "free_mem", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "used_mem", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "io_await", FLOAT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "io_util", FLOAT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "disk_read", FLOAT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "disk_write", FLOAT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "process_read", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "process_write", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "logical_read", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "logical_write", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "read_counts", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "write_counts", INT8OID, -1, 0);

        ereport(DEBUG2, (errmodule(MOD_WLM),
            errmsg("Begin to get instance info with cleanup.")));

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->user_fctx = WLMGetInstanceInfo(&num, true);
        funcctx->max_calls = num;

        MemoryContextSwitchTo(oldcontext);

        if (funcctx->user_fctx == NULL)
            SRF_RETURN_DONE(funcctx);
    }

    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->user_fctx && funcctx->call_cntr < funcctx->max_calls) {
        Datum values[WLM_INSTANCE_INFO_ATTRNUM];
        bool nulls[WLM_INSTANCE_INFO_ATTRNUM];
        HeapTuple tuple;
        Datum result;
        i = -1;
        errno_t rc = 0;

        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");

        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        WLMInstanceInfo* instanceInfo = (WLMInstanceInfo*)funcctx->user_fctx + funcctx->call_cntr;

        values[++i] = CStringGetTextDatum(g_instance.wlm_cxt->instance_manager.instancename);
        values[++i] = Int64GetDatum(instanceInfo->timestamp);
        values[++i] = Int32GetDatum((int)(instanceInfo->usr_cpu + instanceInfo->sys_cpu));
        values[++i] = Int32GetDatum(instanceInfo->free_mem);
        values[++i] = Int32GetDatum(instanceInfo->used_mem);
        values[++i] = Float8GetDatum(instanceInfo->io_await);
        values[++i] = Float8GetDatum(instanceInfo->io_util);
        values[++i] = Float8GetDatum(instanceInfo->disk_read);
        values[++i] = Float8GetDatum(instanceInfo->disk_write);
        values[++i] = Int64GetDatum(instanceInfo->process_read_speed);
        values[++i] = Int64GetDatum(instanceInfo->process_write_speed);
        values[++i] = Int64GetDatum(instanceInfo->logical_read_speed);
        values[++i] = Int64GetDatum(instanceInfo->logical_write_speed);
        values[++i] = Int64GetDatum(instanceInfo->read_counts);
        values[++i] = Int64GetDatum(instanceInfo->write_counts);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, result);
    }

    pfree_ext(funcctx->user_fctx);
    funcctx->user_fctx = NULL;

    ereport(DEBUG2, (errmodule(MOD_WLM),
        errmsg("Get instance info with cleanup finished.")));

    SRF_RETURN_DONE(funcctx);
}

/*
 * @Description: get user resource info
 * @IN void
 * @Return: records
 * @See also:
 */
Datum pg_stat_get_wlm_user_resource_info(PG_FUNCTION_ARGS)
{
#define WLM_USER_RESOURCE_ATTRNUM 17
    char* name = PG_GETARG_CSTRING(0);

    FuncCallContext* funcctx = NULL;
    const int num = 1;
    int i = 0;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        TupleDesc tupdesc;
        i = 0;

        funcctx = SRF_FIRSTCALL_INIT();

        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(WLM_USER_RESOURCE_ATTRNUM, false, TAM_HEAP);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "user_id", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "used_memory", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "total_memory", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "used_cpu", FLOAT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "total_cpu", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "used_space", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "total_space", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "used_temp_space", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "total_temp_space", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "used_spill_space", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "total_spill_space", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "read_kbytes", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "write_kbytes", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "read_counts", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "write_counts", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "read_speed", FLOAT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "write_speed", FLOAT8OID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->user_fctx = GetUserResourceData(name); /* get user resource data */
        funcctx->max_calls = num;

        MemoryContextSwitchTo(oldcontext);

        if (funcctx->user_fctx == NULL)
            SRF_RETURN_DONE(funcctx);
    }

    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->call_cntr < funcctx->max_calls) {
        Datum values[WLM_USER_RESOURCE_ATTRNUM];
        bool nulls[WLM_USER_RESOURCE_ATTRNUM];
        HeapTuple tuple;
        i = -1;

        UserResourceData* rsdata = (UserResourceData*)funcctx->user_fctx + funcctx->call_cntr;

        errno_t rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        /* Values only available to same user or superuser */
        if (superuser() || rsdata->userid == GetUserId()) {
            /* fill in all attribute value */
            values[++i] = ObjectIdGetDatum(rsdata->userid);
            values[++i] = Int32GetDatum(rsdata->used_memory);
            values[++i] = Int32GetDatum(rsdata->total_memory);
            values[++i] = Int32GetDatum(rsdata->used_cpuset * 1.0 / GROUP_ALL_PERCENT);
            values[++i] = Int32GetDatum(rsdata->total_cpuset);
            values[++i] = Int64GetDatum(rsdata->used_space);
            values[++i] = Int64GetDatum(rsdata->total_space);
            values[++i] = Int64GetDatum(rsdata->used_temp_space);
            values[++i] = Int64GetDatum(rsdata->total_temp_space);
            values[++i] = Int64GetDatum(rsdata->used_spill_space);
            values[++i] = Int64GetDatum(rsdata->total_spill_space);
            values[++i] = Int64GetDatum(rsdata->read_bytes / 1024);
            values[++i] = Int64GetDatum(rsdata->write_bytes / 1024);
            values[++i] = Int64GetDatum(rsdata->read_counts);
            values[++i] = Int64GetDatum(rsdata->write_counts);
            values[++i] = Float8GetDatum(rsdata->read_speed);
            values[++i] = Float8GetDatum(rsdata->write_speed);          
        }else {     
            for (int j = 0; j < WLM_USER_RESOURCE_ATTRNUM; ++j) {
                nulls[j] = true;
            }
        }
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }

    pfree_ext(funcctx->user_fctx);
    funcctx->user_fctx = NULL;

    SRF_RETURN_DONE(funcctx);
}

/*
 * @Description: the view will show resource pool info in the resource pool hash table.
 * @IN void
 * @Return: records
 * @See also:
 */
Datum pg_stat_get_resource_pool_info(PG_FUNCTION_ARGS)
{
#define WLM_RESOURCE_POOL_ATTRNUM 7
    FuncCallContext* funcctx = NULL;
    int num = 0, i = 0;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        TupleDesc tupdesc;
        i = 0;

        funcctx = SRF_FIRSTCALL_INIT();

        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(WLM_RESOURCE_POOL_ATTRNUM, false, TAM_HEAP);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "respool_oid", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "ref_count", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "active_points", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "running_count", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "waiting_count", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "iops_limits", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "io_priority", INT4OID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->user_fctx = WLMGetResourcePoolDataInfo(&num);
        funcctx->max_calls = num;

        MemoryContextSwitchTo(oldcontext);

        if (funcctx->user_fctx == NULL)
            SRF_RETURN_DONE(funcctx);
    }

    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->call_cntr < funcctx->max_calls) {
        Datum values[WLM_RESOURCE_POOL_ATTRNUM];
        bool nulls[WLM_RESOURCE_POOL_ATTRNUM] = {false};
        HeapTuple tuple = NULL;
        i = -1;

        ResourcePool* rp = (ResourcePool*)funcctx->user_fctx + funcctx->call_cntr;

        errno_t rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        /* Locking is probably not really necessary */
        values[++i] = ObjectIdGetDatum(rp->rpoid);
        values[++i] = Int32GetDatum(rp->ref_count);
        values[++i] = Int32GetDatum(rp->active_points);
        values[++i] = Int32GetDatum(rp->running_count);
        values[++i] = Int32GetDatum(rp->waiting_count);
        values[++i] = Int32GetDatum(rp->iops_limits);
        values[++i] = Int32GetDatum(rp->io_priority);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }

    pfree_ext(funcctx->user_fctx);
    funcctx->user_fctx = NULL;

    SRF_RETURN_DONE(funcctx);
}

/*
 * @Description:  wlm get all user info in hash table.
 * @IN void
 * @Return: records
 * @See also:
 */
Datum pg_stat_get_wlm_user_info(PG_FUNCTION_ARGS)
{
    if (!superuser()) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_PROC,
            "pg_stat_get_wlm_user_info");
    }
#define WLM_USER_INFO_ATTRNUM 8
    FuncCallContext* funcctx = NULL;
    int num = 0, i = 0;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        TupleDesc tupdesc;
        i = 0;

        funcctx = SRF_FIRSTCALL_INIT();

        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(WLM_USER_INFO_ATTRNUM, false, TAM_HEAP);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "userid", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "sysadmin", BOOLOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "rpoid", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "parentid", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "totalspace", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "spacelimit", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "childcount", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "childlist", TEXTOID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->user_fctx = WLMGetAllUserData(&num);
        funcctx->max_calls = num;

        MemoryContextSwitchTo(oldcontext);

        if (funcctx->user_fctx == NULL)
            SRF_RETURN_DONE(funcctx);
    }

    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->call_cntr < funcctx->max_calls) {
        Datum values[WLM_USER_INFO_ATTRNUM];
        bool nulls[WLM_USER_INFO_ATTRNUM] = {false};
        HeapTuple tuple = NULL;
        i = -1;

        WLMUserInfo* info = (WLMUserInfo*)funcctx->user_fctx + funcctx->call_cntr;

        errno_t rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        if (superuser() || info->userid == GetUserId()) {
            /* Locking is probably not really necessary */
            values[++i] = ObjectIdGetDatum(info->userid);
            values[++i] = BoolGetDatum(info->admin);
            values[++i] = ObjectIdGetDatum(info->rpoid);
            values[++i] = ObjectIdGetDatum(info->parentid);
            values[++i] = Int64GetDatum(info->space);
            values[++i] = Int64GetDatum(info->spacelimit);
            values[++i] = Int32GetDatum(info->childcount);

            if (info->children == NULL)
                info->children = "No Child";

            values[++i] = CStringGetTextDatum(info->children);
        } else {
            for (i = 0; i < WLM_USER_INFO_ATTRNUM; ++i) {
                nulls[i] = true;
            }
        }
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }
    SRF_RETURN_DONE(funcctx);
}

/*
 * @Description: the view will show cgroup info in the cgroup hash table.
 * @IN PG_FUNCTION_ARGS
 * @Return: data record
 * @See also:
 */
Datum pg_stat_get_cgroup_info(PG_FUNCTION_ARGS)
{
#define WLM_CGROUP_INFO_ATTRNUM 9
    FuncCallContext* funcctx = NULL;
    int num = 0, i = 0;
    errno_t rc;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        TupleDesc tupdesc;
        i = 0;

        funcctx = SRF_FIRSTCALL_INIT();

        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(WLM_CGROUP_INFO_ATTRNUM, false, TAM_HEAP);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "cgroup_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "percent", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "usagepct", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "shares", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "usage", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "cpuset", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "relpath", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "valid", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "node_group", TEXTOID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->user_fctx = gscgroup_get_cgroup_info(&num);

        funcctx->max_calls = num;

        MemoryContextSwitchTo(oldcontext);

        if (funcctx->user_fctx == NULL)
            SRF_RETURN_DONE(funcctx);
    }

    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->user_fctx && funcctx->call_cntr < funcctx->max_calls) {
        Datum values[WLM_CGROUP_INFO_ATTRNUM];
        bool nulls[WLM_CGROUP_INFO_ATTRNUM] = {false};
        HeapTuple tuple = NULL;
        Datum result;
        i = -1;

        gscgroup_info_t* info = (gscgroup_info_t*)funcctx->user_fctx + funcctx->call_cntr;

        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        values[++i] = CStringGetTextDatum(info->entry.name);
        values[++i] = Int32GetDatum(info->entry.percent);
        values[++i] = Int32GetDatum(info->entry.cpuUtil);
        values[++i] = Int64GetDatum(info->shares);
        values[++i] = Int64GetDatum(info->entry.cpuUsedAcct / NANOSECS_PER_SEC);
        values[++i] = CStringGetTextDatum(info->cpuset);
        values[++i] = CStringGetTextDatum(info->relpath);
        values[++i] = CStringGetTextDatum(info->valid ? "YES" : "NO");
        values[++i] = CStringGetTextDatum(info->nodegroup);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, result);
    }

    pfree_ext(funcctx->user_fctx);
    funcctx->user_fctx = NULL;

    SRF_RETURN_DONE(funcctx);
}

/*
 * @Description:  wlm get session info for query.
 * @IN tid: thread id
 * @Return: records
 * @See also:
 */
Datum pg_wlm_get_session_info(PG_FUNCTION_ARGS)
{
    ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("The function 'pg_wlm_get_session_info' is not supported now!")));
    return (Datum)0;
}

/*
 * @Description:  wlm get session info for user.
 * @IN tid: thread id
 * @Return: records
 * @See also:
 */
Datum pg_wlm_get_user_session_info(PG_FUNCTION_ARGS)
{
    ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("The function 'gs_wlm_get_user_session_info' is not supported now!")));
    return (Datum)0;
}

/*
 * function name: pg_stat_get_wlm_workload_records
 * description  : the view will show the workload records in cache.
 */
Datum pg_stat_get_workload_records(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_MULTIPLE_NODES
    FuncCallContext* funcctx = NULL;
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        errmsg("unsupported view in single node mode.")));

    SRF_RETURN_DONE(funcctx);
#else
#define WLM_WORKLOAD_RECORDS_ATTRNUM 11
    FuncCallContext* funcctx = NULL;
    int num = 0, i = 0;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        TupleDesc tupdesc;
        i = 0;

        funcctx = SRF_FIRSTCALL_INIT();

        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(WLM_WORKLOAD_RECORDS_ATTRNUM, false, TAM_HEAP);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "node_idx", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "query_pid", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "start_time", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "memory", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "actpts", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "maxpts", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "priority", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "resource_pool", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "queue_type", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "node_group", TEXTOID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->user_fctx = dywlm_get_records(&num);
        funcctx->max_calls = num;

        MemoryContextSwitchTo(oldcontext);

        if (funcctx->user_fctx == NULL)
            SRF_RETURN_DONE(funcctx);
    }

    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->user_fctx && funcctx->call_cntr < funcctx->max_calls) {
        Datum values[WLM_WORKLOAD_RECORDS_ATTRNUM];
        bool nulls[WLM_WORKLOAD_RECORDS_ATTRNUM] = {false};
        HeapTuple tuple = NULL;
        Datum result;
        i = -1;

        DynamicWorkloadRecord* record = (DynamicWorkloadRecord*)funcctx->user_fctx + funcctx->call_cntr;

        errno_t errval = memset_s(&values, sizeof(values), 0, sizeof(values));
        securec_check_errval(errval, , LOG);
        errval = memset_s(&nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check_errval(errval, , LOG);

        const char* qstr = "unknown";

        switch (record->qtype) {
            case PARCTL_GLOBAL:
                qstr = "global";
                break;
            case PARCTL_RESPOOL:
                qstr = "respool";
                break;
            case PARCTL_ACTIVE:
                qstr = "active";
                break;
            default:
                break;
        }

        /* Locking is probably not really necessary */
        values[++i] = ObjectIdGetDatum(record->qid.procId);
        values[++i] = Int64GetDatum(record->qid.queryId);
        values[++i] = Int64GetDatum(record->qid.stamp);
        values[++i] = Int32GetDatum(record->memsize);
        values[++i] = Int32GetDatum(record->actpts);
        values[++i] = Int32GetDatum(record->maxpts < 0 ? 0 : record->maxpts);
        values[++i] = Int32GetDatum(record->priority);
        values[++i] = CStringGetTextDatum(record->rpname);
        values[++i] = CStringGetTextDatum(record->nodename);
        values[++i] = CStringGetTextDatum(qstr);

        if (StringIsValid(record->groupname))
            values[++i] = CStringGetTextDatum(record->groupname);
        else
            values[++i] = CStringGetTextDatum("installation");

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, result);
    }

    pfree_ext(funcctx->user_fctx);
    funcctx->user_fctx = NULL;

    SRF_RETURN_DONE(funcctx);
#endif
}

Datum pg_stat_get_bgwriter_timed_checkpoints(PG_FUNCTION_ARGS)
{
    PG_RETURN_INT64(pgstat_fetch_global()->timed_checkpoints);
}

Datum pg_stat_get_bgwriter_requested_checkpoints(PG_FUNCTION_ARGS)
{
    PG_RETURN_INT64(pgstat_fetch_global()->requested_checkpoints);
}

Datum pg_stat_get_bgwriter_buf_written_checkpoints(PG_FUNCTION_ARGS)
{
    PG_RETURN_INT64(pgstat_fetch_global()->buf_written_checkpoints);
}

Datum pg_stat_get_bgwriter_buf_written_clean(PG_FUNCTION_ARGS)
{
    PG_RETURN_INT64(pgstat_fetch_global()->buf_written_clean);
}

Datum pg_stat_get_bgwriter_maxwritten_clean(PG_FUNCTION_ARGS)
{
    PG_RETURN_INT64(pgstat_fetch_global()->maxwritten_clean);
}

Datum pg_stat_get_checkpoint_write_time(PG_FUNCTION_ARGS)
{
    /* time is already in msec, just convert to double for presentation */
    PG_RETURN_FLOAT8((double)pgstat_fetch_global()->checkpoint_write_time);
}

Datum pg_stat_get_checkpoint_sync_time(PG_FUNCTION_ARGS)
{
    /* time is already in msec, just convert to double for presentation */
    PG_RETURN_FLOAT8((double)pgstat_fetch_global()->checkpoint_sync_time);
}

Datum pg_stat_get_bgwriter_stat_reset_time(PG_FUNCTION_ARGS)
{
    PG_RETURN_TIMESTAMPTZ(pgstat_fetch_global()->stat_reset_timestamp);
}

Datum pg_stat_get_buf_written_backend(PG_FUNCTION_ARGS)
{
    PG_RETURN_INT64(pgstat_fetch_global()->buf_written_backend);
}

Datum pg_stat_get_buf_fsync_backend(PG_FUNCTION_ARGS)
{
    PG_RETURN_INT64(pgstat_fetch_global()->buf_fsync_backend);
}

Datum pg_stat_get_buf_alloc(PG_FUNCTION_ARGS)
{
    PG_RETURN_INT64(pgstat_fetch_global()->buf_alloc);
}

Datum pg_stat_get_xact_numscans(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    int64 result = 0;
    List* stat_list = NIL;
    ListCell* stat_cell = NULL;
    uint32 statFlag = STATFLG_RELATION;
    PgStat_TableStatus* tabentry = NULL;
    pg_stat_get_stat_list(&stat_list, &statFlag, relid);

    foreach (stat_cell, stat_list) {
        Oid relationid = lfirst_oid(stat_cell);
        tabentry = find_tabstat_entry(relationid, statFlag);

        if (!PointerIsValid(tabentry)) {
            result += 0;
        } else {
            result += (int64)(tabentry->t_counts.t_numscans);
        }
    }

    if (PointerIsValid(stat_list)) {
        list_free(stat_list);
    }

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_xact_tuples_returned(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    int64 result = 0;
    List* stat_list = NIL;
    ListCell* stat_cell = NULL;
    uint32 statFlag = STATFLG_RELATION;
    PgStat_TableStatus* tabentry = NULL;
    pg_stat_get_stat_list(&stat_list, &statFlag, relid);

    foreach (stat_cell, stat_list) {
        Oid relationid = lfirst_oid(stat_cell);
        tabentry = find_tabstat_entry(relationid, statFlag);

        if (!PointerIsValid(tabentry)) {
            result += 0;
        } else {
            result += (int64)(tabentry->t_counts.t_tuples_returned);
        }
    }

    if (PointerIsValid(stat_list)) {
        list_free(stat_list);
    }

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_xact_tuples_fetched(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    int64 result = 0;
    List* stat_list = NIL;
    ListCell* stat_cell = NULL;
    uint32 statFlag = STATFLG_RELATION;
    PgStat_TableStatus* tabentry = NULL;
    pg_stat_get_stat_list(&stat_list, &statFlag, relid);

    foreach (stat_cell, stat_list) {
        Oid relationid = lfirst_oid(stat_cell);
        tabentry = find_tabstat_entry(relationid, statFlag);

        if (!PointerIsValid(tabentry)) {
            result += 0;
        } else {
            result += (int64)(tabentry->t_counts.t_tuples_fetched);
        }
    }

    if (PointerIsValid(stat_list)) {
        list_free(stat_list);
    }

    PG_RETURN_INT64(result);
}

static void pg_stat_update_xact_tuples(Oid relid, PgStat_Counter *result, XactAction action)
{
    Assert(result != NULL);

    PgStat_TableStatus* tabentry = NULL;
    PgStat_TableXactStatus* trans = NULL;
    List* stat_list = NIL;
    ListCell* stat_cell = NULL;
    List* sublist = NIL;
    ListCell* subcell = NULL;
    Oid t_statFlag = InvalidOid;
    Oid t_id = InvalidOid;

    int offset1 = 0; /* offset of PgStat_TableCounts */
    int offset2 = 0; /* offset of PgStat_TableXactStatus */
    bool hassubtransactions = false;
    switch (action) {
        /* for iud, subtransactions' counts aren't in t_tuples yet */
        case XACTION_INSERT:
            offset1 = offsetof(PgStat_TableCounts, t_tuples_inserted);
            offset2 = offsetof(PgStat_TableXactStatus, tuples_inserted);
            hassubtransactions = true;
            break;
        case XACTION_UPDATE:
            offset1 = offsetof(PgStat_TableCounts, t_tuples_updated);
            offset2 = offsetof(PgStat_TableXactStatus, tuples_updated);
            hassubtransactions = true;
            break;
        case XACTION_DELETE:
            offset1 = offsetof(PgStat_TableCounts, t_tuples_deleted);
            offset2 = offsetof(PgStat_TableXactStatus, tuples_deleted);
            hassubtransactions = true;
            break;
        case XACTION_HOTUPDATE:
            offset1 = offsetof(PgStat_TableCounts, t_tuples_hot_updated);
            break;
        default:
            return;
    }

    HeapTuple reltuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(reltuple)) {
        return;
    }
    Form_pg_class relation = (Form_pg_class)GETSTRUCT(reltuple);

    if (PARTTYPE_SUBPARTITIONED_RELATION == relation->parttype) {
        stat_list = getPartitionObjectIdList(relid, PART_OBJ_TYPE_TABLE_PARTITION);
        foreach (stat_cell, stat_list) {
            Oid subparentid = lfirst_oid(stat_cell);
            sublist = getPartitionObjectIdList(subparentid, PART_OBJ_TYPE_TABLE_SUB_PARTITION);
            t_statFlag = subparentid;
            foreach (subcell, sublist) {
                t_id = lfirst_oid(subcell);
                tabentry = find_tabstat_entry(t_id, t_statFlag);
                if (tabentry != NULL) {
                    *result += *(PgStat_Counter *)((char *)&tabentry->t_counts + offset1);
                    if (hassubtransactions) {
                        for (trans = tabentry->trans; trans != NULL; trans = trans->upper)
                            *result += *(PgStat_Counter *)((char *)trans + offset2);
                    }
                }
            }
            list_free_ext(sublist);
        }
        list_free_ext(stat_list);
    } else if (PARTTYPE_PARTITIONED_RELATION == relation->parttype) {
        char objtype = PART_OBJ_TYPE_TABLE_PARTITION;
        if (relation->relkind == RELKIND_INDEX) {
            objtype = PART_OBJ_TYPE_INDEX_PARTITION;
        }
        stat_list = getPartitionObjectIdList(relid, objtype);
        t_statFlag = relid;
        foreach (stat_cell, stat_list) {
            t_id = lfirst_oid(stat_cell);
            tabentry = find_tabstat_entry(t_id, t_statFlag);
            if (tabentry != NULL) {
                *result += *(PgStat_Counter *)((char *)&tabentry->t_counts + offset1);
                if (hassubtransactions) {
                    for (trans = tabentry->trans; trans != NULL; trans = trans->upper)
                        *result += *(PgStat_Counter *)((char *)trans + offset2);
                }
            }
        }
        list_free_ext(stat_list);
    } else {
        t_statFlag = InvalidOid;
        t_id = relid;
        tabentry = find_tabstat_entry(t_id, t_statFlag);
        if (tabentry != NULL) {
            *result += *(PgStat_Counter *)((char *)&tabentry->t_counts + offset1);
            if (hassubtransactions) {
                for (trans = tabentry->trans; trans != NULL; trans = trans->upper)
                    *result += *(PgStat_Counter *)((char *)trans + offset2);
            }
        }
    }
    ReleaseSysCache(reltuple);
}

Datum pg_stat_get_xact_tuples_inserted(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    int64 result = 0;

    if (IS_PGXC_COORDINATOR && GetRelationLocInfo((relid)))
        PG_RETURN_INT64(pgxc_exec_tuples_stat(relid, "pg_stat_get_xact_tuples_inserted", EXEC_ON_DATANODES));

    pg_stat_update_xact_tuples(relid, &result, XACTION_INSERT);

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_xact_tuples_updated(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    int64 result = 0;

    if (IS_PGXC_COORDINATOR && GetRelationLocInfo((relid)))
        PG_RETURN_INT64(pgxc_exec_tuples_stat(relid, "pg_stat_get_xact_tuples_updated", EXEC_ON_DATANODES));

    pg_stat_update_xact_tuples(relid, &result, XACTION_UPDATE);

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_xact_tuples_deleted(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    int64 result = 0;

    if (IS_PGXC_COORDINATOR && GetRelationLocInfo((relid)))
        PG_RETURN_INT64(pgxc_exec_tuples_stat(relid, "pg_stat_get_xact_tuples_deleted", EXEC_ON_DATANODES));

    pg_stat_update_xact_tuples(relid, &result, XACTION_DELETE);

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_xact_tuples_hot_updated(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    int64 result = 0;

    if (IS_PGXC_COORDINATOR && GetRelationLocInfo((relid)))
        PG_RETURN_INT64(pgxc_exec_tuples_stat(relid, "pg_stat_get_xact_tuples_hot_updated", EXEC_ON_DATANODES));

    pg_stat_update_xact_tuples(relid, &result, XACTION_HOTUPDATE);

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_xact_blocks_fetched(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    int64 result = 0;
    List* stat_list = NIL;
    ListCell* stat_cell = NULL;
    uint32 statFlag = STATFLG_RELATION;
    PgStat_TableStatus* tabentry = NULL;
    pg_stat_get_stat_list(&stat_list, &statFlag, relid);

    foreach (stat_cell, stat_list) {
        Oid relationid = lfirst_oid(stat_cell);
        tabentry = find_tabstat_entry(relationid, statFlag);

        if (!PointerIsValid(tabentry)) {
            result += 0;
        } else {
            result += (int64)(tabentry->t_counts.t_blocks_fetched);
        }
    }

    if (PointerIsValid(stat_list)) {
        list_free(stat_list);
    }

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_xact_blocks_hit(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    int64 result = 0;
    List* stat_list = NIL;
    ListCell* stat_cell = NULL;
    uint32 statFlag = STATFLG_RELATION;
    PgStat_TableStatus* tabentry = NULL;
    pg_stat_get_stat_list(&stat_list, &statFlag, relid);

    foreach (stat_cell, stat_list) {
        Oid relationid = lfirst_oid(stat_cell);
        tabentry = find_tabstat_entry(relationid, statFlag);

        if (!PointerIsValid(tabentry)) {
            result += 0;
        } else {
            result += (int64)(tabentry->t_counts.t_blocks_hit);
        }
    }

    if (PointerIsValid(stat_list)) {
        list_free(stat_list);
    }

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_xact_function_calls(PG_FUNCTION_ARGS)
{
    Oid funcid = PG_GETARG_OID(0);
    PgStat_BackendFunctionEntry* funcentry = NULL;

    if ((funcentry = find_funcstat_entry(funcid)) == NULL)
        PG_RETURN_NULL();
    PG_RETURN_INT64(funcentry->f_counts.f_numcalls);
}

Datum pg_stat_get_xact_function_total_time(PG_FUNCTION_ARGS)
{
    Oid funcid = PG_GETARG_OID(0);
    PgStat_BackendFunctionEntry* funcentry = NULL;

    if ((funcentry = find_funcstat_entry(funcid)) == NULL)
        PG_RETURN_NULL();
    PG_RETURN_FLOAT8(INSTR_TIME_GET_MILLISEC(funcentry->f_counts.f_total_time));
}

Datum pg_stat_get_xact_function_self_time(PG_FUNCTION_ARGS)
{
    Oid funcid = PG_GETARG_OID(0);
    PgStat_BackendFunctionEntry* funcentry = NULL;

    if ((funcentry = find_funcstat_entry(funcid)) == NULL)
        PG_RETURN_NULL();
    PG_RETURN_FLOAT8(INSTR_TIME_GET_MILLISEC(funcentry->f_counts.f_self_time));
}

/* Discard the active statistics snapshot */
Datum pg_stat_clear_snapshot(PG_FUNCTION_ARGS)
{

    if (!superuser() && !isMonitoradmin(GetUserId())) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied to clear snapshot"),
                errhint("Must be system admin or monitor admin can clear snapshot.")));
    }

    pgstat_clear_snapshot();
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        ExecNodes* exec_nodes = NULL;
        StringInfoData buf;
        ParallelFunctionState* state = NULL;

        exec_nodes = (ExecNodes*)makeNode(ExecNodes);
        exec_nodes->baselocatortype = LOCATOR_TYPE_REPLICATED;
        exec_nodes->accesstype = RELATION_ACCESS_READ;
        exec_nodes->primarynodelist = NIL;
        exec_nodes->en_expr = NULL;
        exec_nodes->en_relid = InvalidOid;
        exec_nodes->nodeList = NIL;

        initStringInfo(&buf);
        appendStringInfo(&buf, "SELECT pg_catalog.pg_stat_clear_snapshot();");
        state = RemoteFunctionResultHandler(buf.data, exec_nodes, NULL, true, EXEC_ON_ALL_NODES, true);
        FreeParallelFunctionState(state);
        pfree_ext(buf.data);
    }

    PG_RETURN_VOID();
}

/* Reset all counters for the current database */
Datum pg_stat_reset(PG_FUNCTION_ARGS)
{
    pgstat_reset_counters();

    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        ExecNodes* exec_nodes = NULL;
        StringInfoData buf;
        ParallelFunctionState* state = NULL;

        exec_nodes = (ExecNodes*)makeNode(ExecNodes);
        exec_nodes->baselocatortype = LOCATOR_TYPE_HASH;
        exec_nodes->accesstype = RELATION_ACCESS_READ;
        exec_nodes->primarynodelist = NIL;
        exec_nodes->en_expr = NULL;
        exec_nodes->en_relid = InvalidOid;
        exec_nodes->nodeList = NIL;

        initStringInfo(&buf);
        appendStringInfo(&buf, "SELECT pg_catalog.pg_stat_reset();");
        state = RemoteFunctionResultHandler(buf.data, exec_nodes, NULL, true, EXEC_ON_ALL_NODES, true);
        FreeParallelFunctionState(state);
        pfree_ext(buf.data);
    }

    PG_RETURN_VOID();
}

/* Reset some shared cluster-wide counters */
Datum pg_stat_reset_shared(PG_FUNCTION_ARGS)
{
    char* target = text_to_cstring(PG_GETARG_TEXT_PP(0));

    pgstat_reset_shared_counters(target);

    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        ExecNodes* exec_nodes = NULL;
        StringInfoData buf;
        ParallelFunctionState* state = NULL;

        exec_nodes = (ExecNodes*)makeNode(ExecNodes);
        exec_nodes->baselocatortype = LOCATOR_TYPE_HASH;
        exec_nodes->accesstype = RELATION_ACCESS_READ;
        exec_nodes->primarynodelist = NIL;
        exec_nodes->en_expr = NULL;
        exec_nodes->en_relid = InvalidOid;
        exec_nodes->nodeList = NIL;

        initStringInfo(&buf);
        appendStringInfo(&buf, "SELECT pg_catalog.pg_stat_reset_shared('%s');", target);
        state = RemoteFunctionResultHandler(buf.data, exec_nodes, NULL, true, EXEC_ON_ALL_NODES, true);
        FreeParallelFunctionState(state);
        pfree_ext(buf.data);
    }

    PG_RETURN_VOID();
}

/* Reset a a single counter in the current database */
Datum pg_stat_reset_single_table_counters(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    List* stat_list = NIL;
    ListCell* stat_cell = NULL;
    List* sublist = NIL;
    ListCell* subcell = NULL;

    /* no syscache in collector thread, we just deal with all partitions here */
    HeapTuple reltuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(reltuple)) {
        pgstat_reset_single_counter(InvalidOid, relid, RESET_TABLE);
    } else {
        Form_pg_class relation = (Form_pg_class)GETSTRUCT(reltuple);

        if (PARTTYPE_SUBPARTITIONED_RELATION == relation->parttype) {
            stat_list = getPartitionObjectIdList(relid, PART_OBJ_TYPE_TABLE_PARTITION);
            foreach (stat_cell, stat_list) {
                Oid subparentid = lfirst_oid(stat_cell);
                sublist = getPartitionObjectIdList(subparentid, PART_OBJ_TYPE_TABLE_SUB_PARTITION);
                foreach (subcell, sublist) {
                    /* reset every partition it is a subpartitioned object */
                    pgstat_reset_single_counter(subparentid, lfirst_oid(subcell), RESET_TABLE);
                }
                list_free_ext(sublist);
            }
            list_free_ext(stat_list);
        } else if (PARTTYPE_PARTITIONED_RELATION == relation->parttype) {
            char objtype = PART_OBJ_TYPE_TABLE_PARTITION;
            if (relation->relkind == RELKIND_INDEX) {
                objtype = PART_OBJ_TYPE_INDEX_PARTITION;
            }
            stat_list = getPartitionObjectIdList(relid, objtype);
            foreach (stat_cell, stat_list) {
                /* reset every partition it is a partitioned object */
                pgstat_reset_single_counter(relid, lfirst_oid(stat_cell), RESET_TABLE);
            }
            list_free_ext(stat_list);
        } else {
            pgstat_reset_single_counter(InvalidOid, relid, RESET_TABLE);
        }
        ReleaseSysCache(reltuple);
    }

    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        Relation rel = NULL;
        ExecNodes* exec_nodes = NULL;
        StringInfoData buf;
        ParallelFunctionState* state = NULL;

        char* relname = NULL;
        char* nspname = NULL;

        rel = try_relation_open(relid, AccessShareLock);
        if (!rel)
            PG_RETURN_NULL();

        if (RELPERSISTENCE_TEMP == rel->rd_rel->relpersistence) {
            relation_close(rel, AccessShareLock);
            return 0;
        }
        relname = repairObjectName(RelationGetRelationName(rel));
        nspname = repairObjectName(get_namespace_name(rel->rd_rel->relnamespace));
        relation_close(rel, AccessShareLock);

        exec_nodes = (ExecNodes*)makeNode(ExecNodes);
        exec_nodes->baselocatortype = LOCATOR_TYPE_HASH;
        exec_nodes->accesstype = RELATION_ACCESS_READ;
        exec_nodes->primarynodelist = NIL;
        exec_nodes->en_expr = NULL;
        exec_nodes->en_relid = relid;
        exec_nodes->nodeList = NIL;

        initStringInfo(&buf);
        appendStringInfo(
            &buf, "SELECT pg_catalog.pg_stat_reset_single_table_counters('%s.%s'::regclass);", nspname, relname);
        state = RemoteFunctionResultHandler(buf.data, exec_nodes, NULL, true, EXEC_ON_ALL_NODES, true);
        FreeParallelFunctionState(state);
        pfree_ext(buf.data);
    }

    PG_RETURN_VOID();
}

Datum pg_stat_reset_single_function_counters(PG_FUNCTION_ARGS)
{
    Oid funcoid = PG_GETARG_OID(0);

    pgstat_reset_single_counter(InvalidOid, funcoid, RESET_FUNCTION);

    PG_RETURN_VOID();
}

Datum pv_os_run_info(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;

    /* stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        TupleDesc tupdesc;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function
         * calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* need a tuple descriptor representing 3 columns */
        tupdesc = CreateTemplateTupleDesc(5, false, TAM_HEAP);

        TupleDescInitEntry(tupdesc, (AttrNumber)1, "id", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "value", NUMERICOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)4, "comments", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)5, "cumulative", BOOLOID, -1, 0);

        /* complete descriptor of the tupledesc */
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        /*collect system infomation*/
        getCpuNums();
        getCpuTimes();
        getVmStat();
        getTotalMem();
        getOSRunLoad();

        (void)MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    while (funcctx->call_cntr < TOTAL_OS_RUN_INFO_TYPES) { /* do when there is more left to send */
        Datum values[5];
        bool nulls[5] = {false};
        HeapTuple tuple = NULL;

        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        if (!u_sess->stat_cxt.osStatDescArray[funcctx->call_cntr].got) {
            ereport(DEBUG3,
                (errmsg("the %s stat has not got on this plate.",
                    u_sess->stat_cxt.osStatDescArray[funcctx->call_cntr].name)));
            funcctx->call_cntr++;
            continue;
        }

        values[0] = Int32GetDatum(funcctx->call_cntr);
        values[1] = CStringGetTextDatum(u_sess->stat_cxt.osStatDescArray[funcctx->call_cntr].name);
        values[2] = u_sess->stat_cxt.osStatDescArray[funcctx->call_cntr].getDatum(
            u_sess->stat_cxt.osStatDataArray[funcctx->call_cntr]);
        values[3] = CStringGetTextDatum(u_sess->stat_cxt.osStatDescArray[funcctx->call_cntr].comments);
        values[4] = BoolGetDatum(u_sess->stat_cxt.osStatDescArray[funcctx->call_cntr].cumulative);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }
    /* do when there is no more left */
    SRF_RETURN_DONE(funcctx);
}

/*
 * @@GaussDB@@
 * Brief		: Collect each session Memory Context status,
 * 				  support pv_session_memory_detail and pv_session_memory_info view.
 * Description	:
 * Notes		:
 */
Datum pv_session_memory_detail(PG_FUNCTION_ARGS)
{
    ReturnSetInfo* rsinfo = (ReturnSetInfo*)fcinfo->resultinfo;
    TupleDesc tupdesc;
    knl_sess_control* sess = NULL;
    MemoryContext oldcontext;

    oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);

    /* need a tuple descriptor representing 9 columns */
    tupdesc = CreateTemplateTupleDesc(NUM_SESSION_MEMORY_DETAIL_ELEM, false, TAM_HEAP);

    TupleDescInitEntry(tupdesc, (AttrNumber)1, "sessid", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)2, "threadid", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)3, "contextname", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)4, "level", INT2OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)5, "parent", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)6, "totalsize", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)7, "freesize", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)8, "usedsize", INT8OID, -1, 0);

    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->setDesc = BlessTupleDesc(tupdesc);

    (void)MemoryContextSwitchTo(oldcontext);

    if (ENABLE_THREAD_POOL) {
        g_threadPoolControler->GetSessionCtrl()->getSessionMemoryDetail(rsinfo->setResult, rsinfo->setDesc, &sess);
    }

    /* clean up and return the tuplestore */
    tuplestore_donestoring(rsinfo->setResult);

    return (Datum)0;
}

/*
 * Description: Collect each session MOT engine's memory usage statistics.
 */
Datum mot_session_memory_detail(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_MOT
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("This function is not supported in cluster mode.")));
    PG_RETURN_NULL();
#else
    FuncCallContext* funcctx = NULL;
    MotSessionMemoryDetail* entry = NULL;
    MemoryContext oldcontext;

    /* stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tupdesc;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function
         * calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* need a tuple descriptor representing 4 columns */
        tupdesc = CreateTemplateTupleDesc(NUM_MOT_SESSION_MEMORY_DETAIL_ELEM, false);

        TupleDescInitEntry(tupdesc, (AttrNumber) 1, "sessid", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 2, "totalsize", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 3, "freesize", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 4, "usedsize", INT8OID, -1, 0);

        /* complete descriptor of the tupledesc */
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        /* total number of tuples to be returned */
        funcctx->user_fctx = (void *)GetMotSessionMemoryDetail(&(funcctx->max_calls));

        (void)MemoryContextSwitchTo(oldcontext);
    }

	/* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();
    entry = (MotSessionMemoryDetail *)funcctx->user_fctx;

    if (funcctx->call_cntr < funcctx->max_calls) {
        /* do when there is more left to send */
        Datum values[NUM_MOT_SESSION_MEMORY_DETAIL_ELEM];
        bool nulls[NUM_MOT_SESSION_MEMORY_DETAIL_ELEM] = {false};
        char sessId[SESSION_ID_LEN];
        HeapTuple tuple = NULL;

        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        rc = memset_s(sessId, sizeof(sessId), 0, sizeof(sessId));
        securec_check(rc, "\0", "\0");

        entry += funcctx->call_cntr;

        getSessionID(sessId, entry->threadStartTime,entry->threadid);
        values[0] = CStringGetTextDatum(sessId);
        values[1] = Int64GetDatum(entry->totalSize);
        values[2] = Int64GetDatum(entry->freeSize);
        values[3] = Int64GetDatum(entry->usedSize);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    } else {
        /* do when there is no more left */
        SRF_RETURN_DONE(funcctx);
    }
#endif
}

/*
 * Description: Produce a view to show all global memory usage on node
 */
Datum mot_global_memory_detail(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_MOT
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("This function is not supported in cluster mode.")));
    PG_RETURN_NULL();
#else
    FuncCallContext* funcctx = NULL;
    MotMemoryDetail* entry = NULL;
    MemoryContext oldcontext;

    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tupdesc;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
         * Switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples */
        tupdesc = CreateTemplateTupleDesc(3, false);

        TupleDescInitEntry(tupdesc, (AttrNumber) 1, "numa_node", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 2, "reserved_size", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 3, "used_size", INT8OID, -1, 0);

        /* complete descriptor of the tupledesc */
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        /* total number of tuples to be returned */
        funcctx->user_fctx = (void *)GetMotMemoryDetail(&(funcctx->max_calls), true);

        (void)MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();
    entry = (MotMemoryDetail *)funcctx->user_fctx;

    if (funcctx->call_cntr < funcctx->max_calls) {
        Datum values[3];
        bool nulls[3] = {false};
        HeapTuple tuple = NULL;

        /*
         * Form tuple with appropriate data.
         */
        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        entry += funcctx->call_cntr;

        /* Locking is probably not really necessary */
        values[0] = Int32GetDatum(entry->numaNode);
        values[1] = Int64GetDatum(entry->reservedMemory);
        values[2] = Int64GetDatum(entry->usedMemory);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);

        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    } else {
        /* do when there is no more left */
        SRF_RETURN_DONE(funcctx);
    }
#endif
}

/*
 * Description: Produce a view to show all local memory usage on node
 */
Datum mot_local_memory_detail(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_MOT
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("This function is not supported in cluster mode.")));
    PG_RETURN_NULL();
#else
    FuncCallContext* funcctx = NULL;
    MotMemoryDetail* entry = NULL;
    MemoryContext oldcontext;

    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tupdesc;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
         * Switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples */
        tupdesc = CreateTemplateTupleDesc(3, false);

        TupleDescInitEntry(tupdesc, (AttrNumber) 1, "numa_node", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 2, "reserved_size", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 3, "used_size", INT8OID, -1, 0);

        /* complete descriptor of the tupledesc */
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        /* total number of tuples to be returned */
        funcctx->user_fctx = (void *)GetMotMemoryDetail(&(funcctx->max_calls), false);

        (void)MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();
    entry = (MotMemoryDetail *)funcctx->user_fctx;

    if (funcctx->call_cntr < funcctx->max_calls) {
        Datum values[3];
        bool nulls[3] = {false};
        HeapTuple tuple = NULL;

        /*
         * Form tuple with appropriate data.
         */
        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        entry += funcctx->call_cntr;

        /* Locking is probably not really necessary */
        values[0] = Int32GetDatum(entry->numaNode);
        values[1] = Int64GetDatum(entry->reservedMemory);
        values[2] = Int64GetDatum(entry->usedMemory);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);

        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    } else {
        /* do when there is no more left */
        SRF_RETURN_DONE(funcctx);
    }
#endif
}

/*
 * @@GaussDB@@
 * Brief		: Collect each thread Memory Context status,
 * 				  support pv_thread_memory_detail and pv_thread_memory_info view.
 * Description	: The PROC strut had add the TopMemory point,
 * 				  will recursive the AllocSet by this TopMemory point.
 * Notes		:
 */
Datum pv_thread_memory_detail(PG_FUNCTION_ARGS)
{
    ReturnSetInfo* rsinfo = (ReturnSetInfo*)fcinfo->resultinfo;
    TupleDesc tupdesc = NULL;
    uint32 procIdx = 0;
    MemoryContext oldcontext;

    oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);

    /* need a tuple descriptor representing 9 columns */
    tupdesc = CreateTemplateTupleDesc(NUM_THREAD_MEMORY_DETAIL_ELEM, false, TAM_HEAP);

    TupleDescInitEntry(tupdesc, (AttrNumber)1, "threadid", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)2, "tid", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)3, "threadtype", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)4, "contextname", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)5, "level", INT2OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)6, "parent", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)7, "totalsize", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)8, "freesize", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)9, "usedsize", INT8OID, -1, 0);

    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->setDesc = BlessTupleDesc(tupdesc);

    (void)MemoryContextSwitchTo(oldcontext);

    getThreadMemoryDetail(rsinfo->setResult, rsinfo->setDesc, &procIdx);

    /* clean up and return the tuplestore */
    tuplestore_donestoring(rsinfo->setResult);

    return (Datum)0;
}

/*
 * @@GaussDB@@
 * Brief		: Collect each thread Memory Context status,
 * 				  support pv_session_memory_detail and pv_session_memory_info view.
 * Description	: The PROC strut had add the TopMemory point,
 * 				  will recursive the AllocSet by this TopMemory point.
 * Notes		:
 */
Datum pg_shared_memory_detail(PG_FUNCTION_ARGS)
{
    ReturnSetInfo* rsinfo = (ReturnSetInfo*)fcinfo->resultinfo;
    TupleDesc tupdesc = NULL;
    MemoryContext oldcontext;

    oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);

    /* need a tuple descriptor representing 9 columns */
    tupdesc = CreateTemplateTupleDesc(NUM_SHARED_MEMORY_DETAIL_ELEM, false, TAM_HEAP);

    TupleDescInitEntry(tupdesc, (AttrNumber)1, "contextname", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)2, "level", INT2OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)3, "parent", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)4, "totalsize", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)5, "freesize", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)6, "usedsize", INT8OID, -1, 0);

    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->setDesc = BlessTupleDesc(tupdesc);

    MemoryContextSwitchTo(oldcontext);

    getSharedMemoryDetail(rsinfo->setResult, rsinfo->setDesc);

    /* clean up and return the tuplestore */
    tuplestore_donestoring(rsinfo->setResult);

    return (Datum)0;
}

/*
 * Function returning data from the shared buffer cache - buffer number,
 * relation node/tablespace/database/blocknum and dirty indicator.
 */
Datum pg_buffercache_pages(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;
    Datum result;
    MemoryContext oldcontext;
    BufferCachePagesContext* fctx = NULL; /* User function context. */
    TupleDesc tupledesc;
    HeapTuple tuple;

    if (SRF_IS_FIRSTCALL()) {
        int i;
        BufferDesc* bufHdr = NULL;

        funcctx = SRF_FIRSTCALL_INIT();

        /* Switch context when allocating stuff to be used in later calls */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* Create a user function context for cross-call persistence */
        fctx = (BufferCachePagesContext*)palloc(sizeof(BufferCachePagesContext));

        /* Construct a tuple descriptor for the result rows. */
        tupledesc = CreateTemplateTupleDesc(NUM_BUFFERCACHE_PAGES_ELEM, false, TAM_HEAP);
        TupleDescInitEntry(tupledesc, (AttrNumber)1, "bufferid", INT4OID, -1, 0);
        TupleDescInitEntry(tupledesc, (AttrNumber)2, "relfilenode", OIDOID, -1, 0);
        TupleDescInitEntry(tupledesc, (AttrNumber)3, "bucketid", INT4OID, -1, 0);
        TupleDescInitEntry(tupledesc, (AttrNumber)4, "storage_type", INT8OID, -1, 0);
        TupleDescInitEntry(tupledesc, (AttrNumber)5, "reltablespace", OIDOID, -1, 0);
        TupleDescInitEntry(tupledesc, (AttrNumber)6, "reldatabase", OIDOID, -1, 0);
        TupleDescInitEntry(tupledesc, (AttrNumber)7, "relforknumber", INT4OID, -1, 0);
        TupleDescInitEntry(tupledesc, (AttrNumber)8, "relblocknumber", OIDOID, -1, 0);
        TupleDescInitEntry(tupledesc, (AttrNumber)9, "isdirty", BOOLOID, -1, 0);
        TupleDescInitEntry(tupledesc, (AttrNumber)10, "isvalid", BOOLOID, -1, 0);
        TupleDescInitEntry(tupledesc, (AttrNumber)11, "usage_count", INT2OID, -1, 0);
        TupleDescInitEntry(tupledesc, (AttrNumber)12, "pinning_backends", INT4OID, -1, 0);

        fctx->tupdesc = BlessTupleDesc(tupledesc);

        /* Allocate g_instance.attr.attr_storage.NBuffers worth of BufferCachePagesRec records. */
        fctx->record =
            (BufferCachePagesRec*)palloc_huge(CurrentMemoryContext,
            sizeof(BufferCachePagesRec) * g_instance.attr.attr_storage.NBuffers);

        /* Set max calls and remember the user function context. */
        funcctx->max_calls = g_instance.attr.attr_storage.NBuffers;
        funcctx->user_fctx = fctx;

        /* Return to original context when allocating transient memory */
        MemoryContextSwitchTo(oldcontext);

        /*
         * To get a consistent picture of the buffer state, we must lock all
         * partitions of the buffer map.  Needless to say, this is horrible
         * for concurrency.  Must grab locks in increasing order to avoid
         * possible deadlocks.
         */
        for (i = 0; i < NUM_BUFFER_PARTITIONS; i++)
            LWLockAcquire(GetMainLWLockByIndex(FirstBufMappingLock + i), LW_SHARED);

        /*
         * Scan though all the buffers, saving the relevant fields in the
         * fctx->record structure.
         */
        for (i = 0; i < g_instance.attr.attr_storage.NBuffers; i++) {

            uint32 buf_state;
            bufHdr = GetBufferDescriptor(i);

            /* Lock each buffer header before inspecting. */
            buf_state = LockBufHdr(bufHdr);

            fctx->record[i].bufferid = BufferDescriptorGetBuffer(bufHdr);
            fctx->record[i].relfilenode = bufHdr->tag.rnode.relNode;
            fctx->record[i].bucketnode = bufHdr->tag.rnode.bucketNode;
            fctx->record[i].storage_type = IsSegmentFileNode(bufHdr->tag.rnode) ? SEGMENT_PAGE : HEAP_DISK;
            fctx->record[i].reltablespace = bufHdr->tag.rnode.spcNode;
            fctx->record[i].reldatabase = bufHdr->tag.rnode.dbNode;
            fctx->record[i].forknum = bufHdr->tag.forkNum;
            fctx->record[i].blocknum = bufHdr->tag.blockNum;
            fctx->record[i].usagecount = BUF_STATE_GET_USAGECOUNT(buf_state);
            fctx->record[i].pinning_backends = BUF_STATE_GET_REFCOUNT(buf_state);

            if (buf_state & BM_DIRTY)
                fctx->record[i].isdirty = true;
            else
                fctx->record[i].isdirty = false;

            /* Note if the buffer is valid, and has storage created */
            if ((buf_state & BM_VALID) && (buf_state & BM_TAG_VALID))
                fctx->record[i].isvalid = true;
            else
                fctx->record[i].isvalid = false;

            UnlockBufHdr(bufHdr, buf_state);
        }

        /*
         * And release locks.  We do this in reverse order for two reasons:
         * (1) Anyone else who needs more than one of the locks will be trying
         * to lock them in increasing order; we don't want to release the
         * other process until it can get all the locks it needs. (2) This
         * avoids O(N^2) behavior inside LWLockRelease.
         */
        for (i = NUM_BUFFER_PARTITIONS; --i >= 0;)
            LWLockRelease(GetMainLWLockByIndex(FirstBufMappingLock + i));
    }

    funcctx = SRF_PERCALL_SETUP();

    /* Get the saved state */
    fctx = (BufferCachePagesContext*)funcctx->user_fctx;

    if (funcctx->call_cntr < funcctx->max_calls) {
        uint32 i = funcctx->call_cntr;
        Datum values[NUM_BUFFERCACHE_PAGES_ELEM];
        bool nulls[NUM_BUFFERCACHE_PAGES_ELEM] = {false};

        values[0] = Int32GetDatum(fctx->record[i].bufferid);
        nulls[0] = false;

        /*
         * Set all fields except the bufferid to null if the buffer is unused
         * or not valid.
         */
        if (fctx->record[i].blocknum == InvalidBlockNumber) {
            nulls[1] = true;
            nulls[2] = true;
            nulls[3] = true;
            nulls[4] = true;
            nulls[5] = true;
            nulls[6] = true;
            nulls[7] = true;
            nulls[8] = true;
            values[9] = BoolGetDatum(fctx->record[i].isvalid);
            nulls[9] = false;
            nulls[10] = true;
            nulls[11] = true;
        } else {
            values[1] = ObjectIdGetDatum(fctx->record[i].relfilenode);
            nulls[1] = false;
            values[2] = Int32GetDatum(fctx->record[i].bucketnode);
            nulls[2] = false;
            values[3] = Int32GetDatum(fctx->record[i].storage_type);
            nulls[3] = false;
            values[4] = ObjectIdGetDatum(fctx->record[i].reltablespace);
            nulls[4] = false;
            values[5] = ObjectIdGetDatum(fctx->record[i].reldatabase);
            nulls[5] = false;
            values[6] = Int32GetDatum(fctx->record[i].forknum);
            nulls[6] = false;
            values[7] = ObjectIdGetDatum((int64)fctx->record[i].blocknum);
            nulls[7] = false;
            values[8] = BoolGetDatum(fctx->record[i].isdirty);
            nulls[8] = false;
            values[9] = BoolGetDatum(fctx->record[i].isvalid);
            nulls[9] = false;
            values[10] = Int16GetDatum(fctx->record[i].usagecount);
            nulls[10] = false;
            values[11] = Int32GetDatum(fctx->record[i].pinning_backends);
            nulls[11] = false;
        }

        /* Build and return the tuple. */
        tuple = heap_form_tuple(fctx->tupdesc, values, nulls);
        result = HeapTupleGetDatum(tuple);

        SRF_RETURN_NEXT(funcctx, result);
    } else {
        SRF_RETURN_DONE(funcctx);
    }
}


Datum pv_session_time(PG_FUNCTION_ARGS)
{
    const int ATT_NUM = 4;

    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    MemoryContext oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);

    /* need a tuple descriptor representing 4 columns */
    TupleDesc tupdesc = CreateTemplateTupleDesc(ATT_NUM, false);

    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_1, "sessid", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_2, "stat_id", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_3, "stat_name", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_4, "value", INT8OID, -1, 0);

    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->setDesc = BlessTupleDesc(tupdesc);

    (void)MemoryContextSwitchTo(oldcontext);


    getSessionTimeStatus(rsinfo->setResult, rsinfo->setDesc, insert_pv_session_time);

    /* clean up and return the tuplestore */
    tuplestore_donestoring(rsinfo->setResult);

    return (Datum) 0;
}

void insert_pv_session_time(Tuplestorestate* tupStore, TupleDesc tupDesc, const SessionTimeEntry* entry)
{
    const int ATT_NUM = 4;
    Datum values[ATT_NUM];
    bool nulls[ATT_NUM];
    char sessId[SESSION_ID_LEN];
    uint32 statId;
    errno_t rc = 0;

    for (uint32 i = 0; i < TOTAL_TIME_INFO_TYPES; i++) { /* do when there is more left to send */
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        statId = i;

        getSessionID(sessId, entry->myStartTime, entry->sessionid);
        values[ARR_0] = CStringGetTextDatum(sessId);
        values[ARR_1] = Int32GetDatum(statId);
        values[ARR_2] = CStringGetTextDatum(TimeInfoTypeName[statId]);
        values[ARR_3] = Int64GetDatum(entry->array[statId]);

        tuplestore_putvalues(tupStore, tupDesc, values, nulls);
    }
}


Datum pv_instance_time(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;
    SessionTimeEntry* instanceEntry = NULL;

    /* stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        TupleDesc tupdesc;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function
         * calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* need a tuple descriptor representing 4 columns */
        tupdesc = CreateTemplateTupleDesc(3, false, TAM_HEAP);

        TupleDescInitEntry(tupdesc, (AttrNumber)1, "stat_id", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "stat_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "value", INT8OID, -1, 0);

        /* complete descriptor of the tupledesc */
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        funcctx->user_fctx = (void*)getInstanceTimeStatus();
        funcctx->max_calls = TOTAL_TIME_INFO_TYPES;

        (void)MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();
    instanceEntry = (SessionTimeEntry*)funcctx->user_fctx;

    if (funcctx->call_cntr < funcctx->max_calls) { /* do when there is more left to send */
        Datum values[3];
        bool nulls[3] = {false};
        HeapTuple tuple = NULL;

        values[0] = Int32GetDatum(funcctx->call_cntr);
        nulls[0] = false;
        values[1] = CStringGetTextDatum(TimeInfoTypeName[funcctx->call_cntr]);
        nulls[1] = false;
        values[2] = Int64GetDatum(instanceEntry->array[funcctx->call_cntr]);
        nulls[2] = false;

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    } else {
        pfree_ext(instanceEntry);
        /* do when there is no more left */
        SRF_RETURN_DONE(funcctx);
    }
}

Datum pg_stat_get_redo_stat(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;
    PgStat_RedoEntry* redoEntry = &redoStatistics;

    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tupdesc;
        MemoryContext oldcontext;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples */
        /* this had better match pv_plan view in system_views.sql */
        tupdesc = CreateTemplateTupleDesc(7, false, TAM_HEAP);
        TupleDescInitEntry(tupdesc, (AttrNumber)1, "phywrts", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "phyblkwrt", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "writetim", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)4, "avgiotim", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)5, "lstiotim", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)6, "miniotim", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)7, "maxiowtm", INT8OID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->max_calls = 1;
        funcctx->call_cntr = 0;

        (void)MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->call_cntr < funcctx->max_calls) {
        /* for each row */
        Datum values[7];
        bool nulls[7] = {false};
        HeapTuple tuple = NULL;

        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        LWLockAcquire(WALWriteLock, LW_SHARED);
        /* Values available to all callers */
        values[0] = Int64GetDatum(redoEntry->writes);
        values[1] = Int64GetDatum(redoEntry->writeBlks);
        values[2] = Int64GetDatum(redoEntry->writeTime);
        values[3] = Int64GetDatum(redoEntry->avgIOTime);
        values[4] = Int64GetDatum(redoEntry->lstIOTime);
        values[5] = Int64GetDatum(redoEntry->minIOTime);
        values[6] = Int64GetDatum(redoEntry->maxIOTime);
        LWLockRelease(WALWriteLock);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    } else {
        /* nothing left */
        SRF_RETURN_DONE(funcctx);
    }
}

const char* SessionStatisticsTypeName[N_TOTAL_SESSION_STATISTICS_TYPES] = {"n_commit",
    "n_rollback",
    "n_sql",
    "n_table_scan",
    "n_blocks_fetched",
    "n_physical_read_operation",
    "n_shared_blocks_dirtied",
    "n_local_blocks_dirtied",
    "n_shared_blocks_read",
    "n_local_blocks_read",
    "n_blocks_read_time",
    "n_blocks_write_time",
    "n_sort_in_memory",
    "n_sort_in_disk",
    "n_cu_mem_hit",
    "n_cu_hdd_sync_read",
    "n_cu_hdd_asyn_read"};

const char* SessionStatisticsTypeUnit[N_TOTAL_SESSION_STATISTICS_TYPES] = {
    "",             /*n_commit*/
    "",             /*n_rollback*/
    "",             /*n_sql*/
    "",             /*n_table_scan*/
    "",             /*n_blocks_fetched*/
    "",             /*n_physical_read_operation*/
    "",             /*n_shared_blocks_dirtied*/
    "",             /*n_local_blocks_dirtied*/
    "",             /*n_shared_blocks_read*/
    "",             /*n_local_blocks_read*/
    "microseconds", /*n_blocks_read_time*/
    "microseconds", /*n_blocks_write_time*/
    "",             /*n_sort_in_memory*/
    "",             /*n_sort_in_disk*/
    "",             /*n_cu_mem_hit*/
    "",             /*n_cu_hdd_sync_read*/
    ""              /*n_cu_hdd_asyn_read*/
};

Datum pv_session_stat(PG_FUNCTION_ARGS)
{
    const int ATT_NUM = 5;
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    MemoryContext oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
    /* need a tuple descriptor representing 5 columns */
    TupleDesc tupdesc = CreateTemplateTupleDesc(ATT_NUM, false);

    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_1, "sessid", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_2, "statid", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_3, "statname", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_4, "statunit", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_5, "value", INT8OID, -1, 0);

    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->setDesc = BlessTupleDesc(tupdesc);

    (void)MemoryContextSwitchTo(oldcontext);

    getSessionStatistics(rsinfo->setResult, rsinfo->setDesc, insert_pv_session_stat);

    /* clean up and return the tuplestore */
    tuplestore_donestoring(rsinfo->setResult);

    return (Datum) 0;
}

void insert_pv_session_stat(Tuplestorestate* tupStore, TupleDesc tupDesc, const SessionLevelStatistic* entry)
{
    const int ATT_NUM = 5;

    Datum values[ATT_NUM];
    bool nulls[ATT_NUM];
    char sessId[SESSION_ID_LEN];
    uint32  statId;
    errno_t rc = 0;

    for (uint32 i = 0; i < N_TOTAL_SESSION_STATISTICS_TYPES; i++) {
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        statId = i;

        getSessionID(sessId, entry->sessionStartTime, entry->sessionid);
        values[ARR_0] = CStringGetTextDatum(sessId);
        values[ARR_1] = Int32GetDatum(statId);
        values[ARR_2] = CStringGetTextDatum(SessionStatisticsTypeName[statId]);
        values[ARR_3] = CStringGetTextDatum(SessionStatisticsTypeUnit[statId]);
        values[ARR_4] = Int64GetDatum(entry->array[statId]);

        tuplestore_putvalues(tupStore, tupDesc, values, nulls);
    }
}

Datum pv_session_memory(PG_FUNCTION_ARGS)
{
    const int ATT_NUM = 4;
    if (!t_thrd.utils_cxt.gs_mp_inited) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("unsupported view for memory protection feature is disabled.")));
    }

    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    MemoryContext oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);

    /* need a tuple descriptor representing 4 columns */
    TupleDesc tupdesc = CreateTemplateTupleDesc(ATT_NUM, false);

    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_1, "sessid", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_2, "init_mem", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_3, "used_mem", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_4, "peak_mem", INT4OID, -1, 0);

    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->setDesc = BlessTupleDesc(tupdesc);

    (void)MemoryContextSwitchTo(oldcontext);

    getSessionMemory(rsinfo->setResult, rsinfo->setDesc, insert_pv_session_memory);

    /* clean up and return the tuplestore */
    tuplestore_donestoring(rsinfo->setResult);

    return (Datum) 0;
}

void insert_pv_session_memory(Tuplestorestate* tupStore, TupleDesc tupDesc, const SessionLevelMemory* entry)
{
    const int ATT_NUM = 4;
    Datum values[ATT_NUM];
    bool nulls[ATT_NUM];
    char sessId[SESSION_ID_LEN];

    errno_t rc = 0;
    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(rc, "\0", "\0");
    rc = memset_s(sessId, sizeof(sessId), 0, sizeof(sessId));
    securec_check(rc, "\0", "\0");

    getSessionID(sessId, entry->threadStartTime, entry->sessionid);
    values[ARR_0] = CStringGetTextDatum(sessId);
    values[ARR_1] = Int32GetDatum(entry->initMemInChunks << (chunkSizeInBits - BITS_IN_MB));
    values[ARR_2] = Int32GetDatum((entry->queryMemInChunks - entry->initMemInChunks) << (chunkSizeInBits - BITS_IN_MB));
    values[ARR_3] = Int32GetDatum((entry->peakChunksQuery - entry->initMemInChunks) << (chunkSizeInBits - BITS_IN_MB));

    tuplestore_putvalues(tupStore, tupDesc, values, nulls);
}


Datum pg_stat_get_file_stat(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;
    PgStat_FileEntry* fileEntry = NULL;

    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tupdesc;
        MemoryContext oldcontext;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples */
        /* this had better match pv_plan view in system_views.sql */
        tupdesc = CreateTemplateTupleDesc(13, false, TAM_HEAP);
        TupleDescInitEntry(tupdesc, (AttrNumber)1, "filenum", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "dbid", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "spcid", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)4, "phyrds", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)5, "phywrts", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)6, "phyblkrd", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)7, "phyblkwrt", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)8, "readtim", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)9, "writetim", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)10, "avgiotim", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)11, "lstiotim", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)12, "miniotim", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)13, "maxiowtm", INT8OID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->max_calls = NUM_FILES;
        fileEntry = (PgStat_FileEntry*)&pgStatFileArray[NUM_FILES - 1];
        if (0 == fileEntry->dbid) {
            funcctx->max_calls = fileStatCount;
        }
        funcctx->call_cntr = 0;

        (void)MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->call_cntr < funcctx->max_calls) {
        /* for each row */
        Datum values[13];
        bool nulls[13] = {false};
        HeapTuple tuple = NULL;

        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        LWLockAcquire(FileStatLock, LW_SHARED);

        fileEntry = (PgStat_FileEntry*)&pgStatFileArray[funcctx->call_cntr];
        /* Values available to all callers */
        values[0] = ObjectIdGetDatum(fileEntry->fn);

        if (0 == fileEntry->dbid)
            nulls[1] = true;
        else
            values[1] = ObjectIdGetDatum(fileEntry->dbid);

        values[2] = ObjectIdGetDatum(fileEntry->spcid);
        values[3] = Int64GetDatum(fileEntry->reads);
        values[4] = Int64GetDatum(fileEntry->writes);
        values[5] = Int64GetDatum(fileEntry->readBlks);
        values[6] = Int64GetDatum(fileEntry->writeBlks);
        values[7] = Int64GetDatum(fileEntry->readTime);
        values[8] = Int64GetDatum(fileEntry->writeTime);
        values[9] = Int64GetDatum(fileEntry->avgIOTime);
        values[10] = Int64GetDatum(fileEntry->lstIOTime);
        values[11] = Int64GetDatum(fileEntry->minIOTime);
        values[12] = Int64GetDatum(fileEntry->maxIOTime);

        LWLockRelease(FileStatLock);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    } else {
        /* nothing left */
        SRF_RETURN_DONE(funcctx);
    }
}

Datum get_local_rel_iostat(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;
    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tupdesc;
        MemoryContext oldcontext;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples */
        tupdesc = CreateTemplateTupleDesc(4, false, TAM_HEAP);
        TupleDescInitEntry(tupdesc, (AttrNumber)1, "phyrds", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "phywrts", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "phyblkrd", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)4, "phyblkwrt", INT8OID, -1, 0);
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->max_calls = 1;
        funcctx->call_cntr = 0;
        (void)MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->call_cntr < funcctx->max_calls) {
        const int colNum = 4;
        /* for each row */
        Datum values[colNum];
        bool nulls[colNum] = {false};
        HeapTuple tuple = NULL;

        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        for (;;) {
            unsigned int save_changecount = g_instance.stat_cxt.fileIOStat->changeCount;
            /* Values available to all callers */
            values[0] = Int64GetDatum(g_instance.stat_cxt.fileIOStat->reads);
            values[1] = Int64GetDatum(g_instance.stat_cxt.fileIOStat->writes);
            values[2] = Int64GetDatum(g_instance.stat_cxt.fileIOStat->readBlks);
            values[3] = Int64GetDatum(g_instance.stat_cxt.fileIOStat->writeBlks);
            if (save_changecount == g_instance.stat_cxt.fileIOStat->changeCount && (save_changecount & 1) == 0)
                break;
            /* Make sure we can break out of loop if stuck... */
            CHECK_FOR_INTERRUPTS();
        }

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    } else {
        /* nothing left */
        SRF_RETURN_DONE(funcctx);
    }
}
Datum total_cpu(PG_FUNCTION_ARGS)
{
    int fd = -1;
    char tempbuf[512] = {0};
    char* S = NULL;
    int64 cpu_usage = 0;
    unsigned long long cpu_utime, cpu_stime;
    int num_read;
    int rc = EOK;

    rc = snprintf_s(tempbuf, sizeof(tempbuf), 256, "/proc/%d/stat", getpid());
    securec_check_ss(rc, "\0", "\0");

    fd = open(tempbuf, O_RDONLY, 0);
    if (fd < 0) {
        PG_RETURN_INT64(cpu_usage);
    }

    num_read = read(fd, tempbuf, sizeof(tempbuf) - 1);
    close(fd);

    if (num_read < 0) {
        PG_RETURN_INT64(cpu_usage);
    }
    tempbuf[num_read] = '\0';
    S = tempbuf;

    S = strchr(S, '(') + 1;
    S = strrchr(S, ')') + 2;
    rc = sscanf_s(S,
        "%*c "
        "%*d %*d %*d %*d %*d "
        "%*d %*d %*d %*d %*d "
        "%llu %llu ",
        &cpu_utime,
        &cpu_stime);
    if (rc != 2) {
        if (rc >= 0) {
            ereport(LOG,
                (errmsg("ERROR at %s : %d : The destination buffer or format is normal but can not read data "
                        "completely..\n",
                    __FILE__,
                    __LINE__)));
        } else
            ereport(LOG,
                (errmsg("ERROR at %s : %d : The destination buffer or format is a NULL pointer or the invalid "
                        "parameter handle is invoked..\n",
                    __FILE__,
                    __LINE__)));
    }

    cpu_usage = cpu_utime + cpu_stime;

    PG_RETURN_INT64(cpu_usage);
}

Datum get_hostname(PG_FUNCTION_ARGS)
{
    char hostname[256] = {0};

    (void)gethostname(hostname, 255);

    PG_RETURN_TEXT_P(cstring_to_text(hostname));
}

Datum sessionid_to_pid(PG_FUNCTION_ARGS)
{
    int64 pid;
    char* sessionid = PG_GETARG_CSTRING(0);
    char* tmp = NULL;
    if (sessionid == NULL) {
        PG_RETURN_INT64(0);
    }

    tmp = strrchr(sessionid, '.');
    if (NULL == tmp) {
        PG_RETURN_INT64(0);
    }
    tmp++;

    pid = atoll(tmp);

    PG_RETURN_INT64(pid);
}

Datum total_memory(PG_FUNCTION_ARGS)
{
    int fd = -1;
    char tempbuf[1024] = {0};
    char* S = NULL;
    char* tmp = NULL;
    int64 total_vm = 0;
    int num_read;
    int rc = 0;

    rc = snprintf_s(tempbuf, sizeof(tempbuf), 256, "/proc/%d/status", getpid());
    securec_check_ss(rc, "\0", "\0");

    fd = open(tempbuf, O_RDONLY, 0);
    if (fd < 0) {
        PG_RETURN_INT64(total_vm);
    }

    num_read = read(fd, tempbuf, sizeof(tempbuf) - 1);
    close(fd);

    if (num_read < 0) {
        PG_RETURN_INT64(total_vm);
    }
    tempbuf[num_read] = '\0';
    S = tempbuf;

    for (;;) {
        if (0 == strncmp("VmSize", S, 6)) {
            tmp = strchr(S, ':');
            if (NULL == tmp) {
                break;
            }
            tmp = tmp + 2;
            total_vm = strtol(tmp, &tmp, 10);
            break;
        }
        S = strchr(S, '\n');
        if (NULL == S) {
            break;
        }
        S++;
    }

    PG_RETURN_INT64(total_vm);
}

Datum table_data_skewness(PG_FUNCTION_ARGS)
{
    Datum array = PG_GETARG_DATUM(0);
    char flag = PG_GETARG_CHAR(1);
    int2 attnum;

    if (IsLocatorDistributedBySlice(flag)) {
        attnum = u_sess->pgxc_cxt.PGXCNodeId;
        PG_RETURN_INT16(attnum);
    }

    if (u_sess->exec_cxt.global_bucket_map == NULL || 
        u_sess->exec_cxt.global_bucket_cnt == 0) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("Bucketmap is NULL")));
    }

    int bucketIndex;

    FunctionCallInfoData funcinfo;

    InitFunctionCallInfoData(funcinfo, NULL, 3, InvalidOid, NULL, NULL);

    funcinfo.arg[0] = array;
    funcinfo.arg[1] = CharGetDatum(flag);
    funcinfo.arg[2] = Int32GetDatum(u_sess->exec_cxt.global_bucket_cnt);
    funcinfo.argnull[0] = false;
    funcinfo.argnull[1] = false;
    funcinfo.argnull[2] = false;

    bucketIndex = getbucketbycnt(&funcinfo);

    if (funcinfo.isnull) {
        attnum = 0;
    } else {
        attnum = u_sess->exec_cxt.global_bucket_map[bucketIndex];
    }

    PG_RETURN_INT16(attnum);
}

ScalarVector* vtable_data_skewness(PG_FUNCTION_ARGS)
{
    VectorBatch* batch = (VectorBatch*)PG_GETARG_DATUM(0);
    ScalarVector* vecflag = PG_GETARG_VECTOR(1);
    int32 nvalues = PG_GETARG_INT32(2);

    ScalarVector* presultVector = PG_GETARG_VECTOR(3);
    uint8* pflagsRes = presultVector->m_flag;
    bool* pselection = PG_GETARG_SELECTION(4);

    if (u_sess->exec_cxt.global_bucket_map == NULL) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("Bucketmap is NULL")));
    }

    ScalarVector* bucketIndex = (ScalarVector*)DirectFunctionCall5((PGFunction)vgetbucket,
        PointerGetDatum(batch),
        PointerGetDatum(vecflag),
        (Datum)nvalues,
        PointerGetDatum(presultVector),
        PointerGetDatum(pselection));

    int rowNum = bucketIndex->m_rows;

    if (pselection == NULL) {
        for (int i = 0; i < rowNum; i++) {
            if (!bucketIndex->IsNull(i)) {
                int index = bucketIndex->m_vals[i];
                bucketIndex->m_vals[i] = u_sess->exec_cxt.global_bucket_map[index];
            } else {
                bucketIndex->m_vals[i] = 0;
                SET_NOTNULL(pflagsRes[i]);
            }
        }
    } else {
        for (int i = 0; i < rowNum; i++) {
            if (pselection[i]) {
                if (!bucketIndex->IsNull(i)) {
                    int index = bucketIndex->m_vals[i];
                    bucketIndex->m_vals[i] = u_sess->exec_cxt.global_bucket_map[index];
                } else {
                    bucketIndex->m_vals[i] = 0;
                    SET_NOTNULL(pflagsRes[i]);
                }
            }
        }
    }
    return bucketIndex;
}

/* walk through the memory context */
void gs_recursive_memctx_dump(MemoryContext context, StringInfoData* memBuf)
{
    MemoryContext child = NULL;
    AllocSet set = (AllocSet)context;

    appendStringInfo(
        memBuf, "%s, %lu, %lu\n", context->name, (unsigned long)set->totalSpace, (unsigned long)set->freeSpace);

    /*recursive MemoryContext's child*/
    for (child = context->firstchild; child != NULL; child = child->nextchild) {
        gs_recursive_memctx_dump(child, memBuf);
    }
}

/* dump the memory context tree when the thread id is specified */
void DumpMemoryContextTree(ThreadId tid, int* procIdx)
{
    char dump_dir[MAX_PATH_LEN] = {0};
    errno_t rc;
    bool is_absolute = false;

    // get the path of dump file
    is_absolute = is_absolute_path(u_sess->attr.attr_common.Log_directory);
    if (is_absolute) {
        rc = snprintf_s(dump_dir,
            sizeof(dump_dir),
            sizeof(dump_dir) - 1,
            "%s/memdump",
            u_sess->attr.attr_common.Log_directory);
        securec_check_ss(rc, "\0", "\0");
    } else {
        rc = snprintf_s(dump_dir,
            sizeof(dump_dir),
            sizeof(dump_dir) - 1,
            "%s/pg_log/memdump",
            g_instance.attr.attr_common.data_directory);
        securec_check_ss(rc, "\0", "\0");
    }

    // create directory "/tmp/dumpmem" to save memory context log file
    int ret;
    struct stat info;
    if (stat(dump_dir, &info) == 0) {
        if (!S_ISDIR(info.st_mode)) { // S_ISDIR() doesn't exist on my windows
            elog(LOG, "%s maybe not a directory.", dump_dir);
            return;
        }
    } else {
        ret = mkdir(dump_dir, S_IRWXU);
        if (ret < 0) {
            elog(LOG, "Fail to create %s", dump_dir);
            return;
        }
    }

    // create file to be dump
    char dump_file[MAX_PATH_LEN] = {0};
    rc = sprintf_s(dump_file, MAX_PATH_LEN, "%s/%lu_%lu.log",
                   dump_dir, (unsigned long)tid, (uint64)time(NULL));
    securec_check_ss(rc, "\0", "\0");
    FILE* dump_fp = fopen(dump_file, "w");
    if (NULL == dump_fp) {
        elog(LOG, "dump_memory: Failed to create file: %s:%m", dump_file);
        return;
    }

    // 4. walk all memory context
    volatile PGPROC* proc = NULL;
    int idx = 0;
    PG_TRY();
    {
        StringInfoData memBuf;

        initStringInfo(&memBuf);

        /* dump the top memory context */
        HOLD_INTERRUPTS();

        for (idx = 0; idx < (int)g_instance.proc_base->allProcCount; idx++) {
            proc = (g_instance.proc_base_all_procs[idx]);
            *procIdx = idx;

            if (proc->pid == tid) {
                /*lock this proc's delete MemoryContext action*/
                (void)syscalllockAcquire(&((PGPROC*)proc)->deleMemContextMutex);
                if (NULL != proc->topmcxt)
                    gs_recursive_memctx_dump(proc->topmcxt, &memBuf);
                (void)syscalllockRelease(&((PGPROC*)proc)->deleMemContextMutex);
                break;
            }
        }

        RESUME_INTERRUPTS();

        uint64 bytes = fwrite(memBuf.data, 1, memBuf.len, dump_fp);

        if (bytes != (uint64)memBuf.len) {
            elog(LOG, "Could not write memory usage information. Attempted to write %d", memBuf.len);
        }

        pfree(memBuf.data);
    }
    PG_CATCH();
    {
        if (*procIdx < (int)g_instance.proc_base->allProcCount) {
            proc = g_instance.proc_base_all_procs[*procIdx];
            (void)syscalllockRelease(&((PGPROC*)proc)->deleMemContextMutex);
        }
        fclose(dump_fp);
        PG_RE_THROW();
    }
    PG_END_TRY();

    fclose(dump_fp);
    return;
}

/*
 * select pv_session_memctx_detail(tid, 'u_sess->cache_mem_cxt');
 * tid is from the 1st item of pv_session_memory_detail();
 */
Datum pv_session_memctx_detail(PG_FUNCTION_ARGS)
{
    ThreadId tid = PG_GETARG_INT64(0);
    if (!superuser()) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("Only system admin user can use this function")));
    }

    if (0 == tid || tid == PostmasterPid) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("invalid thread id %ld for it is 0 or postmaster pid.", tid)));
    }

    char* ctx_name = PG_GETARG_CSTRING(1);
    if (NULL == ctx_name || strlen(ctx_name) == 0) {
        int procIdx = 0;
        DumpMemoryContextTree(tid, &procIdx);
        PG_RETURN_BOOL(true);
    }

#ifdef MEMORY_CONTEXT_CHECKING
    DumpMemoryCtxOnBackend(tid, ctx_name);
#endif

    PG_RETURN_BOOL(true);
}

/*
 * select pv_session_memctx_detail('t_thrd.mem_cxt.cache_mem_cxt');
 */
Datum pg_shared_memctx_detail(PG_FUNCTION_ARGS)
{
    char* ctx_name = PG_GETARG_CSTRING(0);
    if (!superuser()) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("Only system admin user can use this function")));
    }

    if (NULL == ctx_name || strlen(ctx_name) == 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("invalid name of memory context: NULL or \"\"")));
    }

    if (strlen(ctx_name) >= MEMORY_CONTEXT_NAME_LEN) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_NAME),
                errmsg("The name of memory context is too long(>=%dbytes)", MEMORY_CONTEXT_NAME_LEN)));
    }

#ifdef MEMORY_CONTEXT_CHECKING
    errno_t ss_rc = EOK;

    ss_rc = memset_s(dump_memory_context_name, MEMORY_CONTEXT_NAME_LEN, 0, MEMORY_CONTEXT_NAME_LEN);
    securec_check(ss_rc, "\0", "\0");
    ss_rc = strcpy_s(dump_memory_context_name, MEMORY_CONTEXT_NAME_LEN, ctx_name);
    securec_check(ss_rc, "\0", "\0");

    DumpMemoryContext(SHARED_DUMP);
#endif

    PG_RETURN_BOOL(true);
}

/*
 * gs_total_nodegroup_memory_detail
 *      Produce a view to show all memory usage of one node group
 *
 */
#define NG_MEMORY_TYPES_CNT 8
const char* NGMemoryTypeName[] = {"ng_total_memory",
    "ng_used_memory",
    "ng_estimate_memory",
    "ng_foreignrp_memsize",
    "ng_foreignrp_usedsize",
    "ng_foreignrp_peaksize",
    "ng_foreignrp_mempct",
    "ng_foreignrp_estmsize"};

Datum gs_total_nodegroup_memory_detail(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;
    PgStat_NgMemSize *ngmemsize_ptr = NULL;

    if (!t_thrd.utils_cxt.gs_mp_inited) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("unsupported view for memory protection feature is disabled.")));
    }

    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tupdesc;
        MemoryContext oldcontext;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
         * Switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples */
        tupdesc = CreateTemplateTupleDesc(3, false, TAM_HEAP);

        TupleDescInitEntry(tupdesc, (AttrNumber)1, "ngname", TEXTOID, -1, 0);

        TupleDescInitEntry(tupdesc, (AttrNumber)2, "memorytype", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "memorymbytes", INT4OID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        USE_AUTO_LWLOCK(WorkloadNodeGroupLock, LW_SHARED);
        int num = 0;
        int cnt = 0;
        int i = 0;

        if ((num = hash_get_num_entries(g_instance.wlm_cxt->stat_manager.node_group_hashtbl)) <= 0) {
            cnt = 1;
        } else {
            cnt = num + 1;
        }

        num = 0;
        Size elementSize = (Size)(NG_MEMORY_TYPES_CNT * sizeof(int));
        if ((unsigned int)cnt > (MaxAllocSize / elementSize)) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid size of cnt")));
        }
        funcctx->user_fctx = (PgStat_NgMemSize*)palloc0(sizeof(PgStat_NgMemSize));
        ngmemsize_ptr = (PgStat_NgMemSize*)funcctx->user_fctx;
        ngmemsize_ptr->ngmemsize = (int*)palloc0(cnt * NG_MEMORY_TYPES_CNT * sizeof(int));
        ngmemsize_ptr->ngname = (char**)palloc0(cnt * sizeof(char*));

        WLMNodeGroupInfo* hdata = &g_instance.wlm_cxt->MyDefaultNodeGroup;

        ngmemsize_ptr->ngmemsize[num++] = hdata->total_memory;
        ngmemsize_ptr->ngmemsize[num++] = hdata->used_memory;
        ngmemsize_ptr->ngmemsize[num++] = hdata->estimate_memory;
        num += (NG_MEMORY_TYPES_CNT - 3); /* no foreign rp in default node group */
        ngmemsize_ptr->ngname[i++] = pstrdup(hdata->group_name);

        /* search all node group to get its resource usage */
        HASH_SEQ_STATUS hash_seq;
        hash_seq_init(&hash_seq, g_instance.wlm_cxt->stat_manager.node_group_hashtbl);

        while ((hdata = (WLMNodeGroupInfo*)hash_seq_search(&hash_seq)) != NULL) {
            if (!hdata->used) {
                continue;
            }

            ngmemsize_ptr->ngmemsize[num++] = hdata->total_memory;
            ngmemsize_ptr->ngmemsize[num++] = hdata->used_memory;
            ngmemsize_ptr->ngmemsize[num++] = hdata->estimate_memory;
            if (hdata->foreignrp) {
                ngmemsize_ptr->ngmemsize[num++] = hdata->foreignrp->memsize;
                ngmemsize_ptr->ngmemsize[num++] = hdata->foreignrp->memused;
                ngmemsize_ptr->ngmemsize[num++] = hdata->foreignrp->peakmem;
                ngmemsize_ptr->ngmemsize[num++] = hdata->foreignrp->actpct;
                ngmemsize_ptr->ngmemsize[num++] = hdata->foreignrp->estmsize;
            } else {
                num += (NG_MEMORY_TYPES_CNT - 3);  // default as 0
            }

            ngmemsize_ptr->ngname[i++] = pstrdup(hdata->group_name);
        }

        ngmemsize_ptr->allcnt = num;

        (void)MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();
    
    ngmemsize_ptr = (PgStat_NgMemSize*)funcctx->user_fctx;

    while (ngmemsize_ptr != NULL && funcctx->call_cntr < ngmemsize_ptr->allcnt) {
        Datum values[3];
        bool nulls[3] = {false};
        HeapTuple tuple = NULL;

        /*
         * Form tuple with appropriate data.
         */
        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        /* Locking is probably not really necessary */
        values[0] = CStringGetTextDatum(ngmemsize_ptr->ngname[ngmemsize_ptr->cntj]);
        values[1] = CStringGetTextDatum(NGMemoryTypeName[funcctx->call_cntr % NG_MEMORY_TYPES_CNT]);
        values[2] = (Datum)(ngmemsize_ptr->ngmemsize[ngmemsize_ptr->cnti]);

        (ngmemsize_ptr->cnti)++;

        if (ngmemsize_ptr->cnti % NG_MEMORY_TYPES_CNT == 0) {
            pfree_ext(ngmemsize_ptr->ngname[ngmemsize_ptr->cntj]);
            (ngmemsize_ptr->cntj)++;
        }

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }

    pfree_ext(ngmemsize_ptr->ngmemsize);
    pfree_ext(ngmemsize_ptr->ngname);
    pfree_ext(ngmemsize_ptr);

    /* do when there is no more left */
    SRF_RETURN_DONE(funcctx);
}

#define MEMORY_TYPES_CNT 24
const char* MemoryTypeName[] = {"max_process_memory",
    "process_used_memory",
    "max_dynamic_memory",
    "dynamic_used_memory",
    "dynamic_peak_memory",
    "dynamic_used_shrctx",
    "dynamic_peak_shrctx",
    "max_backend_memory",
    "backend_used_memory",
    "max_shared_memory",
    "shared_used_memory",
    "max_cstore_memory",
    "cstore_used_memory",
    "max_sctpcomm_memory",
    "sctpcomm_used_memory",
    "sctpcomm_peak_memory",
    "other_used_memory",
    "gpu_max_dynamic_memory",
    "gpu_dynamic_used_memory",
    "gpu_dynamic_peak_memory",
    "pooler_conn_memory",
    "pooler_freeconn_memory",
    "storage_compress_memory",
    "udf_reserved_memory"};

/*
 * pv_total_memory_detail
 *		Produce a view to show all memory usage on node
 *
 */
Datum pv_total_memory_detail(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;
    int *mem_size = NULL;

    if (!t_thrd.utils_cxt.gs_mp_inited) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("unsupported view for memory protection feature is disabled.")));
    }

    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tupdesc;
        MemoryContext oldcontext;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
         * Switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples */
        tupdesc = CreateTemplateTupleDesc(3, false, TAM_HEAP);

        TupleDescInitEntry(tupdesc, (AttrNumber)1, "nodename", TEXTOID, -1, 0);

        TupleDescInitEntry(tupdesc, (AttrNumber)2, "memorytype", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "memorymbytes", INT4OID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        mem_size = (int*)palloc0(MEMORY_TYPES_CNT * sizeof(int));
        funcctx->user_fctx = (int*)mem_size;

        (void)MemoryContextSwitchTo(oldcontext);

        int mctx_max_size = maxChunksPerProcess << (chunkSizeInBits - BITS_IN_MB);
        int mctx_used_size = processMemInChunks << (chunkSizeInBits - BITS_IN_MB);
        int cu_size = (CUCache->GetCurrentMemSize() + MetaCacheGetCurrentUsedSize()) >> BITS_IN_MB;
        int comm_size = (int)(gs_get_comm_used_memory() >> BITS_IN_MB);
        int comm_peak_size = (int)(gs_get_comm_peak_memory() >> BITS_IN_MB);
        unsigned long total_vm = 0, res = 0, shared = 0, text = 0, lib, data, dt;
        const char* statm_path = "/proc/self/statm";
        FILE* f = fopen(statm_path, "r");
        int pageSize = getpagesize();  // get the size(bytes) for a page
        if (pageSize <= 0) {
            ereport(WARNING,
                (errcode(ERRCODE_WARNING),
                    errmsg("error for call 'getpagesize()', the values for "
                           "process_used_memory and other_used_memory are error!")));
            pageSize = 1;
        }

        if (f != NULL) {
            if (7 == fscanf_s(f, "%lu %lu %lu %lu %lu %lu %lu\n", &total_vm, &res, &shared, &text, &lib, &data, &dt)) {
                /* page translated to MB */
                total_vm = BYTES_TO_MB((unsigned long)(total_vm * pageSize));
                res = BYTES_TO_MB((unsigned long)(res * pageSize));
                shared = BYTES_TO_MB((unsigned long)(shared * pageSize));
                text = BYTES_TO_MB((unsigned long)(text * pageSize));
            }
            fclose(f);
        }

        mem_size[0] = (int)(g_instance.attr.attr_memory.max_process_memory >> BITS_IN_KB);
        mem_size[1] = (int)res;
        mem_size[2] = mctx_max_size;
        mem_size[3] = mctx_used_size;
        if (MEMPROT_INIT_SIZE > mem_size[3])
            mem_size[3] = MEMPROT_INIT_SIZE;
        mem_size[4] = (int)(peakChunksPerProcess << (chunkSizeInBits - BITS_IN_MB));
        mem_size[5] = (int)(shareTrackedMemChunks << (chunkSizeInBits - BITS_IN_MB));
        mem_size[6] = (int)(peakChunksSharedContext << (chunkSizeInBits - BITS_IN_MB));
        mem_size[7] = (int)(backendReservedMemInChunk << (chunkSizeInBits - BITS_IN_MB));
        mem_size[8] = (int)(backendUsedMemInChunk << (chunkSizeInBits - BITS_IN_MB));
        if (mem_size[8] < 0) {
            mem_size[8] = 0;
        }
        mem_size[9] = maxSharedMemory;
        mem_size[10] = shared;
        mem_size[11] = (int)(g_instance.attr.attr_storage.cstore_buffers >> BITS_IN_KB);
        mem_size[12] = cu_size;
        if (comm_size)
            mem_size[13] = comm_original_memory;
        else
            mem_size[13] = 0;
        mem_size[14] = comm_size;
        mem_size[15] = comm_peak_size;

#ifdef ENABLE_MULTIPLE_NODES
        if (is_searchserver_api_load()) {
            void* mem_info = get_searchlet_resource_info(&mem_size[18], &mem_size[19]);
            if (mem_info != NULL) {
                mem_size[17] = g_searchserver_memory;
                pfree_ext(mem_info);
            }
        }
#else
        mem_size[17] = 0;
#endif

        mem_size[16] = (int)(res - shared - text) - mctx_used_size - cu_size - mem_size[16];
        if (0 > mem_size[16])  // res may be changed
            mem_size[16] = 0;

#ifdef ENABLE_MULTIPLE_NODES
        PoolConnStat conn_stat;
        pooler_get_connection_statinfo(&conn_stat);
        mem_size[20] = (int)(conn_stat.memory_size >> BITS_IN_MB);
        mem_size[21] = (int)(conn_stat.free_memory_size >> BITS_IN_MB);
#else
        mem_size[20] = 0;
        mem_size[21] = 0;
#endif
        mem_size[22] = (int)(storageTrackedBytes >> BITS_IN_MB);

        Assert(g_instance.attr.attr_sql.udf_memory_limit >= UDF_DEFAULT_MEMORY);
        mem_size[23] = (int)((g_instance.attr.attr_sql.udf_memory_limit - UDF_DEFAULT_MEMORY) >> BITS_IN_KB);
        ereport(DEBUG2, (errmodule(MOD_LLVM), errmsg("LLVM IR file count is %ld, total memory is %ldKB",
                g_instance.codegen_IRload_process_count,
                g_instance.codegen_IRload_process_count * IR_FILE_SIZE / 1024)));
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    mem_size = (int*)(funcctx->user_fctx);

    while (funcctx->call_cntr < MEMORY_TYPES_CNT) {
        Datum values[3];
        bool nulls[3] = {false};
        HeapTuple tuple = NULL;

        /*
         * Form tuple with appropriate data.
         */
        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        /* Locking is probably not really necessary */
        values[0] = CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);
        values[1] = CStringGetTextDatum(MemoryTypeName[funcctx->call_cntr]);
        values[2] = (Datum)(mem_size[funcctx->call_cntr]);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }

    pfree_ext(mem_size);
    /* do when there is no more left */
    SRF_RETURN_DONE(funcctx);
}

Datum pg_stat_get_pooler_status(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_MULTIPLE_NODES
    FuncCallContext* funcctx = NULL;
    ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("unsupported view in single node mode.")));
    /* do when there is no more left */
    SRF_RETURN_DONE(funcctx);
#else
    FuncCallContext* funcctx = NULL;
    PoolConnectionInfo* conns_entry = NULL;
    const int ARG_NUMS = 12;
    
    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext = NULL;
        TupleDesc tupdesc = NULL;

        funcctx = SRF_FIRSTCALL_INIT();

        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(ARG_NUMS, false, TAM_HEAP);
        TupleDescInitEntry(tupdesc, (AttrNumber) ARG_1, "database_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) ARG_2, "user_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) ARG_3, "threadid", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) ARG_4, "pgoptions", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) ARG_5, "node_oid", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) ARG_6, "in_use", BOOLOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) ARG_7, "session_params", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) ARG_8, "fdsock", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) ARG_9, "remote_pid", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) ARG_10, "used_count", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) ARG_11, "idx", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) ARG_12, "streamid", INT8OID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        funcctx->max_calls = get_pool_connection_info(&conns_entry);
        funcctx->user_fctx = conns_entry;

        MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();
    conns_entry = (PoolConnectionInfo*)(funcctx->user_fctx);

    if (funcctx->call_cntr < funcctx->max_calls) {
        /* for each row */
        Datum values[ARG_NUMS];
        bool nulls[ARG_NUMS] = {false};
        HeapTuple tuple = NULL;

        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        PoolConnectionInfo* poolCon = conns_entry + funcctx->call_cntr;

        // poolCon never be NULL
        values[ARR_0] = CStringGetTextDatum(poolCon->database);

        if (poolCon->user_name != NULL) {
            values[ARR_1] = CStringGetTextDatum(poolCon->user_name);
        } else {
            values[ARR_1] = CStringGetTextDatum("none");
        }

        if (poolCon->tid > 0) {
            values[ARR_2] = Int64GetDatum(poolCon->tid);
        } else {
            nulls[ARR_2] = true;
        }

        if (poolCon->pgoptions != NULL) {
            values[ARR_3] = CStringGetTextDatum(poolCon->pgoptions);
        } else {
            values[ARR_3] = CStringGetTextDatum("none");
        }

        values[ARR_4] = Int64GetDatum(poolCon->nodeOid);
        values[ARR_5] = BoolGetDatum(poolCon->in_use);
        if (poolCon->session_params != NULL) {
            values[ARR_6] = CStringGetTextDatum(poolCon->session_params);
        } else {
            values[ARR_6] = CStringGetTextDatum("none");
        }
        values[ARR_7] = Int64GetDatum(poolCon->fdsock);
        values[ARR_8] = Int64GetDatum(poolCon->remote_pid);
        values[ARR_9] = Int64GetDatum(poolCon->used_count);
        values[ARR_10] = Int64GetDatum(poolCon->idx);
        values[ARR_11] = Int64GetDatum(poolCon->streamid);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);

        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    } else {
        PoolConnectionInfo* pcon = NULL;
        for (uint32 i = 0; i < funcctx->max_calls; i++) {
            pcon = conns_entry + i;
            pfree_ext(pcon->database);

            if (pcon->user_name != NULL) {
                pfree_ext(pcon->user_name);
            }

            if (pcon->pgoptions != NULL) {
                pfree_ext(pcon->pgoptions);
            }

            if (pcon->session_params != NULL) {
                pfree_ext(pcon->session_params);
            }
        }
        pfree_ext(conns_entry);

        /* nothing left */
        SRF_RETURN_DONE(funcctx);
    }
#endif
}

void init_pgxc_comm_get_recv_stream(PG_FUNCTION_ARGS, FuncCallContext *funcctx, const int argNums)
{
    TupleDesc    tupdesc;

    /* build tupdesc for result tuples */
    tupdesc = CreateTemplateTupleDesc(argNums, false, TAM_HEAP);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_1, "node_name", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_2, "local_tid", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_3, "remote_name", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_4, "remote_tid", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_5, "idx", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_6, "sid", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_7, "tcp_socket", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_8, "state", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_9, "query_id", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_10, "send_smp", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_11, "recv_smp", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_12, "pn_id", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_13, "recv_bytes", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_14, "time", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_15, "speed", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_16, "quota", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_17, "buff_usize", INT8OID, -1, 0);
    funcctx->tuple_desc = BlessTupleDesc(tupdesc);
    funcctx->max_calls = u_sess->pgxc_cxt.NumDataNodes + u_sess->pgxc_cxt.NumCoords;
    return;
}

/*
 * pgxc_comm_stream_status
 *        Produce a view with one row per prepared transaction.
 *
 */
Datum global_comm_get_recv_stream(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_MULTIPLE_NODES
    FuncCallContext* funcctx = NULL;
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        errmsg("unsupported view in single node mode.")));

    SRF_RETURN_DONE(funcctx);
#else
    int i;
    const int RECV_STREAM_TUPLE_NATTS = 17;
    FuncCallContext *funcctx = NULL;
    Datum values[RECV_STREAM_TUPLE_NATTS];
    bool nulls[RECV_STREAM_TUPLE_NATTS];
    HeapTuple tuple;
    MemoryContext oldcontext;

    if (SRF_IS_FIRSTCALL()) {
        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();
        /* Switch to memory context appropriate for multiple function calls */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        init_pgxc_comm_get_recv_stream(fcinfo, funcctx, RECV_STREAM_TUPLE_NATTS);
        /* the main call for get table distribution. */
        funcctx->user_fctx = getGlobalCommStatus(funcctx->tuple_desc,  "SELECT * FROM pg_comm_recv_stream;");
        MemoryContextSwitchTo(oldcontext);
        if (funcctx->user_fctx == NULL) {
            SRF_RETURN_DONE(funcctx);
        }
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->user_fctx) {
        Tuplestorestate *tupstore = ((CommInfoParallel *)funcctx->user_fctx)->state->tupstore;
        TupleTableSlot    *slot = ((CommInfoParallel *)funcctx->user_fctx)->slot;
        if (!tuplestore_gettupleslot(tupstore, true, false, slot)) {
            FreeParallelFunctionState(((CommInfoParallel *)funcctx->user_fctx)->state);
            ExecDropSingleTupleTableSlot(slot);
            pfree_ext(funcctx->user_fctx);
            funcctx->user_fctx = NULL;
            SRF_RETURN_DONE(funcctx);
        }
        for (i = 0; i < RECV_STREAM_TUPLE_NATTS; i++) {
            values[i] = tableam_tslot_getattr(slot, i + 1, &nulls[i]);
        }

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        ExecClearTuple(slot);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }

    SRF_RETURN_DONE(funcctx);
#endif
}

/*
 * pg_comm_stream_status
 *		Produce a view with one row per prepared transaction.
 *
 */
Datum pg_comm_recv_stream(PG_FUNCTION_ARGS)
{
    const int RECV_STREAM_TUPLE_NATTS = 17;
    FuncCallContext* funcctx = NULL;
    CommRecvStreamStatus* stream_status = NULL;
    int ii = 0;

    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tupdesc = NULL;
        MemoryContext oldcontext = NULL;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
         * Switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples */
        ii = 1;
        tupdesc = CreateTemplateTupleDesc(RECV_STREAM_TUPLE_NATTS, false, TAM_HEAP);
        TupleDescInitEntry(tupdesc, (AttrNumber)ii++, "node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ii++, "local_tid", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ii++, "remote_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ii++, "remote_tid", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ii++, "idx", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ii++, "sid", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ii++, "tcp_socket", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ii++, "state", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ii++, "query_id", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ii++, "send_smp", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ii++, "recv_smp", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ii++, "pn_id", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ii++, "recv_bytes", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ii++, "time", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ii++, "speed", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ii++, "quota", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ii++, "buff_usize", INT8OID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        stream_status = (CommRecvStreamStatus*)palloc(sizeof(CommRecvStreamStatus));
        errno_t rc = memset_s(stream_status, sizeof(CommRecvStreamStatus), 0, sizeof(CommRecvStreamStatus));
        securec_check(rc, "\0", "\0");
        funcctx->user_fctx = (void*)stream_status;
        stream_status->stream_id = -1;

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    stream_status = (CommRecvStreamStatus*)funcctx->user_fctx;

    if (get_next_recv_stream_status(stream_status) == true) {
        Datum values[RECV_STREAM_TUPLE_NATTS];
        bool nulls[RECV_STREAM_TUPLE_NATTS] = {false};
        HeapTuple tuple = NULL;
        Datum result;

        /*
         * Form tuple with appropriate data.
         */
        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        ii = 0;

        /* Locking is probably not really necessary */
        values[ii++] = CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);
        values[ii++] = Int64GetDatum(stream_status->local_thread_id);
        values[ii++] = (Datum)cstring_to_text(stream_status->remote_node);
        values[ii++] = Int64GetDatum(stream_status->peer_thread_id);
        values[ii++] = (Datum)(stream_status->idx);
        values[ii++] = (Datum)(stream_status->stream_id);
        values[ii++] = (Datum)(stream_status->tcp_sock);
        values[ii++] = (Datum)cstring_to_text(stream_status->stream_state);
        values[ii++] = Int64GetDatum(stream_status->stream_key.queryId);
        values[ii++] = (Datum)(stream_status->stream_key.planNodeId);
        values[ii++] = (Datum)(stream_status->stream_key.producerSmpId);
        values[ii++] = (Datum)(stream_status->stream_key.consumerSmpId);
        values[ii++] = Int64GetDatum(stream_status->bytes);
        values[ii++] = Int64GetDatum(stream_status->time);
        values[ii++] = Int64GetDatum(stream_status->speed);
        values[ii++] = Int64GetDatum(stream_status->quota_size);
        values[ii++] = Int64GetDatum(stream_status->buff_usize);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, result);

    } else {
        SRF_RETURN_DONE(funcctx);
    }
}

Datum pg_comm_send_stream(PG_FUNCTION_ARGS)
{
    const int SEND_STREAM_TUPLE_NATTS = 17;
    FuncCallContext* funcctx = NULL;
    CommSendStreamStatus* stream_status = NULL;
    int ii = 0;
    errno_t rc = 0;

    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tupdesc;
        MemoryContext oldcontext;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
         * Switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples */
        ii = 1;
        tupdesc = CreateTemplateTupleDesc(SEND_STREAM_TUPLE_NATTS, false, TAM_HEAP);
        TupleDescInitEntry(tupdesc, (AttrNumber)ii++, "node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ii++, "local_tid", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ii++, "remote_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ii++, "remote_tid", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ii++, "idx", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ii++, "sid", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ii++, "tcp_socket", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ii++, "state", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ii++, "query_id", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ii++, "pn_id", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ii++, "send_smp", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ii++, "recv_smp", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ii++, "send_bytes", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ii++, "time", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ii++, "speed", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ii++, "quota", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ii++, "wait_quota", INT8OID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        stream_status = (CommSendStreamStatus*)palloc(sizeof(CommSendStreamStatus));
        rc = memset_s(stream_status, sizeof(CommSendStreamStatus), 0, sizeof(CommSendStreamStatus));
        securec_check(rc, "\0", "\0");
        funcctx->user_fctx = (void*)stream_status;
        stream_status->stream_id = -1;

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    stream_status = (CommSendStreamStatus*)funcctx->user_fctx;

    if (get_next_send_stream_status(stream_status) == true) {
        Datum values[SEND_STREAM_TUPLE_NATTS];
        bool nulls[SEND_STREAM_TUPLE_NATTS] = {false};
        HeapTuple tuple = NULL;
        Datum result;

        /*
         * Form tuple with appropriate data.
         */
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        ii = 0;

        /* Locking is probably not really necessary */
        values[ii++] = CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);
        values[ii++] = Int64GetDatum(stream_status->local_thread_id);
        values[ii++] = (Datum)cstring_to_text(stream_status->remote_node);
        values[ii++] = Int64GetDatum(stream_status->peer_thread_id);
        values[ii++] = (Datum)(stream_status->idx);
        values[ii++] = (Datum)(stream_status->stream_id);
        values[ii++] = (Datum)(stream_status->tcp_sock);
        values[ii++] = (Datum)cstring_to_text(stream_status->stream_state);
        values[ii++] = Int64GetDatum(stream_status->stream_key.queryId);
        values[ii++] = (Datum)(stream_status->stream_key.planNodeId);
        values[ii++] = (Datum)(stream_status->stream_key.producerSmpId);
        values[ii++] = (Datum)(stream_status->stream_key.consumerSmpId);
        values[ii++] = Int64GetDatum(stream_status->bytes);
        values[ii++] = Int64GetDatum(stream_status->time);
        values[ii++] = Int64GetDatum(stream_status->speed);
        values[ii++] = Int64GetDatum(stream_status->quota_size);
        values[ii++] = Int64GetDatum(stream_status->wait_quota);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, result);

    } else {
        SRF_RETURN_DONE(funcctx);
    }
}

void init_pgxc_comm_get_send_stream(PG_FUNCTION_ARGS, FuncCallContext *funcctx, const int argNums)
{
    TupleDesc    tupdesc;
    
    /* build tupdesc for result tuples */
    tupdesc = CreateTemplateTupleDesc(argNums, false, TAM_HEAP);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_1, "node_name", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_2, "local_tid", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_3, "remote_name", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_4, "remote_tid", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_5, "idx", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_6, "sid", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_7, "tcp_socket", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_8, "state",TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_9, "query_id", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_10, "pn_id", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_11, "send_smp", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_12, "recv_smp", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_13, "send_bytes", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_14, "time", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_15, "speed", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_16, "quota", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_17, "wait_quota", INT8OID, -1, 0);

    funcctx->tuple_desc = BlessTupleDesc(tupdesc);
    funcctx->max_calls = u_sess->pgxc_cxt.NumDataNodes + u_sess->pgxc_cxt.NumCoords;
    return;
}

Datum global_comm_get_send_stream(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_MULTIPLE_NODES
    FuncCallContext* funcctx = NULL;
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        errmsg("unsupported view in single node mode.")));

    SRF_RETURN_DONE(funcctx);
#else
    const int SEND_STREAM_TUPLE_NATTS = 17;
    FuncCallContext *funcctx = NULL;
    Datum values[SEND_STREAM_TUPLE_NATTS];
    bool nulls[SEND_STREAM_TUPLE_NATTS];
    HeapTuple tuple;
    MemoryContext oldcontext;

    if (SRF_IS_FIRSTCALL()) {
        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();
        /*
         * Switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        init_pgxc_comm_get_send_stream(fcinfo, funcctx, SEND_STREAM_TUPLE_NATTS);
        /* the main call for get table distribution. */
        funcctx->user_fctx = getGlobalCommStatus(funcctx->tuple_desc, "SELECT * FROM pg_comm_send_stream;");
        MemoryContextSwitchTo(oldcontext);
        if (funcctx->user_fctx == NULL) {
            SRF_RETURN_DONE(funcctx);
        }
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->user_fctx) {
        Tuplestorestate *tupstore = ((CommInfoParallel *)funcctx->user_fctx)->state->tupstore;
        TupleTableSlot    *slot = ((CommInfoParallel *)funcctx->user_fctx)->slot;
        if (!tuplestore_gettupleslot(tupstore, true, false, slot)) {
            FreeParallelFunctionState(((CommInfoParallel *)funcctx->user_fctx)->state);
            ExecDropSingleTupleTableSlot(slot);
            pfree_ext(funcctx->user_fctx);
            funcctx->user_fctx = NULL;
            SRF_RETURN_DONE(funcctx);
        }
        for (int i = 0; i < SEND_STREAM_TUPLE_NATTS; i++) {
            values[i] = tableam_tslot_getattr(slot, i + 1, &nulls[i]);
        }

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        ExecClearTuple(slot);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }
    SRF_RETURN_DONE(funcctx);
#endif
}



/*
* @Description: parallel get comm stream status in entire cluster througn ExecRemoteFunctionInParallel in RemoteFunctionResultHandler.
* @return: one tuple slot of result set.
*/
CommInfoParallel *
getGlobalCommStatus(TupleDesc tuple_desc, const char *queryString)
{
    StringInfoData            buf;
    CommInfoParallel* stream_info = NULL;

    initStringInfo(&buf);
    stream_info = (CommInfoParallel *)palloc0(sizeof(CommInfoParallel));
    appendStringInfoString(&buf, queryString);
    stream_info->state = RemoteFunctionResultHandler(buf.data, NULL, NULL, true, EXEC_ON_ALL_NODES, true);
    stream_info->slot = MakeSingleTupleTableSlot(tuple_desc);
    return stream_info;
}

Datum pg_tde_info(PG_FUNCTION_ARGS)
{

    int i = 0;
    errno_t rc = 0;

    TupleDesc tupdesc = CreateTemplateTupleDesc(3, false, TAM_HEAP);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "is_encrypt", BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "g_tde_algo", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "remain", TEXTOID, -1, 0);
    BlessTupleDesc(tupdesc);

    Datum values[3];
    bool nulls[3] = {false};
    HeapTuple tuple = NULL;

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    i = 0;
    const char* algo = "null";
    const char* remain = "remain";
    values[i++] = BoolGetDatum(false);
    values[i++] = CStringGetTextDatum(algo);
    values[i++] = CStringGetTextDatum(remain);
    tuple = heap_form_tuple(tupdesc, values, nulls);

    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

void init_pg_comm_status(PG_FUNCTION_ARGS, FuncCallContext *funcctx, const int argNums)
{
    TupleDesc tupdesc = NULL;
    MemoryContext oldcontext = NULL;

    /* create a function context for cross-call persistence */
    funcctx = SRF_FIRSTCALL_INIT();

    /*
     * Switch to memory context appropriate for multiple function calls
     */
    oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

    /* build tupdesc for result tuples */
    tupdesc = CreateTemplateTupleDesc(argNums, false, TAM_HEAP);

    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_1, "node_name", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_2, "rxpck_rate", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_3, "txpck_rate", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_4, "rxkbyte_rate", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_5, "txkbyte_rate", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_6, "buffer", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_7, "memkbyte_libcomm", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_8, "memkbyte_libpq", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_9, "used_pm", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_10, "used_sflow", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_11, "used_rflow", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_12, "used_rloop", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_13, "stream", INT4OID, -1, 0);

    funcctx->tuple_desc = BlessTupleDesc(tupdesc);
    MemoryContextSwitchTo(oldcontext);
    return;
}

Datum set_pg_comm_status(PG_FUNCTION_ARGS, FuncCallContext *funcctx, const int argNums)
{
    errno_t rc = 0;
    CommStat comm_stat;
    Datum values[argNums];
    bool nulls[argNums] = {false};
    HeapTuple tuple = NULL;
    Datum result;
    int ii = 0;

    funcctx = SRF_PERCALL_SETUP();
    rc = memset_s(&comm_stat, sizeof(CommStat), 0, sizeof(CommStat));

    if (gs_get_comm_stat(&comm_stat) == false) {
        SRF_RETURN_DONE(funcctx);
    }

    /*
     * Form tuple with appropriate data.
     */
    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    /* Locking is probably not really necessary */
    values[ii++] = CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);
    values[ii++] = Int64GetDatum(comm_stat.recv_count_speed);
    values[ii++] = Int32GetDatum(comm_stat.send_count_speed);
    values[ii++] = Int64GetDatum(comm_stat.recv_speed);
    values[ii++] = Int32GetDatum(comm_stat.send_speed);
    values[ii++] = Int64GetDatum(comm_stat.buffer);
    values[ii++] = Int64GetDatum(comm_stat.mem_libcomm);
    values[ii++] = Int64GetDatum(comm_stat.mem_libpq);
    values[ii++] = Int32GetDatum(comm_stat.postmaster);
    values[ii++] = Int32GetDatum(comm_stat.gs_sender_flow);
    values[ii++] = Int32GetDatum(comm_stat.gs_receiver_flow);
    values[ii++] = Int32GetDatum(comm_stat.gs_receiver_loop);
    values[ii++] = Int32GetDatum(comm_stat.stream_conn_num);
    tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
    result = HeapTupleGetDatum(tuple);
    SRF_RETURN_NEXT(funcctx, result);
}

/*
 * pg_comm_status
 *		Produce a view with one row per prepared transaction.
 *
 */
Datum pg_comm_status(PG_FUNCTION_ARGS)
{
    const int SEND_STREAM_TUPLE_NATTS = 13;
    FuncCallContext* funcctx = NULL;
    
    if (SRF_IS_FIRSTCALL()) {
        init_pg_comm_status(fcinfo, funcctx, SEND_STREAM_TUPLE_NATTS);
        return set_pg_comm_status(fcinfo, funcctx, SEND_STREAM_TUPLE_NATTS);
    } else {
        funcctx = SRF_PERCALL_SETUP();
        SRF_RETURN_DONE(funcctx);
    }
}

void init_pgxc_comm_get_status(PG_FUNCTION_ARGS, FuncCallContext *funcctx, const int argNums)
{
    TupleDesc tupdesc;

    /* build tupdesc for result tuples */
    tupdesc = CreateTemplateTupleDesc(argNums, false, TAM_HEAP);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_1, "node_name", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_2, "rxpck_rate", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_3, "txpck_rate", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_4, "rxkbyte_rate", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_5, "txkbyte_rate", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_6, "buffer", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_7, "memkbyte_libcomm", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_8, "memkbyte_libpq", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_9, "used_pm", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_10, "used_sflow", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_11, "used_rflow", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_12, "used_rloop", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ARG_13, "stream", INT4OID, -1, 0);
    funcctx->tuple_desc = BlessTupleDesc(tupdesc);
    funcctx->max_calls = u_sess->pgxc_cxt.NumDataNodes + u_sess->pgxc_cxt.NumCoords;
    return;
}

Datum global_comm_get_status(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_MULTIPLE_NODES
    FuncCallContext* funcctx = NULL;
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        errmsg("unsupported view in single node mode.")));

    SRF_RETURN_DONE(funcctx);
#else
    const int SEND_STREAM_TUPLE_NATTS = 13;
    FuncCallContext *funcctx = NULL;
    Datum values[SEND_STREAM_TUPLE_NATTS];
    bool nulls[SEND_STREAM_TUPLE_NATTS];
    HeapTuple tuple;
    MemoryContext oldcontext;

    if (SRF_IS_FIRSTCALL()) {
        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();
        /*
         * Switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        init_pgxc_comm_get_status(fcinfo, funcctx, SEND_STREAM_TUPLE_NATTS);
        /* the main call for get table distribution. */
        funcctx->user_fctx = getGlobalCommStatus(funcctx->tuple_desc, "SELECT * FROM pg_comm_status;");
        MemoryContextSwitchTo(oldcontext);
        if (funcctx->user_fctx == NULL)
            SRF_RETURN_DONE(funcctx);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();
    if (funcctx->user_fctx) {
        Tuplestorestate *tupstore = ((CommInfoParallel *)funcctx->user_fctx)->state->tupstore;
        TupleTableSlot    *slot = ((CommInfoParallel *)funcctx->user_fctx)->slot;
        if (!tuplestore_gettupleslot(tupstore, true, false, slot)) {
            FreeParallelFunctionState(((CommInfoParallel *)funcctx->user_fctx)->state);
            ExecDropSingleTupleTableSlot(slot);
            pfree_ext(funcctx->user_fctx);
            funcctx->user_fctx = NULL;
            SRF_RETURN_DONE(funcctx);
        }
        for (int i = 0; i < SEND_STREAM_TUPLE_NATTS; i++) {
            values[i] = tableam_tslot_getattr(slot, i + 1, &nulls[i]);
        }

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        ExecClearTuple(slot);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }
    SRF_RETURN_DONE(funcctx);
#endif
}

#ifdef ENABLE_MULTIPLE_NODES
Datum pgxc_node_str(PG_FUNCTION_ARGS)
{
	if (!g_instance.attr.attr_common.PGXCNodeName || g_instance.attr.attr_common.PGXCNodeName[0] == '\0') {
		PG_RETURN_NULL();
        }

	PG_RETURN_DATUM(DirectFunctionCall1(namein, CStringGetDatum(g_instance.attr.attr_common.PGXCNodeName)));
}
#endif

void set_comm_client_info(Datum *values, bool *nulls, const PgBackendStatus *beentry)
{
    /* Values only available to same user or superuser */
    if (superuser() || beentry->st_userid == GetUserId()) {
        values[ARR_0] = CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);
        values[ARR_1] = CStringGetTextDatum(beentry->remote_info.remote_name);
        values[ARR_3] = Int32GetDatum(beentry->st_tid);
        values[ARR_4] = Int64GetDatum(beentry->st_queryid);
    } else {
        nulls[ARR_0] = true;
        nulls[ARR_1] = true;
        nulls[ARR_3] = true;
        nulls[ARR_4] = true;
    }
    values[ARR_2] = Int64GetDatum(beentry->st_procpid);
    values[ARR_5] = Int32GetDatum(beentry->remote_info.socket);
    values[ARR_6] = CStringGetTextDatum(beentry->remote_info.remote_ip);
    values[ARR_7] = CStringGetTextDatum(beentry->remote_info.remote_port);
    values[ARR_8] = Int32GetDatum(beentry->remote_info.logic_id);
    return;
}

Datum comm_client_info(PG_FUNCTION_ARGS)
{
    const int CLIENT_INFO_TUPLE_NATTS = 9;

    if (!superuser()) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("Only system admin user can use this function.")));
    }

    Assert(STATE_WAIT_NUM == sizeof(WaitStateDesc)/sizeof(WaitStateDesc[0]));


    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    MemoryContext oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);

    TupleDesc tupdesc = CreateTemplateTupleDesc(CLIENT_INFO_TUPLE_NATTS, false);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_1, "node_name", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_2, "app", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_3, "tid", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_4, "lwtid", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_5, "query_id", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_6, "socket", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_7, "remote_ip", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_8, "remote_port", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_9, "logic_id", INT4OID, -1, 0);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->setDesc = BlessTupleDesc(tupdesc);

    (void)gs_stat_read_current_status(rsinfo->setResult, rsinfo->setDesc, insert_comm_client_info);

    MemoryContextSwitchTo(oldcontext);
    /* clean up and return the tuplestore */
    tuplestore_donestoring(rsinfo->setResult);

    return (Datum) 0;
}

void insert_comm_client_info(Tuplestorestate *tupStore, TupleDesc tupDesc, const PgBackendStatus *beentry)
{
    const int CLIENT_INFO_TUPLE_NATTS = 9;
    Datum values[CLIENT_INFO_TUPLE_NATTS];
    bool nulls[CLIENT_INFO_TUPLE_NATTS];
    char waitStatus[WAITSTATELEN];
    errno_t rc = 0;

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(rc, "\0", "\0");
    rc = memset_s(waitStatus, sizeof(waitStatus), 0, sizeof(waitStatus));
    securec_check(rc, "\0", "\0");

    /* Values available to all callers */
    rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    set_comm_client_info(values, nulls, beentry);

    tuplestore_putvalues(tupStore, tupDesc, values, nulls);
}

void fill_callcxt_for_comm_check_connection_status(FuncCallContext *funcctx)
{
    const int attNum = 6;
    int att_idx = 1;
    ConnectionStatus *conns_entry = NULL;

    MemoryContext oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

    TupleDesc tupdesc = CreateTemplateTupleDesc(attNum, false);

    TupleDescInitEntry(tupdesc, (AttrNumber) att_idx++, "node_name", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) att_idx++, "remote_name", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) att_idx++, "remote_host", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) att_idx++, "remote_port", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) att_idx++, "is_connected", BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) att_idx++, "no_error_occur", BOOLOID, -1, 0);

    funcctx->tuple_desc = BlessTupleDesc(tupdesc);

    funcctx->max_calls = check_connection_status(&conns_entry);
    funcctx->user_fctx = conns_entry;

    MemoryContextSwitchTo(oldcontext);
    return;
}

void fill_values_for_comm_check_connection_status(Datum *values, 
    FuncCallContext *funcctx, ConnectionStatus *conns_entry)
{
    int att_idx = 0;
    
    ConnectionStatus *poolCon = conns_entry + funcctx->call_cntr;
    
    values[att_idx++]  = CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);
    values[att_idx++]  = CStringGetTextDatum(poolCon->remote_name);
    values[att_idx++] = CStringGetTextDatum(poolCon->remote_host);
    values[att_idx++] = (Datum)(poolCon->remote_port);
    values[att_idx++] = BoolGetDatum(poolCon->is_connected);
    values[att_idx++] = BoolGetDatum(poolCon->no_error_occur);
    return;
}

/*
 * comm_check_connection_status
 *		Produce a view with connections status between cn and all other active nodes.
 *
 */
Datum comm_check_connection_status(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_MULTIPLE_NODES
    FuncCallContext* funcctx = NULL;
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        errmsg("unsupported function/view in single node mode.")));
    SRF_RETURN_DONE(funcctx);
#else
    if (IS_PGXC_DATANODE) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("unsupported functin/view in datenode.")));
    }

    FuncCallContext *funcctx = NULL;
    ConnectionStatus *conns_entry = NULL;
    const int attNum = 6;

    if (SRF_IS_FIRSTCALL()) {
        funcctx = SRF_FIRSTCALL_INIT();
        fill_callcxt_for_comm_check_connection_status(funcctx);;
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();
    conns_entry = (ConnectionStatus *)(funcctx->user_fctx);

    if (funcctx->call_cntr < funcctx->max_calls) {
        /* for each row */
        Datum values[attNum] = {0};
        bool nulls[attNum] = {false};

        fill_values_for_comm_check_connection_status(values, funcctx, conns_entry);
        HeapTuple tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);

        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    } else {
        ConnectionStatus *pcon = NULL;
        for (uint32 i = 0; i < funcctx->max_calls; i++) {
            pcon = conns_entry + i;
            pfree_ext(pcon->remote_name);
            pfree_ext(pcon->remote_host);
        }
        pfree_ext(conns_entry);

        /* nothing left */
        SRF_RETURN_DONE(funcctx);
    }
#endif
}


Datum global_comm_get_client_info(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_MULTIPLE_NODES
    FuncCallContext* funcctx = NULL;
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        errmsg("unsupported view in single node mode.")));

    SRF_RETURN_DONE(funcctx);
#else
    const int CLIENT_INFO_TUPLE_NATTS = 9;
    FuncCallContext *funcctx = NULL;
    Datum values[CLIENT_INFO_TUPLE_NATTS];
    bool nulls[CLIENT_INFO_TUPLE_NATTS];
    HeapTuple tuple;
    int i;

    if (SRF_IS_FIRSTCALL()) {
        TupleDesc    tupdesc;
        MemoryContext oldcontext;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /* Switch to memory context appropriate for multiple function calls */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples */
        tupdesc = CreateTemplateTupleDesc(CLIENT_INFO_TUPLE_NATTS, false, TAM_HEAP);
        TupleDescInitEntry(tupdesc, (AttrNumber) ARG_1, "node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) ARG_2, "app", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) ARG_3, "tid", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) ARG_4, "lwtid", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) ARG_5, "query_id", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) ARG_6, "socket", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) ARG_7, "remote_ip", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) ARG_8, "remote_port", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) ARG_9, "logic_id", INT4OID, -1, 0);
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        funcctx->max_calls = u_sess->pgxc_cxt.NumDataNodes + u_sess->pgxc_cxt.NumCoords;
        /* the main call for get table distribution. */
        funcctx->user_fctx = getGlobalCommStatus(funcctx->tuple_desc,  "SELECT * FROM comm_client_info;");
        MemoryContextSwitchTo(oldcontext);
        if (funcctx->user_fctx == NULL)
            SRF_RETURN_DONE(funcctx);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();
    if (funcctx->user_fctx) {
        Tuplestorestate *tupstore = ((CommInfoParallel *)funcctx->user_fctx)->state->tupstore;
        TupleTableSlot    *slot = ((CommInfoParallel *)funcctx->user_fctx)->slot;

        if (!tuplestore_gettupleslot(tupstore, true, false, slot)) {
            FreeParallelFunctionState(((CommInfoParallel *)funcctx->user_fctx)->state);
            ExecDropSingleTupleTableSlot(slot);
            pfree_ext(funcctx->user_fctx);
            funcctx->user_fctx = NULL;
            SRF_RETURN_DONE(funcctx);
        }
        for (i = 0; i < CLIENT_INFO_TUPLE_NATTS; i++) {
            values[i] = tableam_tslot_getattr(slot, i + 1, &nulls[i]);
        }
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        ExecClearTuple(slot);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }
    SRF_RETURN_DONE(funcctx);
#endif
}

/*
 * pg_comm_delay
 *		Produce a view to show sctp delay infomation
 *
 */
Datum pg_comm_delay(PG_FUNCTION_ARGS)
{
#define DELAY_INFO_TUPLE_NATTS 7
    FuncCallContext* funcctx = NULL;
    CommDelayInfo* delay_info = NULL;
    int ii = 0;
    errno_t rc = 0;

    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tupdesc;
        MemoryContext oldcontext;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
         * Switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples */
        ii = 1;
        tupdesc = CreateTemplateTupleDesc(DELAY_INFO_TUPLE_NATTS, false, TAM_HEAP);
        TupleDescInitEntry(tupdesc, (AttrNumber)ii++, "node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ii++, "remote_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ii++, "remote_host", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ii++, "stream_num", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ii++, "min_delay", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ii++, "average", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ii++, "max_delay", INT4OID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        delay_info = (CommDelayInfo*)palloc(sizeof(CommDelayInfo));
        rc = memset_s(delay_info, sizeof(CommDelayInfo), 0, sizeof(CommDelayInfo));
        securec_check(rc, "\0", "\0");
        funcctx->user_fctx = (void*)delay_info;

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    delay_info = (CommDelayInfo*)funcctx->user_fctx;

    if (get_next_comm_delay_info(delay_info) == true) {
        Datum values[DELAY_INFO_TUPLE_NATTS];
        bool nulls[DELAY_INFO_TUPLE_NATTS] = {false};
        HeapTuple tuple = NULL;
        Datum result;

        /*
         * Form tuple with appropriate data.
         */
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        ii = 0;

        /* Locking is probably not really necessary */
        values[ii++] = CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);
        values[ii++] = (Datum)cstring_to_text(delay_info->remote_node);
        values[ii++] = (Datum)cstring_to_text(delay_info->remote_host);
        values[ii++] = (Datum)(delay_info->stream_num);
        values[ii++] = (Datum)(delay_info->min_delay);
        values[ii++] = (Datum)(delay_info->dev_delay);
        values[ii++] = (Datum)(delay_info->max_delay);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, result);

    } else {
        SRF_RETURN_DONE(funcctx);
    }
}

/*
 * Description: retrieve the cgroup id based on pool name
 * @IN   pool: pool name
 * @OUT  name : control group name in resource pool
 * @OUT  clsname : class group name
 * @OUT  wdname : workload group name
 * @OUT  cls : class group id
 * @OUT  wd  : workload group id
 * return value:
 *    -1: abnormal
 *     0: normal
 */
extern int gscgroup_is_class(WLMNodeGroupInfo* ng, const char* gname);
extern int gscgroup_is_workload(WLMNodeGroupInfo* ng, int cls, const char* gname);
extern int gscgroup_is_timeshare(const char* gname);

WLMNodeGroupInfo* gs_get_cgroup_id(const char* pool, char* name, char* clsname, char* wdname, int* cls, int* wd)
{
    WLMNodeGroupInfo* ng = NULL;

    /* get the pool id */
    Oid rpoid = get_resource_pool_oid(pool);

    if (!OidIsValid(rpoid))
        ereport(
            ERROR, (errcode(ERRCODE_RESOURCE_POOL_ERROR), errmsg("failed to get the oid of resource pool %s!", pool)));

    /* get the pool information */
    FormData_pg_resource_pool_real rpdata;

    if (!get_resource_pool_param(rpoid, &rpdata))
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("failed to get the parameter of resource pool %s!", pool)));

    char* gname = NameStr(rpdata.control_group);

    if (g_instance.wlm_cxt->gscgroup_init_done == 0 || !StringIsValid(gname))
        ereport(ERROR,
            (errcode(ERRCODE_RESOURCE_POOL_ERROR),
                errmsg("cgroup is not initialized or group name %s is invalid!", gname)));

    char* ngname = NameStr(rpdata.nodegroup);
    if (NULL == (ng = WLMGetNodeGroupFromHTAB(ngname)))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("%s logical cluster doesn't exist!", ngname)));

    char* p = NULL;
    char* q = NULL;

    int sret = snprintf_s(name, GPNAME_LEN, GPNAME_LEN - 1, "%s", gname);
    securec_check_intval(sret, , NULL);

    q = pstrdup(gname);

    p = strchr(q, ':');
    if (NULL == p) { /* Class group */
        /* check if the class exists */
        if (-1 == (*cls = gscgroup_is_class(ng, q)))
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("class group %s doesn't exist!", gname)));

        sret = snprintf_s(clsname, GPNAME_LEN, GPNAME_LEN - 1, "%s", q);
        securec_check_intval(sret, pfree_ext(q);, NULL);
    } else {
        *p++ = '\0';

        /* check if the class exists */
        if (-1 == (*cls = gscgroup_is_class(ng, q)))
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("class group %s doesn't exist!", q)));

        sret = snprintf_s(clsname, GPNAME_LEN, GPNAME_LEN - 1, "%s", q);
        securec_check_intval(sret, pfree_ext(q);, NULL);

        if (-1 != (*wd = gscgroup_is_timeshare(p))) {
            sret = snprintf_s(wdname, GPNAME_LEN, GPNAME_LEN - 1, "%s", p);
            securec_check_intval(sret, pfree_ext(q);, NULL);
        } else if (-1 != (*wd = gscgroup_is_workload(ng, *cls, p))) {
            char* tmpstr = strchr(p, ':');
            if (tmpstr != NULL)
                *tmpstr = '\0';

            sret = snprintf_s(wdname, GPNAME_LEN, GPNAME_LEN - 1, "%s", p);
            securec_check_intval(sret, pfree_ext(q);, NULL);
        } else
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("workload group %s doesn't exist!", p)));
    }

    pfree_ext(q);

    return ng;
}

/*
 * function name: cgconf_get_group_type
 * description  : get the string of group type
 * arguments    : group type enum type value
 * return value : the string of group type
 *
 */
char* cgconf_get_group_type(group_type gtype)
{
    if (gtype == GROUP_TOP)
        return "Top";
    else if (gtype == GROUP_CLASS)
        return "CLASS";
    else if (gtype == GROUP_BAKWD)
        return "BAKWD";
    else if (gtype == GROUP_DEFWD)
        return "DEFWD";
    else if (gtype == GROUP_TSWD)
        return "TSWD";

    return "NULL";
}

/*
 * select gs_control_group_info('respoolName');
 */
Datum gs_control_group_info(PG_FUNCTION_ARGS)
{
    if (!superuser()) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_PROC,
            "gs_control_group_info");
    }
    char* pool = PG_GETARG_CSTRING(0);
    if (NULL == pool || strlen(pool) == 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("invalid name of resource pool: NULL or \"\"")));
        PG_RETURN_BOOL(false);
    }

    int cls = 0, wd = 0;
    char gname[GPNAME_LEN] = {0};
    char clsname[GPNAME_LEN] = {0};
    char wdname[GPNAME_LEN] = {0};

    /* get the class and workload id */
    WLMNodeGroupInfo* ng = gs_get_cgroup_id(pool, gname, clsname, wdname, &cls, &wd);

    gscgroup_grp_t* grp = NULL;

    /* parent group */
    if (cls && 0 == wd)
        grp = ng->vaddr[cls];
    else if (cls && wd)
        grp = ng->vaddr[wd];

    FuncCallContext* funcctx = NULL;

    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tupdesc = NULL;
        MemoryContext oldcontext = NULL;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples */
        /* this had better match pv_plan view in system_views.sql */
        tupdesc = CreateTemplateTupleDesc(9, false, TAM_HEAP);
        TupleDescInitEntry(tupdesc, (AttrNumber)1, "name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "class", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "workload", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)4, "type", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)5, "gid", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)6, "shares", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)7, "limits", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)8, "rate", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)9, "cpucores", TEXTOID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->max_calls = 1;
        funcctx->call_cntr = 0;

        (void)MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->call_cntr < funcctx->max_calls) {
        /* for each row */
        Datum values[9];
        bool nulls[9] = {false};
        HeapTuple tuple = NULL;

        /*
         * Form tuple with appropriate data.
         */
        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        /* Locking is probably not really necessary */
        values[0] = CStringGetTextDatum(gname);
        values[1] = CStringGetTextDatum(clsname);
        values[2] = CStringGetTextDatum(wdname);
        values[3] = CStringGetTextDatum(cgconf_get_group_type(grp->gtype));
        values[4] = Int64GetDatum(grp->gid);
        if (cls && 0 == wd) {
            values[5] = Int64GetDatum(grp->ginfo.cls.percent);
            values[6] = Int64GetDatum(grp->ainfo.quota);
            values[7] = Int64GetDatum(0);
        } else if (cls && wd) {
            if (wd >= WDCG_START_ID && wd <= WDCG_END_ID) {
                values[5] = Int64GetDatum(grp->ginfo.wd.percent);
                values[6] = Int64GetDatum(grp->ainfo.quota);
                values[7] = Int64GetDatum(0);
            } else if (wd >= TSCG_START_ID && wd <= TSCG_END_ID) {
                values[5] = Int64GetDatum(0);
                values[6] = Int64GetDatum(0);
                values[7] = Int64GetDatum(grp->ginfo.ts.rate);
            }
        }

        values[8] = CStringGetTextDatum(grp->cpuset);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    } else {
        /* nothing left */
        SRF_RETURN_DONE(funcctx);
    }
}

/*
 * select gs_respool_exception_info('respoolName');
 */
#define EXCP_TYPES_CNT 14
const char* excpname[] = {
    "BlockTime", "ElapsedTime", "AllCpuTime", "QualificationTime", "CPUSkewPercent", "Spillsize", "Broadcastsize"};

Datum gs_respool_exception_info(PG_FUNCTION_ARGS)
{
    char* pool = PG_GETARG_CSTRING(0);
    if (NULL == pool || strlen(pool) == 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("invalid name of resource pool: NULL or \"\"")));
        PG_RETURN_BOOL(false);
    }

    int cls = 0, wd = 0;
    char gname[GPNAME_LEN] = {0};
    char clsname[GPNAME_LEN] = {0};
    char wdname[GPNAME_LEN] = {0};

    /* get the class and workload id */
    WLMNodeGroupInfo* ng = gs_get_cgroup_id(pool, gname, clsname, wdname, &cls, &wd);

    gscgroup_grp_t* grp = NULL;

    /* parent group */
    if (!cls || (wd == 0)) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("parent group '%s' doesn't have exception information!", gname)));
        PG_RETURN_BOOL(false);
    }

    if (wd >= WDCG_START_ID && wd <= WDCG_END_ID)
        grp = ng->vaddr[wd];
    else if (wd >= TSCG_START_ID && wd <= TSCG_END_ID)
        grp = ng->vaddr[cls];

    FuncCallContext* funcctx = NULL;
    unsigned int* excpvalue = NULL;

    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tupdesc;
        MemoryContext oldcontext;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
         * Switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples */
        tupdesc = CreateTemplateTupleDesc(6, false, TAM_HEAP);

        TupleDescInitEntry(tupdesc, (AttrNumber)1, "name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "class", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "workload", TEXTOID, -1, 0);

        TupleDescInitEntry(tupdesc, (AttrNumber)4, "rule", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)5, "type", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)6, "value", INT8OID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        excpvalue = (unsigned int*)palloc0(EXCP_TYPES_CNT * sizeof(unsigned int));
        funcctx->user_fctx = (unsigned int*)excpvalue;

        (void)MemoryContextSwitchTo(oldcontext);

        if (NULL != grp) {
            excpvalue[0] = grp->except[EXCEPT_ABORT].blocktime;
            excpvalue[1] = grp->except[EXCEPT_ABORT].elapsedtime;
            excpvalue[2] = grp->except[EXCEPT_ABORT].allcputime;
            excpvalue[3] = grp->except[EXCEPT_ABORT].qualitime;
            excpvalue[4] = grp->except[EXCEPT_ABORT].skewpercent;
            excpvalue[5] = grp->except[EXCEPT_ABORT].spoolsize;
            excpvalue[6] = grp->except[EXCEPT_ABORT].broadcastsize;
            excpvalue[7] = grp->except[EXCEPT_PENALTY].blocktime;
            excpvalue[8] = grp->except[EXCEPT_PENALTY].elapsedtime;
            excpvalue[9] = grp->except[EXCEPT_PENALTY].allcputime;
            excpvalue[10] = grp->except[EXCEPT_PENALTY].qualitime;
            excpvalue[11] = grp->except[EXCEPT_PENALTY].skewpercent;
            excpvalue[12] = grp->except[EXCEPT_PENALTY].spoolsize;
            excpvalue[13] = grp->except[EXCEPT_PENALTY].broadcastsize;
        }
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    excpvalue = (unsigned int*)funcctx->user_fctx;

    while (funcctx->call_cntr < EXCP_TYPES_CNT) {
        Datum values[6];
        bool nulls[6] = {false};
        HeapTuple tuple = NULL;

        /*
         * Form tuple with appropriate data.
         */
        errno_t errval = memset_s(&values, sizeof(values), 0, sizeof(values));
        securec_check_errval(errval, , LOG);
        errval = memset_s(&nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check_errval(errval, , LOG);

        /* Locking is probably not really necessary */
        values[0] = CStringGetTextDatum(gname);
        values[1] = CStringGetTextDatum(clsname);
        values[2] = CStringGetTextDatum(wdname);
        values[3] = CStringGetTextDatum(excpname[funcctx->call_cntr % (EXCP_TYPES_CNT / 2)]);
        if (funcctx->call_cntr >= (EXCP_TYPES_CNT / 2))
            values[4] = CStringGetTextDatum("Penalty");
        else
            values[4] = CStringGetTextDatum("Abort");

        values[5] = Int64GetDatum(excpvalue[funcctx->call_cntr]);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }

    pfree_ext(excpvalue);
    /* do when there is no more left */
    SRF_RETURN_DONE(funcctx);
}

/*
 * return all control group info
 */
gscgroup_grp_t* getControlGroupInfo(WLMNodeGroupInfo* ng, uint32* num)
{
    gscgroup_grp_t* localArray = NULL;
    gscgroup_grp_t* localEntry = NULL;
    gscgroup_grp_t* entry = NULL;
    int entryIndex;
    uint32 count = 0;

    localArray =
        (gscgroup_grp_t*)palloc0((GSCGROUP_CLASSNUM + GSCGROUP_WDNUM + GSCGROUP_TSNUM) * sizeof(gscgroup_grp_t));
    localEntry = localArray;
    for (entryIndex = 0; entryIndex < (GSCGROUP_CLASSNUM + GSCGROUP_WDNUM + GSCGROUP_TSNUM); entryIndex++) {
        entry = ng->vaddr[CLASSCG_START_ID + entryIndex];
        errno_t rc = memcpy_s(localEntry, sizeof(gscgroup_grp_t), entry, sizeof(gscgroup_grp_t));
        securec_check(rc, "\0", "\0");

        if (entry->used == 0 || (entry->gtype == GROUP_DEFWD && 0 == strcmp(entry->grpname, "TopWD:1")))
            continue;

        localEntry++;
        count++;
    }

    *num = count;
    return localArray;
}

/*
 * select * from gs_all_control_group_info;
 */
Datum gs_all_control_group_info(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;
    gscgroup_grp_t* array = NULL;

    if (g_instance.wlm_cxt->gscgroup_init_done == 0)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("cgroup is not initialized!")));

    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tupdesc;
        MemoryContext oldcontext;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples */
        /* this had better match pv_plan view in system_views.sql */
        tupdesc = CreateTemplateTupleDesc(10, false, TAM_HEAP);
        TupleDescInitEntry(tupdesc, (AttrNumber)1, "name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "type", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "gid", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)4, "classgid", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)5, "class", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)6, "workload", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)7, "shares", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)8, "limits", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)9, "wdlevel", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)10, "cpucores", TEXTOID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->user_fctx = (void*)getControlGroupInfo(&g_instance.wlm_cxt->MyDefaultNodeGroup, &(funcctx->max_calls));

        (void)MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();
    array = (gscgroup_grp_t*)funcctx->user_fctx;

    if (funcctx->call_cntr < funcctx->max_calls) {
        /* for each row */
        Datum values[10];
        bool nulls[10] = {false};
        HeapTuple tuple = NULL;
        gscgroup_grp_t* entry = NULL;
        uint32 entryIndex;

        /*
         * Form tuple with appropriate data.
         */
        errno_t ss_rc = EOK;
        ss_rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(ss_rc, "\0", "\0");
        ss_rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(ss_rc, "\0", "\0");

        entryIndex = funcctx->call_cntr;

        entry = array + entryIndex;

        /* Locking is probably not really necessary */
        values[0] = CStringGetTextDatum(entry->grpname);
        values[1] = CStringGetTextDatum(cgconf_get_group_type(entry->gtype));
        values[2] = Int64GetDatum(entry->gid);
        if (entry->gtype == GROUP_CLASS) {
            values[3] = Int64GetDatum(0);
            values[4] = CStringGetTextDatum(entry->grpname);
            nulls[5] = true;
            values[6] = Int64GetDatum(entry->ginfo.cls.percent);
            values[7] = Int64GetDatum(entry->ainfo.quota);
            values[8] = Int64GetDatum(0);
        } else if (entry->gtype == GROUP_DEFWD) {
            int clsid = entry->ginfo.wd.cgid;
            char wldname[GPNAME_LEN];
            char* p = NULL;

            ss_rc = strcpy_s(wldname, GPNAME_LEN, entry->grpname);
            securec_check(ss_rc, "\0", "\0");
            /* omit the ':' character in the name */
            if (NULL != (p = strchr(wldname, ':')))
                *p = '\0';

            values[3] = Int64GetDatum(clsid);
            values[4] = CStringGetTextDatum(g_instance.wlm_cxt->gscgroup_vaddr[clsid]->grpname);
            values[5] = CStringGetTextDatum(wldname);
            values[6] = Int64GetDatum(entry->ginfo.wd.percent);
            values[7] = Int64GetDatum(entry->ainfo.quota);
            values[8] = Int64GetDatum(entry->ginfo.wd.wdlevel);
        } else if (entry->gtype == GROUP_TSWD) {
            values[3] = Int64GetDatum(0);
            nulls[4] = true;
            values[5] = CStringGetTextDatum(entry->grpname);
            values[6] = Int64GetDatum(0);
            values[7] = Int64GetDatum(0);
            values[8] = Int64GetDatum(0);
        }
        values[9] = CStringGetTextDatum(entry->cpuset);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    } else {
        SRF_RETURN_DONE(funcctx);
    }
}

/*
 * select * from gs_all_nodegroup_control_group_info('nodegroup');
 */
Datum gs_all_nodegroup_control_group_info(PG_FUNCTION_ARGS)
{
    if (!superuser()) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_PROC,
            "gs_all_nodegroup_control_group_info");
    }
#define NODEGROUP_CONTROL_GROUP_INFO_ATTRNUM 10
    char* ngname = PG_GETARG_CSTRING(0);
    if (NULL == ngname || strlen(ngname) == 0)
        ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("invalid name of logical cluster: NULL or \"\"")));

    if (g_instance.wlm_cxt->gscgroup_init_done == 0)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("cgroup is not initialized!")));

    /* get the class and workload id */
    WLMNodeGroupInfo* ng = WLMGetNodeGroupFromHTAB(ngname);
    if (NULL == ng)
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("%s logical cluster doesn't exist!", ngname)));

    FuncCallContext* funcctx = NULL;
    gscgroup_grp_t* array = NULL;
    int i = 0;

    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tupdesc;
        MemoryContext oldcontext;
        i = 0;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples */
        /* this had better match pv_plan view in system_views.sql */
        tupdesc = CreateTemplateTupleDesc(NODEGROUP_CONTROL_GROUP_INFO_ATTRNUM, false, TAM_HEAP);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "type", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "gid", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "classgid", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "class", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "workload", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "shares", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "limits", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "wdlevel", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "cpucores", TEXTOID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->user_fctx = (void*)getControlGroupInfo(ng, &(funcctx->max_calls));

        (void)MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();
    array = (gscgroup_grp_t*)funcctx->user_fctx;

    if (funcctx->call_cntr < funcctx->max_calls) {
        /* for each row */
        Datum values[NODEGROUP_CONTROL_GROUP_INFO_ATTRNUM];
        bool nulls[NODEGROUP_CONTROL_GROUP_INFO_ATTRNUM] = {false};
        HeapTuple tuple = NULL;
        gscgroup_grp_t* entry = NULL;
        uint32 entryIndex;

        i = -1;

        /*
         * Form tuple with appropriate data.
         */
        errno_t ss_rc = EOK;
        ss_rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(ss_rc, "\0", "\0");
        ss_rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(ss_rc, "\0", "\0");

        entryIndex = funcctx->call_cntr;
        entry = array + entryIndex;

        /* Locking is probably not really necessary */
        values[++i] = CStringGetTextDatum(entry->grpname);
        values[++i] = CStringGetTextDatum(cgconf_get_group_type(entry->gtype));
        values[++i] = Int64GetDatum(entry->gid);
        if (entry->gtype == GROUP_CLASS) {
            values[++i] = Int64GetDatum(0);
            values[++i] = CStringGetTextDatum(entry->grpname);
            nulls[++i] = true;
            values[++i] = Int64GetDatum(entry->ginfo.cls.percent);
            values[++i] = Int64GetDatum(entry->ainfo.quota);
            values[++i] = Int64GetDatum(0);
        } else if (entry->gtype == GROUP_DEFWD) {
            int clsid = entry->ginfo.wd.cgid;
            char wldname[GPNAME_LEN];
            char* p = NULL;

            ss_rc = strcpy_s(wldname, GPNAME_LEN, entry->grpname);
            securec_check(ss_rc, "\0", "\0");
            /* omit the ':' character in the name */
            if (NULL != (p = strchr(wldname, ':')))
                *p = '\0';

            values[++i] = Int64GetDatum(clsid);
            values[++i] = CStringGetTextDatum(ng->vaddr[clsid]->grpname);
            values[++i] = CStringGetTextDatum(wldname);
            values[++i] = Int64GetDatum(entry->ginfo.wd.percent);
            values[++i] = Int64GetDatum(entry->ainfo.quota);
            values[++i] = Int64GetDatum(entry->ginfo.wd.wdlevel);
        } else if (entry->gtype == GROUP_TSWD) {
            values[++i] = Int64GetDatum(0);
            nulls[++i] = true;
            values[++i] = CStringGetTextDatum(entry->grpname);
            values[++i] = Int64GetDatum(0);
            values[++i] = Int64GetDatum(0);
            values[++i] = Int64GetDatum(0);
        }
        values[++i] = CStringGetTextDatum(entry->cpuset);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    } else {
        SRF_RETURN_DONE(funcctx);
    }
}

#define NUM_COMPUTE_POOL_WORKLOAD_ELEM 4

Datum pv_compute_pool_workload(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;
    MemoryContext oldcontext = NULL;

    if (IS_PGXC_DATANODE) {
        ereport(NOTICE, (errmodule(MOD_WLM_CP), errmsg("pv_compute_pool_workload() must run on CN.")));
    } else {
        if (0 == g_instance.attr.attr_sql.max_resource_package) {
            ereport(NOTICE,
                (errmodule(MOD_WLM_CP),
                    errmsg("\"max_resource_package == 0\" means that the cluster is NOT a compute pool.")));
        }
    }

    /* stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tupdesc;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function
         * calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* need a tuple descriptor representing 3 columns */
        tupdesc = CreateTemplateTupleDesc(NUM_COMPUTE_POOL_WORKLOAD_ELEM, false, TAM_HEAP);

        TupleDescInitEntry(tupdesc, (AttrNumber)1, "nodename", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "rpinuse", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "maxrp", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)4, "nodestate", TEXTOID, -1, 0);

        /* complete descriptor of the tupledesc */
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        /* total number of tuples to be returned */
        if (IS_PGXC_COORDINATOR && g_instance.attr.attr_sql.max_resource_package) {
            funcctx->user_fctx = get_cluster_state();

            if (funcctx->user_fctx)
                funcctx->max_calls = u_sess->pgxc_cxt.NumDataNodes;
        }

        (void)MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->call_cntr < funcctx->max_calls) { /* do when there is more left to send */
        Datum values[NUM_COMPUTE_POOL_WORKLOAD_ELEM];
        bool nulls[NUM_COMPUTE_POOL_WORKLOAD_ELEM] = {false};
        HeapTuple tuple = NULL;

        errno_t ss_rc = EOK;
        ss_rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(ss_rc, "\0", "\0");
        ss_rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(ss_rc, "\0", "\0");

        char* datanodeName = NULL;
        datanodeName = getNodenameByIndex(funcctx->call_cntr);

        int dn_num = 0;
        DNState* dn_state = NULL;

        ComputePoolState* cps = (ComputePoolState*)funcctx->user_fctx;
        if (cps != NULL) {
            Assert(cps->dn_num);

            dn_num = cps->dn_num;
            dn_state = cps->dn_state;
        }

        int dn = funcctx->call_cntr;

        if (dn < dn_num) {
            int num_rp = dn_state[dn].num_rp;
            if (num_rp > g_instance.attr.attr_sql.max_resource_package)
                num_rp = g_instance.attr.attr_sql.max_resource_package;

            values[0] = CStringGetTextDatum(datanodeName);
            values[1] = Int32GetDatum(num_rp);
            values[2] = Int32GetDatum(g_instance.attr.attr_sql.max_resource_package);
            values[3] = CStringGetTextDatum(dn_state[dn].is_normal ? "normal" : "abnormal");
        } else {
            values[0] = CStringGetTextDatum(datanodeName);
            values[1] = Int32GetDatum(0);
            values[2] = Int32GetDatum(g_instance.attr.attr_sql.max_resource_package);
            values[3] = CStringGetTextDatum("normal");
        }

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);

        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    } else {
        /* do when there is no more left */
        SRF_RETURN_DONE(funcctx);
    }
}

/* on which coordinator for a special table do autovac  */
Datum pg_autovac_coordinator(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    HeapTuple tuple = NULL;
    int coor_idx;
    Datum result;

    if (IS_SINGLE_NODE) {
        PG_RETURN_DATUM(CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName));
    }

    if (!IS_PGXC_COORDINATOR)
        PG_RETURN_NULL();

    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(tuple)) {
        /* it is droppped or does not exist */
        PG_RETURN_NULL();
    }

    if (!allow_autoanalyze(tuple)) {
        /* do not allow to do autovac */
        ReleaseSysCache(tuple);
        PG_RETURN_NULL();
    }
    ReleaseSysCache(tuple);

    coor_idx = PgxcGetCentralNodeIndex();
    LWLockAcquire(NodeTableLock, LW_SHARED);
    if (0 <= coor_idx && coor_idx < *t_thrd.pgxc_cxt.shmemNumCoords) {
        result = CStringGetTextDatum(NameStr(t_thrd.pgxc_cxt.coDefs[coor_idx].nodename));
        LWLockRelease(NodeTableLock);

        PG_RETURN_DATUM(result);
    }

    LWLockRelease(NodeTableLock);

    PG_RETURN_NULL();
}

static int64 pgxc_exec_partition_tuples_stat(HeapTuple classTuple, HeapTuple partTuple, char* funcname)
{
    ExecNodes* exec_nodes = NULL;
    ParallelFunctionState* state = NULL;
    Form_pg_partition partForm = NULL;
    Form_pg_class classForm = NULL;
    char* nspname = NULL;
    char* relname = NULL;
    char* partname = NULL;
    bool replicated = false;
    StringInfoData buf;
    RelationLocInfo* rel_loc_info = NULL;
    int64 result = 0;

    exec_nodes = (ExecNodes*)makeNode(ExecNodes);
    exec_nodes->accesstype = RELATION_ACCESS_READ;
    exec_nodes->primarynodelist = NIL;
    exec_nodes->nodeList = NIL;
    exec_nodes->en_expr = NULL;
    exec_nodes->en_relid = HeapTupleGetOid(classTuple);

    partForm = (Form_pg_partition)GETSTRUCT(partTuple);
    classForm = (Form_pg_class)GETSTRUCT(classTuple);
    Assert(HeapTupleGetOid(classTuple) == partForm->parentid);

    if (RELPERSISTENCE_TEMP == classForm->relpersistence)
        return 0;

    rel_loc_info = GetRelationLocInfo(partForm->parentid);
    replicated = (rel_loc_info && IsRelationReplicated(rel_loc_info));

    nspname = repairObjectName(get_namespace_name(classForm->relnamespace));
    ;
    relname = repairObjectName(NameStr(classForm->relname));
    partname = repairObjectName(NameStr(partForm->relname));

    initStringInfo(&buf);
    appendStringInfo(&buf,
        "SELECT pg_catalog.%s(p.oid) FROM pg_class c "
        "INNER JOIN pg_namespace n ON n.oid = c.relnamespace "
        "INNER JOIN pg_partition p ON p.parentid = c.oid "
        "WHERE n.nspname = '%s' AND c.relname = '%s' AND p.relname = '%s'",
        funcname,
        nspname,
        relname,
        partname);
    state = RemoteFunctionResultHandler(buf.data, exec_nodes, NULL, true, EXEC_ON_DATANODES, true);
    compute_tuples_stat(state, replicated);
    result = state->result;

    pfree_ext(buf.data);
    pfree_ext(nspname);
    pfree_ext(relname);
    pfree_ext(partname);
    FreeRelationLocInfo(rel_loc_info);
    FreeParallelFunctionState(state);

    return result;
}

Datum pg_stat_get_partition_tuples_changed(PG_FUNCTION_ARGS)
{
    Oid partOid = PG_GETARG_OID(0);
    Oid relid = InvalidOid;
    HeapTuple partTuple = NULL;
    HeapTuple classTuple = NULL;
    Form_pg_partition partForm = NULL;
    PgStat_StatTabKey tabkey;
    PgStat_StatTabEntry* tabentry = NULL;
    int64 result = 0;

    partTuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(partOid));
    if (!HeapTupleIsValid(partTuple)) {
        PG_RETURN_INT64(result);
    }
    partForm = (Form_pg_partition)GETSTRUCT(partTuple);
    relid = partForm->parentid;
    classTuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(classTuple)) {
        ReleaseSysCache(partTuple);
        PG_RETURN_INT64(result);
    }

    if (IS_PGXC_COORDINATOR && GetRelationLocInfo((relid))) {
        result = pgxc_exec_partition_tuples_stat(classTuple, partTuple, "pg_stat_get_partition_tuples_changed");
        ReleaseSysCache(classTuple);
        ReleaseSysCache(partTuple);

        PG_RETURN_INT64(result);
    }

    tabkey.tableid = partOid;
    tabkey.statFlag = relid;
    tabentry = pgstat_fetch_stat_tabentry(&tabkey);

    if (tabentry != NULL)
        result = (int64)(tabentry->changes_since_analyze);

    ReleaseSysCache(classTuple);
    ReleaseSysCache(partTuple);

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_partition_tuples_inserted(PG_FUNCTION_ARGS)
{
    Oid partOid = PG_GETARG_OID(0);
    Oid relid = InvalidOid;
    HeapTuple partTuple = NULL;
    HeapTuple classTuple = NULL;
    Form_pg_partition partForm = NULL;
    PgStat_StatTabKey tabkey;
    PgStat_StatTabEntry* tabentry = NULL;
    int64 result = 0;

    partTuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(partOid));
    if (!HeapTupleIsValid(partTuple)) {
        PG_RETURN_INT64(result);
    }
    partForm = (Form_pg_partition)GETSTRUCT(partTuple);
    relid = partForm->parentid;
    classTuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(classTuple)) {
        ReleaseSysCache(partTuple);
        PG_RETURN_INT64(result);
    }

    if (IS_PGXC_COORDINATOR && GetRelationLocInfo((relid))) {
        result = pgxc_exec_partition_tuples_stat(classTuple, partTuple, "pg_stat_get_partition_tuples_inserted");
        ReleaseSysCache(classTuple);
        ReleaseSysCache(partTuple);

        PG_RETURN_INT64(result);
    }

    tabkey.tableid = partOid;
    tabkey.statFlag = relid;
    tabentry = pgstat_fetch_stat_tabentry(&tabkey);

    if (tabentry != NULL)
        result = (int64)(tabentry->tuples_inserted);

    ReleaseSysCache(classTuple);
    ReleaseSysCache(partTuple);

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_partition_tuples_updated(PG_FUNCTION_ARGS)
{
    Oid partOid = PG_GETARG_OID(0);
    Oid relid = InvalidOid;
    HeapTuple partTuple = NULL;
    HeapTuple classTuple = NULL;
    Form_pg_partition partForm = NULL;
    PgStat_StatTabKey tabkey;
    PgStat_StatTabEntry* tabentry = NULL;
    int64 result = 0;

    partTuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(partOid));
    if (!HeapTupleIsValid(partTuple)) {
        PG_RETURN_INT64(result);
    }
    partForm = (Form_pg_partition)GETSTRUCT(partTuple);
    relid = partForm->parentid;
    classTuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(classTuple)) {
        ReleaseSysCache(partTuple);
        PG_RETURN_INT64(result);
    }

    if (IS_PGXC_COORDINATOR && GetRelationLocInfo((relid))) {
        result = pgxc_exec_partition_tuples_stat(classTuple, partTuple, "pg_stat_get_partition_tuples_updated");
        ReleaseSysCache(classTuple);
        ReleaseSysCache(partTuple);

        PG_RETURN_INT64(result);
    }

    tabkey.tableid = partOid;
    tabkey.statFlag = relid;
    tabentry = pgstat_fetch_stat_tabentry(&tabkey);

    if (tabentry != NULL)
        result = (int64)(tabentry->tuples_updated);

    ReleaseSysCache(classTuple);
    ReleaseSysCache(partTuple);

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_partition_tuples_deleted(PG_FUNCTION_ARGS)
{
    Oid partOid = PG_GETARG_OID(0);
    Oid relid = InvalidOid;
    HeapTuple partTuple = NULL;
    HeapTuple classTuple = NULL;
    Form_pg_partition partForm = NULL;
    PgStat_StatTabKey tabkey;
    PgStat_StatTabEntry* tabentry = NULL;
    int64 result = 0;

    partTuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(partOid));
    if (!HeapTupleIsValid(partTuple)) {
        PG_RETURN_INT64(result);
    }
    partForm = (Form_pg_partition)GETSTRUCT(partTuple);
    relid = partForm->parentid;
    classTuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(classTuple)) {
        ReleaseSysCache(partTuple);
        PG_RETURN_INT64(result);
    }

    if (IS_PGXC_COORDINATOR && GetRelationLocInfo((relid))) {
        result = pgxc_exec_partition_tuples_stat(classTuple, partTuple, "pg_stat_get_partition_tuples_deleted");
        ReleaseSysCache(classTuple);
        ReleaseSysCache(partTuple);

        PG_RETURN_INT64(result);
    }

    tabkey.tableid = partOid;
    tabkey.statFlag = relid;
    tabentry = pgstat_fetch_stat_tabentry(&tabkey);

    if (tabentry != NULL)
        result = (int64)(tabentry->tuples_deleted);

    ReleaseSysCache(classTuple);
    ReleaseSysCache(partTuple);

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_partition_tuples_hot_updated(PG_FUNCTION_ARGS)
{
    Oid partOid = PG_GETARG_OID(0);
    Oid relid = InvalidOid;
    HeapTuple partTuple = NULL;
    HeapTuple classTuple = NULL;
    Form_pg_partition partForm = NULL;
    PgStat_StatTabKey tabkey;
    PgStat_StatTabEntry* tabentry = NULL;
    int64 result = 0;

    partTuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(partOid));
    if (!HeapTupleIsValid(partTuple)) {
        PG_RETURN_INT64(result);
    }
    partForm = (Form_pg_partition)GETSTRUCT(partTuple);
    relid = partForm->parentid;
    classTuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(classTuple)) {
        ReleaseSysCache(partTuple);
        PG_RETURN_INT64(result);
    }

    if (IS_PGXC_COORDINATOR && GetRelationLocInfo((relid))) {
        result = pgxc_exec_partition_tuples_stat(classTuple, partTuple, "pg_stat_get_partition_tuples_hot_updated");
        ReleaseSysCache(classTuple);
        ReleaseSysCache(partTuple);

        PG_RETURN_INT64(result);
    }

    tabkey.tableid = partOid;
    tabkey.statFlag = relid;
    tabentry = pgstat_fetch_stat_tabentry(&tabkey);

    if (tabentry != NULL)
        result = (int64)(tabentry->tuples_hot_updated);

    ReleaseSysCache(classTuple);
    ReleaseSysCache(partTuple);

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_partition_dead_tuples(PG_FUNCTION_ARGS)
{
    Oid partOid = PG_GETARG_OID(0);
    Oid relid = InvalidOid;
    HeapTuple partTuple = NULL;
    HeapTuple classTuple = NULL;
    Form_pg_partition partForm = NULL;
    PgStat_StatTabKey tabkey;
    PgStat_StatTabEntry* tabentry = NULL;
    int64 result = 0;

    partTuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(partOid));
    if (!HeapTupleIsValid(partTuple)) {
        PG_RETURN_INT64(result);
    }
    partForm = (Form_pg_partition)GETSTRUCT(partTuple);
    relid = partForm->parentid;
    classTuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(classTuple)) {
        ReleaseSysCache(partTuple);
        PG_RETURN_INT64(result);
    }

    if (IS_PGXC_COORDINATOR && GetRelationLocInfo((relid))) {
        result = pgxc_exec_partition_tuples_stat(classTuple, partTuple, "pg_stat_get_partition_dead_tuples");
        ReleaseSysCache(classTuple);
        ReleaseSysCache(partTuple);

        PG_RETURN_INT64(result);
    }

    tabkey.tableid = partOid;
    tabkey.statFlag = relid;
    tabentry = pgstat_fetch_stat_tabentry(&tabkey);

    if (tabentry != NULL)
        result = (int64)(tabentry->n_dead_tuples);

    ReleaseSysCache(classTuple);
    ReleaseSysCache(partTuple);

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_partition_live_tuples(PG_FUNCTION_ARGS)
{
    Oid partOid = PG_GETARG_OID(0);
    Oid relid = InvalidOid;
    HeapTuple partTuple = NULL;
    HeapTuple classTuple = NULL;
    Form_pg_partition partForm = NULL;
    PgStat_StatTabKey tabkey;
    PgStat_StatTabEntry* tabentry = NULL;
    int64 result = 0;

    partTuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(partOid));
    if (!HeapTupleIsValid(partTuple)) {
        PG_RETURN_INT64(result);
    }
    partForm = (Form_pg_partition)GETSTRUCT(partTuple);
    relid = partForm->parentid;
    classTuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(classTuple)) {
        ReleaseSysCache(partTuple);
        PG_RETURN_INT64(result);
    }

    if (IS_PGXC_COORDINATOR && GetRelationLocInfo((relid))) {
        result = pgxc_exec_partition_tuples_stat(classTuple, partTuple, "pg_stat_get_partition_live_tuples");
        ReleaseSysCache(classTuple);
        ReleaseSysCache(partTuple);

        PG_RETURN_INT64(result);
    }

    tabkey.tableid = partOid;
    tabkey.statFlag = relid;
    tabentry = pgstat_fetch_stat_tabentry(&tabkey);

    if (tabentry != NULL)
        result = (int64)(tabentry->n_live_tuples);

    ReleaseSysCache(classTuple);
    ReleaseSysCache(partTuple);

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_xact_partition_tuples_inserted(PG_FUNCTION_ARGS)
{
    Oid partOid = PG_GETARG_OID(0);
    Oid relid = InvalidOid;
    HeapTuple partTuple = NULL;
    HeapTuple classTuple = NULL;
    Form_pg_partition partForm = NULL;
    PgStat_TableStatus* tabentry = NULL;
    PgStat_TableXactStatus* trans = NULL;
    int64 result = 0;

    partTuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(partOid));
    if (!HeapTupleIsValid(partTuple)) {
        PG_RETURN_INT64(result);
    }
    partForm = (Form_pg_partition)GETSTRUCT(partTuple);
    relid = partForm->parentid;
    classTuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(classTuple)) {
        ReleaseSysCache(partTuple);
        PG_RETURN_INT64(result);
    }

    if (IS_PGXC_COORDINATOR && GetRelationLocInfo((relid))) {
        result = pgxc_exec_partition_tuples_stat(classTuple, partTuple, "pg_stat_get_xact_partition_tuples_inserted");
        ReleaseSysCache(classTuple);
        ReleaseSysCache(partTuple);

        PG_RETURN_INT64(result);
    }

    tabentry = find_tabstat_entry(partOid, relid);
    if (tabentry != NULL) {
        result += tabentry->t_counts.t_tuples_inserted;

        /* live subtransactions' counts aren't in t_tuples_inserted yet */
        for (trans = tabentry->trans; trans != NULL; trans = trans->upper)
            result += trans->tuples_inserted;
    }

    ReleaseSysCache(classTuple);
    ReleaseSysCache(partTuple);

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_xact_partition_tuples_updated(PG_FUNCTION_ARGS)
{
    Oid partOid = PG_GETARG_OID(0);
    Oid relid = InvalidOid;
    HeapTuple partTuple = NULL;
    HeapTuple classTuple = NULL;
    Form_pg_partition partForm = NULL;
    PgStat_TableStatus* tabentry = NULL;
    PgStat_TableXactStatus* trans = NULL;
    int64 result = 0;

    partTuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(partOid));
    if (!HeapTupleIsValid(partTuple)) {
        PG_RETURN_INT64(result);
    }
    partForm = (Form_pg_partition)GETSTRUCT(partTuple);
    relid = partForm->parentid;
    classTuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(classTuple)) {
        ReleaseSysCache(partTuple);
        PG_RETURN_INT64(result);
    }

    if (IS_PGXC_COORDINATOR && GetRelationLocInfo((relid))) {
        result = pgxc_exec_partition_tuples_stat(classTuple, partTuple, "pg_stat_get_xact_partition_tuples_updated");
        ReleaseSysCache(classTuple);
        ReleaseSysCache(partTuple);

        PG_RETURN_INT64(result);
    }

    tabentry = find_tabstat_entry(partOid, relid);

    if (PointerIsValid(tabentry)) {
        result += tabentry->t_counts.t_tuples_updated;

        /* live subtransactions' counts aren't in t_tuples_updated yet */
        for (trans = tabentry->trans; trans != NULL; trans = trans->upper)
            result += trans->tuples_updated;
    }

    ReleaseSysCache(classTuple);
    ReleaseSysCache(partTuple);

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_xact_partition_tuples_deleted(PG_FUNCTION_ARGS)
{
    Oid partOid = PG_GETARG_OID(0);
    Oid relid = InvalidOid;
    HeapTuple partTuple = NULL;
    HeapTuple classTuple = NULL;
    Form_pg_partition partForm = NULL;
    int64 result = 0;
    PgStat_TableStatus* tabentry = NULL;
    PgStat_TableXactStatus* trans = NULL;

    partTuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(partOid));
    if (!HeapTupleIsValid(partTuple)) {
        PG_RETURN_INT64(result);
    }
    partForm = (Form_pg_partition)GETSTRUCT(partTuple);
    relid = partForm->parentid;
    classTuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(classTuple)) {
        ReleaseSysCache(partTuple);
        PG_RETURN_INT64(result);
    }

    if (IS_PGXC_COORDINATOR && GetRelationLocInfo((relid))) {
        result = pgxc_exec_partition_tuples_stat(classTuple, partTuple, "pg_stat_get_xact_partition_tuples_deleted");
        ReleaseSysCache(classTuple);
        ReleaseSysCache(partTuple);

        PG_RETURN_INT64(result);
    }

    tabentry = find_tabstat_entry(partOid, relid);
    if (PointerIsValid(tabentry)) {
        result += tabentry->t_counts.t_tuples_deleted;

        /* live subtransactions' counts aren't in t_tuples_updated yet */
        for (trans = tabentry->trans; trans != NULL; trans = trans->upper)
            result += trans->tuples_deleted;
    }

    ReleaseSysCache(classTuple);
    ReleaseSysCache(partTuple);

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_xact_partition_tuples_hot_updated(PG_FUNCTION_ARGS)
{
    Oid partOid = PG_GETARG_OID(0);
    Oid relid = InvalidOid;
    HeapTuple partTuple = NULL;
    HeapTuple classTuple = NULL;
    Form_pg_partition partForm = NULL;
    PgStat_TableStatus* tabentry = NULL;
    int64 result = 0;

    partTuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(partOid));
    if (!HeapTupleIsValid(partTuple)) {
        PG_RETURN_INT64(result);
    }

    partForm = (Form_pg_partition)GETSTRUCT(partTuple);
    relid = partForm->parentid;
    classTuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(classTuple)) {
        ReleaseSysCache(partTuple);
        PG_RETURN_INT64(result);
    }

    if (IS_PGXC_COORDINATOR && GetRelationLocInfo((relid))) {
        result =
            pgxc_exec_partition_tuples_stat(classTuple, partTuple, "pg_stat_get_xact_partition_tuples_hot_updated");
        ReleaseSysCache(classTuple);
        ReleaseSysCache(partTuple);

        PG_RETURN_INT64(result);
    }

    tabentry = find_tabstat_entry(partOid, relid);
    if (PointerIsValid(tabentry))
        result += tabentry->t_counts.t_tuples_hot_updated;

    ReleaseSysCache(classTuple);
    ReleaseSysCache(partTuple);

    PG_RETURN_INT64(result);
}

static void StrategyFuncTimeout(ParallelFunctionState* state)
{
    TupleTableSlot* slot = NULL;
    Datum datum = 0;
    int64 result = 0;

    Assert(state && state->tupstore);
    if (NULL == state->tupdesc) {
        /* if there is only one coordiantor,  tupdesc will be null */
        state->result = 0;
    }
    slot = MakeSingleTupleTableSlot(state->tupdesc);

    for (;;) {
        bool isnull = false;

        if (!tuplestore_gettupleslot(state->tupstore, true, false, slot))
            break;

        datum = tableam_tslot_getattr(slot, 1, &isnull);
        if (!isnull)
            result += DatumGetInt64(datum);

        ExecClearTuple(slot);
    }

    state->result = result;
}

static int64 pgxc_exec_autoanalyze_timeout(Oid relOid, int32 coor_idx, char* funcname)
{

    Relation rel = NULL;
    char* relname = NULL;
    char* nspname = NULL;
    ExecNodes* exec_nodes = NULL;
    StringInfoData buf;
    ParallelFunctionState* state = NULL;
    int64 result = 0;

    rel = relation_open(relOid, AccessShareLock);
    exec_nodes = (ExecNodes*)makeNode(ExecNodes);
    exec_nodes->accesstype = RELATION_ACCESS_READ;
    exec_nodes->primarynodelist = NIL;
    exec_nodes->en_expr = NULL;
    exec_nodes->nodeList = list_make1_int(coor_idx);

    relname = repairObjectName(RelationGetRelationName(rel));
    nspname = repairObjectName(get_namespace_name(rel->rd_rel->relnamespace));
    relation_close(rel, AccessShareLock);

    initStringInfo(&buf);
    appendStringInfo(&buf, "SELECT pg_catalog.%s('%s.%s'::regclass)", funcname, nspname, relname);
    state = RemoteFunctionResultHandler(buf.data, exec_nodes, StrategyFuncTimeout, true, EXEC_ON_COORDS, true);
    result = state->result;

    FreeParallelFunctionState(state);
    pfree_ext(relname);
    pfree_ext(nspname);
    pfree_ext(buf.data);

    return result;
}

Datum pg_autovac_timeout(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    HeapTuple tuple = NULL;
    Form_pg_class classForm = NULL;
    int coor_idx;
    int64 result = 0;

    if (!IS_PGXC_COORDINATOR)
        PG_RETURN_NULL();

    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(tuple)) {
        /* if it has been droppped or does not exists, return null */
        PG_RETURN_NULL();
    }

    classForm = (Form_pg_class)GETSTRUCT(tuple);
    if (RELPERSISTENCE_PERMANENT != classForm->relpersistence || RELKIND_RELATION != classForm->relkind) {
        /*  for relation that does not support autovac, just return null */
        ReleaseSysCache(tuple);
        PG_RETURN_NULL();
    }

    ReleaseSysCache(tuple);
    coor_idx = PgxcGetCentralNodeIndex();
    if (-1 == coor_idx) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("Fail to find central coordinator")));
    }

    if (u_sess->pgxc_cxt.PGXCNodeId == coor_idx + 1) { /* for central coordinator */
        /* just do autovac on central coordinator */
        PgStat_StatTabKey tabkey;
        PgStat_StatTabEntry* tabentry = NULL;

        tabkey.statFlag = InvalidOid;
        tabkey.tableid = relid;
        tabentry = pgstat_fetch_stat_tabentry(&tabkey);

        if (tabentry != NULL)
            result = CONTINUED_TIMEOUT_COUNT(tabentry->autovac_status);

        PG_RETURN_INT64(result);
    } else {
        Assert(coor_idx < u_sess->pgxc_cxt.NumCoords);

        /* fetch info from central coordinator */
        PG_RETURN_INT64(pgxc_exec_autoanalyze_timeout(relid, coor_idx, "pg_autovac_timeout"));
    }
}

Datum pg_total_autovac_tuples(PG_FUNCTION_ARGS)
{
    bool for_relation = PG_GETARG_BOOL(0);
    ReturnSetInfo* rsinfo = (ReturnSetInfo*)fcinfo->resultinfo;
    TupleDesc tupdesc = NULL;
    Tuplestorestate* tupstore = NULL;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;

    /* check to see if caller supports us returning a tuplestore */
    if ((rsinfo == NULL) || !IsA(rsinfo, ReturnSetInfo)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("set-valued function called in context that cannot accept a set")));
    }
    if (!(rsinfo->allowedModes & SFRM_Materialize)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("materialize mode required, but it is not allowed in this context")));
    }

#define AUTOVAC_TUPLES_ATTR_NUM 7

    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);
    tupdesc = CreateTemplateTupleDesc(AUTOVAC_TUPLES_ATTR_NUM, false, TAM_HEAP);
    TupleDescInitEntry(tupdesc, (AttrNumber)1, "nodename", NAMEOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)2, "nspname", NAMEOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)3, "relname", NAMEOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)4, "partname", NAMEOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)5, "n_dead_tuples", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)6, "n_live_tuples", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)7, "changes_since_analyze", INT8OID, -1, 0);
    tupstore =
        tuplestore_begin_heap(rsinfo->allowedModes & SFRM_Materialize_Random, false, u_sess->attr.attr_memory.work_mem);
    MemoryContextSwitchTo(oldcontext);

    DEBUG_MOD_START_TIMER(MOD_AUTOVAC);
    if (IS_PGXC_COORDINATOR) {
        ExecNodes* exec_nodes = NULL;
        ParallelFunctionState* state = NULL;
        StringInfoData buf;

        /* fecth all tuples stat info in local database */
        initStringInfo(&buf);
        appendStringInfo(&buf,
            "SELECT nodename,nspname,relname,partname, "
            "n_dead_tuples, n_live_tuples, changes_since_analyze "
            "FROM pg_catalog.pg_total_autovac_tuples(%s)",
            for_relation ? "true" : "false");
        AUTOVAC_LOG(DEBUG2, "FETCH GLOABLE AUTOVAC TUPLES : %s", buf.data);

        /*
         * just return nothing if
         * 1. for_local is true which means just fecth stat info for local autovac
         * 2. local coordinator does not allow to do autovac
         *
         * or we will fecth stat info from datanode
         */
        exec_nodes = (ExecNodes*)makeNode(ExecNodes);
        exec_nodes->accesstype = RELATION_ACCESS_READ;
        exec_nodes->baselocatortype = LOCATOR_TYPE_HASH;
        exec_nodes->nodeList = GetAllDataNodes();

        oldcontext = MemoryContextSwitchTo(per_query_ctx);
        state = RemoteFunctionResultHandler(buf.data, exec_nodes, NULL, true, EXEC_ON_DATANODES, true);
        tupstore = state->tupstore;
        MemoryContextSwitchTo(oldcontext);
        pfree_ext(buf.data);
    } else {
        PgStat_StatDBEntry* dbentry = NULL;
        NameData data_name;

        /*
         * We put all the tuples into a tuplestore in one scan of the hashtable.
         * This avoids any issue of the hashtable possibly changing between calls.
         */
        namestrcpy(&data_name, g_instance.attr.attr_common.PGXCNodeName);
        dbentry = pgstat_fetch_stat_dbentry(u_sess->proc_cxt.MyDatabaseId);
        if ((dbentry != NULL) && (dbentry->tables != NULL)) {
            HASH_SEQ_STATUS hash_seq;
            PgStat_StatTabEntry* tabentry = NULL;

            hash_seq_init(&hash_seq, dbentry->tables);
            while (NULL != (tabentry = (PgStat_StatTabEntry*)hash_seq_search(&hash_seq))) {
                Oid relid = tabentry->tablekey.tableid;
                Oid partid = InvalidOid;
                HeapTuple nsptuple = NULL;
                HeapTuple reltuple = NULL;
                HeapTuple parttuple = NULL;
                Form_pg_namespace nspForm = NULL;
                Form_pg_class relForm = NULL;
                Form_pg_partition partForm = NULL;
                Datum values[AUTOVAC_TUPLES_ATTR_NUM];
                bool nulls[AUTOVAC_TUPLES_ATTR_NUM] = {false};
                bool enable_analyze = false;
                bool enable_vacuum = false;
                bool is_internal_relation = false;
                /*
                 * skip if changes_since_analyze/n_dead_tuples is zero
                 * skip if it is a system catalog
                 */
                if ((0 >= tabentry->n_dead_tuples && 0 >= tabentry->changes_since_analyze) ||
                    tabentry->tablekey.tableid < FirstNormalObjectId)
                    continue;

                if (tabentry->tablekey.statFlag) {
                    if (for_relation) /* just fetch relation's stat info */
                        continue;

                    partid = tabentry->tablekey.tableid;
                    relid = tabentry->tablekey.statFlag;
                }

                reltuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
                if (!HeapTupleIsValid(reltuple))
                    continue;

                relation_support_autoavac(reltuple, &enable_analyze, &enable_vacuum, &is_internal_relation);
                if (!enable_analyze && !enable_vacuum) {
                    ReleaseSysCache(reltuple);
                    continue;
                }
                relForm = (Form_pg_class)GETSTRUCT(reltuple);

                nsptuple = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(relForm->relnamespace));
                if (!HeapTupleIsValid(nsptuple)) {
                    ReleaseSysCache(reltuple);
                    continue;
                }
                nspForm = (Form_pg_namespace)GETSTRUCT(nsptuple);

                if (OidIsValid(partid)) {
                    parttuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(partid));
                    if (!HeapTupleIsValid(parttuple)) {
                        ReleaseSysCache(nsptuple);
                        ReleaseSysCache(reltuple);
                        continue;
                    }
                    partForm = (Form_pg_partition)GETSTRUCT(parttuple);
                }

                errno_t rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
                securec_check(rc, "\0", "\0");
                rc = memset_s(values, sizeof(values), 0, sizeof(values));
                securec_check(rc, "\0", "\0");

                values[0] = NameGetDatum(&data_name);
                values[1] = NameGetDatum(&nspForm->nspname);
                values[2] = NameGetDatum(&relForm->relname);
                if (HeapTupleIsValid(parttuple)) {
                    values[3] = NameGetDatum(&partForm->relname);
                    values[4] = Int64GetDatum(tabentry->n_dead_tuples);
                    values[5] = Int64GetDatum(tabentry->n_live_tuples);
                    values[6] = Int64GetDatum(tabentry->changes_since_analyze);
                } else if (PARTTYPE_PARTITIONED_RELATION == relForm->parttype) {
                    nulls[3] = true;
                    values[4] = DirectFunctionCall1(pg_stat_get_dead_tuples, ObjectIdGetDatum(relid));
                    values[5] = DirectFunctionCall1(pg_stat_get_live_tuples, ObjectIdGetDatum(relid));
                    values[6] = Int64GetDatum(tabentry->changes_since_analyze);
                } else {
                    nulls[3] = true;
                    values[4] = Int64GetDatum(tabentry->n_dead_tuples);
                    values[5] = Int64GetDatum(tabentry->n_live_tuples);
                    values[6] = Int64GetDatum(tabentry->changes_since_analyze);
                }

                tuplestore_putvalues(tupstore, tupdesc, values, nulls);

                if (parttuple)
                    ReleaseSysCache(parttuple);
                ReleaseSysCache(nsptuple);
                ReleaseSysCache(reltuple);
            }
        }
    }
    DEBUG_MOD_STOP_TIMER(MOD_AUTOVAC, "EXECUTE pg_total_autovac_tuples");

    /* clean up and return the tuplestore */
    tuplestore_donestoring(tupstore);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;

    return (Datum)0;
}

Datum pg_autovac_status(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    TupleDesc tupdesc;
    HeapTuple classTuple;
    Form_pg_class classForm;
    AutoVacOpts* relopts = NULL;
    bool force_vacuum = false;
    bool av_enabled = false;
    bool dovacuum = false;
    bool doanalyze = false;
    bool is_internal_relation = false;
    char* nspname = NULL;
    char* relname = NULL;
    int coor_idx = -1;
    bool isNull = false;
    TransactionId relfrozenxid = InvalidTransactionId;
    Relation rel = NULL;
    Datum xid64datum = 0;

    if (!superuser()) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("Only superuser can call function pg_autovac_status.")));
    }

    /* If the node is DN in mpp deployment mode */
    if (IS_PGXC_DATANODE && IS_SINGLE_NODE == false)
        PG_RETURN_NULL();

#define STATUS_ATTR_NUM 9

    Datum values[STATUS_ATTR_NUM];
    bool nulls[STATUS_ATTR_NUM] = {false};

    /* Initialise values and NULL flags arrays */
    errno_t rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    tupdesc = CreateTemplateTupleDesc(STATUS_ATTR_NUM, false, TAM_HEAP);
    TupleDescInitEntry(tupdesc, (AttrNumber)1, "nspname", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)2, "relname", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)3, "nodename", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)4, "doanalyze", BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)5, "anltuples", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)6, "anlthresh", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)7, "dovacuum", BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)8, "vactuples", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)9, "vacthresh", INT8OID, -1, 0);
    BlessTupleDesc(tupdesc);

    classTuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(classTuple))
        PG_RETURN_NULL();

    classForm = (Form_pg_class)GETSTRUCT(classTuple);
    nspname = get_namespace_name(classForm->relnamespace);
    relname = NameStr(classForm->relname);

    if ((relname == NULL) || (nspname == NULL) || (classForm->relkind != RELKIND_RELATION)) {
        ReleaseSysCache(classTuple);
        PG_RETURN_NULL();
    }

    relation_support_autoavac(classTuple, &doanalyze, &dovacuum, &is_internal_relation);

    /* constants from reloptions or GUC variables */
    int64 vac_base_thresh;
    int64 anl_base_thresh;
    float4 vac_scale_factor;
    float4 anl_scale_factor;

    /* thresholds calculated from above constants */
    int64 vacthresh;
    int64 anlthresh;

    /* number of vacuum (resp. analyze) tuples at this time */
    int64 vactuples;
    int64 anltuples;

    /* freeze parameters */
    int freeze_max_age;
    TransactionId xidForceLimit;

    relopts = extract_autovac_opts(classTuple, GetDefaultPgClassDesc());

    /*
     * Determine vacuum/analyze equation parameters.  We have two possible
     * sources: the passed reloptions (which could be a main table or a toast
     * table), or the autovacuum GUC variables.
     */

    /* -1 in autovac setting means use plain vacuum_cost_delay */
    vac_scale_factor = (relopts && relopts->vacuum_scale_factor >= 0) ? relopts->vacuum_scale_factor
                                                                      : u_sess->attr.attr_storage.autovacuum_vac_scale;

    vac_base_thresh = (relopts && relopts->vacuum_threshold >= 0) ? relopts->vacuum_threshold
                                                                  : u_sess->attr.attr_storage.autovacuum_vac_thresh;

    anl_scale_factor = (relopts && relopts->analyze_scale_factor >= 0) ? relopts->analyze_scale_factor
                                                                       : u_sess->attr.attr_storage.autovacuum_anl_scale;

    anl_base_thresh = (relopts && relopts->analyze_threshold >= 0) ? relopts->analyze_threshold
                                                                   : u_sess->attr.attr_storage.autovacuum_anl_thresh;

    freeze_max_age = (relopts && relopts->freeze_max_age >= 0)
                         ? Min(relopts->freeze_max_age, g_instance.attr.attr_storage.autovacuum_freeze_max_age)
                         : g_instance.attr.attr_storage.autovacuum_freeze_max_age;

    av_enabled = (relopts ? relopts->enabled : true);

    /* Force vacuum if table need freeze the old tuple for clog recycle */
    if (t_thrd.autovacuum_cxt.recentXid > FirstNormalTransactionId + freeze_max_age)
        xidForceLimit = t_thrd.autovacuum_cxt.recentXid - freeze_max_age;
    else
        xidForceLimit = FirstNormalTransactionId;

    rel = heap_open(RelationRelationId, AccessShareLock);
    xid64datum = heap_getattr(classTuple, Anum_pg_class_relfrozenxid64, RelationGetDescr(rel), &isNull);
    heap_close(rel, AccessShareLock);
    ReleaseSysCache(classTuple);

    if (isNull) {
        relfrozenxid = classForm->relfrozenxid;
        if (TransactionIdPrecedes(t_thrd.xact_cxt.ShmemVariableCache->nextXid, relfrozenxid) ||
            !TransactionIdIsNormal(relfrozenxid))
            relfrozenxid = FirstNormalTransactionId;
    } else {
        relfrozenxid = DatumGetTransactionId(xid64datum);
    }

    force_vacuum = (TransactionIdIsNormal(relfrozenxid) && TransactionIdPrecedes(relfrozenxid, xidForceLimit));

    vacthresh = (int64)(vac_base_thresh + vac_scale_factor * classForm->reltuples);
    anlthresh = (int64)(anl_base_thresh + anl_scale_factor * classForm->reltuples);
    anltuples = DirectFunctionCall1(pg_stat_get_tuples_changed, ObjectIdGetDatum(relid));
    vactuples = DirectFunctionCall1(pg_stat_get_dead_tuples, ObjectIdGetDatum(relid));

    /* Determine if this table needs analyze */
    if (doanalyze)
        doanalyze = (anltuples > anlthresh);

    /* Determine if this table needs vacuum */
    if (force_vacuum) {
        dovacuum = force_vacuum;
    } else if (dovacuum) {
        dovacuum = (vactuples > vacthresh);
    }

    coor_idx = PgxcGetCentralNodeIndex();
    LWLockAcquire(NodeTableLock, LW_SHARED);
    if (0 <= coor_idx && coor_idx < *t_thrd.pgxc_cxt.shmemNumCoords && IS_SINGLE_NODE == false) {
        /* get central coordinator name since just do autovac on central coordinator */
        values[2] = CStringGetTextDatum(NameStr(t_thrd.pgxc_cxt.coDefs[coor_idx].nodename));
    } else if (IS_SINGLE_NODE == true) {
        if (g_instance.attr.attr_common.PGXCNodeName != NULL)
            values[2] = CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);
        else
            values[2] = CStringGetTextDatum("Error Node");
    } else {
        nulls[2] = true;
    }
    LWLockRelease(NodeTableLock);

    if (nulls[2]) {
        ereport(LOG, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("Fail to find central coordinator")));
    }

    values[0] = CStringGetTextDatum(nspname);
    values[1] = CStringGetTextDatum(relname);
    values[3] = BoolGetDatum(doanalyze);
    values[4] = Int64GetDatum(anltuples);
    values[5] = Int64GetDatum(anlthresh);
    values[6] = BoolGetDatum(dovacuum);
    values[7] = Int64GetDatum(vactuples);
    values[8] = Int64GetDatum(vacthresh);

    /* Returns the record as Datum */
    PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

void check_dirty_percent_and_tuples(int dirty_pecent, int n_tuples)
{
    if (dirty_pecent > 100 || dirty_pecent < 0 || n_tuples < 0)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), (errmsg("out of range."))));
}

void dirty_table_init_tupdesc(TupleDesc* tupdesc)
{
    *tupdesc = CreateTemplateTupleDesc(7, false, TAM_HEAP);
    TupleDescInitEntry(*tupdesc, (AttrNumber)1, "relname", NAMEOID, -1, 0);
    TupleDescInitEntry(*tupdesc, (AttrNumber)2, "schemaname", NAMEOID, -1, 0);
    TupleDescInitEntry(*tupdesc, (AttrNumber)3, "n_tup_ins", INT8OID, -1, 0);
    TupleDescInitEntry(*tupdesc, (AttrNumber)4, "n_tup_upd", INT8OID, -1, 0);
    TupleDescInitEntry(*tupdesc, (AttrNumber)5, "n_tup_del", INT8OID, -1, 0);
    TupleDescInitEntry(*tupdesc, (AttrNumber)6, "n_live_tup", INT8OID, -1, 0);
    TupleDescInitEntry(*tupdesc, (AttrNumber)7, "n_dead_tup", INT8OID, -1, 0);
}

void dirty_table_fill_values(Datum* values, TupleTableSlot* slot, bool* nulls)
{
    values[0] = tableam_tslot_getattr(slot, 1, &nulls[0]);
    values[1] = tableam_tslot_getattr(slot, 2, &nulls[1]);
    values[2] = tableam_tslot_getattr(slot, 3, &nulls[2]);
    values[3] = tableam_tslot_getattr(slot, 4, &nulls[3]);
    values[4] = tableam_tslot_getattr(slot, 5, &nulls[4]);
    values[5] = tableam_tslot_getattr(slot, 6, &nulls[5]);
    values[6] = tableam_tslot_getattr(slot, 7, &nulls[6]);
}

/*
 * @Description: get all table's distribution in all datanode.
 * @return : return the distribution of all the table in current database.
 */
Datum pgxc_stat_all_dirty_tables(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;
    Datum values[7];
    bool nulls[7] = {false, false, false, false};
    HeapTuple tuple = NULL;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext = NULL;
        TupleDesc tupdesc = NULL;
        int dirty_pecent = PG_GETARG_INT32(0);
        int n_tuples = PG_GETARG_INT32(1);
        /* check range */
        check_dirty_percent_and_tuples(dirty_pecent, n_tuples);
        funcctx = SRF_FIRSTCALL_INIT();

        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples. */
        dirty_table_init_tupdesc(&tupdesc);
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->max_calls = u_sess->pgxc_cxt.NumDataNodes;

        /* the main call for get pgxc_stat_all_tables2 */
        funcctx->user_fctx = getTableStat(funcctx->tuple_desc, dirty_pecent, n_tuples, NULL);
        MemoryContextSwitchTo(oldcontext);

        if (funcctx->user_fctx == NULL)
            SRF_RETURN_DONE(funcctx);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->user_fctx) {
        Tuplestorestate* tupstore = ((TableDistributionInfo*)funcctx->user_fctx)->state->tupstore;
        TupleTableSlot* slot = ((TableDistributionInfo*)funcctx->user_fctx)->slot;

        if (!tuplestore_gettupleslot(tupstore, true, false, slot)) {
            FreeParallelFunctionState(((TableDistributionInfo*)funcctx->user_fctx)->state);
            ExecDropSingleTupleTableSlot(slot);
            pfree_ext(funcctx->user_fctx);
            funcctx->user_fctx = NULL;
            SRF_RETURN_DONE(funcctx);
        }
        dirty_table_fill_values(values, slot, nulls);
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        ExecClearTuple(slot);

        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }

    SRF_RETURN_DONE(funcctx);
}

Datum pgxc_stat_schema_dirty_tables(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;
    Datum values[7];
    bool nulls[7] = {false, false, false, false};
    HeapTuple tuple = NULL;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext = NULL;
        TupleDesc tupdesc = NULL;
        int dirty_pecent = PG_GETARG_INT32(0);
        int n_tuples = PG_GETARG_INT32(1);

        /* check range */
        check_dirty_percent_and_tuples(dirty_pecent, n_tuples);

        text* relnamespace_t = PG_GETARG_TEXT_PP(2);
        char* relnamespace = text_to_cstring(relnamespace_t);

        funcctx = SRF_FIRSTCALL_INIT();

        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples. */
        dirty_table_init_tupdesc(&tupdesc);
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->max_calls = u_sess->pgxc_cxt.NumDataNodes;

        /* the main call for get pgxc_stat_all_tables2 */
        funcctx->user_fctx = getTableStat(funcctx->tuple_desc, dirty_pecent, n_tuples, relnamespace);
        PG_FREE_IF_COPY(relnamespace_t, 2);
        pfree(relnamespace);
        MemoryContextSwitchTo(oldcontext);

        if (funcctx->user_fctx == NULL)
            SRF_RETURN_DONE(funcctx);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->user_fctx) {
        Tuplestorestate* tupstore = ((TableDistributionInfo*)funcctx->user_fctx)->state->tupstore;
        TupleTableSlot* slot = ((TableDistributionInfo*)funcctx->user_fctx)->slot;

        if (!tuplestore_gettupleslot(tupstore, true, false, slot)) {
            FreeParallelFunctionState(((TableDistributionInfo*)funcctx->user_fctx)->state);
            ExecDropSingleTupleTableSlot(slot);
            pfree_ext(funcctx->user_fctx);
            funcctx->user_fctx = NULL;
            SRF_RETURN_DONE(funcctx);
        }
        dirty_table_fill_values(values, slot, nulls);
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        ExecClearTuple(slot);

        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }

    SRF_RETURN_DONE(funcctx);
}

/*
 * @Description: get all table's distribution in all datanode.
 * @return : return the distribution of all the table in current database.
 */
Datum all_table_distribution(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;
#ifndef ENABLE_MULTIPLE_NODES
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported view in single node mode.")));

    SRF_RETURN_DONE(funcctx);
#else
    Datum values[4];
    bool nulls[4] = {false, false, false, false};

    /* only system admin can view the global distribution information.*/
    if (!superuser()) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("must be system admin to view the global information"))));
    }

    if (SRF_IS_FIRSTCALL()) {
        funcctx = SRF_FIRSTCALL_INIT();

        MemoryContext oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples. */
        TupleDesc tupdesc = CreateTemplateTupleDesc(4, false, TAM_HEAP);
        TupleDescInitEntry(tupdesc, (AttrNumber)1, "schemaname", NAMEOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "tablename", NAMEOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "nodename", NAMEOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)4, "dnsize", INT8OID, -1, 0);
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->max_calls = u_sess->pgxc_cxt.NumDataNodes;

        /* the main call for get table distribution. */
        funcctx->user_fctx = getTableDataDistribution(funcctx->tuple_desc);

        MemoryContextSwitchTo(oldcontext);

        if (funcctx->user_fctx == NULL)
            SRF_RETURN_DONE(funcctx);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->user_fctx) {
        Tuplestorestate* tupstore = ((TableDistributionInfo*)funcctx->user_fctx)->state->tupstore;
        TupleTableSlot* slot = ((TableDistributionInfo*)funcctx->user_fctx)->slot;

        if (!tuplestore_gettupleslot(tupstore, true, false, slot)) {
            FreeParallelFunctionState(((TableDistributionInfo*)funcctx->user_fctx)->state);
            ExecDropSingleTupleTableSlot(slot);
            pfree_ext(funcctx->user_fctx);
            funcctx->user_fctx = NULL;
            SRF_RETURN_DONE(funcctx);
        }
        values[0] = tableam_tslot_getattr(slot, 1, &nulls[0]);
        values[1] = tableam_tslot_getattr(slot, 2, &nulls[1]);
        values[2] = tableam_tslot_getattr(slot, 3, &nulls[2]);
        values[3] = tableam_tslot_getattr(slot, 4, &nulls[3]);
        HeapTuple tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        ExecClearTuple(slot);

        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }

    SRF_RETURN_DONE(funcctx);
#endif
}

/*
 * @Description: show single table's distribution in all datanode.
 * @in namespace : the table's namespace which need be check.
 * @in tablename : the table's name which need be check.
 * @return : return the distribution of the table.
 */
Datum single_table_distribution(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_MULTIPLE_NODES
    FuncCallContext* funcctx = NULL;
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        errmsg("unsupported view in single node mode.")));

    SRF_RETURN_DONE(funcctx);
#else
    FuncCallContext* funcctx = NULL;
    Datum values[4];
    bool nulls[4] = {false, false, false, false};
    HeapTuple tuple = NULL;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext = NULL;
        TupleDesc tupdesc = NULL;
        text* relnamespace_t = PG_GETARG_TEXT_PP(0);
        text* relname_t = PG_GETARG_TEXT_PP(1);

        char* relnamespace = text_to_cstring(relnamespace_t);
        char* relname = text_to_cstring(relname_t);

        /* Check whether the user have SELECT privilege of the table.*/
        if (!superuser()) {
            Oid tableoid;
            Oid namespaceoid;
            AclResult aclresult;

            namespaceoid = get_namespace_oid(relnamespace, false);
            tableoid = get_relname_relid(relname, namespaceoid);
            if (!OidIsValid(tableoid))
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_TABLE),
                        errmsg("relation \"%s.%s\" does not exist", relnamespace, relname)));

            aclresult = pg_class_aclcheck(tableoid, GetUserId(), ACL_SELECT);
            if (aclresult != ACLCHECK_OK)
                ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("permission denied."))));
        }

        funcctx = SRF_FIRSTCALL_INIT();

        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples. */
        tupdesc = CreateTemplateTupleDesc(4, false, TAM_HEAP);
        TupleDescInitEntry(tupdesc, (AttrNumber)1, "schemaname", NAMEOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "tablename", NAMEOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "nodename", NAMEOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)4, "dnsize", INT8OID, -1, 0);
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->max_calls = u_sess->pgxc_cxt.NumDataNodes;

        /* the main call for get table distribution. */
        funcctx->user_fctx = getTableDataDistribution(funcctx->tuple_desc, relnamespace, relname);
        PG_FREE_IF_COPY(relnamespace_t, 0);
        PG_FREE_IF_COPY(relname_t, 1);
        pfree_ext(relnamespace);
        pfree_ext(relname);

        MemoryContextSwitchTo(oldcontext);

        if (funcctx->user_fctx == NULL)
            SRF_RETURN_DONE(funcctx);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->user_fctx) {
        Tuplestorestate* tupstore = ((TableDistributionInfo*)funcctx->user_fctx)->state->tupstore;
        TupleTableSlot* slot = ((TableDistributionInfo*)funcctx->user_fctx)->slot;

        if (!tuplestore_gettupleslot(tupstore, true, false, slot)) {
            FreeParallelFunctionState(((TableDistributionInfo*)funcctx->user_fctx)->state);
            ExecDropSingleTupleTableSlot(slot);
            pfree_ext(funcctx->user_fctx);
            funcctx->user_fctx = NULL;
            SRF_RETURN_DONE(funcctx);
        }
        values[0] = tableam_tslot_getattr(slot, 1, &nulls[0]);
        values[1] = tableam_tslot_getattr(slot, 2, &nulls[1]);
        values[2] = tableam_tslot_getattr(slot, 3, &nulls[2]);
        values[3] = tableam_tslot_getattr(slot, 4, &nulls[3]);
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        ExecClearTuple(slot);

        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }

    SRF_RETURN_DONE(funcctx);
#endif
}

Datum pg_stat_bad_block(PG_FUNCTION_ARGS)
{
#define BAD_BLOCK_STAT_NATTS 9
    FuncCallContext* funcctx = NULL;
    HASH_SEQ_STATUS* hash_seq = NULL;

    LWLockAcquire(BadBlockStatHashLock, LW_SHARED);

    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tupdesc = NULL;
        MemoryContext oldcontext = NULL;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples */
        tupdesc = CreateTemplateTupleDesc(BAD_BLOCK_STAT_NATTS, false, TAM_HEAP);

        TupleDescInitEntry(tupdesc, (AttrNumber)1, "nodename", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "databaseid", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "tablespaceid", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)4, "relfilenode", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)5, "bucketid", INT2OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)6, "forknum", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)7, "error_count", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)8, "first_time", TIMESTAMPTZOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)9, "last_time", TIMESTAMPTZOID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        if (global_bad_block_stat) {
            hash_seq = (HASH_SEQ_STATUS*)palloc0(sizeof(HASH_SEQ_STATUS));
            hash_seq_init(hash_seq, global_bad_block_stat);
        }

        /* NULL if global_bad_block_stat == NULL */
        funcctx->user_fctx = (void*)hash_seq;

        (void)MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->user_fctx != NULL) {
        hash_seq = (HASH_SEQ_STATUS*)funcctx->user_fctx;
        BadBlockHashEnt* badblock_entry = (BadBlockHashEnt*)hash_seq_search(hash_seq);

        if (badblock_entry != NULL) {
            Datum values[BAD_BLOCK_STAT_NATTS];
            bool nulls[BAD_BLOCK_STAT_NATTS] = {false};
            HeapTuple tuple = NULL;

            errno_t rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
            securec_check(rc, "\0", "\0");

            int i = 0;

            values[i++] = CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);
            values[i++] = UInt32GetDatum(badblock_entry->key.relfilenode.dbNode);
            values[i++] = UInt32GetDatum(badblock_entry->key.relfilenode.spcNode);
            values[i++] = UInt32GetDatum(badblock_entry->key.relfilenode.relNode);
            values[i++] = Int16GetDatum(badblock_entry->key.relfilenode.bucketNode);
            values[i++] = Int32GetDatum(badblock_entry->key.forknum);
            values[i++] = Int32GetDatum(badblock_entry->error_count);
            values[i++] = TimestampTzGetDatum(badblock_entry->first_time);
            values[i++] = TimestampTzGetDatum(badblock_entry->last_time);

            tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);

            LWLockRelease(BadBlockStatHashLock);
            SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
        } else {
            LWLockRelease(BadBlockStatHashLock);
            SRF_RETURN_DONE(funcctx);
        }
    } else {
        LWLockRelease(BadBlockStatHashLock);
        SRF_RETURN_DONE(funcctx);
    }
}

Datum pg_stat_bad_block_clear(PG_FUNCTION_ARGS)
{
    if (!superuser())
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("must be system admin to reset bad block statistics counters")));

    resetBadBlockStat();

    PG_RETURN_VOID();
}

typedef enum FuncName {
    PAGEWRITER_FUNC,
    INCRE_CKPT_FUNC,
    INCRE_BGWRITER_FUNC,
    DW_SINGLE_FUNC,
    DW_BATCH_FUNC,
    CANDIDATE_FUNC
} FuncName;

HeapTuple form_function_tuple(int col_num, FuncName name)
{
    TupleDesc tupdesc = NULL;
    HeapTuple tuple = NULL;
    Datum values[col_num];
    bool nulls[col_num];
    errno_t rc;
    int i;

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), 1, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    tupdesc = CreateTemplateTupleDesc(col_num, false, TAM_HEAP);

    switch (name) {
        case PAGEWRITER_FUNC:
            for (i = 0; i < col_num; i++) {
                TupleDescInitEntry(tupdesc,
                    (AttrNumber)(i + 1),
                    g_pagewriter_view_col[i].name,
                    g_pagewriter_view_col[i].data_type,
                    -1,
                    0);
                values[i] = g_pagewriter_view_col[i].get_val();
                nulls[i] = false;
            }
            break;
        case INCRE_CKPT_FUNC:
            for (i = 0; i < col_num; i++) {
                TupleDescInitEntry(
                    tupdesc, (AttrNumber)(i + 1), g_ckpt_view_col[i].name, g_ckpt_view_col[i].data_type, -1, 0);
                values[i] = g_ckpt_view_col[i].get_val();
                nulls[i] = false;
            }
            break;
        case INCRE_BGWRITER_FUNC:
            for (i = 0; i < col_num; i++) {
                TupleDescInitEntry(
                    tupdesc, (AttrNumber)(i + 1), g_bgwriter_view_col[i].name, g_bgwriter_view_col[i].data_type, -1, 0);
                values[i] = g_bgwriter_view_col[i].get_val();
                nulls[i] = false;
            }
            break;
        case DW_SINGLE_FUNC:
            for (i = 0; i < col_num; i++) {
                TupleDescInitEntry(
                    tupdesc, (AttrNumber)(i + 1), g_dw_single_view[i].name, g_dw_single_view[i].data_type, -1, 0);
                values[i] = g_dw_single_view[i].get_data();
                nulls[i] = false;
            }
            break;
        case DW_BATCH_FUNC:
            for (i = 0; i < col_num; i++) {
                TupleDescInitEntry(
                    tupdesc, (AttrNumber)(i + 1), g_dw_view_col_arr[i].name, g_dw_view_col_arr[i].data_type, -1, 0);
                values[i] = g_dw_view_col_arr[i].get_data();
                nulls[i] = false;
            }
            break;
        case CANDIDATE_FUNC:
            for (i = 0; i < col_num; i++) {
                TupleDescInitEntry(
                    tupdesc, (AttrNumber)(i + 1), g_pagewirter_view_two_col[i].name,
                    g_pagewirter_view_two_col[i].data_type, -1, 0);
                values[i] = g_pagewirter_view_two_col[i].get_val();
                nulls[i] = false;
            }
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("unknow func name")));
            break;
    }

    tupdesc = BlessTupleDesc(tupdesc);
    tuple = heap_form_tuple(tupdesc, values, nulls);
    return tuple;
}

Datum local_pagewriter_stat(PG_FUNCTION_ARGS)
{
    HeapTuple tuple = form_function_tuple(PAGEWRITER_VIEW_COL_NUM, PAGEWRITER_FUNC);
    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

Datum local_ckpt_stat(PG_FUNCTION_ARGS)
{
    HeapTuple tuple = form_function_tuple(INCRE_CKPT_VIEW_COL_NUM, INCRE_CKPT_FUNC);
    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

Datum local_bgwriter_stat(PG_FUNCTION_ARGS)
{
    HeapTuple tuple = form_function_tuple(INCRE_CKPT_BGWRITER_VIEW_COL_NUM, INCRE_BGWRITER_FUNC);
    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

Datum local_candidate_stat(PG_FUNCTION_ARGS)
{
    HeapTuple tuple = form_function_tuple(CANDIDATE_VIEW_COL_NUM, CANDIDATE_FUNC);
    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

void xc_stat_view(FuncCallContext* funcctx, int col_num, FuncName name)
{
    MemoryContext oldcontext = NULL;
    TupleDesc tupdesc = NULL;
    int i;

    oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

    /* build tupdesc for result tuples. */
    tupdesc = CreateTemplateTupleDesc(col_num, false, TAM_HEAP);
    switch (name) {
        case PAGEWRITER_FUNC:
            for (i = 0; i < col_num; i++) {
                TupleDescInitEntry(tupdesc, (AttrNumber)(i + 1), g_pagewriter_view_col[i].name,
                    g_pagewriter_view_col[i].data_type, -1, 0);
            }
            break;
        case INCRE_CKPT_FUNC:
            for (i = 0; i < col_num; i++) {
                TupleDescInitEntry(
                    tupdesc, (AttrNumber)(i + 1), g_ckpt_view_col[i].name, g_ckpt_view_col[i].data_type, -1, 0);
            }
            break;
        case INCRE_BGWRITER_FUNC:
            for (i = 0; i < col_num; i++) {
                TupleDescInitEntry(
                    tupdesc, (AttrNumber)(i + 1), g_bgwriter_view_col[i].name, g_bgwriter_view_col[i].data_type, -1, 0);
            }
            break;
        case CANDIDATE_FUNC:
            for (i = 0; i < col_num; i++) {
                TupleDescInitEntry(
                    tupdesc, (AttrNumber)(i + 1), g_pagewirter_view_two_col[i].name,
                    g_pagewirter_view_two_col[i].data_type, -1, 0);
            }
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("unknow func name")));
            break;
    }

    funcctx->tuple_desc = BlessTupleDesc(tupdesc);
    funcctx->max_calls = u_sess->pgxc_cxt.NumDataNodes + u_sess->pgxc_cxt.NumCoords;

    switch (name) {
        case PAGEWRITER_FUNC:
            funcctx->user_fctx = get_remote_stat_pagewriter(funcctx->tuple_desc);
            break;
        case INCRE_CKPT_FUNC:
            funcctx->user_fctx = get_remote_stat_ckpt(funcctx->tuple_desc);
            break;
        case INCRE_BGWRITER_FUNC:
            funcctx->user_fctx = get_remote_stat_bgwriter(funcctx->tuple_desc);
            break;
        case CANDIDATE_FUNC:
            funcctx->user_fctx = get_remote_stat_candidate(funcctx->tuple_desc);
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("unknow func name")));
            break;
    }

    MemoryContextSwitchTo(oldcontext);
    return;
}

Datum remote_pagewriter_stat(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_MULTIPLE_NODES
    FuncCallContext* funcctx = NULL;
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        errmsg("unsupported view in single node mode.")));

    SRF_RETURN_DONE(funcctx);
#else
    FuncCallContext* funcctx = NULL;
    HeapTuple tuple = NULL;
    Datum values[PAGEWRITER_VIEW_COL_NUM];
    bool nulls[PAGEWRITER_VIEW_COL_NUM] = {false};
    int i;
    errno_t rc;

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), 1, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    if (SRF_IS_FIRSTCALL()) {
        funcctx = SRF_FIRSTCALL_INIT();
        xc_stat_view(funcctx, PAGEWRITER_VIEW_COL_NUM, PAGEWRITER_FUNC);

        if (funcctx->user_fctx == NULL) {
            SRF_RETURN_DONE(funcctx);
        }
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->user_fctx) {
        Tuplestorestate* tupstore = ((TableDistributionInfo*)funcctx->user_fctx)->state->tupstore;
        TupleTableSlot* slot = ((TableDistributionInfo*)funcctx->user_fctx)->slot;

        if (!tuplestore_gettupleslot(tupstore, true, false, slot)) {
            FreeParallelFunctionState(((TableDistributionInfo*)funcctx->user_fctx)->state);
            ExecDropSingleTupleTableSlot(slot);
            pfree_ext(funcctx->user_fctx);
            funcctx->user_fctx = NULL;
            SRF_RETURN_DONE(funcctx);
        }
        for (i = 0; i < PAGEWRITER_VIEW_COL_NUM; i++) {
            values[i] = tableam_tslot_getattr(slot, (i + 1), &nulls[i]);
        }
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        ExecClearTuple(slot);

        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }

    SRF_RETURN_DONE(funcctx);
#endif
}

Datum remote_ckpt_stat(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_MULTIPLE_NODES
    FuncCallContext* funcctx = NULL;
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        errmsg("unsupported view in single node mode.")));

    SRF_RETURN_DONE(funcctx);
#else
    FuncCallContext* funcctx = NULL;
    HeapTuple tuple = NULL;
    Datum values[INCRE_CKPT_VIEW_COL_NUM];
    bool nulls[INCRE_CKPT_VIEW_COL_NUM] = {false};
    int i;
    errno_t rc;

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), 1, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    if (SRF_IS_FIRSTCALL()) {
        funcctx = SRF_FIRSTCALL_INIT();
        xc_stat_view(funcctx, INCRE_CKPT_VIEW_COL_NUM, INCRE_CKPT_FUNC);
        if (funcctx->user_fctx == NULL) {
            SRF_RETURN_DONE(funcctx);
        }
    }
    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->user_fctx) {
        Tuplestorestate* tupstore = ((TableDistributionInfo*)funcctx->user_fctx)->state->tupstore;
        TupleTableSlot* slot = ((TableDistributionInfo*)funcctx->user_fctx)->slot;

        if (!tuplestore_gettupleslot(tupstore, true, false, slot)) {
            FreeParallelFunctionState(((TableDistributionInfo*)funcctx->user_fctx)->state);
            ExecDropSingleTupleTableSlot(slot);
            pfree_ext(funcctx->user_fctx);
            funcctx->user_fctx = NULL;
            SRF_RETURN_DONE(funcctx);
        }
        for (i = 0; i < INCRE_CKPT_VIEW_COL_NUM; i++) {
            values[i] = tableam_tslot_getattr(slot, (i + 1), &nulls[i]);
        }
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        ExecClearTuple(slot);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }

    SRF_RETURN_DONE(funcctx);
#endif
}

Datum remote_bgwriter_stat(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_MULTIPLE_NODES
    FuncCallContext* funcctx = NULL;
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        errmsg("unsupported view in single node mode.")));

    SRF_RETURN_DONE(funcctx);
#else
    FuncCallContext* funcctx = NULL;
    HeapTuple tuple = NULL;
    Datum values[INCRE_CKPT_BGWRITER_VIEW_COL_NUM];
    bool nulls[INCRE_CKPT_BGWRITER_VIEW_COL_NUM] = {false};
    int i;
    errno_t rc;

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), 1, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    if (SRF_IS_FIRSTCALL()) {
        funcctx = SRF_FIRSTCALL_INIT();
        xc_stat_view(funcctx, INCRE_CKPT_BGWRITER_VIEW_COL_NUM, INCRE_BGWRITER_FUNC);

        if (funcctx->user_fctx == NULL) {
            SRF_RETURN_DONE(funcctx);
        }
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->user_fctx) {
        Tuplestorestate* tupstore = ((TableDistributionInfo*)funcctx->user_fctx)->state->tupstore;
        TupleTableSlot* slot = ((TableDistributionInfo*)funcctx->user_fctx)->slot;

        if (!tuplestore_gettupleslot(tupstore, true, false, slot)) {
            FreeParallelFunctionState(((TableDistributionInfo*)funcctx->user_fctx)->state);
            ExecDropSingleTupleTableSlot(slot);
            pfree_ext(funcctx->user_fctx);
            SRF_RETURN_DONE(funcctx);
        }
        for (i = 0; i < INCRE_CKPT_BGWRITER_VIEW_COL_NUM; i++) {
            values[i] = tableam_tslot_getattr(slot, (i + 1), &nulls[i]);
        }
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        (void)ExecClearTuple(slot);

        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }

    SRF_RETURN_DONE(funcctx);
#endif
}

Datum remote_candidate_stat(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_MULTIPLE_NODES
    FuncCallContext* funcctx = NULL;
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        errmsg("unsupported view in single node mode.")));

    SRF_RETURN_DONE(funcctx);
#else
    FuncCallContext* funcctx = NULL;
    HeapTuple tuple = NULL;
    Datum values[CANDIDATE_VIEW_COL_NUM];
    bool nulls[CANDIDATE_VIEW_COL_NUM] = {false};
    int i;
    errno_t rc;

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), 1, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    if (SRF_IS_FIRSTCALL()) {
        funcctx = SRF_FIRSTCALL_INIT();
        xc_stat_view(funcctx, CANDIDATE_VIEW_COL_NUM, CANDIDATE_FUNC);

        if (funcctx->user_fctx == NULL) {
            SRF_RETURN_DONE(funcctx);
        }
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->user_fctx) {
        Tuplestorestate* tupstore = ((TableDistributionInfo*)funcctx->user_fctx)->state->tupstore;
        TupleTableSlot* slot = ((TableDistributionInfo*)funcctx->user_fctx)->slot;

        if (!tuplestore_gettupleslot(tupstore, true, false, slot)) {
            FreeParallelFunctionState(((TableDistributionInfo*)funcctx->user_fctx)->state);
            ExecDropSingleTupleTableSlot(slot);
            pfree_ext(funcctx->user_fctx);
            funcctx->user_fctx = NULL;
            SRF_RETURN_DONE(funcctx);
        }
        for (i = 0; i < CANDIDATE_VIEW_COL_NUM; i++) {
            values[i] = tableam_tslot_getattr(slot, (i + 1), &nulls[i]);
        }
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        ExecClearTuple(slot);

        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }

    SRF_RETURN_DONE(funcctx);
#endif
}


Datum local_single_flush_dw_stat(PG_FUNCTION_ARGS)
{
    HeapTuple tuple = form_function_tuple(DW_SINGLE_VIEW_COL_NUM, DW_SINGLE_FUNC);
    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

Datum remote_single_flush_dw_stat(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;
#ifndef ENABLE_MULTIPLE_NODES
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported view in single node mode.")));

    SRF_RETURN_DONE(funcctx);
#else
    Datum values[DW_SINGLE_VIEW_COL_NUM];
    bool nulls[DW_SINGLE_VIEW_COL_NUM] = {false};

    if (SRF_IS_FIRSTCALL()) {
        funcctx = SRF_FIRSTCALL_INIT();

        MemoryContext oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples. */
        TupleDesc tupdesc = CreateTemplateTupleDesc(DW_SINGLE_VIEW_COL_NUM, false, TAM_HEAP);
        for (uint32 i = 0; i < DW_SINGLE_VIEW_COL_NUM; i++) {
            TupleDescInitEntry(
                tupdesc, (AttrNumber)(i + 1), g_dw_single_view[i].name, g_dw_single_view[i].data_type, -1, 0);
            nulls[i] = false;
        }

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->max_calls = u_sess->pgxc_cxt.NumDataNodes + u_sess->pgxc_cxt.NumCoords;

        funcctx->user_fctx = get_remote_single_flush_dw_stat(funcctx->tuple_desc);
        MemoryContextSwitchTo(oldcontext);

        if (funcctx->user_fctx == NULL) {
            SRF_RETURN_DONE(funcctx);
        }
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->user_fctx) {
        Tuplestorestate* tupstore = ((TableDistributionInfo*)funcctx->user_fctx)->state->tupstore;
        TupleTableSlot* slot = ((TableDistributionInfo*)funcctx->user_fctx)->slot;

        if (!tuplestore_gettupleslot(tupstore, true, false, slot)) {
            FreeParallelFunctionState(((TableDistributionInfo*)funcctx->user_fctx)->state);
            ExecDropSingleTupleTableSlot(slot);
            pfree_ext(funcctx->user_fctx);
            funcctx->user_fctx = NULL;
            SRF_RETURN_DONE(funcctx);
        }
        for (uint32 i = 0; i < DW_SINGLE_VIEW_COL_NUM; i++) {
            values[i] = tableam_tslot_getattr(slot, (i + 1), &nulls[i]);
        }
        HeapTuple tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        ExecClearTuple(slot);

        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }

    SRF_RETURN_DONE(funcctx);
#endif
}

static void gs_stat_read_dw_batch(Tuplestorestate *tupStore, TupleDesc tupDesc)
{

    int i, j;
    errno_t rc = 0;
    int row_num = g_instance.dw_batch_cxt.batch_meta_file.dw_file_num;
    int col_num = DW_VIEW_COL_NUM;

    Datum values[col_num];
    bool nulls[col_num];

    for (i = 0; i < row_num; i++) {
        g_stat_file_id = i;
	
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 1, sizeof(nulls));
        securec_check(rc, "\0", "\0");
	
        for (j = 0; j < col_num; j++) {
            values[j] = g_dw_view_col_arr[j].get_data();
            nulls[j] = false;
        }
		
        tuplestore_putvalues(tupStore, tupDesc, values, nulls);
    }
	
    g_stat_file_id = -1;
}

Datum local_double_write_stat(PG_FUNCTION_ARGS)
{
    int i;
    int col_num = DW_VIEW_COL_NUM;
    TupleDesc tupdesc = NULL;

    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	
    MemoryContext oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
	
    tupdesc = CreateTemplateTupleDesc(DW_VIEW_COL_NUM, false, TAM_HEAP);

    for (i = 0; i < col_num; i++) {
        TupleDescInitEntry(tupdesc, (AttrNumber)(i + 1),
            g_dw_view_col_arr[i].name, g_dw_view_col_arr[i].data_type, -1, 0);
    }

    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->setDesc = BlessTupleDesc(tupdesc);

    (void)gs_stat_read_dw_batch(rsinfo->setResult, rsinfo->setDesc);
	
    MemoryContextSwitchTo(oldcontext);
	
    tuplestore_donestoring(rsinfo->setResult);
	
    return (Datum) 0;
}

Datum remote_double_write_stat(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;
#ifndef ENABLE_MULTIPLE_NODES
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported view in single node mode.")));

    SRF_RETURN_DONE(funcctx);
#else
    Datum values[DW_VIEW_COL_NUM];
    bool nulls[DW_VIEW_COL_NUM] = {false};

    if (SRF_IS_FIRSTCALL()) {
        funcctx = SRF_FIRSTCALL_INIT();

        MemoryContext oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples. */
        TupleDesc tupdesc = CreateTemplateTupleDesc(DW_VIEW_COL_NUM, false, TAM_HEAP);
        for (uint32 i = 0; i < DW_VIEW_COL_NUM; i++) {
            TupleDescInitEntry(
                tupdesc, (AttrNumber)(i + 1), g_dw_view_col_arr[i].name, g_dw_view_col_arr[i].data_type, -1, 0);
            nulls[i] = false;
        }

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->max_calls = u_sess->pgxc_cxt.NumDataNodes + u_sess->pgxc_cxt.NumCoords;

        funcctx->user_fctx = get_remote_stat_double_write(funcctx->tuple_desc);
        MemoryContextSwitchTo(oldcontext);

        if (funcctx->user_fctx == NULL) {
            SRF_RETURN_DONE(funcctx);
        }
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->user_fctx) {
        Tuplestorestate* tupstore = ((TableDistributionInfo*)funcctx->user_fctx)->state->tupstore;
        TupleTableSlot* slot = ((TableDistributionInfo*)funcctx->user_fctx)->slot;

        if (!tuplestore_gettupleslot(tupstore, true, false, slot)) {
            FreeParallelFunctionState(((TableDistributionInfo*)funcctx->user_fctx)->state);
            ExecDropSingleTupleTableSlot(slot);
            pfree_ext(funcctx->user_fctx);
            funcctx->user_fctx = NULL;
            SRF_RETURN_DONE(funcctx);
        }
        for (uint32 i = 0; i < DW_VIEW_COL_NUM; i++) {
            values[i] = tableam_tslot_getattr(slot, (i + 1), &nulls[i]);
        }
        HeapTuple tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        ExecClearTuple(slot);

        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }

    SRF_RETURN_DONE(funcctx);
#endif
}

Datum local_redo_time_count(PG_FUNCTION_ARGS)
{
    TupleDesc tupdesc;
    Tuplestorestate *tupstore = BuildTupleResult(fcinfo, &tupdesc);
    const uint32 xlog_redo_static_cols = 21;
    Datum values[xlog_redo_static_cols];
    bool nulls[xlog_redo_static_cols];

    RedoWorkerTimeCountsInfo *workerCountInfoList = NULL;
    uint32 realNum = 0;
    GetRedoWorkerTimeCount(&workerCountInfoList, &realNum);

    for (uint32 i = 0; i < realNum; ++i) {
        uint32 k = 0;
        values[k] = CStringGetTextDatum(workerCountInfoList[i].worker_name);
        nulls[k++] = false;
        pfree(workerCountInfoList[i].worker_name);

        for (uint32 j = 0; j < (uint32)TIME_COST_NUM; ++j) {
            values[k] = Int64GetDatum(workerCountInfoList[i].time_cost[j].totalDuration);
            nulls[k++] = false;
            values[k] = Int64GetDatum(workerCountInfoList[i].time_cost[j].counter);
            nulls[k++] = false;
        }
        tuplestore_putvalues(tupstore, tupdesc, values, nulls);
    }
    if (workerCountInfoList != NULL) {
        pfree(workerCountInfoList);
    }
    tuplestore_donestoring(tupstore);
    return (Datum)0;
}

Datum local_xlog_redo_statics(PG_FUNCTION_ARGS)
{
    TupleDesc tupdesc;
    Tuplestorestate *tupstore = BuildTupleResult(fcinfo, &tupdesc);
    const uint32 xlog_redo_static_cols = 5;
    Datum values[xlog_redo_static_cols];
    bool nulls[xlog_redo_static_cols];
    const uint32 subtypeShiftSize = 4;
    for (uint32 i = 0; i < RM_NEXT_ID; ++i) {
        for (uint32 j = 0; j < MAX_XLOG_INFO_NUM; ++j) {
            if (g_instance.comm_cxt.predo_cxt.xlogStatics[i][j].total_num == 0) {
                continue;
            }
            uint32 k = 0;
            values[k] = CStringGetTextDatum(RmgrTable[i].rm_type_name((j << subtypeShiftSize)));
            nulls[k++] = false;
            values[k] = Int32GetDatum(i);
            nulls[k++] = false;
            values[k] = Int32GetDatum(j << subtypeShiftSize);
            nulls[k++] = false;
            values[k] = Int64GetDatum(g_instance.comm_cxt.predo_cxt.xlogStatics[i][j].total_num);
            nulls[k++] = false;
            values[k] = Int64GetDatum(g_instance.comm_cxt.predo_cxt.xlogStatics[i][j].extra_num);
            nulls[k++] = false;
            tuplestore_putvalues(tupstore, tupdesc, values, nulls);
        }
    }

    tuplestore_donestoring(tupstore);

    return (Datum)0;
}

Datum local_redo_stat(PG_FUNCTION_ARGS)
{
    TupleDesc tupdesc = NULL;
    HeapTuple tuple = NULL;
    Datum values[REDO_VIEW_COL_SIZE];
    bool nulls[REDO_VIEW_COL_SIZE] = {false};
    uint32 i;

    redo_fill_redo_event();
    tupdesc = CreateTemplateTupleDesc(REDO_VIEW_COL_SIZE, false, TAM_HEAP);
    for (i = 0; i < REDO_VIEW_COL_SIZE; i++) {
        TupleDescInitEntry(tupdesc, (AttrNumber)(i + 1), g_redoViewArr[i].name, g_redoViewArr[i].data_type, -1, 0);
        values[i] = g_redoViewArr[i].get_data();
        nulls[i] = false;
    }

    tupdesc = BlessTupleDesc(tupdesc);
    tuple = heap_form_tuple(tupdesc, values, nulls);
    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

Datum remote_redo_stat(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;
#ifndef ENABLE_MULTIPLE_NODES
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported view in single node mode.")));

    SRF_RETURN_DONE(funcctx);
#else
    Datum values[REDO_VIEW_COL_SIZE];
    bool nulls[REDO_VIEW_COL_SIZE] = {false};

    if (SRF_IS_FIRSTCALL()) {
        funcctx = SRF_FIRSTCALL_INIT();

        MemoryContext oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples. */
        TupleDesc tupdesc = CreateTemplateTupleDesc(REDO_VIEW_COL_SIZE, false, TAM_HEAP);
        for (uint32 i = 0; i < REDO_VIEW_COL_SIZE; i++) {
            TupleDescInitEntry(tupdesc, (AttrNumber)(i + 1), g_redoViewArr[i].name, g_redoViewArr[i].data_type, -1, 0);
            nulls[i] = false;
        }

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->max_calls = u_sess->pgxc_cxt.NumDataNodes + u_sess->pgxc_cxt.NumCoords;

        /* the main call for get local_redo_stat */
        funcctx->user_fctx = get_remote_stat_redo(funcctx->tuple_desc);
        MemoryContextSwitchTo(oldcontext);

        if (funcctx->user_fctx == NULL) {
            SRF_RETURN_DONE(funcctx);
        }
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->user_fctx) {
        Tuplestorestate* tupstore = ((TableDistributionInfo*)funcctx->user_fctx)->state->tupstore;
        TupleTableSlot* slot = ((TableDistributionInfo*)funcctx->user_fctx)->slot;

        if (!tuplestore_gettupleslot(tupstore, true, false, slot)) {
            FreeParallelFunctionState(((TableDistributionInfo*)funcctx->user_fctx)->state);
            ExecDropSingleTupleTableSlot(slot);
            pfree_ext(funcctx->user_fctx);
            funcctx->user_fctx = NULL;
            SRF_RETURN_DONE(funcctx);
        }
        for (uint32 i = 0; i < REDO_VIEW_COL_SIZE; i++) {
            values[i] = tableam_tslot_getattr(slot, (i + 1), &nulls[i]);
        }
        HeapTuple tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        ExecClearTuple(slot);

        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }
    SRF_RETURN_DONE(funcctx);
#endif
}

/*
 * @@GaussDB@@
 * Brief		: Get the thread pool info
 * Description	:
 * Notes		:
 */
Datum gs_threadpool_status(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;
    ThreadPoolStat* entry = NULL;
    MemoryContext oldcontext = NULL;

    /* stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tupdesc = NULL;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function
         * calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* need a tuple descriptor representing 10 columns */
        tupdesc = CreateTemplateTupleDesc(NUM_THREADPOOL_STATUS_ELEM, false, TAM_HEAP);

        TupleDescInitEntry(tupdesc, (AttrNumber)1, "nodename", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "groupid", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "bindnumanum", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)4, "bindcpunum", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)5, "listenernum", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)6, "workerinfo", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)7, "sessioninfo", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)8, "streaminfo", TEXTOID, -1, 0);

        /* complete descriptor of the tupledesc */
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        /* total number of tuples to be returned */
        if (ENABLE_THREAD_POOL) {
            funcctx->user_fctx = (void*)g_threadPoolControler->GetThreadPoolStat(&(funcctx->max_calls));
        } else {
            funcctx->max_calls = 0;
        }

        (void)MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();
    entry = (ThreadPoolStat*)funcctx->user_fctx;

    if (funcctx->call_cntr < funcctx->max_calls) { /* do when there is more left to send */
        Datum values[NUM_THREADPOOL_STATUS_ELEM];
        bool nulls[NUM_THREADPOOL_STATUS_ELEM] = {false};
        HeapTuple tuple = NULL;

        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        entry += funcctx->call_cntr;

        values[0] = CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);
        values[1] = Int32GetDatum(entry->groupId);
        values[2] = Int32GetDatum(entry->numaId);
        values[3] = Int32GetDatum(entry->bindCpuNum);
        values[4] = Int32GetDatum(entry->listenerNum);
        values[5] = CStringGetTextDatum(entry->workerInfo);
        values[6] = CStringGetTextDatum(entry->sessionInfo);
        values[7] = CStringGetTextDatum(entry->streamInfo);

        if (entry->numaId == -1) {
            nulls[2] = true;
        }

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    } else {
        /* do when there is no more left */
        SRF_RETURN_DONE(funcctx);
    }
}

Datum gs_globalplancache_status(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx = NULL;
    MemoryContext oldcontext;

    /* stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL())
    {
        TupleDesc tupdesc;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
        * switch to memory context appropriate for multiple function
        * calls
        */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

#define GPC_TUPLES_ATTR_NUM 8

        /* need a tuple descriptor representing 12 columns */
        tupdesc = CreateTemplateTupleDesc(GPC_TUPLES_ATTR_NUM, false, TAM_HEAP);

        TupleDescInitEntry(tupdesc, (AttrNumber) 1, "nodename",
                           TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 2, "query",
                           TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 3, "refcount",
                           INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 4, "valid",
                           BOOLOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 5, "databaseid",
                           OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 6, "schema_name",
                           TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 7, "params_num",
                           INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 8, "func_id",
                           OIDOID, -1, 0);

        /* complete descriptor of the tupledesc */
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        /* total number of tuples to be returned */
        if (ENABLE_THREAD_POOL && ENABLE_GPC) {
            funcctx->user_fctx = (void *)g_instance.plan_cache->GetStatus(&(funcctx->max_calls));
        } else {
            funcctx->max_calls = 0;
        }

        (void)MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();
    GPCViewStatus *entry = (GPCViewStatus *)funcctx->user_fctx;

    if (funcctx->call_cntr < funcctx->max_calls)	 { /* do when there is more left to send */
        Datum values[GPC_TUPLES_ATTR_NUM];
        bool nulls[GPC_TUPLES_ATTR_NUM];
        HeapTuple tuple;

        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        entry += funcctx->call_cntr;

        values[0] = CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);
        values[1] = CStringGetTextDatum(entry->query);
        values[2] = Int32GetDatum(entry->refcount);
        values[3] = BoolGetDatum(entry->valid);
        values[4] = DatumGetObjectId(entry->DatabaseID);
        values[5] = CStringGetTextDatum(entry->schema_name);
        values[6] = Int32GetDatum(entry->params_num);
        values[7] = DatumGetObjectId(entry->func_id);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }
    else
    {
        /* do when there is no more left */
        SRF_RETURN_DONE(funcctx);
    }
}

Datum
gs_globalplancache_prepare_status(PG_FUNCTION_ARGS)
{
    /* can only called in distribute cluster */
#ifndef ENABLE_MULTIPLE_NODES
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
#endif

    FuncCallContext *funcctx = NULL;
    MemoryContext oldcontext;

    /* stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL())
    {
        TupleDesc tupdesc;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
        * switch to memory context appropriate for multiple function
        * calls
        */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

#define GPC_PREPARE_TUPLES_ATTR_NUM 8

        /* need a tuple descriptor representing 12 columns */
        tupdesc = CreateTemplateTupleDesc(GPC_PREPARE_TUPLES_ATTR_NUM, false);

        TupleDescInitEntry(tupdesc, (AttrNumber) 1, "nodename",
                           TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 2, "cn_sess_id",
                           INT8OID, -1, 0);

        TupleDescInitEntry(tupdesc, (AttrNumber) 3, "cn_node_id",
                           INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 4, "cn_time_line",
                           INT4OID, -1, 0);

        TupleDescInitEntry(tupdesc, (AttrNumber) 5, "statement_name",
                           TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 6, "refcount",
                           INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 7, "is_shared",
                           BOOLOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 8, "query",
                           TEXTOID, -1, 0);

        /* complete descriptor of the tupledesc */
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        /* total number of tuples to be returned */
        funcctx->max_calls = 0;

        (void)MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();
    /* do when there is no more left */
    SRF_RETURN_DONE(funcctx);
}

Datum local_rto_stat(PG_FUNCTION_ARGS)
{
    TupleDesc tupdesc = NULL;
    HeapTuple tuple = NULL;
    Datum values[RTO_VIEW_COL_SIZE];
    bool nulls[RTO_VIEW_COL_SIZE] = {false};
    uint32 i;

    tupdesc = CreateTemplateTupleDesc(RTO_VIEW_COL_SIZE, false, TAM_HEAP);
    for (i = 0; i < RTO_VIEW_COL_SIZE; i++) {
        TupleDescInitEntry(tupdesc, (AttrNumber)(i + 1), g_rtoViewArr[i].name, g_rtoViewArr[i].data_type, -1, 0);
        values[i] = g_rtoViewArr[i].get_data();
        nulls[i] = false;
    }

    tupdesc = BlessTupleDesc(tupdesc);
    tuple = heap_form_tuple(tupdesc, values, nulls);
    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

Datum remote_rto_stat(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;
#ifndef ENABLE_MULTIPLE_NODES
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported view in single node mode.")));

    SRF_RETURN_DONE(funcctx);
#else
    Datum values[RTO_VIEW_COL_SIZE];
    bool nulls[RTO_VIEW_COL_SIZE] = {false};
    uint32 i;

    if (SRF_IS_FIRSTCALL()) {
        funcctx = SRF_FIRSTCALL_INIT();

        MemoryContext oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples. */
        TupleDesc tupdesc = CreateTemplateTupleDesc(RTO_VIEW_COL_SIZE, false, TAM_HEAP);
        for (i = 0; i < RTO_VIEW_COL_SIZE; i++) {
            TupleDescInitEntry(tupdesc, (AttrNumber)(i + 1), g_rtoViewArr[i].name, g_rtoViewArr[i].data_type, -1, 0);
            nulls[i] = false;
        }

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->max_calls = u_sess->pgxc_cxt.NumDataNodes + u_sess->pgxc_cxt.NumCoords;

        /* the main call for get get_rto_stat */
        funcctx->user_fctx = get_rto_stat(funcctx->tuple_desc);
        MemoryContextSwitchTo(oldcontext);

        if (funcctx->user_fctx == NULL) {
            SRF_RETURN_DONE(funcctx);
        }
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();
    if (funcctx->user_fctx) {
        Tuplestorestate* tupstore = ((TableDistributionInfo*)funcctx->user_fctx)->state->tupstore;
        TupleTableSlot* slot = ((TableDistributionInfo*)funcctx->user_fctx)->slot;

        if (!tuplestore_gettupleslot(tupstore, true, false, slot)) {
            FreeParallelFunctionState(((TableDistributionInfo*)funcctx->user_fctx)->state);
            ExecDropSingleTupleTableSlot(slot);
            pfree_ext(funcctx->user_fctx);
            funcctx->user_fctx = NULL;
            SRF_RETURN_DONE(funcctx);
        }
        for (i = 0; i < RTO_VIEW_COL_SIZE; i++) {
            values[i] = tableam_tslot_getattr(slot, (i + 1), &nulls[i]);
        }
        HeapTuple tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        ExecClearTuple(slot);

        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }
    SRF_RETURN_DONE(funcctx);
#endif
}

Datum local_recovery_status(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;
    MemoryContext oldcontext = NULL;
    RTOStandbyData* entry = NULL;

    /* stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tupdesc = NULL;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function
         * calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* need a tuple descriptor representing 6 columns */
        tupdesc = CreateTemplateTupleDesc(RECOVERY_RTO_VIEW_COL, false);

        TupleDescInitEntry(tupdesc, (AttrNumber)1, "node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "standby_node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "source_ip", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)4, "source_port", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)5, "dest_ip", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)6, "dest_port", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)7, "current_rto", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)8, "target_rto", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)9, "current_sleep_time", INT8OID, -1, 0);

        /* complete descriptor of the tupledesc */
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        if (g_instance.attr.attr_storage.dcf_attr.enable_dcf) {
#ifndef ENABLE_MULTIPLE_NODES
            funcctx->user_fctx = (void*)GetDCFRTOStat(&(funcctx->max_calls));
#endif
        } else {
            funcctx->user_fctx = (void*)GetRTOStat(&(funcctx->max_calls));
        }

        (void)MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    entry = (RTOStandbyData*)funcctx->user_fctx;

    if (funcctx->call_cntr < funcctx->max_calls) { /* do when there is more left to send */
        Datum values[RECOVERY_RTO_VIEW_COL];
        bool nulls[RECOVERY_RTO_VIEW_COL] = {false};
        HeapTuple tuple = NULL;

        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        entry += funcctx->call_cntr;

        values[0] = CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);
        values[1] = CStringGetTextDatum(entry->id);
        values[2] = CStringGetTextDatum(entry->source_ip);
        values[3] = Int32GetDatum(entry->source_port);
        values[4] = CStringGetTextDatum(entry->dest_ip);
        values[5] = Int32GetDatum(entry->dest_port);
        values[6] = Int64GetDatum(entry->current_rto);
        values[7] = Int64GetDatum(entry->target_rto);
        values[8] = Int64GetDatum(entry->current_sleep_time);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }

    /* do when there is no more left */
    SRF_RETURN_DONE(funcctx);
}

Datum remote_recovery_status(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;
#ifndef ENABLE_MULTIPLE_NODES
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported view in single node mode.")));

    SRF_RETURN_DONE(funcctx);
#else
    Datum values[RECOVERY_RTO_VIEW_COL];
    bool nulls[RECOVERY_RTO_VIEW_COL] = {false};

    if (SRF_IS_FIRSTCALL()) {
        funcctx = SRF_FIRSTCALL_INIT();

        MemoryContext oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples. */
        TupleDesc tupdesc = CreateTemplateTupleDesc(RECOVERY_RTO_VIEW_COL, false, TAM_HEAP);
        TupleDescInitEntry(tupdesc, (AttrNumber)1, "node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "standby_node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "source_ip", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)4, "source_port", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)5, "dest_ip", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)6, "dest_port", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)7, "current_rto", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)8, "target_rto", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)9, "current_sleep_time", INT8OID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->max_calls = u_sess->pgxc_cxt.NumDataNodes + u_sess->pgxc_cxt.NumCoords;

        /* the main call for get get_rto_stat */
        funcctx->user_fctx = get_recovery_stat(funcctx->tuple_desc);
        MemoryContextSwitchTo(oldcontext);

        if (funcctx->user_fctx == NULL) {
            SRF_RETURN_DONE(funcctx);
        }
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();
    if (funcctx->user_fctx) {
        Tuplestorestate* tupstore = ((TableDistributionInfo*)funcctx->user_fctx)->state->tupstore;
        TupleTableSlot* slot = ((TableDistributionInfo*)funcctx->user_fctx)->slot;

        if (!tuplestore_gettupleslot(tupstore, true, false, slot)) {
            FreeParallelFunctionState(((TableDistributionInfo*)funcctx->user_fctx)->state);
            ExecDropSingleTupleTableSlot(slot);
            pfree_ext(funcctx->user_fctx);
            funcctx->user_fctx = NULL;
            SRF_RETURN_DONE(funcctx);
        }
        for (uint32 i = 0; i < RECOVERY_RTO_VIEW_COL; i++) {
            values[i] = tableam_tslot_getattr(slot, (i + 1), &nulls[i]);
        }
        HeapTuple tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        ExecClearTuple(slot);

        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }
    SRF_RETURN_DONE(funcctx);
#endif
}

Datum gs_hadr_local_rto_and_rpo_stat(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;
    MemoryContext oldcontext = NULL;
    HadrRTOAndRPOData* entry = NULL;

    /* stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tupdesc = NULL;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function
         * calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* need a tuple descriptor representing 6 columns */
        tupdesc = CreateTemplateTupleDesc(HADR_RTO_RPO_VIEW_COL, false);

        TupleDescInitEntry(tupdesc, (AttrNumber)1, "hadr_sender_node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "hadr_receiver_node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "source_ip", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)4, "source_port", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)5, "dest_ip", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)6, "dest_port", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)7, "current_rto", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)8, "target_rto", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)9, "current_rpo", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)10, "target_rpo", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)11, "rto_sleep_time", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)12, "rpo_sleep_time", INT8OID, -1, 0);
        /* complete descriptor of the tupledesc */
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        funcctx->user_fctx = (void*)HadrGetRTOStat(&(funcctx->max_calls));

        (void)MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    entry = (HadrRTOAndRPOData*)funcctx->user_fctx;

    if (funcctx->call_cntr < funcctx->max_calls) { /* do when there is more left to send */
        Datum values[HADR_RTO_RPO_VIEW_COL];
        bool nulls[HADR_RTO_RPO_VIEW_COL] = {false};
        HeapTuple tuple = NULL;

        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        entry += funcctx->call_cntr;

        values[0] = CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);
        values[1] = CStringGetTextDatum(entry->id);
        values[2] = CStringGetTextDatum(entry->source_ip);
        values[3] = Int32GetDatum(entry->source_port);
        values[4] = CStringGetTextDatum(entry->dest_ip);
        values[5] = Int32GetDatum(entry->dest_port);
        values[6] = Int64GetDatum(entry->current_rto);
        values[7] = Int64GetDatum(entry->target_rto);
        values[8] = Int64GetDatum(entry->current_rpo);
        values[9] = Int64GetDatum(entry->target_rpo);
        values[10] = Int64GetDatum(entry->rto_sleep_time);
        values[11] = Int64GetDatum(entry->rpo_sleep_time);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }

    /* do when there is no more left */
    SRF_RETURN_DONE(funcctx);

}

Datum gs_hadr_remote_rto_and_rpo_stat(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;
#ifndef ENABLE_MULTIPLE_NODES
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported view in single node mode.")));

    SRF_RETURN_DONE(funcctx);
#else
    Datum values[HADR_RTO_RPO_VIEW_COL];
    bool nulls[HADR_RTO_RPO_VIEW_COL] = {false};

    if (SRF_IS_FIRSTCALL()) {
        funcctx = SRF_FIRSTCALL_INIT();

        MemoryContext oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples. */
        TupleDesc tupdesc = CreateTemplateTupleDesc(HADR_RTO_RPO_VIEW_COL, false, TAM_HEAP);
        TupleDescInitEntry(tupdesc, (AttrNumber)1, "hadr_sender_node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "hadr_receiver_node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "source_ip", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)4, "source_port", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)5, "dest_ip", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)6, "dest_port", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)7, "current_rto", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)8, "target_rto", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)9, "current_rpo", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)10, "target_rpo", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)11, "rto_sleep_time", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)12, "rpo_sleep_time", INT8OID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->max_calls = u_sess->pgxc_cxt.NumDataNodes + u_sess->pgxc_cxt.NumCoords;

        /* the main call for get get_rto_stat */
        funcctx->user_fctx = streaming_hadr_get_recovery_stat(funcctx->tuple_desc);
        MemoryContextSwitchTo(oldcontext);

        if (funcctx->user_fctx == NULL) {
            SRF_RETURN_DONE(funcctx);
        }
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->user_fctx) {
        Tuplestorestate* tupstore = ((TableDistributionInfo*)funcctx->user_fctx)->state->tupstore;
        TupleTableSlot* slot = ((TableDistributionInfo*)funcctx->user_fctx)->slot;

        if (!tuplestore_gettupleslot(tupstore, true, false, slot)) {
            FreeParallelFunctionState(((TableDistributionInfo*)funcctx->user_fctx)->state);
            ExecDropSingleTupleTableSlot(slot);
            pfree_ext(funcctx->user_fctx);
            funcctx->user_fctx = NULL;
            SRF_RETURN_DONE(funcctx);
        }
        for (uint32 i = 0; i < HADR_RTO_RPO_VIEW_COL; i++) {
            values[i] = tableam_tslot_getattr(slot, (i + 1), &nulls[i]);
        }
        HeapTuple tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        ExecClearTuple(slot);

        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }
    SRF_RETURN_DONE(funcctx);
#endif
}

Datum track_memory_context_detail(PG_FUNCTION_ARGS)
{
#define TRACK_MEMORY_DETAIL_ELEMENT_NUMBER 4
    FuncCallContext* funcctx = NULL;
    Datum result;
    MemoryContext oldcontext;
    TupleDesc tupledesc;
    HeapTuple tuple;
    int i = 1;

    if (!superuser() && !isMonitoradmin(GetUserId())) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_PROC,
            "track_memory_context_detail");
    }

    if (SRF_IS_FIRSTCALL()) {
        funcctx = SRF_FIRSTCALL_INIT();

        /* Switch context when allocating stuff to be used in later calls */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* Construct a tuple descriptor for the result rows. */
        tupledesc = CreateTemplateTupleDesc(TRACK_MEMORY_DETAIL_ELEMENT_NUMBER, false);
        TupleDescInitEntry(tupledesc, (AttrNumber)i++, "context_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupledesc, (AttrNumber)i++, "file", TEXTOID, -1, 0);
        TupleDescInitEntry(tupledesc, (AttrNumber)i++, "line", INT4OID, -1, 0);
        TupleDescInitEntry(tupledesc, (AttrNumber)i, "size", INT8OID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupledesc);

        funcctx->user_fctx = GetMemoryTrackInfo();

        /* Return to original context when allocating transient memory */
        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();

    /* Get the saved state */
    MemoryAllocDetailList* node = (MemoryAllocDetailList*)funcctx->user_fctx;
    if (node != NULL) {
        Datum values[TRACK_MEMORY_DETAIL_ELEMENT_NUMBER];
        bool nulls[TRACK_MEMORY_DETAIL_ELEMENT_NUMBER] = {false};
        MemoryAllocDetail* entry = node->entry;

        i = 0;
        values[i++] = CStringGetTextDatum(entry->detail_key.name);
        values[i++] = CStringGetTextDatum(entry->detail_key.file);
        values[i++] = Int32GetDatum(entry->detail_key.line);
        values[i] = Int64GetDatum(entry->size);

        /* Build and return the tuple. */
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);

        funcctx->user_fctx = (void*)node->next;

        SRF_RETURN_NEXT(funcctx, result);
    } else {
        SRF_RETURN_DONE(funcctx);
    }
}

#ifdef ENABLE_MULTIPLE_NODES
/* Get the head row of the view of index status */
TupleDesc get_index_status_view_frist_row()
{
    TupleDesc tupdesc = NULL;
    tupdesc = CreateTemplateTupleDesc(INDEX_STATUS_VIEW_COL_NUM, false, TAM_HEAP);
    TupleDescInitEntry(tupdesc, (AttrNumber)1, "node_name", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)2, "indisready", BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)3, "indisvalid", BOOLOID, -1, 0);
    return BlessTupleDesc(tupdesc);
}

HeapTuple fetch_local_index_status(FuncCallContext *funcctx, char *schname, char *idxname)
{
    if (funcctx->call_cntr < funcctx->max_calls) {
        /* for local cn, get index status */
        Datum values[INDEX_STATUS_VIEW_COL_NUM];
        bool nulls[INDEX_STATUS_VIEW_COL_NUM] = {false};

        Oid idx_oid = InvalidOid;
        if (schname == NULL || strlen(schname) == 0) {
            idx_oid = RangeVarGetRelid(makeRangeVar(NULL, idxname, -1), NoLock, false);
        } else {
            idx_oid = RangeVarGetRelid(makeRangeVar(schname, idxname, -1), NoLock, false);
        }

        if (!OidIsValid(idx_oid)) {
            ereport(ERROR, (errmodule(MOD_FUNCTION), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("The given schema or index name cannot find."),
                errdetail("Cannot find valid oid from the given index name."),
                errcause("Input error schema or index name."),
                erraction("Check the input schema and index name.")));
        }
        HeapTuple indexTuple = SearchSysCacheCopy1(INDEXRELID, ObjectIdGetDatum(idx_oid));
        if (!HeapTupleIsValid(indexTuple)) {
            ereport(ERROR, (errmodule(MOD_CACHE), errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for index %u.", idx_oid),
                errdetail("The index tuple is invalide."),
                errcause("The index is not found in syscache."),
                erraction("Retry this function.")));
        }
        Form_pg_index indexForm = (Form_pg_index)GETSTRUCT(indexTuple);

        values[0] = CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);
        values[1] = BoolGetDatum(indexForm->indisready);
        values[2] = BoolGetDatum(indexForm->indisvalid);
        return heap_form_tuple(funcctx->tuple_desc, values, nulls);
    }
    return NULL;
}

/*
 * @Description : Get index status on all nodes.
 * @in         	: schemaname, idxname
 * @out         : None
 * @return      : Node.
 */
Datum gs_get_index_status(PG_FUNCTION_ARGS)
{
    if (IS_PGXC_DATANODE) {
        ereport(ERROR, (errmodule(MOD_FUNCTION), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("unsupported function on datanodes."),
            errdetail("Cannot execute a global function direct on datanodes."),
            errcause("Execute a global function on datanodes."),
            erraction("Execute this function on coordinators.")));
        PG_RETURN_VOID();
    }

    Datum values[INDEX_STATUS_VIEW_COL_NUM];
    bool nulls[INDEX_STATUS_VIEW_COL_NUM] = {false};

    char* schname = PG_GETARG_CSTRING(0);
    char* idxname = PG_GETARG_CSTRING(1);

    if (schname == NULL || strlen(schname) == 0 || idxname == NULL || strlen(idxname) == 0) {
        ereport(ERROR, (errmodule(MOD_INDEX), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("Invalid input schema name or index name."),
            errdetail("The input schema or index name is null."),
            errcause("Input empty or less parameters."),
            erraction("Please input the correct schema name and index name.")));
        PG_RETURN_VOID();
    }

    FuncCallContext *funcctx = NULL;
    /* get the fist row of the view */
    if (SRF_IS_FIRSTCALL()) {
        funcctx = SRF_FIRSTCALL_INIT();
        MemoryContext oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        funcctx->tuple_desc = get_index_status_view_frist_row();
        /* for coordinator, get a view of all nodes */
        funcctx->max_calls = u_sess->pgxc_cxt.NumDataNodes + u_sess->pgxc_cxt.NumCoords;
        funcctx->user_fctx = get_remote_index_status(funcctx->tuple_desc, schname, idxname);
        (void)MemoryContextSwitchTo(oldcontext);

        if (funcctx->user_fctx == NULL) {
            SRF_RETURN_DONE(funcctx);
        }
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->user_fctx) {
        Tuplestorestate* tupstore = ((TableDistributionInfo*)funcctx->user_fctx)->state->tupstore;
        TupleTableSlot* slot = ((TableDistributionInfo*)funcctx->user_fctx)->slot;

        if (!tuplestore_gettupleslot(tupstore, true, false, slot)) {
            HeapTuple tuple = fetch_local_index_status(funcctx, schname, idxname);
            if (tuple != NULL) {
                SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
            } else {
                /* release context when all tuples are returned */
                FreeParallelFunctionState(((TableDistributionInfo*)funcctx->user_fctx)->state);
                ExecDropSingleTupleTableSlot(slot);
                pfree_ext(funcctx->user_fctx);
                funcctx->user_fctx = NULL;
                SRF_RETURN_DONE(funcctx);
            }
        }

        for (uint32 i = 0; i < INDEX_STATUS_VIEW_COL_NUM; i++) {
            values[i] = tableam_tslot_getattr(slot, (i + 1), &nulls[i]);
        }

        HeapTuple tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        (void)ExecClearTuple(slot);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }
    SRF_RETURN_DONE(funcctx);
}

#endif
