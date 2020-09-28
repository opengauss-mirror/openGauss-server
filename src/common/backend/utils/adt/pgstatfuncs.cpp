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
#include "access/transam.h"
#include "connector.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "catalog/pg_partition_fn.h"
#include "catalog/pg_namespace.h"
#include "commands/dbcommands.h"
#include "commands/user.h"
#include "commands/vacuum.h"
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
#include "storage/lwlock.h"
#include "postgres.h"
#include "knl/knl_variable.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/buf_internals.h"
#include "workload/cpwlm.h"
#include "workload/workload.h"
#include "pgxc/pgxcnode.h"
#include "access/hash.h"
#include "libcomm/libcomm.h"
#include "pgxc/poolmgr.h"
#include "pgxc/execRemote.h"
#include "utils/elog.h"
#include "commands/user.h"
#include "keymanagement/KeyRecord.h"
#include "instruments/list.h"
#include "access/redo_statistic.h"
#include "replication/rto_statistic.h"

#define UINT32_ACCESS_ONCE(var) ((uint32)(*((volatile uint32*)&(var))))

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

/* internal interface for pmk functions. */
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

/* Global bgwriter statistics, from bgwriter.c */
extern PgStat_MsgBgWriter bgwriterStats;

extern GlobalNodeDefinition* global_node_definition;
extern pthread_mutex_t nodeDefCopyLock;

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

/* the size of GaussDB_expr.ir */
#define IR_FILE_SIZE 29800
#define WAITSTATELEN 256
extern TDE_ALGO g_tde_algo;

static int64 pgxc_exec_partition_tuples_stat(HeapTuple classTuple, HeapTuple partTuple, char* funcname);
static inline void InitPlanOperatorInfoTuple(int operator_plan_info_attrnum, FuncCallContext *funcctx);
void pg_stat_get_beentry(FuncCallContext* funcctx, PgBackendStatus** beentry);
void pg_stat_get_stat_list(List** stat_list, uint32* statFlag_ref, Oid relid);

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
    "stream get conn",               // STATE_STREAM_WAIT_CONNECT_NODES
    "wait producer ready",           // STATE_STREAM_WAIT_PRODUCER_READY
    "synchronize quit",              // STATE_STREAM_WAIT_THREAD_SYNC_QUIT
    "wait stream group destroy",     // STATE_STREAM_WAIT_NODEGROUP_DESTROY
    "wait active statement",         // STATE_WAIT_ACTIVE_STATEMENT
    "wait memory",                   // STATE_WAIT_MEMORY
    "Sort",                          // STATE_EXEC_SORT
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
    "wait sync consumer next step",  // STATE_WAIT_SYNC_CONSUMER_NEXT_STEP
    "wait sync producer next step"   // STATE_WAIT_SYNC_PRODUCER_NEXT_STEP
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
    uint32 class_id;
    class_id = wait_event_info & 0xFF000000;

    switch (class_id) {
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

        datum = slot_getattr(slot, 1, &isnull);
        if (!isnull) {
            result = DatumGetInt64(datum);

            if (!replicated)
                state->result += result;
            else if (state->result < result)
                state->result = result;
        }

        (void)ExecClearTuple(slot);
    }
}

static int64 pgxc_exec_tuples_stat(Oid rel_oid, char* func_name, RemoteQueryExecType exec_type)
{
    Relation rel = NULL;
    char* rel_name = NULL;
    char* ns_pname = NULL;
    ExecNodes* exec_nodes = NULL;
    StringInfoData buf;
    ParallelFunctionState* state = NULL;
    int64 result = 0;
    bool replicated = false;

    rel = try_relation_open(rel_oid, AccessShareLock);
    if (!rel) {
        /* it has been dropped */
        return 0;
    }

    exec_nodes = (ExecNodes*)makeNode(ExecNodes);
    exec_nodes->baselocatortype = LOCATOR_TYPE_HASH;
    exec_nodes->accesstype = RELATION_ACCESS_READ;
    exec_nodes->en_relid = rel_oid;
    exec_nodes->primarynodelist = NIL;
    exec_nodes->en_expr = NULL;

    if (exec_type == EXEC_ON_DATANODES) {
        /* for tuples stat(insert/update/delete/changed..) count */
        exec_nodes->en_relid = rel_oid;
        exec_nodes->nodeList = NIL;
        replicated = IsRelationReplicated(RelationGetLocInfo(rel));
    } else if (exec_type == EXEC_ON_COORDS) {
        /* for autovac count */
        exec_nodes->en_relid = InvalidOid;
        exec_nodes->nodeList = GetAllCoordNodes();
    } else {
        Assert(false);
    }

    /*
     * we awalys send some query string to datanode, Maybe it contains some object name such
     * as table name, schema name. If the rel_name has single quotes ('), we have to replace
     * it with two single quotes ('') to avoid syntax error when the sql string excutes in datanode
     */
    rel_name = repairObjectName(RelationGetRelationName(rel));
    ns_pname = repairObjectName(get_namespace_name(rel->rd_rel->relnamespace, true));
    relation_close(rel, AccessShareLock);

    initStringInfo(&buf);
    appendStringInfo(&buf,
        "SELECT pg_catalog.%s(c.oid) FROM pg_class c "
        "INNER JOIN pg_namespace n on c.relnamespace = n.oid "
        "WHERE n.nspname='%s' AND relname = '%s'",
        func_name,
        ns_pname,
        rel_name);

    state = RemoteFunctionResultHandler(buf.data, exec_nodes, NULL, true, exec_type, true);
    compute_tuples_stat(state, replicated);
    result = state->result;

    pfree_ext(ns_pname);
    pfree_ext(rel_name);
    pfree_ext(buf.data);
    FreeParallelFunctionState(state);

    return result;
}

/* get lastest autovac time */
static void strategy_func_max_times_tamptz(ParallelFunctionState* state)
{
    TupleTableSlot* slot = NULL;
    TimestampTz* result = NULL;

    state->resultset = (TimestampTz*)palloc(sizeof(TimestampTz));
    result = (TimestampTz*)state->resultset;
    *result = 0;

    Assert(state && state->tupstore);
    if (state->tupdesc == NULL) {
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

        if (!tuplestore_gettupleslot(state->tupstore, true, false, slot)) {
            break;
        }

        tmp = DatumGetTimestampTz(slot_getattr(slot, 1, &isnull));
        if (!isnull && timestamp_cmp_internal(tmp, *result) > 0) {
            *result = tmp;
        }

        (void)ExecClearTuple(slot);
    }
}

/* get lastest autoanalyze/autovacuum timestamp from remote coordinator */
static TimestampTz pgxc_last_autovac_time(Oid rel_oid, char* func_name)
{
    Oid nspid = get_rel_namespace(rel_oid);
    char* rel_name = NULL;
    char* nsp_name = NULL;
    ExecNodes* exec_nodes = NULL;
    TimestampTz result = 0;
    StringInfoData buf;
    ParallelFunctionState* state = NULL;

    rel_name = get_rel_name(rel_oid);
    nsp_name = get_namespace_name(nspid);
    /* it has been dropped */
    if ((rel_name == NULL) || (nsp_name == NULL)) {
        return 0;
    }
    exec_nodes = (ExecNodes*)makeNode(ExecNodes);
    exec_nodes->accesstype = RELATION_ACCESS_READ;
    exec_nodes->baselocatortype = LOCATOR_TYPE_HASH;
    exec_nodes->nodeList = GetAllCoordNodes();
    exec_nodes->primarynodelist = NIL;
    exec_nodes->en_expr = NULL;

    rel_name = repairObjectName(rel_name);
    nsp_name = repairObjectName(nsp_name);

    initStringInfo(&buf);
    appendStringInfo(&buf,
        "SELECT pg_catalog.%s(c.oid) FROM pg_class c "
        "INNER JOIN pg_namespace n on c.relnamespace = n.oid "
        "WHERE n.nspname='%s' AND relname = '%s' ",
        func_name,
        nsp_name,
        rel_name);
    state = RemoteFunctionResultHandler(buf.data, exec_nodes, strategy_func_max_times_tamptz, true, EXEC_ON_COORDS, true);
    result = *((TimestampTz*)state->resultset);
    FreeParallelFunctionState(state);

    return result;
}

Datum pg_stat_get_numscans(PG_FUNCTION_ARGS)
{
    Oid rel_id = PG_GETARG_OID(0);
    int64 result = 0;
    List* stat_list = NIL;
    ListCell* stat_cell = NULL;
    PgStat_StatTabKey tab_key;
    PgStat_StatTabEntry* tab_entry = NULL;
    pg_stat_get_stat_list(&stat_list, &tab_key.statFlag, rel_id);

    foreach (stat_cell, stat_list) {
        tab_key.tableid = lfirst_oid(stat_cell);
        tab_entry = pgstat_fetch_stat_tabentry(&tab_key);
        if (!PointerIsValid(tab_entry)) {
            result += 0;
        } else {
            result += (int64)(tab_entry->numscans);
        }
    }

    if (PointerIsValid(stat_list)) {
        list_free(stat_list);
    }

    PG_RETURN_INT64(result);
}

void pg_stat_get_stat_list(List** stat_list, uint32* stat_flag_ref, Oid rel_id)
{
    if (isPartitionedObject(rel_id, RELKIND_RELATION, true)) {
        *stat_flag_ref = STATFLG_PARTITION;
        *stat_list = getPartitionObjectIdList(rel_id, PART_OBJ_TYPE_TABLE_PARTITION);
    } else if (isPartitionedObject(rel_id, RELKIND_INDEX, true)) {
        *stat_flag_ref = STATFLG_PARTITION;
        *stat_list = getPartitionObjectIdList(rel_id, PART_OBJ_TYPE_INDEX_PARTITION);
    } else {
        *stat_flag_ref = STATFLG_RELATION;
        *stat_list = lappend_oid(*stat_list, rel_id);
    }
}

Datum pg_stat_get_tuples_returned(PG_FUNCTION_ARGS)
{
    Oid rel_id = PG_GETARG_OID(0);
    int64 result = 0;
    List* stat_list = NIL;
    ListCell* stat_cell = NULL;
    PgStat_StatTabKey tab_key;
    PgStat_StatTabEntry* tab_entry = NULL;
    pg_stat_get_stat_list(&stat_list, &tab_key.statFlag, rel_id);

    foreach (stat_cell, stat_list) {
        tab_key.tableid = lfirst_oid(stat_cell);
        tab_entry = pgstat_fetch_stat_tabentry(&tab_key);
        if (!PointerIsValid(tab_entry)) {
            result += 0;
        } else {
            result += (int64)(tab_entry->tuples_returned);
        }
    }

    if (PointerIsValid(stat_list)) {
        list_free(stat_list);
    }

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_tuples_fetched(PG_FUNCTION_ARGS)
{
    Oid rel_id = PG_GETARG_OID(0);
    int64 result = 0;
    List* stat_list = NIL;
    ListCell* stat_cell = NULL;
    PgStat_StatTabKey tab_key;
    PgStat_StatTabEntry* tab_entry = NULL;
    pg_stat_get_stat_list(&stat_list, &tab_key.statFlag, rel_id);

    foreach (stat_cell, stat_list) {
        tab_key.tableid = lfirst_oid(stat_cell);
        tab_entry = pgstat_fetch_stat_tabentry(&tab_key);
        if (!PointerIsValid(tab_entry)) {
            result += 0;
        } else {
            result += (int64)(tab_entry->tuples_fetched);
        }
    }

    if (PointerIsValid(stat_list)) {
        list_free(stat_list);
    }

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_tuples_inserted(PG_FUNCTION_ARGS)
{
    PgStat_StatTabKey tab_key;
    int64 result = 0;
    Oid table_id = PG_GETARG_OID(0);
    PgStat_StatTabEntry* tab_entry = NULL;

    /* for user-define table, fetch inserted tuples from datanode */
    if (IS_PGXC_COORDINATOR && GetRelationLocInfo(table_id)) {
        PG_RETURN_INT64(pgxc_exec_tuples_stat(table_id, "pg_stat_get_tuples_inserted", EXEC_ON_DATANODES));
    }
    tab_key.statFlag = InvalidOid;
    tab_key.tableid = table_id;
    tab_entry = pgstat_fetch_stat_tabentry(&tab_key);
    if (tab_entry != NULL) {
        result = (int64)(tab_entry->tuples_inserted);
    }
    PG_RETURN_INT64(result);
}

Datum pg_stat_get_tuples_updated(PG_FUNCTION_ARGS)
{
    PgStat_StatTabKey tab_key;
    int64 result = 0;
    Oid table_id = PG_GETARG_OID(0);
    PgStat_StatTabEntry* tab_entry = NULL;

    /* for user-define table, fetch updated tuples from datanode */
    if (IS_PGXC_COORDINATOR && GetRelationLocInfo(table_id)) {
        PG_RETURN_INT64(pgxc_exec_tuples_stat(table_id, "pg_stat_get_tuples_updated", EXEC_ON_DATANODES));
    }
    tab_key.statFlag = InvalidOid;
    tab_key.tableid = table_id;
    tab_entry = pgstat_fetch_stat_tabentry(&tab_key);
    if (tab_entry != NULL) {
        result = (int64)(tab_entry->tuples_updated);
    }
    PG_RETURN_INT64(result);
}

Datum pg_stat_get_tuples_deleted(PG_FUNCTION_ARGS)
{
    Oid table_id = PG_GETARG_OID(0);
    PgStat_StatTabKey tab_key;
    int64 result = 0;
    PgStat_StatTabEntry* tab_entry = NULL;

    /* for user-define table, fetch deleted tuples from datanode */
    if (IS_PGXC_COORDINATOR && GetRelationLocInfo(table_id)) {
        PG_RETURN_INT64(pgxc_exec_tuples_stat(table_id, "pg_stat_get_tuples_deleted", EXEC_ON_DATANODES));
    }

    tab_key.statFlag = InvalidOid;
    tab_key.tableid = table_id;
    tab_entry = pgstat_fetch_stat_tabentry(&tab_key);
    if (tab_entry != NULL) {
        result = (int64)(tab_entry->tuples_deleted);
    }
    PG_RETURN_INT64(result);
}

Datum pg_stat_get_tuples_hot_updated(PG_FUNCTION_ARGS)
{
    Oid rel_id = PG_GETARG_OID(0);
    PgStat_StatTabKey tab_key;
    PgStat_StatTabEntry* tab_entry = NULL;
    int64 result = 0;

    /* for user-define table, fetch hot-updated tuples from datanode */
    if (IS_PGXC_COORDINATOR && GetRelationLocInfo(rel_id)) {
        PG_RETURN_INT64(pgxc_exec_tuples_stat(rel_id, "pg_stat_get_tuples_hot_updated", EXEC_ON_DATANODES));
    }
    tab_key.statFlag = InvalidOid;
    tab_key.tableid = rel_id;
    tab_entry = pgstat_fetch_stat_tabentry(&tab_key);
    if (tab_entry != NULL) {
        result = (int64)(tab_entry->tuples_hot_updated);
    }
    PG_RETURN_INT64(result);
}

Datum pg_stat_get_tuples_changed(PG_FUNCTION_ARGS)
{
    Oid rel_id = PG_GETARG_OID(0);
    int64 result = 0;
    PgStat_StatTabKey tab_key;
    PgStat_StatTabEntry* tab_entry = NULL;
    /* for user-define table, fetch changed tuples from datanode */
    if (IS_PGXC_COORDINATOR && GetRelationLocInfo(rel_id)) {
        PG_RETURN_INT64(pgxc_exec_tuples_stat(rel_id, "pg_stat_get_tuples_changed", EXEC_ON_DATANODES));
    }
    tab_key.statFlag = InvalidOid;
    tab_key.tableid = rel_id;
    tab_entry = pgstat_fetch_stat_tabentry(&tab_key);
    if (tab_entry != NULL) {
        result = (int64)(tab_entry->changes_since_analyze);
    }
    PG_RETURN_INT64(result);
}

Datum pg_stat_get_live_tuples(PG_FUNCTION_ARGS)
{
    Oid rel_id = PG_GETARG_OID(0);
    PgStat_StatTabKey tab_key;
    PgStat_StatTabEntry* tab_entry = NULL;
    int64 result = 0;
    List* stat_list = NIL;
    ListCell* stat_cell = NULL;
    /* for user-define table, fetch live tuples from datanode */
    if (IS_PGXC_COORDINATOR && GetRelationLocInfo((rel_id))) {
        PG_RETURN_INT64(pgxc_exec_tuples_stat(rel_id, "pg_stat_get_live_tuples", EXEC_ON_DATANODES));
    }

    if (isPartitionedObject(rel_id, RELKIND_INDEX, true)) {
        tab_key.statFlag = rel_id;
        stat_list = getPartitionObjectIdList(rel_id, PART_OBJ_TYPE_INDEX_PARTITION);
    } else if (isPartitionedObject(rel_id, RELKIND_RELATION, true)) {
        tab_key.statFlag = rel_id;
        stat_list = getPartitionObjectIdList(rel_id, PART_OBJ_TYPE_TABLE_PARTITION);
    } else {
        tab_key.statFlag = InvalidOid;
        stat_list = lappend_oid(stat_list, rel_id);
    }

    foreach (stat_cell, stat_list) {
        tab_key.tableid = lfirst_oid(stat_cell);
        tab_entry = pgstat_fetch_stat_tabentry(&tab_key);
        if (PointerIsValid(tab_entry))
            result += (int64)(tab_entry->n_live_tuples);
    }

    list_free(stat_list);

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_dead_tuples(PG_FUNCTION_ARGS)
{
    Oid rel_id = PG_GETARG_OID(0);
    PgStat_StatTabKey tab_key;
    PgStat_StatTabEntry* tab_entry = NULL;
    int64 result = 0;
    List* stat_list = NIL;
    ListCell* stat_cell = NULL;

    if (IS_PGXC_COORDINATOR && GetRelationLocInfo((rel_id))) {
        PG_RETURN_INT64(pgxc_exec_tuples_stat(rel_id, "pg_stat_get_dead_tuples", EXEC_ON_DATANODES));
    }

    if (isPartitionedObject(rel_id, RELKIND_INDEX, true)) {
        tab_key.statFlag = rel_id;
        stat_list = getPartitionObjectIdList(rel_id, PART_OBJ_TYPE_INDEX_PARTITION);
    } else if (isPartitionedObject(rel_id, RELKIND_RELATION, true)) {
        tab_key.statFlag = rel_id;
        stat_list = getPartitionObjectIdList(rel_id, PART_OBJ_TYPE_TABLE_PARTITION);
    } else {
        tab_key.statFlag = InvalidOid;
        stat_list = lappend_oid(stat_list, rel_id);
    }

    foreach (stat_cell, stat_list) {
        tab_key.tableid = lfirst_oid(stat_cell);
        tab_entry = pgstat_fetch_stat_tabentry(&tab_key);
        if (PointerIsValid(tab_entry)) {
            result += (int64)(tab_entry->n_dead_tuples);
        }
    }

    if (PointerIsValid(stat_list)) {
        list_free(stat_list);
    }

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_blocks_fetched(PG_FUNCTION_ARGS)
{
    Oid rel_id = PG_GETARG_OID(0);
    int64 result = 0;
    List* stat_list = NIL;
    ListCell* stat_cell = NULL;
    PgStat_StatTabKey tab_key;
    PgStat_StatTabEntry* tab_entry = NULL;
    pg_stat_get_stat_list(&stat_list, &tab_key.statFlag, rel_id);

    foreach (stat_cell, stat_list) {
        tab_key.tableid = lfirst_oid(stat_cell);
        tab_entry = pgstat_fetch_stat_tabentry(&tab_key);
        if (!PointerIsValid(tab_entry)) {
            result += 0;
        } else {
            result += (int64)(tab_entry->blocks_fetched);
        }
    }

    if (PointerIsValid(stat_list)) {
        list_free(stat_list);
    }

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_blocks_hit(PG_FUNCTION_ARGS)
{
    Oid rel_id = PG_GETARG_OID(0);
    int64 result = 0;
    List* stat_list = NIL;
    ListCell* stat_cell = NULL;
    PgStat_StatTabKey tab_key;
    PgStat_StatTabEntry* tab_entry = NULL;
    pg_stat_get_stat_list(&stat_list, &tab_key.statFlag, rel_id);
    foreach (stat_cell, stat_list) {
        tab_key.tableid = lfirst_oid(stat_cell);
        tab_entry = pgstat_fetch_stat_tabentry(&tab_key);
        if (!PointerIsValid(tab_entry)) {
            result += 0;
        } else {
            result += (int64)(tab_entry->blocks_hit);
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
    Oid rel_id = PG_GETARG_OID(0);
    int64 result = 0;
    List* stat_list = NIL;
    ListCell* stat_cell = NULL;
    PgStat_StatTabKey tab_key;
    PgStat_StatTabEntry* tab_entry = NULL;
    pg_stat_get_stat_list(&stat_list, &tab_key.statFlag, rel_id);
    foreach (stat_cell, stat_list) {
        tab_key.tableid = lfirst_oid(stat_cell);
        tab_entry = pgstat_fetch_stat_tabentry(&tab_key);
        if (PointerIsValid(tab_entry)) {
            result += (int64)(tab_entry->cu_mem_hit);
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
    Oid rel_id = PG_GETARG_OID(0);
    int64 result = 0;
    List* stat_list = NIL;
    ListCell* stat_cell = NULL;
    PgStat_StatTabKey tab_key;
    PgStat_StatTabEntry* tab_entry = NULL;
    pg_stat_get_stat_list(&stat_list, &tab_key.statFlag, rel_id);
    foreach (stat_cell, stat_list) {
        tab_key.tableid = lfirst_oid(stat_cell);
        tab_entry = pgstat_fetch_stat_tabentry(&tab_key);
        if (PointerIsValid(tab_entry)) {
            result += (int64)(tab_entry->cu_hdd_sync);
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
    Oid rel_id = PG_GETARG_OID(0);
    int64 result = 0;
    List* stat_list = NIL;
    ListCell* stat_cell = NULL;
    PgStat_StatTabKey tab_key;
    PgStat_StatTabEntry* tab_entry = NULL;
    pg_stat_get_stat_list(&stat_list, &tab_key.statFlag, rel_id);

    foreach (stat_cell, stat_list) {
        tab_key.tableid = lfirst_oid(stat_cell);
        tab_entry = pgstat_fetch_stat_tabentry(&tab_key);
        if (PointerIsValid(tab_entry)) {
            result += (int64)(tab_entry->cu_hdd_asyn);
        }
    }
    if (PointerIsValid(stat_list)) {
        list_free(stat_list);
    }
    PG_RETURN_INT64(result);
}

Datum pg_stat_get_last_data_changed_time(PG_FUNCTION_ARGS)
{
    Oid rel_id = PG_GETARG_OID(0);
    TimestampTz result = 0;
    PgStat_StatTabKey tab_key;
    PgStat_StatTabEntry* tab_entry = NULL;

    tab_key.statFlag = STATFLG_RELATION;
    tab_key.tableid = rel_id;
    tab_entry = pgstat_fetch_stat_tabentry(&tab_key);
    if (!PointerIsValid(tab_entry)) {
        result = 0;
    } else {
        result = tab_entry->data_changed_timestamp;
    }
    if (result == 0) {
        PG_RETURN_NULL();
    } else {
        PG_RETURN_TIMESTAMPTZ(result);
    }
}

Datum pg_stat_set_last_data_changed_time(PG_FUNCTION_ARGS)
{
    if (!IS_PGXC_COORDINATOR && !IS_SINGLE_NODE) {
        PG_RETURN_VOID();
    }

    Oid rel_id = PG_GETARG_OID(0);
    Relation rel = heap_open(rel_id, AccessShareLock);
    pgstat_report_data_changed(rel_id, STATFLG_RELATION, rel->rd_rel->relisshared);
    heap_close(rel, AccessShareLock);

    PG_RETURN_VOID();
}

Datum pg_stat_get_last_vacuum_time(PG_FUNCTION_ARGS)
{
    Oid rel_id = PG_GETARG_OID(0);
    PgStat_StatTabKey tab_key;
    PgStat_StatTabEntry* tab_entry = NULL;
    TimestampTz result = 0;

    tab_key.statFlag = InvalidOid;
    tab_key.tableid = rel_id;
    tab_entry = pgstat_fetch_stat_tabentry(&tab_key);
    if (tab_entry != NULL) {
        result = tab_entry->vacuum_timestamp;
    }

    if (result == 0) {
        PG_RETURN_NULL();
    } else {
        PG_RETURN_TIMESTAMPTZ(result);
    }
}

Datum pg_stat_get_last_autovacuum_time(PG_FUNCTION_ARGS)
{
    Oid rel_id = PG_GETARG_OID(0);
    PgStat_StatTabKey tab_key;
    PgStat_StatTabEntry* tab_entry = NULL;
    TimestampTz result = 0;

    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        result = pgxc_last_autovac_time(rel_id, "pg_stat_get_last_autovacuum_time");
    }

    tab_key.statFlag = InvalidOid;
    tab_key.tableid = rel_id;
    tab_entry = pgstat_fetch_stat_tabentry(&tab_key);
    /* get lastest autovacuum timestamp from local timestamp and remote timestamp */
    if (tab_entry && timestamptz_cmp_internal(tab_entry->autovac_vacuum_timestamp, result) > 0) {
        result = tab_entry->autovac_vacuum_timestamp;
    }
    if (result == 0) {
        PG_RETURN_NULL();
    } else {
        PG_RETURN_TIMESTAMPTZ(result);
    }
}

Datum pg_stat_get_last_analyze_time(PG_FUNCTION_ARGS)
{
    Oid rel_id = PG_GETARG_OID(0);
    PgStat_StatTabKey tab_key;
    PgStat_StatTabEntry* tab_entry = NULL;
    TimestampTz result = 0;

    tab_key.statFlag = InvalidOid;
    tab_key.tableid = rel_id;
    tab_entry = pgstat_fetch_stat_tabentry(&tab_key);
    if (tab_entry != NULL) {
        result = tab_entry->analyze_timestamp;
    }
    if (result == 0) {
        PG_RETURN_NULL();
    } else {
        PG_RETURN_TIMESTAMPTZ(result);
    }
}

Datum pg_stat_get_last_autoanalyze_time(PG_FUNCTION_ARGS)
{
    Oid rel_id = PG_GETARG_OID(0);
    PgStat_StatTabKey tab_key;
    PgStat_StatTabEntry* tab_entry = NULL;
    TimestampTz result = 0;

    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        result = pgxc_last_autovac_time(rel_id, "pg_stat_get_last_autoanalyze_time");
    }

    tab_key.statFlag = InvalidOid;
    tab_key.tableid = rel_id;
    tab_entry = pgstat_fetch_stat_tabentry(&tab_key);
    if (tab_entry && timestamptz_cmp_internal(tab_entry->autovac_analyze_timestamp, result) > 0) {
        result = tab_entry->autovac_analyze_timestamp;
    }
    if (result == 0) {
        PG_RETURN_NULL();
    } else {
        PG_RETURN_TIMESTAMPTZ(result);
    }
}

Datum pg_stat_get_vacuum_count(PG_FUNCTION_ARGS)
{
    Oid rel_id = PG_GETARG_OID(0);
    PgStat_StatTabKey tab_key;
    PgStat_StatTabEntry* tab_entry = NULL;
    int64 result = 0;

    tab_key.statFlag = InvalidOid;
    tab_key.tableid = rel_id;
    tab_entry = pgstat_fetch_stat_tabentry(&tab_key);
    if (tab_entry != NULL) {
        result += (int64)(tab_entry->vacuum_count);
    }

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_autovacuum_count(PG_FUNCTION_ARGS)
{
    Oid rel_id = PG_GETARG_OID(0);
    PgStat_StatTabKey tab_key;
    PgStat_StatTabEntry* tab_entry = NULL;
    int64 result = 0;

    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        result = pgxc_exec_tuples_stat(rel_id, "pg_stat_get_autovacuum_count", EXEC_ON_COORDS);
    }

    tab_key.statFlag = InvalidOid;
    tab_key.tableid = rel_id;
    tab_entry = pgstat_fetch_stat_tabentry(&tab_key);
    if (tab_entry != NULL) {
        result += (int64)(tab_entry->autovac_vacuum_count);
    }

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_analyze_count(PG_FUNCTION_ARGS)
{
    Oid rel_id = PG_GETARG_OID(0);
    PgStat_StatTabKey tab_key;
    PgStat_StatTabEntry* tab_entry = NULL;
    int64 result = 0;

    tab_key.statFlag = InvalidOid;
    tab_key.tableid = rel_id;
    tab_entry = pgstat_fetch_stat_tabentry(&tab_key);
    if ((tab_entry = pgstat_fetch_stat_tabentry(&tab_key))) {
        result += (int64)(tab_entry->analyze_count);
    }

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_autoanalyze_count(PG_FUNCTION_ARGS)
{
    Oid rel_id = PG_GETARG_OID(0);
    PgStat_StatTabKey tab_key;
    PgStat_StatTabEntry* tab_entry = NULL;
    int64 result = 0;

    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        result = pgxc_exec_tuples_stat(rel_id, "pg_stat_get_autoanalyze_count", EXEC_ON_COORDS);
    }

    tab_key.statFlag = InvalidOid;
    tab_key.tableid = rel_id;
    tab_entry = pgstat_fetch_stat_tabentry(&tab_key);
    if (tab_entry != NULL) {
        result += (int64)(tab_entry->autovac_analyze_count);
    }
    PG_RETURN_INT64(result);
}

Datum pg_stat_get_function_calls(PG_FUNCTION_ARGS)
{
    Oid func_id = PG_GETARG_OID(0);
    PgStat_StatFuncEntry* func_entry = NULL;

    if ((func_entry = pgstat_fetch_stat_funcentry(func_id)) == NULL) {
        PG_RETURN_NULL();
    }
    PG_RETURN_INT64(func_entry->f_numcalls);
}

Datum pg_stat_get_function_total_time(PG_FUNCTION_ARGS)
{
    Oid func_id = PG_GETARG_OID(0);
    PgStat_StatFuncEntry* func_entry = NULL;

    if ((func_entry = pgstat_fetch_stat_funcentry(func_id)) == NULL) {
        PG_RETURN_NULL();
    }
    /* convert counter from microsec to millisec for display */
    PG_RETURN_FLOAT8(((double)func_entry->f_total_time) / 1000.0);
}

Datum pg_stat_get_function_self_time(PG_FUNCTION_ARGS)
{
    Oid func_id = PG_GETARG_OID(0);
    PgStat_StatFuncEntry* func_entry = NULL;

    if ((func_entry = pgstat_fetch_stat_funcentry(func_id)) == NULL) {
        PG_RETURN_NULL();
    }
    /* convert counter from microsec to millisec for display */
    PG_RETURN_FLOAT8(((double)func_entry->f_self_time) / 1000.0);
}

Datum pg_stat_get_backend_idset(PG_FUNCTION_ARGS)
{
    FuncCallContext* func_ctx = NULL;
    int* fctx = NULL;
    int32 result;

    /* stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL()) {
        /* create a function context for cross-call persistence */
        func_ctx = SRF_FIRSTCALL_INIT();

        fctx = (int*)MemoryContextAlloc(func_ctx->multi_call_memory_ctx, 2 * sizeof(int));
        func_ctx->user_fctx = fctx;

        fctx[0] = 0;
        fctx[1] = pgstat_fetch_stat_numbackends();
    }

    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();
    fctx = (int*)func_ctx->user_fctx;

    fctx[0] += 1;
    result = fctx[0];

    if (result <= fctx[1]) {
        /* do when there is more left to send */
        SRF_RETURN_NEXT(func_ctx, Int32GetDatum(result));
    } else {
        /* do when there is no more left */
        SRF_RETURN_DONE(func_ctx);
    }
}

Datum pg_stat_get_activity(PG_FUNCTION_ARGS)
{
    bool is_first_call = SRF_IS_FIRSTCALL();
    Datum result_with_Info = pg_stat_get_activity_with_conninfo(fcinfo);

    FuncCallContext* ctx = ((FuncCallContext*)(fcinfo->flinfo->fn_extra));
    TupleDesc tuple_desc = NULL;

    if (ctx != NULL) {
        tuple_desc = ctx->tuple_desc;

        /* Remove the last attribute(connection_info) for previous defination of
         * pg_stat_activity in every tuple. */
        if (is_first_call) {
            tuple_desc->natts--;
        }
    }

    /* It's the first return, whose descriptor will be the actual one. */
    if (result_with_Info != (Datum)0) {
        HeapTupleHeader td;
        td = DatumGetHeapTupleHeader(result_with_Info);
        tuple_desc = lookup_rowtype_tupdesc(HeapTupleHeaderGetTypeId(td), HeapTupleHeaderGetTypMod(td));
        if (tuple_desc) {
            tuple_desc->natts--;
            ReleaseTupleDesc(tuple_desc);
        }
    }

    return result_with_Info;
}

Datum pg_stat_get_activity_for_temptable(PG_FUNCTION_ARGS)
{
    FuncCallContext* func_ctx = NULL;
    errno_t rc = 0;
    const int ATT_COUNT = 4;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext old_context;
        TupleDesc tup_desc;

        func_ctx = SRF_FIRSTCALL_INIT();

        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        tup_desc = CreateTemplateTupleDesc(ATT_COUNT, false);
        TupleDescInitEntry(tup_desc, (AttrNumber)1, "datid", OIDOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)2, "templineid", INT4OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)3, "tempid", INT4OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)4, "sessionid", INT8OID, -1, 0);

        func_ctx->tuple_desc = BlessTupleDesc(tup_desc);

        /* Get all backends */
        func_ctx->max_calls = pgstat_fetch_stat_numbackends();

        MemoryContextSwitchTo(old_context);
    }

    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->call_cntr < func_ctx->max_calls) {
        /* for each row */
        Datum values[ATT_COUNT];
        bool nulls[ATT_COUNT] = {false};
        HeapTuple tuple = NULL;
        PgBackendStatus* beentry = NULL;

        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        /* Get the next one in the list */
        beentry = pgstat_fetch_stat_beentry(func_ctx->call_cntr + 1); /* 1-based index */
        if (beentry == NULL || beentry->st_sessionid == 0) {
            for (int i = 0; i < ATT_COUNT; i++)
                nulls[i] = true;
            tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
            SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
        }

        /* Values available to all callers */
        values[0] = ObjectIdGetDatum(beentry->st_databaseid);
        values[1] = Int32GetDatum(beentry->st_timelineid);
        values[2] = Int32GetDatum(beentry->st_tempid);
        values[3] = Int64GetDatum(beentry->st_sessionid);

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);

        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    } else {
        /* nothing left */
        SRF_RETURN_DONE(func_ctx);
    }
}

Datum pg_stat_get_activity_with_conninfo(PG_FUNCTION_ARGS)
{
    FuncCallContext* func_ctx = NULL;
    errno_t rc = 0;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext old_context;
        TupleDesc tup_desc;

        func_ctx = SRF_FIRSTCALL_INIT();

        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        tup_desc = CreateTemplateTupleDesc(18, false);
        TupleDescInitEntry(tup_desc, (AttrNumber)1, "datid", OIDOID, -1, 0);
        /* This should have been called 'pid';  can't change it. 2011-06-11 */
        TupleDescInitEntry(tup_desc, (AttrNumber)2, "pid", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)3, "sessionid", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)4, "usesysid", OIDOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)5, "application_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)6, "state", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)7, "query", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)8, "waiting", BOOLOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)9, "xact_start", TIMESTAMPTZOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)10, "query_start", TIMESTAMPTZOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)11, "backend_start", TIMESTAMPTZOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)12, "state_change", TIMESTAMPTZOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)13, "client_addr", INETOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)14, "client_hostname", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)15, "client_port", INT4OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)16, "enqueue", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)17, "query_id", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)18, "connection_info", TEXTOID, -1, 0);

        func_ctx->tuple_desc = BlessTupleDesc(tup_desc);

        func_ctx->user_fctx = palloc0(sizeof(int));
        if (PG_ARGISNULL(0)) {
            /* Get all backends */
            func_ctx->max_calls = pgstat_fetch_stat_numbackends();
        } else {
            /*
             * Get one backend - locate by pid.
             *
             * We lookup the backend early, so we can return zero rows if it
             * doesn't exist, instead of returning a single row full of NULLs.
             */
            ThreadId pid = PG_GETARG_INT64(0);
            int i;
            int n = pgstat_fetch_stat_numbackends();

            for (i = 1; i <= n; i++) {
                PgBackendStatus* be = pgstat_fetch_stat_beentry(i);

                if (be != NULL) {
                    if (be->st_procpid == pid) {
                        *(int*)(func_ctx->user_fctx) = i;
                        break;
                    }
                }
            }

            if (*(int*)(func_ctx->user_fctx) == 0)
                /* Pid not found, return zero rows */
                func_ctx->max_calls = 0;
            else
                func_ctx->max_calls = 1;
        }

        MemoryContextSwitchTo(old_context);
    }

    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->call_cntr < func_ctx->max_calls) {
        /* for each row */
        Datum values[18];
        bool nulls[18] = {false};
        HeapTuple tuple = NULL;
        PgBackendStatus* beentry = NULL;

        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        pg_stat_get_beentry(func_ctx, &beentry);
        if (beentry == NULL) {
            int i;

            for (i = 0; (unsigned int)(i) < sizeof(nulls) / sizeof(nulls[0]); i++) {
                nulls[i] = true;
            }

            nulls[6] = false;
            values[6] = CStringGetTextDatum("<backend information not available>");

            tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
            SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
        }

        /* Values available to all callers */
        values[0] = ObjectIdGetDatum(beentry->st_databaseid);
        values[1] = Int64GetDatum(beentry->st_procpid);
        values[2] = Int64GetDatum(beentry->st_sessionid);
        values[3] = ObjectIdGetDatum(beentry->st_userid);
        if (beentry->st_appname) {
            values[4] = CStringGetTextDatum(beentry->st_appname);
        } else {
            nulls[4] = true;
        }

        /* Values only available to same user or superuser */
        if (superuser() || beentry->st_userid == GetUserId()) {
            SockAddr zero_clientaddr;

            switch (beentry->st_state) {
                case STATE_IDLE:
                    values[5] = CStringGetTextDatum("idle");
                    break;
                case STATE_RUNNING:
                    values[5] = CStringGetTextDatum("active");
                    break;
                case STATE_IDLEINTRANSACTION:
                    values[5] = CStringGetTextDatum("idle in transaction");
                    break;
                case STATE_FASTPATH:
                    values[5] = CStringGetTextDatum("fastpath function call");
                    break;
                case STATE_IDLEINTRANSACTION_ABORTED:
                    values[5] = CStringGetTextDatum("idle in transaction (aborted)");
                    break;
                case STATE_DISABLED:
                    values[5] = CStringGetTextDatum("disabled");
                    break;
                case STATE_RETRYING:
                    values[5] = CStringGetTextDatum("retrying");
                    break;
                case STATE_COUPLED:
                    values[5] = CStringGetTextDatum("coupled to session");
                    break;
                case STATE_DECOUPLED:
                    values[5] = CStringGetTextDatum("decoupled from session");
                    break;
                case STATE_UNDEFINED:
                    nulls[5] = true;
                    break;
                default:
                    break;
            }
            if (beentry->st_state == STATE_UNDEFINED || beentry->st_state == STATE_DISABLED) {
                nulls[6] = true;
            } else {
                char* mask_string = NULL;

                /* Mask the statement in the activity view. */
                mask_string = maskPassword(beentry->st_activity);
                if (mask_string == NULL) {
                    mask_string = beentry->st_activity;
                }

                values[6] = CStringGetTextDatum(mask_string);

                if (mask_string != beentry->st_activity) {
                    pfree(mask_string);
                }
            }
            values[7] = BoolGetDatum(pgstat_get_waitlock(beentry->st_waitevent));

            if (beentry->st_xact_start_timestamp != 0) {
                values[8] = TimestampTzGetDatum(beentry->st_xact_start_timestamp);
            } else {
                nulls[8] = true;
            }

            if (beentry->st_activity_start_timestamp != 0) {
                values[9] = TimestampTzGetDatum(beentry->st_activity_start_timestamp);
            } else {
                nulls[9] = true;
            }

            if (beentry->st_proc_start_timestamp != 0) {
                values[10] = TimestampTzGetDatum(beentry->st_proc_start_timestamp);
            } else {
                nulls[10] = true;
            }

            if (beentry->st_state_start_timestamp != 0) {
                values[11] = TimestampTzGetDatum(beentry->st_state_start_timestamp);
            } else {
                nulls[11] = true;
            }

            /* A zeroed client addr means we don't know */
            rc = memset_s(&zero_clientaddr, sizeof(zero_clientaddr), 0, sizeof(zero_clientaddr));
            securec_check(rc, "\0", "\0");
            if (memcmp(&(beentry->st_clientaddr), &zero_clientaddr, sizeof(zero_clientaddr)) == 0) {
                nulls[12] = true;
                nulls[13] = true;
                nulls[14] = true;
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
                        values[12] = DirectFunctionCall1(inet_in, CStringGetDatum(remote_host));
                        if (beentry->st_clienthostname && beentry->st_clienthostname[0])
                            values[13] = CStringGetTextDatum(beentry->st_clienthostname);
                        else
                            nulls[13] = true;
                        values[14] = Int32GetDatum(atoi(remote_port));
                    } else {
                        nulls[12] = true;
                        nulls[13] = true;
                        nulls[14] = true;
                    }
                } else if (beentry->st_clientaddr.addr.ss_family == AF_UNIX) {
                    /*
                     * Unix sockets always reports NULL for host and -1 for
                     * port, so it's possible to tell the difference to
                     * connections we have no permissions to view, or with
                     * errors.
                     */
                    nulls[12] = true;
                    nulls[13] = true;
                    values[14] = DatumGetInt32(-1);
                } else {
                    /* Unknown address type, should never happen */
                    nulls[12] = true;
                    nulls[13] = true;
                    nulls[14] = true;
                }
            }

            switch (beentry->st_waiting_on_resource) {
                case STATE_MEMORY:
                    values[15] = CStringGetTextDatum("memory");
                    break;
                case STATE_ACTIVE_STATEMENTS:
                    values[15] = CStringGetTextDatum("waiting in queue");
                    break;
                case STATE_NO_ENQUEUE:
                    nulls[15] = true;
                    break;
                default:
                    break;
            }

            values[16] = Int64GetDatum(beentry->st_queryid);
            if (beentry->st_conninfo != NULL) {
                values[17] = CStringGetTextDatum(beentry->st_conninfo);
            } else {
                nulls[17] = true;
            }
        } else {
            /* No permissions to view data about this session */
            values[6] = CStringGetTextDatum("<insufficient privilege>");

            nulls[5] = true;
            nulls[7] = true;
            nulls[8] = true;
            nulls[9] = true;
            nulls[10] = true;
            nulls[11] = true;
            nulls[12] = true;
            nulls[13] = true;
            nulls[14] = true;
            nulls[15] = true;
            nulls[16] = true;
            nulls[17] = true;
        }

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);

        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    } else {
        /* nothing left */
        SRF_RETURN_DONE(func_ctx);
    }
}

Datum pg_stat_get_activity_ng(PG_FUNCTION_ARGS)
{
    FuncCallContext* func_ctx = NULL;
    errno_t rc = 0;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext old_context;
        TupleDesc tupdesc;

        func_ctx = SRF_FIRSTCALL_INIT();

        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(4, false);
        TupleDescInitEntry(tupdesc, (AttrNumber)1, "datid", OIDOID, -1, 0);
        /* This should have been called 'pid';  can't change it. 2011-06-11 */
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "pid", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "sessionid", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)4, "node_group", TEXTOID, -1, 0);

        func_ctx->tuple_desc = BlessTupleDesc(tupdesc);

        func_ctx->user_fctx = palloc0(sizeof(int));
        if (PG_ARGISNULL(0)) {
            /* Get all backends */
            func_ctx->max_calls = pgstat_fetch_stat_numbackends();
        } else {
            /*
             * Get one backend - locate by pid.
             *
             * We lookup the backend early, so we can return zero rows if it
             * doesn't exist, instead of returning a single row full of NULLs.
             */
            ThreadId pid = PG_GETARG_INT64(0);
            int i;
            int n = pgstat_fetch_stat_numbackends();

            for (i = 1; i <= n; i++) {
                PgBackendStatus* be = pgstat_fetch_stat_beentry(i);

                if (be != NULL) {
                    if (be->st_procpid == pid) {
                        *(int*)(func_ctx->user_fctx) = i;
                        break;
                    }
                }
            }

            if (*(int*)(func_ctx->user_fctx) == 0) {
                /* Pid not found, return zero rows */
                func_ctx->max_calls = 0;
            } else {
                func_ctx->max_calls = 1;
            }
        }

        MemoryContextSwitchTo(old_context);
    }

    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->call_cntr < func_ctx->max_calls) {
        /* for each row */
        Datum values[4];
        bool nulls[4] = {false};
        HeapTuple tuple = NULL;
        PgBackendStatus* beentry = NULL;

        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        pg_stat_get_beentry(func_ctx, &beentry);
        if (beentry == NULL) {
            int i;

            for (i = 0; (unsigned int)(i) < sizeof(nulls) / sizeof(nulls[0]); i++) {
                nulls[i] = true;
            }

            tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
            SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
        }

        /* Values available to all callers */
        values[0] = ObjectIdGetDatum(beentry->st_databaseid);
        values[1] = Int64GetDatum(beentry->st_procpid);
        values[2] = Int64GetDatum(beentry->st_sessionid);

        /* Values only available to same user or superuser */
        if (superuser() || beentry->st_userid == GetUserId()) {
            WLMStatistics* backstat = (WLMStatistics*)&beentry->st_backstat;
            if (StringIsValid(backstat->groupname)) {
                values[3] = CStringGetTextDatum(backstat->groupname);
            } else {
                values[3] = CStringGetTextDatum("installation");
            }
        } else {
            nulls[3] = true;
        }
        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    } else {
        /* nothing left */
        SRF_RETURN_DONE(func_ctx);
    }
}

char* getThreadWaitStatusDesc(PgBackendStatus* beentry)
{
    char* wait_status = NULL;
    errno_t rc = 0;

    Assert(beentry);

    wait_status = (char*)palloc0(WAITSTATELEN);

    if (beentry->st_waitevent != 0) {
        rc = snprintf_s(wait_status,
            WAITSTATELEN,
            WAITSTATELEN - 1,
            "%s: %s",
            pgstat_get_waitstatusdesc(beentry->st_waitevent),
            pgstat_get_wait_event(beentry->st_waitevent));
        securec_check_ss(rc, "\0", "\0");
    } else if (beentry->st_xid != 0) {
        rc = snprintf_s(wait_status,
            WAITSTATELEN,
            WAITSTATELEN - 1,
            "%s: %lu",
            WaitStateDesc[beentry->st_waitstatus],
            beentry->st_xid);
        securec_check_ss(rc, "\0", "\0");
    } else if (beentry->st_relname && beentry->st_relname[0] != '\0' &&
               (beentry->st_waitstatus == STATE_VACUUM || beentry->st_waitstatus == STATE_VACUUM_FULL ||
                   beentry->st_waitstatus == STATE_ANALYZE)) {
        if (beentry->st_waitstatus_phase != PHASE_NONE) {
            rc = snprintf_s(wait_status,
                WAITSTATELEN,
                WAITSTATELEN - 1,
                "%s: %s, %s",
                WaitStateDesc[beentry->st_waitstatus],
                beentry->st_relname,
                WaitStatePhaseDesc[beentry->st_waitstatus_phase]);
            securec_check_ss(rc, "\0", "\0");
        } else {
            rc = snprintf_s(wait_status,
                WAITSTATELEN,
                WAITSTATELEN - 1,
                "%s: %s",
                WaitStateDesc[beentry->st_waitstatus],
                beentry->st_relname);
            securec_check_ss(rc, "\0", "\0");
        }
    } else if (beentry->st_libpq_wait_nodeid != InvalidOid && beentry->st_libpq_wait_nodecount != 0) {
        rc = snprintf_s(wait_status,
            WAITSTATELEN,
            WAITSTATELEN - 1,
            "%s: %d, total %d",
            WaitStateDesc[beentry->st_waitstatus],
            PgxcGetNodeOid(beentry->st_libpq_wait_nodeid),
            beentry->st_libpq_wait_nodecount);
        securec_check_ss(rc, "\0", "\0");
    } else if (beentry->st_nodeid != -1) {
        if (beentry->st_waitnode_count > 0) {
            rc = snprintf_s(wait_status,
                WAITSTATELEN,
                WAITSTATELEN - 1,
                "%s: %d, total %d",
                WaitStateDesc[beentry->st_waitstatus],
                PgxcGetNodeOid(beentry->st_nodeid),
                beentry->st_waitnode_count);
        } else {
            if (beentry->st_waitstatus_phase != PHASE_NONE)
                rc = snprintf_s(wait_status,
                    WAITSTATELEN,
                    WAITSTATELEN - 1,
                    "%s: %d, %s",
                    WaitStateDesc[beentry->st_waitstatus],
                    PgxcGetNodeOid(beentry->st_nodeid),
                    WaitStatePhaseDesc[beentry->st_waitstatus_phase]);
            else
                rc = snprintf_s(wait_status,
                    WAITSTATELEN,
                    WAITSTATELEN - 1,
                    "%s: %d",
                    WaitStateDesc[beentry->st_waitstatus],
                    PgxcGetNodeOid(beentry->st_nodeid));
        }
        securec_check_ss(rc, "\0", "\0");
    } else if (beentry->st_waitstatus != STATE_WAIT_COMM) {
        rc = snprintf_s(wait_status, WAITSTATELEN, WAITSTATELEN - 1, "%s", WaitStateDesc[beentry->st_waitstatus]);
        securec_check_ss(rc, "\0", "\0");
    }

    return wait_status;
}

/*
 * construct_wait_status
 *
 * form wait status string from beentry object.
 */
static void construct_wait_status(PgBackendStatus* beentry, Datum values[12], bool nulls[12])
{
    char wait_status[WAITSTATELEN];
    errno_t rc = memset_s(wait_status, sizeof(wait_status), 0, sizeof(wait_status));
    securec_check(rc, "\0", "\0");

    /* Values available to all callers */
    if (beentry->st_databaseid != 0 && get_database_name(beentry->st_databaseid) != NULL)
        values[1] = CStringGetTextDatum(get_database_name(beentry->st_databaseid));
    else
        nulls[1] = true;

    if (beentry->st_appname)
        values[2] = CStringGetTextDatum(beentry->st_appname);
    else
        nulls[2] = true;

    values[4] = Int64GetDatum(beentry->st_procpid);
    values[5] = Int64GetDatum(beentry->st_sessionid);

    /* Values only available to same user or superuser */
    if (superuser() || beentry->st_userid == GetUserId()) {
        values[0] = CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);
        values[3] = Int64GetDatum(beentry->st_queryid);
        values[6] = Int32GetDatum(beentry->st_tid);
        if (beentry->st_parent_sessionid != 0) {
            values[7] = Int64GetDatum(beentry->st_parent_sessionid);
        } else {
            nulls[7] = true;
        }
        values[8] = Int32GetDatum(beentry->st_thread_level);
        values[9] = UInt32GetDatum(beentry->st_smpid);

        /* wait_event info */
        if (beentry->st_waitevent != 0) {
            uint32 raw_wait_event;
            /* read only once be atomic */
            raw_wait_event = UINT32_ACCESS_ONCE(beentry->st_waitevent);
            values[10] = CStringGetTextDatum(pgstat_get_waitstatusdesc(raw_wait_event));
            values[11] = CStringGetTextDatum(pgstat_get_wait_event(raw_wait_event));
        } else if (beentry->st_xid != 0) {
            if (beentry->st_waitstatus == STATE_WAIT_XACTSYNC) {
                rc = snprintf_s(wait_status,
                    WAITSTATELEN,
                    WAITSTATELEN - 1,
                    "%s: %lu",
                    WaitStateDesc[beentry->st_waitstatus],
                    beentry->st_xid);
                securec_check_ss(rc, "\0", "\0");
            } else {
                rc =
                    snprintf_s(wait_status, WAITSTATELEN, WAITSTATELEN - 1, "%s", WaitStateDesc[beentry->st_waitstatus]);
                securec_check_ss(rc, "\0", "\0");
            }
            values[10] = CStringGetTextDatum(wait_status);
            nulls[11] = true;
        } else if (beentry->st_relname && beentry->st_relname[0] != '\0' &&
                   (beentry->st_waitstatus == STATE_VACUUM || beentry->st_waitstatus == STATE_ANALYZE ||
                       beentry->st_waitstatus == STATE_VACUUM_FULL)) {
            if (beentry->st_waitstatus_phase != PHASE_NONE) {
                rc = snprintf_s(wait_status,
                    WAITSTATELEN,
                    WAITSTATELEN - 1,
                    "%s: %s, %s",
                    WaitStateDesc[beentry->st_waitstatus],
                    beentry->st_relname,
                    WaitStatePhaseDesc[beentry->st_waitstatus_phase]);
                securec_check_ss(rc, "\0", "\0");
            } else {
                rc = snprintf_s(wait_status,
                    WAITSTATELEN,
                    WAITSTATELEN - 1,
                    "%s: %s",
                    WaitStateDesc[beentry->st_waitstatus],
                    beentry->st_relname);
                securec_check_ss(rc, "\0", "\0");
            }
            values[10] = CStringGetTextDatum(wait_status);
            nulls[11] = true;
        } else if (beentry->st_libpq_wait_nodeid != InvalidOid && beentry->st_libpq_wait_nodecount != 0) {
            if (IS_PGXC_COORDINATOR) {
                NameData node_name = {{0}};
                rc = snprintf_s(wait_status,
                    WAITSTATELEN,
                    WAITSTATELEN - 1,
                    "%s: %s, total %d",
                    WaitStateDesc[beentry->st_waitstatus],
                    get_pgxc_nodename(beentry->st_libpq_wait_nodeid, &node_name),
                    beentry->st_libpq_wait_nodecount);
                securec_check_ss(rc, "\0", "\0");
            } else if (IS_PGXC_DATANODE) {
                if (global_node_definition != NULL && global_node_definition->nodesDefinition &&
                    global_node_definition->num_nodes == beentry->st_numnodes) {
                    AutoMutexLock copyLock(&nodeDefCopyLock);
                    NodeDefinition* nodeDef = global_node_definition->nodesDefinition;
                    copyLock.lock();
                    rc = snprintf_s(wait_status,
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
                        wait_status, WAITSTATELEN, WAITSTATELEN - 1, "%s", WaitStateDesc[beentry->st_waitstatus]);
                    securec_check_ss(rc, "\0", "\0");
                }
            }
            values[10] = CStringGetTextDatum(wait_status);
            nulls[11] = true;
        } else if (beentry->st_nodeid != -1) {
            if (IS_PGXC_COORDINATOR && (Oid)beentry->st_nodeid != InvalidOid) {
                NameData nodename = {{0}};

                if (beentry->st_waitnode_count > 0) {
                    if (beentry->st_waitstatus_phase != PHASE_NONE) {
                        rc = snprintf_s(wait_status,
                            WAITSTATELEN,
                            WAITSTATELEN - 1,
                            "%s: %s, total %d, %s",
                            WaitStateDesc[beentry->st_waitstatus],
                            get_pgxc_nodename(beentry->st_nodeid, &nodename),
                            beentry->st_waitnode_count,
                            WaitStatePhaseDesc[beentry->st_waitstatus_phase]);
                        securec_check_ss(rc, "\0", "\0");
                    } else {
                        rc = snprintf_s(wait_status,
                            WAITSTATELEN,
                            WAITSTATELEN - 1,
                            "%s: %s, total %d",
                            WaitStateDesc[beentry->st_waitstatus],
                            get_pgxc_nodename(beentry->st_nodeid, &nodename),
                            beentry->st_waitnode_count);
                        securec_check_ss(rc, "\0", "\0");
                    }
                } else {
                    if (beentry->st_waitstatus_phase != PHASE_NONE) {
                        rc = snprintf_s(wait_status,
                            WAITSTATELEN,
                            WAITSTATELEN - 1,
                            "%s: %s, %s",
                            WaitStateDesc[beentry->st_waitstatus],
                            get_pgxc_nodename(beentry->st_nodeid, &nodename),
                            WaitStatePhaseDesc[beentry->st_waitstatus_phase]);
                        securec_check_ss(rc, "\0", "\0");
                    } else {
                        rc = snprintf_s(wait_status,
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
                        if (beentry->st_waitstatus_phase != PHASE_NONE) {
                            if (beentry->st_plannodeid != -1) {
                                rc = snprintf_s(wait_status,
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
                                rc = snprintf_s(wait_status,
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
                                rc = snprintf_s(wait_status,
                                    WAITSTATELEN,
                                    WAITSTATELEN - 1,
                                    "%s: %s(%d), total %d",
                                    WaitStateDesc[beentry->st_waitstatus],
                                    nodeDef[beentry->st_nodeid].nodename.data,
                                    beentry->st_plannodeid,
                                    beentry->st_waitnode_count);
                                securec_check_ss(rc, "\0", "\0");
                            } else {
                                rc = snprintf_s(wait_status,
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
                        if (beentry->st_waitstatus_phase != PHASE_NONE) {
                            if (beentry->st_plannodeid != -1) {
                                rc = snprintf_s(wait_status,
                                    WAITSTATELEN,
                                    WAITSTATELEN - 1,
                                    "%s: %s(%d), %s",
                                    WaitStateDesc[beentry->st_waitstatus],
                                    nodeDef[beentry->st_nodeid].nodename.data,
                                    beentry->st_plannodeid,
                                    WaitStatePhaseDesc[beentry->st_waitstatus_phase]);
                                securec_check_ss(rc, "\0", "\0");
                            } else {
                                rc = snprintf_s(wait_status,
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
                                rc = snprintf_s(wait_status,
                                    WAITSTATELEN,
                                    WAITSTATELEN - 1,
                                    "%s: %s(%d)",
                                    WaitStateDesc[beentry->st_waitstatus],
                                    nodeDef[beentry->st_nodeid].nodename.data,
                                    beentry->st_plannodeid);
                                securec_check_ss(rc, "\0", "\0");
                            } else {
                                rc = snprintf_s(wait_status,
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
                        wait_status, WAITSTATELEN, WAITSTATELEN - 1, "%s", WaitStateDesc[beentry->st_waitstatus]);
                    securec_check_ss(rc, "\0", "\0");
                }
            }
            values[10] = CStringGetTextDatum(wait_status);
            nulls[11] = true;
        } else {
            rc = snprintf_s(wait_status, WAITSTATELEN, WAITSTATELEN - 1, "%s", WaitStateDesc[beentry->st_waitstatus]);
            securec_check_ss(rc, "\0", "\0");
            values[10] = CStringGetTextDatum(wait_status);
            nulls[11] = true;
        }
    } else {
        nulls[0] = true;
        nulls[3] = true;
        nulls[6] = true;
        nulls[7] = true;
        nulls[8] = true;
        nulls[9] = true;
        nulls[10] = true;
        nulls[11] = true;
    }
}

/*
 * @Description: get wait status info of all threads in local node.
 * @return: return the status of all threads.
 */
Datum pg_stat_get_status(PG_FUNCTION_ARGS)
{
    int i = 0;
    FuncCallContext* func_ctx = NULL;
    errno_t rc = 0;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext old_context;
        TupleDesc tupdesc;

        func_ctx = SRF_FIRSTCALL_INIT();

        Assert(STATE_WAIT_NUM == sizeof(WaitStateDesc) / sizeof(WaitStateDesc[0]));

        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(12, false);

        TupleDescInitEntry(tupdesc, (AttrNumber)1, "node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "db_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "thread_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)4, "query_id", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)5, "tid", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)6, "sessionid", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)7, "lwtid", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)8, "psessionid", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)9, "tlevel", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)10, "smpid", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)11, "wait_status", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)12, "wait_event", TEXTOID, -1, 0);

        func_ctx->tuple_desc = BlessTupleDesc(tupdesc);

        func_ctx->user_fctx = palloc0(sizeof(int));
        if (PG_ARGISNULL(0)) {
            /* Get all backends */
            func_ctx->max_calls = pgstat_fetch_stat_numbackends();
        } else {
            /*
             * Get one backend - locate by tid.
             *
             * We lookup the backend early, so we can return zero rows if it
             * doesn't exist, instead of returning a single row full of NULLs.
             */
            ThreadId tid = PG_GETARG_INT64(0);
            int i;
            int n = pgstat_fetch_stat_numbackends();

            for (i = 1; i <= n; i++) {
                PgBackendStatus* be = pgstat_fetch_stat_beentry(i);

                if (be != NULL) {
                    if (be->st_procpid == tid) {
                        *(int*)(func_ctx->user_fctx) = i;
                        break;
                    }
                }
            }

            if (*(int*)(func_ctx->user_fctx) == 0)
                /* Pid not found, return zero rows */
                func_ctx->max_calls = 0;
            else
                func_ctx->max_calls = 1;
        }

        MemoryContextSwitchTo(old_context);
    }

    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->call_cntr < func_ctx->max_calls) {
        /* for each row */
        Datum values[12];
        bool nulls[12] = {false};
        HeapTuple tuple = NULL;
        PgBackendStatus* beentry = NULL;

        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        pg_stat_get_beentry(func_ctx, &beentry);
        if (beentry == NULL) {
            for (i = 0; (unsigned int)(i) < sizeof(nulls) / sizeof(nulls[0]); i++) {
                nulls[i] = true;
            }
            tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
            SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
        }

        construct_wait_status(beentry, values, nulls);
        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    } else {
        /* nothing left */
        SRF_RETURN_DONE(func_ctx);
    }
}

/*
 * @Description: get wait status info of all threads in entire cluster.
 * @return: return the status of global threads.
 */
Datum pgxc_stat_get_status(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_MULTIPLE_NODES
    FuncCallContext* func_ctx = NULL;
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        errmsg("unsupported view in single node mode.")));

    SRF_RETURN_DONE(func_ctx);
#else
    FuncCallContext* func_ctx = NULL;
    Datum values[12];
    bool nulls[12] = {false};
    HeapTuple tuple = NULL;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext old_context;
        TupleDesc tupdesc;
        func_ctx = SRF_FIRSTCALL_INIT();

        Assert(STATE_WAIT_NUM == sizeof(WaitStateDesc) / sizeof(WaitStateDesc[0]));
        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples. */
        tupdesc = CreateTemplateTupleDesc(12, false);

        TupleDescInitEntry(tupdesc, (AttrNumber)1, "node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "db_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "thread_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)4, "query_id", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)5, "tid", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)6, "sessionid", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)7, "lwtid", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)8, "psessionid", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)9, "tlevel", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)10, "smpid", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)11, "wait_status", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)12, "wait_event", TEXTOID, -1, 0);

        func_ctx->tuple_desc = BlessTupleDesc(tupdesc);

        /* only for local node to fetch. */
        func_ctx->max_calls = pgstat_fetch_stat_numbackends();

        /* local fetch way. */
        func_ctx->user_fctx = palloc0(sizeof(int));

        MemoryContextSwitchTo(old_context);

        if (func_ctx->user_fctx == NULL) {
            SRF_RETURN_DONE(func_ctx);
        }
    }

    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();
    /* step1. local fetch way. */
    if (func_ctx->call_cntr < func_ctx->max_calls) {
        /* for each row */
        PgBackendStatus* beentry = NULL;
        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        if (*(int*)(func_ctx->user_fctx) > 0) {
            /* Get specific pid slot */
            beentry = pgstat_fetch_stat_beentry(*(int*)(func_ctx->user_fctx));
        } else {
            /* Get the next one in the list */
            beentry = pgstat_fetch_stat_beentry(func_ctx->call_cntr + 1); /* 1-based index */
        }

        /* the last time to fetch on local node, fetch from remote nodes and reset user_fctx for parallel way. */
        if (func_ctx->call_cntr == func_ctx->max_calls - 1) {
            MemoryContext old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);
            func_ctx->user_fctx = getGlobalThreadWaitStatus(func_ctx->tuple_desc);
            MemoryContextSwitchTo(old_context);
            if (func_ctx->user_fctx == NULL) {
                SRF_RETURN_DONE(func_ctx);
            }
        }

        if (beentry == NULL) {
            for (int i = 0; (unsigned int)(i) < sizeof(nulls) / sizeof(nulls[0]); i++) {
                nulls[i] = true;
            }

            tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
            SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
        }

        /* form values and nulls of current beentry. */
        construct_wait_status(beentry, values, nulls);
        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);

        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    } else if (func_ctx->user_fctx) {
        /* step2. fetch way for remote nodes. */
        Tuplestorestate* tupstore = ((ThreadWaitStatusInfo*)func_ctx->user_fctx)->state->tupstore;
        TupleTableSlot* slot = ((ThreadWaitStatusInfo*)func_ctx->user_fctx)->slot;

        if (!tuplestore_gettupleslot(tupstore, true, false, slot)) {
            FreeParallelFunctionState(((ThreadWaitStatusInfo*)func_ctx->user_fctx)->state);
            ExecDropSingleTupleTableSlot(slot);
            pfree_ext(func_ctx->user_fctx);
            func_ctx->user_fctx = NULL;
            SRF_RETURN_DONE(func_ctx);
        }

        for (unsigned int i = 0; i < sizeof(values) / sizeof(values[0]); i++) {
            values[i] = slot_getattr(slot, i + 1, &nulls[i]);
        }

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        (void)ExecClearTuple(slot);

        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    }

    SRF_RETURN_DONE(func_ctx);
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

/*
 * @Description: system function for SQL count. When guc parameter 'pgstat_track_sql_count' is set on
 *			and user selcets view of pg_sql_count, the functoin wiil be called to find user`s SQL count
 *			results from g_instance.stat_cxt.WaitCountStatusList. If system admin selcet the view , the function will be
 * call many times to return all uers` SQL count results.
 * @in  - PG_FUNCTION_ARGS
 * @out - Datum
 */
Datum pg_stat_get_sql_count(PG_FUNCTION_ARGS)
{
    const int SQL_COUNT_ATTR = 26;
    FuncCallContext* func_ctx = NULL;
    Oid ordinary_userid = 0;
    int ordinary_user_idx = 0;
    int i;
    MemoryContext old_context;

    /* the first call the function  */
    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tupdesc;

        func_ctx = SRF_FIRSTCALL_INIT();
        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);
        i = 0;

        /* init the tuple description */
        tupdesc = CreateTemplateTupleDesc(SQL_COUNT_ATTR, false);

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

        func_ctx->tuple_desc = BlessTupleDesc(tupdesc);
        func_ctx->user_fctx = palloc0(sizeof(int));
        Oid userId = GetUserId();

        *(int*)(func_ctx->user_fctx) = 0;
        if (u_sess->attr.attr_common.pgstat_track_activities == false ||
            u_sess->attr.attr_common.pgstat_track_sql_count == false || !CheckUserExist(userId, false)) {
            func_ctx->max_calls = 0;
            if (u_sess->attr.attr_common.pgstat_track_activities == false) {
                /* track_activities is off and report error */
                ereport(LOG, (errcode(ERRCODE_WARNING), (errmsg("GUC parameter 'track_activities' is off"))));
            }
            if (u_sess->attr.attr_common.pgstat_track_sql_count == false) {
                /* track_sql_count is off and report error */
                ereport(LOG, (errcode(ERRCODE_WARNING), (errmsg("GUC parameter 'track_sql_count' is off"))));
            }
            pfree_ext(func_ctx->user_fctx);
            MemoryContextSwitchTo(old_context);
            SRF_RETURN_DONE(func_ctx);
        }

        /* if g_instance.stat_cxt.WaitCountHashTbl is null, re-initSqlCount */
        if (g_instance.stat_cxt.WaitCountHashTbl == NULL) {
            initSqlCount();
        }

        /* find the ordinary user`s  index in g_instance.stat_cxt.WaitCountStatusList */
        if (!superuser()) {
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
        func_ctx->max_calls = ordinary_userid ? 1 : hash_get_num_entries(g_instance.stat_cxt.WaitCountHashTbl);

        MemoryContextSwitchTo(old_context);
    }

    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->call_cntr < func_ctx->max_calls) {
        /* for each row */
        Datum values[SQL_COUNT_ATTR];
        bool nulls[SQL_COUNT_ATTR] = {false};
        HeapTuple tuple = NULL;
        PgStat_WaitCountStatus* wcb = NULL;
        Oid userid;
        char user_name[NAMEDATALEN];
        errno_t ss_rc = EOK;
        int current_idx = ordinary_userid ? ordinary_user_idx : *(int*)(func_ctx->user_fctx);
        int i = -1;
        ss_rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(ss_rc, "\0", "\0");
        ss_rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(ss_rc, "\0", "\0");
        ss_rc = memset_s(user_name, NAMEDATALEN, 0, NAMEDATALEN);
        securec_check(ss_rc, "\0", "\0");

        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);
        LWLockAcquire(WaitCountHashLock, LW_SHARED);
        wcb = wcslist_nthcell_array(current_idx);
        /* if the user has been droped, then find the next exist user  */
        if (!CheckUserExist(wcb->userid, true)) {
            /* find the next exist user in g_instance.stat_cxt.WaitCountStatusList */
            *(int*)(func_ctx->user_fctx) = find_next_user_idx(
                ordinary_userid, func_ctx->call_cntr, func_ctx->max_calls, *(int*)(func_ctx->user_fctx));
            /* no user left, all exist users` SQL count results are return done */
            if (*(int*)(func_ctx->user_fctx) == 0) {
                LWLockRelease(WaitCountHashLock);
                pfree_ext(func_ctx->user_fctx);
                MemoryContextSwitchTo(old_context);
                SRF_RETURN_DONE(func_ctx);
            }

            /* get the next exist user`s SQL count result */
            wcb = wcslist_nthcell_array(*(int*)(func_ctx->user_fctx));
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
        values[++i] = Int64GetDatum(wcb->wc_cnt.selectElapse.avg_time);
        values[++i] = Int64GetDatum(wcb->wc_cnt.selectElapse.max_time);
        values[++i] = Int64GetDatum(wcb->wc_cnt.selectElapse.min_time);
        values[++i] = Int64GetDatum(wcb->wc_cnt.updateElapse.total_time);
        values[++i] = Int64GetDatum(wcb->wc_cnt.updateElapse.avg_time);
        values[++i] = Int64GetDatum(wcb->wc_cnt.updateElapse.max_time);
        values[++i] = Int64GetDatum(wcb->wc_cnt.updateElapse.min_time);
        values[++i] = Int64GetDatum(wcb->wc_cnt.insertElapse.total_time);
        values[++i] = Int64GetDatum(wcb->wc_cnt.insertElapse.avg_time);
        values[++i] = Int64GetDatum(wcb->wc_cnt.insertElapse.max_time);
        values[++i] = Int64GetDatum(wcb->wc_cnt.insertElapse.min_time);
        values[++i] = Int64GetDatum(wcb->wc_cnt.deleteElapse.total_time);
        values[++i] = Int64GetDatum(wcb->wc_cnt.deleteElapse.avg_time);
        values[++i] = Int64GetDatum(wcb->wc_cnt.deleteElapse.max_time);
        values[++i] = Int64GetDatum(wcb->wc_cnt.deleteElapse.min_time);

        /* find the next user for the next function call  */
        LWLockAcquire(WaitCountHashLock, LW_SHARED);

        (*(int*)(func_ctx->user_fctx)) =
            find_next_user_idx(ordinary_userid, func_ctx->call_cntr, func_ctx->max_calls, *(int*)(func_ctx->user_fctx));
        if ((*(int*)(func_ctx->user_fctx)) == 0)
            func_ctx->call_cntr = func_ctx->max_calls;

        LWLockRelease(WaitCountHashLock);

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);

        MemoryContextSwitchTo(old_context);
        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    } else {
        /* nothing left */
        pfree_ext(func_ctx->user_fctx);
        SRF_RETURN_DONE(func_ctx);
    }
}

Datum pg_stat_get_thread(PG_FUNCTION_ARGS)
{
    FuncCallContext* func_ctx = NULL;
    errno_t rc = 0;
    if (SRF_IS_FIRSTCALL()) {
        MemoryContext old_context;
        TupleDesc tupdesc;

        func_ctx = SRF_FIRSTCALL_INIT();

        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(5, false);
        /* This should have been called 'pid';  can't change it. 2011-06-11 */
        TupleDescInitEntry(tupdesc, (AttrNumber)1, "node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "pid", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "lwpid", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)4, "thread_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)5, "creation_time", TIMESTAMPTZOID, -1, 0);

        func_ctx->tuple_desc = BlessTupleDesc(tupdesc);

        func_ctx->user_fctx = palloc0(sizeof(int));

        /* Get all backends */
        func_ctx->max_calls = pgstat_fetch_stat_numbackends();

        MemoryContextSwitchTo(old_context);
    }

    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->call_cntr < func_ctx->max_calls) {
        /* for each row */
        Datum values[5];
        bool nulls[5] = {false};
        HeapTuple tuple = NULL;
        PgBackendStatus* beentry = NULL;

        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        /* Get the next one in the list */
        beentry = pgstat_fetch_stat_beentry(func_ctx->call_cntr + 1); /* 1-based index */
        if (beentry == NULL || beentry->st_procpid == 0) {
            int i;

            for (i = 0; (unsigned int)(i) < sizeof(nulls) / sizeof(nulls[0]); i++) {
                nulls[i] = true;
            }

            nulls[3] = false;
            if (beentry == NULL) {
                values[3] = CStringGetTextDatum("<backend information not available>");
            } else {
                Assert(beentry->st_sessionid != 0);
                values[3] = CStringGetTextDatum("<decoupled session information not displayed>");
            }

            tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
            SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
        }

        /* Values available to all callers */
        values[0] = CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);

        /* Values only available to same user or superuser */
        if (superuser() || beentry->st_userid == GetUserId()) {
            values[1] = Int64GetDatum(beentry->st_procpid);
            values[2] = Int32GetDatum(beentry->st_tid);
            if (beentry->st_appname) {
                values[3] = CStringGetTextDatum(beentry->st_appname);
            } else {
                nulls[3] = true;
            }
            if (beentry->st_state_start_timestamp != 0) {
                values[4] = TimestampTzGetDatum(beentry->st_proc_start_timestamp);
            } else {
                nulls[4] = true;
            }
        } else {
            /* No permissions to view data about this session */
            nulls[1] = true;
            nulls[2] = true;
            values[3] = CStringGetTextDatum("<insufficient privilege>");
            nulls[4] = true;
        }

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);

        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    } else {
        /* nothing left */
        SRF_RETURN_DONE(func_ctx);
    }
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
    FuncCallContext* func_ctx = NULL;

    if (!superuser()) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("permission denied."))));
    }

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext old_context;
        TupleDesc tupdesc;

        func_ctx = SRF_FIRSTCALL_INIT();

        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(7, false);
        /* This should have been called 'pid';  can't change it. 2011-06-11 */
        TupleDescInitEntry(tupdesc, (AttrNumber)1, "node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "host", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "process", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)4, "port", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)5, "installpath", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)6, "datapath", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)7, "log_directory", TEXTOID, -1, 0);

        func_ctx->tuple_desc = BlessTupleDesc(tupdesc);

        func_ctx->user_fctx = palloc0(sizeof(int));
        func_ctx->max_calls = 1;

        MemoryContextSwitchTo(old_context);
    }

    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->call_cntr < func_ctx->max_calls) {
        /* for each row */
        Datum values[7];
        bool nulls[7] = {false};
        HeapTuple tuple = NULL;
        char *node_host = NULL;
        char *install_path = NULL;
        char *data_path = NULL;
        int node_process = 0;
        int node_port = 0;
        char self_path[PATH_MAX] = {0};
        char real_path[PATH_MAX] = {0};

        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        get_network_info(&node_host, &node_port);
        node_process = getpid();
        /* Get envirenment of GAUSSHOME firstly, if it is null, it should get from /proc/self/exe */
        install_path = gs_getenv_r("GAUSSHOME");
        if (install_path == NULL) {
            install_path = self_path;
            int r = readlink("/proc/self/exe", self_path, sizeof(self_path) - 1);
            if (r < 0 || r >= PATH_MAX) {
                install_path = "";
            } else {
                int ret = 0;
                char* ptr = strrchr(self_path, '/');
                if (ptr != NULL) {
                    *ptr = '\0';
                    ret = strcat_s(self_path, PATH_MAX - strlen(self_path), "/..");
                    securec_check_c(ret, "\0", "\0");
                    (void)realpath(self_path, real_path);
                    install_path = real_path;
                }
            }
        }
        char* tmp_install_path = (char*)palloc(strlen(install_path) + 1);
        rc = strcpy_s(tmp_install_path, strlen(install_path) + 1, install_path);
        securec_check(rc, "\0", "\0");

        data_path = t_thrd.proc_cxt.DataDir;
        if (data_path == NULL) {
            data_path = "";
        }
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

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);

        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    } else {
        /* nothing left */
        SRF_RETURN_DONE(func_ctx);
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
    
    if (!superuser()) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("permission denied."))));
    }

    if ((beentry = pgstat_fetch_stat_beentry(beid)) == NULL) {
        PG_RETURN_NULL();
    }
    PG_RETURN_INT64(beentry->st_procpid);
}

Datum pg_stat_get_backend_dbid(PG_FUNCTION_ARGS)
{
    int32 beid = PG_GETARG_INT32(0);
    PgBackendStatus* beentry = NULL;

    if ((beentry = pgstat_fetch_stat_beentry(beid)) == NULL) {
        PG_RETURN_NULL();
    }

    PG_RETURN_OID(beentry->st_databaseid);
}

Datum pg_stat_get_backend_userid(PG_FUNCTION_ARGS)
{
    int32 beid = PG_GETARG_INT32(0);
    PgBackendStatus* beentry = NULL;

    if (!superuser()) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("permission denied."))));
    }

    if ((beentry = pgstat_fetch_stat_beentry(beid)) == NULL) {
        PG_RETURN_NULL();
    }

    PG_RETURN_OID(beentry->st_userid);
}

Datum pg_stat_get_backend_activity(PG_FUNCTION_ARGS)
{
    int32 beid = PG_GETARG_INT32(0);
    PgBackendStatus* beentry = NULL;
    const char* activity = NULL;

    if ((beentry = pgstat_fetch_stat_beentry(beid)) == NULL) {
        activity = "<backend information not available>";
    } else if (!superuser() && beentry->st_userid != GetUserId()) {
        activity = "<insufficient privilege>";
    } else if (*(beentry->st_activity) == '\0') {
        activity = "<command string not enabled>";
    } else {
        activity = beentry->st_activity;
    }

    PG_RETURN_TEXT_P(cstring_to_text(activity));
}

Datum pg_stat_get_backend_waiting(PG_FUNCTION_ARGS)
{
    int32 beid = PG_GETARG_INT32(0);
    bool result = false;
    PgBackendStatus* beentry = NULL;

    if ((beentry = pgstat_fetch_stat_beentry(beid)) == NULL) {
        PG_RETURN_NULL();
    }

    if (!superuser() && beentry->st_userid != GetUserId()) {
        PG_RETURN_NULL();
    }

    result = pgstat_get_waitlock(beentry->st_waitevent);

    PG_RETURN_BOOL(result);
}

Datum pg_stat_get_backend_activity_start(PG_FUNCTION_ARGS)
{
    int32 beid = PG_GETARG_INT32(0);
    TimestampTz result;
    PgBackendStatus* beentry = NULL;

    if ((beentry = pgstat_fetch_stat_beentry(beid)) == NULL) {
        PG_RETURN_NULL();
    }

    if (!superuser() && beentry->st_userid != GetUserId()) {
        PG_RETURN_NULL();
    }

    result = beentry->st_activity_start_timestamp;

    /*
     * No time recorded for start of current query -- this is the case if the
     * user hasn't enabled query-level stats collection.
     */
    if (result == 0) {
        PG_RETURN_NULL();
    }

    PG_RETURN_TIMESTAMPTZ(result);
}

Datum pg_stat_get_backend_xact_start(PG_FUNCTION_ARGS)
{
    int32 beid = PG_GETARG_INT32(0);
    TimestampTz result;
    PgBackendStatus* beentry = NULL;

    if ((beentry = pgstat_fetch_stat_beentry(beid)) == NULL) {
        PG_RETURN_NULL();
    }

    if (!superuser() && beentry->st_userid != GetUserId()) {
        PG_RETURN_NULL();
    }

    result = beentry->st_xact_start_timestamp;

    if (result == 0) { /* not in a transaction */
        PG_RETURN_NULL();
    }

    PG_RETURN_TIMESTAMPTZ(result);
}

Datum pg_stat_get_backend_start(PG_FUNCTION_ARGS)
{
    int32 beid = PG_GETARG_INT32(0);
    TimestampTz result;
    PgBackendStatus* beentry = NULL;

    if ((beentry = pgstat_fetch_stat_beentry(beid)) == NULL) {
        PG_RETURN_NULL();
    }

    if (!superuser() && beentry->st_userid != GetUserId()) {
        PG_RETURN_NULL();
    }

    result = beentry->st_proc_start_timestamp;

    if (result == 0) { /* probably can't happen? */
        PG_RETURN_NULL();
    }

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

    if ((beentry = pgstat_fetch_stat_beentry(beid)) == NULL) {
        PG_RETURN_NULL();
    }

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

    if ((beentry = pgstat_fetch_stat_beentry(beid)) == NULL) {
        PG_RETURN_NULL();
    }

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
    Oid db_id = PG_GETARG_OID(0);
    int32 result;
    int tot_backends = pgstat_fetch_stat_numbackends();
    int beid;

    result = 0;
    for (beid = 1; beid <= tot_backends; beid++) {
        PgBackendStatus* beentry = pgstat_fetch_stat_beentry(beid);

        if ((beentry != NULL) && beentry->st_databaseid == db_id) {
            result++;
        }
    }

    PG_RETURN_INT32(result);
}

Datum pg_stat_get_db_xact_commit(PG_FUNCTION_ARGS)
{
    Oid db_id = PG_GETARG_OID(0);
    int64 result;
    PgStat_StatDBEntry* db_entry = NULL;

    if ((db_entry = pgstat_fetch_stat_dbentry(db_id)) == NULL) {
        result = 0;
    } else {
        result = (int64)(db_entry->n_xact_commit);
    }

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_db_xact_rollback(PG_FUNCTION_ARGS)
{
    Oid db_id = PG_GETARG_OID(0);
    int64 result;
    PgStat_StatDBEntry* db_entry = NULL;

    if ((db_entry = pgstat_fetch_stat_dbentry(db_id)) == NULL) {
        result = 0;
    } else {
        result = (int64)(db_entry->n_xact_rollback);
    }

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_db_blocks_fetched(PG_FUNCTION_ARGS)
{
    Oid db_id = PG_GETARG_OID(0);
    int64 result;
    PgStat_StatDBEntry* db_entry = NULL;

    if ((db_entry = pgstat_fetch_stat_dbentry(db_id)) == NULL) {
        result = 0;
    } else {
        result = (int64)(db_entry->n_blocks_fetched);
    }

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_db_blocks_hit(PG_FUNCTION_ARGS)
{
    Oid db_id = PG_GETARG_OID(0);
    int64 result;
    PgStat_StatDBEntry* db_entry = NULL;

    if ((db_entry = pgstat_fetch_stat_dbentry(db_id)) == NULL) {
        result = 0;
    } else {
        result = (int64)(db_entry->n_blocks_hit);
    }

    PG_RETURN_INT64(result);
}

/**
 * @Description:  get database cu stat mem hit
 * @return  datum
 */
Datum pg_stat_get_db_cu_mem_hit(PG_FUNCTION_ARGS)
{
    Oid db_id = PG_GETARG_OID(0);
    int64 result;
    PgStat_StatDBEntry* db_entry = NULL;

    if ((db_entry = pgstat_fetch_stat_dbentry(db_id)) == NULL) {
        result = 0;
    } else {
        result = (int64)(db_entry->n_cu_mem_hit);
    }

    PG_RETURN_INT64(result);
}

/**
 * @Description:  get database cu stat hdd sync read
 * @return  datum
 */
Datum pg_stat_get_db_cu_hdd_sync(PG_FUNCTION_ARGS)
{
    Oid db_id = PG_GETARG_OID(0);
    int64 result;
    PgStat_StatDBEntry* db_entry = NULL;

    if ((db_entry = pgstat_fetch_stat_dbentry(db_id)) == NULL) {
        result = 0;
    } else {
        result = (int64)(db_entry->n_cu_hdd_sync);
    }

    PG_RETURN_INT64(result);
}

/**
 * @Description:  get database cu stat hdd async read
 * @return  datum
 */
Datum pg_stat_get_db_cu_hdd_asyn(PG_FUNCTION_ARGS)
{
    Oid db_id = PG_GETARG_OID(0);
    int64 result;
    PgStat_StatDBEntry* db_entry = NULL;

    if ((db_entry = pgstat_fetch_stat_dbentry(db_id)) == NULL) {
        result = 0;
    } else {
        result = (int64)(db_entry->n_cu_hdd_asyn);
    }

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_db_tuples_returned(PG_FUNCTION_ARGS)
{
    Oid db_id = PG_GETARG_OID(0);
    int64 result;
    PgStat_StatDBEntry* db_entry = NULL;

    if ((db_entry = pgstat_fetch_stat_dbentry(db_id)) == NULL) {
        result = 0;
    } else {
        result = (int64)(db_entry->n_tuples_returned);
    }

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_db_tuples_fetched(PG_FUNCTION_ARGS)
{
    Oid db_id = PG_GETARG_OID(0);
    int64 result;
    PgStat_StatDBEntry* db_entry = NULL;

    if ((db_entry = pgstat_fetch_stat_dbentry(db_id)) == NULL) {
        result = 0;
    } else {
        result = (int64)(db_entry->n_tuples_fetched);
    }

    PG_RETURN_INT64(result);
}

/* fetch tuples stat info for total database */
static int64 pgxc_exec_db_stat(Oid db_id, char* funcname, RemoteQueryExecType exec_type)
{
    ExecNodes* exec_nodes = NULL;
    StringInfoData buf;
    ParallelFunctionState* state = NULL;
    int64 result = 0;
    char* databasename = NULL;

    databasename = get_database_name(db_id);
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
    Oid db_id = PG_GETARG_OID(0);
    int64 result = 0;
    PgStat_StatDBEntry* db_entry = NULL;
    if (IS_PGXC_COORDINATOR) {
        PG_RETURN_INT64(pgxc_exec_db_stat(db_id, "pg_stat_get_db_tuples_inserted", EXEC_ON_DATANODES));
    }
    if ((db_entry = pgstat_fetch_stat_dbentry(db_id)) != NULL) {
        result = (int64)(db_entry->n_tuples_inserted);
    }
    PG_RETURN_INT64(result);
}

Datum pg_stat_get_db_tuples_updated(PG_FUNCTION_ARGS)
{
    Oid db_id = PG_GETARG_OID(0);
    int64 result = 0;
    PgStat_StatDBEntry* db_entry = NULL;
    if (IS_PGXC_COORDINATOR) {
        PG_RETURN_INT64(pgxc_exec_db_stat(db_id, "pg_stat_get_db_tuples_updated", EXEC_ON_DATANODES));
    }
    if ((db_entry = pgstat_fetch_stat_dbentry(db_id)) != NULL) {
        result = (int64)(db_entry->n_tuples_updated);
    }
    PG_RETURN_INT64(result);
}

Datum pg_stat_get_db_tuples_deleted(PG_FUNCTION_ARGS)
{
    Oid db_id = PG_GETARG_OID(0);
    int64 result = 0;
    PgStat_StatDBEntry* db_entry = NULL;
    if (IS_PGXC_COORDINATOR) {
        PG_RETURN_INT64(pgxc_exec_db_stat(db_id, "pg_stat_get_db_tuples_deleted", EXEC_ON_DATANODES));
    }
    if ((db_entry = pgstat_fetch_stat_dbentry(db_id)) != NULL) {
        result = (int64)(db_entry->n_tuples_deleted);
    }
    PG_RETURN_INT64(result);
}

Datum pg_stat_get_db_stat_reset_time(PG_FUNCTION_ARGS)
{
    Oid db_id = PG_GETARG_OID(0);
    TimestampTz result = 0;
    PgStat_StatDBEntry* db_entry = NULL;
    if ((db_entry = pgstat_fetch_stat_dbentry(db_id)) != NULL) {
        result = db_entry->stat_reset_timestamp;
    }
    if (result == 0) {
        PG_RETURN_NULL();
    } else {
        PG_RETURN_TIMESTAMPTZ(result);
    }
}

Datum pg_stat_get_db_temp_files(PG_FUNCTION_ARGS)
{
    Oid db_id = PG_GETARG_OID(0);
    int64 result;
    PgStat_StatDBEntry* db_entry = NULL;
    if ((db_entry = pgstat_fetch_stat_dbentry(db_id)) == NULL) {
        result = 0;
    } else {
        result = db_entry->n_temp_files;
    }

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_db_temp_bytes(PG_FUNCTION_ARGS)
{
    Oid db_id = PG_GETARG_OID(0);
    int64 result;
    PgStat_StatDBEntry* db_entry = NULL;
    if ((db_entry = pgstat_fetch_stat_dbentry(db_id)) == NULL) {
        result = 0;
    } else {
        result = db_entry->n_temp_bytes;
    }

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_db_conflict_tablespace(PG_FUNCTION_ARGS)
{
    Oid db_id = PG_GETARG_OID(0);
    int64 result = 0;
    PgStat_StatDBEntry* db_entry = NULL;
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        result = pgxc_exec_db_stat(db_id, "pg_stat_get_db_conflict_tablespace", EXEC_ON_ALL_NODES);
    }
    if ((db_entry = pgstat_fetch_stat_dbentry(db_id)) != NULL) {
        result += (int64)(db_entry->n_conflict_tablespace);
    }

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_db_conflict_lock(PG_FUNCTION_ARGS)
{
    Oid db_id = PG_GETARG_OID(0);
    int64 result = 0;
    PgStat_StatDBEntry* db_entry = NULL;
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        result = pgxc_exec_db_stat(db_id, "pg_stat_get_db_conflict_lock", EXEC_ON_ALL_NODES);
    }
    if ((db_entry = pgstat_fetch_stat_dbentry(db_id)) != NULL) {
        result += (int64)(db_entry->n_conflict_lock);
    }
    PG_RETURN_INT64(result);
}

Datum pg_stat_get_db_conflict_snapshot(PG_FUNCTION_ARGS)
{
    Oid db_id = PG_GETARG_OID(0);
    int64 result = 0;
    PgStat_StatDBEntry* db_entry = NULL;
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        result = pgxc_exec_db_stat(db_id, "pg_stat_get_db_conflict_snapshot", EXEC_ON_ALL_NODES);
    }
    if ((db_entry = pgstat_fetch_stat_dbentry(db_id)) != NULL) {
        result += (int64)(db_entry->n_conflict_snapshot);
    }
    PG_RETURN_INT64(result);
}

Datum pg_stat_get_db_conflict_bufferpin(PG_FUNCTION_ARGS)
{
    Oid db_id = PG_GETARG_OID(0);
    int64 result = 0;
    PgStat_StatDBEntry* db_entry = NULL;
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        result = pgxc_exec_db_stat(db_id, "pg_stat_get_db_conflict_bufferpin", EXEC_ON_ALL_NODES);
    }
    if ((db_entry = pgstat_fetch_stat_dbentry(db_id)) != NULL) {
        result += (int64)(db_entry->n_conflict_bufferpin);
    }
    PG_RETURN_INT64(result);
}

Datum pg_stat_get_db_conflict_startup_deadlock(PG_FUNCTION_ARGS)
{
    Oid db_id = PG_GETARG_OID(0);
    int64 result = 0;
    PgStat_StatDBEntry* db_entry = NULL;
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        result = pgxc_exec_db_stat(db_id, "pg_stat_get_db_conflict_startup_deadlock", EXEC_ON_ALL_NODES);
    }
    if ((db_entry = pgstat_fetch_stat_dbentry(db_id)) != NULL) {
        result += (int64)(db_entry->n_conflict_startup_deadlock);
    }
    PG_RETURN_INT64(result);
}

Datum pg_stat_get_db_conflict_all(PG_FUNCTION_ARGS)
{
    Oid db_id = PG_GETARG_OID(0);
    int64 result = 0;
    PgStat_StatDBEntry* db_entry = NULL;
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        result = pgxc_exec_db_stat(db_id, "pg_stat_get_db_conflict_all", EXEC_ON_ALL_NODES);
    }
    if ((db_entry = pgstat_fetch_stat_dbentry(db_id)) != NULL) {
        result += (int64)(db_entry->n_conflict_tablespace + db_entry->n_conflict_lock + db_entry->n_conflict_snapshot +
                          db_entry->n_conflict_bufferpin + db_entry->n_conflict_startup_deadlock);
    }
    PG_RETURN_INT64(result);
}

Datum pg_stat_get_db_deadlocks(PG_FUNCTION_ARGS)
{
    Oid db_id = PG_GETARG_OID(0);
    int64 result = 0;
    PgStat_StatDBEntry* db_entry = NULL;
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        result = pgxc_exec_db_stat(db_id, "pg_stat_get_db_deadlocks", EXEC_ON_ALL_NODES);
    }
    if ((db_entry = pgstat_fetch_stat_dbentry(db_id)) != NULL) {
        result += (int64)(db_entry->n_deadlocks);
    }
    PG_RETURN_INT64(result);
}

Datum pg_stat_get_db_blk_read_time(PG_FUNCTION_ARGS)
{
    Oid db_id = PG_GETARG_OID(0);
    double result;
    PgStat_StatDBEntry* db_entry = NULL;
    /* convert counter from microsec to millisec for display */
    if ((db_entry = pgstat_fetch_stat_dbentry(db_id)) == NULL) {
        result = 0;
    } else {
        result = ((double) db_entry->n_block_read_time) / 1000.0;
    }
    PG_RETURN_FLOAT8(result);
}

Datum pg_stat_get_db_blk_write_time(PG_FUNCTION_ARGS)
{
    Oid db_id = PG_GETARG_OID(0);
    double result;
    PgStat_StatDBEntry* db_entry = NULL;
    /* convert counter from microsec to millisec for display */
    if ((db_entry = pgstat_fetch_stat_dbentry(db_id)) == NULL) {
        result = 0;
    } else {
        result = ((double) db_entry->n_block_write_time) / 1000.0;
    }
    PG_RETURN_FLOAT8(result);
}

Datum pg_stat_get_mem_mbytes_reserved(PG_FUNCTION_ARGS)
{
    ThreadId pid = PG_GETARG_INT64(0);
    const char* result = "GetFailed";

    if (pid == 0) {
        pid = (IS_THREAD_POOL_WORKER ? u_sess->session_id : t_thrd.proc_cxt.MyProcPid);
    }
    StringInfoData buf;

    pgstat_get_current_active_numbackends();

    if (pid <= 0) {
        PG_RETURN_TEXT_P(cstring_to_text(result));
    }
    initStringInfo(&buf);

    /* get backend status with thread id */
    PgBackendStatus* beentry = pgstat_get_backend_entry(pid);

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

        if (beentry->st_debug_info) {
            WLMGetDebugInfo(&buf, (WLMDebugInfo *) beentry->st_debug_info);
        }
        if (beentry->st_cgname) {
            appendStringInfo(&buf, _("ControlGroup: %s\n"), beentry->st_cgname);
        }
        appendStringInfo(&buf, _("IOSTATE: %d\n"), beentry->st_io_state);

        result = buf.data;
    }

    PG_RETURN_TEXT_P(cstring_to_text(result));
}

/* print the data structure info of workload manager */
Datum pg_stat_get_workload_struct_info(PG_FUNCTION_ARGS)
{
    text* result = NULL;

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

    if (result == NULL) {
        result = "FailedToGetSessionInfo";
    }

    PG_RETURN_TEXT_P(cstring_to_text(result));
}

/*
 * function name: pg_stat_get_wlm_realtime_session_info
 * description  : the view will show real time resource in use.
 */
Datum pg_stat_get_wlm_realtime_session_info(PG_FUNCTION_ARGS)
{
    const int WLM_REALTIME_SESSION_INFO_ATTRNUM = 56;
    FuncCallContext* func_ctx = NULL;
    int num = 0;
    int i = 0;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext old_context;
        TupleDesc tupdesc;
        i = 0;

        func_ctx = SRF_FIRSTCALL_INIT();

        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);
        tupdesc = CreateTemplateTupleDesc(WLM_REALTIME_SESSION_INFO_ATTRNUM, false);
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
        func_ctx->tuple_desc = BlessTupleDesc(tupdesc);
        func_ctx->user_fctx = WLMGetSessionStatistics(&num);
        func_ctx->max_calls = num;
        MemoryContextSwitchTo(old_context);
        if (func_ctx->user_fctx == NULL) {
            SRF_RETURN_DONE(func_ctx);
        }
    }

    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->user_fctx && func_ctx->call_cntr < func_ctx->max_calls) {
        const int TOP5 = 5;
        Datum values[WLM_REALTIME_SESSION_INFO_ATTRNUM];
        bool nulls[WLM_REALTIME_SESSION_INFO_ATTRNUM] = {false};
        HeapTuple tuple = NULL;
        Datum result;
        int i = -1;

        char warnstr[WARNING_INFO_LEN] = {0};

        WLMSessionStatistics* statistics = (WLMSessionStatistics*)func_ctx->user_fctx + func_ctx->call_cntr;

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
        if (statistics->query_band && statistics->query_band[0]) {
            values[++i] = CStringGetTextDatum(statistics->query_band);
        } else {
            nulls[++i] = true;
        }
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
        if (warnstr[0]) {
            values[++i] = CStringGetTextDatum(warnstr);
        } else {
            nulls[++i] = true;
        }
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
        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(func_ctx, result);
    }

    pfree_ext(func_ctx->user_fctx);
    func_ctx->user_fctx = NULL;

    SRF_RETURN_DONE(func_ctx);
}

/*
 * @Description:  get realtime ec operator information from hashtable.
 * @return - Datum
 */
Datum pg_stat_get_wlm_realtime_ec_operator_info(PG_FUNCTION_ARGS)
{
#define OPERATOR_REALTIME_SESSION_EC_INFO_ATTRNUM 12
    FuncCallContext* func_ctx = NULL;
    int num = 0;
    int i = 0;;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext old_context;
        TupleDesc tupdesc;
        i = 0;

        func_ctx = SRF_FIRSTCALL_INIT();

        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(OPERATOR_REALTIME_SESSION_EC_INFO_ATTRNUM, false);
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

        func_ctx->tuple_desc = BlessTupleDesc(tupdesc);
        func_ctx->user_fctx = ExplainGetSessionStatistics(&num);
        func_ctx->max_calls = num;
        MemoryContextSwitchTo(old_context);
        if (func_ctx->user_fctx == NULL) {
            SRF_RETURN_DONE(func_ctx);
        }
    }

    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->user_fctx && func_ctx->call_cntr < func_ctx->max_calls) {
        Datum values[OPERATOR_REALTIME_SESSION_EC_INFO_ATTRNUM];
        bool nulls[OPERATOR_REALTIME_SESSION_EC_INFO_ATTRNUM] = {false};
        HeapTuple tuple = NULL;
        Datum result;
        int i = -1;
        errno_t rc = EOK;

        ExplainGeneralInfo* statistics = (ExplainGeneralInfo*)func_ctx->user_fctx + func_ctx->call_cntr;

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
            values[++i] = CStringGetTextDatum(statistics->ec_query);
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

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(func_ctx, result);
    }

    pfree_ext(func_ctx->user_fctx);
    func_ctx->user_fctx = NULL;

    SRF_RETURN_DONE(func_ctx);
}

/*
 * @Description:  get realtime operator information from hashtable.
 * @return - Datum
 */
Datum pg_stat_get_wlm_realtime_operator_info(PG_FUNCTION_ARGS)
{
#define OPERATOR_REALTIME_SESSION_INFO_ATTRNUM 23
    FuncCallContext* func_ctx = NULL;
    int num = 0;
    int i = 0;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext old_context;
        TupleDesc tupdesc;
        i = 0;

        func_ctx = SRF_FIRSTCALL_INIT();

        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(OPERATOR_REALTIME_SESSION_INFO_ATTRNUM, false);
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

        func_ctx->tuple_desc = BlessTupleDesc(tupdesc);
        func_ctx->user_fctx = ExplainGetSessionStatistics(&num);
        func_ctx->max_calls = num;

        MemoryContextSwitchTo(old_context);

        if (func_ctx->user_fctx == NULL) {
            SRF_RETURN_DONE(func_ctx);
        }
    }

    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->user_fctx && func_ctx->call_cntr < func_ctx->max_calls) {
        Datum values[OPERATOR_REALTIME_SESSION_INFO_ATTRNUM];
        bool nulls[OPERATOR_REALTIME_SESSION_INFO_ATTRNUM] = {false};
        HeapTuple tuple = NULL;
        Datum result;
        int i = -1;
        char warnstr[WARNING_INFO_LEN] = {0};
        errno_t rc = EOK;

        ExplainGeneralInfo* statistics = (ExplainGeneralInfo*)func_ctx->user_fctx + func_ctx->call_cntr;

        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        WLMGetWarnInfo(warnstr, sizeof(warnstr), statistics->warn_prof_info, statistics->max_spill_size, 0);
        values[++i] = Int64GetDatum(statistics->query_id);
        values[++i] = Int64GetDatum(statistics->tid);
        values[++i] = Int32GetDatum(statistics->plan_node_id);
        values[++i] = CStringGetTextDatum(statistics->plan_node_name);
        if (statistics->start_time == 0) {
            nulls[++i] = true;
        } else {
            values[++i] = TimestampTzGetDatum(statistics->start_time);
        }
        values[++i] = Int64GetDatum(statistics->duration_time);
        if (statistics->status) {
            values[++i] = CStringGetTextDatum("finished");
        } else {
            values[++i] = CStringGetTextDatum("running");
        }
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
        if (warnstr[0]) {
            values[++i] = CStringGetTextDatum(warnstr);
        } else {
            nulls[++i] = true;
        }

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(func_ctx, result);
    }

    pfree_ext(func_ctx->user_fctx);
    func_ctx->user_fctx = NULL;

    SRF_RETURN_DONE(func_ctx);
}

/*
 * function name: pg_stat_get_wlm_statistics
 * description  : the view will show the info which exception is handled.
 */
Datum pg_stat_get_wlm_statistics(PG_FUNCTION_ARGS)
{
#define WLM_STATISTICS_NUM 9
    FuncCallContext* func_ctx = NULL;
    int num = 0;
    int i = 0;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext old_context;
        TupleDesc tupdesc;
        i = 0;

        func_ctx = SRF_FIRSTCALL_INIT();

        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(WLM_STATISTICS_NUM, false);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "statement", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "block_time", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "elapsed_time", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "total_cpu_time", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "qualification_time", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "skew_percent", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "control_group", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "status", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "action", TEXTOID, -1, 0);

        func_ctx->tuple_desc = BlessTupleDesc(tupdesc);
        func_ctx->user_fctx = WLMGetStatistics(&num);
        func_ctx->max_calls = num;

        MemoryContextSwitchTo(old_context);

        if (func_ctx->user_fctx == NULL) {
            SRF_RETURN_DONE(func_ctx);
        }
    }

    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->user_fctx && func_ctx->call_cntr < func_ctx->max_calls) {
        Datum values[WLM_STATISTICS_NUM];
        bool nulls[WLM_STATISTICS_NUM] = {false};
        HeapTuple tuple = NULL;
        Datum result;
        int i = -1;

        WLMStatistics* statistics = (WLMStatistics*)func_ctx->user_fctx + func_ctx->call_cntr;

        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        /* Locking is probably not really necessary */
        values[++i] = CStringGetTextDatum(statistics->stmt);
        values[++i] = Int64GetDatum(statistics->blocktime);
        values[++i] = Int64GetDatum(statistics->elapsedtime);
        values[++i] = Int64GetDatum(statistics->totalcputime);
        values[++i] = Int64GetDatum(statistics->qualitime);
        values[++i] = Int32GetDatum(statistics->skewpercent);
        values[++i] = CStringGetTextDatum(statistics->cgroup);
        values[++i] = CStringGetTextDatum(statistics->status);
        values[++i] = CStringGetTextDatum(statistics->action);

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(func_ctx, result);
    }

    pfree_ext(func_ctx->user_fctx);
    func_ctx->user_fctx = NULL;

    SRF_RETURN_DONE(func_ctx);
}

/*
 * function name: pg_stat_get_session_wlmstat
 * description  : the view will show current workload state.
 */
Datum pg_stat_get_session_wlmstat(PG_FUNCTION_ARGS)
{
#define SESSION_WLMSTAT_NUM 21
    FuncCallContext* func_ctx = NULL;
    int i = 0;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext old_context;
        TupleDesc tupdesc;
        i = 0;

        func_ctx = SRF_FIRSTCALL_INIT();

        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(SESSION_WLMSTAT_NUM, false);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "datid", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "threadid", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "sessionid", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "threadpid", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "usesysid", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "application_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "statement", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "priority", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "block_time", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "elapsed_time", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "total_cpu_time", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "skew_percent", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "statement_mem", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "active_points", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "dop_value", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "cgroup", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "status", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "enqueue", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "attribute", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "is_plana", BOOLOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "node_group", TEXTOID, -1, 0);

        func_ctx->tuple_desc = BlessTupleDesc(tupdesc);

        func_ctx->user_fctx = palloc0(sizeof(int));
        if (PG_ARGISNULL(0)) {
            /* Get all backends */
            func_ctx->max_calls = pgstat_fetch_stat_numbackends();
        } else {
            /*
             * Get one backend - locate by pid.
             *
             * We lookup the backend early, so we can return zero rows if it
             * doesn't exist, instead of returning a single row full of NULLs.
             */
            ThreadId pid = PG_GETARG_INT64(0);
            int n = pgstat_fetch_stat_numbackends();

            for (i = 1; i <= n; ++i) {
                PgBackendStatus* be = pgstat_fetch_stat_beentry(i);

                if (be != NULL) {
                    if (be->st_procpid == pid) {
                        *(int*)(func_ctx->user_fctx) = i;
                        break;
                    }
                }
            }

            if (*(int*)(func_ctx->user_fctx) == 0) {
                /* Pid not found, return zero rows */
                func_ctx->max_calls = 0;
            } else {
                func_ctx->max_calls = 1;
            }
        }

        MemoryContextSwitchTo(old_context);
    }

    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->call_cntr < func_ctx->max_calls) {
        /* for each row */
        Datum values[SESSION_WLMSTAT_NUM];
        bool nulls[SESSION_WLMSTAT_NUM] = {false};
        HeapTuple tuple = NULL;
        PgBackendStatus* beentry = NULL;

        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        pg_stat_get_beentry(func_ctx, &beentry);
        if (beentry == NULL) {
            for (i = 0; (unsigned int) (i) < lengthof(nulls); ++i) {
                nulls[i] = true;
            }
            nulls[6] = false;
            values[6] = CStringGetTextDatum("<backend information not available>");
            tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
            SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
        }

        i = -1;

        /* Values available to all callers */
        values[++i] = ObjectIdGetDatum(beentry->st_databaseid);
        values[++i] = Int64GetDatum(beentry->st_procpid);
        values[++i] = Int64GetDatum(beentry->st_sessionid);
        values[++i] = Int32GetDatum(beentry->st_tid);
        values[++i] = ObjectIdGetDatum(beentry->st_userid);
        if (beentry->st_appname) {
            values[++i] = CStringGetTextDatum(beentry->st_appname);
        } else {
            nulls[++i] = true;
        }

        /* Values only available to same user or superuser */
        if (superuser() || beentry->st_userid == GetUserId()) {
            pgstat_refresh_statement_wlm_time(beentry);

            if (beentry->st_state == STATE_UNDEFINED || beentry->st_state == STATE_DISABLED) {
                nulls[++i] = true;
            } else {
                values[++i] = CStringGetTextDatum(beentry->st_activity);
            }

            /* use backend state to get all workload info */
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
            if (backstat->status) {
                values[++i] = CStringGetTextDatum(backstat->status);
            } else {
                values[++i] = CStringGetTextDatum("unknown");
            }
            if (backstat->enqueue) {
                values[++i] = CStringGetTextDatum(backstat->enqueue);
            } else {
                nulls[++i] = true;
            }
            if (backstat->qtype) {
                values[++i] = CStringGetTextDatum(backstat->qtype);
            } else {
                values[++i] = CStringGetTextDatum("Unknown");
            }
            values[++i] = BoolGetDatum(backstat->is_planA);
            if (StringIsValid(backstat->groupname)) {
                values[++i] = CStringGetTextDatum(backstat->groupname);
            } else {
                values[++i] = CStringGetTextDatum("installation");
            }
        } else {
            nulls[6] = false;
            values[6] = CStringGetTextDatum("<insufficient privilege>");

            for (i = 7; i < SESSION_WLMSTAT_NUM; ++i) {
                nulls[i] = true;
            }
        }

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);

        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    } else {
        /* nothing left */
        SRF_RETURN_DONE(func_ctx);
    }
}

/*
 * function name: pg_stat_get_session_wlmstat
 * description  : the view will show current workload state.
 */
Datum pg_stat_get_session_respool(PG_FUNCTION_ARGS)
{
#define SESSION_WLMSTAT_RESPOOL_NUM 7
    FuncCallContext* func_ctx = NULL;
    int i = 0;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext old_context;
        TupleDesc tupdesc;
        i = 0;

        func_ctx = SRF_FIRSTCALL_INIT();

        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(SESSION_WLMSTAT_RESPOOL_NUM, false);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "datid", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "threadid", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "sessionid", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "threadpid", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "usesysid", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "cgroup", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "srespool", NAMEOID, -1, 0);

        func_ctx->tuple_desc = BlessTupleDesc(tupdesc);

        func_ctx->user_fctx = palloc0(sizeof(int));

        func_ctx->max_calls = pgstat_fetch_stat_numbackends();

        MemoryContextSwitchTo(old_context);
    }

    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->call_cntr < func_ctx->max_calls) {
        /* for each row */
        Datum values[SESSION_WLMSTAT_RESPOOL_NUM];
        bool nulls[SESSION_WLMSTAT_RESPOOL_NUM] = {false};
        HeapTuple tuple = NULL;
        PgBackendStatus* beentry = NULL;

        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        pg_stat_get_beentry(func_ctx, &beentry);
        if (beentry == NULL) {
            for (i = 0; (unsigned int)(i) < lengthof(nulls); ++i) {
                nulls[i] = true;
            }

            tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
            SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
        }

        i = -1;

        /* Values available to all callers */
        values[++i] = ObjectIdGetDatum(beentry->st_databaseid);
        values[++i] = Int64GetDatum(beentry->st_procpid);
        values[++i] = Int64GetDatum(beentry->st_sessionid);
        values[++i] = Int32GetDatum(beentry->st_tid);
        values[++i] = ObjectIdGetDatum(beentry->st_userid);

        /* Values only available to same user or superuser */
        if (superuser() || beentry->st_userid == GetUserId()) {
            pgstat_refresh_statement_wlm_time(beentry);

            /* use backend state to get all workload info */
            WLMStatistics* backstat = (WLMStatistics*)&beentry->st_backstat;

            values[++i] = CStringGetTextDatum(backstat->cgroup);

            if (ENABLE_WORKLOAD_CONTROL && *backstat->srespool) {
                values[++i] = NameGetDatum((Name)backstat->srespool);
            } else {
                values[++i] = DirectFunctionCall1(namein, CStringGetDatum("unknown"));
            }
        } else {
            for (i = 5; i < SESSION_WLMSTAT_RESPOOL_NUM; ++i) {
                nulls[i] = true;
            }
        }

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);

        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    } else {
        /* nothing left */
        SRF_RETURN_DONE(func_ctx);
    }
}

/*
 * function name: pg_stat_get_wlm_session_info_internal
 * description  : Called by pg_stat_get_wlm_session_info
 *                as internal function. Fetch session info
 *                of the finished query from each data nodes.
 */
Datum pg_stat_get_wlm_session_info_internal(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_MULTIPLE_NODES
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported proc in single node mode.")));
    PG_RETURN_NULL();
#else
    Oid procId = PG_GETARG_OID(0);
    uint64 queryId = PG_GETARG_INT64(1);
    TimestampTz stamp = PG_GETARG_INT64(2);
    Oid removed = PG_GETARG_OID(3);

    char* result = "FailedToGetSessionInfo";

    if (IS_PGXC_COORDINATOR) {
        result = "GetWLMSessionInfo";
    }
    if (IS_PGXC_DATANODE) {
        Qid qid = {procId, queryId, stamp};

        /* get session info with qid */
        result = (char*)WLMGetSessionInfo(&qid, removed, NULL);
        /* no result, failed to get session info */
        if (result == NULL) {
            result = "FailedToGetSessionInfo";
        }
    }
    PG_RETURN_TEXT_P(cstring_to_text(result));
#endif
}

void pg_stat_get_beentry(FuncCallContext* func_ctx, PgBackendStatus** beentry)
{
    if (*(int*)(func_ctx->user_fctx) > 0) {
        /* Get specific pid slot */
        *beentry = pgstat_fetch_stat_beentry(*(int*)(func_ctx->user_fctx));
    } else {
        /* Get the next one in the list */
        *beentry = pgstat_fetch_stat_beentry(func_ctx->call_cntr + 1); /* 1-based index */
    }
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
 * @Description: get user resource info
 * @IN void
 * @Return: records
 * @See also:
 */
Datum pg_stat_get_wlm_user_iostat_info(PG_FUNCTION_ARGS)
{
#define WLM_USER_IO_RESOURCE_ATTRNUM 7
    char* name = PG_GETARG_CSTRING(0);

    FuncCallContext* func_ctx = NULL;
    const int num = 1;
    int i = 0;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext old_context;
        TupleDesc tupdesc;
        i = 0;

        func_ctx = SRF_FIRSTCALL_INIT();

        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(WLM_USER_IO_RESOURCE_ATTRNUM, false);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "user_id", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "mincurr_iops", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "maxcurr_iops", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "minpeak_iops", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "maxpeak_iops", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "iops_limits", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "io_priority", TEXTOID, -1, 0);

        func_ctx->tuple_desc = BlessTupleDesc(tupdesc);
        func_ctx->user_fctx = GetUserResourceData(name); /* get user resource data */
        func_ctx->max_calls = num;

        MemoryContextSwitchTo(old_context);

        if (func_ctx->user_fctx == NULL) {
            SRF_RETURN_DONE(func_ctx);
        }
    }

    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->call_cntr < func_ctx->max_calls) {
        Datum values[WLM_USER_IO_RESOURCE_ATTRNUM];
        bool nulls[WLM_USER_IO_RESOURCE_ATTRNUM] = {false};
        HeapTuple tuple = NULL;
        int i = -1;

        UserResourceData* rsdata = (UserResourceData*)func_ctx->user_fctx + func_ctx->call_cntr;

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
        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    }
    pfree_ext(func_ctx->user_fctx);
    func_ctx->user_fctx = NULL;
    SRF_RETURN_DONE(func_ctx);
}

Datum pg_stat_get_wlm_session_iostat_info(PG_FUNCTION_ARGS)
{
#define WLM_SESSION_IOSTAT_ATTRNUM 7
    FuncCallContext* func_ctx = NULL;
    int num = 0;
    int i = 0;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext old_context;
        TupleDesc tupdesc;
        i = 0;

        func_ctx = SRF_FIRSTCALL_INIT();

        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(WLM_SESSION_IOSTAT_ATTRNUM, false);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "threadid", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "maxcurr_iops", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "mincurr_iops", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "maxpeak_iops", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "minpeak_iops", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "iops_limits", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "io_priority", INT4OID, -1, 0);

        func_ctx->tuple_desc = BlessTupleDesc(tupdesc);
        func_ctx->user_fctx = WLMGetIOStatisticsGeneral(&num);
        func_ctx->max_calls = num;

        MemoryContextSwitchTo(old_context);

        if (func_ctx->user_fctx == NULL) {
            SRF_RETURN_DONE(func_ctx);
        }
    }

    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->user_fctx && func_ctx->call_cntr < func_ctx->max_calls) {
        Datum values[WLM_SESSION_IOSTAT_ATTRNUM];
        bool nulls[WLM_SESSION_IOSTAT_ATTRNUM] = {false};
        HeapTuple tuple = NULL;
        Datum result;
        int i = -1;

        WLMIoStatisticsGenenral* statistics = (WLMIoStatisticsGenenral*)func_ctx->user_fctx + func_ctx->call_cntr;

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

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(func_ctx, result);
    }

    pfree_ext(func_ctx->user_fctx);
    func_ctx->user_fctx = NULL;

    SRF_RETURN_DONE(func_ctx);
}

/*
 * function name: pg_stat_get_wlm_node_resource_info
 * description  : the view will show node resource info.
 */
Datum pg_stat_get_wlm_node_resource_info(PG_FUNCTION_ARGS)
{
#define WLM_NODE_RESOURCE_ATTRNUM 7
    FuncCallContext* func_ctx = NULL;
    const int num = 1;
    int i = 0;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext old_context;
        TupleDesc tupdesc;
        i = 0;

        func_ctx = SRF_FIRSTCALL_INIT();

        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(WLM_NODE_RESOURCE_ATTRNUM, false);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "min_mem_util", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "max_mem_util", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "min_cpu_util", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "max_cpu_util", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "min_io_util", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "max_io_util", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "phy_usemem_rate", INT4OID, -1, 0);

        func_ctx->tuple_desc = BlessTupleDesc(tupdesc);
        func_ctx->user_fctx = dywlm_get_resource_info(&g_instance.wlm_cxt->MyDefaultNodeGroup.srvmgr);
        func_ctx->max_calls = num;

        MemoryContextSwitchTo(old_context);

        if (func_ctx->user_fctx == NULL) {
            SRF_RETURN_DONE(func_ctx);
        }
    }

    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->user_fctx && func_ctx->call_cntr < func_ctx->max_calls) {
        Datum values[WLM_SESSION_IOSTAT_ATTRNUM];
        bool nulls[WLM_SESSION_IOSTAT_ATTRNUM] = {false};
        HeapTuple tuple = NULL;
        Datum result;
        int i = -1;

        DynamicNodeData* nodedata = (DynamicNodeData*)func_ctx->user_fctx;

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

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(func_ctx, result);
    }

    pfree_ext(func_ctx->user_fctx);
    func_ctx->user_fctx = NULL;

    SRF_RETURN_DONE(func_ctx);
}

/*
 * function name: pg_stat_get_wlm_session_info
 * description  : the view will show the session info.
 */
Datum pg_stat_get_wlm_session_info(PG_FUNCTION_ARGS)
{
    const int WLM_SESSION_INFO_ATTRNUM = 68;
    FuncCallContext* func_ctx = NULL;
    int num = 0;
    int i = 0;

    Oid removed = PG_GETARG_OID(0); /* remove session info ? */
    Qid qid = {0, 0, 0};

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext old_context;
        TupleDesc tupdesc;
        i = 0;

        func_ctx = SRF_FIRSTCALL_INIT();
        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);
        tupdesc = CreateTemplateTupleDesc(WLM_SESSION_INFO_ATTRNUM, false);
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
        func_ctx->tuple_desc = BlessTupleDesc(tupdesc);
        func_ctx->user_fctx = WLMGetSessionInfo(&qid, removed, &num);
        func_ctx->max_calls = num;
        MemoryContextSwitchTo(old_context);
        if (func_ctx->user_fctx == NULL) {
            SRF_RETURN_DONE(func_ctx);
        }
    }

    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->user_fctx && func_ctx->call_cntr < func_ctx->max_calls) {
        const int TOP5 = 5;
        Datum values[WLM_SESSION_INFO_ATTRNUM];
        bool nulls[WLM_SESSION_INFO_ATTRNUM] = {false};
        HeapTuple tuple = NULL;
        Datum result;
        int i = -1;
        errno_t rc = 0;
        char spill_info[NAMEDATALEN];
        char warnstr[WARNING_INFO_LEN] = {0};
        SockAddr zero_clientaddr;

        WLMSessionStatistics* detail = (WLMSessionStatistics*)func_ctx->user_fctx + func_ctx->call_cntr;

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
        if (dbname != NULL) {
            values[++i] = CStringGetTextDatum(dbname);
        } else {
            nulls[++i] = true;
        }
        if (detail->schname && detail->schname[0]) {
            values[++i] = CStringGetTextDatum(detail->schname);
        } else {
            nulls[++i] = true;
        }
        values[++i] = CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);
        values[++i] = CStringGetTextDatum(detail->username);
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
                    if (detail->clienthostname && detail->clienthostname[0]) {
                        values[++i] = CStringGetTextDatum(detail->clienthostname);
                    } else {
                        nulls[++i] = true;
                    }
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

        if (detail->query_band && detail->query_band[0]) {
            values[++i] = CStringGetTextDatum(detail->query_band);
        } else {
            nulls[++i] = true;
        }
        values[++i] = Int64GetDatum(detail->block_time);
        values[++i] = TimestampTzGetDatum(detail->start_time);
        values[++i] = TimestampTzGetDatum(detail->fintime);
        values[++i] = Int64GetDatum(detail->duration);
        values[++i] = Int64GetDatum(detail->estimate_time);
        values[++i] = CStringGetTextDatum(GetStatusName(detail->status));
        if (detail->err_msg && detail->err_msg[0]) {
            values[++i] = CStringGetTextDatum(detail->err_msg);
        } else {
            nulls[++i] = true;
        }
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
        if (warnstr[0]) {
            values[++i] = CStringGetTextDatum(warnstr);
        } else {
            nulls[++i] = true;
        }
        values[++i] = Int64GetDatum(detail->debug_query_id);
        if (detail->statement && detail->statement[0]) {
            values[++i] = CStringGetTextDatum(detail->statement);
        } else {
            nulls[++i] = true;
        }
        values[++i] = CStringGetTextDatum(detail->query_plan);
        if (StringIsValid(detail->nodegroup)) {
            values[++i] = CStringGetTextDatum(detail->nodegroup);
        } else {
            values[++i] = CStringGetTextDatum("installation");
        }

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
        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(func_ctx, result);
    }

    pfree_ext(func_ctx->user_fctx);
    func_ctx->user_fctx = NULL;

    SRF_RETURN_DONE(func_ctx);
}

/*
 * @Description:  get ec operator information from hashtable.
 * @return - Datum
 */
Datum pg_stat_get_wlm_ec_operator_info(PG_FUNCTION_ARGS)
{
#define OPERATOR_SESSION_EC_INFO_ATTRNUM 17
    FuncCallContext* func_ctx = NULL;
    int num = 0;
    int i = 0;
    Oid removed = PG_GETARG_OID(0); /* remove session info ? */
    Qpid qid = {0, 0, 0};
    if (SRF_IS_FIRSTCALL()) {
        MemoryContext old_context;
        TupleDesc tupdesc;
        i = 0;
        func_ctx = SRF_FIRSTCALL_INIT();
        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);
        tupdesc = CreateTemplateTupleDesc(OPERATOR_SESSION_EC_INFO_ATTRNUM, false);
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

        func_ctx->tuple_desc = BlessTupleDesc(tupdesc);
        func_ctx->user_fctx = ExplainGetSessionInfo(&qid, removed, &num);
        func_ctx->max_calls = num;

        MemoryContextSwitchTo(old_context);

        if (func_ctx->user_fctx == NULL) {
            SRF_RETURN_DONE(func_ctx);
        }
    }

    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->user_fctx && func_ctx->call_cntr < func_ctx->max_calls) {
        Datum values[OPERATOR_SESSION_EC_INFO_ATTRNUM];
        bool nulls[OPERATOR_SESSION_EC_INFO_ATTRNUM] = {false};
        HeapTuple tuple = NULL;
        Datum result;
        int i = -1;
        errno_t rc = EOK;

        ExplainGeneralInfo* statistics = (ExplainGeneralInfo*)func_ctx->user_fctx + func_ctx->call_cntr;

        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        values[++i] = Int64GetDatum(statistics->query_id);
        values[++i] = Int32GetDatum(statistics->plan_node_id);
        values[++i] = CStringGetTextDatum(statistics->plan_node_name);
        if (statistics->start_time == 0) {
            nulls[++i] = true;
        } else {
            values[++i] = TimestampGetDatum(statistics->start_time);
        }
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
            values[++i] = CStringGetTextDatum(statistics->ec_query);
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

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(func_ctx, result);
    }

    pfree_ext(func_ctx->user_fctx);
    func_ctx->user_fctx = NULL;

    SRF_RETURN_DONE(func_ctx);
}

static inline void GetStaticValInfo(Datum *values, ExplainGeneralInfo *statistics)
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
Datum
gs_stat_get_wlm_plan_operator_info(PG_FUNCTION_ARGS)
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

static inline void InitPlanOperatorInfoTuple(int operator_plan_info_attrnum, FuncCallContext *funcctx)
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
#define OPERATOR_SESSION_INFO_ATTRNUM 22
    FuncCallContext* func_ctx = NULL;
    int num = 0;
    int i = 0;

    Oid removed = PG_GETARG_OID(0); /* remove session info ? */
    Qpid qid = {0, 0, 0};
    if (SRF_IS_FIRSTCALL()) {
        MemoryContext old_context;
        TupleDesc tupdesc;
        i = 0;
        func_ctx = SRF_FIRSTCALL_INIT();
        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);
        tupdesc = CreateTemplateTupleDesc(OPERATOR_SESSION_INFO_ATTRNUM, false);
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

        func_ctx->tuple_desc = BlessTupleDesc(tupdesc);
        func_ctx->user_fctx = ExplainGetSessionInfo(&qid, removed, &num);
        func_ctx->max_calls = num;

        MemoryContextSwitchTo(old_context);

        if (func_ctx->user_fctx == NULL) {
            SRF_RETURN_DONE(func_ctx);
        }
    }

    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->user_fctx && func_ctx->call_cntr < func_ctx->max_calls) {
        Datum values[OPERATOR_SESSION_INFO_ATTRNUM];
        bool nulls[OPERATOR_SESSION_INFO_ATTRNUM] = {false};
        HeapTuple tuple = NULL;
        Datum result;
        int i = -1;
        char warnstr[WARNING_INFO_LEN] = {0};
        errno_t rc = EOK;

        ExplainGeneralInfo* statistics = (ExplainGeneralInfo*)func_ctx->user_fctx + func_ctx->call_cntr;

        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        WLMGetWarnInfo(warnstr, sizeof(warnstr), statistics->warn_prof_info, statistics->max_spill_size, 0);

        values[++i] = Int64GetDatum(statistics->query_id);
        values[++i] = Int64GetDatum(statistics->tid);
        values[++i] = Int32GetDatum(statistics->plan_node_id);
        values[++i] = CStringGetTextDatum(statistics->plan_node_name);
        if (statistics->start_time == 0) {
            nulls[++i] = true;
        } else {
            values[++i] = TimestampGetDatum(statistics->start_time);
        }
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
        if (warnstr[0]) {
            values[++i] = CStringGetTextDatum(warnstr);
        } else {
            nulls[++i] = true;
        }

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(func_ctx, result);
    }

    pfree_ext(func_ctx->user_fctx);
    func_ctx->user_fctx = NULL;

    SRF_RETURN_DONE(func_ctx);
}

/*
 * function name: pg_stat_get_wlm_instance_info
 * description  : the view will show the instance info.
 */
Datum pg_stat_get_wlm_instance_info(PG_FUNCTION_ARGS)
{
#define WLM_INSTANCE_INFO_ATTRNUM 15
    FuncCallContext* func_ctx = NULL;
    int num = 0;
    int i = 0;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext old_context;
        TupleDesc tupdesc;
        i = 0;

        func_ctx = SRF_FIRSTCALL_INIT();

        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(WLM_INSTANCE_INFO_ATTRNUM, false);

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

        func_ctx->tuple_desc = BlessTupleDesc(tupdesc);
        func_ctx->user_fctx = WLMGetInstanceInfo(&num, false);
        func_ctx->max_calls = num;

        MemoryContextSwitchTo(old_context);

        if (func_ctx->user_fctx == NULL) {
            SRF_RETURN_DONE(func_ctx);
        }
    }

    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->user_fctx && func_ctx->call_cntr < func_ctx->max_calls) {
        Datum values[WLM_INSTANCE_INFO_ATTRNUM];
        bool nulls[WLM_INSTANCE_INFO_ATTRNUM];
        HeapTuple tuple;
        Datum result;
        int i = -1;
        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        WLMInstanceInfo* instanceInfo = (WLMInstanceInfo*)func_ctx->user_fctx + func_ctx->call_cntr;
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

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(func_ctx, result);
    }

    pfree_ext(func_ctx->user_fctx);
    func_ctx->user_fctx = NULL;

    SRF_RETURN_DONE(func_ctx);
}

/*
 * function name: pg_stat_get_wlm_instance_info_with_cleanup
 * description  : this function is used for persistent history data.
 */
Datum pg_stat_get_wlm_instance_info_with_cleanup(PG_FUNCTION_ARGS)
{
#define WLM_INSTANCE_INFO_ATTRNUM 15
    FuncCallContext* func_ctx = NULL;
    int num = 0;
    int i = 0;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext old_context;
        TupleDesc tupdesc;
        i = 0;

        func_ctx = SRF_FIRSTCALL_INIT();

        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(WLM_INSTANCE_INFO_ATTRNUM, false);

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

        func_ctx->tuple_desc = BlessTupleDesc(tupdesc);
        func_ctx->user_fctx = WLMGetInstanceInfo(&num, true);
        func_ctx->max_calls = num;

        MemoryContextSwitchTo(old_context);

        if (func_ctx->user_fctx == NULL) {
            SRF_RETURN_DONE(func_ctx);
        }
    }

    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->user_fctx && func_ctx->call_cntr < func_ctx->max_calls) {
        Datum values[WLM_INSTANCE_INFO_ATTRNUM];
        bool nulls[WLM_INSTANCE_INFO_ATTRNUM];
        HeapTuple tuple;
        Datum result;
        int i = -1;
        errno_t rc = 0;

        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");

        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        WLMInstanceInfo* instanceInfo = (WLMInstanceInfo*)func_ctx->user_fctx + func_ctx->call_cntr;

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

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(func_ctx, result);
    }

    pfree_ext(func_ctx->user_fctx);
    func_ctx->user_fctx = NULL;

    SRF_RETURN_DONE(func_ctx);
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

    FuncCallContext* func_ctx = NULL;
    const int num = 1;
    int i = 0;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext old_context;
        TupleDesc tupdesc;
        i = 0;
        func_ctx = SRF_FIRSTCALL_INIT();
        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);
        tupdesc = CreateTemplateTupleDesc(WLM_USER_RESOURCE_ATTRNUM, false);
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

        func_ctx->tuple_desc = BlessTupleDesc(tupdesc);
        func_ctx->user_fctx = GetUserResourceData(name); /* get user resource data */
        func_ctx->max_calls = num;

        MemoryContextSwitchTo(old_context);

        if (func_ctx->user_fctx == NULL) {
            SRF_RETURN_DONE(func_ctx);
        }
    }

    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->call_cntr < func_ctx->max_calls) {
        Datum values[WLM_USER_RESOURCE_ATTRNUM];
        bool nulls[WLM_USER_RESOURCE_ATTRNUM];
        HeapTuple tuple;
        int i = -1;

        UserResourceData* rsdata = (UserResourceData*)func_ctx->user_fctx + func_ctx->call_cntr;

        errno_t rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

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
        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    }
    pfree_ext(func_ctx->user_fctx);
    func_ctx->user_fctx = NULL;
    SRF_RETURN_DONE(func_ctx);
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
    FuncCallContext* func_ctx = NULL;
    int num = 0;
    int i = 0;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext old_context;
        TupleDesc tupdesc;
        i = 0;

        func_ctx = SRF_FIRSTCALL_INIT();

        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(WLM_RESOURCE_POOL_ATTRNUM, false);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "respool_oid", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "ref_count", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "active_points", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "running_count", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "waiting_count", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "iops_limits", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "io_priority", INT4OID, -1, 0);

        func_ctx->tuple_desc = BlessTupleDesc(tupdesc);
        func_ctx->user_fctx = WLMGetResourcePoolDataInfo(&num);
        func_ctx->max_calls = num;

        MemoryContextSwitchTo(old_context);

        if (func_ctx->user_fctx == NULL) {
            SRF_RETURN_DONE(func_ctx);
        }
    }

    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->call_cntr < func_ctx->max_calls) {
        Datum values[WLM_RESOURCE_POOL_ATTRNUM];
        bool nulls[WLM_RESOURCE_POOL_ATTRNUM] = {false};
        HeapTuple tuple = NULL;
        int i = -1;
        ResourcePool* rp = (ResourcePool*)func_ctx->user_fctx + func_ctx->call_cntr;

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

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    }

    pfree_ext(func_ctx->user_fctx);
    func_ctx->user_fctx = NULL;

    SRF_RETURN_DONE(func_ctx);
}

/*
 * @Description:  wlm get all user info in hash table.
 * @IN void
 * @Return: records
 * @See also:
 */
Datum pg_stat_get_wlm_user_info(PG_FUNCTION_ARGS)
{
#define WLM_USER_INFO_ATTRNUM 8
    FuncCallContext* func_ctx = NULL;
    int num = 0;
    int i = 0;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext old_context;
        TupleDesc tupdesc;
        i = 0;

        func_ctx = SRF_FIRSTCALL_INIT();

        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(WLM_USER_INFO_ATTRNUM, false);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "userid", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "sysadmin", BOOLOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "rpoid", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "parentid", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "totalspace", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "spacelimit", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "childcount", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "childlist", TEXTOID, -1, 0);

        func_ctx->tuple_desc = BlessTupleDesc(tupdesc);
        func_ctx->user_fctx = WLMGetAllUserData(&num);
        func_ctx->max_calls = num;

        MemoryContextSwitchTo(old_context);

        if (func_ctx->user_fctx == NULL) {
            SRF_RETURN_DONE(func_ctx);
        }
    }

    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->call_cntr < func_ctx->max_calls) {
        Datum values[WLM_USER_INFO_ATTRNUM];
        bool nulls[WLM_USER_INFO_ATTRNUM] = {false};
        HeapTuple tuple = NULL;
        int i = -1;

        WLMUserInfo* info = (WLMUserInfo*)func_ctx->user_fctx + func_ctx->call_cntr;

        errno_t rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        /* Locking is probably not really necessary */
        values[++i] = ObjectIdGetDatum(info->userid);
        values[++i] = BoolGetDatum(info->admin);
        values[++i] = ObjectIdGetDatum(info->rpoid);
        values[++i] = ObjectIdGetDatum(info->parentid);
        values[++i] = Int64GetDatum(info->space);
        values[++i] = Int64GetDatum(info->spacelimit);
        values[++i] = Int32GetDatum(info->childcount);
        if (info->children == NULL) {
            info->children = "No Child";
        }
        values[++i] = CStringGetTextDatum(info->children);
        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    }

    SRF_RETURN_DONE(func_ctx);
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
    FuncCallContext* func_ctx = NULL;
    int num = 0;
    int i = 0;
    errno_t rc;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext old_context;
        TupleDesc tupdesc;
        i = 0;

        func_ctx = SRF_FIRSTCALL_INIT();

        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(WLM_CGROUP_INFO_ATTRNUM, false);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "cgroup_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "percent", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "usagepct", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "shares", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "usage", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "cpuset", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "relpath", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "valid", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "node_group", TEXTOID, -1, 0);

        func_ctx->tuple_desc = BlessTupleDesc(tupdesc);
        func_ctx->user_fctx = gscgroup_get_cgroup_info(&num);

        func_ctx->max_calls = num;

        MemoryContextSwitchTo(old_context);

        if (func_ctx->user_fctx == NULL) {
            SRF_RETURN_DONE(func_ctx);
        }
    }

    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->user_fctx && func_ctx->call_cntr < func_ctx->max_calls) {
        Datum values[WLM_CGROUP_INFO_ATTRNUM];
        bool nulls[WLM_CGROUP_INFO_ATTRNUM] = {false};
        HeapTuple tuple = NULL;
        Datum result;
        i = -1;

        gscgroup_info_t* info = (gscgroup_info_t*)func_ctx->user_fctx + func_ctx->call_cntr;

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
        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(func_ctx, result);
    }

    pfree_ext(func_ctx->user_fctx);
    func_ctx->user_fctx = NULL;

    SRF_RETURN_DONE(func_ctx);
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
    FuncCallContext* func_ctx = NULL;
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        errmsg("unsupported view in single node mode.")));

    SRF_RETURN_DONE(func_ctx);
#else
#define WLM_WORKLOAD_RECORDS_ATTRNUM 11
    FuncCallContext* func_ctx = NULL;
    int num = 0;
    int i = 0;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext old_context;
        TupleDesc tupdesc;
        i = 0;

        func_ctx = SRF_FIRSTCALL_INIT();

        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(WLM_WORKLOAD_RECORDS_ATTRNUM, false);
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

        func_ctx->tuple_desc = BlessTupleDesc(tupdesc);
        func_ctx->user_fctx = dywlm_get_records(&num);
        func_ctx->max_calls = num;

        MemoryContextSwitchTo(old_context);

        if (func_ctx->user_fctx == NULL) {
            SRF_RETURN_DONE(func_ctx);
        }
    }

    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->user_fctx && func_ctx->call_cntr < func_ctx->max_calls) {
        Datum values[WLM_WORKLOAD_RECORDS_ATTRNUM];
        bool nulls[WLM_WORKLOAD_RECORDS_ATTRNUM] = {false};
        HeapTuple tuple = NULL;
        Datum result;
        int i = -1;

        DynamicWorkloadRecord* record = (DynamicWorkloadRecord*)func_ctx->user_fctx + func_ctx->call_cntr;

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

        if (StringIsValid(record->groupname)) {
            values[++i] = CStringGetTextDatum(record->groupname);
        } else {
            values[++i] = CStringGetTextDatum("installation");
        }

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(func_ctx, result);
    }

    pfree_ext(func_ctx->user_fctx);
    func_ctx->user_fctx = NULL;

    SRF_RETURN_DONE(func_ctx);
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
    Oid rel_id = PG_GETARG_OID(0);
    int64 result = 0;
    List* stat_list = NIL;
    ListCell* stat_cell = NULL;
    uint32 statFlag = STATFLG_RELATION;
    PgStat_TableStatus* tab_entry = NULL;
    pg_stat_get_stat_list(&stat_list, &statFlag, rel_id);
    foreach (stat_cell, stat_list) {
        Oid relation_id = lfirst_oid(stat_cell);
        tab_entry = find_tabstat_entry(relation_id, statFlag);
        if (!PointerIsValid(tab_entry)) {
            result += 0;
        } else {
            result += (int64)(tab_entry->t_counts.t_numscans);
        }
    }
    if (PointerIsValid(stat_list)) {
        list_free(stat_list);
    }

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_xact_tuples_returned(PG_FUNCTION_ARGS)
{
    Oid rel_id = PG_GETARG_OID(0);
    int64 result = 0;
    List* stat_list = NIL;
    ListCell* stat_cell = NULL;
    uint32 stat_flag = STATFLG_RELATION;
    PgStat_TableStatus* tab_entry = NULL;
    pg_stat_get_stat_list(&stat_list, &stat_flag, rel_id);

    foreach (stat_cell, stat_list) {
        Oid relation_id = lfirst_oid(stat_cell);
        tab_entry = find_tabstat_entry(relation_id, stat_flag);
        if (!PointerIsValid(tab_entry)) {
            result += 0;
        } else {
            result += (int64)(tab_entry->t_counts.t_tuples_returned);
        }
    }

    if (PointerIsValid(stat_list)) {
        list_free(stat_list);
    }

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_xact_tuples_fetched(PG_FUNCTION_ARGS)
{
    Oid rel_id = PG_GETARG_OID(0);
    int64 result = 0;
    List* stat_list = NIL;
    ListCell* stat_cell = NULL;
    uint32 stat_flag = STATFLG_RELATION;
    PgStat_TableStatus* tab_entry = NULL;
    pg_stat_get_stat_list(&stat_list, &stat_flag, rel_id);
    foreach (stat_cell, stat_list) {
        Oid relation_id = lfirst_oid(stat_cell);
        tab_entry = find_tabstat_entry(relation_id, stat_flag);
        if (!PointerIsValid(tab_entry)) {
            result += 0;
        } else {
            result += (int64)(tab_entry->t_counts.t_tuples_fetched);
        }
    }
    if (PointerIsValid(stat_list)) {
        list_free(stat_list);
    }

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_xact_tuples_inserted(PG_FUNCTION_ARGS)
{
    Oid rel_id = PG_GETARG_OID(0);
    int64 result = 0;
    List* stat_list = NIL;
    ListCell* stat_cell = NULL;
    uint32 t_stat_flag = InvalidOid;
    PgStat_TableStatus* tab_entry = NULL;
    PgStat_TableXactStatus* trans = NULL;
    if (IS_PGXC_COORDINATOR && GetRelationLocInfo((rel_id)))
        PG_RETURN_INT64(pgxc_exec_tuples_stat(rel_id, "pg_stat_get_xact_tuples_inserted", EXEC_ON_DATANODES));

    if (isPartitionedObject(rel_id, RELKIND_RELATION, true)) {
        t_stat_flag = rel_id;
        stat_list = getPartitionObjectIdList(rel_id, PART_OBJ_TYPE_TABLE_PARTITION);
    } else if (isPartitionedObject(rel_id, RELKIND_INDEX, true)) {
        t_stat_flag = rel_id;
        stat_list = getPartitionObjectIdList(rel_id, PART_OBJ_TYPE_INDEX_PARTITION);
    } else {
        t_stat_flag = InvalidOid;
        stat_list = lappend_oid(stat_list, rel_id);
    }
    foreach (stat_cell, stat_list) {
        Oid t_id = lfirst_oid(stat_cell);
        tab_entry = find_tabstat_entry(t_id, t_stat_flag);
        if (!PointerIsValid(tab_entry)) {
            continue;
        }
        result += tab_entry->t_counts.t_tuples_inserted;
        /* live subtransactions' counts aren't in t_tuples_inserted yet */
        for (trans = tab_entry->trans; trans != NULL; trans = trans->upper) {
            result += trans->tuples_inserted;
        }
    }

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_xact_tuples_updated(PG_FUNCTION_ARGS)
{
    Oid rel_id = PG_GETARG_OID(0);
    int64 result = 0;
    List* stat_list = NIL;
    ListCell* stat_cell = NULL;
    uint32 t_stat_flag = InvalidOid;
    PgStat_TableStatus* tab_entry = NULL;
    PgStat_TableXactStatus* trans = NULL;

    if (IS_PGXC_COORDINATOR && GetRelationLocInfo((rel_id))) {
        PG_RETURN_INT64(pgxc_exec_tuples_stat(rel_id, "pg_stat_get_xact_tuples_updated", EXEC_ON_DATANODES));
    }

    if (isPartitionedObject(rel_id, RELKIND_RELATION, true)) {
        t_stat_flag = rel_id;
        stat_list = getPartitionObjectIdList(rel_id, PART_OBJ_TYPE_TABLE_PARTITION);
    } else if (isPartitionedObject(rel_id, RELKIND_INDEX, true)) {
        t_stat_flag = rel_id;
        stat_list = getPartitionObjectIdList(rel_id, PART_OBJ_TYPE_INDEX_PARTITION);
    } else {
        t_stat_flag = InvalidOid;
        stat_list = lappend_oid(stat_list, rel_id);
    }

    foreach (stat_cell, stat_list) {
        Oid t_id = lfirst_oid(stat_cell);
        tab_entry = find_tabstat_entry(t_id, t_stat_flag);
        if (!PointerIsValid(tab_entry)) {
            continue;
        }
        result += tab_entry->t_counts.t_tuples_updated;
        /* live subtransactions' counts aren't in t_tuples_inserted yet */
        for (trans = tab_entry->trans; trans != NULL; trans = trans->upper) {
            result += trans->tuples_updated;
        }
    }
    PG_RETURN_INT64(result);
}

Datum pg_stat_get_xact_tuples_deleted(PG_FUNCTION_ARGS)
{
    Oid rel_id = PG_GETARG_OID(0);
    int64 result = 0;
    List* stat_list = NIL;
    ListCell* stat_cell = NULL;
    uint32 t_stat_flag = InvalidOid;
    PgStat_TableStatus* tab_entry = NULL;
    PgStat_TableXactStatus* trans = NULL;
    if (IS_PGXC_COORDINATOR && GetRelationLocInfo((rel_id))) {
        PG_RETURN_INT64(pgxc_exec_tuples_stat(rel_id, "pg_stat_get_xact_tuples_deleted", EXEC_ON_DATANODES));
    }
    if (isPartitionedObject(rel_id, RELKIND_RELATION, true)) {
        t_stat_flag = rel_id;
        stat_list = getPartitionObjectIdList(rel_id, PART_OBJ_TYPE_TABLE_PARTITION);
    } else if (isPartitionedObject(rel_id, RELKIND_INDEX, true)) {
        t_stat_flag = rel_id;
        stat_list = getPartitionObjectIdList(rel_id, PART_OBJ_TYPE_INDEX_PARTITION);
    } else {
        t_stat_flag = InvalidOid;
        stat_list = lappend_oid(stat_list, rel_id);
    }

    foreach (stat_cell, stat_list) {
        Oid t_id = lfirst_oid(stat_cell);
        tab_entry = find_tabstat_entry(t_id, t_stat_flag);
        if (!PointerIsValid(tab_entry)) {
            continue;
        }
        result += tab_entry->t_counts.t_tuples_deleted;
        /* live subtransactions' counts aren't in t_tuples_inserted yet */
        for (trans = tab_entry->trans; trans != NULL; trans = trans->upper) {
            result += trans->tuples_deleted;
        }
    }

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_xact_tuples_hot_updated(PG_FUNCTION_ARGS)
{
    Oid rel_id = PG_GETARG_OID(0);
    int64 result = 0;
    List* stat_list = NIL;
    ListCell* stat_cell = NULL;
    uint32 t_stat_flag = InvalidOid;
    PgStat_TableStatus* tab_entry = NULL;
    if (IS_PGXC_COORDINATOR && GetRelationLocInfo((rel_id))) {
        PG_RETURN_INT64(pgxc_exec_tuples_stat(rel_id, "pg_stat_get_xact_tuples_hot_updated", EXEC_ON_DATANODES));
    }
    if (isPartitionedObject(rel_id, RELKIND_RELATION, true)) {
        t_stat_flag = rel_id;
        stat_list = getPartitionObjectIdList(rel_id, PART_OBJ_TYPE_TABLE_PARTITION);
    } else if (isPartitionedObject(rel_id, RELKIND_INDEX, true)) {
        t_stat_flag = rel_id;
        stat_list = getPartitionObjectIdList(rel_id, PART_OBJ_TYPE_INDEX_PARTITION);
    } else {
        t_stat_flag = InvalidOid;
        stat_list = lappend_oid(stat_list, rel_id);
    }
    foreach (stat_cell, stat_list) {
        Oid t_id = lfirst_oid(stat_cell);
        tab_entry = find_tabstat_entry(t_id, t_stat_flag);
        if (!PointerIsValid(tab_entry)) {
            continue;
        }
        result += tab_entry->t_counts.t_tuples_hot_updated;
    }

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_xact_blocks_fetched(PG_FUNCTION_ARGS)
{
    Oid rel_id = PG_GETARG_OID(0);
    int64 result = 0;
    List* stat_list = NIL;
    ListCell* stat_cell = NULL;
    uint32 stat_flag = STATFLG_RELATION;
    PgStat_TableStatus* tab_entry = NULL;
    pg_stat_get_stat_list(&stat_list, &stat_flag, rel_id);
    foreach (stat_cell, stat_list) {
        Oid relation_id = lfirst_oid(stat_cell);
        tab_entry = find_tabstat_entry(relation_id, stat_flag);
        if (!PointerIsValid(tab_entry)) {
            result += 0;
        } else {
            result += (int64)(tab_entry->t_counts.t_blocks_fetched);
        }
    }
    if (PointerIsValid(stat_list)) {
        list_free(stat_list);
    }

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_xact_blocks_hit(PG_FUNCTION_ARGS)
{
    Oid rel_id = PG_GETARG_OID(0);
    int64 result = 0;
    List* stat_list = NIL;
    ListCell* stat_cell = NULL;
    uint32 stat_flag = STATFLG_RELATION;
    PgStat_TableStatus* tab_entry = NULL;
    pg_stat_get_stat_list(&stat_list, &stat_flag, rel_id);
    foreach (stat_cell, stat_list) {
        Oid relation_id = lfirst_oid(stat_cell);
        tab_entry = find_tabstat_entry(relation_id, stat_flag);
        if (!PointerIsValid(tab_entry)) {
            result += 0;
        } else {
            result += (int64)(tab_entry->t_counts.t_blocks_hit);
        }
    }
    if (PointerIsValid(stat_list)) {
        list_free(stat_list);
    }

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_xact_function_calls(PG_FUNCTION_ARGS)
{
    Oid func_id = PG_GETARG_OID(0);
    PgStat_BackendFunctionEntry* func_entry = NULL;
    if ((func_entry = find_funcstat_entry(func_id)) == NULL) {
        PG_RETURN_NULL();
    }
    PG_RETURN_INT64(func_entry->f_counts.f_numcalls);
}

Datum pg_stat_get_xact_function_total_time(PG_FUNCTION_ARGS)
{
    Oid func_id = PG_GETARG_OID(0);
    PgStat_BackendFunctionEntry* func_entry = NULL;
    if ((func_entry = find_funcstat_entry(func_id)) == NULL) {
        PG_RETURN_NULL();
    }
    PG_RETURN_FLOAT8(INSTR_TIME_GET_MILLISEC(func_entry->f_counts.f_total_time));
}

Datum pg_stat_get_xact_function_self_time(PG_FUNCTION_ARGS)
{
    Oid func_id = PG_GETARG_OID(0);
    PgStat_BackendFunctionEntry* func_entry = NULL;
    if ((func_entry = find_funcstat_entry(func_id)) == NULL) {
        PG_RETURN_NULL();
    }
    PG_RETURN_FLOAT8(INSTR_TIME_GET_MILLISEC(func_entry->f_counts.f_self_time));
}

/* Discard the active statistics snapshot */
Datum pg_stat_clear_snapshot(PG_FUNCTION_ARGS)
{
    if (!superuser()) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), 
                (errmsg("Must be system admin to clear snapshot.")))); 
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
    Oid taboid = PG_GETARG_OID(0);
    Oid p_objectid;
    List* stat_list = NIL;
    ListCell* stat_cell = NULL;
    /* no syscache in collector thread, we just deal with all partitions here */
    if (isPartitionedObject(taboid, RELKIND_INDEX, true)) {
        p_objectid = taboid;
        stat_list = getPartitionObjectIdList(taboid, PART_OBJ_TYPE_INDEX_PARTITION);
    } else if (isPartitionedObject(taboid, RELKIND_RELATION, true)) {
        p_objectid = taboid;
        stat_list = getPartitionObjectIdList(taboid, PART_OBJ_TYPE_TABLE_PARTITION);
    } else {
        p_objectid = InvalidOid;
        stat_list = lappend_oid(stat_list, taboid);
    }

    foreach (stat_cell, stat_list) {
        /* reset every partition it is a partitioned object */
        pgstat_reset_single_counter(p_objectid, lfirst_oid(stat_cell), RESET_TABLE);
    }
    list_free(stat_list);
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        Relation rel = NULL;
        ExecNodes* exec_nodes = NULL;
        StringInfoData buf;
        ParallelFunctionState* state = NULL;

        char* rel_name = NULL;
        char* nsp_name = NULL;

        rel = try_relation_open(taboid, AccessShareLock);
        if (!rel) {
            PG_RETURN_NULL();
        }
        if (rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP) {
            relation_close(rel, AccessShareLock);
            return 0;
        }
        rel_name = repairObjectName(RelationGetRelationName(rel));
        nsp_name = repairObjectName(get_namespace_name(rel->rd_rel->relnamespace, true));
        relation_close(rel, AccessShareLock);

        exec_nodes = (ExecNodes*)makeNode(ExecNodes);
        exec_nodes->baselocatortype = LOCATOR_TYPE_HASH;
        exec_nodes->accesstype = RELATION_ACCESS_READ;
        exec_nodes->primarynodelist = NIL;
        exec_nodes->en_expr = NULL;
        exec_nodes->en_relid = taboid;
        exec_nodes->nodeList = NIL;

        initStringInfo(&buf);
        appendStringInfo(
            &buf, "SELECT pg_catalog.pg_stat_reset_single_table_counters('%s.%s'::regclass);", nsp_name, rel_name);
        state = RemoteFunctionResultHandler(buf.data, exec_nodes, NULL, true, EXEC_ON_ALL_NODES, true);
        FreeParallelFunctionState(state);
        pfree_ext(buf.data);
    }

    PG_RETURN_VOID();
}

Datum pg_stat_reset_single_function_counters(PG_FUNCTION_ARGS)
{
    Oid func_oid = PG_GETARG_OID(0);

    pgstat_reset_single_counter(InvalidOid, func_oid, RESET_FUNCTION);

    PG_RETURN_VOID();
}

Datum pv_os_run_info(PG_FUNCTION_ARGS)
{
    FuncCallContext* func_ctx = NULL;
    /* stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL()) {
        MemoryContext old_context;
        TupleDesc tup_desc;

        /* create a function context for cross-call persistence */
        func_ctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function
         * calls
         */
        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        /* need a tuple descriptor representing 3 columns */
        tup_desc = CreateTemplateTupleDesc(5, false);

        TupleDescInitEntry(tup_desc, (AttrNumber)1, "id", INT4OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)2, "name", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)3, "value", NUMERICOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)4, "comments", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)5, "cumulative", BOOLOID, -1, 0);

        /* complete descriptor of the tupledesc */
        func_ctx->tuple_desc = BlessTupleDesc(tup_desc);

        /* collect system infomation */
        getCpuNums();
        getCpuTimes();
        getVmStat();
        getTotalMem();
        getOSRunLoad();

        (void)MemoryContextSwitchTo(old_context);
    }

    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();
    while (func_ctx->call_cntr < TOTAL_OS_RUN_INFO_TYPES) {
        /* do when there is more left to send */
        Datum values[5];
        bool nulls[5] = {false};
        HeapTuple tuple = NULL;

        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        if (!u_sess->stat_cxt.osStatDescArray[func_ctx->call_cntr].got) {
            ereport(DEBUG3,
                (errmsg("the %s stat has not got on this plate.",
                    u_sess->stat_cxt.osStatDescArray[func_ctx->call_cntr].name)));
            func_ctx->call_cntr++;
            continue;
        }

        values[0] = Int32GetDatum(func_ctx->call_cntr);
        values[1] = CStringGetTextDatum(u_sess->stat_cxt.osStatDescArray[func_ctx->call_cntr].name);
        values[2] = u_sess->stat_cxt.osStatDescArray[func_ctx->call_cntr].getDatum(
            u_sess->stat_cxt.osStatDataArray[func_ctx->call_cntr]);
        values[3] = CStringGetTextDatum(u_sess->stat_cxt.osStatDescArray[func_ctx->call_cntr].comments);
        values[4] = BoolGetDatum(u_sess->stat_cxt.osStatDescArray[func_ctx->call_cntr].cumulative);

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    }
    /* do when there is no more left */
    SRF_RETURN_DONE(func_ctx);
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
    FuncCallContext* func_ctx = NULL;
    SessionMemoryDetail* entry = NULL;
    MemoryContext old_context;

    /* stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tup_desc;

        /* create a function context for cross-call persistence */
        func_ctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function
         * calls
         */
        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        /* need a tuple descriptor representing 9 columns */
        tup_desc = CreateTemplateTupleDesc(NUM_SESSION_MEMORY_DETAIL_ELEM, false);

        TupleDescInitEntry(tup_desc, (AttrNumber)1, "sessid", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)2, "threadid", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)3, "contextname", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)4, "level", INT2OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)5, "parent", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)6, "totalsize", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)7, "freesize", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)8, "usedsize", INT8OID, -1, 0);

        /* complete descriptor of the tupledesc */
        func_ctx->tuple_desc = BlessTupleDesc(tup_desc);

        /* total number of tuples to be returned */
        if (ENABLE_THREAD_POOL) {
            func_ctx->user_fctx =
                    (void *) g_threadPoolControler->GetSessionCtrl()->getSessionMemoryDetail(&(func_ctx->max_calls));
        } else {
            func_ctx->max_calls = 0;
        }
        (void)MemoryContextSwitchTo(old_context);
    }

    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();

    if (func_ctx->call_cntr < func_ctx->max_calls) {
        /* do when there is more left to send */
        Datum values[NUM_SESSION_MEMORY_DETAIL_ELEM];
        bool nulls[NUM_SESSION_MEMORY_DETAIL_ELEM] = {false};
        char sessId[SESSION_ID_LEN];
        HeapTuple tuple = NULL;

        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        rc = memset_s(sessId, sizeof(sessId), 0, sizeof(sessId));
        securec_check(rc, "\0", "\0");

        entry = (SessionMemoryDetail *)func_ctx->user_fctx;
        func_ctx->user_fctx = (void *)entry->next;

        getSessionID(sessId, entry->sessStartTime, entry->sessId);
        values[0] = CStringGetTextDatum(sessId);
        values[1] = Int64GetDatum(entry->threadId);
        values[2] = CStringGetTextDatum(entry->contextName);
        values[3] = Int16GetDatum(entry->level);
        if (strlen(entry->parent) == 0) {
            nulls[4] = true;
        } else {
            values[4] = CStringGetTextDatum(entry->parent);
        }
        values[5] = Int64GetDatum(entry->totalSize);
        values[6] = Int64GetDatum(entry->freeSize);
        values[7] = Int64GetDatum(entry->usedSize);

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    } else {
        /* do when there is no more left */
        SRF_RETURN_DONE(func_ctx);
    }
}

/*
 * Description	: Collect each thread main memory engine's memory usage statistics.
 */
Datum mot_session_memory_detail(PG_FUNCTION_ARGS)
{
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
        funcctx->user_fctx = (void *)getMotSessionMemoryDetail(&(funcctx->max_calls));

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
}

/*
 * mot_global_memory_detail
 *		Produce a view to show all global memory usage on node
 *
 */
Datum mot_global_memory_detail(PG_FUNCTION_ARGS)
{
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
        funcctx->user_fctx = (void *)getMotMemoryDetail(&(funcctx->max_calls), true);

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
}

/*
 * mot_local_memory_detail
 *		Produce a view to show all local memory usage on node
 *
 */
Datum mot_local_memory_detail(PG_FUNCTION_ARGS)
{
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
        funcctx->user_fctx = (void *)getMotMemoryDetail(&(funcctx->max_calls), false);

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
    FuncCallContext* func_ctx = NULL;
    ThreadMemoryDetail* entry = NULL;
    MemoryContext old_context;

    /* stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tup_desc;

        /* create a function context for cross-call persistence */
        func_ctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function
         * calls
         */
        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        /* need a tuple descriptor representing 9 columns */
        tup_desc = CreateTemplateTupleDesc(NUM_THREAD_MEMORY_DETAIL_ELEM, false);

        TupleDescInitEntry(tup_desc, (AttrNumber)1, "threadid", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)2, "tid", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)3, "threadtype", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)4, "contextname", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)5, "level", INT2OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)6, "parent", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)7, "totalsize", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)8, "freesize", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)9, "usedsize", INT8OID, -1, 0);

        /* complete descriptor of the tupledesc */
        func_ctx->tuple_desc = BlessTupleDesc(tup_desc);

        /* total number of tuples to be returned */
        func_ctx->user_fctx = (void*)getThreadMemoryDetail(&(func_ctx->max_calls));

        (void)MemoryContextSwitchTo(old_context);
    }

    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();

    if (func_ctx->call_cntr < func_ctx->max_calls) {
        /* do when there is more left to send */
        Datum values[NUM_THREAD_MEMORY_DETAIL_ELEM];
        bool nulls[NUM_THREAD_MEMORY_DETAIL_ELEM] = {false};
        char thrdId[SESSION_ID_LEN];
        HeapTuple tuple = NULL;

        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        rc = memset_s(thrdId, sizeof(thrdId), 0, sizeof(thrdId));
        securec_check(rc, "\0", "\0");

        entry =  (ThreadMemoryDetail *)func_ctx->user_fctx;
        func_ctx->user_fctx = (void *)entry->next;

        getSessionID(thrdId, entry->threadStartTime, entry->threadId);
        values[0] = CStringGetTextDatum(thrdId);
        values[1] = Int64GetDatum(entry->threadId);
        values[2] = CStringGetTextDatum(entry->threadType);
        values[3] = CStringGetTextDatum(entry->contextName);
        values[4] = Int16GetDatum(entry->level);
        if (strlen(entry->parent) == 0) {
            nulls[5] = true;
        } else {
            values[5] = CStringGetTextDatum(entry->parent);
        }
        values[6] = Int64GetDatum(entry->totalSize);
        values[7] = Int64GetDatum(entry->freeSize);
        values[8] = Int64GetDatum(entry->usedSize);

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    } else {
        /* do when there is no more left */
        SRF_RETURN_DONE(func_ctx);
    }
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
    FuncCallContext* func_ctx = NULL;
    ThreadMemoryDetail* entry = NULL;
    MemoryContext old_context;

    /* stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tup_desc;

        /* create a function context for cross-call persistence */
        func_ctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function
         * calls
         */
        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        /* need a tuple descriptor representing 9 columns */
        tup_desc = CreateTemplateTupleDesc(6, false);

        TupleDescInitEntry(tup_desc, (AttrNumber)1, "contextname", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)2, "level", INT2OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)3, "parent", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)4, "totalsize", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)5, "freesize", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)6, "usedsize", INT8OID, -1, 0);

        /* complete descriptor of the tupledesc */
        func_ctx->tuple_desc = BlessTupleDesc(tup_desc);

        /* total number of tuples to be returned */
        func_ctx->user_fctx = (void*)getSharedMemoryDetail(&(func_ctx->max_calls));

        (void)MemoryContextSwitchTo(old_context);
    }

    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();

    if (func_ctx->call_cntr < func_ctx->max_calls) {
        /* do when there is more left to send */
        Datum values[6];
        bool nulls[6] = {false};
        HeapTuple tuple = NULL;

        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        entry = (ThreadMemoryDetail *)func_ctx->user_fctx;
        func_ctx->user_fctx = (void *)entry->next;

        values[0] = CStringGetTextDatum(entry->contextName);
        values[1] = Int16GetDatum(entry->level);
        if (strlen(entry->parent) == 0) {
            nulls[2] = true;
        } else {
            values[2] = CStringGetTextDatum(entry->parent);
        }
        values[3] = Int64GetDatum(entry->totalSize);
        values[4] = Int64GetDatum(entry->freeSize);
        values[5] = Int64GetDatum(entry->usedSize);

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    } else {
        /* do when there is no more left */
        SRF_RETURN_DONE(func_ctx);
    }
}

/*
 * Function returning data from the shared buffer cache - buffer number,
 * relation node/tablespace/database/blocknum and dirty indicator.
 */
Datum pg_buffercache_pages(PG_FUNCTION_ARGS)
{
    FuncCallContext* func_ctx = NULL;
    Datum result;
    MemoryContext old_context;
    BufferCachePagesContext* fctx = NULL; /* User function context. */
    TupleDesc tuple_desc;
    HeapTuple tuple;

    if (SRF_IS_FIRSTCALL()) {
        int i;
        BufferDesc* buf_hdr = NULL;

        func_ctx = SRF_FIRSTCALL_INIT();

        /* Switch context when allocating stuff to be used in later calls */
        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        /* Create a user function context for cross-call persistence */
        fctx = (BufferCachePagesContext*)palloc(sizeof(BufferCachePagesContext));

        /* Construct a tuple descriptor for the result rows. */
        tuple_desc = CreateTemplateTupleDesc(NUM_BUFFERCACHE_PAGES_ELEM, false);
        TupleDescInitEntry(tuple_desc, (AttrNumber)1, "bufferid", INT4OID, -1, 0);
        TupleDescInitEntry(tuple_desc, (AttrNumber)2, "relfilenode", OIDOID, -1, 0);
        TupleDescInitEntry(tuple_desc, (AttrNumber)3, "bucketid", INT2OID, -1, 0);
        TupleDescInitEntry(tuple_desc, (AttrNumber)4, "reltablespace", OIDOID, -1, 0);
        TupleDescInitEntry(tuple_desc, (AttrNumber)5, "reldatabase", OIDOID, -1, 0);
        TupleDescInitEntry(tuple_desc, (AttrNumber)6, "relforknumber", INT2OID, -1, 0);
        TupleDescInitEntry(tuple_desc, (AttrNumber)7, "relblocknumber", INT8OID, -1, 0);
        TupleDescInitEntry(tuple_desc, (AttrNumber)8, "isdirty", BOOLOID, -1, 0);
        TupleDescInitEntry(tuple_desc, (AttrNumber)9, "usage_count", INT2OID, -1, 0);

        fctx->tupdesc = BlessTupleDesc(tuple_desc);

        /* Allocate g_instance.attr.attr_storage.NBuffers worth of BufferCachePagesRec records. */
        fctx->record =
            (BufferCachePagesRec*)palloc(sizeof(BufferCachePagesRec) * g_instance.attr.attr_storage.NBuffers);

        /* Set max calls and remember the user function context. */
        func_ctx->max_calls = g_instance.attr.attr_storage.NBuffers;
        func_ctx->user_fctx = fctx;

        /* Return to original context when allocating transient memory */
        MemoryContextSwitchTo(old_context);

        /*
         * To get a consistent picture of the buffer state, we must lock all
         * partitions of the buffer map.  Needless to say, this is horrible
         * for concurrency.  Must grab locks in increasing order to avoid
         * possible deadlocks.
         */
        for (i = 0; i < NUM_BUFFER_PARTITIONS; i++) {
            LWLockAcquire(GetMainLWLockByIndex(FirstBufMappingLock + i), LW_SHARED);
        }
        /*
         * Scan though all the buffers, saving the relevant fields in the
         * fctx->record structure.
         */
        for (i = 0; i < g_instance.attr.attr_storage.NBuffers; i++) {
            uint32 buf_state;
            buf_hdr = GetBufferDescriptor(i);

            /* Lock each buffer header before inspecting. */
            buf_state = LockBufHdr(buf_hdr);

            fctx->record[i].bufferid = BufferDescriptorGetBuffer(buf_hdr);
            fctx->record[i].relfilenode = buf_hdr->tag.rnode.relNode;
            fctx->record[i].bucketnode = buf_hdr->tag.rnode.bucketNode;
            fctx->record[i].reltablespace = buf_hdr->tag.rnode.spcNode;
            fctx->record[i].reldatabase = buf_hdr->tag.rnode.dbNode;
            fctx->record[i].forknum = buf_hdr->tag.forkNum;
            fctx->record[i].blocknum = buf_hdr->tag.blockNum;
            fctx->record[i].usagecount = BUF_STATE_GET_USAGECOUNT(buf_state);

            if (buf_state & BM_DIRTY) {
                fctx->record[i].isdirty = true;
            } else {
                fctx->record[i].isdirty = false;
            }

            /* Note if the buffer is valid, and has storage created */
            if ((buf_state & BM_VALID) && (buf_state & BM_TAG_VALID)) {
                fctx->record[i].isvalid = true;
            } else {
                fctx->record[i].isvalid = false;
            }

            UnlockBufHdr(buf_hdr, buf_state);
        }

        /*
         * And release locks.  We do this in reverse order for two reasons:
         * (1) Anyone else who needs more than one of the locks will be trying
         * to lock them in increasing order; we don't want to release the
         * other process until it can get all the locks it needs. (2) This
         * avoids O(N^2) behavior inside LWLockRelease.
         */
        for (i = NUM_BUFFER_PARTITIONS; --i >= 0;) {
            LWLockRelease(GetMainLWLockByIndex(FirstBufMappingLock + i));
        }
    }

    func_ctx = SRF_PERCALL_SETUP();

    /* Get the saved state */
    fctx = (BufferCachePagesContext*)func_ctx->user_fctx;

    if (func_ctx->call_cntr < func_ctx->max_calls) {
        uint32 i = func_ctx->call_cntr;
        Datum values[NUM_BUFFERCACHE_PAGES_ELEM];
        bool nulls[NUM_BUFFERCACHE_PAGES_ELEM] = {false};

        values[0] = Int32GetDatum(fctx->record[i].bufferid);
        nulls[0] = false;

        /*
         * Set all fields except the bufferid to null if the buffer is unused
         * or not valid.
         */
        if (fctx->record[i].blocknum == InvalidBlockNumber || fctx->record[i].isvalid == false) {
            nulls[1] = true;
            nulls[2] = true;
            nulls[3] = true;
            nulls[4] = true;
            nulls[5] = true;
            nulls[6] = true;
            nulls[7] = true;
            nulls[8] = true;
        } else {
            values[1] = ObjectIdGetDatum(fctx->record[i].relfilenode);
            nulls[1] = false;
            values[2] = Int16GetDatum(fctx->record[i].bucketnode);
            nulls[2] = false;
            values[3] = ObjectIdGetDatum(fctx->record[i].reltablespace);
            nulls[3] = false;
            values[4] = ObjectIdGetDatum(fctx->record[i].reldatabase);
            nulls[4] = false;
            values[5] = ObjectIdGetDatum(fctx->record[i].forknum);
            nulls[5] = false;
            values[6] = Int64GetDatum((int64)fctx->record[i].blocknum);
            nulls[6] = false;
            values[7] = BoolGetDatum(fctx->record[i].isdirty);
            nulls[7] = false;
            values[8] = Int16GetDatum(fctx->record[i].usagecount);
            nulls[8] = false;
        }

        /* Build and return the tuple. */
        tuple = heap_form_tuple(fctx->tupdesc, values, nulls);
        result = HeapTupleGetDatum(tuple);

        SRF_RETURN_NEXT(func_ctx, result);
    } else {
        SRF_RETURN_DONE(func_ctx);
    }
}

Datum pv_session_time(PG_FUNCTION_ARGS)
{
    FuncCallContext* func_ctx = NULL;
    SessionTimeEntry* array = NULL;

    /* stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL()) {
        MemoryContext old_context;
        TupleDesc tup_desc;

        /* create a function context for cross-call persistence */
        func_ctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function
         * calls
         */
        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        /* need a tuple descriptor representing 4 columns */
        tup_desc = CreateTemplateTupleDesc(4, false);

        TupleDescInitEntry(tup_desc, (AttrNumber)1, "sessid", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)2, "stat_id", INT4OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)3, "stat_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)4, "value", INT8OID, -1, 0);

        /* complete descriptor of the tupledesc */
        func_ctx->tuple_desc = BlessTupleDesc(tup_desc);

        func_ctx->user_fctx = (void*)getSessionTimeStatus(&(func_ctx->max_calls));

        (void)MemoryContextSwitchTo(old_context);
    }

    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();
    array = (SessionTimeEntry*)func_ctx->user_fctx;

    if (func_ctx->call_cntr < func_ctx->max_calls) {
        /* do when there is more left to send */
        Datum values[4];
        bool nulls[4] = {false};
        HeapTuple tuple = NULL;
        SessionTimeEntry* entry = NULL;
        char sessId[SESSION_ID_LEN];
        uint32 entryIndex, statId;
        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        entryIndex = func_ctx->call_cntr / (uint32)TOTAL_TIME_INFO_TYPES;
        statId = func_ctx->call_cntr % (uint32)TOTAL_TIME_INFO_TYPES;

        entry = array + entryIndex;
        getSessionID(sessId, entry->myStartTime, entry->sessionid);
        values[0] = CStringGetTextDatum(sessId);
        values[1] = Int32GetDatum(statId);
        values[2] = CStringGetTextDatum(TimeInfoTypeName[statId]);
        values[3] = Int64GetDatum(entry->array[statId]);

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    } else {
        pfree_ext(array);
        /* do when there is no more left */
        SRF_RETURN_DONE(func_ctx);
    }
}

Datum pv_instance_time(PG_FUNCTION_ARGS)
{
    FuncCallContext* func_ctx = NULL;
    SessionTimeEntry* instance_entry = NULL;

    /* stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL()) {
        MemoryContext old_context;
        TupleDesc tup_desc;

        /* create a function context for cross-call persistence */
        func_ctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function
         * calls
         */
        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        /* need a tuple descriptor representing 4 columns */
        tup_desc = CreateTemplateTupleDesc(3, false);

        TupleDescInitEntry(tup_desc, (AttrNumber)1, "stat_id", INT4OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)2, "stat_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)3, "value", INT8OID, -1, 0);

        /* complete descriptor of the tupledesc */
        func_ctx->tuple_desc = BlessTupleDesc(tup_desc);

        func_ctx->user_fctx = (void*)getInstanceTimeStatus();
        func_ctx->max_calls = TOTAL_TIME_INFO_TYPES;

        (void)MemoryContextSwitchTo(old_context);
    }

    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();
    instance_entry = (SessionTimeEntry*)func_ctx->user_fctx;

    if (func_ctx->call_cntr < func_ctx->max_calls) {
        /* do when there is more left to send */
        Datum values[3];
        bool nulls[3] = {false};
        HeapTuple tuple = NULL;

        values[0] = Int32GetDatum(func_ctx->call_cntr);
        nulls[0] = false;
        values[1] = CStringGetTextDatum(TimeInfoTypeName[func_ctx->call_cntr]);
        nulls[1] = false;
        values[2] = Int64GetDatum(instance_entry->array[func_ctx->call_cntr]);
        nulls[2] = false;

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    } else {
        pfree_ext(instance_entry);
        /* do when there is no more left */
        SRF_RETURN_DONE(func_ctx);
    }
}

Datum pg_stat_get_redo_stat(PG_FUNCTION_ARGS)
{
    FuncCallContext* func_ctx = NULL;
    PgStat_RedoEntry* redo_entry = &redoStatistics;

    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tup_desc;
        MemoryContext old_context;

        /* create a function context for cross-call persistence */
        func_ctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function calls
         */
        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        /* build tup_desc for result tuples */
        /* this had better match pv_plan view in system_views.sql */
        tup_desc = CreateTemplateTupleDesc(7, false);
        TupleDescInitEntry(tup_desc, (AttrNumber)1, "phywrts", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)2, "phyblkwrt", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)3, "writetim", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)4, "avgiotim", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)5, "lstiotim", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)6, "miniotim", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)7, "maxiowtm", INT8OID, -1, 0);

        func_ctx->tuple_desc = BlessTupleDesc(tup_desc);
        func_ctx->max_calls = 1;
        func_ctx->call_cntr = 0;

        (void)MemoryContextSwitchTo(old_context);
    }

    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->call_cntr < func_ctx->max_calls) {
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
        values[0] = Int64GetDatum(redo_entry->writes);
        values[1] = Int64GetDatum(redo_entry->writeBlks);
        values[2] = Int64GetDatum(redo_entry->writeTime);
        values[3] = Int64GetDatum(redo_entry->avgIOTime);
        values[4] = Int64GetDatum(redo_entry->lstIOTime);
        values[5] = Int64GetDatum(redo_entry->minIOTime);
        values[6] = Int64GetDatum(redo_entry->maxIOTime);
        LWLockRelease(WALWriteLock);

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    } else {
        /* nothing left */
        SRF_RETURN_DONE(func_ctx);
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
    "",             /* n_commit */
    "",             /* n_rollback */
    "",             /* n_sql */
    "",             /* n_table_scan */
    "",             /* n_blocks_fetched */
    "",             /* n_physical_read_operation */
    "",             /* n_shared_blocks_dirtied */
    "",             /* n_local_blocks_dirtied */
    "",             /* n_shared_blocks_read */
    "",             /* n_local_blocks_read */
    "microseconds", /* n_blocks_read_time */
    "microseconds", /* n_blocks_write_time */
    "",             /* n_sort_in_memory */
    "",             /* n_sort_in_disk */
    "",             /* n_cu_mem_hit */
    "",             /* n_cu_hdd_sync_read */
    ""              /* n_cu_hdd_asyn_read */
};

Datum pv_session_stat(PG_FUNCTION_ARGS)
{
    FuncCallContext* func_ctx = NULL;
    SessionLevelStatistic* array = NULL;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext old_context;
        TupleDesc tup_desc;

        /* create a function context for cross-call persistence */
        func_ctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function
         * calls
         */
        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        /* need a tuple descriptor representing 5 columns */
        tup_desc = CreateTemplateTupleDesc(5, false);

        TupleDescInitEntry(tup_desc, (AttrNumber)1, "sessid", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)2, "statid", INT4OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)3, "statname", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)4, "statunit", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)5, "value", INT8OID, -1, 0);

        /* complete descriptor of the tupledesc */
        func_ctx->tuple_desc = BlessTupleDesc(tup_desc);

        func_ctx->user_fctx = (void*)getSessionStatistics(&(func_ctx->max_calls));

        (void)MemoryContextSwitchTo(old_context);
    }
    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();
    array = (SessionLevelStatistic*)func_ctx->user_fctx;

    if (func_ctx->call_cntr < func_ctx->max_calls) {
        Datum values[5];
        bool nulls[5] = {false};
        HeapTuple tuple = NULL;
        SessionLevelStatistic* entry = NULL;
        char sess_Id[SESSION_ID_LEN];
        uint32 entry_index;
		uint32 stat_id;
        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        entry_index = func_ctx->call_cntr / (uint32)N_TOTAL_SESSION_STATISTICS_TYPES;
        stat_id = func_ctx->call_cntr % (uint32)N_TOTAL_SESSION_STATISTICS_TYPES;

        entry = array + entry_index;
        getSessionID(sess_Id, entry->sessionStartTime, entry->sessionid);
        values[0] = CStringGetTextDatum(sess_Id);
        values[1] = Int32GetDatum(stat_id);
        values[2] = CStringGetTextDatum(SessionStatisticsTypeName[stat_id]);
        values[3] = CStringGetTextDatum(SessionStatisticsTypeUnit[stat_id]);
        values[4] = Int64GetDatum(entry->array[stat_id]);

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    } else {
        SRF_RETURN_DONE(func_ctx);
    }
}

Datum pv_session_memory(PG_FUNCTION_ARGS)
{
    FuncCallContext* func_ctx = NULL;
    SessionLevelMemory* array = NULL;

    if (!t_thrd.utils_cxt.gs_mp_inited) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("unsupported view for memory protection feature is disabled.")));
    }

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext old_context;
        TupleDesc tup_desc;

        /* create a function context for cross-call persistence */
        func_ctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function
         * calls
         */
        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        /* need a tuple descriptor representing 5 columns */
        tup_desc = CreateTemplateTupleDesc(4, false);

        TupleDescInitEntry(tup_desc, (AttrNumber)1, "sessid", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)2, "init_mem", INT4OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)3, "used_mem", INT4OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)4, "peak_mem", INT4OID, -1, 0);

        /* complete descriptor of the tupledesc */
        func_ctx->tuple_desc = BlessTupleDesc(tup_desc);

        func_ctx->user_fctx = (void*)getSessionMemory(&(func_ctx->max_calls));

        (void)MemoryContextSwitchTo(old_context);
    }
    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();
    array = (SessionLevelMemory*)func_ctx->user_fctx;

    if (func_ctx->call_cntr < func_ctx->max_calls) {
        Datum values[5];
        bool nulls[5] = {false};
        HeapTuple tuple = NULL;
        SessionLevelMemory* entry = NULL;
        uint32 entry_index;
        char sess_id[SESSION_ID_LEN];

        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        rc = memset_s(sess_id, sizeof(sess_id), 0, sizeof(sess_id));
        securec_check(rc, "\0", "\0");

        entry_index = func_ctx->call_cntr;

        entry = array + entry_index;
        getSessionID(sess_id, entry->threadStartTime, entry->sessionid);
        values[0] = CStringGetTextDatum(sess_id);
        values[1] = Int32GetDatum(entry->initMemInChunks << (chunkSizeInBits - BITS_IN_MB));
        values[2] = Int32GetDatum((entry->queryMemInChunks - entry->initMemInChunks) << (chunkSizeInBits - BITS_IN_MB));
        values[3] = Int32GetDatum((entry->peakChunksQuery - entry->initMemInChunks) << (chunkSizeInBits - BITS_IN_MB));

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    } else {
        SRF_RETURN_DONE(func_ctx);
    }
}

Datum pg_stat_get_file_stat(PG_FUNCTION_ARGS)
{
    FuncCallContext* func_ctx = NULL;
    PgStat_FileEntry* file_entry = NULL;

    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tup_desc;
        MemoryContext old_context;

        /* create a function context for cross-call persistence */
        func_ctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function calls
         */
        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        /* build tup_desc for result tuples */
        /* this had better match pv_plan view in system_views.sql */
        tup_desc = CreateTemplateTupleDesc(13, false);
        TupleDescInitEntry(tup_desc, (AttrNumber)1, "filenum", OIDOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)2, "dbid", OIDOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)3, "spcid", OIDOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)4, "phyrds", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)5, "phywrts", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)6, "phyblkrd", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)7, "phyblkwrt", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)8, "readtim", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)9, "writetim", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)10, "avgiotim", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)11, "lstiotim", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)12, "miniotim", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)13, "maxiowtm", INT8OID, -1, 0);

        func_ctx->tuple_desc = BlessTupleDesc(tup_desc);
        func_ctx->max_calls = NUM_FILES;
        file_entry = (PgStat_FileEntry*)&pgStatFileArray[NUM_FILES - 1];
        if (file_entry->dbid == 0) {
            func_ctx->max_calls = fileStatCount;
        }
        func_ctx->call_cntr = 0;

        (void)MemoryContextSwitchTo(old_context);
    }

    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->call_cntr < func_ctx->max_calls) {
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

        file_entry = (PgStat_FileEntry*)&pgStatFileArray[func_ctx->call_cntr];
        /* Values available to all callers */
        values[0] = ObjectIdGetDatum(file_entry->fn);

        if (file_entry->dbid == 0) {
            nulls[1] = true;
        } else {
            values[1] = ObjectIdGetDatum(file_entry->dbid);
        }

        values[2] = ObjectIdGetDatum(file_entry->spcid);
        values[3] = Int64GetDatum(file_entry->reads);
        values[4] = Int64GetDatum(file_entry->writes);
        values[5] = Int64GetDatum(file_entry->readBlks);
        values[6] = Int64GetDatum(file_entry->writeBlks);
        values[7] = Int64GetDatum(file_entry->readTime);
        values[8] = Int64GetDatum(file_entry->writeTime);
        values[9] = Int64GetDatum(file_entry->avgIOTime);
        values[10] = Int64GetDatum(file_entry->lstIOTime);
        values[11] = Int64GetDatum(file_entry->minIOTime);
        values[12] = Int64GetDatum(file_entry->maxIOTime);

        LWLockRelease(FileStatLock);

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    } else {
        /* nothing left */
        SRF_RETURN_DONE(func_ctx);
    }
}

Datum get_local_rel_iostat(PG_FUNCTION_ARGS)
{
    FuncCallContext* func_ctx = NULL;
    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tup_desc;
        MemoryContext old_context;

        /* create a function context for cross-call persistence */
        func_ctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function calls
         */
        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        /* build tup_desc for result tuples */
        tup_desc = CreateTemplateTupleDesc(4, false);
        TupleDescInitEntry(tup_desc, (AttrNumber)1, "phyrds", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)2, "phywrts", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)3, "phyblkrd", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)4, "phyblkwrt", INT8OID, -1, 0);
        func_ctx->tuple_desc = BlessTupleDesc(tup_desc);
        func_ctx->max_calls = 1;
        func_ctx->call_cntr = 0;
        (void)MemoryContextSwitchTo(old_context);
    }

    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->call_cntr < func_ctx->max_calls) {
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

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    } else {
        /* nothing left */
        SRF_RETURN_DONE(func_ctx);
    }
}
Datum total_cpu(PG_FUNCTION_ARGS)
{
    int fd = -1;
    char temp_buf[512] = {0};
    char* S = NULL;
    int64 cpu_usage = 0;
    unsigned long long cpu_utime, cpu_stime;
    int num_read;
    int rc = EOK;

    rc = snprintf_s(temp_buf, sizeof(temp_buf), 256, "/proc/%d/stat", getpid());
    securec_check_ss(rc, "\0", "\0");

    fd = open(temp_buf, O_RDONLY, 0);
    if (fd < 0) {
        PG_RETURN_INT64(cpu_usage);
    }

    num_read = read(fd, temp_buf, sizeof(temp_buf) - 1);
    close(fd);

    if (num_read < 0) {
        PG_RETURN_INT64(cpu_usage);
    }
    temp_buf[num_read] = '\0';
    S = temp_buf;

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
        } else {
            ereport(LOG,
                (errmsg("ERROR at %s : %d : The destination buffer or format is a NULL pointer or the invalid "
                        "parameter handle is invoked..\n",
                    __FILE__,
                    __LINE__)));
        }
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
    char* session_id = PG_GETARG_CSTRING(0);
    char* tmp = NULL;
    if (session_id == NULL) {
        PG_RETURN_INT64(0);
    }

    tmp = strrchr(session_id, '.');
    if (tmp == NULL) {
        PG_RETURN_INT64(0);
    }
    tmp++;

    pid = atoll(tmp);

    PG_RETURN_INT64(pid);
}

Datum total_memory(PG_FUNCTION_ARGS)
{
    int fd = -1;
    char temp_buf[1024] = {0};
    char* S = NULL;
    char* tmp = NULL;
    int64 total_vm = 0;
    int num_read;
    int rc = 0;

    rc = snprintf_s(temp_buf, sizeof(temp_buf), 256, "/proc/%d/status", getpid());
    securec_check_ss(rc, "\0", "\0");

    fd = open(temp_buf, O_RDONLY, 0);
    if (fd < 0) {
        PG_RETURN_INT64(total_vm);
    }

    num_read = read(fd, temp_buf, sizeof(temp_buf) - 1);
    close(fd);

    if (num_read < 0) {
        PG_RETURN_INT64(total_vm);
    }
    temp_buf[num_read] = '\0';
    S = temp_buf;

    for (;;) {
        if (0 == strncmp("VmSize", S, 6)) {
            tmp = strchr(S, ':');
            if (tmp == NULL) {
                break;
            }
            tmp = tmp + 2;
            total_vm = strtol(tmp, &tmp, 10);
            break;
        }
        S = strchr(S, '\n');
        if (S == NULL) {
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
    int2 att_num;

    if (u_sess->exec_cxt.global_bucket_map == NULL) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("Bucketmap is NULL")));
    }

    int bucket_index;

    FunctionCallInfoData func_info;

    InitFunctionCallInfoData(func_info, NULL, 2, InvalidOid, NULL, NULL);

    func_info.arg[0] = array;
    func_info.arg[1] = CharGetDatum(flag);
    func_info.argnull[0] = false;
    func_info.argnull[1] = false;

    bucket_index = getbucket(&func_info);

    if (func_info.isnull) {
        att_num = 0;
    } else {
        att_num = u_sess->exec_cxt.global_bucket_map[bucket_index];
    }

    PG_RETURN_INT16(att_num);
}

ScalarVector* vtable_data_skewness(PG_FUNCTION_ARGS)
{
    VectorBatch* batch = (VectorBatch*)PG_GETARG_DATUM(0);
    ScalarVector* vec_flag = PG_GETARG_VECTOR(1);
    int32 nvalues = PG_GETARG_INT32(2);

    ScalarVector* presult_Vector = PG_GETARG_VECTOR(3);
    uint8* pflags_res = presult_Vector->m_flag;
    bool* pselection = PG_GETARG_SELECTION(4);

    if (u_sess->exec_cxt.global_bucket_map == NULL) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("Bucketmap is NULL")));
    }

    ScalarVector* bucketIndex = (ScalarVector*)DirectFunctionCall5((PGFunction)vgetbucket,
        PointerGetDatum(batch),
        PointerGetDatum(vec_flag),
        (Datum)nvalues,
        PointerGetDatum(presult_Vector),
        PointerGetDatum(pselection));

    int row_num = bucketIndex->m_rows;

    if (pselection == NULL) {
        for (int i = 0; i < row_num; i++) {
            if (!bucketIndex->IsNull(i)) {
                int index = bucketIndex->m_vals[i];
                bucketIndex->m_vals[i] = u_sess->exec_cxt.global_bucket_map[index];
            } else {
                bucketIndex->m_vals[i] = 0;
                SET_NOTNULL(pflags_res[i]);
            }
        }
    } else {
        for (int i = 0; i < row_num; i++) {
            if (pselection[i]) {
                if (!bucketIndex->IsNull(i)) {
                    int index = bucketIndex->m_vals[i];
                    bucketIndex->m_vals[i] = u_sess->exec_cxt.global_bucket_map[index];
                } else {
                    bucketIndex->m_vals[i] = 0;
                    SET_NOTNULL(pflags_res[i]);
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

    /* recursive MemoryContext's child */
    for (child = context->firstchild; child != NULL; child = child->nextchild) {
        gs_recursive_memctx_dump(child, memBuf);
    }
}

static const char* dump_dir = "/tmp/dumpmem";

/* dump the memory context tree when the thread id is specified */
void DumpMemoryContextTree(ThreadId tid)
{
#define DUMPFILE_BUFF_SIZE 128

    // create directory "/tmp/dumpmem" to save memory context log file
    int ret;
    struct stat info;
    if (stat(dump_dir, &info) == 0) {
        if (!S_ISDIR(info.st_mode)) {
            // S_ISDIR() doesn't exist on my windows
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
    char dump_file[128] = {0};
    int rc =
        sprintf_s(dump_file, DUMPFILE_BUFF_SIZE, "%s/%lu_%lu.log", dump_dir, (unsigned long)tid, (uint64)time(NULL));
    securec_check_ss(rc, "\0", "\0");
    FILE* dump_fp = fopen(dump_file, "w");
    if (NULL == dump_fp) {
        elog(LOG, "dump_memory: Failed to create file: %s:%m", dump_file);
        return;
    }

    // 4. walk all memory context
    PG_TRY();
    {
        StringInfoData mem_buf;

        initStringInfo(&mem_buf);

        /* dump the top memory context */
        HOLD_INTERRUPTS();

        for (int idx = 0; idx < (int)g_instance.proc_base->allProcCount; idx++) {
            volatile PGPROC* proc = (g_instance.proc_base_all_procs[idx]);

            if (proc->pid == tid) {
                /* lock this proc's delete MemoryContext action */
                (void)syscalllockAcquire(&((PGPROC*)proc)->deleMemContextMutex);
                if (NULL != proc->topmcxt)
                    gs_recursive_memctx_dump(proc->topmcxt, &mem_buf);
                (void)syscalllockRelease(&((PGPROC*)proc)->deleMemContextMutex);
                break;
            }
        }

        RESUME_INTERRUPTS();
        uint64 bytes = fwrite(mem_buf.data, 1, mem_buf.len, dump_fp);
        if (bytes != (uint64)mem_buf.len) {
            elog(LOG, "Could not write memory usage information. Attempted to write %d", mem_buf.len);
        }
        pfree(mem_buf.data);
    }
    PG_CATCH();
    {
        fclose(dump_fp);
        return;
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
    if (tid == 0 || tid == PostmasterPid) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("invalid thread id %ld for it is 0 or postmaster pid.", tid)));
    }

    char* ctx_name = PG_GETARG_CSTRING(1);
    if (ctx_name == NULL || strlen(ctx_name) == 0) {
        DumpMemoryContextTree(tid);
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
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), 
                (errmsg("Must be system admin to dump memory context."))));
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
#ifndef ENABLE_MULTIPLE_NODES
    FuncCallContext* func_ctx = NULL;
    ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("unsupported view in single node mode.")));
    /* do when there is no more left */
    SRF_RETURN_DONE(func_ctx);
#else
    FuncCallContext* func_ctx = NULL;
    static int* ng_mem_size = NULL;
    static char** ng_name = NULL;
    static uint32 allcnt = 0;
    static uint32 cnti = 0;
    static uint32 cntj = 0;

    if (!t_thrd.utils_cxt.gs_mp_inited) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("unsupported view for memory protection feature is disabled.")));
    }

    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tup_desc;
        MemoryContext old_context;

        /* create a function context for cross-call persistence */
        func_ctx = SRF_FIRSTCALL_INIT();

        /*
         * Switch to memory context appropriate for multiple function calls
         */
        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        /* build tup_desc for result tuples */
        tup_desc = CreateTemplateTupleDesc(3, false);

        TupleDescInitEntry(tup_desc, (AttrNumber)1, "ngname", TEXTOID, -1, 0);

        TupleDescInitEntry(tup_desc, (AttrNumber)2, "memorytype", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)3, "memorymbytes", INT4OID, -1, 0);

        func_ctx->tuple_desc = BlessTupleDesc(tup_desc);

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

        ng_mem_size = (int*)palloc0(cnt * NG_MEMORY_TYPES_CNT * sizeof(int));
        ng_name = (char**)palloc0(cnt * sizeof(char*));

        WLMNodeGroupInfo* hdata = &g_instance.wlm_cxt->MyDefaultNodeGroup;

        ng_mem_size[num++] = hdata->total_memory;
        ng_mem_size[num++] = hdata->used_memory;
        ng_mem_size[num++] = hdata->estimate_memory;
        num += (NG_MEMORY_TYPES_CNT - 3); /* no foreign rp in default node group */
        ng_name[i++] = pstrdup(hdata->group_name);

        /* search all node group to get its resource usage */
        HASH_SEQ_STATUS hash_seq;
        hash_seq_init(&hash_seq, g_instance.wlm_cxt->stat_manager.node_group_hashtbl);

        while ((hdata = (WLMNodeGroupInfo*)hash_seq_search(&hash_seq)) != NULL) {
            if (!hdata->used) {
                continue;
            }

            ng_mem_size[num++] = hdata->total_memory;
            ng_mem_size[num++] = hdata->used_memory;
            ng_mem_size[num++] = hdata->estimate_memory;
            if (hdata->foreignrp) {
                ng_mem_size[num++] = hdata->foreignrp->memsize;
                ng_mem_size[num++] = hdata->foreignrp->memused;
                ng_mem_size[num++] = hdata->foreignrp->peakmem;
                ng_mem_size[num++] = hdata->foreignrp->actpct;
                ng_mem_size[num++] = hdata->foreignrp->estmsize;
            } else {
                num += (NG_MEMORY_TYPES_CNT - 3);  // default as 0
            }

            ng_name[i++] = pstrdup(hdata->group_name);
        }

        cnti = 0;
        cntj = 0;
        allcnt = num;

        (void)MemoryContextSwitchTo(old_context);
    }

    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();

    while (func_ctx->call_cntr < allcnt) {
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
        values[0] = CStringGetTextDatum(ng_name[cntj]);
        values[1] = CStringGetTextDatum(NGMemoryTypeName[func_ctx->call_cntr % NG_MEMORY_TYPES_CNT]);
        values[2] = (Datum)(ng_mem_size[cnti]);

        cnti++;

        if (cnti % NG_MEMORY_TYPES_CNT == 0) {
            pfree_ext(ng_name[cntj]);
            cntj++;
        }

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    }

    pfree_ext(ng_mem_size);
    pfree_ext(ng_name);

    /* do when there is no more left */
    SRF_RETURN_DONE(func_ctx);
#endif
}

#define MEMORY_TYPES_CNT 22
const char* MemoryTypeName[] = {"max_process_memory",
    "process_used_memory",
    "max_dynamic_memory",
    "dynamic_used_memory",
    "dynamic_peak_memory",
    "dynamic_used_shrctx",
    "dynamic_peak_shrctx",
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
    FuncCallContext* func_ctx = NULL;
    static int mem_size[MEMORY_TYPES_CNT];
    errno_t rc = 0;

    if (!t_thrd.utils_cxt.gs_mp_inited) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("unsupported view for memory protection feature is disabled.")));
    }

    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tup_desc;
        MemoryContext old_context;

        /* create a function context for cross-call persistence */
        func_ctx = SRF_FIRSTCALL_INIT();

        /*
         * Switch to memory context appropriate for multiple function calls
         */
        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        /* build tup_desc for result tuples */
        tup_desc = CreateTemplateTupleDesc(3, false);

        TupleDescInitEntry(tup_desc, (AttrNumber)1, "nodename", TEXTOID, -1, 0);

        TupleDescInitEntry(tup_desc, (AttrNumber)2, "memorytype", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)3, "memorymbytes", INT4OID, -1, 0);

        func_ctx->tuple_desc = BlessTupleDesc(tup_desc);

        (void)MemoryContextSwitchTo(old_context);

        rc = memset_s(mem_size, sizeof(mem_size), 0, sizeof(mem_size));
        securec_check(rc, "\0", "\0");

        int mctx_used_size = processMemInChunks << (chunkSizeInBits - BITS_IN_MB);
        int cu_size = (CUCache->GetCurrentMemSize() + MetaCacheGetCurrentUsedSize()) >> BITS_IN_MB;
        int comm_size = (int)(gs_get_comm_used_memory() >> BITS_IN_MB);
        int comm_peak_size = (int)(gs_get_comm_peak_memory() >> BITS_IN_MB);
        unsigned long total_vm = 0;
        unsigned long res = 0;
        unsigned long shared = 0;
        unsigned long text = 0;
        unsigned long lib;
        unsigned long data;
        unsigned long dt;
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
        mem_size[2] = (int)(maxChunksPerProcess << (chunkSizeInBits - BITS_IN_MB));
        mem_size[3] = mctx_used_size;
        if (mem_size[3] < MEMPROT_INIT_SIZE) {
            mem_size[3] = MEMPROT_INIT_SIZE;
        }
        mem_size[4] = (int)(peakChunksPerProcess << (chunkSizeInBits - BITS_IN_MB));
        mem_size[5] = (int)(shareTrackedMemChunks << (chunkSizeInBits - BITS_IN_MB));
        mem_size[6] = (int)(peakChunksSharedContext << (chunkSizeInBits - BITS_IN_MB));
        mem_size[7] = maxSharedMemory;
        mem_size[8] = shared;
        mem_size[9] = (int)(g_instance.attr.attr_storage.cstore_buffers >> BITS_IN_KB);
        mem_size[10] = cu_size;
        if (comm_size) {
            mem_size[11] = comm_original_memory;
        } else {
            mem_size[11] = 0;
        }
        mem_size[12] = comm_size;
        mem_size[13] = comm_peak_size;

#ifdef ENABLE_MULTIPLE_NODES
        if (is_searchserver_api_load()) {
            void* mem_info = get_searchlet_resource_info(&mem_size[16], &mem_size[17]);
            if (mem_info != NULL) {
                mem_size[15] = g_searchserver_memory;
                pfree_ext(mem_info);
            }
        }
#else
        mem_size[15] = 0;
#endif

        mem_size[14] = (int)(res - shared - text) - mctx_used_size - cu_size - mem_size[16];
        if (mem_size[14] < 0) {
            // res may be changed
            mem_size[14] = 0;
        }

#ifdef ENABLE_MULTIPLE_NODES
        PoolConnStat conn_stat;
        pooler_get_connection_statinfo(&conn_stat);
        mem_size[18] = (int)(conn_stat.memory_size >> BITS_IN_MB);
        mem_size[19] = (int)(conn_stat.free_memory_size >> BITS_IN_MB);
#else
        mem_size[18] = 0;
        mem_size[19] = 0;
#endif
        mem_size[20] = (int)(storageTrackedBytes >> BITS_IN_MB);

        Assert(g_instance.attr.attr_sql.udf_memory_limit >= UDF_DEFAULT_MEMORY);
        mem_size[21] = (int)((g_instance.attr.attr_sql.udf_memory_limit - UDF_DEFAULT_MEMORY) >> BITS_IN_KB);
        ereport(LOG,
            (errmsg("LLVM IR file count is %ld, total memory is %ldKB",
                g_instance.codegen_IRload_process_count,
                g_instance.codegen_IRload_process_count * IR_FILE_SIZE / 1024)));
    }

    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();

    while (func_ctx->call_cntr < MEMORY_TYPES_CNT) {
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
        values[1] = CStringGetTextDatum(MemoryTypeName[func_ctx->call_cntr]);
        values[2] = (Datum)(mem_size[func_ctx->call_cntr]);

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    }

    /* do when there is no more left */
    SRF_RETURN_DONE(func_ctx);
}

Datum pg_stat_get_pooler_status(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_MULTIPLE_NODES
    FuncCallContext* func_ctx = NULL;
    ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("unsupported view in single node mode.")));
    /* do when there is no more left */
    SRF_RETURN_DONE(func_ctx);
#else
    FuncCallContext* func_ctx = NULL;
    PoolConnectionInfo* conns_entry = NULL;
    const int ARG_NUMS = 9;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext old_context = NULL;
        TupleDesc tup_desc = NULL;

        func_ctx = SRF_FIRSTCALL_INIT();

        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        tup_desc = CreateTemplateTupleDesc(ARG_NUMS, false);
        TupleDescInitEntry(tup_desc, (AttrNumber) ARG_1, "database_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber) ARG_2, "user_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber) ARG_3, "threadid", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber) ARG_4, "pgoptions", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber) ARG_5, "node_oid", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber) ARG_6, "in_use", BOOLOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber) ARG_7, "session_params", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber) ARG_8, "fdsock", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber) ARG_9, "remote_pid", INT8OID, -1, 0);

        func_ctx->tuple_desc = BlessTupleDesc(tup_desc);

        func_ctx->max_calls = get_pool_connection_info(&conns_entry);
        func_ctx->user_fctx = conns_entry;

        MemoryContextSwitchTo(old_context);
    }

    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();
    conns_entry = (PoolConnectionInfo*)(func_ctx->user_fctx);

    if (func_ctx->call_cntr < func_ctx->max_calls) {
        /* for each row */
        Datum values[ARG_NUMS];
        bool nulls[ARG_NUMS] = {false};
        HeapTuple tuple = NULL;

        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        PoolConnectionInfo* pool_con = conns_entry + func_ctx->call_cntr;

        // pool_con never be NULL
        values[ARR_0] = CStringGetTextDatum(pool_con->database);

        if (pool_con->user_name != NULL) {
            values[ARR_1] = CStringGetTextDatum(pool_con->user_name);
        } else {
            values[ARR_1] = CStringGetTextDatum("none");
        }

        if (pool_con->tid > 0) {
            values[ARR_2] = Int64GetDatum(pool_con->tid);
        } else {
            nulls[ARR_2] = true;
        }

        if (pool_con->pgoptions != NULL) {
            values[ARR_3] = CStringGetTextDatum(pool_con->pgoptions);
        } else {
            values[ARR_3] = CStringGetTextDatum("none");
        }

        values[ARR_4] = Int64GetDatum(pool_con->nodeOid);
        values[ARR_5] = BoolGetDatum(pool_con->in_use);
        if (pool_con->session_params != NULL) {
            values[ARR_6] = CStringGetTextDatum(pool_con->session_params);
        } else {
            values[ARR_6] = CStringGetTextDatum("none");
        }
        values[ARR_7] = Int64GetDatum(pool_con->fdsock);
        values[ARR_8] = Int64GetDatum(pool_con->remote_pid);

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);

        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    } else {
        PoolConnectionInfo* pcon = NULL;
        for (uint32 i = 0; i < func_ctx->max_calls; i++) {
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
        SRF_RETURN_DONE(func_ctx);
    }
#endif
}

void init_pgxc_comm_get_recv_stream(PG_FUNCTION_ARGS, FuncCallContext *funcctx, const int argNums)
{
    TupleDesc tup_desc;

    /* build tup_desc for result tuples */
    tup_desc = CreateTemplateTupleDesc(argNums, false);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_1, "node_name", TEXTOID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_2, "local_tid", INT8OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_3, "remote_name", TEXTOID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_4, "remote_tid", INT8OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_5, "idx", INT4OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_6, "sid", INT4OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_7, "tcp_socket", INT4OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_8, "state", TEXTOID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_9, "query_id", INT8OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_10, "send_smp", INT4OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_11, "recv_smp", INT4OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_12, "pn_id", INT4OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_13, "recv_bytes", INT8OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_14, "time", INT8OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_15, "speed", INT8OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_16, "quota", INT8OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_17, "buff_usize", INT8OID, -1, 0);
    funcctx->tuple_desc = BlessTupleDesc(tup_desc);
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
    FuncCallContext* func_ctx = NULL;
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        errmsg("unsupported view in single node mode.")));

    SRF_RETURN_DONE(func_ctx);
#else
    int i;
    const int RECV_STREAM_TUPLE_NATTS = 17;
    FuncCallContext *func_ctx = NULL;
    Datum values[RECV_STREAM_TUPLE_NATTS];
    bool nulls[RECV_STREAM_TUPLE_NATTS];
    HeapTuple tuple;
    MemoryContext old_context;

    if (SRF_IS_FIRSTCALL()) {
        /* create a function context for cross-call persistence */
        func_ctx = SRF_FIRSTCALL_INIT();
        /* Switch to memory context appropriate for multiple function calls */
        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);
        init_pgxc_comm_get_recv_stream(fcinfo, func_ctx, RECV_STREAM_TUPLE_NATTS);
        /* the main call for get table distribution. */
        func_ctx->user_fctx = getGlobalCommStatus(func_ctx->tuple_desc,  "SELECT * FROM pg_comm_recv_stream;");
        MemoryContextSwitchTo(old_context);
        if (func_ctx->user_fctx == NULL) {
            SRF_RETURN_DONE(func_ctx);
        }
    }

    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->user_fctx) {
        Tuplestorestate *tupstore = ((CommInfoParallel *)func_ctx->user_fctx)->state->tupstore;
        TupleTableSlot *slot = ((CommInfoParallel *)func_ctx->user_fctx)->slot;
        if (!tuplestore_gettupleslot(tupstore, true, false, slot)) {
            FreeParallelFunctionState(((CommInfoParallel *)func_ctx->user_fctx)->state);
            ExecDropSingleTupleTableSlot(slot);
            pfree_ext(func_ctx->user_fctx);
            func_ctx->user_fctx = NULL;
            SRF_RETURN_DONE(func_ctx);
        }
        for (i = 0; i < RECV_STREAM_TUPLE_NATTS; i++) {
            values[i] = slot_getattr(slot, i + 1, &nulls[i]);
        }

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        (void)ExecClearTuple(slot);
        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    }

    SRF_RETURN_DONE(func_ctx);
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
    FuncCallContext* func_ctx = NULL;
    CommRecvStreamStatus* stream_status = NULL;
    int ii = 0;

    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tup_desc = NULL;
        MemoryContext old_context = NULL;

        /* create a function context for cross-call persistence */
        func_ctx = SRF_FIRSTCALL_INIT();

        /*
         * Switch to memory context appropriate for multiple function calls
         */
        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        /* build tup_desc for result tuples */
        ii = 1;
        tup_desc = CreateTemplateTupleDesc(RECV_STREAM_TUPLE_NATTS, false);
        TupleDescInitEntry(tup_desc, (AttrNumber)ii++, "node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)ii++, "local_tid", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)ii++, "remote_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)ii++, "remote_tid", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)ii++, "idx", INT4OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)ii++, "sid", INT4OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)ii++, "tcp_socket", INT4OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)ii++, "state", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)ii++, "query_id", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)ii++, "send_smp", INT4OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)ii++, "recv_smp", INT4OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)ii++, "pn_id", INT4OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)ii++, "recv_bytes", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)ii++, "time", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)ii++, "speed", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)ii++, "quota", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)ii++, "buff_usize", INT8OID, -1, 0);

        func_ctx->tuple_desc = BlessTupleDesc(tup_desc);

        stream_status = (CommRecvStreamStatus*)palloc(sizeof(CommRecvStreamStatus));
        errno_t rc = memset_s(stream_status, sizeof(CommRecvStreamStatus), 0, sizeof(CommRecvStreamStatus));
        securec_check(rc, "\0", "\0");
        func_ctx->user_fctx = (void*)stream_status;
        stream_status->stream_id = -1;

        MemoryContextSwitchTo(old_context);
    }

    func_ctx = SRF_PERCALL_SETUP();
    stream_status = (CommRecvStreamStatus*)func_ctx->user_fctx;

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

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(func_ctx, result);
    } else {
        SRF_RETURN_DONE(func_ctx);
    }
}

Datum pg_comm_send_stream(PG_FUNCTION_ARGS)
{
    const int SEND_STREAM_TUPLE_NATTS = 17;
    FuncCallContext* func_ctx = NULL;
    CommSendStreamStatus* stream_status = NULL;
    int ii = 0;
    errno_t rc = 0;

    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tup_desc;
        MemoryContext old_context;

        /* create a function context for cross-call persistence */
        func_ctx = SRF_FIRSTCALL_INIT();

        /*
         * Switch to memory context appropriate for multiple function calls
         */
        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        /* build tup_desc for result tuples */
        ii = 1;
        tup_desc = CreateTemplateTupleDesc(SEND_STREAM_TUPLE_NATTS, false);
        TupleDescInitEntry(tup_desc, (AttrNumber)ii++, "node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)ii++, "local_tid", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)ii++, "remote_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)ii++, "remote_tid", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)ii++, "idx", INT4OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)ii++, "sid", INT4OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)ii++, "tcp_socket", INT4OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)ii++, "state", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)ii++, "query_id", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)ii++, "pn_id", INT4OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)ii++, "send_smp", INT4OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)ii++, "recv_smp", INT4OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)ii++, "send_bytes", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)ii++, "time", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)ii++, "speed", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)ii++, "quota", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)ii++, "wait_quota", INT8OID, -1, 0);

        func_ctx->tuple_desc = BlessTupleDesc(tup_desc);

        stream_status = (CommSendStreamStatus*)palloc(sizeof(CommSendStreamStatus));
        rc = memset_s(stream_status, sizeof(CommSendStreamStatus), 0, sizeof(CommSendStreamStatus));
        securec_check(rc, "\0", "\0");
        func_ctx->user_fctx = (void*)stream_status;
        stream_status->stream_id = -1;

        MemoryContextSwitchTo(old_context);
    }

    func_ctx = SRF_PERCALL_SETUP();
    stream_status = (CommSendStreamStatus*)func_ctx->user_fctx;

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

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(func_ctx, result);
    } else {
        SRF_RETURN_DONE(func_ctx);
    }
}

void init_pgxc_comm_get_send_stream(PG_FUNCTION_ARGS, FuncCallContext *func_ctx, const int argNums)
{
    TupleDesc tup_desc;

    /* build tup_desc for result tuples */
    tup_desc = CreateTemplateTupleDesc(argNums, false);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_1, "node_name", TEXTOID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_2, "local_tid", INT8OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_3, "remote_name", TEXTOID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_4, "remote_tid", INT8OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_5, "idx", INT4OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_6, "sid", INT4OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_7, "tcp_socket", INT4OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_8, "state", TEXTOID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_9, "query_id", INT8OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_10, "pn_id", INT4OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_11, "send_smp", INT4OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_12, "recv_smp", INT4OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_13, "send_bytes", INT8OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_14, "time", INT8OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_15, "speed", INT8OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_16, "quota", INT8OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_17, "wait_quota", INT8OID, -1, 0);

    func_ctx->tuple_desc = BlessTupleDesc(tup_desc);
    func_ctx->max_calls = u_sess->pgxc_cxt.NumDataNodes + u_sess->pgxc_cxt.NumCoords;
    return;
}

Datum global_comm_get_send_stream(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_MULTIPLE_NODES
    FuncCallContext* func_ctx = NULL;
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        errmsg("unsupported view in single node mode.")));

    SRF_RETURN_DONE(func_ctx);
#else
    const int SEND_STREAM_TUPLE_NATTS = 17;
    FuncCallContext *func_ctx = NULL;
    Datum values[SEND_STREAM_TUPLE_NATTS];
    bool nulls[SEND_STREAM_TUPLE_NATTS];
    HeapTuple tuple;
    MemoryContext old_context;

    if (SRF_IS_FIRSTCALL()) {
        /* create a function context for cross-call persistence */
        func_ctx = SRF_FIRSTCALL_INIT();
        /*
         * Switch to memory context appropriate for multiple function calls
         */
        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);
        init_pgxc_comm_get_send_stream(fcinfo, func_ctx, SEND_STREAM_TUPLE_NATTS);
        /* the main call for get table distribution. */
        func_ctx->user_fctx = getGlobalCommStatus(func_ctx->tuple_desc, "SELECT * FROM pg_comm_send_stream;");
        MemoryContextSwitchTo(old_context);
        if (func_ctx->user_fctx == NULL) {
            SRF_RETURN_DONE(func_ctx);
        }
    }

    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->user_fctx) {
        Tuplestorestate *tupstore = ((CommInfoParallel *)func_ctx->user_fctx)->state->tupstore;
        TupleTableSlot *slot = ((CommInfoParallel *)func_ctx->user_fctx)->slot;
        if (!tuplestore_gettupleslot(tupstore, true, false, slot)) {
            FreeParallelFunctionState(((CommInfoParallel *)func_ctx->user_fctx)->state);
            ExecDropSingleTupleTableSlot(slot);
            pfree_ext(func_ctx->user_fctx);
            func_ctx->user_fctx = NULL;
            SRF_RETURN_DONE(func_ctx);
        }
        for (int i = 0; i < SEND_STREAM_TUPLE_NATTS; i++) {
            values[i] = slot_getattr(slot, i + 1, &nulls[i]);
        }

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        (void)ExecClearTuple(slot);
        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    }
    SRF_RETURN_DONE(func_ctx);
#endif
}

/*
* @Description: parallel get comm stream status in entire cluster througn ExecRemoteFunctionInParallel in RemoteFunctionResultHandler.
* @return: one tuple slot of result set.
*/
CommInfoParallel *getGlobalCommStatus(TupleDesc tuple_desc, char *queryString)
{
    StringInfoData buf;
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

    TupleDesc tup_desc = CreateTemplateTupleDesc(3, false);
    TupleDescInitEntry(tup_desc, (AttrNumber)++i, "is_encrypt", BOOLOID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber)++i, "g_tde_algo", TEXTOID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber)++i, "remain", TEXTOID, -1, 0);
    BlessTupleDesc(tup_desc);

    Datum values[3];
    bool nulls[3] = {false};
    HeapTuple tuple = NULL;

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    i = 0;
    const char* algo;
    if (g_tde_algo == TDE_ALGO_AES_CTR_128) {
        algo = "AES-CTR-128";
    } else if (g_tde_algo == TDE_ALGO_SM4_CTR_128) {
        algo = "SM4-CTR-128";
    } else {
        algo = "null";
    }
    const char* remain = "remain";
    values[i++] = BoolGetDatum(isEncryptedCluster());
    values[i++] = CStringGetTextDatum(algo);
    values[i++] = CStringGetTextDatum(remain);
    tuple = heap_form_tuple(tup_desc, values, nulls);

    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

void init_pg_comm_status(PG_FUNCTION_ARGS, FuncCallContext *func_ctx, const int arg_nums)
{
    TupleDesc tup_desc = NULL;
    MemoryContext old_context = NULL;

    /* create a function context for cross-call persistence */
    func_ctx = SRF_FIRSTCALL_INIT();

    /*
     * Switch to memory context appropriate for multiple function calls
     */
    old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

    /* build tup_desc for result tuples */
    tup_desc = CreateTemplateTupleDesc(arg_nums, false);

    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_1, "node_name", TEXTOID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_2, "rxpck_rate", INT4OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_3, "txpck_rate", INT4OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_4, "rxkB_rate", INT8OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_5, "txkB_rate", INT8OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_6, "buffer", INT8OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_7, "memKB_libcomm", INT8OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_8, "memKB_libpq", INT8OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_9, "used_PM", INT4OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_10, "used_sflow", INT4OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_11, "used_rflow", INT4OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_12, "used_rloop", INT4OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_13, "stream", INT4OID, -1, 0);

    func_ctx->tuple_desc = BlessTupleDesc(tup_desc);
    MemoryContextSwitchTo(old_context);
    return;
}

Datum set_pg_comm_status(PG_FUNCTION_ARGS, FuncCallContext *func_ctx, const int arg_nums)
{
    errno_t rc = 0;
    CommStat comm_stat;
    Datum values[arg_nums];
    bool nulls[arg_nums] = {false};
    HeapTuple tuple = NULL;
    Datum result;
    int ii = 0;

    func_ctx = SRF_PERCALL_SETUP();
    rc = memset_s(&comm_stat, sizeof(CommStat), 0, sizeof(CommStat));

    if (gs_get_comm_stat(&comm_stat) == false) {
        SRF_RETURN_DONE(func_ctx);
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
    tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
    result = HeapTupleGetDatum(tuple);
    SRF_RETURN_NEXT(func_ctx, result);
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

void init_pgxc_comm_get_status(PG_FUNCTION_ARGS, FuncCallContext *func_ctx, const int arg_nums)
{
    TupleDesc tup_desc;

    /* build tup_desc for result tuples */
    tup_desc = CreateTemplateTupleDesc(arg_nums, false);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_1, "node_name", TEXTOID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_2, "rxpck_rate", INT4OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_3, "txpck_rate", INT4OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_4, "rxkB_rate", INT8OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_5, "txkB_rate", INT8OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_6, "buffer", INT8OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_7, "memKB_libcomm", INT8OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_8, "memKB_libpq", INT8OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_9, "used_PM", INT4OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_10, "used_sflow", INT4OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_11, "used_rflow", INT4OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_12, "used_rloop", INT4OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber) ARG_13, "stream", INT4OID, -1, 0);
    func_ctx->tuple_desc = BlessTupleDesc(tup_desc);
    func_ctx->max_calls = u_sess->pgxc_cxt.NumDataNodes + u_sess->pgxc_cxt.NumCoords;
    return;
}

Datum global_comm_get_status(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_MULTIPLE_NODES
    FuncCallContext* func_ctx = NULL;
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        errmsg("unsupported view in single node mode.")));

    SRF_RETURN_DONE(func_ctx);
#else
    const int SEND_STREAM_TUPLE_NATTS = 13;
    FuncCallContext *func_ctx = NULL;
    Datum values[SEND_STREAM_TUPLE_NATTS];
    bool nulls[SEND_STREAM_TUPLE_NATTS];
    HeapTuple tuple;
    MemoryContext old_context;

    if (SRF_IS_FIRSTCALL()) {
        /* create a function context for cross-call persistence */
        func_ctx = SRF_FIRSTCALL_INIT();
        /*
         * Switch to memory context appropriate for multiple function calls
         */
        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);
        init_pgxc_comm_get_status(fcinfo, func_ctx, SEND_STREAM_TUPLE_NATTS);
        /* the main call for get table distribution. */
        func_ctx->user_fctx = getGlobalCommStatus(func_ctx->tuple_desc, "SELECT * FROM pg_comm_status;");
        MemoryContextSwitchTo(old_context);
        if (func_ctx->user_fctx == NULL)
            SRF_RETURN_DONE(func_ctx);
    }

    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->user_fctx) {
        Tuplestorestate *tup_store = ((CommInfoParallel *)func_ctx->user_fctx)->state->tupstore;
        TupleTableSlot *slot = ((CommInfoParallel *)func_ctx->user_fctx)->slot;
        if (!tuplestore_gettupleslot(tup_store, true, false, slot)) {
            FreeParallelFunctionState(((CommInfoParallel *)func_ctx->user_fctx)->state);
            ExecDropSingleTupleTableSlot(slot);
            pfree_ext(func_ctx->user_fctx);
            func_ctx->user_fctx = NULL;
            SRF_RETURN_DONE(func_ctx);
        }
        for (int i = 0; i < SEND_STREAM_TUPLE_NATTS; i++) {
            values[i] = slot_getattr(slot, i + 1, &nulls[i]);
        }

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        (void)ExecClearTuple(slot);
        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    }
    SRF_RETURN_DONE(func_ctx);
#endif
}

void init_pg_comm_client_info(PG_FUNCTION_ARGS, FuncCallContext *func_ctx, const int arg_nums)
{
    TupleDesc tup_desc;
    tup_desc = CreateTemplateTupleDesc(arg_nums, false);
    TupleDescInitEntry(tup_desc, (AttrNumber)ARG_1, "node_name", TEXTOID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber)ARG_2, "app", TEXTOID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber)ARG_3, "tid", INT8OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber)ARG_4, "lwtid", INT4OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber)ARG_5, "query_id", INT8OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber)ARG_6, "socket", INT4OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber)ARG_7, "remote_ip", TEXTOID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber)ARG_8, "remote_port", TEXTOID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber)ARG_9, "logic_id", INT4OID, -1, 0);
    func_ctx->tuple_desc = BlessTupleDesc(tup_desc);
    func_ctx->user_fctx = palloc0(sizeof(int));
    if (PG_ARGISNULL(0)) {
        /* Get all backends */
        func_ctx->max_calls = pgstat_fetch_stat_numbackends();
    } else {
        /*
         * Get one backend - locate by tid.
         *
         * We lookup the backend early, so we can return zero rows if it
         * doesn't exist, instead of returning a single row full of NULLs.
         */
        ThreadId tid = PG_GETARG_INT64(0);
        int i;
        int n = pgstat_fetch_stat_numbackends();

        for (i = 1; i <= n; i++) {
            PgBackendStatus *be = pgstat_fetch_stat_beentry(i);
            if (be != NULL) {
                if (be->st_procpid == tid) {
                    *(int *) (func_ctx->user_fctx) = i;
                    break;
                }
            }
        }

        if (*(int *)(func_ctx->user_fctx) == 0) {
            /* Pid not found, return zero rows */
            func_ctx->max_calls = 0;
        } else {
            func_ctx->max_calls = 1;
        }
    }

    return;
}

void set_comm_client_info(Datum *values, bool *nulls, PgBackendStatus *beentry)
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
    FuncCallContext *func_ctx = NULL;
    MemoryContext old_context;
    Datum values[CLIENT_INFO_TUPLE_NATTS];
    bool nulls[CLIENT_INFO_TUPLE_NATTS] = {false};
    PgBackendStatus *beentry = NULL;
    char wait_status[WAITSTATELEN];
    HeapTuple tuple;
    errno_t rc = 0;

    if (SRF_IS_FIRSTCALL()) {
        func_ctx = SRF_FIRSTCALL_INIT();
        Assert(STATE_WAIT_NUM == sizeof(WaitStateDesc) / sizeof(WaitStateDesc[0]));
        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);
        init_pg_comm_client_info(fcinfo, func_ctx, CLIENT_INFO_TUPLE_NATTS);
        MemoryContextSwitchTo(old_context);
    }

    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->call_cntr >= func_ctx->max_calls) {
        SRF_RETURN_DONE(func_ctx);
    }

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(rc, "\0", "\0");
    rc = memset_s(wait_status, sizeof(wait_status), 0, sizeof(wait_status));
    securec_check(rc, "\0", "\0");

    if (*(int *) (func_ctx->user_fctx) > 0) {
        /* Get specific pid slot */
        beentry = pgstat_fetch_stat_beentry(*(int *) (func_ctx->user_fctx));
    } else {
        /* Get the next one in the list */
        beentry = pgstat_fetch_stat_beentry(func_ctx->call_cntr + 1);
    }
    if (beentry == NULL) {
        for (int i = 0; (unsigned int)(i) < sizeof(nulls) / sizeof(nulls[0]); i++) {
            nulls[i] = true;
        }
        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    }

    /* Values available to all callers */
    rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    set_comm_client_info(values, nulls, beentry);

    tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
    SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
}

Datum global_comm_get_client_info(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_MULTIPLE_NODES
    FuncCallContext* func_ctx = NULL;
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        errmsg("unsupported view in single node mode.")));

    SRF_RETURN_DONE(func_ctx);
#else
    const int CLIENT_INFO_TUPLE_NATTS = 9;
    FuncCallContext *func_ctx = NULL;
    Datum values[CLIENT_INFO_TUPLE_NATTS];
    bool nulls[CLIENT_INFO_TUPLE_NATTS];
    HeapTuple tuple;
    int i;

    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tup_desc;
        MemoryContext old_context;

        /* create a function context for cross-call persistence */
        func_ctx = SRF_FIRSTCALL_INIT();

        /* Switch to memory context appropriate for multiple function calls */
        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        /* build tup_desc for result tuples */
        tup_desc = CreateTemplateTupleDesc(CLIENT_INFO_TUPLE_NATTS, false);
        TupleDescInitEntry(tup_desc, (AttrNumber) ARG_1, "node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber) ARG_2, "app", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber) ARG_3, "tid", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber) ARG_4, "lwtid", INT4OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber) ARG_5, "query_id", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber) ARG_6, "socket", INT4OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber) ARG_7, "remote_ip", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber) ARG_8, "remote_port", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber) ARG_9, "logic_id", INT4OID, -1, 0);
        func_ctx->tuple_desc = BlessTupleDesc(tup_desc);

        func_ctx->max_calls = u_sess->pgxc_cxt.NumDataNodes + u_sess->pgxc_cxt.NumCoords;
        /* the main call for get table distribution. */
        func_ctx->user_fctx = getGlobalCommStatus(func_ctx->tuple_desc,  "SELECT * FROM comm_client_info;");
        MemoryContextSwitchTo(old_context);
        if (func_ctx->user_fctx == NULL)
            SRF_RETURN_DONE(func_ctx);
    }

    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->user_fctx) {
        Tuplestorestate *tup_store = ((CommInfoParallel *)func_ctx->user_fctx)->state->tupstore;
        TupleTableSlot *slot = ((CommInfoParallel *)func_ctx->user_fctx)->slot;

        if (!tuplestore_gettupleslot(tup_store, true, false, slot)) {
            FreeParallelFunctionState(((CommInfoParallel *)func_ctx->user_fctx)->state);
            ExecDropSingleTupleTableSlot(slot);
            pfree_ext(func_ctx->user_fctx);
            func_ctx->user_fctx = NULL;
            SRF_RETURN_DONE(func_ctx);
        }
        for (i = 0; i < CLIENT_INFO_TUPLE_NATTS; i++) {
            values[i] = slot_getattr(slot, i + 1, &nulls[i]);
        }
        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        (void)ExecClearTuple(slot);
        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    }
    SRF_RETURN_DONE(func_ctx);
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
    FuncCallContext* func_ctx = NULL;
    CommDelayInfo* delay_info = NULL;
    int ii = 0;
    errno_t rc = 0;

    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tup_desc;
        MemoryContext old_context;

        /* create a function context for cross-call persistence */
        func_ctx = SRF_FIRSTCALL_INIT();

        /*
         * Switch to memory context appropriate for multiple function calls
         */
        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        /* build tup_desc for result tuples */
        ii = 1;
        tup_desc = CreateTemplateTupleDesc(DELAY_INFO_TUPLE_NATTS, false);
        TupleDescInitEntry(tup_desc, (AttrNumber)ii++, "node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)ii++, "remote_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)ii++, "remote_host", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)ii++, "stream_num", INT4OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)ii++, "min_delay", INT4OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)ii++, "average", INT4OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)ii++, "max_delay", INT4OID, -1, 0);

        func_ctx->tuple_desc = BlessTupleDesc(tup_desc);

        delay_info = (CommDelayInfo*)palloc(sizeof(CommDelayInfo));
        rc = memset_s(delay_info, sizeof(CommDelayInfo), 0, sizeof(CommDelayInfo));
        securec_check(rc, "\0", "\0");
        func_ctx->user_fctx = (void*)delay_info;

        MemoryContextSwitchTo(old_context);
    }

    func_ctx = SRF_PERCALL_SETUP();
    delay_info = (CommDelayInfo*)func_ctx->user_fctx;

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

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(func_ctx, result);
    } else {
        SRF_RETURN_DONE(func_ctx);
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
    if ((ng = WLMGetNodeGroupFromHTAB(ngname)) == NULL) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("%s logical cluster doesn't exist!", ngname)));
    }
    char* p = NULL;
    char* q = NULL;

    int sret = snprintf_s(name, GPNAME_LEN, GPNAME_LEN - 1, "%s", gname);
    securec_check_intval(sret, , NULL);

    q = pstrdup(gname);

    p = strchr(q, ':');
    if (p == NULL) { /* Class group */
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
    char* pool = PG_GETARG_CSTRING(0);
    if (pool == NULL || strlen(pool) == 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("invalid name of resource pool: NULL or \"\"")));
        PG_RETURN_BOOL(false);
    }

    int cls = 0;
    int wd = 0;
    char gname[GPNAME_LEN] = {0};
    char cls_name[GPNAME_LEN] = {0};
    char wd_name[GPNAME_LEN] = {0};

    /* get the class and workload id */
    WLMNodeGroupInfo* ng = gs_get_cgroup_id(pool, gname, cls_name, wd_name, &cls, &wd);

    gscgroup_grp_t* grp = NULL;

    /* parent group */
    if (cls && 0 == wd) {
        grp = ng->vaddr[cls];
    } else if (cls && wd) {
        grp = ng->vaddr[wd];
    }

    FuncCallContext* fun_cctx = NULL;

    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tup_desc = NULL;
        MemoryContext old_context = NULL;

        /* create a function context for cross-call persistence */
        fun_cctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function calls
         */
        old_context = MemoryContextSwitchTo(fun_cctx->multi_call_memory_ctx);

        /* build tup_desc for result tuples */
        /* this had better match pv_plan view in system_views.sql */
        tup_desc = CreateTemplateTupleDesc(9, false);
        TupleDescInitEntry(tup_desc, (AttrNumber)1, "name", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)2, "class", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)3, "workload", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)4, "type", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)5, "gid", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)6, "shares", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)7, "limits", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)8, "rate", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)9, "cpucores", TEXTOID, -1, 0);

        fun_cctx->tuple_desc = BlessTupleDesc(tup_desc);
        fun_cctx->max_calls = 1;
        fun_cctx->call_cntr = 0;

        (void)MemoryContextSwitchTo(old_context);
    }

    /* stuff done on every call of the function */
    fun_cctx = SRF_PERCALL_SETUP();
    if (fun_cctx->call_cntr < fun_cctx->max_calls) {
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
        values[1] = CStringGetTextDatum(cls_name);
        values[2] = CStringGetTextDatum(wd_name);
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

        tuple = heap_form_tuple(fun_cctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(fun_cctx, HeapTupleGetDatum(tuple));
    } else {
        /* nothing left */
        SRF_RETURN_DONE(fun_cctx);
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
    if (pool == NULL || strlen(pool) == 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("invalid name of resource pool: NULL or \"\"")));
        PG_RETURN_BOOL(false);
    }

    int cls = 0;
    int wd = 0;
    char gname[GPNAME_LEN] = {0};
    char cls_name[GPNAME_LEN] = {0};
    char wd_name[GPNAME_LEN] = {0};

    static unsigned int excp_value[EXCP_TYPES_CNT];

    /* get the class and workload id */
    WLMNodeGroupInfo* ng = gs_get_cgroup_id(pool, gname, cls_name, wd_name, &cls, &wd);

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

    FuncCallContext* func_ctx = NULL;

    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tup_desc;
        MemoryContext old_context;

        /* create a function context for cross-call persistence */
        func_ctx = SRF_FIRSTCALL_INIT();

        /*
         * Switch to memory context appropriate for multiple function calls
         */
        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        /* build tup_desc for result tuples */
        tup_desc = CreateTemplateTupleDesc(6, false);

        TupleDescInitEntry(tup_desc, (AttrNumber)1, "name", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)2, "class", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)3, "workload", TEXTOID, -1, 0);

        TupleDescInitEntry(tup_desc, (AttrNumber)4, "rule", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)5, "type", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)6, "value", INT8OID, -1, 0);

        func_ctx->tuple_desc = BlessTupleDesc(tup_desc);

        (void)MemoryContextSwitchTo(old_context);

        errno_t rc = memset_s(excp_value, sizeof(excp_value), 0, sizeof(excp_value));
        securec_check(rc, "\0", "\0");

        if (grp != NULL) {
            excp_value[0] = grp->except[EXCEPT_ABORT].blocktime;
            excp_value[1] = grp->except[EXCEPT_ABORT].elapsedtime;
            excp_value[2] = grp->except[EXCEPT_ABORT].allcputime;
            excp_value[3] = grp->except[EXCEPT_ABORT].qualitime;
            excp_value[4] = grp->except[EXCEPT_ABORT].skewpercent;
            excp_value[5] = grp->except[EXCEPT_ABORT].spoolsize;
            excp_value[6] = grp->except[EXCEPT_ABORT].broadcastsize;
            excp_value[7] = grp->except[EXCEPT_PENALTY].blocktime;
            excp_value[8] = grp->except[EXCEPT_PENALTY].elapsedtime;
            excp_value[9] = grp->except[EXCEPT_PENALTY].allcputime;
            excp_value[10] = grp->except[EXCEPT_PENALTY].qualitime;
            excp_value[11] = grp->except[EXCEPT_PENALTY].skewpercent;
            excp_value[12] = grp->except[EXCEPT_PENALTY].spoolsize;
            excp_value[13] = grp->except[EXCEPT_PENALTY].broadcastsize;
        }
    }

    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();

    while (func_ctx->call_cntr < EXCP_TYPES_CNT) {
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
        values[1] = CStringGetTextDatum(cls_name);
        values[2] = CStringGetTextDatum(wd_name);
        values[3] = CStringGetTextDatum(excpname[func_ctx->call_cntr % (EXCP_TYPES_CNT / 2)]);
        if (func_ctx->call_cntr >= (EXCP_TYPES_CNT / 2)) {
            values[4] = CStringGetTextDatum("Penalty");
        } else {
            values[4] = CStringGetTextDatum("Abort");
        }

        values[5] = Int64GetDatum(excp_value[func_ctx->call_cntr]);

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    }

    /* do when there is no more left */
    SRF_RETURN_DONE(func_ctx);
}

/*
 * return all control group info
 */
gscgroup_grp_t* getControlGroupInfo(WLMNodeGroupInfo* ng, uint32* num)
{
    gscgroup_grp_t* local_array = NULL;
    gscgroup_grp_t* local_entry = NULL;
    gscgroup_grp_t* entry = NULL;
    int entry_index;
    uint32 count = 0;

    local_array =
        (gscgroup_grp_t*)palloc0((GSCGROUP_CLASSNUM + GSCGROUP_WDNUM + GSCGROUP_TSNUM) * sizeof(gscgroup_grp_t));
    local_entry = local_array;
    for (entry_index = 0; entry_index < (GSCGROUP_CLASSNUM + GSCGROUP_WDNUM + GSCGROUP_TSNUM); entry_index++) {
        entry = ng->vaddr[CLASSCG_START_ID + entry_index];
        errno_t rc = memcpy_s(local_entry, sizeof(gscgroup_grp_t), entry, sizeof(gscgroup_grp_t));
        securec_check(rc, "\0", "\0");

        if (entry->used == 0 || (entry->gtype == GROUP_DEFWD && 0 == strcmp(entry->grpname, "TopWD:1"))) {
            continue;
        }

        local_entry++;
        count++;
    }

    *num = count;
    return local_array;
}

/*
 * select * from gs_all_control_group_info;
 */
Datum gs_all_control_group_info(PG_FUNCTION_ARGS)
{
    FuncCallContext* func_ctx = NULL;
    gscgroup_grp_t* array = NULL;

    if (g_instance.wlm_cxt->gscgroup_init_done == 0) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("cgroup is not initialized!")));
    }

    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tup_desc;
        MemoryContext old_context;

        /* create a function context for cross-call persistence */
        func_ctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function calls
         */
        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        /* build tup_desc for result tuples */
        /* this had better match pv_plan view in system_views.sql */
        tup_desc = CreateTemplateTupleDesc(10, false);
        TupleDescInitEntry(tup_desc, (AttrNumber)1, "name", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)2, "type", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)3, "gid", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)4, "classgid", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)5, "class", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)6, "workload", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)7, "shares", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)8, "limits", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)9, "wdlevel", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)10, "cpucores", TEXTOID, -1, 0);

        func_ctx->tuple_desc = BlessTupleDesc(tup_desc);
        func_ctx->user_fctx = (void*)getControlGroupInfo(&g_instance.wlm_cxt->MyDefaultNodeGroup, &(func_ctx->max_calls));

        (void)MemoryContextSwitchTo(old_context);
    }

    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();
    array = (gscgroup_grp_t*)func_ctx->user_fctx;

    if (func_ctx->call_cntr < func_ctx->max_calls) {
        /* for each row */
        Datum values[10];
        bool nulls[10] = {false};
        HeapTuple tuple = NULL;
        gscgroup_grp_t* entry = NULL;
        uint32 entry_index;

        /*
         * Form tuple with appropriate data.
         */
        errno_t ss_rc = EOK;
        ss_rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(ss_rc, "\0", "\0");
        ss_rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(ss_rc, "\0", "\0");

        entry_index = func_ctx->call_cntr;

        entry = array + entry_index;

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
            int cls_id = entry->ginfo.wd.cgid;
            char wld_name[GPNAME_LEN];
            char* p = NULL;

            errno_t ss_rc = strcpy_s(wld_name, GPNAME_LEN, entry->grpname);
            securec_check(ss_rc, "\0", "\0");
            /* omit the ':' character in the name */
            if ((p = strchr(wld_name, ':')) != NULL)
                *p = '\0';

            values[3] = Int64GetDatum(cls_id);
            values[4] = CStringGetTextDatum(g_instance.wlm_cxt->gscgroup_vaddr[cls_id]->grpname);
            values[5] = CStringGetTextDatum(wld_name);
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

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    } else {
        SRF_RETURN_DONE(func_ctx);
    }
}

/*
 * select * from gs_all_nodegroup_control_group_info('nodegroup');
 */
Datum gs_all_nodegroup_control_group_info(PG_FUNCTION_ARGS)
{
#define NODEGROUP_CONTROL_GROUP_INFO_ATTRNUM 10
    char* ngname = PG_GETARG_CSTRING(0);
    if (ngname == NULL || strlen(ngname) == 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("invalid name of logical cluster: NULL or \"\"")));
    }

    if (g_instance.wlm_cxt->gscgroup_init_done == 0) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("cgroup is not initialized!")));
    }

    /* get the class and workload id */
    WLMNodeGroupInfo* ng = WLMGetNodeGroupFromHTAB(ngname);
    if (ng == NULL) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("%s logical cluster doesn't exist!", ngname)));
    }
    FuncCallContext* funcctx = NULL;
    gscgroup_grp_t* array = NULL;
    int i = 0;

    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tup_desc;
        MemoryContext old_context;
        i = 0;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function calls
         */
        old_context = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tup_desc for result tuples */
        /* this had better match pv_plan view in system_views.sql */
        tup_desc = CreateTemplateTupleDesc(NODEGROUP_CONTROL_GROUP_INFO_ATTRNUM, false);
        TupleDescInitEntry(tup_desc, (AttrNumber)++i, "name", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)++i, "type", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)++i, "gid", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)++i, "classgid", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)++i, "class", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)++i, "workload", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)++i, "shares", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)++i, "limits", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)++i, "wdlevel", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)++i, "cpucores", TEXTOID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tup_desc);
        funcctx->user_fctx = (void*)getControlGroupInfo(ng, &(funcctx->max_calls));

        (void)MemoryContextSwitchTo(old_context);
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
        uint32 entry_index;
        int i = -1;

        /*
         * Form tuple with appropriate data.
         */
        errno_t ss_rc = EOK;
        ss_rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(ss_rc, "\0", "\0");
        ss_rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(ss_rc, "\0", "\0");

        entry_index = funcctx->call_cntr;
        entry = array + entry_index;

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
            int cls_id = entry->ginfo.wd.cgid;
            char wld_name[GPNAME_LEN];
            char* p = NULL;

            errno_t ss_rc = strcpy_s(wld_name, GPNAME_LEN, entry->grpname);
            securec_check(ss_rc, "\0", "\0");
            /* omit the ':' character in the name */
            if ((p = strchr(wld_name, ':')) != NULL) {
                *p = '\0';
            }
            values[++i] = Int64GetDatum(cls_id);
            values[++i] = CStringGetTextDatum(ng->vaddr[cls_id]->grpname);
            values[++i] = CStringGetTextDatum(wld_name);
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
    FuncCallContext* func_ctx = NULL;
    MemoryContext old_context = NULL;

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
        func_ctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function
         * calls
         */
        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        /* need a tuple descriptor representing 3 columns */
        tupdesc = CreateTemplateTupleDesc(NUM_COMPUTE_POOL_WORKLOAD_ELEM, false);

        TupleDescInitEntry(tupdesc, (AttrNumber)1, "nodename", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "rpinuse", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "maxrp", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)4, "nodestate", TEXTOID, -1, 0);

        /* complete descriptor of the tupledesc */
        func_ctx->tuple_desc = BlessTupleDesc(tupdesc);

        /* total number of tuples to be returned */
        if (IS_PGXC_COORDINATOR && g_instance.attr.attr_sql.max_resource_package) {
            func_ctx->user_fctx = get_cluster_state();

            if (func_ctx->user_fctx)
                func_ctx->max_calls = u_sess->pgxc_cxt.NumDataNodes;
        }

        (void)MemoryContextSwitchTo(old_context);
    }

    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->call_cntr < func_ctx->max_calls) {
        /* do when there is more left to send */
        Datum values[NUM_COMPUTE_POOL_WORKLOAD_ELEM];
        bool nulls[NUM_COMPUTE_POOL_WORKLOAD_ELEM] = {false};
        HeapTuple tuple = NULL;

        errno_t ss_rc = EOK;
        ss_rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(ss_rc, "\0", "\0");
        ss_rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(ss_rc, "\0", "\0");

        char* data_node_Name = NULL;
        data_node_Name = getNodenameByIndex(func_ctx->call_cntr);

        int dn_num = 0;
        DNState* dn_state = NULL;

        ComputePoolState* cps = (ComputePoolState*)func_ctx->user_fctx;
        if (cps != NULL) {
            Assert(cps->dn_num);

            dn_num = cps->dn_num;
            dn_state = cps->dn_state;
        }

        int dn = func_ctx->call_cntr;

        if (dn < dn_num) {
            int num_rp = dn_state[dn].num_rp;
            if (num_rp > g_instance.attr.attr_sql.max_resource_package)
                num_rp = g_instance.attr.attr_sql.max_resource_package;

            values[0] = CStringGetTextDatum(data_node_Name);
            values[1] = Int32GetDatum(num_rp);
            values[2] = Int32GetDatum(g_instance.attr.attr_sql.max_resource_package);
            values[3] = CStringGetTextDatum(dn_state[dn].is_normal ? "normal" : "abnormal");
        } else {
            values[0] = CStringGetTextDatum(data_node_Name);
            values[1] = Int32GetDatum(0);
            values[2] = Int32GetDatum(g_instance.attr.attr_sql.max_resource_package);
            values[3] = CStringGetTextDatum("normal");
        }

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);

        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    } else {
        /* do when there is no more left */
        SRF_RETURN_DONE(func_ctx);
    }
}

/* on which coordinator for a special table do autovac  */
Datum pg_autovac_coordinator(PG_FUNCTION_ARGS)
{
    Oid rel_id = PG_GETARG_OID(0);
    HeapTuple tuple = NULL;
    int coor_idx;
    Datum result;

    if (IS_SINGLE_NODE) {
        PG_RETURN_DATUM(CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName));
    }

    if (!IS_PGXC_COORDINATOR)
        PG_RETURN_NULL();

    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(rel_id));
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
    if (coor_idx >= 0 && coor_idx < *t_thrd.pgxc_cxt.shmemNumCoords) {
        result = CStringGetTextDatum(NameStr(t_thrd.pgxc_cxt.coDefs[coor_idx].nodename));
        LWLockRelease(NodeTableLock);

        PG_RETURN_DATUM(result);
    }

    LWLockRelease(NodeTableLock);

    PG_RETURN_NULL();
}

static int64 pgxc_exec_partition_tuples_stat(HeapTuple class_tuple, HeapTuple part_tuple, char* func_name)
{
    ExecNodes* exec_nodes = NULL;
    ParallelFunctionState* state = NULL;
    Form_pg_partition part_form = NULL;
    Form_pg_class class_form = NULL;
    char* ns_pname = NULL;
    char* rel_name = NULL;
    char* part_name = NULL;
    bool replicated = false;
    StringInfoData buf;
    RelationLocInfo* rel_loc_info = NULL;
    int64 result = 0;

    exec_nodes = (ExecNodes*)makeNode(ExecNodes);
    exec_nodes->accesstype = RELATION_ACCESS_READ;
    exec_nodes->primarynodelist = NIL;
    exec_nodes->nodeList = NIL;
    exec_nodes->en_expr = NULL;
    exec_nodes->en_relid = HeapTupleGetOid(class_tuple);

    part_form = (Form_pg_partition)GETSTRUCT(part_tuple);
    class_form = (Form_pg_class)GETSTRUCT(class_tuple);
    Assert(HeapTupleGetOid(class_tuple) == part_form->parentid);

    if (class_form->relpersistence == RELPERSISTENCE_TEMP) {
        return 0;
    }
    rel_loc_info = GetRelationLocInfo(part_form->parentid);
    replicated = (rel_loc_info && IsRelationReplicated(rel_loc_info));

    ns_pname = repairObjectName(get_namespace_name(class_form->relnamespace, true));
    ;
    rel_name = repairObjectName(NameStr(class_form->relname));
    part_name = repairObjectName(NameStr(part_form->relname));

    initStringInfo(&buf);
    appendStringInfo(&buf,
        "SELECT pg_catalog.%s(p.oid) FROM pg_class c "
        "INNER JOIN pg_namespace n ON n.oid = c.relnamespace "
        "INNER JOIN pg_partition p ON p.parentid = c.oid "
        "WHERE n.nspname = '%s' AND c.relname = '%s' AND p.relname = '%s'",
        func_name, ns_pname, rel_name, part_name);
    state = RemoteFunctionResultHandler(buf.data, exec_nodes, NULL, true, EXEC_ON_DATANODES, true);
    compute_tuples_stat(state, replicated);
    result = state->result;

    pfree_ext(buf.data);
    pfree_ext(ns_pname);
    pfree_ext(rel_name);
    pfree_ext(part_name);
    FreeRelationLocInfo(rel_loc_info);
    FreeParallelFunctionState(state);

    return result;
}

Datum pg_stat_get_partition_tuples_changed(PG_FUNCTION_ARGS)
{
    Oid part_oid = PG_GETARG_OID(0);
    Oid rel_id = InvalidOid;
    HeapTuple part_tuple = NULL;
    HeapTuple class_tuple = NULL;
    Form_pg_partition part_form = NULL;
    PgStat_StatTabKey tab_key;
    PgStat_StatTabEntry* tab_entry = NULL;
    int64 result = 0;

    part_tuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(part_oid));
    if (!HeapTupleIsValid(part_tuple)) {
        PG_RETURN_INT64(result);
    }
    part_form = (Form_pg_partition)GETSTRUCT(part_tuple);
    rel_id = part_form->parentid;
    class_tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(rel_id));
    if (!HeapTupleIsValid(class_tuple)) {
        ReleaseSysCache(part_tuple);
        PG_RETURN_INT64(result);
    }

    if (IS_PGXC_COORDINATOR && GetRelationLocInfo((rel_id))) {
        result = pgxc_exec_partition_tuples_stat(class_tuple, part_tuple, "pg_stat_get_partition_tuples_changed");
        ReleaseSysCache(class_tuple);
        ReleaseSysCache(part_tuple);

        PG_RETURN_INT64(result);
    }

    tab_key.tableid = part_oid;
    tab_key.statFlag = rel_id;
    tab_entry = pgstat_fetch_stat_tabentry(&tab_key);
    if (tab_entry != NULL) {
        result = (int64)(tab_entry->changes_since_analyze);
    }
    ReleaseSysCache(class_tuple);
    ReleaseSysCache(part_tuple);
    PG_RETURN_INT64(result);
}

Datum pg_stat_get_partition_tuples_inserted(PG_FUNCTION_ARGS)
{
    Oid part_oid = PG_GETARG_OID(0);
    Oid rel_id = InvalidOid;
    HeapTuple part_tuple = NULL;
    HeapTuple class_tuple = NULL;
    Form_pg_partition part_form = NULL;
    PgStat_StatTabKey tab_key;
    PgStat_StatTabEntry* tab_entry = NULL;
    int64 result = 0;

    part_tuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(part_oid));
    if (!HeapTupleIsValid(part_tuple)) {
        PG_RETURN_INT64(result);
    }
    part_form = (Form_pg_partition)GETSTRUCT(part_tuple);
    rel_id = part_form->parentid;
    class_tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(rel_id));
    if (!HeapTupleIsValid(class_tuple)) {
        ReleaseSysCache(part_tuple);
        PG_RETURN_INT64(result);
    }

    if (IS_PGXC_COORDINATOR && GetRelationLocInfo((rel_id))) {
        result = pgxc_exec_partition_tuples_stat(class_tuple, part_tuple, "pg_stat_get_partition_tuples_inserted");
        ReleaseSysCache(class_tuple);
        ReleaseSysCache(part_tuple);
        PG_RETURN_INT64(result);
    }

    tab_key.tableid = part_oid;
    tab_key.statFlag = rel_id;
    tab_entry = pgstat_fetch_stat_tabentry(&tab_key);
    if (tab_entry != NULL) {
        result = (int64)(tab_entry->tuples_inserted);
    }
    ReleaseSysCache(class_tuple);
    ReleaseSysCache(part_tuple);
    PG_RETURN_INT64(result);
}

Datum pg_stat_get_partition_tuples_updated(PG_FUNCTION_ARGS)
{
    Oid part_oid = PG_GETARG_OID(0);
    Oid rel_id = InvalidOid;
    HeapTuple part_tuple = NULL;
    HeapTuple class_tuple = NULL;
    Form_pg_partition part_form = NULL;
    PgStat_StatTabKey tab_key;
    PgStat_StatTabEntry* tab_entry = NULL;
    int64 result = 0;

    part_tuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(part_oid));
    if (!HeapTupleIsValid(part_tuple)) {
        PG_RETURN_INT64(result);
    }
    part_form = (Form_pg_partition)GETSTRUCT(part_tuple);
    rel_id = part_form->parentid;
    class_tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(rel_id));
    if (!HeapTupleIsValid(class_tuple)) {
        ReleaseSysCache(part_tuple);
        PG_RETURN_INT64(result);
    }

    if (IS_PGXC_COORDINATOR && GetRelationLocInfo((rel_id))) {
        result = pgxc_exec_partition_tuples_stat(class_tuple, part_tuple, "pg_stat_get_partition_tuples_updated");
        ReleaseSysCache(class_tuple);
        ReleaseSysCache(part_tuple);

        PG_RETURN_INT64(result);
    }

    tab_key.tableid = part_oid;
    tab_key.statFlag = rel_id;
    tab_entry = pgstat_fetch_stat_tabentry(&tab_key);
    if (tab_entry != NULL) {
        result = (int64)(tab_entry->tuples_updated);
    }
    ReleaseSysCache(class_tuple);
    ReleaseSysCache(part_tuple);

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_partition_tuples_deleted(PG_FUNCTION_ARGS)
{
    Oid part_oid = PG_GETARG_OID(0);
    Oid rel_id = InvalidOid;
    HeapTuple part_tuple = NULL;
    HeapTuple class_tuple = NULL;
    Form_pg_partition part_form = NULL;
    PgStat_StatTabKey tab_key;
    PgStat_StatTabEntry* tab_entry = NULL;
    int64 result = 0;

    part_tuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(part_oid));
    if (!HeapTupleIsValid(part_tuple)) {
        PG_RETURN_INT64(result);
    }
    part_form = (Form_pg_partition)GETSTRUCT(part_tuple);
    rel_id = part_form->parentid;
    class_tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(rel_id));
    if (!HeapTupleIsValid(class_tuple)) {
        ReleaseSysCache(part_tuple);
        PG_RETURN_INT64(result);
    }

    if (IS_PGXC_COORDINATOR && GetRelationLocInfo((rel_id))) {
        result = pgxc_exec_partition_tuples_stat(class_tuple, part_tuple, "pg_stat_get_partition_tuples_deleted");
        ReleaseSysCache(class_tuple);
        ReleaseSysCache(part_tuple);
        PG_RETURN_INT64(result);
    }

    tab_key.tableid = part_oid;
    tab_key.statFlag = rel_id;
    tab_entry = pgstat_fetch_stat_tabentry(&tab_key);
    if (tab_entry != NULL) {
        result = (int64)(tab_entry->tuples_deleted);
    }
    ReleaseSysCache(class_tuple);
    ReleaseSysCache(part_tuple);
    PG_RETURN_INT64(result);
}

Datum pg_stat_get_partition_tuples_hot_updated(PG_FUNCTION_ARGS)
{
    Oid part_oid = PG_GETARG_OID(0);
    Oid relid = InvalidOid;
    HeapTuple part_tuple = NULL;
    HeapTuple class_tuple = NULL;
    Form_pg_partition part_form = NULL;
    PgStat_StatTabKey tab_key;
    PgStat_StatTabEntry* tab_entry = NULL;
    int64 result = 0;

    part_tuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(part_oid));
    if (!HeapTupleIsValid(part_tuple)) {
        PG_RETURN_INT64(result);
    }
    part_form = (Form_pg_partition)GETSTRUCT(part_tuple);
    relid = part_form->parentid;
    class_tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(class_tuple)) {
        ReleaseSysCache(part_tuple);
        PG_RETURN_INT64(result);
    }

    if (IS_PGXC_COORDINATOR && GetRelationLocInfo((relid))) {
        result = pgxc_exec_partition_tuples_stat(class_tuple, part_tuple, "pg_stat_get_partition_tuples_hot_updated");
        ReleaseSysCache(class_tuple);
        ReleaseSysCache(part_tuple);

        PG_RETURN_INT64(result);
    }

    tab_key.tableid = part_oid;
    tab_key.statFlag = relid;
    tab_entry = pgstat_fetch_stat_tabentry(&tab_key);
    if (tab_entry != NULL) {
        result = (int64)(tab_entry->tuples_hot_updated);
    }
    ReleaseSysCache(class_tuple);
    ReleaseSysCache(part_tuple);
    PG_RETURN_INT64(result);
}

Datum pg_stat_get_partition_dead_tuples(PG_FUNCTION_ARGS)
{
    Oid part_oid = PG_GETARG_OID(0);
    Oid rel_id = InvalidOid;
    HeapTuple part_tuple = NULL;
    HeapTuple class_tuple = NULL;
    Form_pg_partition part_form = NULL;
    PgStat_StatTabKey tab_key;
    PgStat_StatTabEntry* tab_entry = NULL;
    int64 result = 0;

    part_tuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(part_oid));
    if (!HeapTupleIsValid(part_tuple)) {
        PG_RETURN_INT64(result);
    }
    part_form = (Form_pg_partition)GETSTRUCT(part_tuple);
    rel_id = part_form->parentid;
    class_tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(rel_id));
    if (!HeapTupleIsValid(class_tuple)) {
        ReleaseSysCache(part_tuple);
        PG_RETURN_INT64(result);
    }

    if (IS_PGXC_COORDINATOR && GetRelationLocInfo((rel_id))) {
        result = pgxc_exec_partition_tuples_stat(class_tuple, part_tuple, "pg_stat_get_partition_dead_tuples");
        ReleaseSysCache(class_tuple);
        ReleaseSysCache(part_tuple);

        PG_RETURN_INT64(result);
    }

    tab_key.tableid = part_oid;
    tab_key.statFlag = rel_id;
    tab_entry = pgstat_fetch_stat_tabentry(&tab_key);
    if (tab_entry != NULL) {
        result = (int64)(tab_entry->n_dead_tuples);
    }
    ReleaseSysCache(class_tuple);
    ReleaseSysCache(part_tuple);
    PG_RETURN_INT64(result);
}

Datum pg_stat_get_partition_live_tuples(PG_FUNCTION_ARGS)
{
    Oid part_oid = PG_GETARG_OID(0);
    Oid rel_id = InvalidOid;
    HeapTuple part_tuple = NULL;
    HeapTuple class_tuple = NULL;
    Form_pg_partition part_form = NULL;
    PgStat_StatTabKey tab_key;
    PgStat_StatTabEntry* tab_entry = NULL;
    int64 result = 0;

    part_tuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(part_oid));
    if (!HeapTupleIsValid(part_tuple)) {
        PG_RETURN_INT64(result);
    }
    part_form = (Form_pg_partition)GETSTRUCT(part_tuple);
    rel_id = part_form->parentid;
    class_tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(rel_id));
    if (!HeapTupleIsValid(class_tuple)) {
        ReleaseSysCache(part_tuple);
        PG_RETURN_INT64(result);
    }

    if (IS_PGXC_COORDINATOR && GetRelationLocInfo((rel_id))) {
        result = pgxc_exec_partition_tuples_stat(class_tuple, part_tuple, "pg_stat_get_partition_live_tuples");
        ReleaseSysCache(class_tuple);
        ReleaseSysCache(part_tuple);

        PG_RETURN_INT64(result);
    }

    tab_key.tableid = part_oid;
    tab_key.statFlag = rel_id;
    tab_entry = pgstat_fetch_stat_tabentry(&tab_key);
    if (tab_entry != NULL) {
        result = (int64)(tab_entry->n_live_tuples);
    }
    ReleaseSysCache(class_tuple);
    ReleaseSysCache(part_tuple);
    PG_RETURN_INT64(result);
}

Datum pg_stat_get_xact_partition_tuples_inserted(PG_FUNCTION_ARGS)
{
    Oid part_oid = PG_GETARG_OID(0);
    Oid rel_id = InvalidOid;
    HeapTuple part_tuple = NULL;
    HeapTuple class_tuple = NULL;
    Form_pg_partition part_form = NULL;
    PgStat_TableStatus* tab_entry = NULL;
    PgStat_TableXactStatus* trans = NULL;
    int64 result = 0;

    part_tuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(part_oid));
    if (!HeapTupleIsValid(part_tuple)) {
        PG_RETURN_INT64(result);
    }
    part_form = (Form_pg_partition)GETSTRUCT(part_tuple);
    rel_id = part_form->parentid;
    class_tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(rel_id));
    if (!HeapTupleIsValid(class_tuple)) {
        ReleaseSysCache(part_tuple);
        PG_RETURN_INT64(result);
    }

    if (IS_PGXC_COORDINATOR && GetRelationLocInfo((rel_id))) {
        result = pgxc_exec_partition_tuples_stat(class_tuple, part_tuple, "pg_stat_get_xact_partition_tuples_inserted");
        ReleaseSysCache(class_tuple);
        ReleaseSysCache(part_tuple);

        PG_RETURN_INT64(result);
    }

    tab_entry = find_tabstat_entry(part_oid, rel_id);
    if (tab_entry != NULL) {
        result += tab_entry->t_counts.t_tuples_inserted;

        /* live subtransactions' counts aren't in t_tuples_inserted yet */
        for (trans = tab_entry->trans; trans != NULL; trans = trans->upper)
            result += trans->tuples_inserted;
    }

    ReleaseSysCache(class_tuple);
    ReleaseSysCache(part_tuple);

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_xact_partition_tuples_updated(PG_FUNCTION_ARGS)
{
    Oid part_oid = PG_GETARG_OID(0);
    Oid rel_id = InvalidOid;
    HeapTuple part_tuple = NULL;
    HeapTuple class_tuple = NULL;
    Form_pg_partition part_form = NULL;
    PgStat_TableStatus* tab_entry = NULL;
    PgStat_TableXactStatus* trans = NULL;
    int64 result = 0;

    part_tuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(part_oid));
    if (!HeapTupleIsValid(part_tuple)) {
        PG_RETURN_INT64(result);
    }
    part_form = (Form_pg_partition)GETSTRUCT(part_tuple);
    rel_id = part_form->parentid;
    class_tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(rel_id));
    if (!HeapTupleIsValid(class_tuple)) {
        ReleaseSysCache(part_tuple);
        PG_RETURN_INT64(result);
    }

    if (IS_PGXC_COORDINATOR && GetRelationLocInfo((rel_id))) {
        result = pgxc_exec_partition_tuples_stat(class_tuple, part_tuple, "pg_stat_get_xact_partition_tuples_updated");
        ReleaseSysCache(class_tuple);
        ReleaseSysCache(part_tuple);

        PG_RETURN_INT64(result);
    }

    tab_entry = find_tabstat_entry(part_oid, rel_id);
    if (PointerIsValid(tab_entry)) {
        result += tab_entry->t_counts.t_tuples_updated;

        /* live subtransactions' counts aren't in t_tuples_updated yet */
        for (trans = tab_entry->trans; trans != NULL; trans = trans->upper) {
            result += trans->tuples_updated;
        }
    }

    ReleaseSysCache(class_tuple);
    ReleaseSysCache(part_tuple);

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_xact_partition_tuples_deleted(PG_FUNCTION_ARGS)
{
    Oid part_oid = PG_GETARG_OID(0);
    Oid rel_id = InvalidOid;
    HeapTuple part_tuple = NULL;
    HeapTuple class_tuple = NULL;
    Form_pg_partition part_form = NULL;
    int64 result = 0;
    PgStat_TableStatus* tab_entry = NULL;
    PgStat_TableXactStatus* trans = NULL;

    part_tuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(part_oid));
    if (!HeapTupleIsValid(part_tuple)) {
        PG_RETURN_INT64(result);
    }
    part_form = (Form_pg_partition)GETSTRUCT(part_tuple);
    rel_id = part_form->parentid;
    class_tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(rel_id));
    if (!HeapTupleIsValid(class_tuple)) {
        ReleaseSysCache(part_tuple);
        PG_RETURN_INT64(result);
    }

    if (IS_PGXC_COORDINATOR && GetRelationLocInfo((rel_id))) {
        result = pgxc_exec_partition_tuples_stat(class_tuple, part_tuple, "pg_stat_get_xact_partition_tuples_deleted");
        ReleaseSysCache(class_tuple);
        ReleaseSysCache(part_tuple);

        PG_RETURN_INT64(result);
    }

    tab_entry = find_tabstat_entry(part_oid, rel_id);
    if (PointerIsValid(tab_entry)) {
        result += tab_entry->t_counts.t_tuples_deleted;

        /* live subtransactions' counts aren't in t_tuples_updated yet */
        for (trans = tab_entry->trans; trans != NULL; trans = trans->upper)
            result += trans->tuples_deleted;
    }

    ReleaseSysCache(class_tuple);
    ReleaseSysCache(part_tuple);

    PG_RETURN_INT64(result);
}

Datum pg_stat_get_xact_partition_tuples_hot_updated(PG_FUNCTION_ARGS)
{
    Oid part_oid = PG_GETARG_OID(0);
    Oid rel_id = InvalidOid;
    HeapTuple part_tuple = NULL;
    HeapTuple class_tuple = NULL;
    Form_pg_partition part_form = NULL;
    PgStat_TableStatus* tab_entry = NULL;
    int64 result = 0;

    part_tuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(part_oid));
    if (!HeapTupleIsValid(part_tuple)) {
        PG_RETURN_INT64(result);
    }

    part_form = (Form_pg_partition)GETSTRUCT(part_tuple);
    rel_id = part_form->parentid;
    class_tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(rel_id));
    if (!HeapTupleIsValid(class_tuple)) {
        ReleaseSysCache(part_tuple);
        PG_RETURN_INT64(result);
    }

    if (IS_PGXC_COORDINATOR && GetRelationLocInfo((rel_id))) {
        result =
            pgxc_exec_partition_tuples_stat(class_tuple, part_tuple, "pg_stat_get_xact_partition_tuples_hot_updated");
        ReleaseSysCache(class_tuple);
        ReleaseSysCache(part_tuple);

        PG_RETURN_INT64(result);
    }

    tab_entry = find_tabstat_entry(part_oid, rel_id);
    if (PointerIsValid(tab_entry))
        result += tab_entry->t_counts.t_tuples_hot_updated;

    ReleaseSysCache(class_tuple);
    ReleaseSysCache(part_tuple);

    PG_RETURN_INT64(result);
}

static void strategy_func_timeout(ParallelFunctionState* state)
{
    TupleTableSlot* slot = NULL;
    Datum datum = 0;
    int64 result = 0;

    Assert(state && state->tupstore);
    if (state->tupdesc == NULL) {
        /* if there is only one coordiantor,  tupdesc will be null */
        state->result = 0;
    }
    slot = MakeSingleTupleTableSlot(state->tupdesc);

    for (;;) {
        bool isnull = false;

        if (!tuplestore_gettupleslot(state->tupstore, true, false, slot))
            break;

        datum = slot_getattr(slot, 1, &isnull);
        if (!isnull)
            result += DatumGetInt64(datum);

        (void)ExecClearTuple(slot);
    }

    state->result = result;
}

static int64 pgxc_exec_autoanalyze_timeout(Oid relOid, int32 coor_idx, char* funcname)
{
    Relation rel = NULL;
    char* rel_name = NULL;
    char* ns_pname = NULL;
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

    rel_name = repairObjectName(RelationGetRelationName(rel));
    ns_pname = repairObjectName(get_namespace_name(rel->rd_rel->relnamespace, true));
    relation_close(rel, AccessShareLock);

    initStringInfo(&buf);
    appendStringInfo(&buf, "SELECT pg_catalog.%s('%s.%s'::regclass)", funcname, ns_pname, rel_name);
    state = RemoteFunctionResultHandler(buf.data, exec_nodes, strategy_func_timeout, true, EXEC_ON_COORDS, true);
    result = state->result;

    FreeParallelFunctionState(state);
    pfree_ext(rel_name);
    pfree_ext(ns_pname);
    pfree_ext(buf.data);

    return result;
}

Datum pg_autovac_timeout(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    HeapTuple tuple = NULL;
    Form_pg_class class_form = NULL;
    int coor_idx;
    int64 result = 0;

    if (!IS_PGXC_COORDINATOR)
        PG_RETURN_NULL();

    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(tuple)) {
        /* if it has been droppped or does not exists, return null */
        PG_RETURN_NULL();
    }

    class_form = (Form_pg_class)GETSTRUCT(tuple);
    if (class_form->relpersistence != RELPERSISTENCE_PERMANENT || class_form->relkind != RELKIND_RELATION) {
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
        if (tabentry != NULL) {
            result = CONTINUED_TIMEOUT_COUNT(tabentry->autovac_status);
        }
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
    ReturnSetInfo* rs_info = (ReturnSetInfo*)fcinfo->resultinfo;
    TupleDesc tup_desc = NULL;
    Tuplestorestate* tup_store = NULL;
    MemoryContext per_query_ctx;
    MemoryContext old_context;

    /* check to see if caller supports us returning a tuplestore */
    if ((rs_info == NULL) || !IsA(rs_info, ReturnSetInfo)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("set-valued function called in context that cannot accept a set")));
    }
    if (!(rs_info->allowedModes & SFRM_Materialize)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("materialize mode required, but it is not allowed in this context")));
    }

#define AUTOVAC_TUPLES_ATTR_NUM 7

    per_query_ctx = rs_info->econtext->ecxt_per_query_memory;
    old_context = MemoryContextSwitchTo(per_query_ctx);
    tup_desc = CreateTemplateTupleDesc(AUTOVAC_TUPLES_ATTR_NUM, false);
    TupleDescInitEntry(tup_desc, (AttrNumber)1, "nodename", NAMEOID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber)2, "nspname", NAMEOID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber)3, "relname", NAMEOID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber)4, "partname", NAMEOID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber)5, "n_dead_tuples", INT8OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber)6, "n_live_tuples", INT8OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber)7, "changes_since_analyze", INT8OID, -1, 0);
    tup_store =
        tuplestore_begin_heap(rs_info->allowedModes & SFRM_Materialize_Random, false, u_sess->attr.attr_memory.work_mem);
    MemoryContextSwitchTo(old_context);

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

        old_context = MemoryContextSwitchTo(per_query_ctx);
        state = RemoteFunctionResultHandler(buf.data, exec_nodes, NULL, true, EXEC_ON_DATANODES, true);
        tup_store = state->tupstore;
        MemoryContextSwitchTo(old_context);
        pfree_ext(buf.data);
    } else {
        PgStat_StatDBEntry* db_entry = NULL;
        NameData data_name;

        /*
         * We put all the tuples into a tuplestore in one scan of the hashtable.
         * This avoids any issue of the hashtable possibly changing between calls.
         */
        (void)namestrcpy(&data_name, g_instance.attr.attr_common.PGXCNodeName);
        db_entry = pgstat_fetch_stat_dbentry(u_sess->proc_cxt.MyDatabaseId);
        if ((db_entry != NULL) && (db_entry->tables != NULL)) {
            HASH_SEQ_STATUS hash_seq;
            PgStat_StatTabEntry* tabentry = NULL;

            hash_seq_init(&hash_seq, db_entry->tables);
            while ((tabentry = (PgStat_StatTabEntry*)hash_seq_search(&hash_seq)) != NULL) {
                Oid rel_id = tabentry->tablekey.tableid;
                Oid part_id = InvalidOid;
                HeapTuple nsp_tuple = NULL;
                HeapTuple rel_tuple = NULL;
                HeapTuple part_tuple = NULL;
                Form_pg_namespace nsp_form = NULL;
                Form_pg_class rel_form = NULL;
                Form_pg_partition part_form = NULL;
                Datum values[AUTOVAC_TUPLES_ATTR_NUM];
                bool nulls[AUTOVAC_TUPLES_ATTR_NUM] = {false};
                bool enable_analyze = false;
                bool enable_vacuum = false;
                bool is_internal_relation = false;
                /*
                 * skip if changes_since_analyze/n_dead_tuples is zero
                 * skip if it is a system catalog
                 */
                if ((tabentry->n_dead_tuples <= 0 && tabentry->changes_since_analyze <= 0) ||
                    tabentry->tablekey.tableid < FirstNormalObjectId) {
                    continue;
                }

                if (tabentry->tablekey.statFlag) {
                    if (for_relation) { /* just fetch relation's stat info */
                        continue;
                    }

                    part_id = tabentry->tablekey.tableid;
                    rel_id = tabentry->tablekey.statFlag;
                }

                rel_tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(rel_id));
                if (!HeapTupleIsValid(rel_tuple)) {
                    continue;
                }

                relation_support_autoavac(rel_tuple, &enable_analyze, &enable_vacuum, &is_internal_relation);
                if (!enable_analyze && !enable_vacuum) {
                    ReleaseSysCache(rel_tuple);
                    continue;
                }
                rel_form = (Form_pg_class)GETSTRUCT(rel_tuple);

                nsp_tuple = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(rel_form->relnamespace));
                if (!HeapTupleIsValid(nsp_tuple)) {
                    ReleaseSysCache(rel_tuple);
                    continue;
                }
                nsp_form = (Form_pg_namespace)GETSTRUCT(nsp_tuple);

                if (OidIsValid(part_id)) {
                    part_tuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(part_id));
                    if (!HeapTupleIsValid(part_tuple)) {
                        ReleaseSysCache(nsp_tuple);
                        ReleaseSysCache(rel_tuple);
                        continue;
                    }
                    part_form = (Form_pg_partition)GETSTRUCT(part_tuple);
                }

                errno_t rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
                securec_check(rc, "\0", "\0");
                rc = memset_s(values, sizeof(values), 0, sizeof(values));
                securec_check(rc, "\0", "\0");

                values[0] = NameGetDatum(&data_name);
                values[1] = NameGetDatum(&nsp_form->nspname);
                values[2] = NameGetDatum(&rel_form->relname);
                if (HeapTupleIsValid(part_tuple)) {
                    values[3] = NameGetDatum(&part_form->relname);
                    values[4] = Int64GetDatum(tabentry->n_dead_tuples);
                    values[5] = Int64GetDatum(tabentry->n_live_tuples);
                    values[6] = Int64GetDatum(tabentry->changes_since_analyze);
                } else if (rel_form->parttype == PARTTYPE_PARTITIONED_RELATION) {
                    nulls[3] = true;
                    values[4] = DirectFunctionCall1(pg_stat_get_dead_tuples, ObjectIdGetDatum(rel_id));
                    values[5] = DirectFunctionCall1(pg_stat_get_live_tuples, ObjectIdGetDatum(rel_id));
                    values[6] = Int64GetDatum(tabentry->changes_since_analyze);
                } else {
                    nulls[3] = true;
                    values[4] = Int64GetDatum(tabentry->n_dead_tuples);
                    values[5] = Int64GetDatum(tabentry->n_live_tuples);
                    values[6] = Int64GetDatum(tabentry->changes_since_analyze);
                }

                tuplestore_putvalues(tup_store, tup_desc, values, nulls);

                if (part_tuple)
                    ReleaseSysCache(part_tuple);
                ReleaseSysCache(nsp_tuple);
                ReleaseSysCache(rel_tuple);
            }
        }
    }
    DEBUG_MOD_STOP_TIMER(MOD_AUTOVAC, "EXECUTE pg_total_autovac_tuples");

    /* clean up and return the tuplestore */
    tuplestore_donestoring(tup_store);
    rs_info->returnMode = SFRM_Materialize;
    rs_info->setResult = tup_store;
    rs_info->setDesc = tup_desc;

    return (Datum)0;
}

Datum pg_autovac_status(PG_FUNCTION_ARGS)
{
    Oid rel_id = PG_GETARG_OID(0);
    TupleDesc tup_desc;
    HeapTuple class_tuple;
    Form_pg_class class_form;
    AutoVacOpts* relopts = NULL;
    bool force_vacuum = false;
    bool av_enabled = false;
    bool do_vacuum = false;
    bool do_analyze = false;
    bool is_internal_relation = false;
    char* nsp_name = NULL;
    char* rel_name = NULL;
    int coor_idx = -1;
    bool isNull = false;
    TransactionId rel_frozenx_id = InvalidTransactionId;
    Relation rel = NULL;
    Datum xid_64_datum = 0;

    /* If the node is DN in mpp deployment mode */
    if (IS_PGXC_DATANODE && IS_SINGLE_NODE == false) {
        PG_RETURN_NULL();
    }

#define STATUS_ATTR_NUM 9

    Datum values[STATUS_ATTR_NUM];
    bool nulls[STATUS_ATTR_NUM] = {false};

    /* Initialise values and NULL flags arrays */
    errno_t rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    tup_desc = CreateTemplateTupleDesc(STATUS_ATTR_NUM, false);
    TupleDescInitEntry(tup_desc, (AttrNumber)1, "nspname", TEXTOID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber)2, "relname", TEXTOID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber)3, "nodename", TEXTOID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber)4, "doanalyze", BOOLOID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber)5, "anltuples", INT8OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber)6, "anlthresh", INT8OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber)7, "dovacuum", BOOLOID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber)8, "vactuples", INT8OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber)9, "vacthresh", INT8OID, -1, 0);
    BlessTupleDesc(tup_desc);

    class_tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(rel_id));
    if (!HeapTupleIsValid(class_tuple)) {
        PG_RETURN_NULL();
    }

    class_form = (Form_pg_class)GETSTRUCT(class_tuple);
    nsp_name = get_namespace_name(class_form->relnamespace);
    rel_name = NameStr(class_form->relname);
    if ((rel_name == NULL) || (nsp_name == NULL) || (class_form->relkind != RELKIND_RELATION)) {
        ReleaseSysCache(class_tuple);
        PG_RETURN_NULL();
    }

    relation_support_autoavac(class_tuple, &do_analyze, &do_vacuum, &is_internal_relation);

    /* constants from reloptions or GUC variables */
    int64 vac_base_thresh;
    int64 anl_base_thresh;
    float4 vac_scale_factor;
    float4 anl_scale_factor;

    /* thresholds calculated from above constants */
    int64 vac_thresh;
    int64 anl_thresh;

    /* number of vacuum (resp. analyze) tuples at this time */
    int64 vac_tuples;
    int64 anl_tuples;

    /* freeze parameters */
    int freeze_max_age;
    TransactionId xid_force_limit;

    relopts = extract_autovac_opts(class_tuple, GetDefaultPgClassDesc());

    /*
     * Determine vacuum/analyze equation parameters.  We have two possible
     * sources: the passed reloptions (which could be a main table or a toast
     * table), or the autovacuum GUC variables.
     *
     * -1 in autovac setting means use plain vacuum_cost_delay
     */
    vac_scale_factor = (relopts && relopts->vacuum_scale_factor >= 0) ? relopts->vacuum_scale_factor :
                       u_sess->attr.attr_storage.autovacuum_vac_scale;

    vac_base_thresh = (relopts && relopts->vacuum_threshold >= 0) ? relopts->vacuum_threshold :
                      u_sess->attr.attr_storage.autovacuum_vac_thresh;

    anl_scale_factor = (relopts && relopts->analyze_scale_factor >= 0) ? relopts->analyze_scale_factor :
                       u_sess->attr.attr_storage.autovacuum_anl_scale;

    anl_base_thresh = (relopts && relopts->analyze_threshold >= 0) ? relopts->analyze_threshold :
                      u_sess->attr.attr_storage.autovacuum_anl_thresh;

    freeze_max_age = (relopts && relopts->freeze_max_age >= 0) ?
                     Min(relopts->freeze_max_age, g_instance.attr.attr_storage.autovacuum_freeze_max_age) :
                     g_instance.attr.attr_storage.autovacuum_freeze_max_age;

    av_enabled = (relopts ? relopts->enabled : true);

    /* Force vacuum if table need freeze the old tuple for clog recycle */
    if (t_thrd.autovacuum_cxt.recentXid > FirstNormalTransactionId + freeze_max_age) {
        xid_force_limit = t_thrd.autovacuum_cxt.recentXid - freeze_max_age;
    } else {
        xid_force_limit = FirstNormalTransactionId;
    }
    rel = heap_open(RelationRelationId, AccessShareLock);
    xid_64_datum = heap_getattr(class_tuple, Anum_pg_class_relfrozenxid64, RelationGetDescr(rel), &isNull);
    heap_close(rel, AccessShareLock);
    ReleaseSysCache(class_tuple);

    if (isNull) {
        rel_frozenx_id = class_form->relfrozenxid;
        if (TransactionIdPrecedes(t_thrd.xact_cxt.ShmemVariableCache->nextXid, rel_frozenx_id) ||
            !TransactionIdIsNormal(rel_frozenx_id)) {
            rel_frozenx_id = FirstNormalTransactionId;
        }
    } else {
        rel_frozenx_id = DatumGetTransactionId(xid_64_datum);
    }

    force_vacuum = (TransactionIdIsNormal(rel_frozenx_id) && TransactionIdPrecedes(rel_frozenx_id, xid_force_limit));

    vac_thresh = (int64)(vac_base_thresh + vac_scale_factor * class_form->reltuples);
    anl_thresh = (int64)(anl_base_thresh + anl_scale_factor * class_form->reltuples);
    anl_tuples = DirectFunctionCall1(pg_stat_get_tuples_changed, ObjectIdGetDatum(rel_id));
    vac_tuples = DirectFunctionCall1(pg_stat_get_dead_tuples, ObjectIdGetDatum(rel_id));

    /* Determine if this table needs analyze */
    if (do_analyze) {
        do_analyze = (anl_tuples > anl_thresh);
    }

    /* Determine if this table needs vacuum */
    if (force_vacuum) {
        do_vacuum = force_vacuum;
    } else if (do_vacuum) {
        do_vacuum = (vac_tuples > vac_thresh);
    }

    coor_idx = PgxcGetCentralNodeIndex();
    LWLockAcquire(NodeTableLock, LW_SHARED);
    if (coor_idx >= 0 && coor_idx < *t_thrd.pgxc_cxt.shmemNumCoords && IS_SINGLE_NODE == false) {
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

    values[0] = CStringGetTextDatum(nsp_name);
    values[1] = CStringGetTextDatum(rel_name);
    values[3] = BoolGetDatum(do_analyze);
    values[4] = Int64GetDatum(anl_tuples);
    values[5] = Int64GetDatum(anl_thresh);
    values[6] = BoolGetDatum(do_vacuum);
    values[7] = Int64GetDatum(vac_tuples);
    values[8] = Int64GetDatum(vac_thresh);

    /* Returns the record as Datum */
    PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tup_desc, values, nulls)));
}

void check_dirty_percent_and_tuples(int dirty_pecent, int n_tuples)
{
    if (dirty_pecent > 100 || dirty_pecent < 0 || n_tuples < 0) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), (errmsg("out of range."))));
    }
}

void dirty_table_init_tupdesc(TupleDesc tup_desc)
{
    tup_desc = CreateTemplateTupleDesc(7, false);
    TupleDescInitEntry(tup_desc, (AttrNumber)1, "relname", NAMEOID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber)2, "schemaname", NAMEOID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber)3, "n_tup_ins", INT8OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber)4, "n_tup_upd", INT8OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber)5, "n_tup_del", INT8OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber)6, "n_live_tup", INT8OID, -1, 0);
    TupleDescInitEntry(tup_desc, (AttrNumber)7, "n_dead_tup", INT8OID, -1, 0);
}

void dirty_table_fill_values(Datum* values, TupleTableSlot* slot, bool* nulls)
{
    values[0] = slot_getattr(slot, 1, &nulls[0]);
    values[1] = slot_getattr(slot, 2, &nulls[1]);
    values[2] = slot_getattr(slot, 3, &nulls[2]);
    values[3] = slot_getattr(slot, 4, &nulls[3]);
    values[4] = slot_getattr(slot, 5, &nulls[4]);
    values[5] = slot_getattr(slot, 6, &nulls[5]);
    values[6] = slot_getattr(slot, 7, &nulls[6]);
}

/*
 * @Description: get all table's distribution in all datanode.
 * @return : return the distribution of all the table in current database.
 */
Datum pgxc_stat_all_dirty_tables(PG_FUNCTION_ARGS)
{
    FuncCallContext* func_ctx = NULL;
    Datum values[7];
    bool nulls[7] = {false, false, false, false};
    HeapTuple tuple = NULL;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext old_context = NULL;
        TupleDesc tup_desc = NULL;
        int dirty_pecent = PG_GETARG_INT32(0);
        int tuples = PG_GETARG_INT32(1);
        /* check range */
        check_dirty_percent_and_tuples(dirty_pecent, tuples);
        func_ctx = SRF_FIRSTCALL_INIT();

        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        /* build tup_desc for result tuples. */
        dirty_table_init_tupdesc(tup_desc);
        func_ctx->tuple_desc = BlessTupleDesc(tup_desc);
        func_ctx->max_calls = u_sess->pgxc_cxt.NumDataNodes;

        /* the main call for get pgxc_stat_all_tables2 */
        func_ctx->user_fctx = getTableStat(func_ctx->tuple_desc, dirty_pecent, tuples, NULL);
        MemoryContextSwitchTo(old_context);

        if (func_ctx->user_fctx == NULL) {
            SRF_RETURN_DONE(func_ctx);
        }
    }

    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->user_fctx) {
        Tuplestorestate* tup_store = ((TableDistributionInfo*)func_ctx->user_fctx)->state->tupstore;
        TupleTableSlot* slot = ((TableDistributionInfo*)func_ctx->user_fctx)->slot;
        if (!tuplestore_gettupleslot(tup_store, true, false, slot)) {
            FreeParallelFunctionState(((TableDistributionInfo*)func_ctx->user_fctx)->state);
            ExecDropSingleTupleTableSlot(slot);
            pfree_ext(func_ctx->user_fctx);
            func_ctx->user_fctx = NULL;
            SRF_RETURN_DONE(func_ctx);
        }
        dirty_table_fill_values(values, slot, nulls);
        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        (void)ExecClearTuple(slot);

        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    }

    SRF_RETURN_DONE(func_ctx);
}

Datum pgxc_stat_schema_dirty_tables(PG_FUNCTION_ARGS)
{
    FuncCallContext* func_ctx = NULL;
    Datum values[7];
    bool nulls[7] = {false, false, false, false};
    HeapTuple tuple = NULL;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext old_context = NULL;
        TupleDesc tup_desc = NULL;
        int dirty_pecent = PG_GETARG_INT32(0);
        int tuples = PG_GETARG_INT32(1);

        /* check range */
        check_dirty_percent_and_tuples(dirty_pecent, tuples);

        text* rel_namespace_t = PG_GETARG_TEXT_PP(2);
        char* rel_namespace = text_to_cstring(rel_namespace_t);

        func_ctx = SRF_FIRSTCALL_INIT();

        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        /* build tup_desc for result tuples. */
        dirty_table_init_tupdesc(tup_desc);
        func_ctx->tuple_desc = BlessTupleDesc(tup_desc);
        func_ctx->max_calls = u_sess->pgxc_cxt.NumDataNodes;

        /* the main call for get pgxc_stat_all_tables2 */
        func_ctx->user_fctx = getTableStat(func_ctx->tuple_desc, dirty_pecent, tuples, rel_namespace);
        PG_FREE_IF_COPY(rel_namespace_t, 2);
        pfree(rel_namespace);
        MemoryContextSwitchTo(old_context);

        if (func_ctx->user_fctx == NULL) {
            SRF_RETURN_DONE(func_ctx);
        }
    }

    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->user_fctx) {
        Tuplestorestate* tupstore = ((TableDistributionInfo*)func_ctx->user_fctx)->state->tupstore;
        TupleTableSlot* slot = ((TableDistributionInfo*)func_ctx->user_fctx)->slot;
        if (!tuplestore_gettupleslot(tupstore, true, false, slot)) {
            FreeParallelFunctionState(((TableDistributionInfo*)func_ctx->user_fctx)->state);
            ExecDropSingleTupleTableSlot(slot);
            pfree_ext(func_ctx->user_fctx);
            func_ctx->user_fctx = NULL;
            SRF_RETURN_DONE(func_ctx);
        }
        dirty_table_fill_values(values, slot, nulls);
        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        (void)ExecClearTuple(slot);
        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    }

    SRF_RETURN_DONE(func_ctx);
}

/*
 * @Description: get all table's distribution in all datanode.
 * @return : return the distribution of all the table in current database.
 */
Datum all_table_distribution(PG_FUNCTION_ARGS)
{
    FuncCallContext* func_ctx = NULL;
#ifndef ENABLE_MULTIPLE_NODES
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported view in single node mode.")));

    SRF_RETURN_DONE(func_ctx);
#else
    Datum values[4];
    bool nulls[4] = {false, false, false, false};

    /* only system admin can view the global distribution information. */
    if (!superuser()) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("must be system admin to view the global information"))));
    }

    if (SRF_IS_FIRSTCALL()) {
        func_ctx = SRF_FIRSTCALL_INIT();

        MemoryContext oldcontext = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples. */
        TupleDesc tupdesc = CreateTemplateTupleDesc(4, false);
        TupleDescInitEntry(tupdesc, (AttrNumber)1, "schemaname", NAMEOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "tablename", NAMEOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "nodename", NAMEOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)4, "dnsize", INT8OID, -1, 0);
        func_ctx->tuple_desc = BlessTupleDesc(tupdesc);
        func_ctx->max_calls = u_sess->pgxc_cxt.NumDataNodes;

        /* the main call for get table distribution. */
        func_ctx->user_fctx = getTableDataDistribution(func_ctx->tuple_desc);

        MemoryContextSwitchTo(oldcontext);

        if (func_ctx->user_fctx == NULL) {
            SRF_RETURN_DONE(func_ctx);
        }
    }

    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->user_fctx) {
        Tuplestorestate* tupstore = ((TableDistributionInfo*)func_ctx->user_fctx)->state->tupstore;
        TupleTableSlot* slot = ((TableDistributionInfo*)func_ctx->user_fctx)->slot;

        if (!tuplestore_gettupleslot(tupstore, true, false, slot)) {
            FreeParallelFunctionState(((TableDistributionInfo*)func_ctx->user_fctx)->state);
            ExecDropSingleTupleTableSlot(slot);
            pfree_ext(func_ctx->user_fctx);
            func_ctx->user_fctx = NULL;
            SRF_RETURN_DONE(func_ctx);
        }
        values[0] = slot_getattr(slot, 1, &nulls[0]);
        values[1] = slot_getattr(slot, 2, &nulls[1]);
        values[2] = slot_getattr(slot, 3, &nulls[2]);
        values[3] = slot_getattr(slot, 4, &nulls[3]);
        HeapTuple tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        (void)ExecClearTuple(slot);

        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    }

    SRF_RETURN_DONE(func_ctx);
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
    FuncCallContext* func_ctx = NULL;
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        errmsg("unsupported view in single node mode.")));

    SRF_RETURN_DONE(func_ctx);
#else
    FuncCallContext* func_ctx = NULL;
    Datum values[4];
    bool nulls[4] = {false, false, false, false};
    HeapTuple tuple = NULL;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext old_context = NULL;
        TupleDesc tup_desc = NULL;
        text* rel_namespace_t = PG_GETARG_TEXT_PP(0);
        text* rel_name_t = PG_GETARG_TEXT_PP(1);

        char* rel_namespace = text_to_cstring(rel_namespace_t);
        char* rel_name = text_to_cstring(rel_name_t);

        /* Check whether the user have SELECT privilege of the table. */
        if (!superuser()) {
            Oid table_oid;
            Oid namespace_oid;
            AclResult acl_result;

            namespace_oid = get_namespace_oid(rel_namespace, false);
            table_oid = get_relname_relid(rel_name, namespace_oid);
            if (!OidIsValid(table_oid))
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_TABLE),
                        errmsg("relation \"%s.%s\" does not exist", rel_namespace, rel_name)));

            acl_result = pg_class_aclcheck(table_oid, GetUserId(), ACL_SELECT);
            if (acl_result != ACLCHECK_OK)
                ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("permission denied."))));
        }

        func_ctx = SRF_FIRSTCALL_INIT();

        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        /* build tup_desc for result tuples. */
        tup_desc = CreateTemplateTupleDesc(4, false);
        TupleDescInitEntry(tup_desc, (AttrNumber)1, "schemaname", NAMEOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)2, "tablename", NAMEOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)3, "nodename", NAMEOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)4, "dnsize", INT8OID, -1, 0);
        func_ctx->tuple_desc = BlessTupleDesc(tup_desc);
        func_ctx->max_calls = u_sess->pgxc_cxt.NumDataNodes;

        /* the main call for get table distribution. */
        func_ctx->user_fctx = getTableDataDistribution(func_ctx->tuple_desc, rel_namespace, rel_name);
        PG_FREE_IF_COPY(rel_namespace_t, 0);
        PG_FREE_IF_COPY(rel_name_t, 1);
        pfree_ext(rel_namespace);
        pfree_ext(rel_name);

        MemoryContextSwitchTo(old_context);

        if (func_ctx->user_fctx == NULL) {
            SRF_RETURN_DONE(func_ctx);
        }
    }

    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->user_fctx) {
        Tuplestorestate* tupstore = ((TableDistributionInfo*)func_ctx->user_fctx)->state->tupstore;
        TupleTableSlot* slot = ((TableDistributionInfo*)func_ctx->user_fctx)->slot;
        if (!tuplestore_gettupleslot(tupstore, true, false, slot)) {
            FreeParallelFunctionState(((TableDistributionInfo*)func_ctx->user_fctx)->state);
            ExecDropSingleTupleTableSlot(slot);
            pfree_ext(func_ctx->user_fctx);
            func_ctx->user_fctx = NULL;
            SRF_RETURN_DONE(func_ctx);
        }
        values[0] = slot_getattr(slot, 1, &nulls[0]);
        values[1] = slot_getattr(slot, 2, &nulls[1]);
        values[2] = slot_getattr(slot, 3, &nulls[2]);
        values[3] = slot_getattr(slot, 4, &nulls[3]);
        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        (void)ExecClearTuple(slot);

        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    }

    SRF_RETURN_DONE(func_ctx);
#endif
}

Datum pg_stat_bad_block(PG_FUNCTION_ARGS)
{
#define BAD_BLOCK_STAT_NATTS 9
    FuncCallContext* func_ctx = NULL;
    HASH_SEQ_STATUS* hash_seq = NULL;

    LWLockAcquire(BadBlockStatHashLock, LW_SHARED);

    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tup_desc = NULL;
        MemoryContext old_context = NULL;

        /* create a function context for cross-call persistence */
        func_ctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function calls
         */
        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        /* build tup_desc for result tuples */
        tup_desc = CreateTemplateTupleDesc(BAD_BLOCK_STAT_NATTS, false);

        TupleDescInitEntry(tup_desc, (AttrNumber)1, "nodename", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)2, "databaseid", INT4OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)3, "tablespaceid", INT4OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)4, "relfilenode", INT4OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)5, "bucketid", INT2OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)6, "forknum", INT4OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)7, "error_count", INT4OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)8, "first_time", TIMESTAMPTZOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)9, "last_time", TIMESTAMPTZOID, -1, 0);

        func_ctx->tuple_desc = BlessTupleDesc(tup_desc);

        if (global_bad_block_stat) {
            hash_seq = (HASH_SEQ_STATUS*)palloc0(sizeof(HASH_SEQ_STATUS));
            hash_seq_init(hash_seq, global_bad_block_stat);
        }

        /* NULL if global_bad_block_stat == NULL */
        func_ctx->user_fctx = (void*)hash_seq;

        (void)MemoryContextSwitchTo(old_context);
    }

    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->user_fctx != NULL) {
        hash_seq = (HASH_SEQ_STATUS*)func_ctx->user_fctx;
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
            tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
            LWLockRelease(BadBlockStatHashLock);
            SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
        } else {
            LWLockRelease(BadBlockStatHashLock);
            SRF_RETURN_DONE(func_ctx);
        }
    } else {
        LWLockRelease(BadBlockStatHashLock);
        SRF_RETURN_DONE(func_ctx);
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

typedef enum FuncName { PAGEWRITER_FUNC, INCRE_CKPT_FUNC, INCRE_BGWRITER_FUNC} FuncName;

HeapTuple form_function_tuple(int col_num, FuncName name)
{
    TupleDesc tup_desc = NULL;
    HeapTuple tuple = NULL;
    Datum values[col_num];
    bool nulls[col_num] = {false};
    errno_t rc;
    int i;

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), 1, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    tup_desc = CreateTemplateTupleDesc(col_num, false);

    switch (name) {
        case PAGEWRITER_FUNC:
            for (i = 0; i < col_num; i++) {
                TupleDescInitEntry(tup_desc,
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
                    tup_desc, (AttrNumber)(i + 1), g_ckpt_view_col[i].name, g_ckpt_view_col[i].data_type, -1, 0);
                values[i] = g_ckpt_view_col[i].get_val();
                nulls[i] = false;
            }
            break;
        case INCRE_BGWRITER_FUNC:
            for (i = 0; i < col_num; i++) {
                TupleDescInitEntry(
                    tup_desc, (AttrNumber)(i + 1), g_bgwriter_view_col[i].name, g_bgwriter_view_col[i].data_type, -1, 0);
                values[i] = g_bgwriter_view_col[i].get_val();
                nulls[i] = false;
            }
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("unknow func name")));
            break;
    }

    tup_desc = BlessTupleDesc(tup_desc);
    tuple = heap_form_tuple(tup_desc, values, nulls);
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

void xc_stat_view(FuncCallContext* funcctx, int col_num, FuncName name)
{
    MemoryContext old_context = NULL;
    TupleDesc tup_desc = NULL;
    int i;

    old_context = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

    /* build tup_desc for result tuples. */
    tup_desc = CreateTemplateTupleDesc(col_num, false);
    switch (name) {
        case PAGEWRITER_FUNC:
            for (i = 0; i < col_num; i++) {
                TupleDescInitEntry(tup_desc,
                    (AttrNumber)(i + 1),
                    g_pagewriter_view_col[i].name,
                    g_pagewriter_view_col[i].data_type,
                    -1,
                    0);
            }
            break;
        case INCRE_CKPT_FUNC:
            for (i = 0; i < col_num; i++) {
                TupleDescInitEntry(
                    tup_desc, (AttrNumber)(i + 1), g_ckpt_view_col[i].name, g_ckpt_view_col[i].data_type, -1, 0);
            }
            break;
        case INCRE_BGWRITER_FUNC:
            for (i = 0; i < col_num; i++) {
                TupleDescInitEntry(
                    tup_desc, (AttrNumber)(i + 1), g_bgwriter_view_col[i].name, g_bgwriter_view_col[i].data_type, -1, 0);
            }
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("unknow func name")));
            break;
    }

    funcctx->tuple_desc = BlessTupleDesc(tup_desc);
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
        default:
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("unknow func name")));
            break;
    }

    MemoryContextSwitchTo(old_context);
    return;
}

Datum remote_pagewriter_stat(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_MULTIPLE_NODES
    FuncCallContext* func_ctx = NULL;
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        errmsg("unsupported view in single node mode.")));

    SRF_RETURN_DONE(func_ctx);
#else
    FuncCallContext* func_ctx = NULL;
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
        func_ctx = SRF_FIRSTCALL_INIT();
        xc_stat_view(func_ctx, PAGEWRITER_VIEW_COL_NUM, PAGEWRITER_FUNC);

        if (func_ctx->user_fctx == NULL) {
            SRF_RETURN_DONE(func_ctx);
        }
    }

    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->user_fctx) {
        Tuplestorestate* tupstore = ((TableDistributionInfo*)func_ctx->user_fctx)->state->tupstore;
        TupleTableSlot* slot = ((TableDistributionInfo*)func_ctx->user_fctx)->slot;

        if (!tuplestore_gettupleslot(tupstore, true, false, slot)) {
            FreeParallelFunctionState(((TableDistributionInfo*)func_ctx->user_fctx)->state);
            ExecDropSingleTupleTableSlot(slot);
            pfree_ext(func_ctx->user_fctx);
            func_ctx->user_fctx = NULL;
            SRF_RETURN_DONE(func_ctx);
        }
        for (i = 0; i < PAGEWRITER_VIEW_COL_NUM; i++) {
            values[i] = slot_getattr(slot, (i + 1), &nulls[i]);
        }
        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        (void)ExecClearTuple(slot);
        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    }

    SRF_RETURN_DONE(func_ctx);
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
    FuncCallContext* func_ctx = NULL;
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
        func_ctx = SRF_FIRSTCALL_INIT();
        xc_stat_view(func_ctx, INCRE_CKPT_VIEW_COL_NUM, INCRE_CKPT_FUNC);
        if (func_ctx->user_fctx == NULL) {
            SRF_RETURN_DONE(func_ctx);
        }
    }
    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->user_fctx) {
        Tuplestorestate* tupstore = ((TableDistributionInfo*)func_ctx->user_fctx)->state->tupstore;
        TupleTableSlot* slot = ((TableDistributionInfo*)func_ctx->user_fctx)->slot;
        if (!tuplestore_gettupleslot(tupstore, true, false, slot)) {
            FreeParallelFunctionState(((TableDistributionInfo*)func_ctx->user_fctx)->state);
            ExecDropSingleTupleTableSlot(slot);
            pfree_ext(func_ctx->user_fctx);
            func_ctx->user_fctx = NULL;
            SRF_RETURN_DONE(func_ctx);
        }
        for (i = 0; i < INCRE_CKPT_VIEW_COL_NUM; i++) {
            values[i] = slot_getattr(slot, (i + 1), &nulls[i]);
        }
        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        (void)ExecClearTuple(slot);
        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    }

    SRF_RETURN_DONE(func_ctx);
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
            values[i] = slot_getattr(slot, (i + 1), &nulls[i]);
        }
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        (void)ExecClearTuple(slot);

        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }

    SRF_RETURN_DONE(funcctx);
#endif
}

Datum local_double_write_stat(PG_FUNCTION_ARGS)
{
    TupleDesc tup_desc = NULL;
    HeapTuple tuple = NULL;
    Datum values[DW_VIEW_COL_NUM];
    bool nulls[DW_VIEW_COL_NUM] = {false};
    uint32 i;

    tup_desc = CreateTemplateTupleDesc(DW_VIEW_COL_NUM, false);
    for (i = 0; i < DW_VIEW_COL_NUM; i++) {
        TupleDescInitEntry(
            tup_desc, (AttrNumber)(i + 1), g_dw_view_col_arr[i].name, g_dw_view_col_arr[i].data_type, -1, 0);
        values[i] = g_dw_view_col_arr[i].get_data();
        nulls[i] = false;
    }

    tup_desc = BlessTupleDesc(tup_desc);
    tuple = heap_form_tuple(tup_desc, values, nulls);
    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

Datum remote_double_write_stat(PG_FUNCTION_ARGS)
{
    FuncCallContext* func_ctx = NULL;
#ifndef ENABLE_MULTIPLE_NODES
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported view in single node mode.")));

    SRF_RETURN_DONE(func_ctx);
#else
    Datum values[DW_VIEW_COL_NUM];
    bool nulls[DW_VIEW_COL_NUM] = {false};

    if (SRF_IS_FIRSTCALL()) {
        func_ctx = SRF_FIRSTCALL_INIT();

        MemoryContext oldcontext = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples. */
        TupleDesc tupdesc = CreateTemplateTupleDesc(DW_VIEW_COL_NUM, false);
        for (uint32 i = 0; i < DW_VIEW_COL_NUM; i++) {
            TupleDescInitEntry(
                tupdesc, (AttrNumber)(i + 1), g_dw_view_col_arr[i].name, g_dw_view_col_arr[i].data_type, -1, 0);
            nulls[i] = false;
        }

        func_ctx->tuple_desc = BlessTupleDesc(tupdesc);
        func_ctx->max_calls = u_sess->pgxc_cxt.NumDataNodes + u_sess->pgxc_cxt.NumCoords;

        func_ctx->user_fctx = get_remote_stat_double_write(func_ctx->tuple_desc);
        MemoryContextSwitchTo(oldcontext);

        if (func_ctx->user_fctx == NULL) {
            SRF_RETURN_DONE(func_ctx);
        }
    }

    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->user_fctx) {
        Tuplestorestate* tupstore = ((TableDistributionInfo*)func_ctx->user_fctx)->state->tupstore;
        TupleTableSlot* slot = ((TableDistributionInfo*)func_ctx->user_fctx)->slot;

        if (!tuplestore_gettupleslot(tupstore, true, false, slot)) {
            FreeParallelFunctionState(((TableDistributionInfo*)func_ctx->user_fctx)->state);
            ExecDropSingleTupleTableSlot(slot);
            pfree_ext(func_ctx->user_fctx);
            func_ctx->user_fctx = NULL;
            SRF_RETURN_DONE(func_ctx);
        }
        for (uint32 i = 0; i < DW_VIEW_COL_NUM; i++) {
            values[i] = slot_getattr(slot, (i + 1), &nulls[i]);
        }
        HeapTuple tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        (void)ExecClearTuple(slot);

        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    }

    SRF_RETURN_DONE(func_ctx);
#endif
}

Datum local_redo_stat(PG_FUNCTION_ARGS)
{
    TupleDesc tup_desc = NULL;
    HeapTuple tuple = NULL;
    Datum values[REDO_VIEW_COL_SIZE];
    bool nulls[REDO_VIEW_COL_SIZE] = {false};
    uint32 i;

    redo_fill_redo_event();
    tup_desc = CreateTemplateTupleDesc(REDO_VIEW_COL_SIZE, false);
    for (i = 0; i < REDO_VIEW_COL_SIZE; i++) {
        TupleDescInitEntry(tup_desc, (AttrNumber)(i + 1), g_redoViewArr[i].name, g_redoViewArr[i].data_type, -1, 0);
        values[i] = g_redoViewArr[i].get_data();
        nulls[i] = false;
    }

    tup_desc = BlessTupleDesc(tup_desc);
    tuple = heap_form_tuple(tup_desc, values, nulls);
    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

Datum remote_redo_stat(PG_FUNCTION_ARGS)
{
    FuncCallContext* func_ctx = NULL;
#ifndef ENABLE_MULTIPLE_NODES
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported view in single node mode.")));

    SRF_RETURN_DONE(func_ctx);
#else
    Datum values[REDO_VIEW_COL_SIZE];
    bool nulls[REDO_VIEW_COL_SIZE] = {false};

    if (SRF_IS_FIRSTCALL()) {
        func_ctx = SRF_FIRSTCALL_INIT();

        MemoryContext oldcontext = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples. */
        TupleDesc tupdesc = CreateTemplateTupleDesc(REDO_VIEW_COL_SIZE, false);
        for (uint32 i = 0; i < REDO_VIEW_COL_SIZE; i++) {
            TupleDescInitEntry(tupdesc, (AttrNumber)(i + 1), g_redoViewArr[i].name, g_redoViewArr[i].data_type, -1, 0);
            nulls[i] = false;
        }

        func_ctx->tuple_desc = BlessTupleDesc(tupdesc);
        func_ctx->max_calls = u_sess->pgxc_cxt.NumDataNodes + u_sess->pgxc_cxt.NumCoords;

        /* the main call for get local_redo_stat */
        func_ctx->user_fctx = get_remote_stat_redo(func_ctx->tuple_desc);
        MemoryContextSwitchTo(oldcontext);

        if (func_ctx->user_fctx == NULL) {
            SRF_RETURN_DONE(func_ctx);
        }
    }

    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->user_fctx) {
        Tuplestorestate* tupstore = ((TableDistributionInfo*)func_ctx->user_fctx)->state->tupstore;
        TupleTableSlot* slot = ((TableDistributionInfo*)func_ctx->user_fctx)->slot;
        if (!tuplestore_gettupleslot(tupstore, true, false, slot)) {
            FreeParallelFunctionState(((TableDistributionInfo*)func_ctx->user_fctx)->state);
            ExecDropSingleTupleTableSlot(slot);
            pfree_ext(func_ctx->user_fctx);
            func_ctx->user_fctx = NULL;
            SRF_RETURN_DONE(func_ctx);
        }
        for (uint32 i = 0; i < REDO_VIEW_COL_SIZE; i++) {
            values[i] = slot_getattr(slot, (i + 1), &nulls[i]);
        }
        HeapTuple tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        (void)ExecClearTuple(slot);

        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    }
    SRF_RETURN_DONE(func_ctx);
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
    FuncCallContext* func_ctx = NULL;
    ThreadPoolStat* entry = NULL;
    MemoryContext old_context = NULL;

    /* stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tup_desc = NULL;

        /* create a function context for cross-call persistence */
        func_ctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function
         * calls
         */
        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        /* need a tuple descriptor representing 10 columns */
        tup_desc = CreateTemplateTupleDesc(NUM_THREADPOOL_STATUS_ELEM, false);

        TupleDescInitEntry(tup_desc, (AttrNumber)1, "nodename", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)2, "groupid", INT4OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)3, "bindnumanum", INT4OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)4, "bindcpunum", INT4OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)5, "listenernum", INT4OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)6, "workerinfo", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)7, "sessioninfo", TEXTOID, -1, 0);

        /* complete descriptor of the tupledesc */
        func_ctx->tuple_desc = BlessTupleDesc(tup_desc);

        /* total number of tuples to be returned */
        if (ENABLE_THREAD_POOL) {
            func_ctx->user_fctx = (void*)g_threadPoolControler->GetThreadPoolStat(&(func_ctx->max_calls));
        } else {
            func_ctx->max_calls = 0;
        }

        (void)MemoryContextSwitchTo(old_context);
    }

    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();
    entry = (ThreadPoolStat*)func_ctx->user_fctx;

    if (func_ctx->call_cntr < func_ctx->max_calls) {
        /* do when there is more left to send */
        Datum values[NUM_THREADPOOL_STATUS_ELEM];
        bool nulls[NUM_THREADPOOL_STATUS_ELEM] = {false};
        HeapTuple tuple = NULL;

        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        entry += func_ctx->call_cntr;

        values[0] = CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);
        values[1] = Int32GetDatum(entry->groupId);
        values[2] = Int32GetDatum(entry->numaId);
        values[3] = Int32GetDatum(entry->bindCpuNum);
        values[4] = Int32GetDatum(entry->listenerNum);
        values[5] = CStringGetTextDatum(entry->workerInfo);
        values[6] = CStringGetTextDatum(entry->sessionInfo);

        if (entry->numaId == -1) {
            nulls[2] = true;
        }

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    } else {
        /* do when there is no more left */
        SRF_RETURN_DONE(func_ctx);
    }
}

Datum gs_globalplancache_status(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_MULTIPLE_NODES
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
#endif

    FuncCallContext *func_ctx = NULL;
    MemoryContext old_context;

    /* stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tup_desc;

        /* create a function context for cross-call persistence */
        func_ctx = SRF_FIRSTCALL_INIT();

        /*
        * switch to memory context appropriate for multiple function
        * calls
        */
        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

#define GPC_TUPLES_ATTR_NUM 7

        /* need a tuple descriptor representing 12 columns */
        tup_desc = CreateTemplateTupleDesc(GPC_TUPLES_ATTR_NUM, false);

        TupleDescInitEntry(tup_desc, (AttrNumber) 1, "nodename",
                           TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber) 2, "query",
                           TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber) 3, "refcount",
                           INT4OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber) 4, "valid",
                           BOOLOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber) 5, "databaseid",
                           OIDOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber) 6, "schema_name",
                           TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber) 7, "params_num",
                           INT4OID, -1, 0);

        /* complete descriptor of the tupledesc */
        func_ctx->tuple_desc = BlessTupleDesc(tup_desc);

        /* total number of tuples to be returned */
        if (ENABLE_THREAD_POOL && ENABLE_DN_GPC) {
            func_ctx->user_fctx = (void *)GPC->GetStatus(&(func_ctx->max_calls));
        } else {
            func_ctx->max_calls = 0;
        }

        (void)MemoryContextSwitchTo(old_context);
    }

    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();
    GPCStatus *entry = (GPCStatus *)func_ctx->user_fctx;

    if (func_ctx->call_cntr < func_ctx->max_calls) {
        /* do when there is more left to send */
        Datum values[GPC_TUPLES_ATTR_NUM];
        bool nulls[GPC_TUPLES_ATTR_NUM];
        HeapTuple tuple;

        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        entry += func_ctx->call_cntr;

        values[0] = CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);
        values[1] = CStringGetTextDatum(entry->query);
        values[2] = Int32GetDatum(entry->refcount);
        values[3] = BoolGetDatum(entry->valid);
        values[4] = DatumGetObjectId(entry->DatabaseID);
        values[5] = CStringGetTextDatum(entry->schema_name);
        values[6] = Int32GetDatum(entry->params_num);

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    } else {
        /* do when there is no more left */
        SRF_RETURN_DONE(func_ctx);
    }
}

Datum gs_globalplancache_prepare_status(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_MULTIPLE_NODES
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
#endif

    FuncCallContext *func_ctx = NULL;
    MemoryContext old_context;

    /* stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tup_desc;

        /* create a function context for cross-call persistence */
        func_ctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function
         * calls
         */
        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

#define GPC_PREPARE_TUPLES_ATTR_NUM 5

        /* need a tuple descriptor representing 12 columns */
        tup_desc = CreateTemplateTupleDesc(GPC_PREPARE_TUPLES_ATTR_NUM, false);

        TupleDescInitEntry(tup_desc, (AttrNumber) 1, "nodename", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber) 2, "global_sess_id", INT4OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber) 3, "statement_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber) 4, "refcount", INT4OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber) 5, "is_shared", BOOLOID, -1, 0);

        /* complete descriptor of the tupledesc */
        func_ctx->tuple_desc = BlessTupleDesc(tup_desc);

        /* total number of tuples to be returned */
        if (ENABLE_THREAD_POOL && ENABLE_DN_GPC) {
            func_ctx->user_fctx = (void *)GPC->GetPrepareStatus(&(func_ctx->max_calls));
        } else {
            func_ctx->max_calls = 0;
        }

        (void)MemoryContextSwitchTo(old_context);
    }

    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();
    GPCPrepareStatus *entry = (GPCPrepareStatus *)func_ctx->user_fctx;

    if (func_ctx->call_cntr < func_ctx->max_calls) {
        /* do when there is more left to send */
        Datum values[GPC_PREPARE_TUPLES_ATTR_NUM];
        bool nulls[GPC_PREPARE_TUPLES_ATTR_NUM];
        HeapTuple tuple;

        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        entry += func_ctx->call_cntr;

        values[0] = CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);
        values[1] = Int32GetDatum(entry->global_session_id);
        values[2] = CStringGetTextDatum(entry->statement_name);
        values[3] = Int32GetDatum(entry->refcount);
        values[4] = BoolGetDatum(entry->is_shared);

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    } else {
        /* do when there is no more left */
        SRF_RETURN_DONE(func_ctx);
    }
}

Datum local_rto_stat(PG_FUNCTION_ARGS)
{
    TupleDesc tup_desc = NULL;
    HeapTuple tuple = NULL;
    Datum values[RTO_VIEW_COL_SIZE];
    bool nulls[RTO_VIEW_COL_SIZE] = {false};
    uint32 i;

    tup_desc = CreateTemplateTupleDesc(RTO_VIEW_COL_SIZE, false);
    for (i = 0; i < RTO_VIEW_COL_SIZE; i++) {
        TupleDescInitEntry(tup_desc, (AttrNumber)(i + 1), g_rtoViewArr[i].name, g_rtoViewArr[i].data_type, -1, 0);
        values[i] = g_rtoViewArr[i].get_data();
        nulls[i] = false;
    }

    tup_desc = BlessTupleDesc(tup_desc);
    tuple = heap_form_tuple(tup_desc, values, nulls);
    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

Datum remote_rto_stat(PG_FUNCTION_ARGS)
{
    FuncCallContext* func_ctx = NULL;
#ifndef ENABLE_MULTIPLE_NODES
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported view in single node mode.")));

    SRF_RETURN_DONE(func_ctx);
#else
    Datum values[RTO_VIEW_COL_SIZE];
    bool nulls[RTO_VIEW_COL_SIZE] = {false};
    uint32 i;

    if (SRF_IS_FIRSTCALL()) {
        func_ctx = SRF_FIRSTCALL_INIT();

        MemoryContext oldcontext = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples. */
        TupleDesc tupdesc = CreateTemplateTupleDesc(RTO_VIEW_COL_SIZE, false);
        for (i = 0; i < RTO_VIEW_COL_SIZE; i++) {
            TupleDescInitEntry(tupdesc, (AttrNumber)(i + 1), g_rtoViewArr[i].name, g_rtoViewArr[i].data_type, -1, 0);
            nulls[i] = false;
        }

        func_ctx->tuple_desc = BlessTupleDesc(tupdesc);
        func_ctx->max_calls = u_sess->pgxc_cxt.NumDataNodes + u_sess->pgxc_cxt.NumCoords;

        /* the main call for get get_rto_stat */
        func_ctx->user_fctx = get_rto_stat(func_ctx->tuple_desc);
        MemoryContextSwitchTo(oldcontext);

        if (func_ctx->user_fctx == NULL) {
            SRF_RETURN_DONE(func_ctx);
        }
    }

    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->user_fctx) {
        Tuplestorestate* tupstore = ((TableDistributionInfo*)func_ctx->user_fctx)->state->tupstore;
        TupleTableSlot* slot = ((TableDistributionInfo*)func_ctx->user_fctx)->slot;
        if (!tuplestore_gettupleslot(tupstore, true, false, slot)) {
            FreeParallelFunctionState(((TableDistributionInfo*)func_ctx->user_fctx)->state);
            ExecDropSingleTupleTableSlot(slot);
            pfree_ext(func_ctx->user_fctx);
            func_ctx->user_fctx = NULL;
            SRF_RETURN_DONE(func_ctx);
        }
        for (i = 0; i < RTO_VIEW_COL_SIZE; i++) {
            values[i] = slot_getattr(slot, (i + 1), &nulls[i]);
        }
        HeapTuple tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        (void)ExecClearTuple(slot);

        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    }
    SRF_RETURN_DONE(func_ctx);
#endif
}

Datum local_recovery_status(PG_FUNCTION_ARGS)
{
    FuncCallContext* func_ctx = NULL;
    MemoryContext old_context = NULL;
    RTOStandbyData* entry = NULL;

    /* stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tup_desc = NULL;

        /* create a function context for cross-call persistence */
        func_ctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function
         * calls
         */
        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        /* need a tuple descriptor representing 6 columns */
        tup_desc = CreateTemplateTupleDesc(RECOVERY_RTO_VIEW_COL, false);

        TupleDescInitEntry(tup_desc, (AttrNumber)1, "node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)2, "standby_node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)3, "source_ip", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)4, "source_port", INT4OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)5, "dest_ip", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)6, "dest_port", INT4OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)7, "current_rto", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)8, "target_rto", INT8OID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)9, "current_sleep_time", INT8OID, -1, 0);

        /* complete descriptor of the tupledesc */
        func_ctx->tuple_desc = BlessTupleDesc(tup_desc);
        func_ctx->user_fctx = (void*)GetRTOStat(&(func_ctx->max_calls));

        (void)MemoryContextSwitchTo(old_context);
    }

    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();

    entry = (RTOStandbyData*)func_ctx->user_fctx;

    if (func_ctx->call_cntr < func_ctx->max_calls) {
        /* do when there is more left to send */
        Datum values[RECOVERY_RTO_VIEW_COL];
        bool nulls[RECOVERY_RTO_VIEW_COL] = {false};
        HeapTuple tuple = NULL;

        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        entry += func_ctx->call_cntr;

        values[0] = CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);
        values[1] = CStringGetTextDatum(entry->id);
        values[2] = CStringGetTextDatum(entry->source_ip);
        values[3] = Int32GetDatum(entry->source_port);
        values[4] = CStringGetTextDatum(entry->dest_ip);
        values[5] = Int32GetDatum(entry->dest_port);
        values[6] = Int64GetDatum(entry->current_rto);
        values[7] = Int64GetDatum(entry->target_rto);
        values[8] = Int64GetDatum(entry->current_sleep_time);

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    }

    /* do when there is no more left */
    SRF_RETURN_DONE(func_ctx);
}

Datum remote_recovery_status(PG_FUNCTION_ARGS)
{
    FuncCallContext* func_ctx = NULL;
#ifndef ENABLE_MULTIPLE_NODES
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported view in single node mode.")));

    SRF_RETURN_DONE(func_ctx);
#else
    Datum values[RECOVERY_RTO_VIEW_COL];
    bool nulls[RECOVERY_RTO_VIEW_COL] = {false};

    if (SRF_IS_FIRSTCALL()) {
        func_ctx = SRF_FIRSTCALL_INIT();

        MemoryContext oldcontext = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples. */
        TupleDesc tupdesc = CreateTemplateTupleDesc(RECOVERY_RTO_VIEW_COL, false);
        TupleDescInitEntry(tupdesc, (AttrNumber)1, "node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "standby_node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "source_ip", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)4, "source_port", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)5, "dest_ip", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)6, "dest_port", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)7, "current_rto", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)8, "target_rto", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)9, "current_sleep_time", INT8OID, -1, 0);

        func_ctx->tuple_desc = BlessTupleDesc(tupdesc);
        func_ctx->max_calls = u_sess->pgxc_cxt.NumDataNodes + u_sess->pgxc_cxt.NumCoords;

        /* the main call for get get_rto_stat */
        func_ctx->user_fctx = get_recovery_stat(func_ctx->tuple_desc);
        MemoryContextSwitchTo(oldcontext);

        if (func_ctx->user_fctx == NULL) {
            SRF_RETURN_DONE(func_ctx);
        }
    }

    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();
    if (func_ctx->user_fctx) {
        Tuplestorestate* tupstore = ((TableDistributionInfo*)func_ctx->user_fctx)->state->tupstore;
        TupleTableSlot* slot = ((TableDistributionInfo*)func_ctx->user_fctx)->slot;

        if (!tuplestore_gettupleslot(tupstore, true, false, slot)) {
            FreeParallelFunctionState(((TableDistributionInfo*)func_ctx->user_fctx)->state);
            ExecDropSingleTupleTableSlot(slot);
            pfree_ext(func_ctx->user_fctx);
            func_ctx->user_fctx = NULL;
            SRF_RETURN_DONE(func_ctx);
        }
        for (uint32 i = 0; i < RECOVERY_RTO_VIEW_COL; i++) {
            values[i] = slot_getattr(slot, (i + 1), &nulls[i]);
        }
        HeapTuple tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        (void)ExecClearTuple(slot);
        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    }
    SRF_RETURN_DONE(func_ctx);
#endif
}
