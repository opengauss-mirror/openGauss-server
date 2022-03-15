/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * instr_unique_sql.cpp
 *   functions for unique SQL
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/instruments/unique_sql/unique_sql.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "instruments/instr_unique_sql.h"
#include "instruments/instr_statement.h"
#include "instruments/instr_slow_query.h"
#include "instruments/unique_query.h"
#include "utils/atomic.h"
#include "executor/hashjoin.h"
#include "utils/lsyscache.h"
#include "utils/hsearch.h"
#include "access/hash.h"
#include "access/xact.h"
#include "utils/memutils.h"
#include "miscadmin.h"
#include "pgxc/pgxc.h"
#include "pgstat.h"
#include "funcapi.h"
#include "parser/analyze.h"
#include "commands/prepare.h"
#include "libpq/pqformat.h"
#include "libpq/libpq.h"
#include "commands/user.h"
#include "instruments/unique_sql_basic.h"
#include "instruments/instr_handle_mgr.h"
#include "optimizer/streamplan.h"

static bool need_reuse_unique_sql_id(Query *query);

namespace UniqueSq {
void unique_sql_post_parse_analyze(ParseState* pstate, Query* query);
int get_conn_count_from_all_handles(PGXCNodeAllHandles* pgxc_handles, bool is_cn);
PGXCNodeHandle** get_handles_from_all_handles(PGXCNodeAllHandles* pgxc_handles, bool is_cn);
}  // namespace UniqueSq
/*
 * Note: for unique sql track type
 *
 * top - only top SQL, for example, when call a proc, there will be
 *       multi-sqls in the proc implementatation, only the original
 *       call proc SQL will be collected by unique sql module.
 * all - (not currently supported) take above case as example, all statements will be recorded
 */
/* unique SQL max hash table size */
const int UNIQUE_SQL_MAX_HASH_SIZE = 1000;

/*
 * CN to collect unique sql stat from DN, each time will
 * send "predefined size" unique sql keys to DN
 */
const int UNIQUE_SQL_IDS_ARRAY_SIZE = 10;

/* used for max unique sql string length,
 * using GUC: pgstat_track_activity_query_size,
 * as PgBackendStatus::st_activity also using the GUC parameter
 */
#define UNIQUE_SQL_MAX_LEN (g_instance.attr.attr_common.pgstat_track_activity_query_size + 1)
#define MAX_UINT32 (0xFFFFFFFF)
/* for each memory allocating, max unique sql count in the memory */
#define MAX_MEM_UNIQUE_SQL_ENTRY_COUNT 1000.0

#define UNIQUE_SQL_HASH_TBL "unique sql hash table"
#define STRING_MAX_LEN 256

typedef struct {
    List *batch_list;
    ListCell *curr_cell;
} UniqueSQLResults;

#ifndef ENABLE_MULTIPLE_NODES
/* The mapping of UniqueSQLKey to updated_time */
typedef struct {
    UniqueSQLKey key;
    TimestampTz updated_time;        /* latest update time for the unique sql entry */
} KeyUpdatedtime;

static KeyUpdatedtime* GetSortedEntryList();
static int KeyUpdatedtimeCmp(const void* a, const void* b);
static bool AutoRecycleUniqueSQLEntry();
#endif

static uint32 uniqueSQLHashCode(const void* key, Size size)
{
    const UniqueSQLKey* k = (const UniqueSQLKey*)key;

    return hash_uint32((uint32)k->cn_id) ^ hash_uint32((uint32)k->user_id) ^ hash_uint32((uint32)k->unique_sql_id);
}

static int uniqueSQLMatch(const void* key1, const void* key2, Size keysize)
{
    const UniqueSQLKey* k1 = (const UniqueSQLKey*)key1;
    const UniqueSQLKey* k2 = (const UniqueSQLKey*)key2;

    if (k1 != NULL && k2 != NULL && k1->user_id == k2->user_id && k1->unique_sql_id == k2->unique_sql_id) {
        return 0;
    } else {
        return 1;
    }
}

static LWLock* LockUniqueSQLHashPartition(uint32 hashCode, LWLockMode lockMode)
{
    LWLock* partitionLock = GetMainLWLockByIndex(FirstUniqueSQLMappingLock + (hashCode % NUM_UNIQUE_SQL_PARTITIONS));
    LWLockAcquire(partitionLock, lockMode);

    return partitionLock;
}

static void UnlockUniqueSQLHashPartition(uint32 hashCode)
{
    LWLock* partitionLock = GetMainLWLockByIndex(FirstUniqueSQLMappingLock + (hashCode % NUM_UNIQUE_SQL_PARTITIONS));
    LWLockRelease(partitionLock);
}

/*
 * resetUniqueSQLEntry - reset UniqueSQL entry except key
 */
static void resetUniqueSQLEntry(UniqueSQL* entry)
{
    if (entry != NULL) {
        pg_atomic_write_u64(&(entry->calls), 0);
        entry->unique_sql = NULL;

        // reset elapse time stat
        gs_lock_test_and_set_64(&(entry->elapse_time.total_time), 0);
        gs_lock_test_and_set_64(&(entry->elapse_time.min_time), 0);
        gs_lock_test_and_set_64(&(entry->elapse_time.max_time), 0);

        // reset row activity stat
        pg_atomic_write_u64(&(entry->row_activity.returned_rows), 0);
        pg_atomic_write_u64(&(entry->row_activity.tuples_fetched), 0);
        pg_atomic_write_u64(&(entry->row_activity.tuples_returned), 0);
        pg_atomic_write_u64(&(entry->row_activity.tuples_inserted), 0);
        pg_atomic_write_u64(&(entry->row_activity.tuples_updated), 0);
        pg_atomic_write_u64(&(entry->row_activity.tuples_deleted), 0);

        // cache_io
        pg_atomic_write_u64(&(entry->cache_io.blocks_fetched), 0);
        pg_atomic_write_u64(&(entry->cache_io.blocks_hit), 0);

        // parse info
        pg_atomic_write_u64(&(entry->parse.soft_parse), 0);
        pg_atomic_write_u64(&(entry->parse.hard_parse), 0);

        // sort hash work_mem info
        pg_atomic_write_u64(&(entry->sort_state.counts), 0);
        gs_lock_test_and_set_64(&(entry->sort_state.total_time), 0);
        gs_lock_test_and_set_64(&(entry->sort_state.used_work_mem), 0);
        pg_atomic_write_u64(&(entry->sort_state.spill_counts), 0);
        pg_atomic_write_u64(&(entry->sort_state.spill_size), 0);
        
        pg_atomic_write_u64(&(entry->hash_state.counts), 0);
        gs_lock_test_and_set_64(&(entry->hash_state.total_time), 0);
        gs_lock_test_and_set_64(&(entry->hash_state.used_work_mem), 0);
        pg_atomic_write_u64(&(entry->hash_state.spill_counts), 0);
        pg_atomic_write_u64(&(entry->hash_state.spill_size), 0);

        // time Info
        for (uint32 idx = 0; idx < TOTAL_TIME_INFO_TYPES; idx++) {
            gs_lock_test_and_set_64(&(entry->timeInfo.TimeInfoArray[idx]), 0);
        }

        // net info
        for (uint32 idx = 0; idx < TOTAL_NET_INFO_TYPES; idx++) {
            pg_atomic_write_u64(&(entry->netInfo.netInfoArray[idx]), 0);
        }

        entry->is_local = false;
    }
}

/*
 * InitUniqueSQL - init unique sql resources
 *
 * Init resource when postmaster startup,
 * basic unique SQL objects are created
 * without GUI parameter control.
 */
void InitUniqueSQL()
{
    // init memory context
    if (g_instance.stat_cxt.UniqueSqlContext == NULL) {
        g_instance.stat_cxt.UniqueSqlContext = AllocSetContextCreate(g_instance.instance_context,
            "UniqueSQLContext",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE,
            SHARED_CONTEXT);
    }

    // init unique sql hash table
    HASHCTL ctl;
    errno_t rc;

    rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check_c(rc, "\0", "\0");

    ctl.hcxt = g_instance.stat_cxt.UniqueSqlContext;
    ctl.keysize = sizeof(UniqueSQLKey);

    // alloc extra space for normalized query(only CN stores sql string)
    if (need_normalize_unique_string()) {
        ctl.entrysize = sizeof(UniqueSQL) + UNIQUE_SQL_MAX_LEN;
    } else {
        ctl.entrysize = sizeof(UniqueSQL);
    }

    ctl.hash = uniqueSQLHashCode;
    ctl.match = uniqueSQLMatch;
    ctl.num_partitions = NUM_UNIQUE_SQL_PARTITIONS;

    g_instance.stat_cxt.UniqueSQLHashtbl = hash_create(UNIQUE_SQL_HASH_TBL,
        UNIQUE_SQL_MAX_HASH_SIZE,
        &ctl,
        HASH_ELEM | HASH_SHRCTX | HASH_FUNCTION | HASH_COMPARE | HASH_PARTITION | HASH_NOEXCEPT);
    init_builtin_unique_sql();
}

void instr_unique_sql_register_hook()
{
    // only register the hooks on CN
    if ((!IS_PGXC_COORDINATOR) && (!IS_SINGLE_NODE)) {
        return;
    }

    // register hooks
    t_thrd.statement_cxt.instr_prev_post_parse_analyze_hook = (void *)post_parse_analyze_hook;
    post_parse_analyze_hook = UniqueSq::unique_sql_post_parse_analyze;
}

/*
 * UpdateUniqueSQLCalls - update unique SQL calls
 */
static void UpdateUniqueSQLCalls(UniqueSQL* unique_sql)
{
    Assert(u_sess->attr.attr_resource.enable_resource_track &&
           (u_sess->attr.attr_common.instr_unique_sql_count > 0));

    if (unique_sql == NULL) {
        return;
    }

    pg_atomic_fetch_add_u64(&(unique_sql->calls), 1);
}

/*
 * updateMaxValueForAtomicType - using atomic type to store max value,
 * we need update the max value by using atomic method
 */
static void updateMaxValueForAtomicType(int64 new_val, int64* max)
{
    int64 prev;
    do {
        prev = *max;
    } while (prev < new_val && !gs_compare_and_swap_64(max, prev, new_val));
}

/*
 * updateMinValueForAtomicType - update ming value for atomic type
 */
static void updateMinValueForAtomicType(int64 new_val, int64* mix)
{
    int64 prev;
    do {
        prev = *mix;
    } while ((prev == 0 || prev > new_val) && !gs_compare_and_swap_64(mix, prev, new_val));
}

/*
 * UpdateUniqueSQLElapseTime - update elase time of the unique sql
 *
 * elapse end time is calculated in this function
 */
static void UpdateUniqueSQLElapseTime(UniqueSQL* unique_sql, int64 elapse_start)
{
    Assert(u_sess->attr.attr_resource.enable_resource_track &&
           (u_sess->attr.attr_common.instr_unique_sql_count > 0));
    if (unique_sql == NULL || elapse_start == 0) {
        return;
    }

    TimestampTz elapse_time = GetCurrentTimestamp() - elapse_start;
    /* Because the time precision is microseconds,
     * all actions less than microseconds are recorded as 0.
     * When the duration is 0, we set the duration to 1
     * */
    elapse_time = (elapse_time == 0) ? 1 : elapse_time;

    /* update unique sql's total/max/min time */
    gs_atomic_add_64(&(unique_sql->elapse_time.total_time), elapse_time);
    updateMaxValueForAtomicType(elapse_time, &(unique_sql->elapse_time.max_time));
    updateMinValueForAtomicType(elapse_time, &(unique_sql->elapse_time.min_time));
}

/*
 * UpdateUniqueSQLStatDetail - update row activity or cache/IO
 */
static void UpdateUniqueSQLStatDetail(UniqueSQL* unique_sql, PgStat_TableCounts* agg_table_stat, uint64 returned_rows)
{
    Assert(u_sess->attr.attr_resource.enable_resource_track &&
           (u_sess->attr.attr_common.instr_unique_sql_count > 0));
    if (unique_sql == NULL) {
        return;
    }

    if (agg_table_stat != NULL) {
        // row activity
        pg_atomic_fetch_add_u64(&unique_sql->row_activity.tuples_fetched, agg_table_stat->t_tuples_fetched);
        pg_atomic_fetch_add_u64(&unique_sql->row_activity.tuples_returned, agg_table_stat->t_tuples_returned);

        pg_atomic_fetch_add_u64(&unique_sql->row_activity.tuples_inserted, agg_table_stat->t_tuples_inserted);
        pg_atomic_fetch_add_u64(&unique_sql->row_activity.tuples_updated, agg_table_stat->t_tuples_updated);
        pg_atomic_fetch_add_u64(&unique_sql->row_activity.tuples_deleted, agg_table_stat->t_tuples_deleted);

        // cache_io
        pg_atomic_fetch_add_u64(&unique_sql->cache_io.blocks_fetched, agg_table_stat->t_blocks_fetched);
        pg_atomic_fetch_add_u64(&unique_sql->cache_io.blocks_hit, agg_table_stat->t_blocks_hit);

#ifdef ENABLE_MULTIPLE_NODES
        /* SQL: 'START TRANSACTION' should not be passed down to DN directly */
        if (IS_PGXC_DATANODE && unique_sql->key.unique_sql_id == START_TRX_UNIQUE_SQL_ID) {
            ereport(LOG, (errmodule(MOD_INSTR), errmsg("[UniqueSQL] recv 'START TRANSACTION' on DN")));
        }
#endif
    }

    if (returned_rows > 0) {
        pg_atomic_fetch_add_u64(&unique_sql->row_activity.returned_rows, returned_rows);
        instr_stmt_report_returned_rows(returned_rows);
    }
}

/*
 * update hard/soft parse counter info
 *
 * soft parse - reuse plan
 * hard parse - generate new plan
 */
static void UpdateUniqueSQLParse(UniqueSQL* unique_sql)
{
    if (unique_sql == NULL) {
        return;
    }

    if (u_sess->unique_sql_cxt.unique_sql_soft_parse > 0) {
        pg_atomic_fetch_add_u64(&unique_sql->parse.soft_parse, u_sess->unique_sql_cxt.unique_sql_soft_parse);
        instr_stmt_report_soft_parse(u_sess->unique_sql_cxt.unique_sql_soft_parse);
    }

    if (u_sess->unique_sql_cxt.unique_sql_hard_parse > 0) {
        pg_atomic_fetch_add_u64(&unique_sql->parse.hard_parse, u_sess->unique_sql_cxt.unique_sql_hard_parse);
        instr_stmt_report_hard_parse(u_sess->unique_sql_cxt.unique_sql_hard_parse);
    }

    ereport(DEBUG1,
        (errmodule(MOD_INSTR),
            errmsg("[UniqueSQL] unique id: %lu, agg soft parse: %lu, hard parse: %lu",
                u_sess->unique_sql_cxt.unique_sql_id,
                u_sess->unique_sql_cxt.unique_sql_soft_parse,
                u_sess->unique_sql_cxt.unique_sql_hard_parse)));

    /* for parse counter, when update unique stat, aggregate&reset last parse counter */
    UniqueSQLStatCountResetParseCounter();
}

/*
 * update sort/hash work_mem info when executor run finished
 */
static void UpdateUniqueSQLSortHashInfo(UniqueSQL* unique_sql)
{
    if (unique_sql == NULL) {
        return;
    }

    if (u_sess->unique_sql_cxt.unique_sql_sort_instr->has_sorthash) {
        /* if query contains SORT operation, the unique sql sort info will be updated */
        unique_sql_sorthash_instr* sort_instr = u_sess->unique_sql_cxt.unique_sql_sort_instr;

        pg_atomic_fetch_add_u64(&unique_sql->sort_state.counts, sort_instr->counts);
        gs_atomic_add_64(&unique_sql->sort_state.total_time, sort_instr->total_time);
        gs_atomic_add_64(&unique_sql->sort_state.used_work_mem, sort_instr->used_work_mem);
        pg_atomic_fetch_add_u64(&unique_sql->sort_state.spill_counts, sort_instr->spill_counts);
        pg_atomic_fetch_add_u64(&unique_sql->sort_state.spill_size, sort_instr->spill_size);

        ereport(DEBUG1,
            (errmodule(MOD_INSTR),
                errmsg("[UniqueSQL] unique id: %lu, sort state updated",
                    u_sess->unique_sql_cxt.unique_sql_id)));

        /* reset the sort counter */
        errno_t rc = memset_s(sort_instr, sizeof(unique_sql_sorthash_instr), 0, sizeof(unique_sql_sorthash_instr));
        securec_check(rc, "", "");
    }

    if (u_sess->unique_sql_cxt.unique_sql_hash_instr->has_sorthash) {
        /* if query contains HASH operation, the unique sql hash info should be updated */
        unique_sql_sorthash_instr* hash_instr = u_sess->unique_sql_cxt.unique_sql_hash_instr;

        pg_atomic_fetch_add_u64(&unique_sql->hash_state.counts, hash_instr->counts);
        gs_atomic_add_64(&unique_sql->hash_state.total_time, hash_instr->total_time);
        gs_atomic_add_64(&unique_sql->hash_state.used_work_mem, hash_instr->used_work_mem);
        pg_atomic_fetch_add_u64(&unique_sql->hash_state.spill_counts, hash_instr->spill_counts);
        pg_atomic_fetch_add_u64(&unique_sql->hash_state.spill_size, hash_instr->spill_size);

        ereport(DEBUG1,
            (errmodule(MOD_INSTR),
                errmsg("[UniqueSQL] unique id: %lu, hash state updated",
                    u_sess->unique_sql_cxt.unique_sql_id)));

        /* reset the hash counter */
        errno_t rc = memset_s(hash_instr, sizeof(unique_sql_sorthash_instr), 0, sizeof(unique_sql_sorthash_instr));
        securec_check(rc, "", "");
    }
}

/*
 * get hash state from hashtable to update unqiue sql hash info later
 */
void UpdateUniqueSQLHashStats(HashJoinTable hashtable, TimestampTz* start_time)
{
    /* isUniqueSQLContextInvalid happens when the query is explain xxx */
    if (!is_unique_sql_enabled() || isUniqueSQLContextInvalid()) {
        return;
    }

    /* the first time enter hash operation, init hash state */
    unique_sql_sorthash_instr* instr = u_sess->unique_sql_cxt.unique_sql_hash_instr;
    instr->has_sorthash = true;

    /* update time info */
    if (*start_time == 0) {
        *start_time = GetCurrentTimestamp();
    } else if (hashtable != NULL) {
        /* increased by hash exec time */
        instr->total_time += GetCurrentTimestamp() - *start_time;

        /* update work mem info */
        instr->counts += 1;
        instr->spill_counts += hashtable->spill_count;

        /* get the space used in kbs */
        instr->spill_size += (*hashtable->spill_size + 1023) / 1024;
        instr->used_work_mem += (hashtable->spacePeak + 1023) / 1024;
    }
}

/* UpdateUniqueSQLVecSortStats - parse the vector sort information from the Batchsortstate,
 * used to update for the uniuqe sql sort infomation.
 */
void UpdateUniqueSQLVecSortStats(Batchsortstate* state, uint64 spill_count, TimestampTz* start_time)
{
    /* isUniqueSQLContextInvalid happens when the query is explain xxx */
    if (!is_unique_sql_enabled() || isUniqueSQLContextInvalid()) {
        return;
    }
    unique_sql_sorthash_instr* instr = u_sess->unique_sql_cxt.unique_sql_sort_instr;
    /* the first time enter sort executor and init the state */
    instr->has_sorthash = true;

    if (*start_time == 0) {
        *start_time = GetCurrentTimestamp();
    } else if (state != NULL) {
        instr->counts += 1;
        instr->total_time += GetCurrentTimestamp() - *start_time;

        /* update vect sort info of space used in kbs */
        if (state->m_tapeset != NULL) {
            instr->spill_counts += spill_count;
            instr->spill_size += LogicalTapeSetBlocks(state->m_tapeset) * (BLCKSZ / 1024);
        } else {
            instr->used_work_mem += (state->m_allowedMem - state->m_availMem + 1023) / 1024;
        }
    }
}

static void set_unique_sql_string_in_entry(UniqueSQL* entry, Query* query, const char* sql, int32 multi_sql_offset)
{
    errno_t rc = EOK;

    // only CN stores normalized query string
    if (need_normalize_unique_string()) {
        entry->unique_sql = (char*)(entry + 1);
        rc = memset_s(entry->unique_sql, UNIQUE_SQL_MAX_LEN, 0, UNIQUE_SQL_MAX_LEN);
        securec_check(rc, "\0", "\0");
    } else {
        entry->unique_sql = NULL;
    }

    if (entry->unique_sql != NULL && sql != NULL && query != NULL) {
        entry->is_local = true;
        // generate and store normalized query string
        if (normalized_unique_querystring(query, sql, entry->unique_sql, UNIQUE_SQL_MAX_LEN - 1,
            multi_sql_offset)) {
            entry->unique_sql = trim(entry->unique_sql);
        } else {
            ereport(LOG,
                (errmodule(MOD_INSTR),
                    errmsg("[UniqueSQL] unique id: %lu, normalized "
                           "SQL failed!",
                        u_sess->unique_sql_cxt.unique_sql_id)));
        }
    }
}

static void UpdateUniqueSQLTimeStat(UniqueSQL* entry, int64 timeInfo[])
{
    if (timeInfo != NULL) {
        int idx;

        for (idx = 0; idx < TOTAL_TIME_INFO_TYPES; idx++) {
            (void)gs_atomic_add_64(&(entry->timeInfo.TimeInfoArray[idx]), timeInfo[idx]);
        }
    }
}
static void UpdateUniqueSQLNetInfo(UniqueSQL* entry, const uint64* netInfo)
{
    if (netInfo == NULL)
        return;
    for (int i = 0; i < TOTAL_NET_INFO_TYPES; i++) {
        (void)pg_atomic_fetch_add_u64(&(entry->netInfo.netInfoArray[i]), netInfo[i]);
    }
}

bool isUniqueSQLContextInvalid()
{
    if (u_sess->unique_sql_cxt.unique_sql_id == 0 ||
        g_instance.stat_cxt.UniqueSQLHashtbl == NULL ||
        !OidIsValid(u_sess->unique_sql_cxt.unique_sql_user_id)) {
        return true;
    }

    if (IS_SINGLE_NODE) {
        return false;
    }

    if (!OidIsValid(u_sess->unique_sql_cxt.unique_sql_cn_id)) {
        return true;
    }

    return false;
}

void UpdateSingleNodeByPassUniqueSQLStat(bool isTopLevel)
{
    if (IS_SINGLE_NODE && is_unique_sql_enabled() && isTopLevel) {
        if (IS_UNIQUE_SQL_TRACK_TOP && IsTopUniqueSQL()) {
            UpdateUniqueSQLStat(NULL, NULL, u_sess->unique_sql_cxt.unique_sql_start_time);
        }

        if (u_sess->unique_sql_cxt.unique_sql_start_time != 0) {
            int64 duration = GetCurrentTimestamp() - u_sess->unique_sql_cxt.unique_sql_start_time;
            pgstat_update_responstime_singlenode(
                u_sess->unique_sql_cxt.unique_sql_id, u_sess->unique_sql_cxt.unique_sql_start_time, duration);
        }
    }
}

/*
 * UpdateUniqueSQLStat - update unique sql's stat info
 *
 * 1, find/create unique sql entry
 * 2, udpate stat info
 *     - calls
 *     - response time(only input the start elapse time)
 */
void UpdateUniqueSQLStat(Query* query, const char* sql, int64 elapse_start_time,
    PgStat_TableCounts* agg_table_stat, UniqueSQLStat* sqlStat)
{
    /* unique sql id can't be zero */
    if (isUniqueSQLContextInvalid()) {
        ereport(DEBUG2,
            (errmodule(MOD_INSTR),
                errmsg("[UniqueSQL] Failed to update entry - cn id: %u, user id: %u, unique sql id: %lu,",
                    u_sess->unique_sql_cxt.unique_sql_cn_id,
                    u_sess->unique_sql_cxt.unique_sql_user_id,
                    u_sess->unique_sql_cxt.unique_sql_id)));
        return;
    }

    UniqueSQLKey key;
    UniqueSQL* entry = NULL;
    bool found = false;

    key.unique_sql_id = u_sess->unique_sql_cxt.unique_sql_id;
    key.cn_id = u_sess->unique_sql_cxt.unique_sql_cn_id;
    key.user_id = u_sess->unique_sql_cxt.unique_sql_user_id;

    ereport(DEBUG1,
        (errmodule(MOD_INSTR),
            errmsg("[UniqueSQL] update entry - cn id: %u, user id: %u, sql id: %lu,",
                key.cn_id,
                key.user_id,
                key.unique_sql_id)));

    uint32 hashCode = uniqueSQLHashCode(&key, sizeof(key));

    (void)LockUniqueSQLHashPartition(hashCode, LW_SHARED);
    entry = (UniqueSQL*)hash_search(g_instance.stat_cxt.UniqueSQLHashtbl, &key, HASH_FIND, NULL);
    if (entry == NULL) {
        UnlockUniqueSQLHashPartition(hashCode);

        /*
         * Handle race condition between HASH insert/clean
         * consider below scenario:
         *   1, user runs SQL S1 firstly.
         *   2, insert normalized sql string to unique sql hash
         *   3, ----> clean unique sql hash table(another session)
         *   4, insert/update other SQL stat, such as, soft/hard parse counter, etc.
         *   5, then the sql text field will be empty forever.
         *
         * solution:
         *   1, on DN/remote CN(utiliti statement), no special control
         *   2, on local CN, if entry not existed, only can insert new entry when query(Query *)
         *   is not NULL
         */
#ifdef ENABLE_MULTIPLE_NODES
        if (is_local_unique_sql() && query == NULL) {
            return;
        }
        /* control unique sql number by instr_unique_sql_count. */
        long totalCount = hash_get_num_entries(g_instance.stat_cxt.UniqueSQLHashtbl);
        if (totalCount >= u_sess->attr.attr_common.instr_unique_sql_count) {
            ereport(DEBUG2,
                (errmodule(MOD_INSTR), errmsg("[UniqueSQL] Failed to insert unique sql for up to limit")));
            return;
        }
#else
        if (is_local_unique_sql() && query == NULL && u_sess->unique_sql_cxt.unique_sql_text == NULL) {
            return;
        }

        /* control unique sql number by instr_unique_sql_count. */
        long totalCount = hash_get_num_entries(g_instance.stat_cxt.UniqueSQLHashtbl);
        if (totalCount >= u_sess->attr.attr_common.instr_unique_sql_count) {
            if (g_instance.attr.attr_common.enable_auto_clean_unique_sql) {
                if (!AutoRecycleUniqueSQLEntry()) {
                    return;
                }
            } else {
                ereport(DEBUG2,
                    (errmodule(MOD_INSTR), errmsg("[UniqueSQL] Failed to insert unique sql for up to limit")));
                return;
            }
        }
#endif

        (void)LockUniqueSQLHashPartition(hashCode, LW_EXCLUSIVE);
        entry = (UniqueSQL*)hash_search(g_instance.stat_cxt.UniqueSQLHashtbl, &key, HASH_ENTER, &found);
        // out of memory
        if (entry == NULL) {
            UnlockUniqueSQLHashPartition(hashCode);
            return;
        }

        if (!found) {
            resetUniqueSQLEntry(entry);
#ifndef ENABLE_MULTIPLE_NODES
            if (query == NULL && u_sess->unique_sql_cxt.unique_sql_text != NULL) {
                entry->unique_sql = (char*)(entry + 1);
                errno_t rc = memset_s(entry->unique_sql, UNIQUE_SQL_MAX_LEN, 0, UNIQUE_SQL_MAX_LEN);
                securec_check(rc, "\0", "\0");
                entry->is_local = true;
                int unique_sql_textLen = strlen(u_sess->unique_sql_cxt.unique_sql_text);
                rc = memcpy_s(entry->unique_sql, UNIQUE_SQL_MAX_LEN, u_sess->unique_sql_cxt.unique_sql_text, unique_sql_textLen);
                securec_check(rc, "\0", "\0");
            } else {
                set_unique_sql_string_in_entry(entry, query, sql, u_sess->unique_sql_cxt.multi_sql_offset);
            }
#else
            set_unique_sql_string_in_entry(entry, query, sql, u_sess->unique_sql_cxt.multi_sql_offset);
#endif
        }
    }

    if ((IS_PGXC_COORDINATOR || IS_SINGLE_NODE) && elapse_start_time != 0) {
        ereport(DEBUG1,
            (errmodule(MOD_INSTR), errmsg("[UniqueSQL] unique id: %lu, update entry n_calls", key.unique_sql_id)));
        UpdateUniqueSQLCalls(entry);
        UpdateUniqueSQLElapseTime(entry, elapse_start_time);
        UpdateUniqueSQLStatDetail(entry, NULL, u_sess->unique_sql_cxt.unique_sql_returned_rows_counter);
        u_sess->unique_sql_cxt.need_update_calls = false;
    } else if (IS_PGXC_DATANODE && agg_table_stat != NULL) {
        UpdateUniqueSQLStatDetail(entry, agg_table_stat, 0);
        instr_stmt_report_unique_sql_info(agg_table_stat, NULL, NULL);
    }
    /* parse info(CN & DN) */
    UpdateUniqueSQLParse(entry);
    
    /* Sort&Hash info */
    UpdateUniqueSQLSortHashInfo(entry);

    if (sqlStat != NULL) {
        // record SQL's time Info
        UpdateUniqueSQLTimeStat(entry, sqlStat->timeInfo);

        /* record SQL's net info */
        UpdateUniqueSQLNetInfo(entry, sqlStat->netInfo);

        // record statement KPI info
        instr_stmt_report_unique_sql_info(NULL, sqlStat->timeInfo, sqlStat->netInfo);
    }
    (void)gs_lock_test_and_set_64(&entry->updated_time, GetCurrentTimestamp());
    UnlockUniqueSQLHashPartition(hashCode);
}

/*
 * SendUniqueSQLIds - send unique sql ids to specific DN
 *   return false failed
 */
static bool SendUniqueSQLIds(
    PGXCNodeHandle* handle, uint32* cn_ids, Oid* user_ids, uint64* query_ids, uint32* slot_indexes, uint count)
{
    /*
     * msg format:
     * 'i' + 4       +  1 + 4 +     (4 + 4 + 4 + 8) * count
     * 'i' + msg_len + 's'+ count + (slot_indexes + cn id + user_oid + unique_sql_id) + (...)
     */
    size_t msg_len = sizeof(uint32) + sizeof(char) + sizeof(uint32) +
                  (sizeof(uint32) + sizeof(uint32) + sizeof(Oid) + sizeof(uint64)) * count;
    int rc;
    uint32 n32;

    if (handle->state != DN_CONNECTION_STATE_IDLE) {
        return false;
    }

    ensure_out_buffer_capacity(1 + msg_len, handle);
    Assert(handle->outBuffer != NULL);
    /* instrumentation */
    handle->outBuffer[handle->outEnd++] = 'i';

    /* message length, not including 'i' */
    msg_len = htonl(msg_len);
    rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, &msg_len, sizeof(uint32));
    securec_check(rc, "\0", "\0");
    handle->outEnd += sizeof(uint32);

    /* unique sql ids to collect stat */
    handle->outBuffer[handle->outEnd++] = 's';

    n32 = htonl(count);
    rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, &n32, sizeof(uint32));
    securec_check(rc, "\0", "\0");
    handle->outEnd += sizeof(uint32);

    for (uint32 i = 0; i < count; i++) {
        /* slot index */
        rc = memcpy_s(
            handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, slot_indexes + i, sizeof(uint32));
        securec_check(rc, "\0", "\0");
        handle->outEnd += sizeof(uint32);

        /* cn id */
        rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, cn_ids + i, sizeof(uint32));
        securec_check(rc, "\0", "\0");
        handle->outEnd += sizeof(uint32);

        /* user id */
        rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, user_ids + i, sizeof(Oid));
        securec_check(rc, "\0", "\0");
        handle->outEnd += sizeof(Oid);

        /* query id */
        rc = memcpy_s(
            handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, query_ids + i, sizeof(uint64));
        securec_check(rc, "\0", "\0");
        handle->outEnd += sizeof(uint64);
    }

    handle->state = DN_CONNECTION_STATE_QUERY;
    ereport(DEBUG1, (errmodule(MOD_INSTR), errmsg("[UniqueSQL] send unique sql IDs to CN/DN, count: %u", count)));

    return pgxc_node_flush(handle) == 0;
}

/*
 * AggUniqueSQLStat - update unique sql's stat from DNs
 *
 * unique_sql_array - preallocated unique sql array
 * msg - msg from other CN/DN
 */
static void AggUniqueSQLStat(UniqueSQL* unique_sql_array, int arr_size, StringInfoData* recv_msg)
{
    if (unique_sql_array == NULL) {
        return;
    }

    uint32 index = (uint32)pq_getmsgint(recv_msg, sizeof(uint32));
    if (index >= (uint32)arr_size) {
        ereport(LOG,
            (errmodule(MOD_INSTR), errmsg("[UniqueSQL] receive stat: index %u >= array size: %d!", index, arr_size)));
        return;
    }

    uint32 recv_cn_id = (uint32)pq_getmsgint(recv_msg, sizeof(uint32));
    Oid recv_user_id = (Oid)pq_getmsgint(recv_msg, sizeof(uint32));
    uint64 recv_unique_sql_id = (uint64)pq_getmsgint64(recv_msg);
    if (unique_sql_array[index].key.cn_id != recv_cn_id || unique_sql_array[index].key.user_id != recv_user_id ||
        unique_sql_array[index].key.unique_sql_id != recv_unique_sql_id) {
        ereport(ERROR, (errmsg("[UniqueSQL] check unique sql array slot index!")));
    }

    // row activity
    unique_sql_array[index].row_activity.returned_rows += (uint64)pq_getmsgint64(recv_msg);
    unique_sql_array[index].row_activity.tuples_fetched += (uint64)pq_getmsgint64(recv_msg);
    unique_sql_array[index].row_activity.tuples_returned += (uint64)pq_getmsgint64(recv_msg);
    unique_sql_array[index].row_activity.tuples_inserted += (uint64)pq_getmsgint64(recv_msg);
    unique_sql_array[index].row_activity.tuples_updated += (uint64)pq_getmsgint64(recv_msg);
    unique_sql_array[index].row_activity.tuples_deleted += (uint64)pq_getmsgint64(recv_msg);

    // cache io
    unique_sql_array[index].cache_io.blocks_fetched += (uint64)pq_getmsgint64(recv_msg);
    unique_sql_array[index].cache_io.blocks_hit += (uint64)pq_getmsgint64(recv_msg);

    // parse info
    unique_sql_array[index].parse.soft_parse += (uint64)pq_getmsgint64(recv_msg);
    unique_sql_array[index].parse.hard_parse += (uint64)pq_getmsgint64(recv_msg);

    // time Info
    for (uint32 idx = 0; idx < TOTAL_TIME_INFO_TYPES; idx++) {
        unique_sql_array[index].timeInfo.TimeInfoArray[idx] += (uint64)pq_getmsgint64(recv_msg);
    }
    // net info
    for (uint32 idx = 0; idx < TOTAL_NET_INFO_TYPES; idx++) {
        unique_sql_array[index].netInfo.netInfoArray[idx] += (uint64)pq_getmsgint64(recv_msg);
    }
    TimestampTz tmp = pq_getmsgint64(recv_msg);
    // updated_time
    if (unique_sql_array[index].updated_time < tmp) {
        unique_sql_array[index].updated_time = tmp;
    }

    // sort&hash info
    unique_sql_array[index].sort_state.counts += (uint64)pq_getmsgint64(recv_msg);
    unique_sql_array[index].sort_state.total_time += (uint64)pq_getmsgint64(recv_msg);
    unique_sql_array[index].sort_state.used_work_mem += (uint64)pq_getmsgint64(recv_msg);
    unique_sql_array[index].sort_state.spill_counts += (uint64)pq_getmsgint64(recv_msg);
    unique_sql_array[index].sort_state.spill_size += (uint64)pq_getmsgint64(recv_msg);

    unique_sql_array[index].hash_state.counts += (uint64)pq_getmsgint64(recv_msg);
    unique_sql_array[index].hash_state.total_time += (uint64)pq_getmsgint64(recv_msg);
    unique_sql_array[index].hash_state.used_work_mem += (uint64)pq_getmsgint64(recv_msg);
    unique_sql_array[index].hash_state.spill_counts += (uint64)pq_getmsgint64(recv_msg);
    unique_sql_array[index].hash_state.spill_size += (uint64)pq_getmsgint64(recv_msg);
}

/*
 *  * @Description: handle response messages from remote(CN/DN)
 *   */
static void handle_message_from_remote(
    UniqueSQL* unique_sql_array, int arr_size, PGXCNodeHandle* handle, bool* hasError, bool* isFinished)
{
    Assert(handle != NULL);
    char* msg = NULL;
    int len = 0;
    char msgType = get_message(handle, &len, &msg);
    switch (msgType) {
        case '\0': /* message is not completed */
        case 'A':  /* NotificationResponse */
        case 'S':  /* SetCommandComplete */
            break;
        case 'E': /* message is error */
            *hasError = true;
            break;
        case 'r': {
            if (len <= 0) {
                ereport(ERROR,
                    (errmodule(MOD_INSTR),
                        errmsg("[UniqueSQL] invalid 'r' message. node: %s", handle->remoteNodeName)));
                break;
            }
            StringInfoData recvMsg;
            initStringInfo(&recvMsg);
            appendBinaryStringInfo(&recvMsg, msg, len);
            AggUniqueSQLStat(unique_sql_array, arr_size, &recvMsg);
            pq_getmsgend(&recvMsg);
            pfree(recvMsg.data);
            break;
        }
        case 'f':
            ereport(DEBUG2,
                (errmodule(MOD_INSTR),
                    errmsg("[UniqueSQL] current CN/DN stat is finished. node: %s", handle->remoteNodeName)));
            *isFinished = true;
            break;
        case 'Z':
            if (*hasError) {
                ereport(LOG,
                    (errmsg("[UniqueSQL] receive stat: get error message. node %s, %s",
                        handle->remoteNodeName,
                        handle->error ? handle->error : "")));
                *isFinished = true;
            }
            break;
        default:
            ereport(LOG,
                (errmodule(MOD_INSTR),
                    errmsg("[UniqueSQL] receive stat: unexpected message type %c, node: %s",
                        msgType,
                        handle->remoteNodeName)));
            *hasError = true;
            *isFinished = true;
            break;
    }
}

/**
 * @Description: get unique sql stat from remote node(CN/DN)
 * @return - true if successful, else false
 */
static bool RecvUniqueSQLStat(UniqueSQL* unique_sql_array, int arr_size, PGXCNodeAllHandles* pgxc_handles, bool is_cn)
{
    ereport(DEBUG1,
        (errmodule(MOD_INSTR),
            errmsg("[UniqueSQL] try to receive unique sql stat from remote :%s", is_cn ? "CN" : "DN")));

    int conn_count;
    PGXCNodeHandle** handles = NULL;

    conn_count = UniqueSq::get_conn_count_from_all_handles(pgxc_handles, is_cn);
    handles = UniqueSq::get_handles_from_all_handles(pgxc_handles, is_cn);
    if (conn_count > 0 && handles == NULL) {
        ereport(
            LOG, (errmodule(MOD_INSTR), errmsg("[UniqueSQL] receive stat: invalid remote connection handler array")));
        return false;
    }

    for (int i = 0; i < conn_count; i++) {
        PGXCNodeHandle* handle = handles[i];

        if (handle == NULL) {
            ereport(LOG, (errmodule(MOD_INSTR), errmsg("[UniqueSQL] receive stat: invalid remote connection handler")));
            return false;
        }

        bool hasError = false;
        bool isFinished = false;
        while (true) {
            if (!pgxc_node_receive(1, &handle, NULL)) {
                handle_message_from_remote(unique_sql_array, arr_size, handle, &hasError, &isFinished);
                if (isFinished || hasError) {
                    break;
                }
            } else {
                hasError = true;
                break;
            }
        }
        handle->state = DN_CONNECTION_STATE_IDLE;

        if (hasError) {
            ereport(LOG,
                (errmodule(MOD_INSTR),
                    errmsg("[UniqueSQL] receive stat: fetch failed, node: %s", handle->remoteNodeName)));
            return false;
        }
    }
    return true;
}

/**
 * @Description: wrapper method to get CN/DN count from PGXCNodeAllHandles
 */
int UniqueSq::get_conn_count_from_all_handles(PGXCNodeAllHandles* pgxc_handles, bool is_cn)
{
    if (pgxc_handles != NULL) {
        return is_cn ? pgxc_handles->co_conn_count : pgxc_handles->dn_conn_count;
    }
    return 0;
}

/**
 * @Description: wrapper method to get CN/DN handle from PGXCNodeAllHandles
 */
PGXCNodeHandle** UniqueSq::get_handles_from_all_handles(PGXCNodeAllHandles* pgxc_handles, bool is_cn)
{
    if (pgxc_handles != NULL) {
        return is_cn ? pgxc_handles->coord_handles : pgxc_handles->datanode_handles;
    }
    return NULL;
}

/**
 * @Description: send unique sql keys to remote CN/DNs
 * @return - return true if send keys to remote node successfully
 */
static bool SendUniqueSQLIDsToRemote(PGXCNodeAllHandles* pgxc_handles, uint32* cn_ids, Oid* user_ids, uint64* query_ids,
    uint32* slot_indexes, uint count, bool is_cn)
{
    int conn_count;
    PGXCNodeHandle** handles = NULL;

    conn_count = UniqueSq::get_conn_count_from_all_handles(pgxc_handles, is_cn);
    handles = UniqueSq::get_handles_from_all_handles(pgxc_handles, is_cn);
    if (conn_count > 0 && handles == NULL) {
        ereport(LOG, (errmodule(MOD_INSTR), errmsg("[UniqueSQL] send key: invalid remote connection handler array")));
        return false;
    }

    for (int i = 0; i < conn_count; i++) {
        PGXCNodeHandle* handle = handles[i];
        if (handle == NULL) {
            ereport(LOG, (errmodule(MOD_INSTR), errmsg("[UniqueSQL] send key: invalid remote connection handler")));
            return false;
        }

        if (handle->state == DN_CONNECTION_STATE_QUERY) {
            BufferConnection(handle);
        }

        handle->state = DN_CONNECTION_STATE_IDLE;
        if (!SendUniqueSQLIds(handle, cn_ids, user_ids, query_ids, slot_indexes, count)) {
            /* connection issue */
            ereport(
                LOG, (errmodule(MOD_INSTR), errmsg("[UniqueSQL] send key: failed,  node: %s", handle->remoteNodeName)));
            return false;
        }
    }

    return true;
}

/*
 * @Description: get unique sql stat from CNs/DNs
 *   including:
 *     - Cache/IO
 *     - Row activities
 *     - etc
 *
 * @return - return false if failed
 */
static bool GetUniqueSQLStatFromRemote(UniqueSQL* unique_sql_array, int arr_size, PGXCNodeAllHandles* pgxc_handles,
    uint32* cn_ids, Oid* user_ids, uint64* query_ids, uint32* slot_indexes, uint count)
{
    Assert(pgxc_handles);
    Assert(unique_sql_array);

    /* 1, send all unique sql ids to CN/DN */
    if (!SendUniqueSQLIDsToRemote(pgxc_handles, cn_ids, user_ids, query_ids, slot_indexes, count, true) ||
        !SendUniqueSQLIDsToRemote(pgxc_handles, cn_ids, user_ids, query_ids, slot_indexes, count, false)) {
        return false;
    }

    /* 2, receive cn/dn's result, append the result to unique sql entry */
    if (RecvUniqueSQLStat(unique_sql_array, arr_size, pgxc_handles, true) &&
        RecvUniqueSQLStat(unique_sql_array, arr_size, pgxc_handles, false)) {
        return true;
    }
    return false;
}

/**
 * @Description: copy unique sql entry from hash table to pre-allocated buffer
 * @return - void
 */
static void copy_unique_sql_entry(UniqueSQL* unique_sql_array, int num, int i, UniqueSQL* entry)
{
    errno_t rc = EOK;

    unique_sql_array[i].key.cn_id = entry->key.cn_id;
    unique_sql_array[i].key.user_id = entry->key.user_id;
    unique_sql_array[i].key.unique_sql_id = entry->key.unique_sql_id;

    unique_sql_array[i].calls = entry->calls;

    if (need_normalize_unique_string() && entry->unique_sql != NULL) {
        unique_sql_array[i].unique_sql = (char*)(unique_sql_array + num) + i * UNIQUE_SQL_MAX_LEN;
        rc = memcpy_s(unique_sql_array[i].unique_sql, UNIQUE_SQL_MAX_LEN, entry->unique_sql, strlen(entry->unique_sql));
        securec_check(rc, "\0", "\0");
    } else {
        unique_sql_array[i].unique_sql = NULL;
    }

    // elapse time
    unique_sql_array[i].elapse_time.total_time = entry->elapse_time.total_time;
    unique_sql_array[i].elapse_time.min_time = entry->elapse_time.min_time;
    unique_sql_array[i].elapse_time.max_time = entry->elapse_time.max_time;
    // row activity
    unique_sql_array[i].row_activity.returned_rows = entry->row_activity.returned_rows;
    unique_sql_array[i].row_activity.tuples_fetched = entry->row_activity.tuples_fetched;
    unique_sql_array[i].row_activity.tuples_returned = entry->row_activity.tuples_returned;
    unique_sql_array[i].row_activity.tuples_inserted = entry->row_activity.tuples_inserted;
    unique_sql_array[i].row_activity.tuples_updated = entry->row_activity.tuples_updated;
    unique_sql_array[i].row_activity.tuples_deleted = entry->row_activity.tuples_deleted;

    // Cache/IO
    unique_sql_array[i].cache_io.blocks_fetched = entry->cache_io.blocks_fetched;
    unique_sql_array[i].cache_io.blocks_hit = entry->cache_io.blocks_hit;

    // parse info
    unique_sql_array[i].parse.soft_parse = entry->parse.soft_parse;
    unique_sql_array[i].parse.hard_parse = entry->parse.hard_parse;

    // sort&hash work mem info
    unique_sql_array[i].sort_state.counts = entry->sort_state.counts;
    unique_sql_array[i].sort_state.total_time = entry->sort_state.total_time;
    unique_sql_array[i].sort_state.used_work_mem = entry->sort_state.used_work_mem;
    unique_sql_array[i].sort_state.spill_counts = entry->sort_state.spill_counts;
    unique_sql_array[i].sort_state.spill_size = entry->sort_state.spill_size;

    unique_sql_array[i].hash_state.counts = entry->hash_state.counts;
    unique_sql_array[i].hash_state.total_time = entry->hash_state.total_time;
    unique_sql_array[i].hash_state.used_work_mem = entry->hash_state.used_work_mem;
    unique_sql_array[i].hash_state.spill_counts = entry->hash_state.spill_counts;
    unique_sql_array[i].hash_state.spill_size = entry->hash_state.spill_size;

    // time Info
    rc = memcpy_s(&unique_sql_array[i].timeInfo, sizeof(UniqueSQLTime), &entry->timeInfo, sizeof(UniqueSQLTime));
    securec_check(rc, "\0", "\0");

    // net info
    rc = memcpy_s(&unique_sql_array[i].netInfo, sizeof(UniqueSQLNetInfo), &entry->netInfo, sizeof(UniqueSQLNetInfo));
    securec_check(rc, "\0", "\0");

    /* if is local, will fetch more information from CN/DNs */
    unique_sql_array[i].is_local = entry->is_local;
    unique_sql_array[i].updated_time = entry->updated_time;
}

/**
 * @Description: send unique sql key to remote node
 * @return - false if failed
 */
static bool package_and_send_unqiue_sql_key_to_remote(
    UniqueSQL* unique_sql_array, int num, PGXCNodeAllHandles* pgxc_handles)
{
    /* key = CN id + user id + query id */
    uint32 cn_ids[UNIQUE_SQL_IDS_ARRAY_SIZE] = {0};
    Oid user_ids[UNIQUE_SQL_IDS_ARRAY_SIZE] = {0};
    uint64 query_ids[UNIQUE_SQL_IDS_ARRAY_SIZE] = {0};
    uint32 slot_indexes[UNIQUE_SQL_IDS_ARRAY_SIZE] = {0};

    /* only send unique sql id to remote CN/DN if entry is local */
    int array_idx = 0;
    int rc = 0;
    const int N32_BIT = 32;

    for (int i = 0; i < num; i++) {
        if (!unique_sql_array[i].is_local) {
            continue;
        }

        int index = array_idx++;
        index = index % UNIQUE_SQL_IDS_ARRAY_SIZE;

        uint32 n32 = 0;
        cn_ids[index] = htonl(unique_sql_array[i].key.cn_id);

        /* -- unique sql id -- */
        /* high order half */
        n32 = (uint32)(unique_sql_array[i].key.unique_sql_id >> N32_BIT);
        n32 = htonl(n32);
        rc = memcpy_s(query_ids + index, sizeof(uint32), &n32, sizeof(uint32));
        securec_check(rc, "\0", "\0");

        /* low order half */
        n32 = (uint32)unique_sql_array[i].key.unique_sql_id;
        n32 = htonl(n32);
        rc = memcpy_s((unsigned char*)(query_ids + index) + sizeof(uint32), sizeof(uint32), &n32, sizeof(uint32));
        securec_check(rc, "\0", "\0");

        user_ids[index] = htonl(unique_sql_array[i].key.user_id);

        /* store original array index */
        slot_indexes[index] = htonl(i);

        /* each time send 10 unique sql ids or
         * at the end of unique_sql_array
         */
        if ((index == UNIQUE_SQL_IDS_ARRAY_SIZE - 1)) {
            if (!GetUniqueSQLStatFromRemote(unique_sql_array,
                    num,
                    pgxc_handles,
                    cn_ids,
                    user_ids,
                    query_ids,
                    slot_indexes,
                    UNIQUE_SQL_IDS_ARRAY_SIZE)) {
                return false;
            }
        } else if (i == num - 1) {
            if (!GetUniqueSQLStatFromRemote(
                    unique_sql_array, num, pgxc_handles, cn_ids, user_ids, query_ids, slot_indexes, index + 1)) {
                return false;
            }
        }
    }
    return true;
}

static int do_copy_entry(const List *unique_sql_batch_list, int num)
{
    UniqueSQL *unique_sql_array = NULL;
    HASH_SEQ_STATUS hash_seq;
    ListCell *tmp_unique_sql_cell = NULL; 
    UniqueSQL* entry = NULL;
    int i = 0;

    hash_seq_init(&hash_seq, g_instance.stat_cxt.UniqueSQLHashtbl);
    while ((entry = (UniqueSQL*)hash_seq_search(&hash_seq)) != NULL && i < num) {
        /* DN - copy all elements; CN - only copy local unique sql */
        if (IS_PGXC_DATANODE || (IS_PGXC_COORDINATOR && entry->is_local)) {
            int curr_idx = i % (int)MAX_MEM_UNIQUE_SQL_ENTRY_COUNT;
            if (curr_idx == 0) {
                if (i == 0) {
                    tmp_unique_sql_cell = list_head(unique_sql_batch_list);
                } else {
                    tmp_unique_sql_cell = lnext(tmp_unique_sql_cell);
                }

                unique_sql_array = (UniqueSQL *)lfirst(tmp_unique_sql_cell); 
            }

            copy_unique_sql_entry(unique_sql_array, (int)MAX_MEM_UNIQUE_SQL_ENTRY_COUNT, curr_idx, entry);
            i++;
        }
    }

    return i;
}

static UniqueSQLResults *build_unique_sql_batch_list(List *unique_sql_batch_list)
{
    UniqueSQLResults *results = (UniqueSQLResults *)palloc0_noexcept(sizeof(UniqueSQLResults));
    if (results == NULL) {
        if (unique_sql_batch_list != NIL)
            list_free_deep(unique_sql_batch_list);
        ereport(ERROR, (errmodule(MOD_INSTR), errmsg("[UniqueSQL] palloc failed for results!")));
    }
    results->batch_list = unique_sql_batch_list;
    results->curr_cell = NULL;

    return results;
}

/**
 * @Description: get unique sql stat from CNs and DNs
 * @in unique_sql_array - local pre-allocated unique sql array
 * @in num - unique sql entry counts on local CN
 * @return - true if success, else will be false
 */
static bool get_unique_sql_stat_from_remote(const List* unique_sql_batch_list, int num)
{
    /*
     * get row activity and cache/io from DN nodes,
     *   1, each time send 10 unique sql ids to DN;
     *   2, DN replies the unique sql's cache/IO and
     *    row activities
     * get all dn connection
     */
    List* cn_list = GetAllCoordNodes();
    PGXCNodeAllHandles* pgxc_handles = NULL;

    PG_TRY();
    {
        pgxc_handles = get_handles(NULL, cn_list, false);
    }
    PG_CATCH();
    {
        release_pgxc_handles(pgxc_handles);
        list_free_ext(cn_list);

        PG_RE_THROW();
    }
    PG_END_TRY();
    Assert(pgxc_handles != NULL);

    int unique_sql_count = 0;
    UniqueSQL* unique_sql_array = NULL;
    ListCell *batch_cell = NULL;
    int batch_count = (int)ceil(num / MAX_MEM_UNIQUE_SQL_ENTRY_COUNT);
    for (int i = 0; i < batch_count; i++) {
        if (i == 0)
            batch_cell = list_head(unique_sql_batch_list);
        else
            batch_cell = lnext(batch_cell);
        unique_sql_array = (UniqueSQL *)lfirst(batch_cell);

        if ((i == (batch_count - 1)) &&
            (num % (int)MAX_MEM_UNIQUE_SQL_ENTRY_COUNT != 0)) {
            unique_sql_count = num % (int)MAX_MEM_UNIQUE_SQL_ENTRY_COUNT;
        } else {
            unique_sql_count = (int)MAX_MEM_UNIQUE_SQL_ENTRY_COUNT;
        }

        if (!package_and_send_unqiue_sql_key_to_remote(unique_sql_array, unique_sql_count, pgxc_handles)) {
            release_pgxc_handles(pgxc_handles);
            list_free_ext(cn_list);

            ereport(ERROR,
                (errmodule(MOD_INSTR), errmsg("[UniqueSQL] during get stat from remote, failed to send/recv data!")));

            return false;
        }
    }

    /* free connection */
    release_pgxc_handles(pgxc_handles);
    list_free_ext(cn_list);

    return true;
}

static void log_unique_sql_result_mem(uint64 total_size, uint64 malloc_size, uint64 index)
{
    if (total_size > 1 * 1024 * 1024 * 1024) {
        ereport(LOG, (errmodule(MOD_INSTR), errmsg("[UniqueSQL] idx[%lu] - new memory allocated: %lu",
            index, malloc_size)));
        ereport(LOG, (errmodule(MOD_INSTR), errmsg("[UniqueSQL] total memory allocated: %lu", total_size)));
    }
}

/*
 * @Description: get unique sql's stat, and save them to pre-allocated array
 * @out num - valid unique sql entry count
 * @return - the pre-allocated buffer
 */
void* GetUniqueSQLStat(long* num)
{
    // must enable pgstat_track_counts to track row activity
    if (!is_unique_sql_enabled()) {
        ereport(LOG,
            (errmsg("[UniqueSQL] GUC parameter 'enable_resource_track' "
                    "or 'instr_unique_sql_count' is zero, unique sql view will be empty!")));
        *num = 0;
        return NULL;
    }
    if (g_instance.stat_cxt.UniqueSQLHashtbl == NULL) {
        *num = 0;
        return NULL;
    }
    int i = 0;
    uint64 total_size = 0;
    UniqueSQL* unique_sql_array = NULL;
    List* unique_sql_batch_list = NIL;

    for (i = 0; i < NUM_UNIQUE_SQL_PARTITIONS; i++) {
        LWLockAcquire(GetMainLWLockByIndex(FirstUniqueSQLMappingLock + i), LW_SHARED);
    }

    *num = hash_get_num_entries(g_instance.stat_cxt.UniqueSQLHashtbl);
    if (*num > 0) {
        int unique_sql_str_len = (IS_PGXC_COORDINATOR || IS_SINGLE_NODE) ? UNIQUE_SQL_MAX_LEN : 0;
        for (int j = 0; j < ceil(*num / MAX_MEM_UNIQUE_SQL_ENTRY_COUNT); j++) {
            /* memory format: [entry_1] [entry_2] [entry_3] ... | [sql_1] [sql_2] [sql_3] ... */
            int malloc_size = (int)MAX_MEM_UNIQUE_SQL_ENTRY_COUNT * (sizeof(UniqueSQL) + unique_sql_str_len);
            unique_sql_array = (UniqueSQL*)palloc0_noexcept(malloc_size);
            if (unique_sql_array == NULL) {
                for (i = 0; i < NUM_UNIQUE_SQL_PARTITIONS; i++) {
                    LWLockRelease(GetMainLWLockByIndex(FirstUniqueSQLMappingLock + i));
                }

                if (unique_sql_batch_list != NIL)
                    list_free_deep(unique_sql_batch_list);
                ereport(ERROR, (errmsg("[UniqueSQL] palloc0 error when querying unique sql stat!")));
            }
            total_size += malloc_size;
            log_unique_sql_result_mem(total_size, malloc_size, j);

            unique_sql_batch_list = lappend(unique_sql_batch_list, unique_sql_array);
        }

        /* copy entries to preallocated memory on source CN/DN(which accepts DBE_PERF.statement) */
        *num = do_copy_entry(unique_sql_batch_list, *num);
    }

    for (i = 0; i < NUM_UNIQUE_SQL_PARTITIONS; i++) {
        LWLockRelease(GetMainLWLockByIndex(FirstUniqueSQLMappingLock + i));
    }

    if (*num == 0) {
        return NULL;
    }

    UniqueSQLResults *results = build_unique_sql_batch_list(unique_sql_batch_list);
    if (IS_PGXC_DATANODE)
        return results;

    if (!get_unique_sql_stat_from_remote(unique_sql_batch_list, *num)) {
        *num = 0;
    }
    return results;
}

static void create_tuple_entry(TupleDesc tupdesc)
{
    int num = 0;
    int i = 0;

    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "node_name", NAMEOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "node_id", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "user_name", NAMEOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "user_id", OIDOID, -1, 0);

    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "unique_sql_id", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "query", TEXTOID, -1, 0);

    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "n_calls", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "min_elapse_time", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "max_elapse_time", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "total_elapse_time", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "n_returned_rows", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "n_tuples_fetched", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "n_tuples_returned", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "n_tuples_inserted", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "n_tuples_updated", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "n_tuples_deleted", INT8OID, -1, 0);

    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "n_blocks_fetched", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "n_blocks_hit", INT8OID, -1, 0);

    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "n_soft_parse", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "n_hard_parse", INT8OID, -1, 0);

    for (num = 0; num < TOTAL_TIME_INFO_TYPES; num++) {
        if (num == NET_SEND_TIME)
            continue;
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, TimeInfoTypeName[num], INT8OID, -1, 0);
    }
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "NET_SEND_INFO", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "NET_RECV_INFO", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "NET_STREAM_SEND_INFO", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "NET_STREAM_RECV_INFO", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "last_updated", TIMESTAMPTZOID, -1, 0);

    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "sort_count", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "sort_time", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "sort_mem_used", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "sort_spill_count", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "sort_spill_size", INT8OID, -1, 0);

    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "hash_count", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "hash_time", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "hash_mem_used", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "hash_spill_count", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "hash_spill_size", INT8OID, -1, 0);
}

static void set_tuple_cn_node_name(UniqueSQL* unique_sql, Datum* values, int* i)
{
    // cn node name
    if (IS_PGXC_COORDINATOR || IS_SINGLE_NODE) {
        char* node_name = get_pgxc_node_name_by_node_id(unique_sql->key.cn_id, false);
        if (node_name != NULL) {
            values[(*i)++] = DirectFunctionCall1(namein, CStringGetDatum(node_name));
            pfree(node_name);
        } else {
#ifdef ENABLE_MULTIPLE_NODES
            values[(*i)++] = DirectFunctionCall1(namein, CStringGetDatum("*REMOVED_NODE*"));
#else
            values[(*i)++] = DirectFunctionCall1(namein, CStringGetDatum(g_instance.attr.attr_common.PGXCNodeName));
#endif
        }
    } else {
        values[(*i)++] = DirectFunctionCall1(namein, CStringGetDatum(""));
    }
}

static void set_tuple_user_name(UniqueSQL* unique_sql, Datum* values, int* i)
{
    char user_name[NAMEDATALEN] = {0};

    // user name
    if (IS_PGXC_COORDINATOR || IS_SINGLE_NODE) {
        if (GetRoleName(unique_sql->key.user_id, user_name, sizeof(user_name)) != NULL) {
            values[(*i)++] = DirectFunctionCall1(namein, CStringGetDatum(user_name));
        } else {
            values[(*i)++] = DirectFunctionCall1(namein, CStringGetDatum("*REMOVED_USER*"));
        }
    } else {
        values[(*i)++] = DirectFunctionCall1(namein, CStringGetDatum(""));
    }
}

static void set_tuple_unique_sql(UniqueSQL* unique_sql, Datum* values, int arr_size, int* i)
{
    if (*i >= arr_size) {
        return;
    }
    if (unique_sql->unique_sql != NULL) {
        values[(*i)++] = CStringGetTextDatum(unique_sql->unique_sql);
    } else {
        values[(*i)++] = CStringGetTextDatum("");
    }
}

static void set_tuple_value(UniqueSQL* unique_sql, Datum* values, bool* nulls, int arr_size)
{
    int i = 0;
    int num = 0;

    set_tuple_cn_node_name(unique_sql, values, &i);
    values[i++] = UInt32GetDatum(unique_sql->key.cn_id);
    set_tuple_user_name(unique_sql, values, &i);

    // basic info
    values[i++] = ObjectIdGetDatum(unique_sql->key.user_id);
    values[i++] = Int64GetDatum(unique_sql->key.unique_sql_id);

    set_tuple_unique_sql(unique_sql, values, arr_size, &i);
    values[i++] = Int64GetDatum(unique_sql->calls);

    // response time
    values[i++] = Int64GetDatum(unique_sql->elapse_time.min_time);
    values[i++] = Int64GetDatum(unique_sql->elapse_time.max_time);
    values[i++] = Int64GetDatum(unique_sql->elapse_time.total_time);
    // row activity
    values[i++] = Int64GetDatum(unique_sql->row_activity.returned_rows);
    values[i++] = Int64GetDatum(unique_sql->row_activity.tuples_fetched);
    values[i++] = Int64GetDatum(unique_sql->row_activity.tuples_returned);
    values[i++] = Int64GetDatum(unique_sql->row_activity.tuples_inserted);
    values[i++] = Int64GetDatum(unique_sql->row_activity.tuples_updated);
    values[i++] = Int64GetDatum(unique_sql->row_activity.tuples_deleted);

    // cache/IO
    values[i++] = Int64GetDatum(unique_sql->cache_io.blocks_fetched);
    values[i++] = Int64GetDatum(unique_sql->cache_io.blocks_hit);

    // parse Info
    values[i++] = Int64GetDatum(unique_sql->parse.soft_parse);
    values[i++] = Int64GetDatum(unique_sql->parse.hard_parse);

    // time Info
    for (num = 0; num < TOTAL_TIME_INFO_TYPES; num++) {
        if (num == NET_SEND_TIME)
            continue;
        values[i++] = Int64GetDatum(unique_sql->timeInfo.TimeInfoArray[num]);
    }
    int idx = 0;
    while (idx < TOTAL_NET_INFO_TYPES) {
        char netInfo[STRING_MAX_LEN];
        uint64 time = unique_sql->netInfo.netInfoArray[idx++];
        uint64 n_calls = unique_sql->netInfo.netInfoArray[idx++];
        uint64 size = unique_sql->netInfo.netInfoArray[idx++];
        errno_t rc = sprintf_s(netInfo, sizeof(netInfo), "{\"time\":%lu, \"n_calls\":%lu, \"size\":%lu}",
            time, n_calls, size);
        securec_check_ss(rc, "\0", "\0");
        values[i++] = CStringGetTextDatum(netInfo);
    }
    // updated_time
    values[i++] = TimestampTzGetDatum(unique_sql->updated_time);

    // sort&hash work mem Info
    values[i++] = Int64GetDatum(unique_sql->sort_state.counts);
    values[i++] = Int64GetDatum(unique_sql->sort_state.total_time);
    values[i++] = Int64GetDatum(unique_sql->sort_state.used_work_mem);
    values[i++] = Int64GetDatum(unique_sql->sort_state.spill_counts);
    values[i++] = Int64GetDatum(unique_sql->sort_state.spill_size);

    values[i++] = Int64GetDatum(unique_sql->hash_state.counts);
    values[i++] = Int64GetDatum(unique_sql->hash_state.total_time);
    values[i++] = Int64GetDatum(unique_sql->hash_state.used_work_mem);
    values[i++] = Int64GetDatum(unique_sql->hash_state.spill_counts);
    values[i++] = Int64GetDatum(unique_sql->hash_state.spill_size);

    Assert(arr_size == i);
}

void set_current_batch_list_cell(FuncCallContext* funcctx)
{
    if (funcctx->call_cntr % (int)MAX_MEM_UNIQUE_SQL_ENTRY_COUNT == 0) {
        if (funcctx->call_cntr == 0) {
            ((UniqueSQLResults *)(funcctx->user_fctx))->curr_cell =
                list_head((List *)(((UniqueSQLResults *)(funcctx->user_fctx))->batch_list));
        } else {
            ((UniqueSQLResults *)(funcctx->user_fctx))->curr_cell =
                lnext(((UniqueSQLResults *)(funcctx->user_fctx))->curr_cell);
        }
    }
}

static void check_unique_sql_permission()
{
    if (!superuser() && !isMonitoradmin(GetUserId())) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            (errmsg("only system/monitor admin can query unique sql view"))));
    }
}
static void CheckVersion()
{
    if (t_thrd.proc->workingVersionNum < STATEMENT_TRACK_VERSION) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            (errmsg("This view cannot be select during upgrade"))));
    }
}
/*
 * get_instr_unique_sql - C function to get unique sql stat info
 */
Datum get_instr_unique_sql(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;
    long num = 0;
#define INSTRUMENTS_UNIQUE_SQL_ATTRNUM (35 + TOTAL_TIME_INFO_TYPES - 1)
    CheckVersion();
    check_unique_sql_permission();

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext = NULL;
        TupleDesc tupdesc = NULL;

        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(INSTRUMENTS_UNIQUE_SQL_ATTRNUM, false, TAM_HEAP);
        create_tuple_entry(tupdesc);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->user_fctx = GetUniqueSQLStat(&num);
        funcctx->max_calls = num;
        MemoryContextSwitchTo(oldcontext);

        if (funcctx->max_calls == 0) {
            if (funcctx->user_fctx) {
                pfree_ext(funcctx->user_fctx);
            }
            SRF_RETURN_DONE(funcctx);
        }
    }

    funcctx = SRF_PERCALL_SETUP();
    if (funcctx->call_cntr < funcctx->max_calls) {
        Datum values[INSTRUMENTS_UNIQUE_SQL_ATTRNUM];
        bool nulls[INSTRUMENTS_UNIQUE_SQL_ATTRNUM] = {false};
        HeapTuple tuple = NULL;
        int rc = 0;

        set_current_batch_list_cell(funcctx);
        UniqueSQL* batch_unique_sql = (UniqueSQL *)lfirst(((UniqueSQLResults *)(funcctx->user_fctx))->curr_cell);
        UniqueSQL* unique_sql = batch_unique_sql + funcctx->call_cntr % (int)MAX_MEM_UNIQUE_SQL_ENTRY_COUNT;

        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        set_tuple_value(unique_sql, values, nulls, INSTRUMENTS_UNIQUE_SQL_ATTRNUM);
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    } else {
        if (funcctx->user_fctx) {
            list_free_deep(((UniqueSQLResults *)(funcctx->user_fctx))->batch_list);
            pfree_ext(funcctx->user_fctx);
        }
        SRF_RETURN_DONE(funcctx);
    }
}

bool CheckSkipSQL(Query* query)
{
    return query->utilityStmt != NULL && (IsA(query->utilityStmt, ExplainStmt) ||
        (IsA(query->utilityStmt, RemoteQuery) &&
        (((RemoteQuery*)query->utilityStmt)->exec_direct_type != EXEC_DIRECT_NONE)));
}
/*
 * GenerateUniqueSQLInfo - generate unique sql info
 *
 * such as unique query id/normalized unique sql string
 */
void GenerateUniqueSQLInfo(const char* sql, Query* query)
{
    /* won't record unique sql info during abort transaction stat,
     *
     * below scenario also won't record unique sql stat:
     * - begin;
     * - select * from not_exist_relation;(cause abort stat)
     * - rollback;
     *
     * rollback is run successfully, but also will ignore it, as now
     * we have limitation, in abort stat, can't access system relation.
     * refer to the assert in method "relation_open"
     */
    if (sql == NULL || query == NULL || g_instance.stat_cxt.UniqueSQLHashtbl == NULL || !is_local_unique_sql() ||
        IsAbortedTransactionBlockState() || CheckSkipSQL(query) ||
        u_sess->unique_sql_cxt.skipUniqueSQLCount != 0) {
        return;
    }

    if (IS_UNIQUE_SQL_TRACK_TOP && IsTopUniqueSQL()) {
        ereport(DEBUG1,
            (errmodule(MOD_INSTR),
                errmsg("[UniqueSQL] unique id: %lu, already has top SQL", u_sess->unique_sql_cxt.unique_sql_id)));
        return;
    }

    const char* current_sql = NULL;
    if (u_sess->unique_sql_cxt.is_multi_unique_sql) {
        current_sql = u_sess->unique_sql_cxt.curr_single_unique_sql;
    } else {
        current_sql = sql;
    }

    if (current_sql == NULL) {
        return;
    }

    if (!need_reuse_unique_sql_id(query)) {
        u_sess->unique_sql_cxt.unique_sql_id = generate_unique_queryid(query, current_sql);
    }

    query->uniqueSQLId = u_sess->unique_sql_cxt.unique_sql_id;
    u_sess->slow_query_cxt.slow_query.unique_sql_id = u_sess->unique_sql_cxt.unique_sql_id;

    /* dynamic enable statement tracking */
    instr_stmt_dynamic_change_level();

    if (!OidIsValid(u_sess->unique_sql_cxt.unique_sql_cn_id)) {
        Oid node_oid = get_pgxc_nodeoid(g_instance.attr.attr_common.PGXCNodeName);
        u_sess->unique_sql_cxt.unique_sql_cn_id = get_pgxc_node_id(node_oid);
        if (u_sess->globalSessionId.sessionId) {
            u_sess->globalSessionId.nodeId = u_sess->unique_sql_cxt.unique_sql_cn_id;
            pgstat_report_global_session_id(u_sess->globalSessionId);
        }
    }

    u_sess->unique_sql_cxt.unique_sql_user_id = GetUserId();
    ereport(DEBUG1, (errmodule(MOD_INSTR), errmsg("[UniqueSQL] generate unique id: %lu, cn id: %u, user id: %u",
        u_sess->unique_sql_cxt.unique_sql_id, u_sess->unique_sql_cxt.unique_sql_cn_id,
        u_sess->unique_sql_cxt.unique_sql_user_id)));

    UpdateUniqueSQLStat(query, current_sql, 0);
    instr_stmt_report_query(u_sess->unique_sql_cxt.unique_sql_id);
    pgstat_report_unique_sql_id(false);

    /* if track top enabled, only TOP SQL will generate unique sql id */
    if (IS_UNIQUE_SQL_TRACK_TOP) {
        SetIsTopUniqueSQL(true);
    }
}


/*
 * unique_sql_post_parse_analyze - generate sql id
 */
void UniqueSq::unique_sql_post_parse_analyze(ParseState* pstate, Query* query)
{
    Assert(IS_PGXC_COORDINATOR || IS_SINGLE_NODE);

    if (pstate != NULL) {
        /* generate unique sql id */        
        if (is_unique_sql_enabled()) {
            GenerateUniqueSQLInfo(pstate->p_sourcetext, query);
        }
    }

    if (t_thrd.statement_cxt.instr_prev_post_parse_analyze_hook != NULL) {
        ((post_parse_analyze_hook_type)(t_thrd.statement_cxt.instr_prev_post_parse_analyze_hook))(pstate, query);
    }
}

/*
 * get Query from query_list, and then set unique sql id to session
 */
static void SetLocalUniqueSQLId(List* query_list)
{
    if (query_list == NULL || IsAbortedTransactionBlockState()) {
        return;
    }

    Query* query = NULL;
    if (list_length(query_list) > 0) {
        if (IsA(linitial(query_list), Query)) {
            query = (Query*)linitial(query_list);
            if (query != NULL) {
                u_sess->unique_sql_cxt.unique_sql_id = query->uniqueSQLId;
                u_sess->slow_query_cxt.slow_query.unique_sql_id = u_sess->unique_sql_cxt.unique_sql_id;
                ereport(DEBUG1,
                    (errmodule(MOD_INSTR),
                        errmsg("SetLocalUniqueSQLId %s to %lu", query->sql_statement, u_sess->unique_sql_cxt.unique_sql_id)));
                if (!OidIsValid(u_sess->unique_sql_cxt.unique_sql_cn_id)) {
                    Oid node_oid = get_pgxc_nodeoid(g_instance.attr.attr_common.PGXCNodeName);
                    u_sess->unique_sql_cxt.unique_sql_cn_id = get_pgxc_node_id(node_oid);
                    u_sess->globalSessionId.nodeId = u_sess->unique_sql_cxt.unique_sql_cn_id;
                    pgstat_report_global_session_id(u_sess->globalSessionId);
                }

                u_sess->unique_sql_cxt.unique_sql_user_id = GetUserId();
#ifndef ENABLE_MULTIPLE_NODES
                /* store the normalized uniquesq text into u_sess->Unique_sql_cxt in stage B
                 * or E of PBE, only if auto-cleanup is enabled
                 */
                if (g_instance.attr.attr_common.enable_auto_clean_unique_sql) {
                    u_sess->unique_sql_cxt.unique_sql_text = query->unique_sql_text;
                }
#endif
                instr_stmt_report_query(u_sess->unique_sql_cxt.unique_sql_id);
                pgstat_report_unique_sql_id(false);

                /* dynamic enable statement tracking */
                instr_stmt_dynamic_change_level();

                /* for BE message, only can have one SQL each time,
                 * so set top to true, then n_calls can be Updated
                 * in PortalRun
                 */
                SetIsTopUniqueSQL(true);
            }
        }
    }
}

/* Set parameters from portal */
void SetParamsFromPortal(Portal portal)
{
    CHECK_STMT_HANDLE();
    Assert(portal);

    if (!u_sess->attr.attr_common.track_stmt_parameter || CURRENT_STMT_METRIC_HANDLE->params) {
        return;
    }
    
    ParamListInfo params = portal->portalParams;
    /* We mustn't call user-defined I/O functions when in an aborted xact */
    if (params && params->numParams > 0 && !IsAbortedTransactionBlockState()) {
        StringInfoData param_str;
        MemoryContext oldcontext;
        int paramno;

        initStringInfo(&param_str);

        for (paramno = 0; paramno < params->numParams; paramno++) {
            ParamExternData* prm = &params->params[paramno];
            Oid typoutput;
            bool typisvarlena = false;
            char* pstring = NULL;
            char* p = NULL;

            appendStringInfo(&param_str, "%s$%d = ", (paramno > 0) ? ", " : " parameters: ", paramno + 1);

            if (prm->isnull || !OidIsValid(prm->ptype)) {
                appendStringInfoString(&param_str, "NULL");
                continue;
            }

            getTypeOutputInfo(prm->ptype, &typoutput, &typisvarlena);

            pstring = OidOutputFunctionCall(typoutput, prm->value);

            appendStringInfoCharMacro(&param_str, '\'');
            for (p = pstring; *p; p++) {
                if (*p == '\'') /* double single quotes */
                    appendStringInfoCharMacro(&param_str, *p);
                appendStringInfoCharMacro(&param_str, *p);
            }
            appendStringInfoCharMacro(&param_str, '\'');

            pfree(pstring);
        }

        oldcontext = MemoryContextSwitchTo(u_sess->statement_cxt.stmt_stat_cxt);

        CURRENT_STMT_METRIC_HANDLE->params = pstrdup(param_str.data);

        pfree(param_str.data);

        if (CURRENT_STMT_METRIC_HANDLE->query) {
            Size len = strlen(CURRENT_STMT_METRIC_HANDLE->query) + strlen(CURRENT_STMT_METRIC_HANDLE->params) + 2;
            char *tmpstr = (char *)palloc(len * sizeof(char));
            errno_t rc = sprintf_s(tmpstr, len, "%s;%s", CURRENT_STMT_METRIC_HANDLE->query,
                                   CURRENT_STMT_METRIC_HANDLE->params);
            securec_check_ss_c(rc, "\0", "\0");
            pfree(CURRENT_STMT_METRIC_HANDLE->query);
            pfree_ext(CURRENT_STMT_METRIC_HANDLE->params);
            CURRENT_STMT_METRIC_HANDLE->query = tmpstr;
        }


        MemoryContextSwitchTo(oldcontext);
    }
}

/*
 * set current prepared statement's unique sql id
 *
 * 1, get portal by name
 * 2, fetch prepared statement from local prepared statement cache
 * 3, get prepared statement's query list
 * 4, for utility stmt:
 *     only should has one Query in list(ToDo: need confirm)
 * 5, for other stmt:
 *     all Query's unique sql id should be same
 */
void SetUniqueSQLIdFromPortal(Portal portal, CachedPlanSource* unnamed_psrc)
{
    if (!is_unique_sql_enabled() || !is_local_unique_sql() || portal == NULL) {
        return;
    }

    SetParamsFromPortal(portal);

    List* query_list = NULL;
    if (portal->prepStmtName && portal->prepStmtName[0] != '\0') {
        /* for named prepared statement */
        PreparedStatement *pstmt = FetchPreparedStatement(portal->prepStmtName, true, false);
        if (pstmt != NULL && pstmt->plansource != NULL) {
            query_list = pstmt->plansource->query_list;
        }
    } else if (unnamed_psrc != NULL) {
        /* for unnamed prepared statement */
        query_list = unnamed_psrc->query_list;
    }

    SetLocalUniqueSQLId(query_list);
    ereport(DEBUG1,
        (errmodule(MOD_INSTR),
            errmsg("[UniqueSQL] unique id: %lu, set unique sql id for PBE", u_sess->unique_sql_cxt.unique_sql_id)));
}

/*
 * set unique sql id from lightProxy::runMsg & bind
 */
void SetUniqueSQLIdFromCachedPlanSource(CachedPlanSource* cplan)
{
    if (!is_unique_sql_enabled() || !is_local_unique_sql() || cplan == NULL) {
        return;
    }

    SetLocalUniqueSQLId(cplan->query_list);

    ereport(DEBUG1,
        (errmodule(MOD_INSTR),
            errmsg("[UniqueSQL] unique id: %lu, set unique sql id for PBE", u_sess->unique_sql_cxt.unique_sql_id)));
}

static void package_unique_sql_msg_on_remote(StringInfoData* buf, UniqueSQL* entry, uint32 recv_slot_index,
    uint32 recv_cn_id, Oid recv_user_id, uint64 recv_unique_sql_id)
{
    /* now we send unique sql stat one by one, later maybe we can
     * send batch of unique sql stat
     * Send one unique sql stat each time, message format:
     *  1   | + 4          + 4       + 8             + 6*8          +
     *  'r' | + slot index + user id + unique sql id + row activity +
     *
     *  2*8      + 2*8        + TOTAL_TIME_INFO_TYPES*8   |
     *  cache/IO + parse info + time info                 |
     */
    pq_beginmessage(buf, 'r');

    /* send back slot indexes(from original CN) */
    pq_sendint(buf, recv_slot_index, sizeof(uint32));

    /* key */
    pq_sendint(buf, recv_cn_id, sizeof(uint32));
    pq_sendint(buf, recv_user_id, sizeof(uint32));
    pq_sendint64(buf, recv_unique_sql_id);

    /* row activity */
    pq_sendint64(buf, entry->row_activity.returned_rows);
    pq_sendint64(buf, entry->row_activity.tuples_fetched);
    pq_sendint64(buf, entry->row_activity.tuples_returned);
    pq_sendint64(buf, entry->row_activity.tuples_inserted);
    pq_sendint64(buf, entry->row_activity.tuples_updated);
    pq_sendint64(buf, entry->row_activity.tuples_deleted);

    /* Cache/IO */
    pq_sendint64(buf, entry->cache_io.blocks_fetched);
    pq_sendint64(buf, entry->cache_io.blocks_hit);

    /* parse info */
    pq_sendint64(buf, entry->parse.soft_parse);
    pq_sendint64(buf, entry->parse.hard_parse);

    /* time Info */
    for (uint32 idx = 0; idx < TOTAL_TIME_INFO_TYPES; idx++) {
        pq_sendint64(buf, entry->timeInfo.TimeInfoArray[idx]);
    }

    /* net info */
    for (uint32 idx = 0; idx < TOTAL_NET_INFO_TYPES; idx++) {
        pq_sendint64(buf, entry->netInfo.netInfoArray[idx]);
    }

    /* send latest updated time */
    if (t_thrd.proc->workingVersionNum >= STATEMENT_TRACK_VERSION) {
        pq_sendint64(buf, (uint64)entry->updated_time);

        /* sort hash work mem info */
        pq_sendint64(buf, entry->sort_state.counts);
        pq_sendint64(buf, entry->sort_state.total_time);
        pq_sendint64(buf, entry->sort_state.used_work_mem);
        pq_sendint64(buf, entry->sort_state.spill_counts);
        pq_sendint64(buf, entry->sort_state.spill_size);

        pq_sendint64(buf, entry->hash_state.counts);
        pq_sendint64(buf, entry->hash_state.total_time);
        pq_sendint64(buf, entry->hash_state.used_work_mem);
        pq_sendint64(buf, entry->hash_state.spill_counts);
        pq_sendint64(buf, entry->hash_state.spill_size);
    }

    /* ... */
    pq_endmessage(buf);
}

static void reset_reply_resource_owner_and_ctx(ResourceOwner old_cur_owner, MemoryContext old_ctx, bool is_commit)
{
    ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, is_commit, true);
    ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_LOCKS, is_commit, true);
    ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_AFTER_LOCKS, is_commit, true);

    ResourceOwner tmpOwner = t_thrd.utils_cxt.CurrentResourceOwner;
    t_thrd.utils_cxt.CurrentResourceOwner = old_cur_owner;
    ResourceOwnerDelete(tmpOwner);

    (void)MemoryContextSwitchTo(old_ctx);
}

/*
 * ReplyUniqueSQLsStat - DN reply unique SQL IDs stat info
 */
void ReplyUniqueSQLsStat(StringInfo msg, uint32 count)
{
    ereport(DEBUG1, (errmodule(MOD_INSTR), errmsg("[UniqueSQL] get unique sql count: %u", count)));

    ResourceOwner old_cur_owner = t_thrd.utils_cxt.CurrentResourceOwner;
    MemoryContext old_ctx = MemoryContextSwitchTo(t_thrd.mem_cxt.msg_mem_cxt);
    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "ReplyUniqueSQL",
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DFX));

    PG_TRY();
    {
        check_unique_sql_permission();
    }
    PG_CATCH();
    {
        reset_reply_resource_owner_and_ctx(old_cur_owner, old_ctx, false);
        PG_RE_THROW();
    }
    PG_END_TRY();
    reset_reply_resource_owner_and_ctx(old_cur_owner, old_ctx, true);

    if (count > 0 && count < UINT_MAX) {
        StringInfoData buf;
        for (uint32 i = 0; i < count; i++) {
            uint32 recv_slot_index = (uint32)pq_getmsgint(msg, sizeof(uint32));
            uint32 recv_cn_id = (uint32)pq_getmsgint(msg, sizeof(uint32));
            Oid recv_user_id = (Oid)pq_getmsgint(msg, sizeof(uint32));
            uint64 recv_unique_sql_id = (uint64)pq_getmsgint64(msg);

            ereport(DEBUG1, (errmodule(MOD_INSTR), errmsg("[UniqueSQL] get cn_id: %u, user id: %u, unique sql id: %lu",
                recv_cn_id, recv_user_id, recv_unique_sql_id)));

            // get unique sql entry from HTAB
            UniqueSQLKey key = {0, 0};
            UniqueSQL* entry = NULL;
            key.unique_sql_id = recv_unique_sql_id;
            key.user_id = recv_user_id;
            key.cn_id = recv_cn_id;

            uint32 hashCode = uniqueSQLHashCode(&key, sizeof(key));

            (void)LockUniqueSQLHashPartition(hashCode, LW_SHARED);
            entry = (UniqueSQL*)hash_search(g_instance.stat_cxt.UniqueSQLHashtbl, &key, HASH_FIND, NULL);
            if (entry != NULL) {
                package_unique_sql_msg_on_remote(
                    &buf, entry, recv_slot_index, recv_cn_id, recv_user_id, recv_unique_sql_id);
            }
            UnlockUniqueSQLHashPartition(hashCode);
        }

        pq_beginmessage(&buf, 'f');
        pq_endmessage(&buf);
        pq_flush();

        if (buf.data) {
            pfree(buf.data);
        }
    } else {
        ereport(ERROR, (errmodule(MOD_INSTR), errmsg("[UniqueSQL] recv invalid sql ids count")));
    }
}

/*
 * PrintPgStatTableCounter - print current PgStat_TableCounts
 *
 * L - last total stat counter after exit pgstat_report_stat
 * T - the time when entering pgstat_report_stat
 * C - current sql row stat counter
 * for debug purposes
 */
void PrintPgStatTableCounter(char type, PgStat_TableCounts* stat)
{
    if (!is_unique_sql_enabled() || !IS_PGXC_DATANODE) {
        return;
    }

    ereport(DEBUG1,
        (errmodule(MOD_INSTR),
            errmsg("[UniqueSQL] unique id: %lu, row stat counter[%c] - "
                   "Returned: %ld, Fetched: %ld, "
                   "Inserted: %ld, Updated: %ld, Deleted: %ld,"
                   "Blocks_Fetched: %ld, Blocks_Hit: %ld",
                u_sess->unique_sql_cxt.unique_sql_id,
                type,
                stat->t_tuples_returned,
                stat->t_tuples_fetched,
                stat->t_tuples_inserted,
                stat->t_tuples_updated,
                stat->t_tuples_deleted,
                stat->t_blocks_fetched,
                stat->t_blocks_hit)));
}

/*
 * UpdateUniqueSQLStatOnRmote - on dn/cn, update unique sql stat,
 *     including row activity, Cache/IO, parse counter, etc
 *
 * main unique sql update entry:
 *   CN -> CN/DN (DDL)
 *   CN -> DN (DML)
 *
 * need consider below scenarios:
 * 1, simple query(pgstat_report_stat will clean pgStatTabList
 *    by calling frequence)
 * 2, transaction block, such as:
 *     begin;
 *     insert xxxx;
 *     delete XXX;
 *     commit;
 *
 * main implementation logic:
 *   - update last stat counter
 *   - new sql enter
 *   - current sql stat counter = current stat counter - last stat counter
 *   - update unique sql HTAB stat
 *   - update last stat counter
 *   - ...
 * Notes:
 *   we reuse pgStatTabList, as each query's row activity is stored in it.
 */
void UpdateUniqueSQLStatOnRemote()
{
    Assert(need_update_unique_sql_row_stat());
    Assert(u_sess->attr.attr_resource.enable_resource_track &&
           (u_sess->attr.attr_common.instr_unique_sql_count > 0));

    ereport(DEBUG1,
        (errmodule(MOD_INSTR),
            errmsg("[UniqueSQL] unique id: %lu, update "
                   "unique sql stat on remote",
                u_sess->unique_sql_cxt.unique_sql_id)));

    /*
     * Some SQLs doesn't have unique sql id on DN,
     * for example:
     * on CN: gsql - create table xxxx(generate unique sql ID)
     * on DN will be:
     *     START TRANSACTION ISOLATION LEVEL read committed READ WRITE
     *     create table xxx(with unique SQL ID)
     *     PREPARE transaction 'xxx'
     *     COMMIT PREPARED 'xxx'
     * we now only collect the stat counter for the create statement.
     * so need update last stat counter for 'START TRANSACTION...'
     */
    if (u_sess->unique_sql_cxt.unique_sql_id != 0) {
        if (CalcSQLRowStatCounter(
                u_sess->unique_sql_cxt.last_stat_counter, u_sess->unique_sql_cxt.current_table_counter)) {
            UpdateUniqueSQLStat(NULL, NULL, 0, u_sess->unique_sql_cxt.current_table_counter);
        } else {
            /* update parse info */
            UpdateUniqueSQLStat(NULL, NULL, 0, NULL);
        }
    }
}

/* is unique sql enabled
 *
 * status = enable_resource_track + unique sql on/off GUI parameter
 */
bool is_unique_sql_enabled()
{
    return u_sess->attr.attr_resource.enable_resource_track && (u_sess->attr.attr_common.instr_unique_sql_count > 0);
}

/* by default, instr_unique_sql_count is 0,
 * when unique sql is "set instr_unique_sql_count = 100",
 * then won't generate unique sql id firstly(GUC/false), but
 * when the sql enter UpdateUniqueSQLStat, is_unique_sql_enabled()
 * will be true, then assert unique_sql_id != 0 will be failed in
 * UpdateUniqueSQLStat.
 *
 * so in this case, we won't call UpdateUniqueSQLStat
 */
bool IsNeedUpdateUniqueSQLStat(Portal portal)
{
    if (u_sess->unique_sql_cxt.unique_sql_id == 0) {
        if (portal != NULL && portal->stmts != NULL && list_length(portal->stmts) == 1 &&
            nodeTag(linitial(portal->stmts)) == T_VariableSetStmt) {
            VariableSetStmt* stmt = (VariableSetStmt*)(linitial(portal->stmts));
            if (stmt->name && strcmp(stmt->name, "instr_unique_sql_count") == 0) {
                return false;
            }
        }
    }

    return true;
}

/* setter and getter for is_top_unique_sql */
void SetIsTopUniqueSQL(bool value)
{
    u_sess->unique_sql_cxt.is_top_unique_sql = value;
}

bool IsTopUniqueSQL()
{
    return u_sess->unique_sql_cxt.is_top_unique_sql;
}

/* return GUC unique sql tracking type */
int GetUniqueSQLTrackType()
{
    return u_sess->attr.attr_common.unique_sql_track_type;
}

/* if CN and not connection from other CN, it is local unique SQL,
 * else is remote unique SQL
 */
bool is_local_unique_sql()
{
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        return true;
    }

    if (IS_SINGLE_NODE) {
        return true;
    }

    return false;
}

bool need_update_unique_sql_row_stat()
{
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        return false;
    }

    return true;
}

bool need_normalize_unique_string()
{
    if (IS_PGXC_COORDINATOR || IS_SINGLE_NODE) {
        return true;
    }

    return false;
}

enum CleanType { INVALIDTYPE = 0, ALL, BY_GUC, BY_USERID, BY_CNID };

/* WDR will take snapshot of unique sql, reset opration
 * will remove unique sql entry from hash table.
 * during two snapshot, there will be 'reset opration',
 * then we cannot generate report between two snapshots.
 */
static void UpdateUniqueSQLValidStatTimestamp()
{
    gs_lock_test_and_set_64(&g_instance.stat_cxt.NodeStatResetTime, GetCurrentTimestamp());
}

static void CleanupAllUniqueSqlEntry()
{
    int i;
    UniqueSQL* entry = NULL;
    HASH_SEQ_STATUS hash_seq;
    for (i = 0; i < NUM_UNIQUE_SQL_PARTITIONS; i++) {
        LWLockAcquire(GetMainLWLockByIndex(FirstUniqueSQLMappingLock + i), LW_EXCLUSIVE);
    }

    /* remove entry, need update valid stat timestamp */
    UpdateUniqueSQLValidStatTimestamp();
    HTAB *old = g_instance.stat_cxt.UniqueSQLHashtbl;
    MemoryContext oldcxt = CurrentMemoryContext;
    uint32 saveInterruptHoldoffCount = t_thrd.int_cxt.InterruptHoldoffCount;
    PG_TRY();
    {
        InitUniqueSQL();
    }
    PG_CATCH();
    {
        /* errfinish will reset InterruptHoldoffCount, then LWLockRelease will be cored */
        t_thrd.int_cxt.InterruptHoldoffCount = saveInterruptHoldoffCount;

        (void)MemoryContextSwitchTo(oldcxt);
        ErrorData* edata = NULL;
        edata = CopyErrorData();
        FlushErrorState();

        /* hash create failed, reuse the old hash table */
        g_instance.stat_cxt.UniqueSQLHashtbl = old;
        old = NULL;
        ereport(WARNING, (errmodule(MOD_INSTR),
            errmsg("[UniqueSQL] re-create hash failed, reason: '%s', so using old one", edata->message)));

        /* remove the allocated memory which throw by hash_create */
        if (t_thrd.dyhash_cxt.CurrentDynaHashCxt != NULL &&
            strcmp(t_thrd.dyhash_cxt.CurrentDynaHashCxt->name, UNIQUE_SQL_HASH_TBL) == 0) {
            MemoryContextDelete(t_thrd.dyhash_cxt.CurrentDynaHashCxt);
        }
    }
    PG_END_TRY();

    if (old != NULL) {
        hash_destroy(old);
    } else {
        /* reuse old one, clean entry but still not return memory to system */
        hash_seq_init(&hash_seq, g_instance.stat_cxt.UniqueSQLHashtbl);
        while ((entry = (UniqueSQL*)hash_seq_search(&hash_seq)) != NULL) {
            hash_search(g_instance.stat_cxt.UniqueSQLHashtbl, &entry->key, HASH_REMOVE, NULL);
        }
    }

    for (i = 0; i < NUM_UNIQUE_SQL_PARTITIONS; i++) {
        LWLockRelease(GetMainLWLockByIndex(FirstUniqueSQLMappingLock + i));
    }
}

static List* GetRemoveEntryList(int cleanType, uint64 cleanValue)
{
    List* entryList = NIL;
    UniqueSQL* entry = NULL;
    HASH_SEQ_STATUS hash_seq;
    int i;
    for (i = 0; i < NUM_UNIQUE_SQL_PARTITIONS; i++) {
        LWLockAcquire(GetMainLWLockByIndex(FirstUniqueSQLMappingLock + i), LW_SHARED);
    }

    hash_seq_init(&hash_seq, g_instance.stat_cxt.UniqueSQLHashtbl);
    while ((entry = (UniqueSQL*)hash_seq_search(&hash_seq)) != NULL) {
        if ((cleanType == BY_USERID && entry->key.user_id == (Oid)cleanValue) ||
            (cleanType == BY_CNID && entry->key.cn_id == (uint32)cleanValue)) {
            UniqueSQLKey* key = (UniqueSQLKey*)palloc0_noexcept(sizeof(UniqueSQLKey));
            if (key == NULL) {
                for (i = 0; i < NUM_UNIQUE_SQL_PARTITIONS; i++) {
                    LWLockRelease(GetMainLWLockByIndex(FirstUniqueSQLMappingLock + i));
                }
                if (entryList != NIL) {
                    list_free(entryList);
                }

                ereport(ERROR, (errmodule(MOD_INSTR), errmsg("out of memory during allocating list element.")));
            }

            key->cn_id = entry->key.cn_id;
            key->user_id = entry->key.user_id;
            key->unique_sql_id = entry->key.unique_sql_id;
            entryList = lappend(entryList, key);
        }
    }

    for (i = 0; i < NUM_UNIQUE_SQL_PARTITIONS; i++) {
        LWLockRelease(GetMainLWLockByIndex(FirstUniqueSQLMappingLock + i));
    }

    return entryList;
}

static void CleanupInstrUniqueSqlEntry(int cleanType, uint64 cleanValue)
{
    ListCell* cell = NULL;
    List* removeList = NIL;

    if (g_instance.stat_cxt.UniqueSQLHashtbl == NULL) {
        ereport(LOG, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("unique sql hashtable is NULL"))));
        return;
    }

    if (cleanType == ALL) {
        CleanupAllUniqueSqlEntry();
    } else {
        removeList = GetRemoveEntryList(cleanType, cleanValue);
        if (removeList != NIL) {
            foreach (cell, removeList) {
                UniqueSQLKey* key = (UniqueSQLKey*)lfirst(cell);
                uint32 hashCode = uniqueSQLHashCode(key, sizeof(UniqueSQLKey));
                (void)LockUniqueSQLHashPartition(hashCode, LW_EXCLUSIVE);

                /* remove entry, need update valid stat timestamp */
                UpdateUniqueSQLValidStatTimestamp();
                hash_search(g_instance.stat_cxt.UniqueSQLHashtbl, key, HASH_REMOVE, NULL);
                UnlockUniqueSQLHashPartition(hashCode);
            }
            list_free(removeList);
        }
    }
}

static int CheckParameter(const char* global, const char* cleanType, int64 value, bool* isGlobal)
{
    int cleantype = INVALIDTYPE;

    if (!is_unique_sql_enabled()) {
        ereport(LOG, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("unique sql is disabled"))));
        return INVALIDTYPE;
    }

    if (strcasecmp(global, "GLOBAL") == 0) {
        *isGlobal = true;
        if (!IS_PGXC_COORDINATOR && !IS_SINGLE_NODE) {
            ereport(ERROR,
                (errcode(ERRCODE_WARNING), (errmsg("Cleanup global unique sql info only support on CN nodes."))));
        }
    } else if (strcasecmp(global, "LOCAL") == 0) {
        *isGlobal = false;
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_WARNING),
                (errmsg("First parameter is wrong. USAGE: [GLOBAL/LOCAL],[ALL/BY_USERID/BY_CNID],[VALUE]"))));
    }

    if (strcasecmp(cleanType, "ALL") == 0) {
        cleantype = ALL;
    } else if (strcasecmp(cleanType, "BY_GUC") == 0) {
        if (AmWLMWorkerProcess()) {
            cleantype = BY_GUC;
        } else {
            ereport(WARNING, (errmsg("[UniqueSQL] BY_GUC is only used by GUC setting, use [ALL/BY_USERID/BY_CNID]")));
            return INVALIDTYPE;
        }
    } else if (strcasecmp(cleanType, "BY_USERID") == 0) {
        if (value <= 0 || value > MAX_UINT32) {
            ereport(WARNING,
                (errmsg("[UniqueSQL] third parameter of reset unique sql is out of range with BY_USERID")));
            return INVALIDTYPE;
        }
        cleantype = BY_USERID;
    } else if (strcasecmp(cleanType, "BY_CNID") == 0) {
        if (value <= INT_MIN || value > MAX_UINT32) {
            ereport(WARNING, (errmsg("[UniqueSQL] third parameter of reset unique sql is out of range with BY_CNID")));
            return INVALIDTYPE;
        }
        cleantype = BY_CNID;
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_WARNING),
                (errmsg("Second parameter is wrong. USAGE:[GLOBAL/LOCAL],[ALL/BY_USERID/BY_CNID],[VALUE]"))));
    }
    return cleantype;
}

#ifndef ENABLE_MULTIPLE_NODES
/*
 * AutoRecycleUniqueSQLEntry
 * This function is called when the hash table is full, and then
 * randomly cleans a certain number of unique SQL in the hash table,
 * which is instr_unique_sql_count * ratio + the bloated number.
 * return:
 * true - successful or there is still free space
 * fasle - failed
 */
static bool AutoRecycleUniqueSQLEntry()
{
    LWLockAcquire(UniqueSqlEvictLock, LW_EXCLUSIVE);
    /* Clean is not needed if there is still free space in unique SQL hash table. */
    long totalCount = hash_get_num_entries(g_instance.stat_cxt.UniqueSQLHashtbl);
    if (totalCount < u_sess->attr.attr_common.instr_unique_sql_count - 1) {
        LWLockRelease(UniqueSqlEvictLock);
        ereport(LOG,
            (errmodule(MOD_INSTR), errmsg("[UniqueSQL] There is still free space in unique SQL hash table and no need to clean it up.")));
        return true;
    }
    int instr_unique_sql_count = u_sess->attr.attr_common.instr_unique_sql_count;
    /* If the number of entries is too large, it may cause the problem of
     * applying for large memory. Aotu-cleanup is not performed in this situation.
     * maxEntryNum - The maximum number of KeyUpdatedtime that 1G memory can store.
     */
    long maxEntryNum = long(1024 * 1024 * 1024) / sizeof(KeyUpdatedtime);
    if (totalCount >= maxEntryNum) {
        LWLockRelease(UniqueSqlEvictLock);
        ereport(WARNING,
            (errmodule(MOD_INSTR), errcode(ERRCODE_LOG), errmsg("[UniqueSQL] instr_unique_sql_count is too large, uniquesql auto-clean will not happen.")));
        return false;
    }
    const double cleanRatio = 0.1;
    int cleanCount = Max(int(cleanRatio * instr_unique_sql_count + (totalCount - instr_unique_sql_count)), 1);
    /* get remove entry list */
    KeyUpdatedtime* removeList = GetSortedEntryList();
    if (removeList == NULL) {
        LWLockRelease(UniqueSqlEvictLock);
        return false;
    }
    /* clean */
    for (int i = 0; i < cleanCount; ++i) {
        UniqueSQLKey* key = &(removeList[i].key);
        uint32 hashCode = uniqueSQLHashCode(key, sizeof(UniqueSQLKey));
        (void)LockUniqueSQLHashPartition(hashCode, LW_EXCLUSIVE);

        /* remove entry, need update valid stat timestamp */
        UpdateUniqueSQLValidStatTimestamp();
        hash_search(g_instance.stat_cxt.UniqueSQLHashtbl, key, HASH_REMOVE, NULL);
        UnlockUniqueSQLHashPartition(hashCode);
    }
    pfree(removeList);
    LWLockRelease(UniqueSqlEvictLock);
    ereport(LOG,
            (errmodule(MOD_INSTR), errmsg("[UniqueSQL] Auto-cleanup over, %d uniquesqls are recycled.", cleanCount)));
    return true;
}

/*
 * GetSortedEntryList
 * get all of entry, and sort them by entry's updated_time
 */
static KeyUpdatedtime* GetSortedEntryList()
{
    UniqueSQL* entry = NULL;
    HASH_SEQ_STATUS hash_seq;
    int j = 0;
    int i;
    for (i = 0; i < NUM_UNIQUE_SQL_PARTITIONS; i++) {
        LWLockAcquire(GetMainLWLockByIndex(FirstUniqueSQLMappingLock + i), LW_SHARED);
    }
    long totalCount = hash_get_num_entries(g_instance.stat_cxt.UniqueSQLHashtbl);
    KeyUpdatedtime* removeList = NULL;
    removeList = (KeyUpdatedtime*)palloc0_noexcept(totalCount * sizeof(KeyUpdatedtime));
    if (removeList == NULL) {
        for (i = 0; i < NUM_UNIQUE_SQL_PARTITIONS; i++) {
            LWLockRelease(GetMainLWLockByIndex(FirstUniqueSQLMappingLock + i));
        }
        return NULL;
    }
    hash_seq_init(&hash_seq, g_instance.stat_cxt.UniqueSQLHashtbl);
    while ((entry = (UniqueSQL*)hash_seq_search(&hash_seq)) != NULL) {
        KeyUpdatedtime keyUpdatedtime;
        keyUpdatedtime.key.cn_id = entry->key.cn_id;
        keyUpdatedtime.key.user_id = entry->key.user_id;
        keyUpdatedtime.key.unique_sql_id = entry->key.unique_sql_id;
        keyUpdatedtime.updated_time = entry->updated_time;
        removeList[j++] = keyUpdatedtime;
    }
    for (i = 0; i < NUM_UNIQUE_SQL_PARTITIONS; i++) {
        LWLockRelease(GetMainLWLockByIndex(FirstUniqueSQLMappingLock + i));
    }
    qsort((void*)removeList, j, sizeof(KeyUpdatedtime), KeyUpdatedtimeCmp);
    return removeList;
}

static int KeyUpdatedtimeCmp(const void* a, const void* b)
{
    const KeyUpdatedtime* i1 = (const KeyUpdatedtime*)a;
    const KeyUpdatedtime* i2 = (const KeyUpdatedtime*)b;
    if (i1->updated_time < i2->updated_time)
        return -1;
    else if (i1->updated_time == i2->updated_time)
        return 0;
    else
        return 1;
}
#endif

/* Reset unique sql stat info for the current database */
Datum reset_unique_sql(PG_FUNCTION_ARGS)
{
    int cleantype;
    ParallelFunctionState* state = NULL;
    bool isGlobal = false;
    char* global = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char* cleanType = text_to_cstring(PG_GETARG_TEXT_PP(1));
    int64 cleanValue = PG_GETARG_INT64(2);
    if (!superuser() && !isMonitoradmin(GetUserId())) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            (errmsg("only system/monitor admin can reset unique sql"))));
    }

    cleantype = CheckParameter(global, cleanType, cleanValue, &isGlobal);
    if (cleantype == INVALIDTYPE) {
        PG_RETURN_INT64(0);
    }

    ereport(LOG,
        (errmodule(MOD_INSTR),
            errmsg("[UniqueSQL] clean unique sql, "
                   "clean scope: %s, clean type: %s, clean value: %lu!",
                global,
                cleanType,
                (uint64)cleanValue)));
    if (cleantype == BY_GUC) {
        cleantype = BY_CNID;
        cleanType = "BY_CNID";
        Oid node_oid = get_pgxc_nodeoid(g_instance.attr.attr_common.PGXCNodeName);
        cleanValue = get_pgxc_node_id(node_oid);
    }

    CleanupInstrUniqueSqlEntry(cleantype, cleanValue);
    if (isGlobal && !IS_SINGLE_NODE) {
        StringInfoData buf;
        ExecNodes* exec_nodes = (ExecNodes*)makeNode(ExecNodes);
        exec_nodes->baselocatortype = LOCATOR_TYPE_HASH;
        exec_nodes->accesstype = RELATION_ACCESS_READ;
        exec_nodes->primarynodelist = NIL;
        exec_nodes->en_expr = NULL;
        exec_nodes->en_relid = InvalidOid;
        exec_nodes->nodeList = NIL;

        initStringInfo(&buf);
        appendStringInfo(&buf, "SELECT pg_catalog.reset_unique_sql('LOCAL','%s',%ld);", cleanType, cleanValue);
        state = RemoteFunctionResultHandler(buf.data, exec_nodes, NULL, true, EXEC_ON_ALL_NODES, true);
        FreeParallelFunctionState(state);
        pfree_ext(buf.data);
    }
    ereport(LOG, (errmodule(MOD_INSTR), errmsg("[UniqueSQL] clean unique sql finished.")));
    PG_RETURN_INT64(1);
}

/*
 * Reset session variables before/after each unique sql
 */
void ResetCurrentUniqueSQL(bool need_reset_cn_id)
{
    u_sess->unique_sql_cxt.unique_sql_id = 0;
    u_sess->unique_sql_cxt.unique_sql_user_id = InvalidOid;
    if (need_reset_cn_id) {
        u_sess->unique_sql_cxt.unique_sql_cn_id = InvalidOid;
    }

    /* multi query only used in exec_simple_query */
    u_sess->unique_sql_cxt.is_multi_unique_sql = false;
    u_sess->unique_sql_cxt.curr_single_unique_sql = NULL;
    u_sess->unique_sql_cxt.multi_sql_offset = 0;
    u_sess->unique_sql_cxt.need_update_calls = true;

    /* used when nested portal calling case */
    u_sess->unique_sql_cxt.portal_nesting_level = 0;
#ifndef ENABLE_MULTIPLE_NODES
    u_sess->unique_sql_cxt.unique_sql_text = NULL;
#endif
    u_sess->unique_sql_cxt.skipUniqueSQLCount = 0;
}

void FindUniqueSQL(UniqueSQLKey key, char* unique_sql)
{
    errno_t rc = 0;
    uint32 hashCode = uniqueSQLHashCode(&key, sizeof(key));
    (void)LockUniqueSQLHashPartition(hashCode, LW_SHARED);
    /* step 2. find the unique query from UniqueSQLHashtbl, then insert into ASHUniqueSQLHashtbl */
    UniqueSQL *entry = (UniqueSQL*)hash_search(g_instance.stat_cxt.UniqueSQLHashtbl, &key, HASH_FIND, NULL);

    if (entry == NULL) {
        rc = strcpy_s(unique_sql, UNIQUE_SQL_MAX_LEN, "");
        securec_check(rc, "\0", "\0");
    } else {
        rc = strcpy_s(unique_sql, UNIQUE_SQL_MAX_LEN, entry->unique_sql);
        securec_check(rc, "\0", "\0");
    }
    UnlockUniqueSQLHashPartition(hashCode);
}

char* FindCurrentUniqueSQL()
{
    const char* hint = "/* missing SQL statement, GUC instr_unique_sql_count is too small. */";
    char *unique_query = NULL;
    UniqueSQLKey key;

    key.unique_sql_id = u_sess->unique_sql_cxt.unique_sql_id;
    key.cn_id = u_sess->unique_sql_cxt.unique_sql_cn_id;
    key.user_id = u_sess->unique_sql_cxt.unique_sql_user_id;

    uint32 hashCode = uniqueSQLHashCode(&key, sizeof(key));

    (void)LockUniqueSQLHashPartition(hashCode, LW_SHARED);
    UniqueSQL *entry = (UniqueSQL*)hash_search(g_instance.stat_cxt.UniqueSQLHashtbl, &key, HASH_FIND, NULL);
    if (entry != NULL && entry->unique_sql != NULL) {
        unique_query = pstrdup(entry->unique_sql);
    }
    UnlockUniqueSQLHashPartition(hashCode);

    if (unique_query == NULL) {
        unique_query = pstrdup(hint);
    }

    return unique_query;
}

static bool need_reuse_unique_sql_id(Query *query) {
    /* for fetch statement, need to reuse unique sql id from source cursor SQL */
    if (query->commandType == CMD_UTILITY && query->utilityStmt != NULL && IsA(query->utilityStmt, FetchStmt)) {
        FetchStmt *fetch_stmt = (FetchStmt*)query->utilityStmt;
        if (fetch_stmt->portalname != NULL) {
            Portal fetch_stmt_portal = GetPortalByName(fetch_stmt->portalname);

            if (PortalIsValid(fetch_stmt_portal) && fetch_stmt_portal->queryDesc != NULL &&
                fetch_stmt_portal->queryDesc->plannedstmt != NULL &&
                fetch_stmt_portal->queryDesc->plannedstmt->uniqueSQLId != 0) {
                u_sess->unique_sql_cxt.unique_sql_id = fetch_stmt_portal->queryDesc->plannedstmt->uniqueSQLId;
                ereport(DEBUG1, (errmodule(MOD_INSTR),
                    errmsg("[UniqueSQL] use cursor SQL's unique id: %lu", u_sess->unique_sql_cxt.unique_sql_id)));
                return true;
            }
        }
    }
    return false;
}

bool is_instr_top_portal()
{
    return u_sess->unique_sql_cxt.portal_nesting_level == 1;
}

void increase_instr_portal_nesting_level()
{
    if (is_local_unique_sql()) {
        u_sess->unique_sql_cxt.portal_nesting_level++;
    }
}

void decrease_instr_portal_nesting_level()
{
    if (is_local_unique_sql()) {
        u_sess->unique_sql_cxt.portal_nesting_level--;
    }
}

static void instr_unique_sql_handle_multi_sql_time_info()
{
    timeInfoRecordEnd();
    timeInfoRecordStart();
}

/* handle multi sql case in exec_simple_query */
void instr_unique_sql_handle_multi_sql(bool is_first_parsetree)
{

    if (!is_first_parsetree) {
        statement_commit_metirc_context();
        statement_init_metric_context();
        instr_stmt_report_start_time();
    }

    if (is_local_unique_sql()) {
        if (IS_SINGLE_NODE || IS_PGXC_COORDINATOR) {
            u_sess->debug_query_id = generate_unique_id64(&gt_queryId);
            pgstat_report_queryid(u_sess->debug_query_id);
        }

        /* reset unique_sql */
        if (is_unique_sql_enabled()) {
            if (!is_first_parsetree) {
                instr_unique_sql_handle_multi_sql_time_info();
            }

            ResetCurrentUniqueSQL();
            u_sess->unique_sql_cxt.unique_sql_start_time = (u_sess->unique_sql_cxt.unique_sql_start_time > 0)
                ? GetCurrentTimestamp() : GetCurrentStatementLocalStartTimestamp();

            /*
             * INSTR: when track type is TOP, before query to start,
             * we reset is_top_unique_sql to false
             */
            if (IS_UNIQUE_SQL_TRACK_TOP)
                SetIsTopUniqueSQL(false);

            /* reset unique sql returned rows(SELECT) */
            UniqueSQLStatCountResetReturnedRows();
            UniqueSQLStatCountResetParseCounter();
        }
    }
}
