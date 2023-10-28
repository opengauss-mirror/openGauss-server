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
 * ---------------------------------------------------------------------------------------
 * 
 * instr_unique_sql.h
 *        definitions for unique sql
 * 
 * 
 * IDENTIFICATION
 *        src/include/instruments/instr_unique_sql.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef INSTR_UNIQUE_SQL_H
#define INSTR_UNIQUE_SQL_H

#include "nodes/execnodes.h"
#include "nodes/parsenodes.h"
#include "pgstat.h"
#include "instruments/unique_sql_basic.h"
#include "utils/batchsort.h"

typedef struct {
    int64 total_time; /* total time for the unique sql entry */
    int64 min_time;   /* min time for unique sql entry's history events */
    int64 max_time;   /* max time for unique sql entry's history events */
} UniqueSQLElapseTime;

typedef struct UniqueSQLTime {
    int64 TimeInfoArray[TOTAL_TIME_INFO_TYPES];
} UniqueSQLTime;

typedef struct UniqueSQLNetInfo {
    uint64 netInfoArray[TOTAL_NET_INFO_TYPES];
} UniqueSQLNetInfo;

typedef struct UniqueSQLWorkMemInfo {
    pg_atomic_uint64 counts; /* # of operation during unique sql */
    int64 used_work_mem; /* space of used work mem by kbs */
    int64 total_time; /* execution time of sort/hash operation */
    pg_atomic_uint64 spill_counts; /* # of spill times during the sort/hash operation */
    pg_atomic_uint64 spill_size;    /* spill size for temp table by kbs */
} UniqueSQLWorkMemInfo;

typedef struct {
    UniqueSQLKey key; /* CN oid + user oid + unique sql id */

    /* alloc extra UNIQUE_SQL_MAX_LEN space to store unique sql string */
    char* unique_sql; /* unique sql text */

    pg_atomic_uint64 calls;          /* calling times */
    UniqueSQLElapseTime elapse_time; /* elapst time stat in ms */
    TimestampTz updated_time;        /* latest update time for the unique sql entry */
    UniqueSQLTime timeInfo;
    UniqueSQLNetInfo netInfo;

    UniqueSQLRowActivity row_activity; /* row activity */
    UniqueSQLCacheIO cache_io;         /* cache/IO */
    UniqueSQLParse parse;              /* hard/soft parse counter */
    bool is_local;                     /* local sql(run from current node) */
    UniqueSQLWorkMemInfo sort_state;   /* work mem info of sort operation */
    UniqueSQLWorkMemInfo hash_state;   /* work mem info of hash operation */
} UniqueSQL;

/* Unique SQL track type */
typedef enum {
    UNIQUE_SQL_NONE = 0,
    UNIQUE_SQL_TRACK_TOP, /* only top SQL will be tracked */
    UNIQUE_SQL_TRACK_ALL  /* all the sql(including plsql) will be tracked */
} UniqueSQLTrackType;

typedef struct {
    int64* timeInfo;
    uint64* netInfo;
} UniqueSQLStat;

extern int GetUniqueSQLTrackType();

#define UniqueSQLStatCountReturnedRows(n)                                     \
    if (is_local_unique_sql() && is_unique_sql_enabled()) {                   \
        (u_sess->unique_sql_cxt.unique_sql_returned_rows_counter += n);       \
        ereport(DEBUG1,                                                       \
            (errmodule(MOD_INSTR),                                            \
                errmsg("[UniqueSQL] unique id: %lu: %lu returned rows",       \
                    u_sess->unique_sql_cxt.unique_sql_id,                     \
                    (uint64)n)));                                             \
    }

#define UniqueSQLStatCountResetReturnedRows()                                                                      \
    if (is_local_unique_sql() && is_unique_sql_enabled()) {                                                        \
        (u_sess->unique_sql_cxt.unique_sql_returned_rows_counter = 0);                                             \
        ereport(DEBUG1,                                                                                            \
            (errmodule(MOD_INSTR),                                                                                 \
                errmsg("[UniqueSQL] unique id: %lu: reset returned rows", u_sess->unique_sql_cxt.unique_sql_id))); \
    }

#define UniqueSQLStatCountSoftParse(n)                                                                          \
    if (is_unique_sql_enabled()) {                                                                              \
        (u_sess->unique_sql_cxt.unique_sql_soft_parse += n);                                                    \
        ereport(DEBUG1,                                                                                         \
            (errmodule(MOD_INSTR),                                                                              \
                errmsg("[UniqueSQL] unique id: %lu: %d soft parse", u_sess->unique_sql_cxt.unique_sql_id, n))); \
    }

#define UniqueSQLStatCountHardParse(n)                                                                          \
    if (is_unique_sql_enabled()) {                                                                              \
        (u_sess->unique_sql_cxt.unique_sql_hard_parse += n);                                                    \
        ereport(DEBUG1,                                                                                         \
            (errmodule(MOD_INSTR),                                                                              \
                errmsg("[UniqueSQL] unique id: %lu: %d hard parse", u_sess->unique_sql_cxt.unique_sql_id, n))); \
    }

#define UniqueSQLStatCountResetParseCounter()                                                                      \
    if (is_unique_sql_enabled()) {                                                                                 \
        (u_sess->unique_sql_cxt.unique_sql_soft_parse = 0);                                                        \
        (u_sess->unique_sql_cxt.unique_sql_hard_parse = 0);                                                        \
        ereport(DEBUG1,                                                                                            \
            (errmodule(MOD_INSTR),                                                                                 \
                errmsg("[UniqueSQL] unique id: %lu: reset parse counter", u_sess->unique_sql_cxt.unique_sql_id))); \
    }

#define UniqueSQLSumTableStatCounter(A, B)             \
    if (IS_PGXC_DATANODE && is_unique_sql_enabled()) { \
        A.t_tuples_returned += B.t_tuples_returned;    \
        A.t_tuples_fetched += B.t_tuples_fetched;      \
        A.t_tuples_inserted += B.t_tuples_inserted;    \
        A.t_tuples_updated += B.t_tuples_updated;      \
        A.t_tuples_deleted += B.t_tuples_deleted;      \
        A.t_blocks_fetched += B.t_blocks_fetched;      \
        A.t_blocks_hit += B.t_blocks_hit;              \
    }

#define UniqueSQLDiffTableStatCounter(A, B, C)                           \
    if (IS_PGXC_DATANODE && is_unique_sql_enabled()) {                   \
        A.t_tuples_returned = B.t_tuples_returned - C.t_tuples_returned; \
        A.t_tuples_fetched = B.t_tuples_fetched - C.t_tuples_fetched;    \
        A.t_tuples_inserted = B.t_tuples_inserted - C.t_tuples_inserted; \
        A.t_tuples_updated = B.t_tuples_updated - C.t_tuples_updated;    \
        A.t_tuples_deleted = B.t_tuples_deleted - C.t_tuples_deleted;    \
        A.t_blocks_fetched = B.t_blocks_fetched - C.t_blocks_fetched;    \
        A.t_blocks_hit = B.t_blocks_hit - C.t_blocks_hit;                \
    }

#define IS_UNIQUE_SQL_TRACK_TOP ((IS_PGXC_COORDINATOR || IS_SINGLE_NODE) && GetUniqueSQLTrackType() == UNIQUE_SQL_TRACK_TOP)
#define IS_UNIQUE_SQL_TRACK_ALL ((IS_PGXC_COORDINATOR || IS_SINGLE_NODE) && GetUniqueSQLTrackType() == UNIQUE_SQL_TRACK_ALL)


#define INIT_UNIQUE_SQL_CXT()                                                                               \
PLSQLStmtTrackStack stack;                                                                                  \
int64 timeInfo[TOTAL_TIME_INFO_TYPES] = {0};                                                                \
if (IS_UNIQUE_SQL_TRACK_ALL) {                                                                              \
    timeInfo[DB_TIME] = GetCurrentTimestamp();                                                              \
    timeInfo[CPU_TIME] = getCpuTime();                                                                      \
}                                                                                                           \


#define BACKUP_UNIQUE_SQL_CXT()                                                             \
        stack.push();

/* In order to ensure the accuracy of WDR statistical time,                                              
we solely measure dbtime and consider db_time as entirely constituted by pl_exec time.*/ 
#define RESTORE_UNIQUE_SQL_CXT()                                                                                 \
if (IS_UNIQUE_SQL_TRACK_ALL) {                                                                                   \
        int64 cur = getCpuTime();                                                                                \
        timeInfo[CPU_TIME] = cur - timeInfo[CPU_TIME];                                                           \
        timeInfo[DB_TIME] = GetCurrentTimestamp() - timeInfo[DB_TIME];                                           \
        timeInfo[PL_EXECUTION_TIME] = timeInfo[DB_TIME];                                                         \
        instr_stmt_report_unique_sql_info(NULL, timeInfo, NULL);                                                 \
    }                                                                                                            \
stack.pop();                                                                                                     \


#define START_TRX_UNIQUE_SQL_ID 2718638560

#define PUSH_SKIP_UNIQUE_SQL_HOOK() u_sess->unique_sql_cxt.skipUniqueSQLCount++;
#define POP_SKIP_UNIQUE_SQL_HOOK() u_sess->unique_sql_cxt.skipUniqueSQLCount--;

void InitUniqueSQL();
void UpdateUniqueSQLStat(Query* query, const char* sql, int64 elapse_start_time,
    PgStat_TableCounts* agg_table_count = NULL, UniqueSQLStat* sql_stat = NULL);
void ResetUniqueSQLString();

void instr_unique_sql_register_hook();
void SetUniqueSQLIdFromPortal(Portal portal, CachedPlanSource* unnamedPsrc);
void SetUniqueSQLIdFromCachedPlanSource(CachedPlanSource* cplan);
void SetUniqueSQLIdInBatchBindExecute(CachedPlanSource* cplan, const ParamListInfo* params_set, int batch_count);

void ReplyUniqueSQLsStat(StringInfo msg, uint32 count);

void PrintPgStatTableCounter(char type, PgStat_TableCounts* stat);
void UpdateUniqueSQLStatOnRemote();

bool is_unique_sql_enabled();

bool IsNeedUpdateUniqueSQLStat(Portal portal);

void SetIsTopUniqueSQL(bool value);
bool IsTopUniqueSQL();

bool is_local_unique_sql();
bool need_normalize_unique_string();
bool need_update_unique_sql_row_stat();
void ResetCurrentUniqueSQL(bool need_reset_cn_id = false);
bool isUniqueSQLContextInvalid();
void UpdateSingleNodeByPassUniqueSQLStat(bool isTopLevel);
void UpdateUniqueSQLHashStats(HashJoinTable hashtable, TimestampTz* start_time);
void UpdateUniqueSQLVecSortStats(Batchsortstate* state, uint64 spill_count, TimestampTz* start_time);
void FindUniqueSQL(UniqueSQLKey key, char* unique_sql);
char* FindCurrentUniqueSQL();

bool is_instr_top_portal();
void increase_instr_portal_nesting_level();
void decrease_instr_portal_nesting_level();
void instr_unique_sql_handle_multi_sql(bool is_first_parsetree);
void instr_unique_sql_report_elapse_time(int64 elapse_start_time);
void instr_unique_sql_set_start_time(bool condition, int64 val1, int64 val2);
#endif
