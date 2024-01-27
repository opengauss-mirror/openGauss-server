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
 * instr_statement.h
 *        definitions for full/slow sql
 *
 *
 * IDENTIFICATION
 *        src/include/instruments/instr_statement.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef INSTR_STATEMENT_H
#define INSTR_STATEMENT_H

#include "c.h"
#include "pgstat.h"
#include "instruments/unique_sql_basic.h"

#define ENABLE_STATEMENT_TRACK u_sess->attr.attr_common.enable_stmt_track
typedef enum {
    STMT_TRACK_OFF = 0,
    STMT_TRACK_L0,
    STMT_TRACK_L1,
    STMT_TRACK_L2,
    LEVEL_INVALID
} StatLevel;

#define MAX_STATEMENT_STAT_LEVEL (STMT_TRACK_L2)

struct LockSummaryStat {
    /* lock */
    int64   lock_start_time;
    int64   lock_cnt;
    int64   lock_time;
    int64   lock_wait_start_time;
    int64   lock_wait_cnt;
    int64   lock_wait_time;

    /* lwlock */
    int64   lwlock_start_time;
    int64   lwlock_cnt;
    int64   lwlock_time;
    int64   lwlock_wait_start_time;
    int64   lwlock_wait_cnt;
    int64   lwlock_wait_time;

    /* the maximum lock count at a time. */
    int64   lock_max_cnt;
    int64   lock_hold_cnt;
};

#pragma pack (1)
typedef struct {
    char eventType;
    int64 timestamp;
    LOCKTAG tag;
    LOCKMODE mode;
} LockEventStartInfo;

typedef struct {
    char eventType;
    int64 timestamp;
    uint16 id;
    LWLockMode mode;
} LWLockEventStartInfo;

typedef struct {
    char eventType;
    int64 timestamp;
} LockEventEndInfo;
#pragma pack()

typedef enum {
    LOCK_START = 1,
    LOCK_END,
    LOCK_WAIT_START,
    LOCK_WAIT_END,
    LOCK_RELEASE,
    LWLOCK_START,
    LWLOCK_END,
    LWLOCK_WAIT_START,
    LWLOCK_WAIT_END,
    LWLOCK_RELEASE,
    TYPE_INVALID
} StmtDetailType;

// type(1 byte), timestamp(8 bytes), locktag(20 bytes), lockmode(4 bytes)
#define LOCK_START_DETAIL_BUFSIZE 33
// type, timestamp
#define LOCK_END_DETAIL_BUFSIZE 9
// type, timestamp, locktag, lockmode
#define LOCK_WAIT_START_DETAIL_BUFSIZE 33
// type, timestamp
#define LOCK_WAIT_END_DETAIL_BUFSIZE 9
// type, timestamp, locktag, lockmode
#define LOCK_RELEASE_START_DETAIL_BUFSIZE 33

// type(1 byte), timestamp(8 bytes), lwlockId(2 bytes), lockmode(4 bytes)
#define LWLOCK_START_DETAIL_BUFSIZE 15
// type, timestamp
#define LWLOCK_END_DETAIL_BUFSIZE 9
// type, timestamp, lwlockId, lockmode
#define LWLOCK_WAIT_START_DETAIL_BUFSIZE 15
// type, timestamp
#define LWLOCK_WAIT_END_DETAIL_BUFSIZE 9
// type, timestamp, lwlockId, lockmode
#define LWLOCK_RELEASE_START_DETAIL_BUFSIZE 15

/* ----------
 * Flags for CAUSE TYPE
 * ----------
 */
#define NUM_F_TYPECASTING (1 << 1) /* cast function exists */
#define NUM_F_LIMIT (1 << 2) /* limit to much rows */
#define NUM_F_LEAKPROOF (1 << 3) /* proleakproof of function is false */

#define INVALID_DETAIL_BUFSIZE 0

#define STATEMENT_DETAIL_BUF_MULTI 10
#define STATEMENT_DETAIL_BUF 256

#define STATEMENT_DETAIL_BUFSIZE 4096
struct StatementDetailItem {
    void *next;     /* next item */
    char buf[STATEMENT_DETAIL_BUFSIZE]; /* [version, [LOCK_START, timestamp, locktag, lockmode], [...]] */
};

struct StatementDetail {
    int n_items;        /* how many of detail items */
    uint32 cur_pos;     /* the write position of the last item */
    bool oom;           /* whether OutOfMemory happened. */
    StatementDetailItem *head;  /* the first detail item. */
    StatementDetailItem *tail;  /* the last detail item. */
};

/* increment this version if more detail is supported. */
#define STATEMENT_DETAIL_VERSION_v1  1
#define STATEMENT_DETAIL_VERSION 2

/* flag for detail content's integrity, CAUTION: modify is_valid_detail_record() if add new flag */
#define STATEMENT_DETAIL_LOCK_STATUS_LEN 1
#define STATEMENT_DETAIL_LOCK_NOT_TRUNCATED 0
#define STATEMENT_DETAIL_LOCK_TRUNCATED 1
#define STATEMENT_DETAIL_LOCK_MISSING_OOM 2

#define STATEMENT_DETAIL_FORMAT_STRING "plaintext"
#define STATEMENT_DETAIL_FORMAT_JSON "json"
#define STATEMENT_DETAIL_TYPE_PRETTY "pretty"

#define Anum_statement_history_finish_time 12
#define FLUSH_USLEEP_INTERVAL 100000
/* entry for full/slow sql stat */
typedef struct StatementStatContext {
    void *next;             /* next item if in free or suspend list */

    // GUC variant
    char* schema_name;      /* search path */
    char* application_name; /* workload identifier */
    StatLevel level;        /* which level metrics to be collected base on GUC */
    StatLevel dynamic_track_level;              /* which dynamic level metrics to be collected */

    // variant, collect at commit handler
    uint64 unique_query_id;     /* from knl_u_unique_sql_context's unique_sql_id */
    uint64 debug_query_id;      /* from knl_session_context's debug_query_id */
    uint32 unique_sql_cn_id;    /* from knl_session_context's unique_sql_cn_id */
    uint64 parent_query_id;
    char trace_id[MAX_TRACE_ID_SIZE]; /* from knl_session_context's trace_id */
    char* query;                /* from PgBackendStatus's st_activity
                                    or knl_u_unique_sql_context's curr_single_unique_sql */
    TimestampTz start_time;     /* from PgBackendStatus's st_activity_start_timestamp */
    TimestampTz finish_time;    /* commit's GetCurrentTimestamp */
    int64 slow_query_threshold;   /* from knl_session_attr_storage's log_min_duration_statement */

    int64 timeModel[TOTAL_TIME_INFO_TYPES];     /* from knl_u_stat_context's localTimeInfoArray */
    uint64 networkInfo[TOTAL_NET_INFO_TYPES];   /* from knl_u_stat_context's localNetInfo */
    UniqueSQLRowActivity row_activity;          /* row activity */
    UniqueSQLCacheIO cache_io;                  /* cache/IO */

    // variant, collect them during its progress
    ThreadId tid;
    TransactionId txn_id;
    UniqueSQLParse parse;
    char* query_plan;        /* query plan */
    char* params;            /* params for pbe statements */
    uint64 plan_size;
    LockSummaryStat lock_summary;
    StatementDetail details;
    uint32 cause_type; /* possible Slow SQL risks */

    /* wait events */
    WaitEventEntry *wait_events;
    Bitmapset      *wait_events_bitmap;
} StatementStatContext;
extern void StatementFlushMain();
extern void CleanStatementMain();
extern bool IsStatementFlushProcess(void);

extern bool check_statement_stat_level(char** newval, void** extra, GucSource source);
extern void assign_statement_stat_level(const char* newval, void* extra);
extern bool check_statement_retention_time(char** newval, void** extra, GucSource source);
extern void assign_statement_retention_time(const char* newval, void* extra);
extern bool check_standby_statement_chain_size(char** newval, void** extra, GucSource source);
extern void assign_standby_statement_chain_size(const char* newval, void* extra);
extern void instr_stmt_report_lock(
    StmtDetailType type, int lockmode = -1, const LOCKTAG *locktag = NULL, uint16 lwlockId = 0);

extern void instr_stmt_report_stat_at_handle_init();
extern void instr_stmt_report_stat_at_handle_commit();
extern void instr_stmt_report_unique_sql_info(const PgStat_TableCounts *agg_table_stat,
    const int64 timeInfo[], const uint64 *netInfo);
extern void instr_stmt_report_txid(uint64 txid);
extern void instr_stmt_report_query(uint64 unique_query_id);
extern void instr_stmt_report_query_plan(QueryDesc *queryDesc);
extern void instr_stmt_report_debug_query_id(uint64 debug_query_id);
extern void instr_stmt_report_trace_id(char *trace_id);
extern void instr_stmt_report_start_time();
extern void instr_stmt_report_finish_time();
extern bool instr_stmt_need_track_plan();
extern void instr_stmt_report_returned_rows(uint64 returned_rows);
extern void instr_stmt_report_soft_parse(uint64 soft_parse);
extern void instr_stmt_report_hard_parse(uint64 hard_parse);
extern void instr_stmt_dynamic_change_level();
extern void instr_stmt_set_wait_events_bitmap(uint32 class_id, uint32 event_id);
extern void instr_stmt_copy_wait_events();
extern void instr_stmt_diff_wait_events();
extern void init_full_sql_wait_events();
extern void instr_stmt_report_cause_type(uint32 type);
extern bool instr_stmt_plan_need_report_cause_type();
extern uint32 instr_stmt_plan_get_cause_type();

#endif

