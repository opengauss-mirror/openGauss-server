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
 * instr_handle_mgr.cpp
 *   functions for handle manager which used in full/slow sql
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/instruments/statement/instr_handle_mgr.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "instruments/instr_handle_mgr.h"
#include "instruments/instr_statement.h"
#include "knl/knl_variable.h"
#include "utils/memutils.h"
#include "utils/palloc.h"

#define CHECK_STMT_TRACK_ENABLED()                                              \
{                                                                               \
    if (IsInitdb || !ENABLE_STATEMENT_TRACK) {                                              \
        return;                                                                 \
    } else if (u_sess->statement_cxt.statement_level[0] == STMT_TRACK_OFF &&    \
        u_sess->statement_cxt.statement_level[1] == STMT_TRACK_OFF) {           \
        return;                                                                 \
    }                                                                           \
}

/* statement track wait events level control >= L1 */
static bool is_stmt_wait_events_enabled()
{
    if (!ENABLE_STATEMENT_TRACK) {
        return false;
    }
    if (CURRENT_STMT_METRIC_HANDLE->level < STMT_TRACK_L0) {
        return false;
    }
    if (t_thrd.shemem_ptr_cxt.MyBEEntry == NULL) {
        return false;
    }
    return true;
}

/* reset the reused handle from freelist */
static void reset_statement_handle()
{
    CHECK_STMT_HANDLE();

    /* release dynamic memory of the entry */
    pfree_ext(CURRENT_STMT_METRIC_HANDLE->schema_name);
    pfree_ext(CURRENT_STMT_METRIC_HANDLE->application_name);
    pfree_ext(CURRENT_STMT_METRIC_HANDLE->query);
    pfree_ext(CURRENT_STMT_METRIC_HANDLE->query_plan);
    pfree_ext(CURRENT_STMT_METRIC_HANDLE->wait_events);

    /* release detail list from the entry */
    StatementDetailItem *cur_pos = CURRENT_STMT_METRIC_HANDLE->details.head;
    StatementDetailItem *pre_pos = NULL;
    while (cur_pos != NULL) {
        pre_pos = cur_pos;
        cur_pos = (StatementDetailItem*)(cur_pos->next);
        pfree_ext(pre_pos);
    }

    /* reset counter and stat */
    Bitmapset *tmpBitmap = CURRENT_STMT_METRIC_HANDLE->wait_events_bitmap;
    errno_t rc = memset_s(CURRENT_STMT_METRIC_HANDLE,
        sizeof(StatementStatContext), 0, sizeof(StatementStatContext));
    securec_check(rc, "\0", "\0");
    CURRENT_STMT_METRIC_HANDLE->wait_events_bitmap = tmpBitmap;
}

static void instr_stmt_reset_wait_events_bitmap()
{
    CHECK_STMT_HANDLE();
    if (CURRENT_STMT_METRIC_HANDLE->wait_events_bitmap != NULL) {
        errno_t rc = memset_s((void*)CURRENT_STMT_METRIC_HANDLE->wait_events_bitmap->words,
            sizeof(bitmapword) * CURRENT_STMT_METRIC_HANDLE->wait_events_bitmap->nwords,
            0, sizeof(bitmapword) * CURRENT_STMT_METRIC_HANDLE->wait_events_bitmap->nwords);
        securec_check(rc, "\0", "\0");
    }
}

/* alloc handle for current session */
void statement_init_metric_context()
{
    StatementStatContext *reusedHandle = NULL;

    /* won't assign handle when statement flush thread not started */
    if (g_instance.pid_cxt.StatementPID == 0 || u_sess -> attr.attr_storage.DefaultXactReadOnly) {
        return;
    }
    CHECK_STMT_TRACK_ENABLED();

    /* create context under TopMemoryContext */
    if (u_sess->statement_cxt.stmt_stat_cxt == NULL) {
        u_sess->statement_cxt.stmt_stat_cxt = AllocSetContextCreate(u_sess->top_mem_cxt, "TrackStmtContext",
            ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
        ereport(DEBUG1, (errmodule(MOD_INSTR), errmsg("init - stmt cxt: %p, parent cxt: %p",
            u_sess->statement_cxt.stmt_stat_cxt, u_sess->top_mem_cxt)));
    }
    init_full_sql_wait_events();

    /* commit for previous allocated handle */
    if (u_sess->statement_cxt.curStatementMetrics != NULL) {
        statement_commit_metirc_context();
    }

    HOLD_INTERRUPTS();
    (void)syscalllockAcquire(&u_sess->statement_cxt.list_protect);

    PG_TRY();
    {
        /* 1, check free list: free detail stat; reuse entry in free list */
        if (u_sess->statement_cxt.free_count > 0) {
            reusedHandle = (StatementStatContext*)u_sess->statement_cxt.toFreeStatementList;
            u_sess->statement_cxt.curStatementMetrics = reusedHandle;
            u_sess->statement_cxt.toFreeStatementList = reusedHandle->next;
            u_sess->statement_cxt.free_count--;
        } else {
            /* 2, no free slot int free list, allocate new one */
            if (u_sess->statement_cxt.allocatedCxtCnt < u_sess->attr.attr_common.track_stmt_session_slot) {
                MemoryContext oldcontext = MemoryContextSwitchTo(u_sess->statement_cxt.stmt_stat_cxt);

                u_sess->statement_cxt.curStatementMetrics = palloc0_noexcept(sizeof(StatementStatContext));
                if (u_sess->statement_cxt.curStatementMetrics != NULL) {
                    u_sess->statement_cxt.allocatedCxtCnt++;
                }
                (void)MemoryContextSwitchTo(oldcontext);
            }
        }
    }
    PG_CATCH();
    {
        (void)syscalllockRelease(&u_sess->statement_cxt.list_protect);
        RESUME_INTERRUPTS();
        PG_RE_THROW();
    }
    PG_END_TRY();
    (void)syscalllockRelease(&u_sess->statement_cxt.list_protect);
    RESUME_INTERRUPTS();

    ereport(DEBUG1, (errmodule(MOD_INSTR), errmsg("[Statement] init - free list length: %d, suspend list length: %d",
        u_sess->statement_cxt.free_count, u_sess->statement_cxt.suspend_count)));

    if (CURRENT_STMT_METRIC_HANDLE == NULL) {
        if (u_sess->statement_cxt.allocatedCxtCnt >= u_sess->attr.attr_common.track_stmt_session_slot) {
            ereport(LOG, (errmodule(MOD_INSTR), errmsg("[Statement] no free slot for statement entry!")));
        } else {
            ereport(LOG, (errmodule(MOD_INSTR), errmsg("[Statement] OOM for statement entry!")));
        }
    } else {
        /* clear handler before reuse it */
        if (reusedHandle == CURRENT_STMT_METRIC_HANDLE) {
            reset_statement_handle();
        }

        instr_stmt_report_stat_at_handle_init();
        instr_stmt_reset_wait_events_bitmap();
        if (is_stmt_wait_events_enabled()) {
            u_sess->statement_cxt.enable_wait_events_bitmap = true;
            instr_stmt_copy_wait_events();
        }
        if (IsConnFromCoord()) {
            instr_stmt_dynamic_change_level();
        }
    }
}

/*
 * for PBE case, now init statement handle in message 'B', but for JDBC application
 * which sets fetch size, message looks like 'PBDES/ES/ES..', the handle will be commmitted
 * after message 'S', so for 'ES/ES' will be no stmt handle in message 'E'.
 */
void statement_init_metric_context_if_needs()
{
    if (CURRENT_STMT_METRIC_HANDLE == NULL) {
        statement_init_metric_context();
        instr_stmt_report_start_time();
    }
}

static void print_stmt_common_debug_log(int log_level)
{
    ereport(log_level, (errmodule(MOD_INSTR), errmsg("*************** statement handle information************")));
    ereport(log_level, (errmodule(MOD_INSTR), errmsg("0, ----statement level: %d", CURRENT_STMT_METRIC_HANDLE->level)));
    ereport(log_level, (errmodule(MOD_INSTR), errmsg("1, ----Basic Information Area(common)----")));
    ereport(log_level, (errmodule(MOD_INSTR), errmsg("\t Database: %s", u_sess->statement_cxt.db_name)));
    ereport(log_level, (errmodule(MOD_INSTR),
        errmsg("\t Origin node: %u", CURRENT_STMT_METRIC_HANDLE->unique_sql_cn_id)));
    ereport(log_level, (errmodule(MOD_INSTR), errmsg("\t User: %s", u_sess->statement_cxt.user_name)));
    ereport(log_level, (errmodule(MOD_INSTR), errmsg("\t Client Addr: %s", u_sess->statement_cxt.client_addr)));
    ereport(log_level, (errmodule(MOD_INSTR), errmsg("\t Client Port: %d", u_sess->statement_cxt.client_port)));
    ereport(log_level, (errmodule(MOD_INSTR), errmsg("\t Session Id: %lu", u_sess->statement_cxt.session_id)));
}

static void print_stmt_basic_debug_log(int log_level)
{
    ereport(log_level, (errmodule(MOD_INSTR), errmsg("2, ----Basic Information Area(related to SQL)----")));
    ereport(log_level, (errmodule(MOD_INSTR), errmsg("\t schema name: %s", CURRENT_STMT_METRIC_HANDLE->schema_name)));
    ereport(log_level, (errmodule(MOD_INSTR), errmsg("\t application name: %s",
        CURRENT_STMT_METRIC_HANDLE->application_name)));

    ereport(log_level, (errmodule(MOD_INSTR), errmsg("\t start time: %s",
        timestamptz_to_str(CURRENT_STMT_METRIC_HANDLE->start_time))));
    ereport(log_level, (errmodule(MOD_INSTR), errmsg("\t end time: %s",
        timestamptz_to_str(CURRENT_STMT_METRIC_HANDLE->finish_time))));

    ereport(log_level, (errmodule(MOD_INSTR),
        errmsg("\t debug query id: %lu", CURRENT_STMT_METRIC_HANDLE->debug_query_id)));
    ereport(log_level,
        (errmodule(MOD_INSTR), errmsg("\t unique query id: %lu", CURRENT_STMT_METRIC_HANDLE->unique_query_id)));
    ereport(log_level, (errmodule(MOD_INSTR), errmsg("\t unique query: %s", CURRENT_STMT_METRIC_HANDLE->query)));
    ereport(log_level, (errmodule(MOD_INSTR),
        errmsg("\t slow query threshold: %ld", CURRENT_STMT_METRIC_HANDLE->slow_query_threshold)));
    ereport(log_level, (errmodule(MOD_INSTR),
        errmsg("\t slow query cause type: %u", CURRENT_STMT_METRIC_HANDLE->cause_type)));
    ereport(log_level, (errmodule(MOD_INSTR), errmsg("\t thread id: %lu", CURRENT_STMT_METRIC_HANDLE->tid)));
    ereport(log_level, (errmodule(MOD_INSTR), errmsg("\t transaction id: %lu", CURRENT_STMT_METRIC_HANDLE->txn_id)));
    ereport(log_level, (errmodule(MOD_INSTR),
        errmsg("\t soft parse: %lu", CURRENT_STMT_METRIC_HANDLE->parse.soft_parse)));
    ereport(log_level, (errmodule(MOD_INSTR),
        errmsg("\t hard parse: %lu", CURRENT_STMT_METRIC_HANDLE->parse.hard_parse)));
    if (CURRENT_STMT_METRIC_HANDLE->level == STMT_TRACK_L1 || CURRENT_STMT_METRIC_HANDLE->level == STMT_TRACK_L2) {
        ereport(log_level, (errmodule(MOD_INSTR),
            errmsg("\t query plan size: %lu", CURRENT_STMT_METRIC_HANDLE->plan_size)));
        ereport(log_level, (errmodule(MOD_INSTR),
            errmsg("\t query plan: %s", CURRENT_STMT_METRIC_HANDLE->query_plan)));
    }
}

static void print_stmt_time_model_debug_log(int log_level)
{
    ereport(log_level, (errmodule(MOD_INSTR), errmsg("3, ----Time Model Info Area(related to SQL)----")));
    ereport(log_level, (errmodule(MOD_INSTR),
        errmsg("\t DB time: %ld", CURRENT_STMT_METRIC_HANDLE->timeModel[DB_TIME])));
    ereport(log_level, (errmodule(MOD_INSTR),
        errmsg("\t CPU time: %ld", CURRENT_STMT_METRIC_HANDLE->timeModel[CPU_TIME])));
    ereport(log_level, (errmodule(MOD_INSTR),
        errmsg("\t execution time: %ld", CURRENT_STMT_METRIC_HANDLE->timeModel[EXECUTION_TIME])));
    ereport(log_level, (errmodule(MOD_INSTR),
        errmsg("\t parse time: %ld", CURRENT_STMT_METRIC_HANDLE->timeModel[PARSE_TIME])));
    ereport(log_level, (errmodule(MOD_INSTR),
        errmsg("\t plan time: %ld", CURRENT_STMT_METRIC_HANDLE->timeModel[PLAN_TIME])));
    ereport(log_level, (errmodule(MOD_INSTR),
        errmsg("\t rewrite time: %ld", CURRENT_STMT_METRIC_HANDLE->timeModel[REWRITE_TIME])));
    ereport(log_level, (errmodule(MOD_INSTR),
        errmsg("\t plpgsql exection time: %ld", CURRENT_STMT_METRIC_HANDLE->timeModel[PL_EXECUTION_TIME])));
    ereport(log_level, (errmodule(MOD_INSTR),
        errmsg("\t plpgsql compilation time: %ld", CURRENT_STMT_METRIC_HANDLE->timeModel[PL_COMPILATION_TIME])));
    ereport(log_level, (errmodule(MOD_INSTR),
        errmsg("\t net send time: %ld", CURRENT_STMT_METRIC_HANDLE->timeModel[NET_SEND_TIME])));
    ereport(log_level, (errmodule(MOD_INSTR),
        errmsg("\t data IO time: %ld", CURRENT_STMT_METRIC_HANDLE->timeModel[DATA_IO_TIME])));
}

static void print_stmt_row_activity_cache_io_debug_log(int log_level)
{
    ereport(log_level, (errmodule(MOD_INSTR),
        errmsg("4, ----Row Activity And Cache IO Info Area(related to SQL)----")));
    ereport(log_level, (errmodule(MOD_INSTR),
        errmsg("\t returned rows: %lu", CURRENT_STMT_METRIC_HANDLE->row_activity.returned_rows)));
    ereport(log_level, (errmodule(MOD_INSTR),
        errmsg("\t tuples fetched: %lu", CURRENT_STMT_METRIC_HANDLE->row_activity.tuples_fetched)));
    ereport(log_level, (errmodule(MOD_INSTR),
        errmsg("\t tuples returned: %lu", CURRENT_STMT_METRIC_HANDLE->row_activity.tuples_returned)));
    ereport(log_level, (errmodule(MOD_INSTR),
        errmsg("\t tuples inserted: %lu", CURRENT_STMT_METRIC_HANDLE->row_activity.tuples_inserted)));
    ereport(log_level, (errmodule(MOD_INSTR),
        errmsg("\t tuples updated: %lu", CURRENT_STMT_METRIC_HANDLE->row_activity.tuples_updated)));
    ereport(log_level, (errmodule(MOD_INSTR),
        errmsg("\t tuples deleted: %lu", CURRENT_STMT_METRIC_HANDLE->row_activity.tuples_deleted)));
    ereport(log_level, (errmodule(MOD_INSTR),
        errmsg("\t blocks fetched: %lu", CURRENT_STMT_METRIC_HANDLE->cache_io.blocks_fetched)));
    ereport(log_level, (errmodule(MOD_INSTR),
        errmsg("\t blocks hit: %lu", CURRENT_STMT_METRIC_HANDLE->cache_io.blocks_hit)));
}

static void print_stmt_net_debug_log(int log_level)
{
    ereport(log_level, (errmodule(MOD_INSTR), errmsg("5, ----Network Info Area----")));
    ereport(log_level, (errmodule(MOD_INSTR),
        errmsg("\t NET_SEND_TIMES: %ld", CURRENT_STMT_METRIC_HANDLE->networkInfo[NET_SEND_TIMES])));
    ereport(log_level, (errmodule(MOD_INSTR),
        errmsg("\t NET_SEND_N_CALLS: %ld", CURRENT_STMT_METRIC_HANDLE->networkInfo[NET_SEND_N_CALLS])));
    ereport(log_level, (errmodule(MOD_INSTR),
        errmsg("\t NET_SEND_SIZE: %ld", CURRENT_STMT_METRIC_HANDLE->networkInfo[NET_SEND_SIZE])));
    ereport(log_level, (errmodule(MOD_INSTR),
        errmsg("\t NET_RECV_TIMES: %ld", CURRENT_STMT_METRIC_HANDLE->networkInfo[NET_RECV_TIMES])));
    ereport(log_level, (errmodule(MOD_INSTR),
        errmsg("\t NET_RECV_N_CALLS: %ld", CURRENT_STMT_METRIC_HANDLE->networkInfo[NET_RECV_N_CALLS])));
    ereport(log_level, (errmodule(MOD_INSTR),
        errmsg("\t NET_RECV_SIZE: %ld", CURRENT_STMT_METRIC_HANDLE->networkInfo[NET_RECV_SIZE])));
    ereport(log_level, (errmodule(MOD_INSTR),
        errmsg("\t NET_STREAM_SEND_TIMES: %ld", CURRENT_STMT_METRIC_HANDLE->networkInfo[NET_STREAM_SEND_TIMES])));
    ereport(log_level, (errmodule(MOD_INSTR),
        errmsg("\t NET_STREAM_SEND_N_CALLS: %ld", CURRENT_STMT_METRIC_HANDLE->networkInfo[NET_STREAM_SEND_N_CALLS])));
    ereport(log_level, (errmodule(MOD_INSTR),
        errmsg("\t NET_STREAM_SEND_SIZE: %ld", CURRENT_STMT_METRIC_HANDLE->networkInfo[NET_STREAM_SEND_SIZE])));
    ereport(log_level, (errmodule(MOD_INSTR),
        errmsg("\t NET_STREAM_RECV_TIMES: %ld", CURRENT_STMT_METRIC_HANDLE->networkInfo[NET_STREAM_RECV_TIMES])));
    ereport(log_level, (errmodule(MOD_INSTR),
        errmsg("\t NET_STREAM_RECV_N_CALLS: %ld", CURRENT_STMT_METRIC_HANDLE->networkInfo[NET_STREAM_RECV_N_CALLS])));
    ereport(log_level, (errmodule(MOD_INSTR),
        errmsg("\t NET_STREAM_RECV_SIZE: %ld", CURRENT_STMT_METRIC_HANDLE->networkInfo[NET_STREAM_RECV_SIZE])));
}

static void print_stmt_summary_lock_debug_log(int log_level)
{
    ereport(log_level, (errmodule(MOD_INSTR), errmsg("6, ----Lock Summary Info Area----")));
    if (CURRENT_STMT_METRIC_HANDLE->level == STMT_TRACK_L1 || CURRENT_STMT_METRIC_HANDLE->level == STMT_TRACK_L2) {
        ereport(log_level, (errmodule(MOD_INSTR),
            errmsg("\t lock cnt: %ld", CURRENT_STMT_METRIC_HANDLE->lock_summary.lock_cnt)));
        ereport(log_level, (errmodule(MOD_INSTR),
            errmsg("\t lock wait cnt: %ld", CURRENT_STMT_METRIC_HANDLE->lock_summary.lock_wait_cnt)));
        ereport(log_level, (errmodule(MOD_INSTR),
            errmsg("\t lock max cnt: %ld", CURRENT_STMT_METRIC_HANDLE->lock_summary.lock_max_cnt)));
        ereport(log_level, (errmodule(MOD_INSTR),
            errmsg("\t lwlock cnt: %ld", CURRENT_STMT_METRIC_HANDLE->lock_summary.lwlock_cnt)));
        ereport(log_level, (errmodule(MOD_INSTR),
            errmsg("\t lwlock wait cnt: %ld", CURRENT_STMT_METRIC_HANDLE->lock_summary.lwlock_wait_cnt)));
    }
    if (CURRENT_STMT_METRIC_HANDLE->level == STMT_TRACK_L2) {
        ereport(log_level, (errmodule(MOD_INSTR),
            errmsg("\t lock start time: %ld", CURRENT_STMT_METRIC_HANDLE->lock_summary.lock_start_time)));
        ereport(log_level, (errmodule(MOD_INSTR),
            errmsg("\t lock time: %ld", CURRENT_STMT_METRIC_HANDLE->lock_summary.lock_time)));
        ereport(log_level, (errmodule(MOD_INSTR),
            errmsg("\t lock wait start time: %ld", CURRENT_STMT_METRIC_HANDLE->lock_summary.lock_wait_start_time)));
        ereport(log_level, (errmodule(MOD_INSTR),
            errmsg("\t lock wait time: %ld", CURRENT_STMT_METRIC_HANDLE->lock_summary.lock_wait_time)));
        ereport(log_level, (errmodule(MOD_INSTR),
            errmsg("\t lwlock start time: %ld", CURRENT_STMT_METRIC_HANDLE->lock_summary.lwlock_start_time)));
        ereport(log_level, (errmodule(MOD_INSTR),
            errmsg("\t lwlock time: %ld", CURRENT_STMT_METRIC_HANDLE->lock_summary.lwlock_time)));
        ereport(log_level, (errmodule(MOD_INSTR),
            errmsg("\t lwlock wait start time: %ld",
                   CURRENT_STMT_METRIC_HANDLE->lock_summary.lwlock_wait_start_time)));
        ereport(log_level, (errmodule(MOD_INSTR),
            errmsg("\t lwlock wait time: %ld", CURRENT_STMT_METRIC_HANDLE->lock_summary.lwlock_wait_time)));
    }
}

static void print_stmt_detail_lock_debug_log(int log_level)
{
    ereport(log_level, (errmodule(MOD_INSTR), errmsg("7, ----Detail Information Area----")));
    if (CURRENT_STMT_METRIC_HANDLE->level == STMT_TRACK_L2) {
        ereport(log_level, (errmodule(MOD_INSTR),
            errmsg("\t guc statement_details_size : %ld", u_sess->attr.attr_common.track_stmt_details_size)));
        ereport(log_level, (errmodule(MOD_INSTR),
            errmsg("\t items num: %d", CURRENT_STMT_METRIC_HANDLE->details.n_items)));
        ereport(log_level, (errmodule(MOD_INSTR),
            errmsg("\t the write position of the last statement detail item: %u",
                   CURRENT_STMT_METRIC_HANDLE->details.cur_pos)));
    }
}

static void print_stmt_wait_event_log(int log_level)
{
    ereport(log_level, (errmodule(MOD_INSTR), errmsg("8, ----Events Information Area----")));
    ereport(log_level, (errmodule(MOD_INSTR),
        errmsg("\t wait events count: %d", bms_num_members(CURRENT_STMT_METRIC_HANDLE->wait_events_bitmap))));
}

static void print_stmt_debug_log()
{
    int log_level = DEBUG2;
    if (u_sess->attr.attr_common.log_min_messages > log_level)
        return;

    print_stmt_common_debug_log(log_level);
    print_stmt_basic_debug_log(log_level);
    print_stmt_time_model_debug_log(log_level);
    print_stmt_row_activity_cache_io_debug_log(log_level);
    print_stmt_net_debug_log(log_level);
    print_stmt_summary_lock_debug_log(log_level);
    print_stmt_detail_lock_debug_log(log_level);
    print_stmt_wait_event_log(log_level);
}

/* put current handle to suspend list */
void statement_commit_metirc_context()
{
    CHECK_STMT_HANDLE();

    instr_stmt_report_stat_at_handle_commit();

    instr_stmt_diff_wait_events();
    u_sess->statement_cxt.enable_wait_events_bitmap = false;
    print_stmt_debug_log();

    (void)syscalllockAcquire(&u_sess->statement_cxt.list_protect);

    /*
     * Rules to persist handle to statement_history
     * - ignore record to persist (to statement_history) if unique sql id = 0
     * - dynamic tracked sql(dynamic_track_level >= L0)
     * - full sql(statement_level[0] >= L0)
     * - slow sql(statement_leve[1] >= L0 && duration >= log_min_duration_statement && log_min_duration_statement > 0)
     */
    if (CURRENT_STMT_METRIC_HANDLE->unique_query_id != 0 &&
        (CURRENT_STMT_METRIC_HANDLE->dynamic_track_level >= STMT_TRACK_L0 ||
        u_sess->statement_cxt.statement_level[0] >= STMT_TRACK_L0 ||
        (u_sess->statement_cxt.statement_level[1] >= STMT_TRACK_L0 &&
        (CURRENT_STMT_METRIC_HANDLE->finish_time - CURRENT_STMT_METRIC_HANDLE->start_time) >=
        CURRENT_STMT_METRIC_HANDLE->slow_query_threshold &&
        CURRENT_STMT_METRIC_HANDLE->slow_query_threshold >= 0 &&
        (!u_sess->attr.attr_common.track_stmt_parameter ||
         (u_sess->attr.attr_common.track_stmt_parameter && CURRENT_STMT_METRIC_HANDLE->timeModel[0] > 0))))) {
        /* need to persist, put to suspend list */
        CURRENT_STMT_METRIC_HANDLE->next = u_sess->statement_cxt.suspendStatementList;
        u_sess->statement_cxt.suspendStatementList = CURRENT_STMT_METRIC_HANDLE;
        u_sess->statement_cxt.suspend_count++;
    } else {
        /* not need to persist, put to free list */
        CURRENT_STMT_METRIC_HANDLE->next = u_sess->statement_cxt.toFreeStatementList;
        u_sess->statement_cxt.toFreeStatementList = CURRENT_STMT_METRIC_HANDLE;
        u_sess->statement_cxt.free_count++;
    }

    (void)syscalllockRelease(&u_sess->statement_cxt.list_protect);
    u_sess->statement_cxt.curStatementMetrics = NULL;

    ereport(DEBUG1, (errmodule(MOD_INSTR),
        errmsg("[Statement] commit - free list length: %d, suspend list length: %d",
               u_sess->statement_cxt.free_count, u_sess->statement_cxt.suspend_count)));
}

void release_statement_context(PgBackendStatus* beentry, const char* func, int line)
{
    ereport(DEBUG1, (errmodule(MOD_INSTR),
        errmsg("release_statement_context - %s:%d, entry: %p", func, line, beentry)));

    if (beentry == NULL || beentry->statement_cxt == NULL) {
        ereport(DEBUG1, (errmodule(MOD_INSTR), errmsg("[Statement] release_statement_context - nothing to do.")));
        return;
    }

    (void)syscalllockAcquire(&beentry->statement_cxt_lock);
    MemoryContext stmtCxt = ((knl_u_statement_context *)(beentry->statement_cxt))->stmt_stat_cxt;
    beentry->statement_cxt = NULL;
    (void)syscalllockRelease(&beentry->statement_cxt_lock);

    /* to avoid using the handle, mark it to NULL */
    u_sess->statement_cxt.curStatementMetrics = NULL;
    if (stmtCxt != NULL) {
        MemoryContextDelete(stmtCxt);
    }
    u_sess->statement_cxt.stmt_stat_cxt = NULL;

    ereport(DEBUG1, (errmodule(MOD_INSTR), errmsg("[Statement] release_statement_context end - entry:%p", beentry)));
}

void* bind_statement_context()
{
    if (!u_sess->attr.attr_common.enable_stmt_track)
        return NULL;

    switch (t_thrd.role) {
        case WORKER:
        case THREADPOOL_WORKER:
        case THREADPOOL_STREAM:
        case STREAM_WORKER:
        case JOB_WORKER:
            ereport(DEBUG1, (errmodule(MOD_INSTR),
                errmsg("[Statement] bind backend entry - entry: %p, statement cxt: %p",
                       t_thrd.shemem_ptr_cxt.MyBEEntry, &u_sess->statement_cxt)));
            return &u_sess->statement_cxt;
        default:
            break;
    }
    return NULL;
}

/*
 * Save the old parent statement information before execution.
 */
void PLSQLStmtTrackStack::save_old_info()
{
    old_unique_sql_id = u_sess->unique_sql_cxt.unique_sql_id;
    old_parent_unique_sql_id = u_sess->unique_sql_cxt.parent_unique_sql_id;
    old_is_top_unique_sql = IsTopUniqueSQL();
    old_is_multi_unique_sql = u_sess->unique_sql_cxt.is_multi_unique_sql;
    old_force_gen_unique_sql = u_sess->unique_sql_cxt.force_generate_unique_sql;
    old_multi_sql_offset = u_sess->unique_sql_cxt.multi_sql_offset;
    old_curr_single_unique_sql = u_sess->unique_sql_cxt.curr_single_unique_sql;
}

/*
 * Reset the information of the current statement.
 */
void PLSQLStmtTrackStack::reset_current_info()
{
    u_sess->unique_sql_cxt.parent_unique_sql_id = u_sess->unique_sql_cxt.unique_sql_id;
    u_sess->unique_sql_cxt.unique_sql_id = 0;
    u_sess->debug_query_id = generate_unique_id64(&gt_queryId);
    pgstat_report_queryid(u_sess->debug_query_id);
    if (old_is_top_unique_sql) {
        SetIsTopUniqueSQL(false);
    }
    if (old_is_multi_unique_sql) {
        u_sess->unique_sql_cxt.is_multi_unique_sql = false;
        u_sess->unique_sql_cxt.multi_sql_offset = 0;
    }
    u_sess->unique_sql_cxt.curr_single_unique_sql = NULL;
    u_sess->unique_sql_cxt.force_generate_unique_sql = true;
}

/*
 * When we want to record the PL/SQL within a procedure or function,
 * Save the parent statement information and initialize the current statement
 * before execution.
 */
void PLSQLStmtTrackStack::push()
{
    if (IsInitdb || (IS_UNIQUE_SQL_TRACK_TOP && !u_sess->unique_sql_cxt.is_open_cursor) || 
        CURRENT_STMT_METRIC_HANDLE == NULL) {
        return;
    }

    uint64 old_debug_query_id = u_sess->debug_query_id;
    save_old_info();
    reset_current_info();
    parent_handler = CURRENT_STMT_METRIC_HANDLE;
    u_sess->statement_cxt.curStatementMetrics = NULL;
    parent_handler->debug_query_id = old_debug_query_id;
    statement_init_metric_context();
    instr_stmt_report_stat_at_handle_init();
    instr_stmt_report_start_time();

}

/*
 * After executing the PL/SQL,
 * upload the information of the current statement and restore the parent statement information.
 */
void PLSQLStmtTrackStack::pop()
{
    if (IsInitdb || (IS_UNIQUE_SQL_TRACK_TOP && !u_sess->unique_sql_cxt.is_open_cursor) || 
        CURRENT_STMT_METRIC_HANDLE == NULL) {
        return;
    }
    u_sess->unique_sql_cxt.unique_sql_id = old_unique_sql_id;
    u_sess->unique_sql_cxt.parent_unique_sql_id = old_parent_unique_sql_id;
    if (old_is_top_unique_sql) {
        SetIsTopUniqueSQL(true);
    }
    if (old_is_multi_unique_sql) {
        u_sess->unique_sql_cxt.is_multi_unique_sql = true;
        u_sess->unique_sql_cxt.multi_sql_offset = old_multi_sql_offset;
    }
    u_sess->unique_sql_cxt.curr_single_unique_sql = old_curr_single_unique_sql;
    u_sess->unique_sql_cxt.force_generate_unique_sql = old_force_gen_unique_sql;

    statement_commit_metirc_context();
    u_sess->statement_cxt.curStatementMetrics = parent_handler;
}