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
 * pagewriter.cpp
 *		Working mode of pagewriter thread, coordinator pagewriter thread copy the dirty
 *		pages to double writer area, then distribute the dirty pages to sub-threads to
 *		flush page to data file.
 *
 * IDENTIFICATION
 *      src/gausskernel/process/postmaster/pagewriter.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/pagewriter.h"
#include "storage/barrier.h"
#include "storage/buf/bufmgr.h"
#include "storage/ipc.h"
#include "storage/smgr/smgr.h"
#include "storage/smgr/segment.h"
#include "storage/pmsignal.h"
#include "storage/standby.h"
#include "access/double_write.h"
#include "access/xlog.h"
#include "utils/aiomem.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/builtins.h"
#include "gssignal/gs_signal.h"
#include "gstrace/gstrace_infra.h"
#include "gstrace/postmaster_gstrace.h"
#include "ddes/dms/ss_dms_bufmgr.h"

#include <zstd.h>

#define MIN(A, B) ((B) < (A) ? (B) : (A))
#define MAX(A, B) ((B) > (A) ? (B) : (A))
#define FULL_CKPT g_instance.ckpt_cxt_ctl->flush_all_dirty_page

const float ONE_HALF = 0.5;
const int TEN_MILLISECOND = 10;
const int TEN_MICROSECOND = 10;
const int SECOND_TO_MILLISECOND = 1000;
const int MILLISECOND_TO_MICROSECOND = 1000;
const int EXIT_MODE_TWO = 2;
const int SIZE_OF_UINT64 = 8;
const int SIZE_OF_TWO_UINT64 = 16;
const float PAGE_QUEUE_SLOT_USED_MAX_PERCENTAGE = 0.8;
const int MAX_THREAD_NAME_LEN = 128;
/*
 * Dirty page queue need remain 2 slots, one used to push the dirty page,
 * another slots to ensure that the slot status is not cleared
 */
const int PAGE_QUEUE_SLOT_MIN_RESERVE_NUM = 2;

static TimestampTz g_last_snapshot_ts = 0;
static XLogRecPtr g_last_snapshot_lsn = InvalidXLogRecPtr;

/* Signal handlers */
static void ckpt_pagewriter_sighup_handler(SIGNAL_ARGS);
static void ckpt_pagewriter_sigint_handler(SIGNAL_ARGS);
static void ckpt_pagewriter_quick_die(SIGNAL_ARGS);
static void ckpt_pagewriter_request_shutdown_handler(SIGNAL_ARGS);
static void ckpt_pagewriter_sigusr1_handler(SIGNAL_ARGS);
static void HandlePageWriterMainInterrupts();

/* dirty queue handle function */
static void ckpt_try_skip_invalid_elem_in_queue_head();
static void ckpt_try_prune_dirty_page_queue();

/* candidate buffer list handle function */
static uint32 calculate_pagewriter_flush_num();
static void candidate_buf_push(CandidateList *list, int buf_id);
static void init_candidate_list();
static uint32 incre_ckpt_pgwr_flush_dirty_page(WritebackContext *wb_context,
    const CkptSortItem *dirty_buf_list, int start, int batch_num);
static void incre_ckpt_pgwr_flush_dirty_queue(WritebackContext *wb_context);
static void incre_ckpt_pgwr_scan_buf_pool(WritebackContext *wb_context);
static void push_to_candidate_list(BufferDesc *buf_desc);
static uint32 get_candidate_buf_and_flush_list(uint32 start, uint32 end, uint32 max_flush_num,
    bool *contain_hashbucket);
static int64 get_thread_candidate_nums(CandidateList *list);

const int XLOG_LSN_SWAP = 32;
Datum ckpt_view_get_node_name()
{
    if (g_instance.attr.attr_common.PGXCNodeName == NULL || g_instance.attr.attr_common.PGXCNodeName[0] == '\0') {
        return CStringGetTextDatum("not define");
    } else {
        return CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);
    }
}

Datum ckpt_view_get_actual_flush_num()
{
    return Int64GetDatum(g_instance.ckpt_cxt_ctl->page_writer_actual_flush);
}

Datum ckpt_view_get_last_flush_num()
{
    return Int32GetDatum(g_instance.ckpt_cxt_ctl->page_writer_last_flush);
}

Datum ckpt_view_get_remian_dirty_page_num()
{
    return Int64GetDatum(g_instance.ckpt_cxt_ctl->actual_dirty_page_num);
}

const int LSN_LENGTH = 64;
Datum ckpt_view_get_min_rec_lsn()
{
    errno_t ret;
    char queue_rec_lsn_s[LSN_LENGTH];
    XLogRecPtr queue_rec_lsn = ckpt_get_min_rec_lsn();

    ret = memset_s(queue_rec_lsn_s, LSN_LENGTH, 0, LSN_LENGTH);
    securec_check(ret, "", "");

    ret = snprintf_s(queue_rec_lsn_s, LSN_LENGTH, LSN_LENGTH - 1, "%X/%X",
        (uint32)(queue_rec_lsn >> XLOG_LSN_SWAP), (uint32)queue_rec_lsn);
    securec_check_ss(ret, "", "");
    return CStringGetTextDatum(queue_rec_lsn_s);
}

Datum ckpt_view_get_queue_rec_lsn()
{
    errno_t ret;
    char queue_rec_lsn_s[LSN_LENGTH];
    XLogRecPtr queue_rec_lsn = get_dirty_page_queue_rec_lsn();

    ret = memset_s(queue_rec_lsn_s, LSN_LENGTH, 0, LSN_LENGTH);
    securec_check(ret, "", "");

    ret = snprintf_s(queue_rec_lsn_s, LSN_LENGTH, LSN_LENGTH - 1, "%X/%X",
        (uint32)(queue_rec_lsn >> XLOG_LSN_SWAP), (uint32)queue_rec_lsn);
    securec_check_ss(ret, "", "");
    return CStringGetTextDatum(queue_rec_lsn_s);
}

Datum ckpt_view_get_current_xlog_insert_lsn()
{
    errno_t ret;
    char current_lsn_s[LSN_LENGTH];
    XLogRecPtr current_xlog_insert = GetXLogInsertRecPtr();

    ret = memset_s(current_lsn_s, LSN_LENGTH, 0, LSN_LENGTH);
    securec_check(ret, "", "");

    ret = snprintf_s(current_lsn_s, LSN_LENGTH, LSN_LENGTH - 1, "%X/%X",
        (uint32)(current_xlog_insert >> XLOG_LSN_SWAP), (uint32)current_xlog_insert);
    securec_check_ss(ret, "", "");
    return CStringGetTextDatum(current_lsn_s);
}

Datum ckpt_view_get_redo_point()
{
    errno_t ret;
    char redo_lsn_s[LSN_LENGTH];
    XLogRecPtr redo_lsn = g_instance.ckpt_cxt_ctl->ckpt_view.ckpt_current_redo_point;

    ret = memset_s(redo_lsn_s, LSN_LENGTH, 0, LSN_LENGTH);
    securec_check(ret, "", "");

    ret = snprintf_s(
        redo_lsn_s, LSN_LENGTH, LSN_LENGTH - 1, "%X/%X", (uint32)(redo_lsn >> XLOG_LSN_SWAP), (uint32)redo_lsn);
    securec_check_ss(ret, "", "");
    return CStringGetTextDatum(redo_lsn_s);
}

Datum ckpt_view_get_clog_flush_num()
{
    return Int64GetDatum(g_instance.ckpt_cxt_ctl->ckpt_view.ckpt_clog_flush_num);
}

Datum ckpt_view_get_csnlog_flush_num()
{
    return Int64GetDatum(g_instance.ckpt_cxt_ctl->ckpt_view.ckpt_csnlog_flush_num);
}

Datum ckpt_view_get_multixact_flush_num()
{
    return Int64GetDatum(g_instance.ckpt_cxt_ctl->ckpt_view.ckpt_multixact_flush_num);
}

Datum ckpt_view_get_predicate_flush_num()
{
    return Int64GetDatum(g_instance.ckpt_cxt_ctl->ckpt_view.ckpt_predicate_flush_num);
}

Datum ckpt_view_get_twophase_flush_num()
{
    return Int64GetDatum(g_instance.ckpt_cxt_ctl->ckpt_view.ckpt_twophase_flush_num);
}

Datum ckpt_view_get_candidate_nums()
{
    int candidate_num = get_curr_candidate_nums(CAND_LIST_NORMAL);
    return Int32GetDatum(candidate_num);
}

Datum ckpt_view_get_num_candidate_list()
{
    return Int64GetDatum(g_instance.ckpt_cxt_ctl->get_buf_num_candidate_list);
}

Datum ckpt_view_get_num_clock_sweep()
{
    return Int64GetDatum(g_instance.ckpt_cxt_ctl->get_buf_num_clock_sweep);
}

Datum ckpt_view_get_seg_candidate_nums()
{
    int candidate_num = get_curr_candidate_nums(CAND_LIST_SEG);
    return Int32GetDatum(candidate_num);
}

Datum ckpt_view_seg_get_num_candidate_list()
{
    return Int64GetDatum(g_instance.ckpt_cxt_ctl->seg_get_buf_num_candidate_list);
}

Datum ckpt_view_seg_get_num_clock_sweep()
{
    return Int64GetDatum(g_instance.ckpt_cxt_ctl->seg_get_buf_num_clock_sweep);
}

const incre_ckpt_view_col g_pagewriter_view_col[PAGEWRITER_VIEW_COL_NUM] = {
    {"node_name", TEXTOID, ckpt_view_get_node_name},
    {"pgwr_actual_flush_total_num", INT8OID, ckpt_view_get_actual_flush_num},
    {"pgwr_last_flush_num", INT4OID, ckpt_view_get_last_flush_num},
    {"remain_dirty_page_num", INT8OID, ckpt_view_get_remian_dirty_page_num},
    {"queue_head_page_rec_lsn", TEXTOID, ckpt_view_get_min_rec_lsn},
    {"queue_rec_lsn", TEXTOID, ckpt_view_get_queue_rec_lsn},
    {"current_xlog_insert_lsn", TEXTOID, ckpt_view_get_current_xlog_insert_lsn},
    {"ckpt_redo_point", TEXTOID, ckpt_view_get_redo_point}};

const incre_ckpt_view_col g_pagewirter_view_two_col[CANDIDATE_VIEW_COL_NUM] = {
    {"node_name", TEXTOID, ckpt_view_get_node_name},
    {"candidate_slots", INT4OID, ckpt_view_get_candidate_nums},
    {"get_buf_from_list", INT8OID, ckpt_view_get_num_candidate_list},
    {"get_buf_clock_sweep", INT8OID, ckpt_view_get_num_clock_sweep},
    {"seg_candidate_slots", INT4OID, ckpt_view_get_seg_candidate_nums},
    {"seg_get_buf_from_list", INT8OID, ckpt_view_seg_get_num_candidate_list},
    {"seg_get_buf_clock_sweep", INT8OID, ckpt_view_seg_get_num_clock_sweep}
};


const incre_ckpt_view_col g_ckpt_view_col[INCRE_CKPT_VIEW_COL_NUM] = {{"node_name", TEXTOID, ckpt_view_get_node_name},
    {"ckpt_redo_point", TEXTOID, ckpt_view_get_redo_point},
    {"ckpt_clog_flush_num", INT8OID, ckpt_view_get_clog_flush_num},
    {"ckpt_csnlog_flush_num", INT8OID, ckpt_view_get_csnlog_flush_num},
    {"ckpt_multixact_flush_num", INT8OID, ckpt_view_get_multixact_flush_num},
    {"ckpt_predicate_flush_num", INT8OID, ckpt_view_get_predicate_flush_num},
    {"ckpt_twophase_flush_num", INT8OID, ckpt_view_get_twophase_flush_num}};

uint64 get_time_ms()
{
    struct timeval tv;
    uint64 time_ms;

    (void)gettimeofday(&tv, NULL);
    time_ms = (int64)tv.tv_sec * 1000 + (int64)tv.tv_usec / 1000;
    return time_ms;
}

bool IsPagewriterProcess(void)
{
    return (t_thrd.role == PAGEWRITER_THREAD);
}

const int MAX_DIRTY_LIST_FLUSH_NUM = 1000 * DW_DIRTY_PAGE_MAX_FOR_NOHBK;

void incre_ckpt_pagewriter_cxt_init()
{
    MemoryContext oldcontext = MemoryContextSwitchTo(g_instance.increCheckPoint_context);
    int thread_num = g_instance.attr.attr_storage.pagewriter_thread_num + 1;  /* sub thread + one main thread */

    g_instance.ckpt_cxt_ctl->pgwr_procs.writer_proc = (PageWriterProc*)palloc0(sizeof(PageWriterProc) * thread_num);
    g_instance.ckpt_cxt_ctl->pgwr_procs.num = thread_num;
    g_instance.ckpt_cxt_ctl->pgwr_procs.sub_num = g_instance.attr.attr_storage.pagewriter_thread_num;
    g_instance.ckpt_cxt_ctl->pgwr_procs.running_num = 0;

    g_instance.ckpt_cxt_ctl->prepared = 0;
    g_instance.ckpt_cxt_ctl->CkptBufferIdsTail = 0;
    g_instance.ckpt_cxt_ctl->CkptBufferIdsFlushPages = 0;
    g_instance.ckpt_cxt_ctl->CkptBufferIdsCompletedPages = 0;
    g_instance.ckpt_cxt_ctl->page_writer_sub_can_exit = false;

    uint32 dirty_list_size = MAX_DIRTY_LIST_FLUSH_NUM / thread_num;

    /* init thread dw cxt  and dirty list */
    for (int i = 0; i < thread_num; i++) {
        PageWriterProc *pgwr = &g_instance.ckpt_cxt_ctl->pgwr_procs.writer_proc[i];
        char *unaligned_buf = (char*)palloc0((DW_BUF_MAX_FOR_NOHBK + 1) * BLCKSZ);
        pgwr->thrd_dw_cxt.dw_buf = (char*)TYPEALIGN(BLCKSZ, unaligned_buf);
        pgwr->thrd_dw_cxt.dw_page_idx = -1;
        pgwr->thrd_dw_cxt.is_new_relfilenode = false;
        pgwr->dirty_list_size = dirty_list_size;
        pgwr->dirty_buf_list = (CkptSortItem *)palloc0(dirty_list_size * sizeof(CkptSortItem));
    }

    if (ENABLE_DMS) {
        /* initialize aio block buffer */
        for (int i = 1; i < thread_num; i++) {
            PageWriterProc *pgwr = &g_instance.ckpt_cxt_ctl->pgwr_procs.writer_proc[i];
            /* 2M AIO buffer */
            char *unaligned_buf = (char *)palloc0(DSS_AIO_BATCH_SIZE * DSS_AIO_UTIL_NUM * BLCKSZ + BLCKSZ);
            pgwr->aio_buf = (char *)TYPEALIGN(BLCKSZ, unaligned_buf);
        }
    }

    init_candidate_list();
    (void)MemoryContextSwitchTo(oldcontext);
}

void candidate_buf_init(void)
{
    bool found_candidate_buf = false;
    bool found_candidate_fm = false;
    int buffer_num = TOTAL_BUFFER_NUM;

    if (!ENABLE_INCRE_CKPT) {
        return;
    }
    /*
     * Each thread manages a part of the buffer. Several slots are reserved to
     * prevent the thread first and last slots equals.
     */
    g_instance.ckpt_cxt_ctl->candidate_buffers = (Buffer *)
        ShmemInitStruct("CandidateBuffers", buffer_num * sizeof(Buffer), &found_candidate_buf);
    g_instance.ckpt_cxt_ctl->candidate_free_map = (bool *)
        ShmemInitStruct("CandidateFreeMap", buffer_num * sizeof(bool), &found_candidate_fm);

    if (found_candidate_buf || found_candidate_fm) {
        Assert(found_candidate_buf && found_candidate_fm);
    } else {
        /* The memory of the memset sometimes exceeds 2 GB. so, memset_s cannot be used. */
        MemSet((char*)g_instance.ckpt_cxt_ctl->candidate_buffers, 0, buffer_num * sizeof(Buffer));
        MemSet((char*)g_instance.ckpt_cxt_ctl->candidate_free_map, 0, buffer_num * sizeof(bool));

        /* switchover will triggers the following code */
        if (g_instance.ckpt_cxt_ctl->pgwr_procs.writer_proc != NULL) {
            init_candidate_list();
        }
    }
}

static void init_candidate_list()
{
    int thread_num = g_instance.ckpt_cxt_ctl->pgwr_procs.sub_num;
    int normal_avg_num = NORMAL_SHARED_BUFFER_NUM / thread_num;
    int nvm_avg_num = NVM_BUFFER_NUM / thread_num;
    int seg_avg_num = SEGMENT_BUFFER_NUM / thread_num;
    PageWriterProc *pgwr = NULL;
    Buffer *cand_buffers = g_instance.ckpt_cxt_ctl->candidate_buffers;

    /* Init main thread, the candidate list only store segment buffer */
    pgwr = &g_instance.ckpt_cxt_ctl->pgwr_procs.writer_proc[0];
    INIT_CANDIDATE_LIST(pgwr->normal_list, NULL, 0, 0, 0);
    INIT_CANDIDATE_LIST(pgwr->nvm_list, NULL, 0, 0, 0);
    INIT_CANDIDATE_LIST(pgwr->seg_list, NULL, 0, 0, 0);

    for (int i = 1; i <= thread_num; i++) {
        pgwr = &g_instance.ckpt_cxt_ctl->pgwr_procs.writer_proc[i];
        int start = normal_avg_num * (i - 1);
        int end = start + normal_avg_num;
        int nvm_start = NvmBufferStartID + nvm_avg_num * (i - 1);
        int nvm_end = nvm_start + nvm_avg_num;
        int seg_start = SegmentBufferStartID + seg_avg_num * (i - 1);
        int seg_end = seg_start + seg_avg_num;
        if (i == thread_num) {
            end += NORMAL_SHARED_BUFFER_NUM % thread_num;
            nvm_end += NVM_BUFFER_NUM % thread_num;
            seg_end += SEGMENT_BUFFER_NUM % thread_num;
        }

        INIT_CANDIDATE_LIST(pgwr->normal_list, &cand_buffers[start], end - start, 0, 0);
        INIT_CANDIDATE_LIST(pgwr->nvm_list, &cand_buffers[nvm_start], nvm_end - nvm_start, 0, 0);
        INIT_CANDIDATE_LIST(pgwr->seg_list, &cand_buffers[seg_start], seg_end - seg_start, 0, 0);
        pgwr->normal_list.buf_id_start = start;
        pgwr->nvm_list.buf_id_start = nvm_start;
        pgwr->seg_list.buf_id_start = seg_start;
    }
}

int get_dirty_page_queue_head_buffer()
{
    uint64 dirty_queue_head = pg_atomic_read_u64(&g_instance.ckpt_cxt_ctl->dirty_page_queue_head);
    uint64 actual_loc = dirty_queue_head % g_instance.ckpt_cxt_ctl->dirty_page_queue_size;
    return g_instance.ckpt_cxt_ctl->dirty_page_queue[actual_loc].buffer;
}

bool is_dirty_page_queue_full(BufferDesc* buf)
{
    if ((get_dirty_page_num() >=
            g_instance.ckpt_cxt_ctl->dirty_page_queue_size * PAGE_QUEUE_SLOT_USED_MAX_PERCENTAGE) &&
        g_instance.ckpt_cxt_ctl->backend_wait_lock != buf->content_lock) {
        Buffer queue_head_buffer = get_dirty_page_queue_head_buffer();
        if (!BufferIsInvalid(queue_head_buffer)) {
            BufferDesc* queue_head_buffer_desc = GetBufferDescriptor(queue_head_buffer - 1);
            if (!LWLockHeldByMeInMode(queue_head_buffer_desc->content_lock, LW_EXCLUSIVE)) {
                return true;
            }
        } else {
            return true;
        }
    }

    return false;
}

#if (defined(__x86_64__) || defined(__aarch64__)) && !defined(__USE_SPINLOCK)
bool atomic_push_pending_flush_queue(XLogRecPtr* queue_head_lsn, uint64* new_tail_loc)
{
    uint128_u compare;
    uint128_u exchange;
    uint128_u current;

    compare = atomic_compare_and_swap_u128((uint128_u*)&g_instance.ckpt_cxt_ctl->dirty_page_queue_reclsn);
    Assert(sizeof(g_instance.ckpt_cxt_ctl->dirty_page_queue_reclsn) == SIZE_OF_UINT64);
    Assert(sizeof(g_instance.ckpt_cxt_ctl->dirty_page_queue_tail) == SIZE_OF_UINT64);

loop:
    exchange.u64[0] = compare.u64[0];
    exchange.u64[1] = compare.u64[1] + 1;
    *new_tail_loc = exchange.u64[1];

    if ((uint64)(get_dirty_page_num() + PAGE_QUEUE_SLOT_MIN_RESERVE_NUM) >=
        g_instance.ckpt_cxt_ctl->dirty_page_queue_size) {
        return false;
    }

    current = atomic_compare_and_swap_u128(
        (uint128_u*)&g_instance.ckpt_cxt_ctl->dirty_page_queue_reclsn, compare, exchange);

    if (!UINT128_IS_EQUAL(compare, current)) {
        UINT128_COPY(compare, current);
        goto loop;
    }

    *queue_head_lsn = current.u64[0];
    *new_tail_loc -= 1;
    return true;
}
#endif

bool push_pending_flush_queue(Buffer buffer)
{
    uint64 new_tail_loc = 0;
    uint64 actual_loc;
    XLogRecPtr queue_head_lsn = InvalidXLogRecPtr;
    BufferDesc* buf_desc = GetBufferDescriptor(buffer - 1);

    Assert(XLogRecPtrIsInvalid(pg_atomic_read_u64(&buf_desc->extra->rec_lsn)));
#if defined(__x86_64__) || defined(__aarch64__) && !defined(__USE_SPINLOCK)
    bool push_finish = false;
    push_finish = atomic_push_pending_flush_queue(&queue_head_lsn, &new_tail_loc);
    if (!push_finish) {
        return false;
    }
#else
    SpinLockAcquire(&g_instance.ckpt_cxt_ctl->queue_lock);

    if ((uint64)(g_instance.ckpt_cxt_ctl->dirty_page_queue_tail
                  - g_instance.ckpt_cxt_ctl->dirty_page_queue_head
                  + PAGE_QUEUE_SLOT_MIN_RESERVE_NUM) >=
        g_instance.ckpt_cxt_ctl->dirty_page_queue_size) {
        SpinLockRelease(&g_instance.ckpt_cxt_ctl->queue_lock);
        return false;
    }

    queue_head_lsn = g_instance.ckpt_cxt_ctl->dirty_page_queue_reclsn;
    new_tail_loc = g_instance.ckpt_cxt_ctl->dirty_page_queue_tail;
    g_instance.ckpt_cxt_ctl->dirty_page_queue_tail++;
    SpinLockRelease(&g_instance.ckpt_cxt_ctl->queue_lock);
#endif

    pg_atomic_write_u64(&buf_desc->extra->rec_lsn, queue_head_lsn);
    actual_loc = new_tail_loc % g_instance.ckpt_cxt_ctl->dirty_page_queue_size;
    buf_desc->extra->dirty_queue_loc = actual_loc;
    g_instance.ckpt_cxt_ctl->dirty_page_queue[actual_loc].buffer = buffer;
    pg_memory_barrier();
    pg_atomic_write_u32(&g_instance.ckpt_cxt_ctl->dirty_page_queue[actual_loc].slot_state, (SLOT_VALID));
    (void)pg_atomic_fetch_add_u32(&g_instance.ckpt_cxt_ctl->actual_dirty_page_num, 1);
    return true;
}

void remove_dirty_page_from_queue(BufferDesc* buf)
{
    Assert(buf->extra->dirty_queue_loc != PG_UINT64_MAX);
    g_instance.ckpt_cxt_ctl->dirty_page_queue[buf->extra->dirty_queue_loc].buffer = 0;
    pg_atomic_write_u64(&buf->extra->rec_lsn, InvalidXLogRecPtr);
    buf->extra->dirty_queue_loc = PG_UINT64_MAX;
    (void)pg_atomic_fetch_sub_u32(&g_instance.ckpt_cxt_ctl->actual_dirty_page_num, 1);
}

uint64 get_dirty_page_queue_tail()
{
    uint64 tail = 0;

#if defined(__x86_64__) || defined(__aarch64__) && !defined(__USE_SPINLOCK)
    tail = pg_atomic_barrier_read_u64(&g_instance.ckpt_cxt_ctl->dirty_page_queue_tail);
#else
    SpinLockAcquire(&g_instance.ckpt_cxt_ctl->queue_lock);
    tail = g_instance.ckpt_cxt_ctl->dirty_page_queue_tail;
    SpinLockRelease(&g_instance.ckpt_cxt_ctl->queue_lock);
#endif

    return tail;
}

int64 get_dirty_page_num()
{
    volatile uint64 dirty_page_head = pg_atomic_read_u64(&g_instance.ckpt_cxt_ctl->dirty_page_queue_head);
    uint64 dirty_page_tail = get_dirty_page_queue_tail();
    int64 page_num = dirty_page_tail - dirty_page_head;
    Assert(page_num >= 0);
    return page_num;
}

static uint32 ckpt_get_expected_flush_num()
{
    /*
     * Full checkpoint, need flush all dirty page.
     * The dw area limit the max numbers of dirty page is 818,
     */
    int flush_num = 0;
    int64 expected_flush_num;
    uint64 dirty_queue_head = pg_atomic_read_u64(&g_instance.ckpt_cxt_ctl->dirty_page_queue_head);

    if (FULL_CKPT) {
        expected_flush_num = g_instance.ckpt_cxt_ctl->full_ckpt_expected_flush_loc - dirty_queue_head;
    } else {
        expected_flush_num = get_dirty_page_queue_tail() - dirty_queue_head;
    }

    if (expected_flush_num <= 0) {
        /*
         * Possible in full checkpoint case. In full checkpoint case,
         * g_instance.ckpt_cxt_ctl->full_ckpt_expected_flush_loc is updated in
         * checkpoint thread. So it is possible to get (expected_flush_num < 0),
         * if the dirty queue head is moved beyond this marked position.
         * if expected_flush_num <= 0, the flush loc is equal to dirty page queue
         * head, full ckpt has finished, the flush_all_dirty_page can set false,
         */
        g_instance.ckpt_cxt_ctl->flush_all_dirty_page = false;
        return 0;
    }

    if (expected_flush_num > DW_DIRTY_PAGE_MAX_FOR_NOHBK) {
        flush_num = calculate_pagewriter_flush_num();
    }

    if (flush_num < DW_DIRTY_PAGE_MAX_FOR_NOHBK) {
        flush_num = DW_DIRTY_PAGE_MAX_FOR_NOHBK;
    }

    return (uint32)Min(expected_flush_num, flush_num);
}

/**
 * @Description: Select a batch of dirty pages from the dirty_page_queue and sort
 * @in           Dirty queue head from which to select the dirty pages
 * @in           Number of dirty pages that are expected to be flushed
 * @out          Offset to the new head
 * @return       Actual number of dirty pages need to flush
 */
const int MAX_SCAN_NUM = 131072;  /* 1GB buffers */
static uint32 ckpt_qsort_dirty_page_for_flush(bool *is_new_relfilenode, uint32 flush_queue_num)
{
    uint32 num_to_flush = 0;
    errno_t rc;
    uint32 i;
    uint32 scan_end = 0;
    int64 dirty_page_num;
    uint64 dirty_queue_head;
    uint32 buf_num = TOTAL_BUFFER_NUM;
    uint32 buffer_slot_num = MIN(flush_queue_num, buf_num);

    rc = memset_s(g_instance.ckpt_cxt_ctl->CkptBufferIds, buffer_slot_num * sizeof(CkptSortItem),
        0, buffer_slot_num * sizeof(CkptSortItem));
    securec_check(rc, "", "");

    /*
     * Before selecting a batch of dirty pages to flush, move dirty page queue head to
     * skip slot of invalid buffer of queue head.
     */
    ckpt_try_prune_dirty_page_queue();
    ckpt_try_skip_invalid_elem_in_queue_head();
    dirty_page_num = get_dirty_page_num();
    dirty_queue_head = pg_atomic_read_u64(&g_instance.ckpt_cxt_ctl->dirty_page_queue_head);
    scan_end = MIN(MAX_SCAN_NUM, dirty_page_num);

    for (i = 0; i < scan_end; i++) {
        uint64 buf_state;
        Buffer buffer;
        BufferDesc* buf_desc = NULL;
        CkptSortItem* item = NULL;
        uint64 temp_loc = (dirty_queue_head + i) % g_instance.ckpt_cxt_ctl->dirty_page_queue_size;
        volatile DirtyPageQueueSlot* slot = &g_instance.ckpt_cxt_ctl->dirty_page_queue[temp_loc];

        /* slot location is pre-occupied, but the buffer not set finish, need break. */
        if (!(pg_atomic_read_u32(&slot->slot_state) & SLOT_VALID)) {
            break;
        }
        pg_memory_barrier();
        buffer = slot->buffer;
        /* slot state is valid, buffer is invalid, the slot buffer set 0 when BufferAlloc or InvalidateBuffer */
        if (BufferIsInvalid(buffer)) {
            continue; /* this tempLoc maybe set 0 when remove dirty page */
        }

        buf_desc = GetBufferDescriptor(buffer - 1);
        buf_state = LockBufHdr(buf_desc);

        if (buf_state & BM_DIRTY) {
            buf_state |= BM_CHECKPOINT_NEEDED;
            item = &g_instance.ckpt_cxt_ctl->CkptBufferIds[num_to_flush++];
            item->buf_id = buffer - 1;
            item->tsId = buf_desc->tag.rnode.spcNode;
            item->relNode = buf_desc->tag.rnode.relNode;
            item->bucketNode = buf_desc->tag.rnode.bucketNode;
            item->forkNum = buf_desc->tag.forkNum;
            item->blockNum = buf_desc->tag.blockNum;
            if(IsSegmentFileNode(buf_desc->tag.rnode) || buf_desc->tag.rnode.opt != 0) {
                *is_new_relfilenode = true;
            }
        }

        UnlockBufHdr(buf_desc, buf_state);
        if (num_to_flush >= buffer_slot_num) {
            break;
        }
    }

    qsort(g_instance.ckpt_cxt_ctl->CkptBufferIds, num_to_flush, sizeof(CkptSortItem), ckpt_buforder_comparator);

    return num_to_flush;
}

static void wakeup_sub_thread()
{
    PageWriterProc *pgwr = NULL;
    for (int thread_loc = 1; thread_loc < g_instance.ckpt_cxt_ctl->pgwr_procs.num; thread_loc++) {
        pgwr = &g_instance.ckpt_cxt_ctl->pgwr_procs.writer_proc[thread_loc];
        pgwr->start_loc = 0;
        pgwr->need_flush_num = 0;

        if (pgwr->proc != NULL) {
            (void)pg_atomic_add_fetch_u32(&g_instance.ckpt_cxt_ctl->pgwr_procs.running_num, 1);
            g_instance.ckpt_cxt_ctl->is_standby_mode = RecoveryInProgress();
            pg_write_barrier();
            g_instance.ckpt_cxt_ctl->pgwr_procs.writer_proc[thread_loc].need_flush = true;
            pg_write_barrier();
            SetLatch(&(pgwr->proc->procLatch));
        }
    }
}

static void prepare_dirty_page_applied_state(uint32 requested_flush_num, bool is_new_relfilenode)
{
    pg_atomic_init_u32(&g_instance.ckpt_cxt_ctl->CkptBufferIdsCompletedPages, 0);
    pg_atomic_init_u32(&g_instance.ckpt_cxt_ctl->CkptBufferIdsTail, 0);
    g_instance.ckpt_cxt_ctl->CkptBufferIdsFlushPages = requested_flush_num;
    g_instance.dw_batch_cxt.is_new_relfilenode = is_new_relfilenode;
    pg_atomic_init_u32(&g_instance.ckpt_cxt_ctl->prepared, 1);
    pg_write_barrier();
}

/**
 * @Description: The main thread can move head only when other threads complete the page flush.
 * @in:          try_move_head, only first enter the loop, need move queue head.
 * @in:          offset_to_new_head, Upper limit of move queue head.
 * @in:          old_dirty_queue_head, Before entering the loop, the head of the dirty page queue.
 */
static void ckpt_move_queue_head_after_flush()
{
    uint64 dirty_queue_head = pg_atomic_read_u64(&g_instance.ckpt_cxt_ctl->dirty_page_queue_head);
    int64 dirty_page_num = get_dirty_page_num();
    uint32 rc;

    while (true) {
        /* wait all sub thread finish flush */
        if (pg_atomic_read_u32(&g_instance.ckpt_cxt_ctl->pgwr_procs.running_num) == 0) {
            /* Finish flush dirty page, move the dirty page queue head, and clear the slot state. */
            for (uint32 i = 0; i < dirty_page_num; i++) {
                uint64 temp_loc = (dirty_queue_head + i) % g_instance.ckpt_cxt_ctl->dirty_page_queue_size;
                volatile DirtyPageQueueSlot* slot = &g_instance.ckpt_cxt_ctl->dirty_page_queue[temp_loc];
                if (!(pg_atomic_read_u32(&slot->slot_state) & SLOT_VALID)) {
                    break;
                }
                pg_read_barrier();
                if (!BufferIsInvalid(slot->buffer)) {
                    /*
                     * This buffer could not be flushed as we failed to acquire the
                     * conditional lock on content_lock. The page_writer should start
                     * from this slot for the next iteration. So we cannot move the
                     * dirty page queue head anymore.
                     */
                    break;
                }

                (void)pg_atomic_fetch_add_u64(&g_instance.ckpt_cxt_ctl->dirty_page_queue_head, 1);
                pg_atomic_init_u32(&slot->slot_state, 0);
            }
            break;
        }
        ckpt_try_prune_dirty_page_queue();
        rc = WaitLatch(&t_thrd.proc->procLatch, WL_TIMEOUT | WL_POSTMASTER_DEATH, (long)TEN_MILLISECOND);
        if (rc & WL_POSTMASTER_DEATH) {
            gs_thread_exit(1);
        }
        HandlePageWriterMainInterrupts();
    }

    if (u_sess->attr.attr_storage.log_pagewriter) {
        ereport(LOG, (errmodule(MOD_INCRE_CKPT),
                errmsg("Page Writer flushed: %u pages, remaining dirty_page_num: %ld",
                    g_instance.ckpt_cxt_ctl->page_writer_last_flush, get_dirty_page_num())));
    }
    return;
}

/**
 * @Description: pagewriter main thread select one batch dirty page, divide this batch page to all thread,
 * wait all thread finish flush, update the statistics.
 */
static void ckpt_pagewriter_main_thread_flush_dirty_page()
{
    WritebackContext wb_context;
    uint32 requested_flush_num;
    int32 expected_flush_num;
    bool is_new_relfilenode = false;

    Assert(t_thrd.pagewriter_cxt.pagewriter_id == 0);
    WritebackContextInit(&wb_context, &t_thrd.pagewriter_cxt.page_writer_after);
    ResourceOwnerEnlargeBuffers(t_thrd.utils_cxt.CurrentResourceOwner);

    expected_flush_num = ckpt_get_expected_flush_num();
    if (expected_flush_num == 0) {
        /* if not dirty page flush, wakeup sub thread can the buffer pool */
        wakeup_sub_thread();
        return;
    }

    g_instance.ckpt_cxt_ctl->page_writer_last_queue_flush = 0;
    g_instance.ckpt_cxt_ctl->page_writer_last_flush = 0;

    requested_flush_num = ckpt_qsort_dirty_page_for_flush(&is_new_relfilenode, expected_flush_num);

    /* Step 1: set up atomic state for dirty page appiled. */
    prepare_dirty_page_applied_state(requested_flush_num, is_new_relfilenode);

    /* Step 2: wake up all subthreads and main thread sleep. */
    wakeup_sub_thread();

    ckpt_move_queue_head_after_flush();
    smgrcloseall();
    return;
}

static int64 get_pagewriter_sleep_time()
{
    uint64 now;
    int64 time_diff;

    if (FULL_CKPT) {
        return 0;
    }

    now = get_time_ms();
    if (t_thrd.pagewriter_cxt.next_flush_time > now) {
        time_diff = MAX(t_thrd.pagewriter_cxt.next_flush_time - now, 1);
    } else {
        time_diff = 0;
    }
    time_diff = MIN(time_diff, u_sess->attr.attr_storage.pageWriterSleep);
    return time_diff;
}

uint64 get_loc_for_lsn(XLogRecPtr target_lsn)
{
    uint64 last_loc = 0;
    XLogRecPtr page_rec_lsn = InvalidXLogRecPtr;
    uint64 queue_loc = pg_atomic_read_u64(&g_instance.ckpt_cxt_ctl->dirty_page_queue_head);

    if (get_dirty_page_num() == 0) {
        return get_dirty_page_queue_tail();
    }

    while (queue_loc < get_dirty_page_queue_tail()) {
        Buffer buffer;
        BufferDesc *buf_desc = NULL;
        uint64 temp_loc = queue_loc % g_instance.ckpt_cxt_ctl->dirty_page_queue_size;
        volatile DirtyPageQueueSlot *slot = &g_instance.ckpt_cxt_ctl->dirty_page_queue[temp_loc];

        /* slot location is pre-occupied, but the buffer not set finish, need wait and retry. */
        if (!(pg_atomic_read_u32(&slot->slot_state) & SLOT_VALID)) {
            pg_usleep(1);
            queue_loc = pg_atomic_read_u64(&g_instance.ckpt_cxt_ctl->dirty_page_queue_head);
            continue;
        }
        queue_loc++;
        pg_memory_barrier();

        buffer = slot->buffer;
        /* slot state is vaild, buffer is invalid, the slot buffer set 0 when BufferAlloc or InvalidateBuffer */
        if (BufferIsInvalid(buffer)) {
            continue;
        }
        buf_desc = GetBufferDescriptor(buffer - 1);
        page_rec_lsn = pg_atomic_read_u64(&buf_desc->extra->rec_lsn);
        if (!BufferIsInvalid(slot->buffer) && XLByteLE(target_lsn, page_rec_lsn)) {
            last_loc = queue_loc - 1;
            break;
        }
    }

    if (last_loc == 0) {
        return get_dirty_page_queue_tail();
    }

    return last_loc;
}

static uint32 get_page_num_for_lsn(XLogRecPtr target_lsn, uint32 max_num)
{
    uint32 i;
    uint32 num_for_lsn = 0;
    XLogRecPtr page_rec_lsn = InvalidXLogRecPtr;
    int64 dirty_page_num = get_dirty_page_num();
    uint64 dirty_queue_head = pg_atomic_read_u64(&g_instance.ckpt_cxt_ctl->dirty_page_queue_head);

    for (i = 0; i < dirty_page_num; i++) {
        Buffer buffer;
        BufferDesc* buf_desc = NULL;
        uint64 temp_loc = (dirty_queue_head + i) % g_instance.ckpt_cxt_ctl->dirty_page_queue_size;
        volatile DirtyPageQueueSlot* slot = &g_instance.ckpt_cxt_ctl->dirty_page_queue[temp_loc];

        /* slot location is pre-occupied, but the buffer not set finish, need break. */
        if (!(pg_atomic_read_u32(&slot->slot_state) & SLOT_VALID)) {
            break;
        }
        pg_read_barrier();
        buffer = slot->buffer;
        /* slot state is valid, buffer is invalid, the slot buffer set 0 when BufferAlloc or InvalidateBuffer */
        if (BufferIsInvalid(buffer)) {
            continue; /* this tempLoc maybe set 0 when remove dirty page */
        }
        buf_desc = GetBufferDescriptor(buffer - 1);
        page_rec_lsn = pg_atomic_read_u64(&buf_desc->extra->rec_lsn);
        if (!BufferIsInvalid(slot->buffer) && XLByteLE(target_lsn, page_rec_lsn)) {
            break;
        }
        num_for_lsn++;
        if (num_for_lsn >= max_num) {
            break;
        }
    }
    return num_for_lsn;
}

const float HIGH_WATER = 0.75;
const int BYTE_PER_KB = 1024;
static void calculate_max_flush_num()
{
    uint32 blk_size = BLCKSZ / BYTE_PER_KB;
    uint32 max_io = u_sess->attr.attr_storage.max_io_capacity / blk_size / 2;
    float rate_lsn;
    float rate_buf;
    uint32 queue_flush_max;
    uint32 list_flush_max;
    XLogRecPtr min_lsn = InvalidXLogRecPtr;
    XLogRecPtr cur_lsn = InvalidXLogRecPtr;
    double lsn_percent;

    if (unlikely(FULL_CKPT && !RecoveryInProgress())) {
        g_instance.ckpt_cxt_ctl->pgwr_procs.queue_flush_max = max_io;
        g_instance.ckpt_cxt_ctl->pgwr_procs.list_flush_max = 0;
        return;
    }

    min_lsn = ckpt_get_min_rec_lsn();
    if (XLogRecPtrIsInvalid(min_lsn)) {
        min_lsn = get_dirty_page_queue_rec_lsn();
    }

    /* primary get the xlog insert loc, standby get the replay loc */
    if (RecoveryInProgress()) {
        cur_lsn = GetXLogReplayRecPtr(NULL);
    } else {
        cur_lsn = GetXLogInsertRecPtr();
    }

    lsn_percent = (double)(cur_lsn - min_lsn) /
            ((double)u_sess->attr.attr_storage.max_redo_log_size * BYTE_PER_KB);

    rate_lsn = 1 - HIGH_WATER / (lsn_percent + HIGH_WATER);

    rate_lsn = MIN(rate_lsn, HIGH_WATER);

    rate_buf = 1 - rate_lsn;

    queue_flush_max = max_io * rate_lsn;
    list_flush_max = max_io * rate_buf;

    g_instance.ckpt_cxt_ctl->pgwr_procs.queue_flush_max = queue_flush_max;
    g_instance.ckpt_cxt_ctl->pgwr_procs.list_flush_max = list_flush_max;

    if (u_sess->attr.attr_storage.log_pagewriter) {
        ereport(LOG, (errmodule(MOD_INCRE_CKPT),
            errmsg("calculate max io, lsn_percent is %f, rate_lsn is %f, queue flush num is %u, list flush num is %u",
            lsn_percent, rate_lsn, queue_flush_max, list_flush_max)));
    }
}

const int AVG_CALCULATE_NUM = 30;
const uint UPDATE_REC_XLOG_NUM = 4;
static uint32 calculate_pagewriter_flush_num()
{
    static XLogRecPtr prev_lsn = InvalidXLogRecPtr;
    static XLogRecPtr avg_lsn_rate = InvalidXLogRecPtr;
    static pg_time_t prev_time = 0;
    static int64 total_flush_num = 0;
    static uint32 avg_flush_num = 0;
    static uint32 prev_lsn_num = 0;
    static int counter = 0;
    XLogRecPtr target_lsn;
    XLogRecPtr cur_lsn;
    XLogRecPtr min_lsn;
    uint32 flush_num = 0;
    uint64 now;
    int64 time_diff;
    float dirty_page_pct;
    float dirty_slot_pct;
    uint32 num_for_dirty;
    uint32 num_for_lsn;
    uint32 min_io = DW_DIRTY_PAGE_MAX_FOR_NOHBK;
    uint32 max_io = g_instance.ckpt_cxt_ctl->pgwr_procs.queue_flush_max;
    uint32 num_for_lsn_max;
    float dirty_percent;
    double lsn_target_percent = 0;
    uint32 lsn_scan_factor = 3;

    min_lsn = ckpt_get_min_rec_lsn();
    if (XLogRecPtrIsInvalid(min_lsn)) {
        min_lsn = get_dirty_page_queue_rec_lsn();
    }

    /* primary get the xlog insert loc, standby get the replay loc */
    if (RecoveryInProgress()) {
        cur_lsn = GetXLogReplayRecPtr(NULL);
    } else {
        cur_lsn = GetXLogInsertRecPtr();
    }

    if (XLogRecPtrIsInvalid(prev_lsn)) {
        prev_lsn = cur_lsn;
        prev_time = get_time_ms();
        avg_flush_num = min_io;
        goto DEFAULT;
    }

    total_flush_num += g_instance.ckpt_cxt_ctl->page_writer_last_queue_flush;
    now = get_time_ms();
    time_diff = now - prev_time;

    /*
     * We update our variables every AVG_CALCULATE_NUM times to smooth
     * pagewriter flush page nums;
     */
    if (++counter > AVG_CALCULATE_NUM ||
        time_diff > AVG_CALCULATE_NUM * u_sess->attr.attr_storage.pageWriterSleep) {
        time_diff = MAX(1, time_diff);

        avg_flush_num = (uint32)((((double)total_flush_num) / time_diff * u_sess->attr.attr_storage.pageWriterSleep
            + avg_flush_num) / 2);
        avg_lsn_rate = ((double)(cur_lsn - prev_lsn) / time_diff * u_sess->attr.attr_storage.pageWriterSleep
            + avg_lsn_rate) / 2;

        /* reset our variables */
        prev_lsn = cur_lsn;
        prev_time = now;
        total_flush_num = 0;
        counter = 0;
    }

    dirty_page_pct = g_instance.ckpt_cxt_ctl->actual_dirty_page_num / (float)(SegmentBufferStartID);
    dirty_slot_pct = get_dirty_page_num() / (float)(g_instance.ckpt_cxt_ctl->dirty_page_queue_size);
    dirty_percent = MAX(dirty_page_pct, dirty_slot_pct) / u_sess->attr.attr_storage.dirty_page_percent_max;

    if (RecoveryInProgress()) {
        max_io = max_io * 0.9;
    }

    if (dirty_percent < HIGH_WATER) {
        num_for_dirty = min_io;
        num_for_lsn_max = max_io;
    } else if (dirty_percent <= 1) {
        num_for_dirty = min_io + (float)(dirty_percent - HIGH_WATER) / (float)(1 - HIGH_WATER) * (max_io - min_io);
        num_for_lsn_max = max_io + (float)(dirty_percent - HIGH_WATER) / (float)(1 - HIGH_WATER) * (max_io);
    } else {
        num_for_dirty = max_io;
        num_for_lsn_max = max_io * 2;
    }

     lsn_target_percent = (double)(cur_lsn - min_lsn) /
            ((double)u_sess->attr.attr_storage.max_redo_log_size * BYTE_PER_KB);
    /*
     * If the xlog generation speed is slower than dirty queue rec lsn update speed and not many dirty pages,
     * no need to scan too many dirty page, because the dirty page rec lsn is same.
     */
    if (dirty_percent < HIGH_WATER && avg_lsn_rate < XLogSegSize * UPDATE_REC_XLOG_NUM &&
        lsn_target_percent < HIGH_WATER) {
        lsn_scan_factor = 1;
    }

    target_lsn = min_lsn + avg_lsn_rate * lsn_scan_factor;
    num_for_lsn = get_page_num_for_lsn(target_lsn, num_for_lsn_max);

    if (lsn_target_percent < HIGH_WATER) {
        num_for_lsn = MIN(num_for_lsn / lsn_scan_factor, max_io);
    } else if (lsn_target_percent < 1) {
        num_for_lsn = MIN(num_for_lsn / lsn_scan_factor, max_io) +
            (float)(lsn_target_percent - HIGH_WATER) / (float)(1 - HIGH_WATER) * max_io;
    } else {
        num_for_lsn = max_io * 2;
    }

    num_for_lsn = (num_for_lsn + prev_lsn_num) / 2;
    prev_lsn_num = num_for_lsn;

    flush_num = (avg_flush_num + num_for_dirty + num_for_lsn) / 3;

DEFAULT:

    if (flush_num > max_io) {
        flush_num = max_io;
    } else if (flush_num < min_io) {
        flush_num  = min_io;
    }

    return flush_num;
}

void dw_upgrade_batch()
{
    if (!dw_enabled()) {
        return;
    }

    ereport(LOG, (errmodule(MOD_DW), errmsg("dw batch upgrade start")));

    uint64 dw_file_size;
    knl_g_dw_context* dw_batch_cxt = &g_instance.dw_batch_cxt;
    dw_batch_file_context* dw_file_cxt = &dw_batch_cxt->batch_file_cxts[0];

    (void)LWLockAcquire(dw_batch_cxt->flush_lock, LW_EXCLUSIVE);
    (void)LWLockAcquire(dw_file_cxt->flush_lock, LW_EXCLUSIVE);

    wait_all_dw_page_finish_flush();

    PageWriterSync();

    /* recovery the guc paramter and working state */
    g_instance.dw_batch_cxt.old_batch_version = false;
    g_instance.attr.attr_storage.dw_file_num = g_instance.dw_batch_cxt.recovery_dw_file_num;
    g_instance.attr.attr_storage.dw_file_size = g_instance.dw_batch_cxt.recovery_dw_file_size;

    MemoryContext oldcxt = MemoryContextSwitchTo(g_instance.dw_batch_cxt.mem_cxt);

    /*
     * DW_BATCH_UPGRADE_META_FILE_NAME is used to judge whether gaussdb quit at this step,
     * stop here means meta file may be incomplete, and it can't be used.
     */
    int fd = open(DW_BATCH_UPGRADE_META_FILE_NAME, (DW_FILE_FLAG | O_CREAT), DW_FILE_PERM);
    if (fd == -1) {
        ereport(PANIC,
            (errcode_for_file_access(), errmodule(MOD_DW),
            errmsg("Could not create file \"%s\"", DW_BATCH_UPGRADE_META_FILE_NAME)));
    }

    /* create new version meta file and batch files */
    dw_generate_meta_file(&dw_batch_cxt->batch_meta_file);

    if (close(fd) != 0 || unlink(DW_BATCH_UPGRADE_META_FILE_NAME) != 0) {
        ereport(PANIC,
                (errcode_for_file_access(), errmodule(MOD_DW),
                    errmsg("Could not close or remove the DW batch meta upgrade file")));
    }

    /*
     * DW_BATCH_UPGRADE_BATCH_FILE_NAME is used to judge whether gaussdb quit at this step,
     * stop here means batch files may be incomplete, and it can't be used.
     */
    fd = open(DW_BATCH_UPGRADE_BATCH_FILE_NAME, (DW_FILE_FLAG | O_CREAT), DW_FILE_PERM);
    if (fd == -1) {
        ereport(PANIC,
            (errcode_for_file_access(), errmodule(MOD_DW),
            errmsg("Could not create file \"%s\"", DW_BATCH_UPGRADE_BATCH_FILE_NAME)));
    }

    dw_file_size = DW_FILE_SIZE_UNIT * dw_batch_cxt->batch_meta_file.dw_file_size;
    dw_generate_batch_files(dw_batch_cxt->batch_meta_file.dw_file_num, dw_file_size);


    if (close(fd) != 0 || unlink(DW_BATCH_UPGRADE_BATCH_FILE_NAME) != 0) {
        ereport(PANIC,
                (errcode_for_file_access(), errmodule(MOD_DW),
                    errmsg("Could not close or remove the DW batch upgrade file")));
    }

    dw_cxt_init_batch();

    MemoryContextSwitchTo(oldcxt);

    /* close and remove old version dw batch file */
    if (close(dw_file_cxt->fd) != 0 || unlink(OLD_DW_FILE_NAME) != 0) {
        ereport(PANIC,
            (errcode_for_file_access(), errmodule(MOD_DW),
            errmsg("Could not close or remove the DW batch old version file")));
    }


    LWLockRelease(dw_file_cxt->flush_lock);

    pfree(dw_file_cxt);

    (void)LWLockRelease(dw_batch_cxt->flush_lock);

    ereport(LOG, (errmodule(MOD_DW), errmsg("dw batch upgrade end")));

    return;
}

void dw_upgrade_single()
{
    if (!dw_enabled()) {
        return;
    }

    ereport(LOG, (errmodule(MOD_DW), errmsg("dw single upgrade start")));

    knl_g_dw_context* dw_single_cxt = &g_instance.dw_single_cxt;
    (void)LWLockAcquire(dw_single_cxt->second_flush_lock, LW_EXCLUSIVE);
    wait_all_single_dw_finish_flush_old();
    PageWriterSync();

    /* create dw batch flush file */
    int fd = open(DW_UPGRADE_FILE_NAME, (DW_FILE_FLAG | O_CREAT), DW_FILE_PERM);
    if (fd == -1) {
        ereport(PANIC,
            (errcode_for_file_access(), errmodule(MOD_DW),
            errmsg("Could not create file \"%s\"", DW_UPGRADE_FILE_NAME)));
    }
    /* close old version file */
    if (close(dw_single_cxt->fd) != 0 || unlink(SINGLE_DW_FILE_NAME) != 0) {
        ereport(PANIC,
                (errcode_for_file_access(), errmodule(MOD_DW),
                errmsg("Could not close or remove the DW single old version file")));
    }
    dw_single_cxt->fd = -1;
    dw_single_cxt->single_stat_info.total_writes = 0;
    dw_single_cxt->single_stat_info.file_trunc_num = 0;
    dw_single_cxt->single_stat_info.file_reset_num = 0;
    dw_generate_new_single_file();
    dw_single_cxt->fd = open(SINGLE_DW_FILE_NAME, DW_FILE_FLAG, DW_FILE_PERM);
    if (dw_single_cxt->fd == -1) {
        ereport(PANIC,
            (errcode_for_file_access(), errmodule(MOD_DW), errmsg("during upgrade, could not open file \"%s\"",
            SINGLE_DW_FILE_NAME)));
    }
    pg_atomic_write_u32(&g_instance.dw_single_cxt.dw_version, DW_SUPPORT_NEW_SINGLE_FLUSH);

    if (close(fd) != 0 || unlink(DW_UPGRADE_FILE_NAME) != 0) {
        ereport(PANIC,
                (errcode_for_file_access(), errmodule(MOD_DW),
                    errmsg("Could not close or remove the DW upgrade file")));
    }

    /* after upgrade, need free the old version buf context */
    if (dw_single_cxt->recovery_buf.unaligned_buf != NULL) {
        pfree(dw_single_cxt->recovery_buf.unaligned_buf);
        dw_single_cxt->recovery_buf.unaligned_buf = NULL;
    }

    if (dw_single_cxt->recovery_buf.single_flush_state != NULL) {
        pfree(dw_single_cxt->recovery_buf.single_flush_state);
        dw_single_cxt->recovery_buf.single_flush_state = NULL;
    }
    LWLockRelease(dw_single_cxt->second_flush_lock);

    ereport(LOG, (errmodule(MOD_DW), errmsg("dw single upgrade end")));

    return;
}

static void HandlePageWriterExit()
{
    /* Let the pagewriter sub thread exit, then main pagewriter thread exits */
    if (t_thrd.pagewriter_cxt.shutdown_requested && g_instance.ckpt_cxt_ctl->page_writer_can_exit) {
        g_instance.ckpt_cxt_ctl->page_writer_sub_can_exit = true;
        pg_write_barrier();

        ereport(LOG,
            (errmodule(MOD_INCRE_CKPT),
                errmsg("waiting all the pagewriter sub threads to exit")));

        /* Wait until all the sub pagewriter thread exit then */
        while (true) {
            if (pg_atomic_read_u32(&g_instance.ckpt_cxt_ctl->current_page_writer_count) == 1) {
                ereport(LOG,
                    (errmodule(MOD_INCRE_CKPT),
                        errmsg("pagewriter thread shut down, id is %d", t_thrd.pagewriter_cxt.pagewriter_id)));

                /*
                 * From here on, elog(ERROR) should end with exit(1), not send
                 * control back to the sigsetjmp block above.
                 */
                u_sess->attr.attr_common.ExitOnAnyError = true;

                /* release compression ctx */
                crps_destory_ctxs();

                /* Normal exit from the pagewriter is here */
                proc_exit(0); /* done */
            }
            pg_usleep(MILLISECOND_TO_MICROSECOND);
        }

        ereport(LOG, (errmodule(MOD_INCRE_CKPT), errmsg("all the pagewriter threads exit")));
    }
}

static void HandlePageWriterMainInterrupts()
{
    if (t_thrd.pagewriter_cxt.got_SIGHUP) {
        t_thrd.pagewriter_cxt.got_SIGHUP = false;
        ProcessConfigFile(PGC_SIGHUP);
    }

    if (t_thrd.pagewriter_cxt.sync_requested || t_thrd.pagewriter_cxt.sync_retry) {
        t_thrd.pagewriter_cxt.sync_requested = false;

        t_thrd.pagewriter_cxt.sync_retry = true;
        PageWriterSyncWithAbsorption();
        t_thrd.pagewriter_cxt.sync_retry = false;
    }
}

static void ckpt_pagewriter_main_thread_loop(void)
{
    uint32 rc = 0;
    uint64 now;
    int64 sleep_time;
    uint32 candidate_num = 0;

    HandlePageWriterMainInterrupts();

    candidate_num = get_curr_candidate_nums(CAND_LIST_NORMAL) + get_curr_candidate_nums(CAND_LIST_NVM) +
        get_curr_candidate_nums(CAND_LIST_SEG);
    while (get_dirty_page_num() == 0 && candidate_num == (uint32)TOTAL_BUFFER_NUM &&
        !t_thrd.pagewriter_cxt.shutdown_requested) {
        rc = WaitLatch(&t_thrd.proc->procLatch, WL_TIMEOUT | WL_POSTMASTER_DEATH, (long)TEN_MILLISECOND);
        if (rc & WL_POSTMASTER_DEATH) {
            /* release compression ctx */
            crps_destory_ctxs();

            gs_thread_exit(1);
        }

        HandlePageWriterMainInterrupts();

        candidate_num = get_curr_candidate_nums(CAND_LIST_NORMAL) + get_curr_candidate_nums(CAND_LIST_NVM) +
            get_curr_candidate_nums(CAND_LIST_SEG);
        if (candidate_num == 0) {
            /* wakeup sub thread scan the buffer pool, init the candidate list */
            wakeup_sub_thread();
        }
    }

    if (pg_atomic_read_u32(&g_instance.ckpt_cxt_ctl->pgwr_procs.running_num) == 0 &&
        pg_atomic_read_u32(&g_instance.ckpt_cxt_ctl->current_page_writer_count) ==
            (uint32)g_instance.ckpt_cxt_ctl->pgwr_procs.num) {
        ckpt_try_skip_invalid_elem_in_queue_head();
        ckpt_try_prune_dirty_page_queue();
        PgwrAbsorbFsyncRequests();
        /* Full checkpoint, don't sleep */
        sleep_time = get_pagewriter_sleep_time();
        while (sleep_time > 0 && !t_thrd.pagewriter_cxt.shutdown_requested && !FULL_CKPT) {
            HandlePageWriterMainInterrupts();
            /* sleep 1ms check whether a full checkpoint is triggered */
            pg_usleep(MILLISECOND_TO_MICROSECOND);
            sleep_time -= 1;
        }

        /* Calculate next flush time before flush this batch dirty page */
        now = get_time_ms();
        t_thrd.pagewriter_cxt.next_flush_time = now + u_sess->attr.attr_storage.pageWriterSleep;

        /* pagewriter thread flush dirty page */
        calculate_max_flush_num();
        ckpt_pagewriter_main_thread_flush_dirty_page();
    }

    /* Control all the pagewriter threads to exit*/
    HandlePageWriterExit();
    return;
}

static void wakeup_pagewriter_main_thread()
{
    PageWriterProc *pgwr = &g_instance.ckpt_cxt_ctl->pgwr_procs.writer_proc[0];

    /* The current candidate list is empty, wake up the buffer writer. */
    if (pgwr->proc != NULL) {
        SetLatch(&pgwr->proc->procLatch);
    }
    return;
}

static bool apply_batch_flush_pages(PageWriterProc* pgwr)
{
    uint32 start_loc;
    int need_flush_num;
    bool is_new_relfilenode = pgwr->thrd_dw_cxt.is_new_relfilenode;
    int dw_batch_page_max = GET_DW_DIRTY_PAGE_MAX(is_new_relfilenode);
    uint32 total_flush_pages = g_instance.ckpt_cxt_ctl->CkptBufferIdsFlushPages;
    start_loc = pg_atomic_fetch_add_u32(&g_instance.ckpt_cxt_ctl->CkptBufferIdsTail, dw_batch_page_max);

    if (start_loc >= total_flush_pages) {
        return false;
    }

    need_flush_num = dw_batch_page_max;
    if (start_loc + need_flush_num > total_flush_pages) {
        need_flush_num = total_flush_pages - start_loc;
    }

    pgwr->start_loc = start_loc;
    pgwr->need_flush_num = need_flush_num;
    return true;
}

static void ckpt_pagewriter_sub_thread_loop()
{
    uint32 rc = 0;
    uint64 now;
    uint32 total_flush_pages;
    uint32 old_running_num;
    uint32 completed_pages;
    WritebackContext wb_context;
    int thread_id = t_thrd.pagewriter_cxt.pagewriter_id;
    WritebackContextInit(&wb_context, &t_thrd.pagewriter_cxt.page_writer_after);

    pg_read_barrier();
    if (g_instance.ckpt_cxt_ctl->page_writer_sub_can_exit) {
        ereport(LOG,
            (errmodule(MOD_INCRE_CKPT),
                errmsg("pagewriter thread shut down, id is %d", t_thrd.pagewriter_cxt.pagewriter_id)));

        if (ENABLE_DMS) {
            PageWriterProc *pgwr = &g_instance.ckpt_cxt_ctl->pgwr_procs.writer_proc[thread_id];
            DSSAioDestroy(&pgwr->aio_cxt);
        }

        /*
         * From here on, elog(ERROR) should end with exit(1), not send control back to
         * the sigsetjmp block above
         */
        u_sess->attr.attr_common.ExitOnAnyError = true;
        /* Normal exit from the pagewriter is here */
        proc_exit(0);
    }

    if (!t_thrd.pagewriter_cxt.shutdown_requested) {
        /* Wait first */
        rc = WaitLatch(&t_thrd.proc->procLatch, WL_TIMEOUT | WL_LATCH_SET | WL_POSTMASTER_DEATH,
            (long)u_sess->attr.attr_storage.pageWriterSleep  /* ms */);
    }

    if (rc & WL_POSTMASTER_DEATH) {
        gs_thread_exit(1);
    }

    ResetLatch(&t_thrd.proc->procLatch);
    PageWriterProc* pgwr = &g_instance.ckpt_cxt_ctl->pgwr_procs.writer_proc[thread_id];

    if (pgwr->need_flush) {
        /* scan buffer pool, get flush list and candidate list */
        now = get_time_ms();
        if (t_thrd.pagewriter_cxt.next_scan_time <= now) {
            incre_ckpt_pgwr_scan_buf_pool(&wb_context);
            now = get_time_ms();
            t_thrd.pagewriter_cxt.next_scan_time = now +
                MAX(u_sess->attr.attr_storage.BgWriterDelay, u_sess->attr.attr_storage.pageWriterSleep);
        } else if ((int64)(t_thrd.pagewriter_cxt.next_scan_time - now) >
            MAX(u_sess->attr.attr_storage.BgWriterDelay, u_sess->attr.attr_storage.pageWriterSleep)) {
            /* preventing Time Jumps */
            t_thrd.pagewriter_cxt.next_scan_time = now;
        }

        pg_read_barrier();
        total_flush_pages = g_instance.ckpt_cxt_ctl->CkptBufferIdsFlushPages;

        while (pg_atomic_read_u32(&g_instance.ckpt_cxt_ctl->prepared) == 1
            && pg_atomic_read_u32(&g_instance.ckpt_cxt_ctl->CkptBufferIdsCompletedPages) < total_flush_pages) {
            /* apply one batch dirty pages */
            if(!apply_batch_flush_pages(pgwr)) {
                break;
            }

            /* flush one batch dirty pages */
            ResourceOwnerEnlargeBuffers(t_thrd.utils_cxt.CurrentResourceOwner);
            incre_ckpt_pgwr_flush_dirty_queue(&wb_context);

            /* add up completed pages */
            completed_pages = pg_atomic_add_fetch_u32(
                &g_instance.ckpt_cxt_ctl->CkptBufferIdsCompletedPages, pgwr->need_flush_num);

            /* if flush finished, set prepared to 0 */
            if (completed_pages == total_flush_pages) {
                pg_atomic_write_u32(&g_instance.ckpt_cxt_ctl->prepared, 0);
                pg_write_barrier();
            }

            pg_read_barrier();
        }

        pgwr->need_flush = false;
        old_running_num = pg_atomic_fetch_sub_u32(&g_instance.ckpt_cxt_ctl->pgwr_procs.running_num, 1);
        if (old_running_num == 1) {
            wakeup_pagewriter_main_thread();
        }

        smgrcloseall();
    }

    return;
}

static void ckpt_pagewriter_handle_exception(MemoryContext pagewriter_context)
{
    /*
     * Close all open files after any error.  This is helpful on Windows,
     * where holding deleted files open causes various strange errors.
     * It's not clear we need it elsewhere, but shouldn't hurt.
     */
    int id = t_thrd.pagewriter_cxt.pagewriter_id;

    /* Since not using PG_TRY, must reset error stack by hand */
    t_thrd.log_cxt.error_context_stack = NULL;

    t_thrd.log_cxt.call_stack = NULL;

    /* Clear the running status of this ereported thread before we proceed to release resources */
    if (g_instance.ckpt_cxt_ctl->pgwr_procs.writer_proc[id].need_flush) {
        g_instance.ckpt_cxt_ctl->pgwr_procs.writer_proc[id].need_flush = false;
        pg_atomic_fetch_sub_u32(&g_instance.ckpt_cxt_ctl->pgwr_procs.running_num, 1);
    }

    g_instance.ckpt_cxt_ctl->pgwr_procs.writer_proc[id].thrd_dw_cxt.dw_page_idx = -1;
    /* Prevent interrupts while cleaning up */
    HOLD_INTERRUPTS();

    /* Report the error to the server log */
    EmitErrorReport();

    /* release resource held by lsc */
    AtEOXact_SysDBCache(false);
    /*
     * These operations are really just a minimal subset of
     * AbortTransaction().  We don't have very many resources to worry
     * about in pagewriter, but we do have LWLocks, buffers, and temp files.
     */
    LWLockReleaseAll();
    AbortBufferIO();
    UnlockBuffers();
    /* buffer pins are released here: */
    ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, false, true);
    /* we needn't bother with the other ResourceOwnerRelease phases */
    AtEOXact_Buffers(false);
    AtEOXact_Files();
    AtEOXact_HashTables(false);

    /* release compression ctx */
    crps_destory_ctxs();

    /*
     * Now return to normal top-level context and clear ErrorContext for
     * next time.
     */
    (void)MemoryContextSwitchTo(pagewriter_context);
    FlushErrorState();

    /* Flush any leaked data in the top-level context */
    MemoryContextResetAndDeleteChildren(pagewriter_context);

    /* Now we can allow interrupts again */
    RESUME_INTERRUPTS();

    /*
     * Sleep at least 1 second after any error.  A write error is likely
     * to be repeated, and we don't want to be filling the error logs as
     * fast as we can.
     */
    pg_usleep(1000000L);

    return;
}

static void pagewriter_kill(int code, Datum arg)
{
    int id = t_thrd.pagewriter_cxt.pagewriter_id;
    Assert(id >= 0 && id < g_instance.ckpt_cxt_ctl->pgwr_procs.num);

    if (id == 0) {
        hash_destroy(u_sess->storage_cxt.pendingOps);
        u_sess->storage_cxt.pendingOps = NULL;
    }
    /* Making sure that we mark our exit status (as sub threads) so that main pagewriter thread would not be waiting for us in vain */
    if (g_instance.ckpt_cxt_ctl->pgwr_procs.writer_proc[id].need_flush) {
        g_instance.ckpt_cxt_ctl->pgwr_procs.writer_proc[id].need_flush = false;
        pg_atomic_fetch_sub_u32(&g_instance.ckpt_cxt_ctl->pgwr_procs.running_num, 1);
    }
    pg_atomic_fetch_sub_u32(&g_instance.ckpt_cxt_ctl->current_page_writer_count, 1);
    g_instance.ckpt_cxt_ctl->pgwr_procs.writer_proc[id].proc = NULL;

    /* release compression ctx */
    crps_destory_ctxs();
}

int get_pagewriter_thread_id(void)
{
    int id;
    int i;

    if (t_thrd.pagewriter_cxt.pagewriter_id != -1) {
        return t_thrd.pagewriter_cxt.pagewriter_id;
    }

    id = pg_atomic_fetch_add_u32(&g_instance.ckpt_cxt_ctl->current_page_writer_count, 1);

    /*
     * The first pagewriter thread start, will be placed in the writer_proc slot in order. Some
     * condiition, some pagewriter thread exit, It must be placed in the corresponding slot.
     */
    if (g_instance.ckpt_cxt_ctl->pgwr_procs.writer_proc[id].proc == NULL) {
        g_instance.ckpt_cxt_ctl->pgwr_procs.writer_proc[id].proc = t_thrd.proc;
        t_thrd.pagewriter_cxt.pagewriter_id = id;
    } else {
        for (i = 0; i < g_instance.ckpt_cxt_ctl->pgwr_procs.num; i++) {
            void *expected = NULL;
            if (pg_atomic_compare_exchange_uintptr(
                (uintptr_t *)&g_instance.ckpt_cxt_ctl->pgwr_procs.writer_proc[i].proc,
                (uintptr_t *)&expected, (uintptr_t)t_thrd.proc)) {
                t_thrd.pagewriter_cxt.pagewriter_id = i;
                break;
            }
        }
    }

    Assert(t_thrd.pagewriter_cxt.pagewriter_id >= 0 &&
           t_thrd.pagewriter_cxt.pagewriter_id < g_instance.ckpt_cxt_ctl->pgwr_procs.num);
    return t_thrd.pagewriter_cxt.pagewriter_id;
}

static void SetupPageWriterSignalHook(void)
{
    /*
     * Reset some signals that are accepted by postmaster but not here
     */
    (void)gspqsignal(SIGHUP, ckpt_pagewriter_sighup_handler);
    (void)gspqsignal(SIGINT, ckpt_pagewriter_sigint_handler);
    (void)gspqsignal(SIGTERM, ckpt_pagewriter_request_shutdown_handler);
    (void)gspqsignal(SIGQUIT, ckpt_pagewriter_quick_die); /* hard crash time */
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, ckpt_pagewriter_sigusr1_handler);
    (void)gspqsignal(SIGUSR2, SIG_IGN);
    (void)gspqsignal(SIGURG, print_stack);
    /*
     * Reset some signals that are accepted by postmaster but not here
     */
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGTTIN, SIG_DFL);
    (void)gspqsignal(SIGTTOU, SIG_DFL);
    (void)gspqsignal(SIGCONT, SIG_DFL);
    (void)gspqsignal(SIGWINCH, SIG_DFL);
}

static void logSnapshotForLogicalDecoding()
{
    if (!SS_SINGLE_CLUSTER && XLogLogicalInfoActive() && !RecoveryInProgress()) {
        TimestampTz timeout = 0;
        TimestampTz currentTime = GetCurrentTimestamp();
        timeout = TimestampTzPlusMilliseconds(g_last_snapshot_ts, LOG_SNAPSHOT_INTERVAL_MS);

        /* Log a new xl_running_xacts every 15 seconds for logical replication */
        if (currentTime >= timeout && !XLByteEQ(g_last_snapshot_lsn, GetXLogInsertRecPtr())) {
            g_last_snapshot_lsn = LogStandbySnapshot();
            g_last_snapshot_ts = currentTime;
        }
    }
}

void crps_create_ctxs(knl_thread_role role)
{
    t_thrd.page_compression_cxt.thrd_role = role;
    t_thrd.page_compression_cxt.zstd_cctx = (void *)ZSTD_createCCtx();
    t_thrd.page_compression_cxt.zstd_dctx = (void *)ZSTD_createDCtx();
}

void crps_destory_ctxs()
{
    if (t_thrd.page_compression_cxt.zstd_cctx != NULL) {
        (void)ZSTD_freeCCtx((ZSTD_CCtx*)t_thrd.page_compression_cxt.zstd_cctx);
        t_thrd.page_compression_cxt.zstd_cctx = NULL;
    }

    if (t_thrd.page_compression_cxt.zstd_dctx != NULL) {
        (void)ZSTD_freeDCtx((ZSTD_DCtx*)t_thrd.page_compression_cxt.zstd_dctx);
        t_thrd.page_compression_cxt.zstd_dctx = NULL;
    }
}

static void incre_ckpt_aio_callback(struct io_event *event)
{
    BufferDesc *buf_desc = (BufferDesc *)(event->data);
    uint32 written_size = event->obj->u.c.nbytes;
    if (written_size != event->res) {
        ereport(WARNING, (errmsg("aio write failed (errno = %d), buffer: %d/%d/%d/%d/%d %d-%d", -(int32)(event->res),
            buf_desc->tag.rnode.spcNode, buf_desc->tag.rnode.dbNode, buf_desc->tag.rnode.relNode,
            (int32)buf_desc->tag.rnode.bucketNode, (int32)buf_desc->tag.rnode.opt,
            buf_desc->tag.forkNum, buf_desc->tag.blockNum)));
        _exit(0);
    }

#ifdef USE_ASSERT_CHECKING
    char *write_buf = (char *)(event->obj->u.c.buf);
    char *origin_buf = (char *)palloc(BLCKSZ + ALIGNOF_BUFFER);
    char *temp_buf = (char *)BUFFERALIGN(origin_buf);
    if (IsSegmentBufferID(buf_desc->buf_id)) {
        SegSpace *spc = spc_open(buf_desc->tag.rnode.spcNode, buf_desc->tag.rnode.dbNode, false);
        seg_physical_read(spc, buf_desc->tag.rnode, buf_desc->tag.forkNum, buf_desc->tag.blockNum, temp_buf);
    } else if (buf_desc->extra->seg_fileno != EXTENT_INVALID) {
        (void)SmgrNetPageCheckRead(buf_desc->tag.rnode.spcNode, buf_desc->tag.rnode.dbNode,
                                            buf_desc->extra->seg_fileno, buf_desc->tag.forkNum,
                                            buf_desc->extra->seg_blockno, (char *)temp_buf);
    }
    Assert(memcmp(write_buf, temp_buf, BLCKSZ) == 0);
    pfree(origin_buf);
#endif

    buf_desc->extra->aio_in_progress = false;
    UnpinBuffer(buf_desc, true);
}

void ckpt_pagewriter_main(void)
{
    sigjmp_buf localSigjmpBuf;
    MemoryContext pagewriter_context;
    char name[MAX_THREAD_NAME_LEN] = {0};

    t_thrd.role = PAGEWRITER_THREAD;

    SetupPageWriterSignalHook();

    /* We allow SIGQUIT (quickdie) at all times */
    (void)sigdelset(&t_thrd.libpq_cxt.BlockSig, SIGQUIT);

    ereport(LOG,
        (errmodule(MOD_INCRE_CKPT),
            errmsg("pagewriter started, thread id is %d", t_thrd.pagewriter_cxt.pagewriter_id)));
    g_last_snapshot_ts = GetCurrentTimestamp();

    /*
     * Create a resource owner to keep track of our resources (currently only
     * buffer pins).
     */
    Assert(t_thrd.pagewriter_cxt.pagewriter_id >= 0);
    errno_t err_rc = snprintf_s(
        name, MAX_THREAD_NAME_LEN, MAX_THREAD_NAME_LEN - 1, "%s%d", "PageWriter", t_thrd.pagewriter_cxt.pagewriter_id);
    securec_check_ss(err_rc, "", "");

    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, name,
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));

    /*
     * Create a memory context that we will do all our work in.  We do this so
     * that we can reset the context during error recovery and thereby avoid
     * possible memory leaks.  Formerly this code just ran in
     * TopMemoryContext, but resetting that would be a really bad idea.
     */
    pagewriter_context = AllocSetContextCreate(
        TopMemoryContext, name, ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    (void)MemoryContextSwitchTo(pagewriter_context);
    on_shmem_exit(pagewriter_kill, (Datum)0);

    /* initialize AIO context */
    if (ENABLE_DMS && t_thrd.pagewriter_cxt.pagewriter_id != 0) {
        PageWriterProc *pgwr = &g_instance.ckpt_cxt_ctl->pgwr_procs.writer_proc[t_thrd.pagewriter_cxt.pagewriter_id];
        DSSAioInitialize(&pgwr->aio_cxt, incre_ckpt_aio_callback);
    }

    /*
     * If an exception is encountered, processing resumes here.
     *
     * See notes in postgres.c about the design of this coding.
     */
    if (sigsetjmp(localSigjmpBuf, 1) != 0) {
        ereport(WARNING, (errmodule(MOD_INCRE_CKPT), errmsg("pagewriter exception occured.")));
        if (ENABLE_DMS && t_thrd.pagewriter_cxt.pagewriter_id != 0) {
            int thread_id = t_thrd.pagewriter_cxt.pagewriter_id;
            PageWriterProc *pgwr = &g_instance.ckpt_cxt_ctl->pgwr_procs.writer_proc[thread_id];
            DSSAioFlush(&pgwr->aio_cxt);
        }
        ckpt_pagewriter_handle_exception(pagewriter_context);
    }

    /* We can now handle ereport(ERROR) */
    t_thrd.log_cxt.PG_exception_stack = &localSigjmpBuf;

    /*
     * Unblock signals (they were blocked when the postmaster forked us)
     */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    /*
     * Use the recovery target timeline ID during recovery
     */
    if (RecoveryInProgress()) {
        t_thrd.xlog_cxt.ThisTimeLineID = GetRecoveryTargetTLI();
    }

    pgstat_report_appname("PageWriter");
    pgstat_report_activity(STATE_IDLE, NULL);

    if (t_thrd.pagewriter_cxt.pagewriter_id == 0) {
        g_instance.ckpt_cxt_ctl->page_writer_sub_can_exit = false;
        pg_write_barrier();

        g_instance.proc_base->pgwrMainThreadLatch = &t_thrd.proc->procLatch;
        g_instance.ckpt_cxt_ctl->incre_ckpt_sync_shmem->pagewritermain_pid = t_thrd.proc_cxt.MyProcPid;
        InitSync();
    }

    pg_time_t now = (pg_time_t) time(NULL);
    t_thrd.pagewriter_cxt.next_flush_time = now + u_sess->attr.attr_storage.pageWriterSleep;
    t_thrd.pagewriter_cxt.next_scan_time = now +
            MAX(u_sess->attr.attr_storage.BgWriterDelay, u_sess->attr.attr_storage.pageWriterSleep);

    /* init compression ctx for page compression */
    crps_create_ctxs(t_thrd.role);

    /*
     * Loop forever
     */
    for (;;) {
        /*
         * main pagewriter thread need choose a batch page flush to double write file,
         * than divide to other sub thread.
         */

        if (t_thrd.pagewriter_cxt.pagewriter_id == 0) {
            if (!t_thrd.pagewriter_cxt.shutdown_requested) {
                logSnapshotForLogicalDecoding();
            }
            /* need generate new version single flush dw file */
            if (pg_atomic_read_u32(&g_instance.dw_single_cxt.dw_version) < DW_SUPPORT_NEW_SINGLE_FLUSH &&
                t_thrd.proc->workingVersionNum >= DW_SUPPORT_NEW_SINGLE_FLUSH) {
                dw_upgrade_single();
            }

            if (pg_atomic_read_u32(&g_instance.dw_batch_cxt.dw_version) < DW_SUPPORT_MULTIFILE_FLUSH &&
                t_thrd.proc->workingVersionNum >= DW_SUPPORT_MULTIFILE_FLUSH) {
                dw_upgrade_batch();
            }

            /*
             * when double write is disabled, pg_dw_meta will be created with dw_file_num = 0, so
             * here is for upgrading process. pagewrite will run when enable_incremetal_checkpoint = on.
             */
            if (pg_atomic_read_u32(&g_instance.dw_batch_cxt.dw_version) < DW_SUPPORT_REABLE_DOUBLE_WRITE
                && t_thrd.proc->workingVersionNum >= DW_SUPPORT_REABLE_DOUBLE_WRITE) {
                dw_upgrade_renable_double_write();
            }

            ckpt_pagewriter_main_thread_loop();
        } else {
            ckpt_pagewriter_sub_thread_loop();
        }
    }
}

const float NEED_PRUNE_DIRTY_QUEUE_SLOT = 0.6;
#define MAX_INVALID_BUF_SLOT (MIN(g_instance.shmem_cxt.MaxConnections, TOTAL_BUFFER_NUM))
#define MAX_VALID_BUF_SLOT (MAX_INVALID_BUF_SLOT * 3)

static void print_dirty_page_queue_info(bool after_prune)
{
    uint64 i = 0;
    uint64 temp_loc;
    volatile DirtyPageQueueSlot* slot = NULL;
    uint64 print_info_num = MIN(((uint64)(MAX_VALID_BUF_SLOT + MAX_VALID_BUF_SLOT)), ((uint64)get_dirty_page_num()));
    uint64 dirty_queue_head = pg_atomic_read_u64(&g_instance.ckpt_cxt_ctl->dirty_page_queue_head);

    for (i = 0; i < print_info_num; i++) {
        temp_loc = (dirty_queue_head + i) % g_instance.ckpt_cxt_ctl->dirty_page_queue_size;
        slot = &g_instance.ckpt_cxt_ctl->dirty_page_queue[temp_loc];
        ereport(DEBUG1,
            (errmodule(MOD_INCRE_CKPT),
                errmsg("%s, dirty page queue loc is %lu, buffer is %d, slot_state is %u",
                    after_prune ? "after prune" : "before prune",
                    temp_loc,
                    slot->buffer,
                    slot->slot_state)));
    }
}

static bool ckpt_found_valid_and_invalid_buffer_loc(
    uint64* valid_buffer_array, uint32 array_size, uint32* valid_slot_num, uint64* last_invalid_slot)
{
    int64 i;
    uint64 temp_loc;
    uint32 invalid_slot_num = 0;
    uint32 max_invalid_slot = MAX_INVALID_BUF_SLOT;
    int64 dirty_page_num;
    uint64 dirty_queue_head;
    volatile DirtyPageQueueSlot* slot = NULL;

    dirty_page_num = get_dirty_page_num();

    if (u_sess->attr.attr_storage.log_pagewriter) {
        print_dirty_page_queue_info(false);
    }

    dirty_queue_head = pg_atomic_read_u64(&g_instance.ckpt_cxt_ctl->dirty_page_queue_head);

    /* get valid buffer loc, push loc to the valid_buffer_array, get last invalid buffer loc */
    for (i = 0; i < dirty_page_num; i++) {
        temp_loc = (dirty_queue_head + i) % g_instance.ckpt_cxt_ctl->dirty_page_queue_size;
        slot = &g_instance.ckpt_cxt_ctl->dirty_page_queue[temp_loc];
        if (!(pg_atomic_read_u32(&slot->slot_state) & SLOT_VALID)) {
            break;
        }
        pg_read_barrier();
        if (invalid_slot_num >= max_invalid_slot) {
            break;
        }
        if (*valid_slot_num >= array_size) {
            break;
        }

        if (!BufferIsInvalid(slot->buffer)) {
            valid_buffer_array[*valid_slot_num] = dirty_queue_head + i;
            *valid_slot_num += 1;
        } else {
            invalid_slot_num++;
            *last_invalid_slot = dirty_queue_head + i;
        }
    }
    if (*valid_slot_num == 0 || invalid_slot_num == 0) {
        return false;
    } else {
        return true;
    }
}
static void ckpt_try_prune_dirty_page_queue()
{
    uint32 valid_slot_num = 0;
    uint64 last_invalid_slot = 0;
    bool can_found = false;
    uint64* valid_buffer_array = NULL;

    if (get_dirty_page_num() < g_instance.ckpt_cxt_ctl->dirty_page_queue_size * NEED_PRUNE_DIRTY_QUEUE_SLOT) {
        return;
    }

    valid_buffer_array = (uint64*)palloc0(MAX_VALID_BUF_SLOT * sizeof(uint64));

    can_found = ckpt_found_valid_and_invalid_buffer_loc(
        valid_buffer_array, MAX_VALID_BUF_SLOT, &valid_slot_num, &last_invalid_slot);

    /*
     * Read valid_buffer_array form the last to first, move the buffer to last_invalid_slot,
     * maybe some slot buffer is valid, but loc after the last_invalid_slot, can't move, read
     * next.
     */
    if (can_found) {
        uint64 temp_loc;
        uint64 move_loc;
        uint64 buf_state;
        volatile DirtyPageQueueSlot* slot = NULL;
        volatile DirtyPageQueueSlot* move_slot = NULL;
        BufferDesc* bufhdr = NULL;

        /*
         * If full checkpoint set the full_ckpt_expected_flush_loc is queue slot 100, some
         * pages are moved to a new position after slot 100 due to this prune queue. than
         * the redo point will be wrong, because some page not flush to disk.
         */
        (void)LWLockAcquire(g_instance.ckpt_cxt_ctl->prune_queue_lock, LW_EXCLUSIVE);
        pg_memory_barrier();
        if (last_invalid_slot > pg_atomic_read_u64(&g_instance.ckpt_cxt_ctl->full_ckpt_expected_flush_loc)) {
            pg_atomic_write_u64(&g_instance.ckpt_cxt_ctl->full_ckpt_expected_flush_loc, (last_invalid_slot + 1));
        }
        gstrace_entry(GS_TRC_ID_ckpt_try_prune_dirty_page_queue);

        for (int32 i = valid_slot_num - 1; i >= 0 && last_invalid_slot > 0; i--) {
            if (valid_buffer_array[i] >= last_invalid_slot) {
                continue;
            }
            temp_loc = (valid_buffer_array[i]) % g_instance.ckpt_cxt_ctl->dirty_page_queue_size;
            move_loc = last_invalid_slot % g_instance.ckpt_cxt_ctl->dirty_page_queue_size;
            slot = &g_instance.ckpt_cxt_ctl->dirty_page_queue[temp_loc];
            move_slot = &g_instance.ckpt_cxt_ctl->dirty_page_queue[move_loc];

            /* InvalidateBuffer will remove page from the dirty page queue */
            if (BufferIsInvalid(slot->buffer)) {
                continue;
            }

            if (!BufferIsInvalid(move_slot->buffer)) {
                ereport(PANIC, (errmodule(MOD_INCRE_CKPT), errmsg("the move_loc buffer should be invalid.")));
            }

            bufhdr = GetBufferDescriptor(slot->buffer - 1);
            buf_state = LockBufHdr(bufhdr);

            /* InvalidateBuffer will remove page from the dirty page queue */
            if (BufferIsInvalid(slot->buffer)) {
                UnlockBufHdr(bufhdr, buf_state);
                continue;
            }
            move_slot->buffer = slot->buffer;
            bufhdr->extra->dirty_queue_loc = move_loc;
            slot->buffer = 0;
            pg_write_barrier();
            UnlockBufHdr(bufhdr, buf_state);

            last_invalid_slot--;
        }
        LWLockRelease(g_instance.ckpt_cxt_ctl->prune_queue_lock);

        if (u_sess->attr.attr_storage.log_pagewriter) {
            print_dirty_page_queue_info(true);
        }
        gstrace_exit(GS_TRC_ID_ckpt_try_prune_dirty_page_queue);
    }
    pfree(valid_buffer_array);
    valid_buffer_array = NULL;
    return;
}

static void ckpt_try_skip_invalid_elem_in_queue_head()
{
    uint64 dirty_queue_head;
    int64 dirty_page_num;
    int64 i = 0;
    uint32 head_move_num = 0;

    dirty_page_num = get_dirty_page_num();
    if (dirty_page_num == 0) {
        return;
    }

    dirty_queue_head = pg_atomic_read_u64(&g_instance.ckpt_cxt_ctl->dirty_page_queue_head);

    for (i = 0; i < dirty_page_num; i++) {
        uint64 temp_loc = (dirty_queue_head + i) % g_instance.ckpt_cxt_ctl->dirty_page_queue_size;
        volatile DirtyPageQueueSlot* slot = &g_instance.ckpt_cxt_ctl->dirty_page_queue[temp_loc];
        /* slot location is pre-occupied, but the buffer not set finish, need break. */
        if (!(pg_atomic_read_u32(&slot->slot_state) & SLOT_VALID)) {
            break;
        }
        pg_read_barrier();
        /* slot state is vaild, buffer is invalid, the slot buffer set 0 when BufferAlloc or InvalidateBuffer */
        if (!BufferIsInvalid(slot->buffer)) {
            break;
        } else {
            (void)pg_atomic_fetch_add_u64(&g_instance.ckpt_cxt_ctl->dirty_page_queue_head, 1);
            pg_atomic_init_u32(&slot->slot_state, 0);
            head_move_num++;
        }
    }
    if (u_sess->attr.attr_storage.log_pagewriter) {
        ereport(DEBUG1,
            (errmodule(MOD_INCRE_CKPT),
                errmsg("skip invalid element dirty, page queue head add %u, dirty page remain: %ld",
                    head_move_num,
                    get_dirty_page_num())));
    }
    return;
}

static uint32 incre_ckpt_pgwr_flush_dirty_page(WritebackContext *wb_context,
    const CkptSortItem *dirty_buf_list, int start, int batch_num)
{
    uint32 num_actual_flush = 0;
    uint64 buf_state;
    uint32 sync_state;
    BufferDesc *buf_desc = NULL;
    int buf_id;
    int thread_id = t_thrd.pagewriter_cxt.pagewriter_id;
    PageWriterProc *pgwr = &g_instance.ckpt_cxt_ctl->pgwr_procs.writer_proc[thread_id];
    DSSAioCxt *aio_cxt = &pgwr->aio_cxt;

    for (int i = start; i < start + batch_num; i++) {
        buf_id = dirty_buf_list[i].buf_id;
        if (buf_id == DW_INVALID_BUFFER_ID) {
            continue;
        }

        /* Make sure we will have room to remember the buffer pin */
        ResourceOwnerEnlargeBuffers(t_thrd.utils_cxt.CurrentResourceOwner);

        buf_desc = GetBufferDescriptor(buf_id);
        buf_state = LockBufHdr(buf_desc);
        if ((buf_state & BM_CHECKPOINT_NEEDED) && (buf_state & BM_DIRTY)) {
            UnlockBufHdr(buf_desc, buf_state);

            sync_state = SyncOneBuffer(buf_id, false, wb_context, true);
            if ((sync_state & BUF_WRITTEN)) {
                num_actual_flush++;
            }
        } else {
            UnlockBufHdr(buf_desc, buf_state);
        }
    }

    if (ENABLE_DMS) {
        DSSAioFlush(aio_cxt);
    }

    return num_actual_flush;
}

static void incre_ckpt_pgwr_flush_dirty_queue(WritebackContext *wb_context)
{
    int thread_id = t_thrd.pagewriter_cxt.pagewriter_id;
    PageWriterProc* pgwr = &g_instance.ckpt_cxt_ctl->pgwr_procs.writer_proc[thread_id];
    bool is_new_relfilenode = g_instance.dw_batch_cxt.is_new_relfilenode;
    uint32 start_loc = pgwr->start_loc;
    int need_flush_num = pgwr->need_flush_num;
    int dw_batch_page_max = GET_DW_DIRTY_PAGE_MAX(is_new_relfilenode);
    int runs = (need_flush_num + dw_batch_page_max - 1) / dw_batch_page_max;
    int num_actual_flush = 0;
    CkptSortItem *dirty_buf_list = g_instance.ckpt_cxt_ctl->CkptBufferIds + start_loc;

    if (unlikely(need_flush_num == 0)) {
        return;
    }

    ResourceOwnerEnlargeBuffers(t_thrd.utils_cxt.CurrentResourceOwner);

    /* Double write can only handle at most DW_DIRTY_PAGE_MAX at one time. */
    for (int i = 0; i < runs; i++) {
        /* Last batch, take the rest of the buffers */
        int offset = i * dw_batch_page_max;
        int batch_num = (i == runs - 1) ? (need_flush_num - offset) : dw_batch_page_max;
        uint32 flush_num;

        pgwr->thrd_dw_cxt.is_new_relfilenode = is_new_relfilenode;
        pgwr->thrd_dw_cxt.dw_page_idx = -1;
        dw_perform_batch_flush(batch_num, dirty_buf_list + offset, thread_id, &pgwr->thrd_dw_cxt);
        flush_num = incre_ckpt_pgwr_flush_dirty_page(wb_context, dirty_buf_list, offset, batch_num);
        pgwr->thrd_dw_cxt.dw_page_idx = -1;
        num_actual_flush += flush_num;
    }

    (void)pg_atomic_fetch_add_u64(&g_instance.ckpt_cxt_ctl->page_writer_actual_flush, num_actual_flush);
    (void)pg_atomic_fetch_add_u32(&g_instance.ckpt_cxt_ctl->page_writer_last_flush, num_actual_flush);
    (void)pg_atomic_fetch_add_u32(&g_instance.ckpt_cxt_ctl->page_writer_last_queue_flush, num_actual_flush);

    return;
}

static void incre_ckpt_pgwr_flush_dirty_list(WritebackContext *wb_context, uint32 need_flush_num,
    bool is_new_relfilenode)
{
    int thread_id = t_thrd.pagewriter_cxt.pagewriter_id;
    PageWriterProc *pgwr = &g_instance.ckpt_cxt_ctl->pgwr_procs.writer_proc[thread_id];
    CkptSortItem *dirty_buf_list = pgwr->dirty_buf_list;
    int dw_batch_page_max = GET_DW_DIRTY_PAGE_MAX(is_new_relfilenode);
    int runs = (need_flush_num + dw_batch_page_max - 1) / dw_batch_page_max;
    int num_actual_flush = 0;
    int buf_id;
    BufferDesc *buf_desc = NULL;

    qsort(dirty_buf_list, need_flush_num, sizeof(CkptSortItem), ckpt_buforder_comparator);
    ResourceOwnerEnlargeBuffers(t_thrd.utils_cxt.CurrentResourceOwner);

    /* Double write can only handle at most DW_DIRTY_PAGE_MAX at one time. */
    for (int i = 0; i < runs; i++) {
        /* Last batch, take the rest of the buffers */
        int offset = i * dw_batch_page_max;
        int batch_num = (i == runs - 1) ? (need_flush_num - offset) : dw_batch_page_max;
        uint32 flush_num;

        pgwr->thrd_dw_cxt.is_new_relfilenode = is_new_relfilenode;
        pgwr->thrd_dw_cxt.dw_page_idx = -1;
        dw_perform_batch_flush(batch_num, dirty_buf_list + offset, thread_id, &pgwr->thrd_dw_cxt);
        flush_num = incre_ckpt_pgwr_flush_dirty_page(wb_context, dirty_buf_list, offset, batch_num);
        pgwr->thrd_dw_cxt.dw_page_idx = -1;
        num_actual_flush += flush_num;
    }

    (void)pg_atomic_fetch_add_u64(&g_instance.ckpt_cxt_ctl->page_writer_actual_flush, num_actual_flush);
    (void)pg_atomic_fetch_add_u32(&g_instance.ckpt_cxt_ctl->page_writer_last_flush, num_actual_flush);

    for (uint32 i = 0; i < need_flush_num; i++) {
        buf_id = dirty_buf_list[i].buf_id;
        if (buf_id == DW_INVALID_BUFFER_ID) {
            continue;
        }
        buf_desc = GetBufferDescriptor(buf_id);
        push_to_candidate_list(buf_desc);
    }

    if (u_sess->attr.attr_storage.log_pagewriter) {
        ereport(LOG,
            (errmodule(MOD_INCRE_CKPT),
                errmsg("flush dirty page %d, thread id is %d", num_actual_flush, thread_id)));
    }
}

static bool check_buffer_dirty_flag(BufferDesc* buf_desc)
{   
    /* This function in the pagewriter thread has a concurrency problem with invalidatebuffer of dms reform rebuild,
     * there probably exists condition that one page is mark as dirty page in pagewriter thread and invalidatebuffer
     * of rebuild is executed before PinBuffer in this function. When flushbuffer function is executed, the tag of 
     * this buffer is cleaned so that flushbuffer cause core problem.The dirty operation is performed by other normal
     * dirty operation logics (for example, the dirty operation is placed back).
     */
    if (ENABLE_DMS) {
        return false;
    }
    
    bool segment_buf = (buf_desc->buf_id >= SegmentBufferStartID);
    Block tmpBlock = BufHdrGetBlock(buf_desc);
    uint64 local_buf_state = pg_atomic_read_u64(&buf_desc->state);
    bool check_lsn_not_match = (local_buf_state & BM_VALID) && !(local_buf_state & BM_DIRTY) &&
        XLByteLT(buf_desc->extra->lsn_on_disk, PageGetLSN(tmpBlock)) && RecoveryInProgress() && !segment_buf;

    if (ENABLE_DMS && check_lsn_not_match &&
        (XLogRecPtrIsInvalid(buf_desc->extra->lsn_on_disk) ||
         GetDmsBufCtrl(buf_desc->buf_id)->state & BUF_DIRTY_NEED_FLUSH)) {
        return false;
    }

    if (check_lsn_not_match) {
        PinBuffer(buf_desc, NULL);
        if (LWLockConditionalAcquire(buf_desc->content_lock, LW_SHARED)) {
            pg_memory_barrier();
            local_buf_state = pg_atomic_read_u64(&buf_desc->state);
            check_lsn_not_match = (local_buf_state & BM_VALID) && !(local_buf_state & BM_DIRTY) &&
                XLByteLT(buf_desc->extra->lsn_on_disk, PageGetLSN(tmpBlock)) && RecoveryInProgress();
            if (check_lsn_not_match) {
                MarkBufferDirty(BufferDescriptorGetBuffer(buf_desc));
                LWLockRelease(buf_desc->content_lock);
                UnpinBuffer(buf_desc, true);
                const uint32 shiftSize = 32;
                ereport(DEBUG1, (errmodule(MOD_INCRE_BG),
                    errmsg("check lsn is not matched on disk:%X/%X on page %X/%X, relnode info:%u/%u/%u %u %u stat:%lu",
                        (uint32)(buf_desc->extra->lsn_on_disk >> shiftSize), (uint32)(buf_desc->extra->lsn_on_disk),
                        (uint32)(PageGetLSN(tmpBlock) >> shiftSize), (uint32)(PageGetLSN(tmpBlock)),
                        buf_desc->tag.rnode.spcNode, buf_desc->tag.rnode.dbNode, buf_desc->tag.rnode.relNode,
                        buf_desc->tag.blockNum, buf_desc->tag.forkNum, local_buf_state)));

                return true;
            } else {
                LWLockRelease(buf_desc->content_lock);
                UnpinBuffer(buf_desc, true);
                return false;
            }
        } else {
            UnpinBuffer(buf_desc, true);
            return false;
        }
    }
    return false;
}

static uint32 get_list_flush_max_num(CandListType type)
{
    int thread_num = g_instance.ckpt_cxt_ctl->pgwr_procs.sub_num;
    uint32 max_io = g_instance.ckpt_cxt_ctl->pgwr_procs.list_flush_max / thread_num;
    uint32 dirty_list_size = MAX_DIRTY_LIST_FLUSH_NUM / thread_num;

    if (type == CAND_LIST_SEG) {
        double seg_percent = ((double)(SEGMENT_BUFFER_NUM) / (double)(TOTAL_BUFFER_NUM));
        max_io = max_io * seg_percent;
    } else if (type == CAND_LIST_NVM) {
        double nvm_percent = ((double)(NVM_BUFFER_NUM) / (double)(TOTAL_BUFFER_NUM));
        max_io = max_io * nvm_percent;
    } else {
        double buffer_percent = ((double)(NORMAL_SHARED_BUFFER_NUM) / (double)(TOTAL_BUFFER_NUM));
        max_io = max_io * buffer_percent;
    }
    max_io = MAX(max_io, DW_DIRTY_PAGE_MAX_FOR_NOHBK);
    max_io = MIN(max_io, dirty_list_size);

    return max_io;
}

const float GAP_PERCENT = 0.15;
static uint32 get_list_flush_num(CandListType type)
{
    double percent_target = u_sess->attr.attr_storage.candidate_buf_percent_target;
    uint32 cur_candidate_num;
    uint32 total_target;
    uint32 high_water_mark;
    uint32 flush_num = 0;
    uint32 min_io = DW_DIRTY_PAGE_MAX_FOR_NOHBK;
    uint32 max_io = get_list_flush_max_num(type);
    uint32 buffer_num;
    if (type == CAND_LIST_SEG) {
        buffer_num = SEGMENT_BUFFER_NUM;
    } else if (type == CAND_LIST_NVM) {
        buffer_num = NVM_BUFFER_NUM;
    } else {
        buffer_num = NORMAL_SHARED_BUFFER_NUM;
    }
    total_target = buffer_num * percent_target;
    high_water_mark = buffer_num * (percent_target / HIGH_WATER);
    cur_candidate_num = get_curr_candidate_nums(type);

    /* If the slots are sufficient, the standby DN does not need to flush too many pages. */
    if (RecoveryInProgress() && cur_candidate_num >= total_target / 2) {
        max_io = max_io / 2;
        if (unlikely(max_io < min_io)) {
            max_io = min_io;
        }
    }

    if (cur_candidate_num >= high_water_mark) {
        flush_num = min_io;  /* only flush one batch dirty page */
    } else if (cur_candidate_num >= total_target) {
        flush_num = min_io + (float)(high_water_mark - cur_candidate_num) /
            (float)(high_water_mark - total_target) * (max_io - min_io);
    } else {
        /* every time flush max_io dirty pages */
        flush_num = max_io;
    }

    ereport(DEBUG1, (errmodule(MOD_INCRE_BG),
        errmsg("list flush_num is %u, now candidate buf is %u", flush_num, cur_candidate_num)));
    return flush_num;
}

/**
 * @Description: First , the pagewriter sub thread scan the normal buffer pool,
 *            then scan the segment buffer pool.
 */
const int MAX_SCAN_BATCH_NUM = 131072 * 10; /* 10GB buffers */

static void incre_ckpt_pgwr_scan_candidate_list(WritebackContext *wb_context, CandidateList *list, CandListType type)
{
    int ratio_buf_id_start;
    int ratio_cand_list_size;
    int ratio_avg_num;
    int ratio_shared_buffers;
    int thread_num = g_instance.ckpt_cxt_ctl->pgwr_procs.sub_num;
    int thread_id = t_thrd.pagewriter_cxt.pagewriter_id;
    uint32 need_flush_num = 0;
    int start = 0;
    int end = 0;
    bool is_new_relfilenode = false;
    int batch_scan_num = 0;
    uint32 max_flush_num = 0;
    bool am_standby = g_instance.ckpt_cxt_ctl->is_standby_mode;
    int shared_buffers = NORMAL_SHARED_BUFFER_NUM;

    if (type == CAND_LIST_NVM) {
        shared_buffers = NVM_BUFFER_NUM;
    } else if (type == CAND_LIST_NORMAL) {
        shared_buffers = NORMAL_SHARED_BUFFER_NUM;
    }

    if (get_thread_candidate_nums(list) < list->cand_list_size) {
        if (am_standby && type != CAND_LIST_SEG) {
            ratio_shared_buffers = int(shared_buffers * u_sess->attr.attr_storage.shared_buffers_fraction);
            ratio_avg_num = ratio_shared_buffers / thread_num;
            ratio_buf_id_start = ratio_avg_num * (thread_id - 1);
            ratio_cand_list_size = ratio_avg_num;
            if (thread_id == thread_num) {
                ratio_cand_list_size += ratio_shared_buffers % thread_num;
            }

            start = MAX(ratio_buf_id_start, list->next_scan_ratio_loc);
            end = ratio_buf_id_start + ratio_cand_list_size;
            batch_scan_num = MIN(ratio_cand_list_size, MAX_SCAN_BATCH_NUM);
        } else {
            start = MAX(list->buf_id_start, list->next_scan_loc);
            end = list->buf_id_start + list->cand_list_size;
            batch_scan_num = MIN(list->cand_list_size, MAX_SCAN_BATCH_NUM);
        }

        end = MIN(start + batch_scan_num, end);
        max_flush_num = get_list_flush_num(type);
        need_flush_num = get_candidate_buf_and_flush_list(start, end, max_flush_num, &is_new_relfilenode);

        if (am_standby && type != CAND_LIST_SEG) {
            if (end >= ratio_buf_id_start + ratio_cand_list_size) {
                list->next_scan_ratio_loc = ratio_buf_id_start;
            } else {
                list->next_scan_ratio_loc = end;
            }
        } else {
            if (end >= list->buf_id_start + list->cand_list_size) {
                list->next_scan_loc = list->buf_id_start;
            } else {
                list->next_scan_loc = end;
            }
        }

        if (need_flush_num > 0) {
            incre_ckpt_pgwr_flush_dirty_list(wb_context, need_flush_num, is_new_relfilenode);
        }
    }
}

static void incre_ckpt_pgwr_scan_buf_pool(WritebackContext *wb_context)
{
    int thread_id = t_thrd.pagewriter_cxt.pagewriter_id;
    PageWriterProc *pgwr = &g_instance.ckpt_cxt_ctl->pgwr_procs.writer_proc[thread_id];

    /* handle the normal\nvm\segment buffer pool */
    incre_ckpt_pgwr_scan_candidate_list(wb_context, &pgwr->normal_list, CAND_LIST_NORMAL);
    incre_ckpt_pgwr_scan_candidate_list(wb_context, &pgwr->nvm_list, CAND_LIST_NVM);
    incre_ckpt_pgwr_scan_candidate_list(wb_context, &pgwr->seg_list, CAND_LIST_SEG);
    return;
}

/**
 * @Description: Scan n buffers in the BufferPool from start to end, put the unreferenced and not dirty
 *             page into the candidate list, the unreferenced and dirty page into the dirty list.
 * @in: start, can the buffer pool start loc
 * @in: end, scan the buffer pool end loc
 * @in: max_flush_num, num of pages that can be flushed this time.
 * @out: Return the number of dirty buffers and dirty buffer list and this batch buffer
 *      whether hashbucket is included.
 */
static uint32 get_candidate_buf_and_flush_list(uint32 start, uint32 end, uint32 max_flush_num,
    bool *contain_hashbucket)
{
    uint32 need_flush_num = 0;
    uint32 candidates = 0;
    BufferDesc *buf_desc = NULL;
    uint64 local_buf_state;
    CkptSortItem* item = NULL;
    bool check_not_need_flush = false;
    bool check_usecount = false;
    int thread_id = t_thrd.pagewriter_cxt.pagewriter_id;
    PageWriterProc *pgwr = &g_instance.ckpt_cxt_ctl->pgwr_procs.writer_proc[thread_id];
    CkptSortItem *dirty_buf_list = pgwr->dirty_buf_list;

    ResourceOwnerEnlargeBuffers(t_thrd.utils_cxt.CurrentResourceOwner);

    max_flush_num = ((FULL_CKPT && !RecoveryInProgress()) ? 0 : max_flush_num);

    for (uint32 buf_id = start; buf_id < end; buf_id++) {
        buf_desc = GetBufferDescriptor(buf_id);
        local_buf_state = pg_atomic_read_u64(&buf_desc->state);

        /* during recovery, check the data page whether not properly marked as dirty */
        if (RecoveryInProgress() && check_buffer_dirty_flag(buf_desc)) {
            if (need_flush_num < max_flush_num) {
                local_buf_state = LockBufHdr(buf_desc);
                goto PUSH_DIRTY;
            } else {
                continue;
            }
        }
        /* Dirty read, pinned buffer, skip */
        if (BUF_STATE_GET_REFCOUNT(local_buf_state) > 0) {
            continue;
        }

        local_buf_state = LockBufHdr(buf_desc);
        if (BUF_STATE_GET_REFCOUNT(local_buf_state) > 0) {
            goto UNLOCK;
        }

        check_usecount = NEED_CONSIDER_USECOUNT && BUF_STATE_GET_USAGECOUNT(local_buf_state) != 0;
        if (check_usecount) {
            local_buf_state -= BUF_USAGECOUNT_ONE;
            goto UNLOCK;
        }

        /* Not dirty, put directly into flushed candidates */
        if (!(local_buf_state & BM_DIRTY)) {
            if (g_instance.ckpt_cxt_ctl->candidate_free_map[buf_id] == false) {
                if (buf_id < (uint32)NvmBufferStartID) {
                    candidate_buf_push(&pgwr->normal_list, buf_id);
                } else if (buf_id < (uint32)SegmentBufferStartID) {
                    candidate_buf_push(&pgwr->nvm_list, buf_id);
                } else {
                    candidate_buf_push(&pgwr->seg_list, buf_id);
                }
                g_instance.ckpt_cxt_ctl->candidate_free_map[buf_id] = true;
                candidates++;
            }
            goto UNLOCK;
        }

        check_not_need_flush = (need_flush_num >= max_flush_num || (!RecoveryInProgress()
            && XLogNeedsFlush(BufferGetLSN(buf_desc))));
        if (check_not_need_flush) {
            goto UNLOCK;
        }

PUSH_DIRTY:
        local_buf_state |= BM_CHECKPOINT_NEEDED;
        item = &dirty_buf_list[need_flush_num++];
        item->buf_id = buf_id;
        item->tsId = buf_desc->tag.rnode.spcNode;
        item->relNode = buf_desc->tag.rnode.relNode;
        item->bucketNode = buf_desc->tag.rnode.bucketNode;
        item->forkNum = buf_desc->tag.forkNum;
        item->blockNum = buf_desc->tag.blockNum;
        if (IsSegmentFileNode(buf_desc->tag.rnode) || IS_COMPRESSED_RNODE(buf_desc->tag.rnode, buf_desc->tag.forkNum)) {
            *contain_hashbucket = true;
        }

UNLOCK:
        UnlockBufHdr(buf_desc, local_buf_state);
    }

    if (u_sess->attr.attr_storage.log_pagewriter) {
        ereport(LOG,
            (errmodule(MOD_INCRE_CKPT),
                errmsg("get candidate buf %d, thread id is %d", candidates, thread_id)));
    }
    return need_flush_num;
}

static void push_to_candidate_list(BufferDesc *buf_desc)
{
    uint32 thread_id = t_thrd.pagewriter_cxt.pagewriter_id;
    PageWriterProc *pgwr = &g_instance.ckpt_cxt_ctl->pgwr_procs.writer_proc[thread_id];
    int buf_id = buf_desc->buf_id;
    uint64 buf_state = pg_atomic_read_u64(&buf_desc->state);
    bool emptyUsageCount = (!NEED_CONSIDER_USECOUNT || BUF_STATE_GET_USAGECOUNT(buf_state) == 0);

    if (BUF_STATE_GET_REFCOUNT(buf_state) > 0 || !emptyUsageCount) {
        return;
    }

    if (g_instance.ckpt_cxt_ctl->candidate_free_map[buf_id] == false) {
        buf_state = LockBufHdr(buf_desc);
        if (g_instance.ckpt_cxt_ctl->candidate_free_map[buf_id] == false) {
            emptyUsageCount = (!NEED_CONSIDER_USECOUNT || BUF_STATE_GET_USAGECOUNT(buf_state) == 0);
            if (BUF_STATE_GET_REFCOUNT(buf_state) == 0 && emptyUsageCount && !(buf_state & BM_DIRTY)) {
                if (buf_id < NvmBufferStartID) {
                    candidate_buf_push(&pgwr->normal_list, buf_id);
                } else if (buf_id < SegmentBufferStartID) {
                    candidate_buf_push(&pgwr->nvm_list, buf_id);
                } else {
                    candidate_buf_push(&pgwr->seg_list, buf_id);
                }
                g_instance.ckpt_cxt_ctl->candidate_free_map[buf_id] = true;
            }
        }
        UnlockBufHdr(buf_desc, buf_state);
    }
    return;
}


/**
 * @Description: Push buffer bufId to thread threadId's candidate list.
 * @in: buf_id, buffer id which need push to the list
 * @in: thread_id, pagewriter thread id.
 */
static void candidate_buf_push(CandidateList *list, int buf_id)
{
    uint32 list_size = list->cand_list_size;
    uint32 tail_loc;

    pg_memory_barrier();
    volatile uint64 head = pg_atomic_read_u64(&list->head);
    pg_memory_barrier();
    volatile uint64 tail = pg_atomic_read_u64(&list->tail);

    if (unlikely(tail - head >= list_size)) {
        return;
    }
    tail_loc = tail % list_size;
    list->cand_buf_list[tail_loc] = buf_id;
    (void)pg_atomic_fetch_add_u64(&list->tail, 1);
}

/**
 * @Description: Pop a buffer from the head of thread threadId's candidate list and store the buffer in buf_id.
 * @in: buf_id, store the buffer id from the list.
 * @in: thread_id, pagewriter thread id
 */
bool candidate_buf_pop(CandidateList *list, int *buf_id)
{
    uint32 list_size = list->cand_list_size;
    uint32 head_loc;

    while (true) {
        pg_memory_barrier();
        uint64 head = pg_atomic_read_u64(&list->head);
        pg_memory_barrier();
        volatile uint64 tail = pg_atomic_read_u64(&list->tail);

        if (unlikely(head >= tail)) {
            return false;       /* candidate list is empty */
        }

        head_loc = head % list_size;
        *buf_id = list->cand_buf_list[head_loc];
        if (pg_atomic_compare_exchange_u64(&list->head, &head, head + 1)) {
            return true;
        }
    }
}

static int64 get_thread_candidate_nums(CandidateList *list)
{
    volatile uint64 head = pg_atomic_read_u64(&list->head);
    pg_memory_barrier();
    volatile uint64 tail = pg_atomic_read_u64(&list->tail);
    int64 curr_cand_num = tail - head;
    Assert(curr_cand_num >= 0);
    return curr_cand_num;
}

/**
 * @Description: Return a rough estimate of the current number of buffers in the candidate list.
 */
uint32 get_curr_candidate_nums(CandListType type)
{
    uint32 currCandidates = 0;
    PageWriterProc *pgwr = NULL;
    CandidateList *list = NULL;

    for (int i = 1; i < g_instance.ckpt_cxt_ctl->pgwr_procs.num; i++) {
        pgwr = &g_instance.ckpt_cxt_ctl->pgwr_procs.writer_proc[i];

        if (type == CAND_LIST_NORMAL) {
            list = &pgwr->normal_list;
        } else if (type == CAND_LIST_NVM) {
            list = &pgwr->nvm_list;
        } else {
            list = &pgwr->seg_list;
        }

        if (pgwr->proc != NULL) {
            currCandidates += get_thread_candidate_nums(list);
        }
    }
    return currCandidates;
}

static void ckpt_pagewriter_sighup_handler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.pagewriter_cxt.got_SIGHUP = true;
    if (t_thrd.proc) {
        SetLatch(&t_thrd.proc->procLatch);
    }

    errno = save_errno;
}

/* SIGINT: set flag to run a normal checkpoint right away */
static void ckpt_pagewriter_sigint_handler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.pagewriter_cxt.sync_requested = true;

    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);

    errno = save_errno;
}

static void ckpt_pagewriter_quick_die(SIGNAL_ARGS)
{
    gs_signal_setmask(&t_thrd.libpq_cxt.BlockSig, NULL);

    /*
     * We DO NOT want to run proc_exit() callbacks -- we're here because
     * shared memory may be corrupted, so we don't want to try to clean up our
     * transaction.  Just nail the windows shut and get out of town.  Now that
     * there's an atexit callback to prevent third-party code from breaking
     * things by calling exit() directly, we have to reset the callbacks
     * explicitly to make this work as intended.
     */
    on_exit_reset();

    /*
     * Note we do exit(2) not exit(0).    This is to force the postmaster into a
     * system reset cycle if some idiot DBA sends a manual SIGQUIT to a random
     * backend.  This is necessary precisely because we don't clean up our
     * shared memory state.  (The "dead man switch" mechanism in pmsignal.c
     * should ensure the postmaster sees this as a crash, too, but no harm in
     * being doubly sure.)
     */
    gs_thread_exit(EXIT_MODE_TWO);
}

static void ckpt_pagewriter_request_shutdown_handler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.pagewriter_cxt.shutdown_requested = true;
    if (t_thrd.proc) {
        SetLatch(&t_thrd.proc->procLatch);
    }

    errno = save_errno;
}

/* SIGUSR1: used for latch wakeups */
static void ckpt_pagewriter_sigusr1_handler(SIGNAL_ARGS)
{
    int save_errno = errno;

    latch_sigusr1_handler();

    errno = save_errno;
}

/* Shutdown all the page writer threads. */
void ckpt_shutdown_pagewriter()
{
    g_instance.ckpt_cxt_ctl->page_writer_can_exit = true;

    /* Wait for all page writer threads to exit. */
    while (pg_atomic_read_u32(&g_instance.ckpt_cxt_ctl->current_page_writer_count) != 0) {
        pg_usleep(MILLISECOND_TO_MICROSECOND);
    }
}


/* The following functions are used by the pagewriter thread to process file sync requests. */

Size PageWriterShmemSize(void)
{
    Size size;

    /* Currently, the size of the requests[] array is arbitrarily set equal TOTAL_BUFFER_NUM */
    size = offsetof(IncreCkptSyncShmemStruct, requests);
    size = add_size(size, mul_size(TOTAL_BUFFER_NUM, sizeof(CheckpointerRequest)));

    return size;
}

/*
 * PageWriterSyncShmemInit
 *        Allocate and initialize shared memory of pagewriter handle sync request.
 */
void PageWriterSyncShmemInit(void)
{
    Size size = PageWriterShmemSize();
    bool found = false;

    g_instance.ckpt_cxt_ctl->incre_ckpt_sync_shmem =
        (IncreCkptSyncShmemStruct*)ShmemInitStruct("Incre Ckpt Sync Data", size, &found);

    if (!found) {
        /* The memory of the memset sometimes exceeds 2 GB. so, memset_s cannot be used. */
        MemSet((char*)g_instance.ckpt_cxt_ctl->incre_ckpt_sync_shmem, 0, size);
        SpinLockInit(&g_instance.ckpt_cxt_ctl->incre_ckpt_sync_shmem->sync_lock);
        g_instance.ckpt_cxt_ctl->incre_ckpt_sync_shmem->max_requests = TOTAL_BUFFER_NUM;
        g_instance.ckpt_cxt_ctl->incre_ckpt_sync_shmem->sync_queue_lwlock = LWLockAssign(LWTRANCHE_PGWR_SYNC_QUEUE);
    }
}
