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
#include "storage/smgr.h"
#include "storage/pmsignal.h"
#include "access/double_write.h"
#include "access/xlog.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/builtins.h"
#include "gssignal/gs_signal.h"
#include "gstrace/gstrace_infra.h"
#include "gstrace/postmaster_gstrace.h"

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

/* Signal handlers */
static void ckpt_pagewriter_sighup_handler(SIGNAL_ARGS);
static void ckpt_pagewriter_quick_die(SIGNAL_ARGS);
static void ckpt_pagewriter_request_shutdown_handler(SIGNAL_ARGS);
static void ckpt_pagewriter_sigusr1_handler(SIGNAL_ARGS);
static void ckpt_try_skip_invalid_elem_in_queue_head();
static void ckpt_try_prune_dirty_page_queue();
static uint32 calculate_pagewriter_flush_num();

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

    ret = snprintf_s(queue_rec_lsn_s,
        LSN_LENGTH,
        LSN_LENGTH - 1,
        "%X/%X",
        (uint32)(queue_rec_lsn >> XLOG_LSN_SWAP),
        (uint32)queue_rec_lsn);
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

    ret = snprintf_s(queue_rec_lsn_s,
        LSN_LENGTH,
        LSN_LENGTH - 1,
        "%X/%X",
        (uint32)(queue_rec_lsn >> XLOG_LSN_SWAP),
        (uint32)queue_rec_lsn);
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

    ret = snprintf_s(current_lsn_s,
        LSN_LENGTH,
        LSN_LENGTH - 1,
        "%X/%X",
        (uint32)(current_xlog_insert >> XLOG_LSN_SWAP),
        (uint32)current_xlog_insert);
    securec_check_ss(ret, "", "");
    return CStringGetTextDatum(current_lsn_s);
}

Datum ckpt_view_get_redo_point()
{
    errno_t ret;
    char redo_lsn_s[LSN_LENGTH];
    XLogRecPtr redo_lsn = g_instance.ckpt_cxt_ctl->ckpt_current_redo_point;

    ret = memset_s(redo_lsn_s, LSN_LENGTH, 0, LSN_LENGTH);
    securec_check(ret, "", "");

    ret = snprintf_s(
        redo_lsn_s, LSN_LENGTH, LSN_LENGTH - 1, "%X/%X", (uint32)(redo_lsn >> XLOG_LSN_SWAP), (uint32)redo_lsn);
    securec_check_ss(ret, "", "");
    return CStringGetTextDatum(redo_lsn_s);
}

Datum ckpt_view_get_clog_flush_num()
{
    return Int64GetDatum(g_instance.ckpt_cxt_ctl->ckpt_clog_flush_num);
}

Datum ckpt_view_get_csnlog_flush_num()
{
    return Int64GetDatum(g_instance.ckpt_cxt_ctl->ckpt_csnlog_flush_num);
}

Datum ckpt_view_get_multixact_flush_num()
{
    return Int64GetDatum(g_instance.ckpt_cxt_ctl->ckpt_multixact_flush_num);
}

Datum ckpt_view_get_predicate_flush_num()
{
    return Int64GetDatum(g_instance.ckpt_cxt_ctl->ckpt_predicate_flush_num);
}

Datum ckpt_view_get_twophase_flush_num()
{
    return Int64GetDatum(g_instance.ckpt_cxt_ctl->ckpt_twophase_flush_num);
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

void incre_ckpt_pagewriter_cxt_init()
{
    MemoryContext oldcontext = MemoryContextSwitchTo(g_instance.increCheckPoint_context);

    g_instance.ckpt_cxt_ctl->page_writer_procs.writer_proc =
        (PageWriterProc*)palloc0(sizeof(PageWriterProc) * g_instance.attr.attr_storage.pagewriter_thread_num);
    g_instance.ckpt_cxt_ctl->page_writer_procs.num = g_instance.attr.attr_storage.pagewriter_thread_num;

    g_instance.ckpt_cxt_ctl->page_writer_procs.running_num = 0;

    char *unaligned_buf = (char*)palloc0((DW_BUF_MAX_FOR_NOHBK + 1) * BLCKSZ);
	g_instance.ckpt_cxt_ctl->page_writer_procs.thrd_dw_cxt.dw_buf = (char*)TYPEALIGN(BLCKSZ, unaligned_buf);
	g_instance.ckpt_cxt_ctl->page_writer_procs.thrd_dw_cxt.dw_page_idx = -1;
	g_instance.ckpt_cxt_ctl->page_writer_procs.thrd_dw_cxt.contain_hashbucket = false;

    (void)MemoryContextSwitchTo(oldcontext);
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

bool atomic_push_pending_flush_queue(Buffer buffer, XLogRecPtr* queue_head_lsn, uint64* new_tail_loc)
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


bool push_pending_flush_queue(Buffer buffer)
{
    uint64 new_tail_loc = 0;
    uint64 actual_loc;
    XLogRecPtr queue_head_lsn = InvalidXLogRecPtr;
    BufferDesc* buf_desc = GetBufferDescriptor(buffer - 1);
    bool push_finish = false;

    Assert(XLogRecPtrIsInvalid(pg_atomic_read_u64(&buf_desc->rec_lsn)));
#if defined(__x86_64__) || defined(__aarch64__)
    push_finish = atomic_push_pending_flush_queue(buffer, &queue_head_lsn, &new_tail_loc);
    if (!push_finish) {
        return false;
    }
#else
    SpinLockAcquire(&g_instance.ckpt_cxt_ctl->queue_lock);

    if ((uint64)(get_dirty_page_num() + PAGE_QUEUE_SLOT_MIN_RESERVE_NUM) >=
        g_instance.ckpt_cxt_ctl->dirty_page_queue_size) {
        SpinLockRelease(&g_instance.ckpt_cxt_ctl->queue_lock);
        return false;
    }
    new_tail_loc = g_instance.ckpt_cxt_ctl->dirty_page_queue_tail;
    g_instance.ckpt_cxt_ctl->dirty_page_queue_tail++;
    SpinLockRelease(&g_instance.ckpt_cxt_ctl->queue_lock);
#endif

    pg_atomic_write_u64(&buf_desc->rec_lsn, queue_head_lsn);
    actual_loc = new_tail_loc % g_instance.ckpt_cxt_ctl->dirty_page_queue_size;
    buf_desc->dirty_queue_loc = actual_loc;
    g_instance.ckpt_cxt_ctl->dirty_page_queue[actual_loc].buffer = buffer;
    pg_write_barrier();
    pg_atomic_write_u32(&g_instance.ckpt_cxt_ctl->dirty_page_queue[actual_loc].slot_state, (SLOT_VALID));
    (void)pg_atomic_fetch_add_u32(&g_instance.ckpt_cxt_ctl->actual_dirty_page_num, 1);
    return true;
}

void remove_dirty_page_from_queue(BufferDesc* buf)
{
    Assert(buf->dirty_queue_loc != PG_UINT64_MAX);
    g_instance.ckpt_cxt_ctl->dirty_page_queue[buf->dirty_queue_loc].buffer = 0;
    pg_atomic_write_u64(&buf->rec_lsn, InvalidXLogRecPtr);
    buf->dirty_queue_loc = PG_UINT64_MAX;
    (void)pg_atomic_fetch_sub_u32(&g_instance.ckpt_cxt_ctl->actual_dirty_page_num, 1);
}

uint64 get_dirty_page_queue_tail()
{
#if defined(__x86_64__) || defined(__aarch64__)
    uint128_u compare;
    compare = atomic_compare_and_swap_u128((uint128_u*)&g_instance.ckpt_cxt_ctl->dirty_page_queue_reclsn);
    /* return the dirty page queue tail */
    return compare.u64[1];
#else
    uint64 tail;
    SpinLockAcquire(&g_instance.ckpt_cxt_ctl->queue_lock);
    tail = g_instance.ckpt_cxt_ctl->dirty_page_queue_tail;
    SpinLockRelease(&g_instance.ckpt_cxt_ctl->queue_lock);

    return tail;
#endif
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

    if (g_instance.ckpt_cxt_ctl->flush_all_dirty_page) {
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
static uint32 ckpt_qsort_dirty_page_for_flush(bool *contain_hashbucket)
{
    uint32 num_to_flush = 0;
    bool retry = false;
    errno_t rc;
    uint32 i;
    int64 dirty_page_num;
    uint64 dirty_queue_head;
    uint32 buffer_slot_num = MIN(DW_DIRTY_PAGE_MAX_FOR_NOHBK, g_instance.attr.attr_storage.NBuffers);

    rc = memset_s(g_instance.ckpt_cxt_ctl->CkptBufferIds,
        buffer_slot_num * sizeof(CkptSortItem),
        0,
        buffer_slot_num * sizeof(CkptSortItem));
    securec_check(rc, "", "");

try_get_buf:
    /*
     * Before selecting a batch of dirty pages to flush, move dirty page queue head to
     * skip slot of invalid buffer of queue head.
     */
    ckpt_try_skip_invalid_elem_in_queue_head();
    dirty_page_num = get_dirty_page_num();
    dirty_queue_head = pg_atomic_read_u64(&g_instance.ckpt_cxt_ctl->dirty_page_queue_head);

    for (i = 0; i < dirty_page_num; i++) {
        uint32 buf_state;
        Buffer buffer;
        BufferDesc* buf_desc = NULL;
        CkptSortItem* item = NULL;
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
        buf_state = LockBufHdr(buf_desc);

        if ((buf_state & BM_DIRTY) && (retry ||!(buf_state & BM_CHECKPOINT_NEEDED))) {
            buf_state |= BM_CHECKPOINT_NEEDED;
            item = &g_instance.ckpt_cxt_ctl->CkptBufferIds[num_to_flush++];
            item->buf_id = buffer - 1;
            item->tsId = buf_desc->tag.rnode.spcNode;
            item->relNode = buf_desc->tag.rnode.relNode;
            item->bucketNode = buf_desc->tag.rnode.bucketNode;
            item->forkNum = buf_desc->tag.forkNum;
            item->blockNum = buf_desc->tag.blockNum;
            if(buf_desc->tag.rnode.bucketNode != InvalidBktId) {
                *contain_hashbucket = true;
            }
        } else {
            if (!(buf_state & BM_DIRTY) && slot->buffer != 0) {
                ereport(WARNING,
                    (errmsg("not dirty page in dirty page queue, the buf_state is %u, filenode is %u/%u/%u/%d",
                    buf_state, buf_desc->tag.rnode.spcNode, buf_desc->tag.rnode.dbNode,
                    buf_desc->tag.rnode.relNode, buf_desc->tag.rnode.bucketNode)));

                buf_state &= (~BM_CHECKPOINT_NEEDED);
                remove_dirty_page_from_queue(buf_desc);
            }
        }
        UnlockBufHdr(buf_desc, buf_state);
        if (num_to_flush >= buffer_slot_num || num_to_flush >= GET_DW_DIRTY_PAGE_MAX(*contain_hashbucket)) {
            break;
        }
    }

    num_to_flush = MIN(num_to_flush, GET_DW_DIRTY_PAGE_MAX(*contain_hashbucket));
    if (num_to_flush == 0 && g_instance.ckpt_cxt_ctl->actual_dirty_page_num > 0) {
        retry = true;
        goto try_get_buf;
    }
    qsort(g_instance.ckpt_cxt_ctl->CkptBufferIds, num_to_flush, sizeof(CkptSortItem), ckpt_buforder_comparator);

    return num_to_flush;
}

/**
 * @Description: Distribute the batch dirty pages to multiple pagewriter threads to flush
 * @in:          num of this batch dirty page
 */
void divide_dirty_page_to_thread(uint32 requested_flush_num)
{
    uint32 thread_min_flush;
    uint32 remain_need_flush;
    int thread_loc;

    thread_min_flush = requested_flush_num / g_instance.ckpt_cxt_ctl->page_writer_procs.num;
    remain_need_flush = requested_flush_num % g_instance.ckpt_cxt_ctl->page_writer_procs.num;

    for (thread_loc = 0; thread_loc < g_instance.ckpt_cxt_ctl->page_writer_procs.num; thread_loc++) {
        if (thread_loc == 0) {
            g_instance.ckpt_cxt_ctl->page_writer_procs.writer_proc[thread_loc].start_loc = 0;
            g_instance.ckpt_cxt_ctl->page_writer_procs.writer_proc[thread_loc].end_loc =
                thread_min_flush + remain_need_flush - 1;
        } else {
            g_instance.ckpt_cxt_ctl->page_writer_procs.writer_proc[thread_loc].start_loc =
                g_instance.ckpt_cxt_ctl->page_writer_procs.writer_proc[thread_loc - 1].end_loc + 1;
            g_instance.ckpt_cxt_ctl->page_writer_procs.writer_proc[thread_loc].end_loc =
                g_instance.ckpt_cxt_ctl->page_writer_procs.writer_proc[thread_loc].start_loc + thread_min_flush - 1;
        }
        (void)pg_atomic_add_fetch_u32(&g_instance.ckpt_cxt_ctl->page_writer_procs.running_num, 1);
        pg_write_barrier();
        g_instance.ckpt_cxt_ctl->page_writer_procs.writer_proc[thread_loc].need_flush = true;
        pg_write_barrier();
        if (thread_loc != 0 && g_instance.ckpt_cxt_ctl->page_writer_procs.writer_proc[thread_loc].proc != NULL) {
            SetLatch(&(g_instance.ckpt_cxt_ctl->page_writer_procs.writer_proc[thread_loc].proc->procLatch));
        }

        if (u_sess->attr.attr_storage.log_pagewriter) {
            int next_flush = g_instance.ckpt_cxt_ctl->page_writer_procs.writer_proc[thread_loc].end_loc -
                             g_instance.ckpt_cxt_ctl->page_writer_procs.writer_proc[thread_loc].start_loc + 1;
            ereport(LOG,
                (errmodule(MOD_INCRE_CKPT),
                    errmsg("needWritten is %u, thread num is %d, need flush page num is %d",
                        requested_flush_num,
                        thread_loc,
                        next_flush)));
        }
    }
}

/**
 * @Description: The main thread can move head only when other threads complete the page flush.
 * @in:          try_move_head, only first enter the loop, need move queue head.
 * @in:          offset_to_new_head, Upper limit of move queue head.
 * @in:          old_dirty_queue_head, Before entering the loop, the head of the dirty page queue.
 */
static void ckpt_move_queue_head_after_flush()
{
    uint32 actual_flushed = 0;
    uint32 i;
    uint32 thread_num = g_instance.ckpt_cxt_ctl->page_writer_procs.num;
    uint64 dirty_queue_head = pg_atomic_read_u64(&g_instance.ckpt_cxt_ctl->dirty_page_queue_head);
    int64 dirty_page_num = get_dirty_page_num();

    while (true) {
        /* wait all sub thread finish flush */
        if (pg_atomic_read_u32(&g_instance.ckpt_cxt_ctl->page_writer_procs.running_num) == 0) {
            g_instance.ckpt_cxt_ctl->page_writer_procs.thrd_dw_cxt.dw_page_idx = -1;
            for (i = 0; i < thread_num; i++) {
                actual_flushed += g_instance.ckpt_cxt_ctl->page_writer_procs.writer_proc[i].actual_flush_num;
            }
            /* Finish flush dirty page, move the dirty page queue head, and clear the slot state. */
            for (i = 0; i < dirty_page_num; i++) {
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
        (void)sched_yield();
    }
    /* We flushed some buffers, so update the statistics */
    if (actual_flushed > 0) {
        g_instance.ckpt_cxt_ctl->page_writer_actual_flush += actual_flushed;
        g_instance.ckpt_cxt_ctl->page_writer_last_flush += actual_flushed;
    }

    if (u_sess->attr.attr_storage.log_pagewriter) {
        ereport(LOG, (errmodule(MOD_INCRE_CKPT),
                errmsg("Page Writer flushed: %u pages, remaining dirty_page_num: %ld",
                    actual_flushed, get_dirty_page_num())));
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
    int thread_id = t_thrd.pagewriter_cxt.pagewriter_id;
    int32 expected_flush_num;
    bool contain_hashbucket = false;

    WritebackContextInit(&wb_context, &t_thrd.pagewriter_cxt.page_writer_after);
    ResourceOwnerEnlargeBuffers(t_thrd.utils_cxt.CurrentResourceOwner);

    expected_flush_num = ckpt_get_expected_flush_num();
    if (expected_flush_num == 0) {
        return;
    }

    g_instance.ckpt_cxt_ctl->page_writer_last_flush = 0;

    while (expected_flush_num > 0) {
        requested_flush_num = ckpt_qsort_dirty_page_for_flush(&contain_hashbucket);

        if (SECUREC_UNLIKELY(requested_flush_num == 0)) {
            break;
        }
        expected_flush_num -= requested_flush_num;

        g_instance.ckpt_cxt_ctl->page_writer_procs.thrd_dw_cxt.contain_hashbucket = contain_hashbucket;
        g_instance.ckpt_cxt_ctl->page_writer_procs.thrd_dw_cxt.dw_page_idx = -1;

        dw_perform_batch_flush(requested_flush_num, NULL, &g_instance.ckpt_cxt_ctl->page_writer_procs.thrd_dw_cxt);

        divide_dirty_page_to_thread(requested_flush_num);

        /* page_writer thread flush dirty page */
        Assert(thread_id == 0); /* main thread id is 0 */
        Assert(g_instance.ckpt_cxt_ctl->page_writer_procs.writer_proc[thread_id].need_flush);
        ckpt_flush_dirty_page(thread_id, wb_context);

        ckpt_move_queue_head_after_flush();

        /*
         * If request flush num less than the batch max, break this loop,
         * It indicates that there are not many dirty pages.
         */
        if (expected_flush_num < (int32)GET_DW_DIRTY_PAGE_MAX(contain_hashbucket) && !FULL_CKPT) {
            break;
        }
    }

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
        page_rec_lsn = pg_atomic_read_u64(&buf_desc->rec_lsn);
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
        page_rec_lsn = pg_atomic_read_u64(&buf_desc->rec_lsn);
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

const int SECOND_TO_MICROSECOND = 1000;
uint32 calculate_thread_max_flush_num(bool is_pagewriter)
{
    uint32 max_io = u_sess->attr.attr_storage.max_io_capacity / 8 / 2;
    float rate = (float)u_sess->attr.attr_storage.pageWriterSleep / (float)u_sess->attr.attr_storage.BgWriterDelay;

    uint32 pagewriter_flush = max_io / (1 + rate); /* flush num every second */
    uint32 bgwriter_flush = max_io - pagewriter_flush;

    if (is_pagewriter) {
        pagewriter_flush = pagewriter_flush * (float)(u_sess->attr.attr_storage.pageWriterSleep /
            (float)SECOND_TO_MICROSECOND);
        return pagewriter_flush;
    } else {
        bgwriter_flush = bgwriter_flush * (float)(u_sess->attr.attr_storage.BgWriterDelay /
            (float)SECOND_TO_MICROSECOND);
        return bgwriter_flush;
    }
}

const int AVG_CALCULATE_NUM = 30;
const float HIGH_WATER = 0.75;
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
    uint32 max_io = calculate_thread_max_flush_num(true);
    uint32 num_for_lsn_max;
    float dirty_percent;

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

    total_flush_num += g_instance.ckpt_cxt_ctl->page_writer_last_flush;
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

    dirty_page_pct = g_instance.ckpt_cxt_ctl->actual_dirty_page_num / (float)(g_instance.attr.attr_storage.NBuffers);
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

    min_lsn = ckpt_get_min_rec_lsn();
    if (XLogRecPtrIsInvalid(min_lsn)) {
        min_lsn = get_dirty_page_queue_rec_lsn();
    }

    target_lsn = min_lsn + avg_lsn_rate;
    num_for_lsn = get_page_num_for_lsn(target_lsn, num_for_lsn_max);
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

static void ckpt_pagewriter_main_thread_loop(void)
{
    uint32 rc = 0;
    uint64 now;
    int64 sleep_time;

    if (t_thrd.pagewriter_cxt.got_SIGHUP) {
        t_thrd.pagewriter_cxt.got_SIGHUP = false;
        ProcessConfigFile(PGC_SIGHUP);
    }

    /* main thread should finally exit. */
    while (t_thrd.pagewriter_cxt.shutdown_requested && g_instance.ckpt_cxt_ctl->page_writer_can_exit) {
        int i;
        if (pg_atomic_read_u32(&g_instance.ckpt_cxt_ctl->current_page_writer_count) == 1) {
            ereport(LOG,
                (errmodule(MOD_INCRE_CKPT),
                    errmsg("pagewriter thread shut down, id is %d", t_thrd.pagewriter_cxt.pagewriter_id)));

            /*
             * From here on, elog(ERROR) should end with exit(1), not send
             * control back to the sigsetjmp block above.
             */
            u_sess->attr.attr_common.ExitOnAnyError = true;
            /* Normal exit from the pagewriter is here */
            proc_exit(0); /* done */
        } else {
            for (i = 1; i < g_instance.ckpt_cxt_ctl->page_writer_procs.num; i++) {
                if (g_instance.ckpt_cxt_ctl->page_writer_procs.writer_proc[i].proc != NULL) {
                    SetLatch(&(g_instance.ckpt_cxt_ctl->page_writer_procs.writer_proc[i].proc->procLatch));
                }
            }
            pg_usleep(MILLISECOND_TO_MICROSECOND);
            continue;
        }
    }

    while (get_dirty_page_num() == 0 && !t_thrd.pagewriter_cxt.shutdown_requested) {
        rc = WaitLatch(&t_thrd.proc->procLatch, WL_TIMEOUT | WL_POSTMASTER_DEATH, (long)TEN_MILLISECOND);
        if (rc & WL_POSTMASTER_DEATH) {
            gs_thread_exit(1);
        }
    }

    if (pg_atomic_read_u32(&g_instance.ckpt_cxt_ctl->page_writer_procs.running_num) == 0 &&
        pg_atomic_read_u32(&g_instance.ckpt_cxt_ctl->current_page_writer_count) ==
            (uint32)g_instance.attr.attr_storage.pagewriter_thread_num) {
        ckpt_try_skip_invalid_elem_in_queue_head();
        ckpt_try_prune_dirty_page_queue();

        /* Full checkpoint, don't sleep */
        sleep_time = get_pagewriter_sleep_time();
        while (sleep_time > 0 && !t_thrd.pagewriter_cxt.shutdown_requested && !FULL_CKPT) {
            /* sleep 1ms check whether a full checkpoint is triggered */
            pg_usleep(MILLISECOND_TO_MICROSECOND);
            sleep_time -= 1;
        }

        /* Calculate next flush time before flush this batch dirty page */
        now = get_time_ms();
        t_thrd.pagewriter_cxt.next_flush_time = now + u_sess->attr.attr_storage.pageWriterSleep;

        /* pagewriter thread flush dirty page */
        ckpt_pagewriter_main_thread_flush_dirty_page();
        return;
    }
    return;
}

static void ckpt_pagewriter_sub_thread_loop()
{
    uint32 rc;
    int thread_id = t_thrd.pagewriter_cxt.pagewriter_id;
    WritebackContext wb_context;

    WritebackContextInit(&wb_context, &t_thrd.pagewriter_cxt.page_writer_after);

    if (t_thrd.pagewriter_cxt.got_SIGHUP) {
        t_thrd.pagewriter_cxt.got_SIGHUP = false;
        ProcessConfigFile(PGC_SIGHUP);
    }

    if (t_thrd.pagewriter_cxt.shutdown_requested && g_instance.ckpt_cxt_ctl->page_writer_can_exit) {
        ereport(LOG,
            (errmodule(MOD_INCRE_CKPT),
                errmsg("pagewriter thread shut down, id is %d", t_thrd.pagewriter_cxt.pagewriter_id)));

        /*
         * From here on, elog(ERROR) should end with exit(1), not send control back to
         * the sigsetjmp block above
         */
        u_sess->attr.attr_common.ExitOnAnyError = true;
        /* Normal exit from the pagewriter is here */
        proc_exit(0);
    }

    /* Wait first */
    rc = WaitLatch(
        &t_thrd.proc->procLatch, WL_TIMEOUT | WL_LATCH_SET | WL_POSTMASTER_DEATH, (long)SECOND_TO_MILLISECOND /* ms */);

    if (rc & WL_POSTMASTER_DEATH) {
        gs_thread_exit(1);
    }

    ResetLatch(&t_thrd.proc->procLatch);

    if (g_instance.ckpt_cxt_ctl->page_writer_procs.writer_proc[thread_id].need_flush) {
        ResourceOwnerEnlargeBuffers(t_thrd.utils_cxt.CurrentResourceOwner);
        ckpt_flush_dirty_page(thread_id, wb_context);
    }

    return;
}

static void ckpt_pagewriter_handle_exception(MemoryContext pagewriter_context)
{
    int id = t_thrd.pagewriter_cxt.pagewriter_id;

    /* Since not using PG_TRY, must reset error stack by hand */
    t_thrd.log_cxt.error_context_stack = NULL;

    /* Clear the running status of this ereported thread before we proceed to release resources */
    if (g_instance.ckpt_cxt_ctl->page_writer_procs.writer_proc[id].need_flush) {
        g_instance.ckpt_cxt_ctl->page_writer_procs.writer_proc[id].need_flush = false;
        pg_atomic_fetch_sub_u32(&g_instance.ckpt_cxt_ctl->page_writer_procs.running_num, 1);
    }
    if (pg_atomic_read_u32(&g_instance.ckpt_cxt_ctl->page_writer_procs.running_num) == 0) {
        g_instance.ckpt_cxt_ctl->page_writer_procs.thrd_dw_cxt.dw_page_idx = -1;
    }
    /* Prevent interrupts while cleaning up */
    HOLD_INTERRUPTS();

    /* Report the error to the server log */
    EmitErrorReport();

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

    /*
     * Close all open files after any error.  This is helpful on Windows,
     * where holding deleted files open causes various strange errors.
     * It's not clear we need it elsewhere, but shouldn't hurt.
     */
    smgrcloseall();
    return;
}

static void pagewriter_kill(int code, Datum arg)
{
    int id = t_thrd.pagewriter_cxt.pagewriter_id;
    Assert(id >= 0 && id < g_instance.attr.attr_storage.pagewriter_thread_num);

    /* Making sure that we mark our exit status (as sub threads) so that main pagewriter thread would not be waiting for us in vain */
    if (g_instance.ckpt_cxt_ctl->page_writer_procs.writer_proc[id].need_flush) {
        g_instance.ckpt_cxt_ctl->page_writer_procs.writer_proc[id].need_flush = false;
        pg_atomic_fetch_sub_u32(&g_instance.ckpt_cxt_ctl->page_writer_procs.running_num, 1);
    }
    pg_atomic_fetch_sub_u32(&g_instance.ckpt_cxt_ctl->current_page_writer_count, 1);
    g_instance.ckpt_cxt_ctl->page_writer_procs.writer_proc[id].proc = NULL;
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
    if (g_instance.ckpt_cxt_ctl->page_writer_procs.writer_proc[id].proc == NULL) {
        g_instance.ckpt_cxt_ctl->page_writer_procs.writer_proc[id].proc = t_thrd.proc;
        t_thrd.pagewriter_cxt.pagewriter_id = id;
    } else {
        for (i = 0; i < g_instance.ckpt_cxt_ctl->page_writer_procs.num; i++) {
            void *expected = NULL;
            if (pg_atomic_compare_exchange_uintptr(
                (uintptr_t *)&g_instance.ckpt_cxt_ctl->page_writer_procs.writer_proc[i].proc,
                (uintptr_t *)&expected, (uintptr_t)t_thrd.proc)) {
                t_thrd.pagewriter_cxt.pagewriter_id = i;
                break;
            }
        }
    }

    Assert(t_thrd.pagewriter_cxt.pagewriter_id >= 0 &&
           t_thrd.pagewriter_cxt.pagewriter_id < g_instance.attr.attr_storage.pagewriter_thread_num);
    return t_thrd.pagewriter_cxt.pagewriter_id;
}

static void SetupPageWriterSignalHook(void)
{
    /*
     * Reset some signals that are accepted by postmaster but not here
     */
    (void)gspqsignal(SIGHUP, ckpt_pagewriter_sighup_handler);
    (void)gspqsignal(SIGINT, SIG_IGN);
    (void)gspqsignal(SIGTERM, ckpt_pagewriter_request_shutdown_handler);
    (void)gspqsignal(SIGQUIT, ckpt_pagewriter_quick_die); /* hard crash time */
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, ckpt_pagewriter_sigusr1_handler);
    (void)gspqsignal(SIGUSR2, SIG_IGN);

    /*
     * Reset some signals that are accepted by postmaster but not here
     */
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGTTIN, SIG_DFL);
    (void)gspqsignal(SIGTTOU, SIG_DFL);
    (void)gspqsignal(SIGCONT, SIG_DFL);
    (void)gspqsignal(SIGWINCH, SIG_DFL);
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

    /*
     * Create a resource owner to keep track of our resources (currently only
     * buffer pins).
     */
    Assert(t_thrd.pagewriter_cxt.pagewriter_id >= 0);
    errno_t err_rc = snprintf_s(
        name, MAX_THREAD_NAME_LEN, MAX_THREAD_NAME_LEN - 1, "%s%d", "PageWriter", t_thrd.pagewriter_cxt.pagewriter_id);
    securec_check_ss(err_rc, "", "");

    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, name, MEMORY_CONTEXT_STORAGE);

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

    /*
     * If an exception is encountered, processing resumes here.
     *
     * See notes in postgres.c about the design of this coding.
     */
    if (sigsetjmp(localSigjmpBuf, 1) != 0) {
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

    pg_time_t now = (pg_time_t) time(NULL);
    t_thrd.pagewriter_cxt.next_flush_time = now + u_sess->attr.attr_storage.pageWriterSleep;

    /*
     * Loop forever
     */
    for (;;) {
        /*
         * main pagewriter thread need choose a batch page flush to double write file,
         * than divide to other sub thread.
         */
        if (t_thrd.pagewriter_cxt.pagewriter_id == 0) {
            ckpt_pagewriter_main_thread_loop();
        } else {
            ckpt_pagewriter_sub_thread_loop();
        }
    }
}

const float NEED_PRUNE_DIRTY_QUEUE_SLOT = 0.6;
#define MAX_INVALID_BUF_SLOT (MIN(g_instance.shmem_cxt.MaxConnections, g_instance.attr.attr_storage.NBuffers))
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
        ereport(LOG,
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

    if (dirty_page_num < g_instance.ckpt_cxt_ctl->dirty_page_queue_size * NEED_PRUNE_DIRTY_QUEUE_SLOT || FULL_CKPT) {
        return false;
    }

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
    uint64* valid_buffer_array = (uint64*)palloc0(MAX_VALID_BUF_SLOT * sizeof(uint64));

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
        uint32 buf_state;
        volatile DirtyPageQueueSlot* slot = NULL;
        volatile DirtyPageQueueSlot* move_slot = NULL;
        BufferDesc* bufhdr = NULL;

        /*
         * If full checkpoint set the full_ckpt_expected_flush_loc is queue slot 100, some
         * pages are moved to a new position after slot 100 due to this prune queue. than
         * the redo point will be wrong, because some page not flush to disk.
         */
        (void)LWLockAcquire(g_instance.ckpt_cxt_ctl->prune_queue_lock, LW_EXCLUSIVE);
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
            bufhdr->dirty_queue_loc = move_loc;
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

static void ckpt_pagewriter_sighup_handler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.pagewriter_cxt.got_SIGHUP = true;
    if (t_thrd.proc) {
        SetLatch(&t_thrd.proc->procLatch);
    }

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
