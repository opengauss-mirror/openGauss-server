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
 * segbuffer.cpp
 *
 * IDENTIFICATION
 *     src/gausskernel/storage/smgr/segment/segbuffer.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/double_write.h"
#include "access/xlogproc.h"
#include "storage/buf/buf_internals.h"
#include "storage/buf/bufmgr.h"
#include "storage/proc.h"
#include "storage/smgr/segment.h"
#include "storage/smgr/smgr.h"
#include "utils/resowner.h"
#include "tsan_annotation.h"
#include "pgstat.h"

/* 
 * Segment buffer, used for segment meta data, e.g., segment head, space map head. We separate segment
 * meta data buffer and normal data buffer (in bufmgr.cpp) to avoid potential dead locks.
 */

// TODO: move to thread local context
thread_local static BufferDesc *InProgressBuf = NULL;
thread_local static bool isForInput = false;

static const int TEN_MICROSECOND = 10;

#define BufHdrLocked(bufHdr) ((bufHdr)->state & BM_LOCKED)
#define SegBufferIsPinned(bufHdr) ((bufHdr)->state & BUF_REFCOUNT_MASK)

static BufferDesc *SegStrategyGetBuffer(uint32 *buf_state);
static bool SegStartBufferIO(BufferDesc *buf, bool forInput);
static void SegTerminateBufferIO(BufferDesc *buf, bool clear_dirty, uint32 set_flag_bits);

extern PrivateRefCountEntry *GetPrivateRefCountEntry(Buffer buffer, bool create, bool do_move);
extern void ForgetPrivateRefCountEntry(PrivateRefCountEntry *ref);

void AbortSegBufferIO(void)
{
    if (InProgressBuf != NULL) {
        LWLockAcquire(InProgressBuf->io_in_progress_lock, LW_EXCLUSIVE);
        SegTerminateBufferIO(InProgressBuf, false, BM_IO_ERROR);
    }
}

static void WaitIO(BufferDesc *buf)
{
    while (true) {
        uint32 buf_state;

        buf_state = LockBufHdr(buf);
        UnlockBufHdr(buf, buf_state);

        if (!(buf_state & BM_IO_IN_PROGRESS)) {
            break;
        }

        LWLockAcquire(buf->io_in_progress_lock, LW_SHARED);
        LWLockRelease(buf->io_in_progress_lock);
    }
}

static bool SegStartBufferIO(BufferDesc *buf, bool forInput)
{
    uint32 buf_state;

    SegmentCheck(!InProgressBuf);

    while (true) {
        LWLockAcquire(buf->io_in_progress_lock, LW_EXCLUSIVE);

        buf_state = LockBufHdr(buf);

        if (!(buf_state & BM_IO_IN_PROGRESS)) {
            break;
        }

        UnlockBufHdr(buf, buf_state);
        LWLockRelease(buf->io_in_progress_lock);
        WaitIO(buf);
    }

    if (forInput ? (buf_state & BM_VALID) : !(buf_state & BM_DIRTY)) {
        /* IO finished */
        UnlockBufHdr(buf, buf_state);
        LWLockRelease(buf->io_in_progress_lock);

        return false;
    }

    buf_state |= BM_IO_IN_PROGRESS;
    UnlockBufHdr(buf, buf_state);

    InProgressBuf = buf;
    isForInput = forInput;

    return true;
}

static void SegTerminateBufferIO(BufferDesc *buf, bool clear_dirty, uint32 set_flag_bits)
{
    SegmentCheck(buf == InProgressBuf);

    uint32 buf_state = LockBufHdr(buf);

    SegmentCheck(buf_state & BM_IO_IN_PROGRESS);

    buf_state &= ~(BM_IO_IN_PROGRESS | BM_IO_ERROR);
    if (clear_dirty) {
        if (ENABLE_INCRE_CKPT) {
            if (!XLogRecPtrIsInvalid(pg_atomic_read_u64(&buf->rec_lsn))) {
                remove_dirty_page_from_queue(buf);
            } else {
                ereport(PANIC, (errmodule(MOD_INCRE_CKPT), errcode(ERRCODE_INVALID_BUFFER),
                                (errmsg("buffer is dirty but not in dirty page queue in TerminateBufferIO_common"))));
            }

            if ((buf_state & BM_JUST_DIRTIED)) {
                buf_state &= ~BM_CHECKPOINT_NEEDED;
                if (!push_pending_flush_queue(BufferDescriptorGetBuffer(buf))) {
                    ereport(PANIC, (errmodule(MOD_INCRE_CKPT), errcode(ERRCODE_INVALID_BUFFER),
                                    (errmsg("TerminateBufferIO_common, dirty page queue is full when trying to "
                                            "push buffer to the queue"))));
                }
            }
        }

        if (!(buf_state & BM_JUST_DIRTIED)) {
            buf_state &= ~(BM_DIRTY | BM_CHECKPOINT_NEEDED);
        }
    }

    buf_state |= set_flag_bits;
    UnlockBufHdr(buf, buf_state);

    InProgressBuf = NULL;
    LWLockRelease(buf->io_in_progress_lock);
}

static uint32 SegWaitBufHdrUnlocked(BufferDesc *buf)
{
#ifndef ENABLE_THREAD_CHECK
    SpinDelayStatus delayStatus = init_spin_delay(buf);
#endif
    uint32 buf_state;

    buf_state = pg_atomic_read_u32(&buf->state);

    while (buf_state & BM_LOCKED) {
#ifndef ENABLE_THREAD_CHECK
        perform_spin_delay(&delayStatus);
#endif
        buf_state = pg_atomic_read_u32(&buf->state);
    }

#ifndef ENABLE_THREAD_CHECK
    finish_spin_delay(&delayStatus);
#endif

    /* ENABLE_THREAD_CHECK only, acqurie semantic */
    TsAnnotateHappensAfter(&buf->state);

    return buf_state;
}

bool SegPinBuffer(BufferDesc *buf)
{
    ereport(DEBUG5, (errmodule(MOD_SEGMENT_PAGE),
                     errmsg("[SegPinBuffer] (%u %u %u %d) %d %u ", buf->tag.rnode.spcNode, buf->tag.rnode.dbNode,
                            buf->tag.rnode.relNode, buf->tag.rnode.bucketNode, buf->tag.forkNum, buf->tag.blockNum)));
    bool result;
    PrivateRefCountEntry * ref = GetPrivateRefCountEntry(BufferDescriptorGetBuffer(buf), true, true);
    SegmentCheck(ref != NULL);

    if (ref->refcount == 0) {
        uint32 buf_state;
        uint32 old_buf_state = pg_atomic_read_u32(&buf->state);

        for (;;) {
            if (old_buf_state & BM_LOCKED) {
                old_buf_state = SegWaitBufHdrUnlocked(buf);
            }
            buf_state = old_buf_state;

            buf_state += BUF_REFCOUNT_ONE;
            if (pg_atomic_compare_exchange_u32(&buf->state, &old_buf_state, buf_state)) {
                result = old_buf_state & BM_VALID;
                break;
            }
        }
    } else {
        result = true;
    }

    ref->refcount++;

    ResourceOwnerEnlargeBuffers(t_thrd.utils_cxt.CurrentResourceOwner);
    ResourceOwnerRememberBuffer(t_thrd.utils_cxt.CurrentResourceOwner, BufferDescriptorGetBuffer(buf));

    return result;
}

static bool SegPinBufferLocked(BufferDesc *buf, const BufferTag *tag)
{
    ereport(DEBUG5, (errmodule(MOD_SEGMENT_PAGE),
                     errmsg("[SegPinBufferLocked] (%u %u %u %d) %d %u ", tag->rnode.spcNode, tag->rnode.dbNode,
                            tag->rnode.relNode, tag->rnode.bucketNode, tag->forkNum, tag->blockNum)));
    SegmentCheck(BufHdrLocked(buf));
    PrivateRefCountEntry * ref = GetPrivateRefCountEntry(BufferDescriptorGetBuffer(buf), true, true);
    SegmentCheck(ref != NULL);

    uint32 buf_state = pg_atomic_read_u32(&buf->state);

    if (ref->refcount == 0) {
        buf_state += BUF_REFCOUNT_ONE;
    }
    UnlockBufHdr(buf, buf_state);

    ref->refcount++;

    ResourceOwnerEnlargeBuffers(t_thrd.utils_cxt.CurrentResourceOwner);
    ResourceOwnerRememberBuffer(t_thrd.utils_cxt.CurrentResourceOwner, BufferDescriptorGetBuffer(buf));

    return buf_state & BM_VALID;
}

void SegUnpinBuffer(BufferDesc *buf)
{
    ereport(DEBUG5, (errmodule(MOD_SEGMENT_PAGE),
                     errmsg("[SegUnpinBuffer] (%u %u %u %d) %d %u ", buf->tag.rnode.spcNode, buf->tag.rnode.dbNode,
                            buf->tag.rnode.relNode, buf->tag.rnode.bucketNode, buf->tag.forkNum, buf->tag.blockNum)));
    PrivateRefCountEntry * ref = GetPrivateRefCountEntry(BufferDescriptorGetBuffer(buf), true, true);
    SegmentCheck(ref != NULL);

    ResourceOwnerForgetBuffer(t_thrd.utils_cxt.CurrentResourceOwner, BufferDescriptorGetBuffer(buf));

    ref->refcount--;

    if (ref->refcount == 0) {
        uint32 buf_state;
        uint32 old_buf_state;

        old_buf_state = pg_atomic_read_u32(&buf->state);
        for (;;) {
            if (old_buf_state & BM_LOCKED) {
                old_buf_state = SegWaitBufHdrUnlocked(buf);
            }

            buf_state = old_buf_state;
            SegmentCheck(BUF_STATE_GET_REFCOUNT(buf_state) > 0);
            buf_state -= BUF_REFCOUNT_ONE;
            if (pg_atomic_compare_exchange_u32(&buf->state, &old_buf_state, buf_state)) {
                break;
            }
        }
        
        SegmentCheck(!(buf_state & BM_PIN_COUNT_WAITER));

        ref->refcount = 0;
        ForgetPrivateRefCountEntry(ref);
    }
}

void SegReleaseBuffer(Buffer buffer)
{
    SegmentCheck(IsSegmentBufferID(buffer - 1));
    SegUnpinBuffer(GetBufferDescriptor(buffer - 1));
}

void SegUnlockReleaseBuffer(Buffer buffer) 
{
    LockBuffer(buffer, BUFFER_LOCK_UNLOCK); 
    SegUnpinBuffer(GetBufferDescriptor(buffer - 1));
}

void SegMarkBufferDirty(Buffer buf)
{
    uint32 old_buf_state, buf_state;
    BufferDesc *bufHdr;

    bufHdr = GetBufferDescriptor(buf - 1);

    SegmentCheck(IsSegmentBufferID(bufHdr->buf_id));
    /* unfortunately we can't check if the lock is held exclusively */
    SegmentCheck(LWLockHeldByMe(bufHdr->content_lock));

    old_buf_state = LockBufHdr(bufHdr);

    buf_state = old_buf_state | (BM_DIRTY | BM_JUST_DIRTIED);

    /*
     * When the page is marked dirty for the first time, needs to push the dirty page queue.
     * Check the BufferDesc rec_lsn to determine whether the dirty page is in the dirty page queue.
     * If the rec_lsn is valid, dirty page is already in the queue, don't need to push it again.
     */
    if (ENABLE_INCRE_CKPT) {
        for (;;) {
            buf_state = old_buf_state | (BM_DIRTY | BM_JUST_DIRTIED);
            if (!XLogRecPtrIsInvalid(pg_atomic_read_u64(&bufHdr->rec_lsn))) {
                break;
            }

            if (!is_dirty_page_queue_full(bufHdr) && push_pending_flush_queue(buf)) {
                break;
            }

            UnlockBufHdr(bufHdr, old_buf_state);
            pg_usleep(TEN_MICROSECOND);
            old_buf_state = LockBufHdr(bufHdr);
        }
    }

    UnlockBufHdr(bufHdr, buf_state);
}

void SegFlushBuffer(BufferDesc *buf, SMgrRelation reln)
{
    if (!SegStartBufferIO(buf, false)) {
        /* Someone else flushed the buffer */
        return;
    }

    ErrorContextCallback errcontext;
    /*
     * Set up error traceback if we are not pagewriter.
     * If we are a page writer, let thread's own callback handles the error.
     */
    if (t_thrd.role != PAGEWRITER_THREAD && t_thrd.role != BGWRITER) {
        errcontext.callback = shared_buffer_write_error_callback;
        errcontext.arg = (void *)buf;
        errcontext.previous = t_thrd.log_cxt.error_context_stack;
        t_thrd.log_cxt.error_context_stack = &errcontext;
    }

    char *buf_to_write = NULL;
    RedoBufferInfo buffer_info;
    
    SegSpace *spc;
    if (reln == NULL || reln->seg_space == NULL) {
        spc = spc_open(buf->tag.rnode.spcNode, buf->tag.rnode.dbNode, false);
    } else {
        spc = reln->seg_space;
    }
    SegmentCheck(spc != NULL);

    uint32 buf_state = LockBufHdr(buf);
    buf_state &= ~BM_JUST_DIRTIED;

    UnlockBufHdr(buf, buf_state);

    GetFlushBufferInfo(buf, &buffer_info, &buf_state, WITH_NORMAL_CACHE);

    /* Must be segment-page metadata page */
    SegmentCheck(PageIsSegmentVersion(buffer_info.pageinfo.page) || PageIsNew(buffer_info.pageinfo.page));

    if (FORCE_FINISH_ENABLED) {
        update_max_page_flush_lsn(buffer_info.lsn, t_thrd.proc_cxt.MyProcPid, false);
    }
    XLogWaitFlush(buffer_info.lsn);

    /* page data encrypt */
    buf_to_write = PageDataEncryptForBuffer(buffer_info.pageinfo.page, buf, true);

    if (unlikely(buf_to_write != (char *)buffer_info.pageinfo.page)) {
        PageSetChecksumInplace((Page)buf_to_write, buffer_info.blockinfo.blkno);
    } else {
        buf_to_write = PageSetChecksumCopy((Page)buf_to_write, buffer_info.blockinfo.blkno, true);
    }

    seg_physical_write(spc, buf->tag.rnode, buf->tag.forkNum, buf->tag.blockNum, (char *)buf_to_write, false);

    SegTerminateBufferIO(buf, true, 0);

    /* Pop the error context stack, if it was set before */
    if (t_thrd.role != PAGEWRITER_THREAD && t_thrd.role != BGWRITER) {
        t_thrd.log_cxt.error_context_stack = errcontext.previous;
    }
}

void ReportInvalidPage(RepairBlockKey key)
{
    /* record bad page, wait the pagerepair thread repair the page */
    if (CheckVerionSupportRepair() && (AmStartupProcess() || AmPageRedoWorker()) &&
        IsPrimaryClusterStandbyDN() && g_instance.repair_cxt.support_repair) {
        XLogPhyBlock pblk_bak = {0};
        RedoPageRepairCallBack(key, pblk_bak);
        log_invalid_page(key.relfilenode, key.forknum, key.blocknum, CRC_CHECK_ERROR, NULL);
        ereport(WARNING, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("invalid page in block %u of relation %s",
                key.blocknum, relpathperm(key.relfilenode, key.forknum))));
        return;
    }
    ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("invalid page in block %u of relation %s",
                                                         key.blocknum, relpathperm(key.relfilenode, key.forknum))));
    return;
}

Buffer ReadBufferFast(SegSpace *spc, RelFileNode rnode, ForkNumber forkNum, BlockNumber blockNum, ReadBufferMode mode)
{
    bool found = false;

    BufferDesc *bufHdr = SegBufferAlloc(spc, rnode, forkNum, blockNum, &found);

    if (!found) {
        SegmentCheck(!(pg_atomic_read_u32(&bufHdr->state) & BM_VALID));

        char *bufBlock = (char *)BufHdrGetBlock(bufHdr);
        if (mode == RBM_ZERO_AND_LOCK || mode == RBM_ZERO_AND_CLEANUP_LOCK) {
            errno_t er = memset_s((char *)bufBlock, BLCKSZ, 0, BLCKSZ);
            securec_check(er, "", "");
        } else {
            seg_physical_read(spc, rnode, forkNum, blockNum, bufBlock);
            if (!PageIsVerified(bufBlock, blockNum)) {
                RepairBlockKey key;
                key.relfilenode = rnode;
                key.forknum = forkNum;
                key.blocknum = blockNum;
                ReportInvalidPage(key);
                return InvalidBuffer;
            }
            if (!PageIsSegmentVersion(bufBlock) && !PageIsNew(bufBlock)) {
                ereport(PANIC,
                        (errmsg("Read segment-page metadata buffer, block %u of relation %s, but page version is %d",
                                blockNum, relpathperm(rnode, forkNum), PageGetPageLayoutVersion(bufBlock))));
            }
        }
        bufHdr->lsn_on_disk = PageGetLSN(bufBlock);
#ifdef USE_ASSERT_CHECKING
        bufHdr->lsn_dirty = InvalidXLogRecPtr;
#endif
        SegTerminateBufferIO(bufHdr, false, BM_VALID);
    }

    if (mode == RBM_ZERO_AND_LOCK || mode == RBM_ZERO_AND_CLEANUP_LOCK) {
        LWLockAcquire(BufferDescriptorGetContentLock(bufHdr), LW_EXCLUSIVE);
    }

    SegmentCheck(SegBufferIsPinned(bufHdr));
    return BufferDescriptorGetBuffer(bufHdr);
}

void LockTwoLWLock(LWLock *new_partition_lock, LWLock *old_partition_lock)
{
    if (old_partition_lock < new_partition_lock) {
        (void)LWLockAcquire(old_partition_lock, LW_EXCLUSIVE);
        (void)LWLockAcquire(new_partition_lock, LW_EXCLUSIVE);
    } else if (old_partition_lock > new_partition_lock) {
        (void)LWLockAcquire(new_partition_lock, LW_EXCLUSIVE);
        (void)LWLockAcquire(old_partition_lock, LW_EXCLUSIVE);
    } else {
        /* only one partition, only one lock */
        (void)LWLockAcquire(new_partition_lock, LW_EXCLUSIVE);
    }
}

BufferDesc * FoundBufferInHashTable(int buf_id, LWLock *new_partition_lock, bool *foundPtr)
{
    BufferDesc *buf = GetBufferDescriptor(buf_id);
    bool valid = SegPinBuffer(buf);
    LWLockRelease(new_partition_lock);
    *foundPtr = true;

    if (!valid) {
        if (SegStartBufferIO(buf, true)) {
            *foundPtr = false;
        }
    }

    return buf;
}

BufferDesc *SegBufferAlloc(SegSpace *spc, RelFileNode rnode, ForkNumber forkNum, BlockNumber blockNum,
                                  bool *foundPtr)
{
    BufferDesc *buf;
    BufferTag new_tag, old_tag;
    uint32 buf_state;
    uint32 old_flags;
    uint32 new_hash, old_hash;
    LWLock *new_partition_lock;
    LWLock *old_partition_lock;
    bool old_flag_valid;

    INIT_BUFFERTAG(new_tag, rnode, forkNum, blockNum);

    new_hash = BufTableHashCode(&new_tag);
    new_partition_lock = BufMappingPartitionLock(new_hash);

    LWLockAcquire(new_partition_lock, LW_SHARED);
    int buf_id = BufTableLookup(&new_tag, new_hash);

    if (buf_id >= 0) {
        return FoundBufferInHashTable(buf_id, new_partition_lock, foundPtr);
    }

    *foundPtr = FALSE;
    LWLockRelease(new_partition_lock);

    for (;;) {
        buf = SegStrategyGetBuffer(&buf_state);
        SegmentCheck(BUF_STATE_GET_REFCOUNT(buf_state) == 0);

        old_flags = buf_state & BUF_FLAG_MASK;

        SegPinBufferLocked(buf, &new_tag);

        if (old_flags & BM_DIRTY) {
            /* backend should not flush dirty pages if working version less than DW_SUPPORT_NEW_SINGLE_FLUSH */
            if (!backend_can_flush_dirty_page()) {
                SegUnpinBuffer(buf);
                (void)sched_yield();
                continue;
            }
            if (LWLockConditionalAcquire(buf->content_lock, LW_SHARED)) {
                FlushOneSegmentBuffer(buf->buf_id + 1);
                LWLockRelease(buf->content_lock);
                ScheduleBufferTagForWriteback(t_thrd.storage_cxt.BackendWritebackContext, &buf->tag);
            } else {
                SegUnpinBuffer(buf);
                continue;
            }
        }

        old_flag_valid = old_flags & BM_TAG_VALID;

        if (old_flag_valid) {
            old_tag = buf->tag;
            old_hash = BufTableHashCode(&old_tag);
            old_partition_lock = BufMappingPartitionLock(old_hash);

            LockTwoLWLock(new_partition_lock, old_partition_lock);
        } else {
            /* if it wasn't valid, we need only the new partition */
            (void)LWLockAcquire(new_partition_lock, LW_EXCLUSIVE);
            /* these just keep the compiler quiet about uninit variables */
            old_hash = 0;
            old_partition_lock = NULL;
        }

        buf_id = BufTableInsert(&new_tag, new_hash, buf->buf_id);

        if (buf_id >= 0) {
            SegUnpinBuffer(buf);
            if (old_flag_valid && old_partition_lock != new_partition_lock)
                LWLockRelease(old_partition_lock);

            return FoundBufferInHashTable(buf_id, new_partition_lock, foundPtr);
        }

        buf_state = LockBufHdr(buf);
        old_flags = buf_state & BUF_FLAG_MASK;

        if (BUF_STATE_GET_REFCOUNT(buf_state) == 1 && !(old_flags & BM_DIRTY)) {
            break;
        }
        UnlockBufHdr(buf, buf_state);
        BufTableDelete(&new_tag, new_hash);
        if (old_flag_valid && old_partition_lock != new_partition_lock) {
            LWLockRelease(old_partition_lock);
        }
        LWLockRelease(new_partition_lock);
        SegUnpinBuffer(buf);
    }

    buf->tag = new_tag;
    buf_state &= ~(BM_VALID | BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED | BM_IO_ERROR | BM_PERMANENT |
                   BUF_USAGECOUNT_MASK);
    buf_state |= BM_TAG_VALID | BM_PERMANENT | BUF_USAGECOUNT_ONE;
    UnlockBufHdr(buf, buf_state);

    if (old_flag_valid) {
        BufTableDelete(&old_tag, old_hash);
        if (old_partition_lock != new_partition_lock) {
            LWLockRelease(old_partition_lock);
        }
    }
    LWLockRelease(new_partition_lock);
    *foundPtr = !SegStartBufferIO(buf, true);

    return buf;
}

static uint32 next_victim_buffer = 0;

static inline uint32 ClockSweepTick(void)
{
    uint32 victim = pg_atomic_fetch_add_u32(&next_victim_buffer, 1);
    if (victim >= (uint32)SEGMENT_BUFFER_NUM) {
        victim = victim % SEGMENT_BUFFER_NUM;
    }
    return victim;
}

static BufferDesc* get_segbuf_from_candidate_list(uint32* buf_state)
{
    BufferDesc* buf = NULL;
    uint32 local_buf_state;
    int buf_id = 0;

    if (ENABLE_INCRE_CKPT && pg_atomic_read_u32(&g_instance.ckpt_cxt_ctl->current_page_writer_count) > 0) {
        int list_num = g_instance.ckpt_cxt_ctl->pgwr_procs.sub_num;
        volatile PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;
        int list_id = beentry->st_tid > 0 ? (beentry->st_tid % list_num) : (beentry->st_sessionid % list_num);

        for (int i = 0; i < list_num; i++) {
            /* the pagewriter sub thread store normal buffer pool, sub thread starts from 1 */
            int thread_id = (list_id + i) % list_num + 1;
            Assert(thread_id > 0 && thread_id <= list_num);
            while (seg_candidate_buf_pop(&buf_id, thread_id)) {
                buf = GetBufferDescriptor(buf_id);
                local_buf_state = LockBufHdr(buf);
                SegmentCheck(buf_id >= SegmentBufferStartID);
                if (g_instance.ckpt_cxt_ctl->candidate_free_map[buf_id]) {
                    g_instance.ckpt_cxt_ctl->candidate_free_map[buf_id] = false;
                    bool available_buffer = BUF_STATE_GET_REFCOUNT(local_buf_state) == 0
                        && !(local_buf_state & BM_IS_META)
                        && !(local_buf_state & BM_DIRTY);
                    if (available_buffer) {
                        *buf_state = local_buf_state;
                        return buf;
                    }
                }
                UnlockBufHdr(buf, local_buf_state);
            }
        }
        wakeup_pagewriter_thread();
    }

    return NULL;
}

/* lock the buffer descriptor before return */
const int RETRY_COUNT = 3;
static BufferDesc *SegStrategyGetBuffer(uint32 *buf_state) 
{
    // todo: add free list
    BufferDesc *buf = get_segbuf_from_candidate_list(buf_state);
    int try_counter = SEGMENT_BUFFER_NUM * RETRY_COUNT;

    if (buf != NULL) {
        (void)pg_atomic_fetch_add_u64(&g_instance.ckpt_cxt_ctl->seg_get_buf_num_candidate_list, 1);
        return buf;
    }

    for (;;) {
        int buf_id = BufferIdOfSegmentBuffer(ClockSweepTick());
        buf = GetBufferDescriptor(buf_id);

        uint32 state = LockBufHdr(buf);

        if (BUF_STATE_GET_REFCOUNT(state) == 0) {
            *buf_state = state;
            (void)pg_atomic_fetch_add_u64(&g_instance.ckpt_cxt_ctl->seg_get_buf_num_clock_sweep, 1);
            return buf;
        } else if (--try_counter == 0) {
            UnlockBufHdr(buf, state);
            ereport(ERROR, (errcode(ERRCODE_INVALID_BUFFER), (errmsg("no unpinned buffers available"))));
        }
        UnlockBufHdr(buf, state);
        ereport(DEBUG5,
                (errmodule(MOD_SEGMENT_PAGE),
                 (errmsg("SegStrategyGetBuffer get a pinned buffer, %d, buffer tag <%u, %u, %u, %u>.%d.%u, state %u",
                         buf_id, buf->tag.rnode.spcNode, buf->tag.rnode.dbNode, buf->tag.rnode.relNode,
                         buf->tag.rnode.bucketNode, buf->tag.forkNum, buf->tag.blockNum, state))));
    }
}

void SegDropSpaceMetaBuffers(Oid spcNode, Oid dbNode)
{
    int i;

    // Release segment head buffer
    smgrcloseall();
    for (i = SegmentBufferStartID; i < TOTAL_BUFFER_NUM; i++) {
        BufferDesc *buf_desc = GetBufferDescriptor(i);
        uint32 buf_state;
        /*
         * As in DropRelFileNodeBuffers, an unlocked precheck should be safe
         * and saves some cycles.
         */
        if (buf_desc->tag.rnode.spcNode != spcNode) {
            continue;
        }

        buf_state = LockBufHdr(buf_desc);
        if (buf_desc->tag.rnode.spcNode == spcNode && buf_desc->tag.rnode.dbNode == dbNode && 
            (buf_state & BM_DIRTY) && (buf_state & BM_VALID)) {
            InvalidateBuffer(buf_desc); /* releases spinlock */
        } else {
            UnlockBufHdr(buf_desc, buf_state);
        }
    }
}

void FlushOneSegmentBuffer(Buffer buffer)
{
    BufferDesc *buf_desc = GetBufferDescriptor(buffer - 1);
    if (!PageIsSegmentVersion(BufferGetBlock(buffer)) && !PageIsNew(BufferGetBlock(buffer))) {
        ereport(PANIC, (errmsg("Flush segment-page metadata buffer, block %u of relation %s, but page version is %d",
                               buf_desc->tag.blockNum, relpathperm(buf_desc->tag.rnode, buf_desc->tag.forkNum),
                               PageGetPageLayoutVersion(BufferGetBlock(buffer)))));
    }
    /* during initdb, not need flush dw file */
    if (dw_enabled() && pg_atomic_read_u32(&g_instance.ckpt_cxt_ctl->current_page_writer_count) > 0) {
        uint32 pos = 0;
        bool flush_old_file = false;
        pos = seg_dw_single_flush(buf_desc, &flush_old_file);
        t_thrd.proc->dw_pos = pos;
        t_thrd.proc->flush_new_dw = !flush_old_file;
        SegFlushBuffer(buf_desc, NULL);
        if (flush_old_file) {
            g_instance.dw_single_cxt.recovery_buf.single_flush_state[pos] = true;
        } else {
            g_instance.dw_single_cxt.single_flush_state[pos] = true;
        }
        t_thrd.proc->dw_pos = -1;
    } else {
        SegFlushBuffer(buf_desc, NULL);
    }
}

/* Flush a data buffer. If double write is on, it will invoke single-page-dw first. Caller should lock the buffer. */
void FlushOneBufferIncludeDW(BufferDesc *buf_desc)
{
    if (dw_enabled()) {
        bool flush_old_file = false;
        uint32 pos = seg_dw_single_flush(buf_desc, &flush_old_file);
        t_thrd.proc->dw_pos = pos;
        t_thrd.proc->flush_new_dw = !flush_old_file;
        FlushBuffer(buf_desc, NULL);
        if (flush_old_file) {
            g_instance.dw_single_cxt.recovery_buf.single_flush_state[pos] = true;
        } else {
            g_instance.dw_single_cxt.single_flush_state[pos] = true;
        }
        t_thrd.proc->dw_pos = -1;
    } else {
        FlushBuffer(buf_desc, NULL);
    }
}
