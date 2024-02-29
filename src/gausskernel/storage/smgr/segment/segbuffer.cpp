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
#include "pgstat.h"
#include "ddes/dms/ss_dms_bufmgr.h"

/* 
 * Segment buffer, used for segment meta data, e.g., segment head, space map head. We separate segment
 * meta data buffer and normal data buffer (in bufmgr.cpp) to avoid potential dead locks.
 */

// TODO: move to thread local context
thread_local static BufferDesc *InProgressBuf = NULL;
thread_local static bool isForInput = false;

static const int TEN_MICROSECOND = 10;

#define BufHdrLocked(bufHdr) ((bufHdr)->state & BM_LOCKED)
#define SegBufferIsPinned(bufHdr) BUF_STATE_GET_REFCOUNT((bufHdr)->state)

static BufferDesc *SegStrategyGetBuffer(uint64 *buf_state);
static bool SegStartBufferIO(BufferDesc *buf, bool forInput);

extern PrivateRefCountEntry *GetPrivateRefCountEntry(Buffer buffer, bool do_move);
extern void ForgetPrivateRefCountEntry(PrivateRefCountEntry *ref);

void SetInProgressFlags(BufferDesc *bufDesc, bool input)
{
    InProgressBuf = bufDesc;
    isForInput = input;
}

bool HasInProgressBuf(void)
{
    return InProgressBuf != NULL;
}

void AbortSegBufferIO(void)
{
    if (InProgressBuf != NULL) {
        LWLockAcquire(InProgressBuf->io_in_progress_lock, LW_EXCLUSIVE);
        SegTerminateBufferIO(InProgressBuf, false, BM_IO_ERROR);
    }
}

static bool SegStartBufferIO(BufferDesc *buf, bool forInput)
{
    uint64 buf_state;

    SegmentCheck(!InProgressBuf);

    while (true) {
        LWLockAcquire(buf->io_in_progress_lock, LW_EXCLUSIVE);

        if (buf->extra->aio_in_progress) {
            LWLockRelease(buf->io_in_progress_lock);
            pg_usleep(1000L);
            continue;
        }

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

void SegTerminateBufferIO(BufferDesc *buf, bool clear_dirty, uint64 set_flag_bits)
{
    SegmentCheck(buf == InProgressBuf);

    uint64 buf_state = LockBufHdr(buf);

    SegmentCheck(buf_state & BM_IO_IN_PROGRESS);

    buf_state &= ~(BM_IO_IN_PROGRESS | BM_IO_ERROR);
    if (clear_dirty) {
        if (ENABLE_INCRE_CKPT) {
            if (!XLogRecPtrIsInvalid(pg_atomic_read_u64(&buf->extra->rec_lsn))) {
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

bool SegPinBuffer(BufferDesc *buf)
{
    ereport(DEBUG5, (errmodule(MOD_SEGMENT_PAGE),
                     errmsg("[SegPinBuffer] (%u %u %u %d) %d %u ", buf->tag.rnode.spcNode, buf->tag.rnode.dbNode,
                            buf->tag.rnode.relNode, buf->tag.rnode.bucketNode, buf->tag.forkNum, buf->tag.blockNum)));
    
    ResourceOwnerEnlargeBuffers(t_thrd.utils_cxt.CurrentResourceOwner);

    Buffer b = BufferDescriptorGetBuffer(buf);
    bool result;
    PrivateRefCountEntry * ref = GetPrivateRefCountEntry(b, true);

    if (ref == NULL) {
        uint64 buf_state;
        uint64 old_buf_state = pg_atomic_read_u64(&buf->state);

        ReservePrivateRefCountEntry();
        ref = NewPrivateRefCountEntry(b);

        for (;;) {
            if (old_buf_state & BM_LOCKED) {
                old_buf_state = WaitBufHdrUnlocked(buf);
            }
            buf_state = old_buf_state;

            buf_state += BUF_REFCOUNT_ONE;
            if (pg_atomic_compare_exchange_u64(&buf->state, &old_buf_state, buf_state)) {
                result = old_buf_state & BM_VALID;
                break;
            }
        }
    } else {
        result = true;
    }

    ref->refcount++;
    ResourceOwnerRememberBuffer(t_thrd.utils_cxt.CurrentResourceOwner, b);

    return result;
}

static bool SegPinBufferLocked(BufferDesc *buf, const BufferTag *tag)
{
    ereport(DEBUG5, (errmodule(MOD_SEGMENT_PAGE),
                     errmsg("[SegPinBufferLocked] (%u %u %u %d) %d %u ", tag->rnode.spcNode, tag->rnode.dbNode,
                            tag->rnode.relNode, tag->rnode.bucketNode, tag->forkNum, tag->blockNum)));
    SegmentCheck(BufHdrLocked(buf));
    Buffer b;
    PrivateRefCountEntry *ref = NULL;

    /*
     * As explained, We don't expect any preexisting pins. That allows us to
     * manipulate the PrivateRefCount after releasing the spinlock
     */
    Assert(GetPrivateRefCountEntry(BufferDescriptorGetBuffer(buf), false) == NULL);

    uint64 buf_state = pg_atomic_read_u64(&buf->state);
    Assert(buf_state & BM_LOCKED);

    buf_state = __sync_add_and_fetch(&buf->state, 1);

    UnlockBufHdr(buf, buf_state);

    b = BufferDescriptorGetBuffer(buf);

    ref = NewPrivateRefCountEntry(b);
    ref->refcount++;

    ResourceOwnerEnlargeBuffers(t_thrd.utils_cxt.CurrentResourceOwner);
    ResourceOwnerRememberBuffer(t_thrd.utils_cxt.CurrentResourceOwner, b);

    return buf_state & BM_VALID;
}

void SegUnpinBuffer(BufferDesc *buf)
{
    ereport(DEBUG5, (errmodule(MOD_SEGMENT_PAGE),
                     errmsg("[SegUnpinBuffer] (%u %u %u %d) %d %u ", buf->tag.rnode.spcNode, buf->tag.rnode.dbNode,
                            buf->tag.rnode.relNode, buf->tag.rnode.bucketNode, buf->tag.forkNum, buf->tag.blockNum)));
    PrivateRefCountEntry * ref = GetPrivateRefCountEntry(BufferDescriptorGetBuffer(buf), true);
    SegmentCheck(ref != NULL);

    ResourceOwnerForgetBuffer(t_thrd.utils_cxt.CurrentResourceOwner, BufferDescriptorGetBuffer(buf));

    ref->refcount--;

    if (ref->refcount == 0) {
        uint64 buf_state;
        uint64 old_buf_state;

        old_buf_state = pg_atomic_read_u64(&buf->state);
        for (;;) {
            if (old_buf_state & BM_LOCKED) {
                old_buf_state = WaitBufHdrUnlocked(buf);
            }

            buf_state = old_buf_state;
            SegmentCheck(BUF_STATE_GET_REFCOUNT(buf_state) > 0);
            buf_state -= BUF_REFCOUNT_ONE;
            if (pg_atomic_compare_exchange_u64(&buf->state, &old_buf_state, buf_state)) {
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
    uint64 old_buf_state, buf_state;
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
            if (!XLogRecPtrIsInvalid(pg_atomic_read_u64(&bufHdr->extra->rec_lsn))) {
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

#ifdef USE_ASSERT_CHECKING
void SegFlushCheckDiskLSN(SegSpace *spc, RelFileNode rNode, ForkNumber forknum, BlockNumber blocknum, char *buf)
{
    if (!RecoveryInProgress() && !SS_IN_ONDEMAND_RECOVERY && ENABLE_DSS && ENABLE_VERIFY_PAGE_VERSION) {
        char *origin_buf = (char *)palloc(BLCKSZ + ALIGNOF_BUFFER);
        char *temp_buf = (char *)BUFFERALIGN(origin_buf);
        seg_physical_read(spc, rNode, forknum, blocknum, temp_buf);
        XLogRecPtr lsn_on_disk = PageGetLSN(temp_buf);
        XLogRecPtr lsn_on_mem = PageGetLSN(buf);
        /* maybe some pages are not protected by WAL-Logged */
        if ((lsn_on_mem != InvalidXLogRecPtr) && (lsn_on_disk > lsn_on_mem)) {
            ereport(PANIC, (errmsg("[%d/%d/%d/%d/%d %d-%d] memory lsn(0x%llx) is less than disk lsn(0x%llx)",
                rNode.spcNode, rNode.dbNode, rNode.relNode, rNode.bucketNode, rNode.opt,
                forknum, blocknum, (unsigned long long)lsn_on_mem, (unsigned long long)lsn_on_disk)));
        }
        pfree(origin_buf);
    }
}
#endif

void SegFlushBuffer(BufferDesc *buf, SMgrRelation reln)
{
    t_thrd.dms_cxt.buf_in_aio = false;

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

    uint64 buf_state = LockBufHdr(buf);
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

#ifdef USE_ASSERT_CHECKING
    SegFlushCheckDiskLSN(spc, buf->tag.rnode, buf->tag.forkNum, buf->tag.blockNum, buf_to_write);
#endif

    if (ENABLE_DMS && t_thrd.role == PAGEWRITER_THREAD && ENABLE_DSS_AIO) {
        int thread_id = t_thrd.pagewriter_cxt.pagewriter_id;
        PageWriterProc *pgwr = &g_instance.ckpt_cxt_ctl->pgwr_procs.writer_proc[thread_id];
        DSSAioCxt *aio_cxt = &pgwr->aio_cxt;
        int aiobuf_id = DSSAioGetIOCBIndex(aio_cxt);
        char *tempBuf = (char *)(pgwr->aio_buf + aiobuf_id * BLCKSZ);
        errno_t ret = memcpy_s(tempBuf, BLCKSZ, buf_to_write, BLCKSZ);
        securec_check(ret, "\0", "\0");

        struct iocb *iocb_ptr = DSSAioGetIOCB(aio_cxt);
        int32 io_ret = seg_physical_aio_prep_pwrite(spc, buf->tag.rnode, buf->tag.forkNum,
            buf->tag.blockNum, tempBuf, (void *)iocb_ptr);
        if (io_ret != DSS_SUCCESS) {
            ereport(PANIC, (errmsg("dss aio failed, buffer: %d/%d/%d/%d/%d %d-%u",
                buf->tag.rnode.spcNode, buf->tag.rnode.dbNode, buf->tag.rnode.relNode, (int)buf->tag.rnode.bucketNode,
                (int)buf->tag.rnode.opt, buf->tag.forkNum, buf->tag.blockNum)));
        }

        if (buf->extra->aio_in_progress) {
            ereport(PANIC, (errmsg("buffer is already in aio progress, buffer: %d/%d/%d/%d/%d %d-%u",
                buf->tag.rnode.spcNode, buf->tag.rnode.dbNode, buf->tag.rnode.relNode, (int)buf->tag.rnode.bucketNode,
                (int)buf->tag.rnode.opt, buf->tag.forkNum, buf->tag.blockNum)));
        }

        buf->extra->aio_in_progress = true;
        t_thrd.dms_cxt.buf_in_aio = true;
        /* should be after io_prep_pwrite, because io_prep_pwrite will memset iocb struct */
        iocb_ptr->data = (void *)buf;
        DSSAioAppendIOCB(aio_cxt, iocb_ptr);
    } else {
        seg_physical_write(spc, buf->tag.rnode, buf->tag.forkNum, buf->tag.blockNum, (char *)buf_to_write, false);
    }

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

void ReadSegBufferForCheck(BufferDesc* bufHdr, ReadBufferMode mode, SegSpace *spc, Block bufBlock)
{
    if (spc == NULL) {
        spc = spc_open(bufHdr->tag.rnode.spcNode, bufHdr->tag.rnode.dbNode, false, false);
        SegmentCheck(spc != NULL);
    }

    seg_physical_read(spc, bufHdr->tag.rnode, bufHdr->tag.forkNum, bufHdr->tag.blockNum, (char *)bufBlock);
    if (!PageIsVerified((char *)bufBlock, bufHdr->tag.blockNum)) {
        ereport(WARNING, (errmsg("[%d/%d/%d/%d %d-%d] verified failed",
            bufHdr->tag.rnode.spcNode, bufHdr->tag.rnode.dbNode, bufHdr->tag.rnode.relNode,
            bufHdr->tag.rnode.bucketNode, bufHdr->tag.forkNum, bufHdr->tag.blockNum)));
        return;
    }

    if (!PageIsSegmentVersion(bufBlock) && !PageIsNew(bufBlock)) {
        ereport(PANIC, (errmsg("[%d/%d/%d/%d %d-%d] page version is %d",
            bufHdr->tag.rnode.spcNode, bufHdr->tag.rnode.dbNode, bufHdr->tag.rnode.relNode,
            bufHdr->tag.rnode.bucketNode, bufHdr->tag.forkNum, bufHdr->tag.blockNum,
            PageGetPageLayoutVersion(bufBlock))));
    }
}

Buffer ReadSegBufferForDMS(BufferDesc* bufHdr, ReadBufferMode mode, SegSpace *spc)
{
    if (spc == NULL) {
        bool found;
        SegSpcTag tag = {.spcNode = bufHdr->tag.rnode.spcNode, .dbNode = bufHdr->tag.rnode.dbNode};
        SegmentCheck(t_thrd.storage_cxt.SegSpcCache != NULL);
        spc = (SegSpace *)hash_search(t_thrd.storage_cxt.SegSpcCache, (void *)&tag, HASH_FIND, &found);
        SegmentCheck(found);
        ereport(DEBUG1, (errmsg("Fetch cached SegSpace success, spcNode:%u dbNode:%u.", bufHdr->tag.rnode.spcNode,
            bufHdr->tag.rnode.dbNode)));
    }

    char *bufBlock = (char *)BufHdrGetBlock(bufHdr);
    if (mode == RBM_ZERO_AND_LOCK || mode == RBM_ZERO_AND_CLEANUP_LOCK || mode == RBM_ZERO) {
        errno_t er = memset_s((char *)bufBlock, BLCKSZ, 0, BLCKSZ);
        securec_check(er, "", "");
#ifdef USE_ASSERT_CHECKING
        if (ENABLE_DSS) {
            seg_physical_write(spc, bufHdr->tag.rnode, bufHdr->tag.forkNum, bufHdr->tag.blockNum, bufBlock, false);
        }
#endif
    } else {
#ifdef USE_ASSERT_CHECKING
        bool need_verify = (!RecoveryInProgress() && !SS_IN_ONDEMAND_RECOVERY &&
            ((pg_atomic_read_u64(&bufHdr->state) & BM_VALID) != 0) && ENABLE_DSS &&
            ENABLE_VERIFY_PAGE_VERSION);
        char *past_image = NULL;
        if (need_verify) {
            past_image = (char *)palloc(BLCKSZ);
            errno_t ret = memcpy_s(past_image, BLCKSZ, bufBlock, BLCKSZ);
            securec_check_ss(ret, "\0", "\0");
        }
#endif
    
        seg_physical_read(spc, bufHdr->tag.rnode, bufHdr->tag.forkNum, bufHdr->tag.blockNum, bufBlock);
        ereport(DEBUG1,
            (errmsg("DMS SegPage ReadBuffer success, bufid:%d, blockNum:%u of reln:%s mode %d.",
                bufHdr->buf_id, bufHdr->tag.blockNum, relpathperm(bufHdr->tag.rnode, bufHdr->tag.forkNum), (int)mode)));
        if (!PageIsVerified(bufBlock, bufHdr->tag.blockNum)) {
            RepairBlockKey key;
            key.relfilenode = bufHdr->tag.rnode;
            key.forknum = bufHdr->tag.forkNum;
            key.blocknum = bufHdr->tag.blockNum;
            ReportInvalidPage(key);
#ifdef USE_ASSERT_CHECKING
            pfree_ext(past_image);
#endif
            return InvalidBuffer;
        }

#ifdef USE_ASSERT_CHECKING
        if (need_verify) {
            XLogRecPtr lsn_past = PageGetLSN(past_image);
            XLogRecPtr lsn_now = PageGetLSN(bufBlock);
            if (lsn_now < lsn_past) {
                RelFileNode rnode = bufHdr->tag.rnode;
                ereport(PANIC, (errmsg("[%d/%d/%d/%d/%d %d-%d] now lsn(0x%llx) is less than past lsn(0x%llx)",
                    rnode.spcNode, rnode.dbNode, rnode.relNode, rnode.bucketNode, rnode.opt,
                    bufHdr->tag.forkNum, bufHdr->tag.blockNum,
                    (unsigned long long)lsn_now, (unsigned long long)lsn_past)));
            }
        }
        pfree_ext(past_image);
#endif

        if (!PageIsSegmentVersion(bufBlock) && !PageIsNew(bufBlock)) {
            ereport(PANIC, (errmsg("Read DMS SegPage buffer, block %u of relation %s, but page version is %d",
                bufHdr->tag.blockNum, relpathperm(bufHdr->tag.rnode, bufHdr->tag.forkNum),
                PageGetPageLayoutVersion(bufBlock))));
        }
    }

    bufHdr->extra->lsn_on_disk = PageGetLSN(bufBlock);
#ifdef USE_ASSERT_CHECKING
    bufHdr->lsn_dirty = InvalidXLogRecPtr;
#endif
    SegTerminateBufferIO(bufHdr, false, BM_VALID);
    SegmentCheck(SegBufferIsPinned(bufHdr));
    return BufferDescriptorGetBuffer(bufHdr);
}

Buffer ReadBufferFast(SegSpace *spc, RelFileNode rnode, ForkNumber forkNum, BlockNumber blockNum, ReadBufferMode mode)
{
    bool found = false;
    dms_buf_ctrl_t *buf_ctrl;

    /* Make sure we will have room to remember the buffer pin */
    ResourceOwnerEnlargeBuffers(t_thrd.utils_cxt.CurrentResourceOwner);

    BufferDesc *bufHdr = SegBufferAlloc(spc, rnode, forkNum, blockNum, &found);

    if (ENABLE_DMS) {
        buf_ctrl = GetDmsBufCtrl(bufHdr->buf_id);
        if (mode == RBM_FOR_ONDEMAND_REALTIME_BUILD) {
            buf_ctrl->state |= BUF_READ_MODE_ONDEMAND_REALTIME_BUILD;
        }
    }

    if (!found) {
        SegmentCheck(!(pg_atomic_read_u64(&bufHdr->state) & BM_VALID));

        char *bufBlock = (char *)BufHdrGetBlock(bufHdr);

        if (ENABLE_DMS && mode != RBM_FOR_REMOTE) {
            Assert(!(pg_atomic_read_u64(&bufHdr->state) & BM_VALID));

            do {
                bool startio;
                if (LWLockHeldByMe(bufHdr->io_in_progress_lock)) {
                    startio = true;
                } else {
                    startio = SegStartBufferIO(bufHdr, true);
                }

                if (!startio) {
                    Assert(pg_atomic_read_u64(&bufHdr->state) & BM_VALID);
                    found = true;
                    goto found_branch;
                }

                LWLockMode lockmode = LW_SHARED;
                if (!LockModeCompatible(buf_ctrl, lockmode)) {
                    if (!StartReadPage(bufHdr, lockmode)) {
                        SegTerminateBufferIO((BufferDesc *)bufHdr, false, 0);
                        // when reform fail, should return InvalidBuffer to reform proc thread
                        if (SSNeedTerminateRequestPageInReform(buf_ctrl)) {
                            SSUnPinBuffer(bufHdr);
                            return InvalidBuffer;
                        }
                        pg_usleep(5000L);
                        continue;
                    }
                } else {
                    /*
                    * previous attempts to read the buffer must have failed,
                    * but DRC has been created, so load page directly again
                    */
                    Assert(pg_atomic_read_u64(&bufHdr->state) & BM_IO_ERROR);
                    buf_ctrl->state |= BUF_NEED_LOAD;
                }

                break;
            } while (true);
            Buffer tmp_buffer = TerminateReadSegPage(bufHdr, mode, spc);
            if (BufferIsInvalid(tmp_buffer) && (mode == RBM_FOR_ONDEMAND_REALTIME_BUILD) &&
                !(buf_ctrl->state & BUF_READ_MODE_ONDEMAND_REALTIME_BUILD)) {
                SSUnPinBuffer(bufHdr);
                return InvalidBuffer;
            }
            return tmp_buffer;
        }

        if (mode == RBM_ZERO_AND_LOCK || mode == RBM_ZERO_AND_CLEANUP_LOCK || mode == RBM_ZERO) {
            errno_t er = memset_s((char *)bufBlock, BLCKSZ, 0, BLCKSZ);
            securec_check(er, "", "");
#ifdef USE_ASSERT_CHECKING
            if (ENABLE_DSS) {
                seg_physical_write(spc, rnode, forkNum, blockNum, bufBlock, false);
            }
#endif
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
        bufHdr->extra->lsn_on_disk = PageGetLSN(bufBlock);
#ifdef USE_ASSERT_CHECKING
        bufHdr->lsn_dirty = InvalidXLogRecPtr;
#endif
        SegTerminateBufferIO(bufHdr, false, BM_VALID);
    }

found_branch:
    if (mode == RBM_ZERO_AND_LOCK || mode == RBM_ZERO_AND_CLEANUP_LOCK) {
        if (ENABLE_DMS) {
            GetDmsBufCtrl(bufHdr->buf_id)->state |= BUF_READ_MODE_ZERO_LOCK;
            LockBuffer(BufferDescriptorGetBuffer(bufHdr), BUFFER_LOCK_EXCLUSIVE);
        } else {
            LWLockAcquire(BufferDescriptorGetContentLock(bufHdr), LW_EXCLUSIVE);
        }
    }

    SegmentCheck(SegBufferIsPinned(bufHdr));
    return BufferDescriptorGetBuffer(bufHdr);
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
    uint64 buf_state;
    uint64 old_flags;
    uint32 new_hash, old_hash;
    LWLock *new_partition_lock;
    LWLock *old_partition_lock;
    bool old_flag_valid;

    INIT_BUFFERTAG(new_tag, rnode, forkNum, blockNum);

    new_hash = BufTableHashCode(&new_tag);

retry:
    int buf_id = BufTableLookup(&new_tag, new_hash);

    if (buf_id >= 0) {
        buf = GetBufferDescriptor(buf_id);
        bool valid = SegPinBuffer(buf);
        if (!BUFFERTAGS_PTR_EQUAL(&buf->tag, &new_tag)) {
            SegUnpinBuffer(buf);
            goto retry;
        }

        *foundPtr = true;

        if (!valid) {
            if (SegStartBufferIO(buf, true)) {
                *foundPtr = false;
            }
        }

        return buf;
    }

    *foundPtr = FALSE;
    new_partition_lock = BufMappingPartitionLock(new_hash);
    for (;;) {
        ReservePrivateRefCountEntry();

        buf = SegStrategyGetBuffer(&buf_state);
        SegmentCheck(BUF_STATE_GET_REFCOUNT(buf_state) == 0);

        old_flags = buf_state & BUF_FLAG_MASK;

        SegPinBufferLocked(buf, &new_tag);

        if (!SSPageCheckIfCanEliminate(buf, old_flags) || !SSOndemandRealtimeBuildAllowFlush(buf)) {
            SegUnpinBuffer(buf);
            continue;
        }

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

        old_flags = buf_state & BUF_FLAG_MASK;
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

        buf_state = LockBufHdr(buf);
        buf_id = BufTableInsert(&new_tag, new_hash, buf->buf_id);

        if (buf_id >= 0) {
            UnlockBufHdr(buf, buf_state);
            SegUnpinBuffer(buf);
            if (old_flag_valid && old_partition_lock != new_partition_lock)
                LWLockRelease(old_partition_lock);

            return FoundBufferInHashTable(buf_id, new_partition_lock, foundPtr);
        }

        old_flags = buf_state & BUF_FLAG_MASK;

        if (BUF_STATE_GET_REFCOUNT(buf_state) == 1 && !(old_flags & BM_DIRTY)) {
            if (ENABLE_DMS && (old_flags & BM_TAG_VALID)) {
                /*
                * notify DMS to release drc owner. if failed, can't recycle this buffer.
                * release owner procedure is in buf header lock, it's not reasonable,
                * need to improve.
                */
                if (DmsReleaseOwner(old_tag, buf->buf_id)) {
                    ClearReadHint(buf->buf_id, true);
                    break;
                }
            } else {
                break;
            }
        }
        BufTableDelete(&new_tag, new_hash);
        if (old_flag_valid && old_partition_lock != new_partition_lock) {
            LWLockRelease(old_partition_lock);
        }
        UnlockBufHdr(buf, buf_state);
        LWLockRelease(new_partition_lock);
        SegUnpinBuffer(buf);
    }

    buf->tag = new_tag;
    buf_state &= ~(BM_VALID | BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED | BM_IO_ERROR | BM_PERMANENT |
                   BUF_USAGECOUNT_MASK);
    buf_state |= BM_TAG_VALID | BM_PERMANENT | BUF_USAGECOUNT_ONE;

    if (ENABLE_DMS) {
        GetDmsBufCtrl(buf->buf_id)->lock_mode = DMS_LOCK_NULL;
        GetDmsBufCtrl(buf->buf_id)->been_loaded = false;
    }

    if (old_flag_valid) {
        BufTableDelete(&old_tag, old_hash);
        if (old_partition_lock != new_partition_lock) {
            LWLockRelease(old_partition_lock);
        }
    }
    LWLockRelease(new_partition_lock);
    UnlockBufHdr(buf, buf_state);
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

static BufferDesc* get_segbuf_from_candidate_list(uint64* buf_state)
{
    BufferDesc* buf = NULL;
    uint64 local_buf_state;
    int buf_id = 0;

    if (ENABLE_INCRE_CKPT && pg_atomic_read_u32(&g_instance.ckpt_cxt_ctl->current_page_writer_count) > 0) {
        int list_num = g_instance.ckpt_cxt_ctl->pgwr_procs.sub_num;
        volatile PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;
        int list_id = beentry->st_tid > 0 ? (beentry->st_tid % list_num) : (beentry->st_sessionid % list_num);

        for (int i = 0; i < list_num; i++) {
            /* the pagewriter sub thread store normal buffer pool, sub thread starts from 1 */
            int thread_id = (list_id + i) % list_num + 1;
            Assert(thread_id > 0 && thread_id <= list_num);
            while (candidate_buf_pop(&g_instance.ckpt_cxt_ctl->pgwr_procs.writer_proc[thread_id].seg_list, &buf_id)) {
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
static BufferDesc *SegStrategyGetBuffer(uint64 *buf_state)
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

        uint64 state = LockBufHdr(buf);

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
                 (errmsg("SegStrategyGetBuffer get a pinned buffer, %d, buffer tag <%u, %u, %u, %u>.%d.%u, state %lu",
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
        uint64 buf_state;
        /*
         * As in DropRelFileNodeBuffers, an unlocked precheck should be safe
         * and saves some cycles.
         */
        if (buf_desc->tag.rnode.spcNode != spcNode || buf_desc->tag.rnode.dbNode != dbNode) {
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
