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
 * nvmbuffer.cpp
 *
 * IDENTIFICATION
 *     src/gausskernel/storage/nvm/nvmbuffer.cpp
 *
 * ------------------------------------------------------------------------
 */

#include "postgres.h"
#include "utils/dynahash.h"
#include "access/double_write.h"
#include "knl/knl_variable.h"
#include "storage/buf/buf_internals.h"
#include "storage/buf/bufmgr.h"
#include "storage/smgr/smgr.h"
#include "storage/smgr/segment_internal.h"
#include "utils/resowner.h"
#include "pgstat.h"

static BufferDesc *NvmStrategyGetBuffer(uint64 *buf_state);
extern PrivateRefCountEntry *GetPrivateRefCountEntry(Buffer buffer, bool do_move);

static const int MILLISECOND_TO_MICROSECOND = 1000;
static const int TEN_MILLISECOND = 10;

static inline bool BypassNvm()
{
    return ((double)gs_random() / (double)MAX_RANDOM_VALUE) > g_instance.attr.attr_storage.nvm_attr.bypassNvm;
}

static inline bool BypassDram()
{
    return ((double)gs_random() / (double)MAX_RANDOM_VALUE) > g_instance.attr.attr_storage.nvm_attr.bypassDram;
}

static BufferLookupEnt* NvmBufTableLookup(BufferTag *tag, uint32 hashcode)
{
    return (BufferLookupEnt *)buf_hash_operate<HASH_FIND>(t_thrd.storage_cxt.SharedBufHash, tag, hashcode, NULL);
}

static bool NvmPinBuffer(BufferDesc *buf, bool *migrate)
{
    int b = BufferDescriptorGetBuffer(buf);
    bool result = false;
    uint64 buf_state;
    uint64 old_buf_state;

    *migrate = false;

    old_buf_state = pg_atomic_read_u64(&buf->state);
    for (;;) {
        if (unlikely(old_buf_state & BM_IN_MIGRATE)) {
            *migrate = true;
            return result;
        } else {
            if (old_buf_state & BM_LOCKED) {
                old_buf_state = WaitBufHdrUnlocked(buf);
            }

            buf_state = old_buf_state;

            /* increase refcount */
            buf_state += BUF_REFCOUNT_ONE;

            /* increase usagecount unless already max */
            if (BUF_STATE_GET_USAGECOUNT(buf_state) != BM_MAX_USAGE_COUNT) {
                buf_state += BUF_USAGECOUNT_ONE;
            }

            if (pg_atomic_compare_exchange_u64(&buf->state, &old_buf_state, buf_state)) {
                result = (buf_state & BM_VALID) != 0;
                break;
            }
        }
    }

    PrivateRefCountEntry *ref = GetPrivateRefCountEntry(b, true);
    if (ref == NULL) {
        ReservePrivateRefCountEntry();
        ref = NewPrivateRefCountEntry(b);
    }
    ref->refcount++;
    Assert(ref->refcount > 0);
    ResourceOwnerRememberBuffer(t_thrd.utils_cxt.CurrentResourceOwner, b);
    return result;
}

static bool NvmPinBufferFast(BufferDesc *buf)
{
    int b = BufferDescriptorGetBuffer(buf);
    PrivateRefCountEntry *ref = NULL;

    /* When the secondly and thirdly parameter all both true, the ret value must not be NULL. */
    ref = GetPrivateRefCountEntry(b, false);

    if (ref == NULL) {
        return false;
    }

    ref->refcount++;
    Assert(ref->refcount > 0);
    ResourceOwnerRememberBuffer(t_thrd.utils_cxt.CurrentResourceOwner, b);
    return true;
}

/*
 * We must set timeout here to avoid deadlock. If
 * backend thread A has pinned nvm buf1 and want to migrate nvm buf2,
 * backend thread B has pinned nvm buf2 and want to migrate nvm buf1,
 * it will result in deadlock.
 */
static bool WaitUntilUnPin(BufferDesc *buf)
{
    uint64 old_buf_state;
    int waits = 0;
    for (;;) {
        old_buf_state = pg_atomic_read_u64(&buf->state);
        if (BUF_STATE_GET_REFCOUNT(old_buf_state) == 1) {
            return true;
        } else {
            pg_usleep(MILLISECOND_TO_MICROSECOND);
            waits++;
            if (waits >= TEN_MILLISECOND) {
                return false;
            }
        }
    }
}

/*
 * Wait until the BM_IN_MIGRATE flag isn't set anymore
 */
static void WaitBufHdrUnMigrate(BufferDesc *buf)
{
#ifndef ENABLE_THREAD_CHECK
    SpinDelayStatus delay_status = init_spin_delay(buf);
#endif
    uint64 buf_state;

    buf_state = pg_atomic_read_u64(&buf->state);

    while (buf_state & BM_IN_MIGRATE) {
#ifndef ENABLE_THREAD_CHECK
        perform_spin_delay(&delay_status);
#endif
        buf_state = pg_atomic_read_u64(&buf->state);
    }

#ifndef ENABLE_THREAD_CHECK
    finish_spin_delay(&delay_status);
#endif
}

static bool SetBufferMigrateFlag(Buffer buffer)
{
    BufferDesc *buf = GetBufferDescriptor(buffer - 1);
    uint64 bufState;
    uint64 oldBufState;
    for (;;) {
        oldBufState = pg_atomic_read_u64(&buf->state);
        if (oldBufState & BM_LOCKED) {
            oldBufState = WaitBufHdrUnlocked(buf);
        }

        if (oldBufState & BM_IN_MIGRATE) {
            return false;
        }

        bufState = oldBufState;
        bufState |= BM_IN_MIGRATE;
        ereport(DEBUG1, (errmsg("mark buffer %d migrate buffer stat %lu.", buffer, bufState)));
        if (pg_atomic_compare_exchange_u64(&buf->state, &oldBufState, bufState)) {
            return true;
        }
    }
}

static void UnSetBufferMigrateFlag(Buffer buffer)
{
    BufferDesc *buf = GetBufferDescriptor(buffer - 1);
    uint64 bufState;
    uint64 oldBufState;
    for (;;) {
        oldBufState = pg_atomic_read_u64(&buf->state);
        if (oldBufState & BM_LOCKED) {
            oldBufState = WaitBufHdrUnlocked(buf);
        }
        bufState = oldBufState;
        bufState &= ~(BM_IN_MIGRATE);
        ereport(DEBUG1, (errmsg("unmark buffer %d migrate buffer stat %lu.", buffer, bufState)));
        if (pg_atomic_compare_exchange_u64(&buf->state, &oldBufState, bufState)) {
            break;
        }
    }
}

static void NvmWaitBufferIO(BufferDesc *buf)
{
    uint64 buf_state;

    Assert(!t_thrd.storage_cxt.InProgressBuf);

    /* To check the InProgressBuf must be NULL. */
    if (t_thrd.storage_cxt.InProgressBuf) {
        ereport(PANIC, (errmsg("InProgressBuf not null: id %d flags %u, buf: id %d flags %u",
            t_thrd.storage_cxt.InProgressBuf->buf_id,
            pg_atomic_read_u64(&t_thrd.storage_cxt.InProgressBuf->state) & BUF_FLAG_MASK,
            buf->buf_id, pg_atomic_read_u64(&buf->state) & BUF_FLAG_MASK)));
    }

    bool ioDone = false;
restart:
    for (;;) {
        (void)LWLockAcquire(buf->io_in_progress_lock, LW_EXCLUSIVE);

        buf_state = LockBufHdr(buf);
        if (buf_state & BM_IO_IN_PROGRESS) {
            ioDone = true;
            UnlockBufHdr(buf, buf_state);
            LWLockRelease(buf->io_in_progress_lock);
            WaitIO(buf);
        } else {
            break;
        }
    }
    if (buf_state & BM_VALID) {
        /* someone else already did the I/O */
        UnlockBufHdr(buf, buf_state);
        LWLockRelease(buf->io_in_progress_lock);
        return;
    } else {
        if (!ioDone) {
            UnlockBufHdr(buf, buf_state);
            LWLockRelease(buf->io_in_progress_lock);
            goto restart;
        } else {
            ereport(PANIC, (errmsg("ioDone is true but buf_state is not valid ")));
        }
    }
    return;
}

BufferDesc *NvmBufferAlloc(const RelFileNode& rel_file_node, char relpersistence, ForkNumber fork_num,
    BlockNumber block_num, BufferAccessStrategy strategy, bool *found, const XLogPhyBlock *pblk)
{
    Assert(!IsSegmentPhysicalRelNode(rel_file_node));

    BufferTag new_tag;                 /* identity of requested block */
    uint32 new_hash;                   /* hash value for newTag */
    LWLock *new_partition_lock = NULL; /* buffer partition lock for it */
    BufferTag old_tag;                 /* previous identity of selected buffer */
    uint32 old_hash;                   /* hash value for oldTag */
    LWLock *old_partition_lock = NULL; /* buffer partition lock for it */
    uint64 old_flags;
    int buf_id;
    BufferDesc *buf = NULL;
    BufferDesc *nvmBuf = NULL;
    bool valid = false;
    uint64 buf_state, nvm_buf_state;
    bool migrate = false;
    errno_t rc;

    /* create a tag so we can lookup the buffer */
    INIT_BUFFERTAG(new_tag, rel_file_node, fork_num, block_num);

    /* determine its hash code and partition lock ID */
    new_hash = BufTableHashCode(&new_tag);
    new_partition_lock = BufMappingPartitionLock(new_hash);

restart:
    *found = FALSE;
    /* see if the block is in the buffer pool already */
    (void)LWLockAcquire(new_partition_lock, LW_SHARED);
    pgstat_report_waitevent(WAIT_EVENT_BUF_HASH_SEARCH);
    BufferLookupEnt *entry = NvmBufTableLookup(&new_tag, new_hash);
    pgstat_report_waitevent(WAIT_EVENT_END);

    if (entry != NULL) {
        *found = TRUE;
        /*
         * Found it.  Now, pin the buffer so no one can steal it from the
         * buffer pool, and check to see if the correct data has been loaded
         * into the buffer.
         */

        buf_id = pg_atomic_read_u32((volatile uint32*)&entry->id);
        if (IsNormalBufferID(buf_id)) {
            buf = GetBufferDescriptor(buf_id);

            valid = PinBuffer(buf, strategy);

            /* Can release the mapping lock as soon as we've pinned it */
            LWLockRelease(new_partition_lock);

            if (!valid) {
                /*
                 * We can only get here if (a) someone else is still reading in
                 * the page, or (b) a previous read attempt failed.  We have to
                 * wait for any active read attempt to finish, and then set up our
                 * own read attempt if the page is still not BM_VALID.
                 * StartBufferIO does it all.
                 */
                if (StartBufferIO(buf, true)) {
                    /*
                     * If we get here, previous attempts to read the buffer must
                     * have failed ... but we shall bravely try again.
                     */
                    *found = FALSE;
                }
            }

            return buf;
        }

        if (IsNvmBufferID(buf_id)) {
            nvmBuf = GetBufferDescriptor(buf_id);
            /* Return buffer immediately if I have pinned the buffer before */
            if (NvmPinBufferFast(nvmBuf)) {
                LWLockRelease(new_partition_lock);
                return nvmBuf;
            }

            /* Haven't pinned the buffer ever */
            if (BypassDram()) {
                /* want to return nvm buffer directly */
                valid = NvmPinBuffer(nvmBuf, &migrate);

                if (migrate) {
                    LWLockRelease(new_partition_lock);
                    WaitBufHdrUnMigrate(nvmBuf);
                    goto restart;
                } else {
                    buf_id = pg_atomic_read_u32((volatile uint32 *)&entry->id);
                    if (IsNormalBufferID(buf_id)) {
                        UnpinBuffer(nvmBuf, true);
                        buf = GetBufferDescriptor(buf_id);
                        valid = PinBuffer(buf, strategy);
                        LWLockRelease(new_partition_lock);
                        Assert(valid);
                        return buf;
                    }
                    /* Can release the mapping lock as soon as we've pinned it */
                    LWLockRelease(new_partition_lock);
                    Assert(nvmBuf->buf_id == buf_id);
                    if (!valid) {
                        if (StartBufferIO(nvmBuf, true)) {
                            *found = FALSE;
                        }
                    }

                    return nvmBuf;
                }
            } else {
                /* wanna migrate */
                if (SetBufferMigrateFlag(buf_id + 1)) {
                    buf_id = pg_atomic_read_u32((volatile uint32 *)&entry->id);

                    if (IsNormalBufferID(buf_id)) {
                        UnSetBufferMigrateFlag(nvmBuf->buf_id + 1);
                        buf = GetBufferDescriptor(buf_id);
                        valid = PinBuffer(buf, strategy);
                        LWLockRelease(new_partition_lock);
                        Assert(valid);
                        return buf;
                    }
                    Assert(nvmBuf->buf_id == buf_id);

                    valid = PinBuffer(nvmBuf, strategy);
                    if (!valid) {
                        /* corner case: the migration thread can not do I/O */
                        NvmWaitBufferIO(nvmBuf);
                    }

                    LWLockRelease(new_partition_lock);

                    if (!WaitUntilUnPin(nvmBuf)) {
                        UnSetBufferMigrateFlag(buf_id + 1);
                        return nvmBuf;
                    }

                    nvm_buf_state = LockBufHdr(nvmBuf);
                    if (nvm_buf_state & BM_DIRTY) {
                        UnlockBufHdr(nvmBuf, nvm_buf_state);
                        UnSetBufferMigrateFlag(buf_id + 1);
                        return nvmBuf;
                    } else {
                        for (;;) {
                            ReservePrivateRefCountEntry();
                            buf = (BufferDesc *)StrategyGetBuffer(strategy, &buf_state);

                            old_flags = buf_state & BUF_FLAG_MASK;

                            if ((old_flags & BM_DIRTY) || (old_flags & BM_IS_META)) {
                                UnlockBufHdr(buf, buf_state);
                                (void)sched_yield();
                                continue;
                            }

                            if (old_flags & BM_TAG_VALID) {
                                PinBuffer_Locked(buf);

                                old_tag = ((BufferDesc *)buf)->tag;
                                old_hash = BufTableHashCode(&old_tag);
                                old_partition_lock = BufMappingPartitionLock(old_hash);

                                (void)LWLockAcquire(old_partition_lock, LW_EXCLUSIVE);

                                buf_state = LockBufHdr(buf);
                                old_flags = buf_state & BUF_FLAG_MASK;
                                if (BUF_STATE_GET_REFCOUNT(buf_state) == 1 && !(old_flags & BM_DIRTY) &&
                                    !(old_flags & BM_IS_META)) {
                                    /* todo DMS */
                                    buf_state &= ~(BM_VALID | BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED |
                                        BM_IO_ERROR | BM_PERMANENT | BUF_USAGECOUNT_MASK);
                                    if (relpersistence == RELPERSISTENCE_PERMANENT || fork_num == INIT_FORKNUM ||
                                        ((relpersistence == RELPERSISTENCE_TEMP) && STMT_RETRY_ENABLED)) {
                                        buf_state |= BM_VALID | BM_TAG_VALID | BM_PERMANENT | BUF_USAGECOUNT_ONE;
                                    } else {
                                        buf_state |= BM_VALID | BM_TAG_VALID | BUF_USAGECOUNT_ONE;
                                    }

                                    UnlockBufHdr(buf, buf_state);
                                    BufTableDelete(&old_tag, old_hash);

                                    /* immediately after deleteing old_hash to avoid ABA Problem */
                                    LWLockRelease(old_partition_lock);

                                    rc = memcpy_s(BufferGetBlock(buf->buf_id + 1), BLCKSZ,
                                        BufferGetBlock(nvmBuf->buf_id + 1), BLCKSZ);
                                    securec_check(rc, "\0", "\0");
                                    buf->tag = nvmBuf->tag;
                                    buf->extra->seg_fileno = nvmBuf->extra->seg_fileno;
                                    buf->extra->seg_blockno = nvmBuf->extra->seg_blockno;
                                    buf->extra->lsn_on_disk = nvmBuf->extra->lsn_on_disk;

                                    /* Assert nvmBuf is not dirty \ cas without buffer header lock */
                                    nvm_buf_state &= ~(BM_TAG_VALID);
                                    UnlockBufHdr(nvmBuf, nvm_buf_state);

                                    UnpinBuffer(nvmBuf, true);

                                    pg_atomic_write_u32((volatile uint32 *)&entry->id, buf->buf_id);
                                    UnSetBufferMigrateFlag(nvmBuf->buf_id + 1);
                                    return buf;
                                }

                                UnlockBufHdr(buf, buf_state);
                                LWLockRelease(old_partition_lock);
                                UnpinBuffer(buf, true);
                            } else {
                                /* this buf is not in hashtable */
                                buf->state &= ~(BM_VALID | BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED |
                                    BM_IO_ERROR | BM_PERMANENT | BUF_USAGECOUNT_MASK);
                                if (relpersistence == RELPERSISTENCE_PERMANENT || fork_num == INIT_FORKNUM ||
                                    ((relpersistence == RELPERSISTENCE_TEMP) && STMT_RETRY_ENABLED)) {
                                    buf->state |= BM_VALID | BM_TAG_VALID | BM_PERMANENT | BUF_USAGECOUNT_ONE;
                                } else {
                                    buf->state |= BM_VALID | BM_TAG_VALID | BUF_USAGECOUNT_ONE;
                                }

                                PinBuffer_Locked(buf);

                                rc = memcpy_s(BufferGetBlock(buf->buf_id + 1), BLCKSZ,
                                    BufferGetBlock(nvmBuf->buf_id + 1), BLCKSZ);
                                securec_check(rc, "\0", "\0");
                                buf->tag = nvmBuf->tag;
                                buf->extra->seg_fileno = nvmBuf->extra->seg_fileno;
                                buf->extra->seg_blockno = nvmBuf->extra->seg_blockno;
                                buf->extra->lsn_on_disk = nvmBuf->extra->lsn_on_disk;

                                // Assert nvmBuf is not dirty
                                nvm_buf_state &= ~(BM_TAG_VALID);

                                UnlockBufHdr(nvmBuf, nvm_buf_state);

                                UnpinBuffer(nvmBuf, true);

                                pg_atomic_write_u32((volatile uint32 *)&entry->id, buf->buf_id);
                                UnSetBufferMigrateFlag(nvmBuf->buf_id + 1);
                                return buf;
                            }
                        }
                    }
                } else {
                    LWLockRelease(new_partition_lock);
                    goto restart;
                }
            }
        }
    }

    /*
     * Didn't find it in the buffer pool.  We'll have to initialize a new
     * buffer.	Remember to unlock the mapping lock while doing the work.
     */
    LWLockRelease(new_partition_lock);
    /* Loop here in case we have to try another victim buffer */
    for (;;) {
        bool needGetLock = false;
        /*
         * Ensure, while the spinlock's not yet held, that there's a free refcount
         * entry.
         */
        ReservePrivateRefCountEntry();
        /*
         * Select a victim buffer.	The buffer is returned with its header
         * spinlock still held!
         */
        pgstat_report_waitevent(WAIT_EVENT_BUF_STRATEGY_GET);
        if (BypassNvm()) {
            buf = (BufferDesc *)StrategyGetBuffer(strategy, &buf_state);
        } else {
            buf = (BufferDesc *)NvmStrategyGetBuffer(&buf_state);
        }
        pgstat_report_waitevent(WAIT_EVENT_END);

        Assert(BUF_STATE_GET_REFCOUNT(buf_state) == 0);

        /* Must copy buffer flags while we still hold the spinlock */
        old_flags = buf_state & BUF_FLAG_MASK;

        /* Pin the buffer and then release the buffer spinlock */
        PinBuffer_Locked(buf);

        PageCheckIfCanEliminate(buf, &old_flags, &needGetLock);
        /*
         * If the buffer was dirty, try to write it out.  There is a race
         * condition here, in that someone might dirty it after we released it
         * above, or even while we are writing it out (since our share-lock
         * won't prevent hint-bit updates).  We will recheck the dirty bit
         * after re-locking the buffer header.
         */
        if (old_flags & BM_DIRTY) {
            /* backend should not flush dirty pages if working version less than DW_SUPPORT_NEW_SINGLE_FLUSH */
            if (!backend_can_flush_dirty_page()) {
                UnpinBuffer(buf, true);
                (void)sched_yield();
                continue;
            }

            /*
             * We need a share-lock on the buffer contents to write it out
             * (else we might write invalid data, eg because someone else is
             * compacting the page contents while we write).  We must use a
             * conditional lock acquisition here to avoid deadlock.  Even
             * though the buffer was not pinned (and therefore surely not
             * locked) when StrategyGetBuffer returned it, someone else could
             * have pinned and exclusive-locked it by the time we get here. If
             * we try to get the lock unconditionally, we'd block waiting for
             * them; if they later block waiting for us, deadlock ensues.
             * (This has been observed to happen when two backends are both
             * trying to split btree index pages, and the second one just
             * happens to be trying to split the page the first one got from
             * StrategyGetBuffer.)
             */
            bool needDoFlush = false;
            if (!needGetLock) {
                needDoFlush = LWLockConditionalAcquire(buf->content_lock, LW_SHARED);
            } else {
                LWLockAcquire(buf->content_lock, LW_SHARED);
                needDoFlush = true;
            }
            if (needDoFlush) {
                /*
                 * If using a nondefault strategy, and writing the buffer
                 * would require a WAL flush, let the strategy decide whether
                 * to go ahead and write/reuse the buffer or to choose another
                 * victim.	We need lock to inspect the page LSN, so this
                 * can't be done inside StrategyGetBuffer.
                 */
                if (strategy != NULL) {
                    XLogRecPtr lsn;

                    /* Read the LSN while holding buffer header lock */
                    buf_state = LockBufHdr(buf);
                    lsn = BufferGetLSN(buf);
                    UnlockBufHdr(buf, buf_state);

                    if (XLogNeedsFlush(lsn) && StrategyRejectBuffer(strategy, buf)) {
                        /* Drop lock/pin and loop around for another buffer */
                        LWLockRelease(buf->content_lock);
                        UnpinBuffer(buf, true);
                        continue;
                    }
                }

                /* during initdb, not need flush dw file */
                if (dw_enabled() && pg_atomic_read_u32(&g_instance.ckpt_cxt_ctl->current_page_writer_count) > 0) {
                    if (!free_space_enough(buf->buf_id)) {
                        LWLockRelease(buf->content_lock);
                        UnpinBuffer(buf, true);
                        continue;
                    }
                    uint32 pos = 0;
                    pos = first_version_dw_single_flush(buf);
                    t_thrd.proc->dw_pos = pos;
                    FlushBuffer(buf, NULL);
                    g_instance.dw_single_cxt.single_flush_state[pos] = true;
                    t_thrd.proc->dw_pos = -1;
                } else {
                    FlushBuffer(buf, NULL);
                }
                LWLockRelease(buf->content_lock);

                ScheduleBufferTagForWriteback(t_thrd.storage_cxt.BackendWritebackContext, &buf->tag);
            } else {
                /*
                 * Someone else has locked the buffer, so give it up and loop
                 * back to get another one.
                 */
                UnpinBuffer(buf, true);
                continue;
            }
        }

        /*
         * To change the association of a valid buffer, we'll need to have
         * exclusive lock on both the old and new mapping partitions.
         */
        if (old_flags & BM_TAG_VALID) {
            /*
             * Need to compute the old tag's hashcode and partition lock ID.
             * XXX is it worth storing the hashcode in BufferDesc so we need
             * not recompute it here?  Probably not.
             */
            old_tag = ((BufferDesc *)buf)->tag;
            old_hash = BufTableHashCode(&old_tag);
            old_partition_lock = BufMappingPartitionLock(old_hash);
            /*
             * Must lock the lower-numbered partition first to avoid
             * deadlocks.
             */
            LockTwoLWLock(new_partition_lock, old_partition_lock);
        } else {
            /* if it wasn't valid, we need only the new partition */
            (void)LWLockAcquire(new_partition_lock, LW_EXCLUSIVE);
            /* these just keep the compiler quiet about uninit variables */
            old_hash = 0;
            old_partition_lock = NULL;
        }

        /*
         * Try to make a hashtable entry for the buffer under its new tag.
         * This could fail because while we were writing someone else
         * allocated another buffer for the same block we want to read in.
         * Note that we have not yet removed the hashtable entry for the old
         * tag.
         */
        buf_id = BufTableInsert(&new_tag, new_hash, buf->buf_id);
        if (buf_id >= 0) {
            /*
             * Got a collision. Someone has already done what we were about to
             * do. We'll just handle this as if it were found in the buffer
             * pool in the first place.  First, give up the buffer we were
             * planning to use.
             */
            UnpinBuffer(buf, true);

            /* Can give up that buffer's mapping partition lock now */
            if ((old_flags & BM_TAG_VALID) && old_partition_lock != new_partition_lock)
                LWLockRelease(old_partition_lock);

            /* remaining code should match code at top of routine */
            buf = GetBufferDescriptor(buf_id);

            valid = PinBuffer(buf, strategy);

            /* Can release the mapping lock as soon as we've pinned it */
            LWLockRelease(new_partition_lock);

            *found = TRUE;

            if (!valid) {
                /*
                 * We can only get here if (a) someone else is still reading
                 * in the page, or (b) a previous read attempt failed.	We
                 * have to wait for any active read attempt to finish, and
                 * then set up our own read attempt if the page is still not
                 * BM_VALID.  StartBufferIO does it all.
                 */
                if (StartBufferIO(buf, true)) {
                    /*
                     * If we get here, previous attempts to read the buffer
                     * must have failed ... but we shall bravely try again.
                     */
                    *found = FALSE;
                }
            }

            return buf;
        }

        /*
         * Need to lock the buffer header too in order to change its tag.
         */
        buf_state = LockBufHdr(buf);

        /*
         * Somebody could have pinned or re-dirtied the buffer while we were
         * doing the I/O and making the new hashtable entry.  If so, we can't
         * recycle this buffer; we must undo everything we've done and start
         * over with a new victim buffer.
         */
        old_flags = buf_state & BUF_FLAG_MASK;
        if (BUF_STATE_GET_REFCOUNT(buf_state) == 1 && !(old_flags & BM_DIRTY)
            && !(old_flags & BM_IS_META)) {
            break;
        }

        UnlockBufHdr(buf, buf_state);
        BufTableDelete(&new_tag, new_hash);
        if ((old_flags & BM_TAG_VALID) && old_partition_lock != new_partition_lock) {
            LWLockRelease(old_partition_lock);
        }
        LWLockRelease(new_partition_lock);
        UnpinBuffer(buf, true);
    }

#ifdef USE_ASSERT_CHECKING
    PageCheckWhenChosedElimination(buf, old_flags);
#endif

    /*
     * Okay, it's finally safe to rename the buffer.
     *
     * Clearing BM_VALID here is necessary, clearing the dirtybits is just
     * paranoia.  We also reset the usage_count since any recency of use of
     * the old content is no longer relevant.  (The usage_count starts out at
     * 1 so that the buffer can survive one clock-sweep pass.)
     *
     * Make sure BM_PERMANENT is set for buffers that must be written at every
     * checkpoint.  Unlogged buffers only need to be written at shutdown
     * checkpoints, except for their "init" forks, which need to be treated
     * just like permanent relations.
     */
    ((BufferDesc *)buf)->tag = new_tag;
    buf_state &= ~(BM_VALID | BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED | BM_IO_ERROR | BM_PERMANENT |
                   BUF_USAGECOUNT_MASK);
    if (relpersistence == RELPERSISTENCE_PERMANENT || fork_num == INIT_FORKNUM ||
        ((relpersistence == RELPERSISTENCE_TEMP) && STMT_RETRY_ENABLED)) {
        buf_state |= BM_TAG_VALID | BM_PERMANENT | BUF_USAGECOUNT_ONE;
    } else {
        buf_state |= BM_TAG_VALID | BUF_USAGECOUNT_ONE;
    }

    UnlockBufHdr(buf, buf_state);

    if (old_flags & BM_TAG_VALID) {
        BufTableDelete(&old_tag, old_hash);
        if (old_partition_lock != new_partition_lock) {
            LWLockRelease(old_partition_lock);
        }
    }

    /* set Physical segment file. */
    if (pblk != NULL) {
        Assert(PhyBlockIsValid(*pblk));
        buf->extra->seg_fileno = pblk->relNode;
        buf->extra->seg_blockno = pblk->block;
    } else {
        buf->extra->seg_fileno = EXTENT_INVALID;
        buf->extra->seg_blockno = InvalidBlockNumber;
    }
    LWLockRelease(new_partition_lock);

    /*
     * Buffer contents are currently invalid.  Try to get the io_in_progress
     * lock.  If StartBufferIO returns false, then someone else managed to
     * read it before we did, so there's nothing left for BufferAlloc() to do.
     */
    if (StartBufferIO(buf, true)) {
        *found = FALSE;
    } else {
        *found = TRUE;
    }

    return buf;
}

static uint32 next_victim_buffer = 0;

static inline uint32 NvmClockSweepTick(void)
{
    uint32 victim = pg_atomic_fetch_add_u32(&next_victim_buffer, 1);
    if (victim >= (uint32)NVM_BUFFER_NUM) {
        victim = victim % NVM_BUFFER_NUM;
    }
    return victim;
}

static BufferDesc* get_nvm_buf_from_candidate_list(uint64* buf_state)
{
    BufferDesc* buf = NULL;
    uint64 local_buf_state;
    int buf_id = 0;
    int list_num = g_instance.ckpt_cxt_ctl->pgwr_procs.sub_num;
    volatile PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;
    int list_id = beentry->st_tid > 0 ? (beentry->st_tid % list_num) : (beentry->st_sessionid % list_num);

    for (int i = 0; i < list_num; i++) {
        /* the pagewriter sub thread store normal buffer pool, sub thread starts from 1 */
        int thread_id = (list_id + i) % list_num + 1;
        Assert(thread_id > 0 && thread_id <= list_num);
        while (candidate_buf_pop(&g_instance.ckpt_cxt_ctl->pgwr_procs.writer_proc[thread_id].nvm_list, &buf_id)) {
            Assert(buf_id >= NvmBufferStartID && buf_id < SegmentBufferStartID);
            buf = GetBufferDescriptor(buf_id);
            local_buf_state = LockBufHdr(buf);

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
    return NULL;
}

const int RETRY_COUNT = 3;
static BufferDesc *NvmStrategyGetBuffer(uint64* buf_state)
{
    BufferDesc *buf = NULL;
    int try_counter = NVM_BUFFER_NUM * RETRY_COUNT;
    uint64 local_buf_state = 0; /* to avoid repeated (de-)referencing */

    /* Check the Candidate list */
    if (ENABLE_INCRE_CKPT && pg_atomic_read_u32(&g_instance.ckpt_cxt_ctl->current_page_writer_count) > 1) {
        buf = get_nvm_buf_from_candidate_list(buf_state);
        if (buf != NULL) {
            (void)pg_atomic_fetch_add_u64(&g_instance.ckpt_cxt_ctl->nvm_get_buf_num_candidate_list, 1);
            return buf;
        }
    }

    for (;;) {
        int buf_id = BufferIdOfNvmBuffer(NvmClockSweepTick());
        buf = GetBufferDescriptor(buf_id);

        local_buf_state = LockBufHdr(buf);

        if (BUF_STATE_GET_REFCOUNT(local_buf_state) == 0 && !(local_buf_state & BM_IS_META) &&
            (backend_can_flush_dirty_page() || !(local_buf_state & BM_DIRTY))) {
            *buf_state = local_buf_state;
            (void)pg_atomic_fetch_add_u64(&g_instance.ckpt_cxt_ctl->nvm_get_buf_num_clock_sweep, 1);
            return buf;
        } else if (--try_counter == 0) {
            UnlockBufHdr(buf, local_buf_state);
            ereport(ERROR, (errcode(ERRCODE_INVALID_BUFFER), (errmsg("no unpinned buffers available"))));
        }
        UnlockBufHdr(buf, local_buf_state);
    }

    /* not reached */
    return NULL;
}

