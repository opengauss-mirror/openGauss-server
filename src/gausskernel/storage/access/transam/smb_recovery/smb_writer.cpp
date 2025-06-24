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
 * smb_writer.cpp
 * Parallel recovery has a centralized log dispatcher which runs inside
 * the StartupProcess.  The dispatcher is responsible for managing the
 * life cycle of PageRedoWorkers and the TxnRedoWorker, analyzing log
 * records and dispatching them to workers for processing.
 *
 * IDENTIFICATION
 * src/gausskernel/storage/access/transam/smb_recovery/smb_writer.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "access/smb.h"
#include "pgstat.h"
#include "storage/ipc.h"
#include "storage/buf/buf_internals.h"
#include "storage/rack_mem_shm.h"

namespace smb_recovery {

const int PAGE_QUEUE_SLOT_MULTI_NSMBBUFFERS = 5;
const int PAGE_QUEUE_SLOT_MIN_RESERVE_NUM = 20;
const float PAGE_QUEUE_SLOT_USED_MAX_PERCENTAGE = 0.8;
static const int TEN_MICROSECOND = 10;
static const uint64 THOUSAND_MICROSECOND = 1000;

void InitSMBWriter()
{
    errno_t rc = memset_s(
        &g_instance.smb_cxt.ctx,
        sizeof(knl_g_smb_context) - offsetof(knl_g_smb_context, ctx),
        0,
        sizeof(knl_g_smb_context) - offsetof(knl_g_smb_context, ctx)
    );
    securec_check(rc, "\0", "\0");
    if (ENABLE_DMS && !(SS_PRIMARY_MODE)) {
        return;
    }
    g_instance.smb_cxt.chunkNum = g_instance.smb_cxt.NSMBBuffers / BLOCKS_PER_CHUNK + 1;
    if (g_instance.smb_cxt.NSMBBuffers % BLOCKS_PER_CHUNK) {
        g_instance.smb_cxt.chunkNum++;
    }
    g_instance.smb_cxt.SMBWriterPID = initialize_util_thread(SMBWRITER);
}

static uint32 SMBGetHashCode(BufferTag tag)
{
    uint32 file_hash = (tag.rnode.dbNode + tag.rnode.relNode) % SMB_WRITER_MAX_FILE;
    uint32 page_hash = tag.blockNum % SMB_WRITER_MAX_BUCKET_PER_FILE;
    uint32 hash_code = file_hash * SMB_WRITER_MAX_BUCKET_PER_FILE + page_hash;
    return hash_code;
}

static void SMBShutdownHandler(SIGNAL_ARGS)
{
    g_instance.smb_cxt.shutdownSMBWriter = true;
}

static void SetupSignalHandlers()
{
    (void)gspqsignal(SIGHUP, SIG_IGN);
    (void)gspqsignal(SIGINT, SMBShutdownHandler);
    (void)gspqsignal(SIGTERM, SMBShutdownHandler);
    (void)gspqsignal(SIGQUIT, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, SIG_IGN);
    (void)gspqsignal(SIGUSR2, SIG_IGN);
    (void)gspqsignal(SIGCHLD, SIG_IGN);
    (void)gspqsignal(SIGTTIN, SIG_IGN);
    (void)gspqsignal(SIGTTOU, SIG_IGN);
    (void)gspqsignal(SIGCONT, SIG_IGN);
    (void)gspqsignal(SIGWINCH, SIG_IGN);
    (void)gspqsignal(SIGURG, print_stack);
    (void)gspqsignal(SIGALRM, SIG_IGN);
    
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();
}

bool CheckPagePullDoneFromSMB(SMBAnalyseBucket *bucket, BufferTag tag, bool *getSharedLock)
{
    if (!LWLockConditionalAcquire(&bucket->lock, LW_SHARED)) {
        *getSharedLock = false;
        return false;
    }
    *getSharedLock = true;

    SMBAnalyseItem *item = SMBAlyGetPageItem(tag);
    if (item == NULL || item->is_verified) {
        LWLockRelease(&bucket->lock);
        return true;
    }
    if (item->lsn < g_instance.smb_cxt.smb_start_lsn ||
        item->lsn > g_instance.smb_cxt.smb_end_lsn) {
        item->is_verified = true;
        LWLockRelease(&bucket->lock);
        return true;
    }
    LWLockRelease(&bucket->lock);
    return false;
}

bool TryLockBucket(SMBAnalyseBucket *bucket, BufferTag tag, bool *noNeedRedo)
{
    if (!LWLockConditionalAcquire(&bucket->lock, LW_EXCLUSIVE)) {
        *noNeedRedo = false;
        return false;
    }
    SMBAnalyseItem *item = SMBAlyGetPageItem(tag);
    if (item == NULL || item->is_verified) {
        LWLockRelease(&bucket->lock);
        *noNeedRedo = true;
        return false;
    }
    if (item->lsn < g_instance.smb_cxt.smb_start_lsn ||
        item->lsn > g_instance.smb_cxt.smb_end_lsn) {
        item->is_verified = true;
        LWLockRelease(&bucket->lock);
        *noNeedRedo = true;
        return false;
    }
    return true;
}

int CheckPagePullStateFromSMB(BufferTag tag)
{
    bool noNeedRedo = false;
    bool getSharedLock = false;
    SMBAnalyseBucket *bucket = SMBAlyGetBucket(tag);
    if (CheckPagePullDoneFromSMB(bucket, tag, &getSharedLock)) {
        return SMB_REDO_DONE;
    } else if (!getSharedLock) {
        return SMB_REDOING;
    } else if (TryLockBucket(bucket, tag, &noNeedRedo)) {
        return SMB_NEED_REDO;
    } else if (noNeedRedo) {
        return SMB_REDO_DONE;
    }
    return SMB_REDOING;
}

SMBBufItem *SMBWriterTagGetItem(BufferTag tag)
{
    SMBBufMetaMem *mgr = g_instance.smb_cxt.SMBBufMgr;
    uint32 hashCode = SMBGetHashCode(tag);
    int mgrId = hashCode % SMB_BUF_MGR_NUM;
    int bktId = hashCode / SMB_BUF_MGR_NUM;
    int curId = mgr[mgrId].smbWriterBuckets[bktId];
    while (curId != SMB_INVALID_ID) {
        SMBBufItem *item = SMBWriterIdGetItem(mgr, curId);
        if (BUFFERTAGS_EQUAL(tag, item->tag)) {
            return item;
        }
        curId = item->nextBktId;
    }
    return nullptr;
}

bool CheckPagePullDoneFromSMBAndLock(SMBAnalyseBucket *bucket, BufferTag tag)
{
    if (LWLockHeldByMe(&bucket->lock)) {
        Assert(LWLockHeldByMeInMode(&bucket->lock, LW_EXCLUSIVE));
        SMBAnalyseItem *alyItem = SMBAlyGetPageItem(tag);
        if (alyItem == NULL || alyItem->is_verified) {
            LWLockRelease(&bucket->lock);
            return true;
        }
        return false;
    }
    (void)LWLockAcquire(&bucket->lock, LW_SHARED);
    SMBAnalyseItem *alyItem = SMBAlyGetPageItem(tag);
    if (alyItem == NULL || alyItem->is_verified) {
        LWLockRelease(&bucket->lock);
        return true;
    }
    LWLockRelease(&bucket->lock);
    (void)LWLockAcquire(&bucket->lock, LW_EXCLUSIVE);
    if (alyItem == NULL || alyItem->is_verified) {
        LWLockRelease(&bucket->lock);
        return true;
    }
    return false;
}

/* use for async
 * while g_instance.smb_cxt.SMBAlyMem != NULL, read page must be checked
 */
void SMBPullOnePageWithBuf(BufferDesc *bufHdr)
{
    int buf;
    Page page;
    Page curPage;
    XLogRecPtr expectLsn;
    errno_t rc = EOK;
    SMBBufItem *item;
    SMBBufMetaMem *mgr = g_instance.smb_cxt.SMBBufMgr;
    SMBAnalyseBucket *bucket = SMBAlyGetBucket(bufHdr->tag);
    SMBAnalyseItem *alyItem = SMBAlyGetPageItem(bufHdr->tag);
    if (alyItem == NULL || alyItem->is_verified) {
        return;
    }

    if (CheckPagePullDoneFromSMBAndLock(bucket, bufHdr->tag)) {
        return;
    }

    expectLsn = alyItem->lsn;
    item = SMBWriterTagGetItem(bufHdr->tag);
    page = SMBWriterGetPage(item->id);
    buf = BufferDescriptorGetBuffer(bufHdr);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    curPage = (Page)BufHdrGetBlock(bufHdr);
    if (expectLsn >= PageGetLSN(curPage) && PageGetLSN(page) > PageGetLSN(curPage)) {
        rc = memcpy_s(curPage, BLCKSZ, page, BLCKSZ);
        securec_check(rc, "", "");
        MarkBufferDirty(buf);
        if (bufHdr->tag.forkNum == INIT_FORKNUM) {
            FlushOneBuffer(buf);
        }
    }
    alyItem->is_verified = true;
    LockBuffer(buf, BUFFER_LOCK_UNLOCK);
    LWLockRelease(&bucket->lock);
}

static void SMBPullOnePage(BufferTag tag, int id)
{
    int buf;
    Page page;
    Page curPage;
    XLogRecPtr expectLsn;
    errno_t rc = EOK;
    SMBBufMetaMem *mgr = g_instance.smb_cxt.SMBBufMgr;
    page = SMBWriterGetPage(id);
    if (PageGetLSN(page) <= g_instance.smb_cxt.cur_lsn) {
        ereport(DEBUG4, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                         errmsg("SMB page is old spc/db/rel/bucket fork-block: %u/%u/%u/%d %d-%u, lsn :%lu, %lu, %lu",
                                tag.rnode.spcNode, tag.rnode.dbNode, tag.rnode.relNode, tag.rnode.bucketNode,
                                tag.forkNum, tag.blockNum, g_instance.smb_cxt.cur_lsn, PageGetLSN(page), expectLsn)));
        return;
    }

    SMBAnalyseBucket *bucket = SMBAlyGetBucket(tag);
    SMBAnalyseItem *analyseItem = SMBAlyGetPageItem(tag);
    if (ENABLE_ASYNC_REDO) {
        LWLockAcquire(&bucket->lock, LW_EXCLUSIVE);
    }
    if (analyseItem == NULL) {
        ereport(ERROR, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
            errmsg("SMB can't find the item spc/db/rel/bucket fork-block: %u/%u/%u/%d %d-%u.",
                tag.rnode.spcNode, tag.rnode.dbNode, tag.rnode.relNode, tag.rnode.bucketNode,
                tag.forkNum, tag.blockNum)));
        return;
    }

    if (analyseItem->is_verified) {
        return;
    }
    expectLsn = analyseItem->lsn;
    
    if (IsSegmentPhysicalRelNode(tag.rnode)) {
        buf = XLogReadBufferExtended(tag.rnode, tag.forkNum, tag.blockNum, RBM_ZERO_AND_LOCK, NULL);
    } else if (IsSegmentFileNode(tag.rnode)) {
        buf = XLogReadBufferExtended(tag.rnode, tag.forkNum, tag.blockNum, RBM_ZERO_AND_LOCK, &analyseItem->pblk);
    } else {
        buf = XLogReadBufferExtended(tag.rnode, tag.forkNum, tag.blockNum, RBM_ZERO_AND_LOCK, NULL);
    }
    curPage = BufferGetPage(buf);
    if (expectLsn >= PageGetLSN(curPage) && PageGetLSN(page) > PageGetLSN(curPage)) {
        rc = memcpy_s(curPage, BLCKSZ, page, BLCKSZ);
        securec_check(rc, "", "");
        MarkBufferDirty(buf);
        if (tag.forkNum == INIT_FORKNUM) {
            FlushOneBuffer(buf);
        }
    } else {
        ereport(DEBUG4, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
            errmsg("SMB error on page spc/db/rel/bucket fork-block: %u/%u/%u/%d %d-%u, lsn :%lu, %lu, %lu.",
                tag.rnode.spcNode, tag.rnode.dbNode, tag.rnode.relNode, tag.rnode.bucketNode,
                tag.forkNum, tag.blockNum, PageGetLSN(curPage), PageGetLSN(page), expectLsn)));
    }
    UnlockReleaseBuffer(buf);
    analyseItem->is_verified = true;
    if (ENABLE_ASYNC_REDO) {
        LWLockRelease(&bucket->lock);
    }
}

static void SMBPullPages()
{
    SMBBufMetaMem *mgr = g_instance.smb_cxt.SMBBufMgr;
    t_thrd.xlog_cxt.InRecovery = true;
    for (int i = 0; i< SMB_BUF_MGR_NUM; i++) {
        int current = *mgr[i].lruHeadId;
        while (current != SMB_INVALID_ID) {
            SMBBufItem *cur = SMBWriterIdGetItem(mgr, current);
            SMBPullOnePage(cur->tag, current);
            current = cur->nextLRUId;
        }
    }
}

void SMBStartPullPages(XLogRecPtr curPtr)
{
    g_instance.smb_cxt.start_flag = true;
    g_instance.smb_cxt.cur_lsn = curPtr;
}

static inline void SMBBufLruRemove(SMBBufItem *item)
{
    SMBBufMetaMem *mgr = g_instance.smb_cxt.SMBBufMgr;
    int mgrId = item->id / SMB_WRITER_ITEM_PER_MGR;
    if (item->prevLRUId != SMB_INVALID_ID) {
        SMBBufItem *prev = SMBWriterIdGetItem(mgr, item->prevLRUId);
        prev->nextLRUId = item->nextLRUId;
    } else {
        *mgr[mgrId].lruHeadId = item->nextLRUId;
    }

    if (item->nextLRUId != SMB_INVALID_ID) {
        SMBBufItem *next = SMBWriterIdGetItem(mgr, item->nextLRUId);
        next->prevLRUId = item->prevLRUId;
    } else {
        *mgr[mgrId].lruTailId = item->prevLRUId;
    }

    item->prevLRUId = SMB_INVALID_ID;
    item->nextLRUId = SMB_INVALID_ID;
}

static inline void SMBBufLruAddTail(SMBBufItem *item)
{
    SMBBufMetaMem *mgr = g_instance.smb_cxt.SMBBufMgr;
    int mgrId = item->id / SMB_WRITER_ITEM_PER_MGR;
    SMBBufItem *tail = SMBWriterIdGetItem(mgr, *mgr[mgrId].lruTailId);
    *mgr[mgrId].lruTailId = item->id;
    tail->nextLRUId = item->id;
    item->prevLRUId = tail->id;
    item->nextLRUId = SMB_INVALID_ID;
}

static inline void SMBBufHashAdd(SMBBufItem *item, uint32 hash_code)
{
    SMBBufMetaMem *mgr = g_instance.smb_cxt.SMBBufMgr;
    int mgrId = hash_code % SMB_BUF_MGR_NUM;
    int bktId = hash_code / SMB_BUF_MGR_NUM;
    int bucket = mgr[mgrId].smbWriterBuckets[bktId];
    item->prevBktId = SMB_INVALID_ID;
    item->nextBktId = bucket;
    if (bucket != SMB_INVALID_ID) {
        SMBBufItem *head = SMBWriterIdGetItem(mgr, bucket);
        head->prevBktId = item->id;
    }
    mgr[mgrId].smbWriterBuckets[bktId] = item->id;
}

static inline void SMBBufHashRemove(SMBBufItem *item)
{
    SMBBufMetaMem *mgr = g_instance.smb_cxt.SMBBufMgr;
    if (item->prevBktId != SMB_INVALID_ID) {
        SMBBufItem *prev = SMBWriterIdGetItem(mgr, item->prevBktId);
        prev->nextBktId = item->nextBktId;
    } else {
        uint32 hash_code = SMBGetHashCode(item->tag);
        int mgrId = hash_code % SMB_BUF_MGR_NUM;
        int bktId = hash_code / SMB_BUF_MGR_NUM;
        mgr[mgrId].smbWriterBuckets[bktId] = item->nextBktId;
    }
    if (item->nextBktId != SMB_INVALID_ID) {
        SMBBufItem *next = SMBWriterIdGetItem(mgr, item->nextBktId);
        next->prevBktId = item->prevBktId;
    }
    item->prevBktId = SMB_INVALID_ID;
    item->nextBktId = SMB_INVALID_ID;
}

static void SMBPushOnePage(BufferDesc *buf_desc, XLogRecPtr lsn)
{
    Page page = (Page)BufHdrGetBlock(buf_desc);
    SMBBufItem *item = NULL;
    SMBBufMetaMem *mgr = g_instance.smb_cxt.SMBBufMgr;
    SMBStatusInfo *info = g_instance.smb_cxt.smbInfo;
    uint32 hash_code = SMBGetHashCode(buf_desc->tag);
    SMBBufMetaMem *curMgr = &mgr[hash_code % SMB_BUF_MGR_NUM];
    int bucket = curMgr->smbWriterBuckets[hash_code / SMB_BUF_MGR_NUM];
    errno_t rc;

    int itemId = bucket;
    while (itemId != SMB_INVALID_ID) {
        item = SMBWriterIdGetItem(mgr, itemId);
        if (BUFFERTAGS_EQUAL(buf_desc->tag, item->tag)) {
            (void)LWLockAcquire(buf_desc->content_lock, LW_SHARED);
            rc = memcpy_s(SMBWriterGetPage(itemId), BLCKSZ, page, BLCKSZ);
            item->lsn = PageGetLSN(page);
            LWLockRelease(buf_desc->content_lock);
            securec_check(rc, "", "");
            SMBBufLruRemove(item);
            SMBBufLruAddTail(item);
            *curMgr->endLsn = lsn;
            return;
        }
        itemId = item->nextBktId;
    }
    /* not found, use head */
    if (*curMgr->startLsn == 0) {
        *curMgr->startLsn = lsn;
    }
    item = SMBWriterIdGetItem(mgr, *curMgr->lruHeadId);
    SMBBufLruRemove(item);
    if (XLogRecPtrIsValid(item->lsn)) {
        *curMgr->startLsn = Max(*curMgr->startLsn, item->lsn);
        SMBBufHashRemove(item);
        info->lruRemovedNum++;
    }
    (void)LWLockAcquire(buf_desc->content_lock, LW_SHARED);
    rc = memcpy_s(SMBWriterGetPage(item->id), BLCKSZ, page, BLCKSZ);
    item->lsn = PageGetLSN(page);
    LWLockRelease(buf_desc->content_lock);
    securec_check(rc, "", "");
    item->tag = buf_desc->tag;
    SMBBufLruAddTail(item);
    *curMgr->endLsn = lsn;
    SMBBufHashAdd(item, hash_code);
}

static void PushSMBMem(int queueId)
{
    BufferDesc *buf_desc = NULL;
    uint64 buf_state;
    uint64 temp_loc;
    XLogRecPtr lsn;
    SMBDirtyPageQueue *queue = &g_instance.smb_cxt.pageQueue[queueId];
    uint64 head = pg_atomic_read_u64(&queue->head);
    uint64 newLoc = pg_atomic_barrier_read_u64(&queue->tail);
    uint64 expected_flush_num = newLoc - head;

    for (uint32 i = 0; i < expected_flush_num; i++) {
        temp_loc = (head + i) % queue->size;
        volatile SMBDirtyPageQueueSlot* slot = &queue->pageQueueSlot[temp_loc];
        if (!(pg_atomic_read_u32(&slot->slot_state) & SLOT_VALID)) {
            break;
        }
        pg_read_barrier();
        ResourceOwnerEnlargeBuffers(t_thrd.utils_cxt.CurrentResourceOwner);
        ReservePrivateRefCountEntry();
        buf_desc = GetBufferDescriptor(slot->buffer - 1);
        buf_state = LockBufHdr(buf_desc);
        /* page in buffer may be replaced by another page */
        if (!BUFFERTAGS_EQUAL(slot->tag, buf_desc->tag)) {
            pg_atomic_write_u64(&buf_desc->extra->smb_rec_lsn, InvalidXLogRecPtr);
            UnlockBufHdr(buf_desc, buf_state);
            pg_atomic_init_u32(&slot->slot_state, 0);
            (void)pg_atomic_fetch_add_u64(&queue->head, 1);
            g_instance.smb_cxt.has_gap = true;
            break;
        }
        lsn = pg_atomic_read_u64(&buf_desc->extra->smb_rec_lsn);
        pg_atomic_write_u64(&buf_desc->extra->smb_rec_lsn, InvalidXLogRecPtr);
        PinBuffer_Locked(buf_desc);
        if (!BUFFERTAGS_EQUAL(slot->tag, buf_desc->tag)) {
            UnpinBuffer(buf_desc, lsn);
            pg_atomic_init_u32(&slot->slot_state, 0);
            (void)pg_atomic_fetch_add_u64(&queue->head, 1);
            g_instance.smb_cxt.has_gap = true;
            break;
        }
        SMBPushOnePage(buf_desc, true);
        UnpinBuffer(buf_desc, true);
        pg_atomic_init_u32(&slot->slot_state, 0);
        (void)pg_atomic_fetch_add_u64(&queue->head, 1);
    }
}

static bool PushSMBQueue(Buffer buffer, XLogRecPtr lsn)
{
    uint64 head;
    uint64 tail;
    uint64 newTail;
    BufferDesc* desc = GetBufferDescriptor(buffer - 1);
    uint32 hashCode = SMBGetHashCode(desc->tag);
    uint32 queueId = hashCode % SMB_BUF_MGR_NUM;
    SMBDirtyPageQueue *queue = &g_instance.smb_cxt.pageQueue[queueId];

loop:
    head = pg_atomic_read_u64(&queue->head);
    tail = pg_atomic_read_u64(&queue->tail);
    if ((tail - head) >= (queue->size - PAGE_QUEUE_SLOT_MIN_RESERVE_NUM)) {
        return false;
    }

    newTail = tail + 1;

    if (!pg_atomic_compare_exchange_u64(&queue->tail, &tail, newTail)) {
        goto loop;
    }

    uint64 actualLoc = tail % queue->size;
    Assert(!(pg_atomic_read_u32(&queue->pageQueueSlot[actualLoc].slot_state) & SLOT_VALID));
    pg_atomic_write_u64(&desc->extra->smb_rec_lsn, lsn);
    queue->pageQueueSlot[actualLoc].buffer = buffer;
    queue->pageQueueSlot[actualLoc].tag = desc->tag;
    (void)pg_atomic_fetch_add_u64(&g_instance.smb_cxt.smbInfo->pageDirtyNums, 1);
    pg_memory_barrier();
    pg_atomic_write_u32(&queue->pageQueueSlot[actualLoc].slot_state, (SLOT_VALID));
    return true;
}

static bool IsSMBQueueFull(Buffer buffer)
{
    BufferDesc* desc = GetBufferDescriptor(buffer - 1);
    uint32 hashCode = SMBGetHashCode(desc->tag);
    uint32 queueId = hashCode % SMB_BUF_MGR_NUM;
    SMBDirtyPageQueue *queue = &g_instance.smb_cxt.pageQueue[queueId];
    uint64 head = pg_atomic_read_u64(&queue->head);
    uint64 newLoc = pg_atomic_barrier_read_u64(&queue->tail);
    if (newLoc - head >= queue->size * PAGE_QUEUE_SLOT_USED_MAX_PERCENTAGE) {
        return true;
    }

    return false;
}

void SMBMarkDirty(Buffer buffer, XLogRecPtr lsn)
{
    if (!BufferIsValid(buffer) || BufferIsLocal(buffer)) {
        ereport(DEBUG4, (errmsg("SMB get local buffer ID: %d", buffer)));
        return;
    }

    BufferDesc *buf_desc = GetBufferDescriptor(buffer - 1);
    BufferTag tag = buf_desc->tag;
    uint64 buf_state = LockBufHdr(buf_desc);

    for (;;) {
        if (!XLogRecPtrIsInvalid(pg_atomic_read_u64(&buf_desc->extra->smb_rec_lsn))) {
            ereport(DEBUG4, (errmsg("SMB pass, page is %u/%u/%u/%d %d-%u, buffer id %d from %lu to %lu",
                tag.rnode.spcNode, tag.rnode.dbNode, tag.rnode.relNode, tag.rnode.bucketNode,
                tag.forkNum, tag.blockNum, buffer, PageGetLSN(BufferGetPage(buffer)), lsn)));
            break;
        }
        if (!IsSMBQueueFull(buffer) && PushSMBQueue(buffer, lsn)) {
            ereport(DEBUG4, (errmsg("SMB push, page is %u/%u/%u/%d %d-%u, buffer id %d from %lu to %lu",
                tag.rnode.spcNode, tag.rnode.dbNode, tag.rnode.relNode, tag.rnode.bucketNode,
                tag.forkNum, tag.blockNum, buffer, PageGetLSN(BufferGetPage(buffer)), lsn)));
            break;
        }
        pg_usleep(TEN_MICROSECOND);
    }

    UnlockBufHdr(buf_desc, buf_state);
}

static void SMBWriterMemCreate()
{
    if (g_instance.smb_cxt.pageQueue == NULL) {
        g_instance.smb_cxt.pageQueue = (SMBDirtyPageQueue*)palloc0(SMB_BUF_MGR_NUM * sizeof(SMBDirtyPageQueue));
        for (int i = 0; i < SMB_BUF_MGR_NUM; i++) {
            g_instance.smb_cxt.pageQueue[i].size = SMB_WRITER_ITEM_PER_MGR * PAGE_QUEUE_SLOT_MULTI_NSMBBUFFERS;
            Size queueMemSize = g_instance.smb_cxt.pageQueue[i].size * sizeof(SMBDirtyPageQueueSlot);
            g_instance.smb_cxt.pageQueue[i].pageQueueSlot =
                (SMBDirtyPageQueueSlot*)palloc_huge(CurrentMemoryContext, queueMemSize);
            MemSet((char*)g_instance.smb_cxt.pageQueue[i].pageQueueSlot, 0, queueMemSize);
        }
    }
    g_instance.smb_cxt.SMBBufMem = (void**)palloc0(g_instance.smb_cxt.chunkNum * sizeof(void*));
    g_instance.smb_cxt.smbInfo = (SMBStatusInfo*)palloc0(sizeof(SMBStatusInfo));
    g_instance.smb_cxt.smbInfo->pageDirtyNums = 0;
    g_instance.smb_cxt.smbInfo->lruRemovedNum = 0;
}

static void MountFailedCallBack()
{
    const char* msg = "\nWARNING:  SMB init rackmem failed. SMB will not be use.";
    size_t len = strlen(msg);
    if (g_instance.smb_cxt.stderrFd != -1) {
        write(g_instance.smb_cxt.stderrFd, msg, len);
        fsync(g_instance.smb_cxt.stderrFd);
        close(g_instance.smb_cxt.stderrFd);
        g_instance.smb_cxt.stderrFd = -1;
    }
    g_instance.smb_cxt.shutdownSMBWriter = true;
    ereport(LOG, (errmsg("SMB init rackmem failed. SMB will not be use")));
}

/* mount hash and lru */
static void SMBWriterMemMount()
{
    ereport(LOG, (errmsg("SMB start init Mem.")));
    g_instance.smb_cxt.mount_end_flag = false;
    /* meta data */
    g_instance.smb_cxt.SMBBufMem[0] = RackMemShmMmap(nullptr, MAX_RACK_ALLOC_SIZE,
                                                     PROT_READ | PROT_WRITE, MAP_SHARED, SHARED_MEM_NAME, 0);
    if (g_instance.smb_cxt.SMBBufMem[0] == NULL) {
        MountFailedCallBack();
        return;
    }
    /* buf data */
    char shmChunkName[MAX_SHM_CHUNK_NAME_LENGTH] = "";
    for (size_t i = 0; i < g_instance.smb_cxt.chunkNum - 1; i++) {
        GetSmbShmChunkName(shmChunkName, i);
        g_instance.smb_cxt.SMBBufMem[i + 1] = RackMemShmMmap(nullptr, MAX_RACK_ALLOC_SIZE,
                                                             PROT_READ | PROT_WRITE, MAP_SHARED, shmChunkName, 0);
        if (g_instance.smb_cxt.SMBBufMem[i + 1] == NULL) {
            MountFailedCallBack();
            return;
        }
    }

    if (g_instance.smb_cxt.stderrFd != -1) {
        close(g_instance.smb_cxt.stderrFd);
        g_instance.smb_cxt.stderrFd = -1;
    }
    SMBBufMetaMem *mgr = NULL;
    uint64 buf_size = SMB_WRITER_MAX_ITEM_SIZE + SMB_WRITER_MAX_BUCKET_SIZE;
    mgr = (SMBBufMetaMem *)palloc0(sizeof(SMBBufMetaMem) * SMB_BUF_MGR_NUM);
    g_instance.smb_cxt.smb_end_lsn = MAX_XLOG_REC_PTR;
    for (int i = 0; i < SMB_BUF_MGR_NUM; i++) {
        mgr[i].smbWriterItems = (SMBBufItem *)(g_instance.smb_cxt.SMBBufMem[0] + i * SMB_BUF_META_SIZE);
        mgr[i].smbWriterBuckets = (int *)((char *)mgr[i].smbWriterItems + SMB_WRITER_ITEM_SIZE_PERMETA);
        mgr[i].lruHeadId = (int *)((char *)mgr[i].smbWriterBuckets + SMB_WRITER_BUCKET_SIZE_PERMETA);
        mgr[i].lruTailId = (int *)((char *)mgr[i].lruHeadId + sizeof(int));
        mgr[i].startLsn = (XLogRecPtr *)((char *)mgr[i].lruTailId + sizeof(int));
        mgr[i].endLsn = (XLogRecPtr *)((char *)mgr[i].startLsn + sizeof(XLogRecPtr));
        while (*mgr[i].lruHeadId != *mgr[i].lruTailId &&
               XLogRecPtrIsInvalid(SMBWriterIdGetItem(mgr, *mgr[i].lruHeadId)->lsn)) {
            *mgr[i].lruHeadId = SMBWriterIdGetItem(mgr, *mgr[i].lruHeadId)->nextLRUId;
        }
        g_instance.smb_cxt.smb_start_lsn = Max(g_instance.smb_cxt.smb_start_lsn, *mgr[i].startLsn);
        g_instance.smb_cxt.smb_end_lsn = Min(g_instance.smb_cxt.smb_end_lsn, *mgr[i].endLsn);
    }
    ereport(LOG, (errmsg("SMB mount success, start_lsn : %lu, end_lsn : %lu",
        g_instance.smb_cxt.smb_start_lsn, g_instance.smb_cxt.smb_end_lsn)));
    g_instance.smb_cxt.SMBBufMgr = mgr;
    g_instance.smb_cxt.mount_end_flag = true;
}

static void SMBWriterMemUnmmap()
{
    int ret = 0;
    for (size_t i = 0; i < g_instance.smb_cxt.chunkNum; i++) {
        ret = RackMemShmUnmmap(g_instance.smb_cxt.SMBBufMem[i], MAX_RACK_ALLOC_SIZE);
        if (ret != 0) {
            ereport(LOG, (errmsg("SMB unmmap rackmem failed.")));
        }
    }
}

/* init hash and lru */
static void SMBWriterResetMgr()
{
    SMBBufMetaMem *mgr = g_instance.smb_cxt.SMBBufMgr;
    for (int i = 0; i < SMB_BUF_MGR_NUM; i++) {
        auto ret = memset_sp(mgr[i].smbWriterItems, SMB_WRITER_ITEM_SIZE_PERMETA, 0, SMB_WRITER_ITEM_SIZE_PERMETA);
        securec_check(ret, "", "");
        ret = memset_sp(mgr[i].smbWriterBuckets, SMB_WRITER_BUCKET_SIZE_PERMETA, -1, SMB_WRITER_BUCKET_SIZE_PERMETA);
        securec_check(ret, "", "");

        *mgr[i].lruHeadId = i * SMB_WRITER_ITEM_PER_MGR;
        *mgr[i].lruTailId = (i + 1) * SMB_WRITER_ITEM_PER_MGR - 1;
        *mgr[i].startLsn = 0;
        *mgr[i].endLsn = 0;
    }
    
    for (int i = 0; i < g_instance.smb_cxt.NSMBBuffers; i++) {
        int mgrId = i / SMB_WRITER_ITEM_PER_MGR;
        int itemId = i % SMB_WRITER_ITEM_PER_MGR;
        mgr[mgrId].smbWriterItems[itemId].id = i;
        mgr[mgrId].smbWriterItems[itemId].nextBktId = SMB_INVALID_ID;
        mgr[mgrId].smbWriterItems[itemId].prevBktId = SMB_INVALID_ID;
        mgr[mgrId].smbWriterItems[itemId].nextLRUId = i + 1;
        mgr[mgrId].smbWriterItems[itemId].prevLRUId = i - 1;
        mgr[mgrId].smbWriterItems[itemId].lsn = InvalidXLogRecPtr;
    }
    for (int i = 0; i < SMB_BUF_MGR_NUM; i++) {
        SMBBufItem *lruHeadItem = SMBWriterIdGetItem(mgr, *mgr[i].lruHeadId);
        SMBBufItem *lruTailItem = SMBWriterIdGetItem(mgr, *mgr[i].lruTailId);
        lruHeadItem->prevLRUId = SMB_INVALID_ID;
        lruTailItem->nextLRUId = SMB_INVALID_ID;
    }
}

static bool IsSMBWriterAuxWriting()
{
    for (int i = 0; i < SMB_BUF_MGR_NUM - 1; i++) {
        if (g_instance.smb_cxt.SMBWriterAuxWriting[i]) {
            return true;
        }
    }
    return false;
}

void SMBWriterMain(void)
{
    ereport(LOG, (errmsg("SMB Writer Start.")));
    pgstat_report_appname("SMB Writer");
    pgstat_report_activity(STATE_IDLE, NULL);
    t_thrd.role = SMBWRITER;
    SetupSignalHandlers();
    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "SMBWriterThread",
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));
    MemoryContext ctx = AllocSetContextCreate(g_instance.instance_context, "SMBWriter",
        ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE, SHARED_CONTEXT);
    (void)MemoryContextSwitchTo(ctx);
    SMBWriterMemCreate();

    while (RecoveryInProgress()) {
        if (g_instance.smb_cxt.analyze_end_flag && g_instance.smb_cxt.analyze_aux_end_flag
            && g_instance.smb_cxt.SMBBufMgr == NULL) {
            ereport(LOG, (errmsg("SMB mount during redo")));
            SMBWriterMemMount();
        }
        if (g_instance.smb_cxt.start_flag && !g_instance.smb_cxt.end_flag) {
            SMBPullPages();
            g_instance.smb_cxt.end_flag = true;
            g_instance.smb_cxt.start_flag = false;
            g_instance.smb_cxt.analyze_end_flag = false;
            g_instance.smb_cxt.analyze_aux_end_flag = false;
            ereport(LOG, (errmsg("SMB pull page end.")));
        }
        pg_usleep(10000L);
    }

    if (g_instance.smb_cxt.ctx != NULL) {
        MemoryContextDelete(g_instance.smb_cxt.ctx);
    }
    if (g_instance.smb_cxt.SMBBufMgr == NULL) {
        ereport(LOG, (errmsg("SMB mount after redo")));
        SMBWriterMemMount();
    }
    t_thrd.xlog_cxt.InRecovery = false;
    g_instance.smb_cxt.has_gap = true;

    for (int i = 0; i < SMB_BUF_MGR_NUM - 1; i++) {
        if (g_instance.smb_cxt.SMBWriterAuxPID[i] == 0) {
            g_instance.smb_cxt.SMBWriterAuxPID[i] = initialize_util_thread(SMBWRITERAUXILIARY);
        }
    }

    for (;;) {
        instr_time start;
        INSTR_TIME_SET_CURRENT(start);
        if (g_instance.smb_cxt.shutdownSMBWriter) {
            PushSMBMem(0);
            break;
        }
        if (g_instance.smb_cxt.has_gap) {
            if (IsSMBWriterAuxWriting()) {
                continue;
            }
            SMBWriterResetMgr();
            g_instance.smb_cxt.has_gap = false;
        }
        PushSMBMem(0);
        instr_time tmp;
        INSTR_TIME_SET_CURRENT(tmp);
        INSTR_TIME_SUBTRACT(tmp, start);
        uint64 totalTime = INSTR_TIME_GET_MICROSEC(tmp);
        if (!g_instance.smb_cxt.has_gap && (totalTime < THOUSAND_MICROSECOND)) {
            pg_usleep(THOUSAND_MICROSECOND - totalTime);
        }
    }

    while (pg_atomic_read_u32(&g_instance.smb_cxt.curSMBWriterIndex) != 0) {
        pg_usleep(10000L);
    }
    if (g_instance.smb_cxt.SMBBufMgr != nullptr) {
        SMBWriterMemUnmmap();
    }
    g_instance.smb_cxt.SMBWriterPID = 0;
    ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, false, true);
    MemoryContextDelete(ctx);
    ereport(LOG, (errmsg("SMB Writer End.")));
    proc_exit(0);
}

int SMBWriterLoadThreadIndex(void)
{
    if (t_thrd.smbWriterCxt.smbWriterAuxIdx != -1) {
        return t_thrd.smbWriterCxt.smbWriterAuxIdx;
    }

    int idx = pg_atomic_fetch_add_u32(&g_instance.smb_cxt.curSMBWriterIndex, 1);
    t_thrd.smbWriterCxt.smbWriterAuxIdx = idx;

    Assert(t_thrd.smbWriterCxt.smbWriterAuxIdx >= 0);
    Assert(t_thrd.smbWriterCxt.smbWriterAuxIdx < SMB_BUF_MGR_NUM - 1);

    return t_thrd.smbWriterCxt.smbWriterAuxIdx;
}

void SMBWriterAuxiliaryMain(void)
{
    t_thrd.role = SMBWRITERAUXILIARY;
    ereport(LOG, (errmsg("SMB writer auxiliary start, thread id is %d.", t_thrd.smbWriterCxt.smbWriterAuxIdx)));
    pgstat_report_appname("SMB Writer Auxiliary");
    pgstat_report_activity(STATE_IDLE, NULL);
    SetupSignalHandlers();
    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "SMBWriterAuxiliaryThread",
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));
    MemoryContext ctx = AllocSetContextCreate(g_instance.instance_context, "SMBWriterAuxiliary",
        ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE, SHARED_CONTEXT);
    (void)MemoryContextSwitchTo(ctx);
    int threadId = t_thrd.smbWriterCxt.smbWriterAuxIdx;

    for (;;) {
        instr_time start;
        INSTR_TIME_SET_CURRENT(start);
        if (g_instance.smb_cxt.has_gap) {
            pg_usleep(THOUSAND_MICROSECOND);
            continue;
        }
        g_instance.smb_cxt.SMBWriterAuxWriting[threadId] = true;
        if (g_instance.smb_cxt.shutdownSMBWriter) {
            PushSMBMem(threadId + 1);
            break;
        }
        PushSMBMem(threadId + 1);
        g_instance.smb_cxt.SMBWriterAuxWriting[threadId] = false;
        instr_time tmp;
        INSTR_TIME_SET_CURRENT(tmp);
        INSTR_TIME_SUBTRACT(tmp, start);
        uint64 totalTime = INSTR_TIME_GET_MICROSEC(tmp);
        if (!g_instance.smb_cxt.has_gap && (totalTime < THOUSAND_MICROSECOND)) {
            pg_usleep(THOUSAND_MICROSECOND - totalTime);
        }
    }

    pg_atomic_fetch_sub_u32(&g_instance.smb_cxt.curSMBWriterIndex, 1);
    ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, false, true);
    MemoryContextDelete(ctx);
    ereport(LOG, (errmsg("SMB writer auxiliary end, thread id is %d.", t_thrd.smbWriterCxt.smbWriterAuxIdx)));
    proc_exit(0);
}

} // namespace smb_recovery