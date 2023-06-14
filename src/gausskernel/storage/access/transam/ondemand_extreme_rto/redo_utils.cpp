/*
 * Copyright (c) 2023 Huawei Technologies Co.,Ltd.
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
 * redo_utils.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/transam/ondemand_extreme_rto/redo_utils.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "access/xlogproc.h"
#include "access/ondemand_extreme_rto/batch_redo.h"
#include "access/ondemand_extreme_rto/dispatcher.h"
#include "access/ondemand_extreme_rto/redo_utils.h"
#include "storage/lock/lwlock.h"

/* add for batch redo mem manager */
void *OndemandXLogMemCtlInit(RedoMemManager *memctl, Size itemsize, int itemnum)
{
    void *allocdata = NULL;
    RedoMemSlot *nextfreeslot = NULL;
    OndemandParseAllocCtrl *ctrl;
    Assert(PARSEBUFFER_SIZE == itemsize);

    allocdata = (void *)palloc(sizeof(OndemandParseAllocCtrl));
    ctrl = (OndemandParseAllocCtrl *)allocdata;
    ctrl->allocNum = itemnum / ONDEMAND_MAX_PARSEBUFF_PREPALLOC;
    if ((int)(ctrl->allocNum * ONDEMAND_MAX_PARSEBUFF_PREPALLOC) != itemnum) {
        ctrl->allocNum++;
    }
    ctrl->memslotEntry = (void *)palloc(sizeof(RedoMemSlot) * itemnum);

    // palloc all parse mem entry
    for (int i = 0; i < ctrl->allocNum; i++) {
        ctrl->allocEntry[i] = (void *)palloc(ONDEMAND_MAX_PARSESIZE_PREPALLOC);
        if (ctrl->allocEntry[i] == NULL) {
            ereport(PANIC,
                    (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                     errmsg("[SS] XLogMemCtlInit Allocated buffer failed!, totalblknum:%d, itemsize:%lu",
                     itemnum, itemsize)));
            /* panic */
        }
        errno_t rc = memset_s(ctrl->allocEntry[i], ONDEMAND_MAX_PARSESIZE_PREPALLOC, 0,
            ONDEMAND_MAX_PARSESIZE_PREPALLOC);
        securec_check(rc, "\0", "\0");
    }
    memctl->totalblknum = itemnum;
    memctl->usedblknum = 0;
    memctl->itemsize = itemsize;
    memctl->memslot = (RedoMemSlot *)ctrl->memslotEntry;
    nextfreeslot = memctl->memslot;
    for (int i = memctl->totalblknum; i > 0; --i) {
        memctl->memslot[i - 1].buf_id = i; /*  start from 1 , 0 is invalidbuffer */
        memctl->memslot[i - 1].freeNext = i - 1;
    }
    memctl->firstfreeslot = memctl->totalblknum;
    memctl->firstreleaseslot = InvalidBuffer;
    return allocdata;
}

RedoMemSlot *OndemandXLogMemAlloc(RedoMemManager *memctl)
{
    RedoMemSlot *nextfreeslot = NULL;
    do {
        LWLockAcquire(OndemandXlogMemAllocLock, LW_EXCLUSIVE);
        if (memctl->firstfreeslot == InvalidBuffer) {
            memctl->firstfreeslot = AtomicExchangeBuffer(&memctl->firstreleaseslot, InvalidBuffer);
            pg_read_barrier();
        }

        if (memctl->firstfreeslot != InvalidBuffer) {
            nextfreeslot = &(memctl->memslot[memctl->firstfreeslot - 1]);
            memctl->firstfreeslot = nextfreeslot->freeNext;
            memctl->usedblknum++;
            nextfreeslot->freeNext = InvalidBuffer;
        }
        LWLockRelease(OndemandXlogMemAllocLock);

        if (memctl->doInterrupt != NULL) {
            memctl->doInterrupt();
        }

    } while (nextfreeslot == NULL);

    return nextfreeslot;
}

void OndemandXLogMemRelease(RedoMemManager *memctl, Buffer bufferid)
{
    RedoMemSlot *bufferslot;
    if (!RedoMemIsValid(memctl, bufferid)) {
        ereport(PANIC, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                        errmsg("XLogMemRelease failed!, taoalblknum:%u, buf_id:%u", memctl->totalblknum, bufferid)));
        /* panic */
    }
    bufferslot = &(memctl->memslot[bufferid - 1]);
    Assert(bufferslot->freeNext == InvalidBuffer);
    LWLockAcquire(OndemandXlogMemAllocLock, LW_EXCLUSIVE);
    Buffer oldFirst = AtomicReadBuffer(&memctl->firstreleaseslot);
    pg_memory_barrier();
    do {
        AtomicWriteBuffer(&bufferslot->freeNext, oldFirst);
    } while (!AtomicCompareExchangeBuffer(&memctl->firstreleaseslot, &oldFirst, bufferid));
    memctl->usedblknum--;
    LWLockRelease(OndemandXlogMemAllocLock);
}


void OndemandXLogParseBufferInit(RedoParseManager *parsemanager, int buffernum, RefOperate *refOperate,
    InterruptFunc interruptOperte)
{
    void *allocdata = NULL;
    allocdata = OndemandXLogMemCtlInit(&(parsemanager->memctl), (sizeof(XLogRecParseState) + sizeof(ParseBufferDesc)), buffernum);
    parsemanager->parsebuffers = allocdata;
    parsemanager->refOperate = refOperate;
    parsemanager->memctl.doInterrupt = interruptOperte;
    parsemanager->memctl.isInit = true;

    g_parseManager = parsemanager;
    return;
}

void OndemandXLogParseBufferDestory(RedoParseManager *parsemanager)
{
    g_parseManager = NULL;
    OndemandParseAllocCtrl *ctrl = (OndemandParseAllocCtrl *)parsemanager->parsebuffers;

    if (ctrl != NULL) {
        for (int i = 0; i < ctrl->allocNum; i++) {
            pfree(ctrl->allocEntry[i]);
        }
        pfree(ctrl->memslotEntry);
        pfree(ctrl);
        parsemanager->parsebuffers = NULL;
    }
    parsemanager->memctl.isInit = false;
}

ParseBufferDesc *OndemandGetParseMemSlot(OndemandParseAllocCtrl *ctrl, int itemIndex)
{
    int entryIndex = itemIndex / ONDEMAND_MAX_PARSEBUFF_PREPALLOC;
    int entryOffset = (itemIndex - (entryIndex * ONDEMAND_MAX_PARSEBUFF_PREPALLOC)) * PARSEBUFFER_SIZE;
    return (ParseBufferDesc *)((char *)ctrl->allocEntry[entryIndex] + entryOffset);
}

XLogRecParseState *OndemandXLogParseBufferAllocList(RedoParseManager *parsemanager, XLogRecParseState *blkstatehead,
    void *record)
{
    RedoMemManager *memctl = &(parsemanager->memctl);
    RedoMemSlot *allocslot = NULL;
    ParseBufferDesc *descstate = NULL;
    XLogRecParseState *recordstate = NULL;

    allocslot = OndemandXLogMemAlloc(memctl);
    if (allocslot == NULL) {
        ereport(PANIC, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                          errmsg("XLogParseBufferAlloc Allocated buffer failed!, taoalblknum:%u, usedblknum:%u",
                                 memctl->totalblknum, memctl->usedblknum)));
        return NULL;
    }

    pg_read_barrier();
    Assert(allocslot->buf_id != InvalidBuffer);
    Assert(memctl->itemsize == (sizeof(XLogRecParseState) + sizeof(ParseBufferDesc)));
    descstate = OndemandGetParseMemSlot((OndemandParseAllocCtrl *)parsemanager->parsebuffers, allocslot->buf_id - 1);
    descstate->buff_id = allocslot->buf_id;
    Assert(descstate->state == 0);
    descstate->state = 1;
    descstate->refcount = 0;
    recordstate = (XLogRecParseState *)((char *)descstate + sizeof(ParseBufferDesc));
    recordstate->nextrecord = NULL;
    recordstate->manager = parsemanager;
    recordstate->refrecord = record;
    recordstate->isFullSync = false;
    recordstate->distributeStatus = XLOG_NO_DISTRIBUTE;
    if (blkstatehead != NULL) {
        recordstate->nextrecord = blkstatehead->nextrecord;
        blkstatehead->nextrecord = (void *)recordstate;
    }

    if (parsemanager->refOperate != NULL)
        parsemanager->refOperate->refCount(record);

    return recordstate;
}

void OndemandXLogParseBufferRelease(XLogRecParseState *recordstate)
{
    RedoMemManager *memctl = &(recordstate->manager->memctl);
    ParseBufferDesc *descstate = NULL;

    descstate = (ParseBufferDesc *)((char *)recordstate - sizeof(ParseBufferDesc));
    if (!RedoMemIsValid(memctl, descstate->buff_id) || descstate->state == 0) {
        ereport(PANIC, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                        errmsg("XLogParseBufferRelease failed!, taoalblknum:%u, buf_id:%u", memctl->totalblknum,
                               descstate->buff_id)));
        /* panic */
    }

    descstate->state = 0;

    OndemandXLogMemRelease(memctl, descstate->buff_id);
}

BufferDesc *RedoForOndemandExtremeRTOQuery(BufferDesc *bufHdr, char relpersistence,
    ForkNumber forkNum, BlockNumber blockNum, ReadBufferMode mode)
{
    bool hashFound = false;
    bool needMarkDirty = false;
    unsigned int new_hash;
    LWLock *xlog_partition_lock;
    Buffer buf = BufferDescriptorGetBuffer(bufHdr);
    ondemand_extreme_rto::RedoItemHashEntry *redoItemEntry = NULL;
    ondemand_extreme_rto::RedoItemTag redoItemTag;
    XLogRecParseState *procState = NULL;
    XLogBlockHead *procBlockHead = NULL;
    XLogBlockHead *blockHead = NULL;
    RedoBufferInfo bufferInfo;
    int rc;

    INIT_REDO_ITEM_TAG(redoItemTag, bufHdr->tag.rnode, forkNum, blockNum);

    uint32 id = ondemand_extreme_rto::GetSlotId(bufHdr->tag.rnode, 0, 0, ondemand_extreme_rto::GetBatchCount());
    HTAB *hashMap = g_instance.comm_cxt.predo_cxt.redoItemHash[id];
    if (hashMap == NULL) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                        errmsg("redo item hash table corrupted, there has invalid hashtable.")));
    }

    new_hash = ondemand_extreme_rto::XlogTrackTableHashCode(&redoItemTag);
    xlog_partition_lock = XlogTrackMappingPartitionLock(new_hash);
    (void)LWLockAcquire(xlog_partition_lock, LW_SHARED);
    redoItemEntry = (ondemand_extreme_rto::RedoItemHashEntry *)hash_search(hashMap, (void *)&redoItemTag, HASH_FIND, &hashFound);

    /* Page is already up-to-date, no need to replay. */
    if (!hashFound || redoItemEntry->redoItemNum == 0 || redoItemEntry->redoDone) {
        LWLockRelease(xlog_partition_lock);
        return bufHdr;
    }

    // switch to exclusive lock in replay
    LWLockRelease(xlog_partition_lock);
    (void)LWLockAcquire(xlog_partition_lock, LW_EXCLUSIVE);

    rc = memset_s(&bufferInfo, sizeof(bufferInfo), 0, sizeof(bufferInfo));
    securec_check(rc, "\0", "\0");
    if (BufferIsValid(buf)) {
        bufferInfo.buf = buf;
        bufferInfo.pageinfo.page = BufferGetPage(buf);
        bufferInfo.pageinfo.pagesize = BufferGetPageSize(buf);
    }

    procState = (XLogRecParseState *)redoItemEntry->head;
    procBlockHead = &procState->blockparse.blockhead;

    XLogBlockInitRedoBlockInfo(procBlockHead, &bufferInfo.blockinfo);

    Assert(mode == RBM_NORMAL || mode == RBM_ZERO_ON_ERROR);

    /* lock the share buffer for replaying the xlog */
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

    while (procState != NULL) {
        XLogRecParseState *redoBlockState = procState;
        ondemand_extreme_rto::ReferenceRecParseState(redoBlockState);

        procState = (XLogRecParseState *)procState->nextrecord;
        procBlockHead = &procState->blockparse.blockhead;

        blockHead = &redoBlockState->blockparse.blockhead;
        uint16 blockValid = XLogBlockHeadGetValidInfo(blockHead);

        if (XLogRecPtrIsInvalid(bufferInfo.lsn)) {
            bufferInfo.lsn = PageGetLSN(bufferInfo.pageinfo.page);
        }
        if (XLByteLE(XLogBlockHeadGetLSN(blockHead), PageGetLSN(bufferInfo.pageinfo.page))) {
            ondemand_extreme_rto::DereferenceRecParseState(redoBlockState);
            continue;
        }

        switch (blockValid) {
            case BLOCK_DATA_MAIN_DATA_TYPE:
            case BLOCK_DATA_UNDO_TYPE:
            case BLOCK_DATA_VM_TYPE:
            case BLOCK_DATA_FSM_TYPE:
                needMarkDirty = true;
                XlogBlockRedoForOndemandExtremeRTOQuery(redoBlockState, &bufferInfo);
                break;
            case BLOCK_DATA_XLOG_COMMON_TYPE:
            case BLOCK_DATA_DDL_TYPE:
            case BLOCK_DATA_DROP_DATABASE_TYPE:
            case BLOCK_DATA_NEWCU_TYPE:
            default:
                Assert(0);
                break;
        }

        ondemand_extreme_rto::DereferenceRecParseState(redoBlockState);
    }

    /* mark the latest buffer dirty */
    if (needMarkDirty) {
        MarkBufferDirty(buf);
    }

    LockBuffer(buf, BUFFER_LOCK_UNLOCK);

    redoItemEntry->redoDone = true;
    LWLockRelease(xlog_partition_lock);

    return bufHdr;
}

void OnDemandSendRecoveryEndMarkToWorkersAndWaitForReach(int code)
{
    ondemand_extreme_rto::SendRecoveryEndMarkToWorkersAndWaitForReach(code);
}

void OnDemandWaitRedoFinish()
{
    ondemand_extreme_rto::WaitRedoFinish();
}
