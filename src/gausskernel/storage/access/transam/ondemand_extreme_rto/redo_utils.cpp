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
#include "access/ondemand_extreme_rto/xlog_read.h"
#include "storage/lock/lwlock.h"

/*
 * Add xlog reader private structure for page read.
 */
typedef struct XLogPageReadPrivate {
    const char* datadir;
    TimeLineID tli;
} XLogPageReadPrivate;

Size OndemandRecoveryShmemSize(void)
{
    Size size = 0;

    size = add_size(size, (Size)g_instance.attr.attr_storage.dms_attr.ondemand_recovery_mem_size << BITS_IN_KB);

    return size;
}

void OndemandRecoveryShmemInit(void)
{
    bool found = false;
    t_thrd.storage_cxt.ondemandXLogMem =
        (char *)ShmemInitStruct("Ondemand Recovery HashMap", OndemandRecoveryShmemSize(), &found);

    if (!found) {
        /* The memory of the memset sometimes exceeds 2 GB. so, memset_s cannot be used. */
        MemSet(t_thrd.storage_cxt.ondemandXLogMem, 0, OndemandRecoveryShmemSize());
    }
}

void OndemandXlogFileIdCacheInit(void)
{
    HASHCTL ctl;

    /* hash accessed by database file id */
    errno_t rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "", "");
    ctl.keysize = sizeof(XLogFileId);
    ctl.entrysize = sizeof(XLogFileIdCacheEntry);
    ctl.hash = tag_hash;
    t_thrd.storage_cxt.ondemandXLogFileIdCache = hash_create("Ondemand extreme rto xlogfile handle cache", 8, &ctl,
        HASH_ELEM | HASH_FUNCTION | HASH_SHRCTX);
    if (!t_thrd.storage_cxt.ondemandXLogFileIdCache)
        ereport(FATAL, (errmsg("could not initialize ondemand xlogfile handle hash table")));
}

/* add for batch redo mem manager */
void *OndemandXLogMemCtlInit(RedoMemManager *memctl, Size itemsize, int itemnum)
{
    Size dataSize = (itemsize + sizeof(RedoMemSlot)) * itemnum;

    Assert(t_thrd.storage_cxt.ondemandXLogMem != NULL);
    Assert(dataSize <= OndemandRecoveryShmemSize());

    memctl->totalblknum = itemnum;
    memctl->usedblknum = 0;
    memctl->itemsize = itemsize;
    memctl->memslot = (RedoMemSlot *)(t_thrd.storage_cxt.ondemandXLogMem + (itemsize * itemnum));
    for (int i = memctl->totalblknum; i > 0; --i) {
        memctl->memslot[i - 1].buf_id = i; /*  start from 1 , 0 is invalidbuffer */
        memctl->memslot[i - 1].freeNext = i - 1;
    }
    memctl->firstfreeslot = memctl->totalblknum;
    memctl->firstreleaseslot = InvalidBuffer;
    return (void *)t_thrd.storage_cxt.ondemandXLogMem;
}

RedoMemSlot *OndemandXLogMemAlloc(RedoMemManager *memctl)
{
    RedoMemSlot *nextfreeslot = NULL;
    do {
        LWLockAcquire(OndemandXLogMemAllocLock, LW_EXCLUSIVE);
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
        LWLockRelease(OndemandXLogMemAllocLock);

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
    LWLockAcquire(OndemandXLogMemAllocLock, LW_EXCLUSIVE);
    Buffer oldFirst = AtomicReadBuffer(&memctl->firstreleaseslot);
    pg_memory_barrier();
    do {
        AtomicWriteBuffer(&bufferslot->freeNext, oldFirst);
    } while (!AtomicCompareExchangeBuffer(&memctl->firstreleaseslot, &oldFirst, bufferid));
    memctl->usedblknum--;
    LWLockRelease(OndemandXLogMemAllocLock);
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
    // do not free parsebuffers, which is managed in shared memory
    parsemanager->parsebuffers = NULL;
    parsemanager->memctl.isInit = false;
}

XLogRecParseState *OndemandXLogParseBufferAllocList(RedoParseManager *parsemanager, XLogRecParseState *blkstatehead,
    void *record)
{
    XLogRecParseState *recordstate = NULL;

    if (parsemanager == NULL) {
        recordstate = (XLogRecParseState*)palloc(sizeof(XLogRecParseState));
        errno_t rc = memset_s((void*)recordstate, sizeof(XLogRecParseState), 0, sizeof(XLogRecParseState));
        securec_check(rc, "\0", "\0");
        recordstate->manager = &(ondemand_extreme_rto::g_dispatcher->parseManager);
        recordstate->distributeStatus = XLOG_SKIP_DISTRIBUTE;
    } else {
        RedoMemManager *memctl = &(parsemanager->memctl);
        RedoMemSlot *allocslot = NULL;
        ParseBufferDesc *descstate = NULL;

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
        descstate = (ParseBufferDesc *)((char *)parsemanager->parsebuffers + memctl->itemsize * (allocslot->buf_id - 1));
        descstate->buff_id = allocslot->buf_id;
        Assert(descstate->state == 0);
        descstate->state = 1;
        descstate->refcount = 0;
        recordstate = (XLogRecParseState *)((char *)descstate + sizeof(ParseBufferDesc));
        recordstate->manager = parsemanager;
        recordstate->distributeStatus = XLOG_NO_DISTRIBUTE;

        if (parsemanager->refOperate != NULL) {
            parsemanager->refOperate->refCount(record);
        }
    }

    recordstate->nextrecord = NULL;
    recordstate->refrecord = record;
    recordstate->isFullSync = false;
    if (blkstatehead != NULL) {
        recordstate->nextrecord = blkstatehead->nextrecord;
        blkstatehead->nextrecord = (void *)recordstate;
    }

    return recordstate;
}

void OndemandXLogParseBufferRelease(XLogRecParseState *recordstate)
{
    if (recordstate->distributeStatus == XLOG_SKIP_DISTRIBUTE) {
        // alloc in pageRedoWorker or backends
        pfree(recordstate);
        return;
    }

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
    XLogRecParseState *reloadBlockState = NULL;
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
                // reload from disk, because RedoPageManager already release refrecord in on-demand build stage
                reloadBlockState = OndemandRedoReloadXLogRecord(redoBlockState);
                XlogBlockRedoForOndemandExtremeRTOQuery(reloadBlockState, &bufferInfo);
                OndemandRedoReleaseXLogRecord(reloadBlockState);
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

bool IsTargetBlockState(XLogRecParseState *targetblockstate, XLogRecParseState* curblockstate)
{
    if (memcmp(&targetblockstate->blockparse.blockhead, &curblockstate->blockparse.blockhead, sizeof(XLogBlockHead)) != 0) {
        return false;
    }
    return true;
}

// only used in ondemand redo stage
XLogRecParseState *OndemandRedoReloadXLogRecord(XLogRecParseState *redoblockstate)
{
    uint32 blockNum = 0;
    char *errormsg = NULL;
    XLogRecParseState *recordBlockState = NULL;
    XLogPageReadPrivate readPrivate = {
        .datadir = NULL,
        .tli = GetRecoveryTargetTLI()
    };

    XLogReaderState *xlogreader = XLogReaderAllocate(&SimpleXLogPageReadInFdCache, &readPrivate);  // do not use pre-read

    // step1: read record
    XLogRecord *record = XLogReadRecord(xlogreader, redoblockstate->blockparse.blockhead.start_ptr, &errormsg,
        true, g_instance.dms_cxt.SSRecoveryInfo.recovery_xlog_dir);
    if (record == NULL) {
        ereport(PANIC, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                        errmsg("[On-demand] reload xlog record failed at %X/%X, errormsg: %s",
                        (uint32)(redoblockstate->blockparse.blockhead.start_ptr >> 32),
                        (uint32)redoblockstate->blockparse.blockhead.start_ptr, errormsg)));
    }

    // step2: parse to block
    do {
        recordBlockState = XLogParseToBlockForExtermeRTO(xlogreader, &blockNum);
        if (recordBlockState != NULL) {
            break;
        }
        Assert(blockNum != 0);   // out of memory
    } while (true);

    // step3: find target parse state
    XLogRecParseState *nextState = recordBlockState;
    XLogRecParseState *targetState = NULL;
    do {
        XLogRecParseState *preState = nextState;
        nextState = (XLogRecParseState *)nextState->nextrecord;
        preState->nextrecord = NULL;

        if (IsTargetBlockState(preState, redoblockstate)) {
            targetState = preState;
        } else {
            OndemandXLogParseBufferRelease(preState);
        }
    } while (nextState != NULL);

    return targetState;
}

// only used in ondemand redo stage
void OndemandRedoReleaseXLogRecord(XLogRecParseState *reloadBlockState)
{
    XLogReaderFree((XLogReaderState*)reloadBlockState->refrecord);
    OndemandXLogParseBufferRelease(reloadBlockState);
}

void OnDemandSendRecoveryEndMarkToWorkersAndWaitForReach(int code)
{
    ondemand_extreme_rto::SendRecoveryEndMarkToWorkersAndWaitForReach(code);
}

void OnDemandWaitRedoFinish()
{
    ondemand_extreme_rto::WaitRedoFinish();
}
