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
#include "access/ondemand_extreme_rto/page_redo.h"
#include "access/ondemand_extreme_rto/dispatcher.h"
#include "access/ondemand_extreme_rto/redo_utils.h"
#include "access/ondemand_extreme_rto/xlog_read.h"
#include "ddes/dms/ss_dms_bufmgr.h"
#include "storage/lock/lwlock.h"
#include "catalog/storage_xlog.h"


static void PrintXLogRecParseStateBlockHead(XLogRecParseState* blockState);

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
    ParseBufferDesc *descstate = NULL;

    Assert(t_thrd.storage_cxt.ondemandXLogMem != NULL);
    Assert(dataSize <= OndemandRecoveryShmemSize());

    memctl->totalblknum = itemnum;
    memctl->usedblknum = 0;
    memctl->itemsize = itemsize;
    memctl->memslot = (RedoMemSlot *)(t_thrd.storage_cxt.ondemandXLogMem + (itemsize * itemnum));
    for (int i = memctl->totalblknum; i > 0; --i) {
        memctl->memslot[i - 1].buf_id = i; /*  start from 1 , 0 is invalidbuffer */
        memctl->memslot[i - 1].freeNext = i - 1;
        // init parsebufferdesc because ondemandXLogMem may not memset 0
        descstate = (ParseBufferDesc *)(t_thrd.storage_cxt.ondemandXLogMem + itemsize * (i - 1));
        descstate->state = 0;
    }
    // only used firstreleaseslot of globalmemctl
    memctl->firstfreeslot = InvalidBuffer;
    memctl->firstreleaseslot = memctl->totalblknum;
    return (void *)t_thrd.storage_cxt.ondemandXLogMem;
}

static RedoMemSlot *OndemandGlobalXLogMemAlloc()
{
    RedoMemManager *glbmemctl = &ondemand_extreme_rto::g_dispatcher->parseManager.memctl;
    Buffer firstreleasebuffer = AtomicReadBuffer(&glbmemctl->firstreleaseslot);
    while (firstreleasebuffer != InvalidBuffer) {
        RedoMemSlot *firstreleaseslot = &glbmemctl->memslot[firstreleasebuffer - 1];
        Buffer nextreleasebuffer = firstreleaseslot->freeNext;
        if (AtomicCompareExchangeBuffer(&glbmemctl->firstreleaseslot, &firstreleasebuffer, nextreleasebuffer)) {
            firstreleaseslot->freeNext = InvalidBuffer;
            return firstreleaseslot;
        }
        firstreleasebuffer = AtomicReadBuffer(&glbmemctl->firstreleaseslot);
    }
    return NULL;
}

static RedoMemSlot *GetTailSlot(RedoMemManager *memctl, Buffer headSlotBuffer)
{
    RedoMemSlot *tailSlot = &memctl->memslot[headSlotBuffer - 1];
    while (tailSlot->freeNext != InvalidBuffer) {
        tailSlot = &memctl->memslot[tailSlot->freeNext - 1];
    }
    return tailSlot;
}

static void InsertBatchXLogMemToSlot(RedoMemManager *memctl, Buffer *targetSlot, Buffer headSlotBuffer)
{
    RedoMemSlot *tailSlot = GetTailSlot(memctl, headSlotBuffer);
    Buffer oldFirst = AtomicReadBuffer(targetSlot);
    pg_memory_barrier();
    do {
        AtomicWriteBuffer(&tailSlot->freeNext, oldFirst);
    } while (!AtomicCompareExchangeBuffer(targetSlot, &oldFirst, headSlotBuffer));
}

// used in hashmap manager, global firstreleaseslot should not be InvalidBuffer in healthy condition
void OndemandGlobalXLogMemReleaseIfNeed(RedoMemManager *memctl)
{
    RedoMemManager *glbmemctl = &ondemand_extreme_rto::g_dispatcher->parseManager.memctl;
    if (AtomicReadBuffer(&glbmemctl->firstreleaseslot) == InvalidBuffer) {
        Buffer firstReleaseSlotBuffer = AtomicExchangeBuffer(&memctl->firstreleaseslot, InvalidBuffer);
        if (firstReleaseSlotBuffer == InvalidBuffer) {
            // set pipeline firstfreeslot to pipeline firstreleaseslot, for next loop to global firstreleaseslot
            Buffer firstFreeSlotBuffer = AtomicExchangeBuffer(&memctl->firstfreeslot, InvalidBuffer);
            if (firstFreeSlotBuffer != InvalidBuffer) {
                InsertBatchXLogMemToSlot(memctl, &memctl->firstreleaseslot, firstFreeSlotBuffer);
            }
        } else {
            // set pipeline firstreleaseslot for global firstreleaseslot
            Buffer invalidBuffer = InvalidBuffer;
            if (!AtomicCompareExchangeBuffer(&glbmemctl->firstreleaseslot, &invalidBuffer, firstReleaseSlotBuffer)) {
                // exchange failed, give back
                InsertBatchXLogMemToSlot(memctl, &memctl->firstreleaseslot, firstReleaseSlotBuffer);
            }
        }
    }
}

RedoMemSlot *OndemandXLogMemAlloc(RedoMemManager *memctl)
{
    RedoMemSlot *nextFreeSlot = NULL;
    do {
        if (AtomicReadBuffer(&memctl->firstfreeslot) == InvalidBuffer) {
            AtomicWriteBuffer(&memctl->firstfreeslot, AtomicExchangeBuffer(&memctl->firstreleaseslot, InvalidBuffer));
            pg_read_barrier();
        }

        Buffer firstFreeSlotBuffer = AtomicExchangeBuffer(&memctl->firstfreeslot, InvalidBuffer);
        if (firstFreeSlotBuffer != InvalidBuffer) {
            nextFreeSlot = &(memctl->memslot[firstFreeSlotBuffer - 1]);
            AtomicWriteBuffer(&memctl->firstfreeslot, nextFreeSlot->freeNext);
            nextFreeSlot->freeNext = InvalidBuffer;
        }

        if (nextFreeSlot == NULL) {
            nextFreeSlot = OndemandGlobalXLogMemAlloc();
        }

        if (memctl->doInterrupt != NULL) {
            memctl->doInterrupt();
        }
    } while (nextFreeSlot == NULL);

    pg_atomic_fetch_add_u32(&memctl->usedblknum, 1);
    return nextFreeSlot;
}

void OndemandXLogMemRelease(RedoMemManager *memctl, Buffer bufferid)
{
    RedoMemSlot *bufferslot;
    RedoMemManager *releasememctl = memctl;
    if (!RedoMemIsValid(memctl, bufferid)) {
        ereport(PANIC, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                        errmsg("XLogMemRelease failed!, totalblknum:%u, buf_id:%u", memctl->totalblknum, bufferid)));
    }
    bufferslot = &(memctl->memslot[bufferid - 1]);
    Assert(bufferslot->freeNext == InvalidBuffer);
    // release to global firstreleaseslot directly if hashmap full
    if (unlikely(SS_ONDEMAND_RECOVERY_HASHMAP_FULL)) {
        releasememctl = &ondemand_extreme_rto::g_dispatcher->parseManager.memctl;
    }
    Buffer oldFirst = AtomicReadBuffer(&releasememctl->firstreleaseslot);
    pg_memory_barrier();
    do {
        AtomicWriteBuffer(&bufferslot->freeNext, oldFirst);
    } while (!AtomicCompareExchangeBuffer(&releasememctl->firstreleaseslot, &oldFirst, bufferid));
    pg_atomic_fetch_sub_u32(&memctl->usedblknum, 1);
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
                            errmsg("XLogParseBufferAlloc Allocated buffer failed!, totalblknum:%u, usedblknum:%u",
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
    }

    if (recordstate->manager->refOperate != NULL) {
        recordstate->manager->refOperate->refCount(record);
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
        Assert(!SS_ONDEMAND_REALTIME_BUILD_NORMAL);
        // alloc in pageRedoWorker or backends
        pfree(recordstate);
        return;
    }

    RedoMemManager *memctl = &(recordstate->manager->memctl);
    ParseBufferDesc *descstate = NULL;

    descstate = (ParseBufferDesc *)((char *)recordstate - sizeof(ParseBufferDesc));
    if (!RedoMemIsValid(memctl, descstate->buff_id) || descstate->state == 0) {
        ereport(PANIC, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                        errmsg("XLogParseBufferRelease failed!, totalblknum:%u, buf_id:%u", memctl->totalblknum,
                               descstate->buff_id)));
        /* panic */
    }

    descstate->state = 0;

    OndemandXLogMemRelease(memctl, descstate->buff_id);
}

/**
 * @brief scanning the hashmap by relationTag, to find out all the
 * redoItem of target relation and read the buffer of each redoItem.
 * The blockStates will be redone at RedoForOndemandExtremeRTOQuery if need.
 * 
 * @param relation the relation need to be redone
 * @return the redoEntry num of target relation
 */
long RedoRelationForOndemandExtremeRTO(Relation relation) {
    long entryNum = 0;
    Assert(RelationIsValid(relation));

    RelFileNode relfilenode = relation->rd_node;
    ondemand_extreme_rto::RedoItemTag relationTag;
    INIT_REDO_ITEM_TAG(relationTag, relfilenode, 0, 0);
    uint32 slotId = ondemand_extreme_rto::GetSlotId(relationTag.rNode, 0, 0, ondemand_extreme_rto::GetBatchCount());
    ondemand_extreme_rto::RedoItemHashEntry *redoItemEntry = NULL;
    HASH_SEQ_STATUS status;

    HTAB *hashMap = g_instance.comm_cxt.predo_cxt.redoItemHashCtrl[slotId]->hTab;
    if (hashMap == NULL) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                        errmsg("redo item hash table corrupted, there has invalid hashtable.")));
    }

    hash_seq_init(&status, hashMap);
    LWLock* scanningXLogTrackLock = XLogTrackMappingScanningLock(slotId);
    LWLockAcquire(scanningXLogTrackLock, LW_SHARED);

    while ((redoItemEntry = (ondemand_extreme_rto::RedoItemHashEntry *)hash_seq_search(&status)) != NULL) {
        ondemand_extreme_rto::RedoItemTag redoItemTag = redoItemEntry->redoItemTag;
        // Check if this redoItemEntry belong to the target relation.
        if (!RelFileNodeRelEquals(redoItemTag.rNode, relationTag.rNode)) {
            continue;
        }

        Buffer buf = ReadBufferExtended(relation, redoItemTag.forkNum, redoItemTag.blockNum, RBM_NORMAL, NULL);
        
        ReleaseBuffer(buf);
        entryNum++;
    }
    LWLockRelease(scanningXLogTrackLock);
    return entryNum;
}

/**
 * @brief scanning all the hashmap of each pipline, to find out all the
 * redoItem of target database and read the buffer of each redoItem.
 * The blockState will be redone at RedoForOndemandExtremeRTOQuery if need.
 * 
 * @param dbId the dbNode of database
 * @return the redoEntry num of target database
 */
long RedoDatabaseForOndemandExtremeRTO(Oid dbNode)
{
    long entryNum = 0;
    Assert(OidIsValid(dbNode));

    // Search the hashmap of each piplines.
    uint32 batchCount = ondemand_extreme_rto::GetBatchCount();
    ondemand_extreme_rto::RedoItemHashEntry *redoItemEntry = NULL;
    for (uint32 slotId = 0; slotId < batchCount; slotId++) {
        HASH_SEQ_STATUS status;
        HTAB *hashMap = g_instance.comm_cxt.predo_cxt.redoItemHashCtrl[slotId]->hTab;
        if (hashMap == NULL) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                        errmsg("redo item hash table corrupted, there has invalid hashtable.")));
        }

        hash_seq_init(&status, hashMap);
        LWLock* scanningXLogTrackLock = XLogTrackMappingScanningLock(slotId);
        LWLockAcquire(scanningXLogTrackLock, LW_SHARED);

        while ((redoItemEntry = (ondemand_extreme_rto::RedoItemHashEntry *)hash_seq_search(&status)) != NULL) {
            ondemand_extreme_rto::RedoItemTag redoItemTag = redoItemEntry->redoItemTag;
            // Check if this redoItemEntry belong to the target namespace.
            if (redoItemEntry->redoDone || redoItemTag.rNode.dbNode != dbNode) {
                continue;
            }
            Buffer buffer = ReadBufferWithoutRelcache(redoItemTag.rNode, redoItemTag.forkNum, redoItemTag.blockNum, RBM_NORMAL, NULL, NULL);
            ReleaseBuffer(buffer);
            entryNum++;
        }
        LWLockRelease(scanningXLogTrackLock);
    }
    return entryNum;
}

BufferDesc *RedoForOndemandExtremeRTOQuery(BufferDesc *bufHdr, char relpersistence,
    ForkNumber forkNum, BlockNumber blockNum, ReadBufferMode mode)
{
    dms_buf_ctrl_t *buf_ctrl = GetDmsBufCtrl(bufHdr->buf_id);

    if (buf_ctrl->state & BUF_ONDEMAND_REDO_DONE) {
        return bufHdr;
    }

    bool needMarkDirty = false;
    LWLock *xlog_partition_lock = NULL;
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

    if (checkBlockRedoDoneFromHashMapAndLock(&xlog_partition_lock, redoItemTag, &redoItemEntry, false)) {
        buf_ctrl->state |= BUF_ONDEMAND_REDO_DONE;
        ereport(DEBUG1, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
            errmsg("RedoForOndemandExtremeRTOQuery, block redo done or need to redo: spc/db/rel/bucket fork-block: %u/%u/%u/%d %d-%u.",
                redoItemTag.rNode.spcNode, redoItemTag.rNode.dbNode, redoItemTag.rNode.relNode, redoItemTag.rNode.bucketNode,
                redoItemTag.forkNum, redoItemTag.blockNum)));
        return bufHdr;
    }

    Assert(xlog_partition_lock != NULL);

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
    buf_ctrl->state |= BUF_ONDEMAND_REDO_DONE;

    ereport(DEBUG1, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
        errmsg("RedoForOndemandExtremeRTOQuery, block redo done: spc/db/rel/bucket fork-block: %u/%u/%u/%d %d-%u.",
                redoItemTag.rNode.spcNode, redoItemTag.rNode.dbNode, redoItemTag.rNode.relNode, redoItemTag.rNode.bucketNode,
                redoItemTag.forkNum, redoItemTag.blockNum)));

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

XLogRecParseType GetCurrentXLogRecParseType(XLogRecParseState *preState)
{
    XLogRecParseType type;
    switch (preState->blockparse.blockhead.block_valid) {
        case BLOCK_DATA_MAIN_DATA_TYPE:
        case BLOCK_DATA_UNDO_TYPE:
        case BLOCK_DATA_VM_TYPE:
        case BLOCK_DATA_FSM_TYPE:
            type = PARSE_TYPE_DATA;
            break;
        case BLOCK_DATA_SEG_EXTEND:
        case BLOCK_DATA_SEG_FILE_EXTEND_TYPE:
            type = PARSE_TYPE_SEG;
            break;
        case BLOCK_DATA_SEG_FULL_SYNC_TYPE:
            {
                uint8 recordType = XLogBlockHeadGetInfo(&preState->blockparse.blockhead) & ~XLR_INFO_MASK;
                if (unlikely((recordType == XLOG_SEG_CREATE_EXTENT_GROUP) || (recordType == XLOG_SEG_NEW_PAGE))) {
                    type = PARSE_TYPE_DDL;
                } else {
                    type = PARSE_TYPE_SEG;
                }
                break;
            }
            
        default:
            type = PARSE_TYPE_DDL;
            break;
    }

    return type;
}

bool IsRecParseStateHaveChildState(XLogRecParseState *checkState)
{
    if (GetCurrentXLogRecParseType(checkState) == PARSE_TYPE_SEG) {
        uint8 info = XLogBlockHeadGetInfo(&checkState->blockparse.blockhead) & ~XLR_INFO_MASK;
        if ((info == XLOG_SEG_ATOMIC_OPERATION) || (info == XLOG_SEG_SEGMENT_EXTEND) ||
            (info == XLOG_SEG_INIT_MAPPAGE) || (info == XLOG_SEG_INIT_INVRSPTR_PAGE) ||
            (info == XLOG_SEG_ADD_NEW_GROUP)) {
            return true;
        }
    }
    return false;
}

/**
 * Find out target blockState from checkState and its nextrecords, by checking if any blockState
 * has the same blockhead with the target blockState, and release others. Used in ondemand-recovery redo phase.
 */
static XLogRecParseState *OndemandFindTargetBlockStateInOndemandRedo(XLogRecParseState *checkState,
    XLogRecParseState *srcState)
{
    Assert(!IsRecParseStateHaveChildState(checkState));
    XLogRecParseState *nextState = checkState;
    XLogRecParseState *targetState = NULL;
    do {
        XLogRecParseState *preState = nextState;
        nextState = (XLogRecParseState *)nextState->nextrecord;
        preState->nextrecord = NULL;

        if (IsTargetBlockState(preState, srcState)) {
            targetState = preState;
        } else {
            OndemandXLogParseBufferRelease(preState);
        }
    } while (nextState != NULL);

    return targetState;
}

/**
 * @brief Reload redoItem according to XLogRecParseState, the redoItem has been released before
 * push into hashmap, so reload it before redo.
 * 
 * @param hashmapBlockState the blockState got from hashmap
 * @return XLogRecParseState* blockState reload from disk
 */
XLogRecParseState *OndemandRedoReloadXLogRecord(XLogRecParseState *hashmapBlockState)
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
    XLogRecord *record = XLogReadRecord(xlogreader, hashmapBlockState->blockparse.blockhead.start_ptr, &errormsg,
        true, g_instance.dms_cxt.SSRecoveryInfo.recovery_xlog_dir);
    if (record == NULL) {
        ereport(PANIC, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                        errmsg("[On-demand] reload xlog record failed at %X/%X, spc/db/rel/bucket "
                        "fork-block: %u/%u/%u/%d %d-%u, errormsg: %s",
                        (uint32)(hashmapBlockState->blockparse.blockhead.start_ptr >> 32),
                        (uint32)hashmapBlockState->blockparse.blockhead.start_ptr,
                        hashmapBlockState->blockparse.blockhead.spcNode, hashmapBlockState->blockparse.blockhead.dbNode,
                        hashmapBlockState->blockparse.blockhead.relNode,
                        hashmapBlockState->blockparse.blockhead.bucketNode,
                        hashmapBlockState->blockparse.blockhead.forknum, hashmapBlockState->blockparse.blockhead.blkno,
                        errormsg)));
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
    XLogRecParseState *targetState = OndemandFindTargetBlockStateInOndemandRedo(recordBlockState, hashmapBlockState);
    if (targetState == NULL) {
        PrintXLogRecParseStateBlockHead(hashmapBlockState);
        ereport(PANIC, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                        errmsg("[On-demand] reload xlog record failed at %X/%X, spc/db/rel/bucket "
                        "fork-block: %u/%u/%u/%d %d-%u, errormsg: can not find target block-record",
                        (uint32)(recordBlockState->blockparse.blockhead.start_ptr >> 32),
                        (uint32)recordBlockState->blockparse.blockhead.start_ptr,
                        recordBlockState->blockparse.blockhead.spcNode, recordBlockState->blockparse.blockhead.dbNode,
                        recordBlockState->blockparse.blockhead.relNode,
                        recordBlockState->blockparse.blockhead.bucketNode,
                        recordBlockState->blockparse.blockhead.forknum, recordBlockState->blockparse.blockhead.blkno)));
    }
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

/**
 * Reform partner shutdown real-time build when failover,
 * it will wait until Startup Thread shutdown.
 */
void OnDemandWaitRealtimeBuildShutDownInPartnerFailover()
{
    if (g_instance.pid_cxt.StartupPID != 0) {
        Assert(SS_ONDEMAND_REALTIME_BUILD_NORMAL && SS_STANDBY_MODE);
        OnDemandWaitRealtimeBuildShutDown();
        ereport(LOG, (errmsg("[SS reform] Partner node shutdown real-time build when failover.")));
    }
}

void OnDemandWaitRealtimeBuildShutDown() 
{
    ondemand_extreme_rto::WaitRealtimeBuildShutdown();
}

// only used in ondemand realtime build, for update xlog redo loc in failover
void OnDemandUpdateRealtimeBuildPrunePtr()
{
    XLogRecPtr primaryRedoLsn = t_thrd.shemem_ptr_cxt.ControlFile->checkPointCopy.redo;
#ifdef USE_ASSERT_CHECKING
    if (XLByteLT(primaryRedoLsn, ondemand_extreme_rto::g_dispatcher->ckptRedoPtr)) {
        ereport(PANIC, (errmodule(MOD_DMS), errmsg("[SS][On-demand] redo loc %X/%X in primary node %d is less than "
            "realtime build node %d, prune loc %X/%X", (uint32)(primaryRedoLsn >> 32), (uint32)primaryRedoLsn,
            SS_PRIMARY_ID, SS_MY_INST_ID, (uint32)(ondemand_extreme_rto::g_dispatcher->ckptRedoPtr >> 32),
            (uint32)ondemand_extreme_rto::g_dispatcher->ckptRedoPtr)));
    }
#endif
    ondemand_extreme_rto::UpdateCheckpointRedoPtrForPrune(primaryRedoLsn);
}

void OnDemandBackupControlFile(ControlFileData* controlFile) {
    ondemand_extreme_rto::BackupControlFileForRealtimeBuild(controlFile);
}

XLogRecPtr GetRedoLocInCheckpointRecord(XLogReaderState *record)
{
    CheckPoint checkPoint;
    CheckPointUndo checkPointUndo;
    errno_t rc;

    Assert(IsCheckPoint(record));

    if (XLogRecGetDataLen(record) >= sizeof(checkPoint) && XLogRecGetDataLen(record) < sizeof(checkPointUndo)) {
        rc = memcpy_s(&checkPoint, sizeof(CheckPoint), XLogRecGetData(record), sizeof(CheckPoint));
        securec_check(rc, "", "");
    } else if (XLogRecGetDataLen(record) >= sizeof(checkPointUndo)) {
        rc = memcpy_s(&checkPointUndo, sizeof(CheckPointUndo), XLogRecGetData(record), sizeof(CheckPointUndo));
        securec_check(rc, "", "");
        checkPoint = checkPointUndo.ori_checkpoint;
    }
    return checkPoint.redo;
}

void OnDemandNotifyHashMapPruneIfNeed()
{
    if (SS_ONDEMAND_RECOVERY_HASHMAP_FULL) {
        ondemand_extreme_rto::StartupSendMarkToBatchRedo(&ondemand_extreme_rto::g_hashmapPruneMark);
    }
}

static void PrintXLogRecParseStateBlockHead(XLogRecParseState* blockState) {
    StringInfoData res;
    initStringInfo(&res);
    appendStringInfo(&res, "{start_ptr: %X/%X, ",  (uint32)(blockState->blockparse.blockhead.start_ptr>> 32),
        (uint32)blockState->blockparse.blockhead.start_ptr);
    appendStringInfo(&res, "end_ptr: %X/%X, ", (uint32)(blockState->blockparse.blockhead.end_ptr>> 32),
        (uint32)blockState->blockparse.blockhead.end_ptr);
    appendStringInfo(&res, "blkno: %u, ", blockState->blockparse.blockhead.blkno);
    appendStringInfo(&res, "relNode: %u, ", blockState->blockparse.blockhead.relNode);
    appendStringInfo(&res, "block_valid: %u, ", (uint32)(blockState->blockparse.blockhead.block_valid));
    appendStringInfo(&res, "xl_info: %u, ", (uint32)(blockState->blockparse.blockhead.xl_info));
    appendStringInfo(&res, "block_valid: %u, ", (uint32)(blockState->blockparse.blockhead.xl_info));
    appendStringInfo(&res, "xl_rmid: %u, ", (uint32)(blockState->blockparse.blockhead.xl_rmid));
    appendStringInfo(&res, "forknum: %d, ", blockState->blockparse.blockhead.forknum);
    appendStringInfo(&res, "xl_xid: %lu, ", blockState->blockparse.blockhead.xl_xid);
    appendStringInfo(&res, "spcNode: %u, ", (uint32)(blockState->blockparse.blockhead.spcNode));
    appendStringInfo(&res, "dbNode: %u, ", (uint32)(blockState->blockparse.blockhead.dbNode));
    appendStringInfo(&res, "bucketNode: %d, ", (int)(blockState->blockparse.blockhead.bucketNode));
    appendStringInfo(&res, "opt: %u, ", (uint32)(blockState->blockparse.blockhead.opt));
    appendStringInfo(&res, "is_conflict_type: %u, ", (uint32)(blockState->blockparse.blockhead.is_conflict_type));
    appendStringInfo(&res, "hasCSN: %u}; ", (uint32)(blockState->blockparse.blockhead.hasCSN));
    ereport(LOG,
        (errmsg("[On-demand][redo] blockState->blockparse.blockhead: %s.",
        res.data)));
}