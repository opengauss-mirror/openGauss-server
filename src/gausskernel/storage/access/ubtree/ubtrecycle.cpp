/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * ubtrecycle.cpp
 *    Recycle logic for UBtree.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/ubtree/ubtrecycle.cpp
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "access/nbtree.h"
#include "access/ubtree.h"
#include "commands/tablespace.h"
#include "storage/lmgr.h"
#include "storage/procarray.h"
#include "utils/aiomem.h"
#include "utils/builtins.h"

static uint32 BlockGetMaxItems(BlockNumber blkno);
static void UBTreeInitRecycleQueuePage(Relation rel, Page page, Size size, BlockNumber blkno);
static void UBTreeRecycleQueueDiscardPage(Relation rel, UBTRecycleQueueAddress addr);
static void UBTreeRecycleQueueAddPage(Relation rel, UBTRecycleForkNumber forkNumber,
    BlockNumber blkno, TransactionId xid);
static Buffer StepNextPage(Relation rel, Buffer buf);
static Buffer GetAvailablePageOnPage(Relation rel, UBTRecycleForkNumber forkNumber, Buffer buf,
    TransactionId waterLevelXid, UBTRecycleQueueAddress *addr, bool *continueScan);
static Buffer MoveToEndpointPage(Relation rel, Buffer buf, bool needHead, int access);
static uint16 PageAllocateItem(Buffer buf);
static void RecycleQueueLinkNewPage(Relation rel, Buffer leftBuf, Buffer newBuf);
static bool QueuePageIsEmpty(Buffer buf);
static Buffer AcquireNextAvailableQueuePage(Relation rel, Buffer buf, UBTRecycleForkNumber forkNumber);
static void InsertOnRecycleQueuePage(Relation rel, Buffer buf, uint16 offset, BlockNumber blkno, TransactionId xid);
static void RemoveOneItemFromPage(Relation rel, Buffer buf, uint16 offset);

const BlockNumber FirstBlockNumber = 0;
const BlockNumber FirstNormalBlockNumber = 2;      /* 0 and 1 are pages which include meta data */
const uint16 FirstNormalOffset = 0;
const uint16 OtherBlockOffset = ((uint16)0) - 2;   /* indicate that previous or next item is in other block */

#define IsMetaPage(blkno) (blkno < FirstNormalBlockNumber)
#define IsNormalOffset(offset) (offset < OtherBlockOffset)

static uint32 BlockGetMaxItems(BlockNumber blkno)
{
    uint32 freeSpace = BLCKSZ - sizeof(PageHeaderData) - offsetof(UBTRecycleQueueHeaderData, items);
    if (IsMetaPage(blkno)) {
        freeSpace -= sizeof(UBTRecycleMetaData);
    }
    return freeSpace / sizeof(UBTRecycleQueueItemData);
}

UBTRecycleQueueHeader GetRecycleQueueHeader(Page page, BlockNumber blkno)
{
    if (!IsMetaPage(blkno)) {
        return (UBTRecycleQueueHeader)PageGetContents(page);
    }
    return (UBTRecycleQueueHeader)(((char*)PageGetContents(page)) + sizeof(UBTRecycleMetaData));
}

static UBTRecycleQueueItem HeaderGetItem(UBTRecycleQueueHeader header, uint16 offset)
{
    if (offset >= 0 && offset <= UBTRecycleMaxItems) {
        return &header->items[offset];
    }
    ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
            errmsg("trying to fetch invalid recycle queue item with offset %u", offset),
            errhint("Can fixed by REINDEX this index")));
    pg_unreachable(); /* won't reach here */
}

static void UBTreeInitRecycleQueuePage(Relation rel, Page page, Size size, BlockNumber blkno)
{
    PageInit(page, size, 0);

    /* init header */
    UBTRecycleQueueHeader header = GetRecycleQueueHeader(page, blkno);
    header->flags = 0;
    header->prevBlkno = InvalidBlockNumber;
    header->nextBlkno = InvalidBlockNumber;
    header->head = InvalidOffset;
    header->tail = InvalidOffset;
    header->freeItems = BlockGetMaxItems(blkno);
    header->freeListHead = InvalidOffset;

    /* init meta if needed */
    if (IsMetaPage(blkno)) {
        header->flags = (URQ_HEAD_PAGE | URQ_TAIL_PAGE);

        UBTRecycleMeta meta = (UBTRecycleMeta)PageGetContents(page);
        meta->flags = 0;
        meta->headBlkno = blkno;
        meta->tailBlkno = blkno;
        /* setup nblocks */
        if (rel != NULL) {
            RelationOpenSmgr(rel);
            meta->nblocksLower = smgrnblocks(rel->rd_smgr, MAIN_FORKNUM);
            meta->nblocksUpper = meta->nblocksLower;
        } else {
            meta->nblocksLower = 0; /* may update later */
            meta->nblocksUpper = 0; /* may update later */
        }
    }
}

static bool RecycleQueueInitialized(Relation rel)
{
    /* open smgr, might have to re-open if a cache flush happened */
    RelationOpenSmgr(rel);
    BlockNumber nblocks = rel->rd_smgr->smgr_fsm_nblocks;
    if (nblocks == InvalidBlockNumber || nblocks < minRecycleQueueBlockNumber) {
        if (smgrexists(rel->rd_smgr, FSM_FORKNUM)) {
            rel->rd_smgr->smgr_fsm_nblocks = smgrnblocks(rel->rd_smgr, FSM_FORKNUM);
            nblocks = rel->rd_smgr->smgr_fsm_nblocks;
        } else {
            rel->rd_smgr->smgr_fsm_nblocks = 0;
            nblocks = 0;
        }
    }
    return nblocks >= minRecycleQueueBlockNumber;
}

/* init a new page with given prev and next blkno */
void UBTreeRecycleQueueInitPage(Relation rel, Page page, BlockNumber blkno, BlockNumber prevBlkno,
    BlockNumber nextBlkno)
{
    PageInit(page, BLCKSZ, 0);
    UBTreeInitRecycleQueuePage(rel, page, BLCKSZ, blkno);
    UBTRecycleQueueHeader header = GetRecycleQueueHeader(page, blkno);
    header->prevBlkno = prevBlkno;
    header->nextBlkno = nextBlkno;
}

/* record the chain changes in prev or next page */
void UBtreeRecycleQueueChangeChain(Buffer buf, BlockNumber newBlkno, bool setNext)
{
    UBTRecycleQueueHeader header = GetRecycleQueueHeader(BufferGetPage(buf), BufferGetBlockNumber(buf));
    if (setNext) {
        header->nextBlkno = newBlkno;
    } else {
        header->prevBlkno = newBlkno;
    }
}

static void LogInitRecycleQueuePage(Relation rel, Buffer buf, Buffer leftBuf, Buffer rightBuf)
{
    xl_ubtree2_recycle_queue_init_page xlrec;
    Page page = BufferGetPage(buf);
    UBTRecycleQueueHeader header = GetRecycleQueueHeader(page, BufferGetBlockNumber(buf));

    xlrec.insertingNewPage = false;
    xlrec.prevBlkno = header->prevBlkno;
    xlrec.currBlkno = BufferGetBlockNumber(buf);
    xlrec.nextBlkno = header->nextBlkno;

    if (BufferIsValid(leftBuf)) {
        xlrec.insertingNewPage = true;
        Page leftPage = BufferGetPage(leftBuf);
        Page rightPage = BufferGetPage(rightBuf);

        XLogBeginInsert();
        XLogRegisterData((char *)&xlrec, SizeOfUBTree2RecycleQueueInitPage);
        XLogRegisterBuffer(UBTREE2_RECYCLE_QUEUE_INIT_PAGE_CURR_BLOCK_NUM, buf, REGBUF_WILL_INIT);
        XLogRegisterBuffer(UBTREE2_RECYCLE_QUEUE_INIT_PAGE_LEFT_BLOCK_NUM, leftBuf, 0);
        XLogRegisterBuffer(UBTREE2_RECYCLE_QUEUE_INIT_PAGE_RIGHT_BLOCK_NUM, rightBuf, 0);

        XLogRecPtr recptr = XLogInsert(RM_UBTREE2_ID, XLOG_UBTREE2_RECYCLE_QUEUE_INIT_PAGE);

        PageSetLSN(page, recptr);
        PageSetLSN(leftPage, recptr);
        PageSetLSN(rightPage, recptr);
    } else {
        XLogBeginInsert();
        XLogRegisterData((char *)&xlrec, SizeOfUBTree2RecycleQueueInitPage);
        XLogRegisterBuffer(0, buf, REGBUF_WILL_INIT);

        XLogRecPtr recptr = XLogInsert(RM_UBTREE2_ID, XLOG_UBTREE2_RECYCLE_QUEUE_INIT_PAGE);

        PageSetLSN(page, recptr);
    }
}

/* generate prev and next blkno for this page, and init it */
static void InitRecycleQueueInitialPage(Relation rel, Buffer buf)
{
    BlockNumber blkno = BufferGetBlockNumber(buf);
    Page page = BufferGetPage(buf);
    /*
     * page 0 2 4 form a circle (Potential Empty Page Queue).
     * page 1 3 5 form another circle (Available Page Queue).
     */
    const int adjBLockNumberDiff = 2;
    BlockNumber prevBlkno = (blkno + minRecycleQueueBlockNumber - adjBLockNumberDiff) % minRecycleQueueBlockNumber;
    BlockNumber nextBlkno = (blkno + adjBLockNumberDiff) % minRecycleQueueBlockNumber;

    /* Do the update.  No ereport(ERROR) until changes are logged */
    START_CRIT_SECTION();

    UBTreeRecycleQueueInitPage(rel, page, blkno, prevBlkno, nextBlkno);

    MarkBufferDirty(buf);

    /* xlog stuff */
    if (RelationNeedsWAL(rel)) {
        /* XLOG stuff, and set LSN as well */
        LogInitRecycleQueuePage(rel, buf, InvalidBuffer, InvalidBuffer);
    }

    END_CRIT_SECTION();
}

Buffer ReadRecycleQueueBuffer(Relation rel, BlockNumber blkno)
{
    Buffer buf = ReadBufferExtended(rel, FSM_FORKNUM, blkno, RBM_NORMAL, NULL);
    /* initial pages may not initialized correctly, before return it we need to check */
    if (blkno != P_NEW && blkno < minRecycleQueueBlockNumber && PageIsNew(BufferGetPage(buf))) {
        LockBuffer(buf, BT_WRITE);
        InitRecycleQueueInitialPage(rel, buf);
        LockBuffer(buf, BUFFER_LOCK_UNLOCK);
    }
    return buf;
}

void UBTreeInitializeRecycleQueue(Relation rel)
{
    LockRelationForExtension(rel, ExclusiveLock);
    if (RecycleQueueInitialized(rel)) {
        /* initialized by other thread, skip */
        UnlockRelationForExtension(rel, ExclusiveLock);
        return;
    }

    RelationOpenSmgr(rel);
    /* create the urq fork if not exists */
    if ((rel->rd_smgr->smgr_fsm_nblocks == InvalidBlockNumber || rel->rd_smgr->smgr_fsm_nblocks == 0) &&
        !smgrexists(rel->rd_smgr, FSM_FORKNUM)) {
        smgrcreate(rel->rd_smgr, FSM_FORKNUM, t_thrd.xlog_cxt.InRecovery);
    }
    BlockNumber nblocksNow = smgrnblocks(rel->rd_smgr, FSM_FORKNUM);
    Assert(nblocksNow <= minRecycleQueueBlockNumber);

    /* first step, check existing pages */
    for (BlockNumber blkno = 0; blkno < nblocksNow; blkno++) {
        Buffer buf = ReadRecycleQueueBuffer(rel, blkno);
        LockBuffer(buf, BT_WRITE);
        Page page = BufferGetPage(buf);
        if (PageIsNew(page) || PageGetLSN(page) == InvalidXLogRecPtr) {
            /* page is not initialized correctly, re-init it */
            InitRecycleQueueInitialPage(rel, buf);
        }
        UnlockReleaseBuffer(buf);
    }

    /* second step, create necessary pages */
    for (BlockNumber blkno = nblocksNow; blkno < minRecycleQueueBlockNumber; blkno++) {
        Buffer buf = ReadRecycleQueueBuffer(rel, P_NEW);
        LockBuffer(buf, BT_WRITE);
        /* check that the blkno is what we expected */
        if (BufferGetBlockNumber(buf) != blkno) {
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("corrupted index recycle queue of index \"%s\": expected blkno is %u, actual blkno is %u",
                           RelationGetRelationName(rel), blkno, BufferGetBlockNumber(buf))));
        }
        InitRecycleQueueInitialPage(rel, buf);
        UnlockReleaseBuffer(buf);
    }

    UnlockRelationForExtension(rel, ExclusiveLock);
}

static bool UBTreeTryRecycleEmptyPageInternal(Relation rel)
{
    UBTRecycleQueueAddress addr;

    Buffer buf = UBTreeGetAvailablePage(rel, RECYCLE_EMPTY_FORK, &addr);
    if (!BufferIsValid(buf)) {
        return false; /* no available page to recycle */
    }
    Page page = BufferGetPage(buf);
    UBTPageOpaqueInternal opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
    if (P_ISDELETED(opaque)) {
        /* deleted by other routine earlier, skip */
        _bt_relbuf(rel, buf);
        UBTreeRecycleQueueDiscardPage(rel, addr);
        return true;
    }

    WHITEBOX_TEST_STUB("UBTreeTryRecycleEmptyPageInternal-prune-page", WhiteboxDefaultErrorEmit);

    if (UBTreePagePruneOpt(rel, buf, true)) {
        /* successfully deleted, move to freed page queue */
        UBTreeRecycleQueueAddPage(rel, RECYCLE_FREED_FORK, addr.indexBlkno, ReadNewTransactionId());
    }
    /* whether the page can be deleted or not, discard from the queue */
    UBTreeRecycleQueueDiscardPage(rel, addr);
    return true;
}

void UBTreeTryRecycleEmptyPage(Relation rel)
{
    bool firstTrySucceed = UBTreeTryRecycleEmptyPageInternal(rel);
    if (firstTrySucceed) {
        /* try to recycle the second page */
        UBTreeTryRecycleEmptyPageInternal(rel);
    }
}

void UBTreeRecordFreePage(Relation rel, BlockNumber blkno, TransactionId xid)
{
    UBTreeRecycleQueueAddPage(rel, RECYCLE_FREED_FORK, blkno, xid);
}

void UBTreeRecordEmptyPage(Relation rel, BlockNumber blkno, TransactionId xid)
{
    UBTreeRecycleQueueAddPage(rel, RECYCLE_EMPTY_FORK, blkno, xid);
}

void UBTreeRecordUsedPage(Relation rel, UBTRecycleQueueAddress addr)
{
    if (addr.queueBuf != InvalidBuffer) {
        UBTreeRecycleQueueDiscardPage(rel, addr);
    }
}

static Buffer StepNextPage(Relation rel, Buffer buf)
{
    Page page = BufferGetPage(buf);
    UBTRecycleQueueHeader header = GetRecycleQueueHeader(page, BufferGetBlockNumber(buf));

    BlockNumber nextBlkno = header->nextBlkno;
    Buffer nextBuf = ReadRecycleQueueBuffer(rel, nextBlkno);

    UnlockReleaseBuffer(buf);
    LockBuffer(nextBuf, BT_WRITE);

    return nextBuf;
}

static Buffer GetAvailablePageOnPage(Relation rel, UBTRecycleForkNumber forkNumber, Buffer buf,
    TransactionId WaterLevelXid, UBTRecycleQueueAddress *addr, bool *continueScan)
{
    Page page = BufferGetPage(buf);
    UBTRecycleQueueHeader header = GetRecycleQueueHeader(page, BufferGetBlockNumber(buf));

    uint16 curOffset = header->head;
    while (IsNormalOffset(curOffset)) {
        UBTRecycleQueueItem item = HeaderGetItem(header, curOffset);
        if (TransactionIdFollowsOrEquals(item->xid, WaterLevelXid)) {
            *continueScan = false;
            return InvalidBuffer;
        }
        if (!BlockNumberIsValid(item->blkno)) {
            curOffset = item->next;
            continue;
        }
        Buffer targetBuf = ReadBuffer(rel, item->blkno);
        _bt_checkbuffer_valid(rel, targetBuf);
        if (ConditionalLockBuffer(targetBuf)) {
            _bt_checkpage(rel, targetBuf);
            bool pageUsable = true;
            if (forkNumber == RECYCLE_FREED_FORK) {
                pageUsable = UBTreePageRecyclable(BufferGetPage(targetBuf));
            } else if (forkNumber == RECYCLE_EMPTY_FORK) {
                /* make sure that it's not half-dead or the deletion is not reserved yet */
                Page indexPage = BufferGetPage(targetBuf);
                UBTPageOpaque opaque = (UBTPageOpaque)PageGetSpecialPointer(indexPage);
                if (P_ISHALFDEAD((UBTPageOpaqueInternal)opaque)) {
                    TransactionId previousXact = opaque->xact;
                    if (TransactionIdIsValid(previousXact) && TransactionIdIsInProgress(previousXact, NULL, true)) {
                        pageUsable = false;
                    }
                }
            }
            if (pageUsable) {
                WHITEBOX_TEST_STUB("GetAvailablePageOnPage-got", WhiteboxDefaultErrorEmit);

                *continueScan = false;
                addr->queueBuf = buf;
                addr->indexBlkno = item->blkno;
                addr->offset = curOffset;
                return targetBuf;
            }
            UBTPageOpaqueInternal opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(BufferGetPage(targetBuf));
            if (!P_ISDELETED(opaque)) {
                /* this page is reused by others, we help to mark it as unusable */
                item->blkno = InvalidBlockNumber;
                MarkBufferDirtyHint(buf, false);
            }
            _bt_relbuf(rel, targetBuf);
        } else {
            ReleaseBuffer(targetBuf);
        }
        curOffset = item->next;
    }
    *continueScan = (curOffset == OtherBlockOffset);
    return InvalidBuffer;
}

Buffer UBTreeGetAvailablePage(Relation rel, UBTRecycleForkNumber forkNumber, UBTRecycleQueueAddress *addr)
{
    TransactionId oldestXmin = u_sess->utils_cxt.RecentGlobalDataXmin;

    Buffer queueBuf = RecycleQueueGetEndpointPage(rel, forkNumber, true, BT_READ);

    Buffer indexBuf = InvalidBuffer;
    bool continueScan = false;
    for (;;) {
        indexBuf = GetAvailablePageOnPage(rel, forkNumber, queueBuf, oldestXmin, addr, &continueScan);
        if (!continueScan) {
            break;
        }
        queueBuf = StepNextPage(rel, queueBuf);
    }
    LockBuffer(queueBuf, BUFFER_LOCK_UNLOCK);

    if (indexBuf != InvalidBuffer) {
        return indexBuf;
    }

    /* no useful page, but we will check non-tracked page if we are finding free pages */
    ReleaseBuffer(queueBuf);
    addr->queueBuf = InvalidBuffer;

    if (forkNumber == RECYCLE_EMPTY_FORK) {
        return InvalidBuffer;
    }

    /* no available page found, but we can check new created pages */
    BlockNumber nblocks = RelationGetNumberOfBlocks(rel);
    bool metaChanged = false;

    const BlockNumber metaBlockNumber = forkNumber;
    Buffer metaBuf = ReadRecycleQueueBuffer(rel, metaBlockNumber);
    LockBuffer(metaBuf, BT_READ);
    UBTRecycleMeta metaData = (UBTRecycleMeta)PageGetContents(BufferGetPage(metaBuf));
    for (BlockNumber curBlkno = metaData->nblocksUpper; curBlkno < nblocks; curBlkno++) {
        if (metaData->nblocksUpper > curBlkno) {
            continue;
        }
        if (metaData->nblocksUpper <= curBlkno) {
            metaData->nblocksUpper = curBlkno + 1; /* update nblocks in meta */
            metaChanged = true;
        }
        indexBuf = ReadBuffer(rel, curBlkno);
        if (ConditionalLockBuffer(indexBuf)) {
            if (PageIsNew(BufferGetPage(indexBuf))) {
                break;
            }
            LockBuffer(indexBuf, BUFFER_LOCK_UNLOCK);
        }
        ReleaseBuffer(indexBuf);
        indexBuf = InvalidBuffer;
    }

    if (metaChanged) {
        MarkBufferDirtyHint(metaBuf, false);
    }
    UnlockReleaseBuffer(metaBuf);

    addr->queueBuf = InvalidBuffer; /* it's not allocated from any queue page */
    return indexBuf;
}

void UBTreeRecycleQueuePageChangeEndpointLeftPage(Buffer buf, bool isHead)
{
    uint32 endpointFlag = (isHead ? URQ_HEAD_PAGE : URQ_TAIL_PAGE);
    UBTRecycleQueueHeader header = GetRecycleQueueHeader(BufferGetPage(buf), BufferGetBlockNumber(buf));
    if (isHead) {
        header->head = InvalidOffset;
        header->tail = InvalidOffset;
    } else {
        Assert(IsNormalOffset(header->tail));
        UBTRecycleQueueItem tailItem = HeaderGetItem(header, header->tail);
        tailItem->next = OtherBlockOffset;
    }
    header->flags &= ~endpointFlag;
}

void UBTreeRecycleQueuePageChangeEndpointRightPage(Buffer buf, bool isHead)
{
    uint32 endpointFlag = (isHead ? URQ_HEAD_PAGE : URQ_TAIL_PAGE);
    UBTRecycleQueueHeader header = GetRecycleQueueHeader(BufferGetPage(buf), BufferGetBlockNumber(buf));
    if (isHead) {
        if (IsNormalOffset(header->head)) {
            UBTRecycleQueueItem headItem = HeaderGetItem(header, header->head);
            headItem->prev = InvalidOffset;
        } else {
            header->head = InvalidOffset;
        }
    } else {
        /* new created tail page must be empty */
        Assert(header->head == InvalidOffset);
    }
    header->flags |= endpointFlag;
}

static void RecycleQueueChangeEndpoint(Relation rel, Buffer buf, Buffer nextBuf, bool isHead)
{
    Page page = BufferGetPage(buf);
    Page nextPage = BufferGetPage(nextBuf);

    WHITEBOX_TEST_STUB("RecycleQueueChangeEndpoint", WhiteboxDefaultErrorEmit);

    /* Do the update.  No ereport(ERROR) until changes are logged */
    START_CRIT_SECTION();

    UBTreeRecycleQueuePageChangeEndpointLeftPage(buf, isHead);
    UBTreeRecycleQueuePageChangeEndpointRightPage(nextBuf, isHead);

    MarkBufferDirty(buf);
    MarkBufferDirty(nextBuf);

    /* xlog stuff */
    if (RelationNeedsWAL(rel)) {
        xl_ubtree2_recycle_queue_endpoint xlrec;
        xlrec.isHead = isHead;
        xlrec.leftBlkno = BufferGetBlockNumber(buf);
        xlrec.rightBlkno = BufferGetBlockNumber(nextBuf);

        XLogBeginInsert();
        XLogRegisterData((char *)&xlrec, SizeOfUBTree2RecycleQueueEndpoint);
        XLogRegisterBuffer(0, buf, 0);
        XLogRegisterBuffer(1, nextBuf, 0);

        XLogRecPtr recptr = XLogInsert(RM_UBTREE2_ID, XLOG_UBTREE2_RECYCLE_QUEUE_ENDPOINT);

        PageSetLSN(page, recptr);
        PageSetLSN(nextPage, recptr);
    }

    END_CRIT_SECTION();
}

static Buffer MoveToEndpointPage(Relation rel, Buffer buf, bool needHead, int access)
{
restart:
    Page page = BufferGetPage(buf);
    UBTRecycleQueueHeader header = GetRecycleQueueHeader(page, BufferGetBlockNumber(buf));

    uint32 endpointFlag = (needHead ? URQ_HEAD_PAGE : URQ_TAIL_PAGE);
    while ((header->flags & endpointFlag) == 0) {
        buf = StepNextPage(rel, buf);
        page = BufferGetPage(buf);
        header = GetRecycleQueueHeader(page, BufferGetBlockNumber(buf));
    }

    if (IsNormalOffset(header->head)) {
        UBTRecycleQueueItem headItem = HeaderGetItem(header, header->head);
        if (!BlockNumberIsValid(headItem->blkno)) {
            if (access == BT_READ) {
                /* we need to switch the head, drop read lock and acquire write lock */
                LockBuffer(buf, BUFFER_LOCK_UNLOCK);
                access = BT_WRITE;
                LockBuffer(buf, BT_WRITE);
                goto restart;
            }
            RemoveOneItemFromPage(rel, buf, header->head);
        }
        header = GetRecycleQueueHeader(page, BufferGetBlockNumber(buf));
    }

    if (needHead && !IsNormalOffset(header->head) && (header->flags & URQ_TAIL_PAGE) == 0) {
        /* it's Head page, but header->head is not normal and it not the Tail. Now we have to switch the Head */
        if (access == BT_READ) {
            /* we need to switch the head, drop read lock and acquire write lock */
            LockBuffer(buf, BUFFER_LOCK_UNLOCK);
            access = BT_WRITE;
            LockBuffer(buf, BT_WRITE);
            goto restart;
        }
        /* by here, buf is write locked */
        Buffer nextBuf = ReadRecycleQueueBuffer(rel, header->nextBlkno);
        LockBuffer(nextBuf, BT_WRITE);
        /* change endpoint, and insert xlog */
        RecycleQueueChangeEndpoint(rel, buf, nextBuf, true);
        /* release current page, and return the next page */
        UnlockReleaseBuffer(buf);
        buf = nextBuf;
    }

    return buf;
}

static uint16 PageAllocateItem(Buffer buf)
{
    Page page = BufferGetPage(buf);
    UBTRecycleQueueHeader header = GetRecycleQueueHeader(page, BufferGetBlockNumber(buf));
    if (header->freeItems > 0) {
        /* allocate from freeItems */
        return header->freeItems - 1;
    }
    if (header->freeListHead != InvalidOffset) {
        /* allocate from freeList */
        return header->freeListHead;
    }
    return InvalidOffset;
}

static Buffer RecycleQueueExtend(Relation rel)
{
    LockRelationForExtension(rel, ExclusiveLock);
    Buffer buf = ReadRecycleQueueBuffer(rel, P_NEW);
    LockBuffer(buf, BT_WRITE);
    UnlockRelationForExtension(rel, ExclusiveLock);
    return buf;
}

static void RecycleQueueLinkNewPage(Relation rel, Buffer leftBuf, Buffer newBuf)
{
    /* new page already allocated, link it into the list */
    BlockNumber leftBlkno = BufferGetBlockNumber(leftBuf);
    Page leftPage = BufferGetPage(leftBuf);
    UBTRecycleQueueHeader leftHeader = GetRecycleQueueHeader(leftPage, leftBlkno);

    WHITEBOX_TEST_STUB("RecycleQueueLinkNewPage", WhiteboxDefaultErrorEmit);

    BlockNumber blkno = BufferGetBlockNumber(newBuf);
    Page page = BufferGetPage(newBuf);
    UBTRecycleQueueHeader header = GetRecycleQueueHeader(page, blkno);

    BlockNumber rightBlkno = leftHeader->nextBlkno;
    Buffer rightBuf = ReadRecycleQueueBuffer(rel, rightBlkno);
    LockBuffer(rightBuf, BT_WRITE);
    Page rightPage = BufferGetPage(rightBuf);
    UBTRecycleQueueHeader rightHeader = GetRecycleQueueHeader(rightPage, rightBlkno);

    /* Do the update.  No ereport(ERROR) until changes are logged */
    START_CRIT_SECTION();

    UBTreeInitRecycleQueuePage(rel, page, BLCKSZ, BufferGetBlockNumber(newBuf));

    leftHeader->nextBlkno = blkno;
    header->prevBlkno = leftBlkno;
    header->nextBlkno = rightBlkno;
    rightHeader->prevBlkno = blkno;

    MarkBufferDirty(leftBuf);
    MarkBufferDirty(newBuf);
    MarkBufferDirty(rightBuf);

    /* xlog stuff */
    if (RelationNeedsWAL(rel)) {
        /* XLOG stuff, and set LSN as well */
        LogInitRecycleQueuePage(rel, newBuf, leftBuf, rightBuf);
    }

    END_CRIT_SECTION();

    UnlockReleaseBuffer(rightBuf);
}

static bool QueuePageIsEmpty(Buffer buf)
{
    UBTRecycleQueueHeader header = GetRecycleQueueHeader(BufferGetPage(buf), BufferGetBlockNumber(buf));
    return (header->flags & URQ_HEAD_PAGE) == 0 &&
           (header->flags & URQ_TAIL_PAGE) == 0 &&
           !IsNormalOffset(header->head);
}

/* Acquire a page to be the new tail block */
static Buffer AcquireNextAvailableQueuePage(Relation rel, Buffer buf, UBTRecycleForkNumber forkNumber)
{
    Page page = BufferGetPage(buf);
    BlockNumber blkno = BufferGetBlockNumber(buf);
    UBTRecycleQueueHeader header = GetRecycleQueueHeader(page, blkno);

    Buffer targetBuf = ReadRecycleQueueBuffer(rel, header->nextBlkno);
    LockBuffer(targetBuf, BT_WRITE);
    if (QueuePageIsEmpty(targetBuf)) {
        /* next page is empty, we can reuse it */
        WHITEBOX_TEST_STUB("RemoveOneItemFromPage-reuse", WhiteboxDefaultErrorEmit);

        RecycleQueueChangeEndpoint(rel, buf, targetBuf, false);
        UnlockReleaseBuffer(buf);
        return targetBuf;
    }
    /* next block is still in use, we need to get a new page */
    UnlockReleaseBuffer(targetBuf);
    /* drop write lock of the tail page before acquire relation extend lock */
    LockBuffer(buf, BUFFER_LOCK_UNLOCK);
    Buffer newBuf = RecycleQueueExtend(rel);
    /* relock and move to the current tail */
    LockBuffer(buf, BT_WRITE);
    buf = MoveToEndpointPage(rel, buf, false, BT_WRITE);
    RecycleQueueLinkNewPage(rel, buf, newBuf);
    if (PageAllocateItem(buf) != InvalidOffset) {
        /* there is still space left in the current tail page, no need to switch tail */
        UnlockReleaseBuffer(newBuf);
        return buf;
    }
    /* by here, Exclusive lock of newBuf is acquired */
    RecycleQueueChangeEndpoint(rel, buf, newBuf, false);
    UnlockReleaseBuffer(buf);
    return newBuf;
}

static void TryFixMetaData(Buffer metaBuf, int32 oldval, int32 newval, bool isHead)
{
    UBTRecycleMeta metaData = (UBTRecycleMeta)PageGetContents(BufferGetPage(metaBuf));
    int32 *addr = (isHead ? (int32 *)&(metaData->headBlkno) : (int32 *)&(metaData->tailBlkno));
    if (gs_compare_and_swap_32(addr, oldval, newval)) {
        /* update succeed, mark buffer dirty */
        if (ConditionalLockBuffer(metaBuf)) {
            MarkBufferDirty(metaBuf);
            LockBuffer(metaBuf, BUFFER_LOCK_UNLOCK);
        }
    }
}

Buffer RecycleQueueGetEndpointPage(Relation rel, UBTRecycleForkNumber forkNumber, bool needHead, int access)
{
    if (!RecycleQueueInitialized(rel)) {
        UBTreeInitializeRecycleQueue(rel);
    }
    const BlockNumber metaBlockNumber = forkNumber;
    Buffer metaBuf = ReadRecycleQueueBuffer(rel, metaBlockNumber);
    /* read out tail block number without lock */
    UBTRecycleMeta metaData = (UBTRecycleMeta)PageGetContents(BufferGetPage(metaBuf));
    BlockNumber givenBlkno = (needHead ? metaData->headBlkno : metaData->tailBlkno);

    /* get the advised block, and move to the true endpoint if necessary */
    Buffer buf = ReadRecycleQueueBuffer(rel, givenBlkno);
    LockBuffer(buf, access);
    buf = MoveToEndpointPage(rel, buf, needHead, access);

    /* try to fix the information in the meta if necessary */
    BlockNumber trueBlkno = BufferGetBlockNumber(buf);
    if (trueBlkno != givenBlkno) {
        TryFixMetaData(metaBuf, givenBlkno, trueBlkno, needHead);
    }
    ReleaseBuffer(metaBuf);

    /* okay, we got the target page */
    return buf;
}

/* we insert into the tail page, ensure that the items in the page are arranged by XID incrementally */
static void UBTreeRecycleQueueAddPage(Relation rel, UBTRecycleForkNumber forkNumber,
    BlockNumber blkno, TransactionId xid)
{
    /* get the tail page */
    Buffer buf = RecycleQueueGetEndpointPage(rel, forkNumber, false, BT_WRITE);

    uint16 offset = PageAllocateItem(buf);
    if (offset == InvalidOffset) {
        /* tail page is full, allocate in next page */
        buf = AcquireNextAvailableQueuePage(rel, buf, forkNumber);
        offset = PageAllocateItem(buf); /* new page must be empty yet */
    }

    Assert(offset != InvalidOffset);
    InsertOnRecycleQueuePage(rel, buf, offset, blkno, xid);
}

static void LogModifyPage(Buffer buf, bool isInsert, uint16 offset, UBTRecycleQueueItem item,
    UBTRecycleQueueHeader header, uint16 freeListOffset)
{
    Page page = BufferGetPage(buf);
    xl_ubtree2_recycle_queue_modify xlrec;
    xlrec.isInsert = isInsert;
    xlrec.offset = offset;
    xlrec.blkno = BufferGetBlockNumber(buf);
    xlrec.item = *item;
    xlrec.header = *header;
    xlrec.header.freeListHead = freeListOffset;

    XLogBeginInsert();
    XLogRegisterData((char *)&xlrec, SizeOfUBTree2RecycleQueueModify);
    XLogRegisterBuffer(0, buf, 0);

    XLogRecPtr recptr = XLogInsert(RM_UBTREE2_ID, XLOG_UBTREE2_RECYCLE_QUEUE_MODIFY);

    PageSetLSN(page, recptr);
}

static void InsertOnRecycleQueuePage(Relation rel, Buffer buf, uint16 offset, BlockNumber blkno, TransactionId xid)
{
    Page page = BufferGetPage(buf);
    UBTRecycleQueueHeader header = GetRecycleQueueHeader(page, BufferGetBlockNumber(buf));

    UBTRecycleQueueItem item = HeaderGetItem(header, offset);

    WHITEBOX_TEST_STUB("InsertOnRecycleQueuePage", WhiteboxDefaultErrorEmit);

    /* Do the update.  No ereport(ERROR) until changes are logged */
    START_CRIT_SECTION();

    item->blkno = blkno;
    item->xid = xid;
    if (header->freeListHead == offset) {
        /* allocated from freeList */
        header->freeListHead = item->next;
    }
    if (header->freeItems == offset + 1) {
        /* allocated from freeItems */
        header->freeItems--;
    }

    if (!IsNormalOffset(header->head)) {
        /* empty page, previous item of a new empty page is in other page */
        item->prev = header->head; /* InvalidOffset or OtherBlockOffset */
        item->next = InvalidOffset;
        header->head = offset;
        header->tail = offset;
    } else {
        UBTRecycleQueueItem headItem = HeaderGetItem(header, header->head);
        if (xid <= headItem->xid) {
            /* insert in the front */
            item->prev = headItem->prev;
            item->next = header->head; /* old head offset */
            headItem->prev = offset;
            header->head = offset;
        } else {
            uint16 curOffset = header->tail;
            UBTRecycleQueueItem curItem = HeaderGetItem(header, curOffset);
            /* find the insert loc */
            while (xid < curItem->xid) {
                curOffset = curItem->prev;
                curItem = HeaderGetItem(header, curOffset);
            }
            item->prev = curOffset;
            item->next = curItem->next;
            if (IsNormalOffset(curItem->next)) {
                UBTRecycleQueueItem nextItem = HeaderGetItem(header, curItem->next);
                nextItem->prev = offset;
            } else {
                /* curItem is tail, update the tail of this page */
                header->tail = offset;
            }
            curItem->next = offset;
        }
    }

    MarkBufferDirty(buf);

    /* XLOG stuff */
    if (RelationNeedsWAL(rel)) {
        LogModifyPage(buf, true, offset, item, header, header->freeListHead);
    }

    END_CRIT_SECTION();

    UnlockReleaseBuffer(buf);
}

void UBTreeXlogRecycleQueueModifyPage(Buffer buf, xl_ubtree2_recycle_queue_modify *xlrec)
{
    Page page = BufferGetPage(buf);
    BlockNumber blkno = BufferGetBlockNumber(buf);
    /* restore header and the modified item */
    UBTRecycleQueueHeader header = GetRecycleQueueHeader(page, blkno);
    *header = xlrec->header;
    UBTRecycleQueueItem item = HeaderGetItem(header, xlrec->offset);
    *item = xlrec->item;

    if (xlrec->isInsert) {
        /* restore adjacent items */
        if (IsNormalOffset(item->prev)) {
            UBTRecycleQueueItem prevItem = HeaderGetItem(header, item->prev);
            prevItem->next = xlrec->offset;
        }
        if (IsNormalOffset(item->next)) {
            UBTRecycleQueueItem nextItem = HeaderGetItem(header, item->next);
            nextItem->prev = xlrec->offset;
        }
    } else {
        /* restore adjacent items */
        if (IsNormalOffset(item->prev)) {
            UBTRecycleQueueItem prevItem = HeaderGetItem(header, item->prev);
            prevItem->next = item->next;
        }
        if (IsNormalOffset(item->next)) {
            UBTRecycleQueueItem nextItem = HeaderGetItem(header, item->next);
            nextItem->prev = item->prev;
        }
        item->blkno = InvalidBlockNumber;
        item->xid = InvalidTransactionId;
        item->prev = InvalidOffset;
        item->next = header->freeListHead;
        header->freeListHead = xlrec->offset;
    }
}

static void RemoveOneItemFromPage(Relation rel, Buffer buf, uint16 offset)
{
    Page page = BufferGetPage(buf);
    UBTRecycleQueueHeader header = GetRecycleQueueHeader(page, BufferGetBlockNumber(buf));
    UBTRecycleQueueItem item = HeaderGetItem(header, offset);
    UBTRecycleQueueItemData oldItem = *item;

    WHITEBOX_TEST_STUB("RemoveOneItemFromPage", WhiteboxDefaultErrorEmit);

    /* Do the update.  No ereport(ERROR) until changes are logged */
    START_CRIT_SECTION();

    if (header->head == offset) {
        header->head = item->next;
    }
    if (header->tail == offset) {
        header->tail = item->prev;
    }
    if (IsNormalOffset(item->prev)) {
        UBTRecycleQueueItem prevItem = HeaderGetItem(header, item->prev);
        prevItem->next = item->next;
    }
    if (IsNormalOffset(item->next)) {
        UBTRecycleQueueItem nextItem = HeaderGetItem(header, item->next);
        nextItem->prev = item->prev;
    }

    item->blkno = InvalidBlockNumber;
    item->xid = InvalidTransactionId;
    item->prev = InvalidOffset;
    item->next = header->freeListHead;
    header->freeListHead = offset;

    MarkBufferDirty(buf);

    /* XLOG stuff */
    if (RelationNeedsWAL(rel)) {
        /* we record the old freeListHead, new freeListHead is known as xlrec.offset */
        LogModifyPage(buf, false, offset, &oldItem, header, item->next);
    }

    END_CRIT_SECTION();

    if (!(IsNormalOffset(header->head))) {
        /* deleting the only item on this page */
        if ((header->flags & URQ_HEAD_PAGE) != 0 && (header->flags & URQ_TAIL_PAGE) == 0) {
            /* it's head page, and it's not tail page, we need to change the head page flag */
            Buffer nextBuf = ReadRecycleQueueBuffer(rel, header->nextBlkno);
            LockBuffer(nextBuf, BT_WRITE);
            /* change endpoint, and insert xlog */
            RecycleQueueChangeEndpoint(rel, buf, nextBuf, true);
            UnlockReleaseBuffer(nextBuf);
        }
    }
}

static void UBTreeRecycleQueueDiscardPage(Relation rel, UBTRecycleQueueAddress addr)
{
    Buffer buf = addr.queueBuf;
    LockBuffer(buf, BT_WRITE);

    Page page = BufferGetPage(buf);
    UBTRecycleQueueHeader header = GetRecycleQueueHeader(page, BufferGetBlockNumber(buf));
    UBTRecycleQueueItem item = HeaderGetItem(header, addr.offset);
    if (item->blkno != addr.indexBlkno && BlockNumberIsValid(item->blkno)) {
        /* already discarded, skip */
        UnlockReleaseBuffer(buf);
        return;
    }
    if (!BlockNumberIsValid(item->blkno) && !TransactionIdIsValid(item->xid)) {
        /* already discarded, skip */
        UnlockReleaseBuffer(buf);
        return;
    }

    /* try remove invalid item in left side */
    for (;;) {
        UBTRecycleQueueItem curItem = HeaderGetItem(header, addr.offset);
        bool removed = false;
        if (IsNormalOffset(curItem->prev)) {
            UBTRecycleQueueItem prevItem = HeaderGetItem(header, curItem->prev);
            if (!BlockNumberIsValid(prevItem->blkno)) {
                RemoveOneItemFromPage(rel, buf, curItem->prev);
                removed = true;
            }
        }
        if (!removed) {
            break;
        }
    }
    /* try remove invalid item in right side */
    for (;;) {
        UBTRecycleQueueItem curItem = HeaderGetItem(header, addr.offset);
        bool removed = false;
        if (IsNormalOffset(curItem->next)) {
            UBTRecycleQueueItem nextItem = HeaderGetItem(header, curItem->next);
            if (!BlockNumberIsValid(nextItem->blkno)) {
                RemoveOneItemFromPage(rel, buf, curItem->next);
                removed = true;
            }
        }
        if (!removed) {
            break;
        }
    }
    /* remove self */
    RemoveOneItemFromPage(rel, buf, addr.offset);

    UnlockReleaseBuffer(buf);
}

static void UBTRecyleQueueRecordOutput(BlockNumber blkno, uint16 offset, UBTRecycleQueueItem item,
    TupleDesc *tupleDesc, Tuplestorestate *tupstore, uint32 cols)
{
    if (item == NULL) {
        return;
    }

    Assert(cols == UBTREE_RECYCLE_OUTPUT_PARAM_CNT);
    bool nulls[cols] = {false};
    Datum values[cols];
    char xidStr[UBTREE_RECYCLE_OUTPUT_XID_STR_LEN] = {'\0'};
    errno_t ret = snprintf_s(xidStr, sizeof(xidStr), sizeof(xidStr) - 1, "%lu", item->xid);
    securec_check_ss(ret, "\0", "\0");
    values[ARR_0] = ObjectIdGetDatum((Oid) blkno);
    values[ARR_1] = ObjectIdGetDatum((Oid) offset);
    values[ARR_2] = CStringGetTextDatum(xidStr);
    values[ARR_3] = ObjectIdGetDatum((Oid) item->blkno);
    values[ARR_4] = ObjectIdGetDatum((Oid) item->prev);
    values[ARR_5] = ObjectIdGetDatum((Oid) item->next);
    tuplestore_putvalues(tupstore, *tupleDesc, values, nulls);
}
/*
 * call UBTreeVerifyRecordError() only when recordEachItem is false.
 *
 * recordEachItem:
 *      true - dump this page
 *      false - verfiy this page
 */
uint32 UBTreeRecycleQueuePageDump(Relation rel, Buffer buf, bool recordEachItem,
    TupleDesc *tupleDesc, Tuplestorestate *tupstore, uint32 cols)
{
    uint32 errVerified = 0;
    TransactionId maxXid = ReadNewTransactionId();
    Page page = BufferGetPage(buf);
    UBTRecycleQueueHeader header = GetRecycleQueueHeader(page, BufferGetBlockNumber(buf));
    uint16 offset = header->head;
    while (IsNormalOffset(offset)) {
        UBTRecycleQueueItem item = NULL;
        if (offset >= 0 && offset <= UBTRecycleMaxItems) {
            item = HeaderGetItem(header, offset);
        } else {
            if (!recordEachItem) {
                errVerified++;
                UBTreeVerifyRecordOutput(VERIFY_RECYCLE_QUEUE_PAGE, BufferGetBlockNumber(buf),
                    VERIFY_RECYCLE_QUEUE_OFFSET_ERROR, tupleDesc, tupstore, cols);
            }
            break;
        }
        if (recordEachItem) {
            UBTRecyleQueueRecordOutput(BufferGetBlockNumber(buf), offset, item, tupleDesc, tupstore, cols);
        } else if (TransactionIdFollows(item->xid, maxXid)) {
            errVerified++;
            UBTreeVerifyRecordOutput(VERIFY_RECYCLE_QUEUE_PAGE, BufferGetBlockNumber(buf),
                VERIFY_RECYCLE_QUEUE_XID_TOO_LARGE, tupleDesc, tupstore, cols);
        }
        offset = item->next;
    }
    if (recordEachItem) {
        return 0;
    }
    /* dump work is already done, the following is pure verify logic */
    if (IsNormalOffset(header->head) && header->tail == InvalidOffset && (header->flags & URQ_TAIL_PAGE) == 0) {
        /* head is normal, but tail is invalid. this should be a tail page, but it's not */
        errVerified++;
        UBTreeVerifyRecordOutput(VERIFY_RECYCLE_QUEUE_PAGE, BufferGetBlockNumber(buf),
            VERIFY_RECYCLE_QUEUE_UNEXPECTED_TAIL, tupleDesc, tupstore, cols);
    }

    if (header->freeListHead != InvalidOffset) {
        /* free list is valid, verify it */
        offset = header->freeListHead;
        while (offset != InvalidOffset) {
            if (offset == OtherBlockOffset) {
                if (!recordEachItem) {
                    errVerified++;
                    UBTreeVerifyRecordOutput(VERIFY_RECYCLE_QUEUE_PAGE, BufferGetBlockNumber(buf),
                        VERIFY_RECYCLE_QUEUE_FREE_LIST_ERROR, tupleDesc, tupstore, cols);
                }
                break;
            }
            if (offset >= 0 && offset <= UBTRecycleMaxItems) {
                UBTRecycleQueueItem item = HeaderGetItem(header, offset);
                offset = item->next;
            } else {
                if (!recordEachItem) {
                    errVerified++;
                    UBTreeVerifyRecordOutput(VERIFY_RECYCLE_QUEUE_PAGE, BufferGetBlockNumber(buf),
                        VERIFY_RECYCLE_QUEUE_FREE_LIST_INVALID_OFFSET, tupleDesc, tupstore, cols);
                }
                break;
            }
        }
    }

    return errVerified;
}

