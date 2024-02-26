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
 * batch_redo.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/transam/ondemand_extreme_rto/batch_redo.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "access/xlogproc.h"
#include "access/visibilitymap.h"
#include "catalog/storage_xlog.h"
#include "commands/dbcommands.h"
#include "commands/tablespace.h"
#include "storage/freespace.h"
#include "storage/smgr/relfilenode_hash.h"
#include "utils/relmapper.h"

#include "access/ondemand_extreme_rto/batch_redo.h"
#include "access/ondemand_extreme_rto/redo_item.h"
#include "access/ondemand_extreme_rto/dispatcher.h"
#include "access/ondemand_extreme_rto/page_redo.h"

#include "access/xlogproc.h"

extern uint32 hashquickany(uint32 seed, register const unsigned char *data, register int len);

namespace ondemand_extreme_rto {
static inline void PRXLogRecGetBlockTag(XLogRecParseState *recordBlockState, RelFileNode *rnode, BlockNumber *blknum,
                                        ForkNumber *forknum)
{
    XLogBlockParse *blockparse = &(recordBlockState->blockparse);

    if (rnode != NULL) {
        rnode->dbNode = blockparse->blockhead.dbNode;
        rnode->relNode = blockparse->blockhead.relNode;
        rnode->spcNode = blockparse->blockhead.spcNode;
        rnode->bucketNode = blockparse->blockhead.bucketNode;
        rnode->opt = blockparse->blockhead.opt;
    }
    if (blknum != NULL) {
        *blknum = blockparse->blockhead.blkno;
    }
    if (forknum != NULL) {
        *forknum = blockparse->blockhead.forknum;
    }
}

uint32 XlogTrackTableHashCode(RedoItemTag *tagPtr)
{
    return hashquickany(0xFFFFFFFF, (unsigned char *)tagPtr, sizeof(RedoItemTag));
}

void PRInitRedoItemEntry(RedoItemHashEntry *redoItemHashEntry)
{
    redoItemHashEntry->redoItemNum = 0;
    redoItemHashEntry->head = NULL;
    redoItemHashEntry->tail = NULL;
    redoItemHashEntry->redoDone = false;
}

uint32 RedoItemTagHash(const void *key, Size keysize)
{
    RedoItemTag redoItemTag = *(const RedoItemTag *)key;
    redoItemTag.rNode.opt = DefaultFileNodeOpt;
    return DatumGetUInt32(hash_any((const unsigned char *)&redoItemTag, (int)keysize));
}

int RedoItemTagMatch(const void *left, const void *right, Size keysize)
{
    const RedoItemTag *leftKey = (const RedoItemTag *)left;
    const RedoItemTag *rightKey = (const RedoItemTag *)right;
    Assert(keysize == sizeof(RedoItemTag));

    /* we just care whether the result is 0 or not */
    if (RelFileNodeEquals(leftKey->rNode, rightKey->rNode) && leftKey->forkNum == rightKey->forkNum &&
        leftKey->blockNum == rightKey->blockNum) {
        return 0;
    }

    return 1;
}

ondemand_htab_ctrl_t *PRRedoItemHashInitialize(MemoryContext context)
{
    HASHCTL ctl;
    ondemand_htab_ctrl_t *htab_ctrl =
        (ondemand_htab_ctrl_t *)MemoryContextAllocZero(context, sizeof(ondemand_htab_ctrl_t));

    /*
     * create hashtable that indexes the redo items
     */
    errno_t rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "\0", "\0");
    ctl.hcxt = context;
    ctl.keysize = sizeof(RedoItemTag);
    ctl.entrysize = sizeof(RedoItemHashEntry);
    ctl.hash = RedoItemTagHash;
    ctl.match = RedoItemTagMatch;
    htab_ctrl->hTab = hash_create("Redo item hash by relfilenode and blocknum", INITredoItemHashSIZE, &ctl,
                       HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT | HASH_COMPARE);
    htab_ctrl->nextHTabCtrl = NULL;
    htab_ctrl->maxRedoItemPtr = InvalidXLogRecPtr;

    return htab_ctrl;
}

ondemand_htab_ctrl_t **PRInitRedoItemHashForAllPipeline(MemoryContext context)
{
    int batchNum = get_batch_redo_num();
    ondemand_htab_ctrl_t **htab_ctrl =
        (ondemand_htab_ctrl_t **)MemoryContextAllocZero(context, batchNum * sizeof(ondemand_htab_ctrl_t *));

    for (int i = 0; i < batchNum; i++) {
        htab_ctrl[i] = PRRedoItemHashInitialize(context);
    }

    return htab_ctrl;
}

void PRRegisterBlockInsertToListHead(RedoItemHashEntry *redoItemHashEntry, XLogRecParseState *record)
{
    ReferenceRecParseState(record);
    if (redoItemHashEntry->head != NULL) {
        Assert(XLByteLE(record->blockparse.blockhead.end_ptr, redoItemHashEntry->head->blockparse.blockhead.end_ptr));
        record->nextrecord = redoItemHashEntry->head;
        redoItemHashEntry->head = record;
    } else {
        redoItemHashEntry->tail = record;
        redoItemHashEntry->head = record;
    }
    redoItemHashEntry->redoItemNum++;
}

void PRRegisterBlockInsertToListTail(RedoItemHashEntry *redoItemHashEntry, XLogRecParseState *record)
{
    ReferenceRecParseState(record);
    if (redoItemHashEntry->tail != NULL) {
        redoItemHashEntry->tail->nextrecord = record;
        redoItemHashEntry->tail = record;
    } else {
        redoItemHashEntry->head = record;
        redoItemHashEntry->tail = record;
    }
    record->nextrecord = NULL;
    redoItemHashEntry->redoItemNum++;
}

#ifdef USE_ASSERT_CHECKING
static void OndemandCheckPRRegister(XLogRecParseState *headRecord, XLogRecParseState *tailRecord, int count)
{
    Assert(headRecord != NULL);
    Assert(tailRecord != NULL);

    int checkCount = 1;
    XLogRecParseState *nextRecord = headRecord;
    while (nextRecord != tailRecord) {
        XLogRecParseState *checkRecord = nextRecord;
        nextRecord = (XLogRecParseState *)checkRecord->nextrecord;
        Assert(XLByteLE(checkRecord->blockparse.blockhead.end_ptr, nextRecord->blockparse.blockhead.end_ptr));
        checkCount++;
    }
    Assert(nextRecord == tailRecord);
    Assert(checkCount == count);
}
#endif

void PRRegisterBatchBlockInsertToListHead(RedoItemHashEntry *redoItemHashEntry, XLogRecParseState *headRecord,
                                          XLogRecParseState *tailRecord, int count)
{
    if (redoItemHashEntry->head != NULL) {
        Assert(XLByteLE(tailRecord->blockparse.blockhead.end_ptr,
                        redoItemHashEntry->head->blockparse.blockhead.end_ptr));
        tailRecord->nextrecord = redoItemHashEntry->head;
        redoItemHashEntry->head = headRecord;
    } else {
        redoItemHashEntry->head = headRecord;
        redoItemHashEntry->tail = tailRecord;
    }
    redoItemHashEntry->redoItemNum += count;
}

void PRRegisterBatchBlockInsertToListTail(RedoItemHashEntry *redoItemHashEntry, XLogRecParseState *headRecord,
                                          XLogRecParseState *tailRecord, int count)
{
    if (redoItemHashEntry->tail != NULL) {
        Assert(XLByteLE(redoItemHashEntry->tail->blockparse.blockhead.end_ptr,
                        headRecord->blockparse.blockhead.end_ptr));
        redoItemHashEntry->tail->nextrecord = headRecord;
        redoItemHashEntry->tail = tailRecord;
    } else {
        redoItemHashEntry->head = headRecord;
        redoItemHashEntry->tail = tailRecord;
    }
    tailRecord->nextrecord = NULL;
    redoItemHashEntry->redoItemNum += count;
}

static RedoItemHashEntry *PRRegisterGetHashEntry(const RelFileNode rNode, ForkNumber forkNum, BlockNumber blkNo,
                                               HTAB *redoItemHash)
{
    RedoItemTag redoItemTag;
    RedoItemHashEntry *redoItemHashEntry = NULL;
    bool found = true;

    INIT_REDO_ITEM_TAG(redoItemTag, rNode, forkNum, blkNo);

    redoItemHashEntry = (RedoItemHashEntry *)hash_search(redoItemHash, (void *)&redoItemTag, HASH_ENTER, &found);
    if (redoItemHashEntry == NULL) {
        ereport(ERROR, (errcode(ERRCODE_FETCH_DATA_FAILED),
                        errmsg("could not find or create redo item entry: rel %u/%u/%u "
                               "forknum %d blkno %u",
                               rNode.spcNode, rNode.dbNode, rNode.relNode, forkNum, blkNo)));
    }
    if (!g_instance.dms_cxt.SSReformInfo.is_hashmap_constructed) {
        g_instance.dms_cxt.SSReformInfo.is_hashmap_constructed = true;
        g_instance.dms_cxt.SSReformInfo.construct_hashmap = GetCurrentTimestamp();
    }
    if (!found) {
        PRInitRedoItemEntry(redoItemHashEntry);
    }
    return redoItemHashEntry;
}

void PRRegisterBlockChangeExtended(XLogRecParseState *recordBlockState, const RelFileNode rNode, ForkNumber forkNum,
                                   BlockNumber blkNo, HTAB *redoItemHash, bool isHead)
{
    RedoItemHashEntry *redoItemHashEntry = PRRegisterGetHashEntry(rNode, forkNum, blkNo, redoItemHash);
    if (unlikely(isHead)) {
        PRRegisterBlockInsertToListHead(redoItemHashEntry, recordBlockState);
    } else {
        PRRegisterBlockInsertToListTail(redoItemHashEntry, recordBlockState);
    }
}

void PRRegisterBatchBlockChangeExtended(XLogRecParseState *headBlockState, XLogRecParseState *tailBlockState, int count,
                                        const RelFileNode rNode, ForkNumber forkNum, BlockNumber blkNo,
                                        HTAB *redoItemHash, bool isHead)
{
    RedoItemHashEntry *redoItemHashEntry = PRRegisterGetHashEntry(rNode, forkNum, blkNo, redoItemHash);
    if (unlikely(isHead)) {
        PRRegisterBatchBlockInsertToListHead(redoItemHashEntry, headBlockState, tailBlockState, count);
    } else {
        PRRegisterBatchBlockInsertToListTail(redoItemHashEntry, headBlockState, tailBlockState, count);
    }
}

void PRTrackRemoveEntry(HTAB *hashMap, RedoItemHashEntry *entry)
{
    XLogRecParseState *recordBlockState = entry->head;
#ifdef USE_ASSERT_CHECKING
    XLogRecParseState *nextBlockState = entry->head;
    while (nextBlockState != NULL) {
        XLogRecParseState *prev = nextBlockState;
        nextBlockState = (XLogRecParseState *)(nextBlockState->nextrecord);

        if (prev->refrecord != NULL) {
            DoRecordCheck(prev, InvalidXLogRecPtr, false);
        }

        ereport(LOG, (errmsg("PRTrackRemoveEntry:record(%X/%X) relation %u/%u/%u forknum %u blocknum %u dropped(%p)",
                             (uint32)(prev->blockparse.blockhead.end_ptr >> 32),
                             (uint32)(prev->blockparse.blockhead.end_ptr), prev->blockparse.blockhead.spcNode,
                             prev->blockparse.blockhead.dbNode, prev->blockparse.blockhead.relNode,
                             prev->blockparse.blockhead.forknum, prev->blockparse.blockhead.blkno, prev->refrecord)));
    }

#endif
    XLogBlockParseStateRelease(recordBlockState);

    if (hash_search(hashMap, entry, HASH_REMOVE, NULL) == NULL) {
        ereport(ERROR, (errmsg("PRTrackRemoveEntry:Redo item hash table corrupted")));
    }
}

void PRTrackRelTruncate(HTAB *hashMap, const RelFileNode rNode, ForkNumber forkNum, BlockNumber blkNo)
{
    HASH_SEQ_STATUS status;
    RedoItemHashEntry *redoItemEntry = NULL;
    hash_seq_init(&status, hashMap);

    while ((redoItemEntry = (RedoItemHashEntry *)hash_seq_search(&status)) != NULL) {
        if (RelFileNodeEquals(redoItemEntry->redoItemTag.rNode, rNode) &&
            redoItemEntry->redoItemTag.forkNum == forkNum && (redoItemEntry->redoItemTag.blockNum >= blkNo)) {
            PRTrackRemoveEntry(hashMap, redoItemEntry);
        }
    }
}

void PRTrackTableSpaceDrop(XLogRecParseState *recordBlockState, HTAB *hashMap)
{
    HASH_SEQ_STATUS status;
    RedoItemHashEntry *redoItemEntry = NULL;
    hash_seq_init(&status, hashMap);

    RelFileNode rNode;
    PRXLogRecGetBlockTag(recordBlockState, &rNode, NULL, NULL);
#ifdef USE_ASSERT_CHECKING
    ereport(LOG, (errmsg("PRTrackRelTruncate:(%X/%X)clear table space %u record",
                         (uint32)(recordBlockState->blockparse.blockhead.end_ptr >> 32),
                         (uint32)(recordBlockState->blockparse.blockhead.end_ptr), rNode.spcNode)));
#endif

    while ((redoItemEntry = (RedoItemHashEntry *)hash_seq_search(&status)) != NULL) {
        if (redoItemEntry->redoItemTag.rNode.spcNode == rNode.spcNode) {
            PRTrackRemoveEntry(hashMap, redoItemEntry);
        }
    }
    XLogBlockParseStateRelease(recordBlockState);
}

void PRTrackDatabaseDrop(XLogRecParseState *recordBlockState, HTAB *hashMap)
{
    HASH_SEQ_STATUS status;
    RedoItemHashEntry *redoItemEntry = NULL;
    hash_seq_init(&status, hashMap);

    RelFileNode rNode;
    PRXLogRecGetBlockTag(recordBlockState, &rNode, NULL, NULL);
#ifdef USE_ASSERT_CHECKING
    ereport(LOG, (errmsg("PRTrackRelTruncate:(%X/%X)clear db %u/%u record",
                         (uint32)(recordBlockState->blockparse.blockhead.end_ptr >> 32),
                         (uint32)(recordBlockState->blockparse.blockhead.end_ptr), rNode.spcNode, rNode.dbNode)));
#endif

    while ((redoItemEntry = (RedoItemHashEntry *)hash_seq_search(&status)) != NULL) {
        if (redoItemEntry->redoItemTag.rNode.spcNode == rNode.spcNode &&
            redoItemEntry->redoItemTag.rNode.dbNode == rNode.dbNode) {
            PRTrackRemoveEntry(hashMap, redoItemEntry);
        }
    }
    XLogBlockParseStateRelease(recordBlockState);
}

void PRTrackAllClear(HTAB *redoItemHash)
{
    HASH_SEQ_STATUS status;
    RedoItemHashEntry *redoItemEntry = NULL;
    hash_seq_init(&status, redoItemHash);

    while ((redoItemEntry = (RedoItemHashEntry *)hash_seq_search(&status)) != NULL) {
        PRTrackRemoveEntry(redoItemHash, redoItemEntry);
    }
}

void PRTrackDropFiles(HTAB *redoItemHash, XLogBlockDdlParse *ddlParse, XLogRecPtr lsn)
{
    ColFileNodeRel *xnodes = (ColFileNodeRel *)ddlParse->mainData;
    bool compress = ddlParse->compress;
    for (int i = 0; i < ddlParse->rels; ++i) {
        ColFileNode colFileNode;
        if (compress) {
            ColFileNode *colFileNodeRel = ((ColFileNode *)(void *)xnodes) + i;
            ColFileNodeFullCopy(&colFileNode, colFileNodeRel);
        } else {
            ColFileNodeRel *colFileNodeRel = xnodes + i;
            ColFileNodeCopy(&colFileNode, colFileNodeRel);
        }

        if (!IsValidColForkNum(colFileNode.forknum)) {
            for (int i = 0; i < MAX_FORKNUM; ++i)
                PRTrackRelTruncate(redoItemHash, colFileNode.filenode, i, 0);
        } else {
            PRTrackRelTruncate(redoItemHash, colFileNode.filenode, colFileNode.forknum, 0);
        }
#ifdef USE_ASSERT_CHECKING
        ereport(LOG, (errmsg("PRTrackRelTruncate(drop):(%X/%X)clear relation %u/%u/%u forknum %d record",
            (uint32)(lsn >> 32), (uint32)(lsn), colFileNode.filenode.spcNode, colFileNode.filenode.dbNode,
            colFileNode.filenode.relNode, colFileNode.forknum)));
#endif
    }
}

void PRTrackRelStorageDrop(XLogRecParseState *recordBlockState, HTAB *redoItemHash)
{
    XLogBlockParse *blockparse = &(recordBlockState->blockparse);
    XLogBlockDdlParse *ddlParse = NULL;
    XLogBlockParseGetDdlParse(recordBlockState, ddlParse);

    if (ddlParse->blockddltype == BLOCK_DDL_TRUNCATE_RELNODE) {
        RelFileNode rNode;
        rNode.spcNode = blockparse->blockhead.spcNode;
        rNode.dbNode = blockparse->blockhead.dbNode;
        rNode.relNode = blockparse->blockhead.relNode;
        rNode.bucketNode = blockparse->blockhead.bucketNode;
        rNode.opt = blockparse->blockhead.opt;
#ifdef USE_ASSERT_CHECKING
        ereport(LOG, (errmsg("PRTrackRelTruncate:(%X/%X)clear relation %u/%u/%u forknum %u record",
            (uint32)(blockparse->blockhead.end_ptr >> 32), (uint32)(blockparse->blockhead.end_ptr), rNode.spcNode,
            rNode.dbNode, rNode.relNode, blockparse->blockhead.forknum)));
#endif
        PRTrackRelTruncate(redoItemHash, rNode, blockparse->blockhead.forknum, blockparse->blockhead.blkno);
    } else {
        PRTrackDropFiles(redoItemHash, ddlParse, blockparse->blockhead.end_ptr);
    }

    XLogBlockParseStateRelease(recordBlockState);
}

// Get relfile node fork num blockNum
void PRTrackRelPageModification(XLogRecParseState *recordBlockState, HTAB *redoItemHash, bool isHead)
{
    RelFileNode relnode;
    ForkNumber forkNum;
    BlockNumber blkNo;

    PRXLogRecGetBlockTag(recordBlockState, &relnode, &blkNo, &forkNum);

    PRRegisterBlockChangeExtended(recordBlockState, relnode, forkNum, blkNo, redoItemHash, isHead);
}

/**
    for block state, put it in to hash
*/
void PRTrackAddBlock(XLogRecParseState *recordBlockState, HTAB *redoItemHash, bool isHead)
{
    Assert(recordBlockState->blockparse.blockhead.block_valid < BLOCK_DATA_DDL_TYPE);
    PRTrackRelPageModification(recordBlockState, redoItemHash, isHead);
}

void PRTrackAddBatchBlock(XLogRecParseState *headBlockState, XLogRecParseState *tailBlockState, int count,
                          HTAB *redoItemHash, bool isHead)
{
#ifdef USE_ASSERT_CHECKING
    OndemandCheckPRRegister(headBlockState, tailBlockState, count);
#endif
    RelFileNode relnode;
    ForkNumber forkNum;
    BlockNumber blkNo;

    PRXLogRecGetBlockTag(headBlockState, &relnode, &blkNo, &forkNum);
    PRRegisterBatchBlockChangeExtended(headBlockState, tailBlockState, count, relnode, forkNum, blkNo, redoItemHash,
                                       isHead);
}

/**
     others state, clear related block state(including release), release it
*/
void PRTrackClearBlock(XLogRecParseState *recordBlockState, HTAB *redoItemHash)
{
    Assert(recordBlockState != NULL);
    Assert(redoItemHash != NULL);
    XLogBlockParse *blockparse = &(recordBlockState->blockparse);
    if (blockparse->blockhead.block_valid == BLOCK_DATA_DDL_TYPE) {
        PRTrackRelStorageDrop(recordBlockState, redoItemHash);
    } else if (blockparse->blockhead.block_valid == BLOCK_DATA_DROP_DATABASE_TYPE) {
        PRTrackDatabaseDrop(recordBlockState, redoItemHash);
    } else if (blockparse->blockhead.block_valid == BLOCK_DATA_DROP_TBLSPC_TYPE) {
        PRTrackTableSpaceDrop(recordBlockState, redoItemHash);
    } else {
        const uint32 rightShiftSize = 32;
        ereport(WARNING,
                (errmsg("PRTrackClearBlock:(%X/%X) not identified %u/%u/%u forknum %d record",
                        (uint32)(blockparse->blockhead.end_ptr >> rightShiftSize),
                        (uint32)(blockparse->blockhead.end_ptr), blockparse->blockhead.spcNode,
                        blockparse->blockhead.dbNode, blockparse->blockhead.relNode, blockparse->blockhead.forknum)));
        XLogBlockParseStateRelease(recordBlockState);
    }
}

}  // namespace ondemand_extreme_rto
