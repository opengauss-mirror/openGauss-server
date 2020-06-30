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
 *    src/gausskernel/storage/access/transam/extreme_rto/batch_redo.cpp
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
#include "utils/relmapper.h"

#include "access/extreme_rto/batch_redo.h"
#include "access/extreme_rto/redo_item.h"
#include "access/extreme_rto/dispatcher.h"
#include "access/extreme_rto/page_redo.h"

#include "access/xlogproc.h"

namespace extreme_rto {
static inline void PRXLogRecGetBlockTag(
    XLogRecParseState* recordblockstate, RelFileNode* rnode, BlockNumber* blknum, ForkNumber* forknum)
{
    XLogBlockParse* blockparse = &(recordblockstate->blockparse);

    if (rnode != NULL) {
        rnode->dbNode = blockparse->blockhead.dbNode;
        rnode->relNode = blockparse->blockhead.relNode;
        rnode->spcNode = blockparse->blockhead.spcNode;
        rnode->bucketNode = blockparse->blockhead.bucketNode;
    }
    if (blknum != NULL) {
        *blknum = blockparse->blockhead.blkno;
    }
    if (forknum != NULL) {
        *forknum = blockparse->blockhead.forknum;
    }
}

void PRInitRedoItemEntry(RedoItemHashEntry* redoItemHashEntry)
{
    redoItemHashEntry->redoItemNum = 0;
    redoItemHashEntry->head = NULL;
    redoItemHashEntry->tail = NULL;
}

HTAB* PRRedoItemHashInitialize(MemoryContext context)
{
    HASHCTL ctl;
    HTAB* hTab = NULL;

    /*
     * create hashtable that indexes the redo items
     */
    errno_t rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "\0", "\0");
    ctl.hcxt = context;
    ctl.keysize = sizeof(RedoItemTag);
    ctl.entrysize = sizeof(RedoItemHashEntry);
    ctl.hash = tag_hash;
    hTab = hash_create("Redo item hash by relfilenode and blocknum",
        INITredoItemHashSIZE,
        &ctl,
        HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

    return hTab;
}

void PRRegisterBlockInsertToList(RedoItemHashEntry* redoItemHashEntry, XLogRecParseState* record)
{
    if (redoItemHashEntry->tail != NULL) {
        redoItemHashEntry->tail->nextrecord = record;
        redoItemHashEntry->tail = record;
    } else {
        redoItemHashEntry->tail = record;
        redoItemHashEntry->head = record;
    }
    record->nextrecord = NULL;
    redoItemHashEntry->redoItemNum++;
}

void PRRegisterBlockChangeExtended(XLogRecParseState* recordblockstate, const RelFileNode rNode, ForkNumber forkNum,
    BlockNumber blkNo, HTAB* redoItemHash)
{
    RedoItemTag redoItemTag;
    RedoItemHashEntry* redoItemHashEntry = NULL;
    bool found = true;

    INIT_REDO_ITEM_TAG(redoItemTag, rNode, forkNum, blkNo);

    redoItemHashEntry = (RedoItemHashEntry*)hash_search(redoItemHash, (void*)&redoItemTag, HASH_ENTER, &found);
    if (redoItemHashEntry == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_FETCH_DATA_FAILED),
                errmsg("could not find or create redo item entry: rel %u/%u/%u "
                       "forknum %d blkno %u",
                    rNode.spcNode,
                    rNode.dbNode,
                    rNode.relNode,
                    forkNum,
                    blkNo)));
    }

    if (!found) {
        PRInitRedoItemEntry(redoItemHashEntry);
    }
    PRRegisterBlockInsertToList(redoItemHashEntry, recordblockstate);
}

void PRTrackRemoveEntry(HTAB* hashMap, RedoItemHashEntry* entry)
{
    XLogRecParseState* recordblockstate = entry->head;
    XLogBlockParseStateRelease(recordblockstate);

    if (hash_search(hashMap, entry, HASH_REMOVE, NULL) == NULL) {
        ereport(
            ERROR, (errcode(ERRCODE_FILE_READ_FAILED), errmsg("PRTrackRemoveEntry:Redo item hash table corrupted")));
    }
}

void PRTrackRelTruncate(HTAB* hashMap, const RelFileNode rNode, ForkNumber forkNum, BlockNumber blkNo)
{
    HASH_SEQ_STATUS status;
    RedoItemHashEntry* redoItemEntry = NULL;
    hash_seq_init(&status, hashMap);

    while ((redoItemEntry = (RedoItemHashEntry*)hash_seq_search(&status)) != NULL) {
        if (redoItemEntry->redoItemTag.rNode.spcNode == rNode.spcNode &&
            redoItemEntry->redoItemTag.rNode.dbNode == rNode.dbNode &&
            redoItemEntry->redoItemTag.rNode.relNode == rNode.relNode &&
            redoItemEntry->redoItemTag.forkNum == forkNum && (redoItemEntry->redoItemTag.blockNum >= blkNo)) {
            PRTrackRemoveEntry(hashMap, redoItemEntry);
        }
    }
}

void PRTrackTableSpaceDrop(XLogRecParseState* recordblockstate, HTAB* hashMap)
{
    HASH_SEQ_STATUS status;
    RedoItemHashEntry* redoItemEntry = NULL;
    hash_seq_init(&status, hashMap);

    RelFileNode rNode;
    PRXLogRecGetBlockTag(recordblockstate, &rNode, NULL, NULL);

    while ((redoItemEntry = (RedoItemHashEntry*)hash_seq_search(&status)) != NULL) {
        if (redoItemEntry->redoItemTag.rNode.spcNode == rNode.spcNode) {
            PRTrackRemoveEntry(hashMap, redoItemEntry);
        }
    }
    XLogBlockParseStateRelease(recordblockstate);
}

void PRTrackDatabaseDrop(XLogRecParseState* recordblockstate, HTAB* hashMap)
{
    HASH_SEQ_STATUS status;
    RedoItemHashEntry* redoItemEntry = NULL;
    hash_seq_init(&status, hashMap);

    RelFileNode rNode;
    PRXLogRecGetBlockTag(recordblockstate, &rNode, NULL, NULL);

    while ((redoItemEntry = (RedoItemHashEntry*)hash_seq_search(&status)) != NULL) {
        if (redoItemEntry->redoItemTag.rNode.spcNode == rNode.spcNode &&
            redoItemEntry->redoItemTag.rNode.dbNode == rNode.dbNode) {
            PRTrackRemoveEntry(hashMap, redoItemEntry);
        }
    }
    XLogBlockParseStateRelease(recordblockstate);
}

void PRTrackRelStorageDrop(XLogRecParseState* recordblockstate, HTAB* redoItemHash)
{
    XLogBlockParse* blockparse = &(recordblockstate->blockparse);
    XLogBlockDdlParse* ddlParse = &(recordblockstate->blockparse.extra_rec.blockddlrec);
    RelFileNode rNode;
    rNode.spcNode = blockparse->blockhead.spcNode;
    rNode.dbNode = blockparse->blockhead.dbNode;
    rNode.relNode = blockparse->blockhead.relNode;
    rNode.bucketNode = blockparse->blockhead.bucketNode;

    if (ddlParse->blockddltype == BLOCK_DDL_TRUNCATE_RELNODE) {
        PRTrackRelTruncate(redoItemHash, rNode, blockparse->blockhead.forknum, blockparse->blockhead.blkno);
    } else if (!IsValidColForkNum(blockparse->blockhead.forknum)) {
        for (int i = 0; i < MAX_FORKNUM; ++i)
            PRTrackRelTruncate(redoItemHash, rNode, i, 0);
    } else {
        PRTrackRelTruncate(redoItemHash, rNode, blockparse->blockhead.forknum, 0);
    }
    XLogBlockParseStateRelease(recordblockstate);
}

// Get relfile node fork num blockNum
void PRTrackRelPageModification(XLogRecParseState* recordblockstate, HTAB* redoItemHash)
{
    RelFileNode relnode;
    ForkNumber forkNum;
    BlockNumber blkNo;

    PRXLogRecGetBlockTag(recordblockstate, &relnode, &blkNo, &forkNum);

    PRRegisterBlockChangeExtended(recordblockstate, relnode, forkNum, blkNo, redoItemHash);
}

/**
    for block state, put it in to hash,
     others state, clear related block state(including release), do not release it
*/
void PRTrackChangeBlock(XLogRecParseState* recordblockstate, HTAB* redoItemHash)
{
    XLogBlockParse* blockparse = &(recordblockstate->blockparse);
    if (blockparse->blockhead.block_valid < BLOCK_DATA_DDL_TYPE ||
        blockparse->blockhead.block_valid == BLOCK_DATA_NEWCU_TYPE ||
        blockparse->blockhead.block_valid == BLOCK_DATA_BCM_TYPE) {
        PRTrackRelPageModification(recordblockstate, redoItemHash);
    } else if (blockparse->blockhead.block_valid == BLOCK_DATA_DDL_TYPE) {
        PRTrackRelStorageDrop(recordblockstate, redoItemHash);
    } else if (blockparse->blockhead.block_valid == BLOCK_DATA_DROP_DATABASE_TYPE) {
        PRTrackDatabaseDrop(recordblockstate, redoItemHash);
    } else if (blockparse->blockhead.block_valid == BLOCK_DATA_DROP_TBLSPC_TYPE) {
        PRTrackTableSpaceDrop(recordblockstate, redoItemHash);
    } else {
        XLogBlockParseStateRelease(recordblockstate);
    }
}

}  // namespace extreme_rto
