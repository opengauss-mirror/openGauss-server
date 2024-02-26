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
 * ---------------------------------------------------------------------------------------
 *
 * batch_redo.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/access/ondemand_extreme_rto/batch_redo.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef ONDEMAND_EXTREME_RTO_BATCH_REDO_H
#define ONDEMAND_EXTREME_RTO_BATCH_REDO_H

#include "c.h"
#include "storage/buf/block.h"
#include "storage/smgr/relfilenode.h"
#include "lib/dllist.h"
#include "utils/hsearch.h"
#include "access/xlogproc.h"
#include "access/xlogutils.h"

namespace ondemand_extreme_rto {

#define PAGETYPE_DROP 0x04
#define PAGETYPE_CREATE 0x02
#define PAGETYPE_TRUNCATE 0x01
#define PAGETYPE_MODIFY 0x00

#define INITredoItemHashSIZE 1024

#define INIT_REDO_ITEM_TAG(a, xx_rnode, xx_forkNum, xx_blockNum) \
    ((a).rNode = (xx_rnode), (a).forkNum = (xx_forkNum), (a).blockNum = (xx_blockNum))

#define XlogTrackTableHashPartition(hashcode) ((hashcode) % NUM_XLOG_TRACK_PARTITIONS)
#define XlogTrackMappingPartitionLock(hashcode) \
    (&t_thrd.shemem_ptr_cxt.mainLWLockArray[FirstXlogTrackLock + XlogTrackTableHashPartition(hashcode)].lock)

/*
 * Note: if there are any pad bytes in the struct, INIT_RedoItemTag have
 * to be fixed to zero them, since this struct is used as a hash key.
 */
typedef struct redoitemtag {
    RelFileNode rNode;
    ForkNumber forkNum;
    BlockNumber blockNum;
} RedoItemTag;

typedef struct redoitemhashentry {
    RedoItemTag redoItemTag;
    XLogRecParseState *head;
    XLogRecParseState *tail;
    int redoItemNum;
    bool redoDone;
} RedoItemHashEntry;

extern void PRPrintRedoItemHashTab(HTAB *redoItemHash);
extern ondemand_htab_ctrl_t *PRRedoItemHashInitialize(MemoryContext context);
extern ondemand_htab_ctrl_t **PRInitRedoItemHashForAllPipeline(MemoryContext context);
extern void PRTrackClearBlock(XLogRecParseState *recordBlockState, HTAB *redoItemHash);
extern void PRTrackAddBlock(XLogRecParseState *recordBlockState, HTAB *redoItemHash, bool isHead = false);
extern void PRTrackAddBatchBlock(XLogRecParseState *headBlockState, XLogRecParseState *tailBlockState, int count,
    HTAB *redoItemHash, bool isHead);
extern void PRTrackAllClear(HTAB *redoItemHash);
extern uint32 XlogTrackTableHashCode(RedoItemTag *tagPtr);

}  // namespace ondemand_extreme_rto
#endif /* ONDEMAND_EXTREME_RTO_BATCH_REDO_H */
