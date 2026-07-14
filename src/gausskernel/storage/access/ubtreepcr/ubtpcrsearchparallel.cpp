/*
 * Copyright (c) 2024 Huawei Technologies Co.,Ltd.
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
 * --------------------------------------------------------------------------------------
 *
 * ubtpcrsearchparallel.cpp
 *  Parallel search code for openGauss ubtree pcr page.
 *
 *
 * IDENTIFICATION
 *        src\gausskernel\storage\access\ubtreepcr\ubtpcrsearchparallel.cpp
 *
 * --------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/nbtree.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "storage/predicate.h"
#include "distributelayer/streamCore.h"
#include "access/parallel_indexscan_core.h"
#include "access/ubtreepcr.h"

/*
 * @brief UBTreePCRFindNextBlock
 *  Find the next block according to the siling pointer (btpo_next/btpo_prev).
 * @param pcr_scan                      IndexScanDesc
 * @param dir                           Scanning direction
 * @param current_block                 Start block number of the current thread.
 * @param num_blocks                    Estimated number of lef nodes scanned by each thread
 * @return BlockNumber                  End block number of the current thread.
 */
BlockNumber UBTreePCRFindNextBlock(IndexScanDesc pcr_scan, ScanDirection dir, uint32 current_block, int num_blocks)
{
    if (current_block == InvalidBlockNumber || current_block == 0) {
        return 0;
    }
    int count = 1;
    BlockNumber pcr_next = current_block;
    BlockNumber pcr_now = current_block;
    Relation pcr_rel = pcr_scan->indexRelation;
    Buffer current_buf = _bt_getbuf(pcr_rel, static_cast<BlockNumber>(current_block), BT_READ);
    UBTPCRPageOpaque opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(BufferGetPage(current_buf));
    pcr_now = pcr_next;
    if (ScanDirectionIsForward(dir)) {
        pcr_next = opaque->btpo_next;
    } else {
        pcr_next = opaque->btpo_prev;
    }
    _bt_relbuf(pcr_rel, current_buf);
    Buffer buf_tmp = InvalidBuffer;
    while (count <= num_blocks) {
        buf_tmp = _bt_getbuf(pcr_rel, pcr_next, BT_READ);
        opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(BufferGetPage(buf_tmp));
        bool empty_page = false;
        if (count == num_blocks && !P_RIGHTMOST(opaque) && !P_LEFTMOST(opaque)) {
            OffsetNumber max_off = UBTreePCRPageGetMaxOffsetNumber(BufferGetPage(buf_tmp));
            empty_page = (P_FIRSTKEY > max_off);
        }
        if (ScanDirectionIsForward(dir)) {
            if (P_IGNORE(opaque) || empty_page) {
                _bt_relbuf(pcr_rel, buf_tmp);
                pcr_now = pcr_next;
                pcr_next = opaque->btpo_next;
                continue;
            }
            if (P_RIGHTMOST(opaque)) {
                _bt_relbuf(pcr_rel, buf_tmp);
                return 0;
            }
            pcr_now = pcr_next;
            pcr_next = opaque->btpo_next;
        } else {
            if (P_IGNORE(opaque) || empty_page) {
                _bt_relbuf(pcr_rel, buf_tmp);
                pcr_now = pcr_next;
                pcr_next = opaque->btpo_prev;
                continue;
            }
            if (P_LEFTMOST(opaque)) {
                _bt_relbuf(pcr_rel, buf_tmp);
                return 0;
            }
            pcr_now = pcr_next;
            pcr_next = opaque->btpo_prev;
        }
        if (count == num_blocks) {
            opaque->btpo_flags |= BTP_PARALLEL_SCAN_END;
            if (((UBTPCRPageOpaque)opaque)->xact < pcr_scan->xs_snapshot->xmin) {
                ((UBTPCRPageOpaque)opaque)->xact = pcr_scan->xs_snapshot->xmin;
            }
            MarkBufferDirtyHint(buf_tmp, true);
        }
        _bt_relbuf(pcr_rel, buf_tmp);
        count++;
    }
    return pcr_now;
}

/*
 * @brief UBTreePCRParallelGetScanTotalBlocks
 *  Calculate the number of block that meet the scankey reuqirement.
 * @param pcr_scan                      IndexScanDesc
 * @param dir                           Scanning direction
 * @param pcr_start_blk                 Start scan block number
 * @return int                          Number of all block that meet the conditions
 */
int UBTreePCRParallelGetScanTotalBlocks(IndexScanDesc pcr_scan, ScanDirection dir, BlockNumber pcr_start_blk)
{
    if (pcr_start_blk == InvalidBuffer) {
        return 0;
    }
    Relation pcr_rel = pcr_scan->indexRelation;
    BTScanOpaque pcr_para_so = (BTScanOpaque)pcr_scan->opaque;
    int total_blocks = 1;
    bool still_loop = true;
    BlockNumber pcr_next = pcr_start_blk;
    while (true) {
        CHECK_FOR_INTERRUPTS();
        Buffer tmp_buf = _bt_getbuf(pcr_rel, pcr_next, BT_READ);
        UBTPCRPageOpaque opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(BufferGetPage(tmp_buf));
        if (ScanDirectionIsForward(dir)) {
            pcr_next = opaque->btpo_next;
            if (P_RIGHTMOST(opaque)) {
                _bt_relbuf(pcr_rel, tmp_buf);
                break;
            }
        } else {
            pcr_next = opaque->btpo_prev;
            if (P_LEFTMOST(opaque)) {
                _bt_relbuf(pcr_rel, tmp_buf);
                break;
            }
        }
        _bt_relbuf(pcr_rel, tmp_buf);
        if (pcr_para_so->numberOfKeys > 0) {
            tmp_buf = _bt_getbuf(pcr_rel, pcr_next, BT_READ);
            Page cur_page = BufferGetPage(tmp_buf);
            opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(cur_page);
            if (ScanDirectionIsForward(dir)) {
                if (P_FIRSTDATAKEY(opaque) > PageGetMaxOffsetNumber(cur_page)) {
                    _bt_relbuf(pcr_rel, tmp_buf);
                    total_blocks++;
                    continue;
                }
                UBTreePCRCheckKeys(pcr_scan, cur_page, P_FIRSTDATAKEY(opaque), dir, &still_loop);
            } else {
                UBTreePCRCheckKeys(pcr_scan, cur_page, PageGetMaxOffsetNumber(cur_page), dir, &still_loop);
            }
            if (!still_loop) {
                _bt_relbuf(pcr_rel, tmp_buf);
                break;
            }
            _bt_relbuf(pcr_rel, tmp_buf);
        }
        total_blocks++;
    }
    return total_blocks;
}

/*
 * @brief UBTreePCRGetInskeyScankeyWithoutRowheader
 *  Initialize inskey->scankey when cur_>sk_flags & SK_ROW_HEADER is 0.
 * @param cur                           No. i startKey
 * @param pcr_rel                       relation of the current index
 * @param i                             Number of startkey iterations
 * @return void
 */
void UBTreePCRGetInskeyScankeyWithoutRowheader(ScanKey cur, Relation pcr_rel, int i, BTScanInsertData* inskey)
{
    if (cur->sk_subtype == pcr_rel->rd_opcintype[i] || cur->sk_subtype == InvalidOid) {
        FmgrInfo* procinfo = index_getprocinfo(pcr_rel, cur->sk_attno, BTORDER_PROC);
        ScanKeyEntryInitializeWithInfo(inskey->scankeys + i, cur->sk_flags, cur->sk_attno, InvalidStrategy,
                                       cur->sk_subtype, cur->sk_collation, procinfo, cur->sk_argument);
    } else {
        RegProcedure cmp_proc =
            get_opfamily_proc(pcr_rel->rd_opfamily[i], pcr_rel->rd_opcintype[i], cur->sk_subtype, BTORDER_PROC);
        if (SECUREC_UNLIKELY(!RegProcedureIsValid(cmp_proc)))
            ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                            errmsg("missing support function %d(%u,%u) for attribute %d of index \"%s\"", BTORDER_PROC,
                                   pcr_rel->rd_opcintype[i], cur->sk_subtype, cur->sk_attno,
                                   RelationGetRelationName(pcr_rel))));
        ScanKeyEntryInitialize(inskey->scankeys + i, cur->sk_flags, cur->sk_attno, InvalidStrategy, cur->sk_subtype,
                               cur->sk_collation, cmp_proc, cur->sk_argument);
    }
    return;
}

/*
 * @brief  UBTreePCRGetGobackNeedToNext
 *  If the number of start conditions is 0, the start buffer and offset are returned.
 * @param pcr_scan                      IndexScanDesc
 * @param *need_to_go_back              need to take a step back
 * @param *need_to_next_key             proceed to the next step
 * @param strat_total                   different scanning conditions
 * @return bool                         returns true if there is no error, false otherwise
 */
bool UBTreePCRGetGobackNeedToNext(IndexScanDesc pcr_scan, ScanDirection dir, bool* pcr_need_to_go_back,
                                 bool* pcr_need_to_next_key, StrategyNumber strat_total)
{
    switch (strat_total) {
        case BTGreaterEqualStrategyNumber:
            break;
        case BTGreaterStrategyNumber:
            *pcr_need_to_next_key = true;
            break;
        case BTEqualStrategyNumber:
            *pcr_need_to_go_back = (ScanDirectionIsBackward(dir)) ? true : false;
            *pcr_need_to_next_key = (ScanDirectionIsBackward(dir)) ? true : false;
            break;
        case BTLessEqualStrategyNumber:
            *pcr_need_to_go_back = true;
            *pcr_need_to_next_key = true;
            break;
        case BTLessStrategyNumber:
            *pcr_need_to_go_back = true;
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                            errmsg("Unrecognized strat_total:%d in index \"%s\".", strat_total,
                                   RelationGetRelationName(pcr_scan->indexRelation))));
            return false;
    }
    return true;
}

/*
 * @brief  UBTreePCRGetBeginParallelScanBuf
 *  Obtains the start scanning buffer.
 * @param  pcr_scan             IndexScanDesc
 * @param  dir                  Scanning direction
 * @param  inskey               BTsacnInsertData
 * @param  *need_to_go_back     need to take a step back
 * @return Buffer               return the start scanning buffer
 */
Buffer UBTreePCRGetBeginParallelScanBuf(IndexScanDesc pcr_scan, ScanDirection dir, BTScanInsertData* inskey,
                                        bool* need_to_go_back)
{
    Relation pcr_rel = pcr_scan->indexRelation;
    bool res = false;
    ScanKey start_keys[INDEX_MAX_KEYS] = {0};
    StrategyNumber strat_total = BTEqualStrategyNumber;
    int keys_count = _bt_get_start_keys(pcr_scan, dir, start_keys, strat_total);
    if (keys_count == 0) {
        return UBTreePCRGetEndPoint(pcr_rel, 0, ScanDirectionIsBackward(dir));
    }
    Assert(keys_count <= INDEX_MAX_KEYS);
    for (int i = 0; i < keys_count; i++) {
        ScanKey cur = start_keys[i];
        Assert(cur->sk_attno == i + 1);
        if (cur->sk_flags & SK_ROW_HEADER) {
            bool continue_loop = true;
            res = _bt_get_inskey_scankey_with_rowheader(cur, inskey, strat_total, keys_count, i, continue_loop);
            if (!res) {
                return InvalidBuffer;
            }
            if (!continue_loop) {
                break;
            }
        } else {
            UBTreePCRGetInskeyScankeyWithoutRowheader(cur, pcr_rel, i, inskey);
        }
    }
    *need_to_go_back = false;
    bool need_to_next_key = false;
    res = UBTreePCRGetGobackNeedToNext(pcr_scan, dir, need_to_go_back, &need_to_next_key, strat_total);
    if (!res) {
        return InvalidBuffer;
    }
    /* Initialize remaining insertion can fileds */
    if (inskey != NULL) {
        inskey->heapkeyspace = true;
        inskey->anynullkeys = false;
        inskey->nextkey = need_to_next_key;
        inskey->pivotsearch = false;
        inskey->scantid = NULL;
        inskey->keysz = keys_count;
    }
    Buffer buf;
    (void)UBTreePCRSearch(pcr_rel, inskey, &buf, BT_READ, false);
    if (!BufferIsValid(buf)) {
        return InvalidBuffer;
    }
    return buf;
}

/*
 * @brief  UBTreePCRParallelFirstThread0Proc
 *  In ther parallel_first func, thread 0 divides the scan blocks and records them to the shared memory,
 *  and assigns the scan start and end blocks of thread 0.
 * @param  pcr_scan         IndexScanDesc
 * @param  dir              Scanning direction
 * @param  begin_buf        Start scanning buffer
 * @param  curr_off_start   2D Offset in Shared Memory
 * @param  index            One-dimensional index of the current index in the shared memory
 * @param  bt_start_blk     Start scan block number of thread 0
 * @param  stream_nodegroup StreamNodeGroup
 * @return bool             returns true if there is no error, false otherwise
 */
bool UBTreePCRParallelFirstThread0Proc(IndexScanDesc pcr_scan, ScanDirection dir, Buffer begin_buf, int curr_off_start,
                                       int index, BlockNumber& bt_start_blk, StreamNodeGroup* stream_nodegroup)
{
    Relation pcr_rel = pcr_scan->indexRelation;
    int curr_th0_start = curr_off_start + OFFSET_START_BASE;
    int curr_th0_end = curr_off_start + OFFSET_END_BASE;
    BTScanOpaque pcr_so = (BTScanOpaque)pcr_scan->opaque;
    BlockNumber blkno = InvalidBuffer;
    BTScanInsertData inskey = {0};
    if (BufferIsValid(begin_buf)) {
        blkno = BufferGetBlockNumber(begin_buf);
        _bt_relbuf(pcr_rel, begin_buf);
    } else {
        PredicateLockRelation(pcr_rel, pcr_scan->xs_snapshot);
        pcr_so->currPos.buf = InvalidBuffer;
        return false;
    }
    int real_scan_blocks = UBTreePCRParallelGetScanTotalBlocks(pcr_scan, dir, blkno);
    int num_blocks = (real_scan_blocks + (pcr_scan->dop - 1)) / pcr_scan->dop;

    pthread_mutex_t* mutex = stream_nodegroup->GetIndexSmpMutex();
    pthread_cond_t* cond = stream_nodegroup->GetIndexSmpCond();
    ereport(LOG,
            (errmsg("pcr ubtree parallel scan oid %u, dop %d, %d blocks per thread, total blocks %d, real scan blocks "
                    "%u, plan nodeid %u.",
                    pcr_rel->rd_id, pcr_scan->dop, num_blocks, RelationGetNumberOfBlocks(pcr_rel), real_scan_blocks,
                    pcr_scan->plan_nodeid)));
    MemoryContext old_mem_context = MemoryContextSwitchTo(stream_nodegroup->m_streamRuntimeContext);
    PthreadMutexLock(t_thrd.utils_cxt.ThreadRootResourceOwner, mutex);
    stream_nodegroup->parallel_indexscan_map[index][curr_off_start + OFFSET_START_BASE] = blkno;
    PthreadMutexUnlock(t_thrd.utils_cxt.ThreadRootResourceOwner, mutex);
    for (int i = OFFSET_START_BASE; i < pcr_scan->dop; i++) {
        PthreadMutexLock(t_thrd.utils_cxt.ThreadRootResourceOwner, mutex);
        uint32 current_block = stream_nodegroup->parallel_indexscan_map[index][curr_off_start + i];
        PthreadMutexUnlock(t_thrd.utils_cxt.ThreadRootResourceOwner, mutex);
        BlockNumber pcr_next = UBTreePCRFindNextBlock(pcr_scan, dir, current_block, num_blocks);
        int next_thread_start_offset = curr_off_start + i + OFFSET_START_BASE;
        PthreadMutexLock(t_thrd.utils_cxt.ThreadRootResourceOwner, mutex);
        stream_nodegroup->parallel_indexscan_map[index][next_thread_start_offset] =
            pcr_next == 0 ? InvalidBlockNumber : pcr_next;
        PthreadMutexUnlock(t_thrd.utils_cxt.ThreadRootResourceOwner, mutex);
    }
    PthreadMutexLock(t_thrd.utils_cxt.ThreadRootResourceOwner, mutex);
    stream_nodegroup->parallel_indexscan_map[index][curr_off_start + pcr_scan->dop + OFFSET_START_BASE] =
        InvalidBlockNumber;
    stream_nodegroup->parallel_indexscan_map[index][curr_off_start] = pcr_scan->plan_nodeid;
    pg_memory_barrier();
    bt_start_blk = stream_nodegroup->parallel_indexscan_map[index][curr_th0_start];
    pcr_scan->btps_end_block = stream_nodegroup->parallel_indexscan_map[index][curr_th0_end];
    pthread_cond_broadcast(cond);
    PthreadMutexUnlock(t_thrd.utils_cxt.ThreadRootResourceOwner, mutex);
    MemoryContextSwitchTo(old_mem_context);
    return true;
}

/*
 * @brief  UBTreePCRParallelFirstGetFirstBuffer
 *  Get start scan block of the current thread.
 * @param  pcr_scan             IndexScanDesc
 * @param  dir                  Scanning direction
 * @param  inskey               BTScanInsertData
 * @param  need_to_go_back      need to take a step back
 * @param  pcr_start_blk        Start scan block number of current thread
 * @return Buffer               Start scan block of the current thread
 */
Buffer UBTreePCRParallelFirstGetFirstBuffer(IndexScanDesc pcr_scan, ScanDirection dir, BTScanInsertData* inskey,
                                            bool* need_to_go_back, BlockNumber pcr_start_blk)
{
    Buffer buf;
    Relation pcr_rel = pcr_scan->indexRelation;
    BTScanOpaque pcr_so = (BTScanOpaque)pcr_scan->opaque;
    int thread_id = (int)(u_sess->stream_cxt.smp_id);
    if (ScanDirectionIsForward(dir)) {
        if (thread_id == 0) {
            buf = UBTreePCRGetBeginParallelScanBuf(pcr_scan, dir, inskey, need_to_go_back);
        } else {
            buf = _bt_getbuf(pcr_rel, pcr_start_blk, BT_READ);
        }
    } else {
        if (thread_id == 0) {
            if (pcr_so->numberOfKeys == 0) {
                BlockNumber end_block = pcr_scan->btps_end_block;
                buf = UBTreePCRGetEndPoint(pcr_rel, 0, true);
            } else {
                buf = UBTreePCRGetBeginParallelScanBuf(pcr_scan, dir, inskey, need_to_go_back);
            }
        } else {
            buf = _bt_getbuf(pcr_rel, pcr_start_blk, BT_READ);
            buf = _bt_walk_left(pcr_rel, buf, pcr_scan->btps_end_block);
        }
    }
    ereport(LOG,
            (errmsg("pcr ubtree parallel scan oid %u, thread id %u begin with block %u end with %u (4294967295 meas "
                    "InvalidBlockNumer), plan nodeid %u.",
                    pcr_rel->rd_id, u_sess->stream_cxt.smp_id, pcr_start_blk, pcr_scan->btps_end_block,
                    pcr_scan->plan_nodeid)));
    if (thread_id > 0) {
        if (!BufferIsValid(buf)) {
            PredicateLockRelation(pcr_rel, pcr_scan->xs_snapshot);
            return InvalidBuffer;
        } else {
            PredicateLockPage(pcr_rel, BufferGetBlockNumber(buf), pcr_scan->xs_snapshot);
        }
    }
    ereport(DEBUG2, (errmodule(MOD_INDEX),
                     errmsg("btree parallel scan oid %u, thread id %u start with block %u buf %d paln nodeid %u.",
                            pcr_rel->rd_id, u_sess->stream_cxt.smp_id,
                            buf == InvalidBuffer ? 0 : BufferGetBlockNumber(buf), buf, pcr_scan->plan_nodeid)));
    return buf;
}

/*
 * @brief  UBTreePCRParallelFirstExecScan
 *  Perform a scan in the current scan interval
 * @param  pcr_scan             IndexScanDesc
 * @param  dir                  Scanning direction
 * @param  inskey               BTScanInsertData
 * @param  need_to_bo_back      need to take a step back
 * @param  pcr_start_blk        Start scan block number of current thread
 * @return bool                 returns true if there is no error, false otherwise
 */
bool UBTreePCRParallelFirstExecScan(IndexScanDesc pcr_scan, ScanDirection dir, BTScanInsertData* inskey,
                                    bool* need_to_bo_back, BlockNumber pcr_start_blk)
{
    BTScanOpaque pcr_so = (BTScanOpaque)pcr_scan->opaque;
    int thread_id = (int)(u_sess->stream_cxt.smp_id);
    pcr_so->currPos.buf =
        UBTreePCRParallelFirstGetFirstBuffer(pcr_scan, dir, inskey, need_to_bo_back, pcr_start_blk);
    if (pcr_so->currPos.buf == InvalidBuffer) {
        return false;
    }
    if (ScanDirectionIsForward(dir) && pcr_scan->btps_end_block == BufferGetBlockNumber(pcr_so->currPos.buf)) {
        _bt_relbuf(pcr_scan->indexRelation, pcr_so->currPos.buf);
        pcr_so->currPos.buf = InvalidBuffer;
        return false;
    }
    /* init moreRight/modeLeft for scan direction */
    pcr_so->currPos.moreRight = (ScanDirectionIsForward(dir)) ? true : false;
    pcr_so->currPos.moreLeft = (ScanDirectionIsForward(dir)) ? false : true;
    pcr_so->markItemIndex = -1;
    pcr_so->numKilled = 0;
    if (pcr_so->numberOfKeys == 0) {
        inskey = NULL;
        *need_to_bo_back = false;
    }
    if (!UBTreePCRReadPage(pcr_scan, dir, inskey, *need_to_bo_back)) {
        if (!UBTreePCRParallelSteppage(pcr_scan, dir)) {
            return false;
        }
    }
    /* unlock the current page, but held the pin */
    LockBuffer(pcr_so->currPos.buf, BUFFER_LOCK_UNLOCK);
    BTScanPosItem* curr_item = &pcr_so->currPos.items[pcr_so->currPos.itemIndex];
    pcr_scan->xs_ctup.t_self = curr_item->heapTid;
    pcr_scan->xs_recheck_itup = false;
    if (pcr_scan->xs_want_itup) {
        pcr_scan->xs_itup = (IndexTuple)(pcr_so->currTuples + curr_item->tupleOffset);
    }
    if (pcr_scan->xs_want_ext_oid && GPIScanCheckPartOid(pcr_scan->xs_gpi_scan, curr_item->partitionOid)) {
        GPISetCurrPartOid(pcr_scan->xs_gpi_scan, curr_item->partitionOid);
    }
    if (pcr_scan->xs_want_bucketid && cbi_scan_need_change_bucket(pcr_scan->xs_cbi_scan, curr_item->bucketid)) {
        cbi_set_bucketid(pcr_scan->xs_cbi_scan, curr_item->bucketid);
    }
    return true;
}

/*
 * @brief  UBTreePCRParallelFirst
 *  Find the first item in a parallel index scan, and mark the start/end block of this thread
 *  during parallel index scanning.
 *      If DOP of current scan is not 1, the index scan should be paralleled.
 *      In the scanning direction, we need to pay attention to the start block and end block
 *      of current thread.
 *      If the end block was marked as InvalidBlockerNumber, the thread would scan until
 *      the last block is met.
 *      It should be noticed that, not all threads will actually do the scanning, some of the thread
 *      may not get any blocks for inappropriate DOP value. For the case, the thread will ust resturn.
 * @param  pcr_scan             IndexScanDesc
 * @param  dir                  Scanning direction
 * @return bool                 returns true if there is no error, false otherwise
 */
bool UBTreePCRParallelFirst(IndexScanDesc pcr_scan, ScanDirection dir)
{
    Relation pcr_rel = pcr_scan->indexRelation;
    BlockNumber real_blocks = RelationGetNumberOfBlocksInFork(pcr_rel, MAIN_FORKNUM, false);
    if (real_blocks <= 1) {
        return false;
    }
    BTScanOpaque pcr_so = (BTScanOpaque)pcr_scan->opaque;
    uint32 thread_id = u_sess->stream_cxt.smp_id;
    int index = 0;
    int curr_off_start = -1;
    int node_interval = _bt_get_node_interval(pcr_scan->dop);
    bool need_to_go_back = false;
    bool res;
    BTScanInsert inskey = NULL;
    BlockNumber pcr_start_blk = InvalidBlockNumber;
    StreamNodeGroup* stream_nodegroup = u_sess->stream_cxt.global_obj;
    pthread_mutex_t* mutex = stream_nodegroup->GetIndexSmpMutex();
    if (thread_id == 0) {
        PthreadMutexLock(t_thrd.utils_cxt.ThreadRootResourceOwner, mutex);
        {
            index = _bt_find_parallel_divd(stream_nodegroup->parallel_indexscan_map, pcr_rel->rd_id,
                                           stream_nodegroup->parallel_indexscan_size);
            MemoryContext old_mem_context = MemoryContextSwitchTo(stream_nodegroup->m_streamRuntimeContext);
            if (index != -1) {
                _bt_parallel_reallocat_shared_memory(stream_nodegroup, index, curr_off_start, node_interval);
            } else {
                _bt_parallel_allocat_shared_memory(pcr_rel, stream_nodegroup, index, curr_off_start, node_interval);
            }
            MemoryContextSwitchTo(old_mem_context);
        }
        PthreadMutexUnlock(t_thrd.utils_cxt.ThreadRootResourceOwner, mutex);
    }
    if (pcr_so->numberOfKeys > 0) {
        inskey = (BTScanInsertData*)palloc0(sizeof(BTScanInsertData));
        if (inskey == NULL) {
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("memory is temporarily unavailable for stack.")));
        }
    }
    Buffer begin_buf = UBTreePCRGetBeginParallelScanBuf(pcr_scan, dir, inskey, &need_to_go_back);
    if (thread_id == 0) {
        res = UBTreePCRParallelFirstThread0Proc(pcr_scan, dir, begin_buf, curr_off_start, index, pcr_start_blk,
                                                stream_nodegroup);
    } else {
        if (BufferIsValid(begin_buf)) {
            _bt_relbuf(pcr_rel, begin_buf);
        }
        res = _bt_parallel_first_threadn_proc(pcr_scan, dir, curr_off_start, pcr_start_blk, stream_nodegroup);
    }
    if (res) {
        res = UBTreePCRParallelFirstExecScan(pcr_scan, dir, inskey, &need_to_go_back, pcr_start_blk);
    }
    if (inskey != NULL) {
        pfree_ext(inskey);
    }
    return res;
}

/*
 * @brief  UBTreePCRParallelNext
 *  Get the next item on parallel index scan
 *  call parallel_step_page to get next page.
 * @param  pcr_scan     IndexScanDesc
 * @param  dir          Scanning direction
 * @return bool         returns true if there is no error, false otherwise.
 */
bool UBTreePCRParallelNext(IndexScanDesc pcr_scan, ScanDirection dir)
{
    BTScanOpaque pcr_so = (BTScanOpaque)pcr_scan->opaque;
    /*
     * Go to the next tuple on the current page,
     * otherwise if thers is no tuple, go to the next page with the data.
     */
    if (ScanDirectionIsForward(dir)) {
        if (++pcr_so->currPos.itemIndex > pcr_so->currPos.lastItem) {
            /* before applying step_page, we must acquire lock */
            Assert(BufferIsValid(pcr_so->currPos.buf));
            LockBuffer(pcr_so->currPos.buf, BT_READ);
            if (!UBTreePCRParallelSteppage(pcr_scan, dir)) {
                return false;
            }
            ereport(
                DEBUG2,
                (errmodule(MOD_INDEX),
                 errmsg(
                     "pcr ubtree forward index parallel scan oid %u, thread id %u deal with block %u paln nodeid %u.",
                     pcr_scan->indexRelation->rd_id, u_sess->stream_cxt.smp_id,
                     BufferGetBlockNumber(pcr_so->currPos.buf), pcr_scan->plan_nodeid)));
            /* Drop the lock, but not pin, on the new page */
            LockBuffer(pcr_so->currPos.buf, BUFFER_LOCK_UNLOCK);
        }
    } else {
        if (--pcr_so->currPos.itemIndex < pcr_so->currPos.firstItem) {
            /* before applying step_page, we must acquire lock */
            Assert(BufferIsValid(pcr_so->currPos.buf));
            LockBuffer(pcr_so->currPos.buf, BT_READ);
            if (!UBTreePCRParallelSteppage(pcr_scan, dir)) {
                return false;
            }
            ereport(DEBUG2,
                    (errmodule(MOD_INDEX), errmsg("pcr ubtree backforward index parallel scan oid %u, thread id %u "
                                                  "deal with block %u paln nodeid %u.",
                                                  pcr_scan->indexRelation->rd_id, u_sess->stream_cxt.smp_id,
                                                  BufferGetBlockNumber(pcr_so->currPos.buf), pcr_scan->plan_nodeid)));
            /* on the new page. we drop the lock, but held the pin */
            LockBuffer(pcr_so->currPos.buf, BUFFER_LOCK_UNLOCK);
        }
    }

    /* OK, itemIndex says what to return */
    BTScanPosItem* para_curr_item = &pcr_so->currPos.items[pcr_so->currPos.itemIndex];
    pcr_scan->xs_ctup.t_self = para_curr_item->heapTid;
    pcr_scan->xs_recheck_itup = false;
    if (pcr_scan->xs_want_itup) {
        pcr_scan->xs_itup = (IndexTuple)(pcr_so->currTuples + para_curr_item->tupleOffset);
    }
    if (pcr_scan->xs_want_ext_oid && GPIScanCheckPartOid(pcr_scan->xs_gpi_scan, para_curr_item->partitionOid)) {
        GPISetCurrPartOid(pcr_scan->xs_gpi_scan, para_curr_item->partitionOid);
    }
    if (pcr_scan->xs_want_bucketid && cbi_scan_need_change_bucket(pcr_scan->xs_cbi_scan, para_curr_item->bucketid)) {
        cbi_set_bucketid(pcr_scan->xs_cbi_scan, para_curr_item->bucketid);
    }
    return true;
}

/*
 * @brief  UBTreePCRParallelStepPageForward
 *  Move right with data to the next page
 * @param  pcr_scan     IndexScanDesc
 * @param  dir          Scanning direction
 * @return bool         returns true if there is no error, false otherwise.
 */
bool UBTreePCRParallelStepPageForward(IndexScanDesc pcr_scan, ScanDirection dir)
{
    Relation index_rel = pcr_scan->indexRelation;
    BTScanOpaque pcr_so = (BTScanOpaque)pcr_scan->opaque;
    BlockNumber blkno = pcr_so->currPos.nextPage;
    for (;;) {
        _bt_relbuf(index_rel, pcr_so->currPos.buf);
        pcr_so->currPos.buf = InvalidBuffer;
        /* reach end, return */
        if (pcr_scan->btps_end_block != InvalidBlockNumber && blkno == pcr_scan->btps_end_block) {
            return false;
        }
        if (blkno == P_NONE || !pcr_so->currPos.moreRight) {
            ereport(DEBUG1, (errmodule(MOD_INDEX),
                             errmsg("index parallel scan reach and thread id: %d.", u_sess->stream_cxt.smp_id)));
            return false;
        }
        CHECK_FOR_INTERRUPTS();
        /* step right */
        pcr_so->currPos.buf = _bt_getbuf(index_rel, blkno, BT_READ);
        /* check for deleted page */
        Page page = BufferGetPage(pcr_so->currPos.buf);
        UBTPCRPageOpaque opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
        if (!P_IGNORE(opaque)) {
            PredicateLockPage(index_rel, blkno, pcr_scan->xs_snapshot);
            bool ret = UBTreePCRReadPage(pcr_scan, dir);
            if (ret) {
                break;
            }
        }
        blkno = opaque->btpo_next;
    }
    return true;
}

/*
 * @brief  UBTreePCRParallelStepPageBackforward
 *  Move Left with data to the next page, which is much more complicated than moving right because when we want to
 *  access it, the page to our left may split, plus the pgae we get may be deleted after we leave.
 * @param  pcr_scan     IndexScanDesc
 * @param  dir          Scanning direction
 * @return bool         returns true if there is no error, false otherwise.
 */
bool UBTreePCRParallelStepPageBackforward(IndexScanDesc pcr_scan, ScanDirection dir)
{
    Relation index_rel = pcr_scan->indexRelation;
    BTScanOpaque pcr_so = (BTScanOpaque)pcr_scan->opaque;
    pcr_so->currPos.moreRight = true;
    for (;;) {
        CHECK_FOR_INTERRUPTS();
        if (pcr_so->currPos.moreLeft) {
            Buffer tmp_buf = pcr_so->currPos.buf;
            pcr_so->currPos.buf = InvalidBuffer;
            pcr_so->currPos.buf = _bt_walk_left(index_rel, tmp_buf, pcr_scan->btps_end_block);
            tmp_buf = InvalidBuffer;
            if (InvalidBuffer == pcr_so->currPos.buf) {
                return false;
            }
            Page page = BufferGetPage(pcr_so->currPos.buf);
            UBTPCRPageOpaque opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
            if (!P_IGNORE(opaque)) {
                PredicateLockPage(index_rel, BufferGetBlockNumber(pcr_so->currPos.buf), pcr_scan->xs_snapshot);
                bool ret = UBTreePCRReadPage(pcr_scan, dir);
                if (ret) {
                    break;
                }
            }
        } else {
            /* Done if we know there are no matching keys to the left */
            _bt_relbuf(index_rel, pcr_so->currPos.buf);
            pcr_so->currPos.buf = InvalidBuffer;
            return false;
        }
    }
    return true;
}

/*
 * @brief  UBTreePCRParallelSteppage
 *  Go to the next page for parallel index scan
 *      The whole process is just like ordinary step_page, except that we just stop read for current thread if end block
 *      is met.
 * @param  pcr_scan     IndexScanDesc
 * @param  dir          Scanning direction
 * @return bool         returns true if there is no error, false otherwise.
 */
bool UBTreePCRParallelSteppage(IndexScanDesc pcr_scan, ScanDirection dir)
{
    BTScanOpaque pcr_so = (BTScanOpaque)pcr_scan->opaque;
    Assert(BufferIsValid(pcr_so->currPos.buf));
    if (pcr_so->markItemIndex > 0) {
        IncrBufferRefCount(pcr_so->currPos.buf);
        errno_t rc = memcpy_s(
            &pcr_so->markPos, offsetof(BTScanPosData, items[1]) + pcr_so->currPos.lastItem * sizeof(BTScanPosItem),
            &pcr_so->currPos, offsetof(BTScanPosData, items[1]) + pcr_so->currPos.lastItem * sizeof(BTScanPosItem));
        securec_check(rc, "", "");
        if (pcr_so->markTuples) {
            rc = memcpy_s(pcr_so->markTuples, (size_t)pcr_so->currPos.nextTupleOffset, pcr_so->currTuples,
                          (size_t)pcr_so->currPos.nextTupleOffset);
            securec_check(rc, "", "");
        }
        pcr_so->markPos.itemIndex = pcr_so->markItemIndex;
        pcr_so->markItemIndex= -1;
    }
    bool res = true;
    if (ScanDirectionIsForward(dir)) {
        res = UBTreePCRParallelStepPageForward(pcr_scan, dir);
    } else {
        res = UBTreePCRParallelStepPageBackforward(pcr_scan, dir);
    }
    return res;
}