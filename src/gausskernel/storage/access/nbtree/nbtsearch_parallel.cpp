/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
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
 * nbtsearch_parallel.cpp
 *
 *
 *
 * IDENTIFICATION
 *        src\gausskernel\storage\access\nbtree\nbtsearch_parallel.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/nbtree.h"
#include "miscadmin.h"
#include "storage/predicate.h"
#include "access/tableam.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "catalog/pg_opfamily.h"
#include "access/parallel_indexscan_core.h"

/*
 * @brief find_next_block
 *  Find the next block according to the siling pointer (btpo_next/btpo_prev).
 * @param bt_scan                       IndexScanDesc
 * @param dir                           Scanning direction
 * @param current_block                 Start block number of the current thread.
 * @param num_blocks                    Estimated number of lef nodes scanned by each thread
 * @return BlockNumber                  End block number of the current thread.
 */
BlockNumber find_next_block(IndexScanDesc bt_scan, ScanDirection dir, BlockNumber current_block, int num_blocks)
{
    if (current_block == InvalidBlockNumber || current_block == 0) {
        return 0;
    }
    int count = 1;
    BlockNumber bt_next = current_block;
    BlockNumber bt_now = current_block;
    Relation bt_rel = bt_scan->indexRelation;
    Buffer current_buf = _bt_getbuf(bt_rel, current_block, BT_READ);
    BTPageOpaqueInternal opaque = (BTPageOpaqueInternal)PageGetSpecialPointer(BufferGetPage(current_buf));
    bt_now = bt_next;
    if (ScanDirectionIsForward(dir)) {
        bt_next = opaque->btpo_next;
    } else {
        bt_next = opaque->btpo_prev;
    }
    _bt_relbuf(bt_rel, current_buf);
    Buffer buf_tmp = InvalidBuffer;
    int access = BT_READ;
    while (count <= num_blocks) {
        access = (count == num_blocks ? BT_WRITE : BT_READ);
        buf_tmp = _bt_getbuf(bt_rel, bt_next, access);
        opaque = (BTPageOpaqueInternal)PageGetSpecialPointer(BufferGetPage(buf_tmp));
        bool empty_page = false;
        if (count == num_blocks && !P_RIGHTMOST(opaque) && !P_LEFTMOST(opaque)) {
            OffsetNumber max_off = PageGetMaxOffsetNumber(BufferGetPage(buf_tmp));
            empty_page = (P_FIRSTKEY > max_off);
        }
        if (ScanDirectionIsForward(dir)) {
            if (P_IGNORE(opaque) || empty_page) {
                _bt_relbuf(bt_rel, buf_tmp);
                bt_now = bt_next;
                bt_next = opaque->btpo_next;
                continue;
            }
            if (P_RIGHTMOST(opaque)) {
                bt_now = opaque->btpo_next;
                _bt_relbuf(bt_rel, buf_tmp);
                return bt_now;
            }
            bt_now = bt_next;
            bt_next = opaque->btpo_next;
        } else {
            if (P_IGNORE(opaque) || empty_page) {
                _bt_relbuf(bt_rel, buf_tmp);
                bt_now = bt_next;
                bt_next = opaque->btpo_prev;
                continue;
            }
            if (P_LEFTMOST(opaque)) {
                bt_now = opaque->btpo_prev;
                _bt_relbuf(bt_rel, buf_tmp);
                return bt_now;
            }
            bt_now = bt_next;
            bt_next = opaque->btpo_prev;
        }
        if (count == num_blocks) {
            opaque->btpo_flags |= BTP_PARALLEL_SCAN_END;
            if (((BTPageOpaque)opaque)->xact < bt_scan->xs_snapshot->xmin) {
                ((BTPageOpaque)opaque)->xact = bt_scan->xs_snapshot->xmin;
            }
            MarkBufferDirtyHint(buf_tmp, true);
        }
        _bt_relbuf(bt_rel, buf_tmp);
        count++;
    }
    return bt_now;
}

/*
 * @brief _bt_get_parallel_scan_total_blocks
 *  Calculate the number of block that meet the scankey reuqirement.
 * @param bt_scan                       IndexScanDesc
 * @param dir                           Scanning direction
 * @param bt_start_blk                  Start scan block number
 * @return int                          Number of all block that meet the conditions
 */
int _bt_get_parallel_scan_total_blocks(IndexScanDesc bt_scan, ScanDirection dir, BlockNumber bt_start_blk)
{
    if (bt_start_blk == InvalidBuffer) {
        return 0;
    }
    Relation bt_rel = bt_scan->indexRelation;
    BTScanOpaque bt_para_so = (BTScanOpaque)bt_scan->opaque;
    int total_blocks = 1;
    bool continuescan = true;
    BlockNumber bt_next = bt_start_blk;
    while (true) {
        CHECK_FOR_INTERRUPTS();
        Buffer tmp_buf = _bt_getbuf(bt_rel, bt_next, BT_READ);
        BTPageOpaqueInternal opaque = (BTPageOpaqueInternal)PageGetSpecialPointer(BufferGetPage(tmp_buf));
        if (ScanDirectionIsForward(dir)) {
            bt_next = opaque->btpo_next;
            if (P_RIGHTMOST(opaque)) {
                _bt_relbuf(bt_rel, tmp_buf);
                break;
            }
        } else {
            bt_next = opaque->btpo_prev;
            if (P_LEFTMOST(opaque)) {
                _bt_relbuf(bt_rel, tmp_buf);
                break;
            }
        }
        _bt_relbuf(bt_rel, tmp_buf);
        if (bt_para_so->numberOfKeys > 0) {
            tmp_buf = _bt_getbuf(bt_rel, bt_next, BT_READ);
            Page cur_page = BufferGetPage(tmp_buf);
            opaque = (BTPageOpaqueInternal)PageGetSpecialPointer(cur_page);
            if (ScanDirectionIsForward(dir)) {
                if (P_FIRSTDATAKEY(opaque) > PageGetMaxOffsetNumber(cur_page)) {
                    _bt_relbuf(bt_rel, tmp_buf);
                    total_blocks++;
                    continue;
                }
                _bt_checkkeys(bt_scan, cur_page, P_FIRSTDATAKEY(opaque), dir, &continuescan, false, false);
            } else {
                _bt_checkkeys(bt_scan, cur_page, PageGetMaxOffsetNumber(cur_page), dir, &continuescan, false, false);
            }
            if (!continuescan) {
                _bt_relbuf(bt_rel, tmp_buf);
                break;
            }
            _bt_relbuf(bt_rel, tmp_buf);
        }
        total_blocks++;
    }
    return total_blocks;
}

/*
 * @brief _bt_get_inskey_scankey_without_rowheader
 *  Initialize inskey->scankey when cur_>sk_flags & SK_ROW_HEADER is 0.
 * @param cur                           No. i startKey
 * @param bt_rel                        relation of the current index
 * @param i                             Number of startkey iterations
 * @return void
 */
void _bt_get_inskey_scankey_without_rowheader(ScanKey cur, Relation bt_rel, int i, BTScanInsertData* inskey)
{
    if (cur->sk_subtype == bt_rel->rd_opcintype[i] || cur->sk_subtype == InvalidOid) {
        FmgrInfo* procinfo = index_getprocinfo(bt_rel, cur->sk_attno, BTORDER_PROC);
        ScanKeyEntryInitializeWithInfo(inskey->scankeys + i, cur->sk_flags, cur->sk_attno, InvalidStrategy,
                                       cur->sk_subtype, cur->sk_collation, procinfo, cur->sk_argument);
    } else {
        RegProcedure cmp_proc;
        if (bt_rel->rd_opfamily[i] == INTEGER_BTREE_FAM_OID && bt_rel->rd_opcintype[i] == INT8OID &&
            cur->sk_subtype == INT4OID) {
            cmp_proc = F_BTINT84CMP;
        } else {
            RegProcedure cmp_proc;
            cmp_proc =
                get_opfamily_proc(bt_rel->rd_opfamily[i], bt_rel->rd_opcintype[i], cur->sk_subtype, BTORDER_PROC);
            if (SECUREC_UNLIKELY(!RegProcedureIsValid(cmp_proc)))
                ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                                errmsg("missing support function %d(%u,%u) for attribute %d of index \"%s\"",
                                       BTORDER_PROC, bt_rel->rd_opcintype[i], cur->sk_subtype, cur->sk_attno,
                                       RelationGetRelationName(bt_rel))));
            ScanKeyEntryInitialize(inskey->scankeys + i, cur->sk_flags, cur->sk_attno, InvalidStrategy, cur->sk_subtype,
                                   cur->sk_collation, cmp_proc, cur->sk_argument);
        }
    }
    return;
}

/*
 * @brief _bt_get_goback_need_to_next
 *  If the number of start conditions is 0, the start buffer and offset are returned.
 * @param dir                           Scanning direction
 * @param *need_to_go_back              need to take a step back
 * @param *need_to_next_key             proceed to the next step
 * @param strat_total                   different scanning conditions
 * @return bool                         returns true if there is no error, false otherwise
 */
bool _bt_get_goback_need_to_next(ScanDirection dir, bool* need_to_go_back, bool* need_to_next_key,
                                 StrategyNumber strat_total)
{
    switch (strat_total) {
        case BTGreaterEqualStrategyNumber:
            break;
        case BTGreaterStrategyNumber:
            *need_to_next_key = true;
            break;
        case BTEqualStrategyNumber:
            *need_to_go_back = (ScanDirectionIsBackward(dir)) ? true : false;
            *need_to_next_key = (ScanDirectionIsBackward(dir)) ? true : false;
            break;
        case BTLessEqualStrategyNumber:
            *need_to_go_back = true;
            *need_to_next_key = true;
            break;
        case BTLessStrategyNumber:
            *need_to_go_back = true;
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("Unrecognized strategy number:%d.", strat_total)));
            return false;
    }
    return true;
}

/*
 * @brief _bt_init_inskey
 *  Initialize the inskey.
 * @param *inskey                       BTScanInsertData
 * @param bt_rel                        relation of the current index
 * @param nextkey                       proceed to the next step
 * @param keys_count                    number of startkeys
 * @return void
 */
void _bt_init_inskey(BTScanInsertData* inskey, Relation bt_rel, bool nextkey, int keys_count)
{
    btree_meta_version(bt_rel, &inskey->heapkeyspace, &inskey->allequalimage);
    inskey->anynullkeys = false;
    inskey->nextkey = nextkey;
    inskey->pivotsearch = false;
    inskey->scantid = NULL;
    inskey->keysz = keys_count;
    return;
}

/*
 * @brief  _bt_get_begin_parallel_scan_buf
 *  Obtains the start scanning buffer.
 * @param  bt_scan              IndexScanDesc
 * @param  dir                  Scanning direction
 * @param  *offnum              start offset
 * @param  inskey               BTsacnInsertData
 * @param  has_init_inskey      whether to initialize the key
 * @return Buffer               return the start scanning buffer
 */
Buffer _bt_get_begin_parallel_scan_buf(IndexScanDesc bt_scan, ScanDirection dir, OffsetNumber* offnum,
                                       BTScanInsertData& inskey, bool* has_init_inskey)
{
    Relation bt_rel = bt_scan->indexRelation;
    Buffer buf;
    bool res = false;
    ScanKey start_keys[INDEX_MAX_KEYS] = {0};
    StrategyNumber strat_total = BTEqualStrategyNumber;
    int keys_count = _bt_get_start_keys(bt_scan, dir, start_keys, strat_total);
    if (keys_count == 0) {
        return _bt_get_first_buf_without_scankey(bt_scan, dir, offnum);
    }
    Assert(keys_count <= INDEX_MAX_KEYS);
    for (int i = 0; i < keys_count; i++) {
        ScanKey cur = start_keys[i];
        Assert(cur->sk_attno == i + 1);
        if (cur->sk_flags & SK_ROW_HEADER) {
            bool continue_loop = true;
            res = _bt_get_inskey_scankey_with_rowheader(cur, &inskey, strat_total, keys_count, i, continue_loop);
            if (!res) {
                return InvalidBuffer;
            }
            if (!continue_loop) {
                break;
            }
        } else {
            _bt_get_inskey_scankey_without_rowheader(cur, bt_rel, i, &inskey);
        }
    }
    bool nextkey = false;
    bool goback = false;
    res = _bt_get_goback_need_to_next(dir, &goback, &nextkey, strat_total);
    if (!res) {
        return InvalidBuffer;
    }
    _bt_init_inskey(&inskey, bt_rel, nextkey, keys_count);
    if (has_init_inskey != nullptr) {
        *has_init_inskey = true;
    }
    BlockNumber end_block = bt_scan->btps_end_block;
    (void)_bt_search(bt_rel, &inskey, &buf, BT_READ, false, end_block);
    if (!BufferIsValid(buf)) {
        PredicateLockRelation(bt_rel, bt_scan->xs_snapshot);
        return InvalidBuffer;
    } else {
        PredicateLockPage(bt_rel, BufferGetBlockNumber(buf), bt_scan->xs_snapshot);
    }
    int posting_off = 0;
    *offnum = _bt_binsrch(bt_rel, &inskey, buf, &posting_off);
    if (goback) {
        *offnum = OffsetNumberPrev(*offnum);
    }
    return buf;
}

/*
 * @brief  _bt_parallel_first_thread0_proc
 *  In ther parallel_first func, thread 0 divides the scan blocks and records them to the shared memory,
 *  and assigns the scan start and end blocks of thread 0.
 * @param  bt_scan          IndexScanDesc
 * @param  dir              Scanning direction
 * @param  curr_off_start   2D Offset in Shared Memory
 * @param  index            One-dimensional index of the current index in the shared memory
 * @param  bt_start_blk     Start scan block number of thread 0
 * @param  stream_nodegroup StreamNodeGroup
 * @param  offnum           Start offset
 * @return bool             returns true if there is no error, false otherwise
 */
bool _bt_parallel_first_thread0_proc(IndexScanDesc bt_scan, ScanDirection dir, int curr_off_start, int index,
                                     BlockNumber& bt_start_blk, StreamNodeGroup* stream_nodegroup, OffsetNumber* offnum)
{
    Relation bt_rel = bt_scan->indexRelation;
    int curr_th0_start = curr_off_start + OFFSET_START_BASE;
    int curr_th0_end = curr_off_start + OFFSET_END_BASE;
    BTScanOpaque bt_para_so = (BTScanOpaque)bt_scan->opaque;
    BlockNumber blkno = InvalidBuffer;
    BTScanInsertData inskey = {0};
    Buffer begin_buf = _bt_get_begin_parallel_scan_buf(bt_scan, dir, offnum, inskey);
    if (BufferIsValid(begin_buf)) {
        blkno = BufferGetBlockNumber(begin_buf);
        _bt_relbuf(bt_rel, begin_buf);
    } else {
        bt_para_so->currPos.buf = InvalidBuffer;
        return false;
    }
    int real_scan_blocks = _bt_get_parallel_scan_total_blocks(bt_scan, dir, blkno);
    int num_blocks = (real_scan_blocks + (bt_scan->dop - 1)) / bt_scan->dop;

    pthread_mutex_t* mutex = stream_nodegroup->GetIndexSmpMutex();
    pthread_cond_t* cond = stream_nodegroup->GetIndexSmpCond();
    ereport(LOG, (errmsg("btree parallel scan oid %u, dop %d, %d blocks per thread, total blocks %d, real scan blocks "
                         "%u, plan nodeid %u.",
                         bt_rel->rd_id, bt_scan->dop, num_blocks, RelationGetNumberOfBlocks(bt_rel), real_scan_blocks,
                         bt_scan->plan_nodeid)));
    MemoryContext old_mem_context = MemoryContextSwitchTo(stream_nodegroup->m_streamRuntimeContext);
    PthreadMutexLock(t_thrd.utils_cxt.ThreadRootResourceOwner, mutex);
    stream_nodegroup->parallel_indexscan_map[index][curr_off_start + OFFSET_START_BASE] = blkno;
    PthreadMutexUnlock(t_thrd.utils_cxt.ThreadRootResourceOwner, mutex);
    for (int i = OFFSET_START_BASE; i < bt_scan->dop; i++) {
        PthreadMutexLock(t_thrd.utils_cxt.ThreadRootResourceOwner, mutex);
        uint32 current_block = stream_nodegroup->parallel_indexscan_map[index][curr_off_start + i];
        PthreadMutexUnlock(t_thrd.utils_cxt.ThreadRootResourceOwner, mutex);
        BlockNumber bt_next = find_next_block(bt_scan, dir, current_block, num_blocks);
        int next_thread_start_offset = curr_off_start + i + OFFSET_START_BASE;
        PthreadMutexLock(t_thrd.utils_cxt.ThreadRootResourceOwner, mutex);
        stream_nodegroup->parallel_indexscan_map[index][next_thread_start_offset] =
            bt_next == 0 ? InvalidBlockNumber : bt_next;
        PthreadMutexUnlock(t_thrd.utils_cxt.ThreadRootResourceOwner, mutex);
    }
    PthreadMutexLock(t_thrd.utils_cxt.ThreadRootResourceOwner, mutex);
    stream_nodegroup->parallel_indexscan_map[index][curr_off_start + bt_scan->dop + OFFSET_START_BASE] =
        InvalidBlockNumber;
    stream_nodegroup->parallel_indexscan_map[index][curr_off_start] = bt_scan->plan_nodeid;
    pg_memory_barrier();
    bt_start_blk = stream_nodegroup->parallel_indexscan_map[index][curr_th0_start];
    bt_scan->btps_end_block = stream_nodegroup->parallel_indexscan_map[index][curr_th0_end];
    pthread_cond_broadcast(cond);
    PthreadMutexUnlock(t_thrd.utils_cxt.ThreadRootResourceOwner, mutex);
    MemoryContextSwitchTo(old_mem_context);
    return true;
}

/*
 * @brief _bt_parallel_first_get_first_buffer
 *  Get start scan block of the current thread.
 * @param  bt_scan          IndexScanDesc
 * @param  dir              Scanning direction
 * @param  bt_start_blk     Start scan block number of current thread
 * @param  offnum           Start offset
 * @param  inskey           BTScanInsertData
 * @param  has_init_inskey  has init key
 * @return Buffer           Start scan block of the current thread
 */
Buffer _bt_parallel_first_get_first_buffer(IndexScanDesc bt_scan, ScanDirection dir, BlockNumber bt_start_blk,
                                           OffsetNumber* offnum, BTScanInsertData& inskey, bool* has_init_inskey)
{
    Buffer buf;
    Relation bt_rel = bt_scan->indexRelation;
    BTScanOpaque bt_para_so = (BTScanOpaque)bt_scan->opaque;
    int thread_id = (int)(u_sess->stream_cxt.smp_id);
    if (ScanDirectionIsForward(dir)) {
        if (thread_id == 0) {
            buf = _bt_get_begin_parallel_scan_buf(bt_scan, dir, offnum, inskey, has_init_inskey);
        } else {
            buf = _bt_getbuf(bt_rel, bt_start_blk, BT_READ);
        }
    } else {
        if (thread_id == 0) {
            if (bt_para_so->numberOfKeys == 0) {
                BlockNumber end_block = bt_scan->btps_end_block;
                buf = _bt_get_endpoint(bt_rel, 0, true, end_block);
            } else {
                buf = _bt_get_begin_parallel_scan_buf(bt_scan, dir, offnum, inskey, has_init_inskey);
            }
        } else {
            buf = _bt_getbuf(bt_rel, bt_start_blk, BT_READ);
            buf = _bt_walk_left(bt_rel, buf, bt_scan->btps_end_block);
        }
    }
    ereport(LOG, (errmsg("btree parallel scan oid %u, thread id %u begin with block %u end with %u (4294967295 meas "
                         "InvalidBlockNumer), plan nodeid %u.",
                         bt_rel->rd_id, u_sess->stream_cxt.smp_id, bt_start_blk, bt_scan->btps_end_block,
                         bt_scan->plan_nodeid)));
    if (thread_id > 0) {
        if (!BufferIsValid(buf)) {
            PredicateLockRelation(bt_rel, bt_scan->xs_snapshot);
            return InvalidBuffer;
        } else {
            PredicateLockPage(bt_rel, BufferGetBlockNumber(buf), bt_scan->xs_snapshot);
        }
    }
    ereport(DEBUG2, (errmodule(MOD_INDEX),
                     errmsg("btree parallel scan oid %u, thread id %u start with block %u buf %d paln nodeid %u.",
                            bt_rel->rd_id, u_sess->stream_cxt.smp_id,
                            buf == InvalidBuffer ? 0 : BufferGetBlockNumber(buf), buf, bt_scan->plan_nodeid)));
    return buf;
}

/*
 * @brief _bt_parallel_first_exec_scan
 *  Perform a scan in the current scan interval
 * @param  bt_scan          IndexScanDesc
 * @param  dir              Scanning direction
 * @param  bt_start_blk     Start scan block number of current thread
 * @param  offnum           Start offset
 * @return bool             returns true if there is no error, false otherwise
 */
bool _bt_parallel_first_exec_scan(IndexScanDesc bt_scan, ScanDirection dir, BlockNumber bt_start_blk,
                                  OffsetNumber offnum)
{
    BTScanOpaque bt_para_so = (BTScanOpaque)bt_scan->opaque;
    int thread_id = (int)(u_sess->stream_cxt.smp_id);
    BTScanInsertData inskey = {0};
    bool has_ini_inskey = false;
    bt_para_so->currPos.buf =
        _bt_parallel_first_get_first_buffer(bt_scan, dir, bt_start_blk, &offnum, inskey, &has_ini_inskey);
    if (bt_para_so->currPos.buf == InvalidBuffer) {
        return false;
    }
    if (ScanDirectionIsForward(dir) && bt_scan->btps_end_block == BufferGetBlockNumber(bt_para_so->currPos.buf)) {
        _bt_relbuf(bt_scan->indexRelation, bt_para_so->currPos.buf);
        bt_para_so->currPos.buf = InvalidBuffer;
        return false;
    }
    /* init moreRight/modeLeft for scan direction */
    bt_para_so->currPos.moreRight = (ScanDirectionIsForward(dir)) ? true : false;
    bt_para_so->currPos.moreLeft = (ScanDirectionIsForward(dir)) ? false : true;
    bt_para_so->markItemIndex = -1;
    bt_para_so->numKilled = 0;
    if (bt_para_so->numberOfKeys == 0 || thread_id != 0) {
        BTPageOpaqueInternal opaque =
            (BTPageOpaqueInternal)PageGetSpecialPointer(BufferGetPage(bt_para_so->currPos.buf));
        if (ScanDirectionIsBackward(dir)) {
            offnum = PageGetMaxOffsetNumber(BufferGetPage(bt_para_so->currPos.buf));
        } else if (ScanDirectionIsForward(dir)) {
            /* There could be dead pages to the left, so not this. */
            offnum = P_FIRSTDATAKEY(opaque);
        } else {
            ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("Invalid scan direction: %d", dir)));
            offnum = 0; /* init start anyway */
        }
    }
    if (!_bt_readpage(bt_scan, dir, offnum)) {
        if (!_bt_parallel_steppage(bt_scan, dir)) {
            return false;
        }
    }
    /* unlock the current page, but held the pin */
    LockBuffer(bt_para_so->currPos.buf, BUFFER_LOCK_UNLOCK);

    BTScanPosItem* curr_item = &bt_para_so->currPos.items[bt_para_so->currPos.itemIndex];
    bt_scan->xs_ctup.t_self = curr_item->heapTid;
    if (bt_scan->xs_want_itup) {
        bt_scan->xs_itup = (IndexTuple)(bt_para_so->currTuples + curr_item->tupleOffset);
    }
    if (bt_scan->xs_want_ext_oid && GPIScanCheckPartOid(bt_scan->xs_gpi_scan, curr_item->partitionOid)) {
        GPISetCurrPartOid(bt_scan->xs_gpi_scan, curr_item->partitionOid);
    }
    if (bt_scan->xs_want_bucketid && cbi_scan_need_change_bucket(bt_scan->xs_cbi_scan, curr_item->bucketid)) {
        cbi_set_bucketid(bt_scan->xs_cbi_scan, curr_item->bucketid);
    }
    return true;
}

/*
* @brief _bt_parallel_first
*  Find the first item in a paralled index scan, and mark the start/end block of this thread
*  during paralled index scanning.
*      If DOP of current scan is not 1, the index scan should be parallelled.
*      In the scanning direction, we need to pay attention to the start block and end block
*      of current thread.
*      If the end block was marked as InvalidBlockNumber, the thread would scan util
*      the last block is met.
*      It should be noticed that, not all threads will actually do the scanning, bt_para_some of the thread
*      may not get ana blocks for inapproriate DOP value. For that case, the thread will just resturn.
* The shared momory is allocated as follows for the first time:
* |    32      |    32      |       32      |    32     |    32     | ... |    32     |    32   |
* | index_oid1 | nodeid_num | plan_node_id0 | th0_start | th1_start | ... | thn_start | thn_end |
* | .......... | .......... | .......... .. | ......... | ......... | ... | ......... | ....... |
* | index_oidn | nodeid_num | plan_node_id0 | th0_start | th1_start | ... | thn_start | thn_end |
*
* When a new plan node performs parallel scan, the realloc shared memory is as follows:
* | oidn | nodeid_num | plan_node_id0 | th0_start | ... | thn_end | plan_node_id1 | th0_start | ... | thn_end | ...
*
* In the hashbucket table, the first 16 bits of the paln_node_id record the pland nodeid value, and the last 16
* bits record the bucket id value. |    32      |    32      |       16      |    16     |    32     |    32     |
* ... |    32     |    32   | | index_oidn | nodeid_num | plan_node_id0 | bucket_id | th0_start | th1_start | ... |
* thn_start | thn_end |
*
* @param bt_scan                       IndexScanDesc
* @param dir                           Scanning direction
* @return bool                         returns true if there is no error, false otherwise.
*/
bool _bt_parallel_first(IndexScanDesc bt_scan, ScanDirection dir)
{
    Relation bt_rel = bt_scan->indexRelation;
    BlockNumber real_blocks = RelationGetNumberOfBlocksInFork(bt_rel, MAIN_FORKNUM);
    if (real_blocks <= 1) {
        return false;
    }
    OffsetNumber offnum = InvalidOffsetNumber;
    uint32 thread_id = u_sess->stream_cxt.smp_id;
    int index = 0;
    int curr_off_start = -1;
    int node_interval = _bt_get_node_interval(bt_scan->dop);
    BlockNumber bt_start_blk = InvalidBlockNumber;
    StreamNodeGroup* stream_nodegroup = u_sess->stream_cxt.global_obj;
    pthread_mutex_t* mutex = stream_nodegroup->GetIndexSmpMutex();
    if (thread_id == 0) {
        PthreadMutexLock(t_thrd.utils_cxt.ThreadRootResourceOwner, mutex);
        {
            index = _bt_find_parallel_divd(stream_nodegroup->parallel_indexscan_map, bt_rel->rd_id,
                                           stream_nodegroup->parallel_indexscan_size);
            MemoryContext old_mem_context = MemoryContextSwitchTo(stream_nodegroup->m_streamRuntimeContext);
            if (index != -1) {
                _bt_parallel_reallocat_shared_memory(stream_nodegroup, index, curr_off_start, node_interval);
            } else {
                _bt_parallel_allocat_shared_memory(bt_rel, stream_nodegroup, index, curr_off_start, node_interval);
            }
            MemoryContextSwitchTo(old_mem_context);
        }
        PthreadMutexUnlock(t_thrd.utils_cxt.ThreadRootResourceOwner, mutex);
    }
    bool res = false;
    if (thread_id == 0) {
        res = _bt_parallel_first_thread0_proc(bt_scan, dir, curr_off_start, index, bt_start_blk, stream_nodegroup,
                                              &offnum);
    } else {
        res = _bt_parallel_first_threadn_proc(bt_scan, dir, curr_off_start, bt_start_blk, stream_nodegroup);
    }
    if (res) {
        res = _bt_parallel_first_exec_scan(bt_scan, dir, bt_start_blk, offnum);
    }
    return res;
}

/*
 * @brief  _bt_parallel_next
 *  Get the next item on parallel index scan
 *  call parallel_step_page to get next page.
 * @param  bt_scan      IndexScanDesc
 * @param  dir          Scanning direction
 * @return bool         returns true if there is no error, false otherwise.
 */
bool _bt_parallel_next(IndexScanDesc bt_scan, ScanDirection dir)
{
    BTScanOpaque bt_para_so = (BTScanOpaque)bt_scan->opaque;
    if (ScanDirectionIsForward(dir)) {
        if (++bt_para_so->currPos.itemIndex > bt_para_so->currPos.lastItem) {
            /* We must acquire lock before, applying _bt_steppage */
            Assert(BufferIsValid(bt_para_so->currPos.buf));
            LockBuffer(bt_para_so->currPos.buf, BT_READ);
            if (!_bt_parallel_steppage(bt_scan, dir)) {
                return false;
            }
            ereport(DEBUG2,
                    (errmodule(MOD_INDEX),
                     errmsg("btree forward index parallel scan oid %u, thread id %u deal with block %u paln nodeid %u.",
                            bt_scan->indexRelation->rd_id, u_sess->stream_cxt.smp_id,
                            BufferGetBlockNumber(bt_para_so->currPos.buf), bt_scan->plan_nodeid)));
            /* Drop the lock, but not pin, on the new page */
            LockBuffer(bt_para_so->currPos.buf, BUFFER_LOCK_UNLOCK);
        }
    } else {
        if (--bt_para_so->currPos.itemIndex < bt_para_so->currPos.firstItem) {
            /* We must acquire lock before, applying _bt_steppage */
            Assert(BufferIsValid(bt_para_so->currPos.buf));
            LockBuffer(bt_para_so->currPos.buf, BT_READ);
            if (!_bt_parallel_steppage(bt_scan, dir)) {
                return false;
            }
            ereport(
                DEBUG2,
                (errmodule(MOD_INDEX),
                 errmsg("btree backforward index parallel scan oid %u, thread id %u deal with block %u paln nodeid %u.",
                        bt_scan->indexRelation->rd_id, u_sess->stream_cxt.smp_id,
                        BufferGetBlockNumber(bt_para_so->currPos.buf), bt_scan->plan_nodeid)));
            /* Drop the lock, but not pin, on the new page */
            LockBuffer(bt_para_so->currPos.buf, BUFFER_LOCK_UNLOCK);
        }
    }

    /* OK, itemIndex says what to return */
    BTScanPosItem* para_curr_item = &bt_para_so->currPos.items[bt_para_so->currPos.itemIndex];
    bt_scan->xs_ctup.t_self = para_curr_item->heapTid;
    if (bt_scan->xs_want_itup) {
        bt_scan->xs_itup = (IndexTuple)(bt_para_so->currTuples + para_curr_item->tupleOffset);
    }
    if (bt_scan->xs_want_ext_oid && GPIScanCheckPartOid(bt_scan->xs_gpi_scan, para_curr_item->partitionOid)) {
        GPISetCurrPartOid(bt_scan->xs_gpi_scan, para_curr_item->partitionOid);
    }
    if (bt_scan->xs_want_bucketid && cbi_scan_need_change_bucket(bt_scan->xs_cbi_scan, para_curr_item->bucketid)) {
        cbi_set_bucketid(bt_scan->xs_cbi_scan, para_curr_item->bucketid);
    }
    return true;
}

/*
 * @brief _bt_parallel_steppage
 *  Go to the next page for parallel index scan
 *  The whole process i just like ordiany step_page, except that we just stopo read for current thread if end block
 *  is met.
 * @param  bt_scan      IndexScanDesc
 * @param  dir          Scanning direction
 * @return bool         returns true if there is no error, false otherwise
 */
bool _bt_parallel_steppage(IndexScanDesc bt_scan, ScanDirection dir)
{
    BTScanOpaque bt_para_so = (BTScanOpaque)bt_scan->opaque;
    Relation bt_rel;
    Page page = NULL;
    BTPageOpaqueInternal opaque = NULL;
    /* we must have the buffer pinned and locked */
    Assert(BufferIsValid(bt_para_so->currPos.buf));

    /* Before leaving current page, deal with any killed items */
    if (bt_para_so->numKilled > 0)
        _bt_killitems(bt_scan, true);

    /*
     * Before we modify currPos, make a copy of the page data if there was a
     * mark position that needs it.
     */
    if (bt_para_so->markItemIndex >= 0) {
        /* bump pin on current buffer for assignment to mark buffer */
        IncrBufferRefCount(bt_para_so->currPos.buf);
        errno_t rc = memcpy_s(&bt_para_so->markPos,
                              offsetof(BTScanPosData, items[1]) + bt_para_so->currPos.lastItem * sizeof(BTScanPosItem),
                              &bt_para_so->currPos,
                              offsetof(BTScanPosData, items[1]) + bt_para_so->currPos.lastItem * sizeof(BTScanPosItem));
        securec_check(rc, "", "");
        if (bt_para_so->markTuples) {
            rc = memcpy_s(bt_para_so->markTuples, (size_t)bt_para_so->currPos.nextTupleOffset, bt_para_so->currTuples,
                          (size_t)bt_para_so->currPos.nextTupleOffset);
            securec_check(rc, "", "");
        }
        bt_para_so->markPos.itemIndex = bt_para_so->markItemIndex;
        bt_para_so->markItemIndex = -1;
    }
    bt_rel = bt_scan->indexRelation;
    if (ScanDirectionIsForward(dir)) {
        BlockNumber blkno = bt_para_so->currPos.nextPage;
        bt_para_so->currPos.moreLeft = true;
        for (;;) {
            Buffer cur_buf = bt_para_so->currPos.buf;
            /*
             * Before step to right sibling, keep the pin of origin page to prevent the origin
             * page from begin compressed and merged (such ILM) to its right sibling.
             * The compressed data will be moved to its right sibling, which will casuse repeatly reads.
             */
            LockBuffer(bt_para_so->currPos.buf, BUFFER_LOCK_UNLOCK);
            bt_para_so->currPos.buf = InvalidBuffer;
            /* if we're at end of scan, give up */
            if (bt_scan->btps_end_block != InvalidBlockNumber && blkno == bt_scan->btps_end_block) {
                ReleaseBuffer(cur_buf);
                return false;
            }
            if (blkno == P_NONE || !bt_para_so->currPos.moreRight) {
                ReleaseBuffer(cur_buf);
                ereport(DEBUG1, (errmodule(MOD_INDEX),
                                 errmsg("index parallel scan reach and thread id: %d.", u_sess->stream_cxt.smp_id)));
                return false;
            }
            ReleaseBuffer(cur_buf);
            CHECK_FOR_INTERRUPTS();
            bt_para_so->currPos.buf = _bt_getbuf(bt_rel, blkno, BT_READ);
            page = BufferGetPage(bt_para_so->currPos.buf);
            opaque = (BTPageOpaqueInternal)PageGetSpecialPointer(page);
            while (P_IGNORE(opaque)) {
                blkno = opaque->btpo_next;
                _bt_relbuf(bt_rel, bt_para_so->currPos.buf);
                bt_para_so->currPos.buf = InvalidBuffer;
                if ((bt_scan->btps_end_block != InvalidBlockNumber && blkno == bt_scan->btps_end_block) ||
                    blkno == P_NONE) {
                    return false;
                }
                /* setp right one page */
                bt_para_so->currPos.buf = _bt_getbuf(bt_rel, blkno, BT_READ);
                page = BufferGetPage(bt_para_so->currPos.buf);
                opaque = (BTPageOpaqueInternal)PageGetSpecialPointer(page);
            }
            PredicateLockPage(bt_rel, blkno, bt_scan->xs_snapshot);
            if (_bt_readpage(bt_scan, dir, P_FIRSTDATAKEY(opaque))) {
                break;
            }
            blkno = opaque->btpo_next;
        }
    } else {
        bt_para_so->currPos.moreRight = true;
        for (;;) {
            CHECK_FOR_INTERRUPTS();
            if (!bt_para_so->currPos.moreLeft) {
                _bt_relbuf(bt_rel, bt_para_so->currPos.buf);
                bt_para_so->currPos.buf = InvalidBuffer;
                return false;
            }
            Buffer temp = bt_para_so->currPos.buf;
            bt_para_so->currPos.buf = _bt_walk_left(bt_rel, temp, bt_scan->btps_end_block);
            if (bt_para_so->currPos.buf == InvalidBuffer) {
                return false;
            }
            page = BufferGetPage(bt_para_so->currPos.buf);
            opaque = (BTPageOpaqueInternal)PageGetSpecialPointer(page);
            if (!P_IGNORE(opaque)) {
                PredicateLockPage(bt_rel, BufferGetBlockNumber(bt_para_so->currPos.buf), bt_scan->xs_snapshot);
                if (_bt_readpage(bt_scan, dir, PageGetMaxOffsetNumber(page))) {
                    break;
                }
            }
        }
    }
    return true;
}
