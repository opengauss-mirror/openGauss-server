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
 * ubtsearchparallel.cpp
 *
 *
 *
 * IDENTIFICATION
 *        src\gausskernel\storage\access\ubtree\ubtsearchparallel.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/nbtree.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "storage/predicate.h"
#include "distributelayer/streamCore.h"
#include "access/parallel_indexscan_core.h"
#include "access/ubtree.h"

bool UBTreeParallelSteppage(IndexScanDesc ubt_scan, ScanDirection dir);

/*
 * @brief UBTreeFindNextBlock
 *  Find the next block according to the siling pointer (btpo_next/btpo_prev).
 * @param ubt_scan                      IndexScanDesc
 * @param dir                           Scanning direction
 * @param current_block                 Start block number of the current thread.
 * @param num_blocks                    Estimated number of lef nodes scanned by each thread
 * @return BlockNumber                  End block number of the current thread.
 */
BlockNumber UBTreeFindNextBlock(IndexScanDesc ubt_scan, ScanDirection dir, uint32 current_block, int num_blocks)
{
    if (current_block == InvalidBlockNumber || current_block == 0) {
        return 0;
    }
    int count = 1;
    BlockNumber ubt_next = current_block;
    BlockNumber ubt_now = current_block;
    Relation ubt_rel = ubt_scan->indexRelation;
    Buffer current_buf = _bt_getbuf(ubt_rel, static_cast<BlockNumber>(current_block), BT_READ);
    UBTPageOpaqueInternal opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(BufferGetPage(current_buf));
    ubt_now = ubt_next;
    if (ScanDirectionIsForward(dir)) {
        ubt_next = opaque->btpo_next;
    } else {
        ubt_next = opaque->btpo_prev;
    }
    _bt_relbuf(ubt_rel, current_buf);

    Buffer buf_tmp = InvalidBuffer;
    while (count <= num_blocks) {
        buf_tmp = _bt_getbuf(ubt_rel, ubt_next, BT_READ);
        opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(BufferGetPage(buf_tmp));
        bool empty_page = false;
        if (count == num_blocks && !P_RIGHTMOST(opaque) && !P_LEFTMOST(opaque)) {
            OffsetNumber max_off = PageGetMaxOffsetNumber(BufferGetPage(buf_tmp));
            empty_page = (P_FIRSTKEY > max_off);
        }
        if (ScanDirectionIsForward(dir)) {
            if (P_IGNORE(opaque) || empty_page) {
                ubt_now = ubt_next;
                ubt_next = opaque->btpo_next;
                _bt_relbuf(ubt_rel, buf_tmp);
                continue;
            }
            if (P_RIGHTMOST(opaque)) {
                _bt_relbuf(ubt_rel, buf_tmp);
                return opaque->btpo_next;
            }
            ubt_now = ubt_next;
            ubt_next = opaque->btpo_next;
        } else {
            if (P_IGNORE(opaque) || empty_page) {
                _bt_relbuf(ubt_rel, buf_tmp);
                ubt_now = ubt_next;
                ubt_next = opaque->btpo_prev;
                continue;
            }
            if (P_LEFTMOST(opaque)) {
                _bt_relbuf(ubt_rel, buf_tmp);
                return opaque->btpo_prev;
            }
            ubt_now = ubt_next;
            ubt_next = opaque->btpo_prev;
        }
        if (count == num_blocks) {
            opaque->btpo_flags |= BTP_PARALLEL_SCAN_END;
            if (((UBTPageOpaque)opaque)->xact < ubt_scan->xs_snapshot->xmin) {
                ((UBTPageOpaque)opaque)->xact = ubt_scan->xs_snapshot->xmin;
            }
            MarkBufferDirtyHint(buf_tmp, true);
        }
        _bt_relbuf(ubt_rel, buf_tmp);
        count++;
    }
    return ubt_now;
}

/*
 * @brief  check_is_need_continue
 *  In th0 of the parallel index scan, when obtaining the number of real scan pages,
 *  the ubtree checks () funcation is used to determine whether to scan the next page.
 *  During this period, debug-related processing such as trace_tuple dose not need to be performed,
 *  and related logic is deleted to simplify the processs.
 * @param  scan         refer to IndexScanDesc
 * @param  page         curr page
 * @param  offnum       start offset
 * @param  dir          Scanning direction
 * @return bool         returns true if there is continue scan, false otherwise
 */
bool check_is_need_continue(IndexScanDesc scan, Page page, OffsetNumber offnum, ScanDirection dir)
{
    bool continue_scan = true;
    bool tupleAlive = false;
    Datum res;
    ItemId iid = PageGetItemId(page, offnum);
    /*
     * If the scan specifies not to return killed tuples, then we treat a
     * killed tuple as not passing the qual.  Most of the time, it's a win to
     * not bother examining the tuple's index keys, but just return
     * immediately with continuescan = true to proceed to the next tuple.
     * However, if this is the last tuple on the page, we should check the
     * index keys to prevent uselessly advancing to the next page.
     */
    if (scan->ignore_killed_tuples && ItemIdIsDead(iid) && (ItemIdHasStorage(iid))) {
        /* return immediately if there are more tuples on the page */
        if (ScanDirectionIsForward(dir)) {
            if (offnum < PageGetMaxOffsetNumber(page)) {
                return continue_scan;
            }
        } else {
            UBTPageOpaqueInternal opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
            if (offnum > P_FIRSTDATAKEY(opaque)) {
                return continue_scan;
            }
        }

        /*
         * OK, we want to check the keys so we can set continuescan correctly,
         * but we'll return NULL even if the tuple passes the key tests.
         */
        tupleAlive = false;
    } else {
        tupleAlive = true;
    }

    IndexTuple tuple = (IndexTuple)PageGetItem(page, iid);
    TupleDesc tupdesc = RelationGetDescr(scan->indexRelation);
    BTScanOpaque ubt_so = (BTScanOpaque)scan->opaque;
    int keysz = ubt_so->numberOfKeys;
    int ikey = 0;
    ScanKey keydata = ubt_so->keyData;
    for (; ikey < keysz; keydata++, ikey++) {
        /* row-comparison keys need special processing */
        if (keydata->sk_flags & SK_ROW_HEADER) {
            if (_bt_check_rowcompare(keydata, tuple, tupdesc, dir, &continue_scan)) {
                continue;
            }
            return continue_scan;
        }
        bool isNull = false;
        Datum datum = index_getattr(tuple, keydata->sk_attno, tupdesc, &isNull);

        if (keydata->sk_flags & SK_ISNULL) {
            /* Handle IS NULL/NOT NULL tests */
            if (keydata->sk_flags & SK_SEARCHNULL) {
                if (isNull) {
                    continue; /* tuple satisfies this qual */
                }
            } else {
                Assert(keydata->sk_flags & SK_SEARCHNOTNULL);
                if (!isNull) {
                    continue; /* tuple satisfies this qual */
                }
            }

            /*
             * Tuple fails this qual.  If it's a required qual for the current
             * scan direction, then we can conclude no further tuples will
             * pass, either.
             */
            if ((keydata->sk_flags & SK_BT_REQFWD) && ScanDirectionIsForward(dir)) {
                continue_scan = false;
            } else if ((keydata->sk_flags & SK_BT_REQBKWD) && ScanDirectionIsBackward(dir)) {
                continue_scan = false;
            }
            return continue_scan;
        }

        if (isNull) {
            if (keydata->sk_flags & SK_BT_NULLS_FIRST) {
                /*
                 * Since NULLs are sorted before non-NULLs, we know we have
                 * reached the lower limit of the range of values for this
                 * index attr.	On a backward scan, we can stop if this qual
                 * is one of the "must match" subset.  We can stop regardless
                 * of whether the qual is > or <, so long as it's required,
                 * because it's not possible for any future tuples to pass. On
                 * a forward scan, however, we must keep going, because we may
                 * have initially positioned to the start of the index.
                 */
                if ((keydata->sk_flags & (SK_BT_REQFWD | SK_BT_REQBKWD)) && ScanDirectionIsBackward(dir)) {
                    continue_scan = false;
                }
            } else {
                /*
                 * Since NULLs are sorted after non-NULLs, we know we have
                 * reached the upper limit of the range of values for this
                 * index attr.	On a forward scan, we can stop if this qual is
                 * one of the "must match" subset.	We can stop regardless of
                 * whether the qual is > or <, so long as it's required,
                 * because it's not possible for any future tuples to pass. On
                 * a backward scan, however, we must keep going, because we
                 * may have initially positioned to the end of the index.
                 */
                if ((keydata->sk_flags & (SK_BT_REQFWD | SK_BT_REQBKWD)) && ScanDirectionIsForward(dir)) {
                    continue_scan = false;
                }
            }
            return continue_scan;
        }

        res = FunctionCall2Coll(&keydata->sk_func, keydata->sk_collation, datum, keydata->sk_argument);
        if (!DatumGetBool(res)) {
            /*
             * Tuple fails this qual.  If it's a required qual for the current
             * scan direction, then we can conclude no further tuples will
             * pass, either.
             *
             * Note: because we stop the scan as soon as any required equality
             * qual fails, it is critical that equality quals be used for the
             * initial positioning in _bt_first() when they are available. See
             * comments in _bt_first().
             */
            if ((keydata->sk_flags & SK_BT_REQFWD) && ScanDirectionIsForward(dir)) {
                continue_scan = false;
            } else if ((keydata->sk_flags & SK_BT_REQBKWD) && ScanDirectionIsBackward(dir)) {
                continue_scan = false;
            }
            return continue_scan;
        }
    }
    return continue_scan;
}

/*
 * @brief UBTreeParallelGetScanTotalBlocks
 *  Calculate the number of block that meet the scankey reuqirement.
 * @param ubt_scan                      IndexScanDesc
 * @param dir                           Scanning direction
 * @param ubt_start_blk                 Start scan block number
 * @return int                          Number of all block that meet the conditions
 */
int UBTreeParallelGetScanTotalBlocks(IndexScanDesc ubt_scan, ScanDirection dir, BlockNumber ubt_start_blk)
{
    if (ubt_start_blk == InvalidBuffer) {
        return 0;
    }
    Relation ubt_rel = ubt_scan->indexRelation;
    BTScanOpaque ubt_so = (BTScanOpaque)ubt_scan->opaque;
    int total_blocks = 1;
    bool continueloop = true;
    BlockNumber ubt_next = ubt_start_blk;
    while (true) {
        CHECK_FOR_INTERRUPTS();
        Buffer tmp_buf = _bt_getbuf(ubt_rel, ubt_next, BT_READ);
        UBTPageOpaqueInternal opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(BufferGetPage(tmp_buf));
        if (ScanDirectionIsForward(dir)) {
            ubt_next = opaque->btpo_next;
            if (P_RIGHTMOST(opaque)) {
                _bt_relbuf(ubt_rel, tmp_buf);
                break;
            }
        } else {
            ubt_next = opaque->btpo_prev;
            if (P_LEFTMOST(opaque)) {
                _bt_relbuf(ubt_rel, tmp_buf);
                break;
            }
        }
        _bt_relbuf(ubt_rel, tmp_buf);
        if (ubt_so->numberOfKeys > 0) {
            tmp_buf = _bt_getbuf(ubt_rel, ubt_next, BT_READ);
            Page cur_page = BufferGetPage(tmp_buf);
            opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(cur_page);
            if (ScanDirectionIsForward(dir)) {
                if (P_FIRSTDATAKEY(opaque) > PageGetMaxOffsetNumber(cur_page)) {
                    _bt_relbuf(ubt_rel, tmp_buf);
                    total_blocks++;
                    continue;
                }
                continueloop = check_is_need_continue(ubt_scan, cur_page, P_FIRSTDATAKEY(opaque), dir);
            } else {
                continueloop = check_is_need_continue(ubt_scan, cur_page, PageGetMaxOffsetNumber(cur_page), dir);
            }
            if (!continueloop) {
                _bt_relbuf(ubt_rel, tmp_buf);
                break;
            }
            _bt_relbuf(ubt_rel, tmp_buf);
        }
        total_blocks++;
    }
    return total_blocks;
}

/*
 * @brief UBTreeGetInskeyScankeyWithoutRowheader
 *  Initialize inskey->scankey when cur_>sk_flags & SK_ROW_HEADER is 0.
 * @param cur                           No. i startKey
 * @param ubt_rel                       relation of the current index
 * @param i                             Number of startkey iterations
 * @return void
 */
void UBTreeGetInskeyScankeyWithoutRowheader(ScanKey cur, Relation ubt_rel, int i, BTScanInsertData* inskey)
{
    if (cur->sk_subtype == ubt_rel->rd_opcintype[i] || cur->sk_subtype == InvalidOid) {
        FmgrInfo* procinfo = index_getprocinfo(ubt_rel, cur->sk_attno, BTORDER_PROC);
        ScanKeyEntryInitializeWithInfo(inskey->scankeys + i, cur->sk_flags, cur->sk_attno, InvalidStrategy,
                                       cur->sk_subtype, cur->sk_collation, procinfo, cur->sk_argument);
    } else {
        RegProcedure cmp_proc =
            get_opfamily_proc(ubt_rel->rd_opfamily[i], ubt_rel->rd_opcintype[i], cur->sk_subtype, BTORDER_PROC);
        if (SECUREC_UNLIKELY(!RegProcedureIsValid(cmp_proc)))
            ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                            errmsg("missing support function %d(%u,%u) for attribute %d of index \"%s\"", BTORDER_PROC,
                                   ubt_rel->rd_opcintype[i], cur->sk_subtype, cur->sk_attno,
                                   RelationGetRelationName(ubt_rel))));
        ScanKeyEntryInitialize(inskey->scankeys + i, cur->sk_flags, cur->sk_attno, InvalidStrategy, cur->sk_subtype,
                               cur->sk_collation, cmp_proc, cur->sk_argument);
    }
    return;
}

/*
 * @brief UBTReeInitInskey
 *  Initialize the inskey.
 * @param *inskey                       BTScanInsertData
 * @param nextkey                       proceed to the next step
 * @param keys_count                    number of startkeys
 * @return void
 */
void UBTReeInitInskey(BTScanInsertData* inskey, bool nextkey, int keys_count)
{
    inskey->heapkeyspace = true;
    inskey->anynullkeys = false;
    inskey->nextkey = nextkey;
    inskey->pivotsearch = false;
    inskey->scantid = NULL;
    inskey->keysz = keys_count;
    return;
}

/*
 * @brief  UBTreeGetGobackNeedToNext
 *  If the number of start conditions is 0, the start buffer and offset are returned.
 * @param ubt_scan                      IndexScanDesc
 * @param *need_to_go_back              need to take a step back
 * @param *need_to_next_key             proceed to the next step
 * @param strat_total                   different scanning conditions
 * @return bool                         returns true if there is no error, false otherwise
 */
bool UBTreeGetGobackNeedToNext(IndexScanDesc ubt_scan, ScanDirection dir, bool* ubt_need_to_go_back,
                                 bool* ubt_need_to_next_key, StrategyNumber strat_total)
{
    switch (strat_total) {
        case BTGreaterEqualStrategyNumber:
            break;
        case BTGreaterStrategyNumber:
            *ubt_need_to_next_key = true;
            break;
        case BTEqualStrategyNumber:
            *ubt_need_to_go_back = (ScanDirectionIsBackward(dir)) ? true : false;
            *ubt_need_to_next_key = (ScanDirectionIsBackward(dir)) ? true : false;
            break;
        case BTLessEqualStrategyNumber:
            *ubt_need_to_go_back = true;
            *ubt_need_to_next_key = true;
            break;
        case BTLessStrategyNumber:
            *ubt_need_to_go_back = true;
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                            errmsg("Unrecognized strat_total:%d in index \"%s\".", strat_total,
                                   RelationGetRelationName(ubt_scan->indexRelation))));
            return false;
    }
    return true;
}

/*
 * @brief  UBTreeGetBeginParallelScanBuf
 *  Obtains the start scanning buffer.
 * @param  ubt_scan             IndexScanDesc
 * @param  dir                  Scanning direction
 * @param  *offnum              start offset
 * @return Buffer               return the start scanning buffer
 */
Buffer UBTreeGetBeginParallelScanBuf(IndexScanDesc ubt_scan, ScanDirection dir, OffsetNumber* offnum)
{
    Relation ubt_rel = ubt_scan->indexRelation;
    Buffer buf;
    bool res = false;
    ScanKey start_keys[INDEX_MAX_KEYS] = {0};
    BTScanInsertData inskey;
    StrategyNumber strat_total = BTEqualStrategyNumber;
    int keys_count = _bt_get_start_keys(ubt_scan, dir, start_keys, strat_total);
    if (keys_count == 0) {
        return _bt_get_first_buf_without_scankey(ubt_scan, dir, offnum);
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
            UBTreeGetInskeyScankeyWithoutRowheader(cur, ubt_rel, i, &inskey);
        }
    }
    bool nextkey = false;
    bool goback = false;
    res = UBTreeGetGobackNeedToNext(ubt_scan, dir, &goback, &nextkey, strat_total);
    if (!res) {
        return InvalidBuffer;
    }
    UBTReeInitInskey(&inskey, nextkey, keys_count);
    BlockNumber end_block = ubt_scan->btps_end_block;
    (void)UBTreeSearch(ubt_rel, &inskey, &buf, BT_READ, false, end_block);
    if (!BufferIsValid(buf)) {
        PredicateLockRelation(ubt_rel, ubt_scan->xs_snapshot);
        return InvalidBuffer;
    } else {
        PredicateLockPage(ubt_rel, BufferGetBlockNumber(buf), ubt_scan->xs_snapshot);
    }
    *offnum = UBTreeBinarySearch(ubt_rel, &inskey, buf, false);
    if (goback) {
        *offnum = OffsetNumberPrev(*offnum);
    }
    return buf;
}

/*
 * @brief  UBTreeParallelFirstThread0Proc
 *  In ther parallel_first func, thread 0 divides the scan blocks and records them to the shared memory,
 *  and assigns the scan start and end blocks of thread 0.
 * @param  ubt_scan          IndexScanDesc
 * @param  dir              Scanning direction
 * @param  curr_off_start   2D Offset in Shared Memory
 * @param  index            One-dimensional index of the current index in the shared memory
 * @param  bt_start_blk     Start scan block number of thread 0
 * @param  stream_nodegroup StreamNodeGroup
 * @param  offnum           Start offset
 * @return bool             returns true if there is no error, false otherwise
 */
bool UBTreeParallelFirstThread0Proc(IndexScanDesc ubt_scan, ScanDirection dir, int curr_off_start, int index,
                                    BlockNumber& bt_start_blk, StreamNodeGroup* stream_nodegroup, OffsetNumber* offnum)
{
    Relation ubt_rel = ubt_scan->indexRelation;
    int curr_th0_start = curr_off_start + OFFSET_START_BASE;
    int curr_th0_end = curr_off_start + OFFSET_END_BASE;
    BTScanOpaque ubt_so = (BTScanOpaque)ubt_scan->opaque;
    BlockNumber blkno = InvalidBuffer;
    Buffer begin_buf = UBTreeGetBeginParallelScanBuf(ubt_scan, dir, offnum);
    if (BufferIsValid(begin_buf)) {
        blkno = BufferGetBlockNumber(begin_buf);
        _bt_relbuf(ubt_rel, begin_buf);
    } else {
        ubt_so->currPos.buf = InvalidBuffer;
        return false;
    }
    int real_scan_blocks = UBTreeParallelGetScanTotalBlocks(ubt_scan, dir, blkno);
    int num_blocks = (real_scan_blocks + (ubt_scan->dop - 1)) / ubt_scan->dop;

    pthread_mutex_t* mutex = stream_nodegroup->GetIndexSmpMutex();
    pthread_cond_t* cond = stream_nodegroup->GetIndexSmpCond();
    ereport(LOG,
            (errmsg("ubtree parallel ubt_scan oid %u, dop %d, %d blocks per thread, total blocks %d, real scan blocks "
                    "%u, plan nodeid %u.",
                    ubt_rel->rd_id, ubt_scan->dop, num_blocks, RelationGetNumberOfBlocks(ubt_rel), real_scan_blocks,
                    ubt_scan->plan_nodeid)));
    MemoryContext old_mem_context = MemoryContextSwitchTo(stream_nodegroup->m_streamRuntimeContext);
    PthreadMutexLock(t_thrd.utils_cxt.ThreadRootResourceOwner, mutex);
    stream_nodegroup->parallel_indexscan_map[index][curr_off_start + OFFSET_START_BASE] = blkno;
    PthreadMutexUnlock(t_thrd.utils_cxt.ThreadRootResourceOwner, mutex);
    for (int i = OFFSET_START_BASE; i < ubt_scan->dop; i++) {
        PthreadMutexLock(t_thrd.utils_cxt.ThreadRootResourceOwner, mutex);
        uint32 current_block = stream_nodegroup->parallel_indexscan_map[index][curr_off_start + i];
        PthreadMutexUnlock(t_thrd.utils_cxt.ThreadRootResourceOwner, mutex);
        BlockNumber bt_next = UBTreeFindNextBlock(ubt_scan, dir, current_block, num_blocks);
        int next_thread_start_offset = curr_off_start + i + OFFSET_START_BASE;
        PthreadMutexLock(t_thrd.utils_cxt.ThreadRootResourceOwner, mutex);
        stream_nodegroup->parallel_indexscan_map[index][next_thread_start_offset] =
            bt_next == 0 ? InvalidBlockNumber : bt_next;
        PthreadMutexUnlock(t_thrd.utils_cxt.ThreadRootResourceOwner, mutex);
    }
    PthreadMutexLock(t_thrd.utils_cxt.ThreadRootResourceOwner, mutex);
    stream_nodegroup->parallel_indexscan_map[index][curr_off_start + ubt_scan->dop + OFFSET_START_BASE] =
        InvalidBlockNumber;
    stream_nodegroup->parallel_indexscan_map[index][curr_off_start] = ubt_scan->plan_nodeid;
    pg_memory_barrier();
    bt_start_blk = stream_nodegroup->parallel_indexscan_map[index][curr_th0_start];
    ubt_scan->btps_end_block = stream_nodegroup->parallel_indexscan_map[index][curr_th0_end];
    pthread_cond_broadcast(cond);
    PthreadMutexUnlock(t_thrd.utils_cxt.ThreadRootResourceOwner, mutex);
    MemoryContextSwitchTo(old_mem_context);
    return true;
}

/*
 * @brief  UBTreeParallelFirstGetFirstBuffer
 *  Get start scan block of the current thread.
 * @param  ubt_scan             IndexScanDesc
 * @param  dir                  Scanning direction
 * @param  ubt_start_blk        Start scan block number of current thread
 * @param  offnum               Start offset
 * @return Buffer               Start scan block of the current thread
 */
Buffer UBTreeParallelFirstGetFirstBuffer(IndexScanDesc ubt_scan, ScanDirection dir, BlockNumber ubt_start_blk,
                                         OffsetNumber* offnum)
{
    Buffer buf;
    Relation ubt_rel = ubt_scan->indexRelation;
    BTScanOpaque ubt_so = (BTScanOpaque)ubt_scan->opaque;
    int thread_id = (int)(u_sess->stream_cxt.smp_id);
    if (ScanDirectionIsForward(dir)) {
        if (thread_id == 0) {
            buf = UBTreeGetBeginParallelScanBuf(ubt_scan, dir, offnum);
        } else {
            buf = _bt_getbuf(ubt_rel, ubt_start_blk, BT_READ);
        }
    } else {
        if (thread_id == 0) {
            if (ubt_so->numberOfKeys == 0) {
                buf = UBTreeGetEndPoint(ubt_rel, 0, true);
            } else {
                buf = UBTreeGetBeginParallelScanBuf(ubt_scan, dir, offnum);
            }
        } else {
            buf = _bt_getbuf(ubt_rel, ubt_start_blk, BT_READ);
            buf = _bt_walk_left(ubt_rel, buf, ubt_scan->btps_end_block);
        }
    }
    ereport(LOG,
            (errmsg("ubtree parallel scan oid %u, thread id %u begin with block %u end with %u (4294967295 meas "
                    "InvalidBlockNumer), plan nodeid %u.",
                    ubt_rel->rd_id, u_sess->stream_cxt.smp_id, ubt_start_blk, ubt_scan->btps_end_block,
                    ubt_scan->plan_nodeid)));
    if (thread_id > 0) {
        if (!BufferIsValid(buf)) {
            PredicateLockRelation(ubt_rel, ubt_scan->xs_snapshot);
            return InvalidBuffer;
        } else {
            PredicateLockPage(ubt_rel, BufferGetBlockNumber(buf), ubt_scan->xs_snapshot);
        }
    }
    ereport(DEBUG2, (errmodule(MOD_INDEX),
                     errmsg("ubtree parallel scan oid %u, thread id %u start with block %u buf %d paln nodeid %u.",
                            ubt_rel->rd_id, u_sess->stream_cxt.smp_id,
                            buf == InvalidBuffer ? 0 : BufferGetBlockNumber(buf), buf, ubt_scan->plan_nodeid)));
    return buf;
}

/*
 * @brief  UBTreeScanSetTupleAndGPIOid
 *  Set heap/index tuple and gpi scan partition oid of this scan
 * @param  ubt_scan     refer to IndexScanDesc
 * @return void
 */
void UBTreeScanSetTupleAndGPIOid(IndexScanDesc ubt_scan)
{
    BTScanOpaque ubt_so = (BTScanOpaque)ubt_scan->opaque;
    BTScanPosItem* curr_item = &ubt_so->currPos.items[ubt_so->currPos.itemIndex];
    ubt_scan->xs_ctup.t_self = curr_item->heapTid;
    ubt_scan->xs_recheck_itup = false;
    if (ubt_scan->xs_want_itup || curr_item->needRecheck) {
        /* in this case, curr_tuples and tupleOffset must be valid. */
        Assert(ubt_so->currTuples != NULL && curr_item->tupleOffset != INVALID_TUPLE_OFFSET);
        ubt_scan->xs_itup = (IndexTuple)(ubt_so->currTuples + curr_item->tupleOffset);
        /* if we can't tell whether this tuple is visible with out CID, we must fetch UHeapTuple to recheck. */
        ubt_scan->xs_recheck_itup = curr_item->needRecheck;
    }
    if (ubt_scan->xs_want_ext_oid && GPIScanCheckPartOid(ubt_scan->xs_gpi_scan, curr_item->partitionOid)) {
        GPISetCurrPartOid(ubt_scan->xs_gpi_scan, curr_item->partitionOid);
    }
    if (ubt_scan->xs_want_bucketid && cbi_scan_need_change_bucket(ubt_scan->xs_cbi_scan, curr_item->bucketid)) {
        cbi_set_bucketid(ubt_scan->xs_cbi_scan, curr_item->bucketid);
    }
}

/*
 * @brief UBTreeParallelFirstExecScan
 *  Perform a scan in the current scan interval
 * @param  ubt_scan         IndexScanDesc
 * @param  dir              Scanning direction
 * @param  ubt_start_blk    Start scan block number of current thread
 * @param  offnum           Start offset
 * @return bool             returns true if there is no error, false otherwise
 */
bool UBTreeParallelFirstExecScan(IndexScanDesc ubt_scan, ScanDirection dir, BlockNumber ubt_start_blk,
                                 OffsetNumber offnum)
{
    BTScanOpaque ubt_so = (BTScanOpaque)ubt_scan->opaque;
    int thread_id = (int)(u_sess->stream_cxt.smp_id);
    ubt_so->currPos.buf = UBTreeParallelFirstGetFirstBuffer(ubt_scan, dir, ubt_start_blk, &offnum);
    if (ubt_so->currPos.buf == InvalidBuffer) {
        return false;
    }
    if (ScanDirectionIsForward(dir) && ubt_scan->btps_end_block == BufferGetBlockNumber(ubt_so->currPos.buf)) {
        _bt_relbuf(ubt_scan->indexRelation, ubt_so->currPos.buf);
        ubt_so->currPos.buf = InvalidBuffer;
        return false;
    }
    /* init moreRight/modeLeft for scan direction */
    ubt_so->currPos.moreRight = (ScanDirectionIsForward(dir)) ? true : false;
    ubt_so->currPos.moreLeft = (ScanDirectionIsForward(dir)) ? false : true;
    ubt_so->markItemIndex = -1;
    ubt_so->numKilled = 0;
    if (ubt_so->numberOfKeys == 0 || thread_id != 0) {
        UBTPageOpaqueInternal opaque =
            (UBTPageOpaqueInternal)PageGetSpecialPointer(BufferGetPage(ubt_so->currPos.buf));
        if (ScanDirectionIsBackward(dir)) {
            offnum = PageGetMaxOffsetNumber(BufferGetPage(ubt_so->currPos.buf));
        } else if (ScanDirectionIsForward(dir)) {
            /* There could be dead pages to the left, so not this. */
            offnum = P_FIRSTDATAKEY(opaque);
        } else {
            ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("Invalid scan direction: %d", dir)));
            offnum = 0; /* init start anyway */
        }
    }
    if (!UBTreeReadPage(ubt_scan, dir, offnum)) {
        if (!UBTreeParallelSteppage(ubt_scan, dir)) {
            return false;
        }
    }
    /* unlock the current page, but held the pin */
    LockBuffer(ubt_so->currPos.buf, BUFFER_LOCK_UNLOCK);

    UBTreeScanSetTupleAndGPIOid(ubt_scan);
    return true;
}

/*
 * @brief  UBTreeParallelFirst
 *  Find the first item in a parallel index scan, and mark the start/end block of this thread
 *  during parallel index scanning.
 *      If DOP of current scan is not 1, the index scan should be paralleled.
 *      In the scanning direction, we need to pay attention to the start block and end block
 *      of current thread.
 *      If the end block was marked as InvalidBlockerNumber, the thread would scan until
 *      the last block is met.
 *      It should be noticed that, not all threads will actually do the scanning, some of the thread
 *      may not get any blocks for inappropriate DOP value. For the case, the thread will ust resturn.
 * @param  ubt_scan             IndexScanDesc
 * @param  dir                  Scanning direction
 * @return bool                 returns true if there is no error, false otherwise
 */
bool UBTreeParallelFirst(IndexScanDesc ubt_scan, ScanDirection dir)
{
    Relation ubt_rel = ubt_scan->indexRelation;
    BlockNumber real_blocks = RelationGetNumberOfBlocksInFork(ubt_rel, MAIN_FORKNUM, false);
    if (real_blocks <= 1) {
        return false;
    }
    bool res;
    OffsetNumber offnum = InvalidOffsetNumber;
    uint32 thread_id = u_sess->stream_cxt.smp_id;
    int idxval = 0;
    int curr_off_start = -1;
    int node_interval = _bt_get_node_interval(ubt_scan->dop);
    BlockNumber ubt_start_blk = InvalidBlockNumber;
    StreamNodeGroup* stream_nodegroup = u_sess->stream_cxt.global_obj;
    pthread_mutex_t* mutex = stream_nodegroup->GetIndexSmpMutex();
    if (thread_id == 0) {
        PthreadMutexLock(t_thrd.utils_cxt.ThreadRootResourceOwner, mutex);
        {
            idxval = _bt_find_parallel_divd(stream_nodegroup->parallel_indexscan_map, ubt_rel->rd_id,
                                           stream_nodegroup->parallel_indexscan_size);
            MemoryContext old_mem_context = MemoryContextSwitchTo(stream_nodegroup->m_streamRuntimeContext);
            if (idxval != -1) {
                _bt_parallel_reallocat_shared_memory(stream_nodegroup, idxval, curr_off_start, node_interval);
            } else {
                _bt_parallel_allocat_shared_memory(ubt_rel, stream_nodegroup, idxval, curr_off_start, node_interval);
            }
            MemoryContextSwitchTo(old_mem_context);
        }
        PthreadMutexUnlock(t_thrd.utils_cxt.ThreadRootResourceOwner, mutex);
    }
    if (thread_id == 0) {
        res = UBTreeParallelFirstThread0Proc(ubt_scan, dir, curr_off_start, idxval, ubt_start_blk, stream_nodegroup,
                                             &offnum);
    } else {
        res = _bt_parallel_first_threadn_proc(ubt_scan, dir, curr_off_start, ubt_start_blk, stream_nodegroup);
    }
    if (res) {
        res = UBTreeParallelFirstExecScan(ubt_scan, dir, ubt_start_blk, offnum);
    }
    return res;
}

/*
 * @brief  UBTreeParallelNext
 *  Get the next item on parallel index scan
 *  call parallel_step_page to get next page.
 * @param  ubt_scan     IndexScanDesc
 * @param  dir          Scanning direction
 * @return bool         returns true if there is no error, false otherwise.
 */
bool UBTreeParallelNext(IndexScanDesc ubt_scan, ScanDirection dir)
{
    BTScanOpaque ubt_so = (BTScanOpaque)ubt_scan->opaque;
    if (ScanDirectionIsForward(dir)) {
        if (++ubt_so->currPos.itemIndex > ubt_so->currPos.lastItem) {
            /* We must acquire lock before, applying _bt_steppage */
            Assert(BufferIsValid(ubt_so->currPos.buf));
            LockBuffer(ubt_so->currPos.buf, BT_READ);
            if (!UBTreeParallelSteppage(ubt_scan, dir)) {
                return false;
            }
            ereport(
                DEBUG2,
                (errmodule(MOD_INDEX),
                 errmsg("ubtree forward index parallel scan oid %u, thread id %u deal with block %u paln nodeid %u.",
                        ubt_scan->indexRelation->rd_id, u_sess->stream_cxt.smp_id,
                        BufferGetBlockNumber(ubt_so->currPos.buf), ubt_scan->plan_nodeid)));
            /* Drop the lock, but not pin, on the new page */
            LockBuffer(ubt_so->currPos.buf, BUFFER_LOCK_UNLOCK);
        }
    } else {
        if (--ubt_so->currPos.itemIndex < ubt_so->currPos.firstItem) {
            /* We must acquire lock before, applying _bt_steppage */
            Assert(BufferIsValid(ubt_so->currPos.buf));
            LockBuffer(ubt_so->currPos.buf, BT_READ);
            if (!UBTreeParallelSteppage(ubt_scan, dir)) {
                return false;
            }
            ereport(
                DEBUG2,
                (errmodule(MOD_INDEX),
                 errmsg(
                     "ubtree backforward index parallel scan oid %u, thread id %u deal with block %u paln nodeid %u.",
                     ubt_scan->indexRelation->rd_id, u_sess->stream_cxt.smp_id,
                     BufferGetBlockNumber(ubt_so->currPos.buf), ubt_scan->plan_nodeid)));
            /* Drop the lock, but not pin, on the new page */
            LockBuffer(ubt_so->currPos.buf, BUFFER_LOCK_UNLOCK);
        }
    }
    UBTreeScanSetTupleAndGPIOid(ubt_scan);
    return true;
}


/*
 * @brief  UBTreeParallelSteppage
 *  Go to the next page for parallel index scan
 *      The whole process is just like ordinary step_page, except that we just stop read for current thread if end block
 *      is met.
 * @param  ubt_scan     IndexScanDesc
 * @param  dir          Scanning direction
 * @return bool         returns true if there is no error, false otherwise.
 */
bool UBTreeParallelSteppage(IndexScanDesc ubt_scan, ScanDirection dir)
{
    BTScanOpaque ubt_so = (BTScanOpaque)ubt_scan->opaque;
    UBTPageOpaqueInternal opaque = NULL;

    /* we must have the buffer pinned and locked */
    Assert(BufferIsValid(ubt_so->currPos.buf));

    /* Before leaving current page, deal with any killed items */
    if (ubt_so->numKilled > 0)
        _bt_killitems(ubt_scan, true);

    /*
     * Before we modify currPos, make a copy of the page data if there was a
     * mark position that needs it.
     */
    if (ubt_so->markItemIndex >= 0) {
        /* bump pin on current buffer for assignment to mark buffer */
        IncrBufferRefCount(ubt_so->currPos.buf);
        errno_t rc = memcpy_s(&ubt_so->markPos,
                              offsetof(BTScanPosData, items[1]) + ubt_so->currPos.lastItem * sizeof(BTScanPosItem),
                              &ubt_so->currPos,
                              offsetof(BTScanPosData, items[1]) + ubt_so->currPos.lastItem * sizeof(BTScanPosItem));
        securec_check(rc, "", "");
        if (ubt_so->markTuples) {
            rc = memcpy_s(ubt_so->markTuples, (size_t)ubt_so->currPos.nextTupleOffset, ubt_so->currTuples,
                          (size_t)ubt_so->currPos.nextTupleOffset);
            securec_check(rc, "", "");
        }
        ubt_so->markPos.itemIndex = ubt_so->markItemIndex;
        ubt_so->markItemIndex = -1;
    }
    Relation index_rel = ubt_scan->indexRelation;
    if (ScanDirectionIsForward(dir)) {
        BlockNumber blkno = ubt_so->currPos.nextPage;
        ubt_so->currPos.moreLeft = true;
        for (;;) {
            /* if we're at end of scan, give up */
            if (ubt_scan->btps_end_block != InvalidBlockNumber && blkno == ubt_scan->btps_end_block) {
                _bt_relbuf(index_rel, ubt_so->currPos.buf);
                ubt_so->currPos.buf = InvalidBuffer;
                return false;
            }
            _bt_relbuf(index_rel, ubt_so->currPos.buf);
            ubt_so->currPos.buf = InvalidBuffer;
            if (blkno == P_NONE || !ubt_so->currPos.moreRight) {
                ereport(DEBUG1, (errmodule(MOD_INDEX),
                                 errmsg("index parallel scan reach and thread id: %d.", u_sess->stream_cxt.smp_id)));
                return false;
            }
            CHECK_FOR_INTERRUPTS();
            ubt_so->currPos.buf = _bt_getbuf(index_rel, blkno, BT_READ);
            Page page = BufferGetPage(ubt_so->currPos.buf);
            opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
            if (!P_IGNORE(opaque)) {
                PredicateLockPage(index_rel, blkno, ubt_scan->xs_snapshot);
                bool ret = UBTreeReadPage(ubt_scan, dir, P_FIRSTDATAKEY(opaque));
                if (ret) {
                    break;
                }
            }
            blkno = opaque->btpo_next;
        }
    } else {
        ubt_so->currPos.moreRight = true;
        for (;;) {
            CHECK_FOR_INTERRUPTS();
            if (!ubt_so->currPos.moreLeft) {
                _bt_relbuf(index_rel, ubt_so->currPos.buf);
                ubt_so->currPos.buf = InvalidBuffer;
                return false;
            }
            Buffer temp = ubt_so->currPos.buf;
            ubt_so->currPos.buf = _bt_walk_left(index_rel, temp, ubt_scan->btps_end_block);
            if (ubt_so->currPos.buf == InvalidBuffer) {
                return false;
            }
            Page page = BufferGetPage(ubt_so->currPos.buf);
            opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
            if (!P_IGNORE(opaque)) {
                PredicateLockPage(index_rel, BufferGetBlockNumber(ubt_so->currPos.buf), ubt_scan->xs_snapshot);
                bool ret = UBTreeReadPage(ubt_scan, dir, PageGetMaxOffsetNumber(page));
                if (ret) {
                    break;
                }
            }
        }
    }
    return true;
}