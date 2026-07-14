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
 * parallel_indexscan_core.h
 *
 *
 *
 * IDENTIFICATION
 *        src\include\access\parallel_indexscan_core.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef PARALLEL_INDEXSCAN_CORE_H
#define PARALLEL_INDEXSCAN_CORE_H
#include "distributelayer/streamCore.h"

extern int _bt_get_start_keys(IndexScanDesc scan, ScanDirection dir, ScanKey (&start_keys)[INDEX_MAX_KEYS],
                              StrategyNumber& para_strat_total);
extern bool _bt_get_inskey_scankey_with_rowheader(ScanKey cur, BTScanInsertData* inskey,
                                                  StrategyNumber& param_strat_total, int& keys_count, int i,
                                                  bool& continue_loop);
extern void _bt_parallel_reallocat_shared_memory(StreamNodeGroup* stream_nodegroup, const int& index,
                                                 int& curr_off_start, int node_interval);
extern void _bt_parallel_allocat_shared_memory(Relation rel, StreamNodeGroup* stream_nodegroup, int& index,
                                               int& curr_off_start, int node_interval);
extern int _bt_find_parallel_divd(volatile uint32** divd_res, Oid index_oid, int size);
extern bool _bt_parallel_get_threadn_scan_range(IndexScanDesc scan, uint32 lid, uint32 rid, BlockNumber& start_block);
extern bool _bt_parallel_first_threadn_proc(IndexScanDesc scan, ScanDirection dir, int curr_off_start,
                                            BlockNumber& start_block, StreamNodeGroup* stream_nodegroup);
extern bool _bt_find_parallel_nodeid(IndexScanDesc scan, volatile uint32* divd_arr, int* curr_off_start);
extern Buffer _bt_get_first_buf_without_scankey(IndexScanDesc scan, ScanDirection dir, OffsetNumber* offnum);
/*
 * @brief _bt_get_node_interval
 *  Length of shared memory occupied by each paln node used for scanning
 *
 * @param dop                           degree of paralled
 * @return bool                         return length Returns the length of the shared memory occupied by the current
 * paln node.
 */
inline int _bt_get_node_interval(int dop)
{
    return dop + FIRST_NODE_OFFSET;
}

#endif