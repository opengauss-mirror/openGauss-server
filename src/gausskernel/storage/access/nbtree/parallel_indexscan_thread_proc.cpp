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
 * parallel_indexscan_thread_proc.cpp
 *
 *
 *
 * IDENTIFICATION
 *        src\gausskernel\storage\access\nbtree\parallel_indexscan_thread_proc.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/nbtree.h"
#include "executor/executor.h"
#include "distributelayer/streamProducer.h"
#include "access/parallel_indexscan_core.h"

#define DIV_TIME_OUT 1

/*
 * @brief _bt_parallel_reallocat_shared_memory
 *  When a new plan node performs a parallel index scan, realloc share memory.
 * @param  stream_nodegroup     StreamNodeGroup
 * @param  index                One_dimensional index of the current index in the shared memory
 * @param  curr_off_start       2D offset in Shared Memory
 * @param  node_interval        Length of shared memory occupied by each plan code used for scanning
 * @return void
 */
void _bt_parallel_reallocat_shared_memory(StreamNodeGroup* stream_nodegroup, const int& index, int& curr_off_start,
                                          int node_interval)
{
    int nodeid_num = stream_nodegroup->parallel_indexscan_map[index][TOTAL_NODEID];
    nodeid_num++;
    stream_nodegroup->parallel_indexscan_map[index] =
        (volatile uint32*)repalloc((void*)stream_nodegroup->parallel_indexscan_map[index],
                                   (node_interval * nodeid_num + FIRST_NODE_OFFSET) * sizeof(uint32));
    stream_nodegroup->parallel_indexscan_map[index][TOTAL_NODEID]++;
    curr_off_start = node_interval * (nodeid_num - 1) + FIRST_NODE_OFFSET;
    stream_nodegroup->parallel_indexscan_map[index][curr_off_start] = InvalidNodeId;
    for (int i = curr_off_start + 1; i < curr_off_start + node_interval; i++) {
        stream_nodegroup->parallel_indexscan_map[index][i] = 0;
    }
}

/*
 * @brief  _bt_parallel_allocat_shared_memory
 *  Initial allocation of shared memory for index parallel scans.
 * @param  rel                  current index relation
 * @param  stream_nodegroup     StreamNodeGroup
 * @param  index                One_dimensional index of the current index in th e shared memory
 * @param  curr_off_start       2D offset in Shared Memory
 * @param  node_interval        Length of shared memory occupied by each plan node used for scanning
 * @return void
 */
void _bt_parallel_allocat_shared_memory(Relation rel, StreamNodeGroup* stream_nodegroup, int& index,
                                        int& curr_off_start, int node_interval)
{
    int size = stream_nodegroup->parallel_indexscan_size;
    size++;
    if (stream_nodegroup->parallel_indexscan_map == NULL) {
        stream_nodegroup->parallel_indexscan_map = (volatile uint32**)palloc0(size * sizeof(uint32*));
    } else {
        stream_nodegroup->parallel_indexscan_map =
            (volatile uint32**)repalloc((void*)stream_nodegroup->parallel_indexscan_map, size * sizeof(uint32*));
    }
    stream_nodegroup->parallel_indexscan_map[size - 1] =
        (uint32*)palloc0((node_interval + FIRST_NODE_OFFSET) * sizeof(uint32));
    stream_nodegroup->parallel_indexscan_size++;
    stream_nodegroup->parallel_indexscan_map[size - 1][INDEX_OID] = rel->rd_id;
    stream_nodegroup->parallel_indexscan_map[size - 1][TOTAL_NODEID] = 1;
    stream_nodegroup->parallel_indexscan_map[size - 1][FIRST_NODE_OFFSET] = InvalidNodeId;
    index = size - 1;
    curr_off_start = FIRST_NODE_OFFSET;
}

 /*
  * @brief  _bt_parallel_get_threadn_scan_range
  *  Initial allocation of shared memory for index parallel scans.
  * @param  scan            IndexScanDesc
  * @param  lid             Start block number of the current thread.
  * @param  rid             End block number of the current thread.
  * @param  start_block     Start scan block number
  * @return bool            returns true if there is no error, false otherwise
  */
bool _bt_parallel_get_threadn_scan_range(IndexScanDesc scan, uint32 lid, uint32 rid, BlockNumber& start_block)
{
    if (lid == InvalidBlockNumber) {
        start_block = InvalidBlockNumber;
        return false;
    }
    start_block = lid;
    if (lid != InvalidBlockNumber && rid == InvalidBlockNumber) {
        scan->btps_end_block = InvalidBlockNumber;
    } else {
        scan->btps_end_block = rid;
    }
    return true;
}

/*
 * @brief  _bt_find_parallel_divd
 *  Obtains the start offset of the current index in the shared memory.
 * @param  divd_res     shared memory
 * @param  index_oid    current index relation oid
 * @param  size         number of indexes
 * @return int          Obtains the one-dimensional index of in the shared memory.
 */
int _bt_find_parallel_divd(volatile uint32** divd_res, Oid index_oid, int size)
{
    if (divd_res == NULL) {
        return -1;
    }
    for (int i = 0; i < size; i++) {
        if (divd_res[i] == NULL) {
            return -1;
        }
        pg_memory_barrier();
        if (divd_res[i][0] == index_oid) {
            return i;
        }
    }
    return -1;
}

/*
 * @brief  stream_find_obj_by_nodeid
 *  Find stream object by nodeid
 * @param  stream_list      stream object list
 * @param  nodeid           plan node identifier
 * @return StreamProducer   pointer to stream producer
 */
StreamProducer* stream_find_obj_by_nodeid(List* stream_list, uint32 nodeid)
{
    StreamProducer* obj = NULL;
    foreach_cell(cell, stream_list) {
        obj = (StreamProducer*)lfirst(cell);
        if (obj->getKey().smpIdentifier == 0 && obj->m_plan->planTree->plan_node_id == (int)nodeid) {
            break;
        } else {
            obj = NULL;
        }
    }
    return obj;
}

/*
 * @brief  is_thread0_quit
 *  check if hte thread 0 for this paln node if is quit
 * @param  stream_nodegroup     stream node group
 * @param  release_lock         need release lock
 * @return bool                 returns true if there is no error, false otherwise.
 */
bool is_thread0_quit(StreamNodeGroup* stream_nodegroup, bool release_lock = false)
{
    pthread_mutex_t* mutex = stream_nodegroup->GetIndexSmpMutex();
    StreamProducer* producer_now = u_sess->stream_cxt.producer_obj;
    StreamProducer* producer_thread0 =
        stream_find_obj_by_nodeid(stream_nodegroup->m_streamProducerList, producer_now->m_plan->planTree->plan_node_id);
    if (producer_thread0 == NULL) {
        if (release_lock) {
            PthreadMutexUnlock(t_thrd.utils_cxt.ThreadRootResourceOwner, mutex);
        }
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("cannot find thread 0 for parallel index scan.")));
    }
    StreamNode* array = stream_nodegroup->GetSteamArray();
    if (array == NULL) {
        if (release_lock) {
            PthreadMutexUnlock(t_thrd.utils_cxt.ThreadRootResourceOwner, mutex);
        }
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("cannot find array for parallel index scan.")));
    }
    int ind = producer_thread0->getNodeGroupIdx();
    bool inited = producer_thread0->getThreadInit();
    bool* stop_flag = array[ind].stopFlag;
    if ((stop_flag == NULL && array[ind].status != STREAM_INPROGRESS && inited) || array[ind].status == STREAM_ERROR) {
        ereport(DEBUG1, (errmsg("index parallel scan thread 0 quit, thread 0 status: %d.", int(array[ind].status))));
        return true;
    }
    return false;
}

/*
 * @brief  _bt_parallel_first_threadn_proc
 *  Obtains the start offset of current index in the shared memory.
 * @param  scan                 IndexScanDesc
 * @param  dir                  scanning direction
 * @param  curr_off_start       2d Offset in Shared Memory
 * @param  start_block          Start scan block number
 * @param  stream_nodegroup     StreamGroup
 * @return bool                 returns true if there is no error, false otherwise.
 */
bool _bt_parallel_first_threadn_proc(IndexScanDesc scan, ScanDirection dir, int curr_off_start,
                                     BlockNumber& start_block, StreamNodeGroup* stream_nodegroup)
{
    int thread_id = (int)(u_sess->stream_cxt.smp_id);
    Relation rel = scan->indexRelation;
    pthread_mutex_t* mutex = stream_nodegroup->GetIndexSmpMutex();
    pthread_cond_t* cond = stream_nodegroup->GetIndexSmpCond();
    MemoryContext old_mem_context = MemoryContextSwitchTo(stream_nodegroup->m_streamRuntimeContext);
    CHECK_FOR_INTERRUPTS();
    PthreadMutexLock(t_thrd.utils_cxt.ThreadRootResourceOwner, mutex);
    int index = _bt_find_parallel_divd(stream_nodegroup->parallel_indexscan_map, rel->rd_id,
                                       stream_nodegroup->parallel_indexscan_size);
    struct timespec timer;
    while (!is_thread0_quit(stream_nodegroup, true) &&
           (index == -1 ||
            _bt_find_parallel_nodeid(scan, stream_nodegroup->parallel_indexscan_map[index], &curr_off_start))) {
        clock_gettime(CLOCK_MONOTONIC, &timer);
        timer.tv_sec += DIV_TIME_OUT;
        timer.tv_nsec = 0;
        pthread_cond_timedwait(cond, mutex, &timer);
        index = _bt_find_parallel_divd(stream_nodegroup->parallel_indexscan_map, rel->rd_id,
                                       stream_nodegroup->parallel_indexscan_size);
    }
    PthreadMutexUnlock(t_thrd.utils_cxt.ThreadRootResourceOwner, mutex);
    if (is_thread0_quit(stream_nodegroup)) {
        PthreadMutexLock(t_thrd.utils_cxt.ThreadRootResourceOwner, mutex);
        /* if the thread 0 is already quit, and we cannot find the divide res for cur relation, we need to quit */
        index = _bt_find_parallel_divd(stream_nodegroup->parallel_indexscan_map, rel->rd_id,
                                       stream_nodegroup->parallel_indexscan_size);
        if (index == -1) {
            PthreadMutexUnlock(t_thrd.utils_cxt.ThreadRootResourceOwner, mutex);
            ereport(
                DEBUG1,
                (errmodule(MOD_INDEX),
                 errmsg("index parallel scan oid %u, thread id %u quit for thread 0 quit and cannot find array index.",
                        rel->rd_id, u_sess->stream_cxt.smp_id)));
            MemoryContextSwitchTo(old_mem_context);
            return false;
        }
        bool continue_wait =
            _bt_find_parallel_nodeid(scan, stream_nodegroup->parallel_indexscan_map[index], &curr_off_start);
        if (continue_wait) {
            PthreadMutexUnlock(t_thrd.utils_cxt.ThreadRootResourceOwner, mutex);
            ereport(DEBUG1,
                    (errmodule(MOD_INDEX),
                     errmsg("index parallel scan oid %u, thread id %u quit for thread 0 quit and cannot find nodeid.",
                            rel->rd_id, u_sess->stream_cxt.smp_id)));
            MemoryContextSwitchTo(old_mem_context);
            return false;
        }
        PthreadMutexUnlock(t_thrd.utils_cxt.ThreadRootResourceOwner, mutex);
    }
    uint32 lid = 0;
    uint32 rid = 0;
    do {
        CHECK_FOR_INTERRUPTS();
        if (is_thread0_quit(stream_nodegroup)) {
            PthreadMutexLock(t_thrd.utils_cxt.ThreadRootResourceOwner, mutex);
            /* if the thread 0 is already quit, and we cannot find the divide res for cur relation, we need to quit */
            pg_memory_barrier();
            lid = stream_nodegroup->parallel_indexscan_map[index][curr_off_start + thread_id + OFFSET_START_BASE];
            rid = stream_nodegroup->parallel_indexscan_map[index][curr_off_start + thread_id + OFFSET_END_BASE];
            if (lid == 0 || rid == 0) {
                PthreadMutexUnlock(t_thrd.utils_cxt.ThreadRootResourceOwner, mutex);
                ereport(DEBUG1,
                        (errmodule(MOD_INDEX),
                         errmsg("index parallel sacn oid %u, thread id %u quit for thread 0 quit and lid/rid is zero.",
                                rel->rd_id, u_sess->stream_cxt.smp_id)));
                MemoryContextSwitchTo(old_mem_context);
                return false;
            }
        } else {
            PthreadMutexLock(t_thrd.utils_cxt.ThreadRootResourceOwner, mutex);
            /* thread 0 is working, we wait for devide res */
            lid = stream_nodegroup->parallel_indexscan_map[index][curr_off_start + thread_id + OFFSET_START_BASE];
            rid = stream_nodegroup->parallel_indexscan_map[index][curr_off_start + thread_id + OFFSET_END_BASE];
        }
        PthreadMutexUnlock(t_thrd.utils_cxt.ThreadRootResourceOwner, mutex);
    } while (lid == 0 || rid == 0);
    MemoryContextSwitchTo(old_mem_context);
    bool res = _bt_parallel_get_threadn_scan_range(scan, lid, rid, start_block);
    if (!res) {
        ereport(DEBUG1,
                (errmodule(MOD_INDEX), errmsg("index parallel sacn oid %u, thread id %u get no blocks and quit.",
                                              rel->rd_id, u_sess->stream_cxt.smp_id)));
        return false;
    }
    /* when the blocks cannot be devied intto threads, some of the thread should do nothing. */
    if (start_block == InvalidBlockNumber || (ScanDirectionIsForward(dir) && start_block == scan->btps_end_block)) {
        ereport(DEBUG1,
                (errmodule(MOD_INDEX), errmsg("index parallel sacn oid %u, thread id %u get no blocks and quit.",
                                              rel->rd_id, u_sess->stream_cxt.smp_id)));
        return false;
    }
    return true;
}

/*
 * @brief  _bt_find_parallel_nodeid
 *  Check whether the current plan node has been allocated in the shared memory.
 *  If the current paln node has been allocated, record the start array subscript of the corresponding paln node.
 * @param  scan                 IndexScanDesc
 * @param  divd_arr             One-dimensional array of the shared memory corresponding to the current index
 * @param  curr_off_start       2D offset in Shared Memory
 * @return bool                 returns true if there is no error, false otherwise.
 */
bool _bt_find_parallel_nodeid(IndexScanDesc scan, volatile uint32* divd_arr, int* curr_off_start)
{
    int interval = _bt_get_node_interval(scan->dop);
    uint32 node_id = scan->plan_nodeid;
    uint32 nodeid_num = divd_arr[TOTAL_NODEID];
    int total = interval * static_cast<int>(nodeid_num) + FIRST_NODE_OFFSET;
    for (int i = FIRST_NODE_OFFSET; i < total; i += interval) {
        pg_memory_barrier();
        if (divd_arr[i] == node_id) {
            *curr_off_start = i;
            return false;
        }
    }
    return true;
}