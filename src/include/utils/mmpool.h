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
 * mmpool.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/utils/mmpool.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_UTILS_MMPOOL_H_
#define SRC_INCLUDE_UTILS_MMPOOL_H_

#include "postgres.h"
#include "utils/memutils.h"
#include "nodes/pg_list.h"

struct MemoryBlock {
    int64 blkSize;
    bool isFree;

    MemoryBlock* prev;
    MemoryBlock* next;
};

#define MEMORY_BLOCK_HEADER TYPEALIGN(ALIGNOF_LONG, sizeof(MemoryBlock))
#define MEMORY_BLOCK_MIN_SIZE (2 * MEMORY_BLOCK_HEADER)

struct MemoryBlockListHeader {
    MemoryBlock* start;
    MemoryBlock* end;
};

#define POOL_INIT_SUCCESS 0
#define POOL_INIT_FAIL 1

class MemoryPool {
public:
    // constructor
    MemoryPool();

    // destructor
    ~MemoryPool();

    // init the memory pool.
    int CreatePool();

    // release the memory pool.
    void ReleasePool();

    // Malloc the memory from memory pool.
    void* Malloc(int64 sizeBytes);

    // Realloc the memory
    void* Realloc(void* addr, int64 sizeBytes);

    // Free the memory
    void Free(void* addr);

    // pool is ready?
    inline bool Ready()
    {
        return m_ready;
    }

    // static function to init/deinit the memory pool
    static int Init();

    static void Deinit();

private:
    // Split a block for fit the size
    bool SplitFreeList(int idx);

    // Merge block and its buddy
    void MergeBlock(void* addr, MemoryBlock* block, MemoryBlock* blockBuddy, int idx);

    MemoryBlock* FindBuddy(MemoryBlock* block, int64 size);

    void AddLast(int idx, MemoryBlock* blk);

    void AddFirst(int idx, MemoryBlock* blk);

    void Remove(int idx, MemoryBlock* blk);

private:
    // the memory buffer
    char* m_buf;  //

    // memory buffer size
    int64 m_size;

    // the usage size of memory buffer
    int64 m_use;

    // concurrency control for mutex
    pthread_mutex_t m_mutex;  // lock

    // free list for the memory pool.
    MemoryBlockListHeader m_freeList[64];

    // flag to indicate the pool is ready
    volatile bool m_ready;
};

#endif /* SRC_INCLUDE_UTILS_MMPOOL_H_ */
