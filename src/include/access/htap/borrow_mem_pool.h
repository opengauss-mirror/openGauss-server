/*
 * Copyright (c) 2025 Huawei Technologies Co.,Ltd.
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
 * borrow_mem_pool.h
 *        routines to support IMColStore
 *
 *
 * IDENTIFICATION
 *        src/include/access/htap/borrow_mem_pool.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef BORROW_MEM_POOL_H
#define BORROW_MEM_POOL_H

#include "postgres.h"

#define BMP_CHUNK_HDSZ (sizeof(BMPChunkHeader))

typedef struct BMPBlock {
    void *ptr;
    int refCnt;
    int currPos;
    BMPBlock *prev;
    BMPBlock *next;
} BMPBlock;

typedef struct {
    BMPBlock *bmpBlock;
} BMPChunkHeader;

class BorrowMemPool : public BaseObject {
public:
    BorrowMemPool(Oid relOid);

    ~BorrowMemPool();

    void *Allocate(uint32 size);

    void DeAllocate(void *ptr);

    bool CanBorrow();

    void Destroy();

private:
    bool AllocateNewBlock();

    void DeAllocateBlock(BMPBlock *blk);

    static const uint32 BLOCK_SIZE = 1 << 27;

    BMPBlock *m_headBlock;

    BMPBlock *m_currBlock;

    uint32 m_block_num{0};

    Oid m_relOid;

    bool m_lastBorrowFailed{false};

    pthread_rwlock_t m_mutex;
};

#endif /* BORROW_MEM_POOL_H */