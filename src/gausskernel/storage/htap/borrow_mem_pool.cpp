/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-server is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 *
 * ---------------------------------------------------------------------------------------
 * borrow_mem_pool.cpp
 *      routines to support IMColStore
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/htap/borrow_mem_pool.cpp
 * ---------------------------------------------------------------------------------------
 */

#include "storage/ubs_mem.h"
#include "postmaster/rack_mem_cleaner.h"
#include "access/htap/borrow_mem_pool.h"

BorrowMemPool::BorrowMemPool(Oid relOid)
{
    m_headBlock = NULL;
    m_currBlock = NULL;
    m_relOid = relOid;
    m_mutex = PTHREAD_RWLOCK_INITIALIZER;
}

BorrowMemPool::~BorrowMemPool()
{}

bool BorrowMemPool::AllocateNewBlock()
{
    BMPBlock *prevBlock = m_currBlock;
    void *rackPtr = nullptr;
    int ret = ubsmem_lease_malloc("default", BLOCK_SIZE, DISTANCE_DIRECT_NODE, 0, &rackPtr);
    if (ret == 0 && rackPtr != NULL) {
        m_currBlock = (BMPBlock*)palloc0(sizeof(BMPBlock));
        m_currBlock->ptr = rackPtr;
        m_currBlock->refCnt = 0;
        m_currBlock->currPos = 0;
        if (prevBlock != NULL) {
            prevBlock->next = m_currBlock;
            m_currBlock->prev = prevBlock;
        } else {
            m_headBlock = m_currBlock;
        }
        ereport(LOG, (errmsg("HTAP BorrowMemPool Allocate: borrow a block, num(%u), size(%u), oid(%u)",
            ++m_block_num, BLOCK_SIZE, m_relOid)));
        return true;
    } else {
        m_lastBorrowFailed = true;
        ereport(WARNING, (errmsg("HTAP BorrowMemPool Allocate: borrow memory failed, size(%u), oid(%u)",
            BLOCK_SIZE, m_relOid)));
        return false;
    }
}

void BorrowMemPool::DeAllocateBlock(BMPBlock *blk)
{
    BMPBlock *prevBlk = blk->prev;
    BMPBlock *nextBlk = blk->next;
    if (m_currBlock == blk) {
        m_currBlock = prevBlk;
    }
    pfree_ext(blk);
    if (prevBlk != NULL) {
        prevBlk->next = nextBlk;
    }
    if (nextBlk != NULL) {
        nextBlk->prev = prevBlk;
    }
}

void* BorrowMemPool::Allocate(uint32 size)
{
    void *result = NULL;
    uint32 chunkSize = BMP_CHUNK_HDSZ + size;

    pthread_rwlock_wrlock(&m_mutex);
    if (m_lastBorrowFailed) {
        pthread_rwlock_unlock(&m_mutex);
        return result;
    }

    if (m_currBlock == NULL || m_currBlock->currPos + chunkSize > BLOCK_SIZE) {
        if (!AllocateNewBlock()) {
            pthread_rwlock_unlock(&m_mutex);
            return result;
        }
    }

    result = (char*)m_currBlock->ptr + m_currBlock->currPos + BMP_CHUNK_HDSZ;
    ((BMPChunkHeader*)((char*)m_currBlock->ptr + m_currBlock->currPos))->bmpBlock = m_currBlock;
    m_currBlock->refCnt++;
    m_currBlock->currPos += chunkSize;

    pthread_rwlock_unlock(&m_mutex);
    return result;
}

void BorrowMemPool::DeAllocate(void *ptr)
{
    pthread_rwlock_wrlock(&m_mutex);
    BMPBlock *blk = ((BMPChunkHeader*)((char*)ptr - BMP_CHUNK_HDSZ))->bmpBlock;
    Assert(blk != NULL);
    if (blk != NULL && --blk->refCnt <= 0 && blk->ptr != NULL) {
        int ret = ubsmem_lease_free(blk->ptr);
        if (ret != 0) {
            RegisterFailedFreeMemory(blk->ptr);
            ereport(LOG, (errmsg("HTAP BorrowMemPool DeAllocate: ubsmem_lease_free failed")));
        }
        DeAllocateBlock(blk);
        ereport(LOG, (errmsg("HTAP BorrowMemPool DeAllocate: return a block, size(%u), oid(%u), %u block left",
            BLOCK_SIZE, m_relOid, --m_block_num)));
    }
    pthread_rwlock_unlock(&m_mutex);
}

bool BorrowMemPool::CanBorrow()
{
    bool canBorrow = true;
    pthread_rwlock_rdlock(&m_mutex);
    canBorrow = !m_lastBorrowFailed;
    pthread_rwlock_unlock(&m_mutex);
    return canBorrow;
}

void BorrowMemPool::Destroy()
{
    pthread_rwlock_wrlock(&m_mutex);
    BMPBlock *blk = m_headBlock;

    while (blk != NULL) {
        if (blk->ptr != NULL) {
        int ret = ubsmem_lease_free(blk->ptr);
        if (ret != 0) {
            RegisterFailedFreeMemory(blk->ptr);
            ereport(LOG, (errmsg("HTAP BorrowMemPool Destroy: ubsmem_lease_free failed")));
        }
            blk->ptr = NULL;
            ereport(LOG, (errmsg("HTAP BorrowMemPool Destroy: return a block, size(%u), oid(%u), %u block left",
                BLOCK_SIZE, m_relOid, --m_block_num)));
        }

        BMPBlock *nextBlk = blk->next;
        pfree_ext(blk);
        blk = nextBlk;
    }
    pthread_rwlock_unlock(&m_mutex);
}
