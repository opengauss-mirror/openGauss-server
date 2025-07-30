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
 * share_mem_pool.cpp
 *      routines to support DSS IMColStore
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/htap/share_mem_pool.cpp
 * ---------------------------------------------------------------------------------------
 */

#include "postgres.h"
#include "securec.h"
#include "knl/knl_instance.h"
#include "access/htap/imcs_hash_table.h"
#include "access/htap/share_mem_pool.h"

#define MAX_SHM_CHUNK_NAME_LENGTH 256

static constexpr auto BASE_NID = "";
static constexpr auto SHARE_MEM_NAME_PREFIX = "ss_imcs_shm";

ShareMemoryPool::ShareMemoryPool(Oid relOid)
{
    pg_atomic_init_u64(&m_allocatedMemSize, 0);
    pg_atomic_init_u64(&m_usedMemSize, 0);
    m_shmChunkNum = 0;
    m_shm_mutex = PTHREAD_RWLOCK_INITIALIZER;
    m_relOid = relOid;
}

ShareMemoryPool::~ShareMemoryPool()
{}

void ShareMemoryPool::Destroy()
{
    int ret = 0;
    char name[MAX_SHM_CHUNK_NAME_LENGTH];
    for (int i = 0; i < m_shmChunkNum; i++) {
        GetShmChunkName(name, m_relOid, i);
        void* shmChunkPtr = m_shmChunks[i];
        ret = RackMemShmUnmmap(shmChunkPtr, SHM_CHUNK_SIZE);
        if (ret != 0) {
            ereport(ERROR,
                    (errmsg("Failed to unmap share memory chunk, chunk name: [%s], code: [%d].", name, ret)));
        }
        shmChunkPtr = nullptr;
        if (SS_PRIMARY_MODE) {
            ret = RackMemShmDelete(name);
            if (ret != 0) {
                ereport(ERROR,
                        (errmsg("Failed to delete share memory chunk, chunk name: [%s], code: [%d].", name, ret)));
            }
        }
    }
}

void ShareMemoryPool::GetShmChunkName(char* chunkName, Oid relOid, int shmChunkNumber)
{
    errno_t rc = EOK;
    rc = sprintf_s(chunkName, MAX_SHM_CHUNK_NAME_LENGTH, "%s_%d_%d", SHARE_MEM_NAME_PREFIX, relOid, shmChunkNumber);
    securec_check_ss_c(rc, "", "");
}

int ShareMemoryPool::CreateNewShmChunk()
{
    int ret = 0;
    int newShmChunkId = m_shmChunkNum;
    void *shmChunkPtr = nullptr;
    SHMRegions regions = SHMRegions();
    char name[MAX_SHM_CHUNK_NAME_LENGTH];

    ret = RackMemShmLookupShareRegions(BASE_NID, ShmRegionType::INCLUDE_ALL_TYPE, &regions);
    // todo, 此处可能返回多个共享域，当前先默认是0
    if (ret != 0 || regions.region[0].num <= 0) {
        ereport(WARNING,
                (errmsg(
                    "Failed to lookup share memory regions, code: [%d], node num: [%d].", ret, regions.region[0].num)));
        return INVALID_SHM_CHUNK_NUMBER;
    }
    GetShmChunkName(name, m_relOid, newShmChunkId);
    ret = RackMemShmCreate(name, SHM_CHUNK_SIZE, BASE_NID, &regions.region[0]);
    if (ret == E_CODE_RESOURCE_EXIST) {
        ereport(WARNING, (errmsg("Reuse share memory chunk, name: [%s], code: [%d]", name, ret)));
    } else if (ret != 0) {
        ereport(WARNING, (errmsg("Failed to create share memory chunk, name: [%s], code: [%d]", name, ret)));
        return INVALID_SHM_CHUNK_NUMBER;
    }
    shmChunkPtr = ShmChunkMmap(name);
    if (shmChunkPtr == nullptr) {
        ereport(WARNING, (errmsg("Failed to mmap share memory chunk, name: [%s]", name)));
        return INVALID_SHM_CHUNK_NUMBER;
    }

    ((SHMChunkHeader*)shmChunkPtr)->usedSize = SHM_CHUNK_HDSZ;
    ((SHMChunkHeader*)shmChunkPtr)->curCuNum = 0;
    m_shmChunks.push_back(shmChunkPtr);
    pg_atomic_add_fetch_u64(&m_allocatedMemSize, (uint64)SHM_CHUNK_SIZE);
    pg_atomic_add_fetch_u64(&m_usedMemSize, (uint64)SHM_CHUNK_HDSZ);
    m_shmChunkNum++;
    return newShmChunkId;
}

int ShareMemoryPool::AllocateFreeShmChunk(Size needSize)
{
    /* no available share memory chunk */
    if (m_shmChunkNum == 0) {
        return CreateNewShmChunk();
    }

    /* find one chunk that meets the needSize */
    for (int i = 0; i < m_shmChunkNum; i++) {
        SHMChunkHeader* shmChunkHeader = (SHMChunkHeader*)m_shmChunks[i];
        if (shmChunkHeader->usedSize + needSize <= SHM_CHUNK_SIZE) {
            return i;
        }
    }

    /* no free trunk meets the need, so create new chunk */
    return CreateNewShmChunk();
}

int ShareMemoryPool::DestoryShmChunk()
{
    int ret = 0;
    char name[MAX_SHM_CHUNK_NAME_LENGTH];
    for (int i = 0; i < m_shmChunkNum; i++) {
        GetShmChunkName(name, m_relOid, i);
        void* shmChunkPtr = m_shmChunks[i];
        ret = RackMemShmUnmmap(shmChunkPtr, SHM_CHUNK_SIZE);
        if (ret != 0) {
            ereport(WARNING, (errmsg("Failed to unmap share memory chunk, chunk name: [%s], code: [%d].", name, ret)));
            return UNMAP_SHAREMEM_ERROR;
        }
        shmChunkPtr = nullptr;
        ret = RackMemShmDelete(name);
        if (ret != 0) {
            ereport(WARNING, (errmsg("Failed to delete share memory chunk, chunk name: [%s], code: [%d].", name, ret)));
            return DELETE_SHAREMEM_ERROR;
        }
    }
    return ret;
}

void* ShareMemoryPool::AllocateCUMem(_in_ Size size, _in_ CacheSlotId_t slot,
                                     _out_ uint32 *shmCUOffset, _out_ int *shmChunkNumber)
{
    int chunkNumber = INVALID_SHM_CHUNK_NUMBER;
    void *shmCuPtr = nullptr;
    void *shmChunkPtr = nullptr;
    Size smpChunkSize = size + SMP_CHUNK_HDSZ;

    pthread_rwlock_wrlock(&m_shm_mutex);
    /* allocate free trunk */
    chunkNumber = AllocateFreeShmChunk(smpChunkSize);
    if (!IS_VALID_SHM_CHUNK_NUMBER(chunkNumber)) {
        pthread_rwlock_unlock(&m_shm_mutex);
        ereport(ERROR, (errmsg("Failed to allocate share memory for cu")));
    }

    shmChunkPtr = m_shmChunks[chunkNumber];
    SHMChunkHeader* shmChunkHeader = (SHMChunkHeader*)shmChunkPtr;

    /* allocate cu memory */
    shmCuPtr = (char*)shmChunkPtr + shmChunkHeader->usedSize;
    ((SMPChunkHeader*)shmCuPtr)->size = size;
    ((SMPChunkHeader*)shmCuPtr)->slot = slot;
    *shmChunkNumber = chunkNumber;
    *shmCUOffset = shmChunkHeader->usedSize;

    /* update shm chunk header info */
    shmChunkHeader->usedSize += smpChunkSize;
    shmChunkHeader->curCuNum++;
    pg_atomic_add_fetch_u64(&m_usedMemSize, (uint64)smpChunkSize);
    pthread_rwlock_unlock(&m_shm_mutex);
    return (char*)shmCuPtr + SMP_CHUNK_HDSZ;
}

void ShareMemoryPool::FreeCUMem(int shmChunkNumber, uint32 shmCUOffset)
{
    Assert(m_shmChunkNum > 0 && shmChunkNumber < m_shmChunkNum);

    pthread_rwlock_wrlock(&m_shm_mutex);
    void* shmChunkPtr = m_shmChunks[shmChunkNumber];
    /* current free base on shm chunk, cu mem mark delete */
    void* shmCuPtr = (char*)shmChunkPtr + shmCUOffset;
    SMPChunkHeader* smpChunkHeader = (SMPChunkHeader*)shmCuPtr;
    smpChunkHeader->slot = CACHE_BLOCK_INVALID_IDX;
    pg_atomic_sub_fetch_u64(&m_usedMemSize, (uint64)(smpChunkHeader->size + SMP_CHUNK_HDSZ));
    /* update cur shm chunk cu nums */
    ((SHMChunkHeader*)shmChunkPtr)->curCuNum--;

    if (((SHMChunkHeader*)shmChunkPtr)->curCuNum == 0) {
        ResetShmChunk(shmChunkPtr);
    }
    pthread_rwlock_unlock(&m_shm_mutex);
}

void* ShareMemoryPool::GetCUBuf(int shmChunkNumber, uint32 shmCUOffset)
{
    Assert(m_shmChunkNum > 0 && shmChunkNumber < m_shmChunkNum);

    pthread_rwlock_rdlock(&m_shm_mutex);
    void* shmChunkPtr = m_shmChunks[shmChunkNumber];
    pthread_rwlock_unlock(&m_shm_mutex);
    return (char*)shmChunkPtr + shmCUOffset + SMP_CHUNK_HDSZ;
}

void ShareMemoryPool::ResetShmChunk(void *shmChunkPtr)
{
    errno_t rc = EOK;
    void *dataPtr = (char*)shmChunkPtr + SHM_CHUNK_HDSZ;
    rc = memset_s(dataPtr, SHM_CHUNK_SIZE - SHM_CHUNK_HDSZ, 0, SHM_CHUNK_SIZE - SHM_CHUNK_HDSZ);
    securec_check(rc, "", "");
    ((SHMChunkHeader*)shmChunkPtr)->usedSize = SHM_CHUNK_HDSZ;
    ((SHMChunkHeader*)shmChunkPtr)->curCuNum = 0;
}

/* for ss standby mmap share memory */
void ShareMemoryPool::ShmChunkMmapAll(int shmChunksNum)
{
    char shmChunkName[MAX_SHM_CHUNK_NAME_LENGTH];
    void *shmChunkPtr = nullptr;

    pthread_rwlock_wrlock(&m_shm_mutex);
    for (int i = m_shmChunkNum; i < shmChunksNum; i++) {
        GetShmChunkName(shmChunkName, m_relOid, i);
        shmChunkPtr = ShmChunkMmap(shmChunkName);
        if (shmChunkPtr == nullptr) {
            pthread_rwlock_unlock(&m_shm_mutex);
            ereport(ERROR, (errmsg("HTAP: dss_imcstore mmap share memory [%s] failed.", shmChunkName)));
        }
        m_shmChunkNum++;
        pg_atomic_add_fetch_u64(&m_allocatedMemSize, (uint64)SHM_CHUNK_SIZE);
        pg_atomic_add_fetch_u64(&m_usedMemSize, (uint64)((SHMChunkHeader*)shmChunkPtr)->usedSize);
        m_shmChunks.push_back(shmChunkPtr);
    }
    pthread_rwlock_unlock(&m_shm_mutex);
}

void* ShareMemoryPool::ShmChunkMmap(char *name)
{
    return static_cast<char*>(RackMemShmMmap(nullptr, SHM_CHUNK_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, name, 0));
}

void ShareMemoryPool::FlushShmChunkAll(ShmCacheOpt shmCacheOpt)
{
    void *shmChunkPtr = nullptr;

    pthread_rwlock_wrlock(&m_shm_mutex);
    for (int i = 0; i < m_shmChunkNum; i++) {
        shmChunkPtr = m_shmChunks[i];
        int ret = RackMemShmCacheOpt(shmChunkPtr, SHM_CHUNK_SIZE, shmCacheOpt);
        if (ret != 0) {
            pthread_rwlock_unlock(&m_shm_mutex);
            ereport(ERROR, (errmsg("HTAP: dss imcstore flush share memory failed, chunk number: [%d].", i)));
        }
    }
    pthread_rwlock_unlock(&m_shm_mutex);
}

int ShareMemoryPool::GetChunkNum()
{
    pthread_rwlock_rdlock(&m_shm_mutex);
    int chunkNum = m_shmChunkNum;
    pthread_rwlock_unlock(&m_shm_mutex);
    return chunkNum;
}