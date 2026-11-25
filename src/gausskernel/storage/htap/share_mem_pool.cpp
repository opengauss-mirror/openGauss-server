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
#include "access/datavec/utils.h"
#include "access/htap/share_mem_pool.h"

#define MAX_SHM_CHUNK_NAME_LENGTH 256

static constexpr auto BASE_NID = "";
static constexpr auto SHARE_MEM_NAME_PREFIX = "ss_imcs_shm";

ShareMemoryPool::ShareMemoryPool()
{
    m_relOid = InvalidOid;
    m_shmChunkNum = 0;
    m_shmChunks = nullptr;
}

ShareMemoryPool::~ShareMemoryPool()
{
    m_relOid = InvalidOid;
    m_shmChunkNum = 0;
    m_shmChunks = nullptr;
}

void ShareMemoryPool::Init(Oid relOid)
{
    m_relOid = relOid;
    m_shmChunkNum = 0;
    int ret = pthread_rwlock_init(&m_shm_mutex, nullptr);
    if (ret != 0) {
        ereport(ERROR,
            (errmsg("Failed to initialize rwlock: %d\n.", ret)));
    }
    m_shmChunks = (VectorList<SHMChunk>*)palloc0(sizeof(VectorList<SHMChunk>));
    m_shmChunks->initialize_vector();
}

void ShareMemoryPool::Destroy()
{
    int ret = 0;
    char name[MAX_SHM_CHUNK_NAME_LENGTH];
    for (int i = 0; i < m_shmChunkNum; i++) {
        GetShmChunkName(name, m_relOid, i);
        ret = ubsmem_shmem_unmap((*m_shmChunks)[i].chunkPtr, SHM_CHUNK_SIZE);
        if (ret != 0 && ret != UBSM_ERR_NOT_FOUND) {
            ereport(ERROR,
                    (errmsg("Failed to unmap share memory chunk, chunk name: [%s], code: [%d].", name, ret)));
        }
        if (SS_PRIMARY_MODE) {
            ret = ubsmem_shmem_deallocate(name);
            if (ret != 0 && ret != UBSM_ERR_NOT_FOUND) {
                ereport(ERROR,
                        (errmsg("Failed to delete share memory chunk, chunk name: [%s], code: [%d].", name, ret)));
            }
        }
    }
    m_shmChunks->clear();
    m_shmChunks = nullptr;
}

void ShareMemoryPool::GetShmChunkName(char* chunkName, Oid relOid, int shmChunkNumber)
{
    errno_t rc = EOK;
    rc = sprintf_s(chunkName, MAX_SHM_CHUNK_NAME_LENGTH, "%s_%u_%d", SHARE_MEM_NAME_PREFIX, relOid, shmChunkNumber);
    securec_check_ss_c(rc, "", "");
}

int ShareMemoryPool::CreateNewShmChunk()
{
    int ret = 0;
    int newShmChunkId = m_shmChunkNum;
    void *chunkPtr = nullptr;
    int flags = 0600;
    char name[MAX_SHM_CHUNK_NAME_LENGTH];

    GetShmChunkName(name, m_relOid, newShmChunkId);
    ret = ubsmem_shmem_allocate("default", name, SHM_CHUNK_SIZE, flags, UBSM_FLAG_CACHE);
    if (ret == UBSM_ERR_ALREADY_EXIST) {
        ereport(WARNING, (errmsg("Reuse share memory chunk, name: [%s], code: [%d]", name, ret)));
    } else if (ret != 0) {
        ereport(WARNING, (errmsg("Failed to create share memory chunk, name: [%s], code: [%d]", name, ret)));
        return INVALID_SHM_CHUNK_NUMBER;
    }
    chunkPtr = ShmChunkMmap(name);
    if (chunkPtr == nullptr) {
        ereport(WARNING, (errmsg("Failed to mmap share memory chunk, name: [%s]", name)));
        return INVALID_SHM_CHUNK_NUMBER;
    }

    SHMChunk shmChunk;
    shmChunk.usedSize = 0;
    shmChunk.curCuNum = 0;
    shmChunk.chunkPtr = chunkPtr;
    m_shmChunks->push_back(shmChunk);
    m_shmChunkNum++;
    return newShmChunkId;
}

int ShareMemoryPool::AllocateFreeShmChunk(Size alignCUSize)
{
    /* no available share memory chunk */
    if (m_shmChunkNum == 0) {
        return CreateNewShmChunk();
    }

    /* find one chunk that meets the needSize */
    for (int i = 0; i < m_shmChunkNum; i++) {
        if ((*m_shmChunks)[i].usedSize + alignCUSize <= SHM_CHUNK_SIZE) {
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
        ret = ubsmem_shmem_unmap((*m_shmChunks)[i].chunkPtr, SHM_CHUNK_SIZE);
        if (ret != 0 && ret != UBSM_ERR_NOT_FOUND) {
            ereport(WARNING, (errmsg("Failed to unmap share memory chunk, chunk name: [%s], code: [%d].", name, ret)));
            return UNMAP_SHAREMEM_ERROR;
        }
        ret = ubsmem_shmem_deallocate(name);
        if (ret != 0 && ret != UBSM_ERR_NOT_FOUND) {
            ereport(WARNING, (errmsg("Failed to delete share memory chunk, chunk name: [%s], code: [%d].", name, ret)));
            return DELETE_SHAREMEM_ERROR;
        }
    }
    m_shmChunks->clear();
    m_shmChunks = nullptr;
    return ret;
}

void* ShareMemoryPool::AllocateCUMem(_in_ Size alignCuSize, _out_ uint32 *shmCUOffset, _out_ int *shmChunkNumber)
{
    int chunkNumber = INVALID_SHM_CHUNK_NUMBER;
    void* cuPtr = nullptr;

    pthread_rwlock_wrlock(&m_shm_mutex);
    /* allocate free trunk */
    chunkNumber = AllocateFreeShmChunk(alignCuSize);
    if (!IS_VALID_SHM_CHUNK_NUMBER(chunkNumber)) {
        pthread_rwlock_unlock(&m_shm_mutex);
        return cuPtr;
    }

    SHMChunk* shmChunk = &(*m_shmChunks)[chunkNumber];
    /* allocate cu memory */
    cuPtr = (void*)((char*)shmChunk->chunkPtr + shmChunk->usedSize);
    *shmChunkNumber = chunkNumber;
    *shmCUOffset = shmChunk->usedSize;

    /* update shm chunk header info */
    shmChunk->usedSize += alignCuSize;
    shmChunk->curCuNum++;
    pthread_rwlock_unlock(&m_shm_mutex);
    return cuPtr;
}

void ShareMemoryPool::FreeCUMem(int shmChunkNumber, uint32 shmCUOffset, Size cuSize)
{
    int ret = 0;
    errno_t rc = EOK;
    char name[MAX_SHM_CHUNK_NAME_LENGTH];

    Assert(m_shmChunkNum > 0 && shmChunkNumber < m_shmChunkNum);
    pthread_rwlock_wrlock(&m_shm_mutex);
    (*m_shmChunks)[shmChunkNumber].curCuNum--;
    if ((*m_shmChunks)[shmChunkNumber].curCuNum == 0) {
        void* chunkPtr = (*m_shmChunks)[shmChunkNumber].chunkPtr;
        GetShmChunkName(name, m_relOid, shmChunkNumber);
        if (SS_PRIMARY_MODE) {
            ret = ubsmem_shmem_set_ownership(name, chunkPtr, SHM_CHUNK_SIZE, (int)READ_WIRTE);
            rc = memset_s(chunkPtr, SHM_CHUNK_SIZE, 0, SHM_CHUNK_SIZE);
            securec_check(rc, "", "");
            ret = ubsmem_shmem_set_ownership(name, chunkPtr, SHM_CHUNK_SIZE, (int)ONLY_READ);
        }
        (*m_shmChunks)[shmChunkNumber].usedSize = 0;
    }
    pthread_rwlock_unlock(&m_shm_mutex);
}

void* ShareMemoryPool::LoadCUBuf(int shmChunkNumber, uint32 shmCUOffset, Size cuSize)
{
    void* shmChunkPtr = nullptr;
    void* cuPtr = nullptr;
    int ret = 0;
    char name[MAX_SHM_CHUNK_NAME_LENGTH];
    Size alignCuSize = TYPEALIGN(ALIGNOF_CUSIZE, cuSize);
    Assert(m_shmChunkNum > 0 && shmChunkNumber < m_shmChunkNum);

    ereport(DEBUG1, (errmsg("LoadCUBuf: shmChunkNumber: [%u], shmCUOffset: [%u]: ", shmChunkNumber, shmCUOffset)));
    pthread_rwlock_wrlock(&m_shm_mutex);
    shmChunkPtr = (*m_shmChunks)[shmChunkNumber].chunkPtr;
    cuPtr = (void*)((char*)shmChunkPtr + shmCUOffset);
    (*m_shmChunks)[shmChunkNumber].usedSize += alignCuSize;
    (*m_shmChunks)[shmChunkNumber].curCuNum++;
    pthread_rwlock_unlock(&m_shm_mutex);

    GetShmChunkName(name, m_relOid, shmChunkNumber);
    ret = ubsmem_shmem_set_ownership(name, cuPtr, alignCuSize, ONLY_READ);
    if (ret != 0) {
        ereport(WARNING, (errmsg("ubsmem shmem set ownership failed, name: [%s], ret: [%d].", name, ret)));
        return nullptr;
    }
    return cuPtr;
}

/* for ss standby mmap share memory */
void ShareMemoryPool::ShmChunkMmapAll(int shmChunksNum)
{
    char name[MAX_SHM_CHUNK_NAME_LENGTH];
    void *chunkPtr = nullptr;

    pthread_rwlock_wrlock(&m_shm_mutex);
    for (int i = m_shmChunkNum; i < shmChunksNum; i++) {
        GetShmChunkName(name, m_relOid, i);
        chunkPtr = ShmChunkMmap(name);
        if (chunkPtr == nullptr) {
            pthread_rwlock_unlock(&m_shm_mutex);
            ereport(ERROR, (errmsg("HTAP: ss_imcstore mmap share memory [%s] failed.", name)));
        }
        SHMChunk shmChunk;
        shmChunk.usedSize = 0;
        shmChunk.curCuNum = 0;
        shmChunk.chunkPtr = chunkPtr;
        m_shmChunkNum++;
        m_shmChunks->push_back(shmChunk);
    }
    pthread_rwlock_unlock(&m_shm_mutex);
}

void* ShareMemoryPool::ShmChunkMmap(char *name)
{
    void *addr = nullptr;
    ubsmem_shmem_map(nullptr, SHM_CHUNK_SIZE, PROT_READ, MAP_SHARED, name, 0, &addr);
    return static_cast<char*>(addr);
}

int ShareMemoryPool::GetChunkNum()
{
    pthread_rwlock_rdlock(&m_shm_mutex);
    int chunkNum = m_shmChunkNum;
    pthread_rwlock_unlock(&m_shm_mutex);
    return chunkNum;
}

void* ShareMemoryPool::CopyShareCUMem(_in_ char* localCUPtr, _in_ uint32 cuSize,
    _out_ uint32 *shmCUOffset, _out_ int *shmChunkNumber)
{
    int ret = 0;
    void* shareCUPtr = nullptr;
    char name[MAX_SHM_CHUNK_NAME_LENGTH];
    errno_t rc = EOK;
    Size alignCuSize = TYPEALIGN(ALIGNOF_CUSIZE, cuSize);

    shareCUPtr = AllocateCUMem(alignCuSize, shmCUOffset, shmChunkNumber);
    if (shareCUPtr == nullptr) {
        return shareCUPtr;
    }

    GetShmChunkName(name, m_relOid, *shmChunkNumber);
    ret = ubsmem_shmem_set_ownership(name, shareCUPtr, alignCuSize, READ_WIRTE);
    if (ret != 0) {
        ereport(WARNING, (errmsg("ubsmem shmem set ownership failed, name: [%s], ret: [%d].", name, ret)));
        return nullptr;
    }

    rc = memcpy_s(shareCUPtr, alignCuSize, localCUPtr, cuSize);
    securec_check(rc, "\0", "\0");

    ret = ubsmem_shmem_set_ownership(name, shareCUPtr, alignCuSize, ONLY_READ);
    if (ret != 0) {
        ereport(WARNING, (errmsg("ubsmem shmem set ownership failed, name: [%s], ret: [%d].", name, ret)));
        return nullptr;
    }
    return shareCUPtr;
}

