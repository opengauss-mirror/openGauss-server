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
 * share_mem_pool.h
 *        routines to support DSS IMColStore
 *
 *
 * IDENTIFICATION
 *        src/include/access/htap/share_mem_pool.h
 *
 * ---------------------------------------------------------------------------------------
 */

 #ifndef SHARE_MEM_POOL_H
 #define SHARE_MEM_POOL_H
 #include <vector>
 #include "postgres.h"
 #include "knl/knl_variable.h"
 #include "storage/cache_mgr.h"
 #include "storage/rack_mem_shm.h"
 
 #define SHM_CHUNK_SIZE (128 * 1024 * 1024)  /* 128MB */
 #define INVALID_SHM_CHUNK_NUMBER (-1)
 #define IS_VALID_SHM_CHUNK_NUMBER(x) ((x) != INVALID_SHM_CHUNK_NUMBER)
 
 typedef struct {
     Size usedSize;
     int curCuNum;
 } SHMChunkHeader;
 
 typedef struct {
     Size size;
     CacheSlotId_t slot;
 } SMPChunkHeader;
 
 typedef enum ShareMemErrorCode : uint16_t {
     UNMAP_SHAREMEM_ERROR = 5001,
     DELETE_SHAREMEM_ERROR
 } SMErrorCode;
 
 #define SHM_CHUNK_HDSZ (sizeof(SHMChunkHeader))
 #define SMP_CHUNK_HDSZ (sizeof(SMPChunkHeader))
 
 /*
 * This class provides some API for dss imcstore share memory.
  */
 class ShareMemoryPool : public BaseObject {
 public:
     static void GetShmChunkName(char* chunkName, Oid relOid, int shmChunkNumber);
 
     void* AllocateCUMem(_in_ Size size, _in_ CacheSlotId_t slot,
                         _out_ uint32 *shmCUOffset, _out_ int *shmChunkNumber);
 
     void FreeCUMem(int shmChunkNumber, uint32 shmCUOffset);
     void* GetCUBuf(int shmChunkNumber, uint32 shmCUOffset);
     void ShmChunkMmapAll(int shmChunksNum);
     void FlushShmChunkAll(ShmCacheOpt shmCacheOpt);
 
     int GetChunkNum();
 
     Oid m_relOid;
     int m_shmChunkNum;
     pg_atomic_uint64 m_usedMemSize;
     pg_atomic_uint64 m_allocatedMemSize;
     ShareMemoryPool(Oid relOid);
     ~ShareMemoryPool();
     void Destroy();
     int DestoryShmChunk();
 
 private:
     int CreateNewShmChunk();
     int AllocateFreeShmChunk(Size needSize);
     void* ShmChunkMmap(char *name);
     void ResetShmChunk(void *shmChunkPtr);
 
     std::vector<void*> m_shmChunks;
     pthread_rwlock_t m_shm_mutex;
 };
 
 #endif /* SHARE_MEM_POOL_H */