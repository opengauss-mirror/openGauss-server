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
 * cucache_mgr.h
 *        routines to support ColStore
 * 
 * 
 * IDENTIFICATION
 *        src/include/storage/cucache_mgr.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef CUCACHEMGR_H
#define CUCACHEMGR_H

#include "postgres.h"
#include "knl/knl_variable.h"
#include "storage/smgr/relfilenode.h"
#include "vecexecutor/vectorbatch.h"
#include "utils/hsearch.h"
#include "storage/lock/lwlock.h"
#include "storage/spin.h"
#include "storage/shmem.h"
#include "storage/cu.h"
#include "storage/cache_mgr.h"
#include "storage/custorage.h"

#define CUCache (DataCacheMgr::GetInstance())
#define ORCCache (DataCacheMgr::GetInstance())
#define OBSCache (DataCacheMgr::GetInstance())

int DataCacheMgrNumLocks();

// MAX_CACHE_TAG_LEN  is max slot tag size
typedef struct CUSlotTag {
    RelFileNodeOld m_rnode;
    int m_colId;
    int32 m_CUId;
    uint32 m_padding;
    CUPointer m_cuPtr;
} CUSlotTag;

typedef struct ORCSlotTag {
    RelFileNodeOld m_rnode;
    int32 m_fileId;
    uint64 m_offset;
    uint64 m_length;
} ORCSlotTag;

/*
 * be careful, the length of OBSSlotTag
 * shoud be <= 32 bytes. The
 * max length is defined by macro
 * MAX_CACHE_TAG_LEN.
 */
typedef struct OBSSlotTag {
    uint32 m_serverHash;
    uint32 m_bucketHash;
    uint32 m_fileFirstHash;
    uint32 m_fileSecondHash;

    uint64 m_offset;
    uint64 m_length;
} OBSSlotTag;

typedef union DataSlotTagKey {
    CUSlotTag cuSlotTag;
    ORCSlotTag orcSlotTag;
    OBSSlotTag obsSlotTag;
} DataSlotTagKey;

typedef struct DataSlotTag {
    DataSlotTagKey slotTag;
    CacheType slotType;
} DataSlotTag;

typedef struct OrcDataValue {
    void* value;
    uint64 size;
} OrcDataValue;

/* returned code about uncompressing CU data in CU cache */
enum CUUncompressedRetCode { CU_OK = 0, CU_ERR_CRC, CU_ERR_MAGIC, CU_ERR_ADIO, CU_RELOADING, CU_ERR_MAX };

/*
 * This class is to manage Data Cache.
 * For integer CU, we should store compressed data.
 * For string CU, because the speed of decompressing data is low,
 * so store source data.
 */
class DataCacheMgr : public BaseObject {
public:
    static DataCacheMgr* GetInstance(void);
    static void NewSingletonInstance(void);

    DataSlotTag InitCUSlotTag(RelFileNodeOld* rnode, int colid, uint32 cuid, CUPointer cuPtr);
    DataSlotTag InitORCSlotTag(RelFileNode* rnode, int32 fileid, uint64 offset, uint64 length);
    DataSlotTag InitOBSSlotTag(uint32 hostNameHash, uint32 bucketNameHash, uint32 fileFirstHalfHash,
        uint32 fileSecondHalfHash, uint64 offset, uint64 length) const;
    CacheSlotId_t FindDataBlock(DataSlotTag* dataSlotTag, bool first_enter_block);
    int ReserveDataBlock(DataSlotTag* dataSlotTag, int size, bool& hasFound);
    bool ReserveDataBlockWithSlotId(int slotId);
    bool ReserveCstoreDataBlockWithSlotId(int slotId);
    CU* GetCUBuf(int cuSlotId);
    OrcDataValue* GetORCDataBuf(int cuSlotId);
    void UnPinDataBlock(int cuSlotId);
    /* Manage I/O busy CUs */
    bool DataBlockWaitIO(int cuSlotId);
    void DataBlockCompleteIO(int cuSlotId);
    int64 GetCurrentMemSize();
    void PrintDataCacheSlotLeakWarning(CacheSlotId_t slotId);

    void AbortCU(CacheSlotId_t slot);
    void TerminateCU(bool abort);
    void TerminateVerifyCU();
    void InvalidateCU(RelFileNodeOld* rnode, int colId, uint32 cuId, CUPointer cuPtr);
    void DropRelationCUCache(const RelFileNode& rnode);
    CUUncompressedRetCode StartUncompressCU(CUDesc* cuDescPtr, CacheSlotId_t slotId, int planNodeId, bool timing, int align_size);

    // async lock used by adio
    bool CULWLockHeldByMe(CacheSlotId_t slotId);
    void CULWLockOwn(CacheSlotId_t slotId);
    void CULWLockDisown(CacheSlotId_t slotId);

    void LockPrivateCache();
    void UnLockPrivateCache();

    void SetORCDataBlockValue(CacheSlotId_t slotId, const void* buffer, uint64 size);
    void SetOBSDataBlockValue(
        CacheSlotId_t slotId, const void* buffer, uint64 size, const char* prefix, const char* dataDNA);

    void AcquireCompressLock(CacheSlotId_t slotId);
    void ReleaseCompressLock(CacheSlotId_t slotId);

    int64 m_cstoreMaxSize;

#ifndef ENABLE_UT
private:
#endif  // ENABLE_UT

    DataCacheMgr()
    {}
    ~DataCacheMgr()
    {}

    static DataCacheMgr* m_data_cache;
    CacheMgr* m_cache_mgr;
    slock_t m_adio_write_cache_lock;  // write private cache, not cucache. I add here because spinlock need init once
                                      // for cstore module
};

/* interface for orc data read */
void ReleaseORCBlock(CacheSlotId_t slot);
CacheSlotId_t ORCCacheAllocBlock(
    RelFileNode* rnode, int32 fileid, uint64 offset, uint64 length, bool& found, bool& err_found);
CacheSlotId_t OBSCacheAllocBlock(const char* hostName, const char* bucketName, const char* prefixName, uint64 offset,
    uint64 length, bool& found, bool& err_found);
bool OBSCacheRenewBlock(CacheSlotId_t slotID);
OrcDataValue* ORCCacheGetBlock(CacheSlotId_t slot);
void ORCCacheSetBlock(CacheSlotId_t slotId, const void* buffer, uint64 size);
void OBSCacheSetBlock(CacheSlotId_t slotId, const void* buffer, uint64 size, const char* prefix, const char* dataDNA);

#endif  // define
