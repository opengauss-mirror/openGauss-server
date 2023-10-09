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
 * cache_mgr.h
 *        routines to support common cache
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/storage/cache_mgr.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef CacheMgr_H
#define CacheMgr_H

#include "postgres.h"
#include "knl/knl_variable.h"
#include "utils/hsearch.h"
#include "storage/lock/lwlock.h"
#include "storage/spin.h"

// CU Cache Pool sizes.
//

#define BUILD_BUG_ON(condition) ((void)sizeof(char[1 - 2 * (int)(!!(condition))]))

typedef int CacheSlotId_t;

// Cache Block flags
typedef unsigned char CacheFlags;               // Modified by CUSlotDesc and CacheMgr
const unsigned char CACHE_BLOCK_NEW = 0x00;     // Slot is not on any list
const unsigned char CACHE_BLOCK_VALID = 0x01;   // In Hashtable and contains valid data
const unsigned char CACHE_BLOCK_INFREE = 0x02;  // slot is in free state
const unsigned char CACHE_BLOCK_FREE = 0x04;    // Slot is on the free list
const unsigned char CACHE_BLOCK_IOBUSY = 0x08;  // Slot buf r/w in progress
const unsigned char CACHE_BLOCK_ERROR = 0x10;   // Slot buf  error

// Special Slot Index values
const int CACHE_BLOCK_INVALID_IDX = -1;  // Invalid index -end of this list
const int CACHE_BLOCK_NO_LIST = -2;      // Invalid index -not in any list

#define IsValidCacheSlotID(x) ((x) != CACHE_BLOCK_INVALID_IDX)

// Max usage count for CLOCK cache strategy
const uint16 CACHE_BLOCK_MAX_USAGE = 5;

/* common buffer cache function for cu cache and orc cache */
#define MAX_CACHE_TAG_LEN (32)

typedef enum CacheType {
    /*only used for init cache type, for not exist type*/
    CACHE_TYPE_NONE = 0,

    /* data block*/
    CACHE_COlUMN_DATA,
    CACHE_ORC_DATA,
    CACHE_OBS_DATA,
    CACHE_CARBONDATA_DATA,

    /*index block*/
    CACHE_ORC_INDEX,

    /* metadata block */
    CACHE_CARBONDATA_METADATA
} CacheType;

typedef enum MgrCacheType {
    /* cache manager type */
    MGR_CACHE_TYPE_DATA,
    MGR_CACHE_TYPE_INDEX
} MgrCacheType;

typedef struct CacheTag {
    /*cu
    RelFileNode rnode;
    int32 col_id;
    int32 cu_id;
    uint32 padding;
    CUPointer offset;
    */

    /*orc compress data
    RelFileNode m_rnode;
    int32 m_fileId;
    uint64	m_offset;
    uint64  m_length;
    */

    /*orc meta data
    RelFileNode fileNode;
    int32 fileID;
    uint32 stripeID;
    uint32 columnID;
    */

    /*common cache tag*/
    int32 type;
    char key[MAX_CACHE_TAG_LEN];
} CacheTag;

typedef struct CacheDesc {
    uint16 m_usage_count;
    uint16 m_ring_count;
    uint32 m_refcount;
    CacheTag m_cache_tag;

    CacheSlotId_t m_slot_id;
    CacheSlotId_t m_freeNext;

    /* Light wight lock: i/o busy lock */
    LWLock *m_iobusy_lock;

    /*
     * Light wight lock: used for CU
     * compress/uncompress busy lock.
     * If there are two or more threads
     * read one CU where is uncompressing
     * we must wait.
     */
    LWLock *m_compress_lock;

    /*The data size in the one slot.*/
    int m_datablock_size;

    /*
     * This flag used in OBS foreign table.
     * Think a scenario, a file located on OBS and
     * touched by DW through foreign table. We
     * cached part of this file. Now two threads
     * want to read this cache and find the file
     * eTag has changed, it is this session's responsibility
     * to update this cache. There must be a machnism
     * to avoid two or more threads to update it concurrently.
     * If thread find m_refreshing is yes. It not wait for
     * updating finish, but read data from OBS directly.
     */
    bool m_refreshing;

    slock_t m_slot_hdr_lock;

    CacheFlags m_flag;
} CacheDesc;

int CacheMgrNumLocks(int64 cache_size, uint32 each_block_size);
int64 CacheMgrCalcSizeByType(MgrCacheType type);

/*
 * This class is to manage Common Cache.
 */
class CacheMgr : public BaseObject {
public:
    CacheMgr(){};
    ~CacheMgr(){};
    void Init(int64 cache_size, uint32 each_block_size, MgrCacheType type, uint32 each_slot_length);
    void Destroy(void);

    /* operate cache block */
    void InitCacheBlockTag(CacheTag* cacheTag, int32 type, const void* key, int32 length) const;
    CacheSlotId_t FindCacheBlock(CacheTag* cacheTag, bool first_enter_block);
    void InvalidateCacheBlock(CacheTag* cacheTag);
    void DeleteCacheBlock(CacheTag* cacheTag);
    CacheSlotId_t ReserveCacheBlock(CacheTag* cacheTag, int size, bool& hasFound);
    bool ReserveCacheBlockWithSlotId(CacheSlotId_t slotId);
    bool ReserveCstoreCacheBlockWithSlotId(CacheSlotId_t slotId);
    void* GetCacheBlock(CacheSlotId_t slotId);
    const CacheTag* GetCacheBlockTag(CacheSlotId_t slotId, uint32* refcount) const;
    bool PinCacheBlock(CacheSlotId_t slotId);
    void UnPinCacheBlock(CacheSlotId_t slotId);

    /* Manage I/O busy cache block */
    bool IsIOBusy(CacheSlotId_t cuSlotId);
    bool WaitIO(CacheSlotId_t slotId);
    void CompleteIO(CacheSlotId_t cuSlotId);

    /* async lock used by adio */
    bool LockHeldByMe(CacheSlotId_t slotId);
    void LockOwn(CacheSlotId_t slotId);
    void LockDisown(CacheSlotId_t slotId);

    /* compress lock */
    void AcquireCompressLock(CacheSlotId_t slotId);
    void ReleaseCompressLock(CacheSlotId_t slotId);

    /* ring strategy, not used for now */
    void IncRingCount(CacheSlotId_t slotId);
    void DecRingCount(CacheSlotId_t slotId);

    /* memory operate */
    int64 GetCurrentMemSize();
    void ReleaseCacheMem(int size);
    bool CompressLockHeldByMe(CacheSlotId_t slotId);

    /* IO lock */
    void AcquireCstoreIOLock(CacheSlotId_t slotId);
    void ReleaseCstoreIOLock(CacheSlotId_t slotId);
    bool CstoreIOLockHeldByMe(CacheSlotId_t slotId);
    void AdjustCacheMem(CacheSlotId_t slotId, int oldSize, int newSize);

    /* error handle */
    void AbortCacheBlock(CacheSlotId_t slotId);
    void SetCacheBlockErrorState(CacheSlotId_t slotId);

    /*
     * get the number of cache slot now used.
     * notice the number will be bigger and bigger, not smaller.
     */
    int GetUsedCacheSlotNum(void)
    {
        return m_CaccheSlotMax;
    }
    void CopyCacheBlockTag(CacheSlotId_t slotId, CacheTag* outTag);

    char* m_CacheSlots;

#ifndef ENABLE_UT
private:
#endif  // ENABLE_UT

    uint32 GetHashCode(CacheTag* cacheTag);

    /* internal block operate */
    CacheSlotId_t EvictCacheBlock(int size, int retryNum);
    CacheSlotId_t GetFreeCacheBlock(int size);

    /* memory operate */
    bool ReserveCacheMem(int size);
    void FreeCacheBlockMem(CacheSlotId_t slotId);
    int GetCacheBlockMemSize(CacheSlotId_t slotId);

    /* free list */
    bool CacheFreeListEmpty() const;
    int GetFreeListCache();
    void PutFreeListCache(CacheSlotId_t freeSlotIdx);

    /* lock */
    LWLock *LockHashPartion(uint32 hashCode, LWLockMode lockMode) const;
    void UnLockHashPartion(uint32 hashCode) const;
    void LockCacheDescHeader(CacheSlotId_t slot);
    void UnLockCacheDescHeader(CacheSlotId_t slot);
    void LockSweep();
    void UnlockSweep();

    bool CacheBlockIsPinned(CacheSlotId_t slotId) const;
    void PinCacheBlock_Locked(CacheSlotId_t slotId);

    CacheSlotId_t AllocateBlockFromCache(CacheTag* cacheTag, uint32 hashCode, int size, bool& hasFound);
    void AllocateBlockFromCacheWithSlotId(CacheSlotId_t slotId);
    void WaitEvictSlot(CacheSlotId_t slotId);

    MgrCacheType m_cache_type;
    HTAB* m_hash;
    uint32 m_slot_length;
    CacheDesc* m_CacheDesc;

    int m_CacheSlotsNum; /* total slot num */
    int m_CaccheSlotMax; /* used max slot id */

    int64 m_cstoreMaxSize;
    int64 m_cstoreCurrentSize;

    int m_freeListHead;
    int m_freeListTail;
    slock_t m_freeList_lock;

    int m_csweep;
    LWLock *m_csweep_lock;

    int m_partition_lock;

    /* protect memory size counter */
    slock_t m_memsize_lock;
};

#endif  // define
