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
 * -------------------------------------------------------------------------
 *
 * cache_mgr.cpp
 *		routines to support common cache
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/cmgr/cache_mgr.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "storage/dfs/dfscache_mgr.h"

#include "postgres.h"
#include "knl/knl_variable.h"
#include "storage/cache_mgr.h"
#include "storage/cu.h"
#include "utils/aiomem.h"
#include "utils/resowner.h"
#include "storage/ipc.h"
#include "miscadmin.h"

const int MAX_LOOPS = 16;

const int MAX_RETRY_NUM = 3;

typedef struct CacheLookupEnt {
    CacheTag cache_tag;
    CacheSlotId_t slot_id;
} CacheLookupEnt;

// 2147483648 = 2 *1024*1024*1024 == 2G
#define MAX_METADATA_CACHE_SIZE 2147483648

// 2097152 = 2 * 1024 * 1024 = 2M
#define MAX_CACHE_SLOT_COUNT 2097152

#define CHECK_CACHE_SLOT_STATUS()                                                                                    \
    {                                                                                                                \
        if (m_csweep == start) {                                                                                     \
            looped++;                                                                                                \
            if (looped > MAX_LOOPS) {                                                                                 \
                if (retryNum > MAX_RETRY_NUM) {                                                                        \
                    ereport(ERROR,                                                                                   \
                            (errcode(ERRCODE_OUT_OF_BUFFER), errmodule(MOD_CACHE),                                   \
                             errmsg("No free Cache Blocks! cstore_buffers maybe too small, scanned=%d,"              \
                                    " pinned=%d, unpinned=%d, invalid=%d, looped=%d, reserved=%d, freepinned = %d, " \
                                    "start=%d, max=%d. RequestSize = %d, CurrentSize = %ld, BufferMaxSize = %ld.",   \
                                    scanned, pinned, unpinned, invalid, looped, reserved, freepinned, start, max,    \
                                    size, m_cstoreCurrentSize, m_cstoreMaxSize)));                                   \
                } else {                                                                                             \
                    UnlockSweep();                                                                                   \
                    return CACHE_BLOCK_INVALID_IDX;                                                                  \
                }                                                                                                    \
            }                                                                                                        \
        }                                                                                                            \
    }

/*
 * @Description: CacheMgrNumLocks
 * Returns the number of LW locks required by the CacheMgr instance.
 * This function is called by NumLWLocks() to calculate the required memory
 * for all the LW Locks.  Since this must be done prior to initializing the
 * instance of the CacheMgr class, the function cannot be defined
 * as a method of the class. the locks number acording to total slots of cache manager
 * @Return: lock number
 * @See also:
 */
int CacheMgrNumLocks(int64 cache_size, uint32 each_block_size)
{
    /* One LW Lock for each cache block include IO lock and compress lock */
    return (Min(cache_size / each_block_size, MAX_CACHE_SLOT_COUNT)) * 2;
}

/*
 * @Description: calc cache size by tyey, meta data cache size 1/4 of cstore buffers and max is 2G,
 * data cache use the left size
 * @IN type: cache type
 * @Return: cache size
 * @See also:
 */
int64 CacheMgrCalcSizeByType(MgrCacheType type)
{
    int64 size = g_instance.attr.attr_storage.cstore_buffers * 1024LL * 1 / 4;
    int64 cache_size = Min(size, MAX_METADATA_CACHE_SIZE);
    if (!g_instance.attr.attr_sql.enable_orc_cache) {
        cache_size = 1024 * 1024;
    }

    if (type == MGR_CACHE_TYPE_DATA) {
        cache_size = g_instance.attr.attr_storage.cstore_buffers * 1024LL - cache_size;
    }
    return cache_size;
}

/*
 * @Description: init all resource of cache instance
 * @IN cache_size: to tal cache size
 * @IN each_block_size: each cache block size, to devide cache slots
 * @IN type: cache type
 * @IN each_slot_size: slot struct size, ep:sizeof(CU)
 * @See also:
 */
void CacheMgr::Init(int64 cache_size, uint32 each_block_size, MgrCacheType type, uint32 each_slot_length)
{
    int i = 0;
    int trancheId = LWTRANCHE_UNKNOWN;
    int32 total_slots = 0;

    m_cache_type = type;
    /* Must be greater than 0 */
    m_CaccheSlotMax = 1;

    m_cstoreCurrentSize = 0;
    m_cstoreMaxSize = cache_size;

    total_slots = Min(cache_size / each_block_size, MAX_CACHE_SLOT_COUNT);
    m_CacheSlots = (char *)palloc0(total_slots * each_slot_length);
    m_CacheDesc = (CacheDesc *)palloc0(total_slots * sizeof(CacheDesc));
    m_CacheSlotsNum = total_slots;
    m_slot_length = each_slot_length;
    for (i = 0; i < total_slots; ++i) {
        m_CacheDesc[i].m_usage_count = 0;
        m_CacheDesc[i].m_refcount = 0;
        m_CacheDesc[i].m_ring_count = 0;
        m_CacheDesc[i].m_slot_id = i;
        m_CacheDesc[i].m_freeNext = i + 1;
        m_CacheDesc[i].m_cache_tag.type = CACHE_TYPE_NONE;
        m_CacheDesc[i].m_flag = CACHE_BLOCK_FREE;
        if (type == MGR_CACHE_TYPE_DATA) {
            trancheId = (int)LWTRANCHE_DATA_CACHE;
        } else if (type == MGR_CACHE_TYPE_INDEX) {
            trancheId = (int)LWTRANCHE_META_CACHE;
        }
        m_CacheDesc[i].m_iobusy_lock = LWLockAssign(trancheId);
        m_CacheDesc[i].m_compress_lock = LWLockAssign(trancheId);
        m_CacheDesc[i].m_refreshing = false;
        m_CacheDesc[i].m_datablock_size = 0;

        SpinLockInit(&m_CacheDesc[i].m_slot_hdr_lock);
    }

    /* Cache Slot Free List */
    m_CacheDesc[total_slots - 1].m_freeNext = CACHE_BLOCK_INVALID_IDX;
    m_freeListHead = 0;
    m_freeListTail = total_slots - 1;
    SpinLockInit(&m_freeList_lock);
    SpinLockInit(&m_memsize_lock);

    /* Clock Sweep Starting point  */
    m_csweep = 0;
    m_csweep_lock = CStoreCUCacheSweepLock;
    m_partition_lock = FirstCacheSlotMappingLock;
    if (type == MGR_CACHE_TYPE_INDEX) {
        m_csweep_lock = MetaCacheSweepLock;
        m_partition_lock = FirstCacheSlotMappingLock + NUM_CACHE_BUFFER_PARTITIONS / 2;
    }

    HASHCTL info;
    errno_t rc = memset_s(&info, sizeof(info), 0, sizeof(info));
    securec_check(rc, "\0", "\0");

    char hash_name[128] = {0};
    rc = snprintf_s(hash_name, sizeof(hash_name), 127, "Cache Buffer Lookup Table(%d)", type);
    securec_check_ss(rc, "\0", "\0");

    /* BufferTag maps to Buffer */
    info.keysize = sizeof(CacheTag);
    info.entrysize = sizeof(CacheLookupEnt);
    info.hash = tag_hash;
    info.num_partitions = NUM_CACHE_BUFFER_PARTITIONS / 2;

    m_hash = HeapMemInitHash(hash_name, total_slots, total_slots, &info, HASH_ELEM | HASH_FUNCTION | HASH_PARTITION);
}

/*
 * @Description: destroy all resource of cache instance,
 *	  excluding this instance itself.
 * @See also:
 */
void CacheMgr::Destroy(void)
{
    /* reset and clear up hash table  */
    HeapMemResetHash(m_hash, "Cache Buffer Lookup Table");

    for (int i = 0; i < m_CacheSlotsNum; ++i) {
        /* free all memory belonged to this slot */
        FreeCacheBlockMem(i);
        SpinLockFree(&m_CacheDesc[i].m_slot_hdr_lock);
    }

    /* free spin lock resource */
    SpinLockFree(&m_memsize_lock);
    SpinLockFree(&m_freeList_lock);

    pfree_ext(m_CacheSlots);
    pfree_ext(m_CacheDesc);
}

/*
 * @Description: init cache block tag(key value)
 * @OUT cacheTag: block unique identification
 * @IN key: hash key
 * @IN length: hash key length, must <= MAX_CACHE_TAG_LEN
 * @IN type: type of key (data or meta)
 * @See also:
 */
void CacheMgr::InitCacheBlockTag(CacheTag *cacheTag, int32 type, const void *key, int32 length) const
{
    Assert(((m_cache_type == MGR_CACHE_TYPE_DATA) &&
            (type == CACHE_COlUMN_DATA || type == CACHE_ORC_DATA || type == CACHE_OBS_DATA)) ||
           ((m_cache_type == MGR_CACHE_TYPE_INDEX) && (type == CACHE_ORC_INDEX || type == CACHE_CARBONDATA_METADATA)));

    Assert(length <= MAX_CACHE_TAG_LEN);

    cacheTag->type = type;
    errno_t rc = memcpy_s(cacheTag->key, MAX_CACHE_TAG_LEN, key, length);
    securec_check(rc, "\0", "\0");
}

/*
 * @Description: find cache block in hash table
 * @IN cacheTag: block unique identification
 * @IN first_enter_block: flag to check whether need to increase usage count,  when block first used,it's usage count
 * may need increase
 * @Return: the block desc and pinned if found, null not found
 * @See also:
 */
CacheSlotId_t CacheMgr::FindCacheBlock(CacheTag *cacheTag, bool first_enter_block)
{
    CacheLookupEnt *result = NULL;
    CacheSlotId_t slotId = CACHE_BLOCK_INVALID_IDX;
    uint32 hashCode = 0;

    /* invalid tag */
    Assert(cacheTag->type > CACHE_TYPE_NONE && cacheTag->type <= CACHE_CARBONDATA_METADATA);

    hashCode = GetHashCode(cacheTag);
    (void)LockHashPartion(hashCode, LW_SHARED);
    result = (CacheLookupEnt *)hash_search_with_hash_value(m_hash, (void *)cacheTag, hashCode, HASH_FIND, NULL);
    if (result != NULL) {
        bool valid = PinCacheBlock(result->slot_id);
        if (!valid) {
            UnLockHashPartion(hashCode);
            return slotId;
        }
        slotId = result->slot_id;
        Assert(((m_cache_type == MGR_CACHE_TYPE_DATA) && (m_CacheDesc[slotId].m_cache_tag.type == CACHE_COlUMN_DATA ||
                                                          m_CacheDesc[slotId].m_cache_tag.type == CACHE_ORC_DATA ||
                                                          m_CacheDesc[slotId].m_cache_tag.type == CACHE_OBS_DATA)) ||
               ((m_cache_type == MGR_CACHE_TYPE_INDEX) &&
                (m_CacheDesc[slotId].m_cache_tag.type == CACHE_ORC_INDEX ||
                 m_CacheDesc[slotId].m_cache_tag.type == CACHE_CARBONDATA_METADATA)));

        LockCacheDescHeader(slotId);
        if (first_enter_block && (m_CacheDesc[slotId].m_usage_count < CACHE_BLOCK_MAX_USAGE)) {
            m_CacheDesc[slotId].m_usage_count += 1;
        }
        UnLockCacheDescHeader(slotId);

        Assert(slotId >= 0 && slotId <= m_CaccheSlotMax && slotId < m_CacheSlotsNum);
        CacheDesc *cacheDesc = m_CacheDesc + slotId;
        Assert(cacheDesc->m_slot_id == slotId);
        Assert(cacheTag->type == cacheDesc->m_cache_tag.type);
        Assert(memcmp(cacheTag->key, cacheDesc->m_cache_tag.key, MAX_CACHE_TAG_LEN) == 0);
        ereport(DEBUG1, (errmodule(MOD_CACHE),
                         errmsg("Find block in cache, slot(%d), type(%d)", cacheDesc->m_slot_id, cacheTag->type)));
    }

    UnLockHashPartion(hashCode);
    return slotId;
}

/*
 * @Description: delete block in hash table , free the memory, and put it to free list
 * @IN cacheTag: block unique identification
 * @See also:
 */
void CacheMgr::InvalidateCacheBlock(CacheTag *cacheTag)
{
    int retryTimes = 0;
    CacheDesc *slotHdr = NULL;
    int slotId = CACHE_BLOCK_INVALID_IDX;
    int blockSize = 0;

    Assert(t_thrd.int_cxt.ImmediateInterruptOK == false);
    while (1) {
        /* search and pin the found cache block */
        slotId = FindCacheBlock(cacheTag, false);
        if (!IsValidCacheSlotID(slotId))
            return;

        /* the other backends now is using it, so wait. */
        Assert(slotId >= 0 && slotId <= m_CaccheSlotMax && slotId < m_CacheSlotsNum);
        slotHdr = m_CacheDesc + slotId;
        if (slotHdr->m_refcount != 1) {
            UnPinCacheBlock(slotId);

            /* it means this backend has to wait continuely.
             so that try some times and then make myself sleeping,
             keep CPU away from wasting as possible. */
            if (++retryTimes >= 64) {
                retryTimes = 0;
                pg_usleep(2000L);
            }
            continue;
        }

        Assert(slotId >= 0 && slotId <= m_CaccheSlotMax && slotId < m_CacheSlotsNum);

        /* delete from hash table */
        DeleteCacheBlock(cacheTag);

        ereport(DEBUG1, (errmodule(MOD_CACHE), errmsg("drop cache block, solt(%d), flag(%d - %d)", slotId,
                                                      m_CacheDesc[slotId].m_flag, CACHE_BLOCK_FREE)));
        /* update slot flag */
        LockCacheDescHeader(slotId);
        blockSize = m_CacheDesc[slotId].m_datablock_size;
        m_CacheDesc[slotId].m_flag = CACHE_BLOCK_FREE;
        m_CacheDesc[slotId].m_datablock_size = 0;
        UnLockCacheDescHeader(slotId);

        /* free this block cache and update its size  before unpin this slot id */
        FreeCacheBlockMem(slotId);
        ReleaseCacheMem(blockSize);
        UnPinCacheBlock(slotId);

        /* put it into free list */
        PutFreeListCache(slotId);
        break;
    }
}

/*
 * @Description: delete block in hash table
 * @IN cacheTag: block unique identification
 * @See also:
 */
void CacheMgr::DeleteCacheBlock(CacheTag *cacheTag)
{
    /* invalid tag */
    Assert(cacheTag->type > CACHE_TYPE_NONE && cacheTag->type <= CACHE_CARBONDATA_METADATA);

    uint32 hashCode = GetHashCode(cacheTag);
    (void)LockHashPartion(hashCode, LW_EXCLUSIVE);
    (void)hash_search_with_hash_value(m_hash, (void *)cacheTag, hashCode, HASH_REMOVE, NULL);
    UnLockHashPartion(hashCode);

    return;
}

/*
 * @Description: use clock-swap algorithm to evict a block
 * @Return: slot id
 * @See also:
 */
CacheSlotId_t CacheMgr::EvictCacheBlock(int size, int retryNum)
{
    CacheSlotId_t slotId = CACHE_BLOCK_INVALID_IDX;

    /* If there is insufficient memory or no slots available
     * we must evict one of the cache blocks using the clock sweep.
     * Get the m_csweep_lock, only one sweeper searches the list. */
    LockSweep();

    /* Initialize statistics for each new sweep */
    int found = 0;
    int start = m_csweep;
    int max = m_CaccheSlotMax;
    int scanned = 0;
    int pinned = 0;
    int unpinned = 0;
    int invalid = 0;
    int looped = 0;
    int reserved = 0;
    int freepinned = 0;

    while (1) {
        /* Set the start slot to the current sweep position(m_csweep),
        Then advance the current sweep position to the next */
        slotId = m_csweep;
        if (m_csweep++ >= m_CaccheSlotMax) {
            m_csweep = 0;
        }
        ereport(DEBUG2,
                (errmodule(MOD_CACHE),
                 errmsg("try evict cache block, solt(%d), flag(%hhu), refcount(%u), usage_count(%hu), ring_count(%hu)",
                        slotId, m_CacheDesc[slotId].m_flag, m_CacheDesc[slotId].m_refcount,
                        m_CacheDesc[slotId].m_usage_count, m_CacheDesc[slotId].m_ring_count)));

        if (IsIOBusy(slotId)) {
            CHECK_CACHE_SLOT_STATUS();
            reserved++;
            pg_usleep(2);
            continue;
        }

        LockCacheDescHeader(slotId);
        scanned++;
        /* skip invalid and error cache blocks */
        if ((m_CacheDesc[slotId].m_flag & CACHE_BLOCK_VALID) || (m_CacheDesc[slotId].m_flag & CACHE_BLOCK_ERROR)) {
            /* skip pinned cache blocks */
            if (m_CacheDesc[slotId].m_refcount == 0) {
                unpinned++;
                /* skip cache blocks with usage count > 0 */
                if (m_CacheDesc[slotId].m_usage_count == 0) {
                    /* skip cache blocks that are in another ring , 1 in my ring,  0 no ring */
                    if (m_CacheDesc[slotId].m_ring_count == 0) {
                        ereport(DEBUG2,
                                (errmodule(MOD_CACHE), errmsg("evict cache block, solt(%d), flag(%d - %d)", slotId,
                                                              m_CacheDesc[slotId].m_flag, CACHE_BLOCK_INFREE)));

                        m_CacheDesc[slotId].m_flag = CACHE_BLOCK_INFREE;  // !Valid
                        PinCacheBlock_Locked(slotId);                     // Released header lock
                        found++;

                        /* Found a slot to be reused with some buffer space,
                         * the space must be freed and reused.
                         * The slot is Invalid and Pinned!!! */
                        break;
                    }
                    reserved++;
                } else {
                    /* decrement the usage count to age the entry */
                    m_CacheDesc[slotId].m_usage_count--;
                }
            } else {
                pinned++;
            }
        } else {
            invalid++;
            if ((m_CacheDesc[slotId].m_flag & CACHE_BLOCK_FREE) && (m_CacheDesc[slotId].m_refcount > 0)) {
                freepinned++;
            }
        }
        UnLockCacheDescHeader(slotId);

        /* A full sweep should rarely happen
         * Bail-out if we cannot find any unpinned blocks to
         * avoid waiting forever.
         *
         * If the next sweep position is the same as
         * the starting point, then we have gone
         * an entire loop
         *
         * If there is not proper slot to replace, it will return CACHE_BLOCK_INVALID_IDX
         * and unlock sweep lock in Macro CHECK_CACHE_SLOT_STATUS(). If this function
         * returns CACHE_BLOCK_INVALID_IDX, it will retry to find freespace in GetFreeCacheBlock
         * function.
         */
        CHECK_CACHE_SLOT_STATUS();
    }
    UnlockSweep();

    // purposely returning pinned Invalid slot !!!
    return slotId;
}

/*
 * @Description:  get an Invalid cache block, first try get from free list cache, if without space,
 * second evict from used cache block, if all cache block are using, return error
 * @IN size: cache memory size needed
 * @Return: return valid block index with pinned  or error return
 * @See also:
 */
CacheSlotId_t CacheMgr::GetFreeCacheBlock(int size)
{
    CacheSlotId_t slotId = CACHE_BLOCK_INVALID_IDX;
    int retryNum = 0;

RETRY_FIND_FREESPACE:

    retryNum++;
    /* If there is memory available, and slots on the free list, just return one from there */
    if (ReserveCacheMem(size)) {
        if ((slotId = GetFreeListCache()) != CACHE_BLOCK_INVALID_IDX) {
            LockSweep();
            if (slotId > m_CaccheSlotMax) {
                m_CaccheSlotMax = slotId;
            }
            UnlockSweep();

            LockCacheDescHeader(slotId);
            m_CacheDesc[slotId].m_flag = CACHE_BLOCK_NEW;  // is !Valid

            PinCacheBlock_Locked(slotId);  // Released header lock

            /* purposely returning pinned Invalid slot !!!
             * Returning from here means we have reserved the memory and
             * a new free slot */
            return slotId;
        } else {
            /* We release the memory now, and get it back again later  once we evict a buffer */
            ReleaseCacheMem(size);
        }
    }

    slotId = EvictCacheBlock(size, retryNum);
    /*
     * If the slotId is CACHE_BLOCK_INVALID_IDX, it means there is not proper slot to replace.
     * However, in this situation, there may be free space in cstore buffer, so we need to retry
     * to find freespace. The default retry time is 3, and if there is not proper slot to find after
     * three times retry, it reports ERROR as before.
     */
    if (slotId == CACHE_BLOCK_INVALID_IDX) {
        goto RETRY_FIND_FREESPACE;
    }

    return slotId;
}

/*
 * @Description: generate hash value
 * @IN cacheTag: block unique identification
 * @Return: hash code
 * @See also:
 */
uint32 CacheMgr::GetHashCode(CacheTag *cacheTag)
{
    return get_hash_value(m_hash, (void *)cacheTag);
}

/*
 * @Description: release cache block memory, this function call by reinit(switch over), and we want to try free all
 * cache list slot memeory, so the slot may equal to m_CacheSlotsNum
 * @IN slot: cache block index
 * @See also:
 */
void CacheMgr::FreeCacheBlockMem(CacheSlotId_t slot)
{
    Assert(slot >= 0 && slot < m_CacheSlotsNum);

    if (m_CacheDesc[slot].m_cache_tag.type == CACHE_ORC_INDEX) {
        OrcMetadataValue *value = (OrcMetadataValue *)(&m_CacheSlots[slot * m_slot_length]);
        if (value->postScript != NULL) {
            delete value->postScript;
            value->postScript = NULL;
        }
        if (value->fileFooter != NULL) {
            delete value->fileFooter;
            value->fileFooter = NULL;
        }
        if (value->stripeFooter != NULL) {
            delete value->stripeFooter;
            value->stripeFooter = NULL;
        }
        if (value->rowIndex != NULL) {
            delete value->rowIndex;
            value->rowIndex = NULL;
        }
        if (value->fileName != NULL) {
            free(value->fileName);
            value->fileName = NULL;
        }
        if (value->dataDNA != NULL) {
            free(value->dataDNA);
            value->dataDNA = NULL;
        }
        value->footerStart = 0;
        value->size = 0;
    } else if (m_CacheDesc[slot].m_cache_tag.type == CACHE_CARBONDATA_METADATA) {
        CarbonMetadataValue* value = (CarbonMetadataValue*)(&m_CacheSlots[slot * m_slot_length]);
        if (NULL != value->fileHeader) {
            free(value->fileHeader);
            value->fileHeader = NULL;
        }
        if (NULL != value->fileFooter) {
            free(value->fileFooter);
            value->fileFooter = NULL;
        }
        if (NULL != value->fileName) {
            free(value->fileName);
            value->fileName = NULL;
        }
        if (NULL != value->dataDNA) {
            free(value->dataDNA);
            value->dataDNA = NULL;
        }

        value->headerSize = 0;
        value->footerSize = 0;
        value->size = 0;
    } else if (m_CacheDesc[slot].m_cache_tag.type == CACHE_COlUMN_DATA) {
        /* Important: the memory with the same slot may be hold and used by CACHE_ORC_DATA,
         * CACHE_COlUMN_DATA and CACHE_ORC_INDEX, so we must confirm that the memory should
         * be set to 0 after the slot is released. at least memory of
         * min(sizeof(CU), sizeof(OrcDataValue)) size must be cleanned up. */
        CU *cu = (CU *)(&m_CacheSlots[slot * m_slot_length]);
        cu->FreeMem<true>();
        cu->Reset();
    } else if (m_CacheDesc[slot].m_cache_tag.type == CACHE_ORC_DATA ||
               m_CacheDesc[slot].m_cache_tag.type == CACHE_OBS_DATA) {
        OrcDataValue *orc_data = (OrcDataValue *)(&m_CacheSlots[slot * m_slot_length]);
        if (orc_data->value != NULL) {
            CStoreMemAlloc::Pfree(orc_data->value, false);
            orc_data->value = NULL;
        }
        orc_data->size = 0;
    } else {
        Assert(m_CacheDesc[slot].m_cache_tag.type == CACHE_TYPE_NONE);
    }
    return;
}

/*
 * @Description: get cache block memory size
 * @IN slot: cache block index
 * @Return: size of  block memory
 * @See also:
 */
int CacheMgr::GetCacheBlockMemSize(CacheSlotId_t slot)
{
    Assert(slot >= 0 && slot <= m_CaccheSlotMax && slot < m_CacheSlotsNum);

    int slot_size = 0;
    if (m_CacheDesc[slot].m_cache_tag.type == CACHE_ORC_INDEX) {
        OrcMetadataValue *value = (OrcMetadataValue *)(&m_CacheSlots[slot * m_slot_length]);
        slot_size = value->size;
    } else if (m_CacheDesc[slot].m_cache_tag.type == CACHE_CARBONDATA_METADATA) {
        CarbonMetadataValue* value = (CarbonMetadataValue*)(&m_CacheSlots[slot * m_slot_length]);
        return value->size;
    } else if (m_CacheDesc[slot].m_cache_tag.type == CACHE_COlUMN_DATA) {
        CU *cu = (CU *)(&m_CacheSlots[slot * m_slot_length]);
        if (!cu->m_cache_compressed) {
            slot_size = cu->GetUncompressBufSize();
        } else {
            slot_size = cu->GetCompressBufSize();
        }
    } else if (m_CacheDesc[slot].m_cache_tag.type == CACHE_ORC_DATA ||
               m_CacheDesc[slot].m_cache_tag.type == CACHE_OBS_DATA) {
        OrcDataValue *orc_data = (OrcDataValue *)(&m_CacheSlots[slot * m_slot_length]);
        slot_size = orc_data->size;
    } else {
        Assert(m_CacheDesc[slot].m_cache_tag.type == CACHE_TYPE_NONE);
    }

    if (slot_size == 0) {
        ereport(
            WARNING,
            (errmsg("The slot size is zero. It may occur error in IO, and the slot is set error flag. The type is %d.",
                    m_CacheDesc[slot].m_cache_tag.type)));
    }
    return slot_size;
}

/*
 * @Description:  If there is sufficient memory reserve it
 * @IN size: Requested allocation, m_cstoreCurrentSize: reserved memory, m_cstoreMaxSize:  maximum allowed block memory
 * @Return: true -- enough cache memory and reserve , flase --no free memory
 * @See also:
 */
bool CacheMgr::ReserveCacheMem(int size)
{
    SpinLockAcquire(&m_memsize_lock);
    if ((m_cstoreCurrentSize + size) <= m_cstoreMaxSize) {
        m_cstoreCurrentSize += size;
        SpinLockRelease(&m_memsize_lock);
        return true;
    }
    SpinLockRelease(&m_memsize_lock);
    return false;
}

/*
 * @Description: Reduce the reserved memory by size
 * @IN size: size to release
 * @See also:
 */
void CacheMgr::ReleaseCacheMem(int size)
{
    SpinLockAcquire(&m_memsize_lock);
    Assert(m_cstoreCurrentSize >= size);
    m_cstoreCurrentSize -= size;
    SpinLockRelease(&m_memsize_lock);
}

/*
 * @Description: Adjust the memory previously acquired for the block
 * The adjustment may cause the reserved memory (m_cstoreCurrentSize)
 *	to exceed the maximum allowed (m_cstoreMaxSize) slightly, until the next block is allocated.
 * @Param[IN] slotId: index of cache block
 * @Param[IN] oldSize: old block size
 * @Param[IN] newSize: new block size
 * @See also:
 */
void CacheMgr::AdjustCacheMem(CacheSlotId_t slotId, int oldSize, int newSize)
{
    this->ReleaseCacheMem(oldSize - newSize);

    LockCacheDescHeader(slotId);
    Assert(m_CacheDesc[slotId].m_datablock_size == oldSize);
    m_CacheDesc[slotId].m_datablock_size = newSize;
    UnLockCacheDescHeader(slotId);
}

/*
 * @Description: check cache free block list use out
 * @Return: true -- use out, false -- still have
 * @See also:
 */
bool CacheMgr::CacheFreeListEmpty() const
{
    /* FreeList is terminated with CACHE_BLOCK_INVALID_IDX */
    return (m_freeListHead == CACHE_BLOCK_INVALID_IDX ? true : false);
}

/*
 * @Description: Retrieve block slot from the free slot list, slots are retrieved from the head of the list.
 * the list and m_freeNext are proteted with the m_freeList_lock.
 * @Return: CACHE_BLOCK_INVALID_IDX -- no free block slot, others normal block slot
 * @See also:
 */
int CacheMgr::GetFreeListCache()
{
    int freeSlotIdx = CACHE_BLOCK_INVALID_IDX;

    SpinLockAcquire(&m_freeList_lock);
    if (m_freeListHead != CACHE_BLOCK_INVALID_IDX) {
        freeSlotIdx = m_freeListHead;
        Assert(freeSlotIdx >= 0 && freeSlotIdx < m_CacheSlotsNum);
        m_freeListHead = m_CacheDesc[freeSlotIdx].m_freeNext;
        m_CacheDesc[freeSlotIdx].m_freeNext = CACHE_BLOCK_NO_LIST;
        if (m_freeListHead == CACHE_BLOCK_INVALID_IDX) {
            m_freeListTail = CACHE_BLOCK_INVALID_IDX;
        }
    }
    SpinLockRelease(&m_freeList_lock);

    return freeSlotIdx;
}

/*
 * @Description: add cache block to free list, m_freeNext and the list protected by m_freeList_lock,
 * the list is stack ordered. We reuse entries we used before to keep the active set closer together.
 * @IN freeSlotIdx: index of cache block
 * @See also:
 */
void CacheMgr::PutFreeListCache(CacheSlotId_t freeSlotIdx)
{
    Assert(freeSlotIdx >= 0 && freeSlotIdx <= m_CaccheSlotMax && freeSlotIdx < m_CacheSlotsNum);

    // Put the slot at the head of the list
    SpinLockAcquire(&m_freeList_lock);
    m_CacheDesc[freeSlotIdx].m_freeNext = m_freeListHead;
    m_freeListHead = freeSlotIdx;
    if (m_freeListTail == CACHE_BLOCK_INVALID_IDX) {
        m_freeListTail = m_freeListHead;
    }
    SpinLockRelease(&m_freeList_lock);
}

/*
 * @Description: lock hash table use partition lock
 * @IN hashCode: hash code
 * @IN lockMode: lock mode
 * @Return: lock*
 * @See also:
 */
LWLock* CacheMgr::LockHashPartion(uint32 hashCode, LWLockMode lockMode) const
{
    LWLock *partionLock = GetMainLWLockByIndex(m_partition_lock + (hashCode % (NUM_CACHE_BUFFER_PARTITIONS / 2)));
    (void)LWLockAcquire(partionLock, lockMode);
    return partionLock;
}

/*
 * @Description: unlock hash table use partition lock
 * @IN hashCode: hash code
 * @See also:
 */
void CacheMgr::UnLockHashPartion(uint32 hashCode) const
{
    LWLock *partionLock = GetMainLWLockByIndex(m_partition_lock + (hashCode % (NUM_CACHE_BUFFER_PARTITIONS / 2)));
    LWLockRelease(partionLock);
}

/*
 * @Description: spin lock cache block desc
 * @IN slotId: cache block index
 * @See also:
 */
void CacheMgr::LockCacheDescHeader(CacheSlotId_t slotId)
{
    Assert(slotId >= 0 && slotId <= m_CaccheSlotMax && slotId < m_CacheSlotsNum);
    SpinLockAcquire(&m_CacheDesc[slotId].m_slot_hdr_lock);
}

/*
 * @Description:  spin unlock cache block desc
 * @IN slotId:	cache block index
 * @See also:
 */
void CacheMgr::UnLockCacheDescHeader(CacheSlotId_t slotId)
{
    Assert(slotId >= 0 && slotId <= m_CaccheSlotMax && slotId < m_CacheSlotsNum);
    SpinLockRelease(&m_CacheDesc[slotId].m_slot_hdr_lock);
}

/*
 * @Description: check cache block desc is pinned (other thread using)
 * @IN slotId: cache block index
 * @Return: true -- add, false --not use
 * @See also:
 */
bool CacheMgr::CacheBlockIsPinned(CacheSlotId_t slotId) const
{
    Assert(slotId >= 0 && slotId <= m_CaccheSlotMax && slotId < m_CacheSlotsNum);
    return (m_CacheDesc[slotId].m_refcount > 0);
}

/*
 * @Description: pin cache block, increase the reference.
 * @IN slotId: cache block index
 * @Return: return false if the slot is not marked valid, this means the block may soon be removed or changed.
 * @See also:
 */
bool CacheMgr::PinCacheBlock(CacheSlotId_t slotId)
{
    Assert(slotId >= 0 && slotId <= m_CaccheSlotMax && slotId < m_CacheSlotsNum);
    Assert(((m_cache_type == MGR_CACHE_TYPE_DATA) && (m_CacheDesc[slotId].m_cache_tag.type == CACHE_COlUMN_DATA ||
                                                      m_CacheDesc[slotId].m_cache_tag.type == CACHE_ORC_DATA ||
                                                      m_CacheDesc[slotId].m_cache_tag.type == CACHE_OBS_DATA)) ||
           ((m_cache_type == MGR_CACHE_TYPE_INDEX) &&
            (m_CacheDesc[slotId].m_cache_tag.type == CACHE_ORC_INDEX ||
             m_CacheDesc[slotId].m_cache_tag.type == CACHE_CARBONDATA_METADATA)));

    LockCacheDescHeader(slotId);

    /* Pinning an invalid slot returns false and does not pin the slot */
    if (!(m_CacheDesc[slotId].m_flag & CACHE_BLOCK_VALID)) {
        UnLockCacheDescHeader(slotId);
        return false;
    }

    /* increase refcount to pin cache block */
    m_CacheDesc[slotId].m_refcount++;
    Assert(m_CacheDesc[slotId].m_refcount > 0);

    /* Increment the usage count */
    UnLockCacheDescHeader(slotId);
    ereport(DEBUG2, (errmodule(MOD_CACHE), errmsg("pin cache block, slot(%d), type(%d) ,flag(%hhu), refcount(%u)",
                                                  slotId, m_CacheDesc[slotId].m_cache_tag.type,
                                                  m_CacheDesc[slotId].m_flag, m_CacheDesc[slotId].m_refcount)));

    if (m_cache_type == MGR_CACHE_TYPE_INDEX) {
        ResourceOwnerEnlargeMetaCacheSlot(t_thrd.utils_cxt.CurrentResourceOwner);
        ResourceOwnerRememberMetaCacheSlot(t_thrd.utils_cxt.CurrentResourceOwner, slotId);
    } else {
        ResourceOwnerEnlargeDataCacheSlot(t_thrd.utils_cxt.CurrentResourceOwner);
        ResourceOwnerRememberDataCacheSlot(t_thrd.utils_cxt.CurrentResourceOwner, slotId);
    }
    return true;
}

/*
 * @Description:  pin cache block, increase the reference  while already holding the header lock.
 * @IN slotId: cache block index
 * @See also:  header lock must be held on enty. This is an internal pin, we do not want to
 * count it used yet.  We know what we are doing here, there  is no need to check whether the Block is valid.
 */
void CacheMgr::PinCacheBlock_Locked(CacheSlotId_t slotId)
{
    Assert(slotId >= 0 && slotId <= m_CaccheSlotMax && slotId < m_CacheSlotsNum);

    m_CacheDesc[slotId].m_refcount++;
    UnLockCacheDescHeader(slotId);

    if (m_cache_type == MGR_CACHE_TYPE_INDEX) {
        ResourceOwnerEnlargeMetaCacheSlot(t_thrd.utils_cxt.CurrentResourceOwner);
        ResourceOwnerRememberMetaCacheSlot(t_thrd.utils_cxt.CurrentResourceOwner, slotId);
    } else {
        ResourceOwnerEnlargeDataCacheSlot(t_thrd.utils_cxt.CurrentResourceOwner);
        ResourceOwnerRememberDataCacheSlot(t_thrd.utils_cxt.CurrentResourceOwner, slotId);
    }
    ereport(DEBUG2, (errmodule(MOD_CACHE),
                     errmsg("pin locked cache block, slot(%d), refcount(%u)", slotId, m_CacheDesc[slotId].m_refcount)));
}

/*
 * @Description:   unpin cache block, decrease the reference.
 * @IN slotId: cache block index
 * @See also:
 */
void CacheMgr::UnPinCacheBlock(CacheSlotId_t slotId)
{
    Assert(slotId >= 0 && slotId <= m_CaccheSlotMax && slotId < m_CacheSlotsNum);
    Assert(((m_cache_type == MGR_CACHE_TYPE_DATA) && (m_CacheDesc[slotId].m_cache_tag.type == CACHE_COlUMN_DATA ||
                                                      m_CacheDesc[slotId].m_cache_tag.type == CACHE_ORC_DATA ||
                                                      m_CacheDesc[slotId].m_cache_tag.type == CACHE_OBS_DATA)) ||
           ((m_cache_type == MGR_CACHE_TYPE_INDEX) &&
            (m_CacheDesc[slotId].m_cache_tag.type == CACHE_ORC_INDEX ||
             m_CacheDesc[slotId].m_cache_tag.type == CACHE_CARBONDATA_METADATA)));

    if (m_cache_type == MGR_CACHE_TYPE_INDEX) {
        ResourceOwnerForgetMetaCacheSlot(t_thrd.utils_cxt.CurrentResourceOwner, slotId);
    } else {
        ResourceOwnerForgetDataCacheSlot(t_thrd.utils_cxt.CurrentResourceOwner, slotId);
    }

    LockCacheDescHeader(slotId);
    Assert(m_CacheDesc[slotId].m_refcount > 0);
    m_CacheDesc[slotId].m_refcount--;

    UnLockCacheDescHeader(slotId);
    ereport(DEBUG2,
            (errmodule(MOD_CACHE), errmsg("unpin cache block, slot(%d), type(%d) ,refcount(%u)", slotId,
                                          m_CacheDesc[slotId].m_cache_tag.type, m_CacheDesc[slotId].m_refcount)));
}

void CacheMgr::WaitEvictSlot(CacheSlotId_t slotId)
{
    Assert(slotId != CACHE_BLOCK_INVALID_IDX);

    /* we must wait for other session not reference the slot */
    while (1) {
        LockCacheDescHeader(slotId);

        /* no other session referenced to this slot anymore */
        if (m_CacheDesc[slotId].m_refcount == 1) {
            m_CacheDesc[slotId].m_flag = CACHE_BLOCK_INFREE;  // !Valid
            /*
            * The slot will be reused with some buffer space,
            * the space must be freed and reused.
            * The slot is Invalid and Pinned!!!
            */
            UnLockCacheDescHeader(slotId);
            break;
        }
        UnLockCacheDescHeader(slotId);
    }
}

void CacheMgr::AllocateBlockFromCacheWithSlotId(CacheSlotId_t slotId)
{
    WaitEvictSlot(slotId);

    Assert(slotId >= 0 && slotId <= m_CaccheSlotMax && slotId < m_CacheSlotsNum);
    Assert(m_CacheDesc[slotId].m_refcount == 1);
    Assert(m_CacheDesc[slotId].m_flag == CACHE_BLOCK_INFREE);

    LockCacheDescHeader(slotId);
    m_CacheDesc[slotId].m_flag = CACHE_BLOCK_FREE;
    UnLockCacheDescHeader(slotId);

    /* when return we still pinned the slotId cache */
}

/*
 * @Description: allocate block from cache
 * @Param[IN] cacheTag: block unique identification
 * @Param[IN/OUT] hasFound: found in cache
 * @Param[IN] hashCode: hash code
 * @Param[IN] size: block size
 * @Return: block index
 * @See also:
 */
CacheSlotId_t CacheMgr::AllocateBlockFromCache(CacheTag *cacheTag, uint32 hashCode, int size, bool &hasFound)
{
    CacheLookupEnt *result = NULL;
    int old_size = 0;
    int slot;

    Assert(size > 0);

    /* try allocate block from free list */
    while (1) {
        slot = GetFreeCacheBlock(size);
        Assert(slot >= 0 && slot <= m_CaccheSlotMax && slot < m_CacheSlotsNum);
        Assert(m_CacheDesc[slot].m_refcount == 1);  // Only ours

        /* If it is a new slot, the buffer space is already reserved,
         * and we still need add it to hash table */
        if (m_CacheDesc[slot].m_flag == CACHE_BLOCK_NEW) {
            /* for new block, cache type is CACHE_TYPE_NONE, we must set
             * the type first, becase this is key to distinguish code logic */
            m_CacheDesc[slot].m_cache_tag.type = cacheTag->type;
            break;
        }

        /* cache block not on the free list, evict from free cache,
         * so the old cache block has changed to invalid and need to free */
        Assert(m_CacheDesc[slot].m_flag == CACHE_BLOCK_INFREE);

        /* remove the hash table entry */
        DeleteCacheBlock(&m_CacheDesc[slot].m_cache_tag);
        /* mark the block free */
        LockCacheDescHeader(slot);
        old_size = m_CacheDesc[slot].m_datablock_size;
        m_CacheDesc[slot].m_flag = CACHE_BLOCK_FREE;  // !Valid and Free
        m_CacheDesc[slot].m_datablock_size = 0;
        UnLockCacheDescHeader(slot);
        /* free the cache block memory */
        FreeCacheBlockMem(slot);

        /* return the old size and get the new size
         * so we could have a net gain here */
        if (ReserveCacheMem(size - old_size)) {
            /* We have reserved the memory, we will use this slot */
            break;
        }

        /* no enough cache memory, just release the old size and go back for another
         *  Now that the slot is dead no need to pin it anymore */
        ReleaseCacheMem(old_size);
        UnPinCacheBlock(slot);
        /* noy used any more, so add to free list */
        PutFreeListCache(slot);
    }

    /* check still pinned */
    Assert(m_CacheDesc[slot].m_refcount > 0);

TRYAGAIN:
    (void)LockHashPartion(hashCode, LW_EXCLUSIVE);
    result = (CacheLookupEnt *)hash_search_with_hash_value(m_hash, (void *)cacheTag, hashCode, HASH_ENTER, &hasFound);

    /* if the tag is already in the table somebody beat us to creating one
     * if the block is invalid, the found block could be in the process of  being removed, so try again */
    if (hasFound) {
        LockCacheDescHeader(result->slot_id);
        if (m_CacheDesc[result->slot_id].m_flag & CACHE_BLOCK_ERROR) {
            hasFound = false;
        }
        UnLockCacheDescHeader(result->slot_id);

        if (hasFound) {
            /* must pin the block before use */
            bool valid = PinCacheBlock(result->slot_id);
            if (!valid) {
                UnLockHashPartion(hashCode);
                goto TRYAGAIN;
            }
        } else {
            goto NOT_FOUND;
        }
        UnLockHashPartion(hashCode);

        /* at this point, we discover the block already in the cache
        // so we need to free ours and use the cached one */
        ReleaseCacheMem(size);

        ereport(DEBUG2,
                (errmodule(MOD_CACHE), errmsg("find cache block already in cache, solt(%d), change flag(%d - %d)", slot,
                                              m_CacheDesc[slot].m_flag, CACHE_BLOCK_FREE)));

        /* mark the block free, unpin the block and put it to free list */
        LockCacheDescHeader(slot);
        m_CacheDesc[slot].m_flag = CACHE_BLOCK_FREE;  // !Valid and Free
        m_CacheDesc[slot].m_datablock_size = 0;
        UnLockCacheDescHeader(slot);
        UnPinCacheBlock(slot);
        PutFreeListCache(slot);

        /* return the found pinned slot */
        return result->slot_id;
    }

NOT_FOUND:
    result->slot_id = slot;

    return slot;
}

/*
 * @Description: release the cache block represented by slotId
 * @IN slotId: the cache block to be evicted
 * @Return: true for the session can evict the cache.
 *			false for some other session is updating it concurrently.
 * In some cases, two or more threads maybe update the cache
 * concurrently. To avoid this situation, we judge the cache state
 * at the beginning of the function to test if we could continue to
 * release and update the cache. When m_CacheDesc[slotId].m_refreshing
 * is true means some other session is updating the related cache now.
 * If not, we lock the cache desc and change it from false to true to
 * keep other late session away from updating the cache. After evict the
 * slot, release the cache block content and make it in IO_busy state,
 * we turn m_CacheDesc[slotId].m_refreshing to false. At this time if
 * other session want to load this slod and cache data, they must be
 * waiting for IO complete.
 */
bool CacheMgr::ReserveCacheBlockWithSlotId(CacheSlotId_t slotId)
{
    /* judge if there is any other thread to fresh this slot content */
    LockCacheDescHeader(slotId);

    if (m_CacheDesc[slotId].m_refreshing == true) {
        UnLockCacheDescHeader(slotId);
        return false;
    } else
        m_CacheDesc[slotId].m_refreshing = true;

    UnLockCacheDescHeader(slotId);

    /* we can safely evict the slot */
    AllocateBlockFromCacheWithSlotId(slotId);

    Assert(slotId >= 0 && slotId <= m_CaccheSlotMax && slotId < m_CacheSlotsNum);

    LockCacheDescHeader(slotId);

    m_CacheDesc[slotId].m_usage_count = 1;
    m_CacheDesc[slotId].m_ring_count = 0;
    m_CacheDesc[slotId].m_flag = CACHE_BLOCK_VALID | CACHE_BLOCK_IOBUSY;

    UnLockCacheDescHeader(slotId);

    /* clear the cache block data and fill in it later,  */
    FreeCacheBlockMem(slotId);

    /*
     * Block subsequent accesses to the data
     * until the I/O is done.This call can
     * block momentarily, when racing with
     * another insert/reserve
     */
    (void)LWLockAcquire(m_CacheDesc[slotId].m_iobusy_lock, LW_EXCLUSIVE);

    /* already make slotId busy, we can safely change m_refreshing to false */
    LockCacheDescHeader(slotId);

    Assert(m_CacheDesc[slotId].m_refreshing == true);
    m_CacheDesc[slotId].m_refreshing = false;

    UnLockCacheDescHeader(slotId);

    IncRingCount(slotId);                        // record io state
    Assert(m_CacheDesc[slotId].m_refcount > 0);  // pinned

    return true;
}

/*
 * @Description: release the cache block represented by slotId if the slot is not IOBUSY
 * @IN slotId: the cache block to be evicted
 * @Return: true for the session can evict the cache.
 *			false for the slot is IOBUSY.
 * If the slot is IOBusy, this thread returns false and then waits the remote read IO complete.
 * If the slot is not IOBusy, this thread marks the slot IOBUSY and VALID flag. Then, the thread
 * needs to get uncompress lock to free the CU, and gets remote read.
 * If the slot is marked the IOBUSY flag, other threads can not uncompress the CU in this slot until
 * the remote read finished.
 */
bool CacheMgr::ReserveCstoreCacheBlockWithSlotId(CacheSlotId_t slotId)
{
    /* judge if there is any other thread to fresh this slot content */
    LockCacheDescHeader(slotId);

    if (m_CacheDesc[slotId].m_flag & CACHE_BLOCK_IOBUSY) {
        UnLockCacheDescHeader(slotId);
        return false;
    }

    m_CacheDesc[slotId].m_flag = CACHE_BLOCK_VALID | CACHE_BLOCK_IOBUSY;

    UnLockCacheDescHeader(slotId);

    /*
     * Block subsequent accesses to the data
     * until the I/O is done.This call can
     * block momentarily, when racing with
     * another insert/reserve
     */
    (void)LWLockAcquire(m_CacheDesc[slotId].m_iobusy_lock, LW_EXCLUSIVE);

    IncRingCount(slotId);                        // record io state
    Assert(m_CacheDesc[slotId].m_refcount > 0);  // pinned

    return true;
}

/*
 * Algorithm:
 * 1. Get a cache block slot
 *		a. If available, get a Free slot and memory for the new block.
 *		b. Otherwise free up block slots and memory, until there is room to hold the new block.
 * 2. Reserve memory for the new block.
 * 3. Initialize the new block as CACHE_BLOCK_VALID and CACHE_BLOCK_IOBUSY.
 * 4. Put the new block into the hashtable.
 *
 * Assumptions:
 * Useable blocks in the hashtable are marked CACHE_BLOCK_VALID or CACHE_BLOCK_ERROR.
 * We mark the block invalid (clear CACHE_BLOCK_VALID), before deleting it from
 * the hashtable. So threads that see that the buffer is invalid,
 * should release it immediately.
 *
 * blocks that are being loaded or saved to disk are marked CACHE_BLOCK_IOBUSY.
 * This means state in the block descriptor slot is valid but the block slot and the
 * data may be changing. Threads that must access the block, will pin the
 * block descriptor slot and wait to be awakened once the block data  is stable.
 */
/*
 * @Description:
 * @IN cacheTag: block unique identification
 * @OUT hasFound: found in cache
 * @IN size: cache block memory size
 * @Return:
 * @See also:
 */
CacheSlotId_t CacheMgr::ReserveCacheBlock(CacheTag *cacheTag, int size, bool &hasFound)
{
    int slot;
    uint32 hashCode = GetHashCode(cacheTag);

    slot = AllocateBlockFromCache(cacheTag, hashCode, size, hasFound);
    Assert(slot >= 0 && slot <= m_CaccheSlotMax && slot < m_CacheSlotsNum);
    if (hasFound) {
        /* add m_usage_count here may not ok, so need think more about it */
        LockCacheDescHeader(slot);
        if (m_CacheDesc[slot].m_usage_count < CACHE_BLOCK_MAX_USAGE) {
            m_CacheDesc[slot].m_usage_count += 1;
        }
        UnLockCacheDescHeader(slot);
        ereport(DEBUG2, (errmodule(MOD_CACHE), errmsg("Reuse cache block, slot(%d), type(%d)", slot, cacheTag->type)));
        Assert(m_CacheDesc[slot].m_refcount > 0);  // pinned
        return slot;
    }
    ereport(DEBUG2, (errmodule(MOD_CACHE),
                     errmsg("Reserve cache block, add IOBUSY flag, slot(%d), type(%d)", slot, cacheTag->type)));

    /* now our block is in the cache. initialize it before anybody sees it */
    LockCacheDescHeader(slot);

    InitCacheBlockTag(&(m_CacheDesc[slot].m_cache_tag), cacheTag->type, cacheTag->key, MAX_CACHE_TAG_LEN);
    m_CacheDesc[slot].m_usage_count = 1;
    m_CacheDesc[slot].m_flag = CACHE_BLOCK_VALID | CACHE_BLOCK_IOBUSY;
    m_CacheDesc[slot].m_datablock_size = size;
    UnLockCacheDescHeader(slot);

    /* clear the block now, and fill it in later,  */
    FreeCacheBlockMem(slot);

    /* Block subsequent accesses to the data until the I/O is done
     *This call can block momentarily, when racing with another insert/reserve */
    (void)LWLockAcquire(m_CacheDesc[slot].m_iobusy_lock, LW_EXCLUSIVE);

    /* unlock the hash partition at last after stablizing the entries */
    UnLockHashPartion(hashCode);

    IncRingCount(slot);                        // record io state
    Assert(m_CacheDesc[slot].m_refcount > 0);  // pinned
    return slot;
}

/*
 * @Description: get cache block
 * @IN slotId: cache block index
 * @See also:
 */
void *CacheMgr::GetCacheBlock(CacheSlotId_t slotId)
{
    Assert(slotId >= 0 && slotId <= m_CaccheSlotMax && slotId < m_CacheSlotsNum);
    Assert(CacheBlockIsPinned(slotId));

    return &(m_CacheSlots[slotId * m_slot_length]);
}

/*
 * @Description: get cache tag of block
 * @IN refcount: reffer count of the block
 * @IN slotId: cache block index
 * @Return:
 * @See also:
 */
const CacheTag *CacheMgr::GetCacheBlockTag(CacheSlotId_t slotId, uint32 *refcount) const
{
    Assert(slotId >= 0 && slotId <= m_CaccheSlotMax && slotId < m_CacheSlotsNum);
    *refcount = m_CacheDesc[slotId].m_refcount;
    return &(m_CacheDesc[slotId].m_cache_tag);
}

/* copy cache block tag to out buffer safely */
void CacheMgr::CopyCacheBlockTag(CacheSlotId_t slotId, CacheTag *outTag)
{
    LockCacheDescHeader(slotId);
    *outTag = m_CacheDesc[slotId].m_cache_tag;
    UnLockCacheDescHeader(slotId);
}

/*
 * @Description: lock cache buffer before evict start
 * @See also:
 */
void CacheMgr::LockSweep()
{
    (void)LWLockAcquire(m_csweep_lock, LW_EXCLUSIVE);
}

/*
 * @Description: unlock cache buffer after evict end
 * @See also:
 */
void CacheMgr::UnlockSweep()
{
    LWLockRelease(m_csweep_lock);
}

/*
 * @Description:  increase ring count
 * @Param[IN] slot: cache block index
 * @See also:
 */
void CacheMgr::IncRingCount(CacheSlotId_t slot)
{
    Assert(slot >= 0 && slot <= m_CaccheSlotMax && slot < m_CacheSlotsNum);
    LockCacheDescHeader(slot);
    m_CacheDesc[slot].m_ring_count++;
    UnLockCacheDescHeader(slot);
}

/*
 * @Description: decrease ring count
 * @Param[IN] slot: slot id
 * @See also:
 */
void CacheMgr::DecRingCount(CacheSlotId_t slot)
{
    Assert(slot >= 0 && slot <= m_CaccheSlotMax && slot < m_CacheSlotsNum);

    LockCacheDescHeader(slot);
    Assert(m_CacheDesc[slot].m_ring_count > 0);
    m_CacheDesc[slot].m_ring_count--;
    UnLockCacheDescHeader(slot);
}

/*
 * @Description: do resource clean if reserver block error when prefetch
 * @Param[IN] slot: slot id
 * @See also:
 */
void CacheMgr::AbortCacheBlock(CacheSlotId_t slot)
{
    // check slot
    if (slot == CACHE_BLOCK_INVALID_IDX) {
        return;
    }
    Assert(slot >= 0 && slot <= m_CaccheSlotMax && slot < m_CacheSlotsNum);

    if (!LockHeldByMe(slot)) {
        LockOwn(slot);
        LWLockRelease(m_CacheDesc[slot].m_iobusy_lock);
        ereport(DEBUG1, (errmodule(MOD_CACHE), errmsg("abort cache block, release iobusy lock, slot(%d), type(%d)",
                                                      slot, m_CacheDesc[slot].m_cache_tag.type)));
    }

    SetCacheBlockErrorState(slot);

    LockCacheDescHeader(slot);
    Assert(m_CacheDesc[slot].m_ring_count > 0);
    m_CacheDesc[slot].m_ring_count--;
    UnLockCacheDescHeader(slot);

    ereport(DEBUG1, (errmodule(MOD_CACHE), errmsg("abort cache block, slot(%d), type(%d), flag(%hhu), refcount(%u)",
                                                  slot, m_CacheDesc[slot].m_cache_tag.type, m_CacheDesc[slot].m_flag,
                                                  m_CacheDesc[slot].m_refcount)));
}

/*
 * @Description: set cache block ERROR state.
 * @Param[IN] slot: cache block index
 * @See also:
 */
void CacheMgr::SetCacheBlockErrorState(CacheSlotId_t slot)
{
    Assert(slot >= 0 && slot <= m_CaccheSlotMax && slot < m_CacheSlotsNum);

    LockCacheDescHeader(slot);
    m_CacheDesc[slot].m_usage_count = 0;
    m_CacheDesc[slot].m_flag = CACHE_BLOCK_ERROR;
    UnLockCacheDescHeader(slot);
    ereport(DEBUG1, (errmodule(MOD_CACHE), errmsg("set cache block error state, slot(%d), type(%d) ,flag(%d)", slot,
                                                  m_CacheDesc[slot].m_cache_tag.type, m_CacheDesc[slot].m_flag)));
}

/*
 * @Description: get current cache memory used size
 * @Return: cache memory size
 * @See also:
 */
int64 CacheMgr::GetCurrentMemSize()
{
    int64 memSize = 0;
    SpinLockAcquire(&m_memsize_lock);
    memSize = m_cstoreCurrentSize;
    SpinLockRelease(&m_memsize_lock);

    return memSize;
}

/* below using by column store cache logic */

/*
 * @Description: just check whether the block is busy
 * @Param[IN] slotId: slot id
 * @Return:true --in IO state; false --not in IO state
 * @See also:
 */
bool CacheMgr::IsIOBusy(CacheSlotId_t slotId)
{
    bool ret = false;

    Assert(slotId >= 0 && slotId <= m_CaccheSlotMax && slotId < m_CacheSlotsNum);

    LockCacheDescHeader(slotId);
    if (m_CacheDesc[slotId].m_flag & CACHE_BLOCK_IOBUSY) {
        ret = true;
    }
    UnLockCacheDescHeader(slotId);

    return ret;
}

/*
 * @Description: WaitIO
 * If the block is IOBUSY then go to sleep waiting on the IO busy lock.
 * If awakened while waiting, release the lock and check again.
 * This method is based upon WaitIO() for rowstore.
 * @Param[IN] slotId: slot id
 * @Return: true, means error happen in preftech
 * @See also:
 */
bool CacheMgr::WaitIO(CacheSlotId_t slotId)
{
    bool check_error = false;
    CacheFlags flag;

    Assert(slotId >= 0 && slotId <= m_CaccheSlotMax && slotId < m_CacheSlotsNum);

    for (;;) {
        LockCacheDescHeader(slotId);
        flag = m_CacheDesc[slotId].m_flag;
        if (flag & CACHE_BLOCK_ERROR) {  // this means find cu in simulate but cancled
            check_error = true;
            break;
        }

        if (!(flag & CACHE_BLOCK_IOBUSY))
            break;

        UnLockCacheDescHeader(slotId);
        (void)LWLockAcquire(m_CacheDesc[slotId].m_iobusy_lock, LW_SHARED);
        LWLockRelease(m_CacheDesc[slotId].m_iobusy_lock);
    }
    UnLockCacheDescHeader(slotId);

    return check_error;
}

/*
 * @Description: CompleteIO
 * When an I/O is finished mark the block as not busy and wake the next waiter.
 * this method is simmilar to the column store TerminateBufferIO
 * @Param[IN] slotId: slot id
 * @See also:
 */
void CacheMgr::CompleteIO(CacheSlotId_t slotId)
{
    Assert(slotId >= 0 && slotId <= m_CaccheSlotMax && slotId < m_CacheSlotsNum);
    Assert(((m_cache_type == MGR_CACHE_TYPE_DATA) && (m_CacheDesc[slotId].m_cache_tag.type == CACHE_COlUMN_DATA ||
                                                      m_CacheDesc[slotId].m_cache_tag.type == CACHE_ORC_DATA ||
                                                      m_CacheDesc[slotId].m_cache_tag.type == CACHE_OBS_DATA)) ||
           ((m_cache_type == MGR_CACHE_TYPE_INDEX) &&
            (m_CacheDesc[slotId].m_cache_tag.type == CACHE_ORC_INDEX ||
             m_CacheDesc[slotId].m_cache_tag.type == CACHE_CARBONDATA_METADATA)));

    LockCacheDescHeader(slotId);
    if (!(m_CacheDesc[slotId].m_flag & CACHE_BLOCK_IOBUSY)) {
        pg_usleep(5000);
        ereport(PANIC, (errmsg("complete IO, slot(%d), flag(%d)", slotId, m_CacheDesc[slotId].m_flag)));
    }

    m_CacheDesc[slotId].m_flag &= ~CACHE_BLOCK_IOBUSY;
    UnLockCacheDescHeader(slotId);
    ereport(DEBUG1,
            (errmodule(MOD_CACHE), errmsg("complete IO, slot(%d), flag(%d)", slotId, m_CacheDesc[slotId].m_flag)));
    LWLockRelease(m_CacheDesc[slotId].m_iobusy_lock);

    DecRingCount(slotId);

    return;
}

/*
 * @Description:  check whether the su desc lock by me
 * @Param[IN] slotId:slot id
 * @Return: true -- lock; false --not lock
 * @See also:
 */
bool CacheMgr::LockHeldByMe(CacheSlotId_t slotId)
{
    Assert(slotId >= 0 && slotId <= m_CaccheSlotMax && slotId < m_CacheSlotsNum);
    return LWLockHeldByMe(m_CacheDesc[slotId].m_iobusy_lock);
}

/*
 * @Description: own the lock
 * @Param[IN] slotId: slot id
 * @See also:
 */
void CacheMgr::LockOwn(CacheSlotId_t slotId)
{
    Assert(slotId >= 0 && slotId <= m_CaccheSlotMax && slotId < m_CacheSlotsNum);
    LWLockOwn(m_CacheDesc[slotId].m_iobusy_lock);
}

/*
 * @Description: disown the lock
 * @Param[IN] slotId: slot id
 * @See also:
 */
void CacheMgr::LockDisown(CacheSlotId_t slotId)
{
    Assert(slotId >= 0 && slotId <= m_CaccheSlotMax && slotId < m_CacheSlotsNum);
    LWLockDisown(m_CacheDesc[slotId].m_iobusy_lock);
}

/*
 * @Description: add cstore IO lock
 * @Param[IN] slotId: slot id
 */
void CacheMgr::AcquireCstoreIOLock(CacheSlotId_t slotId)
{
    Assert(slotId >= 0 && slotId <= m_CaccheSlotMax && slotId < m_CacheSlotsNum);
    (void)LWLockAcquire(m_CacheDesc[slotId].m_iobusy_lock, LW_EXCLUSIVE);
}

/*
 * @Description: release cstore IO lock
 * @Param[IN] slotId: slot id:
 */
void CacheMgr::RealeseCstoreIOLock(CacheSlotId_t slotId)
{
    Assert(slotId >= 0 && slotId <= m_CaccheSlotMax && slotId < m_CacheSlotsNum);
    LWLockRelease(m_CacheDesc[slotId].m_iobusy_lock);
}

/*
 * @Description:  check whether the IO lock by me
 * @Param[IN] slotId:slot id
 * @Return: true -- lock; false --not lock
 */
bool CacheMgr::CstoreIOLockHeldByMe(CacheSlotId_t slotId)
{
    Assert(slotId >= 0 && slotId <= m_CaccheSlotMax && slotId < m_CacheSlotsNum);
    return LWLockHeldByMe(m_CacheDesc[slotId].m_iobusy_lock);
}

/*
 * @Description: add compress lock
 * @Param[IN] slotId: slot id
 * @See also:
 */
void CacheMgr::AcquireCompressLock(CacheSlotId_t slotId)
{
    Assert(slotId >= 0 && slotId <= m_CaccheSlotMax && slotId < m_CacheSlotsNum);
    (void)LWLockAcquire(m_CacheDesc[slotId].m_compress_lock, LW_EXCLUSIVE);
}

/*
 * @Description: release compress lock
 * @Param[IN] slotId: slot id
 * @See also:
 */
void CacheMgr::RealeseCompressLock(CacheSlotId_t slotId)
{
    Assert(slotId >= 0 && slotId <= m_CaccheSlotMax && slotId < m_CacheSlotsNum);
    LWLockRelease(m_CacheDesc[slotId].m_compress_lock);
}

/*
 * @Description:  check whether the compress lock by me
 * @Param[IN] slotId:slot id
 * @Return: true -- lock; false --not lock
 */
bool CacheMgr::CompressLockHeldByMe(CacheSlotId_t slotId)
{
    Assert(slotId >= 0 && slotId <= m_CaccheSlotMax && slotId < m_CacheSlotsNum);
    return LWLockHeldByMe(m_CacheDesc[slotId].m_compress_lock);
}
