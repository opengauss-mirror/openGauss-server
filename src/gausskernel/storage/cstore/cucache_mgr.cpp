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
 * cucache_mgr.cpp
 *      routines to support ColStore
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/cstore/cucache_mgr.cpp
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "storage/cucache_mgr.h"
#include "utils/aiomem.h"
#include "executor/instrument.h"
#include "utils/resowner.h"
#include "storage/ipc.h"
#include "miscadmin.h"
#include "pgxc/pgxc.h"

// FUTURE CASE: cstore_buffers and CUCacheMode should be tunable
int CUCacheMode = 1;

#define BUILD_BUG_ON_CONDITION(condition) ((void)sizeof(char[1 - 2 * (condition)]))

DataCacheMgr* DataCacheMgr::m_data_cache = NULL;

/*
 * @Description: DataCacheMgrNumLocks
 * Returns the number of LW locks required by the DataCacheMgr instance.
 * This function is called by NumLWLocks() to calculate the required memory
 * for all the LW Locks.  Since this must be done prior to initializing the
 * instance of the DataCacheMgr class, the function cannot be defined
 * as a method of the class.
 * @Return: lock number
 * @See also:
 */
int DataCacheMgrNumLocks()
{
    int64 cache_size = CacheMgrCalcSizeByType(MGR_CACHE_TYPE_DATA);
    return CacheMgrNumLocks(cache_size, BLCKSZ);
}

/*
 * @Description: get Singleton Instance of CU cache
 * @Return: CU cache instance
 * @See also:
 */
DataCacheMgr* DataCacheMgr::GetInstance(void)
{
    Assert(m_data_cache != NULL);
    return m_data_cache;
}

/*
 * @Description: create or recreate the Singleton Instance of CU cache
 * @See also:
 */
void DataCacheMgr::NewSingletonInstance(void)
{
    int64 cache_size = 0;
    /* only Postmaster has the privilege to create or recreate CU cache instance */
    if (IsUnderPostmaster)
        return;

    int condition = sizeof(DataSlotTagKey) != MAX_CACHE_TAG_LEN ? 1 : 0;
    BUILD_BUG_ON_CONDITION(condition);

    if (m_data_cache == NULL) {
        /* create this instance at the first time */
        m_data_cache = New(CurrentMemoryContext) DataCacheMgr;
        m_data_cache->m_cache_mgr = New(CurrentMemoryContext) CacheMgr;
    } else {
        /* destroy all resources of its members */
        m_data_cache->m_cache_mgr->Destroy();
        SpinLockFree(&m_data_cache->m_adio_write_cache_lock);
    }
    cache_size = CacheMgrCalcSizeByType(MGR_CACHE_TYPE_DATA);
    m_data_cache->m_cstoreMaxSize = cache_size;
    SpinLockInit(&m_data_cache->m_adio_write_cache_lock);
    /* init or reset this instance */
    m_data_cache->m_cache_mgr->Init(cache_size, BLCKSZ, MGR_CACHE_TYPE_DATA, Max(sizeof(CU), sizeof(OrcDataValue)));
    ereport(LOG, (errmodule(MOD_CACHE), errmsg("set data cache  size(%ld)", cache_size)));
}

/*
 * @Description: init CU unique tag key
 * @IN colid: column id
 * @IN cuid: cu id
 * @IN cuPtr: cu offsert
 * @IN rnode: file node
 * @Return: cu slot tag
 * @See also:
 */
DataSlotTag DataCacheMgr::InitCUSlotTag(RelFileNodeOld* rnode, int colid, uint32 cuid, CUPointer cuPtr)
{
    DataSlotTag tag;
    tag.slotTag.cuSlotTag.m_rnode = *rnode;
    tag.slotTag.cuSlotTag.m_CUId = cuid;
    tag.slotTag.cuSlotTag.m_colId = colid;
    tag.slotTag.cuSlotTag.m_cuPtr = cuPtr;
    tag.slotTag.cuSlotTag.m_padding = 0;
    tag.slotType = CACHE_COlUMN_DATA;
    return tag;
}

/*
 * @Description: init ORC data unique tag key
 * @IN fileid: relation id
 * @IN length: length
 * @IN offset: offset
 * @IN rnode: file node
 * @Return: orc data slot tag
 * @See also:
 */
DataSlotTag DataCacheMgr::InitORCSlotTag(RelFileNode* rnode, int32 fileid, uint64 offset, uint64 length)
{
    DataSlotTag tag;
    tag.slotTag.orcSlotTag.m_rnode = *(RelFileNodeOld*)rnode;
    tag.slotTag.orcSlotTag.m_fileId = fileid;
    tag.slotTag.orcSlotTag.m_offset = offset;
    tag.slotTag.orcSlotTag.m_length = length;
    tag.slotType = CACHE_ORC_DATA;
    return tag;
}

DataSlotTag DataCacheMgr::InitOBSSlotTag(uint32 hostNameHash, uint32 bucketNameHash, uint32 fileFirstHalfHash,
                                         uint32 fileSecondHalfHash, uint64 offset, uint64 length) const
{
    DataSlotTag tag;
    tag.slotTag.obsSlotTag.m_serverHash = hostNameHash;
    tag.slotTag.obsSlotTag.m_bucketHash = bucketNameHash;
    tag.slotTag.obsSlotTag.m_fileFirstHash = fileFirstHalfHash;
    tag.slotTag.obsSlotTag.m_fileSecondHash = fileSecondHalfHash;
    tag.slotTag.obsSlotTag.m_offset = offset;
    tag.slotTag.obsSlotTag.m_length = length;
    tag.slotType = CACHE_OBS_DATA;
    return tag;
}

/*
 * @Description: find data block in cache
 * @IN dataSlotTag: data slot tag key
 * @IN first_enter_block: flag to check whether first use the block
 * @Return: slot id
 * @See also:
 */
CacheSlotId_t DataCacheMgr::FindDataBlock(DataSlotTag* dataSlotTag, bool first_enter_block)
{
    CacheSlotId_t slot = CACHE_BLOCK_INVALID_IDX;
    CacheTag cacheTag = {0};

    m_cache_mgr->InitCacheBlockTag(&cacheTag, dataSlotTag->slotType, &dataSlotTag->slotTag, sizeof(DataSlotTagKey));
    slot = m_cache_mgr->FindCacheBlock(&cacheTag, first_enter_block);

    return slot;
}

/*
 * @Description: invalid cu from data cache, only used for set data type for cloumn store
 * @IN colid: column id
 * @IN cuid: cu id
 * @IN cuPtr: cu offsert
 * @IN rnode: file node
 * @Return: cu slot tag
 * @See also:
 */
void DataCacheMgr::InvalidateCU(RelFileNodeOld* rnode, int colId, uint32 cuId, CUPointer cuPtr)
{
    CacheTag cacheTag = {0};
    DataSlotTag dataSlotTag = InitCUSlotTag(rnode, colId, cuId, cuPtr);

    m_cache_mgr->InitCacheBlockTag(&cacheTag, dataSlotTag.slotType, &dataSlotTag.slotTag, sizeof(DataSlotTagKey));
    m_cache_mgr->InvalidateCacheBlock(&cacheTag);

    return;
}

/* invalid all CUs from data cache which belongs to this column relation */
void DataCacheMgr::DropRelationCUCache(const RelFileNode& rnode)
{
    CacheTag tag = {0};
    CUSlotTag cuTag = {{InvalidOid, InvalidOid, InvalidOid}, 0, 0, 0, 0};
    const int maxSlot = m_cache_mgr->GetUsedCacheSlotNum();

    for (CacheSlotId_t slot = 0; slot <= maxSlot; slot++) {
        m_cache_mgr->CopyCacheBlockTag(slot, &tag);
        cuTag = *(CUSlotTag*)tag.key;
        if (CACHE_COlUMN_DATA == tag.type && RelFileColumnNodeRelEquals(rnode, cuTag.m_rnode)) {
            /* try to invalid this CU */
            InvalidateCU(&cuTag.m_rnode, cuTag.m_colId, cuTag.m_CUId, cuTag.m_cuPtr);
        }
    }
}

/*
 * @Description:  get cu cached buffer
 * @IN cuSlotId: slot id
 * @Return: CU data pointer
 * @See also:
 */
CU* DataCacheMgr::GetCUBuf(int cuSlotId)
{
    return (CU*)m_cache_mgr->GetCacheBlock(cuSlotId);
}

/*
 * @Description: get orc data cached buffer
 * @IN cuSlotId: slot
 * @Return: ORC data pointer
 * @See also:
 */
OrcDataValue* DataCacheMgr::GetORCDataBuf(int cuSlotId)
{
    return (OrcDataValue*)m_cache_mgr->GetCacheBlock(cuSlotId);
}

/*
 * @Description: Evict some cache represented by slotId
 * @IN slotId: the target cache slot to be evicted
 * @Return: true for the session can evict the cache.
 *          false for some other session is updating it concurrently.
 */
bool DataCacheMgr::ReserveDataBlockWithSlotId(CacheSlotId_t slotId)
{
    if (m_cache_mgr->ReserveCacheBlockWithSlotId(slotId)) {
        Assert(!IsValidCacheSlotID(t_thrd.storage_cxt.CacheBlockInProgressIO) ||
               t_thrd.storage_cxt.CacheBlockInProgressIO == slotId);
        t_thrd.storage_cxt.CacheBlockInProgressIO = slotId;
        return true;
    }

    return false;
}

/*
 * @Description: Evict some cache represented by slotId for remote read.
 * @IN slotId: the target cache slot to be evicted
 * @Return: true for the session can evict the cache.
 *          false for the slot is IOBUSY.
 */
bool DataCacheMgr::ReserveCstoreDataBlockWithSlotId(CacheSlotId_t slotId)
{
    if (m_cache_mgr->ReserveCstoreCacheBlockWithSlotId(slotId)) {
        Assert(!IsValidCacheSlotID(t_thrd.storage_cxt.CacheBlockInProgressIO) ||
               t_thrd.storage_cxt.CacheBlockInProgressIO == slotId);
        t_thrd.storage_cxt.CacheBlockInProgressIO = slotId;
        return true;
    }

    return false;
}

/*
 * @Description: reserve  data block from data cache manager
 * @IN dataSlotTag: data slot tag
 * @IN hasFound: whether found or not
 * @IN size: need block size
 * @Return: slot id
 * @See also:
 */
CacheSlotId_t DataCacheMgr::ReserveDataBlock(DataSlotTag* dataSlotTag, int size, bool& hasFound)
{
    CacheSlotId_t slot = CACHE_BLOCK_INVALID_IDX;
    CacheTag cacheTag = {0};

    m_cache_mgr->InitCacheBlockTag(&cacheTag, dataSlotTag->slotType, &dataSlotTag->slotTag, sizeof(DataSlotTagKey));
    slot = m_cache_mgr->ReserveCacheBlock(&cacheTag, size, hasFound);
    if (!hasFound) {
        /* remember block slot in process */
        Assert(!IsValidCacheSlotID(t_thrd.storage_cxt.CacheBlockInProgressIO));
        t_thrd.storage_cxt.CacheBlockInProgressIO = slot;
    }
    if (cacheTag.type == CACHE_COlUMN_DATA) {
        CUSlotTag* cuslotTag = &dataSlotTag->slotTag.cuSlotTag;
        ereport(DEBUG1,
                (errmodule(MOD_CACHE),
                 errmsg("allocate metadata block, type(%d), slotID(%d), spcID(%u), dbID(%u), relID(%u), colId(%d), "
                        "cuID(%d), cuPoint(%lu)",
                        cacheTag.type,
                        slot,
                        cuslotTag->m_rnode.spcNode,
                        cuslotTag->m_rnode.dbNode,
                        cuslotTag->m_rnode.relNode,
                        cuslotTag->m_colId,
                        cuslotTag->m_CUId,
                        cuslotTag->m_cuPtr)));
    } else if (cacheTag.type == CACHE_ORC_DATA) {
        ORCSlotTag* orcslotTag = &dataSlotTag->slotTag.orcSlotTag;
        ereport(DEBUG1,
                (errmodule(MOD_CACHE),
                 errmsg("allocate metadata block, type(%d), slotID(%d), spcID(%u), dbID(%u), relID(%u), fileID(%d), "
                        "length(%lu), offset(%lu)",
                        cacheTag.type,
                        slot,
                        orcslotTag->m_rnode.spcNode,
                        orcslotTag->m_rnode.dbNode,
                        orcslotTag->m_rnode.relNode,
                        orcslotTag->m_fileId,
                        orcslotTag->m_length,
                        orcslotTag->m_offset)));
    } else if (cacheTag.type == CACHE_OBS_DATA) {
        OBSSlotTag* obsslotTag = &dataSlotTag->slotTag.obsSlotTag;
        ereport(DEBUG1,
                (errmodule(MOD_CACHE),
                 errmsg("allocate metadata block, type(%d), slotID(%d), serverHash(%u), bucketHash(%u), fileHash1( %u), "
                        "fileHash2(%u), length(%lu), offset(%lu)",
                        cacheTag.type,
                        slot,
                        obsslotTag->m_serverHash,
                        obsslotTag->m_bucketHash,
                        obsslotTag->m_fileFirstHash,
                        obsslotTag->m_fileSecondHash,
                        obsslotTag->m_length,
                        obsslotTag->m_offset)));
    }

    return slot;
}

/*
 * @Description: remove pin of data block
 * @IN CUSlotId: slot id
 * @See also:
 */
void DataCacheMgr::UnPinDataBlock(int CUSlotId)
{
    m_cache_mgr->UnPinCacheBlock(CUSlotId);
}

/*
 * @Description: do resource clean(set block state and release lock if needed) if reserver data block failed
 * @Param[IN] slot: slot id
 * @See also:
 */
void DataCacheMgr::AbortCU(CacheSlotId_t slot)
{
    m_cache_mgr->AbortCacheBlock(slot);
    return;
}

/*
 * @Description: terminate or abort this CU handling.
 * @Param[IN] abort: whether to delete this CU from cache
 * @See also:
 */
void DataCacheMgr::TerminateCU(bool abort)
{
    if (abort) {
        if (IsValidCacheSlotID(t_thrd.storage_cxt.CacheBlockInProgressIO)) {
            /* invalid this block slot, and include IO state and lock owner */
            Assert(!IsValidCacheSlotID(t_thrd.storage_cxt.CacheBlockInProgressUncompress));
            AbortCU(t_thrd.storage_cxt.CacheBlockInProgressIO);
        }
        if (IsValidCacheSlotID(t_thrd.storage_cxt.CacheBlockInProgressUncompress)) {
            /* invalid this block slot, and don't care IO state or lock owner */
            Assert(!IsValidCacheSlotID(t_thrd.storage_cxt.CacheBlockInProgressIO));
            m_cache_mgr->SetCacheBlockErrorState(t_thrd.storage_cxt.CacheBlockInProgressUncompress);
            CU* cuPtr = GetCUBuf(t_thrd.storage_cxt.CacheBlockInProgressUncompress);
            cuPtr->FreeSrcBuf();
        }
    }
    /* clear record */
    t_thrd.storage_cxt.CacheBlockInProgressIO = CACHE_BLOCK_INVALID_IDX;
    t_thrd.storage_cxt.CacheBlockInProgressUncompress = CACHE_BLOCK_INVALID_IDX;
}

/*
 * @Description: terminate or abort this CU in verify process..
 */
void DataCacheMgr::TerminateVerifyCU()
{
    if (IsValidCacheSlotID(t_thrd.storage_cxt.CacheBlockInProgressIO)) {
        /* invalid this block slot, and include IO state and lock owner */
        Assert (!IsValidCacheSlotID(t_thrd.storage_cxt.CacheBlockInProgressUncompress));
        Assert(m_cache_mgr->CstoreIOLockHeldByMe(t_thrd.storage_cxt.CacheBlockInProgressIO));
        AbortCU(t_thrd.storage_cxt.CacheBlockInProgressIO);
        HOLD_INTERRUPTS();  /* match the upcoming RESUME_INTERRUPTS */
        m_cache_mgr->RealeseCstoreIOLock(t_thrd.storage_cxt.CacheBlockInProgressIO);
    }
    if (IsValidCacheSlotID(t_thrd.storage_cxt.CacheBlockInProgressUncompress)) {
        /* invalid this block slot, and don't care IO state or lock owner */
        Assert (!IsValidCacheSlotID(t_thrd.storage_cxt.CacheBlockInProgressIO));
        Assert (m_cache_mgr->CompressLockHeldByMe(t_thrd.storage_cxt.CacheBlockInProgressUncompress));
        m_cache_mgr->SetCacheBlockErrorState(t_thrd.storage_cxt.CacheBlockInProgressUncompress);
        CU* cuPtr = GetCUBuf(t_thrd.storage_cxt.CacheBlockInProgressUncompress);
        cuPtr->FreeSrcBuf();
        HOLD_INTERRUPTS();  /* match the upcoming RESUME_INTERRUPTS */
        m_cache_mgr->RealeseCompressLock(t_thrd.storage_cxt.CacheBlockInProgressUncompress);
    }

    /* clear record */
    t_thrd.storage_cxt.CacheBlockInProgressIO = CACHE_BLOCK_INVALID_IDX;
    t_thrd.storage_cxt.CacheBlockInProgressUncompress = CACHE_BLOCK_INVALID_IDX;

    return;
}

/*
 * @Description: CU cache will uncompress CU raw data.
 * @Param[IN] cuDescPtr: CU desc info
 * @Param[IN] slotId: CU slot id
 * @Return: CUUncompressedRetCode value
 * @See also:
 */
CUUncompressedRetCode DataCacheMgr::StartUncompressCU(
    CUDesc* cuDescPtr, CacheSlotId_t slotId, int planNodeId, bool timing, int align_size)
{
    CU* cuPtr = GetCUBuf(slotId);

    m_cache_mgr->AcquireCompressLock(slotId);

    /*
     * if the slot flag is set to CACHE_BLOCK_ERROR by some abort threads,
     * the compressed buffer is also valid no matter the CU comes from primary
     * node or remote mode. So, the thread can continue to uncompress.
     */
    if (m_cache_mgr->IsIOBusy(slotId)) {
        /*
         * the CU is being reloading by remote read thread,
         * return CU_RELOADING and retry to load CU.
         */
        m_cache_mgr->RealeseCompressLock(slotId);
        return CU_RELOADING;
    }

    if (!cuPtr->m_cache_compressed) {
        /* another therad have decompressed this CU data */
        m_cache_mgr->RealeseCompressLock(slotId);
        return CU_OK;
    }

    if (cuPtr->m_adio_error) {
        /* IO error */
        this->m_cache_mgr->RealeseCompressLock(slotId);
        return CU_ERR_ADIO;
    }

    /* remember this slot id */
    Assert(!IsValidCacheSlotID(t_thrd.storage_cxt.CacheBlockInProgressUncompress));
    t_thrd.storage_cxt.CacheBlockInProgressUncompress = slotId;

    if (cuPtr->CheckCrc() == false) {
        /* CRC check failed */
        m_cache_mgr->RealeseCompressLock(slotId);
        return CU_ERR_CRC;
    }

    if (cuPtr->CheckMagic(cuDescPtr->magic) == false) {
        m_cache_mgr->RealeseCompressLock(slotId);
        return CU_ERR_MAGIC;
    }

    /* macro for tracing cstore scan */
#define UNCOMPRESS_TRACE(A)     \
    do {                        \
        if (unlikely(timing)) { \
            A;                  \
        }                       \
    } while (0)

    /* Always presume compressed disk and uncompressed cache. */
    UNCOMPRESS_TRACE(TRACK_START(planNodeId, UNCOMPRESS_CU));
    cuPtr->UnCompress(cuDescPtr->row_count, cuDescPtr->magic, align_size);
    UNCOMPRESS_TRACE(TRACK_END(planNodeId, UNCOMPRESS_CU));

    /* Do not put the compressedBuf in the cache
     * In the future we may have compressed data in the cache or let the
     * caller choose, but for now we just keep the uncompressed data.
     */
    cuPtr->FreeCompressBuf();

    /* Adjust the allocation reservation to take into account
     * compression or expansion.
     */
    int cu_uncompress_size = cuPtr->GetUncompressBufSize();
    m_cache_mgr->AdjustCacheMem(slotId, cuDescPtr->cu_size, cu_uncompress_size);
    m_cache_mgr->RealeseCompressLock(slotId);

    TerminateCU(false);
    return CU_OK;
}

/*
 * @Description: get data cache manage current memory cache used size
 * @Return: cache used size
 * @See also:
 */
int64 DataCacheMgr::GetCurrentMemSize()
{
    if (!IS_SINGLE_NODE)
        return m_cache_mgr->GetCurrentMemSize();
    else
        return 0;
}

/*
 * @Description: DataBlockWaitIO
 * If the CU is IOBUSY then go to sleep waiting on the IO busy lock.
 * If awakened while waiting, release the lock and check again.
 * This method is based upon WaitIO() for rowstore.
 * @Param[IN] slotId: slot id
 * @Return: true, means error happen in preftech
 * @See also:
 */
bool DataCacheMgr::DataBlockWaitIO(CacheSlotId_t slotId)
{
    return m_cache_mgr->WaitIO(slotId);
}

/*
 * @Description: DataBlockCompleteIO
 * When an I/O is finished mark the CU as not busy and wake the next waiter.
 * this method is simmilar to the column store TerminateBufferIO
 * @Param[IN] slotId: slot id
 * @See also:
 */
void DataCacheMgr::DataBlockCompleteIO(CacheSlotId_t slotId)
{
    m_cache_mgr->CompleteIO(slotId);

    /* clear block slot in IO progress, aio completer thread does not use it */
    t_thrd.storage_cxt.CacheBlockInProgressIO = CACHE_BLOCK_INVALID_IDX;

    return;
}

/*
 * @Description:  check whether the su desc lock by me
 * @Param[IN] slotId:slot id
 * @Return: true -- lock; false --not lock
 * @See also:
 */
bool DataCacheMgr::CULWLockHeldByMe(CacheSlotId_t slotId)
{
    return m_cache_mgr->LockHeldByMe(slotId);
}

/*
 * @Description: own the lock
 * @Param[IN] slotId: slot id
 * @See also:
 */
void DataCacheMgr::CULWLockOwn(CacheSlotId_t slotId)
{
    return m_cache_mgr->LockOwn(slotId);
}

/*
 * @Description: disown the lock
 * @Param[IN] slotId: slot id
 * @See also:
 */
void DataCacheMgr::CULWLockDisown(CacheSlotId_t slotId)
{
    return m_cache_mgr->LockDisown(slotId);
}

/*
 * @Description:  used for adio write
 * @See also:
 */
void DataCacheMgr::LockPrivateCache()
{
    SpinLockAcquire(&m_adio_write_cache_lock);
}

/*
 * @Description:  used for adio write
 * @See also:
 */
void DataCacheMgr::UnLockPrivateCache()
{
    SpinLockRelease(&m_adio_write_cache_lock);
}

/*
 * @Description: print data block info if resource leak found
 * @IN slotId: slot id
 * @See also:
 */
void DataCacheMgr::PrintDataCacheSlotLeakWarning(CacheSlotId_t slotId)
{
    DataSlotTag tag = {};
    uint32 refcount = 0;

    Assert(IsValidCacheSlotID(slotId));

    const CacheTag* cacheTag = m_cache_mgr->GetCacheBlockTag(slotId, &refcount);
    if (cacheTag == NULL) {
        ereport(ERROR, (errmsg("No cache found when print data cache slot leak warning:  slotId:%d", slotId)));
    }
    errno_t rc = memcpy_s(&(tag.slotTag), sizeof(DataSlotTagKey), cacheTag->key, sizeof(DataSlotTagKey));
    securec_check(rc, "\0", "\0");
    if (cacheTag->type == CACHE_COlUMN_DATA) {
        CUSlotTag* cuslotTag = &tag.slotTag.cuSlotTag;
        ereport(WARNING,
                (errmsg("CUCache refcount leak: type:%d, spaceNode: %u, dbNode: %u, relNode: %u, colId: %d, cuId: %d, "
                        "cuPoint: %lu, refcount: %u, slotId:%d",
                        cacheTag->type,
                        cuslotTag->m_rnode.spcNode,
                        cuslotTag->m_rnode.dbNode,
                        cuslotTag->m_rnode.relNode,
                        cuslotTag->m_colId,
                        cuslotTag->m_CUId,
                        cuslotTag->m_cuPtr,
                        refcount,
                        slotId)));
    } else if (cacheTag->type == CACHE_ORC_DATA) {
        ORCSlotTag* orcslotTag = &tag.slotTag.orcSlotTag;
        ereport(WARNING,
                (errmsg("ORCCache refcount leak: type:%d, spaceNode: %u, dbNode: %u, relNode: %u, fileId: %d, length: %lu, "
                        "offset: %lu, refcount: %u, slotId:%d",
                        cacheTag->type,
                        orcslotTag->m_rnode.spcNode,
                        orcslotTag->m_rnode.dbNode,
                        orcslotTag->m_rnode.relNode,
                        orcslotTag->m_fileId,
                        orcslotTag->m_length,
                        orcslotTag->m_offset,
                        refcount,
                        slotId)));
    } else if (cacheTag->type == CACHE_OBS_DATA) {
        OBSSlotTag* obsslotTag = &tag.slotTag.obsSlotTag;
        ereport(WARNING,
                (errmsg("OBSCache refcount leak: type:%d, server hash: %u, bucket hash: %u, file hash1: %u, file hash2: "
                        "%u, length: %lu, offset: %lu",
                        cacheTag->type,
                        obsslotTag->m_serverHash,
                        obsslotTag->m_bucketHash,
                        obsslotTag->m_fileFirstHash,
                        obsslotTag->m_fileSecondHash,
                        obsslotTag->m_length,
                        obsslotTag->m_offset)));
    }

    return;
}

/*
 * @Description:  set data block value, used for orc data cache
 * @IN buffer: buffer pointer
 * @IN size: buffer size
 * @IN slotId: slot id
 * @See also:
 */
void DataCacheMgr::SetORCDataBlockValue(CacheSlotId_t slotId, const void* buffer, uint64 size)
{
    OrcDataValue* orcDataValue = GetORCDataBuf(slotId);

    orcDataValue->size = size;
    orcDataValue->value = (char*)CStoreMemAlloc::Palloc(size, false);

    errno_t rc = memcpy_s(orcDataValue->value, size, buffer, size);
    securec_check(rc, "\0", "\0");

    ereport(DEBUG1, (errmodule(MOD_CACHE), errmsg("set orc data block slot(%d), size(%lu)", slotId, size)));

    return;
}

void DataCacheMgr::SetOBSDataBlockValue(
    CacheSlotId_t slotId, const void* buffer, uint64 size, const char* prefix, const char* dataDNA)
{
    OrcDataValue* orcDataValue = GetORCDataBuf(slotId);

    /* prefix + '\0' + dataDNA + '\0' + data */
    size_t prefixLen = strlen(prefix);
    size_t dataDNALen = strlen(dataDNA);
    errno_t rc;

    orcDataValue->size = size + prefixLen + 1 + dataDNALen + 1;
    orcDataValue->value = (char*)CStoreMemAlloc::Palloc(orcDataValue->size, false);

    /* set whole buffer to 0 */
    rc = memset_s(orcDataValue->value, orcDataValue->size, 0, orcDataValue->size);

    securec_check(rc, "\0", "\0");

    /* copy prefix */
    rc = memcpy_s(orcDataValue->value, orcDataValue->size, prefix, prefixLen);

    securec_check(rc, "\0", "\0");

    /* copy data DNA */
    rc =
        memcpy_s((char*)orcDataValue->value + prefixLen + 1, orcDataValue->size - (prefixLen + 1), dataDNA, dataDNALen);

    securec_check(rc, "\0", "\0");

    /* copy data */
    rc = memcpy_s((char*)orcDataValue->value + prefixLen + 1 + dataDNALen + 1, size, buffer, size);

    securec_check(rc, "\0", "\0");

    ereport(DEBUG1, (errmodule(MOD_CACHE), errmsg("set orc data block slot(%d), size(%lu)", slotId, size)));

    return;
}

/*
 * @Description: acquire compress lock
 * @IN slotId: slot id
 * @See also:
 */
void DataCacheMgr::AcquireCompressLock(CacheSlotId_t slotId)
{
    return m_cache_mgr->AcquireCompressLock(slotId);
}

/*
 * @Description:  release compress lock
 * @IN slotId: slot id
 * @See also:
 */
void DataCacheMgr::RealeseCompressLock(CacheSlotId_t slotId)
{
    return m_cache_mgr->RealeseCompressLock(slotId);
}

/*
 * @Description: call back for cstore prefetch read
 * @Param[IN] aioDesc:aio desc
 * @Param[IN] res: read size
 * @Return: always success
 * @See also:
 */
int CompltrReadCUReq(void* aioDesc, long res)
{
    AioDispatchCUDesc_t* desc = (AioDispatchCUDesc_t*)aioDesc;

    START_CRIT_SECTION();

    if (res != desc->cuDesc.size) {
        *(desc->cuDesc.io_error) = true;
        ereport(WARNING,
                (errmsg("CompltrReadCUReq error! slotid(%d), expect cu_size(%d), load cu_size(%ld)",
                        desc->cuDesc.slotId,
                        desc->cuDesc.size,
                        res)));
    } else {
        *(desc->cuDesc.io_error) = false;
    }

    Assert(IsValidCacheSlotID(desc->cuDesc.slotId));
    CUCache->CULWLockOwn(desc->cuDesc.slotId);
    CUCache->DataBlockCompleteIO(desc->cuDesc.slotId);

    ereport(DEBUG1, (errmodule(MOD_ADIO), errmsg("CompltrReadCUReq!slotid(%d) ", desc->cuDesc.slotId)));

    END_CRIT_SECTION();
    adio_share_free(desc);

    return 0;
}

/*
 * @Description: call back for cstore back write
 * @Param[IN] aioDesc:aio desc
 * @Param[IN] res: write size
 * @Return: always success
 * @See also:
 */
int CompltrWriteCUReq(void* aioDesc, long res)
{
    AioDispatchCUDesc_t* desc = (AioDispatchCUDesc_t*)aioDesc;

    START_CRIT_SECTION();
    if (res != desc->cuDesc.size) {
        *(desc->cuDesc.io_error) = true;
        ereport(WARNING,
                (errmsg("CompltrWriteCUReq error! cuid(%u), expect cu_size(%d), load cu_size(%ld)",
                        (uint32)desc->cuDesc.slotId,
                        desc->cuDesc.size,
                        res)));
    } else {
        *(desc->cuDesc.io_error) = false;
    }
    desc->cuDesc.io_finish = true;
    END_CRIT_SECTION();

    return 0;
}

/*
 * @Description: remove pin of data block
 * @IN CUSlotId: slot id
 * @See also:
 */
void ReleaseORCBlock(CacheSlotId_t slot)
{
    ORCCache->UnPinDataBlock(slot);
}

/*
 * @Description: allocate orc data cache block
 * @IN fileid: relation id
 * @IN length: length
 * @IN offset: offset
 * @IN rnode: file node
 * @IN/OUT found: whether found or not slot
 * @IN/OUT found: whether found or not  error
 * @Return: orc data slot id
 * @See also: we do not want to ereport(error) when found error, becasue this function called by
 * liborc(orc.HdfsCacheFileInputStream.read)
 */
CacheSlotId_t ORCCacheAllocBlock(
    RelFileNode* rnode, int32 relid, uint64 offset, uint64 length, bool& found, bool& err_found)
{
    err_found = false;
    int maxRetry = 3;
    DataSlotTag dataSlotTag = ORCCache->InitORCSlotTag(rnode, relid, offset, length);
    CacheSlotId_t slotId = ORCCache->FindDataBlock(&dataSlotTag, true);
    if (IsValidCacheSlotID(slotId)) {
        found = true;
    } else {
        found = false;
        slotId = ORCCache->ReserveDataBlock(&dataSlotTag, length, found);
    }

    while (found) {
        if (ORCCache->DataBlockWaitIO(slotId)) {
            ORCCache->UnPinDataBlock(slotId);
            if (maxRetry-- <= 0) {
                err_found = true;
                ereport(LOG,
                        (errmodule(MOD_ORC),
                         errmsg("wait IO find an error when allocate orc data, slotID(%d), spcID(%u), dbID(%u), "
                                "relID(%u), fileID(%d), offset(%lu), length(%lu)",
                                slotId,
                                rnode->spcNode,
                                rnode->dbNode,
                                rnode->relNode,
                                relid,
                                offset,
                                length)));
                break;
            } else {
                slotId = ORCCache->ReserveDataBlock(&dataSlotTag, length, found);
                continue;
            }
        } else {
            break;
        }
    }
    return slotId;
}

/*
 * @Description: release the cache and reload the data from OBS
 * @IN slotID: the cache to be evicted.
 * @Return: true for the session can evict the cache.
 *          false for some other session is updating it concurrently.
 */
bool OBSCacheRenewBlock(CacheSlotId_t slotID)
{
    return (OBSCache->ReserveDataBlockWithSlotId(slotID));
}

/*
 * @Description: allocate orc data cache block
 * @IN fileid: relation id
 * @IN length: length
 * @IN offset: offset
 * @IN rnode: file node
 * @IN/OUT found: whether found or not slot
 * @IN/OUT found: whether found or not  error
 * @Return: orc data slot id
 * @See also: we do not want to ereport(error) when found error, becasue this function called by
 * liborc(orc.HdfsCacheFileInputStream.read)
 */
CacheSlotId_t OBSCacheAllocBlock(const char* hostName, const char* bucketName, const char* prefixName, uint64 offset,
                                 uint64 length, bool& found, bool& err_found)
{
    Assert(hostName && bucketName && prefixName);

    uint32 hostNameLen = strlen(hostName);
    uint32 buckectNameLen = strlen(bucketName);
    uint32 prefixNameLen = strlen(prefixName);

    uint32 hostNameHash = string_hash((void*)hostName, hostNameLen + 1);
    uint32 bucketNameHash = string_hash((void*)bucketName, buckectNameLen + 1);

    uint32 fileFirstHalfHash = string_hash((void*)prefixName, prefixNameLen >> 1);
    uint32 fileSecondHalfHash =
        string_hash((void*)(prefixName + (prefixNameLen >> 1)), strlen(prefixName + (prefixNameLen >> 1)) + 1);

    err_found = false;
    int maxRetry = 3;
    DataSlotTag dataSlotTag =
        OBSCache->InitOBSSlotTag(hostNameHash, bucketNameHash, fileFirstHalfHash, fileSecondHalfHash, offset, length);
    CacheSlotId_t slotId = OBSCache->FindDataBlock(&dataSlotTag, true);
    if (IsValidCacheSlotID(slotId)) {
        found = true;
    } else {
        found = false;
        slotId = OBSCache->ReserveDataBlock(&dataSlotTag, length, found);
    }

    while (found) {
        if (OBSCache->DataBlockWaitIO(slotId)) {
            OBSCache->UnPinDataBlock(slotId);
            if (maxRetry-- > 0) {
                slotId = OBSCache->ReserveDataBlock(&dataSlotTag, length, found);
                continue;
            } else {
                err_found = true;
                break;
            }
        } else {
            break;
        }
    }
    return slotId;
}

OrcDataValue* ORCCacheGetBlock(CacheSlotId_t slot)
{
    return ORCCache->GetORCDataBuf(slot);
}

void ORCCacheSetBlock(CacheSlotId_t slotId, const void* buffer, uint64 size)
{
    ORCCache->SetORCDataBlockValue(slotId, buffer, size);
    ORCCache->DataBlockCompleteIO(slotId);
}

void OBSCacheSetBlock(CacheSlotId_t slotId, const void* buffer, uint64 size, const char* prefix, const char* dataDNA)
{
    OBSCache->SetOBSDataBlockValue(slotId, buffer, size, prefix, dataDNA);
    OBSCache->DataBlockCompleteIO(slotId);
}

