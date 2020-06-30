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
 * dfscache_mgr.cpp
 *      routines to support dfs
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/dfs/dfscache_mgr.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "storage/dfs/dfscache_mgr.h"

#include "postgres.h"
#include "knl/knl_variable.h"
#include "utils/aiomem.h"
#include "utils/resowner.h"
#include "storage/ipc.h"
#include "miscadmin.h"
#include "pgxc/pgxc.h"

MetaCacheMgr *MetaCacheMgr::m_meta_cache = NULL;

#define METADATA_EACH_SLOT_SIZE 1024
#define METADATA_FIX_SIZE 1024

/*
 * @Description: MetaCacheMgrNumLocks
 * Returns the number of LW locks required by the MetaCacheMgr instance.
 * This function is called by NumLWLocks() to calculate the required memory
 * for all the LW Locks.  Since this must be done prior to initializing the
 * instance of the MetaCacheMgr class, the function cannot be defined
 * as a method of the class.
 * @Return: lock number
 * @See also:
 */
int MetaCacheMgrNumLocks()
{
    int64 cache_size = CacheMgrCalcSizeByType(MGR_CACHE_TYPE_INDEX);

    return CacheMgrNumLocks(cache_size, METADATA_EACH_SLOT_SIZE);
}

/*
 * @Description: about orc metadata block
 * @See also:
 */
void HDFSAbortCacheBlock()
{
    /* Don't support ORC table in single node mode */
    if (!IS_SINGLE_NODE) {
        MetaCache->AbortMetaBlock(t_thrd.storage_cxt.MetaBlockInProgressIO);
        t_thrd.storage_cxt.MetaBlockInProgressIO = CACHE_BLOCK_INVALID_IDX;
    }

    return;
}

/*
 * @Description: print metadata block info if resource leak found
 * @IN slot: slot id
 * @See also:
 */
void PrintMetaCacheBlockLeakWarning(CacheSlotId_t slot)
{
    MetaCache->PrintMetaCacheSlotLeakWarning(slot);
}

/*
 * @Description: remove pin of meta cache block
 * @IN slot: slot id
 * @See also:
 */
void ReleaseMetaBlock(CacheSlotId_t slot)
{
    MetaCache->UnPinMetaBlock(slot);
}

/*
 * @Description:  get meta cache manager current memory cache size
 * @Return: cache size
 * @See also:
 */
int64 MetaCacheGetCurrentUsedSize()
{
    if (!IS_SINGLE_NODE)
        return MetaCache->GetCurrentMemSize();
    else
        return 0;
}

bool MetaCacheRenewBlock(CacheSlotId_t slotId)
{
    return (MetaCache->ReserveMetaBlockWithSlotId(slotId));
}

/*
 * @Description:  ge a meta cache block, find in cache first then reserve one if not found,  this is API for function
 * package.
 * @IN columnID: column id
 * @IN fileID: file id
 * @IN fileNode: file node
 * @IN stripeID: stripe id
 * @IN/OUT found: found in cache or not
 * @Return:slot id of cache block
 * @See also:
 */
CacheSlotId_t MetaCacheAllocBlock(RelFileNode *fileNode, int32 fileID, uint32 stripeID, uint32 columnID, bool &found)
{
    CacheSlotId_t slotId = MetaCache->FindMetaBlock(fileNode, fileID, stripeID, columnID);
    int maxRetry = 3;

    if (IsValidCacheSlotID(slotId)) {
        found = true;
    } else {
        found = false;
        slotId = MetaCache->ReserveMetaBlock(fileNode, fileID, stripeID, columnID, found);
    }

    while (found) {
        if (MetaCache->MetaBlockWaitIO(slotId)) {
            MetaCache->UnPinMetaBlock(slotId);
            if (maxRetry-- > 0) {
                slotId = MetaCache->ReserveMetaBlock(fileNode, fileID, stripeID, columnID, found);
                continue;
            } else {
                ereport(ERROR, (errcode(ERRCODE_NO_DATA_FOUND),
                                errmsg("wait IO find an error when allocate meta block, slotID(%d), spcID(%u),"
                                       "dbID(%u), relID(%u), fileID(%d), stripeID(%u), columnID(%u)",
                                       slotId, fileNode->spcNode, fileNode->dbNode, fileNode->relNode, fileID, stripeID,
                                       columnID)));
            }
        } else {
            break;
        }
    }
    return slotId;
}

/*
 * @Description: get metadata cache block
 * @IN slot: slot id
 * @Return: metadata pointer
 * @See also:
 */
OrcMetadataValue *MetaCacheGetBlock(CacheSlotId_t slot)
{
    return MetaCache->GetMetaBlock(slot);
}

/*
 * @Description: get meta cache block size
 * @IN slotId: slot id
 * @Return: block size
 * @See also:
 */
int MetaCacheGetBlockSize(CacheSlotId_t slot)
{
    return MetaCache->GetMetaBlockSize(slot);
}

/*
 * @Description: must get pin first before used
 * @IN fileFooter: file footer
 * @IN footerStart: footer start
 * @IN postScript: post script
 * @IN rowIndex: row index
 * @IN slotId: solt id
 * @IN stripeFooter: stripr footer
 * @See also:
 */
void MetaCacheSetBlock(CacheSlotId_t slotId, uint64 footerStart, const orc::proto::PostScript *postScript,
                       const orc::proto::Footer *fileFooter, const orc::proto::StripeFooter *stripeFooter,
                       const orc::proto::RowIndex *rowIndex, const char *fileName, const char *dataDNA)
{
    MetaCache->SetMetaBlockValue(slotId, footerStart, postScript, fileFooter, stripeFooter, rowIndex, fileName,
                                 dataDNA);
    MetaCache->MetaBlockCompleteIO(slotId);
}

/*
 * @Description: get Singleton Instance of CU cache
 * @Return: CU cache instance
 * @See also:
 */
MetaCacheMgr *MetaCacheMgr::GetInstance(void)
{
    Assert(m_meta_cache != NULL);
    return m_meta_cache;
}

/*
 * @Description: create or recreate the Singleton Instance of meta cache manager
 * @See also:
 */
void MetaCacheMgr::NewSingletonInstance(void)
{
    int64 cache_size = 0;
    /* only Postmaster has the privilege to create or recreate  meta cache instance */
    if (IsUnderPostmaster)
        return;

    BUILD_BUG_ON((sizeof(OrcMetadataTag) != MAX_CACHE_TAG_LEN));

    if (m_meta_cache == NULL) {
        /* create this instance at the first time */
        m_meta_cache = New(CurrentMemoryContext) MetaCacheMgr;
        m_meta_cache->m_cache_mgr = New(CurrentMemoryContext) CacheMgr;
    } else {
        /* destroy all resources of its members */
        m_meta_cache->m_cache_mgr->Destroy();
    }
    cache_size = CacheMgrCalcSizeByType(MGR_CACHE_TYPE_INDEX);
    m_meta_cache->m_cstoreMaxSize = cache_size;

    /* init or reset this instance */
    m_meta_cache->m_cache_mgr->Init(cache_size, METADATA_EACH_SLOT_SIZE, MGR_CACHE_TYPE_INDEX,
                                    sizeof(OrcMetadataValue));
    ereport(LOG, (errmodule(MOD_CACHE), errmsg("set metadata cache  size(%ld)", cache_size)));
}

/*
 * @Description:  init metadata unique tag key
 * @IN columnID: column id
 * @IN fileID: file id
 * @IN fileNode: file node
 * @IN stripeID: stripe id
 * @Return: metadata tag
 * @See also:
 */
OrcMetadataTag MetaCacheMgr::InitMetadataTag(RelFileNode *fileNode, int32 fileID, uint32 stripeID, uint32 columnID)
{
    OrcMetadataTag tag;
    tag.fileNode = *(RelFileNodeOld *)fileNode;
    tag.fileID = fileID;
    tag.stripeID = stripeID;
    tag.columnID = columnID;
    tag.padding = 0;
    return tag;
}

/*
 * @Description:  find metadata in cache
 * @IN columnID: column id
 * @IN fileID: file id
 * @IN fileNode: file node
 * @IN stripeID: stripe id
 * @Return: slot id
 * @See also:
 */
CacheSlotId_t MetaCacheMgr::FindMetaBlock(RelFileNode *fileNode, int32 fileID, uint32 stripeID, uint32 columnID)
{
    CacheSlotId_t slot = CACHE_BLOCK_INVALID_IDX;
    CacheTag cacheTag = {0};
    OrcMetadataTag metaTag = InitMetadataTag(fileNode, fileID, stripeID, columnID);

    m_cache_mgr->InitCacheBlockTag(&cacheTag, CACHE_ORC_INDEX, &metaTag, sizeof(OrcMetadataTag));
    slot = m_cache_mgr->FindCacheBlock(&cacheTag, true);

    ereport(DEBUG1,
            (errmodule(MOD_CACHE),
             errmsg("find metadata block, slotID(%d) , spcID(%u), dbID(%u), relID(%u), fileID(%d), stripeID(%u), "
                    "columnID(%u), found(%d)",
                    slot, fileNode->spcNode, fileNode->dbNode, fileNode->relNode, fileID, stripeID, columnID,
                    (slot != CACHE_BLOCK_INVALID_IDX))));
    return slot;
}

/*
 * @Description: get metadata cache block
 * @IN slot: slot id
 * @Return: metadata pointer
 * @See also:
 */
OrcMetadataValue *MetaCacheMgr::GetMetaBlock(CacheSlotId_t slot)
{
    return (OrcMetadataValue *)m_cache_mgr->GetCacheBlock(slot);
}

bool MetaCacheMgr::ReserveMetaBlockWithSlotId(CacheSlotId_t slotId)
{
    return (m_cache_mgr->ReserveCacheBlockWithSlotId(slotId));
}
/*
 * @Description: reserve  metadata block from index cache manager
 * @IN columnID: column id
 * @IN fileID: file id
 * @IN fileNode: file node
 * @IN/OUT hasFound:  whether found or not
 * @IN stripeID: stripe id
 * @Return: slot id
 * @See also:
 */
CacheSlotId_t MetaCacheMgr::ReserveMetaBlock(RelFileNode *fileNode, int32 fileID, uint32 stripeID, uint32 columnID,
                                             bool &hasFound)
{
    int slot;
    CacheTag cacheTag = {0};
    OrcMetadataTag metaTag = InitMetadataTag(fileNode, fileID, stripeID, columnID);

    m_cache_mgr->InitCacheBlockTag(&cacheTag, CACHE_ORC_INDEX, &metaTag, sizeof(OrcMetadataTag));
    slot = m_cache_mgr->ReserveCacheBlock(&cacheTag, METADATA_FIX_SIZE, hasFound);
    if (!hasFound) {
        /* remember block slot in process */
        Assert(!IsValidCacheSlotID(t_thrd.storage_cxt.MetaBlockInProgressIO));
        t_thrd.storage_cxt.MetaBlockInProgressIO = slot;
        SetMetaBlockSize(slot, METADATA_FIX_SIZE);
    }
    ereport(DEBUG1,
            (errmodule(MOD_CACHE),
             errmsg("allocate metadata block, slotID(%d), spcID(%u), dbID(%u), relID(%u), fileID(%d), stripeID(%u), "
                    "columnID(%u), found(%d)",
                    slot, fileNode->spcNode, fileNode->dbNode, fileNode->relNode, fileID, stripeID, columnID,
                    hasFound)));
    return slot;
}

/*
 * @Description:  remove pin of meta block
 * @IN slot: slot id
 * @See also:
 */
void MetaCacheMgr::UnPinMetaBlock(CacheSlotId_t slot)
{
    m_cache_mgr->UnPinCacheBlock(slot);
}

/*
 * @Description: get meta cache manager current memory cache size
 * @Return: cache size
 * @See also:
 */
int64 MetaCacheMgr::GetCurrentMemSize()
{
    if (!IS_SINGLE_NODE)
        return m_cache_mgr->GetCurrentMemSize();
    else
        return 0;
}

/*
 * @Description: print metadata block info if resource leak found
 * @IN slot: slot id
 * @See also:
 */
void MetaCacheMgr::PrintMetaCacheSlotLeakWarning(CacheSlotId_t slotId) const
{
    OrcMetadataTag tag = {0};
    uint32 refcount = 0;

    Assert(IsValidCacheSlotID(slotId));

    const CacheTag *cacheTag = m_cache_mgr->GetCacheBlockTag(slotId, &refcount);
    errno_t rc = memcpy_s(&tag, sizeof(OrcMetadataTag), cacheTag->key, sizeof(OrcMetadataTag));
    securec_check(rc, "\0", "\0");

    elog(WARNING,
         "Meta data cache refcount leak: type:%d , spcID: %u, dbID: %u, relID: %u, fileID: %d, stripeID: %u, columnID: "
         "%u, refcount: %u, slotId:%d",
         cacheTag->type, tag.fileNode.spcNode, tag.fileNode.dbNode, tag.fileNode.relNode, tag.fileID, tag.stripeID,
         tag.columnID, refcount, slotId);
}

/*
 * @Description: do resource clean if reserver error
 * @Param[IN] slot: slot id
 * @See also:
 */
void MetaCacheMgr::AbortMetaBlock(CacheSlotId_t slot)
{
    m_cache_mgr->AbortCacheBlock(slot);

    return;
}

/*
 * @Description: MetaBlockWaitIO
 * If the cache block is IOBUSY then go to sleep waiting on the IO busy lock.
 * If awakened while waiting, release the lock and check again.
 * This method is based upon WaitIO() for rowstore.
 * @Param[IN] slotId: slot id
 * @Return: true, means error happen in preftech
 * @See also:
 */
bool MetaCacheMgr::MetaBlockWaitIO(CacheSlotId_t slotId)
{
    return m_cache_mgr->WaitIO(slotId);
}

/*
 * @Description: MetaBlockCompleteIO
 * When an I/O is finished mark the CU as not busy and wake the next waiter.
 * this method is simmilar to the column store TerminateBufferIO
 * @Param[IN] slotId: slot id
 * @See also:
 */
void MetaCacheMgr::MetaBlockCompleteIO(CacheSlotId_t slotId)
{
    m_cache_mgr->CompleteIO(slotId);

    /* clear block slot in IO progress, aio completer thread does not use it */
    t_thrd.storage_cxt.MetaBlockInProgressIO = CACHE_BLOCK_INVALID_IDX;

    return;
}

/*
 * @Description: get meta cache block size
 * @IN slotId: slot id
 * @Return: block size
 * @See also:
 */
int MetaCacheMgr::GetMetaBlockSize(CacheSlotId_t slotId)
{
    OrcMetadataValue *value = GetMetaBlock(slotId);

    return value->size;
}

/*
 * @Description: set meta cache block size
 * @IN size: block size
 * @IN slotId: slot id
 * @See also:
 */
void MetaCacheMgr::SetMetaBlockSize(CacheSlotId_t slotId, int size)
{
    OrcMetadataValue *value = GetMetaBlock(slotId);

    value->size = size;
    return;
}

/*
 * @Description: calc metadata value size
 * @IN value: metadata value
 * @Return:
 * @See also:
 */
int MetaCacheMgr::CalcMetaBlockSize(OrcMetadataValue *value) const
{
    unsigned int size = 0;
    if (value->postScript != NULL) {
        size += value->postScript->length();
    }

    if (value->fileFooter != NULL) {
        size += value->fileFooter->length();
    }

    if (value->stripeFooter != NULL) {
        size += value->stripeFooter->length();
    }

    if (value->rowIndex != NULL) {
        size += value->rowIndex->length();
    }

    if (value->fileName != NULL) {
        size += strlen(value->fileName) + 1;
    }

    if (value->dataDNA != NULL) {
        size += strlen(value->dataDNA) + 1;
    }

    return size;
}

/*
 * @Description: must get pin first before used
 * @IN fileFooter: file footer
 * @IN footerStart: footer start
 * @IN postScript: post script
 * @IN rowIndex: row index
 * @IN slotId: solt id
 * @IN stripeFooter: stripr footer
 * @See also:
 */
void MetaCacheMgr::SetMetaBlockValue(CacheSlotId_t slotId, uint64 footerStart, const orc::proto::PostScript *postScript,
                                     const orc::proto::Footer *fileFooter, const orc::proto::StripeFooter *stripeFooter,
                                     const orc::proto::RowIndex *rowIndex, const char *fileName, const char *dataDNA)
{
    OrcMetadataValue *value = GetMetaBlock(slotId);
    errno_t rc = 0;

    /* the last MetaCache memory already released in CacheMgr::ReserveCacheBlock */
    value->footerStart = footerStart;
    value->postScript = NULL;
    value->fileFooter = NULL;
    value->stripeFooter = NULL;
    value->rowIndex = NULL;
    value->fileName = NULL;
    value->dataDNA = NULL;

    int old_size = GetMetaBlockSize(slotId);
    int new_size = 0;

    if (postScript != NULL) {
        value->postScript = new (std::nothrow) std::string();
        if (value->postScript == NULL) {
            goto out_of_memory;
        }
        postScript->SerializeToString(value->postScript);
    }

    if (fileFooter != NULL) {
        value->fileFooter = new (std::nothrow) std::string();
        if (value->fileFooter == NULL) {
            goto out_of_memory;
        }
        fileFooter->SerializeToString(value->fileFooter);
    }

    if (stripeFooter != NULL) {
        value->stripeFooter = new (std::nothrow) std::string();
        if (value->stripeFooter == NULL) {
            goto out_of_memory;
        }
        stripeFooter->SerializeToString(value->stripeFooter);
    }

    if (rowIndex != NULL) {
        value->rowIndex = new (std::nothrow) std::string();
        if (value->rowIndex == NULL) {
            goto out_of_memory;
        }
        rowIndex->SerializeToString(value->rowIndex);
    }

    if (fileName != NULL) {
        size_t fileNameLen = strlen(fileName);
        value->fileName = (char *)malloc(fileNameLen + 1);
        if (value->fileName == NULL) {
            goto out_of_memory;
        }
        rc = memcpy_s(value->fileName, fileNameLen, fileName, fileNameLen);
        securec_check(rc, "\0", "\0");
        value->fileName[fileNameLen] = '\0';
    }

    if (dataDNA != NULL) {
        size_t dataDNALen = strlen(dataDNA);
        value->dataDNA = (char *)malloc(dataDNALen + 1);
        if (value->dataDNA == NULL) {
            goto out_of_memory;
        }
        rc = memcpy_s(value->dataDNA, dataDNALen, dataDNA, dataDNALen);
        securec_check(rc, "\0", "\0");
        value->dataDNA[dataDNALen] = '\0';
    }
    new_size = CalcMetaBlockSize(value);
    SetMetaBlockSize(slotId, new_size);

    ereport(DEBUG1, (errmodule(MOD_CACHE),
                     errmsg("set metadata block slot(%d), size(%d), old size(%d)", slotId, new_size, old_size)));

    m_cache_mgr->AdjustCacheMem(slotId, old_size, new_size);

    return;

out_of_memory:
    /* clean memory */
    ReleaseMetadataValue(value);
    m_cache_mgr->AdjustCacheMem(slotId, old_size, new_size);
    ereport(ERROR, (errcode(ERRCODE_FDW_OUT_OF_MEMORY), errmodule(MOD_CACHE), errmsg("malloc fails, out of memory")));
    return;
}

/*
 * @Description: clean orc meta data value
 * @IN/OUT value: orc meta data value
 */
void MetaCacheMgr::ReleaseMetadataValue(OrcMetadataValue *value) const
{
    if (value == NULL)
        return;

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
}
