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
CacheSlotId_t MetaCacheAllocBlock(
    RelFileNodeOld* fileNode, int32 fileID, uint32 stripeOrBlocketID, uint32 columnID, bool& found, int type)
{
    CacheTag cacheTag = {0};
    errno_t rc = EOK;

    switch (type) {
        case CACHE_ORC_INDEX: {
            OrcMetadataTag OrcmetaTag = MetaCache->InitOrcMetadataTag(fileNode, fileID, stripeOrBlocketID, columnID);
            cacheTag.type = type;
            rc = memcpy_s(cacheTag.key, MAX_CACHE_TAG_LEN, &OrcmetaTag, sizeof(OrcMetadataTag));
            securec_check(rc, "", "");
            break;
        }
        case CACHE_CARBONDATA_METADATA: {
            CarbonMetadataTag CarbonmetaTag =
                MetaCache->InitCarbonMetadataTag(fileNode, fileID, stripeOrBlocketID, columnID);
            cacheTag.type = type;
            rc = memcpy_s(cacheTag.key, MAX_CACHE_TAG_LEN, &CarbonmetaTag, sizeof(CarbonMetadataTag));
            securec_check(rc, "", "");
            break;
        }
        default:
            break;
    }

    CacheSlotId_t slotId = MetaCache->FindMetaBlock(cacheTag);
    int maxRetry = 3;

    if (IsValidCacheSlotID(slotId)) {
        found = true;
    } else {
        found = false;
        slotId = MetaCache->ReserveMetaBlock(cacheTag, found);
    }

    while (found) {
        if (MetaCache->MetaBlockWaitIO(slotId)) {
            MetaCache->UnPinMetaBlock(slotId);
            if (maxRetry-- > 0) {
                slotId = MetaCache->ReserveMetaBlock(cacheTag, found);
                continue;
            } else {
                ereport(ERROR, (errcode(ERRCODE_NO_DATA_FOUND),
                                errmsg("wait IO find an error when allocate meta block, slotID(%d), spcID(%u),"
                                       "dbID(%u), relID(%u), fileID(%d), stripeOrBlocketID(%u), columnID(%u)",
                                       slotId, fileNode->spcNode, fileNode->dbNode, fileNode->relNode, fileID, stripeOrBlocketID,
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
OrcMetadataValue* OrcMetaCacheGetBlock(CacheSlotId_t slot)
{
    return MetaCache->GetOrcMetaBlock(slot);
}

CarbonMetadataValue* CarbonMetaCacheGetBlock(CacheSlotId_t slot)
{
    return MetaCache->GetCarbonMetaBlock(slot);
}

/*
 * @Description: get meta cache block size
 * @IN slotId: slot id
 * @Return: block size
 * @See also:
 */
int OrcMetaCacheGetBlockSize(CacheSlotId_t slot)
{
    return MetaCache->GetOrcMetaBlockSize(slot);
}

int CarbonMetaCacheGetBlockSize(CacheSlotId_t slot)
{
    return MetaCache->GetCarbonMetaBlockSize(slot);
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
    BUILD_BUG_ON((sizeof(CarbonMetadataTag) != MAX_CACHE_TAG_LEN));

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

#ifndef ENABLE_LITE_MODE
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
void OrcMetaCacheSetBlock(CacheSlotId_t slotId, uint64 footerStart, const orc::proto::PostScript* postScript,
                          const orc::proto::Footer* fileFooter, const orc::proto::StripeFooter* stripeFooter,
                          const orc::proto::RowIndex* rowIndex, const char* fileName, const char* dataDNA)
{
    MetaCache->SetOrcMetaBlockValue(
        slotId, footerStart, postScript, fileFooter, stripeFooter, rowIndex, fileName, dataDNA);
    MetaCache->MetaBlockCompleteIO(slotId);
}
#endif

void CarbonMetaCacheSetBlock(CacheSlotId_t slotId, uint64 headerSize, uint64 footerSize, unsigned char* fileHeader,
                             unsigned char* fileFooter, const char* fileName, const char* dataDNA)
{
    MetaCache->SetCarbonMetaBlockValue(slotId, headerSize, footerSize, fileHeader, fileFooter, fileName, dataDNA);
    MetaCache->MetaBlockCompleteIO(slotId);
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
OrcMetadataTag MetaCacheMgr::InitOrcMetadataTag(
    RelFileNodeOld* fileNode, int32 fileID, uint32 stripeID, uint32 columnID)
{
    OrcMetadataTag tag;
    tag.fileNode = *fileNode;
    tag.fileID = fileID;
    tag.stripeID = stripeID;
    tag.columnID = columnID;
    tag.padding = 0;
    return tag;
}

CarbonMetadataTag MetaCacheMgr::InitCarbonMetadataTag(
    RelFileNodeOld* fileNode, int32 fileID, uint32 BlockletID, uint32 columnID)
{
    CarbonMetadataTag tag;
    tag.fileNode = *fileNode;
    tag.fileID = fileID;
    tag.BlockletID = BlockletID;
    tag.columnID = columnID;
    tag.padding = 0;
    return tag;
}

/*
 * @Description:  find metadata in cache
 * @IN cacheTag: CacheTag of metadata
 * @Return: slot id
 * @See also:
 */
CacheSlotId_t MetaCacheMgr::FindMetaBlock(CacheTag cacheTag)
{
    CacheSlotId_t slot = CACHE_BLOCK_INVALID_IDX;

    slot = m_cache_mgr->FindCacheBlock(&cacheTag, true);

    switch (cacheTag.type) {
        case CACHE_ORC_INDEX: {
            OrcMetadataTag* tag = (OrcMetadataTag*)(&cacheTag.key);
            ereport(DEBUG1,
                    (errmodule(MOD_CACHE),
                     errmsg("find Orc metadata block, slotID(%d) , spcID(%u), dbID(%u), relID(%u), fileID(%d), "
                            "stripeID(%u), columnID(%d), found(%d)",
                            slot, tag->fileNode.spcNode, tag->fileNode.dbNode, tag->fileNode.relNode, tag->fileID, tag->stripeID, tag->columnID,
                            (slot != CACHE_BLOCK_INVALID_IDX))));
            break;
        }
        case CACHE_CARBONDATA_METADATA: {
            CarbonMetadataTag* tag = (CarbonMetadataTag*)(&cacheTag.key);
            ereport(DEBUG1,
                    (errmodule(MOD_CACHE),
                     errmsg("find CarbonData metadata block, slotID(%d) , spcID(%u), dbID(%u), relID(%u), fileID(%d), "
                            "BlockletID(%u), columnID(%d), found(%d)",
                            slot, tag->fileNode.spcNode, tag->fileNode.dbNode, tag->fileNode.relNode, tag->fileID, tag->BlockletID, tag->columnID,
                            (slot != CACHE_BLOCK_INVALID_IDX))));
            break;
        }
        default:
            break;
    }

    return slot;
}

/*
 * @Description: get metadata cache block
 * @IN slot: slot id
 * @Return: metadata pointer
 * @See also:
 */
OrcMetadataValue* MetaCacheMgr::GetOrcMetaBlock(CacheSlotId_t slot)
{
    return (OrcMetadataValue *)m_cache_mgr->GetCacheBlock(slot);
}

CarbonMetadataValue* MetaCacheMgr::GetCarbonMetaBlock(CacheSlotId_t slot)
{
    return (CarbonMetadataValue*)m_cache_mgr->GetCacheBlock(slot);
}

bool MetaCacheMgr::ReserveMetaBlockWithSlotId(CacheSlotId_t slotId)
{
    return (m_cache_mgr->ReserveCacheBlockWithSlotId(slotId));
}
/*
 * @Description: reserve  metadata block from index cache manager
 * @IN CacheTag: CacheTag of metadata
 * @IN/OUT hasFound:  whether found or not
 * @Return: slot id
 * @See also:
 */
CacheSlotId_t MetaCacheMgr::ReserveMetaBlock(CacheTag cacheTag, bool& hasFound)
{
    CacheSlotId_t slot = CACHE_BLOCK_INVALID_IDX;

    slot = m_cache_mgr->ReserveCacheBlock(&cacheTag, METADATA_FIX_SIZE, hasFound);
    if (!hasFound) {
        /* remember block slot in process */
        Assert(!IsValidCacheSlotID(t_thrd.storage_cxt.MetaBlockInProgressIO));
        t_thrd.storage_cxt.MetaBlockInProgressIO = slot;

        switch (cacheTag.type) {
            case CACHE_ORC_INDEX: {
                SetOrcMetaBlockSize(slot, METADATA_FIX_SIZE);
                OrcMetadataTag* tag = (OrcMetadataTag*)(&cacheTag.key);
                ereport(DEBUG1,
                        (errmodule(MOD_CACHE),
                         errmsg("allocate Orc metadata block, slotID(%d), spcID(%u), dbID(%u), relID(%u), fileID(%d), "
                                "stripeID(%u), columnID(%d), found(%d)",
                                slot, tag->fileNode.spcNode, tag->fileNode.dbNode, tag->fileNode.relNode, tag->fileID, tag->stripeID, tag->columnID,
                                hasFound)));
                break;
            }
            case CACHE_CARBONDATA_METADATA: {
                SetCarbonMetaBlockSize(slot, METADATA_FIX_SIZE);
                CarbonMetadataTag* tag = (CarbonMetadataTag*)(&cacheTag.key);
                ereport(DEBUG1,
                        (errmodule(MOD_CACHE),
                         errmsg("allocate Carbondata metadata block, slotID(%d), spcID(%u), dbID(%u), relID(%u), "
                                "fileID(%d), BlockletID(%u), columnID(%d), found(%d)",
                                slot, tag->fileNode.spcNode, tag->fileNode.dbNode, tag->fileNode.relNode, tag->fileID, tag->BlockletID, tag->columnID,
                                hasFound)));
                break;
            }
            default:
                break;
        }
    }

    return slot;
}

/*
 * @Description:  remove pin of meta block
 * @IN slot: slot id
 * @See also:
 */
void MetaCacheMgr::UnPinMetaBlock(CacheSlotId_t slotId)
{
    m_cache_mgr->UnPinCacheBlock(slotId);
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
void MetaCacheMgr::PrintMetaCacheSlotLeakWarning(CacheSlotId_t slotId)
{
    uint32 refcount = 0;

    Assert(IsValidCacheSlotID(slotId));

    const CacheTag *cacheTag = m_cache_mgr->GetCacheBlockTag(slotId, &refcount);

    switch (cacheTag->type) {
        case CACHE_ORC_INDEX: {
            OrcMetadataTag tag = {0};
            errno_t rc = memcpy_s(&tag, sizeof(OrcMetadataTag), cacheTag->key, sizeof(OrcMetadataTag));
            securec_check(rc, "\0", "\0");

            elog(WARNING,
                 "Orc Meta data cache refcount leak: type:%d , spcID: %u, dbID: %u, relID: %u, fileID: %u, stripeID: "
                 "%u, columnID: %d, refcount: %u, slotId:%d",
                 cacheTag->type, tag.fileNode.spcNode, tag.fileNode.dbNode, tag.fileNode.relNode, tag.fileID, tag.stripeID,
                 tag.columnID, refcount, slotId);
            break;
        }
        case CACHE_CARBONDATA_METADATA: {
            CarbonMetadataTag tag = {0};
            errno_t rc = memcpy_s(&tag, sizeof(CarbonMetadataTag), cacheTag->key, sizeof(CarbonMetadataTag));
            securec_check(rc, "\0", "\0");

            elog(WARNING,
                 "Carbondata Meta data cache refcount leak: type:%d , spcID: %u, dbID: %u, relID: %u, fileID: %u, "
                 "BlockletID: %u, columnID: %d, refcount: %u, slotId:%d",
                 cacheTag->type, tag.fileNode.spcNode, tag.fileNode.dbNode, tag.fileNode.relNode, tag.fileID, tag.BlockletID,
                 tag.columnID, refcount, slotId);
            break;
        }
        default:
            break;
    }
}

/*
 * @Description: do resource clean if reserver error
 * @Param[IN] slot: slot id
 * @See also:
 */
void MetaCacheMgr::AbortMetaBlock(CacheSlotId_t slotId)
{
    m_cache_mgr->AbortCacheBlock(slotId);

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
int MetaCacheMgr::GetOrcMetaBlockSize(CacheSlotId_t slotId)
{
    OrcMetadataValue* value = GetOrcMetaBlock(slotId);

    return value->size;
}

int MetaCacheMgr::GetCarbonMetaBlockSize(CacheSlotId_t slotId)
{
    CarbonMetadataValue* nvalue = GetCarbonMetaBlock(slotId);

    return nvalue->size;
}

/*
 * @Description: set meta cache block size
 * @IN size: block size
 * @IN slotId: slot id
 * @See also:
 */
void MetaCacheMgr::SetOrcMetaBlockSize(CacheSlotId_t slotId, int size)
{
    OrcMetadataValue* value = GetOrcMetaBlock(slotId);

    value->size = size;
    return;
}

void MetaCacheMgr::SetCarbonMetaBlockSize(CacheSlotId_t slotId, int size)
{
    CarbonMetadataValue* nvalue = GetCarbonMetaBlock(slotId);

    nvalue->size = size;
    return;
}

/*
 * @Description: calc metadata value size
 * @IN value: metadata value
 * @Return:
 * @See also:
 */
int MetaCacheMgr::CalcOrcMetaBlockSize(OrcMetadataValue* nvalue) const
{
    unsigned int size = 0;
    if (nvalue->postScript != NULL) {
        size += (unsigned int)nvalue->postScript->length();
    }

    if (nvalue->fileFooter != NULL) {
        size += (unsigned int)nvalue->fileFooter->length();
    }

    if (nvalue->stripeFooter != NULL) {
        size += (unsigned int)nvalue->stripeFooter->length();
    }

    if (nvalue->rowIndex != NULL) {
        size += (unsigned int)nvalue->rowIndex->length();
    }

    if (nvalue->fileName != NULL) {
        size += (unsigned int)strlen(nvalue->fileName) + 1;
    }

    if (nvalue->dataDNA != NULL) {
        size += (unsigned int)strlen(nvalue->dataDNA) + 1;
    }

    return size;
}

int MetaCacheMgr::CalcCarbonMetaBlockSize(CarbonMetadataValue* nvalue) const
{
    unsigned int size = 0;
    if (NULL != nvalue->fileHeader) {
        size += nvalue->headerSize;
    }

    if (NULL != nvalue->fileFooter) {
        size += nvalue->footerSize;
    }

    if (NULL != nvalue->fileName) {
        size += (unsigned int)strlen(nvalue->fileName) + 1;
    }

    if (NULL != nvalue->dataDNA) {
        size += (unsigned int)strlen(nvalue->dataDNA) + 1;
    }

    return size;
}

#ifndef ENABLE_LITE_MODE
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
void MetaCacheMgr::SetOrcMetaBlockValue(CacheSlotId_t slotId, uint64 footerStart,
                                        const orc::proto::PostScript* postScript, const orc::proto::Footer* fileFooter,
                                        const orc::proto::StripeFooter* stripeFooter, const orc::proto::RowIndex* rowIndex, const char* fileName,
                                        const char* dataDNA)
{
    OrcMetadataValue* nvalue = GetOrcMetaBlock(slotId);
    errno_t rc = 0;

    /* the last MetaCache memory already released in CacheMgr::ReserveCacheBlock */
    nvalue->footerStart = footerStart;
    nvalue->postScript = NULL;
    nvalue->fileFooter = NULL;
    nvalue->stripeFooter = NULL;
    nvalue->rowIndex = NULL;
    nvalue->fileName = NULL;
    nvalue->dataDNA = NULL;

    int old_size = GetOrcMetaBlockSize(slotId);
    int new_size = 0;

    if (postScript != NULL) {
        nvalue->postScript = new (std::nothrow) std::string();
        if (nvalue->postScript == NULL) {
            goto out_of_memory;
        }
        postScript->SerializeToString(nvalue->postScript);
    }

    if (fileFooter != NULL) {
        nvalue->fileFooter = new (std::nothrow) std::string();
        if (nvalue->fileFooter == NULL) {
            goto out_of_memory;
        }
        fileFooter->SerializeToString(nvalue->fileFooter);
    }

    if (stripeFooter != NULL) {
        nvalue->stripeFooter = new (std::nothrow) std::string();
        if (nvalue->stripeFooter == NULL) {
            goto out_of_memory;
        }
        stripeFooter->SerializeToString(nvalue->stripeFooter);
    }

    if (rowIndex != NULL) {
        nvalue->rowIndex = new (std::nothrow) std::string();
        if (nvalue->rowIndex == NULL) {
            goto out_of_memory;
        }
        rowIndex->SerializeToString(nvalue->rowIndex);
    }

    if (fileName != NULL) {
        size_t fileNameLen = strlen(fileName);
        nvalue->fileName = (char*)malloc(fileNameLen + 1);
        if (nvalue->fileName == NULL) {
            goto out_of_memory;
        }
        rc = memcpy_s(nvalue->fileName, fileNameLen, fileName, fileNameLen);
        securec_check(rc, "\0", "\0");
        nvalue->fileName[fileNameLen] = '\0';
    }

    if (dataDNA != NULL) {
        size_t dataDNALen = strlen(dataDNA);
        nvalue->dataDNA = (char*)malloc(dataDNALen + 1);
        if (nvalue->dataDNA == NULL) {
            goto out_of_memory;
        }
        rc = memcpy_s(nvalue->dataDNA, dataDNALen, dataDNA, dataDNALen);
        securec_check(rc, "\0", "\0");
        nvalue->dataDNA[dataDNALen] = '\0';
    }
    new_size = CalcOrcMetaBlockSize(nvalue);
    SetOrcMetaBlockSize(slotId, new_size);

    ereport(DEBUG1, (errmodule(MOD_CACHE),
                     errmsg("set Orc metadata block slot(%d), size(%d), old size(%d)", slotId, new_size, old_size)));

    m_cache_mgr->AdjustCacheMem(slotId, old_size, new_size);

    return;

out_of_memory:
    /* clean memory */
    ReleaseOrcMetadataValue(nvalue);
    m_cache_mgr->AdjustCacheMem(slotId, old_size, new_size);
    ereport(ERROR, (errcode(ERRCODE_FDW_OUT_OF_MEMORY), errmodule(MOD_CACHE), errmsg("malloc fails, out of memory")));
    return;
}
#endif

void MetaCacheMgr::SetCarbonMetaBlockValue(CacheSlotId_t slotId, uint64 headerSize, uint64 footerSize,
                                           unsigned char* fileHeader, unsigned char* fileFooter, const char* fileName, const char* dataDNA)
{
    CarbonMetadataValue* nvalue = GetCarbonMetaBlock(slotId);
    errno_t rc = 0;

    /* the last MetaCache memory already released in CacheMgr::ReserveCacheBlock */
    nvalue->headerSize = headerSize;
    nvalue->footerSize = footerSize;
    nvalue->fileHeader = NULL;
    nvalue->fileFooter = NULL;
    nvalue->fileName = NULL;
    nvalue->dataDNA = NULL;

    int old_size = GetCarbonMetaBlockSize(slotId);
    int new_size = 0;

    if (NULL != fileHeader) {
        nvalue->fileHeader = (unsigned char*)malloc(headerSize);
        if (nvalue->fileHeader == NULL) {
            goto out_of_memory;
        }
        rc = memcpy_s(nvalue->fileHeader, headerSize, fileHeader, headerSize);
        securec_check(rc, "\0", "\0");
    }

    if (NULL != fileFooter) {
        nvalue->fileFooter = (unsigned char*)malloc(footerSize);
        if (nvalue->fileFooter == NULL) {
            goto out_of_memory;
        }
        rc = memcpy_s(nvalue->fileFooter, footerSize, fileFooter, footerSize);
        securec_check(rc, "\0", "\0");
    }

    if (NULL != fileName) {
        size_t fileNameLen = strlen(fileName);
        nvalue->fileName = (char*)malloc(fileNameLen + 1);
        if (nvalue->fileName == NULL) {
            goto out_of_memory;
        }
        rc = memcpy_s(nvalue->fileName, fileNameLen, fileName, fileNameLen);
        securec_check(rc, "\0", "\0");
        nvalue->fileName[fileNameLen] = '\0';
    }

    if (NULL != dataDNA) {
        size_t dataDNALen = strlen(dataDNA);
        nvalue->dataDNA = (char*)malloc(dataDNALen + 1);
        if (nvalue->dataDNA == NULL) {
            goto out_of_memory;
        }
        rc = memcpy_s(nvalue->dataDNA, dataDNALen, dataDNA, dataDNALen);
        securec_check(rc, "\0", "\0");
        nvalue->dataDNA[dataDNALen] = '\0';
    }

    new_size = CalcCarbonMetaBlockSize(nvalue);
    SetCarbonMetaBlockSize(slotId, new_size);

    ereport(DEBUG1,
            (errmodule(MOD_CACHE),
             errmsg("set Carbondata metadata block slotId(%d), size(%d), old size(%d)", slotId, new_size, old_size)));

    m_cache_mgr->AdjustCacheMem(slotId, old_size, new_size);
    return;

out_of_memory:
    /* clean memory */
    ReleaseCarbonMetadataValue(nvalue);
    m_cache_mgr->AdjustCacheMem(slotId, old_size, new_size);
    ereport(ERROR, (errcode(ERRCODE_FDW_OUT_OF_MEMORY), errmodule(MOD_CACHE), errmsg("malloc fails, out of memory")));
    return;
}

/*
 * @Description: clean orc meta data value
 * @IN/OUT value: orc meta data value
 */
void MetaCacheMgr::ReleaseCarbonMetadataValue(CarbonMetadataValue* nvalue) const
{
    if (nvalue == NULL)
        return;

    if (NULL != nvalue->fileHeader) {
        free(nvalue->fileHeader);
        nvalue->fileHeader = NULL;
    }
    if (NULL != nvalue->fileFooter) {
        free(nvalue->fileFooter);
        nvalue->fileFooter = NULL;
    }
    if (NULL != nvalue->fileName) {
        free(nvalue->fileName);
        nvalue->fileName = NULL;
    }
    if (NULL != nvalue->dataDNA) {
        free(nvalue->dataDNA);
        nvalue->dataDNA = NULL;
    }

    nvalue->headerSize = 0;
    nvalue->footerSize = 0;
    nvalue->size = 0;
}

/*
 * @Description: clean orc meta data value
 * @IN/OUT value: orc meta data value
 */
void MetaCacheMgr::ReleaseOrcMetadataValue(OrcMetadataValue* nvalue) const
{
    if (nvalue == NULL)
        return;

    if (nvalue->postScript != NULL) {
        delete nvalue->postScript;
        nvalue->postScript = NULL;
    }
    if (nvalue->fileFooter != NULL) {
        delete nvalue->fileFooter;
        nvalue->fileFooter = NULL;
    }
    if (nvalue->stripeFooter != NULL) {
        delete nvalue->stripeFooter;
        nvalue->stripeFooter = NULL;
    }
    if (nvalue->rowIndex != NULL) {
        delete nvalue->rowIndex;
        nvalue->rowIndex = NULL;
    }
    if (nvalue->fileName != NULL) {
        free(nvalue->fileName);
        nvalue->fileName = NULL;
    }
    if (nvalue->dataDNA != NULL) {
        free(nvalue->dataDNA);
        nvalue->dataDNA = NULL;
    }

    nvalue->footerStart = 0;
    nvalue->size = 0;
}
