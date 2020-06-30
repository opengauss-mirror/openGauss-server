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
 * dfscache_mgr.h
 *        routines to support dfs
 *
 *
 * IDENTIFICATION
 *        src/include/storage/dfs/dfscache_mgr.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef METACACHEMGR_H
#define METACACHEMGR_H

#include <string>
#include "orc/Reader.hh"
#include "dfs_adaptor.h"
#include "storage/cache_mgr.h"
#include "storage/relfilenode.h"

#define MetaCache (MetaCacheMgr::GetInstance())

typedef struct OrcMetadataTag {
    RelFileNodeOld fileNode;
    int32 fileID;
    uint32 stripeID;
    uint32 columnID;
    uint64 padding;
} OrcMetadataTag;

typedef struct OrcMetadataValue {
    /*
     * The value of element pointer could be de-referenced into 'DfsInsert *' or
     * 'PartitionStagingFile *'
     */
    // type 1 metadeta, file level, only need relID,fileID
    uint64 footerStart;
    std::string *postScript;
    std::string *fileFooter;

    // type 2 metadata, column and stripe level
    std::string *stripeFooter;
    std::string *rowIndex;

    int size;

    // used in OBS file
    char *fileName;

    // data DNA, used in OBS foreign table file
    char *dataDNA;
} OrcMetadataValue;

/* Partition search cache support */
typedef struct OrcMetadataEntry {
    /* key of cache entry */
    OrcMetadataTag key;
    int32 slot_id;
} OrcMetadataEntry;

int MetaCacheMgrNumLocks();
void ReleaseMetaBlock(CacheSlotId_t slot);
CacheSlotId_t MetaCacheAllocBlock(RelFileNode *fileNode, int32 fileID, uint32 stripeID, uint32 columnID, bool &found);
bool MetaCacheRenewBlock(CacheSlotId_t slotId);
OrcMetadataValue *MetaCacheGetBlock(CacheSlotId_t slot);
int MetaCacheGetBlockSize(CacheSlotId_t slot);
void MetaCacheSetBlock(CacheSlotId_t slotId, uint64 footerStart, const orc::proto::PostScript *postScript,
                       const orc::proto::Footer *fileFooter, const orc::proto::StripeFooter *stripeFooter,
                       const orc::proto::RowIndex *rowIndex, const char *fileName, const char *dataDNA);
void MetaCacheSetBlockWithFileName(CacheSlotId_t slotId, const char *fileName);

class MetaCacheMgr : public BaseObject {
public:
    virtual ~MetaCacheMgr()
    {
    }
    static MetaCacheMgr *GetInstance(void);
    static void NewSingletonInstance(void);

    CacheSlotId_t FindMetaBlock(RelFileNode *fileNode, int32 fileID, uint32 stripeID, uint32 columnID);
    CacheSlotId_t ReserveMetaBlock(RelFileNode *fileNode, int32 fileID, uint32 stripeID, uint32 columnID,
                                   bool &hasFound);
    bool ReserveMetaBlockWithSlotId(CacheSlotId_t slotId);
    OrcMetadataValue *GetMetaBlock(CacheSlotId_t slot);

    void UnPinMetaBlock(CacheSlotId_t slot);

    int64 GetCurrentMemSize();
    void PrintMetaCacheSlotLeakWarning(CacheSlotId_t slotId) const;

    void AbortMetaBlock(CacheSlotId_t slot);
    // Manage I/O busy CUs
    bool MetaBlockWaitIO(int slot);
    void MetaBlockCompleteIO(int slot);

    int GetMetaBlockSize(CacheSlotId_t slotId);
    void SetMetaBlockValue(CacheSlotId_t slotId, uint64 footerStart, const orc::proto::PostScript *postScript,
                           const orc::proto::Footer *fileFooter, const orc::proto::StripeFooter *stripeFooter,
                           const orc::proto::RowIndex *rowIndex, const char *fileName, const char *dataDNA);
    int64 m_cstoreMaxSize;

private:
    MetaCacheMgr()
    {
    }
    OrcMetadataTag InitMetadataTag(RelFileNode *fileNode, int32 fileID, uint32 stripeID, uint32 columnID);
    int CalcMetaBlockSize(OrcMetadataValue *value) const;
    void SetMetaBlockSize(CacheSlotId_t slotId, int size);
    void ReleaseMetadataValue(OrcMetadataValue *value) const;
    static MetaCacheMgr *m_meta_cache;
    CacheMgr *m_cache_mgr;
};

#endif  // define
