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

#include "pg_config.h"

#include <string>
#ifndef ENABLE_LITE_MODE
#include "orc_proto.pb.h"
#endif
#include "storage/cache_mgr.h"
#include "storage/smgr/relfilenode.h"

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
    /* type 1 metadeta, file level, only need relID,fileID */
    uint64 footerStart;
    std::string* postScript;
    std::string* fileFooter;

    /* type 2 metadata, column and stripe level */
    std::string* stripeFooter;
    std::string* rowIndex;

    int size;

    /* used in OBS file */
    char* fileName;

    /* data DNA, used in OBS foreign table file */
    char* dataDNA;
} OrcMetadataValue;

typedef struct CarbonMetadataTag {
    RelFileNodeOld fileNode;
    int32 fileID;
    uint32 BlockletID;
    uint32 columnID;
    uint64 padding;
} CarbonMetadataTag;

typedef struct CarbonMetadataValue {
    /* type 1 metadeta, file level, only need relID,fileID */
    uint64 headerSize;
    uint64 footerSize;
    unsigned char* fileHeader;
    unsigned char* fileFooter;

    uint64 size;

    /* used in OBS file */
    char* fileName;

    /* data DNA, used in OBS foreign table file */
    char* dataDNA;
} CarbonMetadataValue;

typedef union MetadataTag {
    OrcMetadataTag OrcMetaTag;
    CarbonMetadataTag CarbonMetaTag;
} MetadataTag;

typedef struct MetadataTagKey {
    CacheType type;
    MetadataTag key;
} MetadataTagKey;

/* Partition search cache support */
typedef struct OrcMetadataEntry {
    /* key of cache entry */
    OrcMetadataTag key;
    int32 slot_id;
} OrcMetadataEntry;

int MetaCacheMgrNumLocks();
void ReleaseMetaBlock(CacheSlotId_t slotId);
bool MetaCacheRenewBlock(CacheSlotId_t slotId);
OrcMetadataValue* OrcMetaCacheGetBlock(CacheSlotId_t slotId);
CarbonMetadataValue* CarbonMetaCacheGetBlock(CacheSlotId_t slotId);
int OrcMetaCacheGetBlockSize(CacheSlotId_t slotId);
int CarbonMetaCacheGetBlockSize(CacheSlotId_t slotId);
void MetaCacheSetBlockWithFileName(CacheSlotId_t slotId, const char* fileName);
CacheSlotId_t MetaCacheAllocBlock(
    RelFileNodeOld* fileNode, int32 fileID, uint32 stripeOrBlocketID, uint32 columnID, bool& found, int type);
#ifndef ENABLE_LITE_MODE
void OrcMetaCacheSetBlock(CacheSlotId_t slotId, uint64 footerStart, const orc::proto::PostScript* postScript,
    const orc::proto::Footer* fileFooter, const orc::proto::StripeFooter* stripeFooter,
    const orc::proto::RowIndex* rowIndex, const char* fileName, const char* dataDNA);
#endif
void CarbonMetaCacheSetBlock(CacheSlotId_t slotId, uint64 headerSize, uint64 footerSize, unsigned char* fileHeader,
    unsigned char* fileFooter, const char* fileName, const char* dataDNA);

class MetaCacheMgr : public BaseObject {
public:
    virtual ~MetaCacheMgr()
    {}
    static MetaCacheMgr* GetInstance(void);
    static void NewSingletonInstance(void);
    bool ReserveMetaBlockWithSlotId(CacheSlotId_t slotId);
    void UnPinMetaBlock(CacheSlotId_t slotId);

    int64 GetCurrentMemSize();

    void AbortMetaBlock(CacheSlotId_t slotId);
    /* Manage I/O busy CUs */
    bool MetaBlockWaitIO(int slotId);
    void MetaBlockCompleteIO(int slotId);
    int GetOrcMetaBlockSize(CacheSlotId_t slotId);
    int GetCarbonMetaBlockSize(CacheSlotId_t slotId);
#ifndef ENABLE_LITE_MODE
    void SetOrcMetaBlockValue(CacheSlotId_t slotId, uint64 footerStart, const orc::proto::PostScript* postScript,
        const orc::proto::Footer* fileFooter, const orc::proto::StripeFooter* stripeFooter,
        const orc::proto::RowIndex* rowIndex, const char* fileName, const char* dataDNA);
#endif
    void SetCarbonMetaBlockValue(CacheSlotId_t slotId, uint64 headerSize, uint64 footerSize, unsigned char* fileHeader,
        unsigned char* fileFooter, const char* fileName, const char* dataDNA);

    CacheSlotId_t FindMetaBlock(CacheTag cacheTag);
    OrcMetadataValue* GetOrcMetaBlock(CacheSlotId_t slotId);
    CarbonMetadataValue* GetCarbonMetaBlock(CacheSlotId_t slotId);
    CacheSlotId_t ReserveMetaBlock(CacheTag cacheTag, bool& hasFound);
    void PrintMetaCacheSlotLeakWarning(CacheSlotId_t slotId);
    void ReleaseMetadataValue(CarbonMetadataValue* nvalue);
    OrcMetadataTag InitOrcMetadataTag(RelFileNodeOld* fileNode, int32 fileID, uint32 stripeID, uint32 columnID);
    CarbonMetadataTag InitCarbonMetadataTag(RelFileNodeOld* fileNode, int32 fileID, uint32 stripeID, uint32 columnID);
    int64 m_cstoreMaxSize;

private:
    MetaCacheMgr()
    {}
    int CalcOrcMetaBlockSize(OrcMetadataValue* nvalue) const;
    int CalcCarbonMetaBlockSize(CarbonMetadataValue* nvalue) const;
    void SetOrcMetaBlockSize(CacheSlotId_t slotId, int size);
    void SetCarbonMetaBlockSize(CacheSlotId_t slotId, int size);
    void ReleaseOrcMetadataValue(OrcMetadataValue* nvalue) const;
    void ReleaseCarbonMetadataValue(CarbonMetadataValue* nvalue) const;
    static MetaCacheMgr* m_meta_cache;
    CacheMgr* m_cache_mgr;
};

#endif /* define */
