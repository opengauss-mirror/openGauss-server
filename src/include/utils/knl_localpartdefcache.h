/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
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
 * knl_localpartdefcache.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/utils/knl_localpartdefcache.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef KNL_LOCALPARTDEFCACHE_H
#define KNL_LOCALPARTDEFCACHE_H

#include "utils/knl_localbasedefcache.h"
#include "utils/knl_globalpartdefcache.h"
#include "utils/relcache.h"

class LocalPartDefCache : public LocalBaseDefCache {
public:
    LocalPartDefCache();

    void ResetInitFlag()
    {
        m_bucket_list.ResetContent();
        invalid_entries.ResetInitFlag();
        m_global_partdefcache = NULL;
        PartCacheNeedEOXActWork = false;
        m_is_inited = false;
        m_db_id = InvalidOid;
    }

    void Init();
    void CreateDefBucket()
    {
        LocalBaseDefCache::CreateDefBucket(LOCAL_INIT_PARTCACHE_SIZE);
    }
    Partition SearchPartition(Oid part_id);
    Partition SearchPartitionFromLocal(Oid part_id);
    template <bool insert_into_local>
    Partition SearchPartitionFromGlobalCopy(Oid part_id);

    void InsertPartitionIntoLocal(Partition part);
    void RemovePartition(Partition part);

    void InvalidateGlobalPartition(Oid db_oid, Oid part_oid, bool is_commit);
    void InvalidateAll();
    void AtEOXact_PartitionCache(bool isCommit);
    void AtEOSubXact_PartitionCache(bool isCommit, SubTransactionId mySubid, SubTransactionId parentSubid);
    Partition PartitionIdGetPartition(Oid part_oid, StorageType storage_type);

public:
    bool PartCacheNeedEOXActWork;

private:
    void InsertPartitionIntoGlobal(Partition part, uint32 hash_value);
    void CreateLocalPartEntry(Partition part, Index hash_index);
    void CopyLocalPartition(Partition dest, Partition src);
    Partition RemovePartitionByOid(Oid part_id, Index hash_index);
    LocalPartitionEntry *FindPartitionFromLocal(Oid part_id);
    GlobalPartDefCache *m_global_partdefcache;
    bool m_is_inited;
};

#endif