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
 * knl_localtabdefcache.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/utils/knl_localtabdefcache.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef KNL_LOCALTABDEFCACHE_H
#define KNL_LOCALTABDEFCACHE_H

#include "utils/knl_globaltabdefcache.h"
#include "utils/knl_localbasedefcache.h"
#include "utils/rel.h"

class LocalTabDefCache : public LocalBaseDefCache {
public:
    LocalTabDefCache();
    void ResetInitFlag(bool include_shared);
    Relation SearchRelation(Oid rel_id);
    Relation SearchRelationFromLocal(Oid rel_id);
    template <bool insert_into_local>
    Relation SearchRelationFromGlobalCopy(Oid rel_id);
    void InsertRelationIntoLocal(Relation rel);
    void RemoveRelation(Relation rel);
    void CreateDefBucket()
    {
        LocalBaseDefCache::CreateDefBucket(LOCAL_INIT_RELCACHE_SIZE);
    }
    void Init();
    void InitPhase2();
    void InitPhase3();

    void InvalidateRelationNodeList();
    void InvalidateGlobalRelationNodeList()
    {
        m_global_tabdefcache->InvalidateRelationNodeList();
    }
    void InvalidateRelationAll();
    void InvalidateRelationBucketsAll();
    void InvalidateGlobalRelation(Oid db_id, Oid rel_oid, bool is_commit);
    /* Free all tupleDescs remembered in RememberToFreeTupleDescAtEOX in a batch when a transaction ends */
    void AtEOXact_FreeTupleDesc();
    void AtEOXact_RelationCache(bool isCommit);
    void AtEOSubXact_RelationCache(bool isCommit, SubTransactionId mySubid, SubTransactionId parentSubid);
    /* Remember old tupleDescs when processing invalid messages */
    void RememberToFreeTupleDescAtEOX(TupleDesc td);
    TupleDesc GetPgClassDescriptor()
    {
        if (m_pgclassdesc == NULL) {
            m_pgclassdesc = m_global_tabdefcache->GetPgClassDescriptor();
        }
        return m_pgclassdesc;
    }
    TupleDesc GetPgIndexDescriptor()
    {
        if (m_pgindexdesc == NULL) {
            m_pgindexdesc = m_global_tabdefcache->GetPgIndexDescriptor();
        }
        return m_pgindexdesc;
    }
    Relation RelationIdGetRelation(Oid rel_oid);

private:

    void InsertRelationIntoGlobal(Relation rel, uint32 hash_value);
    void CreateLocalRelEntry(Relation rel, Index hash_index);
    Relation RemoveRelationByOid(Oid rel_id, Index hash_index);
    void CopyLocalRelation(Relation dest, Relation src);
    void LoadCriticalIndex(Oid indexoid, Oid heapoid);
    void FormrDesc(const char *relationName, Oid relationReltype, bool is_shared, bool hasoids, int natts,
        const struct FormData_pg_attribute *attrs);

public:
    /*
     * This flag is false until we have hold the CriticalCacheBuildLock
     */
    bool needNewLocalCacheFile;

    /*
     * This flag is false until we have prepared the critical relcache entries
     * that are needed to do indexscans on the tables read by relcache building.
     * Should be used only by relcache.c and catcache.c
     */
    bool criticalRelcachesBuilt;

    /*
     * This flag is false until we have prepared the critical relcache entries
     * for shared catalogs (which are the tables needed for login).
     * Should be used only by relcache.c and postinit.c
     */
    bool criticalSharedRelcachesBuilt;

    /*
     * This counter counts relcache inval events received since backend startup
     * (but only for rels that are actually in cache).    Presently, we use it only
     * to detect whether data about to be written by write_relcache_init_file()
     * might already be obsolete.
     */
    long relcacheInvalsReceived;

    /*
     * This list remembers the OIDs of the non-shared relations cached in the
     * database's local relcache init file.  Note that there is no corresponding
     * list for the shared relcache init file, for reasons explained in the
     * comments for RelationCacheInitFileRemove.
     */
    List *initFileRelationIds;

    bool RelCacheNeedEOXActWork;

    struct tupleDesc *m_pgclassdesc;
    struct tupleDesc *m_pgindexdesc;

    /*
     * BucketMap Cache, consists of a list of BucketMapCache element.
     * Location information of every rel cache is actually pointed to these list
     * members.
     * Attention: we need to invalidate bucket map caches when accepting
     * SI messages of tuples in PGXC_GROUP or SI reset messages!
     */
    List *g_bucketmap_cache;
    uint32 max_bucket_map_size;

    struct tupleDesc **EOXactTupleDescArray;
    int NextEOXactTupleDescNum;
    int EOXactTupleDescArrayLen; 
private:
    bool m_is_inited;
    bool m_is_inited_phase2;
    bool m_is_inited_phase3;

    GlobalTabDefCache *m_global_tabdefcache;
    GlobalTabDefCache *m_global_shared_tabdefcache;

    struct HTAB *PartRelCache;
};

#endif