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
 * knl_localsysdbcache.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/utils/knl_localsysdbcache.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef KNL_LOCALSYSDBCACHE_H
#define KNL_LOCALSYSDBCACHE_H
#include "utils/knl_localsystabcache.h"
#include "utils/knl_localtabdefcache.h"
#include "utils/knl_localpartdefcache.h"
#include "knl/knl_session.h"

void ReLoadLSCWhenWaitMission();

void ReleaseAllGSCRdConcurrentLock();

void RememberRelSonMemCxtSpace(Relation rel);
void ForgetRelSonMemCxtSpace(Relation rel);

bool CheckMyDatabaseMatch();

bool IsGotPoolReload();
void ResetGotPoolReload(bool value);

bool DeepthInAcceptInvalidationMessageNotZero();
void ResetDeepthInAcceptInvalidationMessage(int value);

void CloseLocalSysDBCache();

extern void CreateLocalSysDBCache();

extern MemoryContext LocalSharedCacheMemCxt();
extern MemoryContext LocalMyDBCacheMemCxt();
extern MemoryContext LocalGBucketMapMemCxt();
extern MemoryContext LocalSmgrStorageMemoryCxt();

extern bool EnableGlobalSysCache();

#if defined(USE_ASSERT_CHECKING) && !defined(ENABLE_MEMORY_CHECK)
extern void CloseLSCCheck();
#endif

knl_u_inval_context *GetInvalCxt();
knl_u_relmap_context *GetRelMapCxt();

struct HTAB *GetTypeCacheHash();
struct HTAB *GetTableSpaceCacheHash();
struct HTAB *GetSMgrRelationHash();

struct vfd *GetVfdCache();
struct vfd **GetVfdCachePtr();
void SetVfdCache(vfd *value);
Size GetSizeVfdCache();
Size *GetSizeVfdCachePtr();
void SetSizeVfdCache(Size value);
int GetVfdNfile();
void AddVfdNfile(int n);

dlist_head *getUnownedReln();

extern void AtEOXact_SysDBCache(bool is_commit);

struct BadPtrObj : public BaseObject {
    int nbadptr;
    void **bad_ptr_lists;
    int maxbadptr;
    BadPtrObj()
    {
        ResetInitFlag();
    }
    void ResetInitFlag()
    {
        nbadptr = 0;
        bad_ptr_lists = NULL;
        maxbadptr = 0;
    }
};

const int MAX_GSC_READLOCK_COUNT = 16;
struct GSCRdLockInfo {
    int count;
    bool *has_concurrent_lock[MAX_GSC_READLOCK_COUNT];
    pthread_rwlock_t *concurrent_lock[MAX_GSC_READLOCK_COUNT];
};

enum LscInitStatus {
    LscNotInit,
    LscIniting,
    LscInitfinished
};

class LocalSysDBCache : public BaseObject {
public:
    LocalSysDBCache();

    GlobalSysTabCache *GetGlobalSysTabCache()
    {
        if (m_global_db == NULL) {
            InitDBRef();
        }
        return m_global_db->m_systabCache;
    }

    GlobalTabDefCache *GetGlobalTabDefCache()
    {
        if (m_global_db == NULL) {
            InitDBRef();
        }
        return m_global_db->m_tabdefCache;
    }

    GlobalPartDefCache *GetGlobalPartDefCache()
    {
        if (m_global_db == NULL) {
            InitDBRef();
        }
        return m_global_db->m_partdefCache;
    }

    GlobalSysTabCache *GetSharedSysTabCache()
    {
        return m_shared_global_db->m_systabCache;
    }
    GlobalTabDefCache *GetSharedTabDefCache()
    {
        return m_shared_global_db->m_tabdefCache;
    }

    struct GlobalSysDBCacheEntry *GetMyGlobalDBEntry()
    {
        return m_global_db;
    }

    struct GlobalSysDBCacheEntry *GetSharedGlobalDBEntry()
    {
        return m_shared_global_db;
    }

    void InitRelMapPhase2();
    void InitRelMapPhase3();
    void LoadRelMapFromGlobal(bool shared);
    void InvalidateGlobalRelMap(bool shared, Oid db_id, RelMapFile* real_map);

    void LocalSysDBCacheReSet();
    void ClearSysCacheIfNecessary(Oid db_id, const char *db_name);
    void CloseLocalSysDBCache();
    void CreateDBObject();
    void InitThreadDatabase(Oid db_id, const char *db_name, Oid db_tabspc);
    void InitSessionDatabase(Oid db_id, const char *db_name, Oid db_tabspc);
    void InitDatabasePath(const char *db_path);

    bool LocalSysDBCacheNeedSwapOut();
    void SetThreadDefExclusive(bool is_exclusive);
    bool GetThreadDefExclusive()
    {
        return m_is_def_exclusive;
    }

    void LocalSysDBCacheReBuild();
    bool LocalSysDBCacheNeedReBuild();
    void LocalSysDBCacheReleaseGlobalReSource(bool is_commit);

    LocalSysTabCache systabcache;
    LocalTabDefCache tabdefcache;
    LocalPartDefCache partdefcache;

    Oid my_database_id;
    char my_database_name[NAMEDATALEN];
    char *my_database_path;
    Oid my_database_tablespace;

    knl_u_inval_context inval_cxt;
    knl_u_relmap_context relmap_cxt;
    /* Hash table for information about each tablespace */
    struct HTAB *TableSpaceCacheHash;
    struct HTAB *TypeCacheHash;
    struct HTAB *SMgrRelationHash;

    struct vfd *VfdCache;
    Size SizeVfdCache;
    /* Number of file descriptors known to be in use by VFD entries. */
    int nfile;
    dlist_head unowned_reln;

    MemoryContext lsc_top_memcxt;
    MemoryContext lsc_share_memcxt;
    MemoryContext lsc_mydb_memcxt;
    /* mark whether we have loaded syscache */
    LscInitStatus init_status;
    void ResetInitStatus()
    {
        init_status = LscNotInit;
    }
    void StartInit()
    {
        init_status = LscIniting;
    }
    void FinishInit()
    {
        init_status = LscInitfinished;
    }

    bool recovery_finished;

    /* used to record multi palloc, unused for now */
    BadPtrObj bad_ptr_obj;
    /* mark lsc close flag, never query lsc if is_closed == true */
    bool is_closed;
    /* mark pgxcpoolreload flag, flush relcache's locatorinfo->nodelist */
    bool got_pool_reload;
    /* record rel's index and rule cxt */
    int64 rel_index_rule_space;
    /* record abort count, which may cause memory leak
     * it seems fmgr_info_cxt rule_cxt rls_cxt index_cxt has no resowner to avoid mem leak */
    uint64 abort_count;
    /* record concurrent rd locks on syscache */
    GSCRdLockInfo rdlock_info;
    double cur_swapout_ratio;
private:
    void InitDBRef();
    void CreateCatBucket();
    void LocalSysDBCacheClearMyDB();
    bool LocalSysDBCacheNeedClearMyDB(Oid db_id, const char *db_name);
    bool DBNotMatch(Oid db_id, const char *db_name);
    bool DBStandbyChanged();
    bool LockAndAttachDBFailed();
    void FixWrongCacheStat(Oid db_id, Oid db_tabspc);

    void LocalSysDBCacheCleanCache();
    bool LocalSysDBCacheNeedCleanCache();

    void LocalSysDBCacheReleaseCritialReSource(bool include_shared);
    void LocalSysDBCacheResetMyDBStat();
    void SetDatabaseName(const char *db_name);
    
    struct GlobalSysDBCacheEntry *m_global_db;
    struct GlobalSysDBCacheEntry *m_shared_global_db;
    bool is_lsc_catbucket_created;
    /* mark me a special thread like stream_worker and bgworker
     * or mark I started a spacial thread */
    bool m_is_def_exclusive;
};
extern void AppendBadPtr(void *elem);
extern void RemoveBadPtr(void *elem);


#define LOCAL_SYSDB_RESOWNER  \
    (t_thrd.utils_cxt.CurrentResourceOwner == NULL ? t_thrd.lsc_cxt.local_sysdb_resowner : \
        t_thrd.utils_cxt.CurrentResourceOwner)
#endif