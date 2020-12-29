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
 *---------------------------------------------------------------------------------------
 *
 * globalpreparestmt.cpp
 *
 *        global prepare statement
 *
 * IDENTIFICATION
 *        /src/common/backend/utils/cache/globalpreparestmt.cpp
 *
 *---------------------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/hash.h"
#include "access/xact.h"
#include "catalog/pgxc_node.h"
#include "commands/prepare.h"
#include "optimizer/nodegroups.h"
#include "pgxc/groupmgr.h"
#include "pgxc/pgxcnode.h"
#include "utils/dynahash.h"
#include "utils/globalplancache.h"
#include "utils/globalpreparestmt.h"
#include "utils/memutils.h"
#include "utils/plancache.h"
#include "utils/syscache.h"
#include "utils/plancache.h"


uint32 GPCPrepareHashFunc(const void *key, Size keysize)
{
    return DatumGetUInt32(hash_any((const unsigned char *)key, keysize));
}

int GPCPrepareHashMatch(const void *left, const void *right, Size keysize)
{
    if (((sess_orient *)left)->cn_sessid != ((sess_orient *)right)->cn_sessid) {
        return 1;
    } else if (((sess_orient *)left)->cn_timeline != ((sess_orient *)right)->cn_timeline){
        return 1;
    } else if (((sess_orient *)left)->cn_nodeid != ((sess_orient *)right)->cn_nodeid){
        return 1;
    }

    return 0;
}


GlobalPrepareStmt::GlobalPrepareStmt()
{
    Init();
}

DListCell* GPCFetchStmtInList(DList* target, const char* stmt) {
    Assert(target != NULL);
    DListCell* iter = target->head;
    for (; iter != NULL; iter = iter->next) {
        PreparedStatement *prepare_statement = (PreparedStatement *)(iter->data.ptr_value);
        if (strcmp(stmt, prepare_statement->stmt_name) == 0)
            return iter;
    }
    return NULL;
}

/* init the HTAB which stores the prepare statements refer to global plancache */
void GlobalPrepareStmt::Init()
{
    HASHCTL     hash_ctl;
    errno_t     rc = 0;

    rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
    securec_check(rc, "\0", "\0");

    hash_ctl.keysize = sizeof(sess_orient);
    hash_ctl.entrysize = sizeof(GPCPreparedStatement);
    hash_ctl.hash = (HashValueFunc)GPCPrepareHashFunc;
    hash_ctl.match = (HashCompareFunc)GPCPrepareHashMatch;

    int flags = HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION | HASH_COMPARE | HASH_EXTERN_CONTEXT;

    m_array = (GPCHashCtl *) MemoryContextAllocZero(g_instance.cache_cxt.global_cache_mem,
                                                                      sizeof(GPCHashCtl) * GPC_NUM_OF_BUCKETS);

    for (uint32 i = 0; i < GPC_NUM_OF_BUCKETS; i++) {
        m_array[i].count = 0;
        m_array[i].lockId = FirstGPCPrepareMappingLock + i;

        m_array[i].context = AllocSetContextCreate(g_instance.cache_cxt.global_cache_mem,
                                                                   "GPC_Prepare_Bucket_Context",
                                                                   ALLOCSET_DEFAULT_MINSIZE,
                                                                   ALLOCSET_DEFAULT_INITSIZE,
                                                                   ALLOCSET_DEFAULT_MAXSIZE,
                                                                   SHARED_CONTEXT);

        hash_ctl.hcxt = m_array[i].context;
        m_array[i].hash_tbl = hash_create("Global Prepared Queries",
                                                               GPC_HTAB_SIZE,
                                                               &hash_ctl,
                                                               flags);
    }
    InitCnTimelineHTAB();
}

void GlobalPrepareStmt::InitCnTimelineHTAB()
{
    HASHCTL ctl_func;
    errno_t rc;
    rc = memset_s(&ctl_func, sizeof(ctl_func), 0, sizeof(ctl_func));
    securec_check_c(rc, "\0", "\0");

    ctl_func.keysize = sizeof(uint32);
    ctl_func.entrysize = sizeof(TimelineEntry);
    ctl_func.hcxt = g_instance.cache_cxt.global_cache_mem;
    m_cn_timeline =
        hash_create("cn_timeline", 64, &ctl_func, HASH_ELEM | HASH_CONTEXT);

}
void GlobalPrepareStmt::Store(const char *stmt_name,
                       CachedPlanSource *plansource,
                       bool from_sql,
                       bool is_share)
{
    GPC_LOG("store prestmt", plansource, stmt_name);

    GPCPreparedStatement *entry = NULL;
    TimestampTz cur_ts = GetCurrentStatementStartTimestamp();
    bool        found = false;

    sess_orient* key = &u_sess->sess_ident;
    uint32 hashCode = GPCPrepareHashFunc(key, sizeof(sess_orient));
    uint32 bucket_id = GetBucket(hashCode);
    Assert (bucket_id >= 0 && bucket_id < GPC_NUM_OF_BUCKETS);
    int lock_id = m_array[bucket_id].lockId;
    LWLockAcquire(GetMainLWLockByIndex(lock_id), LW_EXCLUSIVE);
    MemoryContext old_cxt = MemoryContextSwitchTo(m_array[bucket_id].context);

    /* Add entry to hash table */
    entry = (GPCPreparedStatement *) hash_search(m_array[bucket_id].hash_tbl,
                                                 (const void*)(key),
                                                 HASH_ENTER,
                                                 &found);

    /* Shouldn't get a duplicate entry */
    if (found && entry->prepare_statement_list != NULL) {
        DListCell* find = GPCFetchStmtInList(entry->prepare_statement_list, stmt_name);
        if (find != NULL) {
            PreparedStatement *pstmt = (PreparedStatement *)(find->data.ptr_value);
            CachedPlanSource *old_plansource = ((PreparedStatement *)(find->data.ptr_value))->plansource;
            if (plansource != old_plansource) {
                GPC_LOG("wrong prepare statement", 0, 0);
                pstmt->plansource = plansource;
                if (old_plansource->gpc.status.InShareTable()) {
                    old_plansource->gpc.status.SubRefCount();
                } else {
                    GPC_LOG("Drop plancache when store", plansource, stmt_name);
                    DropCachedPlan(old_plansource);
                }
                if (!plansource->gpc.status.InShareTable()) {
                    Assert (!plansource->is_support_gplan || plansource->gpc.status.IsSharePlan());
                    plansource->gpc.status.SetLoc(GPC_SHARE_IN_LOCAL_SAVE_PLAN_LIST);
                    SaveCachedPlan(plansource);
                }
            } else if (is_share == true) {
                plansource->gpc.status.SubRefCount();
            }

            GPC_LOG("global prestmt already exist", plansource, stmt_name);

            MemoryContextSwitchTo(old_cxt);
            LWLockRelease(GetMainLWLockByIndex(lock_id));
            return ;
        }
    }
    if (found == false) {
        entry->prepare_statement_list = NULL;
        m_array[bucket_id].count++;
        entry->sess_detach = false;
        INSTR_TIME_SET_CURRENT(entry->last_used_time);
    }
    PreparedStatement *stmt = (PreparedStatement*)palloc(sizeof(PreparedStatement));

    stmt->plansource = plansource;
    stmt->from_sql = from_sql;
    stmt->prepare_time = cur_ts;
    int rc = memcpy_s(stmt->stmt_name, NAMEDATALEN, stmt_name, NAMEDATALEN);
    securec_check(rc, "", "");

    entry->prepare_statement_list = dlappend(entry->prepare_statement_list, (void*)stmt);

    if (is_share == false) {
        Assert (!plansource->is_support_gplan || plansource->gpc.status.IsSharePlan());
        plansource->gpc.status.SetLoc(GPC_SHARE_IN_LOCAL_SAVE_PLAN_LIST);
        SaveCachedPlan(plansource);
    }

    MemoryContextSwitchTo(old_cxt);
    LWLockRelease(GetMainLWLockByIndex(lock_id));
    GPC_LOG("store prestmt succ ", plansource, stmt_name);
}

PreparedStatement *GlobalPrepareStmt::Fetch(const char *stmt_name, bool throwError)
{
    GPCPreparedStatement *entry = NULL;
    sess_orient* key = &u_sess->sess_ident;
    GPCCheckGuc();
    uint32 hashCode = GPCPrepareHashFunc(key, sizeof(sess_orient));

    uint32 bucket_id = GetBucket(hashCode);
    Assert (bucket_id >= 0 && bucket_id < GPC_NUM_OF_BUCKETS);
    int lock_id = m_array[bucket_id].lockId;
    LWLockAcquire(GetMainLWLockByIndex(lock_id), LW_SHARED);
    MemoryContext old_cxt = MemoryContextSwitchTo(m_array[bucket_id].context);

    entry = (GPCPreparedStatement *) hash_search(m_array[bucket_id].hash_tbl,
                                                 (const void*)(key),
                                                 HASH_FIND,
                                                 NULL);

    if (entry == NULL) {
        if (throwError == true) {
            MemoryContextSwitchTo(old_cxt);
            LWLockRelease(GetMainLWLockByIndex(lock_id));
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_PSTATEMENT),
                     errmsg("global prepared statement \"%s\" does not exist in PrepareFetch GPC (%lu,%u,%u) dn session_id:%lu",
                            stmt_name, u_sess->sess_ident.cn_sessid, u_sess->sess_ident.cn_timeline, u_sess->sess_ident.cn_nodeid, u_sess->session_id)));
        } else {
            MemoryContextSwitchTo(old_cxt);
            LWLockRelease(GetMainLWLockByIndex(lock_id));
            return NULL;
        }
    } else {
        entry->sess_detach = false;
        INSTR_TIME_SET_CURRENT(entry->last_used_time);
    }

    DListCell* result = GPCFetchStmtInList(entry->prepare_statement_list, stmt_name);
    if (result == NULL) {
        MemoryContextSwitchTo(old_cxt);
        LWLockRelease(GetMainLWLockByIndex(lock_id));
        return NULL;
    }

    MemoryContextSwitchTo(old_cxt);
    LWLockRelease(GetMainLWLockByIndex(lock_id));
    return (PreparedStatement *)(result->data.ptr_value);
}

void GlobalPrepareStmt::Drop(const char *stmt_name, bool showError)
{

    GPC_LOG("prepare remove", 0, stmt_name);

    /* Find the query's hash table entry; raise error if wanted */
    GPCPreparedStatement *entry = NULL;
    sess_orient* key = &u_sess->sess_ident;
    uint32 hashCode = GPCPrepareHashFunc(key, sizeof(sess_orient));
    uint32 bucket_id = GetBucket(hashCode);
    Assert (bucket_id >= 0 && bucket_id < GPC_NUM_OF_BUCKETS);
    int lock_id = m_array[bucket_id].lockId;
    LWLockAcquire(GetMainLWLockByIndex(lock_id), LW_EXCLUSIVE);
    MemoryContext old_cxt = MemoryContextSwitchTo(m_array[bucket_id].context);

    entry = (GPCPreparedStatement *) hash_search(m_array[bucket_id].hash_tbl,
                                                 (const void*)(key),
                                                 HASH_FIND,
                                                 NULL);
    if (entry == NULL) {
        MemoryContextSwitchTo(old_cxt);
        LWLockRelease(GetMainLWLockByIndex(lock_id));
        return;
    }

#ifdef __aarch64__
    if (unlikely(entry->prepare_statement_list == NULL)) {
        GPC_LOG("drop remove entry prepare statement list null ", 0, stmt_name);
        /* Now we can remove the hash table entry */
        hash_search(m_array[bucket_id].hash_tbl, (const void*)(key), HASH_REMOVE, NULL);
        m_array[bucket_id].count--;
        MemoryContextSwitchTo(old_cxt);
        LWLockRelease(GetMainLWLockByIndex(lock_id));
        return;
    }
#endif

    DListCell* result = GPCFetchStmtInList(entry->prepare_statement_list, stmt_name);

    if (result == NULL) {
        MemoryContextSwitchTo(old_cxt);
        LWLockRelease(GetMainLWLockByIndex(lock_id));
        return;
    }

    PreparedStatement *target = (PreparedStatement *)(result->data.ptr_value);
    if (target != NULL) {
        if (target->plansource->gpc.status.InShareTable()) {
            GPC_LOG("prepare remove success", 0, stmt_name);
            target->plansource->gpc.status.SubRefCount();
        } else {
            Assert (!target->plansource->is_support_gplan || target->plansource->gpc.status.IsSharePlan());
            GPC_LOG("prepare remove private", target->plansource, stmt_name);
            DropCachedPlan(target->plansource);
            GPC_LOG("prepare remove private succ", 0, stmt_name);
        }
        entry->prepare_statement_list = dlist_delete_cell(entry->prepare_statement_list, result, true);
    }

    if (entry->prepare_statement_list == NULL) {
        GPC_LOG("prepare remove entry", 0, stmt_name);
        /* Now we can remove the hash table entry */
        hash_search(m_array[bucket_id].hash_tbl, (const void*)(key), HASH_REMOVE, NULL);
        m_array[bucket_id].count--;
    }

    MemoryContextSwitchTo(old_cxt);
    LWLockRelease(GetMainLWLockByIndex(lock_id));
}
/*
 * @Description: Drop ALL global plancaches which belongs to current session.
 * This function only be called before the session aborts.
 * @in num: uint64 global_sess_id ,boolean need_lock
 * @return - void
 */
void GlobalPrepareStmt::DropAll(sess_orient* key, bool need_lock)
{
    /* Find the query's hash table entry; raise error if wanted */
    GPCPreparedStatement *entry = NULL;
    MemoryContext old_cxt = NULL;
    uint32 hashCode = GPCPrepareHashFunc(key, sizeof(sess_orient));
    uint32 bucket_id = GetBucket(hashCode);
    Assert (bucket_id >= 0 && bucket_id < GPC_NUM_OF_BUCKETS);
    int lock_id = m_array[bucket_id].lockId;
    if (need_lock) {
        LWLockAcquire(GetMainLWLockByIndex(lock_id), LW_EXCLUSIVE);
        old_cxt = MemoryContextSwitchTo(m_array[bucket_id].context);
    }
    entry = (GPCPreparedStatement *) hash_search(m_array[bucket_id].hash_tbl,
                                                  (const void*)key,
                                                  HASH_FIND,
                                                  NULL);
    if (entry == NULL) {
        if (need_lock) {
            LWLockRelease(GetMainLWLockByIndex(lock_id));
            MemoryContextSwitchTo(old_cxt);
        }
        return;
    }
#ifdef __aarch64__
        if (unlikely(entry->prepare_statement_list == NULL)) {
            GPC_LOG("dropall remove entry prepare statement list null", 0, 0);
            hash_search(m_array[bucket_id].hash_tbl, key, HASH_REMOVE, NULL);
            m_array[bucket_id].count--;
            if (need_lock) {
                LWLockRelease(GetMainLWLockByIndex(lock_id));
                MemoryContextSwitchTo(old_cxt);
            }
            return;
        }
#endif
    DListCell* iter = entry->prepare_statement_list->head;
    for (; iter != NULL; iter = iter->next) {
        PreparedStatement * target = (PreparedStatement *)(iter->data.ptr_value);
        if (target != NULL) {
            if (target->plansource->gpc.status.InShareTable()) {
                GPC_LOG("drop prepare key sub refcount", target->plansource, target->plansource->stmt_name);
                target->plansource->gpc.status.SubRefCount();
            } else {
                Assert (!target->plansource->is_support_gplan || target->plansource->gpc.status.IsSharePlan());
                GPC_LOG("drop prepare key clean private plansource", target->plansource, target->plansource->stmt_name);
                DropCachedPlan(target->plansource);
                GPC_LOG("drop prepare key clean private plansource succ", target->plansource, 0);
            }
        }
    }

    GPC_LOG("before drop prepare statement list on time", 0, 0);

    dlist_free(entry->prepare_statement_list, true);
    hash_search(m_array[bucket_id].hash_tbl, key, HASH_REMOVE, NULL);
    m_array[bucket_id].count--;
    GPC_LOG("after drop prepare statement list on time", 0, 0);
    if (need_lock) {
        LWLockRelease(GetMainLWLockByIndex(lock_id));
        MemoryContextSwitchTo(old_cxt);
    }

    g_instance.plan_cache->DropInvalid();
}

void GlobalPrepareStmt::Clean(uint32 cn_id)
{
    GPCPreparedStatement *entry = NULL;

    for (uint32 bucket_id = 0; bucket_id < GPC_NUM_OF_BUCKETS; bucket_id++) {
        int lock_id = m_array[bucket_id].lockId;
        LWLockAcquire(GetMainLWLockByIndex(lock_id), LW_EXCLUSIVE);
        MemoryContext old_cxt = MemoryContextSwitchTo(m_array[bucket_id].context);

        HASH_SEQ_STATUS hash_seq;
        hash_seq_init(&hash_seq, m_array[bucket_id].hash_tbl);

        while ((entry = (GPCPreparedStatement *)hash_seq_search(&hash_seq)) != NULL) {
            if (entry->sess_detach == true && cn_id == (uint32)(entry->key.cn_nodeid)) {
                    DropAll(&(entry->key), false);
            }
        }

        MemoryContextSwitchTo(old_cxt);
        LWLockRelease(GetMainLWLockByIndex(lock_id));
    }
}

void GlobalPrepareStmt::CheckTimeline()
{
    LWLockAcquire(GPCTimelineLock, LW_SHARED);
    TimelineEntry *entry = NULL;
    bool found = false;

    GPC_LOG("check time line", 0, 0);

    entry = (TimelineEntry *) hash_search(m_cn_timeline,
                                                   &u_sess->sess_ident.cn_nodeid,
                                                   HASH_FIND,
                                                   &found);

    if (found == false) {
        LWLockRelease(GPCTimelineLock);
        LWLockAcquire(GPCTimelineLock, LW_EXCLUSIVE);

        entry = (TimelineEntry *) hash_search(m_cn_timeline,
                                                       &u_sess->sess_ident.cn_nodeid,
                                                       HASH_ENTER,
                                                       &found);
        if (found == false) {
            entry->node_id = u_sess->sess_ident.cn_nodeid;
            entry->timeline = u_sess->sess_ident.cn_timeline;
        }
    } else {
        if (entry->timeline != u_sess->sess_ident.cn_timeline) {
            LWLockRelease(GPCTimelineLock);
            LWLockAcquire(GPCTimelineLock, LW_EXCLUSIVE);

            if (entry->timeline != u_sess->sess_ident.cn_timeline) {
                GPC_LOG("check timeline failed, build new one", 0, 0);
                Clean(u_sess->sess_ident.cn_nodeid);
                entry->timeline = u_sess->sess_ident.cn_timeline;
            }
        }
    }

    LWLockRelease(GPCTimelineLock);
}

void GlobalPrepareStmt::UpdateUseTime(sess_orient* key, bool sess_detach)
{
    GPCPreparedStatement *entry = NULL;
    uint32 hashCode = GPCPrepareHashFunc(key, sizeof(sess_orient));
    uint32 bucket_id = GetBucket(hashCode);
    Assert (bucket_id >= 0 && bucket_id < GPC_NUM_OF_BUCKETS);
    int lock_id = m_array[bucket_id].lockId;
    LWLockAcquire(GetMainLWLockByIndex(lock_id), LW_SHARED);
    entry = (GPCPreparedStatement *) hash_search(m_array[bucket_id].hash_tbl,
                                                 (const void*)(key),
                                                 HASH_FIND,
                                                 NULL);
    if (entry == NULL) {
        LWLockRelease(GetMainLWLockByIndex(lock_id));
        return;
    }
    entry->sess_detach = sess_detach;

    INSTR_TIME_SET_CURRENT(entry->last_used_time);
    LWLockRelease(GetMainLWLockByIndex(lock_id));
}

void GlobalPrepareStmt::PrepareCleanUpByTime(bool needCheckTime)
{
    GPCPreparedStatement *entry = NULL;


    for (uint32 bucket_id = 0; bucket_id < GPC_NUM_OF_BUCKETS; bucket_id++) {
        int lock_id = m_array[bucket_id].lockId;
        LWLockAcquire(GetMainLWLockByIndex(lock_id), LW_EXCLUSIVE);
        MemoryContext old_cxt = MemoryContextSwitchTo(m_array[bucket_id].context);

        HASH_SEQ_STATUS hash_seq;
        hash_seq_init(&hash_seq, m_array[bucket_id].hash_tbl);

        instr_time curr_time;
        INSTR_TIME_SET_CURRENT(curr_time);
        while ((entry = (GPCPreparedStatement *)hash_seq_search(&hash_seq)) != NULL) {
             if(!needCheckTime || (entry->sess_detach && (INSTR_TIME_GET_DOUBLE(curr_time) - INSTR_TIME_GET_DOUBLE(entry->last_used_time)) > MAX_PREPARE_WAIING_TIME)) {
                	DropAll(&(entry->key), false);
               }
        }

        MemoryContextSwitchTo(old_cxt);
        LWLockRelease(GetMainLWLockByIndex(lock_id));
    }

    g_instance.plan_cache->DropInvalid();

}

void GlobalPrepareStmt::CleanSessionGPC(knl_session_context* currentSession)
{
    CachedPlanSource* plansource = currentSession->pcache_cxt.first_saved_plan;
    while (plansource != NULL) {
        /*
         * When turing on the enable_global_plancache, there are some cases that
         * we cannot insert the plancache in the shared HTAB. Therefore, we have to
         * set the flag in_recreate to true which let the plancache can be recreated next
         * time.
         */
        CachedPlanSource* next_plansource = plansource->next_saved;
        plansource->is_valid = false;
        plansource->gpc.status.SetLoc(GPC_SHARE_IN_PREPARE_STATEMENT);
        plansource->gpc.status.SetStatus(GPC_INVALID);
        plansource->next_saved = NULL;
        plansource = next_plansource;
    }

    currentSession->pcache_cxt.first_saved_plan = NULL;
    currentSession->pcache_cxt.gpc_in_ddl = false;
    if (currentSession->sess_ident.cn_sessid != 0 && currentSession->sess_ident.cn_nodeid != 0) {
        UpdateUseTime(&u_sess->sess_ident, true);
    }
    currentSession->sess_ident.cn_sessid = 0;
    currentSession->sess_ident.cn_timeline = 0;
    currentSession->sess_ident.cn_nodeid = 0;
}

void*
GlobalPrepareStmt::GetStatus(uint32 *num)
{
    int rc = EOK;
    HASH_SEQ_STATUS hash_seq;

    for (int i = 0; i < NUM_GPC_PARTITIONS; i++) {
        LWLockAcquire(GetMainLWLockByIndex(FirstGPCPrepareMappingLock + i), LW_SHARED);
    }

    GPCPreparedStatement *entry = NULL;
    DList *prepare_list = NULL;

    for (uint32 bucket_id = 0; bucket_id < GPC_NUM_OF_BUCKETS; bucket_id++) {
        hash_seq_init(&hash_seq, m_array[bucket_id].hash_tbl);

        /*Get the row numbers.*/
        while ((entry = (GPCPreparedStatement*)hash_seq_search(&hash_seq)) != NULL) {
            if(entry->prepare_statement_list) {
                *num = *num + (uint32)entry->prepare_statement_list->length;
            }
        }
    }

    if ((*num) == 0) {
        for (int i = NUM_GPC_PARTITIONS - 1; i >= 0; i--) {
            LWLockRelease(GetMainLWLockByIndex(FirstGPCPrepareMappingLock + i));
        }

        return NULL;
    }

    uint32 index = 0;
    GPCPrepareStatus *stat_array = (GPCPrepareStatus*) palloc0(*num * sizeof(GPCPrepareStatus));

    for (uint32 bucket_id = 0; bucket_id < GPC_NUM_OF_BUCKETS; bucket_id++) {
        hash_seq_init(&hash_seq, m_array[bucket_id].hash_tbl);

        /*Get all real time session statistics from the register info hash table.*/
        while ((entry = (GPCPreparedStatement*)hash_seq_search(&hash_seq)) != NULL) {
            prepare_list = (DList *) entry->prepare_statement_list;
            if (prepare_list && prepare_list->length != 0) {
                DListCell* iter = prepare_list->head;
                for (; iter != NULL; iter = iter->next) {
                    PreparedStatement *prepare_statement = (PreparedStatement *)(iter->data.ptr_value);
                    uint32 len = (uint32)strlen(prepare_statement->stmt_name) + 1;
                    stat_array[index].statement_name = (char *)palloc0(sizeof(char) * len);
                    rc = memcpy_s(stat_array[index].statement_name, len, prepare_statement->stmt_name, len);
                    securec_check(rc, "\0", "\0");
                    len = (uint32)strlen(prepare_statement->plansource->query_string) + 1;
                    stat_array[index].query = (char *)palloc0(sizeof(char) * len);
                    rc = memcpy_s(stat_array[index].query, len, prepare_statement->plansource->query_string, len);
                    securec_check(rc, "\0", "\0");
                    stat_array[index].refcount = prepare_statement->plansource->gpc.status.GetRefCount();
                    stat_array[index].cn_sessid = entry->key.cn_sessid;
                    stat_array[index].cn_node_id = entry->key.cn_nodeid;
                    stat_array[index].cn_time_line = entry->key.cn_timeline;
                    stat_array[index].is_shared = prepare_statement->plansource->gpc.status.InShareTable();
                    index++;
                }
            }
        }
    }
    Assert (index == *num);
    *num = index;

    for (int i = NUM_GPC_PARTITIONS - 1; i >= 0; i--) {
        LWLockRelease(GetMainLWLockByIndex(FirstGPCPrepareMappingLock + i));
    }

    return stat_array;
}

