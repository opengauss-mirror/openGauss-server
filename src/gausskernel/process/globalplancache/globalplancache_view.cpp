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
* globalplancache_view.cpp
*    global plan cache
*
* IDENTIFICATION
*     src/gausskernel/process/globalplancache/globalplancache_view.cpp
*
* -------------------------------------------------------------------------
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
#include "utils/memutils.h"
#include "utils/plancache.h"
#include "utils/syscache.h"

/*
* @Description: get global plan cache info from hashtable
* @in num: the number of hash entry
* @return - void
*/
void *GlobalPlanCache::GetStatus(uint32 *num)
{
    int rc = EOK;
    HASH_SEQ_STATUS hash_seq;

    for (int i = 0; i < NUM_GPC_PARTITIONS; i++) {
        (void)LWLockAcquire(GetMainLWLockByIndex(FirstGPCMappingLock + i), LW_SHARED);
    }

    *num = 0;

    GPCEntry *entry = NULL;
    GPCEnv *env = NULL;
    CachedPlanSource *ps = NULL;
    
    hash_seq_init(&hash_seq, m_global_plan_cache);
    while ((entry = (GPCEntry*)hash_seq_search(&hash_seq)) != NULL) {
        *num = (*num) + (uint32)entry->cachedPlans->length;
    }

    if (m_gpc_invalid_plansource != NULL) {
        *num = (*num) + m_gpc_invalid_plansource->length;
    }

    if ((*num) == 0) {
        for (int i = NUM_GPC_PARTITIONS - 1; i >= 0; i--) {
            LWLockRelease(GetMainLWLockByIndex(FirstGPCMappingLock + i));
        }
        return NULL;
    }

    GPCStatus *stat_array =
        (GPCStatus*) palloc0(*num * sizeof(GPCStatus));

    hash_seq_init(&hash_seq, m_global_plan_cache);

    uint32 index = 0;
    while ((entry = (GPCEntry*)hash_seq_search(&hash_seq)) != NULL) {
        int numCachedPlans = entry->cachedPlans->length;
        DListCell *cell = entry->cachedPlans->head;

        for (int i = 0; i < numCachedPlans; i++) {
            Assert(cell != NULL);

            env = (GPCEnv *) cell->data.ptr_value;
            if (env && env->plansource) {
                ps = env->plansource;
                
                size_t len = strlen(ps->query_string) + 1;
                stat_array[index].query = (char *)palloc0(sizeof(char) * len);

                rc = memcpy_s(stat_array[index].query, len, ps->query_string, len);
                securec_check(rc, "\0", "\0");

                stat_array[index].refcount = ps->gpc.refcount;
                stat_array[index].valid = ps->gpc.is_valid;
                stat_array[index].DatabaseID = env->database_id;
                stat_array[index].schema_name = (char *)palloc0(sizeof(char) * NAMEDATALEN);
                rc = memcpy_s(stat_array[index].schema_name, NAMEDATALEN, env->schema_name, NAMEDATALEN);
                securec_check(rc, "\0", "\0");
                stat_array[index].params_num = ps->num_params;

                index++;
            } else {
                Assert (0);
            }
        }
    }

    if (m_gpc_invalid_plansource != NULL) {
        DListCell *cell = m_gpc_invalid_plansource->head;

        while (cell != NULL) {
            CachedPlanSource *curr = (CachedPlanSource *)cell->data.ptr_value;

            size_t len = strlen(curr->query_string) + 1;
            stat_array[index].query = (char *)palloc0(sizeof(char) * len);

            rc = memcpy_s(stat_array[index].query, len, curr->query_string, len);
            securec_check(rc, "\0", "\0");

            stat_array[index].refcount = curr->gpc.refcount;
            stat_array[index].valid = curr->gpc.is_valid;
            stat_array[index].DatabaseID = 0;
            stat_array[index].schema_name = "";
            stat_array[index].params_num = 0;

            index++;
            cell = cell->next;
        }
    }

    Assert (index == *num);

    for (int i = NUM_GPC_PARTITIONS - 1; i >= 0; i--) {
        LWLockRelease(GetMainLWLockByIndex(FirstGPCMappingLock + i));
    }

    return stat_array;
}

/*
* @Description: get prepare statement info from prepare hashtable
* @in num: the number of hash entry
* @return - void
*/
void *GlobalPlanCache::GetPrepareStatus(uint32 *num)
{
    int rc = EOK;
    HASH_SEQ_STATUS hash_seq;

    for (int i = 0; i < NUM_GPC_PARTITIONS; i++) {
        (void)LWLockAcquire(GetMainLWLockByIndex(FirstGPCPrepareMappingLock + i), LW_SHARED);
    }

    GPCPreparedStatement *entry = NULL;
    DList *prepare_list = NULL;

    hash_seq_init(&hash_seq, m_global_prepared);

    /* Get the row numbers. */
    while ((entry = (GPCPreparedStatement*)hash_seq_search(&hash_seq)) != NULL) {
        if (entry->prepare_statement_list) {
            *num = *num + (uint32)entry->prepare_statement_list->length;
        } else {
            Assert(0);
        }
    }

    if ((*num) == 0) {
        for (int i = NUM_GPC_PARTITIONS - 1; i >= 0; i--) {
            LWLockRelease(GetMainLWLockByIndex(FirstGPCPrepareMappingLock + i));
        }

        return NULL;
    }

    GPCPrepareStatus *stat_array = (GPCPrepareStatus*) palloc0(*num * sizeof(GPCPrepareStatus));

    hash_seq_init(&hash_seq, m_global_prepared);

    uint32 index = 0;
    /* Get all real time session statistics from the register info hash table. */
    while ((entry = (GPCPreparedStatement*)hash_seq_search(&hash_seq)) != NULL) {
        prepare_list = (DList *) entry->prepare_statement_list;
        if (prepare_list->length != 0) {
            DListCell* iter = prepare_list->head;
            for (; iter != NULL; iter = iter->next) {
                PreparedStatement *prepare_statement = (PreparedStatement *)(iter->data.ptr_value);

                size_t len = strlen(prepare_statement->stmt_name) + 1;
                stat_array[index].statement_name = (char *)palloc0(sizeof(char) * len);
                rc = memcpy_s(stat_array[index].statement_name, len, prepare_statement->stmt_name, len);
                securec_check(rc, "\0", "\0");
                stat_array[index].refcount = prepare_statement->plansource->gpc.refcount;
                stat_array[index].global_session_id = (int)entry->global_sess_id;
                stat_array[index].is_shared = prepare_statement->plansource->gpc.is_share;
                index++;
            } 
        }
    }
    Assert (index == *num);

    for (int i = NUM_GPC_PARTITIONS - 1; i >= 0; i--) {
        LWLockRelease(GetMainLWLockByIndex(FirstGPCPrepareMappingLock + i));
    }

    return stat_array;
}

/*
 * @Description: Clean all the global plancaches which refcount is 0.
 * This function only be called when user call the global_plancache_clean() by themselves.
 * We do not choose to clean these plancaches auto due to the fact that delete the plancaches from HTAB
 * requires an exclusive lock which will cause the decrease of performance.
 * @in num: void
 * @return - Datum
 */
Datum GlobalPlanCache::PlanClean()
{
    DListCell *cell = NULL;

    for (uint32 currBucket = 0; currBucket < GPC_NUM_OF_BUCKETS; currBucket++) {
        int bucketEntriesCount = gs_atomic_add_32(&(m_gpc_bucket_info_array[currBucket].entries_count), 0);
        if (bucketEntriesCount == 0) {
            continue;
        }
        /* Ok so bucket is not empty. Get the bucket S-lock so we can iterate through it. */
        int partitionLock = (int) (FirstGPCMappingLock + currBucket);
        (void)LWLockAcquire(GetMainLWLockByIndex(partitionLock), LW_EXCLUSIVE);
        /* Check the number of entries in the bucket again. 
         * GPC Eviction might have removed the last entry while we were waiting for the shared lock. */
        bucketEntriesCount = gs_atomic_add_32(&(m_gpc_bucket_info_array[currBucket].entries_count), 0);
        if (bucketEntriesCount == 0) {
            LWLockRelease(GetMainLWLockByIndex(partitionLock));
            continue;
        }
        HASH_SEQ_STATUS hash_seq;
        GPCEntry *entry = NULL;
        GPCEnv *env = NULL;

        hash_seq_init(&hash_seq, m_global_plan_cache);
        hash_seq.curBucket = currBucket;
        for (int entryIndex = 0; entryIndex < bucketEntriesCount; entryIndex++){
            entry = (GPCEntry*)hash_seq_search(&hash_seq);
            int numCachedPlans = entry->cachedPlans->length;
            cell = entry->cachedPlans->head;

            for (int i = 0; i < numCachedPlans; i++) {
                Assert(cell != NULL);
                DListCell *next_cell = cell->next;
                env = (GPCEnv *) cell->data.ptr_value;
                if (env->plansource->gpc.refcount == 0) {
                    env->globalplancacheentry->cachedPlans = dlist_delete_cell(env->globalplancacheentry->cachedPlans, 
                                                                               cell, false);
                    env->plansource = NULL;
                    MemoryContextDelete(env->context);
                    (void)gs_atomic_add_32(&entry->refcount, -1);
                }
                cell = next_cell;
            }

            if (entry->refcount == 0) {
                /* Remove the GPC entry */
                Assert(entry->cachedPlans == NULL);

                bool found = false;
                (void *)hash_search(m_global_plan_cache, (void *) &(entry->key), HASH_REMOVE, &found);
                Assert(true == found);
                uint32 gpc_bucket_index = (uint32)(entry->lockId - FirstGPCMappingLock);
                m_gpc_bucket_info_array[gpc_bucket_index].entries_count--;
                entry->magic = 0;
                pfree((void *)entry->key.query_string);
            }
        }
        if (entry) {
            hash_seq_term(&hash_seq);
        }
        LWLockRelease(GetMainLWLockByIndex(partitionLock));
    }
    LWLockAcquire(GPCClearLock, LW_EXCLUSIVE);
    if (m_gpc_invalid_plansource != NULL) {
        cell = m_gpc_invalid_plansource->head;
        while (cell != NULL) {
            CachedPlanSource *curr = (CachedPlanSource *)cell->data.ptr_value;
            if (curr->gpc.refcount == 0) {
                DListCell *next = cell->next;
                m_gpc_invalid_plansource = dlist_delete_cell(m_gpc_invalid_plansource, cell, false);
                DropCachedPlanInternal(curr);
                curr->magic = 0;
                MemoryContextDelete(curr->context);
                cell = next;
            } else {
                cell = cell->next;
            }
        }
    }
    LWLockRelease(GPCClearLock);

    PG_RETURN_BOOL(true);
}

/*
 * @Description: System function global_plancache_clean() entry
 * @in num: PG_FUNCTION_ARGS, expected NULL
 * @return - DATUM
 */
Datum GPCPlanClean(PG_FUNCTION_ARGS) 
{
#ifndef ENABLE_MULTIPLE_NODES
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
#endif

    (void)GPC->PlanClean();

    PG_RETURN_BOOL(true);
}
