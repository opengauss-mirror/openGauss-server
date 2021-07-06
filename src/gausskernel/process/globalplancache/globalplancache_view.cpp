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
#include "opfusion/opfusion.h"
#include "optimizer/nodegroups.h"
#include "pgxc/groupmgr.h"
#include "pgxc/pgxcnode.h"
#include "nodes/print.h"
#include "utils/dynahash.h"
#include "utils/lsyscache.h"
#include "utils/globalplancache.h"
#include "utils/memutils.h"
#include "utils/plancache.h"
#include "utils/syscache.h"

/*
* @Description: get global plan cache info from hashtable
* @in num: the number of hash entry
* @return - void
*/
void*
GlobalPlanCache::GetStatus(uint32 *num)
{
    int rc = EOK;
    HASH_SEQ_STATUS hash_seq;

    for (int i = 0; i < NUM_GPC_PARTITIONS; i++) {
        LWLockAcquire(GetMainLWLockByIndex(FirstGPCMappingLock + i), LW_SHARED);
    }

    uint32 localnum = 0;

    GPCEntry *entry = NULL;
    CachedPlanSource *ps = NULL;

    for (uint32 bucket_id = 0; bucket_id < GPC_NUM_OF_BUCKETS; bucket_id ++) {
         localnum += hash_get_num_entries(m_array[bucket_id].hash_tbl);
    }

    LWLockAcquire(GPCClearLock, LW_SHARED);
    if (m_invalid_list != NULL) {
        localnum += m_invalid_list->length;
    }

    if (localnum == 0) {
        for (int i = NUM_GPC_PARTITIONS - 1; i >= 0; i--) {
            LWLockRelease(GetMainLWLockByIndex(FirstGPCMappingLock + i));
        }
        LWLockRelease(GPCClearLock);
        *num = localnum;
        return NULL;
    }

    GPCViewStatus *stat_array = (GPCViewStatus*) palloc0(localnum * sizeof(GPCViewStatus));
    uint32 index = 0;
    for (uint32 bucket_id = 0; bucket_id < GPC_NUM_OF_BUCKETS; bucket_id++) {
        hash_seq_init(&hash_seq, m_array[bucket_id].hash_tbl);

        while ((entry = (GPCEntry*)hash_seq_search(&hash_seq)) != NULL) {
            Assert(entry->val.plansource != NULL);
            ps = entry->val.plansource;
            int len = strlen(ps->query_string) + 1;
            stat_array[index].query = (char *)palloc0(sizeof(char) * len);
            rc = memcpy_s(stat_array[index].query, len, ps->query_string, len);
            securec_check(rc, "\0", "\0");
            char* maskQuery = maskPassword(stat_array[index].query);
            stat_array[index].query = (maskQuery != NULL) ? maskQuery : stat_array[index].query;
            stat_array[index].refcount = ps->gpc.status.GetRefCount();
            stat_array[index].valid = ps->gpc.status.IsValid();
            stat_array[index].DatabaseID = entry->key.env.plainenv.database_id;
            stat_array[index].schema_name = (char *)palloc0(sizeof(char) * NAMEDATALEN);
            rc = memcpy_s(stat_array[index].schema_name, NAMEDATALEN, entry->key.env.schema_name, NAMEDATALEN);
            securec_check(rc, "\0", "\0");
            stat_array[index].params_num = ps->num_params;
            stat_array[index].func_id = entry->key.spi_signature.func_oid;
            bool printPlan = u_sess->attr.attr_sql.Debug_print_plan && entry->val.plansource->gplan &&
                             u_sess->proc_cxt.MyDatabaseId == entry->key.env.plainenv.database_id;
            if (printPlan) {
                elog(LOG, "gpc query string: %s", stat_array[index].query);
                ListCell* lc = NULL;
                foreach (lc, entry->val.plansource->gplan->stmt_list) {
                    Node* st = NULL;
                    st = (Node*)lfirst(lc);
                    if (IsA(st, PlannedStmt)) {
                        elog_node_display(LOG, "gpc gplan", st, u_sess->attr.attr_sql.Debug_pretty_print);
                    }
                }
            }
            index++;
        }
    }

    if (m_invalid_list != NULL) {
        DListCell *cell = m_invalid_list->head;

        while (cell != NULL) {
            CachedPlanSource *curr = (CachedPlanSource *)cell->data.ptr_value;
            int len = strlen(curr->query_string) + 1;
            stat_array[index].query = (char *)palloc0(sizeof(char) * len);
            rc = memcpy_s(stat_array[index].query, len, curr->query_string, len);
            securec_check(rc, "\0", "\0");
            char* maskQuery = maskPassword(stat_array[index].query);
            stat_array[index].query = (maskQuery != NULL) ? maskQuery : stat_array[index].query;
            stat_array[index].refcount = curr->gpc.status.GetRefCount();
            stat_array[index].valid = curr->gpc.status.IsValid();
            stat_array[index].DatabaseID = 0;
            stat_array[index].schema_name = "";
            stat_array[index].params_num = 0;
            stat_array[index].func_id = curr->gpc.key->spi_signature.func_oid;
            index++;
            cell = cell->next;
        }
    }

    Assert(index == localnum);
    *num = localnum;

    for (int i = NUM_GPC_PARTITIONS - 1; i >= 0; i--) {
        LWLockRelease(GetMainLWLockByIndex(FirstGPCMappingLock + i));
    }
    LWLockRelease(GPCClearLock);

    return stat_array;
}

/*
* @Description: get prepare statement info from prepare hashtable
* @in num: the number of hash entry
* @return - void
*/
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

    for (uint32 bucket_id = 0; bucket_id < GPC_NUM_OF_BUCKETS; bucket_id ++)
    {
        int lock_id = m_array[bucket_id].lockId;
        LWLockAcquire(GetMainLWLockByIndex(lock_id), LW_EXCLUSIVE);
        /* Check the number of entries in the bucket again. 
         * GPC Eviction might have removed the last entry while we were waiting for the shared lock. */
        int bucketEntriesCount = m_array[bucket_id].count;
        if (0 == bucketEntriesCount) {
            LWLockRelease(GetMainLWLockByIndex(lock_id));
            continue;
        }
        HASH_SEQ_STATUS hash_seq;
        GPCEntry *entry = NULL;
        CachedPlanSource* cur = NULL;

        hash_seq_init(&hash_seq, m_array[bucket_id].hash_tbl);
        while ((entry = (GPCEntry*)hash_seq_search(&hash_seq)) != NULL) {
            cur = entry->val.plansource;
            if(cur->gpc.status.RefCountZero()) {
                bool found = false;
                DropCachedPlanInternal(cur);
                cur->magic = 0;
                hash_search(m_array[bucket_id].hash_tbl, (void *) &(entry->key), HASH_REMOVE, &found);
                MemoryContextUnSeal(cur->context);
                MemoryContextUnSeal(cur->query_context);
                if (cur->opFusionObj) {
                    OpFusion::DropGlobalOpfusion((OpFusion*)(cur->opFusionObj));
                }
                MemoryContextDelete(cur->context);
                m_array[bucket_id].count--;
            }
        }
        LWLockRelease(GetMainLWLockByIndex(lock_id));
    }
    LWLockAcquire(GPCClearLock, LW_EXCLUSIVE);
    if (m_invalid_list != NULL) {
        cell = m_invalid_list->head;
        while (cell != NULL) {
            CachedPlanSource *curr = (CachedPlanSource *)cell->data.ptr_value;
            if (curr->gpc.status.RefCountZero()) {
                DListCell *next = cell->next;
                m_invalid_list = dlist_delete_cell(m_invalid_list, cell, false);
                DropCachedPlanInternal(curr);
                curr->magic = 0;
                MemoryContextUnSeal(curr->context);
                MemoryContextUnSeal(curr->query_context);
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
    g_instance.plan_cache->PlanClean();

    PG_RETURN_BOOL(true);
}
