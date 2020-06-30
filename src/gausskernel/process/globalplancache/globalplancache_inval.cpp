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
 * globalplancache_inval.cpp
 *    global plan cache
 *
 * IDENTIFICATION
 *     src/gausskernel/process/globalplancache/globalplancache_inval.cpp
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

bool GlobalPlanCache::MsgCheck(const SharedInvalidationMessage *msg)
{
    if (msg->id >= 0) {
        if (msg->cc.id == PROCOID || msg->cc.id == NAMESPACEOID || msg->cc.id == OPEROID || msg->cc.id == AMOPOPID) {
            return true;
        }
    } else if (msg->id == SHAREDINVALRELCACHE_ID || msg->id == SHAREDINVALPARTCACHE_ID) {
        return true;
    }

    return false;
}

void GlobalPlanCache::LocalMsgCheck(GPCEntry * entry, int tot, const int *idx, const SharedInvalidationMessage *msgs)
{
    int num_cachedplans = gs_atomic_add_32(&entry->cachedPlans->length, 0);
    DListCell *cell = entry->cachedPlans->head;

    for (int i = 0; i < num_cachedplans; i++)
    {
        bool has_drop = false;
        GPCEnv *env = (GPCEnv *) cell->data.ptr_value;
        if (env && env->plansource)
        {
            Assert(env->plansource->magic == CACHEDPLANSOURCE_MAGIC);

            for (int j = 0; j < tot; j++) {
                const SharedInvalidationMessage *msg = &msgs[idx[j]];
                if (msg->id >= 0) {
                    if (msg->cc.dbId == u_sess->proc_cxt.MyDatabaseId || msg->cc.dbId == InvalidOid) {
                        if (msg->cc.id == PROCOID) {
                            CheckInvalItemDependency(env->plansource, msg->cc.id, msg->cc.hashValue);
                        } else if (msg->cc.id == NAMESPACEOID || msg->cc.id == OPEROID || msg->cc.id == AMOPOPID) {
                        ResetPlanCache(env->plansource);
                        }
                    }
                } else if (msg->id == SHAREDINVALRELCACHE_ID) {
                    if (msg->rc.dbId == u_sess->proc_cxt.MyDatabaseId || msg->rc.dbId == InvalidOid)
                    {
                        CheckRelDependency(env->plansource, msg->rc.relId);
                    }
                } else if (msg->id == SHAREDINVALPARTCACHE_ID) {
                    if (msg->pc.dbId == u_sess->proc_cxt.MyDatabaseId || msg->pc.dbId == InvalidOid) {
                        CheckRelDependency(env->plansource, msg->pc.partId);
                    }
                }
                if (env->plansource->gpc.in_revalidate == true) {
                    cell = cell->next;
                        PlanDrop(env);
                        has_drop = true;
                        break;
                }
            }

            if (has_drop == true) {
                continue;
            }
        }
        cell = cell->next;
    }
}

void GlobalPlanCache::InvalMsg(const SharedInvalidationMessage *msgs, int n)
{
    int *idx = (int *)palloc0(n * sizeof(int));
    int tot = 0;

    for (int i = 0; i < n; i++) {
        const SharedInvalidationMessage *msg = &msgs[i];

        if (MsgCheck(msg)) {
            idx[tot++] = i;
        }
    }

    if (tot == 0) {
        pfree_ext(idx);
        return ;
    }

    /* Go through each bucket in the GPC HTAB and do some invalidation depending on the GPCInvalInfo we got.*/
    for (uint32 currBucket = 0; currBucket < GPC_NUM_OF_BUCKETS; currBucket ++)
    {
        /* atomic read the number of entries in the bucket so we can skip it if it is empty.
        * This saves us a bit of time because we do not need to acquire the s-lock if the bucket is currently empty.
        */
        int bucketEntriesCount = gs_atomic_add_32(&(m_gpc_bucket_info_array[currBucket].entries_count), 0);
        if (0 == bucketEntriesCount)
        {
            continue;
        }
        
        /* Ok so bucket is not empty. Get the bucket S-lock so we can iterate through it. */
        int partitionLock = (int) (FirstGPCMappingLock + currBucket);
        (void)LWLockAcquire(GetMainLWLockByIndex(partitionLock), LW_EXCLUSIVE);
        
        /* Check the number of entries in the bucket again. 
        * GPC Eviction might have removed the last entry while we were waiting for the shared lock. */
        bucketEntriesCount = gs_atomic_add_32(&(m_gpc_bucket_info_array[currBucket].entries_count), 0);
        if (0 == bucketEntriesCount)
        {
            LWLockRelease(GetMainLWLockByIndex(partitionLock));
            continue;
        }
        
        /* Now iterate through the bucket up to bucketEntriesCount.
         * We are holding a shared lock so we're protected from delete. 
         * hash_seq_search does not like it when an entry is being removed while it's scanning the hash.
         * This is the main reason why we need to initialize a new hash sequence per bucket
         * instead of the entire hash because GPC eviction could come in and delete entries in the hash
         * while we scan through it. 
         */
        HASH_SEQ_STATUS hash_seq;
        hash_seq_init(&hash_seq, m_global_plan_cache);
        hash_seq.curBucket = currBucket;
        hash_seq.curEntry = NULL;
        GPCEntry *entry = NULL;

        for (int entryIndex = 0; entryIndex < bucketEntriesCount; entryIndex++)
        {
            entry = (GPCEntry *)hash_seq_search(&hash_seq);
            Assert(NULL != entry);
            Assert(partitionLock == entry->lockId);

            /* If entry->magic does not match GLOBALPLANCACHEKEY_MAGIC, then this entry is in the middle of insert
            *  and hash_seq_search picked it up. We cannot read that entry just yet.
            */
            if (GLOBALPLANCACHEKEY_MAGIC != entry->magic || NULL == entry->cachedPlans)
                continue;
            
            /* Atomic read the number of CachedEnvironment in this entry */
            LocalMsgCheck(entry, tot, idx, msgs);
        }

        /* hash_seq_search terminates the sequence if it reaches the last entry in the hash
         * ie: if it returns a NULL. So if it has not returned a NULL, we terminate it explicitly.
         */
        if (entry)
        {
            hash_seq_term(&hash_seq);
        }
        
        LWLockRelease(GetMainLWLockByIndex(partitionLock));
    }

    pfree_ext(idx);
}
