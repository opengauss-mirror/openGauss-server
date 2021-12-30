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

bool GlobalPlanCache::NeedDropEntryByLocalMsg(CachedPlanSource* plansource, int tot, const int *idx, const SharedInvalidationMessage *msgs)
{
    Oid database_id = plansource->gpc.key->env.plainenv.database_id;

    for (int j = 0; j < tot; j++) {
        const SharedInvalidationMessage *msg = &msgs[idx[j]];
        if ((plansource)->raw_parse_tree && IsA((plansource)->raw_parse_tree, TransactionStmt))
            continue;

        if (msg->id >= 0) {

            if (msg->cc.dbId == database_id || msg->cc.dbId == InvalidOid) {
                if (msg->cc.id == PROCOID) {
                    CheckInvalItemDependency(plansource, msg->cc.id, msg->cc.hashValue);
                } else if (msg->cc.id == NAMESPACEOID || msg->cc.id == OPEROID || msg->cc.id == AMOPOPID) {
                    ResetPlanCache(plansource);
                }
            }
        } else if (msg->id == SHAREDINVALRELCACHE_ID) {
            if (msg->rc.dbId == database_id || msg->rc.dbId == InvalidOid)
            {
                CheckRelDependency(plansource, msg->rc.relId);
            }
        } else if (msg->id == SHAREDINVALPARTCACHE_ID) {
            if (msg->pc.dbId == database_id || msg->pc.dbId == InvalidOid) {
                CheckRelDependency(plansource, msg->pc.partId);
            }
        }

        if (plansource->gpc.status.NeedDropSharedGPC()) {
            return true;
        }
    }

    return false;


}

void GlobalPlanCache::InvalMsg(const SharedInvalidationMessage *msgs, int n)
{
    int     *idx = (int *)palloc0(n * sizeof(int));
    int     tot = 0;

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
    for (uint32 bucket_id = 0; bucket_id < GPC_NUM_OF_BUCKETS; bucket_id ++) {
        /* Ok so bucket is not empty. Get the bucket S-lock so we can iterate through it. */
        int lock_id = m_array[bucket_id].lockId;
        LWLockAcquire(GetMainLWLockByIndex(lock_id), LW_EXCLUSIVE);
        MemoryContext oldcontext = MemoryContextSwitchTo(m_array[bucket_id].context);
        
        /* Check the number of entries in the bucket again. 
        * GPC Eviction might have removed the last entry while we were waiting for the shared lock. */
        int bucketEntriesCount = m_array[bucket_id].count;
        if (0 == bucketEntriesCount) {
            MemoryContextSwitchTo(oldcontext);
            LWLockRelease(GetMainLWLockByIndex(lock_id));
            continue;
        }

        HASH_SEQ_STATUS hash_seq;
        hash_seq_init(&hash_seq, m_array[bucket_id].hash_tbl);
        GPCEntry *entry = NULL;

        while ((entry = (GPCEntry *)hash_seq_search(&hash_seq)) != NULL) {
            Assert (entry->val.plansource != NULL);
            /* for standby mode, Invalid Msg send by xlog thread, but xlog thread didn't set db id into MyDatabaseId.
               So we need check each plan's db id by gpc'key in NeedDropEntryByLocalMsg latter */
            if (pmState == PM_RUN &&
                entry->val.plansource->gpc.key->env.plainenv.database_id != u_sess->proc_cxt.MyDatabaseId) {
                continue;
            }

            /* Atomic read the number of CachedEnvironment in this entry */
            if(NeedDropEntryByLocalMsg(entry->val.plansource, tot, idx, msgs)) {
                RemoveEntry(bucket_id, entry);
            }
        }

        MemoryContextSwitchTo(oldcontext);
        LWLockRelease(GetMainLWLockByIndex(lock_id));
    }

    pfree_ext(idx);
}
