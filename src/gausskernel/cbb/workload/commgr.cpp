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
 *   commgr.cpp
 *   functions for nodes collect info
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/workload/commgr.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "catalog/pgxc_node.h"
#include "executor/instrument.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "pgxc/pgxc.h"
#include "pgxc/poolmgr.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/execRemote.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memprot.h"
#include "pgstat.h"
#include "workload/commgr.h"
#include "workload/workload.h"
#include "workload/statctl.h"
#include "optimizer/nodegroups.h"

extern void WLMGetForeignResPoolMemory(ResourcePool* foreign_respool, int* memsize, int* estmsize);

/*
 * @Description : Get lock according to the hashcode for session realtime hash table
 * @in hashCode : hash key value
 * @in lockMode : lockMode value
 * @return lock if successfully.
 */
LWLock* LockSessRealTHashPartition(uint32 hashCode, LWLockMode lockMode)
{
    LWLock* partionLock = GetMainLWLockByIndex(FirstSessionRealTLock + (hashCode % NUM_SESSION_REALTIME_PARTITIONS));

    /*
     * When we abort during acquire lock during collect info from datanodes or update realtime hash
     * table, we should check if current lockid is held by ourself or not, or there will be dead lock
     * because WLMInsertCollectInfoIntoHashTable or WLMReplyCollectInfo need it.
     */
    if (LWLockHeldByMe(partionLock)) {
        HOLD_INTERRUPTS();
        LWLockRelease(partionLock);
    }

    (void)LWLockAcquire(partionLock, lockMode);
    return partionLock;
}

/*
 * @Description : Get lock according to the hashcode for session history hash table
 * @in hashCode : hash key value
 * @in lockMode : lockMode value
 * @return lock if successfully.
 */
LWLock* LockSessHistHashPartition(uint32 hashCode, LWLockMode lockMode)
{
    LWLock* partionLock = GetMainLWLockByIndex(FirstSessionHistLock + (hashCode % NUM_SESSION_HISTORY_PARTITIONS));

    /*
     * When we abort during acquire lock during collect info from datanodes or update history hash
     * table, we should check if current lockid is held by ourself or not, or there will be dead lock
     * because WLMSetSessionInfo or WLMReplyCollectInfo need it.
     */
    if (LWLockHeldByMe(partionLock)) {
        HOLD_INTERRUPTS();
        LWLockRelease(partionLock);
    }

    (void)LWLockAcquire(partionLock, lockMode);
    return partionLock;
}

/*
 * @Description : Release lock according to the hashcode for session realtime hash table
 * @in hashCode : hash key value
 * @return : void
 */
void UnLockSessRealTHashPartition(uint32 hashCode)
{
    LWLock* partionLock = GetMainLWLockByIndex(FirstSessionRealTLock + (hashCode % NUM_SESSION_REALTIME_PARTITIONS));
    LWLockRelease(partionLock);
}

/*
 * @Description : Release lock according to the hashcode for session history hash table
 * @in hashCode : hash key value
 * @return : void
 */
void UnLockSessHistHashPartition(uint32 hashCode)
{
    LWLock* partionLock = GetMainLWLockByIndex(FirstSessionHistLock + (hashCode % NUM_SESSION_HISTORY_PARTITIONS));
    LWLockRelease(partionLock);
}

/*
 * @Description : Get lock according to the hashcode for instance info hash table
 * @in hashCode : hash key value
 * @in lockMode : lockMode value
 * @return lock if successfully.
 */
LWLock* LockInstanceRealTHashPartition(uint32 hashCode, LWLockMode lockMode)
{
    LWLock* partionLock = GetMainLWLockByIndex(FirstInstanceRealTLock + (hashCode % NUM_INSTANCE_REALTIME_PARTITIONS));

    /*
     * When we abort during acquire lock during collect instance info at datanodes or update realtime hash
     * table, we should check if current lockid is held by ourself or not, or there will be dead lock
     * because WLMCollectInstanceStat or WLMGetInstanceInfo need it.
     */
    if (LWLockHeldByMe(partionLock)) {
        HOLD_INTERRUPTS();
        LWLockRelease(partionLock);
    }

    (void)LWLockAcquire(partionLock, lockMode);
    return partionLock;
}

/*
 * @Description : Release lock according to the hashcode for instance info realtime hash table
 * @in hashCode : hash key value
 * @return : void
 */
void UnLockInstanceRealTHashPartition(uint32 hashCode)
{
    LWLock* partionLock = GetMainLWLockByIndex (FirstInstanceRealTLock + (hashCode % NUM_INSTANCE_REALTIME_PARTITIONS));
    LWLockRelease(partionLock);
}

/*
 * @Description: reply collect info from current node
 * @IN keystr: key string
 * @IN tag: collect tag
 * @Return: void
 * @See also:
 */
void WLMReplyCollectInfo(char* keystr, WLMCollectTag tag)
{
    if (keystr == NULL || *keystr == '\0') {
        ereport(ERROR, (errmodule(MOD_WLM_CP),
               errcode(ERRCODE_SYNTAX_ERROR),
               errmsg("The key string of 'R' request could not be NULL")));
    }

    StringInfoData retbuf;

    int retcode = 0;

    if (!ENABLE_WORKLOAD_CONTROL) {
        goto finish;
    }

    switch (tag) {
        /*
         * Collect all query related information, including:
         * cpu time, space information, memory information
         */
        case WLM_COLLECT_ANY: {
            Qid qid;
            WLMActionTag action;

            /* parse key string */
            errno_t ssval = sscanf_s(keystr, "%u,%lu,%ld,%d", &qid.procId, &qid.queryId, &qid.stamp, &action);
            securec_check_ssval(ssval, , LOG);

            int peak_iops = 0;
            int curr_iops = 0;

            if (!IsQidInvalid(&qid)) {
                uint32 bucketId = GetIoStatBucket(&qid);
                int lockId = GetIoStatLockId(bucketId);
                HTAB* hashTbl = g_instance.wlm_cxt->stat_manager.iostat_info_hashtbl[bucketId].hashTable;
                (void)LWLockAcquire(GetMainLWLockByIndex(lockId), LW_SHARED);

                /* get history peak read/write Bps/iops and current total bytes/times of read/write from hash table */
                WLMDNodeIOInfo* ioinfo = (WLMDNodeIOInfo*)hash_search(hashTbl, &qid, HASH_FIND, NULL);

                if (ioinfo != NULL) {
                    peak_iops = ioinfo->io_geninfo.peak_iops;
                    curr_iops = ioinfo->io_geninfo.curr_iops;
                }
                LWLockRelease(GetMainLWLockByIndex(lockId));
            }

            uint32 hashCode = WLMHashCode(&qid, sizeof(Qid));

            (void)LockSessRealTHashPartition(hashCode, LW_SHARED);

            WLMDNodeInfo* pDNodeInfo = (WLMDNodeInfo*)hash_search(
                g_instance.wlm_cxt->stat_manager.collect_info_hashtbl, &qid, HASH_FIND, NULL);
            uint64 space = 0;
            int64 totalCpuTime = 0;
            int64 dnTime = 0;
            int64 spillSize = 0;
            int64 broadcastSize = 0;
            TimestampTz endTime = 0;
            int queryMemInChunks = 0;
            int peakChunksQuery = 0;
            int spillCount = 0;
            int warning = 0;
            bool pDNodeInfo_valid = false;

            if (pDNodeInfo != NULL) {
                pDNodeInfo_valid = true;
                SessionLevelMemory* sessionMemory = (SessionLevelMemory*)pDNodeInfo->mementry;

                totalCpuTime = pDNodeInfo->geninfo.totalCpuTime;
                endTime = (sessionMemory->dnEndTime > 0) ? sessionMemory->dnEndTime : GetCurrentTimestamp();
                dnTime = WLMGetTimestampDuration(sessionMemory->dnStartTime, endTime);
                queryMemInChunks = sessionMemory->queryMemInChunks << (chunkSizeInBits - BITS_IN_MB);
                peakChunksQuery = sessionMemory->peakChunksQuery << (chunkSizeInBits - BITS_IN_MB);
                spillCount = sessionMemory->spillCount;
                spillSize = sessionMemory->spillSize;
                broadcastSize = sessionMemory->broadcastSize;
                warning = sessionMemory->warning;

                /* get space infomation */
                if (pDNodeInfo->userdata != NULL) {
                    space = ((UserData*)pDNodeInfo->userdata)->totalspace;
                }

                /* adjust flag is set, change the cgroup for query */
                if (action == WLM_ACTION_ADJUST) {
                    WLMAdjustCGroup4EachThreadOnDN(pDNodeInfo);
                }
            }

            UnLockSessRealTHashPartition(hashCode);

            /* get query resource infomation */
            if (pDNodeInfo_valid) {
                pq_beginmessage(&retbuf, 'c');
                pq_sendint(&retbuf, queryMemInChunks, sizeof(int));
                pq_sendint(&retbuf, peakChunksQuery, sizeof(int));
                pq_sendint(&retbuf, spillCount, sizeof(int));
                pq_sendint64(&retbuf, space);
                pq_sendint64(&retbuf, dnTime);
                pq_sendint64(&retbuf, spillSize);
                pq_sendint64(&retbuf, broadcastSize);
                pq_sendint64(&retbuf, totalCpuTime);
                pq_sendint(&retbuf, warning, sizeof(int));
                pq_sendint(&retbuf, peak_iops, sizeof(int));
                pq_sendint(&retbuf, curr_iops, sizeof(int));
                pq_sendstring(&retbuf, g_instance.attr.attr_common.PGXCNodeName);
                pq_endmessage(&retbuf);

                retcode = 1;

                ereport(DEBUG4, (errmsg("action flag: %d", action)));
            }
            break;
        }
        case WLM_COLLECT_IO_RUNTIME: {
            Qid qid;
            int curr_iops = 0;
            int peak_iops = 0;

            errno_t ssval = sscanf_s(keystr, "%u,%lu,%ld", &qid.procId, &qid.queryId, &qid.stamp);
            securec_check_ssval(ssval, , LOG);

            uint32 bucketId = GetIoStatBucket(&qid);
            int lockId = GetIoStatLockId(bucketId);
            (void)LWLockAcquire(GetMainLWLockByIndex(lockId), LW_SHARED);

            /* get history peak read/write Bps/iops and current total bytes/times of read/write from hash table */
            WLMDNodeIOInfo* pDNodeIoInfo = (WLMDNodeIOInfo*)hash_search(
                g_instance.wlm_cxt->stat_manager.iostat_info_hashtbl[bucketId].hashTable, &qid, HASH_FIND, NULL);
            if (pDNodeIoInfo != NULL) {
                WLMIoGeninfo* ioinfo = &pDNodeIoInfo->io_geninfo;
                curr_iops = ioinfo->curr_iops;
                peak_iops = ioinfo->peak_iops;
            }
            LWLockRelease(GetMainLWLockByIndex(lockId));

            if (curr_iops != 0) {
                pq_beginmessage(&retbuf, 'c');
                ereport(DEBUG1, (errmsg("collect ioinfo: %d|%d", curr_iops, peak_iops)));
                pq_sendint(&retbuf, curr_iops, sizeof(int));
                pq_sendint(&retbuf, peak_iops, sizeof(int));

                pq_endmessage(&retbuf);

                retcode = 1;
            }
            break;
        }
        case WLM_COLLECT_USERINFO: {
            /* Access to user-level resource information, including: space information */
            char* username = keystr;
            int memsize = 0;
            int64 globalTotalSpace = 0;
            int64 globalTmpSpace = 0;
            int64 globalSpillSpace = 0;
            char* flag = strchr(keystr, '|');
            bool no_need_reply = false;
            if (keystr[0] == 'M') {
                if (flag) {
                    username = flag + 1;
                }
            } else {
                no_need_reply = true;
                if (flag) {
                    flag += 1;
                    errno_t ssval = sscanf_s(flag,
                        "%ld %ld %ld",
                        &globalTotalSpace,
                        &globalTmpSpace,
                        &globalSpillSpace);
                    securec_check_ssval(ssval, , LOG);
                    flag = strrchr(flag, ' ');
                    if (flag) {
                        username = flag + 1;
                    }
                }
            }

            /* get user oid according to user name */
            Oid uid = get_role_oid(username, true);

            if (!OidIsValid(uid)) {
                break;
            }

            WLMAutoLWLock user_lock(WorkloadUserInfoLock, LW_SHARED);
            user_lock.AutoLWLockAcquire();

            UserData* userdata =
                (UserData*)hash_search(g_instance.wlm_cxt->stat_manager.user_info_hashtbl, &uid, HASH_FIND, NULL);
            int usedCpuCnt = 0;
            int totalCpuCnt = 0;
            int curr_iops = 0;
            int peak_iops = 0;
            uint64 readBytes = 0;
            uint64 writeBytes = 0;
            uint64 readCounts = 0;
            uint64 writeCounts = 0;
            int64 totalspace = 0;
            int64 tmpSpace = 0;
            int64 spillSpace = 0;
            bool userdata_valid = false;
            /* get user info from user hash cache */
            if (userdata != NULL) {
                if (no_need_reply) {
                    userdata->global_totalspace = globalTotalSpace;
                    userdata->globalTmpSpace = globalTmpSpace;
                    userdata->globalSpillSpace = globalSpillSpace;
                    return;
                }

                userdata_valid = true;
                peak_iops = userdata->ioinfo.peak_iops;
                curr_iops = userdata->ioinfo.curr_iops;
                totalspace = userdata->totalspace;
                readBytes = userdata->ioinfo.read_bytes[1];
                writeBytes = userdata->ioinfo.write_bytes[1];
                readCounts = userdata->ioinfo.read_counts[1];
                writeCounts = userdata->ioinfo.write_counts[1];
                tmpSpace = userdata->tmpSpace;
                spillSpace = userdata->spillSpace;

                /* get the node group info */
                if (userdata->respool != NULL) {
                    ereport(DEBUG3,
                        (errmsg("RECEIVE data from user %s in nodegroup %s.", username, userdata->respool->ngroup)));

                    WLMNodeGroupInfo* ng = WLMMustGetNodeGroupFromHTAB(userdata->respool->ngroup);

                    /* Get cgroup related information, including: cpu usage and so on */
                    gscgroup_entry_t* cg_entry = NULL;

                    /* search control group from htab */
                    if ((cg_entry = gscgroup_lookup_hashtbl(ng, userdata->respool->cgroup)) != NULL) {
                        usedCpuCnt = cg_entry->usedCpuCount;
                        totalCpuCnt = cg_entry->cpuCount;
                    }

                    ereport(DEBUG3,
                        (errmsg("resource pool info %u of user %u cgroup %s ngroup %s usedcnt %d totalcnt %d ",
                            userdata->respool->rpoid,
                            userdata->userid,
                            userdata->respool->cgroup,
                            userdata->respool->ngroup,
                            usedCpuCnt,
                            totalCpuCnt)));
                }

                if (flag != NULL) {
                    memsize = WLMGetUserMemory(userdata);

                    if (userdata->childlist) {
                        foreach_cell(cell, userdata->childlist)
                        {
                            UserData* childdata = (UserData*)lfirst(cell);

                            if (childdata != NULL) {
                                memsize += WLMGetUserMemory(childdata);
                            }
                        }
                    }
                }
            }
            user_lock.AutoLWLockRelease();

            if (userdata_valid) {
                pq_beginmessage(&retbuf, 'c');
                pq_sendint(&retbuf, memsize, sizeof(int));
                pq_sendint(&retbuf, usedCpuCnt, sizeof(int));
                pq_sendint(&retbuf, totalCpuCnt, sizeof(int));
                pq_sendint(&retbuf, peak_iops, sizeof(int));
                pq_sendint(&retbuf, curr_iops, sizeof(int));
                pq_sendint64(&retbuf, readBytes);
                pq_sendint64(&retbuf, writeBytes);
                pq_sendint64(&retbuf, readCounts);
                pq_sendint64(&retbuf, writeCounts);
                pq_sendint64(&retbuf, totalspace);
                pq_sendint64(&retbuf, tmpSpace);
                pq_sendint64(&retbuf, spillSpace);
                pq_sendstring(&retbuf, g_instance.attr.attr_common.PGXCNodeName);
                pq_endmessage(&retbuf);

                retcode = 1;
            }

            break;
        }
        case WLM_COLLECT_PROCINFO: {
            /*
             * Get process-level memory information, including: the total
             * memory of the process, the memory available to the process,
             * the total memory of the physical node where the process is
             * located, and the available memory
             */
            char* node_host = NULL;

            /* get node host from catalog pgxc_node, we will use it as physical node tag */
            node_host = get_pgxc_node_formdata(g_instance.attr.attr_common.PGXCNodeName);
            if (node_host == NULL) {
                break;
            }

            int phy_totalmem = 0;
            int phy_freemem = 0;

            if (dywlm_client_physical_info(&phy_totalmem, &phy_freemem) != 0) {
                break;
            }

            /* we only use 90% of the total available memory */
            int totalMemSize = ((int)(maxChunksPerProcess << (chunkSizeInBits - BITS_IN_MB)));
            int usedMemSize = processMemInChunks << (chunkSizeInBits - BITS_IN_MB);
            int comm_mempool = (g_instance.attr.attr_network.comm_memory_pool >> BITS_IN_KB) *
                               g_instance.attr.attr_network.comm_memory_pool_percent / 100;

            totalMemSize = totalMemSize - comm_mempool - (gs_get_comm_context_memory() >> BITS_IN_MB);
            usedMemSize =
                usedMemSize - (gs_get_comm_used_memory() >> BITS_IN_MB) - (gs_get_comm_context_memory() >> BITS_IN_MB);

            if (totalMemSize < (MIN_PROCESS_LIMIT >> BITS_IN_KB)) {
                totalMemSize = (MIN_PROCESS_LIMIT >> BITS_IN_KB);
            }

            if (usedMemSize < 0) {
                usedMemSize = 0;
            }

            int ioutil = (int)g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.util;
            int cpuutil = (int)g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.cpu_util;
            int mempct = 0;
            int memsize = 0;
            int usedsize = 0;
            int estmsize = 0;

            /* get the foreign resource pool information for the node group */
            if (g_instance.wlm_cxt->local_dn_nodegroup != NULL &&
                g_instance.wlm_cxt->local_dn_nodegroup->foreignrp != NULL) {
                mempct = g_instance.wlm_cxt->local_dn_nodegroup->foreignrp->actpct;
                if (0 == mempct && g_instance.wlm_cxt->local_dn_nodegroup->foreignrp->mempct) {
                    mempct = 1;
                }
                memsize = totalMemSize * mempct / 100;

                WLMGetForeignResPoolMemory(g_instance.wlm_cxt->local_dn_nodegroup->foreignrp, &usedsize, &estmsize);
            }

            pq_beginmessage(&retbuf, 'c');
            pq_sendint(&retbuf, totalMemSize, sizeof(int));
            pq_sendint(&retbuf, usedMemSize, sizeof(int));
            pq_sendint(&retbuf, phy_totalmem, sizeof(int));
            pq_sendint(&retbuf, phy_freemem, sizeof(int));
            pq_sendint(&retbuf, cpuutil, sizeof(int));
            pq_sendint(&retbuf, g_instance.wlm_cxt->gscgroup_cpucnt, sizeof(int));
            pq_sendint(&retbuf, ioutil, sizeof(int));
            pq_sendint(&retbuf, memsize, sizeof(int));
            pq_sendint(&retbuf, usedsize, sizeof(int));
            pq_sendint(&retbuf, estmsize, sizeof(int));
            pq_sendint(&retbuf, mempct, sizeof(int));
            pq_sendstring(&retbuf, node_host);
            pq_endmessage(&retbuf);

            retcode = 1;
            pfree(node_host);

            break;
        }
        case WLM_COLLECT_JOBINFO: {
            /*
             * Access job-level resource information, including:
             * qid information, thread id, etc.
             */
            int j;
            for (j = 0; j < NUM_SESSION_HISTORY_PARTITIONS; j++) {
                LWLockAcquire(GetMainLWLockByIndex(FirstSessionHistLock + j), LW_SHARED);
            }

            WLMStmtDetail* pDetail = NULL;
            HASH_SEQ_STATUS hash_seq;
            hash_seq_init(&hash_seq, g_instance.wlm_cxt->stat_manager.session_info_hashtbl);

            ereport(DEBUG3,
                (errmsg("------MLW DN ACK jobs info------ total num of jobs: %ld.",
                    hash_get_num_entries(g_instance.wlm_cxt->stat_manager.session_info_hashtbl))));

            List* stmt_detail_list = NULL;
            WLMStmtReplyDetail* reply_node = NULL;

            /* Fetch all session info from the hash table. */
            while ((pDetail = (WLMStmtDetail*)hash_seq_search(&hash_seq)) != NULL) {
                /* only reply valid and active jobs */
                if (pDetail->valid && (pDetail->status == WLM_STATUS_RUNNING)) {
                    reply_node = (WLMStmtReplyDetail*)palloc0_noexcept(sizeof(WLMStmtReplyDetail));
                    if (reply_node == NULL) {
                        continue;
                    }
                    reply_node->procId = pDetail->qid.procId;
                    reply_node->queryId = pDetail->qid.queryId;
                    reply_node->stamp = pDetail->qid.stamp;
                    reply_node->estimate_memory = pDetail->estimate_memory;
                    reply_node->status = pDetail->status;

                    stmt_detail_list = lappend(stmt_detail_list, reply_node);
                }
            }
            for (j = NUM_SESSION_HISTORY_PARTITIONS; --j >= 0;) {
                LWLockRelease(GetMainLWLockByIndex(FirstSessionHistLock + j));
            }

            ListCell* curr = list_head(stmt_detail_list);
            ListCell* next = NULL;

            while (curr != NULL) {
                next = lnext(curr);

                reply_node = (WLMStmtReplyDetail*)lfirst(curr);
                pq_beginmessage(&retbuf, 'j');

                pq_sendint(&retbuf, (int)reply_node->procId, sizeof(int));
                pq_sendint64(&retbuf, (uint64)reply_node->queryId);
                pq_sendint64(&retbuf, (int64)reply_node->stamp);
                pq_sendint(&retbuf, reply_node->estimate_memory, sizeof(int));
                pq_sendint(&retbuf, (int)reply_node->status, sizeof(int));

                pq_endmessage(&retbuf);
                retcode = 1;

                ereport(DEBUG3,
                    (errmsg("------MLW DN ACK jobs info------ "
                            "qid: %lu with estimate_memory: %d.",
                        reply_node->queryId,
                        reply_node->estimate_memory)));

                curr = next;
            }

            list_free_ext(stmt_detail_list);

            break;
        }
        case WLM_COLLECT_SESSINFO: {
            /*
             * Access session-level resource information, including:
             * peak memory information, disk information, cpu time
             */
            Qid qid;
            int removed;
            int tableCounterSize = 0;
            int timeInfoSize = 0;

            errno_t ssval = sscanf_s(keystr, "%u,%lu,%ld,%d", &qid.procId, &qid.queryId, &qid.stamp, &removed);
            securec_check_ssval(ssval, , LOG);

            /* check the qid whether is valid */
            if (IsQidInvalid(&qid)) {
                break;
            }

            WLMStmtDetail* pDetail = NULL;
            uint32 hashCode = WLMHashCode(&qid, sizeof(Qid));
            if (removed > 0) {
                LockSessHistHashPartition(hashCode, LW_EXCLUSIVE);

                WLMStmtDetail* pDetail_hash = (WLMStmtDetail*)hash_search(
                    g_instance.wlm_cxt->stat_manager.session_info_hashtbl, &qid, HASH_FIND, NULL);
                if (pDetail_hash != NULL) {
                    pDetail = (WLMStmtDetail*)palloc0_noexcept(sizeof(WLMStmtDetail));
                    if (pDetail != NULL) {
                        errno_t rc = memcpy_s(pDetail, sizeof(WLMStmtDetail), pDetail_hash, sizeof(WLMStmtDetail));
                        securec_check(rc, "\0", "\0");
                        if (pDetail_hash->plan_size > 0 && pDetail_hash->query_plan != NULL) {
                            pDetail->query_plan = (char*)palloc0(pDetail_hash->plan_size);
                            rc = memcpy_s(pDetail->query_plan, pDetail_hash->plan_size, pDetail_hash->query_plan, pDetail_hash->plan_size);
                            pDetail->plan_size = pDetail_hash->plan_size;
                            securec_check(rc, "\0", "\0");
                        } else {
                            StringInfoData plan_string;
                            initStringInfo(&plan_string);
                            appendStringInfo(&plan_string, "Datanode Name: %s\nNoPlan\n\n", g_instance.attr.attr_common.PGXCNodeName);
                            pDetail->plan_size = plan_string.len + 1;
                            pDetail->query_plan = (char*)palloc0(pDetail->plan_size);
                            rc = strncpy_s(pDetail->query_plan, pDetail->plan_size, plan_string.data, pDetail->plan_size - 1);
                            securec_check(rc, "\0", "\0");
                            pfree_ext(plan_string.data);
                        }

                        tableCounterSize = sizeof(PgStat_TableCounts);
                        pDetail->slowQueryInfo.current_table_counter = (PgStat_TableCounts*)palloc0(tableCounterSize);
                        if (pDetail_hash->slowQueryInfo.current_table_counter != NULL) {
                            rc = memcpy_s(pDetail->slowQueryInfo.current_table_counter, tableCounterSize,
                                      pDetail_hash->slowQueryInfo.current_table_counter, tableCounterSize);
                            securec_check(rc, "\0", "\0");
                        }

                        timeInfoSize = sizeof(int64) * TOTAL_TIME_INFO_TYPES;
                        pDetail->slowQueryInfo.localTimeInfoArray = (int64*)palloc0(timeInfoSize);
                        if (pDetail_hash->slowQueryInfo.localTimeInfoArray != NULL) {
                            rc = memcpy_s(pDetail->slowQueryInfo.localTimeInfoArray, timeInfoSize,
                                      pDetail_hash->slowQueryInfo.localTimeInfoArray, timeInfoSize);
                            securec_check(rc, "\0", "\0");
                        }

                        pfree_ext(pDetail_hash->slowQueryInfo.current_table_counter);
                        pfree_ext(pDetail_hash->slowQueryInfo.localTimeInfoArray);
                        pfree_ext(pDetail_hash->query_plan);
                    }
                }
                /* remove it from the hash table */
                hash_search(g_instance.wlm_cxt->stat_manager.session_info_hashtbl, &qid, HASH_REMOVE, NULL);
                (void)UnLockSessHistHashPartition(hashCode);
            } else {
                LockSessHistHashPartition(hashCode, LW_SHARED);

                WLMStmtDetail* pDetail_hash = (WLMStmtDetail*)hash_search(
                    g_instance.wlm_cxt->stat_manager.session_info_hashtbl, &qid, HASH_FIND, NULL);

                if (pDetail_hash != NULL) {
                    pDetail = (WLMStmtDetail*)palloc0_noexcept(sizeof(WLMStmtDetail));
                    if (pDetail != NULL) {
                        errno_t rc = memcpy_s(pDetail, sizeof(WLMStmtDetail), pDetail_hash, sizeof(WLMStmtDetail));
                        securec_check(rc, "\0", "\0");
                        if (pDetail_hash->plan_size > 0 && pDetail_hash->query_plan != NULL) {
                            pDetail->query_plan = (char*)palloc0(pDetail_hash->plan_size);
                            rc = memcpy_s(pDetail->query_plan, pDetail_hash->plan_size, pDetail_hash->query_plan, pDetail_hash->plan_size);
                            securec_check(rc, "\0", "\0");
                            pDetail->plan_size = pDetail_hash->plan_size;
                        } else {
                            StringInfoData plan_string;
                            initStringInfo(&plan_string);
                            appendStringInfo(&plan_string, "Datanode Name: %s\nNoPlan\n\n", g_instance.attr.attr_common.PGXCNodeName);
                            pDetail->plan_size = plan_string.len + 1;
                            pDetail->query_plan = (char*)palloc0(pDetail->plan_size);
                            rc = strncpy_s(pDetail->query_plan, pDetail->plan_size, plan_string.data, pDetail->plan_size - 1);
                            securec_check(rc, "\0", "\0");
                            pfree_ext(plan_string.data);
                        }

                        tableCounterSize = sizeof(PgStat_TableCounts);
                        pDetail->slowQueryInfo.current_table_counter = (PgStat_TableCounts*)palloc0(tableCounterSize);
                        if (pDetail_hash->slowQueryInfo.current_table_counter != NULL) {
                            rc = memcpy_s(pDetail->slowQueryInfo.current_table_counter, tableCounterSize,
                                      pDetail_hash->slowQueryInfo.current_table_counter, tableCounterSize);
                            securec_check(rc, "\0", "\0");
                        }

                        timeInfoSize = sizeof(int64) * TOTAL_TIME_INFO_TYPES;
                        pDetail->slowQueryInfo.localTimeInfoArray = (int64*)palloc0(timeInfoSize);
                        if (pDetail_hash->slowQueryInfo.localTimeInfoArray != NULL) {
                            rc = memcpy_s(pDetail->slowQueryInfo.localTimeInfoArray, timeInfoSize,
                                      pDetail_hash->slowQueryInfo.localTimeInfoArray, timeInfoSize);
                            securec_check(rc, "\0", "\0");
                        }
                    }
                }
                (void)UnLockSessHistHashPartition(hashCode);
            }

            if (pDetail != NULL) {
                /* Get the result, it must be valid */
                if (pDetail->valid) {
                    pq_beginmessage(&retbuf, 'c');
                    pq_sendint(&retbuf, 0, sizeof(int));
                    pq_sendint(&retbuf, pDetail->geninfo.maxPeakChunksQuery, sizeof(int));
                    pq_sendint(&retbuf, pDetail->geninfo.spillCount, sizeof(int));
                    pq_sendint64(&retbuf, 0L);
                    pq_sendint64(&retbuf, pDetail->geninfo.dnTime);
                    pq_sendint64(&retbuf, pDetail->geninfo.spillSize);
                    pq_sendint64(&retbuf, pDetail->geninfo.broadcastSize);
                    pq_sendint64(&retbuf, pDetail->geninfo.totalCpuTime);
                    pq_sendint(&retbuf, pDetail->warning, sizeof(int));
                    pq_sendint(&retbuf, pDetail->ioinfo.peak_iops, sizeof(int));
                    pq_sendint(&retbuf, pDetail->ioinfo.curr_iops, sizeof(int));
                    pq_sendstring(&retbuf, g_instance.attr.attr_common.PGXCNodeName);

                    if (t_thrd.proc->workingVersionNum >= SLOW_QUERY_VERSION) {
                        /* tuple info & cache IO */
                        pq_sendint64(&retbuf, pDetail->slowQueryInfo.current_table_counter->t_tuples_returned);
                        pq_sendint64(&retbuf, pDetail->slowQueryInfo.current_table_counter->t_tuples_fetched);
                        pq_sendint64(&retbuf, pDetail->slowQueryInfo.current_table_counter->t_tuples_inserted);
                        pq_sendint64(&retbuf, pDetail->slowQueryInfo.current_table_counter->t_tuples_updated);
                        pq_sendint64(&retbuf, pDetail->slowQueryInfo.current_table_counter->t_tuples_deleted);
                        pq_sendint64(&retbuf, pDetail->slowQueryInfo.current_table_counter->t_blocks_fetched);
                        pq_sendint64(&retbuf, pDetail->slowQueryInfo.current_table_counter->t_blocks_hit);

                        /* time Info */
                        for (uint32 idx = 0; idx < TOTAL_TIME_INFO_TYPES; idx++) {
                            pq_sendint64(&retbuf, pDetail->slowQueryInfo.localTimeInfoArray[idx]);
                        }

                        /* plan info */
                        pq_sendint64(&retbuf, pDetail->plan_size);
                        pq_sendstring(&retbuf, pDetail->query_plan);
                    }

                    pq_endmessage(&retbuf);
                    ereport(DEBUG1, (errcode(ERRCODE_SLOW_QUERY), errmsg("%u, %lu, %ld\nplan %s", qid.procId, qid.queryId, qid.stamp, pDetail->query_plan), errhidestmt(true), errhideprefix(true)));
                    retcode = 1;
                    pfree_ext(pDetail->slowQueryInfo.current_table_counter);
                    pfree_ext(pDetail->slowQueryInfo.localTimeInfoArray);
                    pfree_ext(pDetail->query_plan);
                    pfree_ext(pDetail);
                }
            }
            break;
        }

        case WLM_COLLECT_OPERATOR_SESSION: {
            Qpid qid;
            int removed;

            errno_t ssval = sscanf_s(keystr, "%u,%lu,%d,%d", &qid.procId, &qid.queryId, &qid.plannodeid, &removed);
            securec_check_ssval(ssval, , LOG);

            /* check the qid whether is valid */
            if (IsQpidInvalid(&qid)) {
                break;
            }

            uint32 hashCode = GetHashPlanCode(&qid, sizeof(Qpid));
            OperatorInfo operatorMemory;
            bool dataValid = false;

            if (removed > 0) {
                LockOperHistHashPartition(hashCode, LW_EXCLUSIVE);
            } else {
                LockOperHistHashPartition(hashCode, LW_SHARED);
            }

            ExplainDNodeInfo* pDetail =
                (ExplainDNodeInfo*)hash_search(g_operator_table.collected_info_hashtbl, &qid, HASH_FIND, NULL);

            if (pDetail != NULL) {
                errno_t rc = memcpy_s(&operatorMemory, sizeof(OperatorInfo), &pDetail->geninfo, sizeof(OperatorInfo));
                securec_check(rc, "\0", "\0");

                operatorMemory.ec_execute_datanode = pstrdup(pDetail->geninfo.ec_execute_datanode);
                operatorMemory.ec_dsn = pstrdup(pDetail->geninfo.ec_dsn);
                operatorMemory.ec_username = pstrdup(pDetail->geninfo.ec_username);
                operatorMemory.ec_query = pstrdup(pDetail->geninfo.ec_query);
                dataValid = true;
            }

            if (removed > 0) { /* remove it from the hash table. */
                hash_search(g_operator_table.collected_info_hashtbl, &qid, HASH_REMOVE, NULL);
            }
            UnLockOperHistHashPartition(hashCode);

            if (dataValid) {
                sendExplainInfo(&operatorMemory);
                releaseOperatorInfoEC(&operatorMemory);
                retcode = 1;
                pfree_ext(operatorMemory.ec_execute_datanode);
                pfree_ext(operatorMemory.ec_dsn);
                pfree_ext(operatorMemory.ec_username);
                pfree_ext(operatorMemory.ec_query);
            }
            break;
        }
        case WLM_COLLECT_OPERATOR_RUNTIME: {
            Qpid qid;

            errno_t ssval = sscanf_s(keystr, "%u,%lu,%d", &qid.procId, &qid.queryId, &qid.plannodeid);
            securec_check_ssval(ssval, , LOG);

            /* check the qid whether is valid */
            if (IsQpidInvalid(&qid)) {
                break;
            }

            uint32 hashCode = GetHashPlanCode(&qid, sizeof(Qpid));

            LockOperRealTHashPartition(hashCode, LW_SHARED);
            ExplainDNodeInfo* pDNodeInfo =
                (ExplainDNodeInfo*)hash_search(g_operator_table.explain_info_hashtbl, &qid, HASH_FIND, NULL);

            OperatorInfo operatorMemory;
            bool dataValid = false;
            if (pDNodeInfo != NULL) {
                dataValid = true;
                setOperatorInfo(&operatorMemory, (Instrumentation*)pDNodeInfo->explain_entry);
            }
            UnLockOperRealTHashPartition(hashCode);

            if (dataValid) {
                sendExplainInfo(&operatorMemory);
                releaseOperatorInfoEC(&operatorMemory);
                retcode = 1;
            }
            break;
        }
        default:
            break;
    }

    /* completed tag */
finish:
    pq_beginmessage(&retbuf, 'f');
    pq_sendint(&retbuf, retcode, 4);
    pq_endmessage(&retbuf);

    pq_flush();
}

/*
 * @Description: local node collector, run on data nodes
 * @IN msg: message received
 * @Return: void
 * @See also:
 */
void WLMLocalInfoCollector(StringInfo msg)
{
    /* get collect tag */
    WLMCollectTag tag = (WLMCollectTag)pq_getmsgint(msg, 4);

    /* get collect key string */
    char* keystr = pstrdup(pq_getmsgstring(msg));

    pq_getmsgend(msg);

    /* reply collect info from current node */
    WLMReplyCollectInfo(keystr, tag);

    pfree(keystr);
}

/*
 * @Description: check connection is invalid
 * @IN val1: node index
 * @IN val2: null pointer
 * @Return: valid or invalid
 * @See also:
 */
bool IsDNConnInvalid(void* val1, const void* val2)
{
    ListCell* cell = (ListCell*)val1;

    PoolAgent* agent = get_poolagent();

    int nodeidx = lfirst_int(cell);

    return agent->dn_connections[nodeidx] == val2;
}

/*
 * @Description: start to collect info from the remote node
 * @IN void
 * @Return: node connect handles
 * @See also:
 */
PGXCNodeAllHandles* WLMRemoteInfoCollectorStart(void)
{
    List* dnlist = NULL;
    PGXCNodeAllHandles* pgxc_handles = NULL;

    int i = 0;

    /* get all data node index */
    for (i = 0; i < u_sess->pgxc_cxt.NumDataNodes; ++i) {
        dnlist = lappend_int(dnlist, i);
    }

    PG_TRY();
    {
        pgxc_handles = get_handles(dnlist, NULL, false);
        for (int i = 0; i < pgxc_handles->dn_conn_count; i++) {
            if (pgxc_handles->datanode_handles[i]->state == DN_CONNECTION_STATE_QUERY) {
                BufferConnection(pgxc_handles->datanode_handles[i]);
            }
        }
    }
    PG_CATCH();
    {
        release_pgxc_handles(pgxc_handles);
        pgxc_handles = NULL;
        list_free_ext(dnlist);

        PG_RE_THROW();
    }
    PG_END_TRY();

    list_free_ext(dnlist);

    return pgxc_handles;
}

/*
 * @Description: release all error connection node.
 * @Return: void
 * @See also:
 */
void WLMReleaseFailCountAgent(int datanode_count, PoolAgent* agent, PGXCNodeAllHandles* pgxc_handles)
{
    if (agent == NULL) {
        return;
    }

    for (int i = 0; i < datanode_count; ++i) {
        if (agent->dn_connections[i]) {
            release_connection(agent, &(agent->dn_connections[i]), agent->dn_conn_oids[i], true);
        }

        if (pgxc_handles->datanode_handles[i]) {
            pgxc_node_free(pgxc_handles->datanode_handles[i]);
            pgxc_node_init(pgxc_handles->datanode_handles[i], NO_SOCKET);
        }
    }
}

/*
 * @Description: send message to certain nodes by nodegroup to collect info
 * @IN pgxc_handles: node connect handles
 * @IN group_name: node group name
 * @IN keystr: collect key string
 * @IN tag: collect tag
 * @Return: int
 * @See also:
 */
int WLMRemoteInfoSenderByNG(const char* group_name, const char* keystr, WLMCollectTag tag)
{
    ereport(DEBUG3, (errmsg("SEND data to %s by NG: %s.", keystr, group_name)));

    /* get nodegroup info */
    Oid groupoid = ng_get_group_groupoid(group_name);
    Oid* gmembers = NULL;
    int gcnt = 0;
    PGXCNodeAllHandles* pgxc_handles = NULL;

    if (groupoid == InvalidOid) {
        gcnt = get_pgxc_groupmembers(ng_get_installation_group_oid(), &gmembers);
    } else {
        gcnt = get_pgxc_groupmembers(groupoid, &gmembers);
    }

    /* get data node list from dn_handles */
    List* dnlist = GetNodeGroupNodeList(gmembers, gcnt);

    PG_TRY();
    {
        pgxc_handles = get_handles(dnlist, NULL, false);
    }
    PG_CATCH();
    {
        release_pgxc_handles(pgxc_handles);
        pgxc_handles = NULL;
        list_free_ext(dnlist);

        PG_RE_THROW();
    }
    PG_END_TRY();

    list_free_ext(dnlist);

    if (pgxc_handles == NULL) {
        return -1;
    }

    int datanode_count = pgxc_handles->dn_conn_count;

    int failcount = 0;

    PoolAgent* agent = get_poolagent();

    /* send keystr and tag to each data node */
    for (int i = 0; i < datanode_count; ++i) {
        PGXCNodeHandle* dn_handle = pgxc_handles->datanode_handles[i];

        if (dn_handle == NULL) {
            continue;
        }

        ereport(DEBUG3, (errmsg("SEND data to DN %s in NG.", dn_handle->remoteNodeName)));

        if (dn_handle->state == DN_CONNECTION_STATE_QUERY) {
            BufferConnection(dn_handle);
        }

        dn_handle->state = DN_CONNECTION_STATE_IDLE;

        /* If a node has a connection error, we have to stop collecting */
        if (pgxc_node_dywlm_send_record(dn_handle, tag, keystr) != 0) {
            if (failcount == 0) {
                ereport(LOG,
                    (errmsg("Remote Sender: Failed to "
                            "send command to Datanode %s",
                        dn_handle->remoteNodeName)));
            }
            ++failcount;
        }
    }
    if (failcount > 0) {
        WLMReleaseFailCountAgent(datanode_count, agent, pgxc_handles);
        return -1;
    }
    return 0;
}

/*
 * @Description: send message to each node to collect info
 * @IN pgxc_handles: node connect handles
 * @IN keystr: collect key string
 * @IN tag: collect tag
 * @Return: void
 * @See also:
 */
int WLMRemoteInfoSender(PGXCNodeAllHandles* pgxc_handles, const char* keystr, WLMCollectTag tag)
{
    int datanode_count = pgxc_handles->dn_conn_count;
    int failcount = 0;

    PoolAgent* agent = get_poolagent();

    /* send keystr and tag to each data node */
    for (int i = 0; i < datanode_count; ++i) {
        PGXCNodeHandle* dn_handle = pgxc_handles->datanode_handles[i];

        if (dn_handle == NULL) {
            continue;
        }
        if (dn_handle->state == DN_CONNECTION_STATE_QUERY) {
            BufferConnection(dn_handle);
        }

        dn_handle->state = DN_CONNECTION_STATE_IDLE;

        /* If a node has a connection error, we have to stop collecting */
        if (pgxc_node_dywlm_send_record(dn_handle, tag, keystr) != 0) {
            if (failcount == 0) {
                ereport(LOG,
                    (errmsg("Remote Sender: Failed to "
                            "send command to Datanode %u",
                        dn_handle->nodeoid)));
            }
            ++failcount;
        }
    }
    if (failcount > 0) {
        WLMReleaseFailCountAgent(datanode_count, agent, pgxc_handles);
        return -1;
    }
    return 0;
}

/*
 * @Description: receive message from certain nodes by nodegroup or from each node
 * @IN pgxcHandles: node connect handles
 * @IN sumInfo: summary info
 * @IN parseFunc: parse function to parse message received
 * @IN byNg: from certain nodes by nodegroup or from each node
 * @Return: void
 * @See also:
 */
void WLMRemoteInfoWork(PGXCNodeAllHandles* pgxcHandles, void* sumInfo, int size, WLMParseMessage parseFunc, bool byNg)
{
    struct timeval timeout = {120, 0};
    int datanodeCount = pgxcHandles->dn_conn_count;

    for (int i = 0; i < datanodeCount; ++i) {
        PGXCNodeHandle* dnHandle = pgxcHandles->datanode_handles[i];

        if (dnHandle == NULL) {
            continue;
        }

        if (byNg) {
            ereport(DEBUG3, (errmsg("RECEIVE data from DN %s in NG.", dnHandle->remoteNodeName)));
        }

        bool hasError = false;
        bool isFinished = false;

        /* receive workload records from each node */
        for (;;) {
            if (pgxc_node_receive(1, &dnHandle, &timeout)) {
                ereport(LOG,
                    (errmsg("%s:%d recv fail", __FUNCTION__, __LINE__)));
                break;
            }
            int len;
            char* msg = NULL;
            char msg_type = get_message(dnHandle, &len, &msg);
            /* check message type */
            switch (msg_type) {
                case '\0': /* message is not completed */
                case 'E':  /* DN already down or crashed */
                    hasError = true;
                    break;
                case 'c': /* get workload record */
                {
                    StringInfoData input_msg;
                    initStringInfo(&input_msg);
                    appendBinaryStringInfo(&input_msg, msg, len);
                    parseFunc(&input_msg, sumInfo, size);
                    pq_getmsgend(&input_msg);
                    pfree(input_msg.data);
                    break;
                }
                case 'f': /* message is completed */
                {
                    int retcode;
                    errno_t errval = memcpy_s(&retcode, sizeof(int), msg, 4);
                    securec_check_errval(errval, , LOG);
                    retcode = (int)ntohl(retcode);
                    isFinished = true;
                    break;
                }
                case 'u': {
                    StringInfoData input_msg;
                    initStringInfo(&input_msg);
                    appendBinaryStringInfo(&input_msg, msg, len);
                    parseFunc(&input_msg, sumInfo, size);
                    pq_getmsgend(&input_msg);
                    pfree(input_msg.data);
                    break;
                }
                case 'Z':
                    if (hasError) {
                        if (byNg) {
                            ereport(LOG,
                                (errmsg("get message from node %s failed %s",
                                    dnHandle->remoteNodeName,
                                    dnHandle->error ? dnHandle->error : "")));
                        } else {
                            ereport(LOG,
                                (errmsg("get message from node %u failed %s",
                                    dnHandle->nodeoid,
                                    dnHandle->error ? dnHandle->error : "")));
                        }
                        isFinished = true;
                    }
                    break;
                case 'A': /* NotificationResponse */
                case 'S': /* SetCommandComplete */
                {
                    /*
                     * Ignore these to prevent multiple messages, one from
                     * each node. Coordinator will send on for DDL anyway
                     */
                    break;
                }
                default:
                    break;
            }

            if (isFinished) {
                break;
            }
        }

        dnHandle->state = DN_CONNECTION_STATE_IDLE;
    }
}

/*
 * @Description: receive message from certain nodes by nodegroup
 * @IN pgxc_handles: node connect handles
 * @IN group_name: node group name
 * @IN suminfo: summary info
 * @IN parse_func: parse function to parse message received
 * @Return: void
 * @See also:
 */
void WLMRemoteInfoReceiverByNG(const char* group_name, void* suminfo, int size, WLMParseMessage parse_func)
{
    /* get nodegroup info */
    ereport(DEBUG3, (errmsg("RECEIVE data from NG: %s.", group_name)));

    Oid groupoid = ng_get_group_groupoid(group_name);
    Oid* gmembers = NULL;
    int gcnt = 0;
    PGXCNodeAllHandles* pgxc_handles = NULL;

    if (groupoid == InvalidOid) {
        gcnt = get_pgxc_groupmembers(ng_get_installation_group_oid(), &gmembers);
    } else {
        gcnt = get_pgxc_groupmembers(groupoid, &gmembers);
    }

    /* get data node list from dn_handles */
    List* dnlist = GetNodeGroupNodeList(gmembers, gcnt);

    PG_TRY();
    {
        pgxc_handles = get_handles(dnlist, NULL, false);
    }
    PG_CATCH();
    {
        release_pgxc_handles(pgxc_handles);
        pgxc_handles = NULL;
        list_free_ext(dnlist);

        PG_RE_THROW();
    }
    PG_END_TRY();

    list_free_ext(dnlist);

    if (pgxc_handles == NULL) {
        return;
    }

    WLMRemoteInfoWork(pgxc_handles, suminfo, size, parse_func, true);

    return;
}

/*
 * @Description: receive message from each node
 * @IN pgxc_handles: node connect handles
 * @IN suminfo: summary info
 * @IN parse_func: parse function to parse message received
 * @Return: void
 * @See also:
 */
void WLMRemoteInfoReceiver(PGXCNodeAllHandles* pgxc_handles, void* suminfo, int size, WLMParseMessage parse_func)
{
    WLMRemoteInfoWork(pgxc_handles, suminfo, size, parse_func, false);
    return;
}

/*
 * @Description: release node connection handles
 * @IN pgxc_handles: node connect handles
 * @Return: void
 * @See also:
 */
void WLMRemoteInfoCollectorFinish(PGXCNodeAllHandles* pgxc_handles)
{
    release_pgxc_handles(pgxc_handles);
}

/* get top-k num for (random) sending connection request */
int GetTopkNum(int total, int ratio, bool is_top_one)
{
    int topk = 1;
    int left = 0;
    /* only get top one */
    if (is_top_one) {
        return topk;
    }
    /* never get here */
    if (total == 0 || ratio == 0) {
        return 0;
    }

    topk = (total * ratio) / 100;
    left = (total * ratio) % 100;

    /* get at least one node */
    if (topk == 0) {
        topk = 1;
    } else {
        /* round up */
        if (left != 0) {
            topk = topk + 1;
        }
    }

    return topk;
}

/*
 * @Description: CN collect job info from remote DN nodes
 * @IN keystr: key string
 * @IN suminfo: summary info
 * @IN tag: collect tag
 * @Return: List<DynamicWorkloadRecord>
 * @See also:
 */
List* WLMRemoteJobInfoCollector(const char* keystr, void* suminfo, WLMCollectTag tag)
{
    ereport(DEBUG3, (errmsg("------MLW CN COLLECT jobs from DNs------ PGXCNodeName: %s, Tag: %d", keystr, tag)));

    List* jobs_list = NULL;
    List* dnlist = NULL;
    PGXCNodeAllHandles* pgxc_handles = NULL;

    DynamicNodeData* nodedata = (DynamicNodeData*)suminfo;

    int i = 0;
    struct timeval timeout = {120, 0};
    int topk = GetTopkNum(nodedata->group_count, 30, true);
    int topk_cnt = 0;

    /* get data node list from dn_handles */
    dnlist = GetNodeGroupNodeList(nodedata->group_members, nodedata->group_count);

    PG_TRY();
    {
        pgxc_handles = get_handles(dnlist, NULL, false);
    }
    PG_CATCH();
    {
        release_pgxc_handles(pgxc_handles);
        pgxc_handles = NULL;
        list_free_ext(dnlist);

        PG_RE_THROW();
    }
    PG_END_TRY();

    list_free_ext(dnlist);

    if (pgxc_handles == NULL) {
        return NULL;
    }

    int datanode_count = pgxc_handles->dn_conn_count;

    /* send request to each data node */
    for (i = 0; i < datanode_count; ++i) {
        PGXCNodeHandle* dn_handle = pgxc_handles->datanode_handles[i];
        if (dn_handle == NULL) {
            continue;
        }

        dn_handle->state = DN_CONNECTION_STATE_IDLE;

        /* If a node has a connection error, we have to temporarily ignore the node */
        if (pgxc_node_dywlm_send_params_for_jobs(dn_handle, tag, keystr) != 0) {
            Oid nodeoid = dn_handle->nodeoid;
            release_pgxc_handles(pgxc_handles);

            ereport(ERROR,
                (errcode(ERRCODE_INVALID_OPERATION),
                    errmsg("Parallel Function: Failed to send command to Datanode %u", nodeoid)));
        }
        /* get the topk ones */
        topk_cnt++;
        if (topk_cnt >= topk) {
            break;
        }
    }

    /* reset topk count */
    topk_cnt = 0;

    for (i = 0; i < datanode_count; ++i) {
        PGXCNodeHandle* dn_handle = pgxc_handles->datanode_handles[i];
        if (dn_handle == NULL) {
            continue;
        }

        bool hasError = false;
        bool isFinish = false;

        /* receive workload jobs from each node */
        for (;;) {
            if (pgxc_node_receive(1, &dn_handle, &timeout)) {
                ereport(LOG,
                    (errmsg("%s:%d recv fail", __FUNCTION__, __LINE__)));
                break;
            }

            char* msg = NULL;
            int len;

            char msg_type = get_message(dn_handle, &len, &msg);

            /* check message type */
            switch (msg_type) {
                case '\0': /* message is not completed */
                case 'E':  /* DN already down or crashed */
                    hasError = true;
                    break;
                case 'j': /* get workload job */
                {
                    DynamicWorkloadRecord* record =
                        (DynamicWorkloadRecord*)palloc0_noexcept(sizeof(DynamicWorkloadRecord));

                    if (record == NULL) {
                        list_free_ext(dnlist);
                        release_pgxc_handles(pgxc_handles);
                        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory of current node.")));
                    }

                    StringInfoData input_msg;
                    initStringInfo(&input_msg);
                    appendBinaryStringInfo(&input_msg, msg, len);

                    /* parse message to get record */
                    record->qid.procId = (Oid)pq_getmsgint(&input_msg, 4);
                    record->qid.queryId = (uint64)pq_getmsgint64(&input_msg);
                    record->qid.stamp = (TimestampTz)pq_getmsgint64(&input_msg);
                    int tmpAddMemory = pq_getmsgint(&input_msg, 4);
                    if (INT_MAX - nodedata->estimate_memory < tmpAddMemory) {
                        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("estimate_memory will out of MAX_INT, orign[%d], addMemory[%d]",
                                       nodedata->estimate_memory, tmpAddMemory)));
                    }
                    nodedata->estimate_memory += tmpAddMemory;
                    /* from status to qtype */
                    WLMStatusTag status = (WLMStatusTag)pq_getmsgint(&input_msg, 4);
                    record->qtype = ((status == WLM_STATUS_RUNNING) ? PARCTL_ACTIVE : PARCTL_RELEASE);

                    ereport(DEBUG3,
                        (errmsg("------MLW CN RCV jobs info------ "
                                "from node: %u, qid: %lu with qtype: %d and current estimate_memory: %d.",
                            dn_handle->nodeoid,
                            record->qid.queryId,
                            record->qtype,
                            nodedata->estimate_memory)));

                    pq_getmsgend(&input_msg);
                    pfree(input_msg.data);

                    if (record->qtype == PARCTL_ACTIVE) {
                        jobs_list = lappend(jobs_list, record);
                    }

                    break;
                }
                case 'f': /* message is completed */
                {
                    int retcode;

                    errno_t errval = memcpy_s(&retcode, sizeof(int), msg, 4);
                    securec_check_errval(errval, , LOG);
                    retcode = (int)ntohl(retcode);

                    isFinish = true;

                    break;
                }
                case 'Z':
                    if (hasError) {
                        ereport(LOG,
                            (errmsg("get message from node %u failed %s",
                                dn_handle->nodeoid,
                                dn_handle->error ? dn_handle->error : "")));
                        isFinish = true;
                    }

                    break;
                case 'A': /* NotificationResponse */
                case 'S': /* SetCommandComplete */
                {
                    /*
                     * Ignore these to prevent multiple messages, one from
                     * each node. Coordinator will send on for DDL anyway
                     */
                    break;
                }
                default: /* never get here */
                    break;
            }
            if (isFinish) {
                break;
            }
        }
        dn_handle->state = DN_CONNECTION_STATE_IDLE;

        /* get the topk ones */
        topk_cnt++;
        if (topk_cnt >= topk) {
            nodedata->estimate_memory /= topk;
            break;
        }
    }

    list_free_ext(dnlist);
    release_pgxc_handles(pgxc_handles);

    return jobs_list;
}

PGXCNodeAllHandles* WLMRemoteNodeAcquireConn(List* node_list, PoolNodeType type)
{
    PGXCNodeAllHandles* pgxc_handles = NULL;

    PG_TRY();
    {
        if (type == POOL_NODE_CN) {
            pgxc_handles = get_handles(NULL, node_list, true);
        } else {
            pgxc_handles = get_handles(node_list, NULL, false);
        }
    }
    PG_CATCH();
    {
        release_pgxc_handles(pgxc_handles);
        pgxc_handles = NULL;
        list_free(node_list);

        PG_RE_THROW();
    }
    PG_END_TRY();

    return pgxc_handles;
}

/*
 * @Description: collect info from remote nodes
 * @IN keystr: key string
 * @IN suminfo: summary info
 * @IN tag: collect tag
 * @IN size: size of suminfo
 * @IN parse_func: parse function for the message received
 * @Return: void
 * @See also:
 */
void WLMRemoteInfoCollector(const char* keystr, void* suminfo, WLMCollectTag tag, int size, WLMParseMessage parse_func)
{
    List* dnlist = NULL;

    DynamicNodeData* nodedata = (DynamicNodeData*)suminfo;

    int i = 0;

    struct timeval timeout = {120, 0};

    /* get data node list from dn_handles */
    dnlist = GetNodeGroupNodeList(nodedata->group_members, nodedata->group_count);
    PGXCNodeAllHandles* pgxc_handles = WLMRemoteNodeAcquireConn(dnlist, POOL_NODE_DN);
    list_free_ext(dnlist);

    if (pgxc_handles == NULL) {
        return;
    }

    int datanode_count = pgxc_handles->dn_conn_count;

    /* send request to each data node */
    for (i = 0; i < datanode_count; ++i) {
        PGXCNodeHandle* dn_handle = pgxc_handles->datanode_handles[i];

        if (dn_handle == NULL) {
            continue;
        }

        dn_handle->state = DN_CONNECTION_STATE_IDLE;

        /* If a node has a connection error, we have to temporarily ignore the node */
        if (pgxc_node_dywlm_send_record(dn_handle, tag, keystr) != 0) {
            Oid nodeoid = dn_handle->nodeoid;
            release_pgxc_handles(pgxc_handles);

            ereport(ERROR,
                (errcode(ERRCODE_INVALID_OPERATION),
                    errmsg("Parallel Function: Failed to send command to Datanode %u", nodeoid)));
        }
        /* database restart need collect memory info quickly */
        if (!g_instance.wlm_cxt->dynamic_memory_collected) {
            break;
        }
    }

    for (i = 0; i < datanode_count; ++i) {
        PGXCNodeHandle* dn_handle = pgxc_handles->datanode_handles[i];

        if (dn_handle == NULL) {
            continue;
        }

        bool hasError = false;
        bool isFinished = false;

        /* receive workload records from each node */
        while (!pgxc_node_receive(1, &dn_handle, &timeout)) {
            char* msg = NULL;
            int len;

            char msg_type = get_message(dn_handle, &len, &msg);

            /* check message type */
            switch (msg_type) {
                case '\0': /* message is not completed */
                case 'E':  /* DN already down or crashed */
                    hasError = true;
                    break;
                case 'c': /* get workload record */
                {
                    StringInfoData input_msg;
                    initStringInfo(&input_msg);

                    appendBinaryStringInfo(&input_msg, msg, len);

                    parse_func(&input_msg, suminfo, size);

                    pq_getmsgend(&input_msg);

                    pfree(input_msg.data);

                    break;
                }
                case 'f': /* message is completed */
                {
                    int retcode;

                    errno_t errval = memcpy_s(&retcode, sizeof(int), msg, 4);
                    securec_check_errval(errval, , LOG);
                    retcode = (int)ntohl(retcode);

                    isFinished = true;

                    break;
                }
                case 'Z':
                    if (hasError) {
                        ereport(LOG,
                            (errmsg("get message from node %u failed %s",
                                dn_handle->nodeoid,
                                dn_handle->error ? dn_handle->error : "")));
                        isFinished = true;
                    }

                    break;
                case 'A': /* NotificationResponse */
                case 'S': /* SetCommandComplete */
                {
                    /*
                     * Ignore these to prevent multiple messages, one from
                     * each node. Coordinator will send on for DDL anyway
                     */
                    break;
                }
                default: /* never get here */
                    break;
            }

            if (isFinished) {
                break;
            }
        }

        dn_handle->state = DN_CONNECTION_STATE_IDLE;
        if (!g_instance.wlm_cxt->dynamic_memory_collected) {
            break;
        }
    }

    /* set combiner to NULL in case these connections will be reused later */
    for (int i = 0; i < datanode_count; ++i) {
        PGXCNodeHandle* dn_handle = pgxc_handles->datanode_handles[i];
        dn_handle->combiner = NULL;
    }

    release_pgxc_handles(pgxc_handles);

    return;
}

/*
 * @Description: execute sql on remote node
 * @IN pgxc_handles: pgxc handles
 * @IN sql: sql to execute
 * @IN nodeid: node id for executing sql
 * @RETURN: connection handle
 * @See also:
 */
void WLMRemoteNodeExecuteSql(const char* sql, int nodeid)
{
    TupleTableSlot* scanslot = NULL;
    int conn_count = 1;

    RemoteQueryState* remotestate = CreateResponseCombinerForBarrier(0, COMBINE_TYPE_SAME);

    if (nodeid == 0) {
        nodeid = u_sess->pgxc_cxt.PGXCNodeId;
    }

    if (nodeid <= 0 || nodeid > u_sess->pgxc_cxt.NumCoords) {
        return;
    }

    List* cnlist = NULL;
    cnlist = lappend_int(cnlist, nodeid - 1);
    PGXCNodeAllHandles* pgxc_handles = WLMRemoteNodeAcquireConn(cnlist, POOL_NODE_CN);
    list_free(cnlist);

    if (pgxc_handles == NULL) {
        return;
    }

    PGXCNodeHandle* cn_handle = pgxc_handles->coord_handles[0];

    /* If a node has a connection error, we have to temporarily ignore the node */
    if (cn_handle != NULL) {
        cn_handle->state = DN_CONNECTION_STATE_IDLE;
        (void)pgxc_node_send_query(cn_handle, sql);
    }

    while (conn_count > 0) {
        int j = 0;

        if (pgxc_node_receive(conn_count, pgxc_handles->coord_handles, NULL)) {
            ereport(LOG, (errmsg("%s:%d recv fail", __FUNCTION__, __LINE__)));
            break;
        }
        while (j < conn_count) {
            int res = handle_response(cn_handle, remotestate);
            if (res == RESPONSE_EOF) {
                j++;
            } else if (res == RESPONSE_COMPLETE) {
                conn_count--;
            } else if (res == RESPONSE_TUPDESC) {
                /*
                 * Now tuple table slot is responsible for freeing the descriptor
                 */
                if (scanslot == NULL) {
                    scanslot = MakeSingleTupleTableSlot(remotestate->tuple_desc);
                } else {
                    ExecSetSlotDescriptor(scanslot, remotestate->tuple_desc);
                }
            } else if (res == RESPONSE_DATAROW) {
                /*
                 * We already have a tuple and received another one.
                 */
                FetchTuple(remotestate, scanslot);
            }
        }
    }
    release_pgxc_handles(pgxc_handles);
}
