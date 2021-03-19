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
 * dywlm_server.h
 *     definitions for parallel control functions
 * 
 * 
 * IDENTIFICATION
 *        src/include/workload/dywlm_server.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DYWLM_SERVER1_H
#define DYWLM_SERVER1_H

#define UPDATE_FREQUENCY 3 /*updating resource utilization in COMPUTATION ACCELERATION cluster*/

#define COMP_ACC_CLUSTER \
    g_instance.attr.attr_sql.enable_acceleration_cluster_wlm /*COMPUTATION ACCELERATION cluster flag*/

typedef struct ServerDynamicManager {
    int totalsize;       /* total memory size for server */
    int freesize;        /* free size for server */
    int freesize_inc;    /* free memory increment between booking and DNs */
    int freesize_update; /* freesize is updated */
    int freesize_limit;
    int rp_memsize;      /* memory size occupied on resource pool */
    int active_count;    /* COMPUTATION ACCELERATION cluster: active statement count */
    int statement_quota; /* COMPUTATION ACCELERATION cluster*/
    bool recover;        /* recovering mode */
    bool try_wakeup;     /* are waking up some requests*/

    char* group_name; /* group name */

    List* global_waiting_list; /* the queue complicate statements is waiting for waking up */
    HTAB* global_info_hashtbl; /* resource pool is stored in this hash table*/

    ClientDynamicManager* climgr; /* client manager of the current server */

    pthread_mutex_t global_list_mutex; /* a mutex to lock statements waiting list */
} ServerDynamicManager;

struct DynamicNodeData {
    char host[NAMEDATALEN]; /* node host */
    Oid* group_members;     /* group members of one node group */
    int group_count;        /* group count of one node group */
    int used_memory;        /* node used memory */
    int total_memory;       /* node total memory */
    int estimate_memory;    /* node estimate memory */

    int phy_totalmem;    /* physical total memory */
    int phy_freemem;     /* physical free memory */
    int phy_usemem_rate; /* physical memory use rate */

    int fp_memsize;  /* setting memory for foreign users */
    int fp_usedsize; /* memory used for foreign users */
    int fp_estmsize; /* memory used for foreign users */
    int fp_mempct;   /* memory used for foreign users */

    int cpu_util;  /* max physical cpu util of all nodes */
    int cpu_count; /* max physical cpu count of all nodes */
    int io_util;   /* max io count of all nodes */

    int min_freesize; /* minimum free size of nodes */

    int min_memutil; /* minimum memory usage rate */
    int max_memutil; /* maximum memory usage rate */
    int min_cpuutil; /* minimum cpu usage rate */
    int max_cpuutil; /* maximum cpu usage rate */
    int min_ioutil;  /* minimum io usage rate */
    int max_ioutil;  /* maximum io usage rate */

    HTAB* nodedata_htab; /* node data hash table */
};

extern Oid dywlm_get_node_id(Oid procid);
extern void dywlm_server_receive(StringInfo msg);

extern EnqueueType dywlm_server_reserve(ServerDynamicManager*, DynamicMessageInfo* msginfo);
extern EnqueueType dywlm_server_release(ServerDynamicManager*, const DynamicMessageInfo* msginfo);
extern EnqueueType dywlm_server_cancel(ServerDynamicManager*, DynamicMessageInfo* msginfo);
extern void dywlm_server_clean(const char* nodename);
extern EnqueueType dywlm_server_clean_internal(ServerDynamicManager*, const char* nodename);
extern EnqueueType dywlm_server_move_node_to_list(ServerDynamicManager*, const DynamicMessageInfo* msginfo);
extern EnqueueType dywlm_server_jump_queue(ServerDynamicManager*, const DynamicMessageInfo* msginfo);
extern void dywlm_node_recover(bool isForce);
extern DynamicWorkloadRecord* dywlm_server_get_records(ServerDynamicManager*, const char* nodename, int* num);
extern void dywlm_server_collector(void);
extern void dywlm_server_sync_records(void);
extern DynamicNodeData* dywlm_get_resource_info(ServerDynamicManager* srvmgr);
extern void dywlm_server_check_resource_pool(void);
extern void dywlm_server_get_respool_params(const char* rpname, int* running_count, int* waiting_count);

extern void dywlm_server_display_srvmgr_info(StringInfo strinfo);
extern void dywlm_server_display_respool_info(StringInfo strinfo);
#endif
