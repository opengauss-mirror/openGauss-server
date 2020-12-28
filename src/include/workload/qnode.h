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
 * qnode.h
 *     definitions for query node information in workload manager
 * 
 * IDENTIFICATION
 *        src/include/workload/qnode.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef QNODE_H
#define QNODE_H

typedef struct ResourcePool {
    Oid rpoid;                      /* resource pool id */
    Oid parentoid;                  /* resource pool id */
    int ref_count;                  /* ref count of the resource pool */
    int active_points;              /* the count of active statements in the resource pool */
    int server_actpts;              /* server active points */
    int running_count;              /* the count of running statements */
    int waiting_count;              /* the count of waiting statements */
    int running_count_simple;       /* the count of running statements for simple queries */
    int32 iops_limits;              /* iops limit for each resource pool */
    int io_priority;                /* io_percent for each resource pool */
    int memsize;                    /* setting memory size */
    int memused;                    /* used memory size */
    int mempct;                     /* setting percent */
    int actpct;                     /* real percent */
    int peakmem;                    /* peak used memory size */
    int estmsize;                   /* estimated memory size */
    bool is_dirty;                  /* resource pool is dirty, it will be removed while it's dirty */
    bool reset;                     /* reset */
    bool is_foreign;                /* indicate that the resource pool is used for foreign users */
    List* waiters;                  /* list for statements are waiting in resource pool */
    List* waiters_simple;           /* list for statements are waiting in resource pool for simple queries */
    void* node_group;               /* node group pointer */
    List* entry_list;               /* Used to save the foreign query list on datanode */
    struct ResourcePool* foreignrp; /* foreign resource pool of one parent  */
    struct ResourcePool* parentrp;  /* foreign resource pool of one parent  */

    char cgroup[NAMEDATALEN]; /* cgroup information */
    char ngroup[NAMEDATALEN]; /* nodegroup information */

    pthread_mutex_t mutex; /* mutex to lock workload group */
} ResourcePool;

typedef struct WLMQNodeInfo {
#ifndef WIN32
    pthread_cond_t condition; /* pthread condtion */
#endif
    Oid userid;               /* The user id of the thread */
    bool removed;             /* whether is removed from the waiting list */
    bool privilege;           /* the query has privilege? */
    uint64 sessid;            /* session id of current session */
    int priority;             /* priority of the query */
    int max_pts;              /* max pts for complicate or max stmt for simple */
    int act_pts;              /* stmt active points */
    ListCell* lcnode;         /* head of the queue */
    ResourcePool* rp;         /* resource pool of the node */
} WLMQNodeInfo;

#endif
