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
 * dywlm_client.h
 *     definitions for parallel control functions
 * 
 * 
 * IDENTIFICATION
 *        src/include/workload/dywlm_client.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DYWLM_CLIENT1_H
#define DYWLM_CLIENT1_H

#include "utils/hsearch.h"
#include "nodes/pg_list.h"
#include "workload/gscgroup.h"
#include "workload/statctl.h"
#include "workload/parctl.h"
#include "executor/exec/execdesc.h"

#define DYWLM_SEND_OK -5000
#define DYWLM_SEND_FAILED -5001
#define DYWLM_NO_NODE -5002
#define DYWLM_NO_RECORD -5003
#define DYWLM_NO_NGROUP -5004

#define DYWLM_TOP_QUOTA 90
#define DYWLM_HIGH_QUOTA 80
#define DYWLM_MEDIUM_QUOTA 60
#define DYWLM_LOW_QUOTA 10

typedef struct ClientDynamicManager {
    int active_statements;          /* the number of active statements */
    int max_info_count;             /* max info count in the hash table */
    int max_active_statements;      /* global max active statements */
    int max_support_statements;     /* max supported statement count */
    int max_statements;             /* max statements in active */
    int current_support_statements; /* current statements who occupy memory */
    int central_waiting_count;      /* count number in central queue waiting list */

    int usedsize; /* used memory for client */
    int cluster;  /* whether is cluster */
    int freesize; /* free memory size */

    int running_count; /* count of complicated query */

    bool recover; /* recovering mode */

    char* group_name; /* group name */
    void* srvmgr;     /* server manager */

    HTAB* dynamic_info_hashtbl;           /* resource pool is stored in this hash table*/
    List* statements_waiting_list;        /* list for statements are waiting to do due to active statements over
                                             max_active_statements */
    pthread_mutex_t statement_list_mutex; /* a mutex to lock statements waiting list */
} ClientDynamicManager;

struct DynamicInfoNode {
    Qid qid;           /* a id of a query */
    ThreadId threadid; /* thread id for a query */

    int memsize;     /* memory size to reserve */
    int max_memsize; /* max memory size to reserve */
    int min_memsize; /* min memory size to reserve */
    int priority;    /* priority */
    int actpts;      /* active points for a query */
    int max_actpts;  /* max active points for a query */
    int min_actpts;  /* min active points for a query */
    int maxpts;      /* max active points for a query in the resource pool */

    pthread_cond_t condition; /* The condition of the node to wake up */
    pthread_mutex_t mutex;    /* mutex to wake up thread */

    char rpname[NAMEDATALEN]; /* resource pool name */
    char ngroup[NAMEDATALEN]; /* node group name */

    bool wakeup;    /* whether is woken up */
    bool is_dirty;  /* whether is dirty */
    bool use_planb; /* whether use plan b */
};

struct DynamicMessageInfo {
    Qid qid;         /* a id of a query */
    int memsize;     /* memory size to reserve */
    int max_memsize; /* max memory size to reserve */
    int min_memsize; /* min memory size to reserve */
    int priority;    /* priority */
    int actpts;      /* active points for a query */
    int max_actpts;  /* max active points for a query */
    int min_actpts;  /* min active points for a query */
    int maxpts;      /* max active points for a query in the resource pool */

    ParctlType qtype;  /* parallel control type */
    EnqueueType etype; /* enqueue type */

    bool isserver;    /* check the message whether is from server */
    bool use_max_mem; /* check whether use maximum memsize to execute */
    bool subquery;    /* check whether is subquery */
    bool insubquery;  /* check whether is the first subquery in one query*/

    char rpname[NAMEDATALEN];    /* resource pool name */
    char nodename[NAMEDATALEN];  /* node name */
    char groupname[NAMEDATALEN]; /* node group name */
};

struct DynamicWorkloadRecord {
    Qid qid; /* a id of a query */

    int memsize;     /* memory size reserved for a query */
    int max_memsize; /* max memory size reserved for a query */
    int min_memsize; /* min memory size reserved for a query */
    int actpts;      /* active points for a query */
    int max_actpts;  /* max active points for a query */
    int min_actpts;  /* min active points for a query */
    int maxpts;      /* max active points for a resource pool */
    int priority;    /* priority of the record */

    ParctlType qtype;   /* parallel control type */
    WLMListNode* pnode; /* head of the queue, point to priority queue node */

    char rpname[NAMEDATALEN];    /* resource pool name */
    char nodename[NAMEDATALEN];  /* node name */
    char groupname[NAMEDATALEN]; /* node group name */

    bool try_wakeup;    /* trying to wake up the query */
    bool removed;       /* query already be removed */
    bool is_dirty;      /* record is dirty */
    bool maybe_deleted; /* the record is already deleted probably */
    bool use_planb;     /* check whether use planb to execute */

    bool equals(const Qid* qid)
    {
        return IsQidEqual(&this->qid, qid); /* check qid is matched */
    }
};

extern ClientDynamicManager g_client_dywlm;
extern THR_LOCAL bool WLMProcessExiting;

extern void dywlm_client_receive(StringInfo msg);
extern void dywlm_client_release(ParctlState* state);
extern void dywlm_client_reserve(void);
extern void dywlm_parallel_ready(const char* sqltext);
extern int dywlm_client_post(ClientDynamicManager*, const DynamicMessageInfo* msginfo);
extern int dywlm_get_central_node_idx(void);
extern void dywlm_client_write_catalog(StringInfo input_message);
extern void dywlm_client_recover(ClientDynamicManager*);
extern void dywlm_client_max_reserve(void);
extern void dywlm_client_max_release(ParctlState* state);
extern void dywlm_update_max_statements(int active_stetements);

extern char* dywlm_get_node_name(Oid nodeidx, char* nodename);
extern int dywlm_get_node_idx(const char* nodename);
extern int dywlm_client_physical_info(int* total_mem, int* free_mem);
extern DynamicWorkloadRecord* dywlm_get_records(int* num);

extern void dywlm_client_move_node_to_list(void* ng, ThreadId tid, const char* cgroup);
extern int dywlm_client_jump_queue(ClientDynamicManager*, ThreadId tid);
extern bool dywlm_client_is_cluster();
extern int64 dywlm_client_get_memory(void);
extern int64 dywlm_client_get_max_memory(bool* use_tenant);
extern int64 dywlm_client_get_free_memory(void);
extern void dywlm_client_get_memory_info(int* total_mem, int* free_mem, bool* use_tenant);
extern void dywlm_client_set_respool_memory(int size, WLMStatusTag);
extern void dywlm_client_clean(void* ptr);
extern void dywlm_client_manager(QueryDesc* queryDesc, bool isQueryDesc = true);
extern void dywlm_client_verify_register(void);
extern int dywlm_get_cpu_count(void);
extern int dywlm_get_cpu_util(void);
extern int dywlm_get_active_statement_count(void);

extern void dywlm_client_proc_release(void);

extern void dywlm_client_display_climgr_info(StringInfo strinfo);
bool dywlm_is_local_node();
#endif
