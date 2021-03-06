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
 * parctl.h
 *     definitions for parallel control functions
 * 
 * IDENTIFICATION
 *        src/include/workload/parctl.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef PARCTLS_H
#define PARCTLS_H
#define MAX_PARCTL_MEMORY ((unsigned int)maxChunksPerProcess << (chunkSizeInBits - BITS_IN_MB))
/*the process memory while query running */
#define PARCTL_PROCESS_MEMORY 32 /* MB */

/*the memory size of a query to start to do parallel control*/
#define PARCTL_MEMORY_UNIT 3 /* MB */

#define PARCTL_ACTIVE_PERCENT 80
#define PARCTL_ALL_PERCENT 100

typedef enum ParctlType {
    PARCTL_NONE = 0, /* no parallel control */
    PARCTL_RESERVE,  /* reserve resource request */
    PARCTL_GLOBAL,   /* global parallel control */
    PARCTL_RESPOOL,  /* resource pool parallel control */
    PARCTL_ACTIVE,   /* query is running */
    PARCTL_RELEASE,  /* release resource request */
    PARCTL_CANCEL,   /* cancel query request */
    PARCTL_CLEAN,    /* client recover request */
    PARCTL_SYNC,     /* server recover request */
    PARCTL_TRANSACT, /* query is in the transaction block */
    PARCTL_MVNODE,   /* move statement to new priority */
    PARCTL_JPQUEUE,  /* move statement to highest priority */
    PARCTL_TYPES,    /* count of types for parallel control, it's always the last one */
} ParctlType;

typedef enum EnqueueType {
    ENQUEUE_NONE = 0,  /* default state */
    ENQUEUE_BLOCK,     /* query should be blocked */
    ENQUEUE_ERROR,     /* query is in error state */
    ENQUEUE_NORESPOOL, /* target resource pool is not exist */
    ENQUEUE_MEMERR,    /* query is in error state due to out of memory */
    ENQUEUE_RECOVERY,  /* server is in recover state */
    ENQUEUE_CONNERR,   /* connect to target node failed */
    ENQUEUE_PRIVILEGE, /* has privilege to run */
    ENQUEUE_GROUPERR,  /* node group error */
    ENQUEUE_UNKNOWN    /* parctl type is not valid, unknown to handle it */
} EnqueueType;

typedef struct RespoolData {
    Oid rpoid;           /* resource pool oid*/
    int max_pts;         /* active statements for the resource pool */
    int max_stmt_simple; /* max statements for the resource pool for simple queries */
    int max_dop;         /* max dop */
    int mem_size;        /* memory pool size, kb */
    int act_pts;         /* active statement points */
    int max_act_pts;     /* active statement points */
    int min_act_pts;     /* active statement points */
    int iops_limits;     /* iops limits */
    int io_priority;     /* io priority */
    bool superuser;      /* Am I super user? */
    bool cgchange;       /* check cgroup whether is changed */
    char rpname[NAMEDATALEN];
    char* cgroup; /* control group in resource pool */
} RespoolData;

/* used for BuildResourcePoolHash function, avoid dead lock */
typedef struct TmpResourcePool
{
    Oid rpoid;                          /* resource pool id */
    Oid parentoid;                      /* resource pool id */
    int32 iops_limits;                    /* iops limit for each resource pool */
    int io_priority;                    /* io_percent for each resource pool */
    int mempct;                         /* setting percent */
    bool is_foreign;                     /* indicate that the resource pool is used for foreign users */

    char cgroup[NAMEDATALEN];            /* cgroup information */
    char ngroup[NAMEDATALEN];            /* nodegroup information */
} TmpResourcePool;

typedef struct ParctlManager {
    int max_active_statements;      /* global max active statements */
    int statements_waiting_count;   /* count of statements in waiting */
    int statements_runtime_count;   /* count of statements in running */
    int statements_runtime_plus;    /* increased of statements in running */
    int max_statements;             /* max statements in active */
    int max_support_statements;     /* max supported  statements */
    int current_support_statements; /* current supported  statements */
    int respool_waiting_count;      /* count of statements waiting in resource pool */

    List* statements_waiting_list; /* the queue complicate statements is waiting for waking up */

    HTAB* resource_pool_hashtbl; /* resource pool is stored in this hash table*/

    pthread_mutex_t statements_list_mutex; /* a mutex to lock statements waiting list */
} ParctlManager;

typedef struct WLMListNode {
    int data;           /* The priority of the node */
    List* request_list; /* list for statements with this priority in waiting */

    bool equals(const int* data)
    {
        return this->data == *data;
    }

    int compare(const int* data)
    {
        if (this->data == *data)
            return 0;

        return ((this->data < *data) ? 1 : -1);
    }
} WLMListNode;

extern bool IsQueuedSubquery(void);
extern void WLMParctlReady(const char*);
/*Reserve workload resource*/
extern void WLMParctlReserve(ParctlType);
/*Release workload resource*/
extern void WLMParctlRelease(ParctlState*);
/*Handle simple stmt except for dywlm*/
extern void WLMHandleDywlmSimpleExcept(bool proc_exit);
/*Initialize workload group hash table*/
extern void InitializeUserResourcePoolHashTable();
/* Switch node in the waiting list from old priority to new priority */
extern void WLMSwitchQNodeList(ParctlManager*, int priority_old, int priority_new);
extern void WLMCreateResourcePoolInHash(Oid);
extern bool WLMCheckResourcePoolIsIdle(Oid);

extern int WLMJumpQueue(ParctlManager*, ThreadId);
extern void WLMSetMaxStatements(int);
extern int WLMGetActiveStatments();
/* get resource pool data info from hash table */
extern void* WLMGetResourcePoolDataInfo(int* num);
extern ListCell* WLMSearchAndCheckMaxNode(const List* list);

extern int WLMGetMaxDop();
extern void WLMVerifyGlobalParallelControl(ParctlManager*);
extern int WLMGetMaxStatements(int active_statements);
extern void WLMCheckResourcePool();

extern void WLMProcReleaseActiveStatement(void);

extern int WLMReleaseGroupActiveStatement(void);
void WLMCheckDefaultXactReadOnly(void);

#endif
