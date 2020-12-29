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
 * workload.h
 *     definitions for workload manager
 * 
 * IDENTIFICATION
 *        src/include/workload/workload.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef WORKLOAD_H
#define WORKLOAD_H

#include "postgres.h"
#include "knl/knl_variable.h"

#include "gs_threadlocal.h"
#include "nodes/parsenodes.h"
#include "nodes/nodes.h"
#include "nodes/plannodes.h"
#include "utils/datetime.h"
#include "utils/portal.h"
#include "utils/palloc.h"
#include "postmaster/postmaster.h"

#include "workload/gscgroup.h"
#include "workload/ctxctl.h"
#include "workload/parctl.h"
#include "workload/statctl.h"
#include "workload/memctl.h"
#include "workload/dywlm_client.h"
#include "workload/dywlm_server.h"
#include "workload/ioschdl.h"
#include "workload/cpwlm.h"

#define FULL_PERCENT 100
#define OTHER_USED_PERCENT 60 /* 60 aviable memory for other uses */

#define STATEMENT_MIN_MEM 256             /*MB*/
#define STATEMETN_MIN_MODIFY_MEM 2 * 1024 /*MB, 2gb*/
#define MEM_THRESHOLD 32                  /*MB*/
#define SIMPLE_THRESHOLD (32 * 1024)      /* 32MB */
#define HOLD_NODE_GROUP(group) gs_lock_test_and_set((int*)&group->used, 1)
#define RESUME_NODE_GROUP(group) gs_lock_test_and_set((int*)&group->used, 0)

#define NodeGroupIsDefault(group) (NULL == (void*)group || *group == '\0' || strcmp(group, "installation") == 0)

#define ENABLE_WORKLOAD_CONTROL \
    (u_sess->attr.attr_resource.use_workload_manager || u_sess->attr.attr_resource.bypass_workload_manager)

#define DY_MEM_ADJ(stmt)                                                                 \
    g_instance.wlm_cxt->dynamic_workload_inited&& t_thrd.wlm_cxt.parctl_state.enqueue && \
        (stmt)->query_mem[0] >= MEM_THRESHOLD * 1024L
#define ASSIGNED_QUERY_MEM(mem, max_mem) ((max_mem) > 0 ? Min((mem), (max_mem)) : (mem))
#define VALID_QUERY_MEM() \
    (ASSIGNED_QUERY_MEM(u_sess->attr.attr_sql.statement_mem, u_sess->attr.attr_sql.statement_max_mem) > 0)

extern THR_LOCAL bool log_workload_manager;

typedef enum CGSwitchState { CG_ORIGINAL = 0, CG_USERSET, CG_USING, CG_RESPOOL } CGSwitchState;

typedef enum DataSpaceType { SP_PERM = 0, SP_TEMP, SP_SPILL } DataSpaceType;

typedef struct UserData {
    Oid userid;                /* user id */
    bool is_super;             /* user is admin user */
    bool is_dirty;             /* userinfo is dirty, temorarily used in BuildUserInfoHash*/
    unsigned char adjust;      /* adjust space flag */
    unsigned char keepdata;    /* keep memory size */
    int64 totalspace;         /* user used total space */
    int64 global_totalspace;  /* user used total space on all cns and dns in bytes*/
    int64 reAdjustPermSpace;  /* for wlm_readjust_user_space */
    int64 spacelimit;         /* user space limit in bytes*/
    bool spaceUpdate;          /* user used total space or temp space updated ? */
    int64 tmpSpace;           /* user used temp space in bytes */
    int64 globalTmpSpace;     /* user used temp space on all cns and dns in bytes */
    int64 reAdjustTmpSpace;   /* for wlm_readjust_user_space */
    int64 tmpSpaceLimit;      /* user temp space limit in bytes */
    int64 spillSpace;         /* user used spill space in bytes */
    int64 globalSpillSpace;   /* user used spill space on all cns and dns in bytes */
    int64 spillSpaceLimit;    /* user spill space limit in bytes */
    Oid rpoid;                 /* resource pool in htab */
    int memsize;               /* user used memory */
    int usedCpuCnt;            /* average used CPU counts */
    int totalCpuCnt;           /* average total CPU counts */
    volatile int referenceCnt; /* reference count, avoid wrong remove */
    ResourcePool* respool;     /* resource pool in htab */
    UserData* parent;          /* parent user data in htab */
    List* childlist;           /* child list */
    WLMUserInfo* infoptr;      /* user info */
    int query_count;           /* how many IO complicated queries are active now */
    WLMIoGeninfo ioinfo;       /* io info*/
    pthread_mutex_t mutex;     /* entry list mutex */
    List* entry_list;          /* session entry list */

    int compare(const Oid* userid)
    {
        if (this->userid == *userid)
            return 0;

        return ((this->userid < *userid) ? 1 : -1);
    }
} UserData;

/* used for BuildUserInfoHash function, avoid dead lock */
typedef struct TmpUserData {
    Oid             userid;             /* user id */
    Oid             puid;               /* parent id */
    bool            is_super;           /* user is admin user */
    bool            is_dirty;           /* userinfo is dirty, temorarily used in BuildUserInfoHash*/
    int64          spacelimit;         /* user space limit in bytes*/
    int64          tmpSpaceLimit;      /* user temp space limit in bytes */
    int64          spillSpaceLimit;    /* user spill space limit in bytes */
    Oid             rpoid;              /* resource pool in htab */
    ResourcePool   *respool;            /* resource pool in htab */
} TmpUserData;

typedef struct UserResourceData {
    Oid userid;               /* user oid */
    int total_memory;         /* user total memroy */
    int used_memory;          /* user used memory */
    int total_cpuset;         /* user total cpuset */
    int used_cpuset;          /* user used cpuset */
    int64 total_space;       /* user total space limit */
    int64 used_space;        /* user used space */
    int64 total_temp_space;  /* user total temp space limit */
    int64 used_temp_space;   /* user used temp space */
    int64 total_spill_space; /* user total spill space limit */
    int64 used_spill_space;  /* user used spill space */

    /* io collect information for user */
    int mincurr_iops; /* user min current iops */
    int maxcurr_iops; /* user max current iops */
    int minpeak_iops; /* user min peak iops */
    int maxpeak_iops; /* user max peak iops */

    int iops_limits;     /* iops_limits for user */
    int io_priority;     /* io_priority for user */
    int curr_iops_limit; /* iops_limit calculated from io_priority */
    /* IO flow data */
    uint64 read_bytes;   /* read bytes during the monitor interval */
    uint64 write_bytes;  /* write bytes during the monitor interval */
    uint64 read_counts;  /* read counts during the monitor interval */
    uint64 write_counts; /* write counts during the monitor interval */
    uint64 read_speed;   /* read speed */
    uint64 write_speed;  /* write speed */
} UserResourceData;

typedef struct WLMNodeGroupInfo {
    char group_name[NAMEDATALEN]; /* node group name */

    int used_memory;     /* node used memory */
    int total_memory;    /* node total memory */
    int estimate_memory; /* node estimate memory */

    int min_freesize; /* minimum free size of nodes */

    unsigned int used; /* if it is in use */

    ClientDynamicManager climgr; /* client manager */
    ServerDynamicManager srvmgr; /* server manager */
    ParctlManager parctl;        /* static parallel manager */

    gscgroup_grp_t* vaddr[GSCGROUP_ALLNUM]; /* control group info */
    HTAB* cgroups_htab;                     /* cgroup hash table of the node group */
    pthread_mutex_t cgroups_mutex;          /* not used */

    List* node_list; /* node list of the node group */

    ResourcePool* foreignrp; /* foreign resource pool */

    bool is_dirty; /* the node group is dirty */

} WLMNodeGroupInfo;

/* debug info */
typedef struct WLMDebugInfo {
    WLMGeneralParam* wparams;  /* debug info for general params */
    ExceptionManager* statctl; /* debug info for statctl manager */
    WLMCollectInfo* colinfo;   /* debug info for collect info */

    ParctlState* pstate;          /* debug info for parctl state */
    ParctlManager* parctl;        /* debug info for parctl manager */
    ClientDynamicManager* climgr; /* debug info for client manager */
    ServerDynamicManager* srvmgr; /* debug info for server manager */

    int active_statement;
    bool* reserved_in_transaction;

} WLMDebugInfo;

typedef struct knl_g_wlm_context {
    /* The default node group when there is no logical cluster */
    WLMNodeGroupInfo MyDefaultNodeGroup;

    /* the node group information for foreign users */
    WLMNodeGroupInfo* local_dn_nodegroup;

    /* the global variable of workload manager memory context */
    MemoryContext workload_manager_mcxt;

    /* the global variable of query resource track memory context */
    MemoryContext query_resource_track_mcxt;

    /* the global variable of operator resource track memory context */
    MemoryContext oper_resource_track_mcxt;

    /* stat manager */
    WLMStatManager stat_manager;

    /* dn instance statistics manager */
    WLMInstanceStatManager instance_manager;

    /* resource pool is stored in this hash table*/
    HTAB* resource_pool_hashtbl;

    /* flag to enable dynamic workload */
    bool dynamic_workload_inited;

    /* Dynamic DNs memory collected flag */
    bool dynamic_memory_collected = false;

    /* the memory unused in parallel control */
    int parctl_process_memory;

    /* cgroup initialized completed */
    int gscgroup_init_done;

    /* config parsed completed */
    int gscgroup_config_parsed;

    /* CPU count */
    int gscgroup_cpucnt;

    /* the node group for local datanode  */
    char local_dn_ngname[NAMEDATALEN];

    /* configure file */
    gscgroup_grp_t* gscgroup_vaddr[GSCGROUP_ALLNUM];

    /* vacuum cgroup structure */
    struct cgroup* gscgroup_vaccg;

    /* default backend cgroup structure */
    struct cgroup* gscgroup_bkdcg;

    /* root cgroup structure */
    struct cgroup* gscgroup_rootcg;

    /* default workload cgroup structure */
    struct cgroup* gscgroup_defwdcg;

    /* default topwd cgroup structure */
    struct cgroup* gscgroup_deftopwdcg;

    /* used for io scheduler */
    WLMIOContext io_context;

    // cluster state
    struct DNState* cluster_state;

    // dn count in cluster state
    int dnnum_in_cluster_state;

    // indicate if the node is cnn
    bool is_ccn;

    // the index count of cnn
    int ccn_idx;

    /* record the resource package number in datanode */
    int rp_number_in_dn;
} knl_g_wlm_context;

typedef struct knl_u_wlm_context {
    /* save the Cgroup name */
    char control_group[NAMEDATALEN];

    /* indicate if the name of control_group has changed */
    CGSwitchState cgroup_state;

    /* save control group switch status */
    gscgroup_stmt_t cgroup_stmt;
    gscgroup_stmt_t cgroup_last_stmt;

    /* if the session resource pool is changed */
    bool session_respool_switch;

    /* session_respool is initialized or not */
    bool session_respool_initialize;

    /* get is_foreign for updating hash table */
    bool respool_is_foreign;

    /* flag to update user hash table */
    bool wlmcatalog_update_user;

    /*cancel job due to workload manager exception*/
    bool cancel_from_wlm;

    /*cancel job due to space limitation*/
    bool cancel_from_space_limit;

    /*cancel job due to transcation readonly */
    bool cancel_from_defaultXact_readOnly;

    /* save the session_respool name */
    char session_respool[NAMEDATALEN];

    /* get oid of the created resource pool for updating hash table */
    Oid respool_create_oid;

    /* get oid of the altered resource pool for updating hash table */
    Oid respool_alter_oid;

    /*record last stmt session respool*/
    Oid respool_old_oid;

    /* get oid list of the resource pool dropping list for updating hash table */
    List* respool_delete_list;

    /* mempct for updating hash table */
    int respool_foreign_mempct;

    /* actual percent for updating hash table */
    int respool_foreign_actpct;

    /* parentid for updating hash table */
    Oid respool_parentid;

    /* get io_limits for updating hash table */
    int32 respool_io_limit_update;

    /* get io_priority for updating hash table */
    char respool_io_pri_update[NAMEDATALEN];

    /* get io_priority for updating hash table */
    char respool_nodegroup[NAMEDATALEN];

    /* get io_priority for updating hash table */
    char respool_controlgroup[NAMEDATALEN];

    /* which node group resource pool belong to */
    WLMNodeGroupInfo* respool_node_group;

    /* local foreign respool */
    ResourcePool* local_foreign_respool;

    /* workload debug info */
    WLMDebugInfo wlm_debug_info;

    /* workload session info */
    WLMGeneralParam wlm_params;

    /* the num of streams' thread */
    int wlm_num_streams;

    /* the keyname of the control group */
    char group_keyname[GPNAME_LEN];

    /* indicate if it is in transcation */
    bool is_reserved_in_transaction;

    /* indicate if the parallel queue is reset */
    bool is_active_statements_reset;

    /* indicate if the query is forced to run */
    bool forced_running;

    /* indicate if the query has running one time */
    bool query_count_record;

    /* record if the computing pool task is running */
    bool cp_task_running;

    /* reserved count in global active statements */
    int reserved_in_active_statements;

    /* reserved count in resource pool active statements */
    int reserved_in_group_statements;

    /* reserved count for simple query in resource pool active statements */
    int reserved_in_group_statements_simple;

    /* the count which is waiting in resouce pool */
    int reserved_in_respool_waiting;

    int reserved_in_central_waiting;

    /* used to record the user when inserting data */
    UserData* spmgr_userdata;

    HTAB* TmptableCacheHash;

    /* computing pool runtime information */
    CPRuntimeInfo* cp_runtime_info;

    /* Track storage usage in bytes at individual thread level when space increasing */
    uint64 spmgr_space_bytes;

    /* Mark query reach the spill limit */
    bool spill_limit_error;

    /* user pl */
    int64 wlm_userpl;

    /* the reserved query string */
    char reserved_debug_query[1024];

    unsigned char stroedproc_rp_reserve;

    unsigned char stroedproc_rp_release;

    unsigned char stroedproc_release;

    /* mark the session already do paralle control */
    unsigned char parctl_state_control;

    /* mark the session already exit */
    unsigned char parctl_state_exit;
} knl_u_wlm_context;

extern void dywlm_client_init(WLMNodeGroupInfo*);
extern void dywlm_server_init(WLMNodeGroupInfo*);
extern void WLMParctlInit(WLMNodeGroupInfo*);
extern void InitializeWorkloadManager(void);

extern void SetCpuAffinity(int64 setting);

extern void WLMSetControlGroup(const char* cgname);
extern gscgroup_stmt_t WLMIsSpecialCommand(const Node* parsetree, const Portal);
extern bool WLMIsSimpleQuery(const QueryDesc* queryDesc, bool force_control, bool isQueryDesc);
extern bool WLMNeedTrackResource(const QueryDesc* queryDesc);
extern bool WLMIsSpecialQuery(const char* query);
extern unsigned char WLMCheckToAttachCgroup(const QueryDesc* queryDesc);
extern void WLMSetUserInfo();
extern void GetCurrentCgroup(char* cgroup, const char* curr_group, int len);
extern void WLMSwitchCGroup(void);
extern char* GetMemorySizeWithUnit(char* memory, int size, int memsize);
extern bool CheckWLMSessionInfoTableValid(const char* tablename);
extern WLMUserInfo* WLMGetUserInfo(Oid userid, WLMUserInfo* info);
extern int ParseUserInfoConfigFile(void);
extern UserData* GetUserDataFromHTab(Oid roleid, bool is_noexcept);
extern bool BuildUserRPHash(void);
extern void* WLMGetAllUserData(int* num); /* get all user data from htab */
extern void WLMReAdjustUserSpace(UserData* userdata, bool isForce = false);
extern char* GenerateResourcePoolStmt(CreateResourcePoolStmt* stmt, const char* origin_query);

extern void CreateResourcePool(CreateResourcePoolStmt* stmt);
extern void AlterResourcePool(AlterResourcePoolStmt* stmt);
extern void RemoveResourcePool(Oid pool_oid);
extern void DropResourcePool(DropResourcePoolStmt* stmt);

extern void CreateWorkloadGroup(CreateWorkloadGroupStmt* stmt);
extern void AlterWorkloadGroup(AlterWorkloadGroupStmt* stmt);
extern void RemoveWorkloadGroup(Oid group_oid);
extern void DropWorkloadGroup(DropWorkloadGroupStmt* stmt);

extern void CreateAppWorkloadGroupMapping(CreateAppWorkloadGroupMappingStmt* stmt);
extern void AlterAppWorkloadGroupMapping(AlterAppWorkloadGroupMappingStmt* stmt);
extern void RemoveAppWorkloadGroupMapping(Oid app_oid);
extern void DropAppWorkloadGroupMapping(DropAppWorkloadGroupMappingStmt* stmt);

extern bool UsersInOneGroup(UserData* user1, UserData* user2);
extern void CheckUserRelation(Oid roleid, Oid parentid, Oid rpoid, bool isDefault, int issuper);
extern int UserGetChildRoles(Oid roleid, DropRoleStmt* stmt);
extern void GetUserDataFromCatalog(Oid userid, Oid* rpoid, Oid* parentid, bool* issuper, int64* spacelimit,
    int64* tmpspacelimit, int64* spillspacelimit, char* groupname = NULL, int lenNgroup = 0);
extern bool GetUserChildlistFromCatalog(Oid userid, List** childlist, bool findall);
extern UserResourceData* GetUserResourceData(const char* username);
extern void CheckUserSpaceLimit(Oid roleid, Oid parentid, int64 spacelimit, int64 tmpspacelimit,
    int64 spillspacelimit, bool is_default, bool changed, bool tmpchanged, bool spillchanged);
extern ResourcePool* GetRespoolFromHTab(Oid rpoid, bool is_noexcept = true);
extern void perm_space_increase(Oid ownerID, uint64 size, DataSpaceType type);
extern void perm_space_decrease(Oid ownerID, uint64 size, DataSpaceType type);
extern void perm_space_value_reset(void);
extern bool SearchUsedSpace(Oid userID, int64* permSpace, int64* tempSpace);
extern void UpdateUsedSpace(Oid userID, int64 permSpace, int64 tempSpace);

extern void CheckUserInfoHash();
extern void UpdateWlmCatalogInfoHash(void);
extern void ResetWlmCatalogFlag(void);
extern unsigned int GetRPMemorySize(int pct, Oid parentid);

extern void WLMCheckSessionRespool(const char* respool);
extern void WLMSetSessionRespool(const char* respool);

extern int WLMGetUserMemory(UserData* userdata);
extern void WLMCheckSpaceLimit(void);
extern WLMNodeGroupInfo* WLMGetNodeGroupFromHTAB(const char* group_name);
extern WLMNodeGroupInfo* WLMMustGetNodeGroupFromHTAB(const char* group_name);
extern WLMNodeGroupInfo* WLMGetNodeGroupByUserId(Oid userid);
extern WLMNodeGroupInfo* CreateNodeGroupInfoInHTAB(const char* group_name);
extern void RemoveNodeGroupInfoInHTAB(const char* group_name);
extern void WLMInitNodeGroupInfo(WLMNodeGroupInfo* info);

/* get debug info string */
extern void WLMGetDebugInfo(StringInfo strinfo, WLMDebugInfo* debug_info);
/* init cgroup */
extern void gscgroup_init(void);

/* free cgroup */
extern void gscgroup_free(void);

/* get the percent of specified group */
extern int gscgroup_get_percent(WLMNodeGroupInfo* ng, const char* gname);

/* attach task into cgroup */
extern void gscgroup_attach_task(WLMNodeGroupInfo* ng, const char* gname);

/* attach batch task into cgroup with tid */
extern int gscgroup_attach_task_batch(WLMNodeGroupInfo* ng, const char* gname, pid_t tid, int* is_first);

/* attach backend task into cgroup */
extern int gscgroup_attach_backend_task(const char* gname, bool is_noexcept);

/* check if the group has been set */
extern int gscgroup_check_group_name(WLMNodeGroupInfo* ng, const char* gname);

/* check if the node in the waiting list need switch */
extern int gscgroup_check_group_percent(WLMNodeGroupInfo* ng, const char* gname);

/* switch cgroup */
extern bool WLMAjustCGroupByCNSessid(WLMNodeGroupInfo* ng, uint64 sess_id, const char* cgroup);

/* get cgroup name from cgroup */
char* gsutil_get_cgroup_name(struct cgroup* cg);

/* get the group configuration information */
extern gscgroup_grp_t* gscgroup_get_grpconf(WLMNodeGroupInfo* ng, const char* gname);

/* check if the cgroup is class cgroup */
extern int gscgroup_is_class(WLMNodeGroupInfo* ng, const char* gname);

/* switch to TopWD cgroup */
extern void gscgroup_switch_topwd(WLMNodeGroupInfo* ng);

/* Back to Current cgroup */
extern void gscgroup_switch_vacuum(void);

/* update the hash table */
extern void gscgroup_update_hashtbl(WLMNodeGroupInfo* ng, const char* keyname);

/* get group entry from hash table */
extern gscgroup_entry_t* gscgroup_lookup_hashtbl(WLMNodeGroupInfo* ng, const char* name);

/* get the next group */
extern int gscgroup_get_next_group(WLMNodeGroupInfo* ng, char* gname);

/* get the top group name */
extern char* gscgroup_get_top_group_name(WLMNodeGroupInfo* ng, char* gname, int len);

/* look up cgroup from htab */
extern struct cgroup* gscgroup_lookup_cgroup(WLMNodeGroupInfo* ng, const char* gname, bool* found = NULL);

/* update cgroup cpu info */
extern void gscgroup_update_hashtbl_cpuinfo(WLMNodeGroupInfo* ng);

/* get cgroup info */
extern gscgroup_info_t* gscgroup_get_cgroup_info(int* num);

/* get cgroup cpu info */
extern void gscgroup_get_cpuinfo(gscgroup_entry_t* entry);

/* get cpu usage percent */
extern int gscgroup_get_cpu_usage_percent(WLMNodeGroupInfo* ng, const char* gname);

/* check the cgroups whether are in the same class */
extern bool gscgroup_is_brother_group(const char* cgroup1, const char* cgroup2);

/* check whether parentcg is parent cgroup of childcg */
extern bool gscgroup_is_child_group(const char* parentcg, const char* childcg);

/* check whether gname is timeshare or not */
extern int gscgroup_is_timeshare(const char* gname);

/* convert cgroup name to tthe name without level info */
extern char* gscgroup_convert_cgroup(char* gname);

/* check Cgroup percent */
extern int WLMCheckCgroupPercent(WLMNodeGroupInfo* ng, const char*);

/* move node to list */
extern void WLMMoveNodeToList(WLMNodeGroupInfo* ng, ThreadId tid, const char* cgroup);

/* release active statements while proc exiting */
extern void WLMReleaseAtThreadExit();

#endif
