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
 * statctl.h
 *     definitions for statistics control functions
 * 
 * IDENTIFICATION
 *        src/include/workload/statctl.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef STATCTL_H
#define STATCTL_H

#include "workload/parctl.h"
#include "instruments/list.h"
#define StringIsValid(pstr) (pstr && *pstr)
#define SqlIsValid(sql) (StringIsValid(sql) && strcmp(sql, ";") != 0)

#define KBYTES 1024
#define MBYTES (KBYTES * KBYTES)

#define MAX_SLEEP_TIME 5 /*seconds*/

#define STAT_PATH_LEN KBYTES
#define INFO_STR_LEN 94
/*
 * warning includes
 *    - 256 byte reserved for warnings from query-executor
 *    - 2kb reserved for warnings from query-optimizer
 */
#define WARNING_INFO_LEN 2304

#define WARNING_SPILL_SIZE 256     /* 256MB */
#define WARNING_BROADCAST_SIZE 100 /* 100MB */
#define WARNING_SPILL_TIME 3
#define WARNING_HASH_CONFLICT_LEN 256

#define GSUSER_ALLNUM 102400
#define PAST_TIME_COUNT 3
#define SIG_RECVDATA_COUNT 16
#define GSWLM_CONF_DIR "etc"
#define GSWLMCFG_PREFIX "gswlm"
#define GSCONF_NAME "userinfo"
#define MAX_DEVICE_LEN 64
#define ALL_USER "0000"  //"0000" is an invalid user name, means update all user used space

#define AVERAGE_VALUE(m, n, p) (((n) > (m)) ? (((int)((n) - (m))) / (p)) : (0))
#define GET_LARGER_VALUE(x, y) (((x) > (y)) ? (x) : (y))
#define PEAK_VALUE(start, end, intval, cmp) (GET_LARGER_VALUE(AVERAGE_VALUE((start), (end), (intval)), (cmp)))

/*get max and min data in the session info, min value maybe 0, so set -1 as init value*/
#define GetMaxAndMinCollectInfo(info, elem, data) \
    do {                                          \
        if (data > info->max##elem)               \
            info->max##elem = data;               \
        if (info->min##elem == -1)                \
            info->min##elem = data;               \
        else if (data < info->min##elem)          \
            info->min##elem = data;               \
    } while (0)

#define WLM_USER_RESOURCE_HISTORY "gs_wlm_user_resource_history"
#define WLM_INSTANCE_HISTORY "gs_wlm_instance_history"
#define WLM_EC_OPERATOR_INFO "gs_wlm_ec_operator_info"
#define WLM_OPERATOR_INFO "gs_wlm_operator_info"
#define WLM_SESSION_INFO "gs_wlm_session_query_info_all"

enum WLMActionTag {
    WLM_ACTION_REMAIN = 0, /* keep thread running */
    WLM_ACTION_ADJUST,     /* change control group of statement */
    WLM_ACTION_ABORT,      /* abort the statement */
    WLM_ACTION_FINISHED,   /* finish the statement */
    WLM_ACTION_FETCHSESS,  /* fetch session info */
    WLM_ACTION_REMOVED     /* info removed */
};

enum WLMStatusTag {
    WLM_STATUS_RESERVE = 0, /* a status before waiting in wlm queue */
    WLM_STATUS_PENDING,     /* waiting in the wlm queue */
    WLM_STATUS_RUNNING,     /* statement is running */
    WLM_STATUS_FINISHED,    /* statement is finished */
    WLM_STATUS_ABORT,       /* statement is abort */
    WLM_STATUS_RELEASE      /* wlm collect infomation is into db list or release statistics ctrl*/
};

enum WLMAttrTag {
    WLM_ATTR_NORMAL = 0, /* Normal is not into db */
    WLM_ATTR_INDB,       /* push collect infomation into db list */
    WLM_ATTR_INVALID     /* collect info is invalid */
};

enum WLMWarnTag {
    WLM_WARN_NONE = 0, /* no warn */
    WLM_WARN_SPILL,
    WLM_WARN_EARLY_SPILL,
    WLM_WARN_SPILL_TIMES_LARGE,
    WLM_WARN_SPILL_FILE_LARGE,
    WLM_WARN_BROADCAST_LARGE,
    WLM_WARN_SPILL_ON_MEMORY_SPREAD,
    WLM_WARN_HASH_CONFLICT
};

enum WLMExceptTag {
    WLM_EXCEPT_NONE = 0,
    WLM_EXCEPT_CPU,
    WLM_EXCEPT_SPOOL,
    WLM_EXCEPT_QUEUE_TIMEOUT,
    WLM_EXCEPT_RUN_TIMEOUT
};

struct Qid {
    Oid procId;        /* cn id for the statement */
    uint64 queryId;    /* query id for statement, it's a session id */
    TimestampTz stamp; /* query time stamp */
};

/* user info in the user config file */
struct WLMUserInfo {
    Oid userid;        /* user id */
    Oid rpoid;         /* resource pool id */
    Oid parentid;      /* parent user id */
    int childcount;    /* count of the child user */
    int64 space;      /* user space */
    int64 spacelimit; /* user space limit */
    int used;          /* user info is used */
    bool admin;        /* user is admin user? */
    char* children;    /* child list string */
};

/* general params */
struct WLMGeneralParam {
    Qid qid;                         /* a tag of a query */
    char cgroup[NAMEDATALEN];        /* cgroup name */
    char ngroup[NAMEDATALEN];        /* node group name */
    unsigned char cpuctrl;           /* under cpu control? */
    unsigned char memtrack;          /* track memory ? */
    unsigned char iotrack;           /* track IO? */
    unsigned char iocontrol;         /* need control IO ?*/
    unsigned char infoinit;          /* hash tab init */
    unsigned char iostate;           /* workload io state*/
    unsigned char complicate;        /* query is complicated */
    unsigned char complicate_stream; /* query is complicated & thread is stream */
    unsigned char privilege;         /* flags reserved */
    RespoolData rpdata;              /* resource pool in use */
    int memsize;                     /* statement memory size, mb */
    int max_memsize;                 /*max statement memory size */
    int min_memsize;                 /* min statement memory size */
    int dopvalue;                    /* current dop value */
    int iops_limits;                 /* iops_limits */
    int io_priority;                 /* io percentage transfered from io_priority */
    int iocount;                     /* io count */
    void* infoptr;                   /* a pointer to user info in configure file */
    void* ptr;                       /* a pointer in general param */
    void* ioptr;                     /* io info pointer */
    void* rp;                        /* resource pool */
    void* userdata;                  /* user data */
    bool use_planA;                  /* force use plan A*/
};

template <typename DataType>
struct WLMDataIndicator {
    DataType min_value;
    DataType max_value;
    DataType total_value;
    DataType avg_value;
    int skew_percent;
};

/* collect user information from DN */
struct WLMUserCollInfoDetail {
    char nodeName[NAMEDATALEN];           /* node name */
    WLMDataIndicator<int> usedMemoryData; /* used memory data */
    WLMDataIndicator<int> currIopsData;   /* current iops for all DN */
    WLMDataIndicator<int> peakIopsData;   /* peak iops for all DN */
    int cpuUsedCnt;                       /* cpu core counts used */
    int cpuTotalCnt;                      /* cpu core counts used */
    int nodeCount;                        /* node count */
    int64 totalSpace;                    /* total used space */
    /* IO flow data */
    uint64 readBytes;   /* current readKb for all DN */
    uint64 writeBytes;  /* current writeKb for all DN */
    uint64 readCounts;  /* current read counts for all DN */
    uint64 writeCounts; /* current write counts for all DN */

    int64 tmpSpace;   /* total used temp space*/
    int64 spillSpace; /* total used spill space*/
};

/* collect realtime info detail */
struct WLMCollectInfoDetail {
    char nodeName[NAMEDATALEN]; /* node name */
    Oid* group_members;         /* group members of one node group */
    int group_count;            /* group count of one node group */

    int usedMemory;    /* memory used */
    int peakMemory;    /* peak memory */
    int spillCount;    /* count for spill */
    int warning;       /* warning info */
    int nodeCount;     /* node count */
    int maxUsedMemory; /* max used memory */
    int avgUsedMemory; /* average used memory */
    int cpuCnt;        /* cpu core counts used */
    int64 dnTime;      /* query used time on dn */
    uint64 space;      /* table space used */
    int64 cpuTime;     /* cpu time used */

    int maxIOUtil;  /* max IO util */
    int minIOUtil;  /* min IO util */
    int maxCpuUtil; /* max CPU util */
    int minCpuUtil; /* min CPU util */

    int peak_iops; /* peak iops */
    int curr_iops; /* current iops */

    int64 spillSize;     /* spill size*/
    int64 broadcastSize; /* broadcast size */

    char misc[NAMEDATALEN];
};

struct WLMGeneralInfo {
    int64 totalCpuTime; /* total query cpu time on dn */
    int64 maxCpuTime;   /* max query cpu time on dn */
    int64 minCpuTime;   /* min query cpu time on dn */
    int64 dnTime;       /* query used time on dn */

    int maxUsedMemory;      /* max used memory */
    int maxPeakChunksQuery; /* max peak chunks for query */
    int minPeakChunksQuery; /* min peak chunks for query */

    int spillCount;           /* total spill count */
    int spillNodeCount;       /* the count of the data node with spilled */
    int64 spillSize;          /* spill size*/
    int64 broadcastSize;      /* broadcast size, unit is bytes */
    int64 broadcastThreshold; /* Threshold of broadcast size, unit is bytes */

    int scanCount; /* scan counter */
    int nodeCount; /* data node counter */

    uint64 totalSpace;   /* total table space */
    WLMActionTag action; /* data node num */

    char* nodeInfo; /* node info string */
};

struct WLMGeneralData {
    WLMDataIndicator<int> peakMemoryData; /* peak memory data */
    WLMDataIndicator<int> usedMemoryData; /* used memory data */
    WLMDataIndicator<int> spillData;      /* spill data */
    WLMDataIndicator<int> broadcastData;  /* broadcast data */
    WLMDataIndicator<int> currIopsData;   /* current iops for all DN */
    WLMDataIndicator<int> peakIopsData;   /* peak iops for all DN */
    WLMDataIndicator<int64> dnTimeData;   /* query used time on dn */
    WLMDataIndicator<int64> cpuData;      /* query cpu time on dn */

    int nodeCount;      /* node count */
    int spillNodeCount; /* spill node count */
    int warning;        /* warning info */

    uint64 totalSpace;             /* total table space */
    WLMTopDnList* WLMCPUTopDnInfo; /* top5 cpu dn list */
    WLMTopDnList* WLMMEMTopDnInfo; /* top5 mem dn list */
    SlowQueryInfo slowQueryInfo;
    List* query_plan;
    int tag;
};

struct WLMIoGeninfo {
    // calculated variable from io_priority
    int curr_iops_limit; /* current iops_limits calculated from io_priority*/
    int prio_count_down; /* count down for io_priority 3, 2, 1 */
    int io_count;        /* update when write/read is running   reset every io_priority gap (0.5, 1, 1.5) */
    int hist_iops_tbl[PAST_TIME_COUNT]; /* history io_count table */

    // iops collectinfo
    int io_count_persec; /* IO count for io_priority***  ----reset every second*/
    int tick_count_down; /* count down for collect info */

    // iops statistics --update each second
    int curr_iops; /* current mean iops */
    int peak_iops; /* peak mean iops */

    int nodeCount; /* count of node */
    /* IO flow */
    uint64 read_bytes[2];   /* logical read_bytes */
    uint64 write_bytes[2];  /* logical write_bytes */
    uint64 read_counts[2];  /* read operation counts */
    uint64 write_counts[2]; /* write operation counts */
    uint64 group_value[4];  /* used for group user */
    uint64 read_speed;      /* read speed */
    uint64 write_speed;     /* write speed */

    TimestampTz timestampTz; /* last timestamp */

    // the belowing is only updated on CN
    WLMDataIndicator<int> currIopsData; /* current iops for all DN */
    WLMDataIndicator<int> peakIopsData; /* peak iops for all DN */
};

/* reply statement info */
struct WLMStmtReplyDetail {
    Oid procId;          /* cn id for the statement */
    uint64 queryId;      /* query id for statement, it's a session id */
    TimestampTz stamp;   /* query time stamp */
    int estimate_memory; /* estimate total memory */
    WLMStatusTag status; /* wlm status of statement */
};

/* statement info */
struct WLMStmtDetail {
    Qid qid; /* qid for query */

    Oid databaseid;              /* database id */
    Oid userid;                  /* user id */
    uint64 debug_query_id;       /* debug query id */
    uint64 plan_size;            /* plan size */
    Oid rpoid;                   /* resource pool oid */
    char respool[NAMEDATALEN];   /* resource pool */
    char cgroup[NAMEDATALEN];    /* cgroup name */
    char nodegroup[NAMEDATALEN]; /* node group name */
    char appname[NAMEDATALEN];   /* application name */
    char* username;              /* user name */
    char* schname;               /* schema name */
    char* msg;                   /* statement's message to show */
    char* query_plan_issue;      /* query plan issue */
    char* query_plan;            /* query plan */
    char* query_band;            /* query band */
    char* statement;             /* sql statement text */
    char* clienthostname;        /* MUST be null-terminated */
    void* clientaddr;            /* client address */

    WLMGeneralInfo geninfo; /* general session info */
    WLMIoGeninfo ioinfo;    /* session IO info */
    SlowQueryInfo slowQueryInfo; /* slow query info */

    ThreadId threadid; /* thread id */
    bool valid;        /* detail info is valid? */

    TimestampTz start_time; /* query start time stamp */
    TimestampTz fintime;    /* query finish time stamp */
    TimestampTz exptime;    /* record expire time */

    WLMStatusTag status; /* wlm status of statement */
    int64 block_time;    /* query block time */
    int64 estimate_time; /* estimate total time */
    int estimate_memory; /* estimate total memory */
    int warning;         /* warning info */
};

/* data node resource info update */
struct WLMNodeUpdateInfo {
    Qid qid;                     /* id for statement on dn */
    char nodegroup[NAMEDATALEN]; /* node group name */
    unsigned char cpuctrl;       /* reference counter */
    WLMGeneralInfo geninfo;      /* general info for the dnode collect info */
};

/* data node resource info, it's used on CN and DN */
struct WLMDNodeInfo {
    Qid qid;                     /* id for statement on dn */
    Oid userid;                  /* user id */
    char cgroup[NAMEDATALEN];    /* cgroup name */
    char nodegroup[NAMEDATALEN]; /* node group name */
    char schname[NAMEDATALEN];   /* schema name */
    char* statement;             /* sql statement text */
    char* qband;                 /* query band string */
    ThreadId tid;                /* cn pid */
    volatile int threadCount;             /* a counter of thread */
    unsigned char cpuctrl;       /* reference counter */
    unsigned char restrack;      /* resource track flag */
    unsigned char flags[2];      /* reserve flags */
    WLMActionTag tag;            /* action tag */
    void* mementry;              /* session memory entry */
    void* respool;               /* resource pool in htab */
    void* userdata;              /* user info pointer */
    WLMGeneralInfo geninfo;      /* general info for the dnode collect info */
    int64 block_time;            /* query block time */
    TimestampTz start_time;      /* query start time stamp */
};

struct WLMDNodeIOInfo {
    Qid qid;                     /* id for statement on dn */
    ThreadId tid;                /* cn pid */
    WLMActionTag tag;            /* action tag */
    char nodegroup[NAMEDATALEN]; /* node group name */

    // user set GUC variables
    int io_priority; /* user set io percent for query */
    int iops_limits; /* user set iops_limits for each query */

    void* userptr; /* UserData pointer in the htab */

    int io_state;    /* IOSTATE_WRITE? IOSTATE_READ? */
    volatile int threadCount; /* thread attached for the query */

    WLMIoGeninfo io_geninfo; /* io general info for query */
};

/* data node real time resource info */
struct WLMDNRealTimeInfoDetail {
    Qid qid; /* id for statement on dn */

    int64 cpuStartTime; /* start cpu time */
    int64 cpuEndTime;   /* end cpu time */

    pid_t tid;              /* a key of this detail info, it's a thread id */
    unsigned char cpuctrl;  /* cpu control is valid ? */
    unsigned char flags[3]; /* save it in the collect info list? */
    WLMStatusTag status;    /* status of the detail info */
};

/* cgroup info */
struct WLMCGroupInfo {
    gscgroup_grp_t* grpinfo;  /* cgroup exception infomation */
    char cgroup[NAMEDATALEN]; /* cgroup name */
    bool attaching;           /* check the thread whether is attaching a new cgroup */
    bool invalid;             /* check the cgroup whether is invalid */
};

/* collect info on cn */
struct WLMCollectInfo {
    WLMStmtDetail sdetail; /* statement info detail */

    WLMCGroupInfo cginfo; /* control group info */

    TimestampTz blockStartTime; /* query block start time */
    TimestampTz blockEndTime;   /* query block end time for a period of time*/
    TimestampTz execStartTime;  /* query exec start time */
    TimestampTz execEndTime;    /* query exec end time for a period of time*/

    int trait;  /* trait for exception handler */
    int update; /* reserved */

    int64 max_mem;   /* max memory for thread */
    int64 avail_mem; /* available memory for thread */

    WLMStatusTag status;  /* wlm status of statement */
    WLMActionTag action;  /* wlm action for statement process */
    WLMAttrTag attribute; /* wlm attribute of statement */
};

/* exception manager */
struct ExceptionManager {
    TimestampTz blockEndTime;  /* statement block end time */
    TimestampTz execEndTime;   /* statement execute end time */
    TimestampTz adjustEndTime; /* statement penalty end time for a period of time */
    TimestampTz intvalEndTime; /* interval end time */
    TimestampTz qualiEndTime;  /* qualification end time */

    int max_waiting_time;  /* statement max block time */
    int max_running_time;  /* statement max execute time */
    int max_adjust_time;   /* statement max penalty time */
    int max_interval_time; /* statement max interval time */

    except_data_t except[EXCEPT_ALL_KINDS]; /* cgroup exception infomation */
};

struct WLMIoStatisticsGenenral {
    ThreadId tid;

    int maxcurr_iops; /* max current iops for all DN */
    int mincurr_iops; /* max current iops for all DN */

    int maxpeak_iops; /* max current iops for all DN */
    int minpeak_iops; /* max current iops for all DN */

    int iops_limits;
    int io_priority;
    int curr_iops_limit; /* iops_limit calculated from io_priority*/
};

struct WLMIoStatisticsList {
    WLMIoStatisticsGenenral* node;
    WLMIoStatisticsList* next;
};

/* query runtime info for pg_session_wlmstat */
struct WLMStatistics {
    char stmt[KBYTES];           /* statement */
    int64 blocktime;             /* query block time while waiting in the queue */
    int64 elapsedtime;           /* elapsed time of the query */
    int64 maxcputime;            /* max cpu time of the query */
    int64 totalcputime;          /* total cpu time of the query */
    int64 qualitime;             /* the qualification time of the query */
    int skewpercent;             /* the skew percent of the query */
    int priority;                /* priority in the queue */
    int stmt_mem;                /* statement memory */
    int act_pts;                 /* active points */
    int dop_value;               /* dop value */
    char cgroup[NAMEDATALEN];    /* control group */
    char srespool[NAMEDATALEN];  /* session_respool */
    char* status;                /* status */
    char* action;                /* action */
    char* enqueue;               /* enqueue state: respool, global or none. */
    char* qtype;                 /* statement's type, simple or complicated. */
    bool is_planA;               /* statement's type, simple or complicated. */
    char groupname[NAMEDATALEN]; /* node group name */
};

/* session history resource info for gs_wlm_session_query_info_all */
struct WLMSessionStatistics {
    char respool[NAMEDATALEN];   /* resource pool */
    char cgroup[NAMEDATALEN];    /* cgroup name */
    char nodegroup[NAMEDATALEN]; /* node group name */
    char spillInfo[16];          /* spill info */

    WLMGeneralData gendata; /* wlm general data on dn */

    Qid qid;                /* qid for the query */
    Oid databaseid;         /* database id */
    Oid userid;             /* user id */
    uint64 debug_query_id;  /* debug query id */
    TimestampTz start_time; /* query start time stamp */
    TimestampTz fintime;    /* query finish time stamp */
    ThreadId tid;           /* thread id */
    WLMStatusTag status;    /* wlm status of statement */
    int64 block_time;       /* query block time */
    int64 estimate_time;    /* estimate total time */
    int64 duration;         /* query excute time */
    char* query_band;       /* query band */
    char* query_plan;       /* query plan */
    char* query_plan_issue; /* query plan issue */
    char* err_msg;          /* error message */
    char* statement;        /* sql statement text */
    char* schname;          /* schema name */
    char* appname;          /* application name */
    char* clienthostname;   /* client host name */
    char* username;         /* user name */

    void* clientaddr;    /* client address */
    bool remove;         /* remove info from hash table */
    int estimate_memory; /* estimate total memory */
    uint64 n_returned_rows;
};

struct WLMSigReceiveData {
    char cgroup[NAMEDATALEN]; /* cgroup name */
    pthread_t tid;            /* thread id */
    int priority;             /* priority of cgroup */
    int used;                 /* slot is used */
};

typedef struct IOHashCtrl {
    int lockId;         /* lock id */
    HTAB* hashTable;    /* hash tables */
} IOHashCtrl;

/* statistics control */
struct WLMStatManager {
    int max_collectinfo_num; /* max collect info num in list */
    int max_statistics_num;  /* max collect info num in list */
    int max_iostatinfo_num;  /* max iostat info num in list */
    int max_detail_num;      /* max session info num in the hash table */
    int index;               /* index of the statistics memory */
    int comp_count;          /* complicated statement count */

    unsigned char stop;      /* a flag to stop running. */
    unsigned char overwrite; /* check the statistics memory whether is overwritten */
    unsigned char datainit;  /* a flag to check collect_info_hashtbl whether has record. */
    unsigned char remain;

    char user[NAMEDATALEN];     /* user name to use in the backend thread */
    char mon_user[NAMEDATALEN]; /* user name to use in wlm monitor thread */

    char database[NAMEDATALEN]; /* database name to use in the backend thread */

    char execdir[STAT_PATH_LEN]; /* the path for binaries */
    char datadir[STAT_PATH_LEN]; /* the path for log */

    unsigned int infoinit; /* htab init ok */
    unsigned int sendsig;  /* send signal */

    TimestampTz scanEndTime; /* a timer for collect data persistence */

    ThreadId thread_id; /* backend thread id */

    HTAB* collect_info_hashtbl;       /* collect information hash table */
    HTAB* session_info_hashtbl;       /* session fino hash table for the query finished */
    HTAB* user_info_hashtbl;          /* user info hash tbale */
    IOHashCtrl* iostat_info_hashtbl;  /* iostat info control */
    HTAB* node_group_hashtbl;         /* node group hash table */
    HASH_SEQ_STATUS collect_info_seq; /* collect info hash table seq */
    HASH_SEQ_STATUS iostat_info_seq;  /* collect info hash table seq */

    WLMStatistics* statistics;   /* the pointer to statistics memory */
    WLMSigReceiveData* recvdata; /* received data */

    WLMUserInfo** userinfo; /* user info list */

    pthread_mutex_t statistics_mutex;       /* mutex to lock the statistics list */
    pthread_mutex_t collectinfo_list_mutex; /* mutex to lock the list */
};

/* Structures for I/O stats. */
struct DiskIOStats {
    unsigned long long total_io; /* total io operation */
    unsigned long read_sectors;  /* read sectors */
    unsigned long write_sectors; /* write sectors */
    unsigned long read_io;       /* read io operation */
    unsigned long read_merges;   /* read requests merged */
    unsigned long write_io;      /* write io operation */
    unsigned long write_merges;  /* write requests merged */
    unsigned int read_ticks;     /* Time of read requests in queue */
    unsigned int write_ticks;    /* Time of write requests in queue */
    unsigned int discard_ticks;  /* Time of discard requests in queue */
    unsigned int total_ticks;    /* total ticks */
    unsigned int queue_ticks;    /* Time of ticks requests spent in queue */
};

/* I/O stats calculcate result. */
struct DiskIOStatsMetric {
    double rsectors; /* rsec/s */
    double wsectors; /* wsec/s */
    double util;     /* disk util */
    double await;    /* disk await */
};

/* dn instance info */
typedef struct WLMInstanceInfo {
    TimestampTz timestamp;      /* timestamp */
    double usr_cpu;             /* userspace cpu usage */
    double sys_cpu;             /* system cpu usage */
    int free_mem;               /* instance free mem, max_process_mem - process_used_memory */
    int used_mem;               /* instance used mem */
    double io_await;            /* disk io_await */
    double io_util;             /* disk io_util */
    double disk_read;           /* disk read speed */
    double disk_write;          /* disk write speed */
    uint64 process_read_speed;  /* gaussdb process read speed */
    uint64 process_write_speed; /* gaussdb process write speed */
    uint64 read_counts;         /* logical read counts */
    uint64 write_counts;        /* logical write counts */
    uint64 logical_read_speed;  /* logical read speed */
    uint64 logical_write_speed; /* logical write speed */
} WLMInstanceInfo;

/* dn instance statistics control */
typedef struct WLMInstanceStatManager {
    int max_stat_num;         /* max stat info num in list */
    int collect_interval;     /* the collect info interval */
    int persistence_interval; /* the persistence interval */
    int cleanup_interval;     /* the cleanup history info interval */

    char instancename[NAMEDATALEN]; /* dn instance name */
    char diskname[MAX_DEVICE_LEN];  /* the dn diskname */

    uint64 totalCPUTime[2]; /* total time of the cpu,0 is old value,1 is new value */
    uint64 userCPUTime[2];  /*cpu time of the process user mode,0 is old value,1 is new value*/
    uint64 sysCPUTime[2];   /*cpu time of the process systerm mode,0 is old value,1 is new value*/

    unsigned long long uptime[2]; /* uptime array */
    DiskIOStats device[2];        /* dev statistics */

    uint64 process_read_bytes[2];  /* gaussdb process read bytes */
    uint64 process_write_bytes[2]; /* gaussdb process write bytes */

    uint64 logical_read_bytes[2];  /* logical write bytes */
    uint64 logical_write_bytes[2]; /* logical write bytes */

    uint64 read_counts[2];  /* logical read counts */
    uint64 write_counts[2]; /* logical write counts */

    TimestampTz last_timestamp;   /* a timer for collect data*/
    TimestampTz recent_timestamp; /* a timer for collect data*/

    HTAB* instance_info_hashtbl; /* instance info hash table */
} WLMInstanceStatManager;

/* process wlm workload manager */
extern int WLMProcessWorkloadManager();
/* get hashCode of io statistics from qid */
extern uint32 GetIoStatBucket(const Qid* qid);
/* get lockId of io statistics from hashCode */
extern int GetIoStatLockId(const uint32 bucket);
/* initialize stat info */
extern void WLMInitializeStatInfo();
/* set stat info */
extern void WLMSetStatInfo(const char*);
/* reset stat info for exception */
extern void WLMResetStatInfo4Exception();
/* generate a hash code by a key */
extern uint32 WLMHashCode(const void* key, Size keysize);
/* set collect info status to finished */
extern void WLMSetCollectInfoStatusFinish();
/* set collect info status */
extern void WLMSetCollectInfoStatus(WLMStatusTag);
/* create collect info on datanodes */
extern void WLMCreateDNodeInfoOnDN(const QueryDesc*);
/* get wlm backend state */
extern void WLMGetStatistics(TimestampTz*, TimestampTz*, WLMStatistics*);
/* get wlm statistics for handling exception */
extern WLMStatistics* WLMGetStatistics(int*);
/* initialize worker thread */
extern void WLMWorkerInitialize(struct Port*);
/* worker thread main function */
extern int WLMProcessThreadMain();
/* shut down the worker thread*/
extern void WLMProcessThreadShutDown();
/* get session info */
extern void* WLMGetSessionInfo(const Qid* qid, int removed, int* num);
/* get instance info */
extern void* WLMGetInstanceInfo(int* num, bool isCleanup);
/* plan list format string */
extern char* PlanListToString(const List* query_plan);
/* get session info from each data nodes */
extern char* WLMGetSessionLevelInfoInternal(Qid* qid, WLMGeneralInfo* geninfo);
/* get wlm session statistics */
extern void* WLMGetSessionStatistics(int* num);
/* get wlm session io statistics */
extern void* WLMGetIoStatistics(int* num);
/* switch cgroup with cn thread id */
extern bool WLMAjustCGroupByCNSessid(uint64 sess_id, const char* cgroup);
extern void WLMAdjustCGroup4EachThreadOnDN(WLMDNodeInfo* info);
/* get spill info string */
extern char* WLMGetSpillInfo(char* spillInfo, int size, int spillNodeCount);
/* get warn info string */
extern void WLMGetWarnInfo(char* warnInfo, int size, unsigned int warn_bit, int spill_size, int broadcast_size,
    WLMSessionStatistics* session_stat = NULL);
/* get skew ratio of datanodes */
extern int GetSkewRatioOfDN(const WLMGeneralInfo* pGenInfo);
/* get each datanode session info for a thread */
extern WLMCollectInfoDetail* WLMGetSessionInfo4Thread(ThreadId threadid, int* num);
/* parse node info string */
extern void WLMParseInfoString(const char* infostr, WLMCollectInfoDetail* detail);
/* parse collect info string */
extern void WLMParseFunc4CollectInfo(StringInfo msg, void* suminfo, int size);
/* alloc function for hash table */
extern void* WLMAlloc0NoExcept4Hash(Size size);
/* check user info htab init is ok */
extern bool WLMIsInfoInit(void);
/* get user info from the user config */
extern WLMUserInfo* WLMGetUserInfoFromConfig(Oid uid, bool* found);
/* get user cgroup cpu util */
extern int WLMGetGroupCpuUtil(char* nodegroup, void* handles, const char* cgroup);

extern uint32 WLMHashCode(const void* key, Size keysize);
extern int WLMHashMatch(const void* key1, const void* key2, Size keysize);
extern bool IsQidInvalid(const Qid* qid);
extern bool IsQidEqual(const Qid* qid1, const Qid* qid2);
extern void WLMLocalInfoCollector(StringInfo msg);
extern void WLMAdjustCGroup4EachThreadOnDN(WLMDNodeInfo* info);
extern void WLMCreateIOInfoOnDN(void);
extern WLMIoStatisticsList* WLMGetIOStatisticsGeneral();
extern void WLMCleanUpIoInfo(void);
extern void WLMUpdateCgroupCPUInfo(void);
extern void WLMInitTransaction(bool* backinit);
extern void WLMInitPostgres();
extern void WLMCleanIOHashTable();
extern void WLMSetBuildHashStat(int status);
extern bool WLMIsQueryFinished(void);
extern void WLMReadjustAllUserSpace(Oid uid);
extern void WLMReadjustUserSpaceThroughAllDbs(const char* username);
extern void WLMReadjustUserSpaceByNameWithResetFlag(const char* username, bool resetFlag);
extern WLMCollectInfoDetail* WLMGetUserSessionInfo4Thread(const char* uname, int* num);
extern void WLMSetExecutorStartTime(void);
extern bool WLMIsDumpActive(const char* sql);
extern void WLMCheckSigRecvData(void);
extern char* GetStatusName(WLMStatusTag tag);

extern char* WLMGetExceptWarningMsg(WLMExceptTag tag);
extern int64 WLMGetTimestampDuration(TimestampTz start, TimestampTz end);
extern void WLMInitQueryPlan(QueryDesc* queryDesc, bool isQueryDesc = true);
extern double WLMGetTotalCost(const QueryDesc* desc, bool isQueryDesc);
extern int WLMGetQueryMem(const QueryDesc* desc, bool isQueryDesc, bool max = true);
extern bool WLMGetInComputePool(const QueryDesc* desc, bool isQueryDesc);
extern int WLMGetMinMem(const QueryDesc* desc, bool isQueryDesc, bool max = true);
extern int WLMGetAvailbaleMemory(const char* ngname);
extern bool WLMChoosePlanA(PlannedStmt* stmt);
extern void WLMGetWorkloadStruct(StringInfo strinfo);
extern void WLMDefaultXactReadOnlyCheckAndHandle(void);
extern void WLMTopSQLReady(QueryDesc* queryDesc);
extern void WLMReleaseIoInfoFromHash(void);

#define WORKLOAD_STAT_HASH_SIZE 64
// default database name
#define DEFDBNAME "postgres"

#ifdef ENABLE_UT
void ut_to_update(void*);
#endif

#endif
