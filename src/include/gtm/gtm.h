/* -------------------------------------------------------------------------
 *
 * gtm.h
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * $PostgreSQL$
 *
 * -------------------------------------------------------------------------
 */
#ifndef _GTM_H
#define _GTM_H

#include <setjmp.h>

#include "common/config/cm_config.h"
#include "gtm/gtm_c.h"
#include "gtm/utils/palloc.h"
#include "gtm/gtm_lock.h"
#include "gtm/gtm_conn.h"
#include "gtm/utils/elog.h"
#include "gtm/gtm_list.h"
#include "gtm/gtm_semaphore.h"

extern char* GTMLogFileName;
extern FILE* gtmlogFile;

extern volatile long int NumThreadAdded;
extern volatile long int NumThreadRemoved;

extern char* Log_directory;
extern GTM_RWLock gtmlogFileLock;

#define MAXLISTEN 64

#define MAX_IPADDR_LEN 32
#define MAX_IPSTR_LEN 64

#define ETCD_AVA() (g_etcd_num > 0)

typedef struct GTM_ListenEntry {
    char ipaddr[MAX_IPADDR_LEN];
    int portnum;
    bool needcreate;
} GTM_ListenEntry;

typedef struct GTM_ListenAddress {
    int usedNum;
    GTM_ListenEntry listenArray[MAXLISTEN];
} GTM_ListenAddress;

/* gtm synchronous level */
#define SYNC_GTM_OFF (0)  /* asynchronous transmission between gtm and standby */
#define SYNC_GTM_ON (1)   /* synchronous transmission between gtm and standby */
#define SYNC_GTM_AUTO (2) /* available sync mode switch sync to async */

typedef enum GTM_ThreadStatus {
    GTM_THREAD_STARTING,
    GTM_THREAD_RUNNING,
    GTM_THREAD_EXITING,
    GTM_THREAD_BACKUP, /* Backup to standby is in progress */
    GTM_THREAD_SWITCHOVER,
    /* Must be the last */
    GTM_THREAD_INVALID
} GTM_ThreadStatus;

typedef volatile uint32 pg_atomic_uint32;

struct GTM_ConnectionInfo;

#define ERRORDATA_STACK_SIZE 20

#define GTM_MAX_PATH 1024
#define INVALID_GROUP_NEXT (0x7FFFFFFF) /* int32 max value*/

/*
 * This struct is for counting messages send and receive of gtm thread between primary and standby
 * Notice: current counter type is uint32. When the message count is too big than the max value of
 * uint32(overflow uint32), this may have problem, some thread counter will be started from zero again.
 */
typedef struct GTM_ThreadStat {
    uint32 thr_send_count;    /*current thread send message count */
    uint32 thr_receive_count; /*current thread receive message count */
    uint32 thr_localid;       /*the location of current thread in global thread stat array */
    bool thr_used;            /*mark this thread stat slot is used or not. */
} GTM_ThreadStat;

typedef struct GTM_ThreadInfo {
    /*
     * Thread specific information such as connection(s) served by it
     */
    GTM_ThreadID thr_id;
    uint32 thr_localid;
    bool is_main_thread;
    void* (*thr_startroutine)(void*);
    void (*thr_cleanuproutine)(void*);

    MemoryContext thr_thread_context;
    MemoryContext thr_message_context;
    MemoryContext thr_current_context;
    MemoryContext thr_error_context;
    MemoryContext thr_parent_context;

    sigjmp_buf* thr_sigjmp_buf;

    ErrorData thr_error_data[ERRORDATA_STACK_SIZE];
    int thr_error_stack_depth;
    int thr_error_recursion_depth;
    int thr_criticalsec_count;

    GTM_ThreadStatus thr_status;
    GTM_ConnectionInfo* thr_conn;
    /* unique client identifier */
    uint32 thr_client_id;
    bool thr_need_bkup;
    bool thr_seq_bkup;
    bool thr_thread_over;

    GTM_RWLock thr_lock;
    gtm_List* thr_cached_txninfo;

    /* main thread set other thread sema_key. */
    int sema_key;
    /* when this thread want to one sema, create it. */
    PGSemaphoreData sema;

    /* store the next thread index for group sequence. */
    pg_atomic_uint32 group_next;
    /* the end thread in group. */
    pg_atomic_uint32 group_end_index;

    /*
     * if it's INVALID_GROUP_NEXT, that means it's not in blocked list of sequence info.
     * otherwise it's blocked until the sequence is synced to standby/etcd successfully.
     */
    pg_atomic_uint32 next_blocked_thread;

    /*
     * store the sequence infomation, in order to transfer
     * seq info for threads of thread group.
     */
    GTM_ThreadSeqInfo seq_info_set;

    bool success_get_seq;
} GTM_ThreadInfo;

typedef struct GTM_Threads {
    uint32 gt_thread_count;
    uint32 gt_array_size;
    bool gt_standby_ready;
    GTM_ThreadInfo** gt_threads;
    GTM_RWLock gt_lock;
    pg_atomic_uint32 thread_group_first;
} GTM_Threads;

extern volatile GTM_Threads* GTMThreads;
extern int get_gtm_port();

GTM_ThreadStat* GTM_GetThreadStatSlot(uint32 localid);
int GTM_ThreadAdd(GTM_ThreadInfo* thrinfo);
int GTM_ThreadRemove(GTM_ThreadInfo* thrinfo);
int GTM_ThreadJoin(GTM_ThreadInfo* thrinfo);
void GTM_ThreadExit(void);
void ConnFree(Port* port);
void GTM_LockAllOtherThreads(void);
void GTM_UnlockAllOtherThreads(void);
void GTM_DoForAllOtherThreads(void (*process_routine)(GTM_ThreadInfo*));
void FlushPort(Port* myport);
void CollectGTMStatus(Port* myport, bool is_backup);

extern GTM_ThreadInfo* GTM_ThreadCreate(
    GTM_ConnectionInfo* conninfo, bool normal_thread, void* (*startroutine)(void*), void (*cleanuproutine)(void* argp));
extern GTM_ThreadInfo *GTM_ThreadCreateNoBlock(bool normal_thread, pthread_attr_t *pThreadAttr,
    void* (*startroutine)(void*), void (*cleanuproutine)(void* argp));
extern void GTM_ThreadCleanup(void* argp);
extern void GTM_CsnSyncCleanup(void *argp);
extern void GTM_AlarmCheckerCleanup(void* argp);

GTM_ThreadInfo* GTM_GetThreadInfo(GTM_ThreadID thrid);

extern void DestroyConnectControlTable(void);
extern void RebuildConnectControlTable(void);

extern uint64 GTM_ConPointTblFind(const char* cnName);
extern void GTM_ConPointTblInsert(const char* cnName, uint64 consistencyPoint);

/*
 * pthread keys to get thread specific information
 */
extern pthread_key_t threadinfo_key;
extern MemoryContext TopMostMemoryContext;
extern GTM_ThreadID TopMostThreadID;

extern char* ListenAddresses;
extern int GTMPortNumber;
extern GTM_HOST_IP* ListenArray;
extern int ListenLength;

extern char* active_addr;
extern int active_port;
extern GTM_HOST_IP* ActAddressArray;
extern int ActAddressLength;
extern int standby_connection_timeout;

extern char* HALocalHost;
extern int HALocalPort;
extern GTM_HOST_IP* HaAddressArray;
extern int HaAddressLength;
extern bool LocalAddressChanged;
extern bool LocalPortChanged;
extern bool EnableConnectControl;
#define GTMOPTION_GTM 0
#define GTMOPTION_GTMLITE 1
#define GTMOPTION_GTMFREE 2
extern int gtm_option;
extern int CsnSyncInterval;
extern int RestoreDuration;

extern volatile int Backup_synchronously;
extern int guc_standby_only;

extern char* NodeName;
extern char GTMControlFile[GTM_MAX_PATH];
extern char GTMSequenceFile[GTM_MAX_PATH];
extern volatile bool GTMPreAbortPending;
extern volatile bool primary_switchover_processing;
extern bool switchover_processing;

extern GTM_RWLock* mainThreadLock;

#define SetMyThreadInfo(thrinfo) pthread_setspecific(threadinfo_key, (thrinfo))
#define GetMyThreadInfo ((GTM_ThreadInfo*)pthread_getspecific(threadinfo_key))

#define TopMemoryContext (GetMyThreadInfo->thr_thread_context)
#define ThreadTopContext (GetMyThreadInfo->thr_thread_context)
#define MessageContext (GetMyThreadInfo->thr_message_context)
#define CurrentMemoryContext (GetMyThreadInfo->thr_current_context)
#define ErrorContext (GetMyThreadInfo->thr_error_context)
#define errordata (GetMyThreadInfo->thr_error_data)
#define recursion_depth (GetMyThreadInfo->thr_error_recursion_depth)
#define errordata_stack_depth (GetMyThreadInfo->thr_error_stack_depth)
#define CritSectionCount (GetMyThreadInfo->thr_criticalsec_count)
#define gtm_need_bkup (GetMyThreadInfo->thr_need_bkup)
#define seq_need_bkup (GetMyThreadInfo->thr_seq_bkup)

#define PG_exception_stack (GetMyThreadInfo->thr_sigjmp_buf)
#define MyConnection (GetMyThreadInfo->thr_conn)
#define MyPort ((GetMyThreadInfo->thr_conn != NULL) ? GetMyThreadInfo->thr_conn->con_port : NULL)
#define MyThreadID (GetMyThreadInfo->thr_id)
#define IsMainThread() (GetMyThreadInfo->thr_id == TopMostThreadID)

#define GTM_CachedTransInfo (GetMyThreadInfo->thr_cached_txninfo)
#define GTM_HaveFreeCachedTransInfo() (gtm_list_length(GTM_CachedTransInfo))

#define GTM_MAX_CACHED_TRANSINFO 0
#define GTM_HaveEnoughCachedTransInfo() (gtm_list_length(GTM_CachedTransInfo) >= GTM_MAX_CACHED_TRANSINFO)

#define START_CRIT_SECTION() (CritSectionCount++)

#define END_CRIT_SECTION() do { \
    Assert(CritSectionCount > 0); \
    CritSectionCount--;           \
} while (0)

typedef enum GtmHaMode {
    GHM_NO_HA,          /* used for normal. */
    GHM_SINGLE_STANDBY, /* used for mode without etcd. */
    GHM_MULT_STANDBY,   /* used for mode under multistandbys, only sync data with etcd. */
    GHM_MIXED           /* used for version commpatible, one primary & one standby. */
} GtmHaMode;

extern volatile GtmHaMode gtm_ha_mode;
#define IS_SINGLE_STANBY_HA_MODE() (gtm_ha_mode == GHM_SINGLE_STANDBY)
#define IS_MULT_STANDBY_HA_MODE() (gtm_ha_mode == GHM_MULT_STANDBY)
#define IS_MIXED_HA_MODE() (gtm_ha_mode == GHM_MIXED)
#define MINORITY_MODE() (Backup_synchronously == SYNC_GTM_OFF && IS_MULT_STANDBY_HA_MODE())
#define ETCD_MODE() (IS_MULT_STANDBY_HA_MODE() || (!IS_MULT_STANDBY_HA_MODE() && ETCD_AVA()))

#define ETCD_MODE_ACTIVE_PORT 0

#define SET_GTM_HAMODE(mode) do { \
    Assert((mode) >= GHM_SINGLE_STANDBY && (mode) <= GHM_MIXED); \
    gtm_ha_mode = (mode);                                        \
} while (0)

#endif
