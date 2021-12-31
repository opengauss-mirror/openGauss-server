/*--------------------------------------------------------------------
 * bgworker.h
 *      Pluggable background workers interface
 *
 * A background worker is a process able to run when create index for redistribution,
 * create a parallel bgworker framework to scan and rebuild indexes in parallel.
 *
 * Portions Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 *
 * IDENTIFICATION
 *      src/include/postmaster/bgworker.h
 * --------------------------------------------------------------------
 */

#ifndef BGWORKER_H
#define BGWORKER_H

#include "access/xact.h"
#include "access/nbtree.h"
#include "utils/tuplesort.h"
#include "catalog/index.h"

/*---------------------------------------------------------------------
 * External module API.
 *---------------------------------------------------------------------
 */
extern int g_max_worker_processes;
#define BGWORKER_LOOP_SLEEP_TIME 10000
/* bgworker's current status duration limit, 500 * BGWORKER_LOOP_SLEEP_TIME = 5s */
#define BGWORKER_STATUS_DURLIMIT 500

#define BGWORKER_MAX_ERROR_LEN 256

typedef enum BgwHandleStatus {
    BGW_NOT_YET_STARTED,       /* worker hasn't been started yet */
    BGW_STARTED,               /* worker is running */
    BGW_STOPPED,               /* worker has finished work */
    BGW_FAILED,                /* worker has failed */
    BGW_TERMINATED             /* worker has exit successfully */
} BgwHandleStatus;

struct BgWorkerContext;
typedef void (*bgworker_main)(const BgWorkerContext *bwc);
typedef void (*bgworker_exit)(const BgWorkerContext *bwc);

typedef struct BgWorkerContext {
    StreamTxnContext transactionCxt;
    void        *bgshared;
    PGPROC      *leader;
    char        *databaseName;
    char        *userName;
    bool        enable_cluster_resize;
    bgworker_main main_entry;
    bgworker_exit exit_entry;
} BgWorkerContext;

typedef struct BgWorkerErrorData {
    int elevel;
    int sqlerrcode;
    char message[BGWORKER_MAX_ERROR_LEN];
    char detail[BGWORKER_MAX_ERROR_LEN];
} BgWorkerErrorData;

typedef struct BackgroundWorker {
    SHM_QUEUE           links; /* list link if process is in a list */
    uint64              bgw_id;
    ThreadId            bgw_notify_pid;    /* SIGUSR1 this backend on start/stop */
    BgwHandleStatus     bgw_status;        /* Status of this bgworker */
    uint64              bgw_status_dur;    /* duration in this status */
    BgWorkerErrorData   bgw_edata;         /* error information of a bgworker */
    pg_atomic_uint32    disable_count;     /* indicate whether the bgworker is disabled */
    slist_node          rw_lnode;          /* list link */
} BackgroundWorker;

typedef struct BGW_HDR {
    pg_atomic_uint64 bgw_id_seq;
    BackgroundWorker* bgws;
    BackgroundWorker* free_bgws;
} BGW_HDR;

typedef struct BackgroundWorkerArgs {
    BgWorkerContext  *bgwcontext;
    BackgroundWorker *bgworker;
    uint64            bgworkerId;
} BackgroundWorkerArgs;

/* Register a new bgworker during shared_preload_libraries */
extern bool RegisterBackgroundWorker(BackgroundWorker *worker);
extern int LaunchBackgroundWorkers(int nworkers, void *bgshared, bgworker_main bgmain, bgworker_exit bgexit);
extern void BackgroundWorkerMain(void);
extern bool IsBgWorkerProcess(void);
extern void BgworkerListSyncQuit();
extern void BgworkerListWaitFinish(int *nparticipants);
extern void InitBgworkerGlobal(void);

#endif   /* BGWORKER_H */
