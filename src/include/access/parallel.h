/* -------------------------------------------------------------------------
 *
 * parallel.h
 * 	  Infrastructure for launching parallel workers
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/parallel.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef PARALLEL_H
#define PARALLEL_H

#include "access/xlogdefs.h"
#include "lib/ilist.h"
#include "postmaster/bgworker.h"
#include "storage/shm_mq.h"

typedef void (*parallel_worker_main_type)(void *seg);

typedef struct ParallelWorkerInfo {
    BackgroundWorkerHandle *bgwhandle;
    shm_mq_handle *error_mqh;
    ThreadId pid;
} ParallelWorkerInfo;

typedef struct ParallelContext {
    dlist_node node;
    SubTransactionId subid;
    int nworkers;
    int nworkers_launched;
    char *library_name;
    char *function_name;
    ErrorContextCallback *error_context_stack;
    void *seg;
    void *private_memory;
    ParallelWorkerInfo *worker;
    int nknown_attached_workers;
    bool *known_attached_workers;
} ParallelContext;

typedef struct ParallelWorkerContext {
    void *seg;
} ParallelWorkerContext;

#define IsParallelWorker() (t_thrd.bgworker_cxt.ParallelWorkerNumber >= 0)

extern ParallelContext *CreateParallelContext(const char *library_name, const char *function_name, int nworkers);
extern void InitializeParallelDSM(ParallelContext *pcxt, const void *snap);
extern void ReinitializeParallelDSM(ParallelContext *pcxt);
extern void LaunchParallelWorkers(ParallelContext *pcxt);
extern void WaitForParallelWorkersToAttach(ParallelContext *pcxt);
extern void WaitForParallelWorkersToFinish(ParallelContext *pcxt);
extern void DestroyParallelContext(ParallelContext *pcxt);
extern bool ParallelContextActive(void);

extern void HandleParallelMessageInterrupt(void);
extern void HandleParallelMessages(void);
extern void AtEOXact_Parallel(bool isCommit);
extern void AtEOSubXact_Parallel(bool isCommit, SubTransactionId mySubId);
extern void ParallelWorkerReportLastRecEnd(XLogRecPtr last_xlog_end);

extern void ParallelWorkerMain(Datum main_arg);

#endif /* PARALLEL_H */
