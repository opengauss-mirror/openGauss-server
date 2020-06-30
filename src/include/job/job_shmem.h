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
 * job_shmem.h
 *     define the shared memory for jobs.
 * 
 * 
 * IDENTIFICATION
 *        src/include/job/job_shmem.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef JOB_SHMEM_H
#define JOB_SHMEM_H
#include "postgres.h"
#include "knl/knl_variable.h"
#include "catalog/genbki.h"
#include "storage/shmem.h"
#include "utils/timestamp.h"

/* Job worker info define */
typedef struct JobWorkerInfoData {
    SHM_QUEUE job_links;
    ThreadId job_worker_pid;
    int4 job_id;
    Oid job_oid;
    Oid job_dboid;
    NameData username;
    TimestampTz job_launchtime;
} JobWorkerInfoData;

typedef struct JobWorkerInfoData* JobWorkerInfo;

/* shared memeory define */
typedef enum JobShmemE {
    ForkJobWorkerFailed,
    JobScheduleSignalNum /* must be last */
} JobShmemE;

typedef struct JobScheduleShmemStruct {
    sig_atomic_t jsch_signal[JobScheduleSignalNum];
    ThreadId jsch_pid;
    JobWorkerInfo jsch_freeWorkers;
    SHM_QUEUE jsch_runningWorkers;
    JobWorkerInfo jsch_startingWorker;
} JobScheduleShmemStruct;

#endif /* JOB_SHMEM_H */