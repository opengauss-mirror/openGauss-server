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
 * job_scheduler.h
 *     declare functions and guc variables for external
 * 
 * 
 * IDENTIFICATION
 *        src/include/job/job_scheduler.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef JOB_SCHEDULER_H
#define JOB_SCHEDULER_H

/* GUC variables */
extern THR_LOCAL int job_queue_processes;
#define DEFAULT_JOB_QUEUE_PROCESSES 10
#define MIN_JOB_QUEUE_PROCESSES 0
#define MAX_JOB_QUEUE_PROCESSES 1000

/* Job info define */
typedef struct JobInfoData {
    int4 job_id;
    Oid job_oid;
    Oid job_dboid;
    NameData log_user;
    NameData node_name;
    TimestampTz last_start_date; /* arrange jobs in last_start_date order */
} JobInfoData;

typedef struct JobInfoData* JobInfo;

/*****************************************************************************
  Description    : shared memory size.
*****************************************************************************/
extern Size JobInfoShmemSize(void);

/*****************************************************************************
  Description    : init shared memory
*****************************************************************************/
extern void JobInfoShmemInit(void);

/*****************************************************************************
  Description    : start job scheduler process.
*****************************************************************************/
extern ThreadId StartJobScheduler(void);

/*****************************************************************************
  Description    :
*****************************************************************************/
extern void JobScheduleMain();

/* Status inquiry functions */
extern bool IsJobSchedulerProcess(void);

/* called from postmaster when a worker could not be forked */
extern void RecordForkJobWorkerFailed(void);

#endif /* JOB_SCHEDULER_H */
