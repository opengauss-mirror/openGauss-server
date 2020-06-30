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
 * job_worker.h
 *     declare functions for external
 * 
 * 
 * IDENTIFICATION
 *        src/include/job/job_worker.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef JOB_WORKER_H
#define JOB_WORKER_H

/*****************************************************************************
  Description    : start job worker process.
*****************************************************************************/
extern ThreadId StartJobExecuteWorker(void);
extern void JobExecuteWorkerMain();

/* Status inquiry functions */
extern bool IsJobWorkerProcess(void);

#endif /* JOB_WORKER_H */
