/*
 * parallel.h
 *
 *	Parallel support for pg_dump and pg_restore
 *
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
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
 * IDENTIFICATION
 *        src/bin/pg_dump/parallel.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef PARALLEL_H
#define PARALLEL_H
#include "pg_backup_archiver.h"

/* ParallelSlotN is an opaque struct known only within parallel.c */
typedef struct ParallelSlotN ParallelSlotN;

typedef enum T_Action { ACT_DUMP } T_Action;

typedef enum { WFW_NO_WAIT, WFW_GOT_STATUS, WFW_ONE_IDLE, WFW_ALL_IDLE } WFW_WaitOption;

typedef struct {
    int workerNum;
    TocEntry** te;
    ParallelSlotN* parallelSlot;
} ParallelStateN;

extern ParallelStateN* ParallelBackupStart(ArchiveHandle* AH);
extern void ParallelBackupEnd(ArchiveHandle* AH, ParallelStateN* pstate);
extern void DispatchJobForTocEntry(ParallelStateN* pstate, TocEntry* te, T_Action act);
extern void WaitForWorkers(ParallelStateN* pstate, WFW_WaitOption mode);
extern bool IsEveryWorkerIdle(ParallelStateN* pstate);
extern void on_exit_close_parallel_archive(Archive* AHX);
#endif