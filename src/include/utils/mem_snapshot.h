/*
* Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
* mem_snapshot.h
*  Memory snapshot structure definition.
*
*
* IDENTIFICATION
*        src/include/mem_snapshot.h
*
* ---------------------------------------------------------------------------------------
*/

#ifndef MEM_SNAPSHOT_H
#define MEM_SNAPSHOT_H

#include "postgres.h"
#include "knl/knl_variable.h"
#include "catalog/catalog.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/acl.h"
#include "miscadmin.h"
#include "threadpool/threadpool.h"
#include "storage/proc.h"
#include "utils/memprot.h"
#include "pgtime.h"
#include "pgstat.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "postmaster/syslogger.h"
#include "cjson/cJSON.h"

typedef struct MemoryDumpData {
    char *contextName;
    Size freeSize;
    Size totalSize;
} MemoryDumpData;

typedef enum DumpMemoryType{
    MEMORY_CONTEXT_SHARED,
    MEMORY_CONTEXT_SESSION,
    MEMORY_CONTEXT_THREAD
} DumpMemoryType;

typedef enum {
    MEMORY_TRACE_NONE = 0,
    MEMORY_TRACE_LEVEL1,
    MEMORY_TRACE_LEVEL2,
} MemoryTraceLevel;


#define RESET_PERCENT_KIND 2
#define PERCENT_LOW_KIND 0
#define PERCENT_HIGH_KIND 1

#define MEMORY_TRACE_PERCENT 90
#define FULL_PERCENT 100
#define MAX_WAIT_COUNT 10

#define TOP_MEMORY_CONTEXT_NUM 20

#define MAX_INT_NUM_LEN 10

extern void check_stack_depth(void);
extern int kill_backend(ThreadId tid, bool checkPermission);
char* GetRoleName(Oid rolid, char* rolname, size_t size);

void RecursiveSharedMemoryContext(const MemoryContext context, StringInfoDataHuge* buf, bool isShared);
void RecursiveUnSharedMemoryContext(const MemoryContext context, StringInfoDataHuge* buf);
void ExecOverloadEscape();
void InitMemoryLogDirectory();

void AssignThreadpoolResetPercent(const char* newval, void* extra);
bool CheckThreadpoolResetPercent(char** newval, void** extra, GucSource source);
void AssignMemoryResetPercent(const char* newval, void* extra);
bool CheckMemoryResetPercent(char** newval, void** extra, GucSource source);


#endif /* MEM_SNAPSHOT_H */
