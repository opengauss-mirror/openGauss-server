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
 * IDENTIFICATION
 *        src/include/memory_func.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef MEMORY_FUNC_H
#define MEMORY_FUNC_H

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
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "postmaster/syslogger.h"
#include "zlib.h"

#define MIN_FILE_NAME_LEN 16
#define MAX_FILE_NAME_LEN 64

#ifdef MEMORY_CONTEXT_TRACK
extern void GetAllocBlockInfo(AllocSet set, StringInfoDataHuge* buf);
extern void GetAsanBlockInfo(AsanSet set, StringInfoDataHuge* buf);
void gs_recursive_unshared_memory_context(const MemoryContext context,
    const char* ctx_name, StringInfoDataHuge* buf);
void gs_recursive_shared_memory_context(const MemoryContext context,
    const char* ctx_name, StringInfoDataHuge* buf, bool isShared);
AllocChunk gs_collate_memctx_info(StringInfoHuge mem_info, int* res_len);
#endif

Datum gs_get_shared_memctx_detail(PG_FUNCTION_ARGS);
Datum gs_get_session_memctx_detail(PG_FUNCTION_ARGS);
Datum gs_get_thread_memctx_detail(PG_FUNCTION_ARGS);
Datum gs_get_history_memory_detail(PG_FUNCTION_ARGS);

extern void check_stack_depth(void);

#endif /* MEMORY_FUNC_H */
