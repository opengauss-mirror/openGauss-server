
/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
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
 */

#include "memory_func.h"

#ifdef MEMORY_CONTEXT_TRACK
/*
 * for view gs_get_shared_memctx_detail;
 */
void gs_recursive_shared_memory_context(const MemoryContext context,
    const char* ctx_name, StringInfoData* buf, bool isShared)
{
    bool checkLock = false;

    if (context == NULL) {
        return;
    }

    PG_TRY();
    {
        CHECK_FOR_INTERRUPTS();
        check_stack_depth();

        if (isShared) {
            MemoryContextLock(context);
            checkLock = true;
        }

        if (context->type == T_SharedAllocSetContext && strcmp(ctx_name, context->name) == 0) {
#ifndef ENABLE_MEMORY_CHECK
            GetAllocBlockInfo((AllocSet)context, buf);
#else
            appendStringInfo(buf, "context : %s\n", context->name);
            GetAsanBlockInfo((AsanSet)context, buf);
#endif
        }

        /* recursive MemoryContext's child */
        for (MemoryContext child = context->firstchild; child != NULL; child = child->nextchild) {
            if (child->is_shared) {
                gs_recursive_shared_memory_context(child, ctx_name, buf, child->is_shared);
            }
        }
    }
    PG_CATCH();
    {
        if (isShared && checkLock) {
            MemoryContextUnlock(context);
        }
        PG_RE_THROW();
    }
    PG_END_TRY();

    if (isShared) {
        MemoryContextUnlock(context);
    }

    return;
}

/*
 * for view gs_get_thread_memctx_detail and gs_get_session_memctx_detail;
 */
void gs_recursive_unshared_memory_context(const MemoryContext context,
    const char* ctx_name, StringInfoData* buf)
{
    if (context == NULL) {
        return;
    }

#ifndef ENABLE_MEMORY_CHECK
    if ((context->type == T_AllocSetContext) && strcmp(ctx_name, context->name) == 0) {
        GetAllocBlockInfo((AllocSet)context, buf);
    }
#else
    if ((context->type == T_AsanSetContext) && strcmp(ctx_name, context->name) == 0) {
        appendStringInfo(buf, "context : %s\n", context->name);
        GetAsanBlockInfo((AsanSet)context, buf);
    }
#endif

    CHECK_FOR_INTERRUPTS();
    check_stack_depth();

    /* recursive MemoryContext's child */
    for (MemoryContext child = context->firstchild; child != NULL; child = child->nextchild) {
        gs_recursive_unshared_memory_context(child, ctx_name, buf);
    }

    return;
}

/*
 * file dictionary order, line number from small to large
 */
static int gs_alloc_chunk_cmp(const void* cmp_a, const void* cmp_b)
{
    if (cmp_a == NULL && cmp_b == NULL) {
        return 0;
    } else if (cmp_a == NULL) {
        return 1;
    } else if (cmp_b == NULL) {
        return -1;
    }

    AllocChunk chunk_a = (AllocChunk)cmp_a;
    AllocChunk chunk_b = (AllocChunk)cmp_b;

    int cmp_file = strcmp(chunk_a->file, chunk_b->file);
    if (cmp_file != 0) {
        return cmp_file;
    }

    if (chunk_a->line < chunk_b->line) {
        return -1;
    } else if (chunk_a->line == chunk_b->line) {
        return 0;
    } else {
        return 1;
    }

    return 0;
}

/*
 * file dictionary order, line number from small to large; the size of same file and line will be summation;
 */
static void gs_sort_memctx_info(AllocChunk memctx_info_res, int memctx_info_cnt, int* res_len)
{
    int i = 0;
    int j = 1;

    qsort(memctx_info_res, memctx_info_cnt, sizeof(AllocChunkData), gs_alloc_chunk_cmp);

    while (j < memctx_info_cnt) {
        AllocChunk chunk_i = &memctx_info_res[i];
        AllocChunk chunk_j = &memctx_info_res[j];
        if (strcmp(chunk_i->file, chunk_j->file) == 0 && chunk_i->line == chunk_j->line) {
            chunk_i->size += chunk_j->size;
            ++j;
            continue;
        }

        ++i;
        chunk_i = &memctx_info_res[i];
        const char* tmp = chunk_i->file;
        chunk_i->file = chunk_j->file;
        chunk_j->file = tmp;
        chunk_i->line = chunk_j->line;
        chunk_i->size = chunk_j->size;
        ++j;
    }
    *res_len = i + 1;
}

/*
 * collect the file and line info
 */
static AllocChunk gs_collate_memctx_info(StringInfo mem_info, int* res_len)
{
    if (mem_info == NULL) {
        *res_len = 0;
        return NULL;
    }

    int i = 0;
    int memctx_info_cnt = 0;

    /* find alloc chunk info count */
    for (int i = 0; i < mem_info->len; ++i) {
        if (mem_info->data[i] == ':') {
            ++memctx_info_cnt;
        }
    }

    if (memctx_info_cnt == 0) {
        *res_len = 0;
        return NULL;
    }

    /* Traverse memory application information */
    AllocChunk memctx_info_res = (AllocChunk)palloc(sizeof(AllocChunkData) * memctx_info_cnt);
    char* file_name = mem_info->data;
    char* tmp_file_name = mem_info->data;
    char* line = NULL;
    char* real_size = NULL;
    const int divide_size = 2;
    for (i = 0; i < memctx_info_cnt; ++i) {
        file_name = tmp_file_name;
        line = strchr(file_name, ':');
        if (line == NULL) {
            continue;
        }
        *line = '\0';
        ++line;

        real_size = strchr(line, ',');
        if (real_size == NULL) {
            continue;
        }
        *real_size = '\0';
        real_size += divide_size;

        tmp_file_name = strchr(real_size, '\n');
        if (tmp_file_name == NULL) {
            continue;
        }
        *tmp_file_name = '\0';
        ++tmp_file_name;

        if (strcmp(file_name, "(null)") == 0 || strcmp(line, "0") == 0) {
            continue;
        }

        char* file_name_begin_pos = strrchr(file_name, '/');
        if (file_name_begin_pos != NULL) {
            file_name = file_name_begin_pos + 1;
        }

        AllocChunk chunk_res = &memctx_info_res[i];
        int file_name_len = strlen(file_name);
        chunk_res->file = (char*)palloc(sizeof(char) * (file_name_len + 1));
        int rc = memcpy_s((char*)chunk_res->file, file_name_len, file_name, file_name_len);
        securec_check_c(rc, "\0", "\0");
        ((char*)chunk_res->file)[file_name_len] = '\0';
        chunk_res->line = atoi(line);
        chunk_res->size = atoi(real_size);
    }

    gs_sort_memctx_info(memctx_info_res, memctx_info_cnt, res_len);

    return memctx_info_res;
}

static TupleDesc get_memctx_view_first_row(const unsigned col_num)
{
    /* the col num of view */
    TupleDesc tupdesc = CreateTemplateTupleDesc(col_num, false);

    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_1, "file", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_2, "line", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_3, "size", INT8OID, -1, 0);

    return BlessTupleDesc(tupdesc);
}

static HeapTuple fetch_memctx_view_values(FuncCallContext* funcctx, AllocChunk memctx_chunk,
    const unsigned col_num)
{
    Datum values[col_num];
    bool nulls[col_num];

    errno_t rc;
    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check_c(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check_c(rc, "\0", "\0");

    values[ARG_0] = CStringGetTextDatum(memctx_chunk->file);
    values[ARG_1] = Int64GetDatum(memctx_chunk->line);
    values[ARG_2] = Int64GetDatum(memctx_chunk->size);

    return heap_form_tuple(funcctx->tuple_desc, values, nulls);
}

void gs_check_context_name_valid(const char* ctx_name)
{
    if (!t_thrd.utils_cxt.gs_mp_inited) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("unsupported view for memory protection feature is disabled.")));
    }

    if (ctx_name == NULL || strlen(ctx_name) == 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("invalid name of memory context: NULL or \"\"")));
    }

    if (strlen(ctx_name) >= MEMORY_CONTEXT_NAME_LEN) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_NAME),
            errmsg("The name of memory context is too long(>=%dbytes)", MEMORY_CONTEXT_NAME_LEN)));
    }
}
#endif
/*
 * select gs_get_thread_memctx_detail(tid, 'CBBTopMemoryContext');
 * tid is from the 2sd item of pv_thread_memory_context();
 */
Datum gs_get_thread_memctx_detail(PG_FUNCTION_ARGS)
{
#ifndef MEMORY_CONTEXT_TRACK
    FuncCallContext* funcctx = NULL;
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        errmsg("unsupported view in lite mode or numa mode.")));
    SRF_RETURN_DONE(funcctx);
#else

    if (!superuser() && !isMonitoradmin(GetUserId())) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_PROC, "gs_get_thread_memctx_detail");
    }

    const ThreadId tid = PG_GETARG_INT64(0);
    char* ctx_name = TextDatumGetCString(PG_GETARG_TEXT_PP(1));

    gs_check_context_name_valid(ctx_name);

    if (tid == 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("invalid thread id %ld .", tid)));
    }

#define GS_THREAD_MEMCTX_VIEW_ATTRNUM 3
    FuncCallContext* funcctx = NULL;
    AllocChunk memctx_info_res;

    if (SRF_IS_FIRSTCALL()) {
        funcctx = SRF_FIRSTCALL_INIT();
        MemoryContext oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        StringInfoData mem_info;
        initStringInfo(&mem_info);

        if (tid == PostmasterPid) {
            if (!IsNormalProcessingMode()) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("the thread state is abnormal %ld\n", tid)));
            }
            gs_recursive_unshared_memory_context(PmTopMemoryContext, ctx_name, &mem_info);
        } else {
            bool find_thread = false;
            MemoryContext thrd_top_cxt = NULL;
            uint32 max_thread_count = g_instance.proc_base->allProcCount -
                g_instance.attr.attr_storage.max_prepared_xacts * NUM_TWOPHASE_PARTITIONS;
            volatile PGPROC* proc = NULL;
            for (uint32 idx = 0; idx < max_thread_count; idx++) {
                proc = g_instance.proc_base_all_procs[idx];
                if (proc->pid == tid) {
                    thrd_top_cxt = proc->topmcxt;
                    find_thread = true;
                    break;
                }
            }

            if (find_thread == false) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("can not find pid %ld\n", tid)));
            }

            PG_TRY();
            {
                (void)syscalllockAcquire(&((PGPROC*)proc)->deleMemContextMutex);
                gs_recursive_unshared_memory_context(thrd_top_cxt, ctx_name, &mem_info);
                (void)syscalllockRelease(&((PGPROC*)proc)->deleMemContextMutex);
            }
            PG_CATCH();
            {
                (void)syscalllockRelease(&((PGPROC*)proc)->deleMemContextMutex);
                PG_RE_THROW();
            }
            PG_END_TRY();
        }

        int memctx_info_len = 0;
        memctx_info_res = gs_collate_memctx_info(&mem_info, &memctx_info_len);

        funcctx->tuple_desc = get_memctx_view_first_row(GS_THREAD_MEMCTX_VIEW_ATTRNUM);
        funcctx->max_calls = memctx_info_len;
        funcctx->user_fctx = memctx_info_res;

        FreeStringInfo(&mem_info);
        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    memctx_info_res = (AllocChunk)(funcctx->user_fctx);
    if (funcctx->call_cntr < funcctx->max_calls) {
        AllocChunk memctx_chunk = memctx_info_res + funcctx->call_cntr;
        HeapTuple tuple = fetch_memctx_view_values(funcctx, memctx_chunk, GS_THREAD_MEMCTX_VIEW_ATTRNUM);
        pfree_ext(ctx_name);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }

    pfree_ext(memctx_info_res);
    pfree_ext(ctx_name);
    SRF_RETURN_DONE(funcctx);
#endif
}

/*
 * select gs_get_session_memctx_detail('CBBTopMemoryContext');
 */
Datum gs_get_session_memctx_detail(PG_FUNCTION_ARGS)
{
#ifndef MEMORY_CONTEXT_TRACK
        FuncCallContext* funcctx = NULL;
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("unsupported view in lite mode or numa mode.")));
        SRF_RETURN_DONE(funcctx);
#else

    if (!superuser() && !isMonitoradmin(GetUserId())) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_PROC, "gs_get_session_memctx_detail");
    }

    char* ctx_name = TextDatumGetCString(PG_GETARG_TEXT_PP(0));

    gs_check_context_name_valid(ctx_name);

    if (!ENABLE_THREAD_POOL) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("unsupported view for thread pool is disabled.")));
    }

#define GS_SESSION_MEMCTX_VIEW_ATTRNUM 3
    FuncCallContext* funcctx = NULL;
    AllocChunk memctx_info_res;
    knl_sess_control* sess = NULL;

    if (SRF_IS_FIRSTCALL()) {
        funcctx = SRF_FIRSTCALL_INIT();
        MemoryContext oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        StringInfoData mem_info;
        initStringInfo(&mem_info);
        g_threadPoolControler->GetSessionCtrl()->getSessionMemoryContextInfo(ctx_name, &mem_info, &sess);

        int memctx_info_len = 0;
        memctx_info_res = gs_collate_memctx_info(&mem_info, &memctx_info_len);

        funcctx->tuple_desc = get_memctx_view_first_row(GS_SESSION_MEMCTX_VIEW_ATTRNUM);
        funcctx->max_calls = memctx_info_len;
        funcctx->user_fctx = memctx_info_res;

        FreeStringInfo(&mem_info);
        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    memctx_info_res = (AllocChunk)(funcctx->user_fctx);
    if (funcctx->call_cntr < funcctx->max_calls) {
        AllocChunk memctx_chunk = memctx_info_res + funcctx->call_cntr;
        HeapTuple tuple = fetch_memctx_view_values(funcctx, memctx_chunk, GS_SESSION_MEMCTX_VIEW_ATTRNUM);
        pfree_ext(ctx_name);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }

    pfree_ext(memctx_info_res);
    pfree_ext(ctx_name);
    SRF_RETURN_DONE(funcctx);
#endif
}

/*
 * select gs_get_shared_memctx_detail('CBBTopMemoryContext');
 */
Datum gs_get_shared_memctx_detail(PG_FUNCTION_ARGS)
{
#ifndef MEMORY_CONTEXT_TRACK
        FuncCallContext* funcctx = NULL;
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("unsupported view in lite mode or numa mode.")));
        SRF_RETURN_DONE(funcctx);
#else

    if (!superuser() && !isMonitoradmin(GetUserId())) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_PROC, "gs_get_shared_memctx_detail");
    }

    char* ctx_name = TextDatumGetCString(PG_GETARG_TEXT_PP(0));

    gs_check_context_name_valid(ctx_name);

#define GS_SHARED_MEMCTX_VIEW_ATTRNUM 3
    FuncCallContext* funcctx = NULL;
    AllocChunk memctx_info_res;

    if (SRF_IS_FIRSTCALL()) {
        funcctx = SRF_FIRSTCALL_INIT();
        MemoryContext oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        StringInfoData mem_info;
        initStringInfo(&mem_info);
        gs_recursive_shared_memory_context(g_instance.instance_context, ctx_name, &mem_info, true);

        int memctx_info_len = 0;
        memctx_info_res = gs_collate_memctx_info(&mem_info, &memctx_info_len);

        funcctx->tuple_desc = get_memctx_view_first_row(GS_SHARED_MEMCTX_VIEW_ATTRNUM);
        funcctx->max_calls = memctx_info_len;
        funcctx->user_fctx = memctx_info_res;

        FreeStringInfo(&mem_info);
        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    memctx_info_res = (AllocChunk)(funcctx->user_fctx);
    if (funcctx->call_cntr < funcctx->max_calls) {
        AllocChunk memctx_chunk = memctx_info_res + funcctx->call_cntr;
        HeapTuple tuple = fetch_memctx_view_values(funcctx, memctx_chunk, GS_SHARED_MEMCTX_VIEW_ATTRNUM);
        pfree_ext(ctx_name);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }

    pfree_ext(memctx_info_res);
    pfree_ext(ctx_name);
    SRF_RETURN_DONE(funcctx);
#endif
}

