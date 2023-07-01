
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
    const char* ctx_name, StringInfoDataHuge* buf, bool isShared)
{
    bool checkLock = false;

    if (context == NULL) {
        return;
    }

    PG_TRY();
    {
        check_stack_depth();

        if (isShared) {
            MemoryContextLock(context);
            checkLock = true;
        }

        if (context->type == T_SharedAllocSetContext && strcmp(ctx_name, context->name) == 0) {
#ifndef ENABLE_MEMORY_CHECK
            GetAllocBlockInfo((AllocSet)context, buf);
#else
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
    const char* ctx_name, StringInfoDataHuge* buf)
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
static void gs_sort_memctx_info(AllocChunk memctx_info_res, int64 memctx_info_cnt, int* res_len)
{
    int64 i = 0;
    int64 j = 1;

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
    *res_len = (int)(i) + 1;
}

/*
 * collect the file and line info
 */
AllocChunk gs_collate_memctx_info(StringInfoHuge mem_info, int* res_len)
{
    if (mem_info == NULL) {
        *res_len = 0;
        return NULL;
    }

    int64 i = 0;
    int64 memctx_info_cnt = 0;

    /* find alloc chunk info count */
    for (i = 0; i < mem_info->len; ++i) {
        if (mem_info->data[i] == ':') {
            ++memctx_info_cnt;
        }
    }

    if (memctx_info_cnt == 0) {
        *res_len = 0;
        return NULL;
    }

    /* Traverse memory application information */
    AllocChunk memctx_info_res =
        (AllocChunk)palloc_huge(CurrentMemoryContext, sizeof(AllocChunkData) * memctx_info_cnt);
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
        errmsg("unsupported view in lite mode.")));
    SRF_RETURN_DONE(funcctx);
#else

    if (!superuser() && !isMonitoradmin(GetUserId())) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_PROC, "gs_get_thread_memctx_detail");
    }

    if (PG_ARGISNULL(1)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("second parameter should not be empty")));
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
        StringInfoDataHuge mem_info;
        initStringInfoHuge(&mem_info);

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

        FreeStringInfoHuge(&mem_info);
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
            errmsg("unsupported view in lite mode.")));
        SRF_RETURN_DONE(funcctx);
#else

    if (!superuser() && !isMonitoradmin(GetUserId())) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_PROC, "gs_get_session_memctx_detail");
    }

    if (PG_ARGISNULL(0)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("input parameter should not be empty")));
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

        StringInfoDataHuge mem_info;
        initStringInfoHuge(&mem_info);
        g_threadPoolControler->GetSessionCtrl()->getSessionMemoryContextInfo(ctx_name, &mem_info, &sess);

        int memctx_info_len = 0;
        memctx_info_res = gs_collate_memctx_info(&mem_info, &memctx_info_len);

        funcctx->tuple_desc = get_memctx_view_first_row(GS_SESSION_MEMCTX_VIEW_ATTRNUM);
        funcctx->max_calls = memctx_info_len;
        funcctx->user_fctx = memctx_info_res;

        FreeStringInfoHuge(&mem_info);
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
            errmsg("unsupported view in lite mode.")));
        SRF_RETURN_DONE(funcctx);
#else

    if (!superuser() && !isMonitoradmin(GetUserId())) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_PROC, "gs_get_shared_memctx_detail");
    }

    if (PG_ARGISNULL(0)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("input parameter should not be empty")));
    }

    char* ctx_name = TextDatumGetCString(PG_GETARG_TEXT_PP(0));

    gs_check_context_name_valid(ctx_name);

#define GS_SHARED_MEMCTX_VIEW_ATTRNUM 3
    FuncCallContext* funcctx = NULL;
    AllocChunk memctx_info_res;

    if (SRF_IS_FIRSTCALL()) {
        funcctx = SRF_FIRSTCALL_INIT();
        MemoryContext oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        StringInfoDataHuge mem_info;
        initStringInfoHuge(&mem_info);
        gs_recursive_shared_memory_context(g_instance.instance_context, ctx_name, &mem_info, true);

        int memctx_info_len = 0;
        memctx_info_res = gs_collate_memctx_info(&mem_info, &memctx_info_len);

        funcctx->tuple_desc = get_memctx_view_first_row(GS_SHARED_MEMCTX_VIEW_ATTRNUM);
        funcctx->max_calls = memctx_info_len;
        funcctx->user_fctx = memctx_info_res;

        FreeStringInfoHuge(&mem_info);
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


/*
 *------------------------------------------------------------------------
 * Read memory snapshot information
 *------------------------------------------------------------------------
 */
/*
 * get memory history log directory;
 */
static char* get_history_memory_log_dir()
{
    errno_t ss_rc;
    bool is_absolute = false;
    if (g_instance.stat_cxt.memory_log_directory == NULL) {
        return NULL;
    }

    char *dump_dir = (char*)palloc0(MAX_PATH_LEN);

    // get the path of dump file
    is_absolute = is_absolute_path(g_instance.stat_cxt.memory_log_directory);
    if (is_absolute) {
        ss_rc = snprintf_s(dump_dir, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s",
            g_instance.stat_cxt.memory_log_directory);
        securec_check_ss(ss_rc, "\0", "\0");
    } else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("mem_log directory does exist")));
    }

    struct stat info;
    if (stat(dump_dir, &info) == 0) {
        if (!S_ISDIR(info.st_mode)) {
            /* S_ISDIR() doesn't exist on current node */
            ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("mem_log directory does exist")));
        }
    }

    return dump_dir;
}

/*
 * get memory history log file list;
 */
static void get_history_memory_log_list(StringInfoDataHuge* buf)
{
    char *dump_dir = NULL;
    errno_t ss_rc;

    /* get the path of dump file */
    dump_dir = get_history_memory_log_dir();
    if (dump_dir == NULL) {
        return;
    }

    struct dirent* de = NULL;
    DIR *dir = opendir(dump_dir);
    char path_buf[MAXPGPATH] = {0};
    struct stat st;
    while ((de = readdir(dir)) != NULL) {
        if ((strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)) {
            continue;
        }
        ss_rc = snprintf_s(path_buf, MAXPGPATH, MAXPGPATH - 1, "%s/%s", dump_dir, de->d_name);
        securec_check_ss_c(ss_rc, "\0", "\0");
        if (lstat(path_buf, &st) != 0) {
            continue;
        }
        /* only find file */
        if (S_ISREG(st.st_mode)) {
            appendStringInfoHuge(buf, "%s\n", de->d_name);
        } else {
            continue;
        }
    }

    (void)closedir(dir);
}

/*
 * get special log info;
 */
static void get_history_memory_log_detail(StringInfoDataHuge* buf, char *file_name)
{
    char *dump_dir = NULL;
    char dump_file_path[MAX_PATH_LEN] = {0};
    errno_t ss_rc;

    // get the path of dump file
    dump_dir = get_history_memory_log_dir();
    if (dump_dir == NULL) {
        return;
    }

    ss_rc = snprintf_s(dump_file_path, sizeof(dump_file_path), sizeof(dump_file_path) - 1,
                         "%s/%s", dump_dir, file_name);
    securec_check_ss(ss_rc, "\0", "\0");

    char line[1024] = { 0 };
    int len = strlen(file_name);
    if (len > 3 && (strcmp(file_name + (len - 3), ".gz") == 0)) {
        gzFile gzfp = gzopen(dump_file_path, "r");
        if (!gzfp) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("failed to open file:%s", file_name)));
        }
        while (gzgets(gzfp, line, sizeof(line)) != NULL) {
            appendStringInfoHuge(buf, "%s", line);
        }
        (void)gzclose(gzfp);
    } else {
        FILE* fp = NULL;
        if ((fp = fopen(dump_file_path, "r")) == NULL) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("failed to open file:%s", file_name)));
        }

        while (fgets(line, sizeof(line), fp) != NULL) {
            appendStringInfoHuge(buf, "%s", line);
        }
        (void)fclose(fp);
    }
}

/*
 * insert log info per row;
 */
static void insert_memory_log_info(Tuplestorestate* tupStore, TupleDesc tupDesc, const char* meminfo)
{
    const int ATT_NUM = 1;
    Datum values[ATT_NUM];
    bool nulls[ATT_NUM];

    errno_t rc = 0;
    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    values[ARR_0] = CStringGetTextDatum(meminfo);

    tuplestore_putvalues(tupStore, tupDesc, values, nulls);
}

/*
 * encap memory history log info;
 */
static void encap_history_memory_info(Tuplestorestate* tupStore, TupleDesc tupDesc, StringInfoHuge buf,
    void (* insert)(Tuplestorestate* tupStore, TupleDesc tupDesc, const char* meminfo))
{
    int64 i = 0;
    int64 memctx_info_cnt = 0;
    
    /* find alloc chunk info count */
    for (i = 0; i < buf->len; ++i) {
        if (buf->data[i] == '\n') {
            ++memctx_info_cnt;
        }
    }

    if (memctx_info_cnt == 0) {
        return;
    }

    char* mem_info = buf->data;
    char* tmp_mem_info = buf->data;
    for (i = 0; i < memctx_info_cnt; ++i) {
        mem_info = tmp_mem_info;
        tmp_mem_info = strchr(mem_info, '\n');
        if (tmp_mem_info == NULL) {
            continue;
        }
        *tmp_mem_info = '\0';
        ++tmp_mem_info;
        insert(tupStore, tupDesc, mem_info);
    }
}

static void CheckFileNameValid(const char* filename)
{
    if (filename == NULL) {
        return;
    }

    if (path_contains_parent_reference(filename)) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                (errmsg("reference to parent directory (\"..\") not allowed"))));
    }

    int len = strlen(filename);
    if (len < MIN_FILE_NAME_LEN || len > MAX_FILE_NAME_LEN) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("file name is invalid:%s", filename)));
    }

    int prefixLen = strlen("mem_log");
    int suffixLogLen = strlen(".log");
    int suffixLogGzLen = strlen(".log.gz");
    if (strncmp(filename, "mem_log", prefixLen) != 0 ||
        (strncmp(filename + (len - suffixLogLen), ".log", suffixLogLen) != 0 &&
        strncmp(filename + (len - suffixLogGzLen), ".log.gz", suffixLogGzLen) != 0)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("file name is invalid:%s", filename)));
    }

    bool containOtherChar = false;
    for (int i = 0; i < len; i++) {
        if (filename[i] == '-' || filename[i] == '_' || filename[i] == '.' ||
            (filename[i] >= '0' && filename[i] <= '9') ||
            (filename[i] >= 'a' && filename[i] <= 'z')) {
            continue;
        }
        containOtherChar = true;
    }

    if (containOtherChar) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("file name is invalid:%s", filename)));
    }
}

/*
 * select gs_get_history_memory_detail(NULL) or select gs_get_history_memory_detail('meminfo_xxx.log')
 */
Datum gs_get_history_memory_detail(PG_FUNCTION_ARGS)
{
    if (!superuser() && !isMonitoradmin(GetUserId())) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_PROC, "gs_get_history_memory_detail");
    }

    char* file_name = PG_GETARG_CSTRING(0);
    CheckFileNameValid(file_name);

    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    MemoryContext oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);

    /* need a tuple descriptor representing 4 columns */
    TupleDesc tupdesc = CreateTemplateTupleDesc(1, false);

    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_1, "memory_info", TEXTOID, -1, 0);

    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->setDesc = BlessTupleDesc(tupdesc);

    (void)MemoryContextSwitchTo(oldcontext);

    StringInfoDataHuge buf;
    initStringInfoHuge(&buf);
    if (file_name == NULL) {
        get_history_memory_log_list(&buf);
    } else {
        get_history_memory_log_detail(&buf, file_name);
    }

    encap_history_memory_info(rsinfo->setResult, rsinfo->setDesc, &buf, insert_memory_log_info);

    pfree_ext(buf.data);
    /* clean up and return the tuplestore */
    tuplestore_donestoring(rsinfo->setResult);

    PG_RETURN_TEXT_P(NULL);
}

