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
 * gs_bbox.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/cbb/bbox/gs_bbox.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "bbox.h"
#include "bbox_types.h"

#include "postgres.h"

#include "gs_bbox.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "postmaster/postmaster.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/fatal_err.h"

#define BBOX_PATH_SIZE 512
#define DEFAULT_BLACKLIST_MASK (0xFFFFFFFFFFFFFFFF)

#define INVALID_TID (-1)

static char g_bbox_dump_path[BBOX_PATH_SIZE] = {0};

#ifdef ENABLE_UT
#define static
int bbox_handler_exit = 0;
#endif


BlacklistItem g_blacklist_items[] = {
    {SHARED_BUFFER, "SHARED_BUFFER", true},
    {XLOG_BUFFER, "XLOG_BUFFER", true},
    {DW_BUFFER, "DW_BUFFER", false},
    {XLOG_MESSAGE_SEND, "XLOG_MESSAGE_SEND", false},
    {DATA_MESSAGE_SEND, "DATA_MESSAGE_SEND", false},
    {WALRECIVER_CTL_BLOCK, "WALRECIVER_CTL_BLOCK", false},
    {DATA_WRITER_QUEUE, "DATA_WRITER_QUEUE", false}
};

/*
 * do FFIC and return whether is or not the first crash.
 */
static bool do_ffic(int sig, siginfo_t *si, void *uc)
{
    static volatile int64 first_tid = INVALID_TID;
    int64 cur_tid = (int64)pthread_self();

    if (first_tid == INVALID_TID &&
            __sync_bool_compare_and_swap(&first_tid, INVALID_TID, cur_tid)) {
        /* Only first fatal error will set db state and generate fatal error log */
        (void)SetDBStateFileState(COREDUMP_STATE, false);
        if (g_instance.attr.attr_common.enable_ffic_log) {
            (void)gen_err_msg(sig, si, (ucontext_t *)uc);
        }

        return true;
    } else {
        /*
         * Subsequent fatal error will go to here. If it comes from different thread,
         * wait until first error handler end, and if it is a reentry, terminate process.
         */
        if (first_tid != cur_tid) {
            (void)pause();
        }
        return false;
    }
}

/*
 * crash handler - handle signal depends on system kernel configuration
 */
static void coredump_handler(int sig, siginfo_t *si, void *uc)
{
    (void)do_ffic(sig, si, uc);
    (void)pqsignal(sig, SIG_DFL);
    (void)raise(sig);
}

/*
 * bbox_handler - handle signal conditions for bbox
 */
static void bbox_handler(int sig, siginfo_t *si, void *uc)
{
    if (do_ffic(sig, si, uc)) {
#ifndef ENABLE_MEMORY_CHECK
        sigset_t intMask;
        sigset_t oldMask;

        sigfillset(&intMask);
        pthread_sigmask(SIG_SETMASK, &intMask, &oldMask);

        (void)BBOX_CreateCoredump(NULL);
#else
#ifndef ENABLE_UT
        if (sig != SIGABRT)
            abort();
#endif
#endif

#ifdef ENABLE_UT
        if (bbox_handler_exit == 0)
#endif
            _exit(0);
    }
}

/*
 * get_bbox_coredump_pattern_path - get the core dump path from the file "/proc/sys/kernel/core_pattern"
 */
static void get_bbox_coredump_pattern_path(char* path, Size len)
{
    FILE* fp = NULL;
    char* p = NULL;
    struct stat stat_buf;

    if ((fp = fopen("/proc/sys/kernel/core_pattern", "r")) == NULL) {
        write_stderr("cannot open file: /proc/sys/kernel/core_pattern.\n");
        return;
    }

    if (fgets(path, len, fp) == NULL) {
        fclose(fp);
        write_stderr("failed to get the core pattern path.\n ");
        return;
    }
    fclose(fp);

    if ((p = strrchr(path, '/')) == NULL) { /* a relative-path file */
        *path = '\0';
    } else { /* an absolute-path file */
        *(++p) = '\0';
        if (stat(path, &stat_buf) != 0 || !S_ISDIR(stat_buf.st_mode) || access(path, W_OK) != 0) {
            write_stderr("The core dump path is an invalid directory\n");
            *path = '\0';
        }
    }
}

/* compute directory into which bbox dump core files are saved. */
static void build_bbox_corepath(char *bbox_core_path, Size path_size, char *config_path)
{
    struct stat stat_buf;

    /*
     * the guc parameter bbox_dump_path is set to NULL as default.
     * bbox_dump_path has to be a valid directory, if it is altered by users.
     */
    if (config_path != NULL && config_path[0] != '\0') {
        if (stat(config_path, &stat_buf) != 0 || !S_ISDIR(stat_buf.st_mode) || access(config_path, W_OK) != 0) {
            ereport(WARNING,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("bbox_dump_path %s is an invalid directory!\n", config_path)));

            /* if bbox_dump_path is invalid, the path of core dump will be set as default. */
            get_bbox_coredump_pattern_path(bbox_core_path, path_size);
        } else {
            errno_t rc = strcpy_s(bbox_core_path, path_size, config_path);
            securec_check(rc, "\0", "\0");
        }
    } else {
        /*
         * default path of the core dump will be obtained
         * by reading the file "/proc/sys/kernel/core_pattern"
         */
        get_bbox_coredump_pattern_path(bbox_core_path, path_size);
    }
}

/*
 * check_bbox_corepath - check coredump path for bbox
 */
bool check_bbox_corepath(char** newval, void** extra, GucSource source)
{
    if (t_thrd.proc_cxt.MyProcPid != PostmasterPid)
        return true;

    char core_dump_path[BBOX_PATH_SIZE] = {0};

    /* determine which path is used for bbox core dump file */
    build_bbox_corepath(core_dump_path, sizeof(core_dump_path), (newval != NULL) ? *newval : NULL);

    if (core_dump_path[0] != '\0' && BBOX_SetCoredumpPath(core_dump_path) == RET_OK) {
        ereport(LOG,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("bbox_dump_path is set to %s", core_dump_path)));
    }

    char* result = (char*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DFX), BBOX_PATH_SIZE);
    if (newval != NULL && *newval != NULL)
        pfree(*newval);

    errno_t rc = strcpy_s(result, BBOX_PATH_SIZE, core_dump_path);
    securec_check(rc, "\0", "\0");

    rc = strcpy_s(g_bbox_dump_path, sizeof(g_bbox_dump_path), result);
    securec_check(rc, "\0", "\0");

    if (newval != NULL) {
        *newval = result;
    } else {
        free(result);
    }

    return true;
}

/*
 * assign_bbox_corepath - set coredump path for bbox
 */
void assign_bbox_corepath(const char* newval, void* extra)
{
    if (t_thrd.proc_cxt.MyProcPid != PostmasterPid)
        return;
}

const char* show_bbox_dump_path(void)
{
    const char* path = g_bbox_dump_path;

    return (path != NULL) ? path : "";
}

static List* split_string_into_blacklist(const char* source)
{
    List *result = NIL;
    char *str = pstrdup(source);
    char *first_ch = str;
    int len = strlen(str) + 1;

    for (int i = 0; i < len; i++) {
        if (str[i] == ',' || str[i] == '\0') {
            /* replace ',' with '\0'. */
            str[i] = '\0';

            /* copy this into result. */
            result = lappend(result, pstrdup(first_ch));

            /* move to the head of next string. */
            first_ch = str + i + 1;
            i++;
        }
    }
    pfree(str);

    return result;
}


bool check_bbox_blacklist(char** newval, void** extra, GucSource source)
{
    if (t_thrd.proc_cxt.MyProcPid != PostmasterPid)
        return true;

    List *result = split_string_into_blacklist(*newval);
    ListCell *lc = NULL;
    uint64 mask = 0;
    size_t i;

    foreach(lc, result) {
        if (strcmp("", (char*)lfirst(lc)) == 0) {
            continue;
        }
        for (i = 0; i < sizeof(g_blacklist_items) / sizeof(BlacklistItem); i++) {
            if (strcmp(g_blacklist_items[i].blacklist_name, (char*)lfirst(lc)) == 0) {
                mask = mask | BLACKLIST_ITEM_MASK(g_blacklist_items[i].blacklist_ID);
                break;
            }
        }
        if (i == sizeof(g_blacklist_items) / sizeof(BlacklistItem)) {
            ereport(WARNING,
                (errmsg("blacklist item %s does not exist, so it is ignored.", (char*)lfirst(lc))));
        }
    }
    list_free_deep(result);

    *extra = MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DFX), sizeof(uint64));
    if (*extra == NULL)
        return false;

    *((uint64*)*extra) = (mask == 0) ? DEFAULT_BLACKLIST_MASK : mask;
    return true;
}

void assign_bbox_blacklist(const char* newval, void* extra)
{
    if (t_thrd.proc_cxt.MyProcPid == PostmasterPid) {
        g_instance.attr.attr_common.bbox_blacklist_mask = *((uint64*)extra);
    }
}

const char* show_bbox_blacklist()
{
    StringInfoData str;

    initStringInfo(&str);
    for (size_t i = 0; i < sizeof(g_blacklist_items) / sizeof(BlacklistItem); i++) {
        if ((BLACKLIST_ITEM_MASK(g_blacklist_items[i].blacklist_ID) & BBOX_BLACKLIST) != 0) {
            appendStringInfo(&str, "%s,", g_blacklist_items[i].blacklist_name);
        }
    }
    if (str.len >= 0) {
        str.data[--str.len] = '\0';
    }

    return str.data;
}

/*
 * assign_bbox_coredump - set coredump for bbox or not
 */
void assign_bbox_coredump(const bool newval, void* extra)
{
    if (t_thrd.proc_cxt.MyProcPid != PostmasterPid)
        return;

    if (newval && !FencedUDFMasterMode) {
        (void)install_signal(SIGABRT, bbox_handler);
        (void)install_signal(SIGBUS, bbox_handler);
        (void)install_signal(SIGILL, bbox_handler);
        (void)install_signal(SIGSEGV, bbox_handler);
    } else {
        (void)install_signal(SIGABRT, coredump_handler);
        (void)install_signal(SIGBUS, coredump_handler);
        (void)install_signal(SIGILL, coredump_handler);
        (void)install_signal(SIGSEGV, coredump_handler);
    }
}

/*
 * do initilaization for dumping core file
 */
void bbox_initialize()
{
    char core_dump_path[BBOX_PATH_SIZE] = {0};

    /* determine path which is used for bbox core dump file */
    build_bbox_corepath(core_dump_path, sizeof(core_dump_path),
        u_sess->attr.attr_common.bbox_dump_path);

    if (u_sess->attr.attr_common.enable_bbox_dump && *core_dump_path != '\0' &&
        CheckFilenameValid(core_dump_path) == RET_OK &&
        BBOX_SetCoredumpPath(core_dump_path) == 0) {
        write_stderr("bbox_dump_path is set to %s\n", core_dump_path);
    }

    /*
     * no matter bbox_dump_count is default (8) or set by users, call function BBOX_SetCoreFileCount.
     * Note: bbox_dump_count cannot be smaller than 1.
     */
    if (u_sess->attr.attr_common.bbox_dump_count != 0 &&
        BBOX_SetCoreFileCount(u_sess->attr.attr_common.bbox_dump_count) != 0) {
        write_stderr("failed to set coredump count.\n");
    }

    assign_bbox_coredump(u_sess->attr.attr_common.enable_bbox_dump, NULL);
}

/*
 * add an blacklist item to exclude it from core file.
 *      void *pAddress : the head address of excluded memory
 *      u64 uilen : memory size
 */
void bbox_blacklist_add(BlacklistIndex item, void* addr, uint64 size)
{
    if (t_thrd.proc_cxt.MyProcPid == PostmasterPid || !g_blacklist_items[item].pm_only) {
        if (BBOX_AddBlackListAddress(addr, size) != RET_OK) {
            ereport(WARNING,
                (errmsg("failed to add bbox blacklist item [%s]", g_blacklist_items[item].blacklist_name)));
        }
    }
}

/*
 * remove an blacklist item.
 *      void *pAddress : the head address of excluded memory
 */
void bbox_blacklist_remove(BlacklistIndex item, void* addr)
{
    if (addr != NULL && BBOX_RmvBlackListAddress(addr) != RET_OK) {
        ereport(WARNING,
            (errmsg("failed to remove bbox blacklist item [%s]", g_blacklist_items[item].blacklist_name)));
    }
}

/*
 * @Description: check the value from environment variablethe to prevent command injection.
 * @in input_env_value : the input value need be checked.
 *
 */
int CheckFilenameValid(const char* inputEnvValue)
{
    const int maxLen = 1024;

    const char* dangerCharacterList[] = {";", "`", "\\", "'", "\"", ">", "<", "$", "&", "|", "!", "\n", NULL};
    int i = 0;

    if (inputEnvValue == NULL || strlen(inputEnvValue) >= maxLen) {
        return RET_ERR;
    }

    for (i = 0; dangerCharacterList[i] != NULL; i++) {
        if (strstr((const char*)inputEnvValue, dangerCharacterList[i])) {
            return RET_ERR;
        }
    }
    return RET_OK;
}

