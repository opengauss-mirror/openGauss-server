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
 * -------------------------------------------------------------------------
 *
 * capture_view.cpp
 *    support utility to capture view to json file
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/instruments/capture_view/capture_view.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "fmgr.h"
#include "knl/knl_instance.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "commands/dbcommands.h"
#include "access/hash.h"
#include "utils/memutils.h"
#include "executor/spi.h"
#include "catalog/pg_database.h"
#include "postmaster/syslogger.h"
#include "utils/snapmgr.h"
#include "access/tableam.h"

#define VIEW_NAME_LEN (2 * NAMEDATALEN)
#define MAX_PERF_FILE_SIZE (64 * 1024 * 1024)
#define PERF_FILE_PREFIX "pg_perf-"

/*
 * We really want line-buffered mode for logfile output, but Windows does
 * not have it, and interprets _IOLBF as _IOFBF (bozos).  So use _IONBF
 * instead on Windows.
 */
#ifdef WIN32
#define LBF_MODE _IONBF
#else
#define LBF_MODE _IOLBF
#endif

typedef struct {
    char database_name[NAMEDATALEN];
    char view_name[VIEW_NAME_LEN];

} ViewJsonFileKey;

typedef struct {
    ViewJsonFileKey key;
    pthread_mutex_t mutex;
    FILE* perfFile; 
} ViewJsonFile;

typedef struct {
    const char *view_name;
    const char *filter;
} ViewFilter;

/* get valid and controlled json result set,
 * CAUTION:
 *   only support one single "'" in filter.
 */
static const ViewFilter perf_view_filter[] {
    {"pg_stat_activity", "where state != 'idle'"},
    {"dbe_perf.wait_events", "where wait != 0 or failed_wait != 0"},
    {"dbe_perf.session_memory", "where init_mem != 0 or used_mem != 0 or peak_mem != 0"},
    {"dbe_perf.session_memory_detail", "order by totalsize desc limit 1000"}
};

static char *jsonfile_getname(const char* database_name, const char* view_name);
static FILE *jsonfile_open(const char* database_name, const char* view_name);
static void rotate_view_file_if_needs(ViewJsonFile* entry);
static void lock_entry(ViewJsonFile *entry);
static void unlock_entry(ViewJsonFile *entry);
static void generate_json_to_view_file(ViewJsonFile* entry, const char* value);
static List *get_database_list();
static void output_query_to_json(const char *query, const char *database_name, const char *view_name);
static char *gen_perf_path(const char *database_name, const char *view_name);
static const char *get_view_filter(const char *view_name, bool need_double_quotes);

static uint32 view_json_file_hash(const void* key, Size size)
{
    const ViewJsonFileKey* k = (const ViewJsonFileKey*)key;

    return hash_any((const unsigned char*)k->database_name, strlen(k->database_name)) ^
        hash_any((const unsigned char*)k->view_name, strlen(k->view_name));
}

static int view_json_file_match(const void* key1, const void* key2, Size keysize)
{
    const ViewJsonFileKey* k1 = (const ViewJsonFileKey*)key1;
    const ViewJsonFileKey* k2 = (const ViewJsonFileKey*)key2;

    if (k1 != NULL && k2 != NULL &&
            strcmp(k1->database_name, k2->database_name) == 0 &&
            strcmp(k1->view_name, k2->view_name) == 0) {
        return 0;
    } else {
        return 1;
    }
}

void init_capture_view_hash()
{
    MemoryContext mem_cxt = AllocSetContextCreate(g_instance.instance_context,
        "CaptureViewContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        SHARED_CONTEXT);

    HASHCTL ctl;
    errno_t rc;
    rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check_c(rc, "\0", "\0");

    ctl.hcxt = mem_cxt;
    ctl.keysize = sizeof(ViewJsonFileKey);
    ctl.entrysize = sizeof(ViewJsonFile);
    ctl.hash = view_json_file_hash;
    ctl.match = view_json_file_match;

    g_instance.stat_cxt.capture_view_file_hashtbl = hash_create("view json file hash table", 100, &ctl,
        HASH_ELEM | HASH_SHRCTX | HASH_FUNCTION | HASH_COMPARE | HASH_NOEXCEPT);
}

static void output_json_file(ViewJsonFile* entry, const char *database_name, const char *view_name)
{
    lock_entry(entry);
    int saveInterruptHoldoffCount = t_thrd.int_cxt.InterruptHoldoffCount;

    PG_TRY();
    {
        if (entry->perfFile == NULL) {
            entry->perfFile = jsonfile_open(database_name, view_name);
        }
        rotate_view_file_if_needs(entry);

        for (uint32 i = 0; i < SPI_processed; i ++) {
            bool isnull = true;
            Datum val = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull);

            if (!isnull) {
                /* generate json to file */
                char* value = TextDatumGetCString(val);
                generate_json_to_view_file(entry, value);
            }
        }
        unlock_entry(entry);
    }
    PG_CATCH();
    {
        /*
         * handle ereport error will reset InterruptHoldoffCount issue, if not handle, 
         * LWLockRelease will be failed on assert
         */
        t_thrd.int_cxt.InterruptHoldoffCount = saveInterruptHoldoffCount;
        unlock_entry(entry);
        LWLockRelease(CaptureViewFileHashLock);

        PG_RE_THROW();
    }
    PG_END_TRY();
    LWLockRelease(CaptureViewFileHashLock);
}

static void cache_and_ouput_json_file(const char* database_name, const char* view_name)
{
    ViewJsonFileKey key = {0};
    int rc = 0;

    rc = memcpy_s(key.database_name, sizeof(key.database_name),
        database_name, strlen(database_name));
    securec_check(rc, "\0", "\0");

    rc = memcpy_s(key.view_name, sizeof(key.view_name),
        view_name, strlen(view_name));
    securec_check(rc, "\0", "\0");

    LWLockAcquire(CaptureViewFileHashLock, LW_SHARED);
    ViewJsonFile* entry = (ViewJsonFile*)hash_search(g_instance.stat_cxt.capture_view_file_hashtbl, &key, HASH_FIND, NULL);
    if (entry == NULL) {
        LWLockRelease(CaptureViewFileHashLock);
        LWLockAcquire(CaptureViewFileHashLock, LW_EXCLUSIVE);

        bool found = true;
        entry = (ViewJsonFile*)hash_search(g_instance.stat_cxt.capture_view_file_hashtbl, &key, HASH_ENTER, &found);
        if (entry == NULL) {
            LWLockRelease(CaptureViewFileHashLock);
            ereport(ERROR, (errmodule(MOD_INSTR), errcode_for_file_access(),
                errmsg("[CapView] OOM in capture view")));
            return;
        }

        if (!found) {
            /* reset entry, create file descriptor */
            (void)pthread_mutex_init(&entry->mutex, NULL);
            entry->perfFile = NULL;

            char *file_dir = gen_perf_path(database_name, view_name);
            if (file_dir != NULL) {
                /* race condition: need to create dir here */
                (void)pg_mkdir_p(file_dir, S_IRWXU);
                pfree(file_dir);
            }
        }

    }

    /* release lock in this method */
    output_json_file(entry, database_name, view_name);
}

static void generate_json_to_view_file(ViewJsonFile* entry, const char* json_row)
{
    if (entry == NULL || json_row == NULL)
        return;

    StringInfoData json_with_line_break;
    initStringInfo(&json_with_line_break);
    appendStringInfo(&json_with_line_break, "%s\n", json_row);

    size_t count = strlen(json_with_line_break.data);
    size_t rc = 0;

retry1:
    rc = fwrite(json_with_line_break.data, 1, count, entry->perfFile);
    if (rc != count) {
        /*
         * If no disk space, we will retry, and we can not report a log as
         * there is not space to write.
         */
        if (errno == ENOSPC) {
            pg_usleep(1000000);
            goto retry1;
        }

        pfree(json_with_line_break.data);
        ereport(ERROR, (errcode_for_file_access(), errmsg("[CapView] could not write to view perf file: %m")));
    }
    pfree(json_with_line_break.data);
}

static void rotate_view_file_if_needs(ViewJsonFile* entry)
{
    if (entry == NULL || entry->perfFile == NULL) {
        return;
    }

    long current_file_size = ftell(entry->perfFile);
    if (current_file_size < 0) {
        int save_errno = errno;

        ereport(ERROR, (errmodule(MOD_INSTR), errcode_for_file_access(),
            errmsg("[CapView] could not ftell json file :%m")));
        errno = save_errno;
    }
    if (current_file_size >= MAX_PERF_FILE_SIZE) {
        fclose(entry->perfFile);
        entry->perfFile = jsonfile_open(entry->key.database_name, entry->key.view_name);
    }
}

static void check_capture_view_parameter(const char *database_name, const char *view_name)
{
    if (database_name == NULL || view_name == NULL) {
        ereport(ERROR, (errmodule(MOD_INSTR), errmsg("[CapView] pls check database name and view name!")));
    }

    if (strlen(view_name) > VIEW_NAME_LEN) {
        ereport(ERROR, (errmodule(MOD_INSTR), errmsg("[CapView] view name is too long!")));
    }

    for (size_t i = 0; i < strlen(view_name); i++) {
        char chr = view_name[i];
        if ((chr >= 'a' && chr <= 'z') || (chr >= 'A' && chr <= 'Z') || (chr >= '0' && chr <= '9') ||
            chr == '_' || chr == '.') {
            continue;
        } else {
            ereport(ERROR, (errmodule(MOD_INSTR), errmsg("[CapView] invalid view name!")));
        }
    }
}

static void cross_db_view_to_json(const char *view_name, const char *main_query)
{
    /* cross database */
    List *all_db = get_database_list();
    ListCell *cell = NULL;
    if (all_db != NIL) {
        foreach(cell, all_db) {
            char *curr_db = (char *)lfirst(cell);

            StringInfoData db_query;
            initStringInfo(&db_query);
            appendStringInfo(&db_query, "select json from pg_catalog.wdr_xdb_query('dbname=%s', '%s') as r(json text)",
                curr_db, main_query);

            output_query_to_json(db_query.data, curr_db, view_name);
            pfree(db_query.data);
        }
    }
    list_free_deep(all_db);
}

/* support to capture view to json file */
Datum capture_view_to_json(PG_FUNCTION_ARGS)
{
    if (!superuser() && !isMonitoradmin(GetUserId()))
        ereport(ERROR, (errmodule(MOD_INSTR), errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            (errmsg("[CapView] only system/monitor admin can capure view"))));

    if (PG_ARGISNULL(0) || PG_ARGISNULL(1)) {
        ereport(ERROR, (errmodule(MOD_INSTR),
            errmsg("[CapView] in capture_view_to_json proc, view_name or is_all_db can not by null")));
    }

    int is_all_db = PG_GETARG_INT32(1);
    if (is_all_db != 0 && is_all_db != 1) {
        ereport(ERROR, (errmodule(MOD_INSTR),
            errmsg("[CapView] in capture_view_to_json proc, is_all_db can only be 0 or 1")));
    }

    char *view_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char *database_name = get_and_check_db_name(u_sess->proc_cxt.MyDatabaseId, true);
    check_capture_view_parameter(database_name, view_name);

    char quota[3] = {0};
    quota[0] = '\'';

    if (is_all_db == 1) {
        quota[1] = '\'';
    }

    int rc = 0;
    const char *filter = get_view_filter(view_name, is_all_db == 1);

    StringInfoData query;
    initStringInfo(&query);
    appendStringInfo(&query, "select pg_catalog.row_to_json(src_view) as json from ("
                             "    select * from "
                             "    pg_catalog.floor(EXTRACT(epoch FROM pg_catalog.clock_timestamp()) * 1000) "
                             "    as record_time, "
                             "    pg_catalog.current_setting(%spgxc_node_name%s) as perf_node_name, "
                             "    pg_catalog.current_database() as perf_database_name, "
                             "    (select %s%s%s as perf_view_name), "
                             "    %s %s) as src_view;", quota, quota, quota, view_name, quota, view_name, filter);
    pfree_ext(filter);
    MemoryContext oldcontext = CurrentMemoryContext;
    /* get query result to json */
    SPI_STACK_LOG("connect", NULL, NULL);
    if ((rc = SPI_connect()) != SPI_OK_CONNECT) {
        ereport(ERROR,
            (errmodule(MOD_INSTR),
            (errcode(ERRCODE_SPI_CONNECTION_FAILURE),
             errmsg("[CapView] SPI_connect failed: %s", SPI_result_code_string(rc)))));
    }

    ErrorData* edata = NULL;

    PG_TRY();
    {
        if (is_all_db == 0) {
            /* current database */
            output_query_to_json(query.data, database_name, view_name);
        } else if (is_all_db == 1) {
            cross_db_view_to_json(view_name, query.data);
        }
    }
    PG_CATCH();
    {
        SPI_STACK_LOG("finish", NULL, NULL);
        SPI_finish();
        pfree(query.data);
        pfree(view_name);
        pfree(database_name);

        (void)MemoryContextSwitchTo(oldcontext);
        edata = CopyErrorData();
        FlushErrorState();

        ereport(ERROR, (errmodule(MOD_INSTR),
            errmsg("[CapView] capture view to json failed! detail: %s", edata->message)));
    }
    PG_END_TRY();

    SPI_STACK_LOG("finish", NULL, NULL);
    SPI_finish();
    pfree(query.data);
    pfree(view_name);
    pfree(database_name);

    PG_RETURN_INT32(0);
}

static void lock_entry(ViewJsonFile *entry)
{
    pthread_mutex_lock(&entry->mutex);
}

static void unlock_entry(ViewJsonFile *entry)
{
    pthread_mutex_unlock(&entry->mutex);
}

void init_capture_view()
{
    /* create perf directory if not present, ignore errors */
    if (g_instance.attr.attr_common.Perf_directory == NULL) {
        init_instr_log_directory(true, PERF_JOB_TAG);
    } else {
        (void)pg_mkdir_p(g_instance.attr.attr_common.Perf_directory, S_IRWXU);
    }
    init_capture_view_hash();
}

static FILE* jsonfile_open(const char* database_name, const char* view_name)
{
    char* filename = jsonfile_getname(database_name, view_name);
    bool exist = false;

    if (filename == NULL) {
        ereport(ERROR, (errmodule(MOD_INSTR), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
            errmsg("[CapView] json file can not by NULL")));
    }

    char rel_path[PATH_MAX] = {0};
    if (strlen(filename) >= MAXPGPATH || (realpath(filename, rel_path) == NULL && errno != ENOENT)) {
        pfree(filename);
        ereport(ERROR, (errmodule(MOD_INSTR), errmsg("[CapView] calc realpath failed")));
    }

    struct stat checkdir;
    if (stat(rel_path, &checkdir) == 0) {
        exist = true;
    }

    FILE* fh = NULL;
    fh = fopen(rel_path, "a");

    if (fh != NULL) {
        setvbuf(fh, NULL, LBF_MODE, 0);
    } else {
        int save_errno = errno;

        ereport(ERROR, (errcode_for_file_access(),
            errmsg("[CapView] could not open log file \"%s\": %m", filename)));
        errno = save_errno;
    }

    if (!exist) {
        if (chmod(filename, S_IWUSR | S_IRUSR) < 0) {
            int save_errno = errno;

            ereport(ERROR, (errcode_for_file_access(),
                errmsg("[CapView] could not chmod view json file \"%s\": %m", filename)));
            errno = save_errno;
        }
    }

    pfree(filename);
    return fh;
}

/* replace '.' with '_' from view name */
static char *format_view_name(const char *view_name)
{
    if (view_name == NULL)
        return NULL;
    char *tmp_view_name = pstrdup(view_name);
    for (size_t i = 0; i < strlen(tmp_view_name); i ++) {
        if (tmp_view_name[i] == '.') {
            tmp_view_name[i] = '_';
        }
    }

    return tmp_view_name;
}

static char *gen_perf_path(const char *database_name, const char *view_name)
{
    if (database_name == NULL || view_name == NULL)
        return NULL;

    char *formatted_view_name = format_view_name(view_name);
    StringInfoData path;

    initStringInfo(&path);
    appendStringInfo(&path, "%s/", g_instance.attr.attr_common.Perf_directory);
    appendStringInfo(&path, "%s/", formatted_view_name);
    appendStringInfo(&path, "%s/", database_name);
    pfree(formatted_view_name);

    return path.data;
}

static char *jsonfile_getname(const char* database_name, const char* view_name)
{
    int ret = 0;
    size_t len = 0;
    pg_time_t file_time = time(NULL);

    char *path = gen_perf_path(database_name, view_name);
    if (path == NULL) {
        return NULL;
    }

    char *jsonfile_name = NULL;
    jsonfile_name = (char*)palloc0(MAXPGPATH + 1);

    /* If want to use cm agent to cleanup perf files, must obey the
     * file name format: pg_perf-2020-03-23_072301.database-view.log
     * for example:
     *   pg_perf-2020-03-23_072301.postgres-dbe_perf.statement.log
     *   pg_perf-2020-03-23_072301.postgres-pg_proc.log
     */
    ret = snprintf_s(jsonfile_name + len, MAXPGPATH - len, MAXPGPATH - len - 1, "%s/%s",
        path, PERF_FILE_PREFIX); 
    pfree(path);
    securec_check_ss(ret,"\0","\0");

    pg_tm* tz = pg_localtime(&file_time, log_timezone);
    if (tz == NULL) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("[CapView] calc localtime failed")));
    }

    len = strlen(jsonfile_name);
    pg_strftime(jsonfile_name + len, MAXPGPATH - len, "%Y-%m-%d_%H%M%S", tz);

    len = strlen(jsonfile_name);
    ret = snprintf_s(jsonfile_name + len, MAXPGPATH - len, MAXPGPATH - len - 1, ".%s-%s.log", database_name, view_name);
    securec_check_ss(ret,"\0","\0");

    len = strlen(jsonfile_name);

    return jsonfile_name;
}

static void output_query_to_json(const char *query, const char *database_name, const char *view_name) {
    if (SPI_execute(query, true, 0) != SPI_OK_SELECT) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("[CapView] invalid query")));
    }

    if (SPI_processed > 0) {
        cache_and_ouput_json_file(database_name, view_name);
    }
}

static List *get_database_list()
{
    Relation rel = NULL;
    HeapTuple tup = NULL;
    TableScanDesc scan = NULL;
    List *dblist = NIL;

    rel = heap_open(DatabaseRelationId, AccessShareLock);
    scan = tableam_scan_begin(rel, SnapshotNow, 0, NULL);

    while (HeapTupleIsValid(tup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection))) {
        Form_pg_database database = (Form_pg_database) GETSTRUCT(tup);

        if (!database->datistemplate) {
            dblist = lappend(dblist, pstrdup(NameStr(database->datname)));
        }
    }

    tableam_scan_end(scan);
    heap_close(rel, AccessShareLock);

    return dblist;
} 

static const char *get_view_filter(const char *view_name, bool need_double_quotes) {
    Size filter_count = sizeof(perf_view_filter) / sizeof(perf_view_filter[0]);
    const char *filter = NULL;

    StringInfoData filter_str;
    initStringInfo(&filter_str);

    for (Size i = 0; i < filter_count; i ++) {
        if (pg_strcasecmp(perf_view_filter[i].view_name, view_name) == 0) {
            filter = perf_view_filter[i].filter;
            break;
        }
    }
    if (filter == NULL) {
        return filter_str.data;
    }

    if (need_double_quotes) {
        for (Size i = 0; i < strlen(filter); i ++) {
            if (filter[i] == '\'') {
                appendStringInfo(&filter_str, "%c%c", '\'', '\'');
            } else {
                appendStringInfo(&filter_str, "%c", filter[i]);
            }
        }
    } else {
        appendStringInfo(&filter_str, "%s", filter);
    }

    return filter_str.data;
}
