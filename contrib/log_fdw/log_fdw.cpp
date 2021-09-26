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
 * log_fdw.cpp
 *  foreign-data wrapper for accessing log data
 * 
 * 
 * IDENTIFICATION
 *        contrib/log_fdw/log_fdw.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>

#include "log_fdw_private.h"
#include "log_fdw.h"
#include "access/reloptions.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_type.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/pgxc_node.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "getaddrinfo.h"
#include "libpq/ip.h"
#include "nodes/makefuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/predtest.h"
#include "optimizer/var.h"
#include "parser/parse_type.h"
#include "postmaster/syslogger.h"
#include "storage/lock/lock.h"
#include "utils/acl.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"
#include "utils/zfiles.h"
#include "miscadmin.h"
#include "zlib.h"
#include "unzip.h"
#include "ioapi.h"

static const char* valid_log_type[] = {"pg_log", "gs_profile"};

extern Datum get_hostname(PG_FUNCTION_ARGS);

/* SQL functions version */
PG_FUNCTION_INFO_V1(log_fdw_handler);
PG_FUNCTION_INFO_V1(log_fdw_validator);

/* macro for timing */
#define PERF_TRACE(MACRO_FUNC, DESC_TAG)                   \
    do {                                                   \
        if (unlikely(festate->m_need_timing)) {            \
            MACRO_FUNC(festate->m_plan_node_id, DESC_TAG); \
        }                                                  \
    } while (0)

/*
 * FDW callback routines
 */
static bool match_dir_name(const char* subdir_name);
static bool pglog_fname_match(const char* log_name);
static void estimate_costs(
    PlannerInfo* root, RelOptInfo* baserel, logFdwPlanState* fdw_private, Cost* startup_cost, Cost* total_cost);

static void profilelog_begin_fs(ForeignScanState* node, int eflags);
static TupleTableSlot* profilelog_iterate_fs(ForeignScanState* node);
static void profilelog_end_fs(ForeignScanState* node);
static void profilelog_rescan_fs(ForeignScanState* node);
static void profilelog_explain_fs(ForeignScanState* node, ExplainState* es);

/*
 *----------------------------------------------------------
 *      Static and Common Utils Area
 *----------------------------------------------------------
 */

static inline Datum get_hostname_text(void)
{
    /* get hostname */
    FunctionCallInfo fcinfo = NULL;
    return get_hostname(fcinfo);
}

static text* fetch_dirname_from_logfile(const char* logfile)
{
    size_t len = strlen(logfile);
    text* dirname = NULL;
    char* slash1 = (char*)memrchr(logfile, '/', len);
    if (slash1 && '\0' != *(slash1 + 1)) {
        /* skip the first '/' */
        --slash1;

        char* slash2 = (char*)memrchr(logfile, '/', (slash1 - logfile));
        int dirlen = 0;
        if (slash2 && '\0' != *(slash2 + 1)) {
            dirlen = slash1 - slash2; /* not include tail 0 */
            dirname = cstring_to_text_with_len(slash2 + 1, dirlen);
        } else {
            /* used for relative log path */
            dirlen = ++slash1 - logfile;
            dirname = cstring_to_text_with_len(logfile, dirlen);
        }
    } else {
        ereport(
            ERROR, (errcode(ERRCODE_UNDEFINED_FILE), errmsg("not found the first '/' in file path \"%s\" ", logfile)));
    }
    return dirname;
}

static text* fetch_filename_from_logfile(const char* logfile)
{
    size_t len = strlen(logfile);
    text* filename = NULL;
    char* slash1 = (char*)memrchr(logfile, '/', len);
    if (slash1 && '\0' != *(slash1 + 1)) {
        filename = cstring_to_text_with_len(slash1 + 1, len - (slash1 - logfile + 1));
    } else {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FILE), errmsg("not found '/' in file path \"%s\" ", logfile)));
    }
    return filename;
}

static inline text* fetch_dirname_from_path(const char* dpath)
{
    return fetch_filename_from_logfile(dpath);
}

static void set_io_functions(logFdwPlanState* festate)
{
    size_t len = strlen(festate->logfile_info->name);

    if ((len > 3) && 0 == pg_strncasecmp(".gz", (festate->logfile_info->name + (len - 3)), 3)) {
        /* GZ IO functions */
        festate->m_open = gz_open;
        festate->m_read = gz_read;
        festate->m_close = gz_close;
        return;
    }

    if ((len > 4) && 0 == pg_strncasecmp(".zip", (festate->logfile_info->name + (len - 4)), 4)) {
        /* ZIP IO functions */
        festate->m_open = unzip_open;
        festate->m_read = unzip_read;
        festate->m_close = unzip_close;
        return;
    }

    /* default IO functions, for example '.log' */
    festate->m_open = vfd_file_open;
    festate->m_read = vfd_file_read;
    festate->m_close = vfd_file_close;
}

/*
 *----------------------------------------------------------
 *      Filter and Refuted Area
 *
 * 1) all target attrno list don't touch log data, which means disk IO never happens.
 * 2) refuted by hostname info
 * 3) refuted by dirname info
 * 4) refuted by log file name info
 * 5) refuted by log file CREATE and MODIFY time info
 *----------------------------------------------------------
 */

static bool target_list_need_log_data(ForeignScanState* node)
{
    /* targetlist is null when COUNT(*) occurs, need to load log data */
    if (NULL == node->ss.ps.targetlist) {
        return true;
    }

    List* all_attnos = GetAccessedVarnoList(node->ss.ps.targetlist, (node->ss.ps.plan ? node->ss.ps.plan->qual : NULL));
    ListCell* attno_cell = NULL;
    bool need_logdata = false;

    /*
     * if all the attributes both in SELECT and WHERE target list are in
     * { hostname, dirname, filename }, then needn't to open/read/close
     * log files, in order bo avoid disk IO.
     */
    foreach (attno_cell, all_attnos) {
        AttrNumber attno = (AttrNumber)lfirst_int(attno_cell);
        if (attno > PGLOG_ATTR_HOSTNAME + 1) {
            need_logdata = true;
            break;
        }
    }
    list_free(all_attnos);
    all_attnos = NIL;
    return need_logdata;
}

static void fillup_need_check_flags(ForeignScanState* node, FdwLogType logypte)
{
    static const AttrNumber checked_attrs[FLT_UNKNOWN][4] = {/* fields: dirname, filename, hostname, logtime */
        {PGLOG_ATTR_DIRNAME, PGLOG_ATTR_FILENAME, PGLOG_ATTR_HOSTNAME, PGLOG_ATTR_LOGTIME},
        {PROFILELOG_ATTR_DIRNAME, PROFILELOG_ATTR_FILENAME, PROFILELOG_ATTR_HOSTNAME, PROFILELOG_ATTR_LOGTIME}};

    /* only care all the attrno from WHERE clause */
    List* qual = node->ss.ps.plan ? node->ss.ps.plan->qual : NULL;
    if (NULL == qual) {
        return;
    }

    logFdwPlanState* festate = (logFdwPlanState*)node->fdw_state;
    List* vars = pull_var_clause((Node*)qual, PVC_RECURSE_AGGREGATES, PVC_RECURSE_PLACEHOLDERS);
    ListCell* var_cell = NULL;

    /*
     * because it's easy to make a wrong Var for different columns of different types,
     * so we remember them directly. and they will be used to filter optimization.
     */
    foreach (var_cell, vars) {
        Var* var = (Var*)lfirst(var_cell);
        if ((checked_attrs[logypte][0] + 1) == var->varattno) {
            festate->m_need_check_dirname = true;
            festate->m_tmp_dirname_var = *var;
        } else if ((checked_attrs[logypte][1] + 1) == var->varattno) {
            festate->m_need_check_logname = true;
            festate->m_tmp_logname_var = *var;
        } else if ((checked_attrs[logypte][2] + 1) == var->varattno) {
            festate->m_need_check_hostname = true;
            festate->m_tmp_hostname_var = *var;
        } else if ((checked_attrs[logypte][3] + 1) == var->varattno) {
            festate->m_need_check_logtime = true;
            festate->m_tmp_logtime_var = *var;
        }
    }

    if (festate->m_need_check_logtime) {
        /* use the now time as the biggest MODIFY time */
        festate->m_last_modify_tm_maxval = time(NULL);
    }
}

static Oid get_operator_by_typeid(Oid typeID, Oid accessMethodId, int16 strategyNumber)
{
    /* Get default operator class from pg_opclass */
    Oid operatorClassId = GetDefaultOpClass(typeID, accessMethodId);

    if (InvalidOid == operatorClassId) {
        ereport(ERROR,
            (errcode(ERRCODE_CASE_NOT_FOUND), errmodule(MOD_DFS), errmsg("Invalid Oid for operator %d.", typeID)));
    }

    Oid operatorFamily = get_opclass_family(operatorClassId);

    Oid operatorId = get_opfamily_member(operatorFamily, typeID, typeID, strategyNumber);

    return operatorId;
}

static OpExpr* make_operator_expression(Var* variable, int16 strategyNumber)
{
    Oid typeID = variable->vartype;
    Oid typeModId = variable->vartypmod;
    Oid collationId = variable->varcollid;

    Oid accessMethodId = BTREE_AM_OID;
    Oid OpId = InvalidOid;
    Const* ConstValue = NULL;
    OpExpr* expr = NULL;
    Expr* leftop = (Expr*)variable;

    /* Loading the operator from catalogs */
    ConstValue = makeNullConst(typeID, typeModId, collationId);

    OpId = get_operator_by_typeid(typeID, accessMethodId, strategyNumber);

    /* Build the expression with the given variable and a null constant */
    expr = (OpExpr*)make_opclause(OpId,
        InvalidOid, /* no result type yet */
        false,      /* no return set */
        leftop,
        (Expr*)ConstValue,
        InvalidOid,
        collationId);

    /* Build up implementing function id and result type */
    expr->opfuncid = get_opcode(OpId);
    expr->opresulttype = get_func_rettype(expr->opfuncid);

    return expr;
}
static Node* build_const_constraint(Expr* equalExpr, Datum value, bool isNull)
{
    Const* constant = (Const*)get_rightop(equalExpr);
    Assert(NULL != constant);

    constant->constvalue = value;
    constant->constisnull = isNull ? true : false;

    return (Node*)equalExpr;
}

static bool whether_refuted_by_range(
    ForeignScanState* node, Var* var_node, int range_column, Datum const_v1, Datum const_v2)
{
    Var tmp_node = *var_node;
    /* CREATE time <= range < next CREATE time */
    OpExpr* great_than = make_operator_expression(&tmp_node, BTGreaterEqualStrategyNumber);
    OpExpr* less_than = make_operator_expression(&tmp_node, BTLessStrategyNumber);

    Node* min_node = get_rightop((Expr*)great_than);
    Assert(IsA(min_node, Const));
    Const* min_constant = (Const*)min_node;
    min_constant->constvalue = const_v1;
    min_constant->constisnull = false;

    Node* max_node = get_rightop((Expr*)less_than);
    Assert(IsA(max_node, Const));
    Const* max_constant = (Const*)max_node;
    max_constant->constvalue = const_v2;
    max_constant->constisnull = false;

    Node* range_constraint = make_and_qual((Node*)less_than, (Node*)great_than);
    List* base_restriction = lappend(NIL, range_constraint);
    List* qual = node->ss.ps.plan->qual;
    Assert(node->ss.ps.plan && node->ss.ps.plan->qual);
    bool refuted = predicate_refuted_by(base_restriction, qual, true);

    list_free(base_restriction);
    base_restriction = NIL;
    pfree_ext(great_than);
    pfree_ext(less_than);
    return refuted;
}

static bool whether_refuted_by(ForeignScanState* node, Var* var_node, int text_column, Datum const_val)
{
    Var tmp_node = *var_node;
    Expr* expr_node = (Expr*)make_operator_expression(&tmp_node, BTEqualStrategyNumber);
    Node* const_constraint = build_const_constraint(expr_node, const_val, false);

    List* base_restriction = lappend(NIL, const_constraint);
    List* qual = node->ss.ps.plan->qual;
    Assert(node->ss.ps.plan && node->ss.ps.plan->qual);
    bool refuted = predicate_refuted_by(base_restriction, qual, true);

    /* free memory */
    list_free(base_restriction);
    base_restriction = NIL;
    pfree_ext(expr_node);

    return refuted;
}

static bool whether_refuted_by_hostname(ForeignScanState* node)
{
    logFdwPlanState* festate = (logFdwPlanState*)node->fdw_state;
    /* check whether refuted by my hostname */
    bool refuted =
        whether_refuted_by(node, &(festate->m_tmp_hostname_var), PGLOG_ATTR_HOSTNAME, festate->m_tmp_hostname_text);
    return refuted;
}

static bool whether_refuted_by_dirname(ForeignScanState* node, Datum dirname)
{
    logFdwPlanState* festate = (logFdwPlanState*)node->fdw_state;
    return whether_refuted_by(node, &(festate->m_tmp_dirname_var), PGLOG_ATTR_DIRNAME, dirname);
}

static bool whether_refuted_by_logname(ForeignScanState* node, Datum fname)
{
    logFdwPlanState* festate = (logFdwPlanState*)node->fdw_state;
    return whether_refuted_by(node, &(festate->m_tmp_logname_var), PGLOG_ATTR_FILENAME, fname);
}

static bool whether_refuted_by_logtime(ForeignScanState* node, log_file_info* logfile)
{
    /* convert "time_t" to "timestampe with time zone" value and compare */
    TimestampTz create_t = time_t_to_timestamptz((pg_time_t)logfile->tm_create);
    TimestampTz next_create_t = time_t_to_timestamptz((pg_time_t)logfile->tm_last_modify);
    logFdwPlanState* festate = (logFdwPlanState*)node->fdw_state;
    return whether_refuted_by_range(node,
        &(festate->m_tmp_logtime_var),
        PGLOG_ATTR_LOGTIME,
        TimestampTzGetDatum(create_t),
        TimestampTzGetDatum(next_create_t));
}

static List* filter_dirname_list(ForeignScanState* node, List* dir_list)
{
    ListCell* dir_cell = NULL;
    ListCell* dir_prev_cell = NULL;
    ListCell* dir_next_cell = NULL;
    uint32 refuted_num = 0;

    for (dir_cell = list_head(dir_list); dir_cell; dir_cell = dir_next_cell) {
        /* remember the next valid cell */
        dir_next_cell = lnext(dir_cell);

        /* dpath is the absolute path, not the name of its directory */
        char* dpath = (char*)lfirst(dir_cell);
        /* keep the same TEXT datum format with WHERE clause */
        text* dname = fetch_dirname_from_path(dpath);

        if (whether_refuted_by_dirname(node, PointerGetDatum(dname))) {
            /* ignore this directory path */
            dir_list = list_delete_cell(dir_list, dir_cell, dir_prev_cell);
            ++refuted_num;
            /* don't change file_prev_cell */
        } else {
            dir_prev_cell = dir_cell;
        }
        pfree_ext(dname);
    }

    /* remember the number of refuted dirname */
    logFdwPlanState* festate = (logFdwPlanState*)node->fdw_state;
    festate->m_refuted_dirname_num += refuted_num;
    return dir_list;
}

static List* filter_logname_list_by_name(ForeignScanState* node, List* fname_list)
{
    ListCell* file_cell = NULL;
    ListCell* file_next_cell = NULL;
    ListCell* file_prev_cell = NULL;
    uint32 refuted_num_byname = 0;

    for (file_cell = list_head(fname_list); file_cell; file_cell = file_next_cell) {
        /* remember the next valid cell */
        file_next_cell = lnext(file_cell);

        /* file_info->name is the absolute path, not the name of its directory */
        log_file_info* file_info = (log_file_info*)lfirst(file_cell);
        /* keep the same TEXT datum format with WHERE clause */
        text* fname = fetch_filename_from_logfile(file_info->name);

        if (whether_refuted_by_logname(node, PointerGetDatum(fname))) {
            /* ignore this log file */
            fname_list = list_delete_cell(fname_list, file_cell, file_prev_cell);
            ++refuted_num_byname;
            /* don't change file_prev_cell */
        } else {
            file_prev_cell = file_cell;
        }
        pfree_ext(fname);
    }

    /* remember the number of refuted log file */
    logFdwPlanState* festate = (logFdwPlanState*)node->fdw_state;
    festate->m_refuted_logfile_num_by_name += refuted_num_byname;
    return fname_list;
}

static List* filter_logname_list_by_time(ForeignScanState* node, List* fname_list)
{
    logFdwPlanState* festate = (logFdwPlanState*)node->fdw_state;
    ListCell* cell = NULL;
    ListCell* next_cell = NULL;
    ListCell* prev_cell = NULL;
    uint32 refuted_num_bytime = 0;

    for (cell = list_head(fname_list); cell; cell = next_cell) {
        /* remember the next valid cell */
        next_cell = lnext(cell);

        log_file_info* file_info = (log_file_info*)lfirst(cell);
        if (NULL != next_cell) {
            /* delay to set MODIFY time by using CREATE time of next log file */
            log_file_info* next_file = (log_file_info*)lfirst(next_cell);
            file_info->tm_last_modify = next_file->tm_create;
        } else {
            /* set now time info */
            file_info->tm_last_modify = festate->m_last_modify_tm_maxval;
        }

        if (whether_refuted_by_logtime(node, file_info)) {
            fname_list = list_delete_cell(fname_list, cell, prev_cell);
            ++refuted_num_bytime;
        } else {
            prev_cell = cell;
        }
    }

    /* remember the number of refuted log file */
    festate->m_refuted_logfile_num_by_time += refuted_num_bytime;
    return fname_list;
}

/*
 *----------------------------------------------------------
 *      Directory Task Assignment Area
 *----------------------------------------------------------
 */

/* compare function for string type */
static int strcmp_wrapper(const void* s1, const void* s2)
{
    const char* str1 = *(char**)s1;
    const char* str2 = *(char**)s2;
    return strcmp((const char*)str1, (const char*)str2);
}

/* compare function for string-node type */
static int str_node_cmp(const void* s1, const void* s2)
{
    Value* v1 = *(Value**)s1;
    Value* v2 = *(Value**)s2;
    return strcmp(v1->val.str, v2->val.str);
}

/* compare function for log access time */
static int cmp_latest_with_create_time(const void* s1, const void* s2)
{
    log_file_info* log1 = *(log_file_info**)s1;
    log_file_info* log2 = *(log_file_info**)s2;
    /* make the newest file in the front */
    return (log2->tm_create - log1->tm_create);
}

static int rcmp_latest_with_create_time(const void* s1, const void* s2)
{
    return cmp_latest_with_create_time(s2, s1);
}

static void make_string_list_sorted(List* str_list, cmp_func cmp)
{
    int len = list_length(str_list);
    if (0 == len || 1 == len) {
        return; /* empty or single list */
    }

    void** ptr_array = (void**)palloc(len * sizeof(char*));
    ListCell* cell = NULL;

    /* contruct a temp list to sort */
    int idx = 0;
    foreach (cell, str_list) {
        ptr_array[idx++] = (void*)lfirst(cell);
    }

    /* do sorting */
    qsort(ptr_array, len, sizeof(void*), cmp);

    /* write back to make this list sorted */
    idx = 0;
    foreach (cell, str_list) {
        lfirst(cell) = ptr_array[idx++];
    }

    /* free this temp array */
    pfree_ext(ptr_array);
}

/*
 * extract log file's CREATE time from its file name.
 *
 * log file name is like: postgresql-2018-06-19_203948.log
 *                                   ^    ^  ^  ^ ^ ^
 *                                   |    |  |  | | |
 *                                  11   16 19 22 24 26
 */
static time_t extract_create_time_from_log_fname(const char* fname)
{
    time_t tm = 0;
    char* filename = (char*)pstrdup(fname);
    char c = 0;
    struct tm tmp_time;
    int ret = memset_s(&tmp_time, sizeof(struct tm), 0, sizeof(struct tm));
    securec_check(ret, "\0", "\0");

    /* replace all chars of '-' or '_' in file name with '\0' */
    filename[15] = filename[18] = filename[21] = '\0';
    /* year data whose offset is 11 */
    tmp_time.tm_year = atoi(filename + 11);
    /* The number of years since 1900 */
    tmp_time.tm_year -= 1900;
    /* month data whose offset is 16 */
    tmp_time.tm_mon = atoi(filename + 16);
    /* The number of months since January, in the range 0 to 11. */
    tmp_time.tm_mon -= 1;
    /* day data whose offset is 19 */
    tmp_time.tm_mday = atoi(filename + 19);

    /* hour data whose offset is 22 and length is 2 */
    c = filename[24];
    filename[24] = '\0';
    tmp_time.tm_hour = atoi(filename + 22);
    filename[24] = c;
    /* minute data whose offset is 24 and length is 2 */
    c = filename[26];
    filename[26] = '\0';
    tmp_time.tm_min = atoi(filename + 24);
    filename[26] = c;
    /* second data whose offset is 26 and length is 2 */
    c = filename[28];
    filename[28] = '\0';
    tmp_time.tm_sec = atoi(filename + 26);
    filename[28] = c;
    pfree_ext(filename);

    /* convert struct tm to time_t */
    tm = mktime(&tmp_time);
    return tm;
}

static void fill_dirs_list(logdir_scanner scan)
{
    char fullpath[MAXPGPATH] = {0};
    struct stat st;

    DIR* pdir = opendir(scan->top_dir);
    if (NULL != pdir) {
        struct dirent* ent = NULL;
        while (NULL != (ent = readdir(pdir))) {
            if (0 == strcmp(ent->d_name, ".") || 0 == strcmp(ent->d_name, "..") ||
                (scan->dir_cb && !scan->dir_cb(ent->d_name))) {
                continue;
            }

            int ret = snprintf_s(fullpath, MAXPGPATH, MAXPGPATH - 1, "%s/%s", scan->top_dir, ent->d_name);
            securec_check_ss(ret, "", "");

            /*
             * don't use ent->d_type to judge whether it's a direcotry.
             * Only the fields d_name and d_ino are specified in POSIX.1-2001.
             * The remaining fields are available on many, but	not  all  systems.
             * the d_type field is available mainly only on BSD systems.
             */
            if (0 == lstat(fullpath, &st) && !S_ISLNK(st.st_mode) && S_ISDIR(st.st_mode)) {
                scan->dir_list = lappend(scan->dir_list, pstrdup(fullpath));
            }
        }
        (void)closedir(pdir);
        pdir = NULL;
    }
}

static void fill_files_list(logdir_scanner scan)
{
    char fullpath[MAXPGPATH] = {0};
    DIR* pdir = NULL;
    struct dirent* ent = NULL;
    struct stat st;
    int ret = 0;

    pdir = opendir(scan->cur_dir);
    if (NULL != pdir) {
        Assert(NIL == scan->log_file_list);
        while (NULL != (ent = readdir(pdir))) {
            /* skip "." and ".." subdir */
            if (0 == strcmp(ent->d_name, ".") || 0 == strcmp(ent->d_name, "..")) {
                continue;
            }

            ret = snprintf_s(fullpath, MAXPGPATH, MAXPGPATH - 1, "%s/%s", scan->cur_dir, ent->d_name);
            securec_check_ss(ret, "", "");

            /*
             * don't use ent->d_type to judge whether it's a direcotry.
             * Only the fields d_name and d_ino are specified in POSIX.1-2001.
             * The remaining fields are available on many, but	not  all  systems.
             * the d_type field is available mainly only on BSD systems.
             */
            if (0 == lstat(fullpath, &st) && !S_ISLNK(st.st_mode) && S_ISREG(st.st_mode)) {
                if (NULL == scan->fname_cb || scan->fname_cb(ent->d_name)) {
                    log_file_info* log_info = (log_file_info*)palloc0(sizeof(log_file_info));
                    log_info->name = pstrdup(fullpath);
                    log_info->tm_create = extract_create_time_from_log_fname(ent->d_name);
                    scan->log_file_list = lappend(scan->log_file_list, log_info);
                }
            }
            /* else: file doesn't exist, nothing to do */
        }
        (void)closedir(pdir);
        pdir = NULL;
    }
}

/*
 * get all IP address' strings for this node self.
 */
static List* localhost_to_ipinfo(void)
{
    /* first, get hostname */
    char hostname[LOGFDW_HOSTNAME_MAXLEN] = {0};
    (void)gethostname(hostname, LOGFDW_HOSTNAME_MAXLEN);

    struct addrinfo hint;
    int ret = memset_s(&hint, sizeof(hint), 0, sizeof(hint));
    securec_check(ret, "\0", "\0");
    hint.ai_socktype = AI_CANONNAME;
    hint.ai_family = AF_INET;

    /* get all IPs from hostname */
    struct addrinfo* gai_result = NULL;
    ret = getaddrinfo(hostname, 0, &hint, &gai_result);
    if (ret != 0) {
        ereport(ERROR,
            (errcode(ERRCODE_SYSTEM_ERROR),
                errmsg("could not translate hostname \"%s\" to address: %s", hostname, gai_strerror(ret))));
    }

    struct addrinfo* ressave = gai_result;
    List* thisnode_ips = NIL;
    do {
        char ip[LOGFDW_LOCAL_IP_LEN] = {0};
        struct sockaddr_in* h = (struct sockaddr_in*)gai_result->ai_addr;
        const char* ipstr = inet_ntop(AF_INET, &h->sin_addr.s_addr, ip, LOGFDW_LOCAL_IP_LEN);
        if (NULL != ipstr) {
            ListCell* ip_cell = NULL;
            bool found = false;

            /* search this ip list and make sure it doesn't appear */
            foreach (ip_cell, thisnode_ips) {
                if (0 == strcmp(ipstr, (const char*)lfirst(ip_cell))) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                /* this ip string only occurs once in this list, it's unique */
                thisnode_ips = lappend(thisnode_ips, pstrdup(ipstr));
            }

            /* advance to next ip addr */
            gai_result = gai_result->ai_next;
        } else {
            int tmperr = errno;
            ereport(ERROR,
                (errcode(ERRCODE_SYSTEM_ERROR), errmsg("inet_ntop() failed(errno %d): %s", tmperr, strerror(tmperr))));
        }
    } while (NULL != gai_result);

    freeaddrinfo(ressave);

    return thisnode_ips;
}

/*
 * get nodename from pgxc_node table.
 * for datanode there is only one tuple in pgxc_node system table.
 */
static inline char* get_nodename_same_to_cn_pgxcnode_table(void)
{
    Assert(IS_PGXC_DATANODE);

    Relation rel = heap_open(PgxcNodeRelationId, AccessShareLock);
    TableScanDesc scan;
    scan  = heap_beginscan(rel, SnapshotNow, 0, NULL);
    HeapTuple tuple = NULL;
    char* name = NULL;

    /*
     * get node name from pgxc_node table in datanode.
     * this name is the same to node_name in pgxc_node of CN.
     */
    while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL) {
        Form_pgxc_node nodeForm = (Form_pgxc_node)GETSTRUCT(tuple);
        name = pstrdup(NameStr(nodeForm->node_name));
        break;
    }

    heap_endscan(scan);
    heap_close(rel, AccessShareLock);

    if (NULL == name || '\0' == name[0]) {
        ereport(ERROR,
            (errcode(ERRCODE_SYSTEM_ERROR), errmsg("Failed to get nodename from pgxc_node which is an empty table")));
    }
    return name;
}

static void build_hashtbl_of_localhost(HTAB* tmp_htbl, List* localhost_dnnames)
{
    List* local_ips = localhost_to_ipinfo();
    ListCell* ip_cell = NULL;
    char* ip = NULL;
    hvalue_nodes* entry = NULL;
    bool need_insert = true;
    bool found = false;

    foreach (ip_cell, local_ips) {
        ip = (char*)lfirst(ip_cell);
        entry = (hvalue_nodes*)hash_search(tmp_htbl, ip, HASH_FIND, &found);
        if (found) {
            /* append node name directly to the same ip list */
            Assert(entry && entry->nodenames);
            entry->nodenames = list_concat(entry->nodenames, localhost_dnnames);
            need_insert = false;
            break;
        }
    }

    if (need_insert) {
        /* insert a new entry into hash table, and min-ip will be selected */
        make_string_list_sorted(local_ips, strcmp_wrapper);
        ip = (char*)lfirst(list_head(local_ips));
        entry = (hvalue_nodes*)hash_search(tmp_htbl, ip, HASH_ENTER, &found);
        if (NULL != entry) {
            Assert(!found);
            entry->nodenames = localhost_dnnames;
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_SYSTEM_ERROR),
                    errmsg("failed to insert entry into ip-masters hash table for localhost datanode")));
        }
    }

    list_free_deep(local_ips);
}

static void build_hashtbl_of_master_dnname_and_ip(HTAB* tmp_htbl)
{
    HeapTuple tuple = NULL;
    hvalue_nodes* entry = NULL;
    char* master_ip = NULL;
    List* localhost_dnnames = NIL;
    bool found = false;

    Relation rel = heap_open(PgxcNodeRelationId, AccessShareLock);
    TableScanDesc scan;
    scan = heap_beginscan(rel, SnapshotNow, 0, NULL);

    /* scan pgxc_node table and get all info */
    while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL) {
        Form_pgxc_node nodeForm = (Form_pgxc_node)GETSTRUCT(tuple);
        /* master node name and ip will be passed down. so only care dn type */
        if (PGXC_NODE_DATANODE != nodeForm->node_type) {
            continue;
        }

        master_ip = (nodeForm->hostis_primary) ? NameStr(nodeForm->node_host) : NameStr(nodeForm->node_host1);
        if (0 == pg_strcasecmp(master_ip, "localhost") || 0 == pg_strcasecmp(master_ip, "127.0.0.1")) {
            /*
             * convert localhost to ip address.
             * notice: this should not happen in normal cluster configure.
             *         and it makes when this cluster is build by developers without slaves.
             */
            ereport(LOG, (errmsg("\"%s\" should not be configured in pgxc_node for datanode type", master_ip)));

            char* master_name = pstrdup(NameStr(nodeForm->node_name));
            localhost_dnnames = lappend(localhost_dnnames, makeString(master_name));
        } else {
            /* put this ip address into hash table */
            entry = (hvalue_nodes*)hash_search(tmp_htbl, master_ip, HASH_ENTER, &found);
            if (NULL != entry) {
                if (!found) {
                    /* make sure it's NIL at the first time */
                    entry->nodenames = NIL;
                }
                char* master_name = pstrdup(NameStr(nodeForm->node_name));
                /* use T_String node so it can be push down to datanode */
                entry->nodenames = lappend(entry->nodenames, makeString(master_name));
            } else {
                ereport(ERROR,
                    (errcode(ERRCODE_SYSTEM_ERROR),
                        errmsg(
                            "failed to put ip \"%s\" into hash \"get_ip_nodename_pairs_from_pgxc_node\" ", master_ip)));
            }
        }
    }

    heap_endscan(scan);
    heap_close(rel, AccessShareLock);

    if (NULL != localhost_dnnames) {
        /* at last handle all the datanodes in 'localhost' */
        build_hashtbl_of_localhost(tmp_htbl, localhost_dnnames);
    }
}

static inline void htbl_insert_cn_entry(HTAB* tmp_htbl, const char* host, const char* node)
{
    bool found = false;
    hentry_reachable_host* entry = NULL;
    entry = (hentry_reachable_host*)hash_search(tmp_htbl, host, HASH_ENTER, &found);
    if (!found) {
        entry->reachable = UHT_UNKNOWN;
        entry->has_datanode = false;
        entry->nodenames = NIL;
    }
    entry->nodenames = lappend(entry->nodenames, pstrdup(node));
}

static inline char* standby_node_tag(const char* node)
{
    char host_tag[NAMEDATALEN] = {0};

    errno_t ret = strcat_s(host_tag, NAMEDATALEN, node);
    securec_check(ret, "\0", "\0");
    ret = strcat_s(host_tag, NAMEDATALEN, "(standby)");
    securec_check(ret, "\0", "\0");
    return pstrdup(host_tag);
}

static inline void htbl_insert_dn_entry(HTAB* tmp_htbl, const char* host, const char* node, bool hostis_primary)
{
    bool found = false;
    hentry_reachable_host* entry = NULL;

    entry = (hentry_reachable_host*)hash_search(tmp_htbl, host, HASH_ENTER, &found);

    /* init entry */
    if (!found) {
        entry->reachable = UHT_UNKNOWN;
        entry->has_datanode = true;
        entry->nodenames = NIL;
    }

    if (hostis_primary) {
        entry->reachable = UHT_OK;
        entry->nodenames = lappend(entry->nodenames, pstrdup(node));
    } else {
        entry->nodenames = lappend(entry->nodenames, standby_node_tag(node));
    }
}

static void htbl_insert_localhost_nodes(
    HTAB* unreach_htbl, List* localhost_cn, List* localhost_dn, unr_host_type local_reach)
{
    List* localhost_ips = localhost_to_ipinfo();
    ListCell* ip_cell = NULL;
    char* ip = NULL;
    hentry_reachable_host* entry = NULL;
    bool need_insert = true;
    bool found = false;

    foreach (ip_cell, localhost_ips) {
        ip = (char*)lfirst(ip_cell);
        entry = (hentry_reachable_host*)hash_search(unreach_htbl, ip, HASH_FIND, &found);
        if (found) {
            /* append node name directly to the same ip list */
            Assert(entry && entry->nodenames);
            entry->nodenames = list_concat(entry->nodenames, localhost_cn);
            entry->nodenames = list_concat(entry->nodenames, localhost_dn);
            if (UHT_OK != entry->reachable) {
                /* if this host not reachable, then it depends on the input local_reach */
                entry->reachable = local_reach;
            }
            entry->has_datanode = entry->has_datanode || (list_length(localhost_dn) > 0);
            need_insert = false;
            break;
        }
    }

    if (need_insert) {
        /* insert a new entry into hash table, and min-ip will be selected */
        make_string_list_sorted(localhost_ips, strcmp_wrapper);
        ip = (char*)lfirst(list_head(localhost_ips));
        entry = (hentry_reachable_host*)hash_search(unreach_htbl, ip, HASH_ENTER, &found);
        if (NULL != entry) {
            Assert(!found);
            entry->nodenames = NIL;
            entry->nodenames = list_concat(entry->nodenames, localhost_cn);
            entry->nodenames = list_concat(entry->nodenames, localhost_dn);
            entry->reachable = local_reach;
            entry->has_datanode = (list_length(localhost_dn) > 0);
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_SYSTEM_ERROR),
                    errmsg("failed to insert entry into ip-masters hash table for localhost datanode")));
        }
    }

    list_free_deep(localhost_ips);
}

static void build_hashtbl_of_unreachable(HTAB* unreach_htbl)
{
    Relation rel = heap_open(PgxcNodeRelationId, AccessShareLock);
    TableScanDesc scan;
    scan = heap_beginscan(rel, SnapshotNow, 0, NULL);
    HeapTuple tuple = NULL;
    List* localhost_cn = NIL;
    List* localhost_dn = NIL;
    unr_host_type localhost_reachable = UHT_UNKNOWN;

    while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL) {
        Form_pgxc_node nodeForm = (Form_pgxc_node)GETSTRUCT(tuple);
        if (PGXC_NODE_DATANODE == nodeForm->node_type) {
            if (0 != pg_strcasecmp(NameStr(nodeForm->node_host), "localhost")) {
                /* node_host is master only if hostis_primary is true */
                htbl_insert_dn_entry(
                    unreach_htbl, NameStr(nodeForm->node_host), NameStr(nodeForm->node_name), nodeForm->hostis_primary);
            } else {
                if (nodeForm->hostis_primary) {
                    /* localhost is reachable by this master */
                    localhost_dn = lappend(localhost_dn, pstrdup(NameStr(nodeForm->node_name)));
                    localhost_reachable = UHT_OK;
                } else {
                    /* localhost reachable is unknown */
                    localhost_dn = lappend(localhost_dn, standby_node_tag(NameStr(nodeForm->node_name)));
                }
            }

            if (0 != pg_strcasecmp(NameStr(nodeForm->node_host1), "localhost")) {
                /* node_host1 is master only if hostis_primary is false */
                htbl_insert_dn_entry(unreach_htbl,
                    NameStr(nodeForm->node_host1),
                    NameStr(nodeForm->node_name),
                    !nodeForm->hostis_primary);
            } else {
                if (!nodeForm->hostis_primary) {
                    /* localhost is reachable by this master */
                    localhost_dn = lappend(localhost_dn, pstrdup(NameStr(nodeForm->node_name)));
                    localhost_reachable = UHT_OK;
                } else {
                    /* localhost reachable is unknown */
                    localhost_dn = lappend(localhost_dn, standby_node_tag(NameStr(nodeForm->node_name)));
                }
            }
        } else {
            if (0 != pg_strcasecmp(NameStr(nodeForm->node_host), "localhost")) {
                /* localhost reachable depends primary dn but not cn */
                htbl_insert_cn_entry(unreach_htbl, NameStr(nodeForm->node_host), NameStr(nodeForm->node_name));
            } else {
                /* localhost reachable is unknown */
                localhost_cn = lappend(localhost_cn, pstrdup(NameStr(nodeForm->node_name)));
            }
        }
    }
    heap_endscan(scan);
    heap_close(rel, AccessShareLock);

    if (NULL != localhost_cn || NULL != localhost_dn) {
        htbl_insert_localhost_nodes(unreach_htbl, localhost_cn, localhost_dn, localhost_reachable);
    }
}

static List* get_unreachable_hosts(void)
{
    HASHCTL hctl;
    int ret = memset_s(&hctl, sizeof(HASHCTL), 0, sizeof(HASHCTL));
    securec_check(ret, "\0", "\0");
    hctl.keysize = sizeof(hkey_ip);
    hctl.entrysize = sizeof(hentry_reachable_host);
    hctl.hash = string_hash;

    HTAB* unreach_htbl = hash_create("get_unreachable_hosts",
        1024, /* default node number */
        &hctl,
        HASH_ELEM | HASH_FUNCTION);
    build_hashtbl_of_unreachable(unreach_htbl);

    List* unreach_hosts_list = NIL;
    hentry_reachable_host* entry = NULL;
    HASH_SEQ_STATUS hseq_stat;
    hash_seq_init(&hseq_stat, unreach_htbl);
    while ((entry = (hentry_reachable_host*)hash_seq_search(&hseq_stat)) != NULL) {
        if (UHT_OK != entry->reachable) {
            /* update unreachable reason */
            entry->reachable = entry->has_datanode ? UHT_NO_MASTER : UHT_DEPLOY_LIMIT;

            /* append unreachable list */
            hentry_reachable_host* unreach_host = (hentry_reachable_host*)palloc(sizeof(hentry_reachable_host));
            *unreach_host = *entry;
            unreach_hosts_list = lappend(unreach_hosts_list, unreach_host);
        }
    }
    hash_destroy(unreach_htbl);
    return unreach_hosts_list;
}

static void free_unreachable_hosts(List* unreach)
{
    ListCell* host_cell = NULL;
    ListCell* next_cell = NULL;
    for (host_cell = list_head(unreach); host_cell; host_cell = next_cell) {
        /* remember the next cell */
        next_cell = lnext(host_cell);
        /* get the current cell and free its list */
        hentry_reachable_host* host = (hentry_reachable_host*)lfirst(host_cell);
        list_free_deep(host->nodenames);
        /* detach from list and free this cell */
        unreach = list_delete_cell(unreach, host_cell, NULL);
    }
    Assert(NIL == unreach);
}

List* get_ip_nodename_pairs_from_pgxc_node(void)
{
    HASHCTL hctl;
    int ret = memset_s(&hctl, sizeof(HASHCTL), 0, sizeof(HASHCTL));
    securec_check(ret, "\0", "\0");
    hctl.keysize = sizeof(hkey_ip);
    hctl.entrysize = sizeof(hvalue_nodes);
    hctl.hash = string_hash;

    HTAB* tmp_htbl = hash_create("get_ip_nodename_pairs_from_pgxc_node",
        1024, /* default node number */
        &hctl,
        HASH_ELEM | HASH_FUNCTION);

    build_hashtbl_of_master_dnname_and_ip(tmp_htbl);

    /* construct pair list between ip and nodes */
    List* masters = NIL;
    hvalue_nodes* entry = NULL;
    HASH_SEQ_STATUS hseq_stat;
    hash_seq_init(&hseq_stat, tmp_htbl);
    while ((entry = (hvalue_nodes*)hash_seq_search(&hseq_stat)) != NULL) {
        DistFdwDataNodeTask* pair = makeNode(DistFdwDataNodeTask);
        /* IP address */
        pair->dnName = pstrdup(NameStr(entry->host_ip.ip));
        /* node name list */
        pair->task = entry->nodenames;
        entry->nodenames = NULL;
        masters = lappend(masters, pair);
    }
    hash_destroy(tmp_htbl);

    return masters;
}

static List* core_assign_dir_task(List* dir_list, const int nodes_num, const int this_node_pos)
{
    Assert(nodes_num > 0);
    Assert(this_node_pos >= 0 && this_node_pos < nodes_num);

    List* task = NIL;
    ListCell* cell = list_head(dir_list);
    int pos = 0;

    /* the first dir to scan is in the position of this_node_pos */
    for (; (cell) != NULL; (cell) = lnext(cell)) {
        if (this_node_pos == pos) {
            task = lappend(task, lfirst(cell));
            lfirst(cell) = NULL;
            break;
        } else {
            pfree_ext(lfirst(cell));
            ++pos;
        }
    }

    /* the other dirs to scan is in the position of (nodes_num-1) in each period */
    if (NULL != cell) {
        /* skip the first dir found */
        cell = lnext(cell);
        pos = 0;

        for (; cell != NULL; cell = lnext(cell)) {
            if (nodes_num - 1 == pos) {
                task = lappend(task, lfirst(cell));
                lfirst(cell) = NULL;
                pos = 0;
            } else {
                pfree_ext(lfirst(cell));
                ++pos;
            }
        }
    }

    list_free(dir_list);
    dir_list = NIL;
    return task;
}

static void get_and_sort_all_dirs(logdir_scanner scan)
{
    /* fill with dir list */
    fill_dirs_list(scan);
    /* sort all the dirs */
    make_string_list_sorted(scan->dir_list, strcmp_wrapper);
}

static void get_dirs_from_ip_nodename_pairs(List* ip_nodename_pairs, logdir_scanner scan)
{
    ListCell* pair_cell = NULL;
    ListCell* ip_cell = NULL;
    List* pairs_found = NIL;
    List* local_ips = localhost_to_ipinfo();

    /* check all IP addresses */
    foreach (ip_cell, local_ips) {
        char* ip = (char*)lfirst(ip_cell);

        /* to search the ip-node list about my host */
        foreach (pair_cell, ip_nodename_pairs) {
            DistFdwDataNodeTask* pair = (DistFdwDataNodeTask*)lfirst(pair_cell);
            /* dnName stores ip info */
            if (0 == pg_strcasecmp(ip, pair->dnName)) {
                pairs_found = lappend(pairs_found, pair);
                break;
            }
        }
    }
    list_free_deep(local_ips);
    local_ips = NIL;

    char* nodename = get_nodename_same_to_cn_pgxcnode_table();
    DistFdwDataNodeTask* pair_found = NULL;
    int pos = 0;
    bool found = false;

    foreach (pair_cell, pairs_found) {
        DistFdwDataNodeTask* pair = (DistFdwDataNodeTask*)lfirst(pair_cell);

        /* make the nodes list sorted */
        make_string_list_sorted(pair->task, str_node_cmp);

        ListCell* dn_cell = NULL;
        int tmp_pos = 0; /* reset position */

        /* task stores all the datanode name */
        foreach (dn_cell, pair->task) {
            Value* strnode = (Value*)lfirst(dn_cell);
            if (0 == pg_strcasecmp(nodename, strnode->val.str)) {
                if (found) {
                    ereport(ERROR,
                        (errcode(ERRCODE_SYSTEM_ERROR),
                            errmsg("nodename \"%s\" found twice in ip-masters info list", nodename)));
                }
                found = true;
                /* remember matched list and its position info */
                pair_found = pair;
                pos = tmp_pos;
                break;
            }
            ++tmp_pos;
        }
    }

    if (!found) {
        /* this datanode is not found in ip-nodes-pair list */
        ereport(ERROR,
            (errcode(ERRCODE_SYSTEM_ERROR), errmsg("nodename \"%s\" not found in ip-masters info list", nodename)));
    }
    pfree_ext(nodename);

    /* get the sorted directory list */
    get_and_sort_all_dirs(scan);

    /* create the task to scan what directories */
    scan->dir_list = core_assign_dir_task(scan->dir_list, list_length(pair_found->task), pos);
}

static void fetch_one_dir_to_fill_filelist(logdir_scanner scan, ForeignScanState* node)
{
    /* just set cur_dir be null, and don't free it */
    scan->cur_dir = NULL;
    if (scan->dir_list && list_length(scan->dir_list) > 0) {
        scan->cur_dir = (char*)list_nth(scan->dir_list, 0);
        scan->dir_list = list_delete_first(scan->dir_list);
        fill_files_list(scan);

        if (scan->m_latest_files_num > 0) {
            make_string_list_sorted(scan->log_file_list, cmp_latest_with_create_time);
            scan->log_file_list = list_truncate(scan->log_file_list, scan->m_latest_files_num);
        }
        /* make this list sorted by creating time */
        make_string_list_sorted(scan->log_file_list, rcmp_latest_with_create_time);

        logFdwPlanState* festate = (logFdwPlanState*)node->fdw_state;
        /* remember the total number of log files */
        festate->m_total_logfile_num += list_length(scan->log_file_list);
        /* filter and skip some log files by name */
        if (festate->m_need_check_logname) {
            scan->log_file_list = filter_logname_list_by_name(node, scan->log_file_list);
        }
        /* filter and skip some log files by time */
        if (festate->m_need_check_logtime) {
            scan->log_file_list = filter_logname_list_by_time(node, scan->log_file_list);
        }
    }
}

static inline void free_loginfo(log_file_info*& info)
{
    if (NULL != info) {
        pfree_ext(info->name);
        pfree_ext(info);
    }
}

/* topdir should be "$GAUSSLOG/pg_log" or "$GAUSSLOG/gs_profile" */
logdir_scanner new_logdir_scanner(const char* topdir, dirname_match dircb, fname_match fcb)
{
    logdir_scanner scan = NULL;
    size_t topdir_len = strlen(topdir) + 1;
    size_t len = sizeof(ld_scanner) + topdir_len;
    int ret = 0;

    MemoryContext memcnxt = AllocSetContextCreate(CurrentMemoryContext,
        "Log Dir Scanner",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    MemoryContext old_memcnxt = MemoryContextSwitchTo(memcnxt);

    scan = (logdir_scanner)palloc0(len);
    scan->m_memcnxt = memcnxt;
    scan->top_dir = (char*)scan + sizeof(ld_scanner);
    ret = memcpy_s(scan->top_dir, topdir_len, topdir, topdir_len);
    securec_check(ret, "\0", "\0");
    scan->dir_cb = dircb;
    scan->fname_cb = fcb;

    (void)MemoryContextSwitchTo(old_memcnxt);
    return scan;
}

void free_logdir_scanner(logdir_scanner scan)
{
    if (NULL != scan) {
        MemoryContextDelete(scan->m_memcnxt);
    }
}

static inline void set_latest_files_num(logdir_scanner scan, int n)
{
    scan->m_latest_files_num = n;
}

/*
 * begin to scan a log directory.
 */
static void begin_scan_dirs(logdir_scanner scan, ForeignScanState* node)
{
    MemoryContext old_memcnxt = MemoryContextSwitchTo(scan->m_memcnxt);
    fetch_one_dir_to_fill_filelist(scan, node);
    (void)MemoryContextSwitchTo(old_memcnxt);
}

static inline void copy_dirlist_for_rescan(logdir_scanner scan)
{
    MemoryContext old_memcnxt = MemoryContextSwitchTo(scan->m_memcnxt);
    /* shallow copy, so dir string cannot be freed */
    scan->dir_list_copy = list_copy(scan->dir_list);
    (void)MemoryContextSwitchTo(old_memcnxt);
}

static void begin_rescan_dirs(logdir_scanner scan)
{
    if (NULL != scan->dir_list) {
        list_free(scan->dir_list);
        scan->dir_list = NIL;
    }
    /* reset dir list */
    scan->dir_list = list_copy(scan->dir_list_copy);
    scan->cur_dir = NULL;

    free_loginfo(scan->log_file);
    scan->log_file = NULL;
    /* free current log files list */
    if (scan->log_file_list != NULL) {
        ListCell* cell = NULL;
        ListCell* next = NULL;
        for (cell = list_head(scan->log_file_list); cell; cell = next) {
            next = lnext(cell);
            log_file_info* file_info = (log_file_info*)lfirst(cell);
            free_loginfo(file_info);
            scan->log_file_list = list_delete_cell(scan->log_file_list, cell, NULL);
        }
    }
    scan->log_file_list = NULL;
}

/*
 * iterate to find a log file and return its name.
 */
log_file_info* iterate_scan_dirs(logdir_scanner scan, ForeignScanState* node)
{
    free_loginfo(scan->log_file);

    /* fast path: fetch log file directly from files list */
    if (scan->log_file_list && list_length(scan->log_file_list) > 0) {
        scan->log_file = (log_file_info*)list_nth(scan->log_file_list, 0);
        scan->log_file_list = list_delete_first(scan->log_file_list);
        return scan->log_file;
    }

    MemoryContext old_memcnxt = MemoryContextSwitchTo(scan->m_memcnxt);
    fetch_one_dir_to_fill_filelist(scan, node);
    (void)MemoryContextSwitchTo(old_memcnxt);

    if (scan->log_file_list && list_length(scan->log_file_list) > 0) {
        scan->log_file = (log_file_info*)list_nth(scan->log_file_list, 0);
        scan->log_file_list = list_delete_first(scan->log_file_list);
        return scan->log_file;
    }

    /* no log file */
    return NULL;
}

/*
 * finish this scan of log directory
 */
void end_scan_dirs(logdir_scanner scan)
{
    free_loginfo(scan->log_file);
    scan->cur_dir = NULL;
    /*
     * for LIMIT statement, these don't hold, so ignore them.
     * 1) scan->dir_list is always NIL;
     * 2) scan->log_file_list is always NIL;
     */
}

void master_only_set_dirlist(logdir_scanner scan)
{
    MemoryContext old_memcnxt = MemoryContextSwitchTo(scan->m_memcnxt);
    /* top_dir is the only one dir to scan */
    scan->dir_list = lappend(scan->dir_list, pstrdup(scan->top_dir));
    (void)MemoryContextSwitchTo(old_memcnxt);
}

/* get its regex expression for each option */
static char* regex_of_line_prefix_option(const char opt)
{
    switch (opt) {
        case 'a': /* application name */
        case 'c': /* session id */
        case 'd': /* database name */
        case 'e': /* error code, both char and digits may occur */
        case 'h': /* remote host */
        case 'n': /* node name */
        case 'r': /* remote host(remote port) */
        case 'i': /* command tag */
        case 'u': /* user name */
        case 'v': /* virtual transaction id */
            return "([^ ]+)";
        case 'p': /* thread id */
        case 'l': /* line number */
        case 'x': /* transaction id */
            return "(\\d+)";
        case 'm': /* timestamp with time zone */
            return "(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{3} \\S{2,5})";
        case 't': /* timestamp without milliseconds */
        case 's': /* session start timestamp */
            return "(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2} \\S{2,5})";
        case '%':
            return "%";
        case ' ':
        case 'q':
        default:
            return "\\s+";
    }
}

static void fill_attrmap_of_line_prefix(const char opt, int attpos, int* attmap)
{
    switch (opt) {
        case 'a': /* application name */
            attmap[PGLOG_ATTR_APPNAME] = attpos;
            break;
        case 'c': /* session id */
            attmap[PGLOG_ATTR_SESSION_ID] = attpos;
            break;
        case 'd': /* database name */
            attmap[PGLOG_ATTR_DBNAME] = attpos;
            break;
        case 'h': /* remote host */
        case 'r': /* remote host(remote port) */
            attmap[PGLOG_ATTR_REMOTE] = attpos;
            break;
        case 'n': /* node name */
            attmap[PGLOG_ATTR_NODENAME] = attpos;
            break;
        case 'i': /* command tag */
            attmap[PGLOG_ATTR_CMDTAG] = attpos;
            break;
        case 'u': /* user name */
            attmap[PGLOG_ATTR_USERNAME] = attpos;
            break;
        case 'v': /* virtual transaction id */
            attmap[PGLOG_ATTR_VIRTUAL_XID] = attpos;
            break;
        case 'p': /* thread id */
            attmap[PGLOG_ATTR_PID] = attpos;
            break;
        case 'l': /* line number */
            attmap[PGLOG_ATTR_LINENO] = attpos;
            break;
        case 'x': /* transaction id */
            attmap[PGLOG_ATTR_XID] = attpos;
            break;
        case 'e': /* error code */
            attmap[PGLOG_ATTR_ECODE] = attpos;
            break;
        case 'm': /* timestamp with time zone */
        case 't': /* timestamp without milliseconds */
            attmap[PGLOG_ATTR_LOGTIME] = attpos;
            break;
        case 's': /* session start timestamp */
            attmap[PGLOG_ATTR_SESSION_START] = attpos;
            break;
        case '%':
        case ' ':
        case 'q':
        default:
            break;
    }
}

static void fill_pglog_planstate_from_logft_rel(pglogPlanState* pg_log, Relation ftrel)
{
    TupleDesc tupdesc = ftrel->rd_att;
    Oid in_func_oid = (Datum)0;

    Assert(PGLOG_ATTR_MAX == tupdesc->natts);
    for (int i = 0; i < PGLOG_ATTR_MAX; ++i) {
        Form_pg_attribute att = tupdesc->attrs[i];
        pg_log->allattr_typmod[i] = att->atttypmod;
        getTypeInputInfo(att->atttypid, &in_func_oid, &pg_log->allattr_typioparam[i]);
        fmgr_info(in_func_oid, &pg_log->allattr_fmgrinfo[i]);
    }
}

static void fill_pglog_planstate_from_log_line_prefix(pglogPlanState* pg_log, StringInfo reg)
{
    const int format_len = strlen(u_sess->attr.attr_common.Log_line_prefix);
    int nattrs = 0;

    /* -1 means that this attribute is not set */
    for (int j = 0; j < PGLOG_ATTR_MAX; ++j) {
        pg_log->attmap[j] = -1;
    }

    for (int i = 0; i < format_len; i++) {
        if (u_sess->attr.attr_common.Log_line_prefix[i] != '%') {
            /* it should be a space, so ignore it */
            appendStringInfoString(reg, regex_of_line_prefix_option(' '));
            continue;
        }
        /* go to char after '%' */
        i++;
        if (i < format_len) {
            const char opt = u_sess->attr.attr_common.Log_line_prefix[i];
            appendStringInfoString(reg, regex_of_line_prefix_option(opt));
            fill_attrmap_of_line_prefix(opt, nattrs, pg_log->attmap);
            ++nattrs;
            continue;
        }
        break; /* format error - ignore it */
    }

    /* debug query id */
    appendStringInfoString(reg, regex_of_line_prefix_option('l'));
    appendStringInfoString(reg, regex_of_line_prefix_option(' '));
    pg_log->attmap[PGLOG_ATTR_QID] = nattrs;
    ++nattrs;

    /* module name */
    appendStringInfoString(reg, regex_of_line_prefix_option('a'));
    appendStringInfoString(reg, regex_of_line_prefix_option(' '));
    pg_log->attmap[PGLOG_ATTR_MODNAME] = nattrs;
    ++nattrs;

    /* log level */
    appendStringInfoString(reg, regex_of_line_prefix_option('a'));
    appendStringInfoString(reg, regex_of_line_prefix_option(' '));
    pg_log->attmap[PGLOG_ATTR_LEVEL] = nattrs;
    ++nattrs;

    /* log message */
    pg_log->attmap[PGLOG_ATTR_MESSAGE] = nattrs;
    pg_log->nattrs = nattrs + 1;
}

static void format_regrex_text_datum(pglogPlanState* pg_log)
{
    /* set regrex for pg_log line */
    StringInfo regrex = makeStringInfo();
    appendStringInfo(regrex, "%s", "^");
    fill_pglog_planstate_from_log_line_prefix(pg_log, regrex);
    appendStringInfo(regrex, "%s", "(.*)$");
    pg_log->regrex = CStringGetTextDatum((const char*)regrex->data);
    pfree_ext(regrex->data);
    pfree_ext(regrex);
}

static void descript_unreachable_nodes(StringInfo str, List* unreach_host)
{
    ListCell* host_cell = NULL;
    hentry_reachable_host* host = NULL;
    ListCell* node_cell = NULL;
    char* node = NULL;

    foreach (host_cell, unreach_host) {
        host = (hentry_reachable_host*)lfirst(host_cell);
        appendStringInfoChar(str, '\n');
        appendStringInfoString(str, NameStr(host->host_ip.ip));
        if (UHT_NO_MASTER == host->reachable) {
            appendStringInfoString(str, "(hint: no primary datanode, switchover first)");
        } else if (UHT_DEPLOY_LIMIT == host->reachable) {
            appendStringInfoString(str, "(hint: deploy limitation)");
        } else {
            /* nothing to hint */
        }

        int delim = 0;
        foreach (node_cell, host->nodenames) {
            appendStringInfoString(str, ((delim > 0) ? ", " : ": "));
            ++delim;
            node = (char*)lfirst(node_cell);
            appendStringInfoString(str, node);
        }
    }
}

/*
 * EXPLAIN function: unreachable host and nodes
 */
static void explain_unreachable_nodes(StringInfo explain_str)
{
    /* explain which nodes cannot be reachable */
    List* unreach_host = get_unreachable_hosts();
    if (NULL != unreach_host) {
        descript_unreachable_nodes(explain_str, unreach_host);
        free_unreachable_hosts(unreach_host);
        unreach_host = NULL;
    }
}

/*
 * check whether val is found in valid_values[] whose size is nvalues.
 */
static bool check_option_value(const char* val, const char** valid_values, size_t nvalues)
{
    bool found = false;
    for (size_t i = 0; i < nvalues; ++i) {
        if (0 == pg_strcasecmp(val, valid_values[i])) {
            found = true;
            break;
        }
    }
    return found;
}

static void valid_ft_options(List* option_list)
{
    ListCell* cell = NULL;
    foreach (cell, option_list) {
        DefElem* optionDef = (DefElem*)lfirst(cell);
        if (0 == pg_strcasecmp(optionDef->defname, OPTION_LOGTYPE)) {
            char* typetxt = defGetString(optionDef);
            if (!check_option_value(typetxt, valid_log_type, sizeof(valid_log_type) / sizeof(valid_log_type[0]))) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("Invalid value \"%s\" of option \"%s\"", typetxt, OPTION_LOGTYPE)));
            }
        } else if (0 == pg_strcasecmp(optionDef->defname, OPTION_MASTER_ONLY)) {
            /*
             * just check the validation of 'master_only' option's
             * value, so ignore returned value
             */
            (void)defGetBoolean(optionDef);
        } else if (0 == pg_strcasecmp(optionDef->defname, OPTION_LATEST_FILES)) {
            /* not use defGetInt64() becuase this is not a T_Int type node */
            int32 latest_files = pg_strtoint32(defGetString(optionDef));
            if (latest_files <= 0) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("Invalid value \"%d\" of option \"%s\"", latest_files, OPTION_LATEST_FILES),
                        errdetail("Valid values: 1 ~ 2147483647")));
            }
        } else /* ALTER FOREIGN TABLE */
        {
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("Invalid option \"%s\"", optionDef->defname)));
        }
    }
}

static List* relation_get_options(Relation ft_rel)
{
    List* options = NIL;
    Oid ft_relid = RelationGetRelid(ft_rel);

    /* Get server OID for the foreign table. */
    HeapTuple tp = SearchSysCache1(FOREIGNTABLEREL, ObjectIdGetDatum(ft_relid));
    if (!HeapTupleIsValid(tp)) {
        ereport(
            ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("cache lookup failed for foreign table %u", ft_relid)));
    }

    bool isnull = false;
    Datum values = SysCacheGetAttr(FOREIGNTABLEREL, tp, Anum_pg_foreign_table_ftoptions, &isnull);
    if (!isnull) {
        options = untransformRelOptions(values);
    }
    ReleaseSysCache(tp);

    return options;
}

static FdwLogType get_logtype_from_options(List* options)
{
    ListCell* cell = NULL;
    foreach (cell, options) {
        DefElem* def = (DefElem*)lfirst(cell);
        if (pg_strcasecmp(def->defname, OPTION_LOGTYPE) == 0) {
            char* logtype = defGetString(def);
            if (pg_strcasecmp(logtype, valid_log_type[FLT_PG_LOG]) == 0) {
                return FLT_PG_LOG;
            } else if (pg_strcasecmp(logtype, valid_log_type[FLT_GS_PROFILE]) == 0) {
                return FLT_GS_PROFILE;
            }
        }
    }
    return FLT_UNKNOWN;
}

/* get option "logtype" of this foreign table relation */
static FdwLogType relation_getopt_logtype(Relation ft_rel)
{
    List* options = relation_get_options(ft_rel);
    FdwLogType t = get_logtype_from_options(options);
    list_free_deep(options);
    return t;
}

/* get option "master_only" of this foreign table relation */
static bool relation_getopt_masteronly(Relation ft_rel)
{
    List* options = relation_get_options(ft_rel);
    ListCell* cell = NULL;
    bool master_only = false;

    foreach (cell, options) {
        DefElem* def = (DefElem*)lfirst(cell);
        if (pg_strcasecmp(def->defname, OPTION_MASTER_ONLY) == 0) {
            master_only = defGetBoolean(def);
            break;
        }
    }
    list_free_deep(options);
    return master_only;
}

/* get option "latest_files" of this foreign table relation */
static int relation_getopt_latest_files_num(Relation ft_rel)
{
    List* options = relation_get_options(ft_rel);
    ListCell* cell = NULL;
    int latest_files = 0;

    foreach (cell, options) {
        DefElem* def = (DefElem*)lfirst(cell);
        if (pg_strcasecmp(def->defname, OPTION_LATEST_FILES) == 0) {
            /* not use defGetInt64() becuase this is not a T_Int type node */
            latest_files = pg_strtoint32(defGetString(def));
            break;
        }
    }
    list_free_deep(options);
    return latest_files;
}

/* "masters_info" list is generted in CN, and passed down to all DNs */
static List* fdw_get_option_masters_info(List* fdw_private)
{
    ListCell* cell = NULL;
    foreach (cell, fdw_private) {
        DefElem* def = (DefElem*)lfirst(cell);
        if (0 == strcasecmp(def->defname, logft_opt_masters_info)) {
            return (List*)def->arg;
        }
    }

    /*
     * there is no masters_info if
     * 1) connect to datanode and execute query statement;
     * 2) connect to coordinator and run 'EXECUTE DIRECT ON node <query statement>'
     * so degrade the log level to DEBUG2.
     */
    ereport(DEBUG2,
        (errmsg("Start query directly from DN instead of CN"),
            errdetail_internal("%s not found in FDW private data", logft_opt_masters_info)));
    return NULL;
}

static inline void assign_scanned_directorys(ForeignScanState* node, logdir_scanner dir_scan)
{
    List* masters_info = fdw_get_option_masters_info((List*)((ForeignScan*)node->ss.ps.plan)->fdw_private);
    if (NULL != masters_info) {
        get_dirs_from_ip_nodename_pairs(masters_info, dir_scan);
    } else {
        /*
         * even though this query is sent directly from DN,
         * or executed from CN using 'EXECUTE DIRECT ON(datanode) QUERY',
         * we still support to scan all these directories on this host.
         */
        get_and_sort_all_dirs(dir_scan);
    }
}

static void pglog_begin_fs(ForeignScanState* node, int eflags)
{
    logFdwPlanState* festate = (logFdwPlanState*)node->fdw_state;
    pglogPlanState* pg_log = &festate->log_state.pg_log;
    logdir_scanner dir_scan = NULL;

    format_regrex_text_datum(pg_log);
    fill_pglog_planstate_from_logft_rel(pg_log, node->ss.ss_currentRelation);

    if (IS_PGXC_COORDINATOR) {
        /* coordinator node doesn't scan data */
        return;
    }

    /* only query log records of this node myself */
    if (festate->m_master_only) {
        char dn_dir[MAXPGPATH] = {0};
        int ret = strncpy_s(dn_dir, MAXPGPATH, u_sess->attr.attr_common.Log_directory, (MAXPGPATH - 1));
        securec_check(ret, "\0", "\0");

        dir_scan = festate->dir_scan = new_logdir_scanner(dn_dir, NULL, pglog_fname_match);
        set_latest_files_num(dir_scan, festate->m_latest_files);
        master_only_set_dirlist(dir_scan);
    } else {
        char* loghome = gs_getenv_r("GAUSSLOG");
        check_backend_env(loghome);
        if (loghome && '\0' != loghome[0]) {
            char top_dir[MAXPGPATH] = {0};
            int ret = snprintf_s(top_dir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_log", loghome);
            securec_check_ss(ret, "", "");

            dir_scan = festate->dir_scan = new_logdir_scanner(top_dir, match_dir_name, pglog_fname_match);
            set_latest_files_num(dir_scan, festate->m_latest_files);
            assign_scanned_directorys(node, dir_scan);
        } else {
            ereport(ERROR, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("GAUSSLOG not set")));
        }
    }

    /* filter dirname if needed */
    festate->m_total_dirname_num = list_length(dir_scan->dir_list);
    if (festate->m_need_check_dirname) {
        dir_scan->dir_list = filter_dirname_list(node, dir_scan->dir_list);
    }
    copy_dirlist_for_rescan(dir_scan);

    /* begin to scan directory */
    begin_scan_dirs(festate->dir_scan, node);
}

static void set_func_args(FunctionCallInfoData& fcinfo, pglogPlanState* pg_log, int attidx, Datum val)
{
    InitFunctionCallInfoData(fcinfo, &pg_log->allattr_fmgrinfo[attidx], 3, InvalidOid, NULL, NULL);
    fcinfo.argnull[0] = false;
    fcinfo.argnull[1] = false;
    fcinfo.argnull[2] = false;
    fcinfo.arg[0] = CStringGetDatum(text_to_cstring((const text*)val));
    fcinfo.arg[1] = ObjectIdGetDatum(pg_log->allattr_typioparam[attidx]);
    fcinfo.arg[2] = Int32GetDatum(pg_log->allattr_typmod[attidx]);
}

static void fill_target_tuple_values(ForeignScanState* node)
{
    logFdwPlanState* festate = (logFdwPlanState*)node->fdw_state;
    pglogPlanState* pg_log = &festate->log_state.pg_log;
    FunctionCallInfoData fcinfo;
    Datum text_datum = 0;

#define COPY_TEXT_DATUM(_attr)                                                     \
    do {                                                                           \
        festate->m_isnull[(_attr)] = false;                                        \
        festate->m_values[(_attr)] = pg_log->real_values[pg_log->attmap[(_attr)]]; \
    } while (0)

    if (pg_log->attmap[PGLOG_ATTR_LOGTIME] >= 0) {
        text_datum = pg_log->real_values[pg_log->attmap[(PGLOG_ATTR_LOGTIME)]];
        set_func_args(fcinfo, pg_log, PGLOG_ATTR_LOGTIME, text_datum);
        festate->m_isnull[PGLOG_ATTR_LOGTIME] = false;
        festate->m_values[PGLOG_ATTR_LOGTIME] = FunctionCallInvoke(&fcinfo);
    }
    if (pg_log->attmap[PGLOG_ATTR_NODENAME] >= 0) {
        COPY_TEXT_DATUM(PGLOG_ATTR_NODENAME);
    }
    if (pg_log->attmap[PGLOG_ATTR_APPNAME] >= 0) {
        COPY_TEXT_DATUM(PGLOG_ATTR_APPNAME);
    }
    if (pg_log->attmap[PGLOG_ATTR_SESSION_START] >= 0) {
        text_datum = pg_log->real_values[pg_log->attmap[(PGLOG_ATTR_SESSION_START)]];
        set_func_args(fcinfo, pg_log, PGLOG_ATTR_SESSION_START, text_datum);
        festate->m_isnull[PGLOG_ATTR_SESSION_START] = false;
        festate->m_values[PGLOG_ATTR_SESSION_START] = FunctionCallInvoke(&fcinfo);
    }
    if (pg_log->attmap[PGLOG_ATTR_SESSION_ID] >= 0) {
        COPY_TEXT_DATUM(PGLOG_ATTR_SESSION_ID);
    }
    if (pg_log->attmap[PGLOG_ATTR_DBNAME] >= 0) {
        COPY_TEXT_DATUM(PGLOG_ATTR_DBNAME);
    }
    if (pg_log->attmap[PGLOG_ATTR_REMOTE] >= 0) {
        COPY_TEXT_DATUM(PGLOG_ATTR_REMOTE);
    }
    if (pg_log->attmap[PGLOG_ATTR_CMDTAG] >= 0) {
        COPY_TEXT_DATUM(PGLOG_ATTR_CMDTAG);
    }
    if (pg_log->attmap[PGLOG_ATTR_USERNAME] >= 0) {
        COPY_TEXT_DATUM(PGLOG_ATTR_USERNAME);
    }
    if (pg_log->attmap[PGLOG_ATTR_VIRTUAL_XID] >= 0) {
        COPY_TEXT_DATUM(PGLOG_ATTR_VIRTUAL_XID);
    }
    if (pg_log->attmap[PGLOG_ATTR_PID] >= 0) {
        text_datum = pg_log->real_values[pg_log->attmap[(PGLOG_ATTR_PID)]];
        set_func_args(fcinfo, pg_log, PGLOG_ATTR_PID, text_datum);
        festate->m_isnull[PGLOG_ATTR_PID] = false;
        festate->m_values[PGLOG_ATTR_PID] = FunctionCallInvoke(&fcinfo);
    }

    /*
     * %l in log_line_prefix means the line no in the same transaction, but
     * not the same file. in fact, the line no in the same file is usable.
     * so use file line no instead of line no in transaction.
     */

    if (pg_log->attmap[PGLOG_ATTR_XID] >= 0) {
        text_datum = pg_log->real_values[pg_log->attmap[(PGLOG_ATTR_XID)]];
        set_func_args(fcinfo, pg_log, PGLOG_ATTR_XID, text_datum);
        festate->m_isnull[PGLOG_ATTR_XID] = false;
        festate->m_values[PGLOG_ATTR_XID] = FunctionCallInvoke(&fcinfo);
    }
    if (pg_log->attmap[PGLOG_ATTR_QID] >= 0) {
        text_datum = pg_log->real_values[pg_log->attmap[(PGLOG_ATTR_QID)]];
        set_func_args(fcinfo, pg_log, PGLOG_ATTR_QID, text_datum);
        festate->m_isnull[PGLOG_ATTR_QID] = false;
        festate->m_values[PGLOG_ATTR_QID] = FunctionCallInvoke(&fcinfo);
    }
    if (pg_log->attmap[PGLOG_ATTR_ECODE] >= 0) {
        COPY_TEXT_DATUM(PGLOG_ATTR_ECODE);
    }
    if (pg_log->attmap[PGLOG_ATTR_MODNAME] >= 0) {
        COPY_TEXT_DATUM(PGLOG_ATTR_MODNAME);
    }
    if (pg_log->attmap[PGLOG_ATTR_LEVEL] >= 0) {
        COPY_TEXT_DATUM(PGLOG_ATTR_LEVEL);
    }
    if (pg_log->attmap[PGLOG_ATTR_MESSAGE] >= 0) {
        COPY_TEXT_DATUM(PGLOG_ATTR_MESSAGE);
    }
}

static const char* skip_left_space(const char* str)
{
    char ch = *str;
    while ('\0' != ch && (' ' == ch || '\t' == ch)) {
        ch = *(++str);
    }
    return str;
}

static inline void reset_all_attvals(logFdwPlanState* festate, int ncols)
{
    /* reset and make all nulls */
    for (int i = 0; i < ncols; i++) {
        festate->m_isnull[i] = true;
        festate->m_values[i] = (Datum)0;
    }
}

static inline void set_attval_before_hostname(logFdwPlanState* festate)
{
    /* the first column is directory name */
    festate->m_isnull[PGLOG_ATTR_DIRNAME] = false;
    festate->m_values[PGLOG_ATTR_DIRNAME] = PointerGetDatum(festate->dname);

    /* the second column is log file name */
    festate->m_isnull[PGLOG_ATTR_FILENAME] = false;
    festate->m_values[PGLOG_ATTR_FILENAME] = PointerGetDatum(festate->filename);
}

static void set_attval_before_logdata(logFdwPlanState* festate)
{
    set_attval_before_hostname(festate);

    /* the third column is host name */
    festate->m_isnull[PGLOG_ATTR_HOSTNAME] = false;
    festate->m_values[PGLOG_ATTR_HOSTNAME] = festate->m_tmp_hostname_text;
}

static inline void set_not_matched_line_tuple(logFdwPlanState* festate, Datum line)
{
    /* not matched */
    festate->m_isnull[PGLOG_ATTR_MATCHED] = false;
    festate->m_values[PGLOG_ATTR_MATCHED] = BoolGetDatum(false);

    /* take it as a whole message text */
    festate->m_values[PGLOG_ATTR_MESSAGE] = line;
    festate->m_isnull[PGLOG_ATTR_MESSAGE] = false;
}

static inline bool try_to_match_this_line(ForeignScanState* node, Datum line)
{
    logFdwPlanState* festate = (logFdwPlanState*)node->fdw_state;
    pglogPlanState* pg_log = &festate->log_state.pg_log;

    /*
     * it's right to set collation be DEFAULT_COLLATION_OID.
     * see pg_set_regex_collation() function about collation.
     *
     * line will be freed in cleanup_regexp_matches() called by regexp_match_to_array().
     */
    Datum rs = DirectFunctionCall2Coll(regexp_match_to_array,
        DEFAULT_COLLATION_OID, /* default collation */
        line,
        (festate->log_state.pg_log.regrex));
    if (rs) {
        /* this line matches regexp  */
        festate->m_isnull[PGLOG_ATTR_MATCHED] = false;
        festate->m_values[PGLOG_ATTR_MATCHED] = BoolGetDatum(true);

        ArrayIterator it = array_create_iterator(DatumGetArrayTypeP(rs), 0);
        int i = 0;
        while (array_iterate(it, &pg_log->real_values[i], &pg_log->real_isnull[i])) {
            i++;
        }
        Assert(i == pg_log->nattrs);

        /* fill the other values of this tuple */
        fill_target_tuple_values(node);
        return true;
    }
    return false;
}

static bool get_and_open_next_file(ForeignScanState* node)
{
    logFdwPlanState* festate = (logFdwPlanState*)node->fdw_state;

    festate->logfile_info = iterate_scan_dirs(festate->dir_scan, node);
    if (NULL == festate->logfile_info) {
        /* no log file to handle */
        return false;
    }

    /* set IO functions according to log file postfix */
    set_io_functions(festate);
    if (NULL != festate->dname) {
        Assert(festate->filename);
        pfree_ext(festate->dname);
        pfree_ext(festate->filename);
    }
    festate->filename = fetch_filename_from_logfile(festate->logfile_info->name);
    festate->dname = fetch_dirname_from_logfile(festate->logfile_info->name);
    festate->m_lineno_of_file = 0;

    void* fd = festate->m_open(festate->logfile_info->name);
    if (NULL != fd) {
        festate->m_vfd = fd;
    } else {
        ereport(ERROR, (errcode(ERRCODE_IO_ERROR), errmsg("Cannot open log file: %s", festate->logfile_info->name)));
    }

    /* reset log buffer */
    festate->m_log_buf.reset_data_buf();
    return true;
}

static inline void prepare_for_next_file(logFdwPlanState* festate)
{
    /* close this log file */
    void* temp_fd = festate->m_vfd;
    festate->m_close(temp_fd);

    /* try to handle next log file */
    festate->m_vfd = NULL;
    festate->logfile_info = NULL;
}

/* get a line from log file */
static bool get_next_log_line(ForeignScanState* node, char*& one_line)
{
    logFdwPlanState* festate = (logFdwPlanState*)node->fdw_state;
    char* tmp_line = NULL;

    /* fast path: read line from buffer directly */
    if (festate->m_log_buf.has_unhandled_data()) {
        tmp_line = festate->get_next_line();
        if (NULL != tmp_line) {
            /* get a line of log data */
            one_line = tmp_line;
            /* update line no */
            festate->m_lineno_of_file++;
            return true;
        } else {
            /* continue to read log data or next log file */
            festate->m_log_buf.handle_buffered_data();
        }
    } else {
        /* after the log_buf is all used at first time, pointer should set to the first position.*/
        festate->m_log_buf.handle_buffered_data();
    }
read_logdata:

    if (NULL != festate->m_vfd) {
        if (festate->continue_to_load_logdata()) {
            tmp_line = festate->get_next_line();
            if (NULL != tmp_line) {
                /* get a line of log data */
                one_line = tmp_line;
                /* update line no */
                festate->m_lineno_of_file++;
                return true;
            }
            /* this is an incompleted line, discard it */
            ++festate->m_incompleted_files;
        }
        prepare_for_next_file(festate);
    }

    /* get and handle the next log file */
    if (get_and_open_next_file(node)) {
        goto read_logdata;
    }
    return false;
}

static bool get_next_log_record(ForeignScanState* node)
{
    logFdwPlanState* festate = (logFdwPlanState*)node->fdw_state;
    prflog_parse* parser = festate->log_state.profile_log.parser;
    MemoryContext oldcnxt = NULL;
    Datum* vals = festate->m_values + PRFLOG_SYSATTR_NUM;
    bool* nuls = festate->m_isnull + PRFLOG_SYSATTR_NUM;
    int ncols = node->ss.ss_currentRelation->rd_att->natts - PRFLOG_SYSATTR_NUM;
    bool next_found = true;

next_record:

    /* fast path: read line from buffer directly */
    if (festate->m_log_buf.has_unhandled_data()) {
        oldcnxt = MemoryContextSwitchTo(festate->m_pertup_memcnxt);
        next_found = parser->iter_next(&(festate->m_log_buf), nuls, vals, ncols);
        (void)MemoryContextSwitchTo(oldcnxt);
        if (next_found) {
            set_attval_before_hostname(festate);
            festate->m_lineno_of_file++;
            return true; /* ok to get the next record */
        } else {
            /* continue to read log data or next log file */
            festate->m_log_buf.handle_buffered_data();
        }
    } else {
        /* after the log_buf is all used at first time, pointer should set to the first position.*/
        festate->m_log_buf.handle_buffered_data();
    }

    /* continue to load data and iter next tuple */
    if (NULL != festate->m_vfd) {
        if (festate->continue_to_load_logdata()) {
            oldcnxt = MemoryContextSwitchTo(festate->m_pertup_memcnxt);
            next_found = parser->iter_next(&(festate->m_log_buf), nuls, vals, ncols);
            (void)MemoryContextSwitchTo(oldcnxt);
            if (next_found) {
                set_attval_before_hostname(festate);
                festate->m_lineno_of_file++;
                return true; /* ok to get the next record */
            }
            /* this is an incompleted line, discard it */
            ++festate->m_incompleted_files;
        }
        parser->iter_end();
        prepare_for_next_file(festate);
    }

next_file:

    /* get and handle the next log file */
    if (get_and_open_next_file(node)) {
        /* begin to parse this log file and accessing its each record */
        parser->set(LOG_TYPE_PLOG);
        /* load data and begin to iter scanning */
        if (festate->continue_to_load_logdata()) {
            if (0 == parser->iter_begin(&festate->m_log_buf)) {
                goto next_record;
            }
        }
        /* this file is not imcompleted, so ignore it */
        ++festate->m_incompleted_files;

        /* skip this log file */
        parser->iter_end();
        prepare_for_next_file(festate);
        goto next_file;
    }

    /* no more any file to handle */
    return false;
}

static inline bool finish_scan_fast_path(logFdwPlanState* festate)
{
    /*
     * it doesn't need to scan directory if
     * 1) dir_scan is null when run "execute direct on (CN) subquery"
     * 2) refuted by this hostname
     */
    return (NULL == festate->dir_scan || festate->m_refuted_by_hostname);
}

/*
 *Replace str to another
 */
static int replace_tz(char* sSrc, const char* sMatchStr, const char* sReplaceStr)
{
    int StringLen = 0;
    char caNewString[MAX_LINE_LEN] = {0};
    errno_t rc = 0;
    if (strlen(sSrc) > MAX_LINE_LEN) {
        return -1;
    }
    if (NULL == sMatchStr) {
        return -1;
    }
    char* FindPos = strstr(sSrc, sMatchStr);
    /*if sSrc does not contain sMatchStr, do nothing*/
    if (NULL == FindPos) {
        return 0;
    }

    while (NULL != FindPos) {
        rc = memset_s(caNewString, MAX_LINE_LEN, 0, MAX_LINE_LEN);
        securec_check_c(rc, "\0", "\0");
        StringLen = FindPos - sSrc;
        rc = strncpy_s(caNewString, MAX_LINE_LEN, sSrc, StringLen);
        securec_check_c(rc, "\0", "\0");
        if ((StringLen + strlen(sReplaceStr)) >= MAX_LINE_LEN) {
            return -1;
        }
        rc = strcat_s(caNewString, MAX_LINE_LEN, sReplaceStr);
        securec_check_c(rc, "\0", "\0");
        if ((StringLen + strlen(sReplaceStr) + strlen(FindPos + strlen(sMatchStr))) >= MAX_LINE_LEN) {
            return -1;
        }
        rc = strcat_s(caNewString, MAX_LINE_LEN, FindPos + strlen(sMatchStr));
        securec_check_c(rc, "\0", "\0");
        rc = strcpy_s(sSrc, MAX_LINE_LEN, caNewString);
        securec_check_c(rc, "\0", "\0");

        FindPos = strstr(sSrc, sMatchStr);
    }

    return 0;
}

/* Timezone CST abbreviate is not unique,find map from timezone abbreviate to digit,
+ is EAST Timezone,- is WEAT timezone*/
static char* trans_tzname_to_digit(const char* tzname, const char* abbrevs)
{
    /* Timezone for China Standard Time */
    if (strcmp(tzname, "PRC") == 0) {
        return "+8";
    } else if (strcmp(tzname, "Asia/Beijing") == 0) {
        return "+8";
    } else if (strcmp(tzname, "Asia/Shanghai") == 0) {
        return "+8";
    } else if (strcmp(tzname, "Asia/Chongqing") == 0) {
        return "+8";
    } else if (strcmp(tzname, "Asia/Harbin") == 0) {
        return "+8";
    } else if (strcmp(tzname, "Asia/Taipei") == 0) {
        return "+8";
    } else if (strcmp(tzname, "Asia/Macao") == 0) {
        return "+8";
    } else if (strcmp(tzname, "Asia/Urumqi") == 0) {
        return "+8";
    }
    /* Timezone for Central Standard Time */
    else if (strcmp(tzname, "Australia/Darwin") == 0) {
        return "+9:30";
    }
    /* Timezone for Cuba Standard Time */
    else if (strcmp(tzname, "America/Havana") == 0 && strcmp(abbrevs, "CST") == 0) {
        return "-5";
    } else if (strcmp(tzname, "Cuba") == 0 && strcmp(abbrevs, "CST") == 0) {
        return "-5";
    }
    /* Timezone for Cuba Standard Time with daylight*/
    else if (strcmp(tzname, "America/Havana") == 0 && strcmp(abbrevs, "CDT") == 0) {
        return "-4";
    } else if (strcmp(tzname, "Cuba") == 0 && strcmp(abbrevs, "CDT") == 0) {
        return "-4";
    } else {
        return NULL;
    }
}

static char* get_abbrevs_name()
{
    struct timeval tv;
    pg_time_t stamp_time;
    gettimeofday(&tv, NULL);
    stamp_time = (pg_time_t)tv.tv_sec;
    return const_cast<char*>(pg_get_abbrevs_name(&stamp_time, log_timezone));
}

static HeapTuple pglog_get_next_tuple(ForeignScanState* node)
{
    logFdwPlanState* festate = (logFdwPlanState*)node->fdw_state;

    char* line = NULL;
    if (!get_next_log_line(node, line)) {
        return NULL;
    }

    Assert(PGLOG_ATTR_MAX == node->ss.ss_currentRelation->rd_att->natts);
    reset_all_attvals(festate, PGLOG_ATTR_MAX);

    MemoryContext old_memcnxt = MemoryContextSwitchTo(festate->m_pertup_memcnxt);
    set_attval_before_logdata(festate);

    const char* trimed_line = skip_left_space(line);
    char* abbrevs_tz = get_abbrevs_name();
    char* digit_tz = trans_tzname_to_digit(pg_get_timezone_name(log_timezone), abbrevs_tz);

    if (trimed_line == line) {
        /* trans CST digit timezone */
        if (NULL != digit_tz) {
            (void)replace_tz(line, abbrevs_tz, digit_tz);
        }
        if (!try_to_match_this_line(node, CStringGetTextDatum((const char*)line))) {
            /* failed to match */
            set_not_matched_line_tuple(festate, CStringGetTextDatum((const char*)line));
        }
    } else {
        /* trans CST digit timezone */
        if (NULL != digit_tz) {
            (void)replace_tz((char*)trimed_line, abbrevs_tz, digit_tz);
        }
        /* in most cases it's not matched if this line starts with space */
        set_not_matched_line_tuple(festate, CStringGetTextDatum(trimed_line));
    }

    /* set line no */
    festate->m_isnull[PGLOG_ATTR_LINENO] = false;
    festate->m_values[PGLOG_ATTR_LINENO] = Int64GetDatum(festate->m_lineno_of_file);

    /* build a tuple */
    HeapTuple tuple = heap_form_tuple(node->ss.ss_currentRelation->rd_att, festate->m_values, festate->m_isnull);

    (void)MemoryContextSwitchTo(old_memcnxt);
    return tuple;
}

/*
 * log_iterate_foreign_scan
 *
 */
static TupleTableSlot* pglog_iterate_fs(ForeignScanState* node)
{
    logFdwPlanState* festate = (logFdwPlanState*)node->fdw_state;
    if (!finish_scan_fast_path(festate)) {
        HeapTuple tup = NULL;
        if (festate->m_tuple_cur < festate->m_tuple_num) {
            /* fast path, return the next tuple from its cache */
            TupleTableSlot* slot = node->ss.ss_ScanTupleSlot;
            tup = festate->m_tuple_buf[festate->m_tuple_cur++];
            ExecStoreTuple(tup, slot, InvalidBuffer, false);
            return slot;
        }

        /* reset this tuple cache */
        festate->m_tuple_cur = 0;
        festate->m_tuple_num = 0;
        /* reset per-tuple memory context before this iter */
        MemoryContextReset(festate->m_pertup_memcnxt);

        /* try to fill this cache with many tuples */
        int ntuples = 0;
        PERF_TRACE(TRACK_START, GET_PG_LOG_TUPLES);
        while (ntuples < LOGFDW_TUPLE_CACHE_SIZE) {
            tup = pglog_get_next_tuple(node);
            if (tup) {
                festate->m_tuple_buf[ntuples++] = tup;
            } else {
                break;
            }
        }
        PERF_TRACE(TRACK_END, GET_PG_LOG_TUPLES);

        if (ntuples > 0) {
            /* return the first tuple in cache */
            HeapTuple tuple = festate->m_tuple_buf[0];

            /* update tuple cache info */
            festate->m_tuple_num = ntuples;
            festate->m_tuple_cur = 1;

            TupleTableSlot* slot = node->ss.ss_ScanTupleSlot;
            ExecStoreTuple(tuple, slot, InvalidBuffer, false);
            return slot;
        }
    }
    /* there is no any tuple */
    return NULL;
}

static TupleTableSlot* iterate_fs_without_logdata(ForeignScanState* node)
{
    logFdwPlanState* festate = (logFdwPlanState*)node->fdw_state;
    if (finish_scan_fast_path(festate)) {
        return NULL; /* no matter this host */
    }

    /* get the next log file */
    festate->logfile_info = iterate_scan_dirs(festate->dir_scan, node);
    if (NULL == festate->logfile_info) {
        return NULL; /* no other log file */
    }

    if (NULL != festate->dname) {
        Assert(festate->filename);
        pfree_ext(festate->dname);
        pfree_ext(festate->filename);
    }
    /* set filename and dirname */
    festate->filename = fetch_filename_from_logfile(festate->logfile_info->name);
    festate->dname = fetch_dirname_from_logfile(festate->logfile_info->name);

    /* set all values be null */
    reset_all_attvals(festate, node->ss.ss_currentRelation->rd_att->natts);

    /* reset per-tuple memory context before this iter */
    MemoryContextReset(festate->m_pertup_memcnxt);

    MemoryContext old_memcnxt = MemoryContextSwitchTo(festate->m_pertup_memcnxt);
    set_attval_before_logdata(festate);

    /* build a tuple */
    HeapTuple tuple = heap_form_tuple(node->ss.ss_currentRelation->rd_att, festate->m_values, festate->m_isnull);

    TupleTableSlot* slot = node->ss.ss_ScanTupleSlot;
    ExecClearTuple(slot);
    ExecStoreTuple(tuple, slot, InvalidBuffer, false);

    (void)MemoryContextSwitchTo(old_memcnxt);
    return slot;
}

static void append_explain_string(StringInfo str, StringInfo tmpstr, int indent, bool master_only, bool need_log_data)
{
    appendStringInfoSpaces(str, indent);
    if (master_only) {
        appendStringInfo(str, "unreachable nodes: none (master only)\n");
    } else {
        resetStringInfo(tmpstr);
        explain_unreachable_nodes(tmpstr);
        if (tmpstr->len > 0) {
            appendStringInfo(str, "(unreachable nodes: %s)", tmpstr->data);
        } else {
            appendStringInfo(str, "(unreachable nodes: none)");
        }
        appendStringInfo(str, "(need load log data: %s)\n", (need_log_data ? "yes" : "no"));
    }
}

/*
 * log_explain_foreign_scan
 *
 */
static void pglog_explain_fs(ForeignScanState* node, ExplainState* es)
{
    bool master_only = relation_getopt_masteronly(node->ss.ss_currentRelation);
    bool need_log_data = target_list_need_log_data(node);

    StringInfoData explain_str;
    initStringInfo(&explain_str);

    /* different formats */
    if (EXPLAIN_NORMAL != t_thrd.explain_cxt.explain_perf_mode && es->planinfo->m_staticInfo) {
        es->planinfo->m_staticInfo->set_plan_name<true, true>();
        append_explain_string(es->planinfo->m_staticInfo->info_str, &explain_str, 0, master_only, need_log_data);
    }

    if (es->format == EXPLAIN_FORMAT_TEXT) {
        append_explain_string(es->str, &explain_str, es->indent * 2, master_only, need_log_data);
    } else {
        if (master_only) {
            ExplainPropertyText("unreachable nodes", "none (master only)", es);
        } else {
            resetStringInfo(&explain_str);
            explain_unreachable_nodes(&explain_str);
            if (explain_str.len > 0) {
                ExplainPropertyText("unreachable nodes", explain_str.data, es);
            } else {
                ExplainPropertyText("unreachable nodes", "none", es);
            }
        }
        ExplainPropertyText("need load log data", (need_log_data ? "yes" : "no"), es);
    }
    pfree_ext(explain_str.data);
}

/*
 * log_end_foreign_scan
 *		Finish scanning foreign table and dispose objects used for this scan
 */
static void pglog_end_fs(ForeignScanState* node)
{
    logFdwPlanState* festate = (logFdwPlanState*)node->fdw_state;

    if (NULL != festate) {
        if (NULL != festate->dir_scan) {
            end_scan_dirs(festate->dir_scan);
            free_logdir_scanner(festate->dir_scan);
            festate->dir_scan = NULL;
        }
        /* all the other memory will be freed by destroying memorycontext */
    }
}

/*
 * pglog_rescan_fs
 *		Rescan table, possibly with new parameters
 */
static void pglog_rescan_fs(ForeignScanState* node)
{
    if (IS_PGXC_COORDINATOR) {
        return;
    }
    /* now nothing to do with logFdwPlanState.log_state.pg_log */

    /* prepare for next scan */
    logFdwPlanState* festate = (logFdwPlanState*)node->fdw_state;
    begin_scan_dirs(festate->dir_scan, node);
}

/*
 * log_begin_foreign_scan
 */
static void log_begin_foreign_scan(ForeignScanState* node, int eflags)
{
    logFdwPlanState* festate = (logFdwPlanState*)palloc0(sizeof(logFdwPlanState));

    MemoryContext memcnxt = AllocSetContextCreate(CurrentMemoryContext,
        "Log Data Foreign Scan",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    MemoryContext old_memcnxt = MemoryContextSwitchTo(memcnxt);

    node->fdw_state = (void*)festate;
    festate->m_memcnxt = memcnxt;

    /* make m_pertup_memcnxt under my top m_memcnxt */
    festate->m_pertup_memcnxt = AllocSetContextCreate(memcnxt,
        "Per Tuple of Log Data Foreign Scan",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    int ncols = node->ss.ss_currentRelation->rd_att->natts;
    festate->m_isnull = (bool*)palloc0(ncols * sizeof(bool));
    festate->m_values = (Datum*)palloc0(ncols * sizeof(Datum));
    festate->m_master_only = relation_getopt_masteronly(node->ss.ss_currentRelation);
    festate->m_latest_files = relation_getopt_latest_files_num(node->ss.ss_currentRelation);
    festate->m_need_read_logdata = target_list_need_log_data(node);
    festate->m_tmp_hostname_text = get_hostname_text();

    FdwLogType logypte = relation_getopt_logtype(node->ss.ss_currentRelation);
    fillup_need_check_flags(node, logypte);
    if (festate->m_need_check_hostname) {
        festate->m_refuted_by_hostname = whether_refuted_by_hostname(node);
    }
    if (IS_PGXC_DATANODE) {
        /* data buffer for log file */
        logdata_buf::init(&festate->m_log_buf, LOGFDW_BUFFER_SZ);
    }
    festate->m_plan_node_id = node->ss.ps.plan->plan_node_id;

    switch (logypte) {
        case FLT_PG_LOG:
            festate->begin_foreign_scan = pglog_begin_fs;
            festate->iterate_foreign_scan =
                festate->m_need_read_logdata ? pglog_iterate_fs : iterate_fs_without_logdata;
            festate->end_foreign_scan = pglog_end_fs;
            festate->explain_foreign_scan = pglog_explain_fs;
            festate->rescan_foreign_scan = pglog_rescan_fs;
            break;
        case FLT_GS_PROFILE:
            festate->begin_foreign_scan = profilelog_begin_fs;
            festate->iterate_foreign_scan =
                festate->m_need_read_logdata ? profilelog_iterate_fs : iterate_fs_without_logdata;
            festate->end_foreign_scan = profilelog_end_fs;
            festate->explain_foreign_scan = profilelog_explain_fs;
            festate->rescan_foreign_scan = profilelog_rescan_fs;
            break;
        default:
            break;
    }

    festate->begin_foreign_scan(node, eflags);

    (void)MemoryContextSwitchTo(old_memcnxt);
}

/*
 * log_iterate_foreign_scan
 *
 */
static TupleTableSlot* log_iterate_foreign_scan(ForeignScanState* node)
{
    logFdwPlanState* festate = (logFdwPlanState*)node->fdw_state;
    festate->m_need_timing = (node->ss.ps.instrument && node->ss.ps.instrument->need_timer);
    MemoryContext old_memcnxt = MemoryContextSwitchTo(festate->m_memcnxt);
    TupleTableSlot* slot = festate->iterate_foreign_scan(node);
    (void)MemoryContextSwitchTo(old_memcnxt);
    return slot;
}

/*
 * log_explain_foreign_scan
 *
 */
static void log_explain_foreign_scan(ForeignScanState* node, ExplainState* es)
{
    logFdwPlanState* festate = (logFdwPlanState*)node->fdw_state;
    festate->explain_foreign_scan(node, es);
}

/*
 * log_end_foreign_scan
 *		Finish scanning foreign table and dispose objects used for this scan
 */
static void log_end_foreign_scan(ForeignScanState* node)
{
    logFdwPlanState* festate = (logFdwPlanState*)node->fdw_state;

    /* finish this foreign scan */
    festate->end_foreign_scan(node);
    logdata_buf::deinit(&festate->m_log_buf);

    /* destroy private memory context */
    MemoryContextDelete(festate->m_memcnxt);

    Instrumentation* instr = node->ss.ps.instrument;
    if (NULL != instr) {
        /* explain type */
        instr->dfsType = TYPE_LOG_FT;
        /* host name info */
        instr->minmaxCheckStripe = 1;
        instr->minmaxFilterStripe = festate->m_refuted_by_hostname ? 1 : 0;
        /* dirs info */
        instr->minmaxCheckStride = festate->m_total_dirname_num;
        instr->minmaxFilterStride = festate->m_refuted_dirname_num;
        /* files info */
        instr->minmaxCheckFiles = festate->m_total_logfile_num;
        instr->minmaxFilterFiles = festate->m_refuted_logfile_num_by_name + festate->m_refuted_logfile_num_by_time;
        instr->dynamicPrunFiles = festate->m_latest_files;
        instr->staticPruneFiles = festate->m_incompleted_files;
    }

    /* destroy logFdwPlanState object */
    pfree_ext(festate);
    node->fdw_state = NULL;
}

/*
 * log_rescan_foreign_scan
 *		Rescan table, possibly with new parameters
 */
static void log_rescan_foreign_scan(ForeignScanState* node)
{
    logFdwPlanState* festate = (logFdwPlanState*)node->fdw_state;
    MemoryContext old_memcnxt = MemoryContextSwitchTo(festate->m_memcnxt);

    /* clean up EXPLAIN data */
    festate->m_total_logfile_num = 0;
    festate->m_refuted_logfile_num_by_name = 0;
    festate->m_refuted_logfile_num_by_time = 0;
    festate->m_incompleted_files = 0;

    /* close file if needed */
    if (NULL != festate->m_vfd) {
        festate->m_close(festate->m_vfd);
    }
    festate->m_vfd = NULL;

    /* free memory if needed */
    if (NULL != festate->dname) {
        pfree_ext(festate->dname);
    }
    if (NULL != festate->filename) {
        pfree_ext(festate->filename);
    }
    festate->logfile_info = NULL;
    festate->m_tuple_cur = 0;
    festate->m_tuple_num = 0;
    MemoryContextReset(festate->m_pertup_memcnxt);

    /* reset log data buffer */
    festate->m_log_buf.reset_data_buf();
    if (festate->dir_scan != NULL) {
        begin_rescan_dirs(festate->dir_scan);
    }

    festate->rescan_foreign_scan(node);
    (void)MemoryContextSwitchTo(old_memcnxt);
}

static ForeignScan* log_get_foreign_plan(
    PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid, ForeignPath* best_path, List* tlist, List* scan_clauses)
{
    Index scan_relid = baserel->relid;
    List* fdw_data = best_path->fdw_private;

    if (IS_PGXC_COORDINATOR) {
        if (!IS_STREAM) {
            ereport(WARNING, (errmsg("log_fdw enter non-stream mode when getting foreign plan")));
        }

        Relation rel = heap_open(foreigntableid, AccessShareLock);
        bool master_only = relation_getopt_masteronly(rel);
        heap_close(rel, AccessShareLock);

        if (!master_only) {
            /* get pairs about IP and datanode name list from pgxc_node tables */
            List* ip_dnname_pairs = get_ip_nodename_pairs_from_pgxc_node();
            /* append inner option <logft_opt_masters_info> */
            fdw_data = lappend(fdw_data, makeDefElem(pstrdup(logft_opt_masters_info), (Node*)ip_dnname_pairs));
        }

        /* hint the caller all the unreachable nodes if needed */
        List* unreach_host = get_unreachable_hosts();
        if (NULL != unreach_host) {
            StringInfoData unreach_details;
            initStringInfo(&unreach_details);
            descript_unreachable_nodes(&unreach_details, unreach_host);
            ereport(INFO, (errmsg("some hosts/nodes unreachalbe"), errdetail("%s", unreach_details.data)));
            pfree_ext(unreach_details.data);
            free_unreachable_hosts(unreach_host);
            unreach_host = NULL;
        }
    }

    /*
     * We have no native ability to evaluate restriction clauses, so we just
     * put all the scan_clauses into the plan node's qual list for the
     * executor to check.  So all we have to do here is strip RestrictInfo
     * nodes from the clauses and ignore pseudoconstants (which will be
     * handled elsewhere).
     */
    scan_clauses = extract_actual_clauses(scan_clauses, false);
    best_path->fdw_private = fdw_data;

    /* Create the ForeignScan node */
    ForeignScan* fscan = make_foreignscan(tlist,
        scan_clauses,
        scan_relid,
        NIL, /* no expressions to evaluate */
        fdw_data,
        EXEC_ON_DATANODES); /* no private state either */

    return fscan;
}

static void log_get_foreign_paths(PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid)
{
    logFdwPlanState* fdw_private = (logFdwPlanState*)baserel->fdw_private;
    Cost startup_cost = 0;
    Cost total_cost = 0;

    /* Estimate costs */
    estimate_costs(root, baserel, fdw_private, &startup_cost, &total_cost);

    /* Create a ForeignPath node and add it as only possible path */
    add_path(root,
        baserel,
        (Path*)create_foreignscan_path(root,
            baserel,
            startup_cost,
            total_cost,
            NIL,  /* no pathkeys */
            NULL, /* no outer rel either */
            NIL,
            1)); /* no fdw_private data */

    /*
     * If data file was sorted, and we knew it somehow, we could insert
     * appropriate pathkeys into the ForeignPath node to tell the planner
     * that.
     */
}

/*
 * Estimate size of a foreign table.
 *
 * The main result is returned in baserel->rows.  We also set
 * fdw_private->pages and fdw_private->ntuples for later use in the cost
 * calculation.
 */
static void estimate_size(PlannerInfo* root, RelOptInfo* baserel, logFdwPlanState* fdw_private)
{
    /* Save the output-rows estimate for the planner */
    baserel->rows = 100;
}

static void log_get_foreign_relsize(PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid)
{
    logFdwPlanState* fdw_private = NULL;
    /* Estimate relation size */
    estimate_size(root, baserel, fdw_private);
}

/*
 * Estimate costs of scanning a foreign table.
 *
 * Results are returned in *startup_cost and *total_cost.
 */
static void estimate_costs(
    PlannerInfo* root, RelOptInfo* baserel, logFdwPlanState* fdw_private, Cost* startup_cost, Cost* total_cost)
{
    *total_cost = *startup_cost = 100;
}

static bool match_dir_name(const char* subdir_name)
{
    if (0 == strncmp(subdir_name, "cn_", 3) || 0 == strncmp(subdir_name, "dn_", 3)) {
        return true;
    }
    return false;
}

static bool all_digits(const char* start, size_t len)
{
    for (size_t i = 0; i < len; ++i) {
        if (isdigit(*start++))
            continue;
        return false;
    }
    return true;
}

static bool log_fname_matched_with_pattern(const char* fname)
{
    const size_t minlen = strlen("postgresql-YYYY-MM-DD_HHMMSS.log");
    size_t len = strlen(fname);
    if (len >= minlen) {
        if (0 == strncmp("postgresql-", fname, 11) && '-' == fname[15] && '-' == fname[18] && '_' == fname[21] &&
            all_digits(fname + 11, 4) && /* year */
            all_digits(fname + 16, 2) && /* month */
            all_digits(fname + 19, 2) && /* day */
            all_digits(fname + 22, 6)    /* hour, minute, second */
        )
            return true;
    }
    return false;
}

static bool pglog_fname_match(const char* log_name)
{
    /*
     * supported formats includes:
     * 1) postgresql-YYYY-MM-DD_HHMMSS.log
     * 2) postgresql-YYYY-MM-DD_HHMMSS.zip
     * 3) postgresql-YYYY-MM-DD_HHMMSS.log.gz
     */
    const size_t exp_len = strlen("postgresql-YYYY-MM-DD_HHMMSS.log");
    const size_t exp_len2 = strlen("postgresql-YYYY-MM-DD_HHMMSS.log.gz");

    size_t len = strlen(log_name);
    if ((exp_len == len) && log_fname_matched_with_pattern(log_name)) {
        if ((0 == strncmp(log_name + (len - 4), ".log", 4)) || (0 == strncmp(log_name + (len - 4), ".zip", 4))) {
            return true;
        }
    } else if ((exp_len2 == len) && log_fname_matched_with_pattern(log_name)) {
        if (0 == strncmp(log_name + (len - 7), ".log.gz", 7)) {
            return true;
        }
    }
    return false;
}

static bool profilelog_fname_match(const char* log_name)
{
    /*
     * supported formats includes:
     * 1) postgresql-YYYY-MM-DD_HHMMSS.prf
     * 2) postgresql-YYYY-MM-DD_HHMMSS.zip
     */
    const size_t exp_len = strlen("postgresql-YYYY-MM-DD_HHMMSS.prf");

    size_t len = strlen(log_name);
    if ((exp_len == len) && log_fname_matched_with_pattern(log_name)) {
        if ((0 == strncmp(log_name + (len - 4), ".prf", 4)) || (0 == strncmp(log_name + (len - 4), ".zip", 4))) {
            return true;
        }
    }
    return false;
}

/*
 * because profile log data is buffered and to flush only when this
 * buffer is full. so in order to access these buffered data, we have to
 * flush them before starting query.
 * it's done by two steps:
 * 1) set JUST-TO-FLUSH request flag, but not rotate log file;
 * 2) send signal to postmaster who will request syslogger thread to
 *    flush buffer. this signal PMSIGNAL_ROTATE_LOGFILE will be shared by
 *    both ROTATION and FLUSH actions.
 */
static void profilelog_flush_buffered_data(void)
{
    set_flag_to_flush_buffer();
    (void)pg_rotate_logfile(NULL);
}

static void profilelog_begin_fs(ForeignScanState* node, int eflags)
{
    if (IS_PGXC_COORDINATOR) {
        /* coordinator node doesn't scan data */
        return;
    }

    logFdwPlanState* festate = (logFdwPlanState*)node->fdw_state;
    logdir_scanner dir_scan = NULL;

    char* loghome = gs_getenv_r("GAUSSLOG");
    check_backend_env(loghome);
    if (loghome && '\0' != loghome[0]) {
        /* request to flush buffered data before scan files */
        profilelog_flush_buffered_data();

        char top_dir[MAXPGPATH] = {0};
        if (festate->m_master_only) {
            int ret = snprintf_s(top_dir,
                MAXPGPATH,
                MAXPGPATH - 1,
                "%s/gs_profile/%s",
                loghome,
                g_instance.attr.attr_common.PGXCNodeName);
            securec_check_ss(ret, "", "");

            dir_scan = festate->dir_scan = new_logdir_scanner(top_dir, NULL, profilelog_fname_match);
            set_latest_files_num(dir_scan, festate->m_latest_files);
            master_only_set_dirlist(dir_scan);
        } else {
            int ret = snprintf_s(top_dir, MAXPGPATH, MAXPGPATH - 1, "%s/gs_profile", loghome);
            securec_check_ss(ret, "", "");

            /* prepare for scanning log directory and log files */
            dir_scan = festate->dir_scan = new_logdir_scanner(top_dir, match_dir_name, profilelog_fname_match);
            set_latest_files_num(dir_scan, festate->m_latest_files);
            assign_scanned_directorys(node, dir_scan);
        }

        festate->m_total_dirname_num = list_length(dir_scan->dir_list);
        if (festate->m_need_check_dirname) {
            dir_scan->dir_list = filter_dirname_list(node, dir_scan->dir_list);
        }
        copy_dirlist_for_rescan(dir_scan);
        begin_scan_dirs(dir_scan, node);
    } else {
        ereport(ERROR, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("GAUSSLOG not set")));
    }

    /* data parser object for profile log */
    prflog_parse* parser = (prflog_parse*)palloc0(sizeof(prflog_parse));
    parser->init();
    festate->log_state.profile_log.parser = parser;
}

static TupleTableSlot* profilelog_iterate_fs(ForeignScanState* node)
{
    logFdwPlanState* festate = (logFdwPlanState*)node->fdw_state;
    if (!finish_scan_fast_path(festate)) {
        if (festate->m_tuple_cur < festate->m_tuple_num) {
            /* fast path, return the next tuple from its cache */
            TupleTableSlot* slot = node->ss.ss_ScanTupleSlot;
            HeapTuple tuple = festate->m_tuple_buf[festate->m_tuple_cur++];
            ExecStoreTuple(tuple, slot, InvalidBuffer, false);
            return slot;
        }

        /* reset this tuple cache */
        festate->m_tuple_cur = 0;
        festate->m_tuple_num = 0;
        /* reset per-tuple memory context before this iter */
        MemoryContextReset(festate->m_pertup_memcnxt);

        MemoryContext old_memcnxt = NULL;
        int ntuples = 0;
        PERF_TRACE(TRACK_START, GET_GS_PROFILE_TUPLES);
        while (ntuples < LOGFDW_TUPLE_CACHE_SIZE) {
            if (get_next_log_record(node)) {
                old_memcnxt = MemoryContextSwitchTo(festate->m_pertup_memcnxt);
                festate->m_tuple_buf[ntuples++] =
                    heap_form_tuple(node->ss.ss_currentRelation->rd_att, festate->m_values, festate->m_isnull);
                (void)MemoryContextSwitchTo(old_memcnxt);
            } else {
                break;
            }
        }
        PERF_TRACE(TRACK_END, GET_GS_PROFILE_TUPLES);

        if (ntuples > 0) {
            /* return the first tuple in cache */
            HeapTuple tuple = festate->m_tuple_buf[0];

            /* update tuple cache info */
            festate->m_tuple_num = ntuples;
            festate->m_tuple_cur = 1;

            TupleTableSlot* slot = node->ss.ss_ScanTupleSlot;
            ExecStoreTuple(tuple, slot, InvalidBuffer, false);
            return slot;
        }
    }
    return NULL;
}

static void profilelog_end_fs(ForeignScanState* node)
{
    logFdwPlanState* festate = (logFdwPlanState*)node->fdw_state;

    if (NULL != festate) {
        if (NULL != festate->dir_scan) {
            end_scan_dirs(festate->dir_scan);
            free_logdir_scanner(festate->dir_scan);
            festate->dir_scan = NULL;
        }

        festate->logfile_info = NULL;
        if (NULL != festate->dname) {
            pfree_ext(festate->dname);
        }

        prflog_parse* parser = festate->log_state.profile_log.parser;
        if (NULL != parser) {
            pfree_ext(parser);
            festate->log_state.profile_log.parser = NULL;
        }
    }
}

static void profilelog_rescan_fs(ForeignScanState* node)
{
    if (IS_PGXC_COORDINATOR) {
        return;
    }

    logFdwPlanState* festate = (logFdwPlanState*)node->fdw_state;
    /* reset parser */
    prflog_parse* parser = festate->log_state.profile_log.parser;
    parser->iter_end();
    /* prepare for next scan */
    begin_scan_dirs(festate->dir_scan, node);
}

static void profilelog_explain_fs(ForeignScanState* node, ExplainState* es)
{
    /* first call the same EXPLAIN part */
    pglog_explain_fs(node, es);
}

/* ANALYZE for log foreign table */
static bool log_unsupport_analyze(Relation relation, AcquireSampleRowsFunc* func, BlockNumber* totalPageCount,
    void* additionalData, bool estimate_table_rownum)
{
    return false; /* unsupport feature */
}

static inline bool find_logft_option(List* opt_list, const char* opt_name)
{
    ListCell* optcell = NULL;
    foreach (optcell, opt_list) {
        DefElem* opt = (DefElem*)lfirst(optcell);
        if (0 == pg_strcasecmp(opt->defname, opt_name)) {
            return true;
        }
    }
    return false;
}

static void log_validate_table_def(Node* obj)
{
    Assert(NULL != obj);
    switch (nodeTag(obj)) {
        case T_AlterTableStmt: {
            List* cmds = ((AlterTableStmt*)obj)->cmds;
            ListCell* lcmd = NULL;
            foreach (lcmd, cmds) {
                AlterTableCmd* cmd = (AlterTableCmd*)lfirst(lcmd);

                /* forbid to do */
                if (AT_AddColumn == cmd->subtype || AT_SetNotNull == cmd->subtype || AT_DropColumn == cmd->subtype ||
                    AT_AlterColumnType == cmd->subtype) {
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Un-support feature for log foreign table")));
                }
                /* do under some limitation */
                if (AT_ChangeOwner == cmd->subtype) {
                    if (!superuser_arg(get_role_oid(cmd->name, false))) {
                        ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("Cannot change owner to others but superuser for log foreign table")));
                    }
                }
                if (AT_GenericOptions == cmd->subtype) {
                    if (find_logft_option((List*)cmd->def, OPTION_LOGTYPE)) {
                        ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("Cannot change option \"%s\" of log foreign table", OPTION_LOGTYPE)));
                    }
                }
            }
            break;
        }
        case T_CreateForeignTableStmt: {
            /* MUST be real superuser for creating log foreign table */
            if (!isRelSuperuser()) {
                ereport(ERROR,
                    (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Must be superuser to create log foreign table")));
            }

            /* check write only */
            CreateForeignTableStmt* log_ft = (CreateForeignTableStmt*)obj;
            if (log_ft->write_only) {
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Unsupport write only for log foreign table.")));
            }
            /* OPTION_LOGTYPE must be given */
            bool logtype_found = log_ft->options ? find_logft_option(log_ft->options, OPTION_LOGTYPE) : false;
            if (!logtype_found) {
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Option \"%s\" must be specified for log foreign table.", OPTION_LOGTYPE)));
            }
            break;
        }
        default: {
            ereport(ERROR,
                (errcode(ERRCODE_SYSTEM_ERROR),
                    errmsg("unrecognized node type(%d) during validating log foreign table definition",
                        (int)nodeTag(obj))));
            break;
        }
    }
}

/*
 * Validate function
 */
Datum log_fdw_validator(PG_FUNCTION_ARGS)
{
    Oid contextId = PG_GETARG_OID(1);
    Datum options = PG_GETARG_DATUM(0);
    List* option_list = untransformRelOptions(options);

    if (ForeignTableRelationId == contextId) {
        valid_ft_options(option_list);
    }
    PG_RETURN_VOID();
}

Datum log_fdw_handler(PG_FUNCTION_ARGS)
{
    /* palloc0() is called in makeNode() */
    FdwRoutine* fdwroutine = makeNode(FdwRoutine);

    fdwroutine->ExplainForeignScan = log_explain_foreign_scan;
    fdwroutine->BeginForeignScan = log_begin_foreign_scan;
    fdwroutine->IterateForeignScan = log_iterate_foreign_scan;
    fdwroutine->ReScanForeignScan = log_rescan_foreign_scan;
    fdwroutine->EndForeignScan = log_end_foreign_scan;
    fdwroutine->GetForeignRelSize = log_get_foreign_relsize;
    fdwroutine->GetForeignPaths = log_get_foreign_paths;
    fdwroutine->GetForeignPlan = log_get_foreign_plan;
    fdwroutine->AnalyzeForeignTable = log_unsupport_analyze;
    fdwroutine->ValidateTableDef = log_validate_table_def;

    PG_RETURN_POINTER(fdwroutine);
}

/* information for pg_log columns' data type */
static const Oid pglog_column_type[PGLOG_ATTR_MAX] = {TEXTOID,
    TEXTOID,
    TEXTOID,
    BOOLOID,
    TIMESTAMPTZOID,
    TEXTOID,
    TEXTOID,
    TIMESTAMPTZOID,
    TEXTOID,
    TEXTOID,
    TEXTOID,
    TEXTOID,
    TEXTOID,
    TEXTOID,
    INT8OID,
    INT8OID,
    INT8OID,
    INT8OID,
    TEXTOID,
    TEXTOID,
    TEXTOID,
    TEXTOID};

/* information for profile_log columns' data type */
static const Oid prflog_column_type[PROFILELOG_ATTR_MAX] = {TEXTOID,
    TEXTOID,
    TEXTOID,
    TIMESTAMPTZOID,
    TEXTOID,
    INT8OID,
    INT8OID,
    INT8OID,
    TEXTOID,
    TEXTOID,
    INT4OID,
    INT8OID,
    INT8OID,
    INT8OID};

static inline bool the_same_type_with(const TypeName* coltype, const Oid expected_type)
{
    Oid atttypid = 0;
    int32 atttypmod = 0;
    typenameTypeIdAndMod(NULL, coltype, &atttypid, &atttypmod);
    return (expected_type == atttypid);
}

static bool check_logft_schema(List* schema, const Oid* columns_type, const int attrs_num)
{
    if (attrs_num == list_length(schema)) {
        ListCell* cell = NULL;
        int i = 0;
        foreach (cell, schema) {
            ColumnDef* entry = (ColumnDef*)lfirst(cell);
            if (!the_same_type_with(entry->typname, columns_type[i])) {
                return false; /* wrong columns' data type */
            }
            ++i;
        }
        return true;
    }
    return false; /* wrong columns' number */
}

static bool check_pglog_ft_definition(CreateForeignTableStmt* stmt)
{
    return check_logft_schema(stmt->base.tableElts, pglog_column_type, PGLOG_ATTR_MAX);
}

static bool check_prflog_definition(CreateForeignTableStmt* stmt)
{
    return check_logft_schema(stmt->base.tableElts, prflog_column_type, PROFILELOG_ATTR_MAX);
}

void check_log_ft_definition(CreateForeignTableStmt* stmt)
{
    if (0 != strcmp(stmt->servername, LOG_SRV)) {
        return; /* not a log foreign table */
    }

    FdwLogType t = get_logtype_from_options(stmt->options);
    bool check_ok = false;
    if (FLT_PG_LOG == t) {
        check_ok = check_pglog_ft_definition(stmt);
    } else if (FLT_GS_PROFILE == t) {
        check_ok = check_prflog_definition(stmt);
    }

    if (!check_ok) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("incorrect definition of log foreign table.")));
    }
}
