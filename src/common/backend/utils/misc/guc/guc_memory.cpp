/* --------------------------------------------------------------------
 * guc_memory.cpp
 *
 * Support for grand unified configuration schema, including SET
 * command, configuration file, and command line options.
 * See src/backend/utils/misc/README for more information.
 *
 *
 * Copyright (c) 2000-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * Written by Peter Eisentraut <peter_e@gmx.net>.
 *
 * IDENTIFICATION
 * src/backend/utils/misc/guc/guc_memory.cpp
 *
 * --------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <float.h>
#include <math.h>
#include <limits.h>
#include "utils/elog.h"

#ifdef HAVE_SYSLOG
#include <syslog.h>
#endif

#include "access/cbmparsexlog.h"
#include "access/gin.h"
#include "access/gtm.h"
#include "pgxc/pgxc.h"
#include "access/transam.h"
#include "access/twophase.h"
#include "access/xact.h"
#include "access/xlog.h"
#ifdef ENABLE_BBOX
#include "gs_bbox.h"
#endif
#include "catalog/namespace.h"
#include "catalog/pgxc_group.h"
#include "catalog/storage_gtt.h"
#include "commands/async.h"
#include "commands/prepare.h"
#include "commands/vacuum.h"
#include "commands/variable.h"
#include "commands/tablespace.h"
#include "commands/trigger.h"
#include "funcapi.h"
#include "instruments/instr_statement.h"
#include "job/job_scheduler.h"
#include "libpq/auth.h"
#include "libpq/be-fsstubs.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "opfusion/opfusion.h"
#include "optimizer/cost.h"
#include "optimizer/geqo.h"
#include "optimizer/nodegroups.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/prep.h"
#include "optimizer/gtmfree.h"
#include "parser/parse_expr.h"
#include "parser/parse_oper.h"
#include "parser/parse_type.h"
#include "parser/parser.h"
#include "parser/scansup.h"
#include "pgstat.h"
#include "pgxc/route.h"
#include "workload/workload.h"
#include "pgaudit.h"
#include "instruments/instr_unique_sql.h"
#include "commands/tablecmds.h"
#include "nodes/nodes.h"
#include "optimizer/pgxcship.h"
#include "pgxc/execRemote.h"
#include "pgxc/locator.h"
#include "optimizer/pgxcplan.h"
#include "pgxc/poolmgr.h"
#include "pgxc/nodemgr.h"
#include "utils/lsyscache.h"
#include "access/multi_redo_settings.h"
#include "catalog/pg_authid.h"
#include "commands/user.h"
#include "commands/user.h"
#include "flock.h"
#include "gaussdb_version.h"
#include "libcomm/libcomm.h"
#include "libpq/libpq-be.h"
#include "libpq/md5.h"
#include "libpq/sha2.h"
#include "optimizer/planner.h"
#include "optimizer/streamplan.h"
#include "postmaster/alarmchecker.h"
#include "postmaster/autovacuum.h"
#include "postmaster/bgwriter.h"
#include "postmaster/pagewriter.h"
#include "postmaster/postmaster.h"
#include "postmaster/syslogger.h"
#include "postmaster/twophasecleaner.h"
#include "postmaster/walwriter.h"
#include "replication/dataqueue.h"
#include "replication/datareceiver.h"
#include "replication/reorderbuffer.h"
#include "replication/replicainternal.h"
#include "replication/slot.h"
#include "replication/syncrep.h"
#include "replication/walreceiver.h"
#include "replication/walsender.h"
#include "storage/buf/bufmgr.h"
#include "storage/cucache_mgr.h"
#include "storage/smgr/fd.h"
#include "storage/predicate.h"
#include "storage/procarray.h"
#include "storage/standby.h"
#include "storage/remote_adapter.h"
#include "tcop/tcopprot.h"
#include "threadpool/threadpool.h"
#include "tsearch/ts_cache.h"
#include "utils/acl.h"
#include "utils/anls_opt.h"
#include "utils/atomic.h"
#include "utils/be_module.h"
#include "utils/builtins.h"
#include "utils/bytea.h"
#include "utils/distribute_test.h"
#include "utils/guc_tables.h"
#include "utils/memtrack.h"
#include "utils/memutils.h"
#include "utils/pg_locale.h"
#include "utils/plancache.h"
#include "utils/portal.h"
#include "utils/ps_status.h"
#include "utils/rel_gs.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "access/heapam.h"
#include "utils/tzparser.h"
#include "utils/xml.h"
#include "workload/cpwlm.h"
#include "workload/workload.h"
#include "utils/guc_memory.h"
#include "utils/mem_snapshot.h"

static bool check_memory_detail_tracking(char** newval, void** extra, GucSource source);
static void assign_memory_detail_tracking(const char* newval, void* extra);
static const char* show_memory_detail_tracking(void);
static const char* show_enable_memory_limit(void);
static bool check_syscache_threshold_gpc(int* newval, void** extra, GucSource source);
static bool check_uncontrolled_memory_context(char** newval, void** extra, GucSource source);
static void assign_uncontrolled_memory_context(const char* newval, void* extra);
static const char* show_uncontrolled_memory_context(void);
void free_memory_context_list(memory_context_list* head_node);
memory_context_list* split_string_into_list(const char* source);

static void InitMemoryConfigureNamesBool();
static void InitMemoryConfigureNamesInt();
static void InitMemoryConfigureNamesInt64();
static void InitMemoryConfigureNamesReal();
static void InitMemoryConfigureNamesString();
static void InitMemoryConfigureNamesEnum();

/* change the char * memory_tracking_mode to enum */
static const struct config_enum_entry memory_tracking_option[] = {
    {"none", MEMORY_TRACKING_NONE, false},
    {"peak", MEMORY_TRACKING_PEAKMEMORY, false},
    {"normal", MEMORY_TRACKING_NORMAL, false},
    {"executor", MEMORY_TRACKING_EXECUTOR, false},
    {"fullexec", MEMORY_TRACKING_FULLEXEC, false},
    {NULL, 0, false}
};

/* memory_trace_level enum */
static const struct config_enum_entry memory_trace_level[] = {
    {"none", MEMORY_TRACE_NONE, false},
    {"level1", MEMORY_TRACE_LEVEL1, false},
    {"level2", MEMORY_TRACE_LEVEL2, false},
    {NULL, 0, false}
};

/*
 * Contents of GUC tables
 *
 * See src/backend/utils/misc/README for design notes.
 *
 * TO ADD AN OPTION AS FOLLOWS.
 *
 * 1. Declare a global variable of type bool, int, double, or char*
 *	  and make use of it.
 *
 * 2. Decide at what times it's safe to set the option. See guc.h for
 *	  details.
 *
 * 3. Decide on a name, a default value, upper and lower bounds (if
 *	  applicable), etc.
 *
 * 4. Add a record below.
 *
 * 5. Add it to src/backend/utils/misc/postgresql_single.conf.sample or
 *	  src/backend/utils/misc/postgresql_distribute.conf.sample or both,
 *	  if appropriate.
 *
 * 6. Don't forget to document the option (at least in config.sgml).
 *
 * 7. If it's a new GUC_LIST option you must edit pg_dumpall.c to ensure
 *	  it is not single quoted at dump time.
 */
/* ******* option records follow ******* */
void InitMemoryConfigureNames()
{
    InitMemoryConfigureNamesBool();
    InitMemoryConfigureNamesInt();
    InitMemoryConfigureNamesInt64();
    InitMemoryConfigureNamesReal();
    InitMemoryConfigureNamesString();
    InitMemoryConfigureNamesEnum();

    return;
}

static void InitMemoryConfigureNamesBool()
{
    struct config_bool localConfigureNamesBool[] = {
        // variable to enable memory pool
        {{"memorypool_enable",
            PGC_POSTMASTER,
            NODE_ALL,
            RESOURCES_MEM,
            gettext_noop("Using memory pool."),
            NULL},
            &g_instance.attr.attr_memory.memorypool_enable,
            false,
            NULL,
            NULL,
            NULL},
        // variable to enable memory protect
        {{"enable_memory_limit",
            PGC_POSTMASTER,
            NODE_ALL,
            RESOURCES_MEM,
            gettext_noop("Using memory protect feature."),
            NULL},
            &g_instance.attr.attr_memory.enable_memory_limit,
            true,
            NULL,
            NULL,
            show_enable_memory_limit},

        {{"enable_memory_context_control",
            PGC_SIGHUP,
            NODE_ALL,
            RESOURCES_MEM,
            gettext_noop("check the max space size of memory context."),
            NULL},
            &u_sess->attr.attr_memory.enable_memory_context_control,
            false,
            NULL,
            NULL,
            NULL},

        // variable to enable early free policy
        {{"disable_memory_protect",
            PGC_USERSET,
            NODE_ALL,
            RESOURCES_MEM,
            gettext_noop("disable memory protect for query execution."),
            NULL},
            &u_sess->attr.attr_memory.disable_memory_protect,
            false,
            NULL,
            NULL,
            NULL},

        // variable to disable memory stats
        {{"disable_memory_stats",
            PGC_POSTMASTER,
            NODE_ALL,
            RESOURCES_MEM,
            gettext_noop("disable memory stats for query execution."),
            NULL},
            &g_instance.attr.attr_memory.disable_memory_stats,
            false,
            NULL,
            NULL,
            NULL},

#ifdef MEMORY_CONTEXT_CHECKING
        // variable to enable memory check
        {{"enable_memory_context_check_debug",
            PGC_POSTMASTER,
            NODE_ALL,
            RESOURCES_MEM,
            gettext_noop("check the memory context info on debug mode."),
            NULL},
            &g_instance.attr.attr_memory.enable_memory_context_check_debug,
            false,
            NULL,
            NULL,
            NULL},
#endif

        /* End-of-list marker */
        {{NULL,
            (GucContext)0,
            (GucNodeType)0,
            (config_group)0,
            NULL,
            NULL},
            NULL,
            false,
            NULL,
            NULL,
            NULL}
    };

    Size bytes = sizeof(localConfigureNamesBool);
    u_sess->utils_cxt.ConfigureNamesBool[GUC_ATTR_MEMORY] =
        (struct config_bool*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesBool[GUC_ATTR_MEMORY], bytes, localConfigureNamesBool, bytes);
    securec_check_ss(rc, "\0", "\0");
}

static void InitMemoryConfigureNamesInt()
{
    struct config_int localConfigureNamesInt[] = {
        {{"memorypool_size",
            PGC_POSTMASTER,
            NODE_ALL,
            RESOURCES_MEM,
            gettext_noop("Sets the number of memory pool used by the server."),
            NULL,
            GUC_UNIT_KB},
            &g_instance.attr.attr_memory.memorypool_size,
            1024 * 512,
            128 * 1024,
            INT_MAX / 2,
            NULL,
            NULL,
            NULL},
        {{"max_process_memory",
            PGC_POSTMASTER,
            NODE_ALL,
            RESOURCES_MEM,
            gettext_noop("Sets the maximum number of memory used by the process."),
            NULL,
            GUC_UNIT_KB},
            &g_instance.attr.attr_memory.max_process_memory,
            12 * 1024 * 1024,
            2 * 1024 * 1024,
            INT_MAX,
            NULL,
            NULL,
            NULL},
        {{"local_syscache_threshold",
            PGC_SIGHUP,
            NODE_ALL,
            RESOURCES_MEM,
            gettext_noop("Sets the maximum threshold for cleaning cache."),
            NULL,
            GUC_UNIT_KB},
            &u_sess->attr.attr_memory.local_syscache_threshold,
            256 * 1024,
            1 * 1024,
            512 * 1024,
            check_syscache_threshold_gpc,
            NULL,
            NULL},
        {{"global_syscache_threshold",
            PGC_SIGHUP,
            NODE_ALL,
            RESOURCES_MEM,
            gettext_noop("Sets the maximum threshold for cleaning global syscache."),
            NULL,
            GUC_UNIT_KB},
            &g_instance.attr.attr_memory.global_syscache_threshold,
            160 * 1024,
            16 * 1024,
            1024 * 1024 * 1024,
            NULL,
            NULL,
            NULL},
        {{"work_mem",
            PGC_USERSET,
            NODE_ALL,
            RESOURCES_MEM,
            gettext_noop("Sets the maximum memory to be used for query workspaces."),
            gettext_noop("This much memory can be used by each internal "
                         "sort operation and hash table before switching to "
                         "temporary disk files."),
            GUC_UNIT_KB},
            &u_sess->attr.attr_memory.work_mem,
            64 * 1024,
            64,
            MAX_KILOBYTES,
            NULL,
            NULL,
            NULL},
        {{"maintenance_work_mem",
            PGC_USERSET,
            NODE_ALL,
            RESOURCES_MEM,
            gettext_noop("Sets the maximum memory to be used for maintenance operations."),
            gettext_noop("This includes operations such as VACUUM and CREATE INDEX."),
            GUC_UNIT_KB},
            &u_sess->attr.attr_memory.maintenance_work_mem,
            16384,
            1024,
            MAX_KILOBYTES,
            NULL,
            NULL,
            NULL},

        /* End-of-list marker */
        {{NULL,
            (GucContext)0,
            (GucNodeType)0,
            (config_group)0,
            NULL,
            NULL},
            NULL,
            0,
            0,
            0,
            NULL,
            NULL,
            NULL}
    };

    Size bytes = sizeof(localConfigureNamesInt);
    u_sess->utils_cxt.ConfigureNamesInt[GUC_ATTR_MEMORY] =
        (struct config_int*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesInt[GUC_ATTR_MEMORY], bytes,
        localConfigureNamesInt, bytes);
    securec_check_ss(rc, "\0", "\0");
}

static void InitMemoryConfigureNamesReal()
{
    struct config_real localConfigureNamesReal[] = {

        /* End-of-list marker */
        {{NULL,
            (GucContext)0,
            (GucNodeType)0,
            (config_group)0,
            NULL,
            NULL},
            NULL,
            0.0,
            0.0,
            0.0,
            NULL,
            NULL,
            NULL}
    };

    Size bytes = sizeof(localConfigureNamesReal);
    u_sess->utils_cxt.ConfigureNamesReal[GUC_ATTR_MEMORY] =
        (struct config_real*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesReal[GUC_ATTR_MEMORY], bytes,
        localConfigureNamesReal, bytes);
    securec_check_ss(rc, "\0", "\0");
}

static void InitMemoryConfigureNamesInt64()
{
    struct config_int64 localConfigureNamesInt64[] = {

        /* End-of-list marker */
        {{NULL,
            (GucContext)0,
            (GucNodeType)0,
            (config_group)0,
            NULL,
            NULL},
            NULL,
            0,
            0,
            0,
            NULL,
            NULL,
            NULL}
    };

    Size bytes = sizeof(localConfigureNamesInt64);
    u_sess->utils_cxt.ConfigureNamesInt64[GUC_ATTR_MEMORY] =
        (struct config_int64*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesInt64[GUC_ATTR_MEMORY], bytes,
        localConfigureNamesInt64, bytes);
    securec_check_ss(rc, "\0", "\0");
}

static void InitMemoryConfigureNamesString()
{
    struct config_string localConfigureNamesString[] = {
        {{"memory_detail_tracking",
            PGC_USERSET,
            NODE_ALL,
            RESOURCES_MEM,
            gettext_noop("Sets the operator name and peak size for triggering the memory logging in that time."),
            NULL,
            GUC_IS_NAME | GUC_REPORT | GUC_NOT_IN_SAMPLE},
            &u_sess->attr.attr_memory.memory_detail_tracking,
            "",
            check_memory_detail_tracking,
            assign_memory_detail_tracking,
            show_memory_detail_tracking},
        /* uncontrolled_memory_context is a white list of MemoryContext allocation. */
        {{"uncontrolled_memory_context",
            PGC_USERSET,
            NODE_ALL,
            RESOURCES_MEM,
            gettext_noop("Sets the white list of MemoryContext allocation."),
            NULL},
            &u_sess->attr.attr_memory.uncontrolled_memory_context,
            "",
            check_uncontrolled_memory_context,
            assign_uncontrolled_memory_context,
            show_uncontrolled_memory_context},

        {{"resilience_memory_reject_percent",
            PGC_SIGHUP,
            NODE_ALL,
            RESOURCES_MEM,
            gettext_noop("Sets the memory percent."),
            NULL,
            GUC_LIST_INPUT},
            &u_sess->attr.attr_memory.memory_reset_percent_item,
            "0,0",
            CheckMemoryResetPercent,
            AssignMemoryResetPercent,
            NULL},

        {{NULL,
            (GucContext)0,
            (GucNodeType)0,
            (config_group)0,
            NULL,
            NULL},
            NULL,
            NULL,
            NULL,
            NULL,
            NULL}
    };

    Size bytes = sizeof(localConfigureNamesString);
    u_sess->utils_cxt.ConfigureNamesString[GUC_ATTR_MEMORY] =
        (struct config_string*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesString[GUC_ATTR_MEMORY], bytes,
        localConfigureNamesString, bytes);
    securec_check_ss(rc, "\0", "\0");
}

static void InitMemoryConfigureNamesEnum()
{
    struct config_enum localConfigureNamesEnum[] = {
        {{"memory_tracking_mode",
            PGC_USERSET,
            NODE_ALL,
            RESOURCES_MEM,
            gettext_noop("Choose which style to track the memory usage."),
            NULL},
            &u_sess->attr.attr_memory.memory_tracking_mode,
            MEMORY_TRACKING_NONE,
            memory_tracking_option,
            NULL,
            NULL,
            NULL},
        {{"memory_trace_level",
            PGC_SIGHUP,
            NODE_ALL,
            RESOURCES_MEM,
            gettext_noop("Sets the used memory level for memory snapshot."),
            NULL},
            &u_sess->attr.attr_memory.memory_trace_level,
            MEMORY_TRACE_LEVEL1,
            memory_trace_level,
            NULL,
            NULL,
            NULL},

        /* End-of-list marker */
        {{NULL,
            (GucContext)0,
            (GucNodeType)0,
            (config_group)0,
            NULL,
            NULL},
            NULL,
            0,
            NULL,
            NULL,
            NULL,
            NULL}
    };

    Size bytes = sizeof(localConfigureNamesEnum);
    u_sess->utils_cxt.ConfigureNamesEnum[GUC_ATTR_MEMORY] =
        (struct config_enum*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesEnum[GUC_ATTR_MEMORY], bytes,
        localConfigureNamesEnum, bytes);
    securec_check_ss(rc, "\0", "\0");
}
/* ******* end of options list ******* */

/* function: check_memory_detail_tracking
 * description: to verify if the input value is correct.
 * arguments:
 *     newval: the input string for checking
 *     extra: the extra information
 *     source: the GUC Source information
 * return value: ture of false
 *
 * Notes that: this GUC is only used on DEBUG version;
 *             the format should be input as "seqnum:plannodeid".
 *
 */
static bool check_memory_detail_tracking(char** newval, void** extra, GucSource source)
{
#ifdef MEMORY_CONTEXT_CHECKING
    /* Only allow clean ASCII chars in the string of operator memory log */
    char *p, *q;

    for (p = *newval; *p; p++) {
        if (*p < 32 || *p > 126)
            *p = '?';
    }

    p = *newval;
    if (p && *p != '\0' && (NULL == (q = strchr(p, ':')) || (isdigit(*(q + 1)) == 0)))
        return false;

    return true;
#else

#ifndef FASTCHECK
    char* p = *newval;

    if (p && *p != '\0')
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("unsupported to set memory_detail_tracking value under release version.")));
#endif
    return true;
#endif
}

/* function: check_u_sess->attr.attr_memory.memory_detail_tracking
 * description: to parse the GUC value and assign to the global variable
 * arguments: standard GUC arguments
 */
static void assign_memory_detail_tracking(const char* newval, void* extra)
{
#ifdef MEMORY_CONTEXT_CHECKING
    MemoryTrackingParseGUC(newval);
#endif
}

/* function: show_memory_detail_tracking
 * description: to show the information
 * arguments: standard GUC arguments
 */
static const char* show_memory_detail_tracking(void)
{
    char* buf = t_thrd.buf_cxt.show_memory_detail_tracking_buf;

#ifdef MEMORY_CONTEXT_CHECKING
    int size = sizeof(t_thrd.buf_cxt.show_memory_detail_tracking_buf);
    errno_t rc = snprintf_s(buf,
        size,
        size - 1,
        "Memory Context Sequent Count: %d, Plan Nodeid: %d",
        t_thrd.utils_cxt.memory_track_sequent_count,
        t_thrd.utils_cxt.memory_track_plan_nodeid);
    securec_check_ss(rc, "\0", "\0");
#endif
    return buf;
}

static const char* show_enable_memory_limit(void)
{
    char* buf = t_thrd.buf_cxt.show_enable_memory_limit_buf;
    int size = sizeof(t_thrd.buf_cxt.show_enable_memory_limit_buf);
    int rcs;

    if (t_thrd.utils_cxt.gs_mp_inited) {
        rcs = snprintf_s(buf, size, size - 1, "%s", "on");
        securec_check_ss(rcs, "\0", "\0");
    } else {
        rcs = snprintf_s(buf, size, size - 1, "%s", "off");
        securec_check_ss(rcs, "\0", "\0");
    }

    return buf;
}

static bool check_syscache_threshold_gpc(int* newval, void** extra, GucSource source)
{
    /* in case local_syscache_threshold is too low cause gpc does not take effect, we set local_syscache_threshold
       at least 16MB if gpc is on. */
    if (g_instance.attr.attr_common.enable_global_plancache && *newval < 16 * 1024) {
        ereport(WARNING, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("cannot set local_syscache_threshold to %dMB in case gpc is on but not valid.",
                   *newval / 1024)));
        return false;
    }
    return true;
}

/** start dealing with uncontrolled_memory_context GUC parameter **/
void free_memory_context_list(memory_context_list* head_node)
{
    memory_context_list* last = NULL;
    while (head_node != NULL) {
        last = head_node;
        head_node = head_node->next;
        pfree(last);
        last = NULL;
    }
}

/* This function splits a string into substrings by character ','.
 * And these substrings will be appended to a memory_context_list.
 * Referenced by GUC uncontrolled_memory_context.
 */
memory_context_list* split_string_into_list(const char* source)
{
    errno_t rc = EOK;
    int string_length = strlen(source) + 1;
    int first_ch_length = 0;
    char* str = static_cast<char*>(MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), string_length));
    rc = strcpy_s(str, string_length, source);
    securec_check_errno(rc, pfree(str), NULL);

    memory_context_list* head_node = static_cast<memory_context_list*>(MemoryContextAlloc(
        SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), sizeof(memory_context_list)));
    memory_context_list* current_node = head_node;
    current_node->next = NULL;
    current_node->value = NULL;
    char* first_ch = str;
    size_t len = 0;

    for (int i = 0; i < string_length; i++, len++) {
        if (str[i] == ',' || str[i] == '\0') {
            if (NULL != current_node->value) {
                /* append a new node to list */
                current_node->next = static_cast<memory_context_list*>(MemoryContextAlloc(
                    SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), sizeof(memory_context_list)));
                current_node = current_node->next;
                current_node->next = NULL;
                current_node->value = NULL;
            }

            first_ch[len] = '\0';  // replace ',' with '\0'
            first_ch_length = strlen(first_ch) + 1;

            // 'strlen(first_ch) + 1' means including '\0'.
            current_node->value = static_cast<char*>(MemoryContextAlloc(
                SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), first_ch_length));
            rc = strcpy_s(current_node->value, first_ch_length, first_ch);
            securec_check_errno(rc, pfree(str); free_memory_context_list(head_node), NULL);

            /* reset variables so that split next string.
             * 'i++' and 'first_ch += len + 1' means skipping '\0'
             */
            first_ch += len + 1;
            i++;
            len = 0;
        }
    }

    return head_node;
}

static bool check_uncontrolled_memory_context(char** newval, void** extra, GucSource source)
{
    for (char* p = *newval; *p; p++) {
        if (*p < 32 || *p > 126) {
            ereport(ERROR,
                (errcode(ERRCODE_UNTRANSLATABLE_CHARACTER), errmsg("The text entered contains illegal characters!")));
            return false;
        }
    }
    return true;
}

static void assign_uncontrolled_memory_context(const char* newval, void* extra)
{
    if (u_sess->utils_cxt.memory_context_limited_white_list) {
        free_memory_context_list(u_sess->utils_cxt.memory_context_limited_white_list);
        u_sess->utils_cxt.memory_context_limited_white_list = NULL;
    }
    u_sess->utils_cxt.memory_context_limited_white_list = split_string_into_list(newval);
}


/* show the guc parameter -- uncontrolled_memory_context */
static const char* show_uncontrolled_memory_context(void)
{
    const size_t length = 1024;
    char* buff = static_cast<char*>(MemoryContextAllocZero(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), length));
    errno_t rc = EOK;
    size_t len;

    rc = strcpy_s(buff, length, "MemoryContext white list:\n");
    securec_check_errno(rc, buff[length - 1] = '\0', buff);  // truncate string and return it.

    for (memory_context_list* iter = u_sess->utils_cxt.memory_context_limited_white_list; iter && iter->value;
         iter = iter->next) {
        rc = strcat_s(buff, length, iter->value);
        securec_check_errno(rc, buff[length - 1] = '\0', buff);

        len = strlen(buff);
        if (len < (length - 1)) {
            buff[len] = '\n';
            buff[len + 1] = '\0';  // replace '\0' with '\n\0'
        }
    }
    return buff;
}

/** end of dealing with uncontrolled_memory_context GUC parameter **/
