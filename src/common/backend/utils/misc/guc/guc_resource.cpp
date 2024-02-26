/* --------------------------------------------------------------------
 * guc_resource.cpp
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
 * src/backend/utils/misc/guc/guc_resource.cpp
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
#include "utils/guc_resource.h"

const int NO_LIMIT_SIZE = -1;

static void assign_statistics_memory(int newval, void* extra);
static bool check_cgroup_name(char** newval, void** extra, GucSource source);
static void assign_cgroup_name(const char* newval, void* extra);
static const char* show_cgroup_name(void);
static bool check_statistics_memory_limit(int* newval, void** extra, GucSource source);
static void assign_active_statements(int newval, void* extra);
static bool check_history_memory_limit(int* newval, void** extra, GucSource source);
static void assign_history_memory(int newval, void* extra);
static bool check_use_workload_manager(bool* newval, void** extra, GucSource source);
static void assign_use_workload_manager(const bool newval, void* extra);
static void assign_collect_timer(int newval, void* extra);
static bool check_query_band_name(char** newval, void** extra, GucSource source);
static bool check_session_respool(char** newval, void** extra, GucSource source);
static void assign_session_respool(const char* newval, void* extra);
static const char* show_session_respool(void);
static bool check_auto_explain_level(int* newval, void** extra, GucSource source);
static const char* show_enable_dynamic_workload(void);
static void assign_control_group(const bool newval, void* extra);
static void assign_enable_cgroup_switch(bool newval, void* extra);
static void assign_memory_quota(int newval, void* extra);

static void InitResourceConfigureNamesBool();
static void InitResourceConfigureNamesInt();
static void InitResourceConfigureNamesInt64();
static void InitResourceConfigureNamesReal();
static void InitResourceConfigureNamesString();
static void InitResourceConfigureNamesEnum();

static const struct config_enum_entry resource_track_option[] = {
    {"none", RESOURCE_TRACK_NONE, false},
    {"query", RESOURCE_TRACK_QUERY, false},
    {"operator", RESOURCE_TRACK_OPERATOR, false},
    {NULL, 0, false}
};

static const struct config_enum_entry io_priority_level_options[] = {
    {"None", IOPRIORITY_NONE, false},
    {"Low", IOPRIORITY_LOW, false},
    {"Medium", IOPRIORITY_MEDIUM, false},
    {"High", IOPRIORITY_HIGH, false},
    {NULL, 0, false}
};

static const struct config_enum_entry auto_explain_level_options[] = {
    {"log", LOG, false},
    {"notice", NOTICE, false},
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
void InitResourceConfigureNames()
{
    InitResourceConfigureNamesBool();
    InitResourceConfigureNamesInt();
    InitResourceConfigureNamesInt64();
    InitResourceConfigureNamesReal();
    InitResourceConfigureNamesString();
    InitResourceConfigureNamesEnum();

    return;
}

static void InitResourceConfigureNamesBool()
{
    struct config_bool localConfigureNamesBool[] = {
#ifdef BTREE_BUILD_STATS
        {{"log_btree_build_stats",
            PGC_SUSET,
            NODE_ALL,
            DEVELOPER_OPTIONS,
            gettext_noop("Logs system resource usage statistics on various B-tree operations."),
            NULL,
            GUC_NOT_IN_SAMPLE},
            &u_sess->attr.attr_resource.log_btree_build_stats,
            false,
            NULL,
            NULL,
            NULL},
#endif
#ifdef TRACE_SYNCSCAN
        /* this is undocumented because not exposed in a standard build */
        {{"trace_syncscan",
            PGC_USERSET,
            NODE_ALL,
            DEVELOPER_OPTIONS,
            gettext_noop("Generates debugging output for synchronized scanning."),
            NULL,
            GUC_NOT_IN_SAMPLE},
            &u_sess->attr.attr_resource.trace_syncscan,
            false,
            NULL,
            NULL,
            NULL},
#endif
        {{"use_workload_manager",
            PGC_SIGHUP,
            NODE_ALL,
            RESOURCES_WORKLOAD,
            gettext_noop("Enables workload manager in the system. "),
            NULL},
            &u_sess->attr.attr_resource.use_workload_manager,
            true,
            check_use_workload_manager,
            assign_use_workload_manager,
            NULL},
        {{"enable_resource_track",
            PGC_SIGHUP,
            NODE_ALL,
            RESOURCES_WORKLOAD,
            gettext_noop("enable resources tracking and recording functionality in the system. "),
            NULL},
            &u_sess->attr.attr_resource.enable_resource_track,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_resource_record",
            PGC_SIGHUP,
            NODE_ALL,
            RESOURCES_WORKLOAD,
            gettext_noop("enable insert the session info into the user table. "),
            NULL},
            &u_sess->attr.attr_resource.enable_resource_record,
            false,
            NULL,
            NULL,
            NULL},
        {{"enable_user_metric_persistent",
            PGC_SIGHUP,
            NODE_ALL,
            RESOURCES_WORKLOAD,
            gettext_noop("enable user resource info persistent function."),
            NULL},
            &u_sess->attr.attr_resource.enable_user_metric_persistent,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_instance_metric_persistent",
            PGC_SIGHUP,
            NODE_ALL,
            RESOURCES_WORKLOAD,
            gettext_noop("enable instance resource info persistent function."),
            NULL},
            &u_sess->attr.attr_resource.enable_instance_metric_persistent,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_logical_io_statistics",
            PGC_SIGHUP,
            NODE_ALL,
            RESOURCES_WORKLOAD,
            gettext_noop("enable logical io statistics function."),
            NULL},
            &u_sess->attr.attr_resource.enable_logical_io_statistics,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_auto_explain",
            PGC_USERSET,
            NODE_ALL,
            RESOURCES_WORKLOAD,
            gettext_noop("enable auto explain plans. "),
            NULL},
            &u_sess->attr.attr_resource.enable_auto_explain,
            false,
            NULL,
            NULL,
            NULL},
        {{"enable_hotkeys_collection",
            PGC_SUSET,
            NODE_DISTRIBUTE,
            RESOURCES_WORKLOAD,
            gettext_noop("enable hotkey collection functionality in the system. "),
            NULL},
            &u_sess->attr.attr_resource.enable_hotkeys_collection,
            false,
            NULL,
            NULL,
            NULL},
        {{"bypass_workload_manager",
            PGC_SIGHUP,
            NODE_DISTRIBUTE,
            RESOURCES_WORKLOAD,
            gettext_noop("bypass workload manager in the system. "),
            NULL},
            &u_sess->attr.attr_resource.bypass_workload_manager,
            false,
            NULL,
            NULL,
            NULL},
        {{"enable_dynamic_workload",
            PGC_POSTMASTER,
            NODE_DISTRIBUTE,
            RESOURCES_WORKLOAD,
            gettext_noop("Enables dynamic workload manager in the system. "),
            NULL},
            &g_instance.attr.attr_resource.enable_dynamic_workload,
            true,
            NULL,
            NULL,
            show_enable_dynamic_workload},
        {{"enable_control_group",
            PGC_SIGHUP,
            NODE_DISTRIBUTE,
            RESOURCES_WORKLOAD,
            gettext_noop("Enables control group in the system. "),
            NULL},
            &u_sess->attr.attr_resource.enable_control_group,
            true,
            NULL,
            assign_control_group,
            NULL},
        {{"enable_backend_control",
            PGC_POSTMASTER,
            NODE_DISTRIBUTE,
            RESOURCES_WORKLOAD,
            gettext_noop("Enables workload manager to control backend thread. "),
            NULL},
            &g_instance.attr.attr_resource.enable_backend_control,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_vacuum_control",
            PGC_POSTMASTER,
            NODE_DISTRIBUTE,
            RESOURCES_WORKLOAD,
            gettext_noop("Enables workload manager to control vacuum thread. "),
            NULL},
            &g_instance.attr.attr_resource.enable_vacuum_control,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_cgroup_switch",
            PGC_USERSET,
            NODE_DISTRIBUTE,
            RESOURCES_WORKLOAD,
            gettext_noop("Enables cgroup switch to TopWD. "),
            NULL},
            &u_sess->attr.attr_resource.enable_cgroup_switch,
            false,
            NULL,
            assign_enable_cgroup_switch,
            NULL},
        {{"enable_verify_active_statements",
            PGC_SIGHUP,
            NODE_DISTRIBUTE,
            RESOURCES_WORKLOAD,
            gettext_noop("enable verify active statements. "),
            NULL},
            &u_sess->attr.attr_resource.enable_verify_statements,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_force_memory_control",
            PGC_SIGHUP,
            NODE_DISTRIBUTE,
            RESOURCES_WORKLOAD,
            gettext_noop("enable the resource pool memory control regardless of simple query. "),
            NULL},
            &u_sess->attr.attr_resource.enable_force_memory_control,
            false,
            NULL,
            NULL,
            NULL},
        {{"enable_dywlm_adjust",
            PGC_SIGHUP,
            NODE_DISTRIBUTE,
            RESOURCES_WORKLOAD,
            gettext_noop("enable dynamic adjust inaccurate resource value. "),
            NULL},
            &u_sess->attr.attr_resource.enable_dywlm_adjust,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_reaper_backend",
            PGC_SIGHUP,
            NODE_DISTRIBUTE,
            RESOURCES_WORKLOAD,
            gettext_noop("enable reaper backend threads in a signal thread, not postmaster thread. "),
            NULL},
            &u_sess->attr.attr_resource.enable_reaper_backend,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_perm_space",
            PGC_POSTMASTER,
            NODE_DISTRIBUTE,
            RESOURCES_WORKLOAD,
            gettext_noop("enable perm space functionality in the system. "),
            NULL},
            &g_instance.attr.attr_resource.enable_perm_space,
            true,
            NULL,
            NULL,
            NULL},
        {{"enable_transaction_parctl",
            PGC_USERSET,
            NODE_DISTRIBUTE,
            RESOURCES_WORKLOAD,
            gettext_noop("consider transaction or stored procedure as simple or not. "),
            NULL},
            &u_sess->attr.attr_resource.enable_transaction_parctl,
            true,
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
            false,
            NULL,
            NULL,
            NULL}
    };

    Size bytes = sizeof(localConfigureNamesBool);
    u_sess->utils_cxt.ConfigureNamesBool[GUC_ATTR_RESOURCE] =
        (struct config_bool*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesBool[GUC_ATTR_RESOURCE], bytes,
        localConfigureNamesBool, bytes);
    securec_check_ss(rc, "\0", "\0");
}

static void InitResourceConfigureNamesInt()
{
    struct config_int localConfigureNamesInt[] = {
        {{"session_statistics_memory",
            PGC_SIGHUP,
            NODE_ALL,
            RESOURCES_MEM,
            gettext_noop("Sets the maximum number of session statistics memory used by the process."),
            NULL,
            GUC_UNIT_KB},
            &u_sess->attr.attr_resource.session_statistics_memory,
            5 * 1024,
            5 * 1024,
            INT_MAX,
            check_statistics_memory_limit,
            assign_statistics_memory,
            NULL},
        {{"session_history_memory",
            PGC_SIGHUP,
            NODE_ALL,
            RESOURCES_MEM,
            gettext_noop("Sets the maximum number of session history memory used by the process."),
            NULL,
            GUC_UNIT_KB},
            &u_sess->attr.attr_resource.session_history_memory,
            10 * 1024,
            10 * 1024,
            INT_MAX,
            check_history_memory_limit,
            assign_history_memory,
            NULL},
        {{"resource_track_cost",
            PGC_USERSET,
            NODE_ALL,
            RESOURCES_WORKLOAD,
            gettext_noop("Sets the minimum cost to do resource track."),
            gettext_noop("This value is set to 1000000 as default."),
            },
            &u_sess->attr.attr_resource.resource_track_cost,
            100000,
            -1,
            INT_MAX,
            NULL,
            NULL,
            NULL},
        {{"topsql_retention_time",
            PGC_SIGHUP,
            NODE_ALL,
            RESOURCES_WORKLOAD,
            gettext_noop("the retention time of TopSql"),
            gettext_noop("This value is set to 7 as default. "),
            },
            &u_sess->attr.attr_resource.topsql_retention_time,
            0,
            0,
            3650,
            NULL,
            NULL,
            NULL},
        {{"unique_sql_retention_time",
            PGC_SIGHUP,
            NODE_ALL,
            RESOURCES_WORKLOAD,
            gettext_noop("the retention time of unique sql text"),
            gettext_noop("This value is set to 30 min as default. "),
            GUC_UNIT_MIN},
            &u_sess->attr.attr_resource.unique_sql_retention_time,
            30,
            1,
            3650,
            NULL,
            NULL,
            NULL},
        {{"resource_track_duration",
            PGC_USERSET,
            NODE_ALL,
            RESOURCES_WORKLOAD,
            gettext_noop("Sets the minimum duration to record history session info."),
            gettext_noop("This value is set to 60 secs as default."),
            GUC_UNIT_S},
            &u_sess->attr.attr_resource.resource_track_duration,
            60,
            0,
            INT_MAX,
            NULL,
            NULL,
            NULL},

        {{"user_metric_retention_time",
            PGC_SIGHUP,
            NODE_ALL,
            RESOURCES_WORKLOAD,
            gettext_noop("the user resource info retention time."),
            gettext_noop("This value is set to 7 as default. "),
            },
            &u_sess->attr.attr_resource.user_metric_retention_time,
            7,
            0,
            3650,
            NULL,
            NULL,
            NULL},

        {{"instance_metric_retention_time",
            PGC_SIGHUP,
            NODE_ALL,
            RESOURCES_WORKLOAD,
            gettext_noop("the instance resource info retention time."),
            gettext_noop("This value is set to 7 as default. "),
            },
            &u_sess->attr.attr_resource.instance_metric_retention_time,
            7,
            0,
            3650,
            NULL,
            NULL,
            NULL},

        {{"cpu_collect_timer",
            PGC_SIGHUP,
            NODE_ALL,
            RESOURCES_WORKLOAD,
            gettext_noop("Sets the maximum cpu collect time."),
            gettext_noop("This value is set to 30 secs as default."),
            },
            &u_sess->attr.attr_resource.cpu_collect_timer,
            30,
            1,
            INT_MAX,
            NULL,
            assign_collect_timer,
            NULL},
        {{"io_limits",
            PGC_USERSET,
            NODE_ALL,
            RESOURCES_WORKLOAD,
            gettext_noop("Sets io_limit  for each query."),
            gettext_noop("This value is set to 0 as default. "),
            },
            &u_sess->attr.attr_resource.iops_limits,
            0,
            0,
            INT_MAX / 2,
            NULL,
            NULL,
            NULL},
        {{"io_control_unit",
            PGC_SIGHUP,
            NODE_ALL,
            RESOURCES_WORKLOAD,
            gettext_noop("Sets the io control unit for reading or writing row tuple."),
            gettext_noop("This value is set to 6000 as default. "),
            },
            &u_sess->attr.attr_resource.io_control_unit,
            IOCONTROL_UNIT,
            1000,
            1000000,
            NULL,
            NULL,
            NULL},
        {{"autovacuum_io_limits",
            PGC_SIGHUP,
            NODE_ALL,
            RESOURCES_WORKLOAD,
            gettext_noop("Sets io_limit for autovacum."),
            gettext_noop("This value is set to 0 as default. "),
            },
            &u_sess->attr.attr_resource.autovac_iops_limits,
            -1,
            -1,
            INT_MAX / 2,
            NULL,
            NULL,
            NULL},
        {{"transaction_pending_time",
            PGC_USERSET,
            NODE_ALL,
            RESOURCES_WORKLOAD,
            gettext_noop("Sets pend_time for transaction or Stored Procedure."),
            gettext_noop("This value is set to 60 as default. "),
            },
            &u_sess->attr.attr_resource.transaction_pending_time,
            0,
            -1,
            INT_MAX / 2,
            NULL,
            NULL,
            NULL},
        {{"sql_use_spacelimit",
            PGC_USERSET,
            NODE_ALL,
            RESOURCES_DISK,
            gettext_noop("Limit the single sql used space on a single DN."),
            gettext_noop("-1 means no limit."),
            GUC_UNIT_KB},
            &u_sess->attr.attr_resource.sqlUseSpaceLimit,
            NO_LIMIT_SIZE,
            NO_LIMIT_SIZE,
            INT_MAX,
            NULL,
            NULL,
            NULL},
        {{"max_active_statements",
            PGC_SIGHUP,
            NODE_DISTRIBUTE,
            RESOURCES_WORKLOAD,
            gettext_noop("Sets the maximum global active statements to be used for query."),
            gettext_noop("This value is set to -1 as default. "),
            },
            &u_sess->attr.attr_resource.max_active_statements,
            60,
            -1,
            INT_MAX,
            NULL,
            assign_active_statements,
            NULL},
        {{"dynamic_memory_quota",
            PGC_SIGHUP,
            NODE_DISTRIBUTE,
            RESOURCES_WORKLOAD,
            gettext_noop("Sets the maximum memory quota for dynamic workload."),
            gettext_noop("This value is set to 80 as default. "),
            },
            &u_sess->attr.attr_resource.dynamic_memory_quota,
            80,
            1,
            100,
            NULL,
            assign_memory_quota,
            NULL},
        {{"memory_fault_percent",
            PGC_SIGHUP,
            NODE_ALL,
            RESOURCES_WORKLOAD,
            gettext_noop("Sets the percent when memory allocation is failed."),
            gettext_noop("This value is set to 0 as default and is used in debug version. "),
            },
            &u_sess->attr.attr_resource.memory_fault_percent,
            0,
            0,
            MAX_MEMORY_FAULT_PERCENT,
            NULL,
            NULL,
            NULL},
        {{"parctl_min_cost",
            PGC_SIGHUP,
            NODE_DISTRIBUTE,
            RESOURCES_WORKLOAD,
            gettext_noop("Sets the minimum cost to do parallel control."),
            gettext_noop("This value is set to 1000000 as default. "),
            },
            &u_sess->attr.attr_resource.parctl_min_cost,
            100000,
            -1,
            INT_MAX,
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
    u_sess->utils_cxt.ConfigureNamesInt[GUC_ATTR_RESOURCE] =
        (struct config_int*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesInt[GUC_ATTR_RESOURCE], bytes,
        localConfigureNamesInt, bytes);
    securec_check_ss(rc, "\0", "\0");
}

static void InitResourceConfigureNamesReal()
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
    u_sess->utils_cxt.ConfigureNamesReal[GUC_ATTR_RESOURCE] =
        (struct config_real*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesReal[GUC_ATTR_RESOURCE], bytes,
        localConfigureNamesReal, bytes);
    securec_check_ss(rc, "\0", "\0");
}

static void InitResourceConfigureNamesInt64()
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
    u_sess->utils_cxt.ConfigureNamesInt64[GUC_ATTR_RESOURCE] =
        (struct config_int64*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesInt64[GUC_ATTR_RESOURCE], bytes,
        localConfigureNamesInt64, bytes);
    securec_check_ss(rc, "\0", "\0");
}

static void InitResourceConfigureNamesString()
{
    struct config_string localConfigureNamesString[] = {
        {{"cgroup_name",
            PGC_USERSET,
            NODE_ALL,
            RESOURCES_WORKLOAD,
            gettext_noop("Sets the cgroup name to control the queries resource."),
            NULL,
            GUC_IS_NAME | GUC_REPORT | GUC_NOT_IN_SAMPLE},
            &u_sess->attr.attr_resource.cgroup_name,
            "",
            check_cgroup_name,
            assign_cgroup_name,
            show_cgroup_name},
        {{"query_band",
            PGC_USERSET,
            NODE_ALL,
            RESOURCES_WORKLOAD,
            gettext_noop("Sets query band."),
            NULL,
            GUC_REPORT | GUC_NOT_IN_SAMPLE},
            &u_sess->attr.attr_resource.query_band,
            "",
            check_query_band_name,
            NULL,
            NULL},

        {{"session_respool",
            PGC_USERSET,
            NODE_ALL,
            RESOURCES_WORKLOAD,
            gettext_noop("Sets the session resource pool to control the queries resource."),
            NULL,
            GUC_IS_NAME | GUC_REPORT | GUC_NOT_IN_SAMPLE},
            &u_sess->attr.attr_resource.session_resource_pool,
            "invalid_pool",
            check_session_respool,
            assign_session_respool,
            show_session_respool},

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
    u_sess->utils_cxt.ConfigureNamesString[GUC_ATTR_RESOURCE] =
        (struct config_string*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesString[GUC_ATTR_RESOURCE], bytes,
        localConfigureNamesString, bytes);
    securec_check_ss(rc, "\0", "\0");
}

static void InitResourceConfigureNamesEnum()
{
    struct config_enum localConfigureNamesEnum[] = {
        {{"resource_track_level",
            PGC_USERSET,
            NODE_ALL,
            RESOURCES_WORKLOAD,
            gettext_noop("Choose which level info to be collected. "),
            NULL},
            &u_sess->attr.attr_resource.resource_track_level,
            RESOURCE_TRACK_QUERY,
            resource_track_option,
            NULL,
            NULL,
            NULL},
        {{"io_priority",
            PGC_USERSET,
            NODE_ALL,
            RESOURCES_WORKLOAD,
            gettext_noop("Sets the IO priority for queries."),
            NULL},
            &u_sess->attr.attr_resource.io_priority,
            IOPRIORITY_NONE,
            io_priority_level_options,
            NULL,
            NULL,
            NULL},
        {{"auto_explain_level",
            PGC_USERSET,
            NODE_ALL,
            RESOURCES_WORKLOAD,
            gettext_noop("auto_explain_level."),
            gettext_noop("auto_explain_level")},
            &u_sess->attr.attr_resource.auto_explain_level,
            LOG,
            auto_explain_level_options,
            check_auto_explain_level,
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
    u_sess->utils_cxt.ConfigureNamesEnum[GUC_ATTR_RESOURCE] =
        (struct config_enum*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
    errno_t rc = memcpy_s(u_sess->utils_cxt.ConfigureNamesEnum[GUC_ATTR_RESOURCE], bytes,
        localConfigureNamesEnum, bytes);
    securec_check_ss(rc, "\0", "\0");
}

/* ******* end of options list ******* */


/*
 * Brief    : assignthe value of statistics memory to g_statManager.max_collectinfo_num
 */
static void assign_statistics_memory(int newval, void* extra)
{
    if (currentGucContext == PGC_SIGHUP) {
        u_sess->attr.attr_resource.session_statistics_memory = newval;
        if (IS_PGXC_COORDINATOR)
            g_instance.wlm_cxt->stat_manager.max_collectinfo_num = newval * 1024 / sizeof(WLMDNodeInfo);

        if (IS_PGXC_DATANODE)
            g_instance.wlm_cxt->stat_manager.max_collectinfo_num = newval * 1024 / sizeof(WLMDNRealTimeInfoDetail);

        g_instance.wlm_cxt->stat_manager.max_iostatinfo_num = newval * 1024 / sizeof(WLMDNodeInfo);
    }
}

static bool check_cgroup_name(char** newval, void** extra, GucSource source)
{
    /* Only allow clean ASCII chars in the control group name */
    char* p = NULL;

    for (p = *newval; *p; p++) {
        if (*p < 32 || *p > 126) {
            *p = '?';
        }
    }

    p = *newval;

    if (StringIsValid(p) && IS_SERVICE_NODE && t_thrd.shemem_ptr_cxt.MyBEEntry &&
        (currentGucContext == PGC_SUSET || currentGucContext == PGC_USERSET)) {
        if (g_instance.wlm_cxt->gscgroup_init_done == 0)
            ereport(ERROR,
                (errcode(ERRCODE_SYSTEM_ERROR),
                    errmsg("Failed to initialize Cgroup. "
                           "Please check if workload manager is enabled "
                           "and Cgroups have been created!")));

        if (!OidIsValid(t_thrd.shemem_ptr_cxt.MyBEEntry->st_userid))
            return true;

        WLMNodeGroupInfo* ng = WLMGetNodeGroupByUserId(t_thrd.shemem_ptr_cxt.MyBEEntry->st_userid);
        if (NULL == ng)
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("Failed to get logic cluster information by user(oid %d).",
                        t_thrd.shemem_ptr_cxt.MyBEEntry->st_userid)));

        if (-1 == gscgroup_check_group_name(ng, p))
            return false;
    }

    return true;
}

static void assign_cgroup_name(const char* newval, void* extra)
{
    /* set "control_group" global variable */
    if (IS_SERVICE_NODE && t_thrd.shemem_ptr_cxt.MyBEEntry && newval && *newval) {
        if (g_instance.wlm_cxt->gscgroup_init_done == 0)
            return;

        if (!OidIsValid(t_thrd.shemem_ptr_cxt.MyBEEntry->st_userid))
            return;

        if (currentGucContext == PGC_SUSET || currentGucContext == PGC_USERSET) {
            WLMNodeGroupInfo* ng = WLMGetNodeGroupByUserId(t_thrd.shemem_ptr_cxt.MyBEEntry->st_userid);
            if (NULL == ng)
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("Failed to get logic cluster information by user(oid %d).",
                            t_thrd.shemem_ptr_cxt.MyBEEntry->st_userid)));

            gscgroup_update_hashtbl(ng, u_sess->wlm_cxt->group_keyname);
            WLMSetControlGroup(u_sess->wlm_cxt->group_keyname);
        }
    }
}

static const char* show_cgroup_name(void)
{
    return u_sess->wlm_cxt->control_group;
}

/*
 * Brief    : Check the value of statistics memory can not bigger than 50 percent of max process memory
 */
static bool check_statistics_memory_limit(int* newval, void** extra, GucSource source)
{
    if (*newval * 2 > g_instance.attr.attr_memory.max_process_memory) {
        GUC_check_errdetail("\"session_statistics_memory\" cannot be greater 50 percent \"max_process_memory\". "
                            "max_process_memory is %dkB",
            g_instance.attr.attr_memory.max_process_memory);
        ereport(WARNING,
            (errmsg("\"session_history_memory\" cannot be greater 50 percent \"max_process_memory\". "
                    "max_process_memory is %dkB",
                g_instance.attr.attr_memory.max_process_memory)));
        return false;
    }

    return true;
}

/* function: assign_active_statements
 * description: assign active statements
 * arguments: standard GUC arguments
 */
static void assign_active_statements(int newval, void* extra)
{
    if (get_guc_context() == PGC_SIGHUP) {
        if (g_instance.wlm_cxt->dynamic_workload_inited)
            dywlm_update_max_statements(newval);
        else
            WLMSetMaxStatements(newval);
    }
}

/*
 * Brief    : Check the value of history memory can not bigger than 50 percent of max process memory
 */
static bool check_history_memory_limit(int* newval, void** extra, GucSource source)
{
    if (*newval * 2 > g_instance.attr.attr_memory.max_process_memory) {
        ereport(WARNING,
            (errmsg("\"session_history_memory\" cannot be greater 50 percent \"max_process_memory\". "
                    "max_process_memory is %dkB",
                g_instance.attr.attr_memory.max_process_memory)));
        GUC_check_errdetail("\"session_history_memory\" cannot be greater 50 percent \"max_process_memory\". "
                            "max_process_memory is %dkB",
            g_instance.attr.attr_memory.max_process_memory);
        return false;
    }

    return true;
}

/*
 * Brief    : assignthe value of history memory to g_statManager.max_detail_num
 */
static void assign_history_memory(int newval, void* extra)
{
    if (currentGucContext == PGC_SIGHUP) {
        u_sess->attr.attr_resource.session_history_memory = newval;
        g_instance.wlm_cxt->stat_manager.max_detail_num = newval * 1024 / sizeof(WLMStmtDetail);
    }
}

static bool check_use_workload_manager(bool* newval, void** extra, GucSource source)
{
    /* disable WLM during inplace upgrade */
    if (u_sess->attr.attr_common.upgrade_mode == 1)
        *newval = false;

    return true;
}

static void assign_use_workload_manager(const bool newval, void* extra)
{
    if (t_thrd.proc_cxt.MyProcPid != PostmasterPid)
        return;

    g_instance.wlm_cxt->gscgroup_init_done =
        newval && u_sess->attr.attr_resource.enable_control_group && g_instance.wlm_cxt->gscgroup_config_parsed;
}

/* function: assign_collect_timer
 * description: assign collect timer
 * arguments: standard GUC arguments
 */
static void assign_collect_timer(int newval, void* extra)
{
const int WLM_COLLECT_INVALID_TIME = 5;
const int WLM_COLLECT_MIN_TIME = 30;

    if (newval < WLM_COLLECT_INVALID_TIME && u_sess->utils_cxt.guc_new_value)
        *u_sess->utils_cxt.guc_new_value = WLM_COLLECT_MIN_TIME;
}

static bool check_query_band_name(char** newval, void** extra, GucSource source)
{
    /* Only allow clean ASCII chars in the application name */
    char* p = NULL;

    for (p = *newval; *p; p++) {
        if (*p < 32 || *p > 126) {
            *p = '?';
        }
    }

    return true;
}

/* function: check_session_respool
 * description: check GUC session_respool is invalid or not
 * arguments:
 *     newval: the input string for checking
 *     extra: the extra information
 *     source: the GUC Source information
 * return value: ture of false
 */
static bool check_session_respool(char** newval, void** extra, GucSource source)
{
    /* Only allow clean ASCII chars in the session_respool name */
    char* p = NULL;

    for (p = *newval; *p; p++) {
        if (*p < 32 || *p > 126) {
            *p = '?';
        }
    }

    p = *newval;

    if (StringIsValid(p) && t_thrd.shemem_ptr_cxt.MyBEEntry &&
        (currentGucContext == PGC_SUSET || currentGucContext == PGC_USERSET))
        WLMCheckSessionRespool(p);

    return true;
}

/* function: assign_session_respool
 * description: to parse the GUC value and assign to the global variable
 * arguments: standard GUC arguments
 */
static void assign_session_respool(const char* newval, void* extra)
{
    /* set "control_group" global variable */
    if (newval && *newval && t_thrd.shemem_ptr_cxt.MyBEEntry &&
        (currentGucContext == PGC_SUSET || currentGucContext == PGC_USERSET))
        WLMSetSessionRespool(newval);
    else
        WLMSetSessionRespool(INVALID_POOL_NAME);
}

/* function: show_session_respool
 * description: to show session_respool
 */
static const char* show_session_respool(void)
{
    return u_sess->wlm_cxt->session_respool;
}

static bool check_auto_explain_level(int* newval, void** extra, GucSource source)
{
    if (*newval == LOG || *newval == NOTICE) {
        return true;
    }
    GUC_check_errdetail("auto_explain_level should be either 'LOG' or 'NOTICE'");
    return false;
}

static const char* show_enable_dynamic_workload(void)
{
    char* buf = t_thrd.buf_cxt.show_enable_dynamic_workload_buf;
    int size = sizeof(t_thrd.buf_cxt.show_enable_dynamic_workload_buf);
    int rcs = 0;

    if (g_instance.wlm_cxt->dynamic_workload_inited != false) {
        rcs = snprintf_s(buf, size, size - 1, "%s", "on");
        securec_check_ss(rcs, "\0", "\0");
    } else {
        rcs = snprintf_s(buf, size, size - 1, "%s", "off");
        securec_check_ss(rcs, "\0", "\0");
    }

    return buf;
}

static void assign_control_group(const bool newval, void* extra)
{
    if (t_thrd.proc_cxt.MyProcPid != PostmasterPid)
        return;
    g_instance.wlm_cxt->gscgroup_init_done =
        newval && u_sess->attr.attr_resource.use_workload_manager && g_instance.wlm_cxt->gscgroup_config_parsed;
}

static void assign_enable_cgroup_switch(bool newval, void* extra)
{
    if (get_guc_context() == PGC_SUSET || get_guc_context() == PGC_USERSET) {
        u_sess->wlm_cxt->cgroup_state = CG_USERSET;
        u_sess->attr.attr_resource.enable_cgroup_switch = newval;
    }
}

/* function: assign_memory_quota
 * description: assign dynamic memory quota
 * arguments: standard GUC arguments
 */
static void assign_memory_quota(int newval, void* extra)
{
    if (get_guc_context() == PGC_SIGHUP && AmPostmasterProcess() && g_instance.wlm_cxt->dynamic_workload_inited) {
        u_sess->attr.attr_resource.dynamic_memory_quota = newval;

        ServerDynamicManager* srvmgr = &g_instance.wlm_cxt->MyDefaultNodeGroup.srvmgr;

        int old_freesize_limit = srvmgr->freesize_limit;
        gs_lock_test_and_set(&srvmgr->freesize_limit,
            srvmgr->totalsize * u_sess->attr.attr_resource.dynamic_memory_quota / FULL_PERCENT);
        gs_compare_and_swap_32(&srvmgr->freesize, old_freesize_limit, srvmgr->freesize_limit);
    }
}
